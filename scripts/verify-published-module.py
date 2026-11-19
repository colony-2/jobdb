#!/usr/bin/env python3
"""Verify normal Go module consumption of the managed publication's exact commit."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

MODULE = "github.com/colony-2/jobdb"


def validate_origin(origin, commit, label):
    if (not isinstance(origin, dict) or origin.get("VCS") != "git"
            or not isinstance(origin.get("URL"), str) or not origin["URL"].strip()
            or origin.get("Hash") != commit):
        raise ValueError(f"{label} origin must identify Git, a nonblank URL and publication {commit}: {origin}")


def validate_checksum(value, label):
    try:
        if not isinstance(value, str) or not value.startswith("h1:"):
            raise ValueError("missing h1 prefix")
        digest = base64.b64decode(value[3:], validate=True)
        if len(digest) != 32 or base64.b64encode(digest).decode("ascii") != value[3:]:
            raise ValueError("expected a base64 SHA-256 digest")
    except ValueError as error:
        raise ValueError(f"Download {label} is not a valid Go h1 checksum: {value!r}") from error


def verify(commit, report):
    lines = ["# Published JobDB module consumption", "", f"Commit: `{commit}`", ""]
    try:
        if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
            raise ValueError("A full immutable Git commit is required")
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        if head != commit:
            raise ValueError(f"Managed checkout {head} differs from publication {commit}")
        with tempfile.TemporaryDirectory(prefix="jobdb-published-consumer-") as scratch:
            root = Path(scratch)
            consumer, cache = root / "consumer", root / "modules"
            consumer.mkdir()
            cache.mkdir()
            if any(cache.iterdir()):
                raise ValueError("Module cache must start empty")
            env = dict(os.environ, GOWORK="off", GOFLAGS="", GOMODCACHE=str(cache),
                       GOTOOLCHAIN="local", GOPRIVATE="github.com/colony-2/*", GOPROXY="direct")
            lines += ["Fresh external consumer; initially empty module cache; `GOWORK=off`;",
                      "empty `GOFLAGS`; `GOPROXY=direct`; no replacement.", ""]

            def run(args):
                result = subprocess.run(args, cwd=consumer, env=env, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        timeout=300)
                if result.returncode:
                    raise RuntimeError(f"{' '.join(args)} failed ({result.returncode}):\n"
                                       f"{result.stdout}\n{result.stderr}")
                return result.stdout

            lines += [f"Toolchain: `{run(['go', 'version']).strip()}`", ""]
            run(["go", "mod", "init", "example.invalid/jobdb-published-consumer"])
            resolved = json.loads(run(["go", "list", "-m", "-json", f"{MODULE}@{commit}"]))
            lines += [f"Resolved version: `{resolved.get('Version')}`", "",
                      "Resolution origin:", "", "```json",
                      json.dumps(resolved.get("Origin"), ensure_ascii=False, indent=2), "```", ""]
            if resolved.get("Replace") or resolved.get("Path") != MODULE:
                raise ValueError("Resolution used a replacement or a different module")
            validate_origin(resolved.get("Origin"), commit, "Resolution")
            version = resolved.get("Version")
            if not isinstance(version, str) or not version.strip():
                raise ValueError("Resolution returned a blank or missing version")
            downloaded = json.loads(run(["go", "mod", "download", "-json", f"{MODULE}@{version}"]))
            lines += [f"Downloaded version: `{downloaded.get('Version')}`", "",
                      "Download origin:", "", "```json",
                      json.dumps(downloaded.get("Origin"), ensure_ascii=False, indent=2), "```", "",
                      f"Sum: `{downloaded.get('Sum')}`", "",
                      f"GoModSum: `{downloaded.get('GoModSum')}`", ""]
            if (downloaded.get("Error") or downloaded.get("Replace")
                    or downloaded.get("Path") != MODULE or downloaded.get("Version") != version):
                raise ValueError(f"Module download failed: {downloaded}")
            validate_origin(downloaded.get("Origin"), commit, "Download")
            for field in ("Sum", "GoModSum"):
                validate_checksum(downloaded.get(field), field)
            archive = Path(downloaded["Zip"]).resolve()
            if not archive.is_relative_to(cache.resolve()) or not archive.is_file() or archive.stat().st_size == 0:
                raise ValueError("Downloaded module archive is missing from the fresh cache")
            lines += [f"Downloaded archive: {archive.stat().st_size} bytes.", "",
                      f"Archive SHA-256: `{hashlib.sha256(archive.read_bytes()).hexdigest()}`", ""]
            run(["go", "mod", "edit", f"-require={MODULE}@{version}"])
            examples = Path("pkg/jobdb/example_execution_infrastructure_test.go").read_text(encoding="utf-8")
            # The consumer's package has no JobDB identifiers of its own. Keep
            # the copied examples executable without binding their names to it.
            examples = examples.replace("package jobdb_test", "package main")
            examples = examples.replace("func Example", "func Example_jobdb")
            (consumer / "execution_test.go").write_text(examples, encoding="utf-8")
            (consumer / "main.go").write_text('''package main
import (
 "context"
 "errors"
 "fmt"
 "os"
 "github.com/colony-2/jobdb/pkg/jobdb"
)
func main() {
 info:=jobdb.CurrentBuildInfo()
 if info.Path!="github.com/colony-2/jobdb" || info.Version!=os.Getenv("EXPECTED_JOBDB_VERSION") || info.Replacement!=nil {panic(fmt.Sprintf("unexpected linked build: %+v",info))}
 cause:=errors.New("local I/O cause")
 err:=jobdb.ClassifyExecutionInfrastructure(cause,jobdb.ExecutionInfrastructureRuntimeIO,"example",nil,nil)
 if !jobdb.IsExecutionInfrastructureError(err) || !errors.Is(err,cause) {panic("classifier lost cause")}
 _,err=jobdb.ReadExecutionProvenance(context.Background(),struct{}{},jobdb.ReadExecutionProvenanceRequest{})
 if !errors.Is(err,jobdb.ErrExecutionProvenanceUnsupported) {panic("optional capability missing")}
 fmt.Println(info.Version)
}
''', encoding="utf-8")
            run(["go", "mod", "tidy"])
            run(["go", "test", "-count=1", "./..."])
            env["EXPECTED_JOBDB_VERSION"] = version
            observed = run(["go", "run", "."]).strip()
            if observed != version:
                raise ValueError(f"Linked build version {observed} differs from {version}")
            # go.mod must still be a plain consumer with no replace directives.
            consumer_mod = json.loads(run(["go", "mod", "edit", "-json"]))
            if consumer_mod.get("Replace"):
                raise ValueError("Consumer contains module replacements")
            if run(["go", "env", "GOWORK"]).strip() != "off" or run(["go", "env", "GOFLAGS"]).strip():
                raise ValueError("Workspace or GOFLAGS isolation was lost")
            lines += ["Commands completed:", "",
                      f"- `go list -m -json {MODULE}@{commit}`",
                      f"- `go mod download -json {MODULE}@{version}`", "",
                      "Public classifier, optional reader and build-info examples compiled and ran.", "",
                      f"Linked CurrentBuildInfo version: `{observed}`", "",
                      "- `go test -count=1 ./...`", "- `go run .`", "",
                      "Result: passed."]
        status = 0
    except Exception as error:
        lines += ["Result: failed.", "", "```text", str(error), "```"]
        status = 1
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return status


if __name__ == "__main__":
    sys.exit(verify(sys.argv[1], Path(sys.argv[2])))
