#!/usr/bin/env python3
"""Acquire and execute an exact managed JobDB publication from a fresh consumer.

Run only after native publication. --merge-receipt is the native merge outputs
JSON (or the preserved native_merge_task wrapper). No Git publication is done.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile

MODULE = "github.com/colony-2/jobdb"
ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("module_receipt", ROOT / "scripts/verify-published-module.py")
receipt_checks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(receipt_checks)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def json_stream(raw):
    decoder, items = json.JSONDecoder(), []
    while raw.strip():
        raw = raw.lstrip()
        item, end = decoder.raw_decode(raw)
        items.append(item)
        raw = raw[end:]
    return items


def publication_hash(receipt):
    native = receipt.get("native_merge_task", receipt)
    require(native.get("status", "succeeded") == "succeeded", "Native merge did not succeed")
    native = native.get("outputs", native)
    commit = native.get("merged_hash", "")
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None, "Native merge receipt needs a full merged_hash")
    require(native.get("publication_disposition") == "published", "Native merge receipt does not record publication")
    require(native.get("publication_branch") == "refs/heads/main", "Native publication must be on main")
    return commit


def check_module(item, version=None):
    require(item.get("Path") == MODULE and not item.get("Replace") and not item.get("Error"),
            f"Wrong, replaced or unavailable JobDB module: {item}")
    require(isinstance(item.get("Version"), str) and re.fullmatch(r"v\d+\.\d+\.\d+[^\s]*", item["Version"]),
            "Missing canonical module version")
    if version is not None:
        require(item["Version"] == version, "Selected module version differs from publication")


def check_download(item, version, commit, sums=None):
    check_module(item, version)
    receipt_checks.validate_origin(item.get("Origin"), commit, "Download")
    for field in ("Sum", "GoModSum"):
        receipt_checks.validate_checksum(item.get(field), field)
        if sums:
            require(item[field] == sums[field], f"Download {field} changed")


def check_test_events(raw):
    events = json_stream(raw)
    # go test ./... also builds the identity executable package, which has no
    # tests. Its package-level skip is not a skipped backend scenario.
    require(not any(e.get("Action") == "fail" or (e.get("Action") == "skip" and
                    (e.get("Test") or e.get("Package") != "example.com/jobdb-publication-acceptance/cmd/identity"))
                    for e in events), "Required consumer tests failed or skipped")
    passed = {e.get("Test") for e in events if e.get("Action") == "pass"}
    require({"TestPublishedGoals", "TestPublishedGoals/toy", "TestPublishedGoals/sqlite"} <= passed,
            "Both required toy/SQLite consumer tests must actually pass")


def check_linked(probe, version, checksum, binary_info):
    linked, current = probe.get("Linked") or {}, probe.get("Current") or {}
    check_module(linked, version)
    require(linked.get("Sum") == checksum, "Linked checksum differs from downloaded module")
    require(current.get("Path") == MODULE and current.get("Version") == version and not current.get("Replacement"),
            "CurrentBuildInfo differs from selected module")
    require(current.get("Revision") is None and current.get("Modified") is None,
            "Dependency build info incorrectly claims consumer VCS identity")
    records = [line.split() for line in binary_info.splitlines()]
    require(["dep", MODULE, version, checksum] in records, "go version -m does not identify exact linked module/checksum")
    require(not any(parts and parts[0] == "=>" for parts in records), "Linked executable contains a replacement")


class Commands:
    def __init__(self, directory, consumer, env):
        self.directory, self.consumer, self.env = directory, consumer, env
        self.records = []

    def run(self, args, optional=False):
        index = len(self.records) + 1
        stem = f"{index:02d}-{args[0]}"
        record = {"args": args, "cwd": str(self.consumer), "allowed_nonzero_for_inspection": optional,
                  "stdout": stem + ".stdout", "stderr": stem + ".stderr"}
        self.records.append(record)
        try:
            result = subprocess.run(args, cwd=self.consumer, env=self.env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=1200)
            out, err, code = result.stdout, result.stderr, result.returncode
        except subprocess.TimeoutExpired as error:
            out, err, code = error.stdout or "", error.stderr or "", -1
            out = out.decode(errors="replace") if isinstance(out, bytes) else out
            err = err.decode(errors="replace") if isinstance(err, bytes) else err
            record["timed_out"] = True
        except OSError as error:
            out, err, code = "", str(error), -1
        (self.directory / record["stdout"]).write_text(out, encoding="utf-8")
        (self.directory / record["stderr"]).write_text(err, encoding="utf-8")
        record["exit_code"] = code
        (self.directory / "commands.json").write_text(json.dumps(self.records, indent=2) + "\n", encoding="utf-8")
        require(optional or code == 0, f"{shlex.join(args)} failed ({code}); see {record['stderr']}")
        return out


def verify(merge_receipt, output, module_receipt=None):
    # Never overwrite earlier failed acquisition evidence.
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {"status": "failed", "stage": "publication receipt", "behavior": "unexecuted"}
    commands = None
    try:
        native = json.loads(merge_receipt.read_text(encoding="utf-8"))
        commit = publication_hash(native)
        result["merged_hash"] = commit
        shutil.copyfile(merge_receipt, output / "native-merge-receipt.json")
        # A new root on every attempt, outside the source tree, with no reused
        # module/build cache. Leave caches entirely under Go's normal control.
        with tempfile.TemporaryDirectory(prefix="jobdb-external-goals-") as scratch:
            root = Path(scratch)
            consumer = root / "consumer"
            consumer.mkdir()
            for name in ("modules", "build", "gopath"):
                (root / name).mkdir()
                require(not any((root / name).iterdir()), f"{name} cache was not empty")
            env = dict(os.environ, GOENV="off", GOWORK="off", GOFLAGS="", GOPROXY="direct", GOCACHEPROG="",
                       GOTOOLCHAIN="local", GOPRIVATE="github.com/colony-2/*",
                       GOMODCACHE=str(root / "modules"), GOCACHE=str(root / "build"), GOPATH=str(root / "gopath"))
            isolation = {key: env[key] for key in ("GOENV", "GOWORK", "GOFLAGS", "GOPROXY", "GOTOOLCHAIN", "GOPRIVATE", "GOMODCACHE", "GOCACHE", "GOCACHEPROG", "GOPATH")}
            isolation["initial_caches_empty"] = True
            (output / "isolation.json").write_text(json.dumps(isolation, indent=2) + "\n", encoding="utf-8")
            commands = Commands(output, consumer, env)
            commands.run(["go", "version"])
            commands.run(["git", "--version"])
            commands.run(["go", "env", "-json", *[k for k in isolation if k != "initial_caches_empty"]])
            commands.run(["go", "mod", "init", "example.com/jobdb-publication-acceptance"])
            result["stage"] = "exact-version acquisition"
            resolved = json.loads(commands.run(["go", "list", "-m", "-json", f"{MODULE}@{commit}"]))
            check_module(resolved)
            receipt_checks.validate_origin(resolved.get("Origin"), commit, "Resolution")
            version = resolved["Version"]
            result["version"] = version
            downloaded = json.loads(commands.run(["go", "mod", "download", "-json", f"{MODULE}@{version}"]))
            check_download(downloaded, version, commit)
            if module_receipt:
                previous = json.loads(module_receipt.read_text(encoding="utf-8"))
                check_download(previous, version, commit, downloaded)
                shutil.copyfile(module_receipt, output / "supplied-module-receipt.json")
            archive = Path(downloaded["Zip"]).resolve()
            require(archive.is_relative_to((root / "modules").resolve()) and archive.is_file() and archive.stat().st_size > 0,
                    "Downloaded archive missing from fresh module cache")
            result["archive_sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
            (output / "module-receipt.json").write_text(json.dumps({"resolution": resolved, "download": downloaded}, indent=2) + "\n", encoding="utf-8")
            # Copy only the two authored consumer sources. No implementation,
            # go.mod, go.sum, generated source, replacement or overlay is copied.
            (consumer / "cmd/identity").mkdir(parents=True)
            for name in ("goal_publication_test.go", "cmd/identity/main.go"):
                shutil.copyfile(ROOT / "tests/published-module" / name, consumer / name)
            commands.run(["go", "get", f"{MODULE}@{version}"])
            commands.run(["go", "mod", "tidy"])
            result["stage"] = "selected dependencies and checksums"
            selected = json.loads(commands.run(["go", "list", "-m", "-json", MODULE]))
            check_module(selected, version)
            require(selected.get("Sum") == downloaded["Sum"] and selected.get("GoModSum") == downloaded["GoModSum"],
                    "Selected checksums differ from downloaded module")
            inventory = json_stream(commands.run(["go", "list", "-m", "-e", "-json", "all"], optional=True))
            result["inventory_exit_code"] = commands.records[-1]["exit_code"]
            require(not any(m.get("Replace") for m in inventory), "Module inventory contains replacements")
            result["unused_inventory_errors"] = [m for m in inventory if m.get("Error")]
            packages = json_stream(commands.run(["go", "list", "-mod=readonly", "-deps", "-test", "-json", "./..."]))
            require(packages and not any(p.get("Error") or p.get("DepsErrors") or p.get("Incomplete") for p in packages),
                    "Required consumer package dependencies are unavailable")
            modules = [p["Module"] for p in packages if p.get("Module")]
            require(not any(m.get("Replace") for m in modules), "Selected package dependencies contain replacements")
            jobdb_modules = [m for m in modules if m.get("Path") == MODULE]
            require(jobdb_modules, "Consumer did not select any JobDB packages")
            for module in jobdb_modules:
                check_module(module, version)
            downloaded_again = json.loads(commands.run(["go", "mod", "download", "-json", MODULE]))
            check_download(downloaded_again, version, commit, downloaded)
            consumer_mod = json.loads(commands.run(["go", "mod", "edit", "-json"]))
            require(not consumer_mod.get("Replace"), "Consumer go.mod contains replacements")
            sums = {tuple(line.split()) for line in (consumer / "go.sum").read_text().splitlines()}
            require((MODULE, version, downloaded["Sum"]) in sums and
                    (MODULE, version + "/go.mod", downloaded["GoModSum"]) in sums, "Consumer go.sum differs from receipt")
            commands.run(["go", "mod", "verify"])
            for name in ("go.mod", "go.sum"):
                shutil.copyfile(consumer / name, output / ("consumer-" + name))
            result["stage"] = "published behavior"
            events = commands.run(["go", "test", "-mod=readonly", "-count=1", "-timeout=5m", "-json", "./..."], optional=True)
            if any(e.get("Action") == "run" and e.get("Test", "").startswith("TestPublishedGoals/") for e in json_stream(events)):
                result["behavior"] = "failed (required execution did not pass)"
            require(commands.records[-1]["exit_code"] == 0, "Published consumer go test failed; inspect captured events and stderr")
            check_test_events(events)
            result["behavior"] = "passed: toy and SQLite, including SQLite reopen"
            result["stage"] = "linked executable identity"
            commands.run(["go", "build", "-mod=readonly", "-o", "identity", "./cmd/identity"])
            binary_info = commands.run(["go", "version", "-m", "./identity"])
            probe = json.loads(commands.run([str(consumer / "identity")]))
            check_linked(probe, version, downloaded["Sum"], binary_info)
            require(commands.run(["go", "env", "GOWORK"]).strip() == "off" and
                    not commands.run(["go", "env", "GOFLAGS"]).strip(), "Consumer isolation changed")
            result.update(status="passed", stage="complete")
    except Exception as error:
        result["error"] = str(error)
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    lines = ["# Published immutable-goal consumer", "", f"Result: **{result['status']}**.", "",
             f"Stage: {result['stage']}. Behavior: {result['behavior']}.", "",
             "This checks bounded JobDB source publication, not deployment or downstream backend/integration completion.", "",
             "```json", json.dumps(result, indent=2), "```", "", "Commands and raw results:", ""]
    for record in commands.records if commands else []:
        lines.append(f"- `{shlex.join(record['args'])}` → {record['exit_code']}; [{record['stdout']}]({record['stdout']}), [{record['stderr']}]({record['stderr']}).")
    (output / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--merge-receipt", required=True, type=Path)
    parser.add_argument("--module-receipt", type=Path, help="Optional native exact-version Go download JSON to compare")
    parser.add_argument("--output", required=True, type=Path, help="New evidence directory; existing attempts are never overwritten")
    args = parser.parse_args()
    raise SystemExit(verify(args.merge_receipt, args.output, args.module_receipt))
