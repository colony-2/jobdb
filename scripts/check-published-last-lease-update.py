#!/usr/bin/env python3
"""Verify both list routes from the exact JobDB commit on GitHub main."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile

ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("published_goals", ROOT / "scripts/check-published-goals.py")
publication = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publication)
MODULE = publication.MODULE
GITHUB = f"https://{MODULE}.git"
TEST = "TestPublishedLastLeaseUpdateLists"


def verify(receipt, output):
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {"status": "failed", "stage": "managed publication receipt"}
    try:
        commit = publication.publication_hash(json.loads(receipt.read_text(encoding="utf-8")))
        result["commit"] = commit
        shutil.copyfile(receipt, output / "native-merge-receipt.json")
        with tempfile.TemporaryDirectory(prefix="jobdb-last-lease-consumer-") as scratch:
            root = Path(scratch)
            consumer = root / "consumer"
            consumer.mkdir()
            github_checkout = root / "github"
            github_checkout.mkdir()
            env = {name: value for name, value in os.environ.items()
                   if not name.startswith("GIT_CONFIG") and name not in
                   {"GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "JOBDB_LAST_LEASE_BASELINE"}}
            env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull, GIT_CONFIG_NOSYSTEM="1",
                       GIT_TERMINAL_PROMPT="0", GOENV="off", GOWORK="off", GOFLAGS="", GOCACHEPROG="",
                       GOTOOLCHAIN="local", GOPRIVATE=MODULE, GONOPROXY=MODULE, GONOSUMDB=MODULE,
                       GOPROXY="https://proxy.golang.org,direct", GOMODCACHE=str(root / "modules"),
                       GOCACHE=str(root / "build"), GOPATH=str(root / "gopath"))
            for name in ("GOMODCACHE", "GOCACHE", "GOPATH"):
                Path(env[name]).mkdir()
            commands = publication.Commands(output, consumer, env)
            run = commands.run
            result["stage"] = "GitHub main ancestry (no Git config rewrites)"
            main = run(["git", "ls-remote", "--exit-code", GITHUB, "refs/heads/main"]).split()
            publication.require(len(main) == 2 and main[1] == "refs/heads/main", "GitHub main ref is missing")
            result["github_main"] = main[0]
            if main[0] != commit:
                run(["git", "-C", str(github_checkout), "init", "-q"])
                run(["git", "-C", str(github_checkout), "fetch", "--filter=blob:none", GITHUB, "refs/heads/main"])
                run(["git", "-C", str(github_checkout), "merge-base", "--is-ancestor", commit, "FETCH_HEAD"])
                result["github_main"] = run(["git", "-C", str(github_checkout), "rev-parse", "FETCH_HEAD"]).strip()
            result["stage"] = "fresh Go module resolution and download"
            run(["go", "mod", "init", "example.invalid/jobdb-last-lease-consumer"])
            resolved = json.loads(run(["go", "list", "-m", "-json", f"{MODULE}@{commit}"]))
            publication.check_module(resolved)
            version = resolved["Version"]
            result["version"] = version
            downloaded = json.loads(run(["go", "mod", "download", "-json", f"{MODULE}@{version}"]))
            publication.check_download(downloaded, version, commit)
            for module in (resolved, downloaded):
                origin = module.get("Origin")
                publication.receipt_checks.validate_origin(origin, commit, "JobDB")
                publication.require(origin["URL"] in (GITHUB, GITHUB.removesuffix(".git")),
                                    f"Unexpected JobDB origin: {origin['URL']}")
            archive = Path(downloaded["Zip"]).resolve()
            publication.require(archive.is_relative_to(Path(env["GOMODCACHE"])) and archive.stat().st_size > 0,
                                "No downloaded archive in the fresh module cache")
            run(["go", "mod", "edit", f"-require={MODULE}@{version}"])
            fixture = ROOT / "pkg/jobdb/runtime/remote/last_lease_update_http_test.go"
            (consumer / "list_test.go").write_text(fixture.read_text(encoding="utf-8").replace(
                "package remote_test", "package consumer", 1), encoding="utf-8")
            run(["go", "mod", "tidy"])
            selected = json.loads(run(["go", "list", "-m", "-json", MODULE]))
            publication.check_module(selected, version)
            mod = json.loads(run(["go", "mod", "edit", "-json"]))
            publication.require(not mod.get("Replace") and run(["go", "env", "GOWORK"]).strip() == "off",
                                "Consumer selected a workspace or module replacement")
            result["stage"] = "published HTTP list contract"
            events = publication.json_stream(run(["go", "test", "-mod=readonly", "-count=1", "-json", "-run", f"^{TEST}$", "."]))
            passed = {event.get("Test") for event in events if event.get("Action") == "pass"}
            publication.require({TEST, f"{TEST}/tenants", f"{TEST}/projects"} <= passed,
                                "The published tenant and project cases did not both pass")
            result.update(status="passed", stage="complete")
    except Exception as error:
        result["error"] = str(error)
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--merge-receipt", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    raise SystemExit(verify(args.merge_receipt, args.output))
