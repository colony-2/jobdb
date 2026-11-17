#!/usr/bin/env python3
"""Execute a two-file test overlay against an unchanged exact-pin runtime.

All downloads, vendoring and compilation happen in a new disposable directory.
A local source-dir run verifies compatibility, but cannot certify publication.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

MODULE = "github.com/colony-2/jobdb"
PIN = "v0.0.87-0.20261116191523-78a27e5f3db6"
ORIGINAL = {
    "pkg/jobdb/runtimetest/submission_availability.go": "36eb77e8bbb0ccd9473b98e2d8142e742e9d6bac046d04449ef774c58124c9e7",
    "pkg/jobdb/runtimetest/restart_submission.go": "bc07fe4f837681515950a243ade67d50c7bfd3c7f393b70fb3956a9d4130e400",
}
SELECTION = "TestBackport/(submission_availability|submission_restart_boundaries|submission_restart_execution)$|TestDelegatedAvailability"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(root):
    return {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob("*")) if p.is_file()}


def unchanged(root, expected):
    actual = inventory(root)
    if actual != expected:
        delta = sorted(k for k in actual.keys() | expected.keys() if actual.get(k) != expected.get(k))
        raise ValueError(f"integrity changed: {delta}")


def validate_overlay(mapping, vendor, replacements):
    expected = {str((vendor / name).resolve()): str((replacements / name).resolve()) for name in ORIGINAL}
    if mapping != {"Replace": expected}:
        raise ValueError("overlay must contain exactly the two approved absolute source mappings")


def validate_pin(selected):
    if selected.get("Version") != PIN or selected.get("Replace"):
        raise ValueError("exact runtime pin required; replacement modules prohibited")


def rejection_controls():
    # Exercise the same filesystem verifier used after Go execution.
    with tempfile.TemporaryDirectory(prefix="jobdb-integrity-controls-") as tmp:
        root = Path(tmp)
        for name in ("source.go", "go.mod", "replacement.go"):
            (root / name).write_text("original")
        expected = inventory(root)
        rejected = []
        for name in expected:
            (root / name).write_text("corrupt")
            try:
                unchanged(root, expected)
            except ValueError:
                rejected.append(name)
            finally:
                (root / name).write_text("original")
        (root / "extra.go").write_text("extra")
        try:
            unchanged(root, expected)
        except ValueError:
            rejected.append("extra inventory")
        (root / "extra.go").unlink()
        (root / "source.go").unlink()
        try:
            unchanged(root, expected)
        except ValueError:
            rejected.append("missing inventory")
        assert len(rejected) == 5, rejected
        vendor, replacements = root / "vendor", root / "replacement"
        good = {"Replace": {str((vendor / n).resolve()): str((replacements / n).resolve()) for n in ORIGINAL}}
        validate_overlay(good, vendor, replacements)
        variants = [{"Replace": {}}, {"Replace": {**good["Replace"], str(root / "extra.go"): "extra"}}, {"Replace": {"relative.go": "wrong.go"}}]
        for bad in variants:
            try:
                validate_overlay(bad, vendor, replacements)
            except ValueError:
                rejected.append("overlay bounds")
        assert len(rejected) == 8, rejected
        validate_pin({"Version": PIN})
        for bad in ({"Version": "v0.0.86"}, {"Version": PIN, "Replace": {"Dir": "/tmp/foreign"}}):
            try:
                validate_pin(bad)
            except ValueError:
                rejected.append("runtime pin/replacement")
        assert len(rejected) == 10, rejected
        return rejected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--revision", help="published full 40-hex Git commit; fetched independently")
    source.add_argument("--source-dir", type=Path, help="unpublished candidate compatibility check only")
    parser.add_argument("--repository", default="https://github.com/colony-2/jobdb.git")
    parser.add_argument("--availability-sha256", required=True)
    parser.add_argument("--restart-sha256", required=True)
    parser.add_argument("--work-dir", type=Path, required=True, help="new, disposable output directory")
    parser.add_argument("--consumer", type=Path, help="copy an existing consumer and its unchanged module graph")
    args = parser.parse_args()
    if args.revision and not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("revision must be a full immutable commit")
    expected_results = dict(zip(ORIGINAL, (args.availability_sha256, args.restart_sha256)))
    if any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in expected_results.values()):
        parser.error("replacement hashes must be full SHA-256 values")
    if os.environ.get("GOWORK", "off") not in ("", "off"):
        parser.error("workspace substitution is prohibited; set GOWORK=off")
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOWORK="off", GOMODCACHE=str(work / "modcache"), GOPATH=str(work / "gopath"), GOFLAGS="-mod=readonly", GOPRIVATE="github.com/colony-2/*")

    def run(command, cwd=work, capture=False):
        print("+", " ".join(map(str, command)), flush=True)
        return subprocess.run(list(map(str, command)), cwd=cwd, env=env, check=True, text=True, stdout=subprocess.PIPE if capture else None).stdout

    corrections = work / "corrections"
    corrections.mkdir()
    if args.revision:
        checkout = work / "published"
        run(["git", "init", checkout])
        run(["git", "fetch", "--depth=1", args.repository, args.revision], checkout)
        actual_revision = run(["git", "rev-parse", "FETCH_HEAD"], checkout, True).strip()
        if actual_revision != args.revision:
            raise ValueError("fetch resolved a different revision")
        for name in ORIGINAL:
            dest = corrections / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            # Read bytes without text newline/encoding conversion.
            with dest.open("wb") as out:
                subprocess.run(["git", "show", f"{args.revision}:{name}"], cwd=checkout, check=True, stdout=out)
    else:
        for name in ORIGINAL:
            dest = corrections / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(args.source_dir.resolve() / name, dest)
    if inventory(corrections) != expected_results:
        raise ValueError("replacement source hashes do not match")

    consumer = work / "consumer"
    if args.consumer:
        shutil.copytree(args.consumer.resolve(), consumer, ignore=shutil.ignore_patterns(".git", "vendor"))
    else:
        consumer.mkdir()
        (consumer / "go.mod").write_text(f"module example.test/jobdb-backport\n\ngo 1.26.0\n\nrequire {MODULE} {PIN}\n")
    scaffold = consumer / "jobdbcorrectionvalidation"
    scaffold.mkdir(exist_ok=False)
    shutil.copyfile(Path(__file__).parent / "conformance-backport/backport_test.go.txt", scaffold / "backport_test.go")
    if not args.consumer:
        # Resolve the standalone fixture's graph once, before its immutable baseline.
        run(["go", "mod", "tidy"], consumer)
    metadata = {name: (consumer / name).read_bytes() for name in ("go.mod", "go.sum")}
    modules = json.loads(run(["go", "mod", "download", "-json", MODULE], consumer, True))
    # Check the selected graph as well as the requested version, including replaces.
    selected = json.loads(run(["go", "list", "-m", "-json", MODULE], consumer, True))
    validate_pin(selected)
    validate_pin(modules)
    pinned = Path(modules["Dir"])
    if any(digest(pinned / n) != sha for n, sha in ORIGINAL.items()):
        raise ValueError("original pinned source hash mismatch")
    run(["go", "mod", "vendor"], consumer)
    vendor = consumer / "vendor" / MODULE
    vendor_before = inventory(consumer / "vendor")
    jobdb_before = inventory(vendor)
    # Go vendor intentionally excludes dependency tests and module files. Compare
    # every included byte and reject any source absent from the exact module.
    for name, sha in jobdb_before.items():
        if not (pinned / name).is_file() or digest(pinned / name) != sha:
            raise ValueError(f"vendored JobDB differs from exact module: {name}")
    if any(jobdb_before.get(n) != sha for n, sha in ORIGINAL.items()):
        raise ValueError("original vendored source hash mismatch")
    mapping = {"Replace": {str((vendor / n).resolve()): str((corrections / n).resolve()) for n in ORIGINAL}}
    validate_overlay(mapping, vendor, corrections)
    overlay = work / "overlay.json"
    overlay.write_text(json.dumps(mapping, indent=2) + "\n")
    controls = rejection_controls()
    command = ["go", "test", "-mod=vendor", f"-overlay={overlay}", "-json", "-count=1", "-run", SELECTION, "./jobdbcorrectionvalidation"]
    print("+", " ".join(command), flush=True)
    with (work / "tests.json").open("w") as output, (work / "tests.stderr").open("w") as error_output:
        status = subprocess.run(command, cwd=consumer, env=env, stdout=output, stderr=error_output).returncode
    # Check even after a failing acceptance run. Overlays never write vendor files.
    unchanged(consumer / "vendor", vendor_before)
    unchanged(vendor, jobdb_before)
    unchanged(corrections, expected_results)
    validate_overlay(json.loads(overlay.read_text()), vendor, corrections)
    for name, value in metadata.items():
        if (consumer / name).read_bytes() != value:
            raise ValueError(f"module metadata changed: {name}")
    events = [json.loads(line) for line in (work / "tests.json").read_text().splitlines() if line.startswith("{")]
    passes = {e["Test"] for e in events if e.get("Action") == "pass" and "Test" in e}
    failed = [e.get("Test", e.get("Package")) for e in events if e.get("Action") == "fail"]
    skipped = [e["Test"] for e in events if e.get("Action") == "skip" and "Test" in e]
    required = set()
    for kind in ("toy", "sqlite", "http-toy", "http-sqlite"):
        required.add(f"TestBackport/submission_restart_boundaries/{kind}")
        for state in ("ready", "cancelled", "executed"):
            required.add(f"TestBackport/submission_restart_execution/{kind}/{state}")
        for parent in ("false", "true"):
            for case in availability_cases():
                required.add(f"TestBackport/submission_availability/{kind}/parent={parent}/{case}")
    for kind in ("toy", "sqlite"):
        for case in availability_cases():
            required.add(f"TestDelegatedAvailability/{kind}/{case}")
    missing = sorted(required - passes)
    report = {"runtime_version": PIN, "correction_revision": args.revision, "publication_verified": bool(args.revision),
              "original_sha256": ORIGINAL, "result_sha256": expected_results,
              "immutable_source_locations": {n: f"https://github.com/colony-2/jobdb/blob/{args.revision}/{n}" for n in ORIGINAL} if args.revision else {},
              "test_exit": status, "passed_test_nodes": len(passes), "required_cases": len(required), "missing_cases": missing,
              "failed": failed, "skipped": skipped, "vendored_jobdb_files_verified": len(jobdb_before),
              "all_vendor_files_verified": len(vendor_before), "integrity": "passed", "integrity_rejection_controls": controls}
    (work / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if status or failed or skipped or missing:
        raise SystemExit("exact-pin acceptance FAILED; inspect tests.json/tests.stderr/result.json")


def availability_cases():
    cases = [f"{kind}/time-first={first}/poll={poll}" for kind in ("completion", "exact-success", "goal-success", "all-of") for first in ("false", "true") for poll in ("false", "true")]
    return cases + [f"{when}/poll={poll}" for when in ("omitted", "past", "equal") for poll in ("false", "true")]


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        print(json.dumps({"mechanism_controls": rejection_controls()}, indent=2))
    else:
        main()
