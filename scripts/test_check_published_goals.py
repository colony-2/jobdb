"""Rejection tests for the external checker; these do not publish a module."""
import base64
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("published_goals", Path(__file__).with_name("check-published-goals.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
COMMIT = "a" * 40
SUM = "h1:" + base64.b64encode(b"a" * 32).decode()
MODSUM = "h1:" + base64.b64encode(b"b" * 32).decode()
VERSION = "v0.0.0-20261120230000-" + COMMIT[:12]


class PublishedGoalsChecks(unittest.TestCase):
    def setUp(self):
        self.download = {"Path": checker.MODULE, "Version": VERSION, "Sum": SUM, "GoModSum": MODSUM,
                         "Origin": {"VCS": "git", "URL": "https://github.com/colony-2/jobdb", "Hash": COMMIT}}
        self.native = {"merged_hash": COMMIT, "publication_disposition": "published", "publication_branch": "refs/heads/main"}

    def test_native_publication_required(self):
        self.assertEqual(checker.publication_hash(self.native), COMMIT)
        for field, value in (("merged_hash", "snapshot"), ("publication_disposition", "prepared"),
                             ("publication_branch", "refs/heads/candidate")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                checker.publication_hash(dict(self.native, **{field: value}))
        with self.assertRaises(ValueError):
            checker.publication_hash({"status": "failed", "outputs": self.native})

    def test_exact_origin_version_and_checksums(self):
        checker.check_download(self.download, VERSION, COMMIT)
        for changes in ({"Version": "v0.0.1"}, {"Path": "example.com/other"}, {"Replace": {"Path": "/tmp/source"}},
                        {"Error": "unavailable"}, {"Sum": "h1:invalid"}, {"GoModSum": ""},
                        {"Origin": {"VCS": "git", "URL": "origin", "Hash": "b" * 40}}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                checker.check_download(dict(self.download, **changes), VERSION, COMMIT)
        with self.assertRaisesRegex(ValueError, "changed"):
            checker.check_download(dict(self.download, Sum=MODSUM), VERSION, COMMIT, self.download)

    def test_required_leaf_tests_must_execute_without_skips(self):
        events = [{"Action": "pass", "Test": test} for test in
                  ("TestPublishedGoals", "TestPublishedGoals/toy", "TestPublishedGoals/sqlite")]
        encode = lambda items: "\n".join(json.dumps(e) for e in items)
        checker.check_test_events(encode(events))
        for bad in (events[:-1], [], events + [{"Action": "skip", "Test": "TestPublishedGoals/sqlite"}],
                    events + [{"Action": "fail"}]):
            with self.subTest(events=bad), self.assertRaises(ValueError):
                checker.check_test_events(encode(bad))

    def test_linked_identity_and_checksum(self):
        probe = {"Linked": {"Path": checker.MODULE, "Version": VERSION, "Sum": SUM},
                 "Current": {"Path": checker.MODULE, "Version": VERSION}}
        binary = f"\tdep\t{checker.MODULE}\t{VERSION}\t{SUM}\n"
        checker.check_linked(probe, VERSION, SUM, binary)
        for path, value in (("Sum", MODSUM), ("Version", "v0.0.1"), ("Replace", {"Path": "/tmp/source"})):
            bad = copy.deepcopy(probe)
            bad["Linked"][path] = value
            with self.subTest(field=path), self.assertRaises(ValueError):
                checker.check_linked(bad, VERSION, SUM, binary)
        with self.assertRaises(ValueError):
            checker.check_linked(probe, VERSION, SUM, binary + "\t=>\t/tmp/source\n")
        for changes in ({"Replacement": {"Path": "/tmp/source"}}, {"Revision": COMMIT}, {"Version": "v0.0.1"}):
            bad = copy.deepcopy(probe)
            bad["Current"].update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                checker.check_linked(bad, VERSION, SUM, binary)

    def test_identity_package_without_tests_does_not_skip_required_cases(self):
        events = [{"Action": "pass", "Test": test} for test in
                  ("TestPublishedGoals", "TestPublishedGoals/toy", "TestPublishedGoals/sqlite")]
        events.append({"Action": "skip", "Package": "example.com/jobdb-publication-acceptance/cmd/identity"})
        encode = lambda items: "\n".join(json.dumps(e) for e in items)
        checker.check_test_events(encode(events))
        with self.assertRaises(ValueError):
            checker.check_test_events(encode(events[1:]))
        events[-1]["Test"] = "TestPublishedGoals/sqlite"
        with self.assertRaises(ValueError):
            checker.check_test_events(encode(events))

    def test_subprocess_failure_preserves_actual_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = checker.Commands(root, root, {})
            with patch.object(checker.subprocess, "run", return_value=subprocess.CompletedProcess(
                    ["go"], 1, "partial output", "acquisition denied")):
                with self.assertRaisesRegex(ValueError, "failed.*1"):
                    commands.run(["go", "list", "module@hash"])
            self.assertEqual((root / "01-go.stderr").read_text(), "acquisition denied")
            self.assertEqual(json.loads((root / "commands.json").read_text())[0]["exit_code"], 1)

    def test_timeout_is_failure_with_partial_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = checker.Commands(root, root, {})
            with patch.object(checker.subprocess, "run", side_effect=subprocess.TimeoutExpired(["go"], 1, b"partial", b"timeout")):
                with self.assertRaises(ValueError):
                    commands.run(["go", "test"])
            self.assertTrue(commands.records[0]["timed_out"])
            self.assertEqual((root / "01-go.stdout").read_text(), "partial")

    def test_failed_acquisition_leaves_behavior_unexecuted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = root / "native.json"
            receipt.write_text(json.dumps(self.native))
            def run(args, **kwargs):
                failed = "list" in args
                return subprocess.CompletedProcess(args, 1 if failed else 0, "", "origin unavailable" if failed else "")
            with patch.object(checker.subprocess, "run", side_effect=run):
                self.assertEqual(checker.verify(receipt, root / "evidence"), 1)
            result = json.loads((root / "evidence/result.json").read_text())
            self.assertEqual(result["stage"], "exact-version acquisition")
            self.assertEqual(result["behavior"], "unexecuted")
            with self.assertRaises(FileExistsError):
                checker.verify(receipt, root / "evidence")


if __name__ == "__main__":
    unittest.main()
