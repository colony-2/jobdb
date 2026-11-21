"""Receipt acceptance fixtures; these do not publish or prove module availability."""
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "published_module", Path(__file__).with_name("verify-published-module.py"))
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)
COMMIT = "a" * 40
VERSION = "v0.0.0-20261119120000-aaaaaaaaaaaa"
ARCHIVE = b"downloaded archive fixture"
SUM = "h1:" + base64.b64encode(bytes(range(32))).decode("ascii")
MOD_SUM = "h1:" + base64.b64encode(bytes(reversed(range(32)))).decode("ascii")


class PublishedModuleTest(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.report = Path(self.scratch.name) / "report.md"
        origin = {"VCS": "git", "URL": "https://github.com/colony-2/jobdb", "Hash": COMMIT}
        self.resolved = {"Path": helper.MODULE, "Version": VERSION, "Origin": dict(origin)}
        self.downloaded = {"Path": helper.MODULE, "Version": VERSION, "Origin": dict(origin),
                           "Sum": SUM, "GoModSum": MOD_SUM}
        self.archive_mode = "valid"
        self.failure = None
        self.linked_version = VERSION
        self.consumer_mod = {}
        self.workspace, self.flags = "off", ""
        self.calls = []
        self.expected_transport = {}

    def run_command(self, args, **kwargs):
        self.calls.append(args)
        consumer = Path(kwargs["cwd"])
        env = kwargs["env"]
        cache = Path(env["GOMODCACHE"])
        self.assertNotEqual(consumer, Path.cwd())
        self.assertEqual(consumer.parent, cache.parent)
        self.assertEqual(env["GOWORK"], "off")
        self.assertEqual(env["GOFLAGS"], "")
        self.assertEqual(env["GOPROXY"], "direct")
        self.assertEqual(env["GOTOOLCHAIN"], "local")
        self.assertEqual(env["GOPRIVATE"], "github.com/colony-2/*")
        for key, value in self.expected_transport.items():
            self.assertEqual(env[key], value, key)
        if args == ["go", "version"]:
            self.assertEqual(list(cache.iterdir()), [])
        if self.failure and args[:len(self.failure[0])] == self.failure[0]:
            result = self.failure[1]
            if isinstance(result, Exception):
                raise result
            return subprocess.CompletedProcess(args, 1, "", result)
        output = ""
        if args == ["go", "version"]:
            output = "go version go1.26.8 linux/arm64"
        elif args[:4] == ["go", "list", "-m", "-json"]:
            self.assertEqual(args[4], helper.MODULE + "@" + COMMIT)
            output = json.dumps(self.resolved)
        elif args[:4] == ["go", "mod", "download", "-json"]:
            self.assertEqual(args[4], helper.MODULE + "@" + VERSION)
            archive = cache / "module.zip"
            if self.archive_mode == "outside":
                archive = consumer / "outside.zip"
            if self.archive_mode != "missing":
                archive.write_bytes(b"" if self.archive_mode == "empty" else ARCHIVE)
            self.downloaded["Zip"] = str(archive)
            output = json.dumps(self.downloaded)
        elif args == ["go", "mod", "edit", "-json"]:
            output = json.dumps(self.consumer_mod)
        elif args == ["go", "env", "GOWORK"]:
            output = self.workspace
        elif args == ["go", "env", "GOFLAGS"]:
            output = self.flags
        elif args == ["go", "test", "-count=1", "./..."]:
            self.assertEqual(env["EXPECTED_JOBDB_VERSION"], VERSION)
            fixture = (consumer / "append_test.go").read_text()
            self.assertIn("TestPublishedCancelledReads", fixture)
            self.assertIn("TestPublishedSemanticAppend", fixture)
            output = "ok example.invalid/jobdb-published-consumer"
        elif args == ["go", "run", "."]:
            self.assertEqual(env["EXPECTED_JOBDB_VERSION"], VERSION)
            self.assertIn("CurrentBuildInfo", (consumer / "main.go").read_text())
            self.assertIn("func Example_jobdb", (consumer / "execution_test.go").read_text())
            output = self.linked_version
        elif args not in (["go", "mod", "init", "example.invalid/jobdb-published-consumer"],
                          ["go", "mod", "edit", "-require=" + helper.MODULE + "@" + VERSION],
                          ["go", "mod", "tidy"], ["go", "test", "-count=1", "./..."]):
            self.fail(f"Unexpected command: {args}")
        return subprocess.CompletedProcess(args, 0, output, "")

    def verify(self, expected=0, commit=COMMIT, head=COMMIT, message=None):
        with patch.object(helper.subprocess, "check_output", return_value=head), \
                patch.object(helper.subprocess, "run", side_effect=self.run_command):
            status = helper.verify(commit, self.report)
        report = self.report.read_text()
        self.assertEqual(status, expected, report)
        self.assertIn("Result: passed." if expected == 0 else "Result: failed.", report)
        if message:
            self.assertIn(message, report)
        return report

    def test_complete_receipt_and_isolation(self):
        # Full objects survive independently, without requiring optional Ref/Subdir.
        self.verify()
        self.resolved["Origin"]["FutureField"] = {"雪": [1, True]}
        self.downloaded["Origin"]["Ref"] = "refs/heads/main"
        self.downloaded["Origin"]["Subdir"] = ""
        self.downloaded["Origin"]["FutureField"] = ["independent", {"value": None}]
        report = self.verify()
        origins = [json.loads(block.split("```", 1)[0]) for block in report.split("```json\n")[1:]]
        self.assertEqual(origins, [self.resolved["Origin"], self.downloaded["Origin"]])
        for value in (COMMIT, VERSION, SUM, MOD_SUM, hashlib.sha256(ARCHIVE).hexdigest()):
            self.assertIn(value, report)
        self.assertIn(f"Downloaded archive: {len(ARCHIVE)} bytes.", report)
        self.assertIn(f"Downloaded version: `{VERSION}`", report)
        self.assertIn(["go", "test", "-count=1", "./..."], self.calls)
        self.assertIn(["go", "run", "."], self.calls)

    def test_direct_git_routing_preserves_transport(self):
        # Public and private GitHub modules share the operator-managed Git route.
        self.expected_transport = {
            "GONOPROXY": "operator.example/*", "GONOSUMDB": "operator.example/*",
            "PUBLICATION_TEST_TRANSPORT": "preserved",
        }
        inherited = dict(self.expected_transport, GOPROXY="https://proxy.invalid",
                         GOPRIVATE="*", GOWORK="/inherited/go.work",
                         GOFLAGS="-modfile=/inherited/go.mod", GOTOOLCHAIN="auto")
        with patch.dict(os.environ, inherited):
            report = self.verify()
        self.assertIn("GOPROXY=direct", report)
        self.assertNotIn("proxy.golang.org", report)

    def test_commit_and_checkout_rejected_before_resolution(self):
        for commit, head in (("main", COMMIT), ("b" * 40, COMMIT)):
            with self.subTest(commit=commit):
                self.calls = []
                self.verify(1, commit=commit, head=head)
                self.assertEqual(self.calls, [])

    def test_origin_identity_required_independently(self):
        for name in ("resolved", "downloaded"):
            for field, value in (("VCS", None), ("VCS", "hg"), ("URL", None),
                                 ("URL", " \t"), ("Hash", "b" * 40)):
                with self.subTest(observation=name, field=field, value=value):
                    origin = getattr(self, name)["Origin"]
                    old = origin[field]
                    origin[field] = value
                    self.verify(1, message="origin must identify Git")
                    origin[field] = old
            with self.subTest(observation=name, missing=True):
                old = getattr(self, name).pop("Origin")
                self.verify(1, message="origin must identify Git")
                getattr(self, name)["Origin"] = old

    def test_go_checksums_required(self):
        for field in ("Sum", "GoModSum"):
            for value in (None, "", "h2:" + SUM[3:], "h1:!!!", "h1:YWJj", "h1:" + SUM[3:] + "="):
                with self.subTest(field=field, value=value):
                    old = self.downloaded.pop(field)
                    if value is not None:
                        self.downloaded[field] = value
                    self.verify(1, message=field + " is not a valid Go h1 checksum")
                    self.downloaded[field] = old

    def test_module_and_version_validation(self):
        cases = (("resolved", "Path", "other"), ("downloaded", "Path", "other"),
                 ("resolved", "Version", None), ("resolved", "Version", ""),
                 ("resolved", "Version", " \t"), ("downloaded", "Version", ""),
                 ("downloaded", "Version", "other"), ("downloaded", "Error", "download failed"),
                 ("resolved", "Replace", {"Dir": "/workspace"}),
                 ("downloaded", "Replace", {"Dir": "/workspace"}))
        for name, field, value in cases:
            with self.subTest(observation=name, field=field, value=value):
                data = getattr(self, name)
                old = dict(data)
                if value is None:
                    data.pop(field)
                else:
                    data[field] = value
                self.verify(1)
                data.clear()
                data.update(old)

    def test_archive_must_be_nonempty_and_in_fresh_cache(self):
        for mode in ("missing", "empty", "outside"):
            with self.subTest(mode=mode):
                self.archive_mode = mode
                self.verify(1, message="archive is missing from the fresh cache")

    def test_command_failure_timeout_and_invalid_json(self):
        prefix = ["go", "list", "-m", "-json"]
        for args, error in ((prefix, "resolution failed"),
                            (prefix, subprocess.TimeoutExpired(prefix, 300)),
                            (["go", "mod", "download"], "download failed"),
                            (["go", "test"], "external cancellation/append behavior failed"),
                            (["go", "run"], "external consumer failed")):
            with self.subTest(command=args, error=error):
                self.failure = (args, error)
                report = self.verify(1, message=str(error) if isinstance(error, str) else "timed out")
                if args[:2] in (["go", "test"], ["go", "run"]):
                    self.assertIn(SUM, report)
                    self.assertIn("Download origin:", report)
        self.failure = None
        self.resolved = "not a module object"
        self.verify(1)
        with patch.object(helper.json, "loads", side_effect=json.JSONDecodeError("invalid JSON", "x", 0)):
            self.verify(1, message="invalid JSON")

    def test_linked_version_and_isolation_leakage_fail(self):
        self.linked_version = "wrong"
        self.verify(1, message="Linked build version")
        self.linked_version = VERSION
        self.consumer_mod = {"Replace": [{"New": {"Path": "/workspace"}}]}
        self.verify(1, message="Consumer contains module replacements")
        self.consumer_mod = {}
        for workspace, flags in (("/workspace/go.work", ""), ("off", "-modfile=local.mod")):
            self.workspace, self.flags = workspace, flags
            self.verify(1, message="Workspace or GOFLAGS isolation was lost")


if __name__ == "__main__":
    unittest.main()
