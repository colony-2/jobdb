import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("resource_check", Path(__file__).with_name("check-resource-tests.py"))
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class ResourceInventoryTest(unittest.TestCase):
    def run_check(self, events):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tests.jsonl"
            path.write_text("\n".join(json.dumps(e) for e in events))
            with contextlib.redirect_stdout(io.StringIO()):
                return checker.check([path])

    def complete(self):
        events = [{"Action": "pass", "Package": "unit", "Test": "TestResourceBackoffControl" + name} for name in checker.REQUIRED]
        events += [{"Action": "pass", "Package": "remote", "Test": name} for name in ["TestRetainedReadPermissionDenied", "TestRetainedReadInfrastructureControl", "TestRetainedReaderRevocation"]]
        events += [{"Action": "pass", "Package": "github.com/colony-2/jobdb/pkg/jobdb/" + suffix, "Test": "TestResourceBackoffConformance/B06_exact"} for suffix in ["runtime/sqlite", "runtime/toy/internal/toyimpl"]]
        events += [{"Action": "pass", "Package": "github.com/colony-2/jobdb/pkg/jobdb/" + suffix, "Test": "TestResourceBackoffConformance/" + name} for suffix in ["runtime/sqlite", "runtime/toy/internal/toyimpl"] for name in checker.DEADLINE_REGRESSIONS]
        return events

    def test_deadline_regressions_required_for_each_backend(self):
        for suffix in ["runtime/sqlite", "runtime/toy/internal/toyimpl"]:
            for name in checker.DEADLINE_REGRESSIONS:
                events = [e for e in self.complete() if not (e["Package"].endswith(suffix) and e["Test"] == "TestResourceBackoffConformance/" + name)]
                with self.subTest(backend=suffix, case=name), self.assertRaises(ValueError):
                    self.run_check(events)

    def test_nonempty_complete_and_suffix_ids(self):
        self.assertTrue(self.run_check(self.complete()))

    def test_empty_failed_skipped_or_missing_rejected(self):
        complete = self.complete()
        missing_id = [event for event in complete if event["Test"] != "TestResourceBackoffControlB01"]
        missing_deadline = [event for event in complete if not (
            event["Package"].endswith("runtime/sqlite") and
            event["Test"] == "TestResourceBackoffConformance/B09b_deadline_keeps_resource_gate"
        )]
        cases = {
            "empty": [],
            "missing_id": missing_id,
            "failed": complete + [{"Action": "fail", "Package": "unit"}],
            "skipped": complete + [{"Action": "skip", "Package": "unit", "Test": "required"}],
            "missing_deadline": missing_deadline,
        }
        for reason, events in cases.items():
            with self.subTest(reason=reason), self.assertRaises(ValueError):
                self.run_check(events)

    def test_only_empty_public_toy_package_may_have_no_tests(self):
        package = "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
        skip = {"Action": "skip", "Package": package}
        with self.assertRaises(ValueError):
            self.run_check(self.complete() + [skip])
        self.run_check(self.complete() + [{"Action": "output", "Package": package, "Output": "? toy [no test files]"}, skip])
        with self.assertRaises(ValueError):
            self.run_check(self.complete() + [{"Action": "skip", "Package": package, "Test": "required"}])
