#!/usr/bin/env python3
"""Reject empty, failed, skipped, or incomplete local resource acceptance runs."""
import json
import re
import sys
from pathlib import Path

# PostgreSQL B06b/P01–P04 and postpublication R01/R02 have separate owners/stages.
REQUIRED = set("""
B01 B01a B01b B01c B01d B01e B02 B02a B02b B02c
B03 B03a B03b B03c B03d B03e B03f B04 B04a B05 B05a B05b
B06 B06a B07 B07a B08 B08a B09 B09a B09b B10 B10a B10b
B11 B11a B12 B13 B13a B13b B13c B14 B14a B15 B15a B15b
B15c B15d B16 B16a B16b B17 B17a M01 M02 M03 M04 S01 S02 C01 C01a C02 H01
""".split())

# Require these exact real backend cases as well as the original statement IDs.
# Another B09/B11 test must not silently substitute for the deadline regressions.
DEADLINE_REGRESSIONS = {
    "B09b_deadline_keeps_resource_gate",
    "B11a_zero_credit_deadline_job_target",
    "B11a_zero_credit_deadline_job_poll",
    "B11a_zero_credit_deadline_task_target",
    "B11a_zero_credit_deadline_task_poll",
    "B11_zero_credit_engine_timeout_external_false",
    "B11_zero_credit_engine_timeout_external_true",
}

def check(paths):
    events = []
    for path in paths:
        for line in Path(path).read_text().splitlines():
            if line.startswith("{"):
                events.append(json.loads(line))
    # The public toy package is a forwarding package with no test files. Its
    # internal implementation must still execute the real conformance suite.
    empty_toy = "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
    failures = [e for e in events if e.get("Action") == "fail" or
                e.get("Action") == "skip" and
                not (e.get("Package") == empty_toy and "Test" not in e and
                     any(o.get("Package") == empty_toy and "[no test files]" in o.get("Output", "") for o in events))]
    passed = {(e.get("Package", ""), e["Test"]) for e in events
              if e.get("Action") == "pass" and "Test" in e}
    observed = set()
    for _, name in passed:
        observed.update(re.findall(r"(?:B\d{2}[a-z]?|M\d{2}|S\d{2}|C\d{2}[a-z]?|H01)(?![A-Za-z0-9])", name))
    missing = sorted(REQUIRED - observed)
    # Retain the real read-revocation controls; an ID alone cannot stand in for them.
    retained = {"TestRetainedReadPermissionDenied", "TestRetainedReadInfrastructureControl", "TestRetainedReaderRevocation"}
    missing += sorted(retained - {name for _, name in passed})
    for suffix in ("runtime/sqlite", "runtime/toy/internal/toyimpl"):
        if not any(p.endswith(suffix) and n.startswith("TestResourceBackoffConformance/") for p,n in passed):
            missing.append(suffix + " conformance")
        for name in sorted(DEADLINE_REGRESSIONS):
            if not any(p.endswith(suffix) and n == "TestResourceBackoffConformance/" + name for p, n in passed):
                missing.append(suffix + "/" + name)
    if failures or missing or not passed:
        raise ValueError(f"resource acceptance failed: failures/skips={failures}; missing={missing}")
    print(f"Resource acceptance passed: {len(passed)} selected named tests/subtests; {len(REQUIRED)} planned resource IDs and C03 retained-read controls; no failed or skipped test cases (the public toy forwarding package has no test files).")
    return passed

if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            raise ValueError("at least one Go JSON test result is required")
        check(sys.argv[1:])
    except (ValueError, OSError) as error:
        sys.exit(str(error))
