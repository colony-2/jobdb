#!/usr/bin/env python3
"""Go -exec launcher: isolate embedded PostgreSQL tests and never run PG as root.

Other test packages retain their source working directory. This helper accepts
only the executable and arguments supplied by `go test`, never hook inputs.
The enclosing verification operation owns and removes TMPDIR and its children.
"""
import os
from pathlib import Path
import pwd
import shutil
import sys
import tempfile

binary = Path(sys.argv[1]).resolve()
if binary.name not in {"postgres.test", "configuration.test"}:
    os.execv(str(binary), [str(binary), *sys.argv[2:]])

scratch = Path(tempfile.mkdtemp(prefix="postgres-test-", dir=os.environ["TMPDIR"]))
executable = scratch / binary.name
shutil.copy2(binary, executable)
# Without a home directory embedded-postgres caches beneath the isolated cwd.
# Do not touch the worker's home, global Git configuration, or credentials.
os.environ.pop("HOME", None)
os.environ["TMPDIR"] = str(scratch)
if os.geteuid() == 0:
    account = pwd.getpwnam("nobody")
    for path in (scratch, executable):
        os.chown(path, account.pw_uid, account.pw_gid)
    os.setgroups([])
    os.setgid(account.pw_gid)
    os.setuid(account.pw_uid)
os.chdir(scratch)
os.execv(str(executable), [str(executable), *sys.argv[2:]])
