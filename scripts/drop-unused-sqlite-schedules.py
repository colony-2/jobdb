#!/usr/bin/env python3
"""Drop the unused recurring-schedule table while JobDB is stopped."""

import argparse
from pathlib import Path
import sqlite3


def cleanup(path: Path) -> bool:
    # mode=rw prevents a typo from creating a new database.
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=rw", uri=True, isolation_level=None)
    try:
        db.execute("BEGIN IMMEDIATE")
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "jobdb_jobs" not in tables:
            raise ValueError("not a JobDB SQLite database")
        if "jobdb_schedules" not in tables:
            db.commit()
            return False
        if db.execute("SELECT EXISTS(SELECT 1 FROM jobdb_schedules)").fetchone()[0]:
            raise ValueError("schedule table is not empty; no changes made")
        if db.execute("""SELECT EXISTS(SELECT 1 FROM jobdb_jobs
            WHERE CASE WHEN json_valid(metadata)
                THEN json_type(metadata, '$.internal.schedule') NOT IN ('null')
                ELSE 0 END)""").fetchone()[0]:
            raise ValueError("scheduled job metadata exists; no changes made")
        db.execute("DROP TABLE jobdb_schedules")
        db.commit()
        return True
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    try:
        changed = cleanup(args.database)
    except (sqlite3.Error, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print("Dropped jobdb_schedules and its indexes." if changed else "No schedule table; nothing to do.")


if __name__ == "__main__":
    main()
