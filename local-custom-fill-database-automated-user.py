"""Fill database with automated user (author) data from NDJSON using psycopg3 for fast bulk inserts."""

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


BATCH_SIZE = 50_000
# AutomatedUserDataset is ~200M+ rows; batch much larger so we commit far less
# often (commits are the dominant per-row overhead once indexes/FKs are dropped).
LINK_BATCH_SIZE = 1_000_000

# Records are always written as {"automatedUserId":123,"datasetId":456} by
# generate-authors.py, so we can skip json.loads (which dominates CPU time at
# this row count) and pull the two ints out with a precompiled regex instead.
_LINK_RE = re.compile(r'"automatedUserId"\s*:\s*(-?\d+)\s*,\s*"datasetId"\s*:\s*(-?\d+)')

# Constraints on AutomatedUserDataset, dropped before the bulk load and
# rebuilt afterwards. Rebuilding a PK index/FK in one pass over the finished
# table is far cheaper than maintaining them on every one of ~200M inserts.
_AUTOMATED_USER_DATASET_CONSTRAINTS = [
    (
        "AutomatedUserDataset_pkey",
        'ALTER TABLE "AutomatedUserDataset" ADD CONSTRAINT "AutomatedUserDataset_pkey" '
        'PRIMARY KEY ("automatedUserId", "datasetId")',
    ),
    (
        "AutomatedUserDataset_automatedUserId_fkey",
        'ALTER TABLE "AutomatedUserDataset" ADD CONSTRAINT "AutomatedUserDataset_automatedUserId_fkey" '
        'FOREIGN KEY ("automatedUserId") REFERENCES "AutomatedUser"(id) ON UPDATE CASCADE ON DELETE CASCADE',
    ),
    (
        "AutomatedUserDataset_datasetId_fkey",
        'ALTER TABLE "AutomatedUserDataset" ADD CONSTRAINT "AutomatedUserDataset_datasetId_fkey" '
        'FOREIGN KEY ("datasetId") REFERENCES "Dataset"(id) ON UPDATE CASCADE ON DELETE CASCADE',
    ),
]


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def load_ndjson_files(directory: Path) -> List[Path]:
    """Load and sort ndjson files from directory."""
    files = list(directory.glob("*.ndjson"))
    return sorted(files, key=natural_sort_key)


def insert_automated_users_batch(
    conn: psycopg.Connection, user_rows: List[tuple]
) -> None:
    """Insert a batch of AutomatedUser rows using COPY."""
    if not user_rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """COPY "AutomatedUser" (id, name, "nameIdentifiers", affiliations)
               FROM STDIN"""
        ) as copy:
            for row in user_rows:
                copy.write_row(row)
    conn.commit()


def _open_link_copy(conn: psycopg.Connection):
    """Open a cursor + COPY stream that rows can be written to one at a time.

    Kept open across many files/rows so parsed rows go straight onto the
    wire (true streaming) instead of being buffered into a Python list and
    handed to COPY in one shot.

    cursor.copy() is a @contextmanager, so it returns a generator-based
    context manager whose __enter__() yields the actual Copy object — that
    return value (not the context manager) is what has .write_row().
    """
    cur = conn.cursor()
    copy_ctx = cur.copy(
        """COPY "AutomatedUserDataset" ("automatedUserId", "datasetId", created, updated)
           FROM STDIN"""
    )
    copy = copy_ctx.__enter__()
    return cur, copy_ctx, copy


def _close_link_copy(conn: psycopg.Connection, cur, copy_ctx) -> None:
    """Finish the COPY stream and commit, closing off a checkpoint."""
    copy_ctx.__exit__(None, None, None)
    cur.close()
    conn.commit()


def _fast_count_lines(file_path: Path) -> int:
    """Count newlines by scanning raw bytes (no decode/strip per line).

    Used only to size the progress bar, so an off-by-one on a missing
    trailing newline doesn't matter; this is orders of magnitude faster
    than iterating the file in text mode for files with 100k+ lines.
    """
    total = 0
    with open(file_path, "rb") as f:
        while buf := f.read(1 << 20):
            total += buf.count(b"\n")
    return total


def _count_records(ndjson_files: List[Path]) -> int:
    """Count total lines across ndjson files."""
    total = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            total += _fast_count_lines(file_path)
        except Exception:
            continue
    return total


def _count_link_rows(ndjson_files: List[Path]) -> int:
    """Count total lines (link rows) across AutomatedUserDataset ndjson files."""
    return _count_records(ndjson_files)


def step1_insert_automated_users(conn: psycopg.Connection, authors_dir: Path) -> int:
    """Step 1: Read author NDJSON and batch-insert all AutomatedUser rows."""
    print("📦 Step 1: Inserting AutomatedUser...")

    ndjson_files = load_ndjson_files(authors_dir)
    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0

    total_records = _count_records(ndjson_files)
    print(f"  Processing {total_records:,} author records...")

    user_rows: List[tuple] = []
    total_users = 0
    pbar = tqdm(
        total=total_records, desc="  AutomatedUser", unit="record", unit_scale=True
    )

    for file_path in ndjson_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                        user_id = record.get("id")
                        if user_id is None:
                            tqdm.write(
                                f"    ⚠️  Skipping record without id in {file_path.name}"
                            )
                            pbar.update(1)
                            continue
                        try:
                            user_id = int(user_id)
                        except (TypeError, ValueError):
                            tqdm.write(
                                f"    ⚠️  Skipping record with non-int id in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        name = record.get("name") or ""
                        name_identifiers = record.get("nameIdentifiers") or []
                        if not isinstance(name_identifiers, list):
                            name_identifiers = []
                        affiliations = record.get("affiliations") or []
                        if not isinstance(affiliations, list):
                            affiliations = []

                        user_rows.append(
                            (
                                user_id,
                                name,
                                name_identifiers,
                                affiliations,
                            )
                        )
                        total_users += 1
                        pbar.update(1)

                        if len(user_rows) >= BATCH_SIZE:
                            insert_automated_users_batch(conn, user_rows)
                            user_rows = []

                    except json.JSONDecodeError as e:
                        tqdm.write(
                            f"    ⚠️  Error parsing line in {file_path.name}: {e}"
                        )
                        pbar.update(1)
                    except Exception as e:
                        tqdm.write(
                            f"    ⚠️  Error processing record in {file_path.name}: {e}"
                        )
                        pbar.update(1)

        except Exception as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")

    pbar.close()
    if user_rows:
        insert_automated_users_batch(conn, user_rows)

    # Ensure sequence is past max id so future inserts don't conflict
    with conn.cursor() as cur:
        cur.execute(
            '''SELECT setval(pg_get_serial_sequence('"AutomatedUser"', 'id'),
                            COALESCE((SELECT MAX(id) FROM "AutomatedUser"), 1))'''
        )
        conn.commit()

    print(f"  ✅ Inserted {total_users:,} AutomatedUser rows")
    return total_users


def _constraint_exists(conn: psycopg.Connection, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_constraint WHERE conname = %s", (name,))
        return cur.fetchone() is not None


def drop_automated_user_dataset_constraints(conn: psycopg.Connection) -> None:
    """Drop the PK + FKs on AutomatedUserDataset before the bulk load.

    Maintaining a composite PK btree and validating two FKs on every one of
    ~200M inserts is the dominant cost. Dropping them and rebuilding once
    at the end (a single sorted index build + two set-based FK scans) is
    dramatically faster.
    """
    print("  🔧 Dropping PK/FK constraints on AutomatedUserDataset...")
    with conn.cursor() as cur:
        cur.execute(
            'ALTER TABLE "AutomatedUserDataset" '
            'DROP CONSTRAINT IF EXISTS "AutomatedUserDataset_automatedUserId_fkey"'
        )
        cur.execute(
            'ALTER TABLE "AutomatedUserDataset" '
            'DROP CONSTRAINT IF EXISTS "AutomatedUserDataset_datasetId_fkey"'
        )
        cur.execute(
            'ALTER TABLE "AutomatedUserDataset" '
            'DROP CONSTRAINT IF EXISTS "AutomatedUserDataset_pkey"'
        )
    conn.commit()


def restore_automated_user_dataset_constraints(conn: psycopg.Connection) -> None:
    """Rebuild the PK and FKs on AutomatedUserDataset after the bulk load.

    If the source data contains duplicate (automatedUserId, datasetId)
    pairs, the PRIMARY KEY rebuild below is what will raise — earlier than
    that there's nothing left to enforce uniqueness during the load itself.
    """
    print("  🔧 Rebuilding PK/FK constraints on AutomatedUserDataset...")
    try:
        with conn.cursor() as cur:
            cur.execute("SET maintenance_work_mem = '1GB'")
    except psycopg.Error:
        conn.rollback()

    for name, ddl in _AUTOMATED_USER_DATASET_CONSTRAINTS:
        if _constraint_exists(conn, name):
            continue
        label = "primary key" if name.endswith("_pkey") else "foreign key"
        print(f"    Building {label} {name}...")
        started = time.time()
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        print(f"    ✅ {name} ready ({time.time() - started:.1f}s)")


def step2_insert_automated_user_datasets(
    conn: psycopg.Connection, automateduserdataset_dir: Path
) -> int:
    """Step 2: Stream AutomatedUserDataset NDJSON straight into a COPY pipe."""
    print("\n📦 Step 2: Inserting AutomatedUserDataset...")

    ndjson_files = load_ndjson_files(automateduserdataset_dir)
    if not ndjson_files:
        return 0

    total_links_to_insert = _count_link_rows(ndjson_files)
    print(f"  Processing {total_links_to_insert:,} user-dataset link rows...")

    drop_automated_user_dataset_constraints(conn)

    total_links = 0
    now = datetime.now()
    pbar = tqdm(
        total=total_links_to_insert,
        desc="  AutomatedUserDataset",
        unit="link",
        unit_scale=True,
    )

    rows_since_checkpoint = 0
    pending_pbar = 0
    cur, copy_ctx, copy = _open_link_copy(conn)

    try:
        for file_path in ndjson_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        if match := _LINK_RE.search(line):
                            automated_user_id = int(match.group(1))
                            dataset_id = int(match.group(2))
                        else:
                            # Fallback for any line that doesn't match the
                            # expected shape (e.g. hand-edited/legacy files).
                            try:
                                record = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            automated_user_id = record.get("automatedUserId")
                            dataset_id = record.get("datasetId")
                            if automated_user_id is None or dataset_id is None:
                                continue
                            try:
                                automated_user_id = int(automated_user_id)
                                dataset_id = int(dataset_id)
                            except (TypeError, ValueError):
                                continue

                        copy.write_row((automated_user_id, dataset_id, now, now))
                        total_links += 1
                        rows_since_checkpoint += 1
                        pending_pbar += 1

                        if pending_pbar >= 20_000:
                            pbar.update(pending_pbar)
                            pending_pbar = 0

                        if rows_since_checkpoint >= LINK_BATCH_SIZE:
                            _close_link_copy(conn, cur, copy_ctx)
                            cur, copy_ctx, copy = _open_link_copy(conn)
                            rows_since_checkpoint = 0

            except Exception as e:
                tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
    finally:
        _close_link_copy(conn, cur, copy_ctx)
        if pending_pbar:
            pbar.update(pending_pbar)
        pbar.close()

    print(f"  ✅ Inserted {total_links:,} AutomatedUserDataset rows")

    restore_automated_user_dataset_constraints(conn)

    return total_links


def main() -> None:
    """Main function to fill database with automated user data."""
    print("🚀 Starting automated user database fill...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    automated_author_dir = downloads_dir / "pulled-database" / "automated-author"
    authors_dir = automated_author_dir / "authors"
    automateduserdataset_dir = automated_author_dir / "automateduserdataset"

    print(f"Authors directory: {authors_dir}")
    print(f"AutomatedUserDataset directory: {automateduserdataset_dir}")

    if not authors_dir.exists():
        raise FileNotFoundError(
            f"Authors directory not found: {authors_dir}. "
            "Please run generate-authors.py first."
        )
    if not automateduserdataset_dir.exists():
        raise FileNotFoundError(
            f"AutomatedUserDataset directory not found: {automateduserdataset_dir}. "
            "Please run generate-authors.py first."
        )

    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            print("\n🗑️  Truncating automated user tables...")
            with conn.cursor() as cur:
                # Name both tables explicitly (not just CASCADE from
                # AutomatedUser) since a prior interrupted run may have left
                # the AutomatedUserDataset FK dropped, which would make
                # CASCADE silently skip it.
                cur.execute(
                    'TRUNCATE TABLE "AutomatedUser", "AutomatedUserDataset" CASCADE'
                )
                conn.commit()
            print("  ✅ Tables truncated")

            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off")

            user_count = step1_insert_automated_users(conn, authors_dir)
            link_count = step2_insert_automated_user_datasets(
                conn, automateduserdataset_dir
            )

            print("\n✅ Automated user database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - AutomatedUser rows: {user_count:,}")
            print(f"  - AutomatedUserDataset rows: {link_count:,}")

    except psycopg.Error as e:
        print(f"\n❌ Database error: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
