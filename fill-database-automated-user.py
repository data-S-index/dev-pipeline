"""Fill database with automated user (author) data from NDJSON using psycopg3 for fast bulk inserts."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


BATCH_SIZE = 10000


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
            """COPY "AutomatedUser" (id, "nameType", name, "nameIdentifiers", affiliations)
               FROM STDIN"""
        ) as copy:
            for row in user_rows:
                copy.write_row(row)
    conn.commit()


def insert_automated_user_datasets_batch(
    conn: psycopg.Connection, link_rows: List[tuple]
) -> None:
    """Insert a batch of AutomatedUserDataset rows using COPY."""
    if not link_rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """COPY "AutomatedUserDataset" ("automatedUserId", "datasetId", created, updated)
               FROM STDIN"""
        ) as copy:
            for row in link_rows:
                copy.write_row(row)
    conn.commit()


def _count_records(ndjson_files: List[Path]) -> int:
    """Count total non-empty lines across ndjson files."""
    total = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        total += 1
        except Exception:
            continue
    return total


def _count_link_rows(ndjson_files: List[Path]) -> int:
    """Count total lines (link rows) across AutomatedUserDataset ndjson files."""
    total = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        total += 1
        except Exception:
            continue
    return total


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

                        name_type = record.get("nameType")
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
                                name_type,
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


def step2_insert_automated_user_datasets(
    conn: psycopg.Connection, automateduserdataset_dir: Path
) -> int:
    """Step 2: Read AutomatedUserDataset NDJSON and batch-insert all link rows."""
    print("\n📦 Step 2: Inserting AutomatedUserDataset...")

    ndjson_files = load_ndjson_files(automateduserdataset_dir)
    if not ndjson_files:
        return 0

    total_links_to_insert = _count_link_rows(ndjson_files)
    print(f"  Processing {total_links_to_insert:,} user-dataset link rows...")
    link_rows: List[tuple] = []
    total_links = 0
    now = datetime.now()
    pbar = tqdm(
        total=total_links_to_insert,
        desc="  AutomatedUserDataset",
        unit="link",
        unit_scale=True,
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
                        automated_user_id = record.get("automatedUserId")
                        dataset_id = record.get("datasetId")
                        if automated_user_id is None or dataset_id is None:
                            continue
                        try:
                            automated_user_id = int(automated_user_id)
                            dataset_id = int(dataset_id)
                        except (TypeError, ValueError):
                            continue

                        link_rows.append(
                            (automated_user_id, dataset_id, now, now)
                        )
                        total_links += 1
                        pbar.update(1)

                        if len(link_rows) >= BATCH_SIZE:
                            insert_automated_user_datasets_batch(conn, link_rows)
                            link_rows = []

                    except json.JSONDecodeError:
                        pass
                    except Exception:
                        pass

        except Exception as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")

    pbar.close()
    if link_rows:
        insert_automated_user_datasets_batch(conn, link_rows)

    print(f"  ✅ Inserted {total_links:,} AutomatedUserDataset rows")
    return total_links


def main() -> None:
    """Main function to fill database with automated user data."""
    print("🚀 Starting automated user database fill...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    database_dir = downloads_dir / "database"
    authors_dir = database_dir / "authors"
    automateduserdataset_dir = database_dir / "automateduserdataset"

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
                cur.execute('TRUNCATE TABLE "AutomatedUser" CASCADE')
                conn.commit()
            print("  ✅ Tables truncated")

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
