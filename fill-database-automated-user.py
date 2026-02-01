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
    conn: psycopg.Connection,
    user_rows: List[tuple],
    link_rows: List[tuple],
) -> None:
    """Insert a batch of AutomatedUser and AutomatedUserDataset rows using COPY."""
    with conn.cursor() as cur:
        if user_rows:
            with cur.copy(
                """COPY "AutomatedUser" (id, "nameType", name, "nameIdentifiers", affiliations)
                   FROM STDIN"""
            ) as copy:
                for row in user_rows:
                    copy.write_row(row)
        if link_rows:
            with cur.copy(
                """COPY "AutomatedUserDataset" ("automatedUserId", "datasetId", created, updated)
                   FROM STDIN"""
            ) as copy:
                for row in link_rows:
                    copy.write_row(row)
        conn.commit()


def process_author_files(
    conn: psycopg.Connection, authors_dir: Path
) -> tuple[int, int]:
    """Process author NDJSON files and insert AutomatedUser and AutomatedUserDataset."""
    print("📦 Processing automated user (author) files...")

    ndjson_files = load_ndjson_files(authors_dir)
    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0, 0

    print(f"  Found {len(ndjson_files)} ndjson file(s)")

    total_records = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        total_records += 1
        except Exception:
            continue

    print(f"  Processing {total_records:,} author records...")

    user_rows: List[tuple] = []
    link_rows: List[tuple] = []
    total_users = 0
    total_links = 0
    now = datetime.now()

    pbar = tqdm(
        total=total_records, desc="  Processing", unit="record", unit_scale=True
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
                        if not user_id:
                            tqdm.write(
                                f"    ⚠️  Skipping record without id in {file_path.name}"
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

                        dataset_ids = record.get("datasetIds") or []
                        if not isinstance(dataset_ids, list):
                            dataset_ids = []
                        for dataset_id in dataset_ids:
                            link_rows.append((user_id, dataset_id, now, now))
                            total_links += 1

                        pbar.update(1)

                        if len(user_rows) >= BATCH_SIZE:
                            insert_automated_users_batch(conn, user_rows, link_rows)
                            user_rows = []
                            link_rows = []

                    except json.JSONDecodeError as e:
                        tqdm.write(
                            f"    ⚠️  Error parsing line in {file_path.name}: {e}"
                        )
                        pbar.update(1)
                        continue
                    except Exception as e:
                        tqdm.write(
                            f"    ⚠️  Error processing record in {file_path.name}: {e}"
                        )
                        pbar.update(1)
                        continue

        except Exception as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

    pbar.close()

    if user_rows or link_rows:
        insert_automated_users_batch(conn, user_rows, link_rows)

    return total_users, total_links


def main() -> None:
    """Main function to fill database with automated user data."""
    print("🚀 Starting automated user database fill...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    authors_dir = downloads_dir / "database" / "authors"

    print(f"Authors directory: {authors_dir}")

    if not authors_dir.exists():
        raise FileNotFoundError(
            f"Authors directory not found: {authors_dir}. "
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

            user_count, link_count = process_author_files(conn, authors_dir)

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
