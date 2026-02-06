"""Fill database with automated organization data from NDJSON using psycopg3 for fast bulk inserts."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


BATCH_SIZE = 1000


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def load_ndjson_files(directory: Path) -> List[Path]:
    """Load and sort ndjson files from directory."""
    files = list(directory.glob("*.ndjson"))
    return sorted(files, key=natural_sort_key)


def insert_automated_organizations_batch(
    conn: psycopg.Connection,
    org_rows: List[tuple],
    link_rows: List[tuple],
) -> None:
    """Insert a batch of AutomatedOrganization and AutomatedOrganizationDataset rows using COPY."""
    with conn.cursor() as cur:
        if org_rows:
            with cur.copy(
                """COPY "AutomatedOrganization" (id, name)
                   FROM STDIN"""
            ) as copy:
                for row in org_rows:
                    copy.write_row(row)
        if link_rows:
            total_links = len(link_rows)
            with tqdm(
                total=total_links,
                desc="  Copying link rows",
                unit="link",
                unit_scale=True,
                leave=False,
            ) as link_pbar:
                with cur.copy(
                    """COPY "AutomatedOrganizationDataset" ("automatedOrganizationId", "datasetId", created, updated)
                       FROM STDIN"""
                ) as copy:
                    for start in range(0, total_links, BATCH_SIZE):
                        batch = link_rows[start : start + BATCH_SIZE]
                        for row in batch:
                            copy.write_row(row)
                        link_pbar.update(len(batch))
        conn.commit()


def process_organization_files(
    conn: psycopg.Connection, organizations_dir: Path
) -> tuple[int, int]:
    """Process organization NDJSON files and insert AutomatedOrganization and AutomatedOrganizationDataset."""
    print("📦 Processing automated organization files...")

    ndjson_files = load_ndjson_files(organizations_dir)
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

    print(f"  Processing {total_records:,} organization records...")

    org_rows: List[tuple] = []
    link_rows: List[tuple] = []
    total_orgs = 0
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

                        org_id = record.get("id")
                        if not org_id:
                            tqdm.write(
                                f"    ⚠️  Skipping record without id in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        name = record.get("name") or ""

                        org_rows.append((org_id, name))
                        total_orgs += 1

                        dataset_ids = record.get("datasetIds") or []
                        if not isinstance(dataset_ids, list):
                            dataset_ids = []
                        for dataset_id in dataset_ids:
                            link_rows.append((org_id, dataset_id, now, now))
                            total_links += 1

                        pbar.update(1)

                        if len(org_rows) >= BATCH_SIZE:
                            insert_automated_organizations_batch(
                                conn, org_rows, link_rows
                            )
                            org_rows = []
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

    if org_rows or link_rows:
        insert_automated_organizations_batch(conn, org_rows, link_rows)

    return total_orgs, total_links


def main() -> None:
    """Main function to fill database with automated organization data."""
    print("🚀 Starting automated organization database fill...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    organizations_dir = downloads_dir / "database" / "organizations"

    print(f"Organizations directory: {organizations_dir}")

    if not organizations_dir.exists():
        raise FileNotFoundError(
            f"Organizations directory not found: {organizations_dir}. "
            "Please run generate-organizations.py first."
        )

    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            print("\n🗑️  Truncating automated organization tables...")
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE "AutomatedOrganization" CASCADE')
                conn.commit()
            print("  ✅ Tables truncated")

            org_count, link_count = process_organization_files(conn, organizations_dir)

            print("\n✅ Automated organization database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - AutomatedOrganization rows: {org_count:,}")
            print(f"  - AutomatedOrganizationDataset rows: {link_count:,}")

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
