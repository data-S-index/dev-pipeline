"""Fill database with automated organization data from NDJSON using psycopg3 for fast bulk inserts."""

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


def insert_automated_organizations_batch(
    conn: psycopg.Connection, org_rows: List[tuple]
) -> None:
    """Insert a batch of AutomatedOrganization rows using COPY."""
    if not org_rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """COPY "AutomatedOrganization" (id, name)
               FROM STDIN"""
        ) as copy:
            for row in org_rows:
                copy.write_row(row)
    conn.commit()


def insert_automated_organization_datasets_batch(
    conn: psycopg.Connection, link_rows: List[tuple]
) -> None:
    """Insert a batch of AutomatedOrganizationDataset rows using COPY."""
    if not link_rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """COPY "AutomatedOrganizationDataset" ("automatedOrganizationId", "datasetId", created, updated)
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
    """Count total lines (link rows) across AutomatedOrganizationDataset ndjson files."""
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


def step1_insert_automated_organizations(
    conn: psycopg.Connection, organizations_dir: Path
) -> int:
    """Step 1: Read organization NDJSON and batch-insert all AutomatedOrganization rows."""
    print("📦 Step 1: Inserting AutomatedOrganization...")

    ndjson_files = load_ndjson_files(organizations_dir)
    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0

    total_records = _count_records(ndjson_files)
    print(f"  Processing {total_records:,} organization records...")

    org_rows: List[tuple] = []
    total_orgs = 0
    pbar = tqdm(
        total=total_records, desc="  AutomatedOrganization", unit="record", unit_scale=True
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
                        if org_id is None:
                            tqdm.write(
                                f"    ⚠️  Skipping record without id in {file_path.name}"
                            )
                            pbar.update(1)
                            continue
                        try:
                            org_id = int(org_id)
                        except (TypeError, ValueError):
                            tqdm.write(
                                f"    ⚠️  Skipping record with non-int id in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        name = record.get("name") or ""
                        org_rows.append((org_id, name))
                        total_orgs += 1
                        pbar.update(1)

                        if len(org_rows) >= BATCH_SIZE:
                            insert_automated_organizations_batch(conn, org_rows)
                            org_rows = []

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
    if org_rows:
        insert_automated_organizations_batch(conn, org_rows)

    # Ensure sequence is past max id so future inserts don't conflict
    with conn.cursor() as cur:
        cur.execute(
            """SELECT setval(pg_get_serial_sequence('"AutomatedOrganization"', 'id'),
                            COALESCE((SELECT MAX(id) FROM "AutomatedOrganization"), 1))"""
        )
        conn.commit()

    print(f"  ✅ Inserted {total_orgs:,} AutomatedOrganization rows")
    return total_orgs


def step2_insert_automated_organization_datasets(
    conn: psycopg.Connection, automatedorganizationdataset_dir: Path
) -> int:
    """Step 2: Read AutomatedOrganizationDataset NDJSON and batch-insert all link rows."""
    print("\n📦 Step 2: Inserting AutomatedOrganizationDataset...")

    ndjson_files = load_ndjson_files(automatedorganizationdataset_dir)
    if not ndjson_files:
        return 0

    total_links_to_insert = _count_link_rows(ndjson_files)
    print(f"  Processing {total_links_to_insert:,} organization-dataset link rows...")
    link_rows: List[tuple] = []
    total_links = 0
    now = datetime.now()
    pbar = tqdm(
        total=total_links_to_insert,
        desc="  AutomatedOrganizationDataset",
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
                        org_id = record.get("automatedOrganizationId")
                        dataset_id = record.get("datasetId")
                        if org_id is None or dataset_id is None:
                            continue
                        try:
                            org_id = int(org_id)
                            dataset_id = int(dataset_id)
                        except (TypeError, ValueError):
                            continue

                        link_rows.append((org_id, dataset_id, now, now))
                        total_links += 1
                        pbar.update(1)

                        if len(link_rows) >= BATCH_SIZE:
                            insert_automated_organization_datasets_batch(
                                conn, link_rows
                            )
                            link_rows = []

                    except json.JSONDecodeError:
                        pass
                    except Exception:
                        pass

        except Exception as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")

    pbar.close()
    if link_rows:
        insert_automated_organization_datasets_batch(conn, link_rows)

    print(f"  ✅ Inserted {total_links:,} AutomatedOrganizationDataset rows")
    return total_links


def main() -> None:
    """Main function to fill database with automated organization data."""
    print("🚀 Starting automated organization database fill...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    database_dir = downloads_dir / "database"
    organizations_dir = database_dir / "organizations"
    automatedorganizationdataset_dir = database_dir / "automatedorganizationdataset"

    print(f"Organizations directory: {organizations_dir}")
    print(f"AutomatedOrganizationDataset directory: {automatedorganizationdataset_dir}")

    if not organizations_dir.exists():
        raise FileNotFoundError(
            f"Organizations directory not found: {organizations_dir}. "
            "Please run generate-organizations.py first."
        )
    if not automatedorganizationdataset_dir.exists():
        raise FileNotFoundError(
            f"AutomatedOrganizationDataset directory not found: {automatedorganizationdataset_dir}. "
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

            org_count = step1_insert_automated_organizations(
                conn, organizations_dir
            )
            link_count = step2_insert_automated_organization_datasets(
                conn, automatedorganizationdataset_dir
            )

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
