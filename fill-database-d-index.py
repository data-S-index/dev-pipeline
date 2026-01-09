"""Fill database with d-index data from NDJSON files using psycopg3 for fast bulk inserts."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


# Batch size for processing
BATCH_SIZE = 10000


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    # Split filename into text and numeric parts
    parts = re.split(r"(\d+)", name)
    # Convert numeric parts to int, keep text parts as strings
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def load_ndjson_files(directory: Path) -> List[Path]:
    """Load and sort ndjson files from directory."""
    files = list(directory.glob("*.ndjson"))
    # Sort by filename using natural sort (alphabetical then numerical)
    return sorted(files, key=natural_sort_key)


def insert_dindex_batch(
    conn: psycopg.Connection,
    dindex_rows: List[tuple],
) -> None:
    """Insert a batch of d-index records using COPY."""
    with conn.cursor() as cur:
        if dindex_rows:
            with cur.copy(
                """COPY "DIndex" ("datasetId", score, created)
                   FROM STDIN"""
            ) as copy:
                for row in dindex_rows:
                    copy.write_row(row)
        conn.commit()


def process_dindex_files(conn: psycopg.Connection, dindex_dir: Path) -> int:
    """Process d-index files and insert records."""
    print("📊 Processing d-index files...")

    ndjson_files = load_ndjson_files(dindex_dir)
    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0

    print(f"  Found {len(ndjson_files)} ndjson file(s)")

    # Count total records
    total_records = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        total_records += 1
        except Exception:
            continue

    print(f"  Processing {total_records:,} d-index records...")

    # Batch storage
    dindex_rows: List[tuple] = []
    total_dindices = 0

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

                        dataset_id = record.get("datasetId")
                        if not dataset_id:
                            tqdm.write(
                                f"    ⚠️  Skipping record without datasetId in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        score = record.get("score")
                        if score is None:
                            tqdm.write(
                                f"    ⚠️  Skipping record without score in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        # Parse created date (default to now if not provided or parsing fails)
                        created_date = datetime.now()
                        created_str = record.get("created")
                        if created_str:
                            try:
                                if isinstance(created_str, str):
                                    if created_str.endswith("Z"):
                                        created_str = created_str[:-1] + "+00:00"
                                    created_date = datetime.fromisoformat(created_str)
                            except (ValueError, AttributeError, TypeError):
                                pass

                        # Prepare row tuple matching database schema order
                        # datasetId, score, created
                        row = (
                            dataset_id,
                            float(score),
                            created_date,
                        )
                        dindex_rows.append(row)
                        total_dindices += 1
                        pbar.update(1)

                        # Insert batch when it reaches BATCH_SIZE
                        if len(dindex_rows) >= BATCH_SIZE:
                            insert_dindex_batch(conn, dindex_rows)
                            dindex_rows = []

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

    # Insert remaining records
    if dindex_rows:
        insert_dindex_batch(conn, dindex_rows)

    return total_dindices


def main() -> None:
    """Main function to fill database with d-index data."""
    print("🚀 Starting d-index database fill process...")

    # Locate data directories
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    dindex_dir = downloads_dir / "database" / "dindex"

    print(f"D-index directory: {dindex_dir}")

    # Check directory exists
    if not dindex_dir.exists():
        raise FileNotFoundError(
            f"D-index directory not found: {dindex_dir}. "
            f"Please run generate-d-index.py first."
        )

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            # Truncate DIndex table first
            print("\n🗑️  Truncating DIndex table...")
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE "DIndex" RESTART IDENTITY CASCADE')
                conn.commit()
            print("  ✅ DIndex table truncated")

            # Process and insert d-index records
            dindex_count = process_dindex_files(conn, dindex_dir)

            print("\n✅ D-index database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - D-index records inserted: {dindex_count:,}")

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
