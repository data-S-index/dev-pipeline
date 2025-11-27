"""Fill database with processed dataset data using psycopg3 for fast bulk inserts."""

import json
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import MINI_DATABASE_URL


DOI_TO_ID_MAP_FILE = "doi_to_id_map.ndjson"


def insert_datasets(conn: psycopg.Connection, mapping_file: Path) -> int:
    """Insert datasets using COPY for maximum speed, processing in batches."""
    print("📦 Inserting datasets...")

    # Check if mapping file exists
    if not mapping_file.exists():
        raise FileNotFoundError(
            f"Mapping file not found: {mapping_file}. "
            f"Please run build-doi-datasetid-map.py first."
        )

    print(f"  Loading mapping file: {mapping_file}")

    # Process ndjson file in batches
    BATCH_SIZE = 1000
    total_inserted = 0
    batch_rows: List[tuple] = []

    try:
        with open(mapping_file, "r", encoding="utf-8") as f:
            # Count total lines for progress bar
            print("  Counting records...")
            total_lines = sum(1 for _ in f)
            print(f"  Found {total_lines:,} DOI mappings")

            # Reset file pointer
            f.seek(0)

            # Process file line by line in batches
            pbar = tqdm(
                total=total_lines, desc="  Processing", unit="record", unit_scale=True
            )

            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    doi = record.get("doi", "")
                    dataset_id = record.get("id")

                    if doi and dataset_id:
                        # Prepare row tuple matching database schema order
                        # id, doi (score and evaluationDate have defaults)
                        row = (dataset_id, doi)
                        batch_rows.append(row)

                        # Insert batch when it reaches BATCH_SIZE
                        if len(batch_rows) >= BATCH_SIZE:
                            total_inserted += _insert_batch(conn, batch_rows)
                            batch_rows = []

                    pbar.update(1)
                except json.JSONDecodeError as e:
                    tqdm.write(f"    ⚠️  Error parsing line: {e}")
                    pbar.update(1)
                    continue

            pbar.close()

            # Insert remaining records
            if batch_rows:
                total_inserted += _insert_batch(conn, batch_rows)

    except Exception as e:
        raise FileNotFoundError(f"Error reading mapping file: {e}")

    print(f"  ✅ Inserted {total_inserted:,} datasets")
    return total_inserted


def _insert_batch(conn: psycopg.Connection, batch_rows: List[tuple]) -> int:
    """Insert a batch of rows using COPY."""
    if not batch_rows:
        return 0

    with conn.cursor() as cur:
        # Use COPY for bulk insert (fastest method for PostgreSQL)
        with cur.copy(
            """COPY "Dataset" (id, doi)
               FROM STDIN"""
        ) as copy:
            for row in batch_rows:
                copy.write_row(row)
        conn.commit()

    return len(batch_rows)


def main() -> None:
    """Main function to fill database with dataset data."""
    print("🚀 Starting dataset database fill process...")

    # Locate mapping file
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    mapping_file = downloads_dir / "database" / DOI_TO_ID_MAP_FILE

    print(f"Mapping file: {mapping_file}")

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(MINI_DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            # Insert datasets
            dataset_count = insert_datasets(conn, mapping_file)

            print("\n✅ Dataset database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - Datasets inserted: {dataset_count:,}")

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
