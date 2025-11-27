"""Fill database with processed dataset data using psycopg3 for fast bulk inserts."""

import json
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


# Batch sizes for optimal performance
DATASET_BATCH_SIZE = 10000


def load_json_files(directory: Path) -> List[Path]:
    """Load and sort JSON files from directory."""
    files = []
    for file_path in directory.glob("*.json"):
        try:
            # Try to parse filename as integer for proper numeric sorting
            int(file_path.stem)
            files.append(file_path)
        except ValueError:
            # Skip files that don't have numeric names
            continue
    return sorted(files, key=lambda p: int(p.stem))


def insert_datasets(conn: psycopg.Connection, dataset_dir: Path) -> int:
    """Insert datasets using COPY for maximum speed."""
    print("📦 Inserting datasets...")

    dataset_files = load_json_files(dataset_dir)
    if not dataset_files:
        print("  ⚠️  No dataset files found")
        return 0

    print(f"  Found {len(dataset_files)} dataset file(s)")

    # Count total records
    total_records = 0
    for file_path in tqdm(dataset_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    total_records += len(data)
        except Exception:
            continue

    print(f"  Processing {total_records:,} dataset records...")

    # Prepare data for COPY
    all_rows: List[tuple] = []
    pbar = tqdm(total=total_records, desc="  Loading", unit="record", unit_scale=True)

    for file_path in dataset_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    continue

                for record in data:
                    # Map JSON fields to database columns
                    dataset_id = record.get("id")
                    doi = record.get("doi", "").lower() if record.get("doi") else None
                    title = record.get("title", "")
                    description = record.get("description")
                    version = record.get("version")
                    publisher = record.get("publisher")

                    # Parse publishedAt
                    published_at = None
                    published_at_str = record.get("publishedAt")
                    if published_at_str:
                        try:
                            if isinstance(published_at_str, str):
                                if published_at_str.endswith("Z"):
                                    published_at_str = published_at_str[:-1] + "+00:00"
                                published_at = datetime.fromisoformat(published_at_str)
                        except (ValueError, AttributeError, TypeError):
                            pass

                    # If no published_at, use a default (required field)
                    if not published_at:
                        published_at = datetime.now()

                    authors = record.get("authors", "[]")
                    domain = record.get("domain")
                    subjects = record.get("subjects", [])
                    random_int = record.get("randomInt", 0)

                    # Prepare row tuple matching database schema order
                    # id, doi, title, description, version, publisher, publishedAt, authors, domain, subjects, randomInt
                    row = (
                        dataset_id,
                        doi,
                        title,
                        description,
                        version,
                        publisher,
                        published_at,
                        authors,
                        domain,
                        subjects,
                        random_int,
                    )
                    all_rows.append(row)
                    pbar.update(1)
        except Exception as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

    pbar.close()

    if not all_rows:
        print("  ⚠️  No dataset records to insert")
        return 0

    # Use COPY for maximum speed (fastest bulk insert method)
    print(f"  💾 Inserting {len(all_rows):,} datasets using COPY...")

    with conn.cursor() as cur:
        # Use COPY for bulk insert (fastest method for PostgreSQL)
        with cur.copy(
            """COPY "Dataset" (id, doi, title, description, version, publisher, "publishedAt", authors, domain, subjects, "randomInt")
               FROM STDIN"""
        ) as copy:
            for row in tqdm(
                all_rows, desc="  Inserting", unit="record", unit_scale=True
            ):
                copy.write_row(row)
        conn.commit()

    print(f"  ✅ Inserted {len(all_rows):,} datasets")
    return len(all_rows)


def main() -> None:
    """Main function to fill database with dataset data."""
    print("🚀 Starting dataset database fill process...")

    # Locate data directories
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    dataset_dir = downloads_dir / "database" / "dataset"

    print(f"Dataset directory: {dataset_dir}")

    # Check directory exists
    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {dataset_dir}. "
            f"Please run format-raw-data.py first."
        )

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            # Insert datasets
            dataset_count = insert_datasets(conn, dataset_dir)

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
