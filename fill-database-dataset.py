"""Fill database with processed dataset data using psycopg3 for fast bulk inserts."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


# Batch size for processing
BATCH_SIZE = 1000


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


def insert_datasets_batch(
    conn: psycopg.Connection,
    dataset_rows: List[tuple],
    author_rows: List[tuple],
    identifier_rows: List[tuple],
) -> None:
    """Insert a batch of datasets, authors, and identifiers using COPY."""
    with conn.cursor() as cur:
        # Insert datasets
        if dataset_rows:
            with cur.copy(
                """COPY "Dataset" (id, source, identifier, "identifierType", url, title, description, version, publisher, "publishedAt", domain, subjects, "created", "updated")
                   FROM STDIN"""
            ) as copy:
                for row in dataset_rows:
                    copy.write_row(row)

        # Insert authors
        if author_rows:
            with cur.copy(
                """COPY "DatasetAuthor" (id, "datasetId", "nameType", name, "nameIdentifiers", affiliations, "created", "updated")
                   FROM STDIN"""
            ) as copy:
                for row in author_rows:
                    copy.write_row(row)

        # Insert identifiers
        if identifier_rows:
            with cur.copy(
                """COPY "DatasetIdentifier" (id, "datasetId", identifier, "identifierType", "created", "updated")
                   FROM STDIN"""
            ) as copy:
                for row in identifier_rows:
                    copy.write_row(row)

        conn.commit()


def process_ndjson_files(
    conn: psycopg.Connection, dataset_dir: Path
) -> tuple[int, int, int]:
    """Process ndjson files and insert datasets, authors, and identifiers."""
    print("📦 Processing dataset files...")

    ndjson_files = load_ndjson_files(dataset_dir)

    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0, 0, 0

    print(f"  Found {len(ndjson_files)} ndjson file(s)")

    # Count total records
    total_records = 49061167
    # for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
    #     try:
    #         with open(file_path, "r", encoding="utf-8") as f:
    #             for line in f:
    #                 if line.strip():
    #                     total_records += 1
    #     except Exception:
    #         continue

    print(f"  Processing {total_records:,} dataset records...")

    # Track counters for authors and identifiers (starting from 1)
    author_id_counter = 1
    identifier_id_counter = 1

    # Batch storage
    dataset_rows: List[tuple] = []
    author_rows: List[tuple] = []
    identifier_rows: List[tuple] = []

    total_datasets = 0
    total_authors = 0
    total_identifiers = 0

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
                        record: Dict[str, Any] = json.loads(line)

                        # Extract dataset fields
                        dataset_id = record.get("id")
                        if not dataset_id:
                            tqdm.write(
                                f"    ⚠️  Skipping record without id in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        source = record.get("source")
                        identifier = record.get("identifier", "")
                        identifier_type = record.get("identifierType", "")
                        title = record.get("title", "")
                        description = record.get("description")
                        version = record.get("version")
                        publisher = record.get("publisher")
                        published_at = record.get("publishedAt")
                        subjects = record.get("subjects", [])
                        if not isinstance(subjects, list):
                            subjects = []

                        # Prepare dataset row
                        # id, source, identifier, identifierType, url, title, description, version, publisher, publishedAt, domain, subjects
                        dataset_row = (
                            dataset_id,
                            source,
                            identifier,
                            identifier_type,
                            None,  # url
                            title,
                            description,
                            version,
                            publisher,
                            published_at,
                            None,  # domain
                            subjects,
                            datetime.now(),  # created
                            datetime.now(),  # updated
                        )
                        dataset_rows.append(dataset_row)
                        total_datasets += 1

                        # Process authors
                        authors = record.get("authors", [])
                        if isinstance(authors, list):
                            for author in authors:
                                if isinstance(author, dict):
                                    name_type = author.get("nameType")
                                    name = author.get("name", "")
                                    name_identifiers = author.get("nameIdentifiers", [])
                                    if not isinstance(name_identifiers, list):
                                        name_identifiers = []
                                    affiliations = author.get("affiliations", [])
                                    if not isinstance(affiliations, list):
                                        affiliations = []

                                    # Prepare author row
                                    # id, datasetId, nameType, name, nameIdentifiers, affiliations
                                    author_row = (
                                        author_id_counter,
                                        dataset_id,
                                        name_type,
                                        name,
                                        name_identifiers,
                                        affiliations,
                                        datetime.now(),  # created
                                        datetime.now(),  # updated
                                    )
                                    author_rows.append(author_row)
                                    author_id_counter += 1
                                    total_authors += 1

                        # Process identifiers
                        extracted_identifiers = record.get("extractedIdentifiers", [])
                        if isinstance(extracted_identifiers, list):
                            for ext_id in extracted_identifiers:
                                if isinstance(ext_id, dict):
                                    ext_identifier = ext_id.get("identifier", "")
                                    ext_identifier_type = ext_id.get(
                                        "identifierType", ""
                                    )

                                    if ext_identifier and ext_identifier_type:
                                        # Prepare identifier row
                                        # id, datasetId, identifier, identifierType
                                        identifier_row = (
                                            identifier_id_counter,
                                            dataset_id,
                                            ext_identifier,
                                            ext_identifier_type,
                                            datetime.now(),  # created
                                            datetime.now(),  # updated
                                        )
                                        identifier_rows.append(identifier_row)
                                        identifier_id_counter += 1
                                        total_identifiers += 1

                        pbar.update(1)

                        # Insert batch when it reaches BATCH_SIZE
                        if len(dataset_rows) >= BATCH_SIZE:
                            insert_datasets_batch(
                                conn, dataset_rows, author_rows, identifier_rows
                            )
                            dataset_rows = []
                            author_rows = []
                            identifier_rows = []

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
                        exit(1)

        except Exception as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

    pbar.close()

    # Insert remaining records
    if dataset_rows:
        insert_datasets_batch(conn, dataset_rows, author_rows, identifier_rows)

    return total_datasets, total_authors, total_identifiers


def main() -> None:
    """Main function to fill database with dataset data."""
    print("🚀 Starting dataset database fill process...")

    # Locate data directory
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

            # Truncate dataset table first
            print("\n🗑️  Truncating dataset table...")
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE "Dataset" RESTART IDENTITY CASCADE')
                conn.commit()
            print("  ✅ Dataset table truncated")

            # Process and insert datasets
            dataset_count, author_count, identifier_count = process_ndjson_files(
                conn, dataset_dir
            )

            print("\n✅ Dataset database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - Datasets inserted: {dataset_count:,}")
            print(f"  - Authors inserted: {author_count:,}")
            print(f"  - Identifiers inserted: {identifier_count:,}")

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
