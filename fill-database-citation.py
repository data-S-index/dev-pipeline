"""Fill database with processed citation data using psycopg3 for fast bulk inserts."""

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


def insert_citations_batch(
    conn: psycopg.Connection,
    citation_rows: List[tuple],
) -> None:
    """Insert a batch of citations using COPY."""
    with conn.cursor() as cur:
        if citation_rows:
            with cur.copy(
                """COPY "Citation" ("datasetId", "citationLink", datacite, mdc, "openAlex", "citedDate", "citationWeight", created, updated)
                   FROM STDIN"""
            ) as copy:
                for row in citation_rows:
                    copy.write_row(row)
        conn.commit()


def process_citation_files(conn: psycopg.Connection, citation_dir: Path) -> int:
    """Process citation files and insert citations."""
    print("📚 Processing citation files...")

    ndjson_files = load_ndjson_files(citation_dir)
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

    print(f"  Processing {total_records:,} citation records...")

    # Batch storage
    citation_rows: List[tuple] = []
    total_citations = 0

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

                        citation_link = record.get("citationLink", "")
                        datacite = record.get("datacite", False)
                        mdc = record.get("mdc", False)
                        open_alex = record.get("openAlex", False)

                        # Parse citedDate (default to now if not provided or parsing fails)
                        cited_date = datetime.now()
                        cited_date_str = record.get("citedDate")
                        if cited_date_str:
                            try:
                                if isinstance(cited_date_str, str):
                                    if cited_date_str.endswith("Z"):
                                        cited_date_str = cited_date_str[:-1] + "+00:00"
                                    cited_date = datetime.fromisoformat(cited_date_str)
                            except (ValueError, AttributeError, TypeError):
                                pass

                        citation_weight = float(record.get("citationWeight", 1.0))

                        # Prepare row tuple matching database schema order
                        # datasetId, citationLink, datacite, mdc, openAlex, citedDate, citationWeight, created, updated
                        row = (
                            dataset_id,
                            citation_link,
                            datacite,
                            mdc,
                            open_alex,
                            cited_date,
                            citation_weight,
                            datetime.now(),  # created
                            datetime.now(),  # updated
                        )
                        citation_rows.append(row)
                        total_citations += 1
                        pbar.update(1)

                        # Insert batch when it reaches BATCH_SIZE
                        if len(citation_rows) >= BATCH_SIZE:
                            insert_citations_batch(conn, citation_rows)
                            citation_rows = []

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
    if citation_rows:
        insert_citations_batch(conn, citation_rows)

    return total_citations


def main() -> None:
    """Main function to fill database with citation data."""
    print("🚀 Starting citation database fill process...")

    # Locate data directories
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    citation_dir = downloads_dir / "database" / "citations"

    print(f"Citation directory: {citation_dir}")

    # Check directory exists
    if not citation_dir.exists():
        raise FileNotFoundError(
            f"Citation directory not found: {citation_dir}. "
            f"Please run format-mdc-citation.py first."
        )

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            # Truncate citation table first
            print("\n🗑️  Truncating citation table...")
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE "Citation" RESTART IDENTITY CASCADE')
                conn.commit()
            print("  ✅ Citation table truncated")

            # Process and insert citations
            citation_count = process_citation_files(conn, citation_dir)

            print("\n✅ Citation database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - Citations inserted: {citation_count:,}")

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
