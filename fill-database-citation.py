"""Fill database with processed citation data using psycopg3 for fast bulk inserts."""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Callable
from cuid2 import cuid_wrapper

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


# Batch sizes for optimal performance
CITATION_BATCH_SIZE = 50000


cuid_generator: Callable[[], str] = cuid_wrapper()


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


def insert_citations(conn: psycopg.Connection, citation_dir: Path) -> int:
    """Insert citations using COPY for maximum speed."""
    print("📚 Inserting citations...")

    citation_files = load_json_files(citation_dir)
    if not citation_files:
        print("  ⚠️  No citation files found")
        return 0

    print(f"  Found {len(citation_files)} citation file(s)")

    # Count total records
    total_records = 0
    for file_path in tqdm(citation_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    total_records += len(data)
        except Exception:
            continue

    print(f"  Processing {total_records:,} citation records...")

    # Prepare data for COPY
    all_rows: List[tuple] = []
    pbar = tqdm(total=total_records, desc="  Loading", unit="record", unit_scale=True)

    for file_path in citation_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    continue

                for record in data:
                    # Generate CUID2 ID
                    citation_id = cuid_generator()
                    dataset_id = record.get("datasetId")
                    citation_link = record.get("citationLink", "")
                    datacite = record.get("datacite", False)
                    mdc = record.get("mdc", False)
                    open_alex = record.get("openAlex", False)

                    # Parse citedDate
                    cited_date = None
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
                    # id, datasetId, citationLink, datacite, mdc, openAlex, citedDate, citationWeight
                    row = (
                        citation_id,
                        dataset_id,
                        citation_link,
                        datacite,
                        mdc,
                        open_alex,
                        cited_date,
                        citation_weight,
                    )
                    all_rows.append(row)
                    pbar.update(1)
        except Exception as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

    pbar.close()

    if not all_rows:
        print("  ⚠️  No citation records to insert")
        return 0

    # Use COPY for maximum speed (fastest bulk insert method)
    print(f"  💾 Inserting {len(all_rows):,} citations using COPY...")

    with conn.cursor() as cur:
        # Use COPY for bulk insert (fastest method for PostgreSQL)
        with cur.copy(
            """COPY "Citation" (id, "datasetId", "citationLink", datacite, mdc, "openAlex", "citedDate", "citationWeight")
               FROM STDIN"""
        ) as copy:
            for row in tqdm(
                all_rows, desc="  Inserting", unit="record", unit_scale=True
            ):
                copy.write_row(row)
        conn.commit()

    print(f"  ✅ Inserted {len(all_rows):,} citations")
    return len(all_rows)


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

            # Insert citations
            citation_count = insert_citations(conn, citation_dir)

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
