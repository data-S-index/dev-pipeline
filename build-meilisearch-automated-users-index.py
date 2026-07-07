"""Build Meilisearch index from automated user (author) NDJSON files."""

import json
import re
import time
from pathlib import Path
from typing import Dict, Any, List

from meilisearch import Client
from tqdm import tqdm

from config import SEARCH_API_URL, SEARCH_API_KEY

BATCH_SIZE = 200000  # Number of documents to batch before sending
DELAY_INTERVAL = 5_000_000  # Delay after every 5 million records
DELAY_DURATION = 180  # 3 minutes in seconds


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


def process_ndjson_files(index, authors_dir: Path) -> int:
    """Process ndjson files and add documents to Meilisearch index."""
    print("📦 Processing automated user (author) files...")

    ndjson_files = load_ndjson_files(authors_dir)

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

    print(f"  Processing {total_records:,} automated user records...")

    # Batch storage
    documents: List[Dict[str, Any]] = []
    total_documents = 0

    pbar = tqdm(
        total=total_records,
        desc="  Processing",
        unit="record",
        unit_scale=True,
        smoothing=0,
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

                        # Meilisearch requires a primary key field
                        if "id" not in record:
                            tqdm.write(
                                f"    ⚠️  Skipping record without id in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        search_record = {
                            "id": record.get("id"),
                            "name": record.get("name"),
                            "nameIdentifiers": record.get("nameIdentifiers", []),
                            "affiliations": record.get("affiliations", []),
                        }

                        documents.append(search_record)
                        total_documents += 1
                        pbar.update(1)

                        # Send batch when it reaches BATCH_SIZE
                        if len(documents) >= BATCH_SIZE:
                            index.add_documents(documents)
                            documents = []

                        # Add delay after every 5 million records
                        if (
                            total_documents > 0
                            and total_documents % DELAY_INTERVAL == 0
                        ):
                            print(
                                f"\n  ⏸️  Processed {total_documents:,} records. Waiting {DELAY_DURATION} seconds..."
                            )
                            time.sleep(DELAY_DURATION)
                            print("  ▶️  Resuming processing...\n")

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

    # Add remaining documents
    if documents:
        index.add_documents(documents)

    return total_documents


def main() -> None:
    """Main function to build Meilisearch index from automated user (author) files."""
    print("🚀 Starting Meilisearch automated-users index build...")

    print(f"Meilisearch URL: {SEARCH_API_URL}")

    # Locate data directory (output of generate-authors.py)
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    authors_dir = downloads_dir / "pulled-database" / "automated-author" / "authors"

    print(f"Authors directory: {authors_dir}")

    # Check directory exists
    if not authors_dir.exists():
        raise FileNotFoundError(
            f"Authors directory not found: {authors_dir}. "
            f"Please run generate-authors.py first."
        )

    # Connect to Meilisearch
    print("\n🔌 Connecting to Meilisearch...")
    try:
        client = Client(SEARCH_API_URL, SEARCH_API_KEY)

        # Test connection
        health = client.health()
        print(
            f"  ✅ Connected to Meilisearch (version: {health.get('version', 'unknown')})"
        )

        # Delete index if it exists, then create a new one
        index_name = "automated-user"
        print(f"\n📇 Setting up '{index_name}' index...")
        try:
            client.delete_index(index_name)
            print("  🗑️  Deleted existing index")
        except Exception:
            print("  ℹ️  No existing index to delete")

        # Create new index with primary key
        client.create_index(index_name, {"primaryKey": "id"})
        print("  ✅ Index created with primary key 'id'")
        print("  ✅ Index ready")

        index = client.index(index_name)

        # Process and add documents
        document_count = process_ndjson_files(index, authors_dir)

        print("\n✅ Meilisearch automated-users index build completed successfully!")
        print("📊 Summary:")
        print(f"  - Documents indexed: {document_count:,}")

    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
