"""Fill database with processed mention data using psycopg3 for fast bulk inserts."""

import contextlib
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
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def load_ndjson_files(directory: Path) -> List[Path]:
    """Load and sort ndjson files from directory."""
    files = list(directory.glob("*.ndjson"))
    return sorted(files, key=natural_sort_key)


def insert_mentions_batch(
    conn: psycopg.Connection,
    mention_rows: List[tuple],
) -> None:
    """Insert a batch of mentions using COPY."""
    with conn.cursor() as cur:
        if mention_rows:
            with cur.copy(
                """COPY "Mention" ("datasetId", "mentionLink", source, "mentionedDate", "mentionWeight", created, updated)
                   FROM STDIN"""
            ) as copy:
                for row in mention_rows:
                    copy.write_row(row)
        conn.commit()


def process_mention_files(conn: psycopg.Connection, mention_dir: Path) -> int:
    """Process mention files and insert mentions."""
    print("📚 Processing mention files...")

    ndjson_files = load_ndjson_files(mention_dir)
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

    print(f"  Processing {total_records:,} mention records...")

    mention_rows: List[tuple] = []
    total_mentions = 0

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

                        mention_link = record.get("mentionLink", "")
                        if not mention_link:
                            tqdm.write(
                                f"    ⚠️  Skipping record without mentionLink in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        source = record.get("source", [])
                        if isinstance(source, list):
                            source_list = [str(s) for s in source if s is not None]
                        else:
                            source_list = []

                        # Parse mentionedDate (default to now if not provided or parsing fails)
                        mentioned_date = datetime.now()
                        if mentioned_date_str := record.get("mentionedDate"):
                            with contextlib.suppress(
                                ValueError, AttributeError, TypeError
                            ):
                                if isinstance(mentioned_date_str, str):
                                    if mentioned_date_str.endswith("Z"):
                                        mentioned_date_str = (
                                            f"{mentioned_date_str[:-1]}+00:00"
                                        )
                                    mentioned_date = datetime.fromisoformat(
                                        mentioned_date_str
                                    )

                        mention_weight = float(record.get("mentionWeight", 1.0))

                        # Row tuple: datasetId, mentionLink, source, mentionedDate, mentionWeight, created, updated
                        row = (
                            dataset_id,
                            mention_link,
                            source_list,
                            mentioned_date,
                            mention_weight,
                            datetime.now(),  # created
                            datetime.now(),  # updated
                        )
                        mention_rows.append(row)
                        total_mentions += 1
                        pbar.update(1)

                        if len(mention_rows) >= BATCH_SIZE:
                            insert_mentions_batch(conn, mention_rows)
                            mention_rows = []

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

    if mention_rows:
        insert_mentions_batch(conn, mention_rows)

    return total_mentions


def main() -> None:
    """Main function to fill database with mention data."""
    print("🚀 Starting mention database fill process...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    mention_dir = downloads_dir / "database" / "mentions"

    print(f"Mention directory: {mention_dir}")

    if not mention_dir.exists():
        raise FileNotFoundError(
            f"Mention directory not found: {mention_dir}. "
            f"Please run format-mention.py first."
        )

    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            print("\n🗑️  Truncating Mention table...")
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE "Mention" RESTART IDENTITY CASCADE')
                conn.commit()
            print("  ✅ Mention table truncated")

            mention_count = process_mention_files(conn, mention_dir)

            print("\n✅ Mention database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - Mentions inserted: {mention_count:,}")

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
