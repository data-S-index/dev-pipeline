"""Fill database with processed topic data using psycopg3 for fast bulk inserts."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List

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


def insert_topics_batch(
    conn: psycopg.Connection,
    topic_rows: List[tuple],
) -> None:
    """Insert a batch of topics using COPY."""
    with conn.cursor() as cur:
        if topic_rows:
            with cur.copy(
                """COPY "DatasetTopic" ("datasetId", "topicId", "topicName", "subfieldId", "subfieldName", "fieldId", "fieldName", "domainId", "domainName", score, source, created, updated)
                   FROM STDIN"""
            ) as copy:
                for row in topic_rows:
                    copy.write_row(row)
        conn.commit()


def process_topic_files(conn: psycopg.Connection, topic_dir: Path) -> int:
    """Process topic files and insert topics.

    DatasetTopic has datasetId as primary key (one row per dataset).
    If multiple topics exist per dataset, we keep the one with the highest score.
    """
    print("📚 Processing topic files...")

    ndjson_files = load_ndjson_files(topic_dir)
    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0

    print(f"  Found {len(ndjson_files)} ndjson file(s)")

    # Count total records for progress
    total_records = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        total_records += 1
        except Exception:
            continue

    print(f"  Processing {total_records:,} topic records...")

    # One topic per dataset (schema PK): datasetId -> best topic row
    best_by_dataset: Dict[int, tuple] = {}

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
                        if dataset_id is None:
                            tqdm.write(
                                f"    ⚠️  Skipping record without datasetId in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        topic_id = record.get("topicId")
                        topic_name = record.get("topicName") or None
                        subfield_id = record.get("subfieldId") or None
                        subfield_name = record.get("subfieldName") or None
                        field_id = record.get("fieldId") or None
                        field_name = record.get("fieldName") or None
                        domain_id = record.get("domainId") or None
                        domain_name = record.get("domainName") or None

                        score_raw = record.get("score")
                        score = None
                        if score_raw is not None:
                            try:
                                score = float(score_raw)
                            except (TypeError, ValueError):
                                pass

                        source = record.get("source") or None
                        if source is not None and isinstance(source, str):
                            source = source.strip() or None

                        now = datetime.now()
                        row = (
                            dataset_id,
                            topic_id,
                            topic_name,
                            subfield_id,
                            subfield_name,
                            field_id,
                            field_name,
                            domain_id,
                            domain_name,
                            score,
                            source,
                            now,
                            now,
                        )

                        # Keep one topic per dataset: prefer higher score
                        existing = best_by_dataset.get(dataset_id)
                        if existing is None:
                            best_by_dataset[dataset_id] = row
                        else:
                            existing_score = existing[9]  # score at index 9
                            if (score is not None and existing_score is None) or (
                                score is not None
                                and existing_score is not None
                                and score > existing_score
                            ):
                                best_by_dataset[dataset_id] = row

                        pbar.update(1)

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

    # Flatten to list and insert in batches
    all_rows = list(best_by_dataset.values())
    total_inserted = len(all_rows)
    batch_ranges = range(0, len(all_rows), BATCH_SIZE)
    for i in tqdm(
        batch_ranges,
        desc="  DB insert",
        unit="batch",
        total=(len(all_rows) + BATCH_SIZE - 1) // BATCH_SIZE if all_rows else 0,
    ):
        batch = all_rows[i : i + BATCH_SIZE]
        insert_topics_batch(conn, batch)

    return total_inserted


def main() -> None:
    """Main function to fill database with topic data."""
    print("🚀 Starting topic database fill process...")

    # Locate data directories (same as format-topics output)
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    topic_dir = downloads_dir / "database" / "topics"

    print(f"Topic directory: {topic_dir}")

    if not topic_dir.exists():
        raise FileNotFoundError(
            f"Topic directory not found: {topic_dir}. "
            f"Please run format-topics.py first."
        )

    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            print("\n🗑️  Truncating DatasetTopic table...")
            with conn.cursor() as cur:
                cur.execute('TRUNCATE TABLE "DatasetTopic" CASCADE')
                conn.commit()
            print("  ✅ DatasetTopic table truncated")

            topic_count = process_topic_files(conn, topic_dir)

            print("\n✅ Topic database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - Topics inserted: {topic_count:,}")

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
