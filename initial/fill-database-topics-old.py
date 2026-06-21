"""Fill database with processed topic data using psycopg3 for fast bulk inserts."""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


# Batch size for processing
BATCH_SIZE = 20000


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def load_ndjson_files(directory: Path) -> List[Path]:
    """Load and sort ndjson files from directory."""
    files = list(directory.glob("*.ndjson"))
    return sorted(files, key=natural_sort_key)


def upsert_topics_batch(
    conn: psycopg.Connection,
    topic_rows: List[tuple],
) -> List[int]:
    """Upsert a batch of topics using a temp table and ON CONFLICT.

    The table is truncated once up front (see main), but the same datasetId
    can still appear in two different batches as files are streamed in, so
    inserts must tolerate a conflict and keep whichever row has the higher
    score.

    Rows whose datasetId has no matching "Dataset" row are skipped (rather
    than left to raise a foreign key violation, which would abort the whole
    batch's transaction). Returns the datasetIds that were skipped.
    """
    if not topic_rows:
        return []

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE temp_dataset_topics (
                "datasetId" INT,
                "topicId" TEXT,
                "topicName" TEXT,
                "subfieldId" TEXT,
                "subfieldName" TEXT,
                "fieldId" TEXT,
                "fieldName" TEXT,
                "domainId" TEXT,
                "domainName" TEXT,
                score FLOAT,
                source TEXT,
                created TIMESTAMP,
                updated TIMESTAMP
            ) ON COMMIT DROP
            """
        )
        with cur.copy(
            """COPY temp_dataset_topics ("datasetId", "topicId", "topicName", "subfieldId", "subfieldName", "fieldId", "fieldName", "domainId", "domainName", score, source, created, updated)
               FROM STDIN"""
        ) as copy:
            for row in topic_rows:
                copy.write_row(row)

        cur.execute(
            """
            SELECT t."datasetId"
            FROM temp_dataset_topics t
            WHERE NOT EXISTS (
                SELECT 1 FROM "Dataset" d WHERE d.id = t."datasetId"
            )
            """
        )
        missing_dataset_ids = [row[0] for row in cur.fetchall()]
        if missing_dataset_ids:
            preview = ", ".join(str(i) for i in missing_dataset_ids[:10])
            if len(missing_dataset_ids) > 10:
                preview += ", ..."
            tqdm.write(
                f"    ⚠️  Skipping {len(missing_dataset_ids)} topic row(s) "
                f"for dataset(s) that don't exist: {preview}"
            )

        cur.execute(
            """
            INSERT INTO "DatasetTopic" ("datasetId", "topicId", "topicName", "subfieldId", "subfieldName", "fieldId", "fieldName", "domainId", "domainName", score, source, created, updated)
            SELECT t."datasetId", t."topicId", t."topicName", t."subfieldId", t."subfieldName", t."fieldId", t."fieldName", t."domainId", t."domainName", t.score, t.source, t.created, t.updated
            FROM temp_dataset_topics t
            JOIN "Dataset" d ON d.id = t."datasetId"
            ON CONFLICT ("datasetId")
            DO UPDATE SET
                "topicId" = EXCLUDED."topicId",
                "topicName" = EXCLUDED."topicName",
                "subfieldId" = EXCLUDED."subfieldId",
                "subfieldName" = EXCLUDED."subfieldName",
                "fieldId" = EXCLUDED."fieldId",
                "fieldName" = EXCLUDED."fieldName",
                "domainId" = EXCLUDED."domainId",
                "domainName" = EXCLUDED."domainName",
                score = EXCLUDED.score,
                source = EXCLUDED.source,
                updated = NOW()
            WHERE
                EXCLUDED.score IS NOT NULL
                AND (
                    "DatasetTopic".score IS NULL
                    OR EXCLUDED.score > "DatasetTopic".score
                )
            """
        )
        conn.commit()
        return missing_dataset_ids


def process_topic_files(
    conn: psycopg.Connection, topic_dir: Path, only_file: Optional[str] = None
) -> int:
    """Process topic files and upsert topics as they're read.

    DatasetTopic has datasetId as primary key (one row per dataset). Records
    are buffered into batches of up to BATCH_SIZE unique datasets and upserted
    immediately, rather than accumulating everything in memory until the end.
    The highest-scoring topic per dataset wins, both within a batch (kept in
    memory) and across batches (enforced by the conditional ON CONFLICT in
    upsert_topics_batch).

    A DB error during flush is fatal: it leaves the transaction aborted, so
    it's allowed to propagate and stop the run rather than being treated as a
    skippable per-record error.
    """
    print("📚 Processing topic files...")

    ndjson_files = load_ndjson_files(topic_dir)
    if only_file:
        ndjson_files = [f for f in ndjson_files if f.name == only_file]
        if not ndjson_files:
            print(f"  ⚠️  No file named {only_file} found in {topic_dir}")
            return 0

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

    # Current batch: datasetId -> best topic row seen so far in this batch
    batch_buffer: Dict[int, tuple] = {}
    seen_dataset_ids: set = set()
    missing_dataset_ids: set = set()
    flushed_count = 0

    def flush() -> None:
        nonlocal flushed_count
        if not batch_buffer:
            return
        missing_dataset_ids.update(upsert_topics_batch(conn, list(batch_buffer.values())))
        flushed_count += len(batch_buffer)
        batch_buffer.clear()
        pbar.set_postfix(flushed=f"{flushed_count:,}")

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

                        # Keep one topic per dataset within this batch: prefer higher score
                        existing = batch_buffer.get(dataset_id)
                        if existing is None:
                            batch_buffer[dataset_id] = row
                        else:
                            existing_score = existing[9]  # score at index 9
                            if (score is not None and existing_score is None) or (
                                score is not None
                                and existing_score is not None
                                and score > existing_score
                            ):
                                batch_buffer[dataset_id] = row

                        seen_dataset_ids.add(dataset_id)
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

                    # Outside the per-record try/except: a DB error here means
                    # the transaction is aborted, so it must stop the run
                    # instead of being swallowed as a per-record error.
                    if len(batch_buffer) >= BATCH_SIZE:
                        flush()

        except (OSError, UnicodeDecodeError) as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

    # Flush any remaining records that didn't fill a full batch
    flush()

    pbar.close()

    if missing_dataset_ids:
        print(
            f"  ⚠️  {len(missing_dataset_ids):,} dataset(s) skipped overall "
            "because they don't exist: "
            f"{', '.join(str(i) for i in sorted(missing_dataset_ids)[:20])}"
            + (" ..." if len(missing_dataset_ids) > 20 else "")
        )

    return len(seen_dataset_ids - missing_dataset_ids)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fill database with topic data")
    parser.add_argument(
        "--file",
        dest="only_file",
        help=(
            "Restrict processing to a single ndjson file by name "
            "(e.g. 2013.ndjson), for debugging a specific file. "
            "Skips truncating the DatasetTopic table."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Main function to fill database with topic data."""
    args = parse_args()

    print("🚀 Starting topic database fill process...")

    # Locate data directories (same as format-topics output)
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    topic_dir = downloads_dir / "database-old" / "topics"

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

            if args.only_file:
                print(
                    f"\n🔍 Debug mode: only processing {args.only_file} "
                    "(table will not be truncated)"
                )
            else:
                print("\n🗑️  Truncating DatasetTopic table...")
                with conn.cursor() as cur:
                    cur.execute('TRUNCATE TABLE "DatasetTopic" CASCADE')
                    conn.commit()
                print("  ✅ DatasetTopic table truncated")

            topic_count = process_topic_files(
                conn, topic_dir, only_file=args.only_file
            )

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
