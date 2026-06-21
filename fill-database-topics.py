"""Fill database with processed topic data using psycopg3 for fast bulk upserts.

Topic records in Downloads/topics-split use a DOI-style "dataset_id" identifier
(e.g. "10.57451/lhd.a.nb4a_fraction.75456.1") rather than the internal Dataset
id. Each identifier is resolved to its internal datasetId using the
identifier -> datasetId map pulled from the live database (see
pull-identifier-datasetid-map.py), the same map used by merge-citations.py.
"""

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


def map_key(identifier: str) -> str:
    """Build a lookup key from the identifier value alone."""
    return identifier.strip().lower()


def load_identifier_to_dataset_id_map(map_dir: Path) -> Dict[str, int]:
    """Load the identifier -> datasetId map (built by pull-identifier-datasetid-map.py)."""
    print(f"  Loading identifier-to-datasetId map from {map_dir}...")

    if not map_dir.exists():
        raise FileNotFoundError(
            f"Map directory not found: {map_dir}. "
            "Run pull-identifier-datasetid-map.py first."
        )

    map_files = sorted(map_dir.glob("*.ndjson"))
    if not map_files:
        raise FileNotFoundError(f"No NDJSON files found in {map_dir}.")

    mapping: Dict[str, int] = {}
    for file_path in tqdm(map_files, desc="  Loading map files", unit="file"):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                identifier = record.get("identifier")
                dataset_id = record.get("datasetId")
                if identifier and dataset_id is not None:
                    mapping[map_key(identifier)] = dataset_id

    print(f"  ✓ Loaded {len(mapping):,} identifier entries")
    return mapping


def upsert_topics_batch(
    conn: psycopg.Connection,
    topic_rows: List[tuple],
) -> None:
    """Upsert a batch of topics using a temp table and ON CONFLICT."""
    if not topic_rows:
        return

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
            INSERT INTO "DatasetTopic" ("datasetId", "topicId", "topicName", "subfieldId", "subfieldName", "fieldId", "fieldName", "domainId", "domainName", score, source, created, updated)
            SELECT "datasetId", "topicId", "topicName", "subfieldId", "subfieldName", "fieldId", "fieldName", "domainId", "domainName", score, source, created, updated
            FROM temp_dataset_topics
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
            """
        )
        conn.commit()


def process_topic_files(
    conn: psycopg.Connection,
    topic_dir: Path,
    identifier_to_id: Dict[str, int],
) -> int:
    """Process raw topics-split files and upsert topics.

    Each record's "dataset_id" is a DOI-style identifier that must be resolved
    to the internal integer Dataset id via identifier_to_id before upserting.

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
    unmatched_count = 0

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

                        dataset_identifier = record.get("dataset_id")
                        if not dataset_identifier:
                            tqdm.write(
                                f"    ⚠️  Skipping record without dataset_id in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        dataset_id = identifier_to_id.get(
                            map_key(dataset_identifier)
                        )
                        if dataset_id is None:
                            unmatched_count += 1
                            pbar.update(1)
                            continue

                        topic_id = record.get("topic_id")
                        topic_name = record.get("topic_name") or None
                        subfield_id = record.get("subfield_id") or None
                        subfield_name = record.get("subfield_name") or None
                        field_id = record.get("field_id") or None
                        field_name = record.get("field_name") or None
                        domain_id = record.get("domain_id") or None
                        domain_name = record.get("domain_name") or None

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

    if unmatched_count > 0:
        print(
            f"  ⚠️  {unmatched_count:,} record(s) had a dataset_id with no matching dataset in the database"
        )

    # Flatten to list and upsert in batches
    all_rows = list(best_by_dataset.values())
    total_upserted = len(all_rows)
    batch_ranges = range(0, len(all_rows), BATCH_SIZE)
    for i in tqdm(
        batch_ranges,
        desc="  DB upsert",
        unit="batch",
        total=(len(all_rows) + BATCH_SIZE - 1) // BATCH_SIZE if all_rows else 0,
    ):
        batch = all_rows[i : i + BATCH_SIZE]
        upsert_topics_batch(conn, batch)

    return total_upserted


def main() -> None:
    """Main function to fill database with topic data."""
    print("🚀 Starting topic database fill process...")

    # Locate data directories
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    topic_dir = downloads_dir / "topics-split"
    map_dir = downloads_dir / "pulled-database" / "dataset-id-identifier"

    print(f"Topic directory: {topic_dir}")
    print(f"Identifier map directory: {map_dir}")

    if not topic_dir.exists():
        raise FileNotFoundError(f"Topic directory not found: {topic_dir}.")

    print("\n🗺️  Loading identifier to dataset ID mapping...")
    identifier_to_id = load_identifier_to_dataset_id_map(map_dir)

    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            topic_count = process_topic_files(conn, topic_dir, identifier_to_id)

            print("\n✅ Topic database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - Topics upserted: {topic_count:,}")

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
