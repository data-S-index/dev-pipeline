"""Pull dataset data needed to recompute the d-index into a single NDJSON folder.

For every Dataset, exports one NDJSON record containing everything
generate-d-index-files.py needs to recompute the d-index from scratch:
  - id, identifier, identifierType, publishedAt, pubYear, publisherId
  - fuji: {score, metricVersion, softwareVersion}            (FujiScore, if present)
  - citations: [{id, citedDate, citationWeight}, ...]        (Citation)
  - mentions: [{id, mentionedDate, mentionWeight}, ...]      (Mention)
  - normalizationFactor: {ft, ctw, mtw, topicIdUsed, yearUsed,
        topicIdRequested, yearRequested, usedYearClamp}      (NormalizationFactor, if present)
  - datasetTopic: {topicId, topicName, subfieldId, subfieldName,
        fieldId, fieldName, domainId, domainName, score, source}  (DatasetTopic, if present)

Schema reference: prisma/schema.prisma.
Output: Downloads/pulled-database/datasets/*.ndjson (directory is wiped first).
"""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Number of dataset IDs fetched from the DB per round trip.
DB_FETCH_BATCH_SIZE = 10000

# Number of dataset records written per output NDJSON file.
RECORDS_PER_FILE = 10000


def serialize_datetime(obj):
    """Serialize datetime objects to ISO format strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def write_batch_to_file(batch: list, file_number: int, output_dir: Path) -> None:
    """Write a batch of dataset records to an NDJSON file."""
    file_path = output_dir / f"{file_number}.ndjson"
    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(
                json.dumps(record, ensure_ascii=False, default=serialize_datetime)
                + "\n"
            )


def main() -> None:
    """Pull all dataset data needed for d-index recomputation into NDJSON files."""
    print("Starting database pull for d-index recomputation...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    output_dir = downloads_dir / "pulled-database" / "datasets"

    print(f"Output directory: {output_dir}")

    if output_dir.exists():
        shutil.rmtree(output_dir)
        print("Output directory cleaned")
    output_dir.mkdir(parents=True, exist_ok=True)
    print("Output directory ready")

    print("\nConnecting to database...")
    with psycopg.connect(DATABASE_URL) as conn:
        print("Connected to database")

        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*), COALESCE(MAX(id), 0) FROM "Dataset"')
            total_datasets, max_dataset_id = cur.fetchone()

        print(f"Processing {total_datasets:,} datasets (max ID: {max_dataset_id:,})")

        file_number = 1
        current_batch: List[dict] = []
        total_records = 0
        current_id = 1

        with conn.cursor() as cur:
            pbar = tqdm(
                total=total_datasets,
                desc="Pulling datasets",
                unit="dataset",
                unit_scale=True,
            )

            while current_id <= max_dataset_id:
                batch_end = min(current_id + DB_FETCH_BATCH_SIZE - 1, max_dataset_id)

                cur.execute(
                    """
                    SELECT id, identifier, "identifierType", "publishedAt", "pubYear", "publisherId"
                    FROM "Dataset"
                    WHERE id >= %s AND id <= %s
                    ORDER BY id
                    """,
                    (current_id, batch_end),
                )
                datasets_batch = cur.fetchall()

                if not datasets_batch:
                    current_id = batch_end + 1
                    continue

                fuji_by_dataset: Dict[int, dict] = {}
                cur.execute(
                    """
                    SELECT "datasetId", score, "metricVersion", "softwareVersion"
                    FROM "FujiScore"
                    WHERE "datasetId" >= %s AND "datasetId" <= %s
                    """,
                    (current_id, batch_end),
                )
                for dataset_id, score, metric_version, software_version in cur.fetchall():
                    fuji_by_dataset[dataset_id] = {
                        "score": score,
                        "metricVersion": metric_version,
                        "softwareVersion": software_version,
                    }

                citations_by_dataset: Dict[int, list] = {}
                cur.execute(
                    """
                    SELECT "datasetId", id, "citedDate", "citationWeight"
                    FROM "Citation"
                    WHERE "datasetId" >= %s AND "datasetId" <= %s
                    ORDER BY "datasetId"
                    """,
                    (current_id, batch_end),
                )
                for dataset_id, citation_id, cited_date, citation_weight in cur.fetchall():
                    citations_by_dataset.setdefault(dataset_id, []).append(
                        {
                            "id": citation_id,
                            "citedDate": cited_date,
                            "citationWeight": citation_weight,
                        }
                    )

                mentions_by_dataset: Dict[int, list] = {}
                cur.execute(
                    """
                    SELECT "datasetId", id, "mentionedDate", "mentionWeight"
                    FROM "Mention"
                    WHERE "datasetId" >= %s AND "datasetId" <= %s
                    ORDER BY "datasetId"
                    """,
                    (current_id, batch_end),
                )
                for dataset_id, mention_id, mentioned_date, mention_weight in cur.fetchall():
                    mentions_by_dataset.setdefault(dataset_id, []).append(
                        {
                            "id": mention_id,
                            "mentionedDate": mentioned_date,
                            "mentionWeight": mention_weight,
                        }
                    )

                normalization_by_dataset: Dict[int, dict] = {}
                cur.execute(
                    """
                    SELECT "datasetId", ft, ctw, mtw, "topicIdUsed", "yearUsed",
                           "topicIdRequested", "yearRequested", "usedYearClamp"
                    FROM "NormalizationFactor"
                    WHERE "datasetId" >= %s AND "datasetId" <= %s
                    """,
                    (current_id, batch_end),
                )
                for (
                    dataset_id,
                    ft,
                    ctw,
                    mtw,
                    topic_id_used,
                    year_used,
                    topic_id_requested,
                    year_requested,
                    used_year_clamp,
                ) in cur.fetchall():
                    normalization_by_dataset[dataset_id] = {
                        "ft": ft,
                        "ctw": ctw,
                        "mtw": mtw,
                        "topicIdUsed": topic_id_used,
                        "yearUsed": year_used,
                        "topicIdRequested": topic_id_requested,
                        "yearRequested": year_requested,
                        "usedYearClamp": used_year_clamp,
                    }

                dataset_topic_by_dataset: Dict[int, dict] = {}
                cur.execute(
                    """
                    SELECT "datasetId", "topicId", "topicName", "subfieldId", "subfieldName",
                           "fieldId", "fieldName", "domainId", "domainName", score, source
                    FROM "DatasetTopic"
                    WHERE "datasetId" >= %s AND "datasetId" <= %s
                    """,
                    (current_id, batch_end),
                )
                for (
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
                ) in cur.fetchall():
                    dataset_topic_by_dataset[dataset_id] = {
                        "topicId": topic_id,
                        "topicName": topic_name,
                        "subfieldId": subfield_id,
                        "subfieldName": subfield_name,
                        "fieldId": field_id,
                        "fieldName": field_name,
                        "domainId": domain_id,
                        "domainName": domain_name,
                        "score": score,
                        "source": source,
                    }

                for (
                    dataset_id,
                    identifier,
                    identifier_type,
                    published_at,
                    pub_year,
                    publisher_id,
                ) in datasets_batch:
                    record = {
                        "id": dataset_id,
                        "identifier": identifier,
                        "identifierType": identifier_type,
                        "publishedAt": published_at,
                        "pubYear": pub_year,
                        "publisherId": publisher_id or "unknown",
                        "fuji": fuji_by_dataset.get(dataset_id),
                        "citations": citations_by_dataset.get(dataset_id, []),
                        "mentions": mentions_by_dataset.get(dataset_id, []),
                        "normalizationFactor": normalization_by_dataset.get(dataset_id),
                        "datasetTopic": dataset_topic_by_dataset.get(dataset_id),
                    }
                    current_batch.append(record)
                    total_records += 1
                    if len(current_batch) >= RECORDS_PER_FILE:
                        write_batch_to_file(current_batch, file_number, output_dir)
                        file_number += 1
                        current_batch = []

                pbar.update(len(datasets_batch))
                current_id = batch_end + 1

            pbar.close()

            if current_batch:
                write_batch_to_file(current_batch, file_number, output_dir)

        print("\nDatabase pull completed!")
        print("Summary:")
        print(f"  - Datasets exported: {total_records:,}")
        print(f"  - Output files created: {file_number}")
        print(f"Exported files are available in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
