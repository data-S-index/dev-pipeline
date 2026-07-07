"""Pull dataset ids with their authors into NDJSON files for author-split work.

For every Dataset, exports one NDJSON record containing:
  - id (Dataset.id)
  - authors: [{id, nameType, name, nameIdentifiers, affiliations}, ...] (DatasetAuthor)

Schema reference: prisma/schema.prisma.
Output: E:/s-index-custom/dataset/*.ndjson (directory is wiped first).
"""

import json
import shutil
from pathlib import Path
from typing import Dict, List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Number of dataset IDs fetched from the DB per round trip.
DB_FETCH_BATCH_SIZE = 10000

# Number of dataset records written per output NDJSON file.
RECORDS_PER_FILE = 10000


def write_batch_to_file(batch: list, file_number: int, output_dir: Path) -> None:
    """Write a batch of dataset records to an NDJSON file."""
    file_path = output_dir / f"{file_number}.ndjson"
    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    """Pull dataset ids and their authors into NDJSON files."""
    print("Starting database pull for dataset authors...")

    output_dir = Path("E:/s-index-custom") / "dataset"

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
                    SELECT id
                    FROM "Dataset"
                    WHERE id >= %s AND id <= %s
                    ORDER BY id
                    """,
                    (current_id, batch_end),
                )
                dataset_ids = [row[0] for row in cur.fetchall()]

                if not dataset_ids:
                    current_id = batch_end + 1
                    continue

                authors_by_dataset: Dict[int, list] = {}
                cur.execute(
                    """
                    SELECT "datasetId", id, "nameType", name, "nameIdentifiers", affiliations
                    FROM "DatasetAuthor"
                    WHERE "datasetId" >= %s AND "datasetId" <= %s
                    ORDER BY "datasetId"
                    """,
                    (current_id, batch_end),
                )
                for (
                    dataset_id,
                    author_id,
                    name_type,
                    name,
                    name_identifiers,
                    affiliations,
                ) in cur.fetchall():
                    authors_by_dataset.setdefault(dataset_id, []).append(
                        {
                            "id": author_id,
                            "nameType": name_type,
                            "name": name,
                            "nameIdentifiers": name_identifiers,
                            "affiliations": affiliations,
                        }
                    )

                for dataset_id in dataset_ids:
                    record = {
                        "id": dataset_id,
                        "authors": authors_by_dataset.get(dataset_id, []),
                    }
                    current_batch.append(record)
                    total_records += 1
                    if len(current_batch) >= RECORDS_PER_FILE:
                        write_batch_to_file(current_batch, file_number, output_dir)
                        file_number += 1
                        current_batch = []

                pbar.update(len(dataset_ids))
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
