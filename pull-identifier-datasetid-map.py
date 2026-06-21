"""Pull dataset ID, identifier, and identifierType fields from the database.

For every Dataset, exports one NDJSON record containing:
  - datasetId, identifier, identifierType

Schema reference: prisma/schema.prisma.
Output: Downloads/pulled-database/dataset-id-identifier/*.ndjson (directory is wiped first).
"""

import json
import shutil
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Number of dataset rows fetched from the DB per round trip.
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
    """Pull dataset id, identifier, and identifierType into NDJSON files."""
    print("Starting database pull for identifier-datasetid map...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    output_dir = downloads_dir / "pulled-database" / "dataset-id-identifier"

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
                    SELECT id, identifier, "identifierType"
                    FROM "Dataset"
                    WHERE id >= %s AND id <= %s
                    ORDER BY id
                    """,
                    (current_id, batch_end),
                )
                datasets_batch = cur.fetchall()

                for dataset_id, identifier, identifier_type in datasets_batch:
                    record = {
                        "datasetId": dataset_id,
                        "identifier": identifier,
                        "identifierType": identifier_type,
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
