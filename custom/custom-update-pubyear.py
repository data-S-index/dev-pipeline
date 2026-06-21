"""Apply staged Dataset.pubYear / publisherId updates to the database.

Streams the staging NDJSON files written by build-pubyear-staging.py from
Downloads/pulled-database/pubyear-update and applies the pubYear and
publisherId updates to the database in batches, using a single
UPDATE ... FROM (VALUES ...) statement per batch. Reading and writing are
interleaved (each batch is applied as soon as it's read) so the whole
70M-row staging set never has to sit in memory at once.
"""

from pathlib import Path
from typing import Tuple
import json

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Number of (datasetId, pubYear, publisherId) rows applied per UPDATE statement.
# Matches build-pubyear-staging.py's RECORDS_PER_FILE so each staging file
# normally produces exactly one UPDATE batch.
DB_UPDATE_BATCH_SIZE = 10000


def run_batch_update(cur, batch: list[Tuple[int, int, str]]) -> None:
    """Apply one batch of (datasetId, pubYear, publisherId) updates."""
    values_clause = ", ".join(["(%s::int, %s::int, %s::text)"] * len(batch))
    query = f"""
        UPDATE "Dataset" AS d
        SET "pubYear" = v.pub_year, "publisherId" = v.publisher_id
        FROM (VALUES {values_clause}) AS v(dataset_id, pub_year, publisher_id)
        WHERE d.id = v.dataset_id
    """
    params = [value for row in batch for value in row]
    cur.execute(query, params)


def apply_updates(staging_dir: Path) -> int:
    """Stream staging files and apply their updates to the database as they're read.

    Progress is tracked per staging file (each holds ~DB_UPDATE_BATCH_SIZE
    records) rather than per row, since the total row count isn't known
    until all files are read.
    """
    staging_files = sorted(staging_dir.glob("*.ndjson"))
    if not staging_files:
        raise FileNotFoundError(
            f"No staging NDJSON files found in {staging_dir}. "
            "Run build-pubyear-staging.py first."
        )

    total = 0
    batch: list[Tuple[int, int, str]] = []
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for file_path in tqdm(staging_files, desc="Updating Dataset", unit="file"):
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        record = json.loads(line)
                        batch.append(
                            (record["datasetId"], record["pubYear"], record["publisherId"])
                        )
                        if len(batch) >= DB_UPDATE_BATCH_SIZE:
                            run_batch_update(cur, batch)
                            total += len(batch)
                            batch = []
            if batch:
                run_batch_update(cur, batch)
                total += len(batch)
        conn.commit()

    print(f"  Database update committed ({total:,} rows)")
    return total


def main() -> None:
    """Apply the staged pubYear/publisherId updates to the database."""
    print("Starting pubYear database update...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    staging_dir = downloads_dir / "pulled-database" / "pubyear-update"

    print(f"Staging input directory: {staging_dir}")

    apply_updates(staging_dir)

    print("\npubYear update completed successfully!")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
