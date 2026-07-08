"""Pull latest D-index rows up to a dataset id cutoff into NDJSON files.

Exports one row per dataset from the "DIndex" table, keeping only the latest
entry per dataset by:
  1) highest year
  2) newest created timestamp (tie-breaker)

Each output record contains exactly:
	- datasetId
	- identifier
	- identifierType
	- score

Default cutoff is inclusive and set to datasetId 49061167.
Output: ~/Downloads/pulled-database/dindex/*.ndjson (directory is wiped first).
"""

import json
import shutil
from pathlib import Path
from typing import List, Optional

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Upper bound for dataset ids (inclusive).
DEFAULT_MAX_DATASET_ID = 49061167

# Number of latest d-index rows fetched from DB per round trip.
DB_FETCH_BATCH_SIZE = 100000

# Number of records written per output NDJSON file.
RECORDS_PER_FILE = 10000


def write_batch_to_file(batch: List[dict], file_number: int, output_dir: Path) -> None:
	"""Write one batch of rows to an NDJSON file."""
	file_path = output_dir / f"{file_number}.ndjson"
	with open(file_path, "w", encoding="utf-8") as f:
		for record in batch:
			f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _fetch_next_batch(
	cur: psycopg.Cursor,
	last_dataset_id: int,
	max_dataset_id: int,
	batch_size: int,
) -> List[dict]:
	"""Fetch the next batch of latest D-index rows using keyset pagination."""
	cur.execute(
		"""
		WITH latest AS (
			SELECT DISTINCT ON ("datasetId")
				"datasetId", score, year, created
			FROM "DIndex"
			WHERE "datasetId" > %s
			  AND "datasetId" <= %s
			ORDER BY "datasetId", year DESC, created DESC
			LIMIT %s
		)
		SELECT l."datasetId", d.identifier, d."identifierType", l.score
		FROM latest l
		JOIN "Dataset" d ON d.id = l."datasetId"
		ORDER BY l."datasetId"
		""",
		(last_dataset_id, max_dataset_id, batch_size),
	)

	return [
		{
			"datasetId": dataset_id,
			"identifier": identifier,
			"identifierType": identifier_type,
			"score": float(score),
		}
		for dataset_id, identifier, identifier_type, score in cur.fetchall()
	]


def main(max_dataset_id: Optional[int] = None) -> None:
	"""Pull latest d-index rows into NDJSON files up to an inclusive dataset id."""
	max_dataset_id = (
		DEFAULT_MAX_DATASET_ID if max_dataset_id is None else int(max_dataset_id)
	)

	if max_dataset_id < 1:
		raise ValueError("max_dataset_id must be >= 1")

	print("Starting database pull for latest d-index rows...")
	print(f"Dataset ID cutoff (inclusive): {max_dataset_id:,}")

	output_dir = Path.home() / "Downloads" / "pulled-database" / "dindex"
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
			cur.execute(
				"""
				SELECT COUNT(DISTINCT "datasetId")
				FROM "DIndex"
				WHERE "datasetId" <= %s
				""",
				(max_dataset_id,),
			)
			(total_datasets_with_dindex,) = cur.fetchone()

		print(
			"Processing "
			f"{total_datasets_with_dindex:,} dataset(s) with D-index rows "
			f"up to datasetId {max_dataset_id:,}"
		)

		file_number = 1
		current_batch: List[dict] = []
		total_records = 0
		last_dataset_id = 0

		with conn.cursor() as cur:
			pbar = tqdm(
				total=total_datasets_with_dindex,
				desc="Pulling latest d-index",
				unit="dataset",
				unit_scale=True,
			)

			while last_dataset_id < max_dataset_id:
				rows = _fetch_next_batch(
					cur,
					last_dataset_id=last_dataset_id,
					max_dataset_id=max_dataset_id,
					batch_size=DB_FETCH_BATCH_SIZE,
				)

				if not rows:
					break

				for record in rows:
					current_batch.append(record)
					total_records += 1
					if len(current_batch) >= RECORDS_PER_FILE:
						write_batch_to_file(current_batch, file_number, output_dir)
						file_number += 1
						current_batch = []

				pbar.update(len(rows))
				last_dataset_id = rows[-1]["datasetId"]

			pbar.close()

			if current_batch:
				write_batch_to_file(current_batch, file_number, output_dir)

		print("\nDatabase pull completed!")
		print("Summary:")
		print(f"  - Latest d-index rows exported: {total_records:,}")
		print(f"  - Output files created: {file_number}")
		print(f"Exported files are available in: {output_dir}")


if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		print(f"\nFatal error: {e}")
		exit(1)
