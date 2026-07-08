"""Pull latest D-index rows up to a dataset id cutoff into NDJSON files.

Runs in two phases, each independently re-runnable:

  1) pull_raw_csv: bulk-export every raw "DIndex" row (datasetId, score,
     year, created) up to the datasetId cutoff into local CSV files via a
     single server-side `COPY ... TO STDOUT`. This replaces the previous
     approach of paginated `SELECT DISTINCT ON` queries, which was slow
     over a remote connection due to per-batch round trips.
  2) load_raw_csv_latest + main: read the local CSV files back and reduce
     to the latest row per dataset (highest year, then newest created,
     matching the old SQL's `ORDER BY year DESC, created DESC`), join with
     the identifier map, and write the final NDJSON output. This is all
     local file I/O -- no further DB round trips.

Pass --skip-pull to reuse a previous raw CSV pull and only re-run phase 2
(e.g. while iterating on the reduction/join logic). The datasetId cutoff
used with --skip-pull must be <= the cutoff used for the original pull,
since rows above that cutoff were never downloaded.

Each output record contains exactly:
        - datasetId
        - identifier
        - identifierType
        - score

identifier/identifierType are looked up locally from
~/Downloads/pulled-database/dataset-id-identifier (pull-identifier-datasetid-map.py's
output) instead of joining the "Dataset" table in the DB query.

Default cutoff is inclusive and set to datasetId 49061167.
Raw pull:  ~/Downloads/pulled-database/dindex-raw/*.csv (wiped before each pull).
Output:    ~/Downloads/pulled-database/dindex/*.ndjson (directory is wiped first).
"""

import argparse
import csv
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import orjson
import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Upper bound for dataset ids (inclusive).
DEFAULT_MAX_DATASET_ID = 49061167

# Rows per raw CSV file pulled from the DB.
RAW_ROWS_PER_FILE = 1_000_000

# Number of records written per output NDJSON file.
RECORDS_PER_FILE = 10000


def load_dataset_identifiers(
    identifier_dir: Path, max_dataset_id: int
) -> Dict[int, Tuple[str, str]]:
    """Load datasetId -> (identifier, identifierType) from pulled-database/dataset-id-identifier.

    Only records with datasetId <= max_dataset_id are kept.
    """
    mapping: Dict[int, Tuple[str, str]] = {}
    files = sorted(identifier_dir.glob("*.ndjson"), key=lambda p: int(p.stem))

    for file_path in tqdm(files, desc="Loading dataset id/identifier map", unit="file"):
        with open(file_path, "rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = orjson.loads(line)
                dataset_id = record["datasetId"]
                if dataset_id > max_dataset_id:
                    continue
                mapping[dataset_id] = (
                    record["identifier"],
                    record["identifierType"],
                )

    return mapping


def write_batch_to_file(batch: List[dict], file_number: int, output_dir: Path) -> None:
    """Write one batch of rows to an NDJSON file."""
    file_path = output_dir / f"{file_number}.ndjson"
    with open(file_path, "wb") as f:
        for record in batch:
            f.write(orjson.dumps(record) + b"\n")


def pull_raw_csv(conn: psycopg.Connection, max_dataset_id: int, raw_dir: Path) -> int:
    """Bulk-export raw DIndex rows (datasetId, score, year, created) to local CSV files.

    Uses a single server-side COPY (sequential scan, no ORDER BY/DISTINCT ON)
    instead of paginated queries, then splits the stream into local files of
    up to RAW_ROWS_PER_FILE rows each. Returns the total number of rows written.
    """
    with conn.cursor() as count_cur:
        count_cur.execute(
            'SELECT COUNT(*) FROM "DIndex" WHERE "datasetId" <= %s',
            (max_dataset_id,),
        )
        (total_rows,) = count_cur.fetchone()

    file_number = 1
    rows_in_file = 0
    total_rows_written = 0
    buffer = b""
    out_f = open(raw_dir / f"{file_number}.csv", "wb")

    with conn.cursor() as cur, cur.copy(
        'COPY (SELECT "datasetId", score, year, created '
        'FROM "DIndex" WHERE "datasetId" <= %s) TO STDOUT WITH (FORMAT csv)',
        (max_dataset_id,),
    ) as copy, tqdm(
        total=total_rows, desc="Pulling raw DIndex rows", unit="row", unit_scale=True
    ) as pbar:
        for data in copy:
            buffer += data
            last_newline = buffer.rfind(b"\n")
            if last_newline == -1:
                continue

            complete, buffer = buffer[: last_newline + 1], buffer[last_newline + 1 :]
            out_f.write(complete)

            n_lines = complete.count(b"\n")
            rows_in_file += n_lines
            total_rows_written += n_lines
            pbar.update(n_lines)

            if rows_in_file >= RAW_ROWS_PER_FILE:
                out_f.close()
                file_number += 1
                out_f = open(raw_dir / f"{file_number}.csv", "wb")
                rows_in_file = 0

        if buffer:
            out_f.write(buffer)
            total_rows_written += 1
            pbar.update(1)

    out_f.close()
    return total_rows_written


def load_raw_csv_latest(
    raw_dir: Path, max_dataset_id: int
) -> Dict[int, Tuple[float, int, str]]:
    """Reduce the raw CSV pull to the latest row per dataset.

    "Latest" matches the old SQL ordering: highest year, then newest created
    (as a tie-breaker). Returns datasetId -> (score, year, created).
    """
    latest: Dict[int, Tuple[float, int, str]] = {}
    files = sorted(raw_dir.glob("*.csv"), key=lambda p: int(p.stem))

    for file_path in tqdm(files, desc="Reducing raw DIndex rows", unit="file"):
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            for dataset_id_str, score_str, year_str, created in csv.reader(f):
                dataset_id = int(dataset_id_str)
                if dataset_id > max_dataset_id:
                    continue
                year = int(year_str)
                existing = latest.get(dataset_id)
                if existing is None or (year, created) > (existing[1], existing[2]):
                    latest[dataset_id] = (float(score_str), year, created)

    return latest


def main(max_dataset_id: Optional[int] = None, skip_pull: bool = False) -> None:
    """Pull latest d-index rows into NDJSON files up to an inclusive dataset id."""
    max_dataset_id = (
        DEFAULT_MAX_DATASET_ID if max_dataset_id is None else int(max_dataset_id)
    )

    if max_dataset_id < 1:
        raise ValueError("max_dataset_id must be >= 1")

    print("Starting database pull for latest d-index rows...")
    print(f"Dataset ID cutoff (inclusive): {max_dataset_id:,}")

    identifier_dir = (
        Path.home() / "Downloads" / "pulled-database" / "dataset-id-identifier"
    )
    if not identifier_dir.exists():
        raise FileNotFoundError(
            f"Identifier directory not found: {identifier_dir}. "
            "Run pull-identifier-datasetid-map.py first."
        )

    raw_dir = Path.home() / "Downloads" / "pulled-database" / "dindex-raw"

    if skip_pull:
        if not raw_dir.exists() or not any(raw_dir.glob("*.csv")):
            raise FileNotFoundError(
                f"--skip-pull was set but no raw CSV files found in {raw_dir}. "
                "Run without --skip-pull first."
            )
        print(f"\nSkipping DB pull, reusing raw CSV files in: {raw_dir}")
    else:
        if raw_dir.exists():
            shutil.rmtree(raw_dir)
        raw_dir.mkdir(parents=True, exist_ok=True)

        print("\nConnecting to database...")
        with psycopg.connect(DATABASE_URL) as conn:
            print("Connected to database")
            print(
                f"\nPulling raw DIndex rows (datasetId <= {max_dataset_id:,}) to: {raw_dir}"
            )
            total_raw_rows = pull_raw_csv(conn, max_dataset_id, raw_dir)
            print(f"Raw rows pulled: {total_raw_rows:,}")

    print("\nReducing raw CSV rows to latest per dataset...")
    latest_by_dataset = load_raw_csv_latest(raw_dir, max_dataset_id)
    print(f"Datasets with D-index rows: {len(latest_by_dataset):,}")

    print(f"\nLoading dataset id/identifier map from: {identifier_dir}")
    id_to_identifier = load_dataset_identifiers(identifier_dir, max_dataset_id)
    print(f"Loaded {len(id_to_identifier):,} dataset id/identifier pairs")

    output_dir = Path.home() / "Downloads" / "pulled-database" / "dindex"
    print(f"Output directory: {output_dir}")

    if output_dir.exists():
        shutil.rmtree(output_dir)
        print("Output directory cleaned")
    output_dir.mkdir(parents=True, exist_ok=True)
    print("Output directory ready")

    file_number = 1
    current_batch: List[dict] = []
    total_records = 0

    for dataset_id, (score, _year, _created) in tqdm(
        latest_by_dataset.items(),
        desc="Building final NDJSON",
        unit="dataset",
        unit_scale=True,
    ):
        identifier_info = id_to_identifier.get(dataset_id)
        if identifier_info is None:
            continue
        identifier, identifier_type = identifier_info
        current_batch.append(
            {
                "datasetId": dataset_id,
                "identifier": identifier,
                "identifierType": identifier_type,
                "score": score,
            }
        )
        total_records += 1
        if len(current_batch) >= RECORDS_PER_FILE:
            write_batch_to_file(current_batch, file_number, output_dir)
            file_number += 1
            current_batch = []

    if current_batch:
        write_batch_to_file(current_batch, file_number, output_dir)

    print("\nDatabase pull completed!")
    print("Summary:")
    print(f"  - Latest d-index rows exported: {total_records:,}")
    print(f"  - Output files created: {file_number}")
    print(f"Exported files are available in: {output_dir}")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-dataset-id",
        type=int,
        default=None,
        help=f"Inclusive dataset id cutoff (default: {DEFAULT_MAX_DATASET_ID:,}).",
    )
    parser.add_argument(
        "--skip-pull",
        action="store_true",
        help="Reuse a previous raw CSV pull instead of re-querying the database.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        main(max_dataset_id=args.max_dataset_id, skip_pull=args.skip_pull)
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
