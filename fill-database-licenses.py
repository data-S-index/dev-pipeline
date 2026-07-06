"""Fill database with dataset rights/license data from raw NDJSON files.

Two-phase process, mirroring merge-citations.py's identifier resolution:

  1. extract_rights_files(): read the raw NDJSON files exported from the
     licenses source (Downloads/licenses/*.ndjson), where each record looks
     like:
         {"identifiers": [{"identifier": "<doi>", "identifier_type": "doi"}],
          "rights": [{"rights": "...", "rights_uri": "...",
                      "rights_identifier": "...", "scheme_uri": "..."}]}
     Resolve each record's DOI to an internal datasetId using the
     identifier -> datasetId map (built by pull-identifier-datasetid-map.py),
     and write one resolved output record per rights entry to
     Downloads/pulled-database/rights/<n>.ndjson (sequentially numbered,
     1.ndjson, 2.ndjson, ...), each with an explicit incrementing "id" for
     tracking/inspection (not used as the DB row id — DatasetRights still
     autoincrements its own id on insert). Records with no "rights" field are
     skipped (nothing to insert). Unmatched identifiers are recorded in a
     report.

  2. process_rights_files(): read the resolved NDJSON files back and bulk
     load them into the DatasetRights table. DatasetRights has no natural
     unique key (a dataset can have multiple rights rows), so the table is
     truncated first, allowing this script to be re-run without duplicating
     rows.
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

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

    print(f"  Loaded {len(mapping):,} identifier entries")
    return mapping


def pick_identifier(record: dict) -> str:
    """Pick the DOI from a record's identifiers list, preferring identifier_type == 'doi'."""
    identifiers = record.get("identifiers") or []
    for entry in identifiers:
        if entry.get("identifier_type") == "doi":
            return entry.get("identifier")
    return identifiers[0].get("identifier") if identifiers else None


def clean_uri(uri: Optional[str]) -> Optional[str]:
    """Return uri only if it looks like an actual http(s) URL.

    Some rights_uri values are non-URL scheme identifiers (e.g.
    "info:eu-repo/semantics/restrictedAccess"), which shouldn't be stored
    in the uri column.
    """
    if not uri:
        return None
    parsed = urlparse(uri)
    if parsed.scheme in ("http", "https") and parsed.netloc:
        return uri
    return None


def extract_rights_files(
    licenses_dir: Path,
    output_dir: Path,
    identifier_to_id: Dict[str, int],
) -> Tuple[int, int, Dict[str, int]]:
    """Resolve raw license NDJSON files to DatasetRights rows.

    Reads every *.ndjson file in licenses_dir, resolves each record's DOI to
    a datasetId, and writes one JSON line per rights entry (id, datasetId,
    name, identifier, uri) to a sequentially numbered file in output_dir.
    Records without a "rights" field are skipped. Returns
    (total_records_read, total_rows_written, unmatched_identifier_counts).
    """
    print("📄 Extracting rights records from raw NDJSON files...")

    ndjson_files = load_ndjson_files(licenses_dir)
    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0, 0, {}

    print(f"  Found {len(ndjson_files)} ndjson file(s)")
    output_dir.mkdir(parents=True, exist_ok=True)

    line_count = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        line_count += 1
        except Exception:
            continue

    print(f"  Processing {line_count:,} raw records...")

    total_records = 0
    total_rows = 0
    no_rights_count = 0
    next_id = 1
    unmatched: Dict[str, int] = {}

    pbar = tqdm(total=line_count, desc="  Extracting", unit="record", unit_scale=True)

    for file_index, file_path in enumerate(ndjson_files, start=1):
        out_path = output_dir / f"{file_index}.ndjson"
        try:
            with open(file_path, "r", encoding="utf-8") as f, open(
                out_path, "w", encoding="utf-8"
            ) as out_f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    total_records += 1

                    try:
                        record = json.loads(line)

                        rights_list = record.get("rights")
                        if not rights_list:
                            no_rights_count += 1
                            pbar.update(1)
                            continue

                        identifier = pick_identifier(record)
                        if not identifier:
                            pbar.update(1)
                            continue

                        key = map_key(identifier)
                        dataset_id = identifier_to_id.get(key)
                        if dataset_id is None:
                            unmatched[key] = unmatched.get(key, 0) + 1
                            pbar.update(1)
                            continue

                        for rights_entry in rights_list:
                            name = rights_entry.get("rights") or None
                            rights_identifier = (
                                rights_entry.get("rights_identifier") or None
                            )
                            if rights_identifier is not None:
                                rights_identifier = rights_identifier.strip().lower()
                            uri = clean_uri(rights_entry.get("rights_uri"))
                            if (
                                name is None
                                and rights_identifier is None
                                and uri is None
                            ):
                                continue

                            out_f.write(
                                json.dumps(
                                    {
                                        "id": next_id,
                                        "datasetId": dataset_id,
                                        "name": name,
                                        "identifier": rights_identifier,
                                        "uri": uri,
                                    }
                                )
                                + "\n"
                            )
                            next_id += 1
                            total_rows += 1

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

    print(f"  Records without rights: {no_rights_count:,}")
    print(f"  Unmatched identifiers: {len(unmatched):,}")

    return total_records, total_rows, unmatched


def insert_rights_batch(
    conn: psycopg.Connection,
    rights_rows: List[tuple],
) -> None:
    """Insert a batch of DatasetRights rows via COPY."""
    if not rights_rows:
        return

    with conn.cursor() as cur:
        with cur.copy(
            """COPY "DatasetRights" ("datasetId", name, identifier, uri, created, updated)
               FROM STDIN"""
        ) as copy:
            for row in rights_rows:
                copy.write_row(row)
    conn.commit()


def process_rights_files(conn: psycopg.Connection, rights_dir: Path) -> int:
    """Stream resolved rights NDJSON files and load into DatasetRights table.

    The table is truncated first so this can be re-run without duplicating
    rows (DatasetRights has no natural unique key beyond its own id).
    """
    print("📊 Loading rights records into the database...")

    ndjson_files = load_ndjson_files(rights_dir)
    if not ndjson_files:
        print("  ⚠️  No resolved ndjson files found")
        return 0

    print(f"  Found {len(ndjson_files)} ndjson file(s)")

    line_count = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        line_count += 1
        except Exception:
            continue

    print(f"  Loading {line_count:,} resolved records...")

    with conn.cursor() as cur:
        cur.execute('TRUNCATE TABLE "DatasetRights" RESTART IDENTITY')
    conn.commit()

    rights_rows: List[tuple] = []
    total_inserted = 0
    now = datetime.now()

    pbar = tqdm(total=line_count, desc="  Loading", unit="record", unit_scale=True)

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
                            pbar.update(1)
                            continue

                        row = (
                            int(dataset_id),
                            record.get("name"),
                            record.get("identifier"),
                            record.get("uri"),
                            now,
                            now,
                        )
                        rights_rows.append(row)
                        total_inserted += 1
                        pbar.update(1)

                        if len(rights_rows) >= BATCH_SIZE:
                            insert_rights_batch(conn, rights_rows)
                            rights_rows = []

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

    if rights_rows:
        insert_rights_batch(conn, rights_rows)

    return total_inserted


def main() -> None:
    """Main function to fill database with dataset rights/license data."""
    print("🚀 Starting dataset rights database fill...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    licenses_dir = downloads_dir / "licenses"
    rights_dir = downloads_dir / "pulled-database" / "rights"
    map_dir = downloads_dir / "pulled-database" / "dataset-id-identifier"
    report_dir = downloads_dir / "pulled-database" / "rights-merge-report"

    print(f"Licenses directory: {licenses_dir}")
    print(f"Resolved rights directory: {rights_dir}")
    print(f"Identifier map directory: {map_dir}")

    if not licenses_dir.exists():
        raise FileNotFoundError(f"Licenses directory not found: {licenses_dir}")

    print("\n🗺️  Loading identifier to dataset ID mapping...")
    identifier_to_id = load_identifier_to_dataset_id_map(map_dir)

    print()
    total_records, total_rows, unmatched = extract_rights_files(
        licenses_dir, rights_dir, identifier_to_id
    )

    report_dir.mkdir(parents=True, exist_ok=True)
    with open(report_dir / "unmatched.json", "w", encoding="utf-8") as f:
        json.dump(unmatched, f, ensure_ascii=False, indent=2)

    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            inserted_count = process_rights_files(conn, rights_dir)

            print("\n✅ Dataset rights database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - Raw records read: {total_records:,}")
            print(f"  - Rights rows extracted: {total_rows:,}")
            print(f"  - Rights rows inserted: {inserted_count:,}")
            print(
                f"  - Unmatched identifiers: {len(unmatched):,} -> {report_dir / 'unmatched.json'}"
            )

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
