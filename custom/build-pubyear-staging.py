"""Build staging files for the Dataset.pubYear / publisherId bulk update.

Pipeline:
  1. Load the identifier -> datasetId map (built by pull-identifier-datasetid-map.py)
     from Downloads/pulled-database/dataset-id-identifier into memory.
  2. Walk every NDJSON file in Downloads/pubyear. Each line looks like:
       {"identifiers": [{"identifier": "...", "identifier_type": "doi"}],
        "pubyear": 2026, "publisher_id": "..."}
     Resolve the single identifier to a datasetId via the in-memory map and
     write {"datasetId": ..., "pubYear": ..., "publisherId": ...} staging
     records to Downloads/pulled-database/pubyear-update (directory is wiped
     first). If the same identifier is seen again in a later file, it is
     skipped and recorded for investigation.

Entries that don't resolve to a datasetId, or that are duplicates of an
identifier already staged, are written to duplicates.json/unmatched.json
in the staging directory for later investigation.

Run custom-update-pubyear.py afterwards to apply the staged updates.
"""

import json
from pathlib import Path
from typing import Dict

from tqdm import tqdm

# Number of staging records written per output NDJSON file.
RECORDS_PER_FILE = 10000


def map_key(identifier: str) -> str:
    """Build a lookup key from the identifier value alone."""
    return identifier.strip().lower()


def load_identifier_to_dataset_id_map(map_dir: Path) -> Dict[str, int]:
    """Load the identifier -> datasetId map into memory."""
    print(f"Loading identifier-to-datasetId map from {map_dir}...")

    if not map_dir.exists():
        raise FileNotFoundError(
            f"Map directory not found: {map_dir}. "
            "Run pull-identifier-datasetid-map.py first."
        )

    map_files = sorted(map_dir.glob("*.ndjson"))
    if not map_files:
        raise FileNotFoundError(f"No NDJSON files found in {map_dir}.")

    mapping: Dict[str, int] = {}
    for file_path in tqdm(map_files, desc="Loading map files", unit="file"):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                identifier = record.get("identifier")
                identifier_type = record.get("identifierType")
                dataset_id = record.get("datasetId")
                if not identifier or not identifier_type or dataset_id is None:
                    continue
                mapping[map_key(identifier)] = dataset_id

    print(f"  Loaded {len(mapping):,} identifier entries")
    return mapping


def write_batch_to_file(batch: list, file_number: int, output_dir: Path) -> None:
    """Write a batch of staging records to an NDJSON file."""
    file_path = output_dir / f"{file_number}.ndjson"
    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def build_staging_files(
    pubyear_dir: Path, identifier_map: Dict[str, int], staging_dir: Path
) -> None:
    """Resolve pubyear entries to datasetIds and write staging NDJSON files."""
    pubyear_files = sorted(pubyear_dir.glob("*.ndjson"))
    if not pubyear_files:
        raise FileNotFoundError(f"No NDJSON files found in {pubyear_dir}.")

    print(f"Found {len(pubyear_files)} pubyear file(s) in {pubyear_dir}")

    if staging_dir.exists():
        for p in staging_dir.glob("*.ndjson"):
            p.unlink()
        for p in staging_dir.glob("*.json"):
            p.unlink()
        print(f"  Removed existing staging files in {staging_dir}")
    staging_dir.mkdir(parents=True, exist_ok=True)

    seen_identifiers: Dict[str, str] = {}  # key -> file it was first staged from
    file_number = 1
    current_batch: list = []
    matched_count = 0
    unmatched_records: list = []
    duplicate_records: list = []

    for file_path in tqdm(pubyear_files, desc="Processing pubyear files", unit="file"):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    tqdm.write(f"  Error parsing line in {file_path.name}: {e}")
                    continue

                identifiers = record.get("identifiers") or []
                if not identifiers:
                    continue
                entry = identifiers[0]
                identifier = entry.get("identifier")
                identifier_type = entry.get("identifier_type")
                pub_year = record.get("pubyear")
                publisher_id = record.get("publisher_id") or "unknown"
                if not identifier or not identifier_type or pub_year is None:
                    continue

                key = map_key(identifier)
                if key in seen_identifiers:
                    duplicate_records.append(
                        {
                            "identifier": identifier,
                            "identifierType": identifier_type,
                            "pubYear": pub_year,
                            "publisherId": publisher_id,
                            "file": file_path.name,
                            "firstSeenInFile": seen_identifiers[key],
                        }
                    )
                    continue
                seen_identifiers[key] = file_path.name

                dataset_id = identifier_map.get(key)
                if dataset_id is None:
                    unmatched_records.append(
                        {
                            "identifier": identifier,
                            "identifierType": identifier_type,
                            "pubYear": pub_year,
                            "publisherId": publisher_id,
                            "file": file_path.name,
                        }
                    )
                    continue

                current_batch.append(
                    {
                        "datasetId": dataset_id,
                        "pubYear": int(pub_year),
                        "publisherId": publisher_id,
                    }
                )
                matched_count += 1
                if len(current_batch) >= RECORDS_PER_FILE:
                    write_batch_to_file(current_batch, file_number, staging_dir)
                    file_number += 1
                    current_batch = []

    if current_batch:
        write_batch_to_file(current_batch, file_number, staging_dir)

    with open(staging_dir / "unmatched.json", "w", encoding="utf-8") as f:
        json.dump(unmatched_records, f, ensure_ascii=False, indent=2)
    with open(staging_dir / "duplicates.json", "w", encoding="utf-8") as f:
        json.dump(duplicate_records, f, ensure_ascii=False, indent=2)

    print("Staging build complete:")
    print(f"  - Matched: {matched_count:,}")
    print(f"  - Unmatched (no datasetId found): {len(unmatched_records):,} -> unmatched.json")
    print(f"  - Duplicate identifiers skipped: {len(duplicate_records):,} -> duplicates.json")


def main() -> None:
    """Resolve pubyear data to datasetIds and write staging files for review."""
    print("Starting pubYear staging build...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    pubyear_dir = downloads_dir / "pubyear"
    map_dir = downloads_dir / "pulled-database" / "dataset-id-identifier"
    staging_dir = downloads_dir / "pulled-database" / "pubyear-update"

    print(f"Pubyear input directory: {pubyear_dir}")
    print(f"Identifier map directory: {map_dir}")
    print(f"Staging output directory: {staging_dir}")

    identifier_map = load_identifier_to_dataset_id_map(map_dir)
    build_staging_files(pubyear_dir, identifier_map, staging_dir)

    print("\nStaging build completed successfully!")
    print(f"Review unmatched.json / duplicates.json in {staging_dir}")
    print("Run custom-update-pubyear.py to apply the staged updates.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
