"""Build identifier to dataset ID mapping from processed dataset NDJSON files."""

import json
import re
from pathlib import Path
from typing import Dict

from tqdm import tqdm

from identifier_mapping import IDENTIFIER_TO_ID_MAP_DIR

# Total record count for progress bar (dataset size)
TOTAL_RECORDS = 49061167
# Max records per output NDJSON file
RECORDS_PER_FILE = 50000


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    # Split filename into text and numeric parts
    parts = re.split(r"(\d+)", name)
    # Convert numeric parts to int, keep text parts as strings
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def build_identifier_to_id_mapping(
    dataset_dir: Path, mapping_dir: Path
) -> Dict[str, int]:
    """Build identifier to ID mapping from processed dataset NDJSON files and save to multiple NDJSON files.

    Writes NDJSON files named 1.ndjson, 2.ndjson, 3.ndjson, ... with up to RECORDS_PER_FILE records each.
    Reads the already-processed dataset files which contain id and identifier fields.
    """
    print("  Building identifier to ID mapping...")

    # Remove mapping directory if it exists and recreate it
    if mapping_dir.exists():
        for p in mapping_dir.glob("*.ndjson"):
            p.unlink()
        print(f"  ✓ Removed existing mapping files in {mapping_dir}")
    mapping_dir.mkdir(parents=True, exist_ok=True)

    # Check if dataset directory exists
    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {dataset_dir}. "
            f"Please run format-raw-data.py first to create the dataset files."
        )

    # Find all NDJSON files in dataset directory
    # Sort by filename using natural sort (alphabetical then numerical)
    dataset_files = []
    for file_path in dataset_dir.glob("*.ndjson"):
        dataset_files.append(file_path)
    dataset_files = sorted(dataset_files, key=natural_sort_key)

    if not dataset_files:
        raise FileNotFoundError(
            f"No NDJSON files found in {dataset_dir}. "
            f"Please run format-raw-data.py first to create the dataset files."
        )

    print(f"  Found {len(dataset_files)} dataset file(s) to process")

    # Process files to build mapping; write NDJSON files 1.ndjson, 2.ndjson, ... (max RECORDS_PER_FILE each)
    mapping: Dict[str, int] = {}
    duplicate_count = 0
    conflict_count = 0
    duplicate_identifiers = set()
    conflict_details = []
    pbar = tqdm(
        total=TOTAL_RECORDS, desc="  Building mapping", unit="record", unit_scale=True
    )

    out_index = 1
    records_in_current_file = 0
    current_out_file = open(mapping_dir / f"{out_index}.ndjson", "w", encoding="utf-8")

    try:
        for file_path in dataset_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError as e:
                            tqdm.write(
                                f"    ⚠️  Error parsing line in {file_path.name}: {e}"
                            )
                            continue

                        dataset_id = record.get("id")
                        identifier = record.get("identifier")

                        if not identifier and "doi" in record:
                            identifier = record.get("doi")
                        if dataset_id is None and "datasetId" in record:
                            dataset_id = record.get("datasetId")
                        if not dataset_id or not identifier:
                            continue
                        try:
                            identifier_lower = (
                                identifier.lower()
                                if isinstance(identifier, str)
                                else str(identifier).strip().lower()
                            )
                            if not identifier_lower:
                                continue
                            dataset_id = int(dataset_id)
                        except (TypeError, ValueError):
                            continue

                        # Write one record to current output file
                        current_out_file.write(
                            json.dumps(
                                {"identifier": identifier_lower, "id": dataset_id},
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
                        records_in_current_file += 1
                        if records_in_current_file >= RECORDS_PER_FILE:
                            current_out_file.close()
                            out_index += 1
                            current_out_file = open(
                                mapping_dir / f"{out_index}.ndjson",
                                "w",
                                encoding="utf-8",
                            )
                            records_in_current_file = 0

                        # Track first occurrence globally for duplicate/conflict stats
                        if identifier_lower in mapping:
                            duplicate_count += 1
                            duplicate_identifiers.add(identifier_lower)
                            if mapping[identifier_lower] != dataset_id:
                                conflict_count += 1
                                conflict_details.append(
                                    {
                                        "identifier": identifier_lower,
                                        "existing_id": mapping[identifier_lower],
                                        "new_id": dataset_id,
                                        "file": file_path.name,
                                    }
                                )
                        else:
                            mapping[identifier_lower] = dataset_id

                        pbar.update(1)
            except FileNotFoundError as e:
                tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
    finally:
        current_out_file.close()

    pbar.close()

    # Report duplicate statistics
    if duplicate_count > 0:
        print(f"  ⚠️  Found {duplicate_count:,} duplicate identifier(s)")
        print(
            f"  ⚠️  {len(duplicate_identifiers):,} unique identifier(s) appear multiple times"
        )
        if conflict_count > 0:
            print(
                f"  ❌ Found {conflict_count:,} conflict(s) (same identifier, different dataset IDs)"
            )
            print("  ⚠️  Showing first 10 conflicts:")
            for i, conflict in enumerate(conflict_details[:10], 1):
                print(
                    f"    {i}. Identifier '{conflict['identifier']}': "
                    f"existing ID {conflict['existing_id']} vs new ID {conflict['new_id']} "
                    f"(from {conflict['file']})"
                )
            if len(conflict_details) > 10:
                print(f"    ... and {len(conflict_details) - 10} more conflict(s)")
        else:
            print("  ✓ All duplicates map to the same dataset ID (no conflicts)")
    else:
        print("  ✓ No duplicate identifiers found")

    # Summary: mapping was written to 1.ndjson, 2.ndjson, ... (max 50k records each)
    num_files = len(list(mapping_dir.glob("*.ndjson")))
    print(
        f"  ✓ Wrote {num_files} NDJSON file(s) to {mapping_dir} ({len(mapping):,} total identifier mappings)"
    )

    return mapping


def main() -> None:
    """Main function to build identifier to ID mapping."""
    print("🚀 Starting identifier to ID mapping build...")

    dataset_folder_name = "dataset"

    # Get OS-agnostic paths
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    dataset_dir = downloads_dir / "database" / dataset_folder_name
    mapping_dir = downloads_dir / "database" / IDENTIFIER_TO_ID_MAP_DIR

    print(f"Reading dataset files from: {dataset_dir}")
    print(f"Mapping output directory: {mapping_dir}")

    # Ensure mapping directory exists
    mapping_dir.mkdir(parents=True, exist_ok=True)

    # Build or load identifier to ID mapping
    print("\n🗺️  Building/loading identifier to ID mapping...")
    identifier_to_id = build_identifier_to_id_mapping(dataset_dir, mapping_dir)
    print(f"  ✓ Mapping contains {len(identifier_to_id):,} identifier entries")

    print("\n✅ Identifier to ID mapping build completed successfully!")
    print(f"🎉 Mapping files are in: {mapping_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during mapping build:")
        print(e)
        exit(1)
