"""Build identifier to dataset ID mapping from processed dataset NDJSON files."""

import json
import re
from pathlib import Path
from typing import Dict

from tqdm import tqdm


IDENTIFIER_TO_ID_MAP_FILE = "identifier_to_id_map.ndjson"  # Intermediate mapping file


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    # Split filename into text and numeric parts
    parts = re.split(r"(\d+)", name)
    # Convert numeric parts to int, keep text parts as strings
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def build_identifier_to_id_mapping(
    dataset_dir: Path, mapping_file: Path
) -> Dict[str, int]:
    """Build identifier to ID mapping from processed dataset NDJSON files and save to file.

    Reads the already-processed dataset files which contain id and identifier fields.
    """
    print("  Building identifier to ID mapping...")

    # Remove mapping file if it exists
    if mapping_file.exists():
        mapping_file.unlink()
        print(f"  ✓ Removed existing mapping file: {mapping_file}")

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
        if file_path.stem.startswith("datacite-") or file_path.stem.startswith("emdb-"):
            dataset_files.append(file_path)
    dataset_files = sorted(dataset_files, key=natural_sort_key)

    if not dataset_files:
        raise FileNotFoundError(
            f"No NDJSON files found in {dataset_dir}. "
            f"Please run format-raw-data.py first to create the dataset files."
        )

    print(f"  Found {len(dataset_files)} dataset file(s) to process")

    # Count total records first for progress bar
    print("  Counting records in dataset files...")
    total_records = 0
    for file_path in tqdm(
        dataset_files, desc="  Counting records", unit="file", leave=False
    ):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    # Only count valid JSON records
                    try:
                        json.loads(line)
                        total_records += 1
                    except json.JSONDecodeError:
                        continue
        except Exception:
            continue

    # Process files to build mapping
    mapping: Dict[str, int] = {}
    duplicate_count = 0
    conflict_count = 0
    duplicate_identifiers = set()
    conflict_details = []
    pbar = tqdm(
        total=total_records, desc="  Building mapping", unit="record", unit_scale=True
    )

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

                    try:
                        dataset_id = record.get("id")
                        identifier = record.get("identifier")

                        if dataset_id and identifier:
                            identifier_lower = identifier.lower()
                            # Check for duplicates and conflicts
                            if identifier_lower in mapping:
                                duplicate_count += 1
                                duplicate_identifiers.add(identifier_lower)
                                # Check if it's a conflict (different dataset_id)
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
                                # Store mapping (first occurrence)
                                mapping[identifier_lower] = dataset_id
                    except (KeyError, TypeError):
                        pass

                    pbar.update(1)
        except FileNotFoundError as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

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

    # Save mapping to file in NDJSON format
    print(f"  💾 Saving mapping to {mapping_file}...")
    with tqdm(
        total=len(mapping),
        desc="  Saving mapping",
        unit="record",
        leave=False,
        unit_scale=True,
    ) as save_pbar:
        with open(mapping_file, "w", encoding="utf-8") as f:
            for identifier, dataset_id in mapping.items():
                record = {"identifier": identifier, "id": dataset_id}
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                save_pbar.update(1)
    print(f"  ✓ Saved {len(mapping):,} identifier mappings")

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
    mapping_file = downloads_dir / "database" / IDENTIFIER_TO_ID_MAP_FILE

    print(f"Reading dataset files from: {dataset_dir}")
    print(f"Mapping file: {mapping_file}")

    # Ensure mapping file directory exists
    mapping_file.parent.mkdir(parents=True, exist_ok=True)

    # Build or load identifier to ID mapping
    print("\n🗺️  Building/loading identifier to ID mapping...")
    identifier_to_id = build_identifier_to_id_mapping(dataset_dir, mapping_file)
    print(f"  ✓ Mapping contains {len(identifier_to_id):,} identifier entries")

    print("\n✅ Identifier to ID mapping build completed successfully!")
    print(f"🎉 Mapping file is available at: {mapping_file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during mapping build:")
        print(e)
        exit(1)
