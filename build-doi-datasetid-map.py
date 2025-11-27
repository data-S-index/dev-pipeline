"""Build DOI to dataset ID mapping from processed dataset JSON files."""

import json
from pathlib import Path
from typing import Dict

from tqdm import tqdm


DOI_TO_ID_MAP_FILE = "doi_to_id_map.ndjson"  # Intermediate mapping file


def build_doi_to_id_mapping(dataset_dir: Path, mapping_file: Path) -> Dict[str, int]:
    """Build DOI to ID mapping from processed dataset JSON files and save to file.

    Reads the already-processed dataset files which contain id and doi fields.
    """
    print("  Building DOI to ID mapping...")

    # Check if mapping file already exists
    if mapping_file.exists():
        print(f"  ✓ Found existing mapping file: {mapping_file}")
        try:
            mapping: Dict[str, int] = {}
            with open(mapping_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        record = json.loads(line)
                        doi = record.get("doi", "").lower()
                        dataset_id = record.get("id")
                        if doi and dataset_id:
                            mapping[doi] = dataset_id
            print(f"  ✓ Loaded {len(mapping):,} DOI mappings from file")
            return mapping
        except Exception as e:
            print(f"  ⚠️  Error reading mapping file: {e}")
            print("  Rebuilding mapping...")

    # Check if dataset directory exists
    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {dataset_dir}. "
            f"Please run format-raw-data.py first to create the dataset files."
        )

    # Find all JSON files in dataset directory
    # Sort numerically by filename (e.g., 1.json, 2.json, ...)
    dataset_files = []
    for file_path in dataset_dir.glob("*.json"):
        try:
            # Try to parse filename as integer for proper numeric sorting
            int(file_path.stem)
            dataset_files.append(file_path)
        except ValueError:
            # Skip files that don't have numeric names
            continue
    dataset_files = sorted(dataset_files, key=lambda p: int(p.stem))

    if not dataset_files:
        raise FileNotFoundError(
            f"No JSON files found in {dataset_dir}. "
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
                data = json.load(f)
                if isinstance(data, list):
                    total_records += len(data)
        except Exception:
            continue

    # Process files to build mapping
    mapping: Dict[str, int] = {}
    pbar = tqdm(
        total=total_records, desc="  Building mapping", unit="record", unit_scale=True
    )

    for file_path in dataset_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    continue

                for record in data:
                    try:
                        dataset_id = record.get("id")
                        doi = record.get("doi")

                        if dataset_id and doi:
                            doi_lower = doi.lower()
                            # Store mapping (handle duplicate DOIs by keeping first occurrence)
                            if doi_lower not in mapping:
                                mapping[doi_lower] = dataset_id
                    except (KeyError, TypeError):
                        pass

                    pbar.update(1)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

    pbar.close()

    # Save mapping to file in NDJSON format
    print(f"  💾 Saving mapping to {mapping_file}...")
    with tqdm(
        total=len(mapping), desc="  Saving mapping", unit="record", leave=False
    ) as save_pbar:
        with open(mapping_file, "w", encoding="utf-8") as f:
            for doi, dataset_id in mapping.items():
                record = {"doi": doi, "id": dataset_id}
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                save_pbar.update(1)
    print(f"  ✓ Saved {len(mapping):,} DOI mappings")

    return mapping


def main() -> None:
    """Main function to build DOI to ID mapping."""
    print("🚀 Starting DOI to ID mapping build...")

    dataset_folder_name = "dataset"

    # Get OS-agnostic paths
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    dataset_dir = downloads_dir / "database" / dataset_folder_name
    mapping_file = downloads_dir / "database" / DOI_TO_ID_MAP_FILE

    print(f"Reading dataset files from: {dataset_dir}")
    print(f"Mapping file: {mapping_file}")

    # Ensure mapping file directory exists
    mapping_file.parent.mkdir(parents=True, exist_ok=True)

    # Build or load DOI to ID mapping
    print("\n🗺️  Building/loading DOI to ID mapping...")
    doi_to_id = build_doi_to_id_mapping(dataset_dir, mapping_file)
    print(f"  ✓ Mapping contains {len(doi_to_id):,} DOI entries")

    print("\n✅ DOI to ID mapping build completed successfully!")
    print(f"🎉 Mapping file is available at: {mapping_file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during mapping build:")
        print(e)
        exit(1)
