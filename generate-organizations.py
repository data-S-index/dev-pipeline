"""Generate a distinct list of unique organizations from format-raw-data output (dataset NDJSON)."""

import json
import re
from pathlib import Path
from typing import Any, Dict, List

from tqdm import tqdm
from ulid import ULID


ORGANIZATIONS_PER_FILE = 10_000


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def collect_unique_organizations_with_datasets(
    dataset_dir: Path,
) -> List[Dict[str, Any]]:
    """Read all dataset NDJSON files; return unique organizations (by name) with their dataset IDs."""
    ndjson_files = sorted(dataset_dir.glob("*.ndjson"), key=natural_sort_key)
    if not ndjson_files:
        return []

    # lowercased name -> (display name from first occurrence, set of dataset ids)
    organization_map: Dict[str, tuple] = {}

    for file_path in tqdm(ndjson_files, desc="Scanning dataset files", unit="file"):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                dataset_id = record.get("id")
                if dataset_id is None:
                    continue
                authors = record.get("authors") or []
                for author in authors:
                    if not isinstance(author, dict):
                        continue
                    affiliations = author.get("affiliations")
                    if not affiliations or not isinstance(affiliations, list):
                        continue
                    for aff in affiliations:
                        if not isinstance(aff, str):
                            continue
                        name = aff.strip()
                        # if name is between parentheses, remove the parentheses
                        name = re.sub(r"\(.*\)", "", name)
                        name = name.strip()
                        if not name:
                            continue
                        key = name.lower()
                        if key not in organization_map:
                            organization_map[key] = (name, set())
                        organization_map[key][1].add(dataset_id)

    result: List[Dict[str, Any]] = [
        {"id": str(ULID()), "name": display_name, "datasetIds": sorted(dataset_ids)}
        for display_name, dataset_ids in organization_map.values()
    ]
    return result


def write_organization_batches(
    organizations: List[Dict[str, Any]],
    output_dir: Path,
    organizations_per_file: int = ORGANIZATIONS_PER_FILE,
) -> int:
    """Write organizations to NDJSON files with at most organizations_per_file per file. Returns file count."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_number = 0
    for i in range(0, len(organizations), organizations_per_file):
        batch = organizations[i : i + organizations_per_file]
        file_number += 1
        file_path = output_dir / f"organization-{file_number}.ndjson"
        with open(file_path, "w", encoding="utf-8") as f:
            for org in batch:
                f.write(json.dumps(org, ensure_ascii=False) + "\n")
    return file_number


def main() -> None:
    """Read format-raw-data output, collect unique organizations by name, write NDJSON batches."""
    print("🚀 Generating unique organizations from dataset NDJSON...")

    downloads_dir = Path.home() / "Downloads"
    dataset_dir = downloads_dir / "database" / "dataset"
    output_dir = downloads_dir / "database" / "organizations"

    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {dataset_dir}. "
            "Run format-raw-data.py first to generate dataset NDJSON files."
        )

    print(f"  Input:  {dataset_dir}")
    print(f"  Output: {output_dir}")

    if output_dir.exists():
        import shutil

        shutil.rmtree(output_dir)
        print("✓ Output directory cleaned")
    else:
        print("✓ Output directory not found")

    output_dir.mkdir(parents=True, exist_ok=True)
    print("✓ Created output directory")

    unique_organizations = collect_unique_organizations_with_datasets(dataset_dir)
    print(f"\n  Found {len(unique_organizations):,} unique organization(s)")

    if not unique_organizations:
        print("  No organizations to write.")
        return

    unique_organizations.sort(key=lambda a: a.get("name", "").lower())
    print("  Sorted by name")

    file_count = write_organization_batches(
        unique_organizations, output_dir, ORGANIZATIONS_PER_FILE
    )
    print(
        f"  Wrote {file_count} file(s) (~{ORGANIZATIONS_PER_FILE:,} organizations per file)"
    )
    print(f"🎉 Organization NDJSON files: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)
