"""Generate a distinct list of unique organizations from format-raw-data output (dataset NDJSON)."""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

from tqdm import tqdm


ORGANIZATIONS_PER_FILE = 10_000
LINKS_PER_FILE = 100_000  # (automatedOrganizationId, datasetId) rows per ndjson file


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def _strip_affiliation_parens(s: str) -> str:
    """Remove matching () at start and end of affiliation string."""
    s = s.strip()
    while s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()
    return s


def _normalize_org_name(affiliation: str) -> str:
    """Normalize organization name for deduplication: strip, remove outer parens, strip inner (..), lower."""
    s = _strip_affiliation_parens(affiliation.strip())
    s = re.sub(r"\(.*?\)", "", s).strip()
    return s.lower()


def organization_canonical_key(display_name: str) -> tuple:
    """Canonical key for deduplication: group by normalized name."""
    return ("by_name", _normalize_org_name(display_name))


def collect_unique_organizations_with_datasets(
    dataset_dir: Path,
) -> Tuple[List[Dict[str, Any]], List[Tuple[int, int]]]:
    """Read all dataset NDJSON files; return unique organizations and (automatedOrganizationId, datasetId) links."""
    ndjson_files = sorted(dataset_dir.glob("*.ndjson"), key=natural_sort_key)
    if not ndjson_files:
        return [], []

    # canonical_key -> (display name from first occurrence, set of dataset ids)
    organization_map: Dict[tuple, tuple] = {}

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
                    affiliations = author.get("affiliations") or []
                    if not isinstance(affiliations, list):
                        continue
                    for aff in affiliations:
                        if not isinstance(aff, str):
                            continue
                        name = _strip_affiliation_parens(aff.strip())
                        if not name:
                            continue
                        key = organization_canonical_key(name)
                        if key not in organization_map:
                            organization_map[key] = (name, {dataset_id})
                        else:
                            organization_map[key][1].add(dataset_id)

    # Build organization list (no datasetIds) and links list
    result: List[Dict[str, Any]] = []
    links: List[Tuple[int, int]] = []
    for display_name, dataset_ids in tqdm(
        organization_map.values(),
        desc="Building organization list",
        unit="organization",
    ):
        org_id = len(result) + 1  # int id per schema AutomatedOrganization.id
        out = {"id": org_id, "name": display_name}
        result.append(out)
        for did in sorted(dataset_ids):
            links.append((org_id, did))
    return result, links


def write_organization_batches(
    organizations: List[Dict[str, Any]],
    output_dir: Path,
    organizations_per_file: int = ORGANIZATIONS_PER_FILE,
) -> int:
    """Write organizations to NDJSON files with at most organizations_per_file per file. Returns file count."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_number = 0
    batch_range = range(0, len(organizations), organizations_per_file)
    for i in tqdm(batch_range, desc="Writing organization batches", unit="batch"):
        batch = organizations[i : i + organizations_per_file]
        file_number += 1
        file_path = output_dir / f"organization-{file_number}.ndjson"
        with open(file_path, "w", encoding="utf-8") as f:
            for org in tqdm(
                batch, desc=f"Batch {file_number}", unit="organization", leave=False
            ):
                out = dict(org)
                f.write(json.dumps(out, ensure_ascii=False) + "\n")
    return file_number


def write_automated_organization_dataset_batches(
    links: List[Tuple[int, int]],
    output_dir: Path,
    links_per_file: int = LINKS_PER_FILE,
) -> int:
    """Write (automatedOrganizationId, datasetId) link rows to NDJSON files. Returns file count."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_number = 0
    links_in_current = 0
    current_file = None

    def flush_file():
        nonlocal current_file
        if current_file is not None:
            current_file.close()
            current_file = None

    for org_id, dataset_id in tqdm(
        links,
        desc="Writing AutomatedOrganizationDataset batches",
        unit="link",
    ):
        if links_in_current >= links_per_file or current_file is None:
            flush_file()
            file_number += 1
            file_path = (
                output_dir / f"automatedorganizationdataset-{file_number}.ndjson"
            )
            current_file = open(file_path, "w", encoding="utf-8")
            links_in_current = 0
        row = {"automatedOrganizationId": org_id, "datasetId": dataset_id}
        current_file.write(json.dumps(row, ensure_ascii=False) + "\n")
        links_in_current += 1

    flush_file()
    return file_number


def main() -> None:
    """Read format-raw-data output, collect unique organizations, write NDJSON batches."""
    print("🚀 Generating unique organizations from dataset NDJSON...")

    downloads_dir = Path.home() / "Downloads"
    database_dir = downloads_dir / "database"
    dataset_dir = database_dir / "dataset"
    organizations_dir = database_dir / "organizations"
    automatedorganizationdataset_dir = database_dir / "automatedorganizationdataset"

    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {dataset_dir}. "
            "Run format-raw-data.py first to generate dataset NDJSON files."
        )

    print(f"  Input:  {dataset_dir}")
    print(f"  Output (organizations): {organizations_dir}")
    print(
        f"  Output (AutomatedOrganizationDataset): {automatedorganizationdataset_dir}"
    )

    import shutil

    for output_dir in (organizations_dir, automatedorganizationdataset_dir):
        if output_dir.exists():
            shutil.rmtree(output_dir)
            print(f"✓ Cleaned {output_dir.name}")
    print("✓ Output directories ready")

    unique_organizations, org_dataset_links = (
        collect_unique_organizations_with_datasets(dataset_dir)
    )
    print(f"\n  Found {len(unique_organizations):,} unique organization(s)")

    if not unique_organizations:
        print("  No organizations to write.")
        return

    org_file_count = write_organization_batches(
        unique_organizations, organizations_dir, ORGANIZATIONS_PER_FILE
    )
    print(
        f"  Wrote {org_file_count} organization file(s) (~{ORGANIZATIONS_PER_FILE:,} organizations per file)"
    )

    link_file_count = write_automated_organization_dataset_batches(
        org_dataset_links, automatedorganizationdataset_dir, LINKS_PER_FILE
    )
    print(
        f"  Wrote {link_file_count} AutomatedOrganizationDataset file(s) (~{LINKS_PER_FILE:,} links per file)"
    )

    print(f"🎉 Organization NDJSON: {organizations_dir}")
    print(f"🎉 AutomatedOrganizationDataset NDJSON: {automatedorganizationdataset_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)
