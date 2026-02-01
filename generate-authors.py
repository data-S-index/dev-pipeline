"""Generate a distinct list of unique authors from format-raw-data output (dataset NDJSON)."""

import json
import re
from pathlib import Path
from typing import Any, Dict, List

from tqdm import tqdm
from ulid import ULID


AUTHORS_PER_FILE = 10_000


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def _normalize_identifiers(identifiers: List[str]) -> tuple:
    """Normalize nameIdentifiers for comparison: strip, drop empty, sort."""
    if not identifiers:
        return ()
    cleaned = [s.strip() for s in identifiers if s and isinstance(s, str)]
    return tuple(sorted(cleaned))


def _normalize_affiliations(affiliations: List[str]) -> tuple:
    """Normalize affiliation list for comparison: strip, drop empty, sort."""
    if not affiliations:
        return ()
    cleaned = [s.strip() for s in affiliations if s and isinstance(s, str)]
    return tuple(sorted(cleaned))


def author_canonical_key(author: Dict[str, Any]) -> str:
    """Canonical key for deduplication. Split by: (1) name, (2) identifiers, (3) affiliation.
    Same name → one group; within that, different identifiers → different authors; remainder split by affiliation.
    Name is lowercased so case variants (e.g. Müller vs müller) dedupe together.
    """
    name_type = author.get("nameType", "")
    name = (author.get("name") or "").lower()
    identifiers = _normalize_identifiers(author.get("nameIdentifiers", []) or [])
    affiliations = _normalize_affiliations(author.get("affiliations", []) or [])
    return json.dumps(
        [name_type, name, list(identifiers), list(affiliations)],
        sort_keys=False,
        ensure_ascii=False,
    )


def collect_unique_authors_with_datasets(
    dataset_dir: Path,
) -> List[Dict[str, Any]]:
    """Read all dataset NDJSON files; return unique authors with their dataset IDs."""
    ndjson_files = sorted(dataset_dir.glob("*.ndjson"), key=natural_sort_key)
    if not ndjson_files:
        return []

    # canonical_key -> (author dict, set of dataset ids)
    author_map: Dict[str, tuple] = {}

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
                    key = author_canonical_key(author)
                    if key not in author_map:
                        author_map[key] = (dict(author), {dataset_id})
                    else:
                        author_map[key][1].add(dataset_id)

    # Build list of authors with datasetIds (sorted for stable output)
    result: List[Dict[str, Any]] = []
    for author, dataset_ids in author_map.values():
        out = dict(author)
        out["id"] = str(ULID())
        out["datasetIds"] = sorted(dataset_ids)
        result.append(out)
    return result


def write_author_batches(
    authors: List[Dict[str, Any]],
    output_dir: Path,
    authors_per_file: int = AUTHORS_PER_FILE,
) -> int:
    """Write authors to NDJSON files with at most authors_per_file per file. Returns file count."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_number = 0
    for i in range(0, len(authors), authors_per_file):
        batch = authors[i : i + authors_per_file]
        file_number += 1
        file_path = output_dir / f"author-{file_number}.ndjson"
        with open(file_path, "w", encoding="utf-8") as f:
            for author in batch:
                out = dict(author)
                f.write(json.dumps(out, ensure_ascii=False) + "\n")
    return file_number


def main() -> None:
    """Read format-raw-data output, collect unique authors, write NDJSON batches."""
    print("🚀 Generating unique authors from dataset NDJSON...")

    downloads_dir = Path.home() / "Downloads"
    dataset_dir = downloads_dir / "database" / "dataset"
    output_dir = downloads_dir / "database" / "authors"

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

    unique_authors = collect_unique_authors_with_datasets(dataset_dir)
    print(f"\n  Found {len(unique_authors):,} unique author(s)")

    if not unique_authors:
        print("  No authors to write.")
        return

    unique_authors.sort(
        key=lambda a: (a.get("name", "").lower(), a.get("nameType", ""))
    )
    print("  Sorted by name")

    file_count = write_author_batches(unique_authors, output_dir, AUTHORS_PER_FILE)
    print(f"  Wrote {file_count} file(s) (~{AUTHORS_PER_FILE:,} authors per file)")
    print(f"🎉 Author NDJSON files: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)
