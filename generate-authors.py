"""Generate a distinct list of unique authors from format-raw-data output (dataset NDJSON)."""

import json
import re
from pathlib import Path
from typing import Any, Dict, List

from tqdm import tqdm


AUTHORS_PER_FILE = 10_000
LINKS_PER_FILE = 100_000  # (automatedUserId, datasetId) rows per ndjson file


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def _normalize_single_identifier(raw_id_string: str) -> str:
    """Normalize ORCID URLs/prefixes to bare identifier (lower, trim); otherwise return as-is.
    Matches proposal analysis: strip https://orcid.org/ and orcid: prefix, then LOWER(TRIM(...)).
    """
    if not raw_id_string or not isinstance(raw_id_string, str):
        return ""
    s = raw_id_string.strip()
    if "orcid.org/" in s:
        parts = s.split("orcid.org/", 1)
        s = parts[1] if len(parts) > 1 else s
    elif s.lower().startswith("orcid:"):
        parts = s.split("orcid:", 1)
        s = parts[1] if len(parts) > 1 else s
    return s.lower().strip()


def _normalize_identifiers(identifiers: List[str]) -> tuple:
    """Normalize nameIdentifiers for comparison: ORCID-normalize, strip, drop empty, sort."""
    if not identifiers:
        return ()
    cleaned = [
        _normalize_single_identifier(s) for s in identifiers if s and isinstance(s, str)
    ]
    cleaned = [s for s in cleaned if s]
    return tuple(sorted(cleaned))


def _is_orcid(normalized_id: str) -> bool:
    """Return True if normalized_id looks like an ORCID (4-4-4-4 hex, last group can end in x)."""
    if not normalized_id or len(normalized_id) != 19:
        return False
    # 0000-0000-0000-0000 or 0000-0000-0000-000x
    parts = normalized_id.split("-")
    if len(parts) != 4:
        return False
    for i, part in enumerate(parts):
        if len(part) != 4:
            return False
        allowed = "0123456789abcdef" if i < 3 else "0123456789abcdefx"
        if any(c not in allowed for c in part.lower()):
            return False
    return True


def _canonical_identifier(normalized_identifiers: tuple) -> str:
    """Pick one canonical ID for deduplication: ORCID if present, else first in list."""
    if not normalized_identifiers:
        return ""
    for nid in normalized_identifiers:
        if _is_orcid(nid):
            return nid
    return normalized_identifiers[0]


def _strip_affiliation_parens(s: str) -> str:
    """Remove matching () at start and end of affiliation string."""
    s = s.strip()
    while s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()
    return s


def _normalize_affiliations(affiliations: List[str]) -> tuple:
    """Normalize affiliation list for comparison: strip, drop outer parens, drop empty, sort."""
    if not affiliations:
        return ()
    cleaned = [
        _strip_affiliation_parens(s) for s in affiliations if s and isinstance(s, str)
    ]
    return tuple(sorted(cleaned))


def author_canonical_key(author: Dict[str, Any]) -> tuple:
    """Canonical key for deduplication.
    1. If author has identifiers: group by a single canonical ID (ORCID if present, else first).
    2. Else (no identifiers): group by name, then split by affiliation (same name + same affiliation = same person).
    """
    if identifiers := _normalize_identifiers(author.get("nameIdentifiers", []) or []):
        return ("by_identifier", _canonical_identifier(identifiers))
    name_type = author.get("nameType", "")
    name = (author.get("name") or "").lower()
    affiliations = _normalize_affiliations(author.get("affiliations", []) or [])
    # Case-insensitive affiliation match for deduplication
    affiliations_key = tuple(s.lower() for s in affiliations)
    return ("by_name_affiliation", name_type, name, affiliations_key)


def collect_unique_authors_with_datasets(
    dataset_dir: Path,
) -> List[Dict[str, Any]]:
    """Read all dataset NDJSON files; return unique authors with their dataset IDs."""
    ndjson_files = sorted(dataset_dir.glob("*.ndjson"), key=natural_sort_key)
    if not ndjson_files:
        return []

    # canonical_key -> (author dict, set of dataset ids)
    author_map: Dict[tuple, tuple] = {}

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
                    if not (author.get("name") or "").strip():
                        continue
                    if (
                        author.get("nameType") or ""
                    ).strip().lower() == "organizational":
                        continue
                    key = author_canonical_key(author)
                    if key not in author_map:
                        author_map[key] = (dict(author), {dataset_id})
                    else:
                        author_map[key][1].add(dataset_id)

    # Build list of authors with datasetIds (sorted for stable output); drop any with empty name
    result: List[Dict[str, Any]] = []
    for author, dataset_ids in tqdm(
        author_map.values(), desc="Building author list", unit="author"
    ):
        if not (author.get("name") or "").strip():
            continue
        if (author.get("nameType") or "").strip().lower() == "organizational":
            continue
        out = dict(author)
        out["id"] = len(result) + 1  # int id per schema AutomatedUser.id
        out["datasetIds"] = sorted(dataset_ids)
        # Store normalized affiliations (strip outer parens, sort) for consistency
        out["affiliations"] = list(
            _normalize_affiliations(author.get("affiliations", []) or [])
        )
        # Store normalized identifiers (bare ORCIDs, etc.) for consistency
        ids = author.get("nameIdentifiers") or []
        if ids:
            normalized = [
                _normalize_single_identifier(s) for s in ids if s and isinstance(s, str)
            ]
            out["nameIdentifiers"] = sorted(s for s in normalized if s)
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
    batch_range = range(0, len(authors), authors_per_file)
    for i in tqdm(batch_range, desc="Writing author batches", unit="batch"):
        batch = authors[i : i + authors_per_file]
        file_number += 1
        file_path = output_dir / f"author-{file_number}.ndjson"
        with open(file_path, "w", encoding="utf-8") as f:
            for author in tqdm(
                batch, desc=f"Batch {file_number}", unit="author", leave=False
            ):
                out = dict(author)
                f.write(json.dumps(out, ensure_ascii=False) + "\n")
    return file_number


def write_automated_user_dataset_batches(
    authors: List[Dict[str, Any]],
    output_dir: Path,
    links_per_file: int = LINKS_PER_FILE,
) -> int:
    """Write (automatedUserId, datasetId) link rows to NDJSON files. Returns file count."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_number = 0
    links_in_current = 0
    current_file = None

    def flush_file():
        nonlocal current_file
        if current_file is not None:
            current_file.close()
            current_file = None

    for author in tqdm(
        authors, desc="Writing AutomatedUserDataset batches", unit="author"
    ):
        author_id = author.get("id")
        if author_id is None:
            continue
        dataset_ids = author.get("datasetIds") or []
        if not dataset_ids:
            continue
        for dataset_id in dataset_ids:
            if links_in_current >= links_per_file or current_file is None:
                flush_file()
                file_number += 1
                file_path = output_dir / f"automateduserdataset-{file_number}.ndjson"
                current_file = open(file_path, "w", encoding="utf-8")
                links_in_current = 0
            row = {"automatedUserId": author_id, "datasetId": dataset_id}
            current_file.write(json.dumps(row, ensure_ascii=False) + "\n")
            links_in_current += 1

    flush_file()
    return file_number


def main() -> None:
    """Read format-raw-data output, collect unique authors, write NDJSON batches."""
    print("🚀 Generating unique authors from dataset NDJSON...")

    downloads_dir = Path.home() / "Downloads"
    database_dir = downloads_dir / "database"
    dataset_dir = database_dir / "dataset"
    authors_dir = database_dir / "authors"
    automateduserdataset_dir = database_dir / "automateduserdataset"

    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory not found: {dataset_dir}. "
            "Run format-raw-data.py first to generate dataset NDJSON files."
        )

    print(f"  Input:  {dataset_dir}")
    print(f"  Output (authors): {authors_dir}")
    print(f"  Output (AutomatedUserDataset): {automateduserdataset_dir}")

    import shutil

    for output_dir in (authors_dir, automateduserdataset_dir):
        if output_dir.exists():
            shutil.rmtree(output_dir)
            print(f"✓ Cleaned {output_dir.name}")
    print("✓ Output directories ready")

    unique_authors = collect_unique_authors_with_datasets(dataset_dir)
    print(f"\n  Found {len(unique_authors):,} unique author(s)")

    if not unique_authors:
        print("  No authors to write.")
        return

    author_file_count = write_author_batches(
        unique_authors, authors_dir, AUTHORS_PER_FILE
    )
    print(
        f"  Wrote {author_file_count} author file(s) (~{AUTHORS_PER_FILE:,} authors per file)"
    )

    link_file_count = write_automated_user_dataset_batches(
        unique_authors, automateduserdataset_dir, LINKS_PER_FILE
    )
    print(
        f"  Wrote {link_file_count} AutomatedUserDataset file(s) (~{LINKS_PER_FILE:,} links per file)"
    )

    print(f"🎉 Author NDJSON: {authors_dir}")
    print(f"🎉 AutomatedUserDataset NDJSON: {automateduserdataset_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)
