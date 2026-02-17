"""Generate a distinct list of unique authors from format-raw-data output (dataset NDJSON)."""

import json
import os
import re
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Tuple

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


def _process_one_dataset_file(
    file_path: str,
) -> Dict[tuple, Tuple[Dict[str, Any], set]]:
    """Process a single NDJSON file; return local author_map (canonical_key -> (author, set(dataset_ids))).
    Module-level for pickling in ProcessPoolExecutor.
    """
    path = Path(file_path)
    author_map: Dict[tuple, Tuple[Dict[str, Any], set]] = {}
    with open(path, "r", encoding="utf-8") as f:
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
                if (author.get("nameType") or "").strip().lower() == "organizational":
                    continue
                key = author_canonical_key(author)
                if key not in author_map:
                    author_map[key] = (dict(author), {dataset_id})
                else:
                    author_map[key][1].add(dataset_id)
    return author_map


def collect_author_map(
    dataset_dir: Path,
    *,
    max_workers: int | None = None,
) -> Dict[tuple, Tuple[Dict[str, Any], set]]:
    """Read all dataset NDJSON files; return author_map (canonical_key -> (author, set(dataset_ids)))."""
    ndjson_files = sorted(dataset_dir.glob("*.ndjson"), key=natural_sort_key)
    if not ndjson_files:
        return {}

    workers = max_workers or min(os.cpu_count() or 4, len(ndjson_files))
    author_map: Dict[tuple, Tuple[Dict[str, Any], set]] = {}
    file_paths_str = [str(p) for p in ndjson_files]

    with ProcessPoolExecutor(max_workers=workers) as executor:
        for per_file_map in tqdm(
            executor.map(_process_one_dataset_file, file_paths_str),
            total=len(ndjson_files),
            desc="Scanning dataset files",
            unit="file",
            smoothing=0,
        ):
            for key, (author, dataset_ids) in per_file_map.items():
                if key not in author_map:
                    author_map[key] = (dict(author), set(dataset_ids))
                else:
                    author_map[key][1].update(dataset_ids)

    return author_map


def write_authors_and_links_streaming(
    author_map: Dict[tuple, Tuple[Dict[str, Any], set]],
    authors_dir: Path,
    automateduserdataset_dir: Path,
    *,
    authors_per_file: int = AUTHORS_PER_FILE,
    links_per_file: int = LINKS_PER_FILE,
) -> Tuple[int, int, int]:
    """
    Stream authors + (automatedUserId, datasetId) link rows directly to NDJSON batches.
    Returns: (author_count, author_file_count, link_file_count)
    """
    authors_dir.mkdir(parents=True, exist_ok=True)
    automateduserdataset_dir.mkdir(parents=True, exist_ok=True)

    author_count = 0
    author_file_count = 0
    link_file_count = 0

    authors_in_current_file = 0
    links_in_current_file = 0

    author_f = None
    link_f = None

    def open_next_author_file() -> None:
        nonlocal author_f, author_file_count, authors_in_current_file
        if author_f:
            author_f.close()
        author_file_count += 1
        authors_in_current_file = 0
        author_f = open(
            authors_dir / f"author-{author_file_count}.ndjson", "w", encoding="utf-8"
        )

    def open_next_link_file() -> None:
        nonlocal link_f, link_file_count, links_in_current_file
        if link_f:
            link_f.close()
        link_file_count += 1
        links_in_current_file = 0
        link_f = open(
            automateduserdataset_dir / f"automateduserdataset-{link_file_count}.ndjson",
            "w",
            encoding="utf-8",
        )

    open_next_author_file()
    open_next_link_file()

    # tqdm over number of unique authors (cheap), not number of links (can be enormous)
    for author, dataset_ids in tqdm(
        author_map.values(),
        total=len(author_map),
        desc="Writing authors + links",
        unit="author",
    ):
        # Keep your filters
        if not (author.get("name") or "").strip():
            continue
        if (author.get("nameType") or "").strip().lower() == "organizational":
            continue

        author_count += 1
        author_id = author_count  # stable incremental ID

        # Rotate author file if needed
        if authors_in_current_file >= authors_per_file:
            open_next_author_file()

        out = dict(author)
        out["id"] = author_id
        out["affiliations"] = list(
            _normalize_affiliations(author.get("affiliations", []) or [])
        )

        ids = author.get("nameIdentifiers") or []
        if ids:
            normalized = [
                _normalize_single_identifier(s) for s in ids if s and isinstance(s, str)
            ]
            out["nameIdentifiers"] = sorted(s for s in normalized if s)

        # Write author line
        author_f.write(json.dumps(out, ensure_ascii=False) + "\n")
        authors_in_current_file += 1

        # Write link lines (streaming; NO giant list)
        # Faster than json.dumps for this tiny object:
        # {"automatedUserId":123,"datasetId":456}\n
        for did in dataset_ids:
            if links_in_current_file >= links_per_file:
                open_next_link_file()
            link_f.write(f'{{"automatedUserId":{author_id},"datasetId":{did}}}\n')
            links_in_current_file += 1

    if author_f:
        author_f.close()
    if link_f:
        link_f.close()

    return author_count, author_file_count, link_file_count


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

    author_map = collect_author_map(dataset_dir)
    print(f"\n  Found {len(author_map):,} unique author key(s)")

    author_count, author_file_count, link_file_count = (
        write_authors_and_links_streaming(
            author_map,
            authors_dir,
            automateduserdataset_dir,
            authors_per_file=AUTHORS_PER_FILE,
            links_per_file=LINKS_PER_FILE,
        )
    )

    print(f"\n  Wrote {author_count:,} author(s)")
    print(
        f"  Wrote {author_file_count} author file(s) (~{AUTHORS_PER_FILE:,} authors per file)"
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
