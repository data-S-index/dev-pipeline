"""Write all author identifiers from dataset NDJSON to a file for inspecting types."""

import json
import re
from pathlib import Path

from tqdm import tqdm


def _natural_sort_key(path: Path) -> tuple:
    """Natural sort key for filenames (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def _normalize_single_identifier(raw_id_string: str) -> str:
    """Normalize ORCID URLs/prefixes to bare identifier; otherwise return as-is (lower, trim)."""
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


def _infer_identifier_type(raw: str) -> str:
    """Infer a simple type label from raw identifier string."""
    if not raw or not isinstance(raw, str):
        return "empty"
    s = raw.strip().lower()
    if "orcid.org" in s or s.startswith("orcid:"):
        return "orcid"
    if "isni.org" in s or s.startswith("isni:"):
        return "isni"
    if "scopus" in s or "elsevier" in s:
        return "scopus"
    if "researcherid" in s or "wos" in s:
        return "researcherid"
    if "viaf" in s:
        return "viaf"
    if "doi.org" in s or s.startswith("doi:"):
        return "doi"
    return "other"


def write_all_identifiers_file(
    dataset_dir: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    """Collect all author nameIdentifiers from dataset NDJSON and write them to a file.

    Creates a file in the current directory (or output_path) with one identifier per line,
    prefixed by an inferred type (orcid, isni, other, ...) so you can sort and see what
    types exist. Returns the path of the written file.
    """
    if dataset_dir is None:
        dataset_dir = Path.home() / "Downloads" / "database" / "dataset"
    if output_path is None:
        output_path = Path.cwd() / "identifiers.txt"

    ndjson_files = sorted(dataset_dir.glob("*.ndjson"), key=_natural_sort_key)
    if not ndjson_files:
        raise FileNotFoundError(f"No *.ndjson files in {dataset_dir}")

    seen: set[str] = set()
    type_counts: dict[str, int] = {}

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
                for author in record.get("authors") or []:
                    if not isinstance(author, dict):
                        continue
                    for raw in author.get("nameIdentifiers") or []:
                        if not raw or not isinstance(raw, str):
                            continue
                        raw = raw.strip()
                        if not raw or raw in seen:
                            continue
                        seen.add(raw)
                        t = _infer_identifier_type(raw)
                        type_counts[t] = type_counts.get(t, 0) + 1

    lines = []
    for raw in sorted(seen):
        t = _infer_identifier_type(raw)
        norm = _normalize_single_identifier(raw)
        lines.append(f"{t}\t{raw}\t{norm}")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# type\traw_identifier\tnormalized\n")
        f.write("\n".join(lines))
        f.write("\n")

    summary_path = Path.cwd() / "identifiers_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("Identifier type counts:\n")
        for t in sorted(type_counts.keys()):
            f.write(f"  {t}: {type_counts[t]}\n")
        f.write(f"\nTotal unique identifiers: {len(seen)}\n")

    print(f"Wrote {len(seen)} identifiers to {output_path}")
    print(f"Wrote type summary to {summary_path}")
    return output_path


if __name__ == "__main__":
    write_all_identifiers_file()
