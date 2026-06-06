"""Shared constant and loader for identifier-to-dataset-ID mapping (multiple NDJSON files)."""

import json
import re
from pathlib import Path
from typing import Dict

from tqdm import tqdm

IDENTIFIER_TO_ID_MAP_DIR = "identifier_to_id_map"  # Directory of NDJSON files (one per dataset file)


def _natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def load_identifier_to_id_mapping_from_dir(
    mapping_dir: Path, show_progress: bool = True
) -> Dict[str, int]:
    """Load identifier to ID mapping from directory of NDJSON files (one per dataset file).

    Files are processed in natural sort order. First occurrence of each identifier wins.
    Progress can be measured by file count when show_progress is True.
    """
    mapping: Dict[str, int] = {}
    if not mapping_dir.exists():
        raise FileNotFoundError(
            f"Mapping directory not found: {mapping_dir}. "
            f"Please run build-identifier-datasetid-map.py first."
        )
    map_files = sorted(mapping_dir.glob("*.ndjson"), key=_natural_sort_key)
    if not map_files:
        raise FileNotFoundError(
            f"No NDJSON files in {mapping_dir}. "
            f"Please run build-identifier-datasetid-map.py first."
        )
    iterator = map_files
    if show_progress:
        iterator = tqdm(
            map_files, desc="  Loading identifier mapping", unit="file", leave=False
        )
    for file_path in iterator:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                identifier = record.get("identifier", "").lower()
                dataset_id = record.get("id")
                if identifier and dataset_id and identifier not in mapping:
                    mapping[identifier] = dataset_id
    return mapping
