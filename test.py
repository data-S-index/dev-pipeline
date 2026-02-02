"""Scan all ndjson files in slim-metadata and output a distinct list of field names."""

import json
import re
from pathlib import Path

from tqdm import tqdm


def natural_sort_key(path: Path):
    """Sort paths with numeric parts in natural order (1, 2, ..., 10)."""
    parts = re.split(r"(\d+)", path.name)
    return tuple(int(p) if p.isdigit() else p.lower() for p in parts)


def collect_top_level_keys(obj, prefix: str = "") -> set:
    """Recursively collect all keys; use dotted notation for nested dicts."""
    keys = set()
    if not isinstance(obj, dict):
        return keys
    for key, value in obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        keys.add(full_key)
        if isinstance(value, dict):
            keys.update(collect_top_level_keys(value, full_key))
    return keys


def main():
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    base = downloads_dir / "slim-records" / "datacite-slim-records"
    if not base.is_dir():
        print(f"Directory not found: {base.resolve()}")
        return

    ndjson_files = sorted(base.glob("*.ndjson"), key=natural_sort_key)
    if not ndjson_files:
        print(f"No *.ndjson files in {base.resolve()}")
        return

    all_fields = set()
    for file_path in tqdm(ndjson_files, desc="Scanning ndjson files"):
        with open(file_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    all_fields.update(collect_top_level_keys(record))
                except json.JSONDecodeError:
                    continue

    distinct = sorted(all_fields)
    print("Distinct fields across all ndjson files:")
    for name in distinct:
        print(name)


if __name__ == "__main__":
    main()
