"""Process Fuji/FAIR score NDJSON from fuji-score and emdb_fair_scores.ndjson into unified NDJSON files."""

import contextlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from tqdm import tqdm

from identifier_mapping import (
    IDENTIFIER_TO_ID_MAP_DIR,
    load_identifier_to_id_mapping_from_dir,
)

FUJI_RECORDS_PER_FILE = 10000  # Records per output file


def _natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def _normalize_evaluation_date(value: Any) -> Optional[str]:
    """Normalize evaluationDate to ISO string; accept Z suffix as +00:00."""
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = f"{s[:-1]}+00:00"
    return s


def extract_fuji_from_record(
    record: Dict[str, Any], identifier_to_id: Dict[str, int]
) -> Optional[Dict[str, Any]]:
    """Extract a single Fuji/FAIR score record from input.

    Supports two input formats:

    Fuji (id + doi):
        {"id": 49000002, "doi": "10.25346/s6eecgix/tvr7kf", "score": 13.46,
         "evaluationDate": "2026-01-24T17:12:18.527000", "metricVersion": "estimated",
         "softwareVersion": "estimated"}

    EMDB (dataset_id):
        {"dataset_id":"EMD-60162","score":42.31,
         "evaluationDate":"2026-01-25T10:55:47+00:00","metricVersion":"0.8",
         "softwareVersion":"3.5.1"}
    """
    # Resolve identifier: Fuji uses "doi", EMDB uses "dataset_id"
    identifier = ((record.get("doi") or record.get("dataset_id")) or "").strip()
    if not identifier:
        return None

    identifier_lower = identifier.lower()
    dataset_id_int = identifier_to_id.get(identifier_lower)
    if dataset_id_int is None:
        return None

    score_raw = record.get("score")
    if score_raw is not None:
        try:
            score = float(score_raw)
        except (TypeError, ValueError):
            score = None
    else:
        score = None

    evaluation_date = _normalize_evaluation_date(
        record.get("evaluationDate") or record.get("evaluation_date")
    )
    if evaluation_date is None:
        evaluation_date = datetime.now(timezone.utc).isoformat()

    metric_version = (
        record.get("metricVersion") or record.get("metric_version")
    ) or None
    if metric_version is not None and isinstance(metric_version, str):
        metric_version = metric_version.strip() or None
    if metric_version is None:
        metric_version = "estimated"

    software_version = (
        record.get("softwareVersion") or record.get("software_version")
    ) or None
    if software_version is not None and isinstance(software_version, str):
        software_version = software_version.strip() or None
    if software_version is None:
        software_version = "estimated"

    return {
        "datasetId": dataset_id_int,
        "identifier": identifier_lower,
        "score": score,
        "evaluationDate": evaluation_date,
        "metricVersion": metric_version,
        "softwareVersion": software_version,
    }


def _collect_ndjson_paths(path: Path) -> List[Path]:
    """Return a single-file list if path is a file, else sorted *.ndjson in directory."""
    if path.is_file():
        return [path]
    if path.is_dir():
        files = sorted(path.glob("*.ndjson"), key=_natural_sort_key)
        return list(files)
    return []


def count_fuji_records(paths: List[Path]) -> int:
    """Count total valid Fuji/FAIR score records across the given files (for progress bar)."""
    total = 0
    print("  Counting records in input file(s)...")
    pbar = tqdm(total=len(paths), desc="  Counting", unit="file")
    for file_path in paths:
        if not file_path.exists():
            pbar.update(1)
            continue
        with contextlib.suppress(Exception):
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    with contextlib.suppress(json.JSONDecodeError, KeyError, TypeError):
                        record = json.loads(line)
                        if record.get("doi") or record.get("dataset_id"):
                            total += 1
        pbar.update(1)
    pbar.close()
    return total


def write_fuji_batch(
    batch: List[Dict[str, Any]], file_number: int, output_dir: Path
) -> None:
    """Write a batch of Fuji records to a numbered NDJSON file."""
    file_name = f"{file_number}.ndjson"
    file_path = output_dir / file_name
    with open(file_path, "w", encoding="utf-8") as f:
        for rec in batch:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def process_fuji_files(
    paths: List[Path],
    output_dir: Path,
    identifier_to_id: Dict[str, int],
    total_records: int,
) -> None:
    """Process one or more NDJSON files and write batched Fuji NDJSON files."""
    file_number = 1
    current_batch: List[Dict[str, Any]] = []
    total_processed = 0
    total_skipped = 0

    pbar = tqdm(
        total=total_records, desc="  Processing", unit="record", unit_scale=True
    )

    for file_path in paths:
        if not file_path.exists():
            continue
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        if fuji := extract_fuji_from_record(record, identifier_to_id):
                            current_batch.append(fuji)
                            total_processed += 1
                            pbar.update(1)
                            if len(current_batch) >= FUJI_RECORDS_PER_FILE:
                                write_fuji_batch(current_batch, file_number, output_dir)
                                file_number += 1
                                current_batch = []
                        else:
                            total_skipped += 1
                    except (json.JSONDecodeError, KeyError, TypeError) as error:
                        total_skipped += 1
                        tqdm.write(
                            f"    ⚠️  Failed to parse line in {file_path.name}: {error}"
                        )
        except FileNotFoundError:
            tqdm.write(f"    ⚠️  File not found: {file_path}")
        except Exception as error:
            tqdm.write(f"    ⚠️  Error reading {file_path}: {error}")

    pbar.close()

    if current_batch:
        write_fuji_batch(current_batch, file_number, output_dir)

    print(f"\n  📊 Total records processed: {total_processed:,}")
    if total_skipped > 0:
        print(f"  ⚠️  Total records skipped: {total_skipped:,}")
    print(f"  📁 Total output files created: {file_number}")


def main() -> None:
    """Process Fuji/FAIR scores from fuji-score and emdb_fair_scores.ndjson."""
    print("🚀 Starting Fuji/FAIR score processing...")

    output_folder_name = "fuji"
    fuji_input_name = "fuji-score"
    emdb_input_name = "emdb_fair_scores.ndjson"

    print("📍 Step 1: Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    fuji_input_path = downloads_dir / fuji_input_name
    emdb_input_path = downloads_dir / emdb_input_name
    output_dir = downloads_dir / "database" / output_folder_name
    mapping_dir = downloads_dir / "database" / IDENTIFIER_TO_ID_MAP_DIR

    print(f"Fuji input (file or dir): {fuji_input_path}")
    print(f"EMDB input file: {emdb_input_path}")
    print(f"Output directory: {output_dir}")
    print(f"Mapping directory: {mapping_dir}")

    fuji_paths = _collect_ndjson_paths(fuji_input_path)
    all_paths: List[Path] = []
    if fuji_paths:
        all_paths.extend(fuji_paths)
        print(f"✓ Fuji input: {len(fuji_paths)} file(s)")
    elif fuji_input_path.exists():
        print("⚠️  Fuji path exists but has no .ndjson files")
    else:
        print("⚠️  Fuji input path not found (will skip if EMDB provided)")

    if emdb_input_path.exists():
        all_paths.append(emdb_input_path)
        print("✓ EMDB input file found")
    else:
        print("⚠️  EMDB input file not found")

    if not all_paths:
        raise FileNotFoundError(
            "No input files found. Ensure at least one of the following exists:\n"
            f"  - {fuji_input_path} (file or directory with *.ndjson)\n"
            f"  - {emdb_input_path}"
        )

    if output_dir.exists():
        import shutil

        shutil.rmtree(output_dir)
        print("✓ Output directory cleaned")
    output_dir.mkdir(parents=True, exist_ok=True)
    print("✓ Created output directory")

    mapping_dir.mkdir(parents=True, exist_ok=True)

    print("\n🗺️  Step 2: Loading identifier to ID mapping...")
    start_time = time.perf_counter()
    try:
        identifier_to_id = load_identifier_to_id_mapping_from_dir(mapping_dir)
        elapsed_time = time.perf_counter() - start_time
        print(f"  ✓ Loaded {len(identifier_to_id):,} identifier mappings")
        print(f"  ⏱️  Time taken: {elapsed_time:.2f} seconds")
    except FileNotFoundError:
        raise
    except Exception as e:
        raise RuntimeError(
            f"Error reading mapping from {mapping_dir}: {e}. "
            "Please run build-identifier-datasetid-map.py to rebuild the mapping."
        )

    print("\n📊 Step 3: Counting records in input file(s)...")
    total_records = count_fuji_records(all_paths)
    print(f"  Found {total_records:,} records to process")

    print(
        f"\n✂️  Step 4: Processing Fuji/FAIR scores and creating files "
        f"(~{FUJI_RECORDS_PER_FILE:,} records each)..."
    )
    process_fuji_files(all_paths, output_dir, identifier_to_id, total_records)

    print("\n✅ All Fuji/FAIR scores have been processed successfully!")
    print(f"🎉 Processed files are available in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during Fuji processing:")
        print(e)
        exit(1)
