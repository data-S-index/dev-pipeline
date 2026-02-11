"""Process mentions from NDJSON and create NDJSON files with mention records."""

import contextlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from tqdm import tqdm

from identifier_mapping import (
    IDENTIFIER_TO_ID_MAP_DIR,
    load_identifier_to_id_mapping_from_dir,
)

MENTIONS_PER_FILE = 10000  # Mentions per output file


def clean_string(s: Optional[str]) -> str:
    """Clean string to be URL-safe only.

    Keeps only characters that are safe for URLs:
    - Alphanumeric (A-Z, a-z, 0-9)
    - Common URL-safe characters: . / : - _ ~
    """
    if not s:
        return ""

    cleaned = ""
    for char in s:
        if char.isalnum():
            cleaned += char
        elif char in (".", "/", ":", "-", "_", "~"):
            cleaned += char
    return cleaned


def extract_mention_from_record(
    record: Dict[str, Any], identifier_to_id: Dict[str, int]
) -> Optional[Dict[str, Any]]:
    """Extract a single mention from a mention record.

    Record format:
    {
        "dataset_id": "10.57967/hf/0737",
        "source": ["hf"],
        "mention_link": "https://huggingface.co/CNXT/CHaTx",
        "mention_weight": 1.0,
        "mention_date": "2023-04-04T16:14:32+00:00",
        "placeholder_date": false,
        "placeholder_year": false
    }
    """
    dataset_identifier = record.get("dataset_id")
    if not dataset_identifier:
        return None

    dataset_identifier_lower = dataset_identifier.lower()
    dataset_id = identifier_to_id.get(dataset_identifier_lower)

    if not dataset_id:
        return None

    mention_link = record.get("mention_link") or record.get("mentionLink")
    if not mention_link:
        return None

    mention_link_cleaned = clean_string(mention_link)
    if not mention_link_cleaned:
        return None

    # Parse mention date
    mention_date_raw = record.get("mention_date") or record.get("mentionDate")
    if mention_date_raw:
        try:
            if isinstance(mention_date_raw, str):
                if mention_date_raw.endswith("Z"):
                    mention_date_raw = f"{mention_date_raw[:-1]}+00:00"
                datetime.fromisoformat(mention_date_raw)
            else:
                mention_date_raw = None
        except (ValueError, AttributeError, TypeError):
            mention_date_raw = None
    else:
        mention_date_raw = None

    mention_weight = 1.0
    weight_value = record.get("mention_weight") or record.get("mentionWeight")
    if weight_value is not None:
        with contextlib.suppress(ValueError, TypeError):
            mention_weight = float(weight_value)

    source = record.get("source", [])
    if isinstance(source, list):
        source_list = [s.lower() for s in source if isinstance(s, str)]
    else:
        source_list = []

    if mention_date_raw:
        mention_date_str = (
            mention_date_raw.replace("+00:00", "")
            if isinstance(mention_date_raw, str)
            else None
        )
    else:
        mention_date_str = datetime.now().isoformat()

    out: Dict[str, Any] = {
        "datasetId": dataset_id,
        "identifier": dataset_identifier_lower,
        "mentionLink": mention_link_cleaned,
        "source": source_list,
        "mentionWeight": mention_weight,
        "mentionedDate": mention_date_str,
    }
    return out


def count_total_mentions(ndjson_file: Path) -> int:
    """Count total number of mentions in NDJSON file (for progress bar)."""
    if not ndjson_file.exists():
        return 0

    print("  Counting lines in input file...")
    total = 0
    try:
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    total += 1
    except Exception:
        return 0
    return total


def write_mention_batch(
    batch: List[Dict[str, Any]], file_number: int, output_dir: Path
) -> None:
    """Write a batch of mentions to a numbered NDJSON file."""
    file_path = output_dir / f"{file_number}.ndjson"
    with open(file_path, "w", encoding="utf-8") as f:
        for mention in batch:
            f.write(json.dumps(mention, ensure_ascii=False) + "\n")


def process_mentions(
    ndjson_file: Path,
    output_dir: Path,
    identifier_to_id: Dict[str, int],
    total_mentions: int,
) -> None:
    """Process NDJSON file and create mention NDJSON files."""
    mentions_by_key: Dict[tuple[int, str], Dict[str, Any]] = {}
    ordered_keys: List[tuple[int, str]] = []
    total_processed = 0
    total_skipped = 0

    pbar = tqdm(
        total=total_mentions, desc="  Processing", unit="mention", unit_scale=True
    )

    try:
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    if mention := extract_mention_from_record(record, identifier_to_id):
                        key = (mention["datasetId"], mention["mentionLink"])
                        if key in mentions_by_key:
                            orig = mentions_by_key[key]
                            # Merge source: union of both lists (unique, order from orig then new)
                            seen = set(orig["source"])
                            for s in mention["source"]:
                                if s not in seen:
                                    orig["source"].append(s)
                                    seen.add(s)
                            total_skipped += 1
                            continue
                        mentions_by_key[key] = mention
                        ordered_keys.append(key)
                        total_processed += 1
                        pbar.update(1)
                    else:
                        total_skipped += 1

                except (json.JSONDecodeError, KeyError, TypeError) as error:
                    total_skipped += 1
                    tqdm.write(f"    ⚠️  Failed to parse line: {error}")

    except FileNotFoundError:
        tqdm.write(f"    ⚠️  File not found: {ndjson_file}")
    except Exception as error:
        tqdm.write(f"    ⚠️  Error reading file: {error}")

    pbar.close()

    file_number = 1
    for i in range(0, len(ordered_keys), MENTIONS_PER_FILE):
        batch_keys = ordered_keys[i : i + MENTIONS_PER_FILE]
        write_mention_batch(
            [mentions_by_key[k] for k in batch_keys],
            file_number,
            output_dir,
        )
        file_number += 1

    print(f"\n  📊 Total mentions processed: {total_processed:,}")
    if total_skipped > 0:
        print(f"  ⚠️  Total mentions skipped: {total_skipped:,}")
    print(f"  📁 Total output files created: {file_number - 1}")


def main() -> None:
    """Main function to process mentions from NDJSON."""
    print("🚀 Starting mention processing...")

    ndjson_file_name = "mentions.ndjson"
    output_folder_name = "mentions"

    print("📍 Step 1: Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    ndjson_file = downloads_dir / ndjson_file_name
    output_dir = downloads_dir / "database" / output_folder_name
    mapping_dir = downloads_dir / "database" / IDENTIFIER_TO_ID_MAP_DIR

    print(f"Reading NDJSON file: {ndjson_file}")
    print(f"Output directory: {output_dir}")
    print(f"Mapping directory: {mapping_dir}")

    if not ndjson_file.exists():
        raise FileNotFoundError(
            f"File not found: {ndjson_file}. "
            f"Please ensure the {ndjson_file_name} file exists in your Downloads directory."
        )
    print("✓ NDJSON file found")

    if output_dir.exists():
        import shutil

        shutil.rmtree(output_dir)
        print("✓ Output directory cleaned")
    else:
        print("✓ Output directory not found")

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
            f"Please run build-identifier-datasetid-map.py to rebuild the mapping."
        )

    print("\n📊 Step 3: Counting total mentions in input file...")
    total_mentions = count_total_mentions(ndjson_file)
    print(f"  Found {total_mentions:,} total mentions to process")

    print(
        f"\n✂️  Step 4: Processing mentions and creating files "
        f"(~{MENTIONS_PER_FILE:,} mentions each)..."
    )
    process_mentions(ndjson_file, output_dir, identifier_to_id, total_mentions)

    print("\n✅ All mentions have been processed successfully!")
    print(f"🎉 Processed files are available in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during mention processing:")
        print(e)
        exit(1)
