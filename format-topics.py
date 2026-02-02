"""Process topics from NDJSON files in topics_split directory and create NDJSON files with topic records."""

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from tqdm import tqdm

from identifier_mapping import (
    IDENTIFIER_TO_ID_MAP_DIR,
    load_identifier_to_id_mapping_from_dir,
)

TOPICS_PER_FILE = 10000  # Topics per output file


def extract_topic_from_record(
    record: Dict[str, Any], identifier_to_id: Dict[str, int]
) -> Optional[Dict[str, Any]]:
    """Extract a single topic from a topics split record.

    Record format:
    {
        "dataset_id": "10.57451/lhd.a.ha3.166772.1",
        "topic_id": "T10765",
        "topic_name": "Marine Biology and Ecology Research",
        "score": 0.7918194,
        "source": "openalex",
        "subfield_id": "1910",
        "subfield_name": "Oceanography",
        "field_id": "19",
        "field_name": "Earth and Planetary Sciences",
        "domain_id": "3",
        "domain_name": "Physical Sciences",
        "keywords": "...",
        "summary": "...",
        "wikipedia_url": "https://en.wikipedia.org/wiki/Marine_biology"
    }
    """
    dataset_identifier = record.get("dataset_id")
    if not dataset_identifier:
        return None

    dataset_identifier_lower = dataset_identifier.lower()
    dataset_id_int = identifier_to_id.get(dataset_identifier_lower)

    if dataset_id_int is None:
        # This dataset is not in our mapping, skip
        return None

    topic_id = record.get("topic_id")
    topic_name = record.get("topic_name") or ""
    score = record.get("score")
    if score is not None:
        try:
            score = float(score)
        except (TypeError, ValueError):
            score = None
    source = (record.get("source") or "").strip().lower() or None
    subfield_id = record.get("subfield_id") or None
    subfield_name = record.get("subfield_name") or None
    field_id = record.get("field_id") or None
    field_name = record.get("field_name") or None
    domain_id = record.get("domain_id") or None
    domain_name = record.get("domain_name") or None

    return {
        "datasetId": dataset_id_int,
        "identifier": dataset_identifier_lower,
        "topicId": topic_id,
        "topicName": topic_name,
        "score": score,
        "source": source,
        "subfieldId": subfield_id,
        "subfieldName": subfield_name,
        "fieldId": field_id,
        "fieldName": field_name,
        "domainId": domain_id,
        "domainName": domain_name,
    }


def get_ndjson_files(input_dir: Path) -> List[Path]:
    """Return sorted list of .ndjson files in input directory."""
    if not input_dir.exists() or not input_dir.is_dir():
        return []
    files = sorted(input_dir.glob("*.ndjson"))
    return files


def count_total_topics(ndjson_files: List[Path]) -> int:
    """Count total number of topic records across NDJSON files (for progress bar)."""
    total_topics = 0

    for ndjson_file in ndjson_files:
        if not ndjson_file.exists():
            continue
        try:
            with open(ndjson_file, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                        if record.get("dataset_id") and record.get("topic_id"):
                            total_topics += 1
                    except (json.JSONDecodeError, KeyError, TypeError):
                        pass
        except Exception:
            pass
    return total_topics


def write_topic_batch(
    batch: List[Dict[str, Any]], file_number: int, output_dir: Path
) -> None:
    """Write a batch of topics to a numbered NDJSON file."""
    file_name = f"{file_number}.ndjson"
    file_path = output_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        for topic in batch:
            f.write(json.dumps(topic, ensure_ascii=False) + "\n")


def process_topics(
    ndjson_files: List[Path],
    output_dir: Path,
    identifier_to_id: Dict[str, int],
    total_topics: int,
) -> None:
    """Process NDJSON files and create topic NDJSON files."""
    file_number = 1
    current_batch: List[Dict[str, Any]] = []
    total_topics_processed = 0
    total_topics_skipped = 0

    pbar = tqdm(total=total_topics, desc="  Processing", unit="topic", unit_scale=True)

    for ndjson_file in ndjson_files:
        if not ndjson_file.exists():
            tqdm.write(f"    ⚠️  File not found: {ndjson_file}")
            continue
        try:
            with open(ndjson_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                        topic = extract_topic_from_record(record, identifier_to_id)

                        if topic:
                            current_batch.append(topic)
                            total_topics_processed += 1
                            pbar.update(1)

                            if len(current_batch) >= TOPICS_PER_FILE:
                                write_topic_batch(
                                    current_batch, file_number, output_dir
                                )
                                file_number += 1
                                current_batch = []
                        else:
                            total_topics_skipped += 1

                    except (json.JSONDecodeError, KeyError, TypeError) as error:
                        total_topics_skipped += 1
                        tqdm.write(f"    ⚠️  Failed to parse line: {error}")

        except Exception as error:
            tqdm.write(f"    ⚠️  Error reading file {ndjson_file}: {error}")

    pbar.close()

    if current_batch:
        write_topic_batch(current_batch, file_number, output_dir)

    print(f"\n  📊 Total topics processed: {total_topics_processed:,}")
    if total_topics_skipped > 0:
        print(f"  ⚠️  Total topics skipped: {total_topics_skipped:,}")
    print(f"  📁 Total output files created: {file_number}")


def main() -> None:
    """Main function to process topics from NDJSON files in topics_split directory."""
    print("🚀 Starting topic processing...")

    input_dir_name = "topics_split"
    output_folder_name = "topics"

    print("📍 Step 1: Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    input_dir = downloads_dir / input_dir_name
    output_dir = downloads_dir / "database" / output_folder_name
    mapping_dir = downloads_dir / "database" / IDENTIFIER_TO_ID_MAP_DIR

    print(f"Input directory (NDJSON files): {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Mapping directory: {mapping_dir}")

    ndjson_files = get_ndjson_files(input_dir)
    if not ndjson_files:
        raise FileNotFoundError(
            f"No .ndjson files found in {input_dir}. "
            f"Please ensure the {input_dir_name} directory exists and contains .ndjson files."
        )
    print(f"✓ Found {len(ndjson_files)} NDJSON file(s)")

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

    print("\n📊 Step 3: Counting total topics in input files...")
    total_topics = count_total_topics(ndjson_files)
    print(f"  Found {total_topics:,} total topics to process")

    print(
        f"\n✂️  Step 4: Processing topics and creating files "
        f"(~{TOPICS_PER_FILE:,} topics each)..."
    )

    process_topics(ndjson_files, output_dir, identifier_to_id, total_topics)

    print("\n✅ All topics have been processed successfully!")
    print(f"🎉 Processed files are available in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during topic processing:")
        print(e)
        exit(1)
