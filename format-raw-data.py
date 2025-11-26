"""Process ndjson files from datacite-slim-records and create JSON files with processed datasets."""

import json
import random
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from tqdm import tqdm


RECORDS_PER_FILE = 1000  # 10k records per file
NUM_WORKERS = cpu_count()  # Use all available CPU cores


def clean_string(s: Optional[str]) -> str:
    """Clean string to remove bad characters."""
    if not s:
        return ""
    # Remove replacement characters (\uFFFD) and control characters (\x00-\x1F and \x7F-\x9F)
    result = s.replace("\ufffd", "")
    # Remove control characters: 0x00-0x1F and 0x7F-0x9F
    cleaned = ""
    for char in result:
        code = ord(char)
        if code < 0x20 or (0x7F <= code < 0xA0):  # Remove control chars
            continue
        cleaned += char
    return cleaned


def parse_datacite_record(record: Dict[str, Any], dataset_id: int) -> Dict[str, Any]:
    """Parse a datacite record into dataset format (db insert ready)."""
    doi = record.get("doi", "").lower() if record.get("doi") else None
    title = record.get("title", "")
    description = (
        clean_string(record.get("description")) if record.get("description") else None
    )
    publisher = record.get("publisher") or None
    version = record.get("version") or None
    published_at = None
    if record.get("publication_date"):
        try:
            date_str = record["publication_date"]
            if isinstance(date_str, str):
                # Handle ISO format with Z suffix
                if date_str.endswith("Z"):
                    date_str = date_str[:-1] + "+00:00"
                published_at = datetime.fromisoformat(date_str)
        except (ValueError, AttributeError, TypeError):
            published_at = None

    random_int = random.randint(0, 999999)
    subjects = record.get("subjects", [])

    # Clean authors JSON string
    authors_raw = record.get("creators", [])
    authors = "[]"
    try:
        cleaned_authors = json.dumps(authors_raw)
        authors = clean_string(cleaned_authors)
    except (TypeError, ValueError):
        authors = "[]"

    return {
        "id": dataset_id,
        "doi": doi,
        "title": title,
        "description": description,
        "version": version,
        "publisher": publisher,
        "publishedAt": published_at.isoformat() if published_at else None,
        "subjects": subjects,
        "authors": authors,
        "randomInt": random_int,
    }


def count_lines_in_files(ndjson_files: List[Path], source_dir: Path) -> int:
    """Count total number of non-empty lines across all ndjson files."""
    total_lines = 0
    print("  Counting lines in input files...")
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file"):
        full_path = source_dir / file_path
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        total_lines += 1
        except Exception:
            # Skip files that can't be read, will be handled in processing
            continue
    return total_lines


def write_batch_to_file(
    batch: List[Dict[str, Any]], file_number: int, output_dir: Path
) -> None:
    """Write a batch of processed records to a numbered JSON file."""
    file_name = f"{file_number}.json"
    file_path = output_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(batch, f, indent=False, ensure_ascii=False)


def process_single_file(
    args: Tuple[Path, Path],
) -> Tuple[List[Dict[str, Any]], int, int]:
    """Process a single ndjson file and return processed records and stats.

    Returns:
        Tuple of (processed_records, records_processed, records_skipped)
    """
    file_path, source_dir = args
    full_path = source_dir / file_path

    processed_records: List[Dict[str, Any]] = []
    records_processed = 0
    records_skipped = 0

    try:
        with open(full_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    # Use temporary ID 0, will be reassigned sequentially later
                    processed_dataset = parse_datacite_record(record, 0)
                    processed_records.append(processed_dataset)
                    records_processed += 1
                except (json.JSONDecodeError, KeyError, TypeError):
                    records_skipped += 1
                    # Note: Can't use tqdm.write in worker process, will log after
    except FileNotFoundError:
        pass  # Will be handled in main process
    except Exception:
        pass  # Will be handled in main process

    return processed_records, records_processed, records_skipped


def process_all_files(
    ndjson_files: List[Path], source_dir: Path, output_dir: Path, total_lines: int
) -> None:
    """Process all ndjson files in parallel and create new JSON files with processed records."""
    # Prepare arguments for parallel processing
    process_args = [(file_path, source_dir) for file_path in ndjson_files]

    # Create progress bar for overall processing
    pbar = tqdm(total=total_lines, desc="  Processing", unit="record", unit_scale=True)

    # Process files in parallel
    all_processed_records: List[Dict[str, Any]] = []
    total_records_processed = 0
    total_records_skipped = 0
    errors: List[str] = []

    with Pool(processes=NUM_WORKERS) as pool:
        # Process files in parallel
        results = pool.imap(process_single_file, process_args)

        for idx, (processed_records, records_processed, records_skipped) in enumerate(
            results
        ):
            file_path = ndjson_files[idx]
            file_name = file_path.name

            all_processed_records.extend(processed_records)
            total_records_processed += records_processed
            total_records_skipped += records_skipped

            pbar.update(records_processed + records_skipped)

            # Check for file errors
            full_path = source_dir / file_path
            if not full_path.exists():
                errors.append(f"    ⚠️  File not found: {full_path}")
            elif records_processed == 0 and records_skipped == 0:
                errors.append(f"    ⚠️  No records processed from: {file_name}")

    pbar.close()

    # Print any errors
    for error in errors:
        print(error)

    # Assign sequential dataset IDs starting from 1
    print("  Assigning sequential dataset IDs...")
    for idx, record in enumerate(all_processed_records, start=1):
        record["id"] = idx

    # Write records in batches
    file_number = 1
    current_batch: List[Dict[str, Any]] = []

    for record in all_processed_records:
        current_batch.append(record)
        if len(current_batch) >= RECORDS_PER_FILE:
            write_batch_to_file(current_batch, file_number, output_dir)
            file_number += 1
            current_batch = []

    # Write any remaining records as the final file
    if current_batch:
        write_batch_to_file(current_batch, file_number, output_dir)

    print(f"\n  📊 Total records processed: {total_records_processed:,}")
    if total_records_skipped > 0:
        print(f"  ⚠️  Total records skipped: {total_records_skipped:,}")
    print(f"  📁 Total output files created: {file_number}")


def main() -> None:
    """Main function to process ndjson files and create JSON output files."""
    print("🚀 Starting database preparation process...")

    source_folder_name = "datacite-slim-records"
    output_folder_name = "database"

    # Step 1: Get OS-agnostic path to Downloads/datacite-slim-records
    print("📍 Step 1: Locating source directory...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    source_dir = downloads_dir / source_folder_name
    output_dir = downloads_dir / output_folder_name

    print(f"Reading ndjson files from: {source_dir}")
    print(f"Output directory: {output_dir}")

    # Check if source directory exists
    if not source_dir.exists():
        raise FileNotFoundError(
            f"Directory not found: {source_dir}. "
            f"Please ensure the {source_folder_name} folder exists in your Downloads directory."
        )
    print("✓ Source directory found")

    # Clean output directory
    if output_dir.exists():
        import shutil

        shutil.rmtree(output_dir)
        print("✓ Output directory cleaned")
    else:
        print("✓ Output directory not found")

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    print("✓ Created output directory")

    # Step 2: Find all ndjson files
    print("\n📖 Step 2: Finding ndjson files...")
    ndjson_files = list(source_dir.glob("*.ndjson"))

    print(f"  Found {len(ndjson_files)} .ndjson file(s) to process")

    if not ndjson_files:
        raise FileNotFoundError("No .ndjson files found in source directory")

    # Step 3: Count total lines in all files
    print("\n📊 Step 3: Counting total lines in input files...")
    total_lines = count_lines_in_files(ndjson_files, source_dir)
    print(f"  Found {total_lines:,} total lines to process")

    # Step 4: Process all files and create new files with processed records
    print(
        f"\n✂️  Step 4: Processing files and creating new files "
        f"(~{RECORDS_PER_FILE:,} records each)..."
    )
    print(f"  Using {NUM_WORKERS} worker process(es) for parallel processing")

    process_all_files(ndjson_files, source_dir, output_dir, total_lines)

    print("\n✅ All files have been processed successfully!")
    print(f"🎉 Processed files are available in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during database preparation:")
        print(e)
        exit(1)
