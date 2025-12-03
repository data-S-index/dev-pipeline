"""Process ndjson files from datacite-slim-records and emdb-slim-records and create NDJSON files with processed datasets."""

import json
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from tqdm import tqdm


RECORDS_PER_FILE = 10000  # Records per output file


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
    source = record.get("source", "")
    doi = record.get("doi", "")
    doi = doi.lower() if doi else None
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
                    date_str = f"{date_str[:-1]}+00:00"
                published_at = datetime.fromisoformat(date_str)
        except (ValueError, AttributeError, TypeError):
            published_at = None

    random_int = random.randint(0, 999999)

    # Clean subjects
    subjects_raw = record.get("subjects", [])
    subjects = []
    if subjects_raw:
        for subject in subjects_raw:
            if isinstance(subject, str):
                if cleaned_subject := clean_string(subject):
                    subjects.append(cleaned_subject)

    # Clean authors JSON string
    authors_raw = record.get("creators", [])
    authors = "[]"
    try:
        cleaned_authors = json.dumps(authors_raw)
        authors = clean_string(cleaned_authors)
    except (TypeError, ValueError):
        authors = "[]"

    # Clean identifiers
    identifiers_raw = record.get("identifiers", [])
    identifiers = []
    if identifiers_raw:
        # {"identifier": "10.1000/187", "identifierType": "doi"}
        for identifier in identifiers_raw:
            iv = identifier.get("identifier", "")
            identifier_value = iv.lower() if iv else None
            it = identifier.get("identifier_type", "") or identifier.get(
                "identifierType", ""
            )
            identifier_type = it.lower() if it else None
            if identifier_value and identifier_type:
                identifiers.append(
                    {"identifier": identifier_value, "identifierType": identifier_type}
                )
    else:
        identifiers = []

    return {
        "id": dataset_id,
        "source": source,
        "identifier": doi,
        "identifierType": "doi",
        "title": title,
        "extractedIdentifiers": identifiers,
        "description": description,
        "version": version,
        "publisher": publisher,
        "publishedAt": published_at.isoformat() if published_at else None,
        "subjects": subjects,
        "authors": authors,
        "randomInt": random_int,
    }


def parse_emdb_record(record: Dict[str, Any], dataset_id: int) -> Dict[str, Any]:
    """Parse an EMDB record into dataset format (db insert ready)."""
    source = record.get("source", "")
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
                    date_str = f"{date_str[:-1]}+00:00"
                published_at = datetime.fromisoformat(date_str)
        except (ValueError, AttributeError, TypeError):
            published_at = None

    random_int = random.randint(0, 999999)

    # Clean subjects
    subjects_raw = record.get("subjects", [])
    subjects = []
    if subjects_raw:
        for subject in subjects_raw:
            if isinstance(subject, str):
                if cleaned_subject := clean_string(subject):
                    subjects.append(cleaned_subject)

    # Clean authors JSON string
    authors_raw = record.get("creators", [])
    authors = "[]"
    try:
        cleaned_authors = json.dumps(authors_raw)
        authors = clean_string(cleaned_authors)
    except (TypeError, ValueError):
        authors = "[]"

    # There is only one identifier in EMDB, so we can use it as the main identifier
    identifiers_raw = record.get("identifiers", [])
    main_identifier = identifiers_raw[0].get("identifier", "").lower()
    main_identifier_type = identifiers_raw[0].get(
        "identifier_type", ""
    ) or identifiers_raw[0].get("identifierType", "")
    main_identifier_type = (main_identifier_type or "").lower()

    identifiers = [
        {
            "identifier": main_identifier,
            "identifierType": main_identifier_type,
        }
    ]

    return {
        "id": dataset_id,
        "source": source,
        "identifier": main_identifier,
        "identifierType": "emdb",
        "title": title,
        "extractedIdentifiers": identifiers,
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
    """Write a batch of processed records to a numbered NDJSON file."""
    file_name = f"{file_number}.ndjson"
    file_path = output_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def process_all_files(
    ndjson_files: List[Path],
    source_dir: Path,
    output_dir: Path,
    total_lines: int,
    parser_func: Callable[[Dict[str, Any], int], Dict[str, Any]],
    starting_dataset_id: int = 1,
) -> int:
    """Process all ndjson files and create new NDJSON files with processed records.

    Args:
        ndjson_files: List of ndjson file paths to process
        source_dir: Directory containing the ndjson files
        output_dir: Directory to write output JSON files
        total_lines: Total number of lines to process (for progress bar)
        parser_func: Function to parse records (parse_datacite_record or parse_emdb_record)
        starting_dataset_id: Starting dataset ID (default: 1)

    Returns:
        Final dataset_id after processing all records
    """
    dataset_id = starting_dataset_id
    file_number = 1
    current_batch: List[str] = []  # Store raw lines until we have enough
    total_records_processed = 0
    total_records_skipped = 0

    # Create progress bar for overall processing
    pbar = tqdm(total=total_lines, desc="  Processing", unit="record", unit_scale=True)

    # Process files sequentially
    for file_path in ndjson_files:
        file_name = file_path.name
        full_path = source_dir / file_path

        with open(full_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                # Add raw line to batch
                current_batch.append(line)
                pbar.update(1)

                # When batch reaches RECORDS_PER_FILE, process and write
                if len(current_batch) >= RECORDS_PER_FILE:
                    processed_batch = []
                    for raw_line in current_batch:
                        try:
                            record = json.loads(raw_line)
                            processed_dataset = parser_func(record, dataset_id)
                            processed_batch.append(processed_dataset)
                            dataset_id += 1
                            total_records_processed += 1
                        except (json.JSONDecodeError, KeyError, TypeError) as error:
                            print(f"line: {raw_line}")
                            dataset_id += 1
                            total_records_skipped += 1
                            tqdm.write(
                                f"    ⚠️  Failed to parse line in {file_name}: {error}"
                            )

                    # Write the processed batch
                    write_batch_to_file(processed_batch, file_number, output_dir)
                    file_number += 1
                    current_batch = []

    pbar.close()

    # Process and write any remaining records as the final file
    if current_batch:
        processed_batch = []
        for raw_line in current_batch:
            try:
                record = json.loads(raw_line)
                processed_dataset = parser_func(record, dataset_id)
                processed_batch.append(processed_dataset)
                dataset_id += 1
                total_records_processed += 1
            except (json.JSONDecodeError, KeyError, TypeError) as error:
                dataset_id += 1
                total_records_skipped += 1
                tqdm.write(f"    ⚠️  Failed to parse line: {error}")

        write_batch_to_file(processed_batch, file_number, output_dir)

    print(f"\n  📊 Total records processed: {total_records_processed:,}")
    if total_records_skipped > 0:
        print(f"  ⚠️  Total records skipped: {total_records_skipped:,}")
    print(f"  📁 Total output files created: {file_number}")

    return dataset_id


def main() -> None:
    """Main function to process ndjson files and create NDJSON output files."""
    print("🚀 Starting database preparation process...")

    datacite_source_folder_name = "datacite-slim-records"
    emdb_source_folder_name = "emdb-slim-records"
    output_folder_name = "dataset"

    # Step 1: Get OS-agnostic path to Downloads/datacite-slim-records
    print("📍 Step 1: Locating source directory...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    datacite_source_dir = downloads_dir / "slim-records" / datacite_source_folder_name
    emdb_source_dir = downloads_dir / "slim-records" / emdb_source_folder_name
    output_dir = downloads_dir / "database" / output_folder_name

    print(f"Reading ndjson files from: {datacite_source_dir}")
    print(f"Reading ndjson files from: {emdb_source_dir}")
    print(f"Output directory: {output_dir}")

    # Check if source directory exists
    if not datacite_source_dir.exists():
        raise FileNotFoundError(
            f"Directory not found: {datacite_source_dir}. "
            f"Please ensure the {datacite_source_folder_name} folder exists in your Downloads directory."
        )
    print("✓ Datacite source directory found")

    if not emdb_source_dir.exists():
        raise FileNotFoundError(
            f"Directory not found: {emdb_source_dir}. "
            f"Please ensure the {emdb_source_folder_name} folder exists in your Downloads directory."
        )
    print("✓ EMDB source directory found")

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

    # Step 2: Find all ndjson files in datacite source directory
    print("\n📖 Step 2: Finding ndjson files...")
    ndjson_files = list(datacite_source_dir.glob("*.ndjson"))

    # sort by filename
    ndjson_files = sorted(ndjson_files, key=lambda x: x.name)

    print(f"  Found {len(ndjson_files)} .ndjson file(s) to process")

    if not ndjson_files:
        raise FileNotFoundError("No .ndjson files found in source directory")

    # Step 3: Count total lines in all files
    print("\n📊 Step 3: Counting total lines in input files...")
    # total_lines = count_lines_in_files(ndjson_files, datacite_source_dir)
    total_lines = 49009522
    print(f"  Found {total_lines:,} total lines to process")

    # Step 4: Process all files and create new files with processed records
    print(
        f"\n✂️  Step 4: Processing files and creating new files "
        f"(~{RECORDS_PER_FILE:,} records each)..."
    )

    final_dataset_id = process_all_files(
        ndjson_files,
        datacite_source_dir,
        output_dir,
        total_lines,
        parse_datacite_record,
    )

    print("\n✅ Datacite files have been processed successfully!")
    print(f"🎉 Processed files are available in: {output_dir}")

    # Step 5: Find all ndjson files in emdb source directory
    print("\n📖 Step 5: Finding ndjson files...")
    emdb_ndjson_files = list(emdb_source_dir.glob("*.ndjson"))
    print(f"  Found {len(emdb_ndjson_files)} .ndjson file(s) to process")
    if not emdb_ndjson_files:
        raise FileNotFoundError("No .ndjson files found in EMDB source directory")

    # Step 6: Count total lines in all files
    print("\n📊 Step 6: Counting total lines in input files...")
    emdb_total_lines = count_lines_in_files(emdb_ndjson_files, emdb_source_dir)
    print(f"  Found {emdb_total_lines:,} total lines to process")

    # Step 7: Process all files and create new files with processed records
    # Continue dataset_id from where datacite left off
    print(
        f"\n✂️  Step 7: Processing files and creating new files "
        f"(~{RECORDS_PER_FILE:,} records each)..."
    )
    print(f"  Continuing dataset_id from {final_dataset_id}...")

    process_all_files(
        emdb_ndjson_files,
        emdb_source_dir,
        output_dir,
        emdb_total_lines,
        parse_emdb_record,
        starting_dataset_id=final_dataset_id,
    )

    print("\n✅ All files have been processed successfully!")
    print(f"🎉 Processed files are available in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during database preparation:")
        print(e)
        exit(1)
