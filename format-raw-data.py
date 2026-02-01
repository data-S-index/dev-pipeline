"""Process ndjson files from datacite-slim-records and emdb-slim-records and create NDJSON files with processed datasets."""

import contextlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from tqdm import tqdm


RECORDS_PER_FILE = 10000  # Records per output file


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    # Split filename into text and numeric parts
    parts = re.split(r"(\d+)", name)
    # Convert numeric parts to int, keep text parts as strings
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


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


def parse_publication_date(record: Dict[str, Any]) -> Optional[datetime]:
    """Parse publication date from record."""
    if not record.get("publication_date"):
        return None
    with contextlib.suppress(ValueError, AttributeError, TypeError):
        date_str = record["publication_date"]
        if isinstance(date_str, str):
            # Handle ISO format with Z suffix
            if date_str.endswith("Z"):
                date_str = f"{date_str[:-1]}+00:00"
            return datetime.fromisoformat(date_str)
    return None


def clean_subjects(record: Dict[str, Any]) -> List[str]:
    """Clean subjects from record."""
    subjects = []
    if subjects_raw := record.get("subjects", []):
        for subject in subjects_raw:
            if isinstance(subject, str):
                if cleaned_subject := clean_string(subject):
                    subjects.append(cleaned_subject)
    return subjects


def clean_authors(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Clean authors from record."""
    authors = []
    if authors_raw := record.get("creators", []):
        for author in authors_raw:
            if isinstance(author, dict):
                name_type = author.get("name_type", "")
                name = author.get("name", "")
                name_identifiers = author.get("identifiers", [])
                affiliations = author.get("affiliations", [])

                cleaned_author = {
                    "nameType": name_type,
                    "name": clean_string(name),
                    "nameIdentifiers": [
                        clean_string(identifier) for identifier in name_identifiers
                    ],
                    "affiliations": [
                        clean_string(affiliation) for affiliation in affiliations
                    ],
                }
                authors.append(cleaned_author)
    return authors


def parse_datacite_record(record: Dict[str, Any], dataset_id: int) -> Dict[str, Any]:
    """Parse a datacite record into dataset format (db insert ready)."""
    source = record.get("source", "")
    title = record.get("title", "")
    description = (
        clean_string(record.get("description")) if record.get("description") else None
    )
    publisher = record.get("publisher") or None
    version = record.get("version") or None
    published_at = parse_publication_date(record)

    # Clean subjects
    subjects = clean_subjects(record)

    # Clean authors - keep as list of dicts, clean name and nameIdentifiers
    authors = clean_authors(record)

    identifiers = []
    if identifiers_raw := record.get("identifiers", []):
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

        # remove duplicates from identifiers
        # Convert to tuples for deduplication (dicts are unhashable)
        seen = set()
        unique_identifiers = []
        for identifier in identifiers:
            identifier_tuple = (identifier["identifier"], identifier["identifierType"])
            if identifier_tuple not in seen:
                seen.add(identifier_tuple)
                unique_identifiers.append(identifier)
        identifiers = unique_identifiers
    else:
        identifiers = []

    main_identifier = identifiers[0].get("identifier", "")
    main_identifier_type = identifiers[0].get("identifierType", "")
    if main_identifier_type in ["doi", "emdb_id"]:
        main_identifier = main_identifier.lower()

    return {
        "id": dataset_id,
        "source": source,
        "identifier": main_identifier,
        "identifierType": main_identifier_type,
        "title": title,
        "extractedIdentifiers": identifiers,
        "description": description,
        "version": version,
        "publisher": publisher,
        "publishedAt": published_at.isoformat() if published_at else None,
        "subjects": subjects,
        "authors": authors,
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
    published_at = parse_publication_date(record)

    # Clean subjects
    subjects = clean_subjects(record)

    # Clean authors - keep as list of dicts, clean name and nameIdentifiers
    authors = clean_authors(record)

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
    batch: List[Dict[str, Any]],
    file_number: int,
    output_dir: Path,
    prefix: Optional[str] = None,
) -> None:
    """Write a batch of processed records to a numbered NDJSON file."""
    file_name = f"{prefix}{file_number}.ndjson" if prefix else f"{file_number}.ndjson"
    file_path = output_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def process_record_line(
    raw_line: str,
    dataset_id: int,
    parser_func: Callable[[Dict[str, Any], int], Dict[str, Any]],
    file_name: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], int, bool]:
    """Process a single record line.

    Args:
        raw_line: Raw JSON line to process
        dataset_id: Current dataset ID
        parser_func: Function to parse records
        file_name: Optional file name for error messages

    Returns:
        Tuple of (processed_dataset or None, next_dataset_id, success)
    """
    try:
        record = json.loads(raw_line)
        processed_dataset = parser_func(record, dataset_id)
        return processed_dataset, dataset_id + 1, True
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        if file_name:
            tqdm.write(f"    ⚠️  Failed to parse line in {file_name}: {error}")
        else:
            tqdm.write(f"    ⚠️  Failed to parse line: {error}")
        return None, dataset_id + 1, False


def process_all_files(
    ndjson_files: List[Path],
    source_dir: Path,
    output_dir: Path,
    total_lines: int,
    parser_func: Callable[[Dict[str, Any], int], Dict[str, Any]],
    starting_dataset_id: int = 1,
    prefix: Optional[str] = None,
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
                        processed_dataset, dataset_id, success = process_record_line(
                            raw_line, dataset_id, parser_func, file_name
                        )
                        if success:
                            processed_batch.append(processed_dataset)
                            total_records_processed += 1
                        else:
                            total_records_skipped += 1

                    # Write the processed batch
                    write_batch_to_file(
                        processed_batch, file_number, output_dir, prefix
                    )
                    file_number += 1
                    current_batch = []

    pbar.close()

    # Process and write any remaining records as the final file
    if current_batch:
        processed_batch = []
        for raw_line in current_batch:
            processed_dataset, dataset_id, success = process_record_line(
                raw_line, dataset_id, parser_func
            )
            if success:
                processed_batch.append(processed_dataset)
                total_records_processed += 1
            else:
                total_records_skipped += 1

        write_batch_to_file(processed_batch, file_number, output_dir, prefix)

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

    # sort by filename using natural sort (alphabetical then numerical)
    ndjson_files = sorted(ndjson_files, key=natural_sort_key)

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
        prefix="datacite-",
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
        prefix="emdb-",
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
