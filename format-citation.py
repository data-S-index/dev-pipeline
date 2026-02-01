"""Process citations from NDJSON (datacite/mdc/openalex) and create NDJSON files with citation records."""

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

CITATIONS_PER_FILE = 10000  # Citations per output file


def clean_string(s: Optional[str]) -> str:
    """Clean string to be URL-safe only.

    Keeps only characters that are safe for URLs:
    - Alphanumeric (A-Z, a-z, 0-9)
    - Common URL-safe characters: . / : - _ ~
    """
    if not s:
        return ""

    # URL-safe characters: alphanumeric and common URL characters
    # For identifiers: alphanumeric, dots, slashes, colons, hyphens, underscores
    cleaned = ""
    for char in s:
        # Allow alphanumeric
        if char.isalnum():
            cleaned += char
        # Allow common URL-safe characters for identifiers
        elif char in (".", "/", ":", "-", "_", "~"):
            cleaned += char
        # Remove everything else (including control chars, spaces, special chars)

    return cleaned


def extract_citation_from_record(
    record: Dict[str, Any], identifier_to_id: Dict[str, int]
) -> Optional[Dict[str, Any]]:
    """Extract a single citation from a citation record (datacite/mdc/openalex).

    Record format (supports both "doi" and "dataset_id" for citing dataset):
    {
        "dataset_id": "10.5517/ccs6ckw",  # or "doi"
        "source": ["datacite", "mdc", "openalex"],
        "citation_link": "https://doi.org/10.1039/b916280c",
        "citation_weight": 1.0,
        "citation_date": "2009-01-01T00:00:00",
        "placeholder_date": false
    }
    """
    # Citing dataset identifier: accept "dataset_id" (current format) or "doi" (legacy)
    citing_identifier = record.get("dataset_id") or record.get("doi")
    if not citing_identifier:
        return None

    citing_identifier_lower = citing_identifier.lower()
    citing_dataset_id = identifier_to_id.get(citing_identifier_lower)

    if not citing_dataset_id:
        # This dataset is not in our mapping, skip
        return None

    # Get the citation link (cited DOI)
    citation_link = record.get("citation_link") or record.get("citationLink")
    if not citation_link:
        return None

    # Clean and normalize the citation link
    cited_link_cleaned = clean_string(citation_link)

    # Skip if cleaning removed all characters
    if not cited_link_cleaned:
        return None

    # Parse cited date if available
    cited_date = record.get("citation_date") or record.get("citedDate")
    if cited_date:
        # Already in ISO format, but validate and normalize
        try:
            if isinstance(cited_date, str):
                # Handle ISO format with Z suffix
                if cited_date.endswith("Z"):
                    cited_date = f"{cited_date[:-1]}+00:00"
                # Validate by parsing
                datetime.fromisoformat(cited_date)
            else:
                cited_date = None
        except (ValueError, AttributeError, TypeError):
            cited_date = None
    else:
        cited_date = None

    # Parse citation weight if available
    citation_weight = 1.0
    weight_value = record.get("citation_weight") or record.get("citationWeight")
    if weight_value is not None:
        with contextlib.suppress(ValueError, TypeError):
            citation_weight = float(weight_value)
    # Build source list (lowercase): ["datacite", "mdc", "openalex"]
    source = record.get("source", [])
    if isinstance(source, list):
        source_list = [s.lower() for s in source if isinstance(s, str)]
    else:
        source_list = []

    mdc = "mdc" in source_list
    datacite = "datacite" in source_list
    openalex = "openalex" in source_list

    # citation_date: ISO string or None
    if cited_date:
        citation_date_str = (
            cited_date.replace("+00:00", "") if isinstance(cited_date, str) else None
        )
    else:
        citation_date_str = datetime.now().isoformat()  # today's date

    return {
        "datasetId": citing_dataset_id,
        "identifier": citing_identifier_lower,
        "citationLink": cited_link_cleaned,
        "datacite": datacite,
        "mdc": mdc,
        "openAlex": openalex,
        "citationWeight": citation_weight,
        "citedDate": citation_date_str,
    }


def count_total_citations(ndjson_file: Path) -> int:
    """Count total number of citations in NDJSON file (for progress bar)."""
    total_citations = 0

    if not ndjson_file.exists():
        return 0

    # Count total lines first
    print("  Counting lines in input file...")
    total_lines = 0
    try:
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    total_lines += 1
    except Exception:
        return 0

    # Count citations with progress bar
    print("  Counting citations in input file...")
    pbar = tqdm(
        total=total_lines, desc="  Counting citations", unit="record", unit_scale=True
    )

    with contextlib.suppress(Exception):
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                with contextlib.suppress(json.JSONDecodeError, KeyError, TypeError):
                    record = json.loads(line)
                    # Each line is one citation record (dataset_id or doi + citation_link)
                    has_citing = record.get("dataset_id") or record.get("doi")
                    has_link = record.get("citation_link") or record.get("citationLink")
                    if has_citing and has_link:
                        total_citations += 1
                pbar.update(1)
    pbar.close()
    return total_citations


def write_citation_batch(
    batch: List[Dict[str, Any]], file_number: int, output_dir: Path
) -> None:
    """Write a batch of citations to a numbered NDJSON file."""
    file_name = f"{file_number}.ndjson"
    file_path = output_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        for citation in batch:
            f.write(json.dumps(citation, ensure_ascii=False) + "\n")


def process_citations(
    ndjson_file: Path,
    output_dir: Path,
    identifier_to_id: Dict[str, int],
    total_citations: int,
) -> None:
    """Process NDJSON file and create citation NDJSON files."""
    file_number = 1
    current_batch: List[Dict[str, Any]] = []
    total_citations_processed = 0
    total_citations_skipped = 0

    # Create progress bar for overall processing
    pbar = tqdm(
        total=total_citations, desc="  Processing", unit="citation", unit_scale=True
    )

    # Process NDJSON file line by line
    try:
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    citation = extract_citation_from_record(record, identifier_to_id)

                    if citation:
                        current_batch.append(citation)
                        total_citations_processed += 1
                        pbar.update(1)

                        # When batch reaches CITATIONS_PER_FILE, write to file
                        if len(current_batch) >= CITATIONS_PER_FILE:
                            write_citation_batch(current_batch, file_number, output_dir)
                            file_number += 1
                            current_batch = []
                    else:
                        total_citations_skipped += 1

                except (json.JSONDecodeError, KeyError, TypeError) as error:
                    total_citations_skipped += 1
                    tqdm.write(f"    ⚠️  Failed to parse line: {error}")

    except FileNotFoundError:
        tqdm.write(f"    ⚠️  File not found: {ndjson_file}")
    except Exception as error:
        tqdm.write(f"    ⚠️  Error reading file: {error}")

    pbar.close()

    # Write any remaining citations as the final file
    if current_batch:
        write_citation_batch(current_batch, file_number, output_dir)

    print(f"\n  📊 Total citations processed: {total_citations_processed:,}")
    if total_citations_skipped > 0:
        print(f"  ⚠️  Total citations skipped: {total_citations_skipped:,}")
    print(f"  📁 Total output files created: {file_number}")


def main() -> None:
    """Main function to process citations from NDJSON (datacite/mdc/openalex)."""
    print("🚀 Starting citation processing...")

    ndjson_file_name = "citations.ndjson"
    dataset_folder_name = "dataset"
    output_folder_name = "citations"

    # Step 1: Get OS-agnostic paths
    print("📍 Step 1: Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    ndjson_file = downloads_dir / ndjson_file_name
    dataset_dir = downloads_dir / "database" / dataset_folder_name
    output_dir = downloads_dir / "database" / output_folder_name
    mapping_dir = downloads_dir / "database" / IDENTIFIER_TO_ID_MAP_DIR

    print(f"Reading NDJSON file: {ndjson_file}")
    print(f"Reading dataset files from: {dataset_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Mapping directory: {mapping_dir}")

    # Check if NDJSON file exists
    if not ndjson_file.exists():
        raise FileNotFoundError(
            f"File not found: {ndjson_file}. "
            f"Please ensure the {ndjson_file_name} file exists in your Downloads directory."
        )
    print("✓ NDJSON file found")

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

    # Ensure mapping directory exists
    mapping_dir.mkdir(parents=True, exist_ok=True)

    # Step 2: Load identifier to ID mapping (one NDJSON per dataset file; progress by file count)
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

    # Step 3: Count total citations
    print("\n📊 Step 3: Counting total citations in input file...")
    total_citations = count_total_citations(ndjson_file)
    print(f"  Found {total_citations:,} total citations to process")

    # Step 4: Process citations
    print(
        f"\n✂️  Step 4: Processing citations and creating files "
        f"(~{CITATIONS_PER_FILE:,} citations each)..."
    )

    process_citations(ndjson_file, output_dir, identifier_to_id, total_citations)

    print("\n✅ All citations have been processed successfully!")
    print(f"🎉 Processed files are available in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during citation processing:")
        print(e)
        exit(1)
