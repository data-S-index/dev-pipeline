"""Process citations from MDC NDJSON file and create JSON files with citation records."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from tqdm import tqdm


CITATIONS_PER_FILE = 10000  # Citations per output file
DOI_TO_ID_MAP_FILE = "doi_to_id_map.json"  # Intermediate mapping file


def clean_string(s: Optional[str]) -> str:
    """Clean string to be URL-safe only.

    Keeps only characters that are safe for URLs:
    - Alphanumeric (A-Z, a-z, 0-9)
    - Common URL-safe characters: . / : - _ ~
    """
    if not s:
        return ""

    # URL-safe characters: alphanumeric and common URL characters
    # For DOIs: alphanumeric, dots, slashes, colons, hyphens, underscores
    cleaned = ""
    for char in s:
        # Allow alphanumeric
        if char.isalnum():
            cleaned += char
        # Allow common URL-safe characters for DOIs
        elif char in (".", "/", ":", "-", "_", "~"):
            cleaned += char
        # Remove everything else (including control chars, spaces, special chars)

    return cleaned


def extract_citation_from_mdc_record(
    record: Dict[str, Any], doi_to_id: Dict[str, int]
) -> Optional[Dict[str, Any]]:
    """Extract a single citation from an MDC citation record.

    MDC record format:
    {
        "doi": "10.3886/icpsr36361",  # Citing dataset DOI
        "source": ["mdc"],
        "citation_link": "https://doi.org/10.2105/ajph.2017.303743",  # Cited DOI
        "citation_date": "2017-06-01T00:00:00+00:00",
        "citation_weight": 1.26
    }
    """
    # Get the DOI of the citing dataset
    citing_doi = record.get("doi")
    if not citing_doi:
        return None

    citing_doi_lower = citing_doi.lower()
    citing_dataset_id = doi_to_id.get(citing_doi_lower)

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
                    cited_date = cited_date[:-1] + "+00:00"
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
        try:
            citation_weight = float(weight_value)
        except (ValueError, TypeError):
            pass

    # Determine source flags
    source = record.get("source", [])
    if not isinstance(source, list):
        source = []

    # Create citation record
    citation = {
        "datasetId": citing_dataset_id,
        "doi": citing_doi_lower,  # Source DOI (citing dataset)
        "citationLink": cited_link_cleaned,  # Target link (cited, cleaned for URL safety)
        "datacite": False,
        "mdc": True,
        "openAlex": False,
        "citedDate": cited_date,
        "citationWeight": citation_weight,
    }

    return citation


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

    try:
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    # Each line is one citation record
                    if record.get("doi") and (
                        record.get("citation_link") or record.get("citationLink")
                    ):
                        total_citations += 1
                except (json.JSONDecodeError, KeyError, TypeError):
                    pass
                pbar.update(1)
    except Exception:
        pass

    pbar.close()
    return total_citations


def write_citation_batch(
    batch: List[Dict[str, Any]], file_number: int, output_dir: Path
) -> None:
    """Write a batch of citations to a numbered JSON file."""
    file_name = f"{file_number}.json"
    file_path = output_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(batch, f, indent=False, ensure_ascii=False)


def process_citations(
    ndjson_file: Path,
    output_dir: Path,
    doi_to_id: Dict[str, int],
    total_citations: int,
) -> None:
    """Process NDJSON file and create citation JSON files."""
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
                    citation = extract_citation_from_mdc_record(record, doi_to_id)

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
    """Main function to process citations from MDC NDJSON file."""
    print("🚀 Starting citation processing...")

    ndjson_file_name = "citations-mdc-full-with-weight.ndjson"
    dataset_folder_name = "dataset"
    output_folder_name = "citations"

    # Step 1: Get OS-agnostic paths
    print("📍 Step 1: Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    ndjson_file = downloads_dir / ndjson_file_name
    dataset_dir = downloads_dir / "database" / dataset_folder_name
    output_dir = downloads_dir / "database" / output_folder_name
    mapping_file = downloads_dir / "database" / DOI_TO_ID_MAP_FILE

    print(f"Reading NDJSON file: {ndjson_file}")
    print(f"Reading dataset files from: {dataset_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Mapping file: {mapping_file}")

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

    # Ensure mapping file directory exists
    mapping_file.parent.mkdir(parents=True, exist_ok=True)

    # Step 2: Load DOI to ID mapping
    print("\n🗺️  Step 2: Loading DOI to ID mapping...")
    if not mapping_file.exists():
        raise FileNotFoundError(
            f"Mapping file not found: {mapping_file}. "
            f"Please run build-doi-datasetid-map.py first to create the mapping file."
        )

    try:
        with open(mapping_file, "r", encoding="utf-8") as f:
            doi_to_id = json.load(f)
        print(f"  ✓ Loaded {len(doi_to_id):,} DOI mappings from file")
    except Exception as e:
        raise RuntimeError(
            f"Error reading mapping file {mapping_file}: {e}. "
            f"Please run build-doi-datasetid-map.py to rebuild the mapping file."
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

    process_citations(ndjson_file, output_dir, doi_to_id, total_citations)

    print("\n✅ All citations have been processed successfully!")
    print(f"🎉 Processed files are available in: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during citation processing:")
        print(e)
        exit(1)
