"""Estimate Fuji scores for missing records based on prefix distribution."""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import psycopg
from tqdm import tqdm

from config import DATABASE_URL
from identifier_mapping import (
    IDENTIFIER_TO_ID_MAP_DIR,
    load_identifier_to_id_mapping_from_dir,
)

RECORDS_PER_FILE = 10000  # Records per output file
DB_FETCH_BATCH_SIZE = 50000  # Records to fetch from database at once


def extract_doi_prefix(doi: str) -> Optional[str]:
    """Extract DOI prefix (e.g., '10.5517' from '10.5517/cc7gs7p')."""
    if not doi or not doi.startswith("10."):
        return None

    # Find the first '/' after '10.'
    slash_index = doi.find("/")
    if slash_index > 0:
        return doi[:slash_index]
    # If no slash found, return the whole DOI as prefix
    return doi


def load_prefix_distributions(
    distribution_file: Path,
) -> Dict[str, str]:
    """
    Load score distributions from the JSON file and return the most frequent score for each prefix.

    Returns:
        Dictionary mapping prefix to most frequent score (as string)
    """
    print("  Loading score distributions from file...")

    if not distribution_file.exists():
        raise FileNotFoundError(
            f"Distribution file not found: {distribution_file}. "
            f"Please run analyze-fuji-distribution.py first to create the distribution file."
        )

    try:
        with open(distribution_file, "r", encoding="utf-8") as f:
            distribution_data = json.load(f)

        # Extract the most frequent score for each prefix
        # The file format is: {prefix: {score: count}}
        prefix_to_score: Dict[str, str] = {}

        for prefix, score_counts in distribution_data.items():
            if score_counts:
                # Find the score with the highest count
                most_frequent_score = max(score_counts.items(), key=lambda x: x[1])[0]
                prefix_to_score[prefix] = most_frequent_score

        print(f"  ✓ Loaded distributions for {len(prefix_to_score):,} prefixes")
        return prefix_to_score

    except Exception as e:
        raise RuntimeError(f"Error reading distribution file {distribution_file}: {e}")


def estimate_score_from_prefix(
    prefix: Optional[str], prefix_to_score: Dict[str, str]
) -> Optional[float]:
    """
    Estimate a score for a prefix using the most frequent score from the distribution.
    """
    if not prefix or prefix not in prefix_to_score:
        return None

    score_str = prefix_to_score[prefix]
    return float(score_str)


def serialize_datetime(obj):
    """Serialize datetime objects to ISO format strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def write_batch_to_file(batch: list, file_number: int, output_dir: Path) -> None:
    """Write a batch of records to an NDJSON file."""
    file_name = f"{file_number}.ndjson"
    file_path = output_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(
                json.dumps(record, ensure_ascii=False, default=serialize_datetime)
                + "\n"
            )


def copy_existing_files(source_dir: Path, output_dir: Path) -> int:
    """
    Copy existing NDJSON files from source directory to output directory.
    Returns the next file number to use.
    """
    print("  Copying existing files...")

    if not source_dir.exists():
        print(f"  ⚠️  Source directory not found: {source_dir}")
        return 1

    # Find all NDJSON files and sort them
    ndjson_files = sorted(source_dir.glob("*.ndjson"), key=lambda p: int(p.stem))

    if not ndjson_files:
        print("  ⚠️  No existing NDJSON files found")
        return 1

    print(f"  Found {len(ndjson_files):,} existing files")

    import shutil

    for ndjson_file in tqdm(ndjson_files, desc="  Copying", unit="file"):
        shutil.copy2(ndjson_file, output_dir / ndjson_file.name)

    # Get the highest file number and return next
    max_file_num = max(int(f.stem) for f in ndjson_files)
    next_file_num = max_file_num + 1

    print(f"  ✓ Copied {len(ndjson_files):,} files (next file number: {next_file_num})")
    return next_file_num


def export_all_records_with_estimates(
    conn: psycopg.Connection,
    identifier_to_id: Dict[str, int],
    prefix_to_score: Dict[str, str],
    source_dir: Path,
    output_dir: Path,
) -> None:
    """
    Copy existing records from source directory and add estimated scores from FujiJob.
    """
    print("  Processing records (copying existing + adding estimates)...")

    # Copy existing files first
    file_number = copy_existing_files(source_dir, output_dir)

    default_evaluation_date = datetime.now()
    default_metric_version = "estimated"
    default_software_version = "estimated"

    # Get count of records in FujiJob
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM "FujiJob" fj
            INNER JOIN "Dataset" d ON fj."datasetId" = d.id
            WHERE d."identifierType" = 'doi'
            """
        )
        total_estimated_records = cur.fetchone()[0]
        print(f"  Found {total_estimated_records:,} records in FujiJob to estimate")

    current_batch = []
    total_processed = 0
    total_estimated = 0
    total_no_score = 0
    total_mapped = 0
    total_unmapped = 0

    pbar = tqdm(
        total=total_estimated_records,
        desc="  Adding estimates",
        unit="record",
        unit_scale=True,
    )

    with conn.cursor() as cur:

        # Fetch records from FujiJob (missing scores)
        cur.execute(
            """
            SELECT
                d.id,
                d.identifier
            FROM "FujiJob" fj
            INNER JOIN "Dataset" d ON fj."datasetId" = d.id
            WHERE d."identifierType" = 'doi'
            ORDER BY d.id
            """
        )

        # Process records from FujiJob (missing scores)
        while True:
            rows = cur.fetchmany(DB_FETCH_BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                dataset_id_db, doi = row

                # Try to get dataset_id from mapping file
                identifier_lower = doi.lower() if doi else None
                dataset_id = (
                    identifier_to_id.get(identifier_lower) if identifier_lower else None
                )

                # Try to estimate score from prefix
                prefix = extract_doi_prefix(doi) if doi else None
                score = estimate_score_from_prefix(prefix, prefix_to_score)
                estimated = False

                if score is not None:
                    estimated = True
                    total_estimated += 1
                else:
                    total_no_score += 1

                # Create record with estimated score (or null if no distribution)
                record = {
                    "id": dataset_id or dataset_id_db,
                    "doi": doi,
                    "score": float(score) if score is not None else None,
                    "evaluationDate": (
                        default_evaluation_date.isoformat()
                        if default_evaluation_date
                        else None
                    ),
                    "metricVersion": default_metric_version,
                    "softwareVersion": default_software_version,
                }

                if estimated:
                    record["estimated"] = True

                # Add database ID if different from mapped ID
                if dataset_id and dataset_id != dataset_id_db:
                    record["databaseId"] = dataset_id_db

                current_batch.append(record)
                total_processed += 1

                if dataset_id:
                    total_mapped += 1
                else:
                    total_unmapped += 1

                # Write batch when it reaches RECORDS_PER_FILE
                if len(current_batch) >= RECORDS_PER_FILE:
                    write_batch_to_file(current_batch, file_number, output_dir)
                    file_number += 1
                    current_batch = []

                pbar.update(1)

    pbar.close()

    # Write remaining records as final file
    if current_batch:
        write_batch_to_file(current_batch, file_number, output_dir)

    print("\n  📊 Export Summary:")
    print(f"    - Records with estimated scores added: {total_estimated:,}")
    print(
        f"    - Records without scores (prefix not in distribution): {total_no_score:,}"
    )
    print(f"    - Records with mapped dataset_id: {total_mapped:,}")
    print(f"    - Records without mapping: {total_unmapped:,}")
    print(f"    - New files created: {file_number - 1}")


def main() -> None:
    """Main function to estimate Fuji scores for missing records."""
    print("🚀 Starting Fuji score estimation...")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set in environment or .env file")

    # Get OS-agnostic paths
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    source_dir = downloads_dir / "database" / "fuji-score"
    output_dir = downloads_dir / "database" / "fuji-score-estimated"
    mapping_dir = downloads_dir / "database" / IDENTIFIER_TO_ID_MAP_DIR
    distribution_file = downloads_dir / "database" / "fuji-score-distribution.json"

    print(f"Source directory: {source_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Mapping directory: {mapping_dir}")
    print(f"Distribution file: {distribution_file}")

    # Clean output directory
    if output_dir.exists():
        import shutil

        shutil.rmtree(output_dir)
        print("✓ Output directory cleaned")
    else:
        print("✓ Output directory not found")

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    print("✓ Output directory ready")

    # Load identifier to ID mapping
    print("\n🗺️  Loading identifier to ID mapping...")
    identifier_to_id = load_identifier_to_id_mapping_from_dir(mapping_dir)
    print(f"  ✓ Loaded {len(identifier_to_id):,} identifier mappings")

    # Load prefix distributions
    print("\n📊 Loading prefix distributions...")
    prefix_to_score = load_prefix_distributions(distribution_file)

    # Connect to database
    print("\n💾 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            # Copy existing files and add estimated records
            print("\n✨ Processing records...")
            export_all_records_with_estimates(
                conn, identifier_to_id, prefix_to_score, source_dir, output_dir
            )

        print("\n✅ Estimation completed successfully!")
        print(f"🎉 Estimated files are available in: {output_dir}")

    except psycopg.Error as e:
        print(f"\n❌ Database error: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
