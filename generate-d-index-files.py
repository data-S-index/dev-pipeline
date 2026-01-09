"""Calculate and export d-index for every dataset at multiple time points to NDJSON files."""

import json
import math
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Normalization factors (set to 1 as requested)
F_T = 1.0
C_T_w = 1.0
M_T_w = 1.0

# Batch processing configuration
BATCH_SIZE = 100000


def calculate_time_delta_years(published_at: datetime, event_date: datetime) -> float:
    """
    Calculate time delta in years between publication date and event date.

    Args:
        published_at: Publication date of the dataset
        event_date: Date of citation or event

    Returns:
        Time delta in years
    """
    delta = event_date - published_at
    return max(0.0, delta.total_seconds() / (365.25 * 24 * 3600))


def calculate_weighted_citation_count(
    citations: List[Tuple[Optional[datetime], float]],
    published_at: datetime,
    cutoff_date: datetime,
) -> float:
    """
    Calculate weighted citation count up to cutoff_date: C_i^w = sum[1 + 0.33 * ln(1 + Δt)]

    Args:
        citations: List of (cited_date, citation_weight) tuples
        published_at: Publication date of the dataset
        cutoff_date: Only count citations up to this date

    Returns:
        Weighted citation count
    """
    weighted_sum = 0.0

    for cited_date, citation_weight in citations:
        # Only count citations up to the cutoff date
        if cited_date is None or cited_date > cutoff_date:
            continue

        delta_t = calculate_time_delta_years(published_at, cited_date)
        # Weight formula: 1 + 0.33 * ln(1 + Δt)
        weight = 1.0 + 0.33 * math.log(1.0 + delta_t)
        weighted_sum += weight * citation_weight

    return weighted_sum


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


def calculate_d_index(
    fair_score: float,
    weighted_citation_count: float,
) -> float:
    """
    Calculate d-index: D(d_i) = (1/3) * (F_i/F_T + C_i^w/C_T^w)
    Note: Mentions are ignored as requested.

    Args:
        fair_score: FAIR score (F_i) from FujiScore (0 if not available)
        weighted_citation_count: Weighted citation count (C_i^w)

    Returns:
        d-index value
    """
    # Avoid division by zero (though normalization factors are 1.0)
    F_T_safe = max(F_T, 0.001)
    C_T_w_safe = max(C_T_w, 0.001)

    # Calculate components
    fair_component = fair_score / F_T_safe
    citation_component = weighted_citation_count / C_T_w_safe

    # Calculate d-index (mentions component is 0 since we ignore mentions)
    return (1.0 / 3.0) * (fair_component + citation_component + 0.0)


def get_all_time_points(
    published_at: datetime,
    citation_dates: List[Optional[datetime]],
) -> List[datetime]:
    """
    Get all time points for d-index calculation:
    1. Dataset creation date (publishedAt)
    2. Every citation date

    Args:
        published_at: Publication date of the dataset
        citation_dates: List of citation dates

    Returns:
        Sorted list of unique time points
    """
    time_points = {published_at}

    # Add all citation dates (filter out None)
    for cited_date in citation_dates:
        if cited_date is not None:
            time_points.add(cited_date)

    # Return sorted list
    return sorted(time_points)


def process_dataset(
    dataset_id: int,
    published_at: datetime,
    fair_score: float,
    citations: List[Tuple[Optional[datetime], float]],
) -> List[Tuple[datetime, float]]:
    """
    Calculate d-index for a dataset at all relevant time points.

    Args:
        dataset_id: ID of the dataset
        published_at: Publication date of the dataset
        fair_score: FAIR score for the dataset (0 if not available)
        citations: List of (cited_date, citation_weight) tuples

    Returns:
        List of (time_point, d_index) tuples
    """
    # Extract citation dates
    citation_dates = [cited_date for cited_date, _ in citations]

    # Get all time points
    time_points = get_all_time_points(published_at, citation_dates)

    # Calculate d-index at each time point
    results = []
    for time_point in time_points:
        # Calculate weighted citation count up to this time point
        weighted_citations = calculate_weighted_citation_count(
            citations, published_at, time_point
        )

        # Calculate d-index
        d_index = calculate_d_index(fair_score, weighted_citations)

        results.append((time_point, d_index))

    return results


def main() -> None:
    """Main function to calculate and export d-index for all datasets to NDJSON files."""
    print("🚀 Starting d-index calculation process...")

    # Get OS-agnostic paths (matching generate-fuji-files.py pattern)
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    output_dir = downloads_dir / "database" / "dindex"

    print(f"Output directory: {output_dir}")

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

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            print("  ✅ Connected to database")

            # Get max dataset ID (since it's autoincrement)
            print("\n📊 Getting dataset ID range...")
            with conn.cursor() as cur:
                cur.execute('SELECT MAX(id) FROM "Dataset"')
                max_dataset_id = cur.fetchone()[0] or 0

            total_datasets = max_dataset_id
            print(f"  Found {total_datasets:,} datasets (max ID: {max_dataset_id})")

            # Process datasets in batches
            print(
                f"\n📈 Calculating d-index values (processing {BATCH_SIZE:,} datasets per batch)..."
            )
            total_records = 0
            processed_datasets = 0
            file_number = 1
            current_batch = []
            current_id = 1  # Start from ID 1

            with conn.cursor() as cur:
                # Create progress bar for datasets
                pbar = tqdm(
                    total=total_datasets,
                    desc="  Processing datasets",
                    unit="dataset",
                    unit_scale=True,
                )

                # Process in batches
                while current_id <= max_dataset_id:
                    # Calculate batch range
                    batch_end = min(current_id + BATCH_SIZE - 1, max_dataset_id)

                    # Fetch publishedAt for datasets in this batch
                    cur.execute(
                        """
                        SELECT id, "publishedAt"
                        FROM "Dataset"
                        WHERE id >= %s AND id <= %s
                        ORDER BY id
                    """,
                        (current_id, batch_end),
                    )
                    datasets_batch = cur.fetchall()

                    # Fetch FAIR scores for this batch
                    fair_scores = {}
                    if datasets_batch:
                        cur.execute(
                            """
                            SELECT "datasetId", score
                            FROM "FujiScore"
                            WHERE "datasetId" >= %s AND "datasetId" <= %s
                        """,
                            (current_id, batch_end),
                        )
                        for dataset_id, score in cur.fetchall():
                            fair_scores[dataset_id] = (
                                score if score is not None else 0.0
                            )

                    # Fetch citations for this batch
                    citations_by_dataset = {}
                    if datasets_batch:
                        cur.execute(
                            """
                            SELECT "datasetId", "citedDate", "citationWeight"
                            FROM "Citation"
                            WHERE "datasetId" >= %s AND "datasetId" <= %s
                            ORDER BY "datasetId", "citedDate" NULLS LAST
                        """,
                            (current_id, batch_end),
                        )
                        citations = cur.fetchall()

                        # Group citations by dataset_id
                        for dataset_id, cited_date, citation_weight in citations:
                            if dataset_id not in citations_by_dataset:
                                citations_by_dataset[dataset_id] = []
                            citations_by_dataset[dataset_id].append(
                                (cited_date, citation_weight)
                            )

                    # Process each dataset in the batch
                    for dataset_id, published_at in datasets_batch:
                        # Get FAIR score (default to 0.0 if not found)
                        fair_score = fair_scores.get(dataset_id, 0.0)

                        # Get citations for this dataset (empty list if none)
                        citations = citations_by_dataset.get(dataset_id, [])

                        # Calculate d-index at all time points
                        d_index_results = process_dataset(
                            dataset_id, published_at, fair_score, citations
                        )

                        # Create records for each d-index value
                        for time_point, d_index in d_index_results:
                            record = {
                                "datasetId": dataset_id,
                                "score": d_index,
                                "created": (
                                    time_point.isoformat() if time_point else None
                                ),
                            }
                            current_batch.append(record)
                            total_records += 1

                            # Write batch when it reaches BATCH_SIZE
                            if len(current_batch) >= BATCH_SIZE:
                                write_batch_to_file(
                                    current_batch, file_number, output_dir
                                )
                                file_number += 1
                                current_batch = []

                        processed_datasets += 1
                        pbar.update(1)

                    # Move to next batch
                    current_id = batch_end + 1

                pbar.close()

                # Write remaining records as final file
                if current_batch:
                    write_batch_to_file(current_batch, file_number, output_dir)

            print("\n✅ d-index calculation completed!")
            print("📊 Summary:")
            print(f"  - Datasets processed: {processed_datasets:,}")
            print(f"  - D-index records exported: {total_records:,}")
            print(f"  - Output files created: {file_number}")
            print(f"🎉 Exported files are available in: {output_dir}")

    except psycopg.Error as e:
        print(f"\n❌ Database error: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
