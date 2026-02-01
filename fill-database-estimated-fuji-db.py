"""Submit estimated Fuji scores to the database."""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Batch size for processing results
# Increased for slow upload speed - larger batches = fewer network round trips
BATCH_SIZE = 1000

# Number of threads for parallel processing
# Reduced for slow upload speed - network is the bottleneck, not CPU
# Fewer threads = less connection overhead and better for slow networks
NUM_THREADS = 3


def format_eta(seconds: float) -> str:
    """
    Format seconds into a human-readable ETA string.

    Args:
        seconds: Number of seconds

    Returns:
        Formatted ETA string (e.g., "2h 30m 15s" or "45s")
    """
    if seconds < 0:
        return "calculating..."

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def calculate_eta(elapsed_time: float, completed: int, total: int) -> str:
    """
    Calculate ETA based on elapsed time and progress.

    Args:
        elapsed_time: Time elapsed in seconds
        completed: Number of items completed
        total: Total number of items

    Returns:
        Formatted ETA string
    """
    if completed == 0 or total == 0:
        return "calculating..."

    if completed >= total:
        return "0s"

    rate = completed / elapsed_time if elapsed_time > 0 else 0
    if rate == 0:
        return "calculating..."

    remaining = total - completed
    eta_seconds = remaining / rate

    return format_eta(eta_seconds)


def load_estimated_records(input_dir: Path) -> List[Dict[str, Any]]:
    """
    Load estimated records from NDJSON files.

    Args:
        input_dir: Directory containing NDJSON files with estimated scores

    Returns:
        List of records with estimated scores
    """
    print("  Loading estimated records from NDJSON files...")

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    # Find all NDJSON files
    ndjson_files = sorted(input_dir.glob("*.ndjson"), key=lambda p: int(p.stem))

    if not ndjson_files:
        raise FileNotFoundError(f"No NDJSON files found in {input_dir}")

    print(f"  Found {len(ndjson_files):,} NDJSON files")

    estimated_records = []
    total_records = 0

    # First, count total lines for progress tracking
    print("  Counting records in files...")
    total_lines = 0
    for ndjson_file in tqdm(ndjson_files, desc="  Counting", unit="file"):
        with open(ndjson_file, "r", encoding="utf-8") as f:
            total_lines += sum(1 for line in f if line.strip())

    # Now process files with record-level progress
    read_start_time = time.time()
    last_print_time = read_start_time
    with tqdm(total=total_lines, desc="  Reading records", unit="record") as pbar:
        for ndjson_file in ndjson_files:
            with open(ndjson_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        pbar.update(1)
                        continue

                    try:
                        record = json.loads(line)
                        total_records += 1

                        # Only include records marked as estimated
                        if (
                            record.get("estimated") is True
                            and record.get("score") is not None
                        ):
                            estimated_records.append(record)

                    except json.JSONDecodeError as e:
                        print(f"\n⚠️  Error parsing JSON in {ndjson_file}: {e}")
                    except Exception as e:
                        print(f"\n⚠️  Error processing record in {ndjson_file}: {e}")
                    finally:
                        pbar.update(1)

                        # Print progress with ETA every 5 seconds
                        current_time = time.time()
                        if current_time - last_print_time >= 5.0:
                            elapsed = current_time - read_start_time
                            completed = pbar.n
                            eta = calculate_eta(elapsed, completed, total_lines)
                            print(
                                f"  Reading: {completed:,}/{total_lines:,} records (ETA: {eta})"
                            )
                            last_print_time = current_time

    print(
        f"  ✓ Loaded {len(estimated_records):,} estimated records out of {total_records:,} total records"
    )
    return estimated_records


def format_result(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a record for database submission.

    Args:
        record: Record from NDJSON file

    Returns:
        Formatted result dictionary
    """
    # Get dataset ID (prefer 'id' over 'databaseId')
    dataset_id = record.get("id")
    if dataset_id is None:
        dataset_id = record.get("databaseId")

    if dataset_id is None:
        raise ValueError(f"Record missing dataset ID: {record}")

    # Convert to int if it's a string
    if isinstance(dataset_id, str):
        try:
            dataset_id = int(dataset_id)
        except ValueError:
            raise ValueError(f"Invalid dataset ID format: {dataset_id}")

    # Parse evaluationDate string to datetime and convert to ISO format string
    evaluation_date_str = record.get("evaluationDate") or ""
    if evaluation_date_str:
        try:
            evaluation_date = datetime.fromisoformat(
                evaluation_date_str.replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            evaluation_date = datetime.now()
    else:
        evaluation_date = datetime.now()

    result = {
        "datasetId": int(dataset_id),
        "score": float(record["score"]),
        "evaluationDate": evaluation_date.isoformat(),
        "metricVersion": record.get("metricVersion") or "estimated",
        "softwareVersion": record.get("softwareVersion") or "estimated",
    }

    return result


def upsert_results_to_db(results: List[Dict[str, Any]]) -> bool:
    """
    Upsert results directly to the database using COPY for efficiency.
    Uses COPY to bulk insert into temp table, then upserts to main table.
    This is much more efficient than individual INSERT statements, especially
    for slow network connections.

    Args:
        results: List of result dictionaries with datasetId, score, evaluationDate, metricVersion, softwareVersion

    Returns:
        True if successful, False otherwise
    """
    if not results:
        print("  ℹ️  No results to upsert")
        return True

    print(f"  💾 Upserting {len(results)} results to database (using COPY)")

    try:
        # Create a new connection for this thread
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            with conn.cursor() as cur:
                # Create temporary table for batch insert
                cur.execute(
                    """
                    CREATE TEMP TABLE temp_fuji_scores (
                        "datasetId" INT,
                        score FLOAT,
                        "evaluationDate" TIMESTAMP,
                        "metricVersion" TEXT,
                        "softwareVersion" TEXT
                    ) ON COMMIT DROP
                    """
                )

                # Prepare rows for COPY
                fuji_score_rows = []
                for result in results:
                    dataset_id = result["datasetId"]
                    score = result["score"]
                    evaluation_date = datetime.fromisoformat(
                        result["evaluationDate"].replace("Z", "+00:00")
                    )
                    metric_version = result["metricVersion"]
                    software_version = result["softwareVersion"]

                    fuji_score_rows.append(
                        (
                            dataset_id,
                            score,
                            evaluation_date,
                            metric_version,
                            software_version,
                        )
                    )

                # Use COPY to insert into temp table (much faster than individual INSERTs)
                with cur.copy(
                    """COPY temp_fuji_scores ("datasetId", score, "evaluationDate", "metricVersion", "softwareVersion")
                       FROM STDIN"""
                ) as copy:
                    for row in fuji_score_rows:
                        copy.write_row(row)

                # Upsert from temp table to FujiScore table (single operation)
                cur.execute(
                    """
                    INSERT INTO "FujiScore" (
                        "datasetId", 
                        score, 
                        "evaluationDate", 
                        "metricVersion", 
                        "softwareVersion",
                        created,
                        updated
                    )
                    SELECT 
                        "datasetId",
                        score,
                        "evaluationDate",
                        "metricVersion",
                        "softwareVersion",
                        NOW(),
                        NOW()
                    FROM temp_fuji_scores
                    ON CONFLICT ("datasetId")
                    DO UPDATE SET
                        score = EXCLUDED.score,
                        "evaluationDate" = EXCLUDED."evaluationDate",
                        "metricVersion" = EXCLUDED."metricVersion",
                        "softwareVersion" = EXCLUDED."softwareVersion",
                        updated = NOW()
                    """
                )

                conn.commit()
        print(f"  ✅ Successfully upserted {len(results)} results")
        return True
    except psycopg.Error as e:
        print(f"  ⚠️  Database error upserting results: {str(e)}")
        return False
    except Exception as e:
        print(f"  ⚠️  Unexpected error upserting results: {str(e)}")
        return False


def submit_estimated_scores(input_dir: Path) -> None:
    """
    Submit estimated scores from NDJSON files to the database.

    Args:
        input_dir: Directory containing NDJSON files with estimated scores
    """
    print("🚀 Starting estimated score submission...")
    print(f"💾 Database: {DATABASE_URL[:50]}..." if DATABASE_URL else "💾 Database: (using config)")

    # Load estimated records
    print("\n📖 Loading estimated records...")
    estimated_records = load_estimated_records(input_dir)

    if not estimated_records:
        print("\n✅ No estimated records to submit")
        return

    # Format records
    print("\n📝 Formatting records...")
    formatted_results = []
    errors = []

    format_start_time = time.time()
    last_print_time = format_start_time
    for i, record in enumerate(
        tqdm(estimated_records, desc="  Formatting", unit="record")
    ):
        try:
            formatted_result = format_result(record)
            formatted_results.append(formatted_result)
        except Exception as e:
            errors.append((record, str(e)))

        # Print progress with ETA every 5 seconds
        current_time = time.time()
        if current_time - last_print_time >= 5.0:
            elapsed = current_time - format_start_time
            completed = i + 1
            eta = calculate_eta(elapsed, completed, len(estimated_records))
            print(
                f"  Formatting: {completed:,}/{len(estimated_records):,} records (ETA: {eta})"
            )
            last_print_time = current_time

    if errors:
        print(f"\n⚠️  Encountered {len(errors)} errors while formatting:")
        for record, error in errors[:10]:  # Show first 10 errors
            print(f"    - {error}: {record.get('doi', 'unknown')}")
        if len(errors) > 10:
            print(f"    ... and {len(errors) - 10} more errors")

    if not formatted_results:
        print("\n❌ No valid results to submit after formatting")
        return

    # Upsert results to database in batches using multithreading
    print(
        f"\n💾 Processing {len(formatted_results):,} results in batches of {BATCH_SIZE} using {NUM_THREADS} threads..."
    )

    total_successful_batches = 0
    total_failed_batches = 0

    # Create batches
    batches = [
        formatted_results[i : i + BATCH_SIZE]
        for i in range(0, len(formatted_results), BATCH_SIZE)
    ]

    # Create progress bar for all results
    post_start_time = time.time()
    last_print_time = post_start_time
    with tqdm(
        total=len(formatted_results),
        desc="  Upserting scores",
        unit="record",
    ) as pbar:
        # Use ThreadPoolExecutor for parallel batch processing
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            # Submit all batches to the thread pool
            future_to_batch = {
                executor.submit(upsert_results_to_db, batch): batch for batch in batches
            }

            # Process completed futures as they finish
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    success = future.result()
                    if success:
                        total_successful_batches += 1
                    else:
                        total_failed_batches += 1
                except Exception as e:
                    print(f"\n⚠️  Exception in batch processing: {e}")
                    total_failed_batches += 1

                # Update progress bar by batch size
                pbar.update(len(batch))

                # Print progress with ETA every 5 seconds
                current_time = time.time()
                if current_time - last_print_time >= 5.0:
                    elapsed = current_time - post_start_time
                    completed = pbar.n
                    eta = calculate_eta(elapsed, completed, len(formatted_results))
                    print(
                        f"  Upserting: {completed:,}/{len(formatted_results):,} records (ETA: {eta})"
                    )
                    last_print_time = current_time

    # Summary
    print("\n📊 Submission Summary:")
    print(f"    - Total estimated records loaded: {len(estimated_records):,}")
    print(f"    - Successfully formatted: {len(formatted_results):,}")
    print(f"    - Formatting errors: {len(errors):,}")
    print(f"    - Successful batches: {total_successful_batches}")
    print(f"    - Failed batches: {total_failed_batches}")

    print("\n✅ All results processed successfully!")


def main() -> None:
    """Main function to submit estimated scores."""
    print("🚀 Starting Fuji estimated score submission...")

    # Get OS-agnostic paths
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    input_dir = downloads_dir / "database" / "fuji-score-estimated"

    print(f"Input directory: {input_dir}")

    if not input_dir.exists():
        raise FileNotFoundError(
            f"Input directory not found: {input_dir}. "
            f"Please run estimate-fuji-scores.py first to create the estimated files."
        )

    # Submit estimated scores
    try:
        submit_estimated_scores(input_dir)
        print("\n✅ Submission completed successfully!")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
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
