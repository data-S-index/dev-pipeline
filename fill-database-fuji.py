"""Fill database with Fuji scores by querying datasets without scores and calling the FUJI API."""

import base64
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, Tuple

import psycopg
import requests
from tqdm import tqdm

from config import MINI_DATABASE_URL


"""
CREATE TABLE "Dataset" (
    id BIGSERIAL PRIMARY KEY,
    doi TEXT,
    score FLOAT DEFAULT NULL,
    evaluationdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dataset_doi ON "Dataset" (doi);
"""

# Generate a list of ports from 54001 to 54010
MIN_PORT = 54001
INSTANCE_COUNT = 30
PORTS = list(range(MIN_PORT, MIN_PORT + INSTANCE_COUNT))

print(PORTS)

FUJI_ENDPOINTS = [f"http://localhost:{port}/fuji/api/v1/evaluate" for port in PORTS]

# Default credentials for the FUJI API
USERNAME = "marvel"
PASSWORD = "wonderwoman"

BASIC_AUTH = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("ascii")).decode("ascii")
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Basic {BASIC_AUTH}",
    "Content-Type": "application/json",
}

# Batch size for processing
BATCH_SIZE = 100

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def get_fuji_score_and_date(
    results: dict,
) -> Tuple[Optional[float], Optional[datetime]]:
    """
    Extract score and evaluation date from FUJI API response.

    Args:
        results: FUJI API response dictionary

    Returns:
        Tuple of (score, evaluation_date)
        Score is 0.0-100.0, or None if extraction fails
        Evaluation date is a datetime object, or None if extraction fails
    """
    if not results or not isinstance(results, dict):
        return None, None

    # Extract FAIR score from results["summary"]["score_percent"]["FAIR"]
    score = None
    try:
        summary = results.get("summary", {})
        score_percent = summary.get("score_percent", {})
        score = score_percent.get("FAIR")
        if score is not None:
            score = float(score)
    except (KeyError, ValueError, TypeError):
        score = None

    # Extract evaluation date from results["end_timestamp"]
    evaluation_date = None
    try:
        end_timestamp = results.get("end_timestamp")
        if end_timestamp:
            # Parse ISO format timestamp
            if isinstance(end_timestamp, str):
                # Handle various timestamp formats
                if end_timestamp.endswith("Z"):
                    end_timestamp = end_timestamp[:-1] + "+00:00"
                evaluation_date = datetime.fromisoformat(
                    end_timestamp.replace("Z", "+00:00")
                )
            elif isinstance(end_timestamp, (int, float)):
                # Handle Unix timestamp
                evaluation_date = datetime.fromtimestamp(end_timestamp)
    except (KeyError, ValueError, TypeError):
        evaluation_date = None

    return score, evaluation_date


def score_row(
    row: Tuple[int, str], endpoint: str, database_url: str
) -> Tuple[int, bool, Optional[str]]:
    """
    Score a single dataset row by calling the FUJI API.

    Args:
        row: Tuple of (dataset_id, doi)
        endpoint: FUJI API endpoint URL
        database_url: Database connection string

    Returns:
        Tuple of (dataset_id, success, error_message)
    """
    dataset_id, doi = row

   

    # Retry logic for API calls
    for attempt in range(MAX_RETRIES):
        try:
            payload = {
                "object_identifier": doi,
                "test_debug": True,
                "metadata_service_endpoint": "http://ws.pangaea.de/oai/provider",
                "metadata_service_type": "oai_pmh",
                "use_datacite": True,
                "use_github": False,
                "metric_version": "metrics_v0.5",
            }
            r = requests.post(endpoint, json=payload, headers=HEADERS, timeout=60)
            r.raise_for_status()
            data = r.json()

            # Extract score and evaluation date from API response
            score, evaluation_date = get_fuji_score_and_date(data)

            # Use current time if evaluation_date is not available
            if evaluation_date is None:
                evaluation_date = datetime.now()

            # Update database with new connection (thread-safe)
            with psycopg.connect(database_url, autocommit=False) as conn:
                with conn.cursor() as cur:
                    # Update the Dataset table directly
                    cur.execute(
                        """
                        UPDATE "Dataset"
                        SET score = %s, "evaluationdate" = %s
                        WHERE id = %s
                        """,
                        (score, evaluation_date, dataset_id),
                    )
                conn.commit()

            return (dataset_id, True, None)

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
                continue
            return (dataset_id, False, f"API error: {str(e)}")
        except psycopg.Error as e:
            return (dataset_id, False, f"Database error: {str(e)}")
        except Exception as e:
            return (dataset_id, False, f"Unexpected error: {str(e)}")

    return (dataset_id, False, "Max retries exceeded")


def get_datasets_without_scores(conn: psycopg.Connection, limit: int) -> list:
    """
    Query for datasets that don't have a score (score IS NULL).

    Returns:
        List of tuples (dataset_id, doi)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, doi
            FROM "Dataset"
            WHERE score IS NULL
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (limit,),
        )
        return cur.fetchall()


def main() -> None:
    """Main function to fill database with Fuji scores."""
    print("🚀 Starting Fuji score database fill process...")
    print(f"📡 Using {len(FUJI_ENDPOINTS)} FUJI API endpoints")

    if not MINI_DATABASE_URL:
        raise ValueError("MINI_DATABASE_URL not set in environment or .env file")

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(MINI_DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            # Check how many datasets need scoring
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM "Dataset"
                    WHERE score IS NULL
                    """
                )
                total_pending = cur.fetchone()[0]

            if total_pending == 0:
                print("\n✅ All datasets already have Fuji scores!")
                return

            print(f"  📊 Found {total_pending:,} datasets without Fuji scores")

            # Process in batches
            total_processed = 0
            total_succeeded = 0
            total_failed = 0

            with tqdm(total=total_pending, desc="Processing", unit="dataset") as pbar:
                while True:
                    # Get next batch of datasets
                    rows = get_datasets_without_scores(conn, BATCH_SIZE)

                    if not rows:
                        break

                    # Process batch with thread pool
                    with ThreadPoolExecutor(max_workers=len(FUJI_ENDPOINTS)) as pool:
                        futures = {
                            pool.submit(
                                score_row,
                                row,
                                FUJI_ENDPOINTS[i % len(FUJI_ENDPOINTS)],
                                MINI_DATABASE_URL,
                            ): row
                            for i, row in enumerate(rows)
                        }

                        # Process completed futures
                        for future in as_completed(futures):
                            dataset_id, success, error = future.result()
                            total_processed += 1

                            if success:
                                total_succeeded += 1
                            else:
                                total_failed += 1
                                tqdm.write(f"  ⚠️  Failed dataset {dataset_id}: {error}")

                            pbar.update(1)

            print("\n✅ Fuji score database fill completed!")
            print("📊 Summary:")
            print(f"  - Total processed: {total_processed:,}")
            print(f"  - Succeeded: {total_succeeded:,}")
            print(f"  - Failed: {total_failed:,}")

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
