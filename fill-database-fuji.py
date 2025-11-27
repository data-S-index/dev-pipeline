"""Fill database with Fuji scores by querying datasets without scores and calling the FUJI API."""

import base64
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, Tuple

import psycopg
from psycopg_pool import ConnectionPool
import requests

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

# Batch size for processing - one query per FUJI instance at a time
BATCH_SIZE = INSTANCE_COUNT

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
DB_RETRY_DELAY = 1  # seconds for database retries
MAX_DB_RETRIES = 5  # Maximum retries for database operations

# Connection pool configuration
# Lowered to allow multiple instances to run concurrently without exhausting DB connections
CONNECTION_POOL_MIN = 2
CONNECTION_POOL_MAX = 10


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


def update_database_with_retry(
    connection_pool: ConnectionPool,
    dataset_id: int,
    score: Optional[float],
    evaluation_date: datetime,
) -> Tuple[bool, Optional[str]]:
    """
    Update database with retry logic for connection failures.

    Args:
        connection_pool: Database connection pool
        dataset_id: Dataset ID to update
        score: Fuji score to set
        evaluation_date: Evaluation date to set

    Returns:
        Tuple of (success, error_message)
    """
    for db_attempt in range(MAX_DB_RETRIES):
        try:
            with connection_pool.connection() as conn:
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
            return (True, None)
        except psycopg.OperationalError as e:
            # Connection errors - retry with backoff
            if db_attempt < MAX_DB_RETRIES - 1:
                time.sleep(DB_RETRY_DELAY * (db_attempt + 1))
                continue
            return (False, f"Database connection error: {str(e)}")
        except psycopg.Error as e:
            # Other database errors - don't retry
            return (False, f"Database error: {str(e)}")
        except Exception as e:
            return (False, f"Unexpected database error: {str(e)}")

    return (False, "Max database retries exceeded")


def score_row(
    row: Tuple[int, str], endpoint: str, connection_pool: ConnectionPool
) -> Tuple[int, bool, Optional[str]]:
    """
    Score a single dataset row by calling the FUJI API.

    Args:
        row: Tuple of (dataset_id, doi)
        endpoint: FUJI API endpoint URL
        connection_pool: Database connection pool

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

            # Update database with retry logic
            success, error = update_database_with_retry(
                connection_pool, dataset_id, score, evaluation_date
            )

            if success:
                return (dataset_id, True, None)
            else:
                # If database update fails, retry the whole operation
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return (dataset_id, False, error)

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
                continue
            return (dataset_id, False, f"API error: {str(e)}")
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return (dataset_id, False, f"Unexpected error: {str(e)}")

    return (dataset_id, False, "Max retries exceeded")


def get_datasets_without_scores(conn: psycopg.Connection, limit: int) -> list:
    """
    Query for datasets that don't have a score (score IS NULL).

    Note: We intentionally do NOT use FOR UPDATE SKIP LOCKED here because:
    - This script runs as a single instance (no multi-process concurrency)
    - Row-level locking would cause deadlocks: fetch_conn would hold locks,
      but worker threads use different connections from the pool to update rows,
      causing them to block waiting for locks that are never released until
      the end of the batch
    - We already protect against duplicate processing by only selecting rows
      where score IS NULL, and setting score once processed

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

    # Create connection pool
    print("\n🔌 Creating database connection pool...")
    try:
        # Create connection pool with keepalive settings
        connection_pool = ConnectionPool(
            MINI_DATABASE_URL,
            min_size=CONNECTION_POOL_MIN,
            max_size=CONNECTION_POOL_MAX,
            kwargs={
                "autocommit": False,
                "connect_timeout": 10,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
        )
        print(
            f"  ✅ Connection pool created (min={CONNECTION_POOL_MIN}, max={CONNECTION_POOL_MAX})"
        )

        # Test connection
        with connection_pool.connection() as conn:
            print("  ✅ Database connection test successful")

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
                connection_pool.close()
                return

            print(f"  📊 Found {total_pending:,} datasets without Fuji scores")

        # Process in batches using a single persistent connection for fetching
        # and the connection pool for worker thread updates
        total_processed = 0
        total_succeeded = 0
        total_failed = 0

        # Use a single connection for batch fetching (more efficient than getting from pool each time)
        with connection_pool.connection() as fetch_conn:
            last_progress_print = 0
            progress_print_interval = 100  # Print progress every 100 datasets

            while True:
                # Get next batch of datasets using the persistent connection
                rows = get_datasets_without_scores(fetch_conn, BATCH_SIZE)

                if not rows:
                    break

                # Process batch with thread pool
                with ThreadPoolExecutor(max_workers=len(FUJI_ENDPOINTS)) as executor:
                    futures = {
                        executor.submit(
                            score_row,
                            row,
                            FUJI_ENDPOINTS[i % len(FUJI_ENDPOINTS)],
                            connection_pool,
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
                            print(f"  ⚠️  Failed dataset {dataset_id}: {error}")

                        # Print progress periodically
                        if (
                            total_processed - last_progress_print
                            >= progress_print_interval
                        ):
                            progress_pct = (
                                (total_processed / total_pending * 100)
                                if total_pending > 0
                                else 0
                            )
                            print(
                                f"  📊 Progress: {total_processed:,}/{total_pending:,} "
                                f"({progress_pct:.1f}%) - "
                                f"Succeeded: {total_succeeded:,}, Failed: {total_failed:,}"
                            )
                            last_progress_print = total_processed

        print("\n✅ Fuji score database fill completed!")
        print("📊 Summary:")
        print(f"  - Total processed: {total_processed:,}")
        print(f"  - Succeeded: {total_succeeded:,}")
        print(f"  - Failed: {total_failed:,}")

        # Close connection pool
        connection_pool.close()

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
