"""Fill database with Fuji scores by querying datasets without scores and calling the FUJI API."""

import asyncio
import base64
import os
import time
from datetime import datetime
from typing import Optional, Tuple

import httpx
import psycopg
from psycopg_pool import ConnectionPool

from config import MINI_DATABASE_URL, INSTANCE_COUNT


"""
CREATE TABLE "Dataset" (
    id BIGSERIAL PRIMARY KEY,
    doi TEXT,
    score FLOAT DEFAULT NULL,
    evaluationdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dataset_doi ON "Dataset" (doi);

CREATE TABLE fuji_jobs (
    id          BIGSERIAL PRIMARY KEY,
    dataset_id  BIGINT NOT NULL UNIQUE,
    doi         TEXT   NOT NULL
);

-- Pre-fill the queue:
INSERT INTO fuji_jobs (dataset_id, doi)
SELECT id, doi
FROM "Dataset"
WHERE score IS NULL
ON CONFLICT (dataset_id) DO NOTHING;
"""

# Generate a list of ports from 54001 to 54030
MIN_PORT = 54001
PORTS = list(range(MIN_PORT, MIN_PORT + INSTANCE_COUNT))

# Allow FUJI endpoints to be configured via environment variable
# Default to localhost for local development
# In Docker, set FUJI_HOST=docker to use service names (fuji1, fuji2, etc.)
FUJI_HOST = os.getenv("FUJI_HOST", "localhost")
FUJI_PORT = os.getenv(
    "FUJI_PORT", None
)  # If set, use this port; otherwise use PORTS list

if FUJI_PORT:
    # Single endpoint mode (useful when FUJI_PORT is explicitly set)
    FUJI_ENDPOINTS = [f"http://{FUJI_HOST}:{FUJI_PORT}/fuji/api/v1/evaluate"]
    INSTANCE_COUNT = 1
elif FUJI_HOST == "docker":
    # Docker mode: use service names (fuji1, fuji2, etc.) with internal port 1071
    FUJI_ENDPOINTS = [
        f"http://fuji{i+1}:1071/fuji/api/v1/evaluate" for i in range(INSTANCE_COUNT)
    ]
else:
    # Local development mode: use localhost with external ports
    FUJI_ENDPOINTS = [f"http://localhost:{port}/fuji/api/v1/evaluate" for port in PORTS]

print(f"Using {len(FUJI_ENDPOINTS)} FUJI endpoint(s)")
if len(FUJI_ENDPOINTS) <= 5:
    print(f"Endpoints: {FUJI_ENDPOINTS}")
else:
    print(f"First 3 endpoints: {FUJI_ENDPOINTS[:3]}...")

# Default credentials for the FUJI API
USERNAME = "marvel"
PASSWORD = "wonderwoman"

BASIC_AUTH = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode("ascii")).decode("ascii")
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Basic {BASIC_AUTH}",
    "Content-Type": "application/json",
}

# Batch size for processing - instance count x 3 concurrent requests
BATCH_SIZE = INSTANCE_COUNT * 3

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


async def score_row(
    row: Tuple[int, str],
    endpoint: str,
    connection_pool: ConnectionPool,
    client: httpx.AsyncClient,
) -> Tuple[int, bool, Optional[str]]:
    """
    Score a single dataset row by calling the FUJI API.

    Args:
        row: Tuple of (dataset_id, doi)
        endpoint: FUJI API endpoint URL
        connection_pool: Database connection pool
        client: Async HTTP client

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
            r = await client.post(endpoint, json=payload, headers=HEADERS, timeout=60.0)
            r.raise_for_status()
            data = r.json()

            # Extract score and evaluation date from API response
            score, evaluation_date = get_fuji_score_and_date(data)

            # Use current time if evaluation_date is not available
            if evaluation_date is None:
                evaluation_date = datetime.now()

            # Update database with retry logic (run in executor since it's sync)
            loop = asyncio.get_running_loop()
            success, error = await loop.run_in_executor(
                None,
                update_database_with_retry,
                connection_pool,
                dataset_id,
                score,
                evaluation_date,
            )

            if success:
                return (dataset_id, True, None)
            else:
                # If database update fails, retry the whole operation
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return (dataset_id, False, error)

        except httpx.RequestError as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
                continue
            return (dataset_id, False, f"API error: {str(e)}")
        except httpx.HTTPStatusError as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return (dataset_id, False, f"HTTP error: {str(e)}")
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return (dataset_id, False, f"Unexpected error: {str(e)}")

    return (dataset_id, False, "Max retries exceeded")


def get_jobs_batch(conn: psycopg.Connection, limit: int) -> list:
    """
    Claim a batch of jobs from the fuji_jobs queue using DELETE ... RETURNING.

    This atomically removes jobs from the queue and returns them, ensuring
    that each job is processed by exactly one worker across all machines.
    Postgres handles row locking internally, so no explicit locking is needed.

    Args:
        conn: Database connection
        limit: Maximum number of jobs to claim

    Returns:
        List of tuples (dataset_id, doi)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM fuji_jobs
            WHERE id IN (
                SELECT id
                FROM fuji_jobs
                ORDER BY id
                LIMIT %s
            )
            RETURNING dataset_id, doi;
            """,
            (limit,),
        )
        return cur.fetchall()


async def main() -> None:
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

            # Ensure fuji_jobs table exists
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fuji_jobs (
                        id          BIGSERIAL PRIMARY KEY,
                        dataset_id  BIGINT NOT NULL UNIQUE,
                        doi         TEXT   NOT NULL
                    );
                    """
                )
                conn.commit()
                print("  ✅ fuji_jobs table ready")

            # Pre-fill the queue if it's empty
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM fuji_jobs")
                queue_count = cur.fetchone()[0]

                if queue_count == 0:
                    print("  📥 Pre-filling fuji_jobs queue from Dataset table...")
                    cur.execute(
                        """
                        INSERT INTO fuji_jobs (dataset_id, doi)
                        SELECT id, doi
                        FROM "Dataset"
                        WHERE score IS NULL
                        ON CONFLICT (dataset_id) DO NOTHING
                        """
                    )
                    conn.commit()
                    cur.execute("SELECT COUNT(*) FROM fuji_jobs")
                    queue_count = cur.fetchone()[0]
                    print(f"  ✅ Pre-filled {queue_count:,} jobs into queue")
                else:
                    print(f"  📊 Found {queue_count:,} jobs already in queue")

            # Check how many jobs are in the queue
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM fuji_jobs")
                total_pending = cur.fetchone()[0]

            if total_pending == 0:
                print(
                    "\n✅ No jobs in queue - all datasets may already have Fuji scores!"
                )
                connection_pool.close()
                return

            print(f"  📊 Queue contains {total_pending:,} jobs to process")

        # Process in batches using a single persistent connection for fetching
        total_processed = 0
        total_succeeded = 0
        total_failed = 0
        start_time = time.time()  # Track start time for rate calculation
        last_remaining_update = 0  # Track when we last updated the remaining count
        remaining_update_interval = 1000  # Update remaining count every 1000 IDs

        # Create async HTTP client
        async with httpx.AsyncClient() as client:
            # Use a single connection for batch fetching
            with connection_pool.connection() as fetch_conn:
                last_progress_print = 0
                progress_print_interval = 100  # Print progress every 100 datasets

                while True:
                    # Claim next batch of jobs from the queue
                    rows = get_jobs_batch(fetch_conn, BATCH_SIZE)
                    fetch_conn.commit()  # Commit the DELETE transaction

                    if not rows:
                        print("  ✅ No more jobs in queue, exiting")
                        break

                    # Create tasks for all rows in the batch (round-robin endpoints)
                    tasks = [
                        score_row(
                            row,
                            FUJI_ENDPOINTS[i % len(FUJI_ENDPOINTS)],
                            connection_pool,
                            client,
                        )
                        for i, row in enumerate(rows)
                    ]

                    # Use asyncio.gather to process all tasks concurrently (like Promise.all)
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Process results
                    for result in results:
                        if isinstance(result, Exception):
                            total_processed += 1
                            total_failed += 1
                            print(f"  ⚠️  Exception: {result}")
                        else:
                            dataset_id, success, error = result
                            total_processed += 1

                            if success:
                                total_succeeded += 1
                            else:
                                total_failed += 1
                                print(f"  ⚠️  Failed dataset {dataset_id}: {error}")

                        # Update remaining count every 1000 IDs (for multi-machine scenarios)
                        if (
                            total_processed - last_remaining_update
                            >= remaining_update_interval
                        ):
                            with fetch_conn.cursor() as cur:
                                cur.execute("SELECT COUNT(*) FROM fuji_jobs")
                                total_pending = cur.fetchone()[0]
                            last_remaining_update = total_processed

                        # Print progress periodically
                        if (
                            total_processed - last_progress_print
                            >= progress_print_interval
                        ):
                            elapsed_time = time.time() - start_time
                            total_estimated = total_processed + total_pending
                            progress_pct = (
                                (total_processed / total_estimated * 100)
                                if total_estimated > 0
                                else 0
                            )
                            rate_per_second = (
                                total_processed / elapsed_time
                                if elapsed_time > 0
                                else 0
                            )
                            print(
                                f"  📊 Progress: {total_processed:,} processed, "
                                f"{total_pending:,} remaining "
                                f"({progress_pct:.1f}%) - "
                                f"Succeeded: {total_succeeded:,}, Failed: {total_failed:,} - "
                                f"Rate: {rate_per_second:.2f}/s"
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
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
