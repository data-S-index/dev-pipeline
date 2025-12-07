"""Process Fuji scores by fetching jobs from API and calling the FUJI API."""

import base64
import os
import random
import time
import threading
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import requests

from config import INSTANCE_COUNT

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
        f"http://fuji{i + 1}:1071/fuji/api/v1/evaluate" for i in range(INSTANCE_COUNT)
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

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

DOMAIN = "https://scholardata.io"


# API endpoints for fetching jobs and posting results
JOBS_API_URL = f"{DOMAIN}/api/fuji/jobs"
PRIORITY_JOBS_API_URL = f"{DOMAIN}/api/fuji/jobs/priority"
RESULTS_API_URL = f"{DOMAIN}/api/fuji/jobs/results"


def random_sleep(min_seconds: float = 5.0, max_seconds: float = 10.0) -> None:
    """
    Sleep for a random duration between min_seconds and max_seconds.

    Args:
        min_seconds: Minimum sleep duration (default: 5.0)
        max_seconds: Maximum sleep duration (default: 10.0)
    """
    sleep_time = random.uniform(min_seconds, max_seconds)
    time.sleep(sleep_time)


# Global shutdown event for graceful thread termination
shutdown_event = threading.Event()


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
    print("  🔍 Extracting score and date from FUJI response...")
    if not results or not isinstance(results, dict):
        print(f"  ⚠️  Invalid results format: {type(results)}")
        return None, None

    # Extract FAIR score from results["summary"]["score_percent"]["FAIR"]
    score = None
    try:
        summary = results.get("summary", {})
        print(
            f"  📋 Summary keys: {list(summary.keys()) if isinstance(summary, dict) else 'not a dict'}"
        )
        score_percent = summary.get("score_percent", {})
        print(f"  📊 Score percent: {score_percent}")
        score = score_percent.get("FAIR")
        print(f"  🎯 Raw FAIR score: {score}")
        if score is not None:
            score = float(score)
            print(f"  ✅ Extracted score: {score}")
    except (KeyError, ValueError, TypeError) as e:
        print(f"  ⚠️  Error extracting score: {e}")
        score = None

    # Extract evaluation date from results["end_timestamp"]
    evaluation_date = None
    try:
        end_timestamp = results.get("end_timestamp")
        print(f"  📅 Raw end_timestamp: {end_timestamp} (type: {type(end_timestamp)})")
        if end_timestamp:
            # Parse ISO format timestamp
            if isinstance(end_timestamp, str):
                # Handle various timestamp formats
                if end_timestamp.endswith("Z"):
                    end_timestamp = f"{end_timestamp[:-1]}+00:00"
                evaluation_date = datetime.fromisoformat(
                    end_timestamp.replace("Z", "+00:00")
                )
                print(f"  ✅ Parsed evaluation date: {evaluation_date}")
            elif isinstance(end_timestamp, (int, float)):
                # Handle Unix timestamp
                evaluation_date = datetime.fromtimestamp(end_timestamp)
                print(f"  ✅ Parsed evaluation date from timestamp: {evaluation_date}")
    except (KeyError, ValueError, TypeError) as e:
        print(f"  ⚠️  Error extracting evaluation date: {e}")
        evaluation_date = None

    return score, evaluation_date


def fetch_jobs_from_api() -> List[Dict[str, Any]]:
    """
    Fetch jobs from the API endpoints (priority first, then regular).

    Returns:
        List of job dictionaries with 'id' and 'identifier' keys
    """
    all_jobs = []

    # Fetch priority jobs first
    print(f"  📥 Fetching priority jobs from {PRIORITY_JOBS_API_URL}...")
    try:
        response = requests.get(PRIORITY_JOBS_API_URL, timeout=30.0)
        response.raise_for_status()
        priority_jobs = response.json()
        print(
            f"  ✅ Priority jobs response: {len(priority_jobs) if isinstance(priority_jobs, list) else 'not a list'}"
        )
        if isinstance(priority_jobs, list):
            all_jobs.extend(priority_jobs)
            print(f"  📋 Added {len(priority_jobs)} priority jobs")
    except Exception as e:
        print(f"  ⚠️  Error fetching priority jobs: {e}")

    # Fetch regular jobs
    print(f"  📥 Fetching regular jobs from {JOBS_API_URL}...")
    try:
        response = requests.get(JOBS_API_URL, timeout=120.0)
        response.raise_for_status()
        regular_jobs = response.json()
        print(
            f"  ✅ Regular jobs response: {len(regular_jobs) if isinstance(regular_jobs, list) else 'not a list'}"
        )
        if isinstance(regular_jobs, list):
            all_jobs.extend(regular_jobs)
            print(f"  📋 Added {len(regular_jobs)} regular jobs")
    except Exception as e:
        print(f"  ⚠️  Error fetching regular jobs: {e}")

    # remove any jobs that don't have an identifierType of 'doi'
    all_jobs = [job for job in all_jobs if job.get("identifierType") == "doi"]

    print(f"  📊 Total jobs fetched: {len(all_jobs)}")
    return all_jobs


def score_job(
    job: Dict[str, Any],
    endpoint: str,
) -> Optional[Dict[str, Any]]:
    """
    Score a single job by calling the FUJI API.

    Args:
        job: Dictionary with 'id' and 'identifier' keys
        endpoint: FUJI API endpoint URL

    Returns:
        Result dictionary matching the schema, or None if failed
    """
    job_id = job.get("id")
    identifier = job.get("identifier")

    print(f"  🎯 Scoring job {job_id} with identifier: {identifier}")
    print(f"  🔗 Using endpoint: {endpoint}")

    if not job_id or not identifier:
        print(f"  ⚠️  Invalid job: missing id or identifier. Job: {job}")
        return None

    # Retry logic for API calls
    for attempt in range(MAX_RETRIES):
        try:
            print(f"  🔄 Attempt {attempt + 1}/{MAX_RETRIES} for job {job_id}")
            # Sleep before making FUJI API request
            random_sleep()

            payload = {
                "object_identifier": identifier,
                "metric_version": "metrics_v0.8",
            }
            print(f"  📤 Sending payload: {payload}")
            r = requests.post(endpoint, json=payload, headers=HEADERS, timeout=60.0)
            print(f"  📥 Response status: {r.status_code}")
            r.raise_for_status()
            data = r.json()

            print(
                f"  📊 Response data keys: {list(data.keys()) if isinstance(data, dict) else 'not a dict'}"
            )
            print(f"  📄 Response data (first 500 chars): {str(data)[:500]}")

            # Extract score and evaluation date from API response
            score, evaluation_date = get_fuji_score_and_date(data)

            # Only return result if we successfully extracted a score
            # Skip jobs where score extraction failed
            if score is None:
                print(f"  ⚠️  Failed to extract score for job {job_id}, skipping")
                return None

            # Use current time if evaluation_date is not available
            if evaluation_date is None:
                evaluation_date = datetime.now()
                print(f"  ⏰ Using current time as evaluation date: {evaluation_date}")

            # Extract metric version and software version from FUJI response
            metric_version = data.get("metric_version", "metrics_v0.5")
            software_version = data.get("software_version", "unknown")
            print(
                f"  📦 Metric version: {metric_version}, Software version: {software_version}"
            )

            # If not directly available, try to extract from other fields
            if metric_version == "unknown":
                metric_version = payload.get("metric_version", "metrics_v0.5")
                print(f"  🔄 Using default metric version: {metric_version}")

            # Return result in the specified schema format
            result = {
                "datasetId": job_id,
                "score": float(score),
                "evaluationDate": evaluation_date.isoformat(),
                "metricVersion": str(metric_version),
                "softwareVersion": str(software_version),
            }
            print(f"  ✅ Successfully scored job {job_id}: {result}")
            return result

        except requests.RequestException as e:
            print(
                f"  ⚠️  Request error for job {job_id} (attempt {attempt + 1}): {str(e)}"
            )
            if attempt < MAX_RETRIES - 1:
                backoff_time = RETRY_DELAY * (attempt + 1)
                print(f"  ⏳ Waiting {backoff_time}s before retry...")
                time.sleep(backoff_time)  # Exponential backoff
                continue
            print(
                f"  ❌ API error for job {job_id} after {MAX_RETRIES} attempts: {str(e)}"
            )
            return None
        except Exception as e:
            print(
                f"  ⚠️  Unexpected error for job {job_id} (attempt {attempt + 1}): {str(e)}"
            )
            if attempt < MAX_RETRIES - 1:
                backoff_time = RETRY_DELAY * (attempt + 1)
                print(f"  ⏳ Waiting {backoff_time}s before retry...")
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            print(
                f"  ❌ Unexpected error for job {job_id} after {MAX_RETRIES} attempts: {str(e)}"
            )
            return None

    print(f"  ❌ Failed to score job {job_id} after all retries")
    return None


def is_valid_result(result: Dict[str, Any]) -> bool:
    """
    Validate that a result has all required fields and valid data.

    Args:
        result: Result dictionary to validate

    Returns:
        True if valid, False otherwise
    """
    if not result or not isinstance(result, dict):
        return False

    required_fields = [
        "datasetId",
        "score",
        "evaluationDate",
        "metricVersion",
        "softwareVersion",
    ]
    for field in required_fields:
        if field not in result:
            return False

    # Validate data types
    if not isinstance(result.get("datasetId"), (int, float)):
        return False
    if not isinstance(result.get("score"), (int, float)):
        return False
    if not isinstance(result.get("evaluationDate"), str):
        return False
    if not isinstance(result.get("metricVersion"), str):
        return False
    if not isinstance(result.get("softwareVersion"), str):
        return False

    return True


def post_results_to_api(results: List[Dict[str, Any]]) -> bool:
    """
    POST results to the API endpoint.

    Args:
        results: List of result dictionaries

    Returns:
        True if successful, False otherwise
    """
    if not results:
        print("  ℹ️  No results to post")
        return True

    payload = {"results": results}
    print(f"  📤 Posting {len(results)} results to {RESULTS_API_URL}")
    print(
        f"  📋 Results summary: {[{'datasetId': r.get('datasetId'), 'score': r.get('score')} for r in results[:5]]}"
    )
    if len(results) > 5:
        print(f"  ... and {len(results) - 5} more results")

    try:
        response = requests.post(
            RESULTS_API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120.0,
        )
        print(f"  📥 Response status: {response.status_code}")
        print(f"  📄 Response text: {response.text[:200]}")
        response.raise_for_status()
        print(f"  ✅ Successfully posted {len(results)} results")
        return True
    except requests.RequestException as e:
        print(f"  ⚠️  Request error posting results: {str(e)}")
        if hasattr(e, "response") and e.response is not None:
            print(f"  📄 Error response: {e.response.text[:500]}")
        return False
    except Exception as e:
        print(f"  ⚠️  Unexpected error posting results: {str(e)}")
        return False


def worker_thread(thread_id: int, endpoint: str) -> None:
    """
    Worker thread function that continuously fetches jobs, processes them, and posts results.

    Args:
        thread_id: Unique identifier for this thread
        endpoint: FUJI API endpoint URL to use for this thread
    """
    print(f"🧵 Thread {thread_id} starting...")

    while not shutdown_event.is_set():
        try:
            # Fetch jobs from API
            print(f"🧵 Thread {thread_id}: 📥 Fetching jobs...")
            jobs = fetch_jobs_from_api()

            if not jobs:
                print(f"🧵 Thread {thread_id}: ✅ No jobs found, waiting...")
                # Check shutdown event during wait
                if shutdown_event.wait(timeout=10):
                    break
                continue

            # Sleep before processing jobs
            random_sleep()

            print(f"🧵 Thread {thread_id}: 📊 Processing {len(jobs):,} jobs...")

            # Process all jobs
            results = []
            print(f"🧵 Thread {thread_id}: 🔄 Starting to process {len(jobs)} jobs...")

            for idx, job in enumerate(jobs, 1):
                # Check for shutdown signal
                if shutdown_event.is_set():
                    print(f"🧵 Thread {thread_id}: ⚠️  Shutdown requested, stopping...")
                    break

                print(
                    f"🧵 Thread {thread_id}: 📝 Processing job {idx}/{len(jobs)}: {job}"
                )
                result = score_job(job, endpoint)
                if result is not None:
                    if is_valid_result(result):
                        print(
                            f"🧵 Thread {thread_id}: ✅ Valid result for job {job.get('id')}"
                        )
                        results.append(result)
                    else:
                        print(
                            f"🧵 Thread {thread_id}: ⚠️  Invalid result for job {job.get('id')}: {result}"
                        )
                else:
                    print(
                        f"🧵 Thread {thread_id}: ⚠️  No result returned for job {job.get('id')}"
                    )

            # Post results to API after processing all jobs
            if results:
                print(f"🧵 Thread {thread_id}: 📤 Posting {len(results)} results...")
                success = post_results_to_api(results)
                if success:
                    print(
                        f"🧵 Thread {thread_id}: ✅ Successfully posted {len(results)} results"
                    )
                else:
                    print(
                        f"🧵 Thread {thread_id}: ❌ Failed to post {len(results)} results"
                    )
            else:
                print(f"🧵 Thread {thread_id}: ℹ️  No results to post")

            print(
                f"🧵 Thread {thread_id}: ✅ Completed batch (processed {len(jobs)} jobs, got {len(results)} results)"
            )

            # Sleep before fetching next batch of jobs
            random_sleep()

        except Exception as e:
            if shutdown_event.is_set():
                print(f"🧵 Thread {thread_id}: ⚠️  Shutdown requested")
                break
            print(f"🧵 Thread {thread_id}: ❌ Error: {e}")
            print(f"🧵 Thread {thread_id}: Retrying...")
            # Check shutdown event during wait
            if shutdown_event.wait(timeout=5):
                break
            continue

    print(f"🧵 Thread {thread_id}: 🛑 Stopped")


def main() -> None:
    """Main function to create and start worker threads that run continuously."""
    print("🚀 Starting Fuji score processing...")
    print(f"📡 Using {len(FUJI_ENDPOINTS)} FUJI API endpoint(s)")
    if len(FUJI_ENDPOINTS) <= 5:
        print(f"📡 Endpoints: {FUJI_ENDPOINTS}")
    else:
        print(f"📡 First 3 endpoints: {FUJI_ENDPOINTS[:3]}...")
    print(f"🧵 Creating {INSTANCE_COUNT} worker thread(s)...")
    print(f"🌐 Domain: {DOMAIN}")
    print(f"📥 Jobs API: {JOBS_API_URL}")
    print(f"📥 Priority Jobs API: {PRIORITY_JOBS_API_URL}")
    print(f"📤 Results API: {RESULTS_API_URL}")
    print("💡 Program will run continuously. Press Ctrl+C to stop.\n")

    threads = []

    # Create and start threads
    for i in range(INSTANCE_COUNT):
        # Round-robin assignment of endpoints to threads
        endpoint = FUJI_ENDPOINTS[i % len(FUJI_ENDPOINTS)]
        print(f"🔧 Creating thread {i + 1} with endpoint: {endpoint}")
        thread = threading.Thread(
            target=worker_thread, args=(i + 1, endpoint), name=f"Worker-{i + 1}"
        )
        threads.append(thread)
        thread.start()
        print(f"✅ Thread {i + 1} started")

    # Wait for all threads to complete (they run indefinitely until interrupted)
    print(f"\n🔄 All {len(threads)} threads started. Waiting for completion...\n")
    try:
        for thread in threads:
            print(f"⏳ Waiting for {thread.name}...")
            thread.join()
    except KeyboardInterrupt:
        print("\n\n⚠️  Keyboard interrupt detected. Shutting down threads...")
        shutdown_event.set()  # Signal all threads to stop
        print("🛑 Shutdown signal sent to all threads")
        # Wait for threads to finish current iteration
        for thread in threads:
            print(
                f"⏳ Waiting for {thread.name} to finish gracefully (timeout: 10s)..."
            )
            thread.join(timeout=10)  # Give threads time to finish gracefully
            if thread.is_alive():
                print(f"⚠️  {thread.name} did not finish within timeout")

    print("\n✅ All threads stopped!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
