"""Sample DOI identifiers per publisher to test FUJI score extrapolation.

Reads the NDJSON files produced by pull-db-for-d-index.py, finds every DOI
identifier, and groups them by publisherId. For each publisher it keeps a
random sample of up to SAMPLE_SIZE identifiers (reservoir sampling, so the
whole 21M+ record file set only needs a single pass and a small amount of
memory). Records with an unknown or missing publisherId are skipped.

The idea: if FUJI scores turn out identical for every sampled dataset under
a publisher, the score can be extrapolated to the rest of that publisher's
datasets instead of running FUJI on all ~70M datasets.

Output: a single JSON file in fuji-extrapolation-test/ mapping each
publisherId to its count, lowest score info, and sampled identifiers, and the
sampled dataset IDs are loaded into the FujiJob table so a FUJI worker can
pick them up.
"""

import json
import random
from pathlib import Path
from typing import Dict, List, Tuple

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Max number of randomly sampled identifiers kept per publisher.
SAMPLE_SIZE = 100

# Only datasets with an existing FUJI score at or below this are eligible for sampling.
SCORE_THRESHOLD = 14


def reservoir_sample_by_publisher(
    input_dir: Path,
) -> Tuple[
    Dict[str, int],
    Dict[str, int],
    Dict[str, List[Tuple[int, str]]],
    Dict[str, Tuple[float, int]],
]:
    """Stream every NDJSON file in input_dir and reservoir-sample low-scoring DOI identifiers by publisher.

    Only datasets with an existing FUJI score <= SCORE_THRESHOLD are eligible for the
    sample (up to SAMPLE_SIZE per publisher, or all of them if fewer than SAMPLE_SIZE exist).
    Also tracks, per publisher, the lowest FUJI score seen and how many records have
    that exact score (a quick signal for whether scores look uniform within a publisher).
    Records with a missing or "unknown" publisherId are skipped.

    Returns:
        counts: publisherId -> total number of DOI identifiers seen for that publisher
        low_score_counts: publisherId -> total number of DOI identifiers with score <= SCORE_THRESHOLD
        samples: publisherId -> up to SAMPLE_SIZE (id, identifier) tuples, uniformly sampled from the low-score subset
        min_scores: publisherId -> (lowest fuji score seen, number of records with that score)
    """
    ndjson_files = sorted(input_dir.glob("*.ndjson"))
    if not ndjson_files:
        raise FileNotFoundError(f"No NDJSON files found in {input_dir}")

    print(f"Found {len(ndjson_files):,} NDJSON files to scan")

    counts: Dict[str, int] = {}
    low_score_counts: Dict[str, int] = {}
    samples: Dict[str, List[Tuple[int, str]]] = {}
    min_scores: Dict[str, Tuple[float, int]] = {}

    total_records = 0
    total_doi_records = 0

    for ndjson_file in tqdm(ndjson_files, desc="Scanning files", unit="file"):
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                total_records += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    tqdm.write(f"  Skipping bad JSON in {ndjson_file.name}: {e}")
                    continue

                if record.get("identifierType") != "doi":
                    continue

                identifier = record.get("identifier")
                if not identifier:
                    continue

                publisher_id = record.get("publisherId")
                if not publisher_id or publisher_id == "unknown":
                    continue

                dataset_id = record.get("id")
                if dataset_id is None:
                    continue

                total_doi_records += 1
                counts[publisher_id] = counts.get(publisher_id, 0) + 1

                fuji = record.get("fuji")
                score = fuji.get("score") if fuji else None
                if score is None:
                    continue

                current = min_scores.get(publisher_id)
                if current is None or score < current[0]:
                    min_scores[publisher_id] = (score, 1)
                elif score == current[0]:
                    min_scores[publisher_id] = (current[0], current[1] + 1)

                if score <= SCORE_THRESHOLD:
                    low_count = low_score_counts.get(publisher_id, 0)
                    reservoir = samples.setdefault(publisher_id, [])

                    if low_count < SAMPLE_SIZE:
                        reservoir.append((dataset_id, identifier))
                    else:
                        j = random.randint(0, low_count)
                        if j < SAMPLE_SIZE:
                            reservoir[j] = (dataset_id, identifier)

                    low_score_counts[publisher_id] = low_count + 1

    print("\nScan summary:")
    print(f"  - Total records scanned: {total_records:,}")
    print(f"  - DOI records found: {total_doi_records:,}")
    print(f"  - Unique publishers: {len(counts):,}")
    print(f"  - DOI records with score <= {SCORE_THRESHOLD}: {sum(low_score_counts.values()):,}")

    return counts, low_score_counts, samples, min_scores


OUTPUT_FILE_NAME = "fuji-extrapolation-candidates.json"


def write_outputs(
    counts: Dict[str, int],
    low_score_counts: Dict[str, int],
    samples: Dict[str, List[Tuple[int, str]]],
    min_scores: Dict[str, Tuple[float, int]],
    output_dir: Path,
) -> Path:
    """Write a single JSON file mapping each publisherId to its count, lowest score info, and samples."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_FILE_NAME

    result = {
        publisher_id: {
            "totalCount": count,
            "lowScoreCount": low_score_counts.get(publisher_id, 0),
            "sampleSize": len(samples.get(publisher_id, [])),
            "lowestScore": min_scores[publisher_id][0]
            if publisher_id in min_scores
            else None,
            "lowestScoreCount": min_scores[publisher_id][1]
            if publisher_id in min_scores
            else 0,
            "samples": [
                {"id": dataset_id, "identifier": identifier}
                for dataset_id, identifier in samples.get(publisher_id, [])
            ],
        }
        for publisher_id, count in counts.items()
    }
    sorted_result = dict(
        sorted(result.items(), key=lambda x: x[1]["totalCount"], reverse=True)
    )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(sorted_result, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(sorted_result):,} publishers to {output_path}")
    return output_path


def load_sample_dataset_ids(output_path: Path) -> List[int]:
    """Read back the sampled dataset IDs from the combined candidates JSON file."""
    with open(output_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    dataset_ids: List[int] = []
    for publisher_info in data.values():
        for sample in publisher_info.get("samples", []):
            dataset_id = sample.get("id")
            if dataset_id is not None:
                dataset_ids.append(dataset_id)
    return dataset_ids


def insert_into_fuji_job(conn: psycopg.Connection, dataset_ids: List[int]) -> int:
    """Insert sampled dataset IDs into FujiJob, skipping ones already queued."""
    print(f"\nLoading {len(dataset_ids):,} sampled dataset IDs into FujiJob...")

    # Shuffle so consecutive FujiJob rows aren't clustered by publisher/id range,
    # giving workers that pull jobs in order a mixed spread of publishers.
    shuffled_ids = dataset_ids.copy()
    random.shuffle(shuffled_ids)

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE fuji_job_staging ("datasetId" INT) ON COMMIT DROP
            """
        )
        with cur.copy('COPY fuji_job_staging ("datasetId") FROM STDIN') as copy:
            for dataset_id in shuffled_ids:
                copy.write_row((dataset_id,))

        cur.execute(
            """
            INSERT INTO "FujiJob" ("datasetId")
            SELECT "datasetId" FROM fuji_job_staging
            ON CONFLICT ("datasetId") DO NOTHING
            """
        )
        inserted = cur.rowcount
        conn.commit()

    print(f"  Inserted {inserted:,} new FujiJob rows (skipped existing duplicates)")
    return inserted


def main() -> None:
    """Sample DOI identifiers per publisher and queue them for FUJI scoring."""
    print("Starting FUJI extrapolation candidate sampling...")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set in environment or .env file")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    input_dir = downloads_dir / "pulled-database" / "datasets"
    output_dir = downloads_dir / "pulled-database" / "fuji-extrapolation-test"

    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")

    if not input_dir.exists():
        raise FileNotFoundError(
            f"Directory not found: {input_dir}. Please run pull-db-for-d-index.py first."
        )

    print("\nScanning NDJSON files for DOI identifiers...")
    counts, low_score_counts, samples, min_scores = reservoir_sample_by_publisher(
        input_dir
    )

    print("\nWriting candidates file...")
    output_path = write_outputs(
        counts, low_score_counts, samples, min_scores, output_dir
    )

    print("\nReloading sampled dataset IDs from disk...")
    dataset_ids = load_sample_dataset_ids(output_path)
    print(f"  Loaded {len(dataset_ids):,} sampled dataset IDs")

    print("\nConnecting to database...")
    with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
        inserted = insert_into_fuji_job(conn, dataset_ids)

    print("\nDone!")
    print("Summary:")
    print(f"  - Unique publishers: {len(counts):,}")
    print(f"  - Sampled dataset IDs: {len(dataset_ids):,}")
    print(f"  - New FujiJob rows inserted: {inserted:,}")
    print(f"Output file is available at: {output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
