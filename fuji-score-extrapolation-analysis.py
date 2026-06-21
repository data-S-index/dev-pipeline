"""Check whether sampled FUJI scores are consistent within each publisher.

Reads the candidate samples produced by fuji-score-extrapolation-candidates.py
(dataset IDs grouped by publisherId), then compares each sampled dataset's
"before" score (the score it had when the candidates file was built, read
from the pulled-database/datasets NDJSON dump) against its "after" score
(the freshly computed FUJI score sitting in the FujiScore table once a worker
has processed the queued FujiJob rows). It reports per publisher whether all
sampled "after" scores agree. A publisher with a single consistent score
across its whole sample is a good candidate for extrapolating that score to
the rest of the publisher's datasets instead of running FUJI on them.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

CANDIDATES_FILE_NAME = "fuji-extrapolation-candidates.json"
ANALYSIS_FILE_NAME = "fuji-extrapolation-analysis.json"


def load_candidate_ids(candidates_path: Path) -> Dict[int, str]:
    """Load the candidates file and return a mapping of dataset id -> publisherId."""
    with open(candidates_path, "r", encoding="utf-8") as f:
        candidates = json.load(f)

    id_to_publisher: Dict[int, str] = {}
    for publisher_id, info in candidates.items():
        for sample in info.get("samples", []):
            dataset_id = sample.get("id")
            if dataset_id is not None:
                id_to_publisher[dataset_id] = publisher_id

    print(f"Loaded {len(id_to_publisher):,} sampled dataset IDs across {len(candidates):,} publishers")
    return candidates, id_to_publisher


def collect_before_scores(
    datasets_dir: Path, id_to_publisher: Dict[int, str]
) -> Dict[str, List[Optional[float]]]:
    """Scan the dataset NDJSON dump and collect the "before" FUJI score for each sampled id."""
    ndjson_files = sorted(datasets_dir.glob("*.ndjson"))
    if not ndjson_files:
        raise FileNotFoundError(f"No NDJSON files found in {datasets_dir}")

    print(f"Found {len(ndjson_files):,} NDJSON files to scan")

    scores_by_publisher: Dict[str, List[Optional[float]]] = {}
    remaining = set(id_to_publisher.keys())

    for ndjson_file in tqdm(ndjson_files, desc="Scanning datasets", unit="file"):
        if not remaining:
            break
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                dataset_id = record.get("id")
                if dataset_id not in remaining:
                    continue

                publisher_id = id_to_publisher[dataset_id]
                fuji = record.get("fuji")
                score = fuji.get("score") if fuji else None
                scores_by_publisher.setdefault(publisher_id, []).append(score)
                remaining.discard(dataset_id)

    if remaining:
        print(f"  Warning: {len(remaining):,} sampled IDs were not found in {datasets_dir}")

    return scores_by_publisher


AFTER_SCORE_BATCH_SIZE = 1000


def collect_after_scores(
    conn: psycopg.Connection, id_to_publisher: Dict[int, str]
) -> Dict[str, List[Optional[float]]]:
    """Query FujiScore for the current ("after") score of each sampled dataset id.

    These are the scores written by a FUJI worker after processing the FujiJob
    rows queued by fuji-score-extrapolation-candidates.py, as opposed to the
    "before" scores read from the NDJSON dump. Looked up in batches of
    AFTER_SCORE_BATCH_SIZE dataset ids per query.
    """
    dataset_ids = list(id_to_publisher.keys())
    scores_by_publisher: Dict[str, List[Optional[float]]] = {}
    found_ids = set()

    with conn.cursor() as cur:
        for i in tqdm(
            range(0, len(dataset_ids), AFTER_SCORE_BATCH_SIZE),
            desc="Fetching FujiScore batches",
            unit="batch",
        ):
            batch = dataset_ids[i : i + AFTER_SCORE_BATCH_SIZE]
            cur.execute(
                'SELECT "datasetId", score FROM "FujiScore" WHERE "datasetId" = ANY(%s)',
                (batch,),
            )
            for dataset_id, score in cur.fetchall():
                publisher_id = id_to_publisher[dataset_id]
                scores_by_publisher.setdefault(publisher_id, []).append(score)
                found_ids.add(dataset_id)

    missing = set(dataset_ids) - found_ids
    if missing:
        print(f"  Warning: {len(missing):,} sampled IDs have no FujiScore row yet")

    return scores_by_publisher


def build_analysis(
    candidates: Dict[str, dict],
    before_scores_by_publisher: Dict[str, List[Optional[float]]],
    after_scores_by_publisher: Dict[str, List[Optional[float]]],
) -> Dict[str, dict]:
    """Build the per-publisher before/after consistency report."""
    analysis: Dict[str, dict] = {}

    for publisher_id, info in candidates.items():
        before_found = [
            s for s in before_scores_by_publisher.get(publisher_id, []) if s is not None
        ]
        before_unique = sorted(set(before_found))
        before_consistent = len(before_found) > 0 and len(before_unique) == 1

        after_found = [
            s for s in after_scores_by_publisher.get(publisher_id, []) if s is not None
        ]
        after_unique = sorted(set(after_found))
        after_consistent = len(after_found) > 0 and len(after_unique) == 1

        analysis[publisher_id] = {
            "totalCount": info.get("totalCount"),
            "lowScoreCount": info.get("lowScoreCount"),
            "sampleSize": info.get("sampleSize"),
            "beforeScoresFound": len(before_found),
            "beforeUniqueScores": before_unique,
            "beforeConsistent": before_consistent,
            "beforeScore": before_unique[0] if before_consistent else None,
            "afterScoresFound": len(after_found),
            "afterUniqueScores": after_unique,
            "afterConsistent": after_consistent,
            "afterScore": after_unique[0] if after_consistent else None,
            # Whether the freshly computed scores agree within the publisher -
            # the actual signal for whether extrapolation is safe.
            "consistent": after_consistent,
            "score": after_unique[0] if after_consistent else None,
        }

    return analysis


def main() -> None:
    """Compare sampled datasets' before/after FUJI scores within each publisher."""
    print("Starting FUJI extrapolation consistency analysis...")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set in environment or .env file")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    test_dir = downloads_dir / "pulled-database" / "fuji-extrapolation-test"
    datasets_dir = downloads_dir / "pulled-database" / "datasets"
    candidates_path = test_dir / CANDIDATES_FILE_NAME
    analysis_path = test_dir / ANALYSIS_FILE_NAME

    print(f"Candidates file: {candidates_path}")
    print(f"Datasets directory: {datasets_dir}")

    if not candidates_path.exists():
        raise FileNotFoundError(
            f"Candidates file not found: {candidates_path}. "
            f"Please run fuji-score-extrapolation-candidates.py first."
        )
    if not datasets_dir.exists():
        raise FileNotFoundError(f"Datasets directory not found: {datasets_dir}")

    print("\nLoading candidate sample IDs...")
    candidates, id_to_publisher = load_candidate_ids(candidates_path)

    print("\nScanning 'before' dataset scores from NDJSON dump...")
    before_scores_by_publisher = collect_before_scores(datasets_dir, id_to_publisher)

    print("\nFetching 'after' dataset scores from FujiScore table...")
    with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
        after_scores_by_publisher = collect_after_scores(conn, id_to_publisher)

    print("\nBuilding consistency report...")
    analysis = build_analysis(
        candidates, before_scores_by_publisher, after_scores_by_publisher
    )

    sorted_analysis = dict(
        sorted(
            analysis.items(),
            key=lambda x: (not x[1]["consistent"], -(x[1]["totalCount"] or 0)),
        )
    )

    with open(analysis_path, "w", encoding="utf-8") as f:
        json.dump(sorted_analysis, f, indent=2, ensure_ascii=False)

    consistent_count = sum(v["consistent"] for v in analysis.values())
    inconsistent_count = len(analysis) - consistent_count
    consistent_dataset_total = sum(
        v["totalCount"] or 0 for v in analysis.values() if v["consistent"]
    )
    inconsistent_dataset_total = sum(
        v["totalCount"] or 0 for v in analysis.values() if not v["consistent"]
    )
    overall_dataset_total = consistent_dataset_total + inconsistent_dataset_total

    print("\nDone!")
    print("Summary:")
    print(f"  - Publishers analyzed: {len(analysis):,}")
    print(f"  - Datasets analyzed: {overall_dataset_total:,}")
    print(f"  - Consistent publishers: {consistent_count:,} ({consistent_dataset_total:,} datasets)")
    print(f"  - Non-consistent publishers: {inconsistent_count:,} ({inconsistent_dataset_total:,} datasets)")
    print(f"Analysis written to: {analysis_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
