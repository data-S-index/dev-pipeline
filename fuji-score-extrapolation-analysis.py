"""Check whether sampled FUJI scores are consistent within each DOI prefix.

Reads the candidate samples produced by fuji-score-extrapolation-candidates.py
(dataset IDs grouped by DOI prefix), looks up each sampled dataset's original
FUJI score from the pulled-database/datasets NDJSON dump, and reports per
prefix whether all sampled scores agree. A prefix with a single consistent
score across its whole sample is a good candidate for extrapolating that
score to the rest of the prefix's datasets instead of running FUJI on them.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

from tqdm import tqdm

CANDIDATES_FILE_NAME = "fuji-extrapolation-candidates.json"
ANALYSIS_FILE_NAME = "fuji-extrapolation-analysis.json"


def load_candidate_ids(candidates_path: Path) -> Dict[int, str]:
    """Load the candidates file and return a mapping of dataset id -> prefix."""
    with open(candidates_path, "r", encoding="utf-8") as f:
        candidates = json.load(f)

    id_to_prefix: Dict[int, str] = {}
    for prefix, info in candidates.items():
        for sample in info.get("samples", []):
            dataset_id = sample.get("id")
            if dataset_id is not None:
                id_to_prefix[dataset_id] = prefix

    print(f"Loaded {len(id_to_prefix):,} sampled dataset IDs across {len(candidates):,} prefixes")
    return candidates, id_to_prefix


def collect_original_scores(
    datasets_dir: Path, id_to_prefix: Dict[int, str]
) -> Dict[str, List[Optional[float]]]:
    """Scan the dataset NDJSON dump and collect the original FUJI score for each sampled id."""
    ndjson_files = sorted(datasets_dir.glob("*.ndjson"))
    if not ndjson_files:
        raise FileNotFoundError(f"No NDJSON files found in {datasets_dir}")

    print(f"Found {len(ndjson_files):,} NDJSON files to scan")

    scores_by_prefix: Dict[str, List[Optional[float]]] = {}
    remaining = set(id_to_prefix.keys())

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

                prefix = id_to_prefix[dataset_id]
                fuji = record.get("fuji")
                score = fuji.get("score") if fuji else None
                scores_by_prefix.setdefault(prefix, []).append(score)
                remaining.discard(dataset_id)

    if remaining:
        print(f"  Warning: {len(remaining):,} sampled IDs were not found in {datasets_dir}")

    return scores_by_prefix


def build_analysis(
    candidates: Dict[str, dict], scores_by_prefix: Dict[str, List[Optional[float]]]
) -> Dict[str, dict]:
    """Build the per-prefix consistency report."""
    analysis: Dict[str, dict] = {}

    for prefix, info in candidates.items():
        scores = scores_by_prefix.get(prefix, [])
        found_scores = [s for s in scores if s is not None]
        unique_scores = sorted(set(found_scores))
        consistent = len(found_scores) > 0 and len(unique_scores) == 1

        analysis[prefix] = {
            "totalCount": info.get("totalCount"),
            "lowScoreCount": info.get("lowScoreCount"),
            "sampleSize": info.get("sampleSize"),
            "scoresFound": len(found_scores),
            "uniqueScores": unique_scores,
            "consistent": consistent,
            "score": unique_scores[0] if consistent else None,
        }

    return analysis


def main() -> None:
    """Compare sampled datasets' original FUJI scores within each prefix."""
    print("Starting FUJI extrapolation consistency analysis...")

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
    candidates, id_to_prefix = load_candidate_ids(candidates_path)

    print("\nScanning original dataset scores...")
    scores_by_prefix = collect_original_scores(datasets_dir, id_to_prefix)

    print("\nBuilding consistency report...")
    analysis = build_analysis(candidates, scores_by_prefix)

    sorted_analysis = dict(
        sorted(
            analysis.items(),
            key=lambda x: (not x[1]["consistent"], -(x[1]["totalCount"] or 0)),
        )
    )

    with open(analysis_path, "w", encoding="utf-8") as f:
        json.dump(sorted_analysis, f, indent=2, ensure_ascii=False)

    consistent_count = sum(v["consistent"] for v in analysis.values())
    consistent_dataset_total = sum(
        v["totalCount"] or 0 for v in analysis.values() if v["consistent"]
    )

    print("\nDone!")
    print("Summary:")
    print(f"  - Prefixes analyzed: {len(analysis):,}")
    print(f"  - Prefixes with a consistent score: {consistent_count:,}")
    print(f"  - Datasets covered by consistent prefixes: {consistent_dataset_total:,}")
    print(f"Analysis written to: {analysis_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
