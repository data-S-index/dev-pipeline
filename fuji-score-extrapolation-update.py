"""Build FujiScore staging NDJSON files from the extrapolation consistency analysis.

Reads fuji-extrapolation-analysis.json (produced by fuji-score-extrapolation-analysis.py),
which reports, per publisher (repository), whether the freshly computed "after" FUJI
scores agreed across the sampled datasets ("consistent") or not.

For every consistent repository, the agreed-upon score is extrapolated to all of that
repository's datasets. For every inconsistent repository, the median of its sampled
afterUniqueScores is computed and extrapolated only to datasets whose existing ("before")
FUJI score, as read from the pulled-database/datasets NDJSON dump, is below SCORE_THRESHOLD.

Output: NDJSON files under pulled-database/fuji-scores/, shaped to match the columns
fill-database-fuji.py expects (datasetId, score, evaluationDate, metricVersion,
softwareVersion), ready to be imported into the FujiScore table later.
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Dict, List, Tuple

from tqdm import tqdm

ANALYSIS_FILE_NAME = "fuji-extrapolation-analysis.json"
RECORDS_PER_FILE = 10000

# Inconsistent repositories only have their low-scoring datasets extrapolated.
SCORE_THRESHOLD = 14

METRIC_VERSION = "0.8"
SOFTWARE_VERSION = "extrapolated"
SOFTWARE_VERSION_INCONSISTENT = "extrapolated-inconsistent"


def load_analysis(analysis_path: Path) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Split the analysis report into consistent scores and inconsistent medians.

    Returns:
        consistent_scores: publisherId -> the single agreed-upon "after" score
        inconsistent_medians: publisherId -> median of the sampled afterUniqueScores
    """
    with open(analysis_path, "r", encoding="utf-8") as f:
        analysis = json.load(f)

    consistent_scores: Dict[str, float] = {}
    inconsistent_medians: Dict[str, float] = {}
    skipped_no_data = 0

    for publisher_id, info in analysis.items():
        if info.get("consistent"):
            consistent_scores[publisher_id] = info["score"]
            continue

        after_unique = info.get("afterUniqueScores") or []
        if not after_unique:
            skipped_no_data += 1
            continue

        inconsistent_medians[publisher_id] = median(after_unique)

    print(
        f"Loaded {len(consistent_scores):,} consistent and {len(inconsistent_medians):,} "
        f"inconsistent repositories from analysis"
    )
    if skipped_no_data:
        print(
            f"  Warning: {skipped_no_data:,} inconsistent repositories had no after "
            f"scores and were skipped"
        )

    return consistent_scores, inconsistent_medians


def write_batch_to_file(batch: List[dict], file_number: int, output_dir: Path) -> None:
    """Write a batch of FujiScore staging records to an NDJSON file."""
    file_path = output_dir / f"{file_number}.ndjson"
    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def build_staging_files(
    datasets_dir: Path,
    consistent_scores: Dict[str, float],
    inconsistent_medians: Dict[str, float],
    output_dir: Path,
) -> Dict[str, int]:
    """Scan the dataset NDJSON dump and write FujiScore staging files for updated datasets."""
    ndjson_files = sorted(datasets_dir.glob("*.ndjson"))
    if not ndjson_files:
        raise FileNotFoundError(f"No NDJSON files found in {datasets_dir}")

    print(f"Found {len(ndjson_files):,} NDJSON files to scan")

    evaluation_date = datetime.now(timezone.utc).isoformat()

    current_batch: List[dict] = []
    file_number = 1
    consistent_updates = 0
    inconsistent_updates = 0

    for ndjson_file in tqdm(ndjson_files, desc="Scanning datasets", unit="file"):
        with open(ndjson_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                publisher_id = record.get("publisherId")
                dataset_id = record.get("id")
                if not publisher_id or dataset_id is None:
                    continue

                fuji = record.get("fuji")
                current_score = fuji.get("score") if fuji else None

                if publisher_id in consistent_scores:
                    new_score = consistent_scores[publisher_id]
                    if current_score == new_score:
                        continue
                    software_version = SOFTWARE_VERSION
                    consistent_updates += 1
                elif publisher_id in inconsistent_medians:
                    if current_score is None or current_score >= SCORE_THRESHOLD:
                        continue
                    new_score = inconsistent_medians[publisher_id]
                    if current_score == new_score:
                        continue
                    software_version = SOFTWARE_VERSION_INCONSISTENT
                    inconsistent_updates += 1
                else:
                    continue

                current_batch.append(
                    {
                        "datasetId": dataset_id,
                        "score": new_score,
                        "evaluationDate": evaluation_date,
                        "metricVersion": METRIC_VERSION,
                        "softwareVersion": software_version,
                    }
                )

                if len(current_batch) >= RECORDS_PER_FILE:
                    write_batch_to_file(current_batch, file_number, output_dir)
                    file_number += 1
                    current_batch = []

    if current_batch:
        write_batch_to_file(current_batch, file_number, output_dir)
    else:
        file_number -= 1

    return {
        "filesWritten": file_number,
        "consistentUpdates": consistent_updates,
        "inconsistentUpdates": inconsistent_updates,
    }


def main() -> None:
    """Build FujiScore staging NDJSON files from the extrapolation analysis."""
    print("Starting FUJI extrapolation staging file generation...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    test_dir = downloads_dir / "pulled-database" / "fuji-extrapolation-test"
    datasets_dir = downloads_dir / "pulled-database" / "datasets"
    analysis_path = test_dir / ANALYSIS_FILE_NAME
    output_dir = downloads_dir / "pulled-database" / "fuji-scores"

    print(f"Analysis file: {analysis_path}")
    print(f"Datasets directory: {datasets_dir}")
    print(f"Output directory: {output_dir}")

    if not analysis_path.exists():
        raise FileNotFoundError(
            f"Analysis file not found: {analysis_path}. "
            f"Please run fuji-score-extrapolation-analysis.py first."
        )
    if not datasets_dir.exists():
        raise FileNotFoundError(f"Datasets directory not found: {datasets_dir}")

    print("\nLoading analysis report...")
    consistent_scores, inconsistent_medians = load_analysis(analysis_path)

    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\nScanning datasets and building staging files...")
    stats = build_staging_files(
        datasets_dir, consistent_scores, inconsistent_medians, output_dir
    )

    total_updates = stats["consistentUpdates"] + stats["inconsistentUpdates"]

    print("\nDone!")
    print("Summary:")
    print(f"  - Consistent repositories: {len(consistent_scores):,}")
    print(f"  - Inconsistent repositories: {len(inconsistent_medians):,}")
    print(f"  - Datasets updated from consistent repositories: {stats['consistentUpdates']:,}")
    print(f"  - Datasets updated from inconsistent repositories (score < {SCORE_THRESHOLD}): {stats['inconsistentUpdates']:,}")
    print(f"  - Total staged updates: {total_updates:,}")
    print(f"  - Staging files written: {stats['filesWritten']:,}")
    print(f"Staging files are available at: {output_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
