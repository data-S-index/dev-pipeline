"""Fill database with Fuji/FAIR scores from formatted NDJSON files."""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL


BATCH_SIZE = 10000


def natural_sort_key(path: Path) -> tuple:
    """Generate a sort key for natural sorting (alphabetical then numerical)."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def load_ndjson_files(directory: Path) -> List[Path]:
    """Load and sort ndjson files from directory."""
    files = list(directory.glob("*.ndjson"))
    return sorted(files, key=natural_sort_key)


def upsert_fuji_scores_batch(
    conn: psycopg.Connection,
    fuji_score_rows: List[tuple],
) -> None:
    """Upsert a batch of Fuji scores using temp table and ON CONFLICT."""
    if not fuji_score_rows:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE temp_fuji_scores (
                "datasetId" INT,
                score FLOAT,
                "evaluationDate" TIMESTAMP WITH TIME ZONE,
                "metricVersion" TEXT,
                "softwareVersion" TEXT
            ) ON COMMIT DROP
            """
        )
        with cur.copy(
            """COPY temp_fuji_scores ("datasetId", score, "evaluationDate", "metricVersion", "softwareVersion")
               FROM STDIN"""
        ) as copy:
            for row in fuji_score_rows:
                copy.write_row(row)

        cur.execute(
            """
            INSERT INTO "FujiScore" ("datasetId", score, "evaluationDate", "metricVersion", "softwareVersion", created, updated)
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


def process_fuji_files(conn: psycopg.Connection, fuji_dir: Path) -> int:
    """Process Fuji NDJSON files and upsert into FujiScore table."""
    print("📊 Processing Fuji score files...")

    ndjson_files = load_ndjson_files(fuji_dir)
    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0

    print(f"  Found {len(ndjson_files)} ndjson file(s)")

    total_records = 0
    for file_path in tqdm(ndjson_files, desc="  Counting", unit="file", leave=False):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        total_records += 1
        except Exception:
            continue

    print(f"  Processing {total_records:,} Fuji score records...")

    fuji_score_rows: List[tuple] = []
    total_upserted = 0

    pbar = tqdm(
        total=total_records, desc="  Processing", unit="record", unit_scale=True
    )

    for file_path in ndjson_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)

                        dataset_id = record.get("datasetId")
                        if dataset_id is None:
                            tqdm.write(
                                f"    ⚠️  Skipping record without datasetId in {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        score_raw = record.get("score")
                        score = None
                        if score_raw is not None:
                            try:
                                score = float(score_raw)
                            except (TypeError, ValueError):
                                pass

                        evaluation_date_str = record.get("evaluationDate")
                        evaluation_date = datetime.now()
                        if evaluation_date_str:
                            try:
                                if isinstance(evaluation_date_str, str):
                                    if evaluation_date_str.endswith("Z"):
                                        evaluation_date_str = (
                                            evaluation_date_str[:-1] + "+00:00"
                                        )
                                    evaluation_date = datetime.fromisoformat(
                                        evaluation_date_str
                                    )
                            except (ValueError, AttributeError, TypeError):
                                pass

                        metric_version = (
                            record.get("metricVersion") or "estimated"
                        ).strip() or "estimated"
                        software_version = (
                            record.get("softwareVersion") or "estimated"
                        ).strip() or "estimated"

                        row = (
                            int(dataset_id),
                            score,
                            evaluation_date,
                            metric_version,
                            software_version,
                        )
                        fuji_score_rows.append(row)
                        total_upserted += 1
                        pbar.update(1)

                        if len(fuji_score_rows) >= BATCH_SIZE:
                            upsert_fuji_scores_batch(conn, fuji_score_rows)
                            fuji_score_rows = []

                    except json.JSONDecodeError as e:
                        tqdm.write(
                            f"    ⚠️  Error parsing line in {file_path.name}: {e}"
                        )
                        pbar.update(1)
                        continue
                    except Exception as e:
                        tqdm.write(
                            f"    ⚠️  Error processing record in {file_path.name}: {e}"
                        )
                        pbar.update(1)
                        continue

        except Exception as e:
            tqdm.write(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

    pbar.close()

    if fuji_score_rows:
        upsert_fuji_scores_batch(conn, fuji_score_rows)

    return total_upserted


def main() -> None:
    """Main function to fill database with Fuji score data."""
    print("🚀 Starting Fuji score database fill...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    fuji_dir = downloads_dir / "database" / "fuji"

    print(f"Fuji directory: {fuji_dir}")

    if not fuji_dir.exists():
        raise FileNotFoundError(
            f"Fuji directory not found: {fuji_dir}. "
            "Please run format-fuji-score.py first."
        )

    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")

            count = process_fuji_files(conn, fuji_dir)

            print("\n✅ Fuji score database fill completed successfully!")
            print("📊 Summary:")
            print(f"  - Records upserted: {count:,}")

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
