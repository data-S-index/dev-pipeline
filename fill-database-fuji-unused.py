"""Fill database with Fuji scores from NDJSON files."""

import json
from datetime import datetime
from pathlib import Path
from typing import List

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

BATCH_SIZE = 1000  # Records per batch


def load_ndjson_files(directory: Path) -> List[Path]:
    """Load all NDJSON files from directory, sorted naturally."""
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")

    files = list(directory.glob("*.ndjson"))
    if not files:
        raise FileNotFoundError(f"No NDJSON files found in {directory}")

    # Natural sort (1.ndjson, 2.ndjson, ..., 10.ndjson, ...)
    def natural_sort_key(path: Path) -> tuple:
        try:
            # Extract number from filename (e.g., "1.ndjson" -> 1)
            num = int(path.stem)
            return (0, num)
        except ValueError:
            return (1, path.name)

    return sorted(files, key=natural_sort_key)


def upsert_fuji_scores_batch(
    conn: psycopg.Connection,
    fuji_score_rows: List[tuple],
) -> None:
    """Upsert a batch of Fuji scores using COPY and ON CONFLICT."""
    if not fuji_score_rows:
        return

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

        # Use COPY to insert into temp table
        with cur.copy(
            """COPY temp_fuji_scores ("datasetId", score, "evaluationDate", "metricVersion", "softwareVersion")
               FROM STDIN"""
        ) as copy:
            for row in fuji_score_rows:
                copy.write_row(row)

        # Upsert from temp table to FujiScore table
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


def process_ndjson_files(
    conn: psycopg.Connection, fuji_score_dir: Path
) -> tuple[int, int]:
    """Process NDJSON files and insert Fuji scores into database."""
    files = load_ndjson_files(fuji_score_dir)
    print(f"  Found {len(files)} NDJSON file(s)")

    total_records = 0
    total_inserted = 0
    fuji_score_rows = []

    # Default values for required fields not in the output
    DEFAULT_METRIC_VERSION = "metrics_v0.5"
    DEFAULT_SOFTWARE_VERSION = "unknown"

    # Process all files
    for file_path in files:
        print(f"  Processing {file_path.name}...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                # Count lines for progress bar
                line_count = sum(1 for _ in f)
                f.seek(0)  # Reset file pointer

                pbar = tqdm(
                    total=line_count,
                    desc=f"    {file_path.name}",
                    unit="record",
                    unit_scale=True,
                    leave=False,
                )

                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        pbar.update(1)
                        continue

                    try:
                        record = json.loads(line)

                        # Extract required fields
                        dataset_id = record.get("id")
                        if not dataset_id:
                            tqdm.write(
                                f"    ⚠️  Missing 'id' in record at line {line_num} of {file_path.name}"
                            )
                            pbar.update(1)
                            continue

                        score = record.get("score")
                        evaluation_date_str = record.get("evaluationDate")

                        # Parse evaluation date
                        evaluation_date = None
                        if evaluation_date_str:
                            try:
                                # Handle ISO format strings
                                if isinstance(evaluation_date_str, str):
                                    # Remove 'Z' and replace with '+00:00' if needed
                                    if evaluation_date_str.endswith("Z"):
                                        evaluation_date_str = (
                                            evaluation_date_str[:-1] + "+00:00"
                                        )
                                    evaluation_date = datetime.fromisoformat(
                                        evaluation_date_str
                                    )
                                elif isinstance(evaluation_date_str, (int, float)):
                                    # Handle Unix timestamp
                                    evaluation_date = datetime.fromtimestamp(
                                        evaluation_date_str
                                    )
                            except (ValueError, TypeError) as e:
                                tqdm.write(
                                    f"    ⚠️  Invalid evaluationDate at line {line_num} of {file_path.name}: {e}"
                                )
                                # Use current time as fallback
                                evaluation_date = datetime.now()
                        else:
                            # Use current time if not provided
                            evaluation_date = datetime.now()

                        # Get metric and software versions if available, otherwise use defaults
                        metric_version = record.get(
                            "metricVersion", DEFAULT_METRIC_VERSION
                        )
                        software_version = record.get(
                            "softwareVersion", DEFAULT_SOFTWARE_VERSION
                        )

                        # Prepare row tuple matching database schema order
                        # datasetId, score, evaluationDate, metricVersion, softwareVersion
                        row = (
                            int(dataset_id),
                            float(score) if score is not None else None,
                            evaluation_date,
                            str(metric_version),
                            str(software_version),
                        )

                        fuji_score_rows.append(row)
                        total_records += 1
                        pbar.update(1)

                        # Insert batch when it reaches BATCH_SIZE
                        if len(fuji_score_rows) >= BATCH_SIZE:
                            upsert_fuji_scores_batch(conn, fuji_score_rows)
                            total_inserted += len(fuji_score_rows)
                            fuji_score_rows = []

                    except json.JSONDecodeError as e:
                        tqdm.write(
                            f"    ⚠️  Error parsing line {line_num} in {file_path.name}: {e}"
                        )
                        pbar.update(1)
                        continue
                    except Exception as e:
                        tqdm.write(
                            f"    ⚠️  Error processing record at line {line_num} in {file_path.name}: {e}"
                        )
                        pbar.update(1)
                        continue

                pbar.close()

        except Exception as e:
            print(f"    ⚠️  Error reading {file_path.name}: {e}")
            continue

    # Insert remaining records
    if fuji_score_rows:
        upsert_fuji_scores_batch(conn, fuji_score_rows)
        total_inserted += len(fuji_score_rows)

    return total_records, total_inserted


def main() -> None:
    """Main function to fill database with Fuji score data."""
    print("🚀 Starting Fuji score database fill...")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set in environment or .env file")

    # Get OS-agnostic paths (matching generate-fuji-file.py pattern)
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    fuji_score_dir = downloads_dir / "database" / "fuji-score"

    print(f"Fuji score directory: {fuji_score_dir}")

    if not fuji_score_dir.exists():
        raise FileNotFoundError(
            f"Fuji score directory not found: {fuji_score_dir}. "
            f"Please run generate-fuji-file.py first to create the NDJSON files."
        )

    # Connect to database and process files
    print("\n💾 Processing NDJSON files and inserting into database...")
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            total_records, total_inserted = process_ndjson_files(conn, fuji_score_dir)

        print("\n✅ Database fill completed successfully!")
        print(f"📊 Summary:")
        print(f"    - Total records processed: {total_records:,}")
        print(f"    - Records inserted/updated: {total_inserted:,}")

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
