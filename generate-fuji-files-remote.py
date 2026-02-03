"""Export records with Fuji scores to NDJSON files.

Data model: prisma/schema.prisma (Dataset, FujiScore).
"""

import json
from datetime import datetime
from pathlib import Path
import psycopg
from tqdm import tqdm

from config import DATABASE_URL

RECORDS_PER_FILE = 10000  # Records per output file
DB_FETCH_BATCH_SIZE = 50000  # Records to fetch from database at once


def serialize_datetime(obj):
    """Serialize datetime objects to ISO format strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def write_batch_to_file(batch: list, file_number: int, output_dir: Path) -> None:
    """Write a batch of records to an NDJSON file."""
    file_name = f"{file_number}.ndjson"
    file_path = output_dir / file_name

    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            f.write(
                json.dumps(record, ensure_ascii=False, default=serialize_datetime)
                + "\n"
            )


def export_scored_records(
    conn: psycopg.Connection,
    output_dir: Path,
) -> None:
    """Export records with scores to NDJSON files."""
    print("  Querying database for records with scores...")

    # Query records with scores
    with conn.cursor() as cur:
        # Get total count for progress bar
        cur.execute(
            """
            SELECT COUNT(*)
            FROM "FujiScore" fs
            INNER JOIN "Dataset" d ON fs."datasetId" = d.id
            """
        )
        total_records = cur.fetchone()[0]
        print(f"  Found {total_records:,} records with scores")

        if total_records == 0:
            print("  ⚠️  No records with scores found")
            return

        # Process records in batches
        file_number = 1
        current_batch = []
        total_processed = 0

        pbar = tqdm(
            total=total_records,
            desc="  Exporting",
            unit="record",
            unit_scale=True,
        )

        # Fetch records from FujiScore table joined with Dataset in batches
        cur.execute(
            """
            SELECT
                d.id,
                d.identifier,
                fs.score,
                fs."evaluationDate",
                fs."metricVersion",
                fs."softwareVersion"
            FROM "FujiScore" fs
            INNER JOIN "Dataset" d ON fs."datasetId" = d.id
            ORDER BY d.id
            """
        )

        # Process records in batches to avoid loading all 50M into memory
        while True:
            rows = cur.fetchmany(DB_FETCH_BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                (
                    dataset_id,
                    identifier,
                    score,
                    evaluation_date,
                    metric_version,
                    software_version,
                ) = row

                # Create record with all FujiScore fields
                record = {
                    "id": dataset_id,
                    "identifier": identifier,
                    "score": float(score) if score is not None else None,
                    "evaluationDate": (
                        evaluation_date.isoformat() if evaluation_date else None
                    ),
                    "metricVersion": metric_version,
                    "softwareVersion": software_version,
                }

                current_batch.append(record)
                total_processed += 1

                # Write batch when it reaches RECORDS_PER_FILE
                if len(current_batch) >= RECORDS_PER_FILE:
                    write_batch_to_file(current_batch, file_number, output_dir)
                    file_number += 1
                    current_batch = []

                pbar.update(1)

        pbar.close()

        # Write remaining records as final file
        if current_batch:
            write_batch_to_file(current_batch, file_number, output_dir)

        print("\n  📊 Export Summary:")
        print(f"    - Total records exported: {total_processed:,}")
        print(f"    - Output files created: {file_number}")


def main() -> None:
    """Main function to export scored records to NDJSON files."""
    print("🚀 Starting Fuji scored records export...")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set in environment or .env file")

    # Get OS-agnostic paths (matching format-raw-data.py pattern)
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    output_dir = downloads_dir / "database" / "fuji-score"

    print(f"Output directory: {output_dir}")

    # Clean output directory
    if output_dir.exists():
        import shutil

        shutil.rmtree(output_dir)
        print("✓ Output directory cleaned")
    else:
        print("✓ Output directory not found")

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    print("✓ Output directory ready")

    # Connect to database and export records
    print("\n💾 Exporting records from database...")
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            export_scored_records(conn, output_dir)

        print("\n✅ Export completed successfully!")
        print(f"🎉 Exported files are available in: {output_dir}")

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
