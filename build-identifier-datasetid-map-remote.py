"""Build identifier to dataset ID mapping from database using streaming."""

import csv
import json
from pathlib import Path

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

IDENTIFIER_TO_ID_MAP_FILE = "identifier_to_id_map.ndjson"  # Intermediate mapping file


def export_identifier_map(conn: psycopg.Connection, mapping_file: Path) -> int:
    """Export identifier to ID mapping from database using streaming COPY.

    Uses COPY TO STDOUT to stream directly to file without building a giant dict.
    Exports all identifier and id pairs from the Dataset table.
    """
    print("  Building identifier to ID mapping from database (streaming)...")

    # Remove mapping file if it exists
    if mapping_file.exists():
        mapping_file.unlink()
        print(f"  ✓ Removed existing mapping file: {mapping_file}")

    # Get count of records for progress bar
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM "Dataset"')
        total_records = cur.fetchone()[0]
        print(f"  Found {total_records:,} dataset record(s) to process")

        if total_records == 0:
            raise ValueError("No dataset records found in database.")

    # SQL query: get all identifier and id pairs
    sql = """
    COPY (
      SELECT lower(identifier) AS identifier,
             id
      FROM "Dataset"
      ORDER BY id
    ) TO STDOUT WITH (FORMAT CSV, HEADER true);
    """

    # Stream CSV from database and convert to NDJSON on the fly
    print(f"  💾 Streaming mapping to {mapping_file}...")
    record_count = 0

    with conn.cursor() as cur, mapping_file.open("w", encoding="utf-8") as f:
        with cur.copy(sql) as copy:
            # Create progress bar
            pbar = tqdm(
                total=total_records,
                desc="  Streaming mapping",
                unit="record",
                unit_scale=True,
            )

            # Process CSV chunks and convert to NDJSON
            buffer = b""
            fieldnames = ["identifier", "id"]  # Known column order from SQL
            first_line = True

            for chunk in copy:
                buffer += chunk
                # Process complete lines (ending with newline)
                while b"\n" in buffer:
                    line_bytes, buffer = buffer.split(b"\n", 1)
                    if not line_bytes.strip():
                        continue

                    # Skip header line
                    if first_line:
                        first_line = False
                        continue

                    # Parse CSV line
                    try:
                        line = line_bytes.decode("utf-8")
                        # Simple CSV parsing (identifier,id format)
                        parts = next(csv.reader([line]))
                        if len(parts) >= 2:
                            identifier = parts[0].strip()
                            dataset_id_str = parts[1].strip()

                            if identifier and dataset_id_str:
                                try:
                                    dataset_id = int(dataset_id_str)
                                    # Write NDJSON record
                                    json_record = {"identifier": identifier, "id": dataset_id}
                                    f.write(
                                        json.dumps(json_record, ensure_ascii=False) + "\n"
                                    )
                                    record_count += 1
                                    pbar.update(1)
                                except ValueError:
                                    # Skip invalid ID
                                    continue
                    except Exception:
                        # Skip malformed lines
                        continue

            # Process remaining buffer (last line without newline)
            if buffer.strip():
                try:
                    # Skip if it's just the header
                    if not first_line:
                        line = buffer.decode("utf-8")
                        parts = next(csv.reader([line]))
                        if len(parts) >= 2:
                            identifier = parts[0].strip()
                            dataset_id_str = parts[1].strip()

                            if identifier and dataset_id_str:
                                try:
                                    dataset_id = int(dataset_id_str)
                                    json_record = {"identifier": identifier, "id": dataset_id}
                                    f.write(json.dumps(json_record, ensure_ascii=False) + "\n")
                                    record_count += 1
                                    pbar.update(1)
                                except ValueError:
                                    pass
                except Exception:
                    pass

            pbar.close()

    print(f"  ✓ Saved {record_count:,} identifier mappings")
    return record_count


def main() -> None:
    """Main function to build identifier to ID mapping from database."""
    print("🚀 Starting identifier to ID mapping build from database...")

    if not DATABASE_URL:
        raise ValueError(
            "DATABASE_URL not set. Please set it in your .env file or environment variables."
        )

    # Get OS-agnostic paths
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    mapping_file = downloads_dir / "database" / IDENTIFIER_TO_ID_MAP_FILE

    print(f"Mapping file: {mapping_file}")

    # Ensure mapping file directory exists
    mapping_file.parent.mkdir(parents=True, exist_ok=True)

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            # Build identifier to ID mapping using streaming
            print("\n🗺️  Building identifier to ID mapping from database...")
            record_count = export_identifier_map(conn, mapping_file)
            print(f"  ✓ Mapping contains {record_count:,} identifier entries")
    except Exception as e:
        raise RuntimeError(f"Database connection error: {e}")

    print("\n✅ Identifier to ID mapping build completed successfully!")
    print(f"🎉 Mapping file is available at: {mapping_file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error occurred during mapping build:")
        print(e)
        exit(1)
