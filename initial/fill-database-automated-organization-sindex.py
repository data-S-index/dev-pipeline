"""Compute AutomatedOrganizationSIndex from AutomatedOrganizationDataset and DIndex.

By default: reads from DB, aggregates in Python, and writes local NDJSON files.
Use --load to load those NDJSON files into the database afterwards.

For each AutomatedOrganization, looks up datasets via AutomatedOrganizationDataset,
aggregates DIndex scores by year for those datasets in Python. Processes in small batches.
"""

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set, Tuple

import psycopg
from psycopg.rows import dict_row
from tqdm import tqdm

from config import DATABASE_URL

# Process this many automated organizations per batch
ORG_BATCH_SIZE = 2000
# Max dataset IDs per DIndex query (avoids huge IN lists for orgs with millions of datasets)
DINDEX_DATASET_BATCH_SIZE = 50_000
# Rows per output ndjson file when writing locally
ROWS_PER_FILE = 50_000
# Rows per COPY batch when loading from files
INSERT_BATCH_SIZE = 50_000


def get_org_id_batch(cur, last_id: Optional[int], batch_size: int) -> List[int]:
    """Return next batch of AutomatedOrganization ids (keyset pagination)."""
    if last_id is None:
        cur.execute(
            """SELECT id FROM "AutomatedOrganization" ORDER BY id LIMIT %s""",
            (batch_size,),
        )
    else:
        cur.execute(
            """SELECT id FROM "AutomatedOrganization" WHERE id > %s ORDER BY id LIMIT %s""",
            (last_id, batch_size),
        )
    return [row["id"] for row in cur.fetchall()]


def fetch_links_for_orgs(cur, org_ids: List[int]) -> List[Tuple[int, int]]:
    """Return list of (automatedOrganizationId, datasetId) for the given org IDs."""
    cur.execute(
        """SELECT "automatedOrganizationId", "datasetId" FROM "AutomatedOrganizationDataset"
           WHERE "automatedOrganizationId" = ANY(%s)""",
        (org_ids,),
    )
    return [(r["automatedOrganizationId"], r["datasetId"]) for r in cur.fetchall()]


def fetch_dindex_for_datasets(
    cur, dataset_ids: Set[int]
) -> List[Tuple[int, int, float]]:
    """Return list of (datasetId, year, score) for the given dataset IDs. Fetches in batches with progress."""
    if not dataset_ids:
        return []
    ids_list = list(dataset_ids)
    result: List[Tuple[int, int, float]] = []
    for start in tqdm(
        range(0, len(ids_list), DINDEX_DATASET_BATCH_SIZE),
        desc="  DIndex batches",
        unit="batch",
        total=(len(ids_list) + DINDEX_DATASET_BATCH_SIZE - 1) // DINDEX_DATASET_BATCH_SIZE,
    ):
        batch = ids_list[start : start + DINDEX_DATASET_BATCH_SIZE]
        cur.execute(
            """SELECT "datasetId", year, score FROM "DIndex"
               WHERE "datasetId" = ANY(%s)""",
            (batch,),
        )
        result.extend((r["datasetId"], r["year"], r["score"]) for r in cur.fetchall())
    return result


def _effective_score_for_year(
    sorted_year_scores: List[Tuple[int, float]], target_year: int
) -> float:
    """
    For a dataset's sorted (year, score) list, return the score to use for target_year:
    the d-index for target_year if present, else the latest one before it (carry forward).
    """
    if not sorted_year_scores:
        return 0.0
    # sorted_year_scores is sorted by year ascending; find latest (year, score) where year <= target_year
    best_score = 0.0
    for y, s in sorted_year_scores:
        if y <= target_year:
            best_score = s
        else:
            break
    return best_score


def aggregate_sindex_in_python(
    links: List[Tuple[int, int]],
    dindex_rows: List[Tuple[int, int, float]],
    current_year: int,
) -> List[Tuple[int, float, int]]:
    """
    Aggregate (automatedOrganizationId, year) -> sum of d-index scores in Python.
    - For each year, use each dataset's d-index for that year, or the latest one before it (carry forward).
    - Year range: from earliest d-index year (across org's datasets) to
      min(current_year - 1, latest d-index year).
    Returns list of (automatedOrganizationId, score, year) for insert.
    """
    # datasetId -> [(year, score), ...] sorted by year
    by_dataset: dict[int, List[Tuple[int, float]]] = defaultdict(list)
    for dataset_id, year, score in dindex_rows:
        by_dataset[dataset_id].append((year, score))
    for dataset_id in by_dataset:
        by_dataset[dataset_id].sort(key=lambda x: x[0])

    # org_id -> set of dataset_ids
    org_datasets: dict[int, Set[int]] = defaultdict(set)
    for org_id, dataset_id in links:
        org_datasets[org_id].add(dataset_id)

    result: List[Tuple[int, float, int]] = []
    for org_id, dataset_ids in org_datasets.items():
        # Year range: earliest d-index year to min(current_year - 1, latest d-index year)
        min_y = None
        max_y = None
        for did in dataset_ids:
            ys = by_dataset.get(did, [])
            if not ys:
                continue
            y_min = ys[0][0]
            y_max = ys[-1][0]
            if min_y is None or y_min < min_y:
                min_y = y_min
            if max_y is None or y_max > max_y:
                max_y = y_max
        if min_y is None or max_y is None:
            continue
        end_year = min(current_year - 1, max_y)
        if min_y > end_year:
            continue

        for year in range(min_y, end_year + 1):
            total = 0.0
            for did in dataset_ids:
                total += _effective_score_for_year(by_dataset.get(did, []), year)
            result.append((org_id, total, year))

    # Sort by org then by year (earliest first per org)
    result.sort(key=lambda row: (row[0], row[2]))
    return result


def _natural_sort_key(path: Path) -> tuple:
    """Natural sort key for filenames."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def compute_and_write_sindex_ndjson(conn: psycopg.Connection, output_dir: Path) -> int:
    """
    Process automated organizations in batches: load links and DIndex, aggregate in Python,
    write (automatedOrganizationId, score, year) to local NDJSON files. Returns total rows written.
    """
    print("📦 Computing AutomatedOrganizationSIndex and writing local NDJSON...")

    output_dir.mkdir(parents=True, exist_ok=True)
    total_written = 0
    file_number = 0
    current_file = None
    rows_in_current = 0

    def flush_file():
        nonlocal current_file
        if current_file is not None:
            current_file.close()
            current_file = None

    last_id: Optional[int] = None
    pbar = tqdm(desc="  Organization batches", unit="batch")

    with conn.cursor(row_factory=dict_row) as cur:
        while True:
            org_ids = get_org_id_batch(cur, last_id, ORG_BATCH_SIZE)
            if not org_ids:
                break
            last_id = org_ids[-1]

            links = fetch_links_for_orgs(cur, org_ids)
            if not links:
                pbar.update(1)
                continue

            dataset_ids: Set[int] = {did for _, did in links}
            dindex_rows = fetch_dindex_for_datasets(cur, dataset_ids)
            current_year = datetime.now().year
            sindex_rows = aggregate_sindex_in_python(links, dindex_rows, current_year)
            if not sindex_rows:
                pbar.update(1)
                continue

            for oid, score, year in sindex_rows:
                if rows_in_current >= ROWS_PER_FILE or current_file is None:
                    flush_file()
                    file_number += 1
                    current_file = open(
                        output_dir
                        / f"automatedorganizationsindex-{file_number}.ndjson",
                        "w",
                        encoding="utf-8",
                    )
                    rows_in_current = 0
                row = {"automatedOrganizationId": oid, "score": score, "year": year}
                current_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                rows_in_current += 1
                total_written += 1

            pbar.update(1)

    flush_file()
    pbar.close()
    print(f"  ✅ Wrote {total_written:,} rows to {output_dir}")
    return total_written


def load_sindex_from_ndjson(conn: psycopg.Connection, input_dir: Path) -> int:
    """Load AutomatedOrganizationSIndex from local NDJSON files into the database. Returns rows loaded."""
    print("📦 Loading AutomatedOrganizationSIndex from local NDJSON...")

    ndjson_files = sorted(input_dir.glob("*.ndjson"), key=_natural_sort_key)
    if not ndjson_files:
        print("  ⚠️  No ndjson files found")
        return 0

    now = datetime.now()
    total_loaded = 0
    insert_rows: List[tuple] = []

    for file_path in tqdm(ndjson_files, desc="  Files", unit="file"):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    oid = record.get("automatedOrganizationId")
                    score = record.get("score")
                    year = record.get("year")
                    if oid is None or score is None or year is None:
                        continue
                    insert_rows.append((int(oid), float(score), int(year), now))
                    if len(insert_rows) >= INSERT_BATCH_SIZE:
                        _insert_sindex_batch(conn, insert_rows)
                        total_loaded += len(insert_rows)
                        insert_rows = []
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue

    if insert_rows:
        _insert_sindex_batch(conn, insert_rows)
        total_loaded += len(insert_rows)

    print(f"  ✅ Loaded {total_loaded:,} AutomatedOrganizationSIndex rows")
    return total_loaded


def _insert_sindex_batch(conn: psycopg.Connection, rows: List[tuple]) -> None:
    """Insert a batch of AutomatedOrganizationSIndex rows (automatedOrganizationId, score, year, created)."""
    if not rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """COPY "AutomatedOrganizationSIndex" ("automatedOrganizationId", score, year, created)
               FROM STDIN"""
        ) as copy:
            for row in rows:
                copy.write_row(row)
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute AutomatedOrganizationSIndex (write NDJSON) or load from NDJSON into DB."
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load from local NDJSON files into database (default: compute and write NDJSON)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for NDJSON (default: ~/Downloads/database/automatedorganizationsindex)",
    )
    args = parser.parse_args()

    downloads_dir = Path.home() / "Downloads"
    database_dir = downloads_dir / "database"
    default_output = database_dir / "automatedorganizationsindex"

    if args.load:
        input_dir = args.output_dir or default_output
        if not input_dir.exists():
            raise FileNotFoundError(
                f"Input directory not found: {input_dir}. Run without --load first."
            )
        print("🚀 Loading automated organization s-index from NDJSON...")
        try:
            with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
                print("  ✅ Connected to database")
                print("\n🗑️  Truncating AutomatedOrganizationSIndex...")
                with conn.cursor() as cur:
                    cur.execute(
                        'TRUNCATE TABLE "AutomatedOrganizationSIndex" RESTART IDENTITY'
                    )
                    conn.commit()
                count = load_sindex_from_ndjson(conn, input_dir)
                print("\n✅ Load completed successfully!")
                print(f"📊 AutomatedOrganizationSIndex rows: {count:,}")
        except psycopg.Error as e:
            print(f"\n❌ Database error: {e}")
            raise
    else:
        print("🚀 Computing automated organization s-index (writing local NDJSON)...")
        output_dir = args.output_dir or default_output
        if output_dir.exists():
            import shutil

            shutil.rmtree(output_dir)
        try:
            with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
                print("  ✅ Connected to database")
                count = compute_and_write_sindex_ndjson(conn, output_dir)
                print("\n✅ Compute completed successfully!")
                print(f"📊 Wrote {count:,} rows to {output_dir}")
                print("  Run with --load to load into the database.")
        except psycopg.Error as e:
            print(f"\n❌ Database error: {e}")
            raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
