"""Compute AutomatedUserSIndex from DB and write NDJSON, or load NDJSON into DB.

Default: read from DB, aggregate in Python, write local NDJSON.
--load: load from local NDJSON into database (truncate then COPY).
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

USER_BATCH_SIZE = 2000
LINKS_BATCH_SIZE = 50_000
DINDEX_DATASET_BATCH_SIZE = 50_000
ROWS_PER_FILE = 50_000
INSERT_BATCH_SIZE = 50_000


def get_user_id_batch(cur, last_id: Optional[int], batch_size: int) -> List[int]:
    """Return next batch of AutomatedUser ids (keyset pagination)."""
    if last_id is None:
        cur.execute(
            """SELECT id FROM "AutomatedUser" ORDER BY id LIMIT %s""",
            (batch_size,),
        )
    else:
        cur.execute(
            """SELECT id FROM "AutomatedUser" WHERE id > %s ORDER BY id LIMIT %s""",
            (last_id, batch_size),
        )
    return [row["id"] for row in cur.fetchall()]


def fetch_links_for_users(cur, user_ids: List[int]) -> List[Tuple[int, int]]:
    """Return list of (automatedUserId, datasetId) for the given user IDs."""
    if not user_ids:
        return []
    result: List[Tuple[int, int]] = []
    last_uid: Optional[int] = None
    last_did: Optional[int] = None
    with tqdm(desc="  Links batches", unit="batch") as pbar:
        while True:
            if last_uid is None and last_did is None:
                cur.execute(
                    """SELECT "automatedUserId", "datasetId" FROM "AutomatedUserDataset"
                       WHERE "automatedUserId" = ANY(%s)
                       ORDER BY "automatedUserId", "datasetId"
                       LIMIT %s""",
                    (user_ids, LINKS_BATCH_SIZE),
                )
            else:
                cur.execute(
                    """SELECT "automatedUserId", "datasetId" FROM "AutomatedUserDataset"
                       WHERE "automatedUserId" = ANY(%s)
                         AND ("automatedUserId", "datasetId") > (%s, %s)
                       ORDER BY "automatedUserId", "datasetId"
                       LIMIT %s""",
                    (user_ids, last_uid, last_did, LINKS_BATCH_SIZE),
                )
            rows = cur.fetchall()
            if not rows:
                break
            for r in rows:
                link = (r["automatedUserId"], r["datasetId"])
                result.append(link)
                last_uid, last_did = link
            pbar.update(1)
            if len(rows) < LINKS_BATCH_SIZE:
                break
    return result


def fetch_dindex_for_datasets(
    cur, dataset_ids: Set[int]
) -> List[Tuple[int, int, float]]:
    """Return list of (datasetId, year, score) for the given dataset IDs."""
    if not dataset_ids:
        return []
    ids_list = list(dataset_ids)
    result: List[Tuple[int, int, float]] = []
    for start in tqdm(
        range(0, len(ids_list), DINDEX_DATASET_BATCH_SIZE),
        desc="  DIndex batches",
        unit="batch",
        total=(len(ids_list) + DINDEX_DATASET_BATCH_SIZE - 1)
        // DINDEX_DATASET_BATCH_SIZE,
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
    """Score for target_year: d-index for that year if present, else latest before it."""
    if not sorted_year_scores:
        return 0.0
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
    """Aggregate (automatedUserId, year) -> sum of d-index scores. Returns (uid, score, year)."""
    by_dataset: dict[int, List[Tuple[int, float]]] = defaultdict(list)
    for dataset_id, year, score in dindex_rows:
        by_dataset[dataset_id].append((year, score))
    for dataset_id in by_dataset:
        by_dataset[dataset_id].sort(key=lambda x: x[0])

    user_datasets: dict[int, Set[int]] = defaultdict(set)
    for user_id, dataset_id in links:
        user_datasets[user_id].add(dataset_id)

    result: List[Tuple[int, float, int]] = []
    for user_id, dataset_ids in user_datasets.items():
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
            result.append((user_id, total, year))

    result.sort(key=lambda row: (row[0], row[2]))
    return result


def _natural_sort_key(path: Path) -> tuple:
    """Natural sort key for filenames."""
    name = path.name
    parts = re.split(r"(\d+)", name)
    return tuple(int(part) if part.isdigit() else part.lower() for part in parts)


def compute_and_write_sindex_ndjson(conn: psycopg.Connection, output_dir: Path) -> int:
    """Read from DB, aggregate in Python, write (automatedUserId, score, year) to NDJSON."""
    print("📦 Computing AutomatedUserSIndex and writing local NDJSON...")

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
    pbar = tqdm(desc="  User batches", unit="batch")

    with conn.cursor(row_factory=dict_row) as cur:
        while True:
            user_ids = get_user_id_batch(cur, last_id, USER_BATCH_SIZE)
            if not user_ids:
                break
            last_id = user_ids[-1]

            links = fetch_links_for_users(cur, user_ids)
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

            for uid, score, year in sindex_rows:
                if rows_in_current >= ROWS_PER_FILE or current_file is None:
                    flush_file()
                    file_number += 1
                    current_file = open(
                        output_dir / f"automatedusersindex-{file_number}.ndjson",
                        "w",
                        encoding="utf-8",
                    )
                    rows_in_current = 0
                row = {"automatedUserId": uid, "score": score, "year": year}
                current_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                rows_in_current += 1
                total_written += 1

            pbar.update(1)

    flush_file()
    pbar.close()
    print(f"  ✅ Wrote {total_written:,} rows to {output_dir}")
    return total_written


def load_sindex_from_ndjson(conn: psycopg.Connection, input_dir: Path) -> int:
    """Load AutomatedUserSIndex from local NDJSON into the database."""
    print("📦 Loading AutomatedUserSIndex from local NDJSON...")

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
                    uid = record.get("automatedUserId")
                    score = record.get("score")
                    year = record.get("year")
                    if uid is None or score is None or year is None:
                        continue
                    insert_rows.append((int(uid), float(score), int(year), now))
                    if len(insert_rows) >= INSERT_BATCH_SIZE:
                        _insert_sindex_batch(conn, insert_rows)
                        total_loaded += len(insert_rows)
                        insert_rows = []
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue

    if insert_rows:
        _insert_sindex_batch(conn, insert_rows)
        total_loaded += len(insert_rows)

    print(f"  ✅ Loaded {total_loaded:,} AutomatedUserSIndex rows")
    return total_loaded


def _insert_sindex_batch(conn: psycopg.Connection, rows: List[tuple]) -> None:
    """Insert a batch of AutomatedUserSIndex rows (automatedUserId, score, year, created)."""
    if not rows:
        return
    with conn.cursor() as cur:
        with cur.copy(
            """COPY "AutomatedUserSIndex" ("automatedUserId", score, year, created)
               FROM STDIN"""
        ) as copy:
            for row in rows:
                copy.write_row(row)
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute AutomatedUserSIndex from DB (write NDJSON) or load NDJSON into DB."
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load from local NDJSON into database (default: compute from DB and write NDJSON)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="NDJSON directory (default: ~/Downloads/database/automatedusersindex)",
    )
    args = parser.parse_args()

    default_output = Path.home() / "Downloads" / "database" / "automatedusersindex"
    output_dir = args.output_dir or default_output

    try:
        with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
            print("  ✅ Connected to database")
            if args.load:
                if not output_dir.exists():
                    raise FileNotFoundError(
                        f"Input directory not found: {output_dir}. Run without --load first."
                    )
                print("\n🗑️  Truncating AutomatedUserSIndex...")
                with conn.cursor() as cur:
                    cur.execute('TRUNCATE TABLE "AutomatedUserSIndex" RESTART IDENTITY')
                    conn.commit()
                count = load_sindex_from_ndjson(conn, output_dir)
                print("\n✅ Load completed successfully!")
                print(f"📊 AutomatedUserSIndex rows: {count:,}")
            else:
                if output_dir.exists():
                    import shutil

                    shutil.rmtree(output_dir)
                print("🚀 Computing automated user s-index (writing local NDJSON)...")
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
