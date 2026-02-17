"""Compute AutomatedUserSIndex from local NDJSON only (no database).

Reads from ~/Downloads/database/: dindex/, authors/ (AutomatedUser),
automateduserdataset/. Processes in batches to limit RAM: only one batch
of user→datasets and dindex data is in memory at a time.
Writes (automatedUserId, score, year) to ~/Downloads/database/automatedusersindex/.
If DATABASE_URL is set, loads the result into the database when done.
"""

import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Set, Tuple

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Users per batch; larger = more RAM but fewer passes over dindex/automateduserdataset.
# Tuned for ~32GB RAM: fewer batches = faster (fewer full file scans).
USER_BATCH_SIZE = 250_000
# Rows per output ndjson file
ROWS_PER_FILE = 50_000
# Rows per DB insert batch when loading into database
INSERT_BATCH_SIZE = 50_000


def _effective_score_for_year(
    sorted_year_scores: List[Tuple[int, float]], target_year: int
) -> float:
    """
    For a dataset's sorted (year, score) list, return the score to use for target_year:
    the d-index for target_year if present, else the latest one before it (carry forward).
    """
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
    """
    Aggregate (automatedUserId, year) -> sum of d-index scores in Python.
    Returns list of (automatedUserId, score, year) for output.
    """
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


def _iter_ordered_user_ids(authors_dir: Path) -> Iterator[int]:
    """Yield (AutomatedUser) author ids in file order; no full list in memory."""
    ndjson_files = sorted(authors_dir.glob("*.ndjson"), key=_natural_sort_key)
    for file_path in tqdm(ndjson_files, desc="  Authors files", unit="file"):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    uid = record.get("id")
                    if uid is not None:
                        yield int(uid)
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue


def _load_user_datasets_for_users(
    automateduserdataset_dir: Path, user_ids: Set[int]
) -> Dict[int, Set[int]]:
    """Load automatedUserId -> set(datasetId) only for the given user_ids."""
    ndjson_files = sorted(
        automateduserdataset_dir.glob("*.ndjson"), key=_natural_sort_key
    )
    user_datasets: Dict[int, Set[int]] = defaultdict(set)
    for file_path in tqdm(
        ndjson_files,
        desc="  AutomatedUserDataset files",
        unit="file",
        leave=False,
    ):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    uid = record.get("automatedUserId")
                    did = record.get("datasetId")
                    if uid is None or did is None or int(uid) not in user_ids:
                        continue
                    user_datasets[int(uid)].add(int(did))
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
    return dict(user_datasets)


def _load_dindex_for_datasets(
    dindex_dir: Path, dataset_ids: Set[int]
) -> Dict[int, List[Tuple[int, float]]]:
    """Load datasetId -> [(year, score), ...] only for the given dataset_ids."""
    ndjson_files = sorted(dindex_dir.glob("*.ndjson"), key=_natural_sort_key)
    by_dataset: Dict[int, List[Tuple[int, float]]] = defaultdict(list)
    for file_path in tqdm(
        ndjson_files, desc="  DIndex files", unit="file", leave=False
    ):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    did = record.get("datasetId")
                    score = record.get("score")
                    year = record.get("year")
                    if did is None or score is None or year is None:
                        continue
                    did = int(did)
                    if did not in dataset_ids:
                        continue
                    by_dataset[did].append((int(year), float(score)))
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
    for ys in tqdm(
        by_dataset.values(),
        desc="  DIndex sort",
        unit="dataset",
        leave=False,
    ):
        ys.sort(key=lambda x: x[0])
    return dict(by_dataset)


def _take_batch(it: Iterator[int], size: int) -> List[int]:
    """Take up to `size` items from iterator."""
    batch: List[int] = []
    for _ in range(size):
        try:
            batch.append(next(it))
        except StopIteration:
            break
    return batch


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


def load_sindex_into_db(output_dir: Path) -> int:
    """Truncate AutomatedUserSIndex and load from NDJSON in output_dir. Returns rows loaded."""
    if not DATABASE_URL:
        print("  ⚠️  DATABASE_URL not set; skipping load into database.")
        return 0
    ndjson_files = sorted(output_dir.glob("*.ndjson"), key=_natural_sort_key)
    if not ndjson_files:
        print("  ⚠️  No NDJSON files found; skipping load.")
        return 0
    now = datetime.now()
    total_loaded = 0
    insert_rows: List[tuple] = []
    with psycopg.connect(DATABASE_URL, autocommit=False) as conn:
        print("  ✅ Connected to database")
        with conn.cursor() as cur:
            cur.execute('TRUNCATE TABLE "AutomatedUserSIndex" RESTART IDENTITY')
            conn.commit()
        print("  🗑️  Truncated AutomatedUserSIndex")
        for file_path in tqdm(ndjson_files, desc="  Load files", unit="file"):
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
    print(f"  ✅ Loaded {total_loaded:,} rows into AutomatedUserSIndex")
    return total_loaded


def main() -> None:
    database_dir = Path.home() / "Downloads" / "database"
    dindex_dir = database_dir / "dindex"
    authors_dir = database_dir / "authors"
    automateduserdataset_dir = database_dir / "automateduserdataset"
    output_dir = database_dir / "automatedusersindex"

    for d, name in [
        (dindex_dir, "dindex"),
        (authors_dir, "authors"),
        (automateduserdataset_dir, "automateduserdataset"),
    ]:
        if not d.exists():
            raise FileNotFoundError(f"Directory not found: {d} ({name})")

    if output_dir.exists():
        import shutil

        shutil.rmtree(output_dir)

    print("📦 Computing AutomatedUserSIndex from files (batched, low memory)...")
    output_dir.mkdir(parents=True, exist_ok=True)
    total_written = 0
    file_number = 0
    current_file = None
    rows_in_current = 0
    current_year = datetime.now().year

    def flush_file():
        nonlocal current_file
        if current_file is not None:
            current_file.close()
            current_file = None

    def write_row(uid: int, score: float, year: int) -> None:
        nonlocal current_file, file_number, rows_in_current, total_written
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

    user_iter = _iter_ordered_user_ids(authors_dir)
    batch_num = 0
    with tqdm(desc="  Batches", unit="batch") as pbar:
        while True:
            batch_user_ids = _take_batch(user_iter, USER_BATCH_SIZE)
            if not batch_user_ids:
                break
            batch_num += 1
            user_ids_set = set(batch_user_ids)
            user_datasets = _load_user_datasets_for_users(
                automateduserdataset_dir, user_ids_set
            )
            dataset_ids = set()
            for uids in user_datasets.values():
                dataset_ids.update(uids)
            dindex_map = _load_dindex_for_datasets(dindex_dir, dataset_ids)

            def dindex_rows_for_datasets(
                ds_ids: Set[int],
            ) -> List[Tuple[int, int, float]]:
                out: List[Tuple[int, int, float]] = []
                for did in ds_ids:
                    for year, score in dindex_map.get(did, []):
                        out.append((did, year, score))
                return out

            for user_id in tqdm(
                batch_user_ids,
                desc="  Users (batch)",
                unit="user",
                leave=False,
            ):
                ds_ids = user_datasets.get(user_id)
                if not ds_ids:
                    continue
                links = [(user_id, did) for did in ds_ids]
                dindex_rows = dindex_rows_for_datasets(ds_ids)
                if not dindex_rows:
                    continue
                sindex_rows = aggregate_sindex_in_python(
                    links, dindex_rows, current_year
                )
                if not sindex_rows:
                    continue
                for uid, score, year in sindex_rows:
                    write_row(uid, score, year)

            del user_datasets
            del dindex_map
            del dataset_ids
            del user_ids_set
            del batch_user_ids
            pbar.update(1)

    flush_file()
    print(f"  ✅ Wrote {total_written:,} rows to {output_dir}")

    print("\n📤 Loading into database...")
    load_sindex_into_db(output_dir)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
