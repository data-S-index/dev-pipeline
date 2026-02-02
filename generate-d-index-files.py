"""Calculate and export d-index for every dataset at multiple time points to NDJSON files.

Uses the same computation as s-index-api's dataset_index_timeseries (sindex.metrics.datasetindex)
so that file-based DBs produce identical d-index values. Schema reference: schema.prisma.

Database (Prisma schema):
  - Dataset: id, publishedAt
  - DatasetTopic: datasetId, topicId (optional; for topic-specific normalization)
  - FujiScore: datasetId, score
  - Citation: datasetId, citedDate, citationWeight
  - Mention: datasetId, mentionedDate, mentionWeight
  - DIndex (output shape): datasetId, score, created

Normalization: input/mock_norm/mock_norm.duckdb (table topic_norm_factors_mock:
  topic_id, year, ft_median, ctw_median, mtw_median). Topic from DatasetTopic.topicId;
  fallback topic_id "ALL" and defaults FT=0.5, CTw=1.0, MTw=1.0 if missing.

Output: NDJSON under output_dir (d-index only: datasetId, score, created); normalization NDJSON under
  norm_dir (datasetId, normalization_factors: {FT, CTw, MTw, topic_id_used, year_used, topic_id_requested, year_requested, used_year_clamp}).
--------------------------------------------------------------------------------
OTHER REPO SETUP (when this file lives in a different repository):
--------------------------------------------------------------------------------
1. Dependencies: psycopg (or psycopg[binary]), tqdm; duckdb if using normalization.
2. Config: DATABASE_URL (e.g. from config import DATABASE_URL).
3. Tables: Dataset (id, publishedAt), FujiScore (datasetId, score), Citation (datasetId, citedDate,
   citationWeight), Mention (datasetId, mentionedDate, mentionWeight), DatasetTopic (datasetId, topicId).
4. Normalization: copy mock_norm.duckdb from s-index-api or set path; topic_norm_factors_mock.
5. Output: d-index NDJSON per batch (datasetId, score, created); normalization NDJSON per batch (datasetId, normalization_factors).
--------------------------------------------------------------------------------
"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import json
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# -----------------------------------------------------------------------------
# Normalization defaults (must match s-index-api for identical d-index values)
# -----------------------------------------------------------------------------
# FT = median FAIR score for topic/year; used to normalize Fi in d-index formula
FT_DEFAULT = 0.5
# CTw = median citation weight for topic/year; normalizes cumulative citation weight
CTw_DEFAULT = 1.0
# MTw = median mention weight for topic/year; normalizes cumulative mention weight
MTw_DEFAULT = 1.0

# -----------------------------------------------------------------------------
# Batch and parallelism
# -----------------------------------------------------------------------------
# How many datasets to fetch from DB per batch; also used as records-per-file threshold
BATCH_SIZE = 10000
# Number of worker processes for d-index computation (0 = single-threaded).
# Set to 4 or more (e.g. os.cpu_count() - 1) to use multiple cores after enabling norm cache.
N_WORKERS = 0

# -----------------------------------------------------------------------------
# Normalization DB (DuckDB) configuration
# -----------------------------------------------------------------------------
# Table name in mock_norm.duckdb; same schema as s-index-api
NORM_TABLE = "topic_norm_factors_mock"
# When topic-specific norms are missing, use norms for this topic_id
FALLBACK_TOPIC_ID = "ALL"
# Sentinel year when publication year is unknown; norm table may have rows for this year
UNKNOWN_YEAR = -1

# OpenAlex topic IDs: DB may store "T12345" or "https://openalex.org/T12345"; norm table may use either
OPENALEX_TOPIC_PREFIX = "https://openalex.org/"


# -----------------------------------------------------------------------------
# OpenAlex topic ID helpers (normalization table may use short or full form)
# -----------------------------------------------------------------------------


def _openalex_topic_id_short(topic_id: Optional[str]) -> Optional[str]:
    """Return short form (e.g. T12345) when topic_id is an OpenAlex URL; else None."""
    if not topic_id or not isinstance(topic_id, str):
        return None
    s = topic_id.strip()
    # Strip the OpenAlex URL prefix to get "T12345"
    if s.startswith(OPENALEX_TOPIC_PREFIX) and len(s) > len(OPENALEX_TOPIC_PREFIX):
        return s[len(OPENALEX_TOPIC_PREFIX) :]
    return None


def _openalex_topic_id_full(topic_id: Optional[str]) -> Optional[str]:
    """Return full URL (e.g. https://openalex.org/T12345) when topic_id looks like short form; else None."""
    if not topic_id or not isinstance(topic_id, str):
        return None
    s = topic_id.strip()
    if s.startswith(("http://", "https://")):
        return None  # already full form
    # Prepend prefix for short IDs like "T12180"; don't turn "ALL" into a URL
    if len(s) > 0 and s != FALLBACK_TOPIC_ID:
        return OPENALEX_TOPIC_PREFIX + s
    return None


# -----------------------------------------------------------------------------
# Normalization factor construction and year clamping
# -----------------------------------------------------------------------------


def _build_normalization_factors(
    FT: float,
    CTw: float,
    MTw: float,
    topic_id_used: str,
    year_used: Optional[int],
    topic_id_requested: Optional[str],
    year_requested: Optional[int],
    used_year_clamp: bool = False,
) -> Dict:
    """Build the normalization_factors dict for output (JSON-serializable)."""
    # Show topic_id_requested in canonical full URL form when it's short (e.g. T12180)
    topic_id_requested_display = topic_id_requested
    if topic_id_requested is not None:
        full_form = _openalex_topic_id_full(topic_id_requested)
        if full_form is not None:
            topic_id_requested_display = full_form
    return {
        "FT": round(FT, 6),
        "CTw": round(CTw, 6),
        "MTw": round(MTw, 6),
        "topic_id_used": topic_id_used,
        "year_used": year_used,
        "topic_id_requested": topic_id_requested_display,
        "year_requested": year_requested,
        "used_year_clamp": used_year_clamp,
    }


def _clamp_year_to_available_range(
    con,
    *,
    table: str,
    topic_id: Optional[str],
    year: int,
) -> Tuple[int, bool]:
    """Clamp year to [min_year, max_year] in table (years >= 0). Returns (year_used, used_clamp)."""
    # Query min/max year for this topic (and fallback ALL) so we can clamp requested year
    if topic_id:
        row = con.execute(
            f"""
            SELECT MIN(year), MAX(year)
            FROM {table}
            WHERE topic_id IN (?, ?) AND year >= 0
            """,
            [topic_id, FALLBACK_TOPIC_ID],
        ).fetchone()
    else:
        row = con.execute(
            f"""
            SELECT MIN(year), MAX(year)
            FROM {table}
            WHERE topic_id = ? AND year >= 0
            """,
            [FALLBACK_TOPIC_ID],
        ).fetchone()
    if not row or row[0] is None or row[1] is None:
        return year, False
    min_y, max_y = int(row[0]), int(row[1])
    if year < min_y:
        return min_y, True
    if year > max_y:
        return max_y, True
    return year, False


def _fetch_norm_row(
    con,
    *,
    table: str,
    topic_id: str,
    year: int,
):
    """Return (ft_median, ctw_median, mtw_median) or None."""
    # Single-row lookup: one (topic_id, year) -> (FT, CTw, MTw)
    row = con.execute(
        f"""
        SELECT ft_median, ctw_median, mtw_median
        FROM {table}
        WHERE topic_id = ? AND year = ?
        LIMIT 1
        """,
        [topic_id, year],
    ).fetchone()
    return row


def _load_norm_cache(
    norm_db_path: Optional[Path],
    table: str = NORM_TABLE,
) -> Tuple[
    Optional[Dict[Tuple[str, int], Tuple[float, float, float]]],
    Optional[Dict[str, Tuple[int, int]]],
]:
    """
    Load the entire normalization table into memory (one DuckDB connection, two queries).
    Returns (norm_cache, year_range_by_topic). norm_cache key is (topic_id, year), value is (ft, ctw, mtw).
    year_range_by_topic[topic_id] = (min_year, max_year) for year clamping.
    """
    if not norm_db_path or not norm_db_path.exists():
        return None, None
    try:
        import duckdb
    except ImportError:
        return None, None
    try:
        # Read-only connection; load all rows in one go for fast lookups later
        with duckdb.connect(str(norm_db_path), read_only=True) as con:
            rows = con.execute(
                f"""
                SELECT topic_id, year, ft_median, ctw_median, mtw_median
                FROM {table}
                WHERE year >= 0
                """
            ).fetchall()
            norm_cache: Dict[Tuple[str, int], Tuple[float, float, float]] = {}
            year_range: Dict[str, Tuple[int, int]] = {}
            for topic_id, year, ft, ctw, mtw in rows:
                tid = str(topic_id).strip()
                y = int(year)
                norm_cache[(tid, y)] = (float(ft), float(ctw), float(mtw))
                # Track min/max year per topic for clamping without hitting DB again
                if tid not in year_range:
                    year_range[tid] = (y, y)
                else:
                    lo, hi = year_range[tid]
                    year_range[tid] = (min(lo, y), max(hi, y))
            return norm_cache, year_range
    except Exception:
        return None, None


def _clamp_year_using_cache(
    year_range_by_topic: Dict[str, Tuple[int, int]],
    topic_id: Optional[str],
    year: int,
) -> Tuple[int, bool]:
    """Clamp year to available range using preloaded year ranges. Returns (year_used, used_clamp)."""
    # Consider both requested topic and ALL for valid year range
    keys = [FALLBACK_TOPIC_ID]
    if topic_id and isinstance(topic_id, str) and topic_id.strip():
        keys.append(topic_id.strip())
    mins = [year_range_by_topic.get(k, (None, None))[0] for k in keys]
    maxs = [year_range_by_topic.get(k, (None, None))[1] for k in keys]
    # Use the union of ranges: smallest min and largest max across topic + ALL
    min_y = (
        min(m for m in mins if m is not None)
        if any(m is not None for m in mins)
        else None
    )
    max_y = (
        max(m for m in maxs if m is not None)
        if any(m is not None for m in maxs)
        else None
    )
    if min_y is None or max_y is None:
        return year, False
    if year < min_y:
        return min_y, True
    if year > max_y:
        return max_y, True
    return year, False


def _get_norm_factors_from_cache(
    norm_cache: Dict[Tuple[str, int], Tuple[float, float, float]],
    year_range_by_topic: Dict[str, Tuple[int, int]],
    topic_id: Optional[str],
    year: Optional[int],
) -> Optional[Dict]:
    """
    Same fallback order as _get_norm_factors_from_duckdb but using in-memory cache (no DB calls).
    """
    # Resolve year: clamp to available range if we have year ranges
    used_year_clamp = False
    if year is None:
        year_used = UNKNOWN_YEAR
        year_requested = year
    else:
        year_requested = int(year)
        year_used, used_year_clamp = _clamp_year_using_cache(
            year_range_by_topic, topic_id, year_requested
        )

    def try_key(tid: str, y: int) -> Optional[Dict]:
        row = norm_cache.get((tid, y))
        if row:
            ft, ctw, mtw = row
            return _build_normalization_factors(
                FT=ft,
                CTw=ctw,
                MTw=mtw,
                topic_id_used=tid,
                year_used=y,
                topic_id_requested=topic_id,
                year_requested=year,
                used_year_clamp=used_year_clamp,
            )
        return None

    # Fallback order: 1) topic as-is 2) full URL 3) short ID 4) ALL; then same with UNKNOWN_YEAR
    topic_id_short = _openalex_topic_id_short(topic_id) if topic_id else None
    topic_id_full = _openalex_topic_id_full(topic_id) if topic_id else None
    if topic_id:
        out = try_key(topic_id, year_used)
        if out:
            return out
    if topic_id_full and topic_id_full != topic_id:
        out = try_key(topic_id_full, year_used)
        if out:
            return out
    if topic_id_short and topic_id_short != topic_id:
        out = try_key(topic_id_short, year_used)
        if out:
            return out
    out = try_key(FALLBACK_TOPIC_ID, year_used)
    if out:
        return out
    # Year fallback: try UNKNOWN_YEAR for each topic variant
    if year is not None:
        if topic_id:
            out = try_key(topic_id, UNKNOWN_YEAR)
            if out:
                return out
        if topic_id_full and topic_id_full != topic_id:
            out = try_key(topic_id_full, UNKNOWN_YEAR)
            if out:
                return out
        if topic_id_short and topic_id_short != topic_id:
            out = try_key(topic_id_short, UNKNOWN_YEAR)
            if out:
                return out
        out = try_key(FALLBACK_TOPIC_ID, UNKNOWN_YEAR)
        if out:
            return out
    return None


def _get_norm_factors_from_duckdb(
    norm_db_path: Optional[Path],
    topic_id: Optional[str],
    year: Optional[int],
    table: str = NORM_TABLE,
    clamp_out_of_range_year: bool = True,
) -> Optional[Dict]:
    """
    Look up FT, CTw, MTw from the normalization DuckDB (same schema as s-index-api).
    Fallback order (matches sindex.metrics.normalization.get_topic_year_norm_factors):
      1) (topic_id, year_used)   full form e.g. https://openalex.org/T12345
      2) (topic_id_short, year_used)  short form e.g. T12345 if norm table uses short IDs
      3) (ALL, year_used)
      4) if year provided: (topic_id, UNKNOWN_YEAR), then (topic_id_short, UNKNOWN_YEAR), then (ALL, UNKNOWN_YEAR)
    If topic_id_used is "ALL" for every record: ensure DatasetTopic.topicId is populated
    and that the norm table has rows for those topic IDs (or short form).
    Returns a full normalization_factors-style dict or None if no row (caller uses defaults).
    """
    if not norm_db_path or not norm_db_path.exists():
        return None
    try:
        import duckdb
    except ImportError:
        return None
    try:
        with duckdb.connect(str(norm_db_path), read_only=True) as con:
            # Resolve year: optionally clamp to [min_year, max_year] in norm table
            used_year_clamp = False
            if year is None:
                year_used = UNKNOWN_YEAR
                year_requested = year
            else:
                year_requested = int(year)
                if clamp_out_of_range_year:
                    year_used, used_year_clamp = _clamp_year_to_available_range(
                        con, table=table, topic_id=topic_id, year=year_requested
                    )
                else:
                    year_used = year_requested

            def try_row(tid: str, y: int):
                r = _fetch_norm_row(con, table=table, topic_id=tid, year=y)
                if r:
                    return _build_normalization_factors(
                        FT=float(r[0]),
                        CTw=float(r[1]),
                        MTw=float(r[2]),
                        topic_id_used=tid,
                        year_used=y,
                        topic_id_requested=topic_id,
                        year_requested=year,
                        used_year_clamp=used_year_clamp,
                    )
                return None

            # Lookup order (matches s-index-api): topic+year, then ALL+year, then UNKNOWN_YEAR variants
            # 1) topic_id + year_used (as stored: "T12180" or "https://openalex.org/T12345")
            topic_id_short = _openalex_topic_id_short(topic_id) if topic_id else None
            topic_id_full = _openalex_topic_id_full(topic_id) if topic_id else None
            if topic_id:
                out = try_row(topic_id, year_used)
                if out:
                    return out
            # 2) full URL form when DB has short form (e.g. T12180) and norm table has full URL
            if topic_id_full and topic_id_full != topic_id:
                out = try_row(topic_id_full, year_used)
                if out:
                    return out
            # 3) short form when DB has full URL and norm table uses short IDs
            if topic_id_short and topic_id_short != topic_id:
                out = try_row(topic_id_short, year_used)
                if out:
                    return out
            # 4) ALL + year_used (topic-agnostic normalization)
            out = try_row(FALLBACK_TOPIC_ID, year_used)
            if out:
                return out
            # 5) UNKNOWN_YEAR fallbacks only when year was provided (year-agnostic row)
            if year is not None:
                if topic_id:
                    out = try_row(topic_id, UNKNOWN_YEAR)
                    if out:
                        return out
                if topic_id_full and topic_id_full != topic_id:
                    out = try_row(topic_id_full, UNKNOWN_YEAR)
                    if out:
                        return out
                if topic_id_short and topic_id_short != topic_id:
                    out = try_row(topic_id_short, UNKNOWN_YEAR)
                    if out:
                        return out
                out = try_row(FALLBACK_TOPIC_ID, UNKNOWN_YEAR)
                if out:
                    return out
    except Exception:
        pass
    return None


# -----------------------------------------------------------------------------
# Date parsing (matches s-index-api behavior)
# -----------------------------------------------------------------------------


def _to_datetime_utc(s: Optional[str]) -> Optional[datetime]:
    """Parse date string to timezone-aware UTC datetime. Returns None if missing/invalid."""
    if not s:
        return None
    try:
        s = s.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _dt_utc_or_today(s: Optional[str], *, today_dt: datetime) -> datetime:
    """Return parsed UTC datetime or today_dt if missing/invalid (matches sindex.core.dates)."""
    dt = _to_datetime_utc(s)
    return dt if dt is not None else today_dt


# -----------------------------------------------------------------------------
# D-index formula (same as s-index-api dataset_index_timeseries)
# -----------------------------------------------------------------------------


def _dataset_index_single(
    Fi: float,
    Ciw: float,
    Miw: float,
    *,
    FT: float = FT_DEFAULT,
    CTw: float = CTw_DEFAULT,
    MTw: float = MTw_DEFAULT,
) -> float:
    """
    Single d-index value: (1/3) * (Fi/FT + Ciw/CTw + Miw/MTw).
    Matches sindex.metrics.datasetindex.dataset_index.
    """
    FT = FT if FT and FT > 0 else FT_DEFAULT
    CTw = CTw if CTw and CTw > 0 else CTw_DEFAULT
    MTw = MTw if MTw and MTw > 0 else MTw_DEFAULT
    return ((Fi / FT) + (Ciw / CTw) + (Miw / MTw)) / 3.0


def dataset_index_timeseries_external(
    *,
    Fi: float,
    citations: List[dict],
    mentions: List[dict],
    pubdate: Optional[str],
    FT: float = FT_DEFAULT,
    CTw: float = CTw_DEFAULT,
    MTw: float = MTw_DEFAULT,
    citation_date_key: str = "citation_date",
    citation_weight_key: str = "citation_weight",
    mention_date_key: str = "mention_date",
    mention_weight_key: str = "mention_weight",
) -> List[dict]:
    """
    Same logic as sindex.metrics.datasetindex.dataset_index_timeseries.
    Returns [{"date": <iso>, "dataset_index": <float>}, ...].
    """
    now_utc = datetime.now(timezone.utc)
    today_dt = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)

    # Collect all citation and mention events with (datetime, type, weight)
    events: List[Tuple[datetime, str, float]] = []  # (dt, type, weight)
    for c in citations:
        dt = _dt_utc_or_today(c.get(citation_date_key), today_dt=today_dt)
        w = float(c.get(citation_weight_key, 0.0) or 0.0)
        events.append((dt, "citation", w))
    for m in mentions:
        dt = _dt_utc_or_today(m.get(mention_date_key), today_dt=today_dt)
        w = float(m.get(mention_weight_key, 0.0) or 0.0)
        events.append((dt, "mention", w))
    events.sort(key=lambda t: t[0])

    # Build list of time points: publication date first, then each event date (deduplicated)
    eval_dates: List[datetime] = []
    seen: set = set()
    pub_dt = _to_datetime_utc(pubdate) if pubdate else None
    if pub_dt is not None:
        eval_dates.append(pub_dt)
        seen.add(pub_dt)
    for dt, _, _ in events:
        if dt not in seen:
            eval_dates.append(dt)
            seen.add(dt)
    # Publication date is always first; remaining dates sorted
    if pub_dt is not None:
        rest = sorted([d for d in eval_dates if d != pub_dt])
        eval_dates = [pub_dt] + rest
    else:
        eval_dates = sorted(eval_dates)
    if not eval_dates:
        eval_dates = [today_dt]

    # For each eval date, accumulate citations/mentions up to that date and compute d-index
    out: List[dict] = []
    ciw, miw = 0.0, 0.0
    i = 0
    for dt in eval_dates:
        # Consume all events on or before this date to get cumulative weights
        while i < len(events) and events[i][0] <= dt:
            _, typ, w = events[i]
            if typ == "citation":
                ciw += w
            else:
                miw += w
            i += 1
        idx = _dataset_index_single(Fi=Fi, Ciw=ciw, Miw=miw, FT=FT, CTw=CTw, MTw=MTw)
        out.append(
            {
                "date": dt.isoformat().replace("+00:00", "Z"),
                "dataset_index": idx,
            }
        )
    return out


def serialize_datetime(obj):
    """Serialize datetime objects to ISO format strings for JSON output."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# -----------------------------------------------------------------------------
# NDJSON output: d-index and normalization files
# -----------------------------------------------------------------------------


def write_batch_to_file(batch: list, file_number: int, output_dir: Path) -> None:
    """Write a batch of d-index records to an NDJSON file (datasetId, score, created only; no normalization)."""
    file_name = f"{file_number}.ndjson"
    file_path = output_dir / file_name
    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            dindex_line = {
                "datasetId": record["datasetId"],
                "score": record["score"],
                "created": record["created"],
            }
            f.write(
                json.dumps(dindex_line, ensure_ascii=False, default=serialize_datetime)
                + "\n"
            )


def write_normalization_batch_to_file(
    d_index_batch: list, file_number: int, norm_dir: Path
) -> None:
    """Write one NDJSON line per unique dataset in d_index_batch to norm_dir/{file_number}.ndjson."""
    # Each dataset may have multiple time points; we output one line per dataset with its norm factors
    seen: set = set()
    norm_lines: list = []
    for record in d_index_batch:
        did = record["datasetId"]
        if did not in seen:
            seen.add(did)
            norm_lines.append(
                {
                    "datasetId": did,
                    "normalization_factors": record["normalization_factors"],
                }
            )
    if not norm_lines:
        return
    file_path = norm_dir / f"{file_number}.ndjson"
    with open(file_path, "w", encoding="utf-8") as f:
        for rec in norm_lines:
            f.write(
                json.dumps(rec, ensure_ascii=False, default=serialize_datetime) + "\n"
            )


# -----------------------------------------------------------------------------
# Single-dataset and chunk processing (used by main and by worker processes)
# -----------------------------------------------------------------------------


def _process_one_dataset_to_records(
    dataset_id: int,
    published_at: Optional[datetime],
    topic_id: Optional[str],
    fair_score: float,
    citations: List[Tuple[Optional[datetime], float]],
    mentions: List[dict],
    norm_cache: Optional[Dict[Tuple[str, int], Tuple[float, float, float]]],
    year_range_by_topic: Optional[Dict[str, Tuple[int, int]]],
    norm_db_path: Optional[Path],
) -> List[dict]:
    """
    Compute d-index time series for one dataset and return list of records (for multiprocessing).
    Normalize: empty topic_id -> None. If norm_cache/year_range_by_topic are set, use cache; else DuckDB.
    """
    # Normalize empty/whitespace topic_id to None
    if topic_id is not None and (not isinstance(topic_id, str) or not topic_id.strip()):
        topic_id = None
    year = published_at.year if published_at else None
    # Prefer in-memory norm cache when available (faster); otherwise query DuckDB
    if norm_cache is not None and year_range_by_topic is not None:
        norm = _get_norm_factors_from_cache(
            norm_cache, year_range_by_topic, topic_id, year
        )
    else:
        norm = (
            _get_norm_factors_from_duckdb(norm_db_path, topic_id, year)
            if norm_db_path
            else None
        )
    if norm is not None:
        FT, CTw, MTw = norm["FT"], norm["CTw"], norm["MTw"]
        normalization_factors = norm
    else:
        # Use defaults when no norm row found (e.g. missing DuckDB or no matching topic/year)
        FT, CTw, MTw = FT_DEFAULT, CTw_DEFAULT, MTw_DEFAULT
        normalization_factors = _build_normalization_factors(
            FT=FT,
            CTw=CTw,
            MTw=MTw,
            topic_id_used=FALLBACK_TOPIC_ID,
            year_used=year,
            topic_id_requested=topic_id,
            year_requested=year,
            used_year_clamp=False,
        )
    d_index_results = process_dataset(
        dataset_id,
        published_at,
        fair_score,
        citations,
        mentions,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )
    records = []
    for time_point, d_index in d_index_results:
        records.append(
            {
                "datasetId": dataset_id,
                "score": d_index,
                "created": (time_point.isoformat() if time_point else None),
                "normalization_factors": normalization_factors,
            }
        )
    return records


def _process_chunk_of_datasets(args: Tuple) -> List[dict]:
    """Worker: process a chunk of datasets and return list of records (for ProcessPoolExecutor)."""
    # Unpack payload (must be picklable: norm_db_path as str, worker converts to Path)
    (
        chunk,
        norm_cache,
        year_range_by_topic,
        norm_db_path,
    ) = args
    norm_db_path = Path(norm_db_path) if norm_db_path else None
    out: List[dict] = []
    for (
        dataset_id,
        published_at,
        topic_id,
        fair_score,
        citations,
        mentions,
    ) in chunk:
        out.extend(
            _process_one_dataset_to_records(
                dataset_id=dataset_id,
                published_at=published_at,
                topic_id=topic_id,
                fair_score=fair_score,
                citations=citations,
                mentions=mentions,
                norm_cache=norm_cache,
                year_range_by_topic=year_range_by_topic,
                norm_db_path=norm_db_path,
            )
        )
    return out


def process_dataset(
    dataset_id: int,
    published_at: Optional[datetime],
    fair_score_raw: float,
    citations: List[Tuple[Optional[datetime], float]],
    mentions: List[dict],
    FT: float = FT_DEFAULT,
    CTw: float = CTw_DEFAULT,
    MTw: float = MTw_DEFAULT,
) -> List[Tuple[datetime, float]]:
    """
    Calculate d-index at all time points using same logic as dataset_index_timeseries.
    FAIR score is normalized to [0,1] as Fi = fair_score_raw / 100.
    """
    # Fi in [0, 1] as in s-index-api jobs (FAIR score 0–100 -> 0–1)
    Fi = (float(fair_score_raw) / 100.0) if fair_score_raw is not None else 0.0
    Fi = max(0.0, min(1.0, Fi))

    # Convert to format expected by dataset_index_timeseries_external (ISO strings + dicts)
    pubdate_iso = published_at.isoformat() if published_at else None
    citations_list = [
        {
            "citation_date": (d.isoformat() if d else None),
            "citation_weight": w,
        }
        for d, w in citations
    ]
    series = dataset_index_timeseries_external(
        Fi=Fi,
        citations=citations_list,
        mentions=mentions,
        pubdate=pubdate_iso,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )
    return [
        (
            datetime.fromisoformat(rec["date"].replace("Z", "+00:00")),
            rec["dataset_index"],
        )
        for rec in series
    ]


def main() -> None:
    """Main function to calculate and export d-index for all datasets to NDJSON files."""
    print("🚀 Starting d-index calculation process...")

    # -------------------------------------------------------------------------
    # Paths: output and normalization directories; DuckDB path for norm factors
    # -------------------------------------------------------------------------
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    output_dir = downloads_dir / "database" / "dindex"
    norm_dir = downloads_dir / "database" / "normalization"
    # Prefer s-index-api layout (input/mock_norm/mock_norm.duckdb); fallback for other repo.
    norm_db_path = Path.cwd() / "input" / "mock_norm" / "mock_norm.duckdb"
    if not norm_db_path.exists():
        norm_db_path = Path.cwd() / "mock_norm.duckdb"
    if norm_db_path.exists():
        print(f"  Normalization DB: {norm_db_path}")
    else:
        norm_db_path = None
        print("  Normalization DB: not found (using default FT, CTw, MTw)")

    # Preload normalization table into memory so we avoid per-dataset DuckDB lookups
    norm_cache: Optional[Dict[Tuple[str, int], Tuple[float, float, float]]] = None
    year_range_by_topic: Optional[Dict[str, Tuple[int, int]]] = None
    if norm_db_path:
        print("  Loading normalization table into memory...")
        norm_cache, year_range_by_topic = _load_norm_cache(norm_db_path)
        if norm_cache is not None:
            print(f"  Loaded {len(norm_cache):,} norm rows")
        else:
            print("  Norm cache failed (will use per-dataset DuckDB lookups)")

    print(f"Output directory: {output_dir}")
    print(f"Normalization directory: {norm_dir}")

    # -------------------------------------------------------------------------
    # Prepare output directories (clean and create)
    # -------------------------------------------------------------------------
    import shutil

    if output_dir.exists():
        shutil.rmtree(output_dir)
        print("✓ Output directory cleaned")
    else:
        print("✓ Output directory not found")

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    print("✓ Output directory ready")

    # Clean and create norm_dir for normalization NDJSON (one file per d-index batch)
    if norm_dir.exists():
        shutil.rmtree(norm_dir)
        print("✓ Normalization directory cleaned")
    norm_dir.mkdir(parents=True, exist_ok=True)
    print("✓ Normalization directory ready")

    # -------------------------------------------------------------------------
    # Database connection and batch loop
    # -------------------------------------------------------------------------
    print("\n🔌 Connecting to database...")
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            print("  ✅ Connected to database")

            # Get max dataset ID (since it's autoincrement)
            print("\n📊 Getting dataset ID range...")
            with conn.cursor() as cur:
                cur.execute('SELECT MAX(id) FROM "Dataset"')
                max_dataset_id = cur.fetchone()[0] or 0

            total_datasets = max_dataset_id
            print(
                f"  Processing {total_datasets:,} datasets (max ID: {max_dataset_id})"
            )

            # Process datasets in batches
            print(
                f"\n📈 Calculating d-index values (processing {BATCH_SIZE:,} datasets per batch)..."
            )
            if N_WORKERS > 0:
                print(
                    f"  Using {N_WORKERS} worker process(es) for parallel computation."
                )
            total_records = 0
            processed_datasets = 0
            file_number = 1
            current_batch = (
                []
            )  # Accumulates records until we hit BATCH_SIZE, then write to file
            current_id = (
                1  # Dataset ID range: process [current_id, batch_end] per DB batch
            )

            with conn.cursor() as cur:
                # Create progress bar for datasets
                pbar = tqdm(
                    total=total_datasets,
                    desc="  Processing datasets",
                    unit="dataset",
                    unit_scale=True,
                )

                # Process in batches by dataset ID range
                while current_id <= max_dataset_id:
                    batch_end = min(current_id + BATCH_SIZE - 1, max_dataset_id)

                    # Fetch publishedAt and topicId (DatasetTopic) for datasets in this batch
                    cur.execute(
                        """
                        SELECT d.id, d."publishedAt", dt."topicId"
                        FROM "Dataset" d
                        LEFT JOIN "DatasetTopic" dt ON d.id = dt."datasetId"
                        WHERE d.id >= %s AND d.id <= %s
                        ORDER BY d.id
                    """,
                        (current_id, batch_end),
                    )
                    datasets_batch = cur.fetchall()

                    # Fetch FAIR scores for this batch
                    fair_scores = {}
                    if datasets_batch:
                        cur.execute(
                            """
                            SELECT "datasetId", score
                            FROM "FujiScore"
                            WHERE "datasetId" >= %s AND "datasetId" <= %s
                        """,
                            (current_id, batch_end),
                        )
                        for dataset_id, score in cur.fetchall():
                            fair_scores[dataset_id] = (
                                score if score is not None else 0.0
                            )

                    # Fetch citations for this batch
                    citations_by_dataset = {}
                    if datasets_batch:
                        cur.execute(
                            """
                            SELECT "datasetId", "citedDate", "citationWeight"
                            FROM "Citation"
                            WHERE "datasetId" >= %s AND "datasetId" <= %s
                            ORDER BY "datasetId", "citedDate" NULLS LAST
                        """,
                            (current_id, batch_end),
                        )
                        citations = cur.fetchall()
                        for dataset_id, cited_date, citation_weight in citations:
                            if dataset_id not in citations_by_dataset:
                                citations_by_dataset[dataset_id] = []
                            citations_by_dataset[dataset_id].append(
                                (cited_date, citation_weight)
                            )

                    # Fetch mentions for this batch (schema: Mention.mentionedDate, mentionWeight)
                    mentions_by_dataset: Dict[int, List[dict]] = {}
                    if datasets_batch:
                        cur.execute(
                            """
                            SELECT "datasetId", "mentionedDate", "mentionWeight"
                            FROM "Mention"
                            WHERE "datasetId" >= %s AND "datasetId" <= %s
                            ORDER BY "datasetId", "mentionedDate" NULLS LAST
                        """,
                            (current_id, batch_end),
                        )
                        for (
                            dataset_id,
                            mentioned_date,
                            mention_weight,
                        ) in cur.fetchall():
                            if dataset_id not in mentions_by_dataset:
                                mentions_by_dataset[dataset_id] = []
                            mentions_by_dataset[dataset_id].append(
                                {
                                    "mention_date": (
                                        mentioned_date.isoformat()
                                        if mentioned_date
                                        else None
                                    ),
                                    "mention_weight": float(
                                        mention_weight
                                        if mention_weight is not None
                                        else 1.0
                                    ),
                                }
                            )

                    # Build list of records for this batch (parallel or sequential)
                    batch_records: List[dict] = []
                    if N_WORKERS > 0:
                        # Build payload: list of (dataset_id, published_at, topic_id, fair_score, citations, mentions)
                        rows = []
                        for dataset_id, published_at, topic_id in datasets_batch:
                            fair_score = fair_scores.get(dataset_id, 0.0)
                            citations = citations_by_dataset.get(dataset_id, [])
                            mentions = mentions_by_dataset.get(dataset_id, [])
                            rows.append(
                                (
                                    dataset_id,
                                    published_at,
                                    topic_id,
                                    fair_score,
                                    citations,
                                    mentions,
                                )
                            )
                        chunk_size = max(1, len(rows) // N_WORKERS)
                        chunk_payloads = []
                        for i in range(0, len(rows), chunk_size):
                            chunk = rows[i : i + chunk_size]
                            chunk_payloads.append(
                                (
                                    chunk,
                                    norm_cache,
                                    year_range_by_topic,
                                    str(norm_db_path) if norm_db_path else None,
                                )
                            )
                        # norm_db_path must be passed as str for pickling; worker will use Path
                        with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
                            for rec_list in executor.map(
                                _process_chunk_of_datasets, chunk_payloads
                            ):
                                batch_records.extend(rec_list)
                    else:
                        # Sequential path
                        for dataset_id, published_at, topic_id in datasets_batch:
                            if topic_id is not None and (
                                not isinstance(topic_id, str) or not topic_id.strip()
                            ):
                                topic_id = None
                            fair_score = fair_scores.get(dataset_id, 0.0)
                            citations = citations_by_dataset.get(dataset_id, [])
                            mentions = mentions_by_dataset.get(dataset_id, [])
                            year = published_at.year if published_at else None
                            if norm_cache is not None and year_range_by_topic:
                                norm = _get_norm_factors_from_cache(
                                    norm_cache,
                                    year_range_by_topic,
                                    topic_id,
                                    year,
                                )
                            else:
                                norm = (
                                    _get_norm_factors_from_duckdb(
                                        norm_db_path, topic_id, year
                                    )
                                    if norm_db_path
                                    else None
                                )
                            if norm is not None:
                                FT = norm["FT"]
                                CTw = norm["CTw"]
                                MTw = norm["MTw"]
                                normalization_factors = norm
                            else:
                                FT = FT_DEFAULT
                                CTw = CTw_DEFAULT
                                MTw = MTw_DEFAULT
                                normalization_factors = _build_normalization_factors(
                                    FT=FT,
                                    CTw=CTw,
                                    MTw=MTw,
                                    topic_id_used=FALLBACK_TOPIC_ID,
                                    year_used=year,
                                    topic_id_requested=topic_id,
                                    year_requested=year,
                                    used_year_clamp=False,
                                )
                            d_index_results = process_dataset(
                                dataset_id,
                                published_at,
                                fair_score,
                                citations,
                                mentions,
                                FT=FT,
                                CTw=CTw,
                                MTw=MTw,
                            )
                            for time_point, d_index in d_index_results:
                                batch_records.append(
                                    {
                                        "datasetId": dataset_id,
                                        "score": d_index,
                                        "created": (
                                            time_point.isoformat()
                                            if time_point
                                            else None
                                        ),
                                        "normalization_factors": normalization_factors,
                                    }
                                )

                    # Drain batch_records into current_batch; flush to NDJSON when full
                    for record in batch_records:
                        current_batch.append(record)
                        total_records += 1
                        if len(current_batch) >= BATCH_SIZE:
                            write_batch_to_file(current_batch, file_number, output_dir)
                            write_normalization_batch_to_file(
                                current_batch, file_number, norm_dir
                            )
                            file_number += 1
                            current_batch = []

                    processed_datasets += len(datasets_batch)
                    pbar.update(len(datasets_batch))

                    # Advance to next ID range for next DB batch
                    current_id = batch_end + 1

                pbar.close()

                # Flush any remaining records (last partial batch) to final NDJSON file
                if current_batch:
                    write_batch_to_file(current_batch, file_number, output_dir)
                    write_normalization_batch_to_file(
                        current_batch, file_number, norm_dir
                    )

            print("\n✅ d-index calculation completed!")
            print("📊 Summary:")
            print(f"  - Datasets processed: {processed_datasets:,}")
            print(f"  - D-index records exported: {total_records:,}")
            print(f"  - Output files created: {file_number} (d-index + normalization)")
            print(f"🎉 Exported files are available in: {output_dir}")
            print(f"   Normalization NDJSON in: {norm_dir}")

    except psycopg.Error as e:
        print(f"\n❌ Database error: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        raise


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
