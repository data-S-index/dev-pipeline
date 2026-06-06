"""Calculate and export d-index for every dataset at multiple time points to NDJSON files.

Uses the same computation as s-index-api's dataset_index_timeseries (sindex.metrics.datasetindex)
so that file-based DBs produce identical d-index values. Schema reference: schema.prisma.

Database (Prisma schema):
  - Dataset: id, publishedAt
  - DatasetTopic: datasetId, topicId (optional; for topic-specific normalization)
  - FujiScore: datasetId, score
  - Citation: datasetId, citedDate, citationWeight
  - Mention: datasetId, mentionedDate, mentionWeight
  - DIndex (output shape): datasetId, score, year

Normalization: Subfield+year via input/subfield_norm_factors.duckdb and
  input/openalex_topic_mapping_table.csv (required; no default). Run from s-index-api repo.

Output: NDJSON under output_dir (d-index per time point: datasetId, score, year);
  time points are Dec 31 of each year from first (published) through last full year, then today if not Dec 31.
  Normalization NDJSON under norm_dir (datasetId, normalization_factors: {FT, CTw, MTw, ...}).
--------------------------------------------------------------------------------
OTHER REPO SETUP (when this file lives in a different repository):
--------------------------------------------------------------------------------
1. Dependencies: psycopg (or psycopg[binary]), tqdm; duckdb if using normalization.
2. Config: DATABASE_URL (e.g. from config import DATABASE_URL).
3. Tables: Dataset (id, publishedAt), FujiScore (datasetId, score), Citation (datasetId, citedDate,
   citationWeight), Mention (datasetId, mentionedDate, mentionWeight), DatasetTopic (datasetId, topicId).
4. Normalization: required; no default. Run this script from s-index-api (input/subfield_norm_factors.duckdb + input/openalex_topic_mapping_table.csv).
5. Output: d-index NDJSON per batch (datasetId, score, year); normalization NDJSON per batch (datasetId, normalization_factors).
--------------------------------------------------------------------------------
"""

import csv
import json
import re
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Subfield normalization (local only; this file may live in a different repo).
_HAS_SUBFIELD_NORM = True


def load_topic_to_subfield_cache(file_path: str) -> Dict[str, str]:
    """Load OpenAlex topic_id -> subfield_id mapping from CSV once. Key is digits-only topic id."""
    cache: Dict[str, str] = {}
    with open(file_path, mode="r", encoding="utf-8-sig") as f:
        content = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(content)
            reader = csv.DictReader(f, dialect=dialect)
        except csv.Error:
            reader = csv.DictReader(f, delimiter="\t")
        reader.fieldnames = (
            [name.strip() for name in reader.fieldnames] if reader.fieldnames else []
        )
        if "topic_id" not in reader.fieldnames:
            available = ", ".join(reader.fieldnames)
            raise KeyError(
                f"Column 'topic_id' not found. Available columns: {available}"
            )
        for row in reader:
            tid = row.get("topic_id") and row["topic_id"].strip()
            if tid:
                normalized_id = re.sub(r"\D", "", tid)
                if normalized_id:
                    cache[normalized_id] = str(row.get("subfield_id", "")).strip()
    return cache


def get_subfield_id_from_topic_id(file_path: str, topic_id: str | int) -> Optional[str]:
    """Resolve topic_id -> subfield_id via OpenAlex mapping CSV."""
    normalized_id = re.sub(r"\D", "", str(topic_id))
    with open(file_path, mode="r", encoding="utf-8-sig") as f:
        content = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(content)
            reader = csv.DictReader(f, dialect=dialect)
        except csv.Error:
            reader = csv.DictReader(f, delimiter="\t")
        reader.fieldnames = (
            [name.strip() for name in reader.fieldnames] if reader.fieldnames else []
        )
        if "topic_id" not in reader.fieldnames:
            available = ", ".join(reader.fieldnames)
            raise KeyError(
                f"Column 'topic_id' not found. Available columns: {available}"
            )
        for row in reader:
            if row["topic_id"] and row["topic_id"].strip() == normalized_id:
                return str(row["subfield_id"]).strip()
    return None


def get_subfield_id_cached(
    topic_to_subfield: Dict[str, str], topic_id: Optional[str]
) -> Optional[str]:
    """Look up subfield_id from preloaded topic->subfield cache (no I/O)."""
    if not topic_id or not topic_to_subfield:
        return None
    normalized_id = re.sub(r"\D", "", str(topic_id))
    return topic_to_subfield.get(normalized_id)


# Subfield norm cache: list of (subfield_id, pubyear, FT, CTw, MTw) for in-memory "closest past year" lookup.
SubfieldNormRow = Tuple[str, int, float, float, float]
# Indexed cache: subfield_id -> list of (pubyear, FT, CTw, MTw) sorted by pubyear desc for fast lookup.
SubfieldNormCacheIndexed = Dict[str, List[Tuple[int, float, float, float]]]


def _safe_float(value, default: float) -> float:
    """Convert value to float; use default if None (DuckDB NULL)."""
    return float(value) if value is not None else default


def load_subfield_norm_cache(db_path: str) -> List[SubfieldNormRow]:
    """Load full normalization_factors_subfields_floored table from DuckDB once.
    NULLs in median columns are coerced to defaults (matches FT_DEFAULT, CTw_DEFAULT, MTw_DEFAULT).
    """
    import duckdb

    query = """
    SELECT subfield_id, pubyear, median_fair_score_3yr, median_cit_weight_3yr, median_men_weight_3yr
    FROM normalization_factors_subfields_floored
    WHERE pubyear >= 0
    """
    with duckdb.connect(db_path, read_only=True) as con:
        rows = con.execute(query).fetchall()
    # Coerce NULLs so we don't fail on float(None)
    return [
        (
            str(r[0]),
            int(r[1]) if r[1] is not None else 0,
            _safe_float(r[2], 13.46),
            _safe_float(r[3], 1.0),
            _safe_float(r[4], 1.0),
        )
        for r in rows
    ]


def index_subfield_norm_cache(
    cache: List[SubfieldNormRow],
) -> SubfieldNormCacheIndexed:
    """Group norm rows by subfield_id; each list (pubyear, FT, CTw, MTw) sorted by pubyear descending."""
    indexed: SubfieldNormCacheIndexed = {}
    for sid, py, ft, ctw, mtw in cache:
        if sid not in indexed:
            indexed[sid] = []
        indexed[sid].append((py, ft, ctw, mtw))
    for sid in indexed:
        indexed[sid].sort(key=lambda t: t[0], reverse=True)  # newest first
    return indexed


def get_subfield_year_norm_from_cache(
    cache: List[SubfieldNormRow], subfield_id: str, pubyear: int
) -> Optional[Dict]:
    """
    Same logic as get_subfield_year_norm_factors but using preloaded list.
    Finds row with subfield_id and pubyear <= pubyear, max(pubyear); falls back to DEFAULT.
    """
    # Best match for requested subfield: pubyear <= pubyear, max pubyear
    best_ft = best_ctw = best_mtw = None
    best_pubyear = -1
    for sid, py, ft, ctw, mtw in cache:
        if sid == subfield_id and py <= pubyear and py > best_pubyear:
            best_pubyear = py
            best_ft, best_ctw, best_mtw = ft, ctw, mtw
    # Fallback: DEFAULT
    def_ft = def_ctw = def_mtw = None
    def_pubyear = -1
    for sid, py, ft, ctw, mtw in cache:
        if sid == "DEFAULT" and py <= pubyear and py > def_pubyear:
            def_pubyear = py
            def_ft, def_ctw, def_mtw = ft, ctw, mtw
    FT = best_ft if best_ft is not None else def_ft
    CTw = best_ctw if best_ctw is not None else def_ctw
    MTw = best_mtw if best_mtw is not None else def_mtw
    if FT is None or CTw is None or MTw is None:
        return None
    n_year_gap = (
        (pubyear - best_pubyear) if best_pubyear >= 0 else (pubyear - def_pubyear)
    )
    if best_pubyear < 0:
        method = "Default"
    elif pubyear == best_pubyear:
        method = "Exact Year"
    else:
        method = "Closest Past Year"
    return {
        "FT": FT,
        "CTw": CTw,
        "MTw": MTw,
        "n_year_gap": n_year_gap,
        "method": method,
    }


def get_subfield_year_norm_from_indexed_cache(
    indexed: SubfieldNormCacheIndexed, subfield_id: str, pubyear: int
) -> Optional[Dict]:
    """Faster lookup using pre-indexed cache (by subfield_id). Same semantics as get_subfield_year_norm_from_cache."""

    def find_closest(
        rows: List[Tuple[int, float, float, float]],
    ) -> Optional[Tuple[int, float, float, float]]:
        for py, ft, ctw, mtw in rows:
            if py <= pubyear:
                return (py, ft, ctw, mtw)
        return None

    best = find_closest(indexed.get(subfield_id, []))
    default_rows = indexed.get("DEFAULT", [])
    def_row = find_closest(default_rows)
    FT = (best[1] if best else def_row[1]) if (best or def_row) else None
    CTw = (best[2] if best else def_row[2]) if (best or def_row) else None
    MTw = (best[3] if best else def_row[3]) if (best or def_row) else None
    if FT is None or CTw is None or MTw is None:
        return None
    best_pubyear = best[0] if best else -1
    def_pubyear = def_row[0] if def_row else -1
    n_year_gap = (
        (pubyear - best_pubyear) if best_pubyear >= 0 else (pubyear - def_pubyear)
    )
    if best_pubyear < 0:
        method = "Default"
    elif pubyear == best_pubyear:
        method = "Exact Year"
    else:
        method = "Closest Past Year"
    return {
        "FT": FT,
        "CTw": CTw,
        "MTw": MTw,
        "n_year_gap": n_year_gap,
        "method": method,
    }


def get_subfield_year_norm_factors(db_path, subfield_id, pubyear):
    """Return normalization factors for subfield_id and pubyear from DuckDB."""
    import duckdb

    query = """
    SELECT 
        COALESCE(ns.median_fair_score_3yr, ns_def.median_fair_score_3yr) as FT,
        COALESCE(ns.median_cit_weight_3yr, ns_def.median_cit_weight_3yr) as CTw,
        COALESCE(ns.median_men_weight_3yr, ns_def.median_men_weight_3yr) as MTw,
        (? - ns.pubyear) as n_year_gap,
        CASE 
            WHEN ns.subfield_id IS NULL THEN 'Default'
            WHEN ? = ns.pubyear THEN 'Exact Year'
            ELSE 'Closest Past Year'
        END as method
    FROM (SELECT 1)
    LEFT JOIN (
        SELECT * FROM normalization_factors_subfields_floored 
        WHERE subfield_id = ? AND pubyear <= ? 
        ORDER BY pubyear DESC LIMIT 1
    ) ns ON true
    LEFT JOIN (
        SELECT * FROM normalization_factors_subfields_floored 
        WHERE subfield_id = 'DEFAULT'
    ) ns_def ON true
    """
    params = [pubyear, pubyear, subfield_id, pubyear]
    with duckdb.connect(db_path) as con:
        result = con.execute(query, params).fetchone()
    if not result:
        return None
    return {
        "FT": result[0],
        "CTw": result[1],
        "MTw": result[2],
        "n_year_gap": result[3],
        "method": result[4],
    }


# Same defaults as dataset_index_series_from_doi (sindex.metrics.jobs) / datasetindex.dataset_index
FT_DEFAULT = 13.46
CTw_DEFAULT = 1.0
MTw_DEFAULT = 1.0

# Batch processing configuration
BATCH_SIZE = 10000
# Number of worker processes for d-index computation (0 = single-threaded).
N_WORKERS = 0

# Normalization: required (subfield norm); no default. Run from s-index-api.
NORM_TABLE = "topic_norm_factors_mock"  # used only by legacy helpers kept for reference
FALLBACK_TOPIC_ID = "ALL"
UNKNOWN_YEAR = -1

# OpenAlex topic IDs: DB may store "T12345" or "https://openalex.org/T12345"; norm table may use either
OPENALEX_TOPIC_PREFIX = "https://openalex.org/"


def _openalex_topic_id_short(topic_id: Optional[str]) -> Optional[str]:
    """Return short form (e.g. T12345) when topic_id is an OpenAlex URL; else None."""
    if not topic_id or not isinstance(topic_id, str):
        return None
    s = topic_id.strip()
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
    if len(s) > 0 and s != FALLBACK_TOPIC_ID:
        return OPENALEX_TOPIC_PREFIX + s
    return None


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


def _build_normalization_factors_subfield(
    norm_result: Dict,
    subfield_id_used: str,
    year_used: Optional[int],
    topic_id_requested: Optional[str],
    year_requested: Optional[int],
) -> Dict:
    """Build normalization_factors dict from get_subfield_year_norm_factors result (matches API)."""
    topic_id_requested_display = topic_id_requested
    if topic_id_requested is not None:
        full_form = _openalex_topic_id_full(topic_id_requested)
        if full_form is not None:
            topic_id_requested_display = full_form
    out = {
        "FT": round(float(norm_result["FT"]), 6),
        "CTw": round(float(norm_result["CTw"]), 6),
        "MTw": round(float(norm_result["MTw"]), 6),
        "topic_id_used": subfield_id_used,
        "year_used": year_used,
        "topic_id_requested": topic_id_requested_display,
        "year_requested": year_requested,
        "used_year_clamp": False,
    }
    if norm_result.get("method") is not None:
        out["method"] = norm_result["method"]
    return out


def _get_norm_factors_subfield(
    norm_db_path: Optional[Path],
    topics_table_path: Optional[Path],
    topic_id: Optional[str],
    year: Optional[int],
) -> Optional[Dict]:
    """
    Look up FT, CTw, MTw using subfield normalization (same as dataset_index_series_from_doi).
    Resolves topic_id -> subfield_id via OpenAlex mapping; uses get_subfield_year_norm_factors.
    Returns normalization_factors-style dict or None (caller uses defaults).
    """
    if not _HAS_SUBFIELD_NORM or not norm_db_path or not norm_db_path.exists():
        return None
    if year is None:
        return None
    year = int(year)
    subfield_id = None
    if topic_id and topics_table_path and topics_table_path.exists():
        try:
            subfield_id = get_subfield_id_from_topic_id(
                str(topics_table_path), topic_id
            )
        except Exception:
            pass
    lookup_subfield = subfield_id if subfield_id else "DEFAULT"
    try:
        norm = get_subfield_year_norm_factors(str(norm_db_path), lookup_subfield, year)
    except Exception:
        return None
    if not norm:
        return None
    return _build_normalization_factors_subfield(
        norm,
        subfield_id_used=lookup_subfield,
        year_used=year,
        topic_id_requested=topic_id,
        year_requested=year,
    )


def _get_norm_factors_subfield_cached(
    topic_to_subfield: Dict[str, str],
    subfield_norm_cache: Union[List[SubfieldNormRow], SubfieldNormCacheIndexed],
    topic_id: Optional[str],
    year: Optional[int],
) -> Optional[Dict]:
    """
    Same as _get_norm_factors_subfield but uses preloaded caches (no file/DB I/O).
    subfield_norm_cache can be list (SubfieldNormRow) or indexed dict for faster lookups.
    """
    if year is None:
        return None
    year = int(year)
    subfield_id = get_subfield_id_cached(topic_to_subfield, topic_id)
    lookup_subfield = subfield_id if subfield_id else "DEFAULT"
    if isinstance(subfield_norm_cache, dict):
        norm = get_subfield_year_norm_from_indexed_cache(
            subfield_norm_cache, lookup_subfield, year
        )
    else:
        norm = get_subfield_year_norm_from_cache(
            subfield_norm_cache, lookup_subfield, year
        )
    if not norm:
        return None
    return _build_normalization_factors_subfield(
        norm,
        subfield_id_used=lookup_subfield,
        year_used=year,
        topic_id_requested=topic_id,
        year_requested=year,
    )


def _clamp_year_to_available_range(
    con,
    *,
    table: str,
    topic_id: Optional[str],
    year: int,
) -> Tuple[int, bool]:
    """Clamp year to [min_year, max_year] in table (years >= 0). Returns (year_used, used_clamp)."""
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
    keys = [FALLBACK_TOPIC_ID]
    if topic_id and isinstance(topic_id, str) and topic_id.strip():
        keys.append(topic_id.strip())
    mins = [year_range_by_topic.get(k, (None, None))[0] for k in keys]
    maxs = [year_range_by_topic.get(k, (None, None))[1] for k in keys]
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
            # 4) ALL + year_used
            out = try_row(FALLBACK_TOPIC_ID, year_used)
            if out:
                return out
            # 5) UNKNOWN_YEAR fallbacks only when year was provided
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


def dataset_index_year_timeseries_external(
    *,
    Fi: float,
    citations: List[dict],
    mentions: List[dict],
    pubyear: Optional[int],
    FT: float = FT_DEFAULT,
    CTw: float = CTw_DEFAULT,
    MTw: float = MTw_DEFAULT,
    citation_year_key: str = "citation_year",
    citation_weight_key: str = "citation_weight",
    mention_year_key: str = "mention_year",
    mention_weight_key: str = "mention_weight",
) -> List[dict]:
    """
    Same logic as sindex.metrics.datasetindex.dataset_index_year_timeseries (per-year only).
    Returns [{"year": <int>, "dataset_index": <float>}, ...]. Matches dataset_index_series_from_doi.
    """
    citations = citations or []
    mentions = mentions or []
    current_year = datetime.now(timezone.utc).year

    events: List[Tuple[int, str, float]] = []  # (year, type, weight)
    for c in citations:
        yr = c.get(citation_year_key)
        yr = int(yr) if yr is not None else current_year
        w = float(c.get(citation_weight_key, 0.0) or 0.0)
        events.append((yr, "citation", w))
    for m in mentions:
        yr = m.get(mention_year_key)
        yr = int(yr) if yr is not None else current_year
        w = float(m.get(mention_weight_key, 0.0) or 0.0)
        events.append((yr, "mention", w))
    events.sort(key=lambda t: t[0])

    eval_years: List[int] = []
    seen: set = set()
    if pubyear is not None:
        eval_years.append(pubyear)
        seen.add(pubyear)
    for yr, _, _ in events:
        if yr not in seen:
            eval_years.append(yr)
            seen.add(yr)
    if pubyear is not None:
        rest = sorted([y for y in eval_years if y != pubyear])
        eval_years = [pubyear] + rest
    else:
        eval_years = sorted(eval_years)
    if not eval_years:
        eval_years = [current_year]

    out: List[dict] = []
    ciw, miw = 0.0, 0.0
    i = 0
    for yr in eval_years:
        while i < len(events) and events[i][0] <= yr:
            _, typ, w = events[i]
            if typ == "citation":
                ciw += w
            else:
                miw += w
            i += 1
        idx = _dataset_index_single(Fi=Fi, Ciw=ciw, Miw=miw, FT=FT, CTw=CTw, MTw=MTw)
        out.append({"year": yr, "dataset_index": idx})
    return out


def serialize_datetime(obj):
    """Serialize datetime objects to ISO format strings."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def write_batch_to_file(batch: list, file_number: int, output_dir: Path) -> None:
    """Write a batch of d-index records to an NDJSON file (datasetId, score, year only; no normalization)."""
    file_name = f"{file_number}.ndjson"
    file_path = output_dir / file_name
    with open(file_path, "w", encoding="utf-8") as f:
        for record in batch:
            dindex_line = {
                "datasetId": record["datasetId"],
                "score": record["score"],
                "year": record["year"],
            }
            f.write(
                json.dumps(dindex_line, ensure_ascii=False, default=serialize_datetime)
                + "\n"
            )


def write_normalization_batch_to_file(
    d_index_batch: list, file_number: int, norm_dir: Path
) -> None:
    """Write one NDJSON line per unique dataset in d_index_batch to norm_dir/{file_number}.ndjson."""
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
    use_subfield_norm: bool = False,
    topics_table_path: Optional[Path] = None,
    topic_to_subfield: Optional[Dict[str, str]] = None,
    subfield_norm_cache: Optional[
        Union[List[SubfieldNormRow], SubfieldNormCacheIndexed]
    ] = None,
) -> List[dict]:
    """
    Compute d-index time series for one dataset and return list of records (for multiprocessing).
    Normalize: if use_subfield_norm use subfield (cached if topic_to_subfield/subfield_norm_cache
    provided, else DB/CSV); else if norm_cache/year_range_by_topic set use cache; else topic DuckDB.
    """
    if topic_id is not None and (not isinstance(topic_id, str) or not topic_id.strip()):
        topic_id = None
    year = published_at.year if published_at else None
    if use_subfield_norm:
        if topic_to_subfield is not None and subfield_norm_cache is not None:
            norm = _get_norm_factors_subfield_cached(
                topic_to_subfield, subfield_norm_cache, topic_id, year
            )
        else:
            norm = _get_norm_factors_subfield(
                norm_db_path, topics_table_path, topic_id, year
            )
    else:
        norm = None
    if norm is not None:
        FT, CTw, MTw = norm["FT"], norm["CTw"], norm["MTw"]
        normalization_factors = norm
    else:
        FT, CTw, MTw = FT_DEFAULT, CTw_DEFAULT, MTw_DEFAULT
        normalization_factors = _build_normalization_factors(
            FT=FT,
            CTw=CTw,
            MTw=MTw,
            topic_id_used=FALLBACK_TOPIC_ID if not use_subfield_norm else "DEFAULT",
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
                "year": (time_point.year if time_point else None),
                "created": (time_point.isoformat() if time_point else None),
                "normalization_factors": normalization_factors,
            }
        )
    return records


def _process_chunk_of_datasets(args: Tuple) -> List[dict]:
    """Worker: process a chunk of datasets and return list of records (for ProcessPoolExecutor)."""
    (
        chunk,
        norm_cache,
        year_range_by_topic,
        norm_db_path,
        use_subfield_norm,
        topics_table_path,
        topic_to_subfield,
        subfield_norm_cache,
    ) = args
    norm_db_path = Path(norm_db_path) if norm_db_path else None
    topics_table_path = Path(topics_table_path) if topics_table_path else None
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
                use_subfield_norm=use_subfield_norm,
                topics_table_path=topics_table_path,
                topic_to_subfield=topic_to_subfield,
                subfield_norm_cache=subfield_norm_cache,
            )
        )
    return out


def _year_from_date(d: Optional[datetime]) -> Optional[int]:
    """Extract calendar year from datetime/date or None."""
    if d is None:
        return None
    if hasattr(d, "year"):
        return int(d.year)
    return None


def _year_from_date_str(s: Optional[str]) -> Optional[int]:
    """Extract calendar year from ISO date string (YYYY-MM-DD...) or None."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()[:4]
    if len(s) == 4 and s.isdigit():
        return int(s)
    return None


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
    Calculate d-index per year only (same as dataset_index_series_from_doi).
    FAIR score is in [0, 100]; Fi is used as-is. Returns one (created, score) per evaluation year.
    """
    # Fi in [0, 100] to match sindex.metrics.datasetindex.dataset_index
    Fi = float(fair_score_raw) if fair_score_raw is not None else 0.0
    Fi = max(0.0, min(100.0, Fi))

    pubyear = _year_from_date(published_at) if published_at else None
    current_year = datetime.now(timezone.utc).year

    citations_list = []
    for d, w in citations:
        yr = _year_from_date(d)
        if yr is None:
            yr = current_year
        citations_list.append({"citation_year": yr, "citation_weight": float(w)})

    mentions_list = []
    for m in mentions:
        yr = m.get("mention_year")
        if yr is None:
            yr = _year_from_date_str(m.get("mention_date"))
        if yr is None:
            yr = current_year
        else:
            yr = int(yr)
        w = float(m.get("mention_weight", 0.0) or 0.0)
        mentions_list.append({"mention_year": yr, "mention_weight": w})

    series = dataset_index_year_timeseries_external(
        Fi=Fi,
        citations=citations_list,
        mentions=mentions_list,
        pubyear=pubyear,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )
    utc = timezone.utc
    return [
        (
            datetime(rec["year"], 1, 1, tzinfo=utc),
            rec["dataset_index"],
        )
        for rec in series
    ]


def main() -> None:
    """Main function to calculate and export d-index for all datasets to NDJSON files."""
    print("🚀 Starting d-index calculation process...")

    # Get OS-agnostic paths (matching generate-fuji-files.py pattern)
    print("📍 Locating directories...")
    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    output_dir = downloads_dir / "database" / "dindex"
    norm_dir = downloads_dir / "database" / "normalization"
    # Subfield normalization is required; it lives in s-index-api (norm DB + topics table).
    if not _HAS_SUBFIELD_NORM:
        print(
            "  ERROR: Subfield normalization is required. Run this script from the s-index-api repo."
        )
        exit(1)
    subfield_norm_path = Path("subfield_norm_factors.duckdb")
    topics_table_path = Path("openalex_topic_mapping_table.csv")
    if not subfield_norm_path.exists() or not topics_table_path.exists():
        print(
            "  ERROR: Subfield normalization required. Missing subfield_norm_factors.duckdb or openalex_topic_mapping_table.csv"
        )
        exit(1)
    norm_db_path = subfield_norm_path
    use_subfield_norm = True
    print(f"  Normalization DB (subfield): {norm_db_path}")
    print(f"  Topics table: {topics_table_path}")

    # Load subfield caches once (avoids opening CSV/DuckDB per dataset; enables parallel workers).
    print("  Loading topic -> subfield cache (CSV)...")
    topic_to_subfield: Dict[str, str] = load_topic_to_subfield_cache(
        str(topics_table_path)
    )
    print(f"    Loaded {len(topic_to_subfield):,} topic -> subfield mappings")
    print("  Loading subfield norm factors (DuckDB)...")
    subfield_norm_list: List[SubfieldNormRow] = load_subfield_norm_cache(
        str(norm_db_path)
    )
    subfield_norm_cache: SubfieldNormCacheIndexed = index_subfield_norm_cache(
        subfield_norm_list
    )
    print(f"    Loaded {len(subfield_norm_list):,} subfield norm rows (indexed)")

    norm_cache: Optional[Dict[Tuple[str, int], Tuple[float, float, float]]] = None
    year_range_by_topic: Optional[Dict[str, Tuple[int, int]]] = None

    print(f"Output directory: {output_dir}")
    print(f"Normalization directory: {norm_dir}")

    # Clean output directory
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

    # Connect to database
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
            current_batch = []
            current_id = 1  # Start from ID 1

            with conn.cursor() as cur:
                # Create progress bar for datasets
                pbar = tqdm(
                    total=total_datasets,
                    desc="  Processing datasets",
                    unit="dataset",
                    unit_scale=True,
                )

                # Process in batches
                while current_id <= max_dataset_id:
                    # Calculate batch range
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
                                    use_subfield_norm,
                                    (
                                        str(topics_table_path)
                                        if topics_table_path
                                        else None
                                    ),
                                    topic_to_subfield,
                                    subfield_norm_cache,
                                )
                            )
                        # norm_db_path must be passed as str for pickling; worker will use Path
                        with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
                            for rec_list in executor.map(
                                _process_chunk_of_datasets, chunk_payloads
                            ):
                                batch_records.extend(rec_list)
                    else:
                        # Sequential path (uses in-memory caches when available)
                        for dataset_id, published_at, topic_id in datasets_batch:
                            if topic_id is not None and (
                                not isinstance(topic_id, str) or not topic_id.strip()
                            ):
                                topic_id = None
                            fair_score = fair_scores.get(dataset_id, 0.0)
                            citations = citations_by_dataset.get(dataset_id, [])
                            mentions = mentions_by_dataset.get(dataset_id, [])
                            year = published_at.year if published_at else None
                            if use_subfield_norm:
                                norm = _get_norm_factors_subfield_cached(
                                    topic_to_subfield,
                                    subfield_norm_cache,
                                    topic_id,
                                    year,
                                )
                            else:
                                norm = None
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
                                    topic_id_used=(
                                        FALLBACK_TOPIC_ID
                                        if not use_subfield_norm
                                        else "DEFAULT"
                                    ),
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
                                        "year": (
                                            time_point.year if time_point else None
                                        ),
                                        "created": (
                                            time_point.isoformat()
                                            if time_point
                                            else None
                                        ),
                                        "normalization_factors": normalization_factors,
                                    }
                                )

                    # Drain batch_records into current_batch and write files
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

                    # Move to next batch
                    current_id = batch_end + 1

                pbar.close()

                # Write remaining records as final file
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


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)
