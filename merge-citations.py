"""Merge a citations NDJSON file into the live Citation table.

Unlike format-citation.py / fill-database-citation.py (which truncate and
rebuild the whole table), this script does an incremental merge against the
existing data:

  1. Load the identifier -> datasetId map (built by
     pull-identifier-datasetid-map.py) from
     Downloads/pulled-database/dataset-id-identifier.
  2. Read Downloads/citations.ndjson, resolve each "dataset_id" (a DOI-style
     identifier) to its internal datasetId, and group records by datasetId
     so the existing-citation lookups below can be done in one query per
     batch of datasetIds instead of one query per row.
  3. For each batch of datasetIds, fetch the citations already in the
     database for those datasets, keyed by (datasetId, citationLink).
  4. For each incoming citation:
       - if (datasetId, citationLink) already exists, merge sources (OR)
         and keep the larger citationWeight; queue an UPDATE if anything
         changed.
       - otherwise queue an INSERT, assigning it the next id after the
         table's current MAX(id) so ids keep counting up from where the
         table left off.
  5. Apply queued UPDATEs in batched "UPDATE ... FROM (VALUES ...)"
     statements and queued INSERTs via COPY, then bring the id sequence
     back in sync with MAX(id) (we assigned ids manually, bypassing
     nextval()).

Identifiers that don't resolve to a datasetId are recorded in
unmatched.json in the report directory for investigation.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from tqdm import tqdm

from config import DATABASE_URL

# Number of distinct datasetIds looked up (existing citations) per round trip.
DATASET_BATCH_SIZE = 2000

# Number of rows applied per UPDATE/INSERT statement.
DB_WRITE_BATCH_SIZE = 10000

# Number of update/insert decisions echoed to the console in --dry-run mode.
DRY_RUN_CONSOLE_SAMPLE = 20

Citation = Dict[str, Any]


def map_key(identifier: str) -> str:
    """Build a lookup key from the identifier value alone."""
    return identifier.strip().lower()


def clean_citation_link(s: Optional[str]) -> str:
    """Keep only URL-safe characters: alphanumeric and . / : - _ ~"""
    if not s:
        return ""
    return "".join(c for c in s if c.isalnum() or c in (".", "/", ":", "-", "_", "~"))


def parse_cited_date(value: Any) -> datetime:
    """Parse an ISO citation date, defaulting to now() if missing/invalid."""
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = f"{value[:-1]}+00:00"
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    return datetime.now()


def load_identifier_to_dataset_id_map(map_dir: Path) -> Dict[str, int]:
    """Load the identifier -> datasetId map into memory."""
    print(f"Loading identifier-to-datasetId map from {map_dir}...")

    if not map_dir.exists():
        raise FileNotFoundError(
            f"Map directory not found: {map_dir}. "
            "Run pull-identifier-datasetid-map.py first."
        )

    map_files = sorted(map_dir.glob("*.ndjson"))
    if not map_files:
        raise FileNotFoundError(f"No NDJSON files found in {map_dir}.")

    mapping: Dict[str, int] = {}
    for file_path in tqdm(map_files, desc="Loading map files", unit="file"):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                identifier = record.get("identifier")
                dataset_id = record.get("datasetId")
                if identifier and dataset_id is not None:
                    mapping[map_key(identifier)] = dataset_id

    print(f"  Loaded {len(mapping):,} identifier entries")
    return mapping


def build_resolved_citations(
    ndjson_file: Path, identifier_map: Dict[str, int]
) -> Tuple[Dict[int, Dict[str, Citation]], int, Dict[str, int]]:
    """Read the citations NDJSON file and group resolved citations by datasetId.

    Returns (resolved, total_lines, unmatched_identifier_counts), where
    resolved maps datasetId -> {citationLink: citation}, merging duplicate
    (datasetId, citationLink) pairs within the file by OR-ing sources and
    keeping the larger citationWeight.
    """
    resolved: Dict[int, Dict[str, Citation]] = {}
    unmatched: Dict[str, int] = {}
    total_lines = 0

    with open(ndjson_file, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Reading citations.ndjson", unit="line", unit_scale=True):
            line = line.strip()
            if not line:
                continue
            total_lines += 1

            record = json.loads(line)

            identifier = record.get("dataset_id") or record.get("doi")
            if not identifier:
                continue
            key = map_key(identifier)
            dataset_id = identifier_map.get(key)
            if dataset_id is None:
                unmatched[key] = unmatched.get(key, 0) + 1
                continue

            citation_link = clean_citation_link(record.get("citation_link"))
            if not citation_link:
                continue

            source = record.get("source") or []
            source_list = [s.lower() for s in source if isinstance(s, str)]

            weight_value = record.get("citation_weight")
            citation: Citation = {
                "datacite": "datacite" in source_list,
                "mdc": "mdc" in source_list,
                "openAlex": "openalex" in source_list,
                "citationWeight": float(weight_value) if weight_value is not None else 1.0,
                "citedDate": parse_cited_date(record.get("citation_date")),
            }

            by_link = resolved.setdefault(dataset_id, {})
            existing = by_link.get(citation_link)
            if existing is None:
                by_link[citation_link] = citation
            else:
                existing["datacite"] = existing["datacite"] or citation["datacite"]
                existing["mdc"] = existing["mdc"] or citation["mdc"]
                existing["openAlex"] = existing["openAlex"] or citation["openAlex"]
                existing["citationWeight"] = max(
                    existing["citationWeight"], citation["citationWeight"]
                )

    return resolved, total_lines, unmatched


def get_max_citation_id(cur) -> int:
    """Return the current MAX(id) in the Citation table (0 if empty)."""
    cur.execute('SELECT COALESCE(MAX(id), 0) FROM "Citation"')
    (max_id,) = cur.fetchone()
    return max_id


def fetch_existing_citations(
    cur, dataset_ids: List[int]
) -> Dict[Tuple[int, str], Tuple[int, bool, bool, bool, float]]:
    """Fetch existing citations for a batch of datasetIds, keyed by (datasetId, citationLink)."""
    cur.execute(
        """
        SELECT id, "datasetId", "citationLink", datacite, mdc, "openAlex", "citationWeight"
        FROM "Citation"
        WHERE "datasetId" = ANY(%s)
        """,
        (dataset_ids,),
    )
    existing: Dict[Tuple[int, str], Tuple[int, bool, bool, bool, float]] = {}
    for row_id, dataset_id, citation_link, datacite, mdc, open_alex, weight in cur.fetchall():
        existing[(dataset_id, citation_link)] = (row_id, datacite, mdc, open_alex, weight)
    return existing


def apply_update_batch(cur, batch: List[Tuple[int, bool, bool, bool, float]]) -> None:
    """Apply one batch of (id, datacite, mdc, openAlex, citationWeight) updates."""
    values_clause = ", ".join(
        ["(%s::bigint, %s::boolean, %s::boolean, %s::boolean, %s::float8)"] * len(batch)
    )
    query = f"""
        UPDATE "Citation" AS c
        SET datacite = v.datacite, mdc = v.mdc, "openAlex" = v.open_alex,
            "citationWeight" = v.citation_weight, updated = now()
        FROM (VALUES {values_clause}) AS v(id, datacite, mdc, open_alex, citation_weight)
        WHERE c.id = v.id
    """
    params = [value for row in batch for value in row]
    cur.execute(query, params)


def apply_insert_batch(conn, batch: List[Tuple]) -> None:
    """Apply one batch of new Citation rows via COPY."""
    with conn.cursor() as cur:
        with cur.copy(
            """COPY "Citation"
               (id, "datasetId", "citationLink", datacite, mdc, "openAlex",
                "citedDate", "citationWeight", created, updated)
               FROM STDIN"""
        ) as copy:
            for row in batch:
                copy.write_row(row)


def merge_citations(
    conn,
    resolved: Dict[int, Dict[str, Citation]],
    report_dir: Path,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """Merge resolved citations into the database. Returns (updated_count, inserted_count).

    In dry-run mode, existing citations are still read from the database (so
    the diff is accurate) but no UPDATE/INSERT/setval is executed. The first
    DRY_RUN_CONSOLE_SAMPLE decisions of each kind are echoed to the console,
    and the full set of would-be changes is written to
    dry-run-updates.ndjson / dry-run-inserts.ndjson in report_dir.
    """
    with conn.cursor() as cur:
        next_id = get_max_citation_id(cur) + 1
    print(f"  Next available Citation id: {next_id:,}")

    dataset_ids = list(resolved.keys())
    update_batch: List[Tuple[int, bool, bool, bool, float]] = []
    insert_batch: List[Tuple] = []
    updated_count = 0
    inserted_count = 0
    now = datetime.now()

    dry_run_updates_file = dry_run_inserts_file = None
    if dry_run:
        report_dir.mkdir(parents=True, exist_ok=True)
        dry_run_updates_file = open(
            report_dir / "dry-run-updates.ndjson", "w", encoding="utf-8"
        )
        dry_run_inserts_file = open(
            report_dir / "dry-run-inserts.ndjson", "w", encoding="utf-8"
        )

    try:
        with conn.cursor() as cur:
            for i in tqdm(
                range(0, len(dataset_ids), DATASET_BATCH_SIZE),
                desc="Merging citations",
                unit="batch",
            ):
                batch_dataset_ids = dataset_ids[i : i + DATASET_BATCH_SIZE]
                existing = fetch_existing_citations(cur, batch_dataset_ids)

                for dataset_id in batch_dataset_ids:
                    for citation_link, citation in resolved[dataset_id].items():
                        key = (dataset_id, citation_link)
                        found = existing.get(key)

                        if found is not None:
                            row_id, old_datacite, old_mdc, old_open_alex, old_weight = found
                            new_datacite = old_datacite or citation["datacite"]
                            new_mdc = old_mdc or citation["mdc"]
                            new_open_alex = old_open_alex or citation["openAlex"]
                            new_weight = max(old_weight, citation["citationWeight"])
                            if (
                                new_datacite != old_datacite
                                or new_mdc != old_mdc
                                or new_open_alex != old_open_alex
                                or new_weight != old_weight
                            ):
                                if not dry_run:
                                    update_batch.append(
                                        (row_id, new_datacite, new_mdc, new_open_alex, new_weight)
                                    )
                                if dry_run:
                                    if updated_count < DRY_RUN_CONSOLE_SAMPLE:
                                        tqdm.write(
                                            f"[DRY RUN] UPDATE id={row_id} datasetId={dataset_id} "
                                            f"link={citation_link} "
                                            f"datacite {old_datacite}->{new_datacite} "
                                            f"mdc {old_mdc}->{new_mdc} "
                                            f"openAlex {old_open_alex}->{new_open_alex} "
                                            f"weight {old_weight}->{new_weight}"
                                        )
                                    dry_run_updates_file.write(
                                        json.dumps(
                                            {
                                                "id": row_id,
                                                "datasetId": dataset_id,
                                                "citationLink": citation_link,
                                                "before": {
                                                    "datacite": old_datacite,
                                                    "mdc": old_mdc,
                                                    "openAlex": old_open_alex,
                                                    "citationWeight": old_weight,
                                                },
                                                "after": {
                                                    "datacite": new_datacite,
                                                    "mdc": new_mdc,
                                                    "openAlex": new_open_alex,
                                                    "citationWeight": new_weight,
                                                },
                                            }
                                        )
                                        + "\n"
                                    )
                                updated_count += 1
                        else:
                            if dry_run:
                                if inserted_count < DRY_RUN_CONSOLE_SAMPLE:
                                    tqdm.write(
                                        f"[DRY RUN] INSERT wouldBeId={next_id} datasetId={dataset_id} "
                                        f"link={citation_link} datacite={citation['datacite']} "
                                        f"mdc={citation['mdc']} openAlex={citation['openAlex']} "
                                        f"weight={citation['citationWeight']}"
                                    )
                                dry_run_inserts_file.write(
                                    json.dumps(
                                        {
                                            "wouldBeId": next_id,
                                            "datasetId": dataset_id,
                                            "citationLink": citation_link,
                                            "datacite": citation["datacite"],
                                            "mdc": citation["mdc"],
                                            "openAlex": citation["openAlex"],
                                            "citationWeight": citation["citationWeight"],
                                            "citedDate": citation["citedDate"].isoformat(),
                                        }
                                    )
                                    + "\n"
                                )
                            else:
                                insert_batch.append(
                                    (
                                        next_id,
                                        dataset_id,
                                        citation_link,
                                        citation["datacite"],
                                        citation["mdc"],
                                        citation["openAlex"],
                                        citation["citedDate"],
                                        citation["citationWeight"],
                                        now,
                                        now,
                                    )
                                )
                            next_id += 1
                            inserted_count += 1

                        if not dry_run:
                            if len(update_batch) >= DB_WRITE_BATCH_SIZE:
                                apply_update_batch(cur, update_batch)
                                update_batch = []
                            if len(insert_batch) >= DB_WRITE_BATCH_SIZE:
                                apply_insert_batch(conn, insert_batch)
                                insert_batch = []

            if not dry_run:
                if update_batch:
                    apply_update_batch(cur, update_batch)
                if insert_batch:
                    apply_insert_batch(conn, insert_batch)

                # We assigned ids manually above, bypassing nextval(), so bring the
                # sequence back in sync with MAX(id) for future autoincrement inserts.
                cur.execute(
                    """SELECT setval(pg_get_serial_sequence('"Citation"', 'id'),
                                      (SELECT MAX(id) FROM "Citation"))"""
                )

        if not dry_run:
            conn.commit()
        else:
            conn.rollback()
    finally:
        if dry_run_updates_file:
            dry_run_updates_file.close()
        if dry_run_inserts_file:
            dry_run_inserts_file.close()

    return updated_count, inserted_count


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Compute updates/inserts without writing to the database. "
            "Prints a sample to the console and writes the full set of "
            "would-be changes to dry-run-updates.ndjson / dry-run-inserts.ndjson "
            "in the report directory."
        ),
    )
    return parser.parse_args()


def main() -> None:
    """Merge citations.ndjson into the Citation table."""
    args = parse_args()
    if args.dry_run:
        print("Starting citation merge (DRY RUN — no changes will be written)...")
    else:
        print("Starting citation merge...")

    home_dir = Path.home()
    downloads_dir = home_dir / "Downloads"
    ndjson_file = downloads_dir / "citations.ndjson"
    map_dir = downloads_dir / "pulled-database" / "dataset-id-identifier"
    report_dir = downloads_dir / "pulled-database" / "citation-merge-report"

    print(f"Citations file: {ndjson_file}")
    print(f"Identifier map directory: {map_dir}")

    if not ndjson_file.exists():
        raise FileNotFoundError(f"Citations file not found: {ndjson_file}")

    identifier_map = load_identifier_to_dataset_id_map(map_dir)

    print("\nReading and grouping citations by datasetId...")
    resolved, total_lines, unmatched = build_resolved_citations(ndjson_file, identifier_map)
    total_citations = sum(len(v) for v in resolved.values())
    print(f"  Read {total_lines:,} lines")
    print(f"  Resolved {total_citations:,} unique citations across {len(resolved):,} datasets")
    print(f"  Unmatched identifiers: {len(unmatched):,}")

    report_dir.mkdir(parents=True, exist_ok=True)
    with open(report_dir / "unmatched.json", "w", encoding="utf-8") as f:
        json.dump(unmatched, f, ensure_ascii=False, indent=2)

    print("\nConnecting to database...")
    with psycopg.connect(DATABASE_URL) as conn:
        updated_count, inserted_count = merge_citations(
            conn, resolved, report_dir, dry_run=args.dry_run
        )

    if args.dry_run:
        print("\nDry run completed — no changes were written to the database.")
        print("Summary:")
        print(f"  - Citations that would be updated: {updated_count:,}")
        print(f"  - Citations that would be inserted: {inserted_count:,}")
        print(f"  - Unmatched identifiers: {len(unmatched):,} -> {report_dir / 'unmatched.json'}")
        print(f"  - Would-be updates: {report_dir / 'dry-run-updates.ndjson'}")
        print(f"  - Would-be inserts: {report_dir / 'dry-run-inserts.ndjson'}")
    else:
        print("\nCitation merge completed successfully!")
        print("Summary:")
        print(f"  - Citations updated: {updated_count:,}")
        print(f"  - Citations inserted: {inserted_count:,}")
        print(f"  - Unmatched identifiers: {len(unmatched):,} -> {report_dir / 'unmatched.json'}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
