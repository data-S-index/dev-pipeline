"""
Query the database for dashboard metrics and output Vue refs:
- datasetsByYear: counts by year (1950-2010, 2011..2025)
- institutions: top organizations by dataset count (+ Other)
- fields: top fields from DatasetTopic by dataset count (+ Other)
"""

import json
import sys

import psycopg
from config import DATABASE_URL

# Exact year buckets for datasetsByYear
YEAR_BUCKETS = ["1950-2010"] + [str(y) for y in range(2011, 2026)]


def datasets_by_year(conn: psycopg.Connection) -> list[dict]:
    """Count datasets by publication year; bucket pre-2011 as '1950-2010'."""
    query = """
    SELECT
        CASE
            WHEN EXTRACT(YEAR FROM d."publishedAt")::int < 2011 THEN '1950-2010'
            ELSE EXTRACT(YEAR FROM d."publishedAt")::int::text
        END AS year,
        COUNT(*) AS value
    FROM "Dataset" d
    WHERE d."publishedAt" IS NOT NULL
      AND EXTRACT(YEAR FROM d."publishedAt")::int BETWEEN 1950 AND 2025
    GROUP BY 1
    ORDER BY MIN(EXTRACT(YEAR FROM d."publishedAt"));
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = {r["year"]: r["value"] for r in cur.fetchall()}

    return [{"year": y, "value": rows.get(y, 0)} for y in YEAR_BUCKETS]


def institutions(conn: psycopg.Connection, top_n: int = 5) -> list[dict]:
    """Top organizations by dataset count; rest as Other."""
    query = """
    SELECT o.name, COUNT(aod."datasetId") AS value
    FROM "AutomatedOrganization" o
    JOIN "AutomatedOrganizationDataset" aod ON aod."automatedOrganizationId" = o.id
    GROUP BY o.id, o.name
    ORDER BY value DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        all_rows = cur.fetchall()

    if not all_rows:
        return [{"name": "Other", "value": 0}]

    top = [{"name": r["name"], "value": r["value"]} for r in all_rows[:top_n]]
    other_sum = sum(r["value"] for r in all_rows[top_n:])
    if other_sum > 0:
        top.append({"name": "Other", "value": other_sum})
    return top


def fields(conn: psycopg.Connection, top_n: int = 5) -> list[dict]:
    """Top fields from DatasetTopic by dataset count; rest as Other."""
    query = """
    SELECT COALESCE(NULLIF(TRIM(dt."fieldName"), ''), 'Other') AS name, COUNT(*) AS value
    FROM "DatasetTopic" dt
    WHERE dt."fieldName" IS NOT NULL
    GROUP BY dt."fieldName"
    ORDER BY value DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        all_rows = cur.fetchall()

    if not all_rows:
        return [{"name": "Other", "value": 0}]

    # If there's already an "Other" or empty group, merge it into our trailing Other
    named = []
    other_sum = 0
    for r in all_rows:
        if r["name"] == "Other" or not r["name"]:
            other_sum += r["value"]
        else:
            named.append({"name": r["name"], "value": r["value"]})

    top = named[:top_n]
    other_sum += sum(r["value"] for r in named[top_n:])
    if other_sum > 0:
        top.append({"name": "Other", "value": other_sum})
    return top


def ref_js(name: str, data: list[dict]) -> str:
    """Format a list of dicts as a Vue ref(...) declaration."""
    items = ",\n  ".join(
        "{" + ", ".join(f"{k}: {json.dumps(v)}" for k, v in row.items()) + "}"
        for row in data
    )
    return f"const {name} = ref([\n  {items}\n]);"


def main() -> None:
    if not DATABASE_URL:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    with psycopg.connect(DATABASE_URL, row_factory=psycopg.rows.dict_row) as conn:
        by_year = datasets_by_year(conn)
        inst = institutions(conn, top_n=12)
        flds = fields(conn, top_n=10)

    print(ref_js("datasetsByYear", by_year))
    print()
    print(ref_js("institutions", inst))
    print()
    print(ref_js("fields", flds))


if __name__ == "__main__":
    main()
