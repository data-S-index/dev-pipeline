"""
List automated users (people), automated organizations, and top datasets.

Writes results to:
- top-users-by-avg-dataset-index.tsv
- top-organizations-by-avg-dataset-index.tsv
- top-datasets-by-mentions.tsv

Users/Orgs: Average Dataset Index (S-index / dataset_count > 1), citations > 5, sorted by S-index.
Datasets: Ranked by mention count (Mention table); citations and d-index included for context.
"""

from pathlib import Path

import psycopg
from config import DATABASE_URL

OUTPUT_DIR = Path(__file__).resolve().parent
USERS_FILE = OUTPUT_DIR / "top-users-by-avg-dataset-index.tsv"
ORGS_FILE = OUTPUT_DIR / "top-organizations-by-avg-dataset-index.tsv"
DATASETS_FILE = OUTPUT_DIR / "top-datasets-by-mentions.tsv"

HEADER = (
    "id\tname\tnameType\taffiliations\tdataset_count\tsindex\tsindex_year\t"
    "avg_dataset_index\ttotal_citations"
)
ORG_HEADER = (
    "id\tname\tdataset_count\tsindex\tsindex_year\t"
    "avg_dataset_index\ttotal_citations"
)
DATASET_HEADER = (
    "id\tidentifier\tidentifierType\ttitle\tpublisher\tpublishedAt\t"
    "mention_count\tmention_weight_sum\tcitation_count\tdindex_latest"
)


def _format_affiliations(affiliations) -> str:
    if isinstance(affiliations, list):
        return "; ".join((affiliations or [])[:2])
    return str(affiliations or "")


def run_users(conn: psycopg.Connection) -> list:
    query = """
    WITH
    user_sindex AS (
        SELECT DISTINCT ON ("automatedUserId")
            "automatedUserId",
            score AS sindex,
            year AS sindex_year
        FROM "AutomatedUserSIndex"
        ORDER BY "automatedUserId", year DESC
    ),
    user_dataset_count AS (
        SELECT "automatedUserId", COUNT(*) AS dataset_count
        FROM "AutomatedUserDataset"
        GROUP BY "automatedUserId"
        HAVING COUNT(*) > 5
    ),
    user_citations AS (
        SELECT aud."automatedUserId", COUNT(c.id) AS total_citations
        FROM "AutomatedUserDataset" aud
        JOIN "Citation" c ON c."datasetId" = aud."datasetId"
        GROUP BY aud."automatedUserId"
        HAVING COUNT(c.id) > 5
    )
    SELECT
        u.id,
        u.name,
        u."nameType",
        u.affiliations,
        udc.dataset_count,
        us.sindex,
        us.sindex_year,
        (us.sindex / NULLIF(udc.dataset_count, 0)) AS avg_dataset_index,
        uc.total_citations
    FROM "AutomatedUser" u
    JOIN user_dataset_count udc ON udc."automatedUserId" = u.id
    JOIN user_sindex us ON us."automatedUserId" = u.id
    JOIN user_citations uc ON uc."automatedUserId" = u.id
    WHERE (us.sindex / NULLIF(udc.dataset_count, 0)) > 1
    ORDER BY us.sindex DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def run_organizations(conn: psycopg.Connection) -> list:
    query = """
    WITH
    org_sindex AS (
        SELECT DISTINCT ON ("automatedOrganizationId")
            "automatedOrganizationId",
            score AS sindex,
            year AS sindex_year
        FROM "AutomatedOrganizationSIndex"
        ORDER BY "automatedOrganizationId", year DESC
    ),
    org_dataset_count AS (
        SELECT "automatedOrganizationId", COUNT(*) AS dataset_count
        FROM "AutomatedOrganizationDataset"
        GROUP BY "automatedOrganizationId"
        HAVING COUNT(*) > 5
    ),
    org_citations AS (
        SELECT aod."automatedOrganizationId", COUNT(c.id) AS total_citations
        FROM "AutomatedOrganizationDataset" aod
        JOIN "Citation" c ON c."datasetId" = aod."datasetId"
        GROUP BY aod."automatedOrganizationId"
        HAVING COUNT(c.id) > 5
    )
    SELECT
        o.id,
        o.name,
        odc.dataset_count,
        os.sindex,
        os.sindex_year,
        (os.sindex / NULLIF(odc.dataset_count, 0)) AS avg_dataset_index,
        oc.total_citations
    FROM "AutomatedOrganization" o
    JOIN org_dataset_count odc ON odc."automatedOrganizationId" = o.id
    JOIN org_sindex os ON os."automatedOrganizationId" = o.id
    JOIN org_citations oc ON oc."automatedOrganizationId" = o.id
    WHERE (os.sindex / NULLIF(odc.dataset_count, 0)) > 1
    ORDER BY os.sindex DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def run_datasets(conn: psycopg.Connection) -> list:
    """Top datasets by mention count (Mention table). Citations and d-index for context."""
    query = """
    WITH
    mention_stats AS (
        SELECT
            "datasetId",
            COUNT(*) AS mention_count,
            COALESCE(SUM("mentionWeight"), 0) AS mention_weight_sum
        FROM "Mention"
        GROUP BY "datasetId"
        HAVING COUNT(*) > 0
    ),
    citation_counts AS (
        SELECT "datasetId", COUNT(*) AS citation_count
        FROM "Citation"
        GROUP BY "datasetId"
    ),
    dindex_latest AS (
        SELECT DISTINCT ON ("datasetId") "datasetId", score AS dindex_latest
        FROM "DIndex"
        ORDER BY "datasetId", year DESC
    )
    SELECT
        d.id,
        d.identifier,
        d."identifierType",
        d.title,
        d.publisher,
        d."publishedAt",
        ms.mention_count,
        ms.mention_weight_sum,
        COALESCE(cc.citation_count, 0) AS citation_count,
        dl.dindex_latest
    FROM "Dataset" d
    JOIN mention_stats ms ON ms."datasetId" = d.id
    LEFT JOIN citation_counts cc ON cc."datasetId" = d.id
    LEFT JOIN dindex_latest dl ON dl."datasetId" = d.id
    ORDER BY ms.mention_count DESC, ms.mention_weight_sum DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def _tsv_escape(s: str) -> str:
    """Replace tabs and newlines so TSV stays valid."""
    if s is None:
        return ""
    s = str(s)
    return s.replace("\t", " ").replace("\n", " ").replace("\r", " ")


def write_tsv(
    path: Path,
    header: str,
    rows: list,
    *,
    is_org: bool = False,
    is_dataset: bool = False,
) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(header + "\n")
        for row in rows:
            if is_dataset:
                (
                    id_,
                    identifier,
                    identifier_type,
                    title,
                    publisher,
                    published_at,
                    mention_count,
                    mention_weight_sum,
                    citation_count,
                    dindex_latest,
                ) = row
                pub = published_at.strftime("%Y-%m-%d") if published_at else ""
                dindex_str = f"{dindex_latest:.2f}" if dindex_latest is not None else ""
                line = (
                    f"{id_}\t{_tsv_escape(identifier)}\t{_tsv_escape(identifier_type)}\t"
                    f"{_tsv_escape(title)}\t{_tsv_escape(publisher)}\t{pub}\t"
                    f"{mention_count}\t{mention_weight_sum}\t{citation_count}\t{dindex_str}\n"
                )
            elif is_org:
                id_, name, dataset_count, sindex, sindex_year, avg_di, citations = row
                line = f"{id_}\t{_tsv_escape(name)}\t{dataset_count}\t{sindex}\t{sindex_year}\t{avg_di:.2f}\t{citations}\n"
            else:
                (
                    id_,
                    name,
                    name_type,
                    affiliations,
                    dataset_count,
                    sindex,
                    sindex_year,
                    avg_di,
                    citations,
                ) = row
                aff_str = _format_affiliations(affiliations)
                line = (
                    f"{id_}\t{_tsv_escape(name)}\t{name_type or ''}\t{_tsv_escape(aff_str)}\t{dataset_count}\t{sindex}\t{sindex_year}\t"
                    f"{avg_di:.2f}\t{citations}\n"
                )
            f.write(line)


def main() -> None:
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set")

    with psycopg.connect(DATABASE_URL) as conn:
        user_rows = run_users(conn)
        org_rows = run_organizations(conn)
        dataset_rows = run_datasets(conn)

    write_tsv(USERS_FILE, HEADER, user_rows, is_org=False)
    write_tsv(ORGS_FILE, ORG_HEADER, org_rows, is_org=True)
    write_tsv(DATASETS_FILE, DATASET_HEADER, dataset_rows, is_dataset=True)

    print(f"Users:       {len(user_rows):,} → {USERS_FILE.name}")
    print(f"Organizations: {len(org_rows):,} → {ORGS_FILE.name}")
    print(f"Datasets:    {len(dataset_rows):,} → {DATASETS_FILE.name} (by mentions)")
    print(
        "(users/orgs: dataset_count > 5, avg_dataset_index > 1, citations > 5, by S-index)"
    )


if __name__ == "__main__":
    main()
