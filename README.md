# Pipeline

This repository keeps the scholar-data database up to date. It does two kinds of
work:

1. **Ingest new datasets** - turn raw DataCite/EMDB exports into `Dataset` (+
   author/identifier) rows and a search index entry.
2. **Enrich existing datasets** - layer in topics and citations for datasets
   that are already in the database, without touching anything else.

All of the root-level scripts read their input from subfolders of
`~/Downloads` and are **non-destructive** (insert/upsert/update only) unless
noted otherwise. The `initial/` folder holds the original one-time,
full-rebuild scripts (they `TRUNCATE` before reloading) - see
[initial/ (legacy, destructive)](#initial-legacy-destructive) before running
anything in there.

## Getting started

### Prerequisites/Dependencies

You will need the following installed on your system:

- Python 3.8+
- [Pip](https://pip.pypa.io/en/stable/)

### Setup

1. Create a local virtual environment and activate it:

   ```bash
   python -m venv .venv #or py -m venv .venv
   source .venv/bin/activate # linux
   .venv\Scripts\activate # windows
   ```

2. Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Add your environment variables. An example is provided at `.env.example`

   ```bash
   cp .env.example .env
   ```

   Make sure to update the values in `.env` to match your local setup.
   Scripts that talk to Postgres need `DATABASE_URL`; the Meilisearch
   indexer needs `SEARCH_API_URL` / `SEARCH_API_KEY`.

4. Format the code:

   ```bash
   poe format_with_isort
   poe format_with_black
   ```

   You can also run `poe format` to run both commands at once.

5. Check the code quality:

   ```bash
   poe typecheck
   poe pylint
   poe flake8
   ```

   You can also run `poe lint` to run all three commands at once.

## Adding new datasets

Run these in order whenever you have a new batch of raw DataCite/EMDB
records to add.

1. **`format-raw-data.py`**
   Input: `Downloads/slim-records/datacite-slim-records/*.ndjson` and
   `Downloads/slim-records/emdb-slim-records/*.ndjson` (raw exports).
   Cleans/normalizes each record into a `Dataset`-ready shape (identifiers,
   authors, subjects, etc.) and assigns sequential dataset ids.
   Output: `Downloads/database/dataset/*.ndjson` (directory is wiped first).

   ⚠️ The starting dataset id (`starting_dataset_id` in `main()`) is
   hardcoded - bump it to `MAX(Dataset.id) + 1` before each run or new ids
   will collide with existing ones.

2. **`fill-database-dataset.py`**
   Input: `Downloads/database/dataset/*.ndjson` (output of step 1).
   Bulk-inserts (`COPY`) into `Dataset`, `DatasetAuthor`, and
   `DatasetIdentifier`.

   ⚠️ Also has hardcoded starting ids (`STARTING_AUTHOR_ID`,
   `STARTING_IDENTIFIER_ID`) that must be bumped to the current max id + 1
   before each run.

3. **`build-meilisearch-datasets-index.py`**
   Input: same `Downloads/database/dataset/*.ndjson`.
   Pushes `id`/`identifier`/`title`/`publishedAt`/`subjects`/authors into the
   existing Meilisearch `dataset` index.

After new datasets exist in the database, re-run
[`pull-identifier-datasetid-map.py`](#identifier-mapping) so the enrichment
scripts below can resolve the new DOIs.

## Enriching existing datasets

These scripts resolve a DOI-style identifier (e.g.
`10.57451/lhd.a.nb4a_fraction.75456.1`) to the internal integer `Dataset.id`
and then insert/update - they never truncate a table.

### Identifier mapping

Everything in this section depends on an `identifier -> datasetId` map.

- **`pull-identifier-datasetid-map.py`** (preferred) - queries the live
  `Dataset` table and dumps `{datasetId, identifier, identifierType}` to
  `Downloads/pulled-database/dataset-id-identifier/*.ndjson` (directory is
  wiped first). This is the map `merge-citations.py` and
  `fill-database-topics.py` both load. Re-run it after adding new datasets
  so the map includes them.

- **`build-identifier-datasetid-map.py`** (legacy fallback) - builds the same
  kind of map without hitting the database, from the _locally processed_
  dataset files in `Downloads/database/dataset` and
  `Downloads/database-old/dataset` (i.e. the output of `format-raw-data.py`).
  Output: `Downloads/database/identifier_to_id_map/*.ndjson`. Only useful if
  you don't have DB access when building the map; prefer
  `pull-identifier-datasetid-map.py` otherwise.

### Topics

- **`fill-database-topics.py`**
  Input: `Downloads/topics-split/*.ndjson`, one topic per line:
  `{"dataset_id": "<doi>", "topic_id", "topic_name", "score", "source", "subfield_id", "subfield_name", "field_id", "field_name", "domain_id", "domain_name"}`.
  Resolves `dataset_id` via the identifier map (from
  `Downloads/pulled-database/dataset-id-identifier`), keeps the
  highest-scoring topic per dataset, and upserts into `DatasetTopic`
  (`ON CONFLICT ("datasetId") DO UPDATE`). Requires
  `pull-identifier-datasetid-map.py` to have been run first.

### Citations

- **`merge-citations.py`**
  Input: `Downloads/citations.ndjson`, one citation per line:
  `{"dataset_id"/"doi", "citation_link", "source": [...], "citation_weight", "citation_date"}`.
  Resolves identifiers via the same map, merges sources (OR) and the larger
  `citationWeight` into existing `(datasetId, citationLink)` rows, and inserts
  new ones (manually assigning ids, then resyncing the `Citation` id
  sequence). Unmatched identifiers are written to
  `Downloads/pulled-database/citation-merge-report/unmatched.json`. Supports
  `--dry-run` to preview changes without writing.

## D-index recomputation

- **`pull-db-for-d-index.py`**
  Dumps everything `initial/generate-d-index-files.py` needs to recompute the
  d-index - `Dataset` joined with `FujiScore`, `Citation`, `Mention`,
  `NormalizationFactor`, and `DatasetTopic` - into
  `Downloads/pulled-database/datasets/*.ndjson` (directory wiped first).

## FUJI score extrapolation

This is a one-off/custom-tier task (not part of the regular ingest/enrich
flow): instead of running FUJI on every one of the ~70M+ datasets, it checks
whether a publisher's FUJI scores are consistent enough across a sample to
extrapolate that score to the rest of the publisher's datasets.

- **`fuji-score-extrapolation-candidates.py`**
  Reads the dump from `pull-db-for-d-index.py`
  (`Downloads/pulled-database/datasets`), reservoir-samples low-scoring DOI
  identifiers per publisher, writes
  `Downloads/pulled-database/fuji-extrapolation-test/fuji-extrapolation-candidates.json`,
  and queues the sampled dataset ids into `FujiJob` so a FUJI worker scores
  them.

- **`fuji-score-extrapolation-analysis.py`**
  Compares each sampled dataset's pre-sample ("before") score against its
  freshly computed ("after") `FujiScore`, and reports per publisher whether
  scores are consistent enough to extrapolate that score to the rest of the
  publisher's datasets. Output:
  `Downloads/pulled-database/fuji-extrapolation-test/fuji-extrapolation-analysis.json`.

  (The FUJI scores themselves come from a separate Dockerized FUJI worker
  that picks up the queued `FujiJob` rows and writes straight to
  `FujiScore` - that worker isn't one of the scripts in this repo.)

- **`fuji-score-extrapolation-update.py`**
  Reads the analysis file above and splits publishers into **consistent**
  (a single agreed "after" score) and **inconsistent** (no agreement -
  takes the median of `afterUniqueScores` instead) groups. Streams the
  dataset dump (`Downloads/pulled-database/datasets`) and stages an update
  for every dataset under a consistent publisher (to that publisher's
  score), and for datasets under an inconsistent publisher whose existing
  FUJI score is below 14 (to the publisher's median). Output:
  `Downloads/pulled-database/fuji-scores/*.ndjson` (directory wiped first;
  `FujiScore`-shaped rows - `datasetId`, `score`, `evaluationDate`,
  `metricVersion`, `softwareVersion` - 10k records per file), ready for
  `fill-database-fuji.py` to import.

- **`fuji-score-extrapolation-analysis-script-output.py`**
  Reads the same analysis file and prints a TypeScript
  `HARDCODED_REPOSITORY_ID_SCORES` map (`publisherId -> score`) for every
  consistent publisher, sorted by dataset count, plus a small
  `EXTRA_SCORES` dict for manually-set overrides (e.g. `emdb`) - for
  pasting into application code that wants to skip FUJI entirely for known
  publishers.

- **`fill-database-fuji.py`**
  Input: `Downloads/pulled-database/fuji-scores/*.ndjson` (output of
  `fuji-score-extrapolation-update.py`). Streams the staging files
  single-pass and upserts into `FujiScore`
  (`ON CONFLICT ("datasetId") DO UPDATE`). The similarly-named
  `initial/fill-database-fuji.py` is the original, now-superseded version
  kept for reference.

## `initial/` (legacy, destructive)

Scripts under `initial/` were used for the **original full database build**
and `TRUNCATE` the tables they touch before reloading from scratch (e.g.
`fill-database-fuji.py`, `fill-database-citation.py`,
`fill-database-d-index.py`, `fill-database-topics-old.py`,
`format-topics.py`, `generate-d-index-files.py`, and the original
`fill-database-dataset.py`/`format-raw-data.py`). Don't run
them against a live database unless you intend a full rebuild - use the
root-level scripts above for incremental additions instead. They're kept
here for reference, and `initial/identifier_mapping.py` is still imported by
`build-identifier-datasetid-map.py`.
