# Chess Data Lakehouse

A data engineering project that turns public Lichess PGN exports into Bronze, Silver, quarantine and Gold datasets. It uses Python for a local pipeline and PySpark/Delta Lake for a managed-table implementation. The accompanying dashboard explores verified aggregate metrics without publishing player names, game IDs or raw PGN.

**Dashboard:** [GitHub Pages](https://tasachii.github.io/chess-data-lakehouse/) (publication target) · [Dashboard source](docs/index.html) · [Aggregate data](docs/assets/snapshot.json)

![Dashboard preview: October 2026 archive sample, outcomes and opening explorer](docs/assets/dashboard-preview.png)

## Contents

- [Quick start](#quick-start)
- [Real data and dashboard](#real-data-and-dashboard)
- [Architecture](#architecture)
- [Tests](#tests)
- [Databricks](#databricks)
- [Source, license and limits](#source-license-and-limits)

## Quick start

**Requirements:** Python 3.11 or 3.12. Commands below use Python 3.12 and a Unix-style shell.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/chess-lakehouse run \
  --input tests/fixtures/sample.pgn \
  --output output/fixture-v2 \
  --source-url fixture://sample.pgn
```

The synthetic fixture contains three records (one duplicate and one malformed game). The first run produces two Bronze records, one valid Silver record and one quarantined record. Repeat the same command to confirm `inserted_bronze: 0`. The fixture is not real Lichess data.

## Real data and dashboard

The current dashboard uses the first **10,000 games** from the October 2026 Lichess rated standard archive. The validated games have start times between 00:00:00 and 00:08:08 UTC on October 1, 2026. This is a static archive snapshot, not a live feed or random sample.

| Metric | Measured value |
| --- | ---: |
| Unique Bronze records | 10,000 |
| Valid Silver games | 9,996 |
| Quarantined | 4 |
| Opening groups / rating buckets / speeds | 1,107 / 14 / 4 |
| White wins / black wins / draws | 5,026 / 4,571 / 399 |

The four quarantined games have invalid time-control fields. A second pipeline run inserted zero records. The dashboard exporter independently compares every Gold group with Silver counts before exporting; it includes only aggregate labels, game counts, outcome counts and source metadata.

### Reproduce the snapshot

```bash
SOURCE_URL=https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst
.venv/bin/chess-lakehouse download \
  --url "$SOURCE_URL" --output data/dashboard_sample_10000.pgn --max-games 10000
.venv/bin/chess-lakehouse run \
  --input data/dashboard_sample_10000.pgn --output output/dashboard-10000 --source-url "$SOURCE_URL"
.venv/bin/python scripts/export_dashboard.py --lakehouse output/dashboard-10000
```

Use a fresh output directory for a different sample. The downloader saves a receipt next to the PGN with its bound, source checksum and retrieval time. The exporter verifies that a single source batch produced the lakehouse and that the group counts reconcile. Re-downloading may produce a new retrieval timestamp; the PGN checksum and group metrics identify the sample itself.

### Preview the dashboard locally

```bash
.venv/bin/python -m http.server 8765 --directory docs
```

Open [http://localhost:8765/](http://localhost:8765/). Search openings, choose a minimum game count, sort by number of games or outcomes, and switch the rating chart between counts and rates. Each Gold chart is a separate grouping; filtering the opening chart does not change the other dimensions.

The site uses plain HTML, CSS and JavaScript. It requires no build step, server API or account. GitHub Pages can serve `main:/docs` directly.

## Architecture

```text
Lichess PGN.zst → bounded download and receipt → Bronze (raw, deduplicated)
                                                   │
                                                   ├── Quarantine (invalid)
                                                   ↓
                                                Silver (typed)
                                                   ↓
                   Dashboard JSON ← Gold (opening, rating, speed)
```

| Layer | Grain | Storage / update |
| --- | --- | --- |
| Bronze | One record per unique game ID | JSONL or Delta; insert only |
| Silver | One validated game | JSONL or Delta; local rebuild, Delta insert only |
| Quarantine | One invalid unique game | JSONL or Delta; retained with errors |
| Gold | One row per opening, rating bucket or speed | CSV or Delta; full aggregate rebuild |
| Run history | One source file per successful invocation | JSON manifests or `ingestion_runs` Delta table; append only |

| Decision | Reason |
| --- | --- |
| Stable ID from the Lichess `Site` URL | Recognizes both `/game/<id>` and `/<id>`; a content hash is used for nonmatching URLs. Prevents replay double counts. |
| Split on each Event tag | Keeps incomplete records from contaminating valid ones. |
| Validate before aggregation | Required fields, integer Elo within 100–4000, UTC timestamp, result, time control and move-text presence. |
| Durable batch identity | Source URL and exact file SHA-256 identify a batch; each Bronze row refers to its first batch. |
| Aggregate-only export | Dashboard never needs the raw source or individual game records. |

A rating bucket is the integer mean of White and Black Elo, grouped by 200 points. Speed uses an estimated duration of `initial seconds + 40 × increment`: under 180 is bullet (including ultrabullet), under 600 blitz, under 1,800 rapid, otherwise classical. The outcome rate divides the relevant count by validated games in the group.

More on the data contracts, replay behavior and cloud configuration: [Operations and validation notes](DEV_HANDOFF.md).

## Tests

```bash
.venv/bin/python -m pytest -ra
.venv/bin/ruff check .
.venv/bin/ruff format --check src tests scripts
```

The default suite tests ingestion, validation, deduplication, corrupted/truncated inputs, lineage and aggregate export. Spark tests are optional. To run the full suite, install a compatible JDK and the pinned Spark/Delta extra (PySpark 3.5.3, Delta 3.3.2):

```bash
.venv/bin/python -m pip install -e '.[dev,spark]'
.venv/bin/chess-lakehouse download \
  --url https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst \
  --output data/real_sample_100.pgn --max-games 100
SPARK_LOCAL_IP=127.0.0.1 CHESS_TEST_DELTA=1 \
  CHESS_REAL_SAMPLE="$PWD/data/real_sample_100.pgn" \
  .venv/bin/python -m pytest -s -ra
```

Independent testing confirmed the local pipeline at commit `94f58a11304cc11c92e102f3894aabcbdcc80ce9`. The dashboard does not change pipeline behavior. The measured test and run evidence is in [Run evidence](docs/run-evidence.md). The optional locally executed Delta JAR route and browser test commands are in [Operations and validation notes](DEV_HANDOFF.md) and [Publication notes](PUBLICATION_HANDOFF.md).

## Databricks

The [Databricks bundle](databricks.yml) passed official CLI validation and was deployed as a serverless Python job. Two runs completed successfully on the same bounded 10,000-game October sample. Direct SQL queries after the second run confirmed the following results across seven managed Delta tables:

| Cloud verification | Result |
| --- | ---: |
| Bronze rows / distinct game IDs | 10,000 / 10,000 |
| Silver rows / quarantine rows | 9,996 / 4 |
| Sum of games in each of the three Gold tables | 9,996 |
| Run history rows | 2 |
| Second-run inserts / duplicates | 0 / 10,000 |

The replay demonstrates idempotency: it keeps the same unique records and adds no duplicate games. The dashboard is a static snapshot; the Databricks job is a bounded batch, not live continuous ingestion. Local Spark and Delta tests also verified parity on a separate 100-game sample. See [cloud run instructions](DEV_HANDOFF.md#databricks-run-instructions) for an authorized workspace.

## Source, license and limits

- **Data:** [Lichess Open Database](https://database.lichess.org/) · [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/). The [October 2026 archive](https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst) is a monthly export; only a bounded prefix was downloaded.
- **Code:** [MIT License](LICENSE).
- **Limits:** This is a bounded micro-batch pipeline, not a continuous stream or full-month analysis. The local pipeline and Spark read bounded files into memory; serial execution and stable input files are required. It does not validate chess move legality or provide cross-table atomic transactions. Rates are descriptive, not causal estimates of player skill.
- **Privacy:** Raw PGN, local environments, receipts and generated lakehouse outputs are ignored by Git. The public dashboard contains only grouped statistics and source metadata.
