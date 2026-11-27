# Chess Data Lakehouse

A data engineering project that turns public Lichess PGN exports into Bronze, Silver, quarantine and Gold datasets. Version 0.3 adds opening analysis by ECO, rating and time control, plus rating-gap, termination and game-length metrics. It uses Python for a small local pipeline and PySpark/Delta Lake for larger samples and managed tables. The accompanying dashboard explores verified aggregate metrics without publishing player names, game IDs or raw PGN.

I enjoy chess, and I wanted to see if I could build a repeatable data pipeline using real game data. This project grew out of that curiosity.

**Dashboard:** [Live dashboard](https://tasachii.github.io/chess-data-lakehouse/) (earlier release) · [Dashboard source](docs/index.html) · [1,000,000-game aggregate data](docs/assets/snapshot.json) · [Passing CI run for the previous release](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046)

![Dashboard preview: one million games from the October 2026 archive](docs/assets/dashboard-preview.png)

![Opening study: Italian Game filtered by Elo and time control with White, Black and draw results](docs/assets/opening-study.png)

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
  --output output/fixture-v3 \
  --source-url fixture://sample.pgn
```

The synthetic fixture contains three records (one duplicate and one malformed game). The first run produces two Bronze records, one valid Silver record and one quarantined record. Repeat the same command to confirm `inserted_bronze: 0`. The fixture is not real Lichess data.

## Real data and dashboard

The current dashboard uses the first **1,000,000 games** from the October 2026 Lichess rated standard archive. The validated games have start times between 00:00:00 and 10:45:58 UTC on October 1, 2026. This is a static archive snapshot, not a live feed or random sample.

| Metric | Measured value |
| --- | ---: |
| Unique Bronze records | 1,000,000 |
| Valid Silver games | 998,976 |
| Quarantined | 1,024 |
| Opening labels / rating buckets / speeds | 2,675 / 15 / 4 |
| Opening / ECO / rating / speed combinations | 42,618 |
| Games with valid ECO / known mainline length | 997,485 / 997,485 |
| White wins / Black wins / draws | 497,294 / 463,906 / 37,776 |

Local Spark processed all 100 source files (10,000 games each). A separate streaming Python pass matched every group and rounded rate in all seven Gold marts, totaling 45,332 groups. The dashboard exports aggregate labels, counts and source metadata only. For example, the literal Italian Game search matches 40,544 validated games (20,212 White wins), Ruy Lopez matches 16,814 (8,421 White wins), and Sicilian Defense matches 96,289 (46,148 Black wins). These totals include matching variations.

The one-million-game scale check took 300.13 seconds including receipt checks, Spark transformations and independent Python verification. The previous 100,000-game sample also reproduced the exact earlier dashboard bytes. See the [checkpoint measurements](docs/assets/scale-run-report.json) and [one-million-game chunk manifest](docs/assets/sample-chunks-1m-manifest.json).

### Reproduce the snapshot

Install a compatible JDK (Java 17 or 21) and the Spark extra. The following commands download a bounded sample, split it and build a verified aggregate snapshot locally, without a workspace or cloud credentials.

```bash
.venv/bin/python -m pip install -e '.[dev,spark]'
SOURCE_URL=https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst
.venv/bin/chess-lakehouse download \
  --url "$SOURCE_URL" --output data/dashboard_sample_1000000.pgn --max-games 1000000
.venv/bin/python scripts/split_sample.py \
  --input data/dashboard_sample_1000000.pgn \
  --output output/scale-1m/chunks --games-per-file 10000
SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv/bin/python" \
  .venv/bin/python -m scripts.scale_sample \
    --chunks output/scale-1m/chunks --games 1000000 \
    --output output/scale-1m/checkpoint-1000000
```

Use new output directories. The generated candidate is `output/scale-1m/checkpoint-1000000/snapshot.json`; the checked-in snapshot is preserved. The raw PGN and chunks together occupy about 4.64 GB, plus Spark scratch space; the runner requires at least 15 GiB free at its stage checks. The measured local machine has 16 GiB RAM and used a 4 GiB Spark driver with two local cores. Start with 100,000 and 250,000-game checkpoints as described in the [operations notes](DEV_HANDOFF.md#local-spark-scale-check). A re-download creates a new retrieval timestamp; the source checksum and group metrics identify the underlying sample.

### Preview the dashboard locally

```bash
.venv/bin/python -m http.server 8765 --directory docs
```

Open [http://localhost:8765/](http://localhost:8765/). Search opening names, family names or ECO codes (for example `C50`). White opening ideas and Black defenses are shortcuts to real Lichess labels. Select an average-Elo bucket, speed and minimum games to compare a meaningful slice. The table explicitly shows White wins, Black wins and draws with counts and rates. Select a color perspective to emphasize its win rate, or a result filter to count only matching outcomes. Rates always divide by all games in the selected opening group and slice. Family means the label before the first colon; it is not an official taxonomy or an exclusive assignment to one color. The separate rating-gap, termination and length charts show the full sample.

The site uses plain HTML, CSS and JavaScript. It requires no build step, server API or account. The earlier 10,000-game release was published on GitHub Pages from `main:/docs` and passed independent live checks. This one-million-game dashboard was tested locally; its GitHub Pages deployment and hosted CI have not yet been verified.

## Architecture

```text
Lichess PGN.zst → bounded download and receipt → Bronze (raw, deduplicated)
                                                   │
                                                   ├── Quarantine (invalid)
                                                   ↓
                                                Silver (typed)
                                                   ↓
                   Dashboard JSON ← Gold (opening context, rating, speed,
                                            rating gap, termination, length)
```

| Layer | Grain | Storage / update |
| --- | --- | --- |
| Bronze | One record per unique game ID | JSONL or Delta; insert only |
| Silver | One validated game | JSONL or Delta; local rebuild, Delta insert only |
| Quarantine | One invalid unique game | JSONL or Delta; retained with errors |
| Gold | One row per opening, rating bucket or speed | CSV or Delta; full aggregate rebuild |
| Opening context | One row per opening, ECO, rating bucket and speed | Exact slice counts; stable cross-filtering |
| Additional Gold | One row per signed rating-gap bucket, termination or mainline-length bucket | Outcome counts; length also stores known-game and total-ply counts |
| Run history | One source file per successful invocation | JSON manifests or `ingestion_runs` Delta table; append only |

| Decision | Reason |
| --- | --- |
| Stable ID from the Lichess `Site` URL | Recognizes both `/game/<id>` and `/<id>`; a content hash is used for nonmatching URLs. Prevents replay double counts. |
| Split on each Event tag | Keeps incomplete records from contaminating valid ones. |
| Validate before aggregation | Required fields, integer Elo within 100–4000, UTC timestamp, result, time control and move-text presence. |
| Durable batch identity | Source URL and exact file SHA-256 identify a batch; each Bronze row refers to its first batch. |
| Aggregate-only export | Dashboard never needs the raw source or individual game records. |

For the one-million-game local scale check, Spark uses the same deduplication, Silver validation and Gold functions as the managed-table job, but does not write full Bronze/Silver tables. It retains a compact temporary analytics projection and verifies the exported Gold groups against a streaming Python pass. The regular Python JSONL and managed Delta paths remain available for smaller samples and persistent ingestion.

A rating bucket is the integer mean of White and Black Elo, grouped by 200 points. Speed uses an estimated duration of `initial seconds + 40 × increment`: under 180 is bullet (including ultrabullet), under 600 blitz, under 1,800 rapid, otherwise classical. The outcome rate divides the relevant count by validated games in the group. Stored rates are rounded to four decimal places with half-even ties in Python and Spark; the dashboard recomputes rates directly from integer counts, not other rounded rates. A signed rating gap is White Elo minus Black Elo before the game. Valid ECO and recognized termination tags are preserved; unsupported or missing values are `Unknown`. Mainline length counts SAN-shaped half-moves after removing simple PGN comments, without testing move legality; result-only or unsupported move text is null and grouped as `Unknown`. Clock/eval annotations are audited for coverage but not used as performance measures.

More on the data contracts, replay behavior and cloud configuration: [Operations and validation notes](DEV_HANDOFF.md).

## Tests

```bash
.venv/bin/python -m pytest -ra
.venv/bin/ruff check .
.venv/bin/ruff format --check src tests scripts
```

The [Chess Lakehouse CI workflow](.github/workflows/ci.yml) runs on pushes, pull requests and manual dispatch using GitHub-hosted Ubuntu runners. It checks the base Python tests, including the aggregate export, Ruff lint and formatting, and JavaScript syntax. A separate job runs Spark/Delta fixture tests with Python 3.12 and Java 17; it needs no credentials, real archive download or Databricks workspace. Both hosted jobs passed for the previous release in [this CI run](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046). Real-data Spark parity is tested locally, and browser interactions have passed independent live-site checks.

The default local suite tests ingestion, validation, deduplication, corrupted/truncated inputs, lineage and aggregate export. Spark tests are optional. To run the full suite, install a compatible JDK and the pinned Spark/Delta extra (PySpark 3.5.3, Delta 3.3.2):

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

The v0.3 [Databricks bundle](databricks.yml) passed official CLI validation and deployment. A serverless Python job ran twice successfully in a fresh schema on the first 100,000 games of the October archive, split into ten 10,000-game PGN files. Their exact hashes match the first ten chunks of the expanded local sample. Direct SQL queries after the second run confirmed:

| Cloud verification | Result |
| --- | ---: |
| Bronze rows / distinct game IDs | 100,000 / 100,000 |
| Silver rows / quarantine rows | 99,875 / 125 |
| Managed Delta tables | 11 |
| Sum of games in each of the seven Gold marts | 99,875 |
| Total Gold groups across seven marts | 20,399 |
| Source hashes and attempted counts matching the manifest | All 10 files in both runs |
| Run history rows | 20 (10 per run) |
| Second-run inserts / duplicate attempts | 0 / 100,000 |

The replay confirms idempotency at 100,000 games: the same input added no new records. The job reads the ten chunks through one glob and checks each file’s record bound. The one-million-game snapshot was verified locally with Spark; managed Delta writes and replay at this scale have not been verified. The dashboard is not connected to Databricks, and the cloud runs were bounded batches, not continuous ingestion or a full-archive run. See the [cloud run instructions](DEV_HANDOFF.md#databricks-run-instructions), [chunk manifest](docs/assets/sample-chunks-manifest.json) and [measured results](docs/run-evidence.md#databricks-cloud-verification).

The earlier v0.2 cloud runs remain separate historical evidence: two successful runs on 10,000 games produced 10,000 unique Bronze records, 9,996 Silver records, 4 quarantined records and seven Delta tables. Each of three Gold marts summed to 9,996, with two history rows and zero inserts / 10,000 duplicate attempts on replay.

## Source, license and limits

- **Data:** [Lichess Open Database](https://database.lichess.org/) · [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/). The [October 2026 archive](https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst) is a monthly export; only a bounded prefix was downloaded.
- **Code:** [MIT License](LICENSE).
- **Limits:** This is a bounded micro-batch pipeline, not a continuous stream or full-month analysis. The small Python pipeline materializes its input; the scalable Spark path reads bounded chunks and uses disk-backed scratch space. Serial ingestion and stable input files are required. It does not validate chess move legality or provide cross-table atomic transactions. Unknown optional fields stay unknown; the average length excludes unknown lengths. Rates are descriptive, not causal estimates of player skill.
- **Privacy:** Raw PGN, local environments, receipts and generated lakehouse outputs are ignored by Git. The public dashboard contains only grouped statistics and source metadata.
