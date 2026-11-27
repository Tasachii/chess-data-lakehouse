# Chess Data Lakehouse

Version 0.2.0 (TEST1-01–04 remediation). A small, reproducible data-engineering project that turns bounded Lichess PGN exports into
Bronze, Silver, quarantine, and Gold datasets. The default path runs locally with Python 3.11
or 3.12; `src/databricks_job.py` implements the same contracts as managed Delta tables for an
existing Databricks workspace.

## What is verified

- Local tests and fixture pipeline: verified on macOS with Python 3.12.
- Real public source: verified by streaming the first 100 complete games from Lichess's October
  2026 standard rated monthly export. The 31 GB archive was **not** downloaded in full.
- Local Spark/Delta: PySpark 3.5.3 and Delta Lake 3.3.2 matched all 100 real Silver rows
  and all Gold metrics. Managed Delta tables were created and replayed locally for both
  the fixture and real sample; no duplicates were added.
- Databricks bundle: syntax/YAML checked locally, but **not executed or CLI-validated in Databricks**.
  No workspace credentials, account creation, billing, or cloud resources were used.

Developer remediation evidence is in [DEV_HANDOFF.md](DEV_HANDOFF.md) and historical runs in
[`docs/run-evidence.md`](docs/run-evidence.md). Independent QA retest is pending.

## Architecture

```text
Lichess monthly PGN.zst
        │ bounded streaming decompression (stop after N complete games)
        ▼
Bronze: stable game_id + raw PGN + source URL + batch ID + SHA-256
        │ validate timestamp, result, ratings, and time control
        ├──────────────► Quarantine: raw PGN + validation errors
        ▼
Silver: one validated, typed row per game
        │ group by opening / rating bucket / speed
        ▼
Gold: games, white wins, draws, white win rate
```

The local path writes JSONL/CSV so every layer is inspectable without Spark. The Databricks path
uses Delta tables and insert-only SQL `MERGE` on `game_id`, then rebuilds the small Gold aggregates
from the full Silver table. Both paths use the exact 8-character Lichess ID from either
`https://lichess.org/<id>` or `https://lichess.org/game/<id>` in the `Site` tag (optional trailing
path). The October 2026 sample uses the latter form. Nonmatching URLs use a SHA-256 hash of
canonical PGN content; an unrelated hostname containing `lichess.org` is not trusted.
Both parsers split on every line beginning `[Event `, even if the previous game has no moves.

**Upgrade from 0.1.0:** old Bronze keys could be content hashes for actual Lichess games, and old
rows lack batch lineage. Do not append v0.2.0 data to old output directories or Delta tables.
The pipeline rejects populated legacy Bronze instead of silently double-counting. Rebuild from
the original raw input(s) in a fresh output directory or dedicated fresh Delta schema. Original
artifacts can be retained for historical evidence. The commands below use new `-v2` output paths.

## Quick start: offline fixture

```bash
cd chess-data-lakehouse  # from the parent directory
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'

.venv/bin/chess-lakehouse run \
  --input tests/fixtures/sample.pgn \
  --output output/fixture-v2 \
  --source-url fixture://sample.pgn

.venv/bin/pytest
.venv/bin/ruff check .
```

Run the same command twice. The second result reports `"inserted_bronze": 0`; existing games are
not appended again. The synthetic fixture intentionally contains one duplicate and one malformed
game. It is test data, not evidence about Lichess production quality. The base test run skips the
optional Spark module if PySpark is not installed.

## Bounded real-source run

The downloader validates TLS, incrementally decompresses Zstandard, and stops after the requested
number of Event-delimited PGN records. The observed real sample contains 100 complete games.
If EOF is reached before the requested record count or a frame ends prematurely before the
bound, the download fails without publishing a new sample or receipt. Deliberate early stop at
the bound does **not** validate the unread monthly-archive suffix. Local `run` fully validates
all compressed frame boundaries for bounded `.zst` inputs before writing any output.

```bash
SOURCE_URL=https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst

.venv/bin/chess-lakehouse download \
  --url "$SOURCE_URL" \
  --output data/real_sample_100.pgn \
  --max-games 100

.venv/bin/chess-lakehouse run \
  --input data/real_sample_100.pgn \
  --output output/real-v2 \
  --source-url "$SOURCE_URL"
```

`data/*.pgn`, `data/*.zst`, `data/*.receipt.json`, and `output/` are ignored: raw source data and generated lakehouse layers are
reproducible artifacts, not repository content. Inspect `output/real-v2/manifests/latest.json`, the
three layer directories, and `output/real-v2/gold/*.csv` after a run. The downloaded PGN has a
matching `.pgn.receipt.json` containing the publisher URL, receipt SHA-256, requested sample
bound, actual byte/game counts and retrieval time.

## Optional local Spark / Delta verification

The Spark integration tests use the same `build_lakehouse` function as the Databricks job,
with local managed tables in a temporary Spark warehouse. Install Java (JDK 17 or compatible),
then run from the project root:

```bash
.venv/bin/python -m pip install -e '.[dev,spark]'
# Fixture Spark transformations (no Delta JVM artifacts required):
SPARK_LOCAL_IP=127.0.0.1 .venv/bin/python -m pytest -ra
# Include real-data parity and two Delta replay tests (requires the bounded sample above):
SPARK_LOCAL_IP=127.0.0.1 CHESS_TEST_DELTA=1 \
  CHESS_REAL_SAMPLE="$PWD/data/real_sample_100.pgn" \
  .venv/bin/python -m pytest -s -ra
```

The Delta tests resolve compatible JVM artifacts from Maven via the official
[Delta Lake quickstart](https://docs.delta.io/quick-start/). If you already have the matching
Delta Spark and Delta Storage JARs locally, set `CHESS_DELTA_JARS` to their comma-separated
absolute paths instead. The full test command also exercises the TEST1 regressions.
Without the optional real-data path or Delta flag, the corresponding tests are skipped.
The exact locally executed alternative using an existing PySpark installation is in
[`DEV_HANDOFF.md`](DEV_HANDOFF.md).

## Data contracts and grain

| Layer/table | Grain | Key fields | Update behavior |
|---|---|---|---|
| Bronze games | one unique raw game | `game_id`, `raw_pgn`, source URL, `source_sha256`, `batch_id` (Delta also records path/metadata) | insert only by `game_id` |
| Silver games | one validated game | timestamp, players, Elo, result, opening, time control, dimensions | insert only by `game_id` |
| Quarantine games | one invalid unique game | `game_id`, errors, raw PGN | insert only by `game_id` |
| Gold opening | one row per opening | games, wins, draws, win rate | full aggregate rebuild |
| Gold rating | one row per 200-point average-rating bucket | same metrics | full aggregate rebuild |
| Gold time control | one row per speed | same metrics | full aggregate rebuild |
| Run lineage | one row per source file per successful invocation | `run_id`, `batch_id`, source metadata, per-delivery counts, cumulative totals | append only |

Speed uses Lichess's estimated duration convention (`initial + 40 × increment`): under 180 seconds
is bullet (including ultrabullet), under 600 blitz, under 1,800 rapid, otherwise classical. A game's rating bucket uses
the integer mean of White and Black Elo. `white_win_rate` is descriptive, not a causal measure.

## Machine-readable lineage and count semantics

Each source file has a stable `batch_id = SHA256(source_url + "\n" + source_sha256)`, where
`source_sha256` hashes the exact ingested file bytes (decompressed PGN for downloader output,
compressed bytes if calling local `run` directly on a `.zst`). `run_id` identifies each invocation,
including replays. Each Bronze row references the batch that first inserted its ID. The local
pipeline writes `manifests/runs/<run_id>.json` and updates `manifests/latest.json`; earlier runs are
not overwritten. Delta writes equivalent per-file rows to `ingestion_runs`, with the same `run_id`
for all files in a multi-file run. For a multi-file run, per-file counters sum to run counters;
cumulative totals are repeated on each row and should not be summed.

| Field | Meaning |
|---|---|
| `source_kind`, `source_license`, `source_license_url` | `lichess_public` / CC0-1.0 for the known publisher; `synthetic_fixture` / MIT for `fixture://` test inputs; otherwise `external` and unknown license (`null`). |
| `requested_max_games`, `retrieved_at_utc` | Actual download bound/time from a matching receipt; `null` if unknown (e.g., manually supplied PGN or synthetic fixture). Never substituted with ingestion timestamps. |
| `pipeline_version`, `run_at_utc` | Code release (0.2.0) and successful run completion time in UTC. |
| `attempted` | All parsed records in this delivery, including duplicates and malformed games. |
| `duplicates` | Attempted records not inserted (duplicate within delivery or already in Bronze). |
| `inserted`, `accepted`, `quarantined` | Newly inserted unique Bronze IDs, and their valid/invalid split. |
| `cumulative_counts` / Delta `*_total` | Persisted Bronze/Silver/quarantine totals after the run. |

The invariants are `attempted = duplicates + inserted`, `inserted = accepted + quarantined`,
and cumulative `bronze = silver + quarantine`. The legacy CLI `counts` fields remain compatible:
`input_games` and `inserted_bronze` are batch counts, `valid_silver` and `quarantined` are cumulative.
On empty uncompressed PGN, attempted/inserted/accepted/quarantined are 0; existing data stays
unchanged, but an empty delivery receipt is recorded. Unknown extra PGN tags are preserved in raw
Bronze and ignored for analytics; missing Opening is `Unknown`, missing players `unknown`.
A header-only record is quarantined (missing move text); moves are not checked for legality.
A corrupted/truncated compressed file fails before any lakehouse publication, and a mismatched
receipt is rejected rather than silently accepted.

## Databricks / Delta Job (existing authorized workspace only)

The bundle uses a serverless Python task and runtime-provided PySpark/Delta. No workspace execution
has been verified. Check the target
workspace's pricing and permissions before deploying; these commands can create billable compute.

1. Install and authenticate the Databricks CLI to an existing authorized Unity Catalog workspace
   with serverless Jobs available. Use a dedicated project schema and permissions to create/modify
   tables and read/write the input volume.
2. Create a Unity Catalog schema and volume with SQL:

   ```sql
   CREATE SCHEMA IF NOT EXISTS main.chess_portfolio;
   CREATE VOLUME IF NOT EXISTS main.chess_portfolio.input;
   ```

3. Generate the bounded local sample, then upload only that small decompressed file:

   ```bash
   databricks fs cp data/real_sample_100.pgn \
     dbfs:/Volumes/main/chess_portfolio/input/real_sample_100.pgn
   # Populate the job's receipt parameters from the actual local download receipt.
   export BUNDLE_VAR_retrieved_at_utc=$(python3 -c 'import json; print(json.load(open("data/real_sample_100.pgn.receipt.json"))["retrieved_at_utc"])')
   export BUNDLE_VAR_expected_source_sha256=$(python3 -c 'import json; print(json.load(open("data/real_sample_100.pgn.receipt.json"))["source_sha256"])')
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run -t dev chess_lakehouse
   ```

Override `catalog`, `schema`, `source_path`, `source_url`, `requested_max_games`, or optional receipt
metadata in `databricks.yml` or with supported Job parameter options. For other input files,
use correct metadata or blank unknown fields; when supplied, receipt SHA-256 and bound are checked
before table writes. The job creates `bronze_games`, `silver_games`, `quarantine_games`, three `gold_*`
and `ingestion_runs` managed Delta tables. Delta `MERGE` makes input replays idempotent. Run the same Job again and
inspect the counts and duplicate keys in the task logs / SQL editor (expected for a freshly created
schema and the 100-game sample: 100 Bronze, 100 Silver, 0 quarantine, 100 distinct Bronze IDs):

```sql
SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT game_id) AS unique_ids
FROM main.chess_portfolio.bronze_games;
SELECT COUNT(*) FROM main.chess_portfolio.silver_games;
SELECT COUNT(*) FROM main.chess_portfolio.quarantine_games;
SELECT * FROM main.chess_portfolio.gold_time_control_metrics ORDER BY speed;
SELECT run_id, batch_id, attempted, duplicates, inserted, accepted, quarantined
FROM main.chess_portfolio.ingestion_runs ORDER BY run_at_utc;
```

These are verification instructions, not observed cloud results.

The configuration follows the official documentation for
[Python script Job tasks](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types),
[Job parameters](https://docs.databricks.com/aws/en/dev-tools/bundles/job-parameters), and
[Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/settings),
and [serverless supported data sources](https://docs.databricks.com/aws/en/compute/serverless/limitations).

## Source, license, and limitations

Lichess publishes monthly, non-cumulative standard-rated game exports at the
[Lichess Open Database](https://database.lichess.org/) under
[Creative Commons CC0](https://creativecommons.org/publicdomain/zero/1.0/). The source page reports
95,395,628 games and 31 GB for the October 2026 archive; this project samples only its leading 100
records and does not generalize sample distributions to the full archive.

Important limits:

- This is bounded micro-batch ingestion, not an always-on event stream.
- The local implementation rewrites its small Silver/Gold outputs on each run; Bronze remains
  insert-only. It is designed for portfolio-scale samples, not multi-gigabyte local processing.
- Spark reads each bounded decompressed input as a whole binary file; do not upload an entire
  monthly archive. Local and Delta ingestion retain the first observed delivery for any ID,
  even if later versions conflict. Run serially; concurrent local writers are not supported.
- Input files must be stable during a run. The local path and Delta Job assume serial operation;
  history records successful runs only, not atomic multi-table transactions or interrupted attempts.
- PGN validation checks analytics-required tags and move-text presence, not chess move legality or arbitrary PGN formats.
  Elo is constrained to 100–4000 and time control uses nonnegative integer `initial+increment`.
- Missing `Opening` becomes `Unknown`; missing/invalid required analytics fields are quarantined.
- Rating and outcome aggregates are descriptive. They do not measure player skill causally.
- Databricks execution remains unverified until someone with an authorized workspace runs it.

Project code is MIT-licensed; Lichess source records remain CC0.
