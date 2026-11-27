# Operations and validation notes

These notes explain how to run, inspect and reproduce the lakehouse. The local pipeline passed independent testing at commit `94f58a11304cc11c92e102f3894aabcbdcc80ce9`. The v0.3 dashboard adds optional Silver fields and four Gold marts while retaining the original validation and deduplication behavior. The v0.3 Databricks bundle passed CLI validation and deployment, two serverless job runs on the 100,000-game sample, and the subsequent SQL checks. The previous 10,000-game [live dashboard](https://tasachii.github.io/chess-data-lakehouse/) and [hosted CI](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046) both passed verification. The one-million-game dashboard was exported and verified locally with Spark, then the existing Databricks schema was extended to the same one-million-game sample. Direct SQL checks matched every Gold group with the local results.

## Local replay and lineage

The local CLI writes `bronze/games.jsonl`, `silver/games.jsonl`, `quarantine/games.jsonl`, seven Gold CSVs and `manifests/latest.json` under the output directory. Every successful invocation also writes `manifests/runs/<run_id>.json`. Delta records equivalent per-file history in `ingestion_runs`.

| Field | Meaning |
| --- | --- |
| `game_id` | Eight-character canonical Lichess ID from `/game/<id>` or `/<id>`; SHA-256 fallback for other PGN. |
| `batch_id` | SHA-256 of `source_url + "\n" + source_sha256`, where the latter hashes the exact input file. |
| `run_id` | Unique ID for each successful invocation, including replays. |
| `requested_max_games`, `retrieved_at_utc` | Bound and retrieval timestamp from a matching download receipt, or null when unknown. |
| `source_kind`, `source_license`, `source_license_url` | Known public Lichess inputs are CC0-1.0, synthetic fixtures are MIT, unclassified external inputs have unknown license. |
| `pipeline_version`, `run_at_utc` | Pipeline release and run completion time. |
| `attempted`, `duplicates`, `inserted` | Parsed records, records not inserted, and newly inserted unique IDs. |
| `accepted`, `quarantined` | Valid and invalid newly inserted records. |
| Cumulative totals | Persisted Bronze, Silver and quarantine counts after the run. |
| `eco`, `termination`, `rating_gap`, `rating_gap_bucket`, `plies`, `length_bucket` | Optional v0.3 analytics. Signed gap = White Elo minus Black Elo; plies is null if mainline length is unsupported. |

Invariants: `attempted = duplicates + inserted`, `inserted = accepted + quarantined`, and `bronze = silver + quarantine`. The CLI's legacy `input_games` and `inserted_bronze` fields are batch counts; `valid_silver` and `quarantined` are cumulative. A multi-file Delta run repeats its cumulative totals on each history row; do not sum those totals across files.

Empty uncompressed input is a no-op for data and records an empty delivery receipt. Extra PGN tags stay in raw Bronze but are ignored for analytics. A missing opening becomes `Unknown`; missing player names become `unknown`. Header-only records are quarantined. Corrupt or truncated compressed input and mismatched download receipts fail before the local pipeline publishes layers. A bounded archive download deliberately stops at the requested record count, so it does not establish the integrity of the unread suffix.

**Upgrading old data:** v0.3 adds Silver columns and four Gold marts. It retains the original three Gold marts and stable IDs but uses exact half-even rate rounding. The local pipeline rejects older manifest versions, and the Delta job rejects legacy Bronze versions or Silver schemas before writing. Keep the previous v0.2 cloud schema intact and rebuild the same source in a fresh output directory or schema.

## Local Spark and Delta tests

The portable test instructions appear in [README.md](README.md#tests). The measured local Delta run used host PySpark 3.5.3, Java 21 and matching Delta 3.3.2 package/JARs. If PySpark 3.5.3 is already installed on the host, this alternative avoids downloading the full Spark package and resolves the Delta JARs explicitly. Do not run it with an incompatible host Spark version.

```bash
python3.12 -c 'import pyspark; print(pyspark.__version__)'
mkdir -p output/spark-deps/jars
.venv/bin/python -m pip install --no-cache-dir --no-deps \
  --target output/spark-deps/python 'delta-spark==3.3.2'
curl -fLsS https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
  -o output/spark-deps/jars/delta-spark_2.12-3.3.2.jar
curl -fLsS https://repo.maven.apache.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
  -o output/spark-deps/jars/delta-storage-3.3.2.jar
HOST_SITE_PACKAGES=$(python3.12 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
PYTHONPATH="$PWD/output/spark-deps/python:$HOST_SITE_PACKAGES" \
CHESS_TEST_DELTA=1 \
CHESS_DELTA_JARS="$PWD/output/spark-deps/jars/delta-spark_2.12-3.3.2.jar,$PWD/output/spark-deps/jars/delta-storage-3.3.2.jar" \
CHESS_REAL_SAMPLE="$PWD/data/real_sample_100.pgn" \
.venv/bin/python -m pytest -s -ra tests --basetemp output/spark-tests
```

Generate `data/real_sample_100.pgn` using the 100-game download command in the README before running. The local tests create temporary managed Delta tables and compare the real Silver fields and all Gold metrics. The fresh optional Spark/Delta installation and explicit matching JAR route also passed for the earlier release in [hosted CI](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046). The automatic Maven resolver was not separately tested.

## Local Spark scale check

For larger samples, use `scripts/scale_sample.py` instead of the in-memory JSONL pipeline. It reads one bounded PGN chunk per Spark input file, uses the same stable IDs, validation rules and Gold functions as the Databricks job, and caches only the compact analytics projection on disk. It checks each input file’s SHA-256 and attempted record count in Spark. A separate Python pass streams the original PGN, keeps only unique IDs and grouped counts, and reconciles all seven Spark Gold marts before exporting a dashboard. No full Bronze or Silver JSON copy is written. This path measures local transformations and aggregate export, not a managed Delta write or cloud replay.

```bash
.venv/bin/python -m pip install -e '.[dev,spark]'
SOURCE_URL=https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst
.venv/bin/chess-lakehouse download \
  --url "$SOURCE_URL" --output data/dashboard_sample_1000000.pgn --max-games 1000000
.venv/bin/python scripts/split_sample.py \
  --input data/dashboard_sample_1000000.pgn \
  --output output/scale-1m/chunks --games-per-file 10000
for games in 100000 250000 1000000; do
  SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv/bin/python" \
    .venv/bin/python -m scripts.scale_sample \
      --chunks output/scale-1m/chunks --games "$games" \
      --output "output/scale-1m/checkpoint-$games"
done
```

Use new checkpoint output directories. Each writes seven Gold CSVs, an aggregate-only `snapshot.json`, and a `report.json` with source, counts, group counts and timings. The full sample reads a single `part-*.pgn` glob; checkpoints read a verified prefix of the ordered manifest. The runner checks for at least 15 GiB of free storage before and during major stages. The measured machine has 16 GiB RAM; the local Spark driver is configured for 4 GiB and two cores. Monitor disk and memory during larger runs. The CLI’s streaming downloader stages files and preserves existing samples on a short or failed download.

If PySpark 3.5.3 is available outside the virtual environment, use the host-site-packages route from the previous section, without Delta configuration, for the scale check. The script does not need Databricks credentials or local Delta JARs. Do not run several large Spark checkpoints concurrently on a small machine.

## Databricks run instructions

The previous v0.2 serverless job passed official CLI validation and ran twice successfully on the same 10,000-game October 2026 sample. SQL verified the seven original managed Delta tables: Bronze 10,000 rows and distinct IDs, Silver 9,996, quarantine 4, all three Gold tables summed to 9,996, two run-history rows, and zero inserts / 10,000 duplicates on replay. These are historical cloud results, not a claim about v0.3 or the 100,000-game input.

### Prepare the bounded v0.3 cloud input

The local 100,000-game source is 234,164,347 decompressed bytes. Spark's binary-file reader creates one row per file, so upload ten bounded files instead of one large input. This strategy was verified in local Spark glob tests and both Databricks executions. Regenerate the original 100,000-game source using the bounded download command with `--max-games 100000` and `--output data/dashboard_sample_100000.pgn`, then:

```bash
.venv/bin/python scripts/split_sample.py \
  --input data/dashboard_sample_100000.pgn \
  --output output/dashboard-100000/chunks --games-per-file 10000
```

Use a new output directory if the chunks already exist. The splitter checks the parent's receipt and size, produces per-file receipts and a manifest, and verifies that rejoining the files with one separating newline reproduces the parent bytes. The [100,000-game chunk manifest](docs/assets/sample-chunks-manifest.json) records ten actual SHA-256 values and 10,000 records per file. Files range from 23,193,716 to 23,614,481 bytes. It contains metadata only, not raw games.

### Reproduce the verified v0.3 serverless runs

Use an authorized workspace and keep the verified v0.2 schema unchanged. The example uses a fresh generic schema and volume, `main.chess_portfolio_v3`. If retaining the old Job configuration is necessary, deploy this version as a separate bundle deployment. Cloud compute may incur charges.

```sql
CREATE SCHEMA IF NOT EXISTS main.chess_portfolio_v3;
CREATE VOLUME IF NOT EXISTS main.chess_portfolio_v3.input;
```

```bash
for part in output/dashboard-100000/chunks/part-*.pgn; do
  databricks fs cp "$part" "dbfs:/Volumes/main/chess_portfolio_v3/input/$(basename "$part")"
done
export BUNDLE_VAR_schema=chess_portfolio_v3
export BUNDLE_VAR_source_path='/Volumes/main/chess_portfolio_v3/input/part-*.pgn'
export BUNDLE_VAR_requested_max_games=10000
export BUNDLE_VAR_expected_source_sha256=''
export BUNDLE_VAR_retrieved_at_utc=$(python3 -c 'import json; print(json.load(open("data/dashboard_sample_100000.pgn.receipt.json"))["retrieved_at_utc"])')
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev chess_lakehouse
```

This is one job reading the ten PGN files via a glob. The requested bound is per file. The optional expected SHA argument accepts one file only, so it is blank for the glob; the Job records each file's computed SHA-256 and record count in `ingestion_runs`. Compare all ten source basenames, hashes and `attempted` counts with the local manifest after each successful run. Never upload receipts or raw PGN to a public repository. Repeat the same job only after checking the first run.

Both actual v0.3 serverless runs completed successfully after CLI validation and deployment in a fresh schema. Direct SQL confirmed 100,000 Bronze rows and distinct IDs, 99,875 Silver rows, 125 quarantine rows, 11 managed Delta tables, and seven Gold marts each summing to 99,875 games (20,399 groups in total). All ten file hashes and attempted counts matched the local manifest in both runs. The history contains 20 rows, ten per run; the replay inserted zero new records and counted 100,000 duplicate attempts.

```sql
SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT game_id) AS unique_ids
FROM main.chess_portfolio_v3.bronze_games;
SELECT COUNT(*) FROM main.chess_portfolio_v3.silver_games;
SELECT COUNT(*) FROM main.chess_portfolio_v3.quarantine_games;
SELECT run_id, source_path, source_sha256, attempted, inserted, duplicates
FROM main.chess_portfolio_v3.ingestion_runs ORDER BY run_id, source_path;
SELECT run_id, COUNT(*) AS files, SUM(attempted) AS attempted,
       SUM(inserted) AS inserted, SUM(duplicates) AS duplicates
FROM main.chess_portfolio_v3.ingestion_runs GROUP BY run_id;
```

Check `SUM(games)` in `gold_opening_metrics`, `gold_rating_metrics`, `gold_time_control_metrics`, `gold_opening_context`, `gold_rating_gap_metrics`, `gold_termination_metrics`, and `gold_length_metrics`. Cumulative totals are repeated on history rows; do not sum them. The verified v0.3 runs used the bounded sample, not the full archive or continuous ingestion. The job assumes immutable input and serial invocations and does not provide cross-table atomicity.

### Verified incremental expansion to 1,000,000 games

The existing v0.3 job, schema and volume were reused. Before the incremental run, SQL confirmed the original 100,000 unique Bronze rows, 99,875 Silver rows, 125 quarantined and 20 history rows. The first ten entries of the [one-million-game manifest](docs/assets/sample-chunks-1m-manifest.json) match the historical input exactly. The remaining 90 files (`part-011.pgn` through `part-100.pgn`) were added and all 100 uploaded file sizes matched the manifest.

The bundle passed validation and deployment with the all-file `part-*.pgn` glob, `requested_max_games=10000` per file, the parent receipt retrieval timestamp and a blank single-file expected SHA. The job ID was unchanged. One incremental serverless invocation completed successfully in 324.55 s.

| Direct SQL check | Observed |
| --- | ---: |
| Bronze / distinct Bronze | 1,000,000 / 1,000,000 |
| Silver / distinct Silver | 998,976 / 998,976 |
| Quarantine / distinct quarantine | 1,024 / 1,024 |
| Latest attempts / inserts / duplicates | 1,000,000 / 900,000 / 100,000 |
| Newly accepted / quarantined | 899,101 / 899 |
| Latest file SHA-256 and attempted counts matching manifest | 100 of 100 |
| Total history rows | 120 (20 historical + 100 latest) |
| Managed Delta tables | 11 |
| Gold games summed per mart / total groups | 998,976 / 45,332 |

The original 20 history rows remain present; their source hashes and counts match the historical manifest. All 100 new receipts matched source hashes, 10,000 attempted records per file, batch identities and the parent receipt timestamp. Every Gold row in all seven marts matched the local CSVs, including exact half-even rates, outcomes and known-length totals. The first ten files inserted zero new records; the other 90 each inserted 10,000. The one-million-game run was incremental; a full replay at this scale was not executed. The earlier 100,000-game replay remains the verified same-input replay. No continuous ingestion, full-archive processing or paid upgrade was involved.

References: [job task types](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types), [bundle settings](https://docs.databricks.com/aws/en/dev-tools/bundles/settings), [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations), [Delta Lake quickstart](https://docs.delta.io/quick-start/).
