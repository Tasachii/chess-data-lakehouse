# Operations and validation notes

These notes explain how to run, inspect and reproduce the lakehouse. The local pipeline passed independent testing at commit `94f58a11304cc11c92e102f3894aabcbdcc80ce9`. The dashboard adds an aggregate exporter, tests and static files without changing pipeline behavior. Databricks CLI validation, deployment, two serverless job runs and the subsequent SQL checks also passed. The previous 10,000-game [live dashboard](https://tasachii.github.io/chess-data-lakehouse/) and [hosted CI](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046) both passed verification. The current 100,000-game update was processed and tested locally.

## Local replay and lineage

The local CLI writes `bronze/games.jsonl`, `silver/games.jsonl`, `quarantine/games.jsonl`, three Gold CSVs and `manifests/latest.json` under the output directory. Every successful invocation also writes `manifests/runs/<run_id>.json`. Delta records equivalent per-file history in `ingestion_runs`.

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

Invariants: `attempted = duplicates + inserted`, `inserted = accepted + quarantined`, and `bronze = silver + quarantine`. The CLI's legacy `input_games` and `inserted_bronze` fields are batch counts; `valid_silver` and `quarantined` are cumulative. A multi-file Delta run repeats its cumulative totals on each history row; do not sum those totals across files.

Empty uncompressed input is a no-op for data and records an empty delivery receipt. Extra PGN tags stay in raw Bronze but are ignored for analytics. A missing opening becomes `Unknown`; missing player names become `unknown`. Header-only records are quarantined. Corrupt or truncated compressed input and mismatched download receipts fail before the local pipeline publishes layers. A bounded archive download deliberately stops at the requested record count, so it does not establish the integrity of the unread suffix.

**Upgrading old data:** version 0.1.0 Bronze IDs and lineage are incompatible with 0.2.0. The code rejects populated legacy Bronze rather than risking double-counting. Rebuild the original source files in a fresh output directory or new Delta schema.

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

Generate `data/real_sample_100.pgn` using the 100-game download command in the README before running. The local tests create temporary managed Delta tables and compare the real Silver fields and all Gold metrics. The fresh optional Spark/Delta installation and explicit matching JAR route also passed in [hosted CI](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046). The automatic Maven resolver was not separately tested.

## Databricks run instructions

The official Databricks CLI successfully validated the bundle, which was deployed as a serverless job. Both cloud runs succeeded on the same 10,000-game October 2026 sample. SQL queries verified all seven managed Delta tables:

| Cloud check after the second run | Verified result |
| --- | ---: |
| Bronze rows | 10,000 |
| Distinct Bronze game IDs | 10,000 |
| Silver rows | 9,996 |
| Quarantine rows | 4 |
| Games summed in each Gold table | 9,996 |
| Run history rows | 2 |
| Last run inserted | 0 |
| Last run duplicates | 10,000 |

These counts show the replay did not add duplicates. This was two bounded runs, not continuous ingestion of new games. The example commands below use the public bundle's sample schema and volume names; they do not identify any particular workspace. Running them requires an authorized Unity Catalog workspace, Databricks CLI, serverless Jobs permission and table/volume permissions. Cloud compute may incur charges.

1. Create a dedicated schema and volume in the workspace:

   ```sql
   CREATE SCHEMA IF NOT EXISTS main.chess_portfolio;
   CREATE VOLUME IF NOT EXISTS main.chess_portfolio.input;
   ```

2. Generate the separate bounded 10,000-game sample used in the verified cloud run. The default bundle is configured for 100 games, so override the input path and record bound. The 100,000-game dashboard sample has only been tested locally; do not treat the cloud results as verification at that size. Upload only the decompressed PGN and use its receipt metadata:

   ```bash
   .venv/bin/chess-lakehouse download \
     --url https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst \
     --output data/dashboard_sample_10000.pgn --max-games 10000
   databricks fs cp data/dashboard_sample_10000.pgn dbfs:/Volumes/main/chess_portfolio/input/dashboard_sample_10000.pgn
   export BUNDLE_VAR_source_path=/Volumes/main/chess_portfolio/input/dashboard_sample_10000.pgn
   export BUNDLE_VAR_requested_max_games=10000
   export BUNDLE_VAR_retrieved_at_utc=$(python3 -c 'import json; print(json.load(open("data/dashboard_sample_10000.pgn.receipt.json"))["retrieved_at_utc"])')
   export BUNDLE_VAR_expected_source_sha256=$(python3 -c 'import json; print(json.load(open("data/dashboard_sample_10000.pgn.receipt.json"))["source_sha256"])')
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run -t dev chess_lakehouse
   ```

3. Run the same job again, then check the managed tables. With a fresh schema and the verified 10,000-game sample, the expected results are listed above:

   ```sql
   SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT game_id) AS unique_ids
   FROM main.chess_portfolio.bronze_games;
   SELECT COUNT(*) FROM main.chess_portfolio.silver_games;
   SELECT COUNT(*) FROM main.chess_portfolio.quarantine_games;
   SELECT SUM(games) FROM main.chess_portfolio.gold_opening_metrics;
   SELECT SUM(games) FROM main.chess_portfolio.gold_rating_metrics;
   SELECT SUM(games) FROM main.chess_portfolio.gold_time_control_metrics;
   SELECT COUNT(*) FROM main.chess_portfolio.ingestion_runs;
   SELECT run_id, batch_id, attempted, duplicates, inserted, accepted, quarantined
   FROM main.chess_portfolio.ingestion_runs ORDER BY run_at_utc;
   ```

The job creates `bronze_games`, `silver_games`, `quarantine_games`, `gold_opening_metrics`, `gold_rating_metrics`, `gold_time_control_metrics` and `ingestion_runs`. Override catalog, schema, source path, source URL, requested bound and optional receipt metadata in the bundle variables for other inputs. If supplied, the expected checksum and bound are checked before table writes. Source paths must be bounded decompressed `.pgn` files. Use serial execution and keep input files unchanged during a run. The job does not provide cross-table atomicity or a history of interrupted attempts.

References: [job task types](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types), [bundle settings](https://docs.databricks.com/aws/en/dev-tools/bundles/settings), [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations), [Delta Lake quickstart](https://docs.delta.io/quick-start/).
