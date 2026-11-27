# Operations and validation notes

These notes explain how to run, inspect and reproduce the lakehouse. The local pipeline passed independent QA at commit `94f58a11304cc11c92e102f3894aabcbdcc80ce9`. The dashboard adds an aggregate exporter, tests and static files; the approved pipeline is unchanged. Databricks workspace execution remains unverified.

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

Generate `data/real_sample_100.pgn` using the 100-game download command in the README before running. The local tests create temporary managed Delta tables and compare the real Silver fields and all Gold metrics. The full optional fresh installation and the automatic Maven resolver were not independently verified; the explicit host/JAR route was.

## Databricks run instructions

The following is a documented deployment path, not a record of a successful cloud run. It requires an existing authorized Unity Catalog workspace, the Databricks CLI, serverless Jobs permission and the ability to create tables and read/write a volume. Cloud compute may incur charges.

1. Create a dedicated schema and volume in the workspace:

   ```sql
   CREATE SCHEMA IF NOT EXISTS main.chess_portfolio;
   CREATE VOLUME IF NOT EXISTS main.chess_portfolio.input;
   ```

2. Generate the bounded 100-game sample from the README. The default Job parameters refer to that sample. Upload only the decompressed PGN file and use its receipt values for the bundle:

   ```bash
   databricks fs cp data/real_sample_100.pgn dbfs:/Volumes/main/chess_portfolio/input/real_sample_100.pgn
   export BUNDLE_VAR_retrieved_at_utc=$(python3 -c 'import json; print(json.load(open("data/real_sample_100.pgn.receipt.json"))["retrieved_at_utc"])')
   export BUNDLE_VAR_expected_source_sha256=$(python3 -c 'import json; print(json.load(open("data/real_sample_100.pgn.receipt.json"))["source_sha256"])')
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run -t dev chess_lakehouse
   ```

3. Run again and check the managed tables. With a fresh schema and the known 100-game sample, the expected counts (from local Delta tests) are 100 Bronze, 100 Silver and 0 quarantine:

   ```sql
   SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT game_id) AS unique_ids
   FROM main.chess_portfolio.bronze_games;
   SELECT COUNT(*) FROM main.chess_portfolio.silver_games;
   SELECT COUNT(*) FROM main.chess_portfolio.quarantine_games;
   SELECT * FROM main.chess_portfolio.gold_time_control_metrics ORDER BY speed;
   SELECT run_id, batch_id, attempted, duplicates, inserted, accepted, quarantined
   FROM main.chess_portfolio.ingestion_runs ORDER BY run_at_utc;
   ```

The job creates `bronze_games`, `silver_games`, `quarantine_games`, `gold_opening_metrics`, `gold_rating_metrics`, `gold_time_control_metrics` and `ingestion_runs`. Override catalog, schema, source path, source URL, requested bound and optional receipt metadata in the bundle variables for other inputs. If supplied, the expected checksum and bound are checked before table writes. Source paths must be bounded decompressed `.pgn` files. Use serial execution and keep input files unchanged during a run. The job does not provide cross-table atomicity or a history of interrupted attempts.

References: [job task types](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types), [bundle settings](https://docs.databricks.com/aws/en/dev-tools/bundles/settings), [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations), [Delta Lake quickstart](https://docs.delta.io/quick-start/).
