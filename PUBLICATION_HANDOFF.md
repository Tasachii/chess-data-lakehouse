# Publication notes — one-million-game snapshot

The one-million-game dashboard is published from `main:/docs`. The [Pages build](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46038266738) at commit `7f45e0e` and [hosted CI](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46038266966) passed; the live snapshot matched the committed bytes. The subsequent Databricks incremental run also passed direct SQL verification against the local one-million-game results.

## Measured local results

| Check | Result |
| --- | ---: |
| Actual bounded input | 1,000,000 records / 2,321,066,081 bytes |
| Download / chunk split | 221.06 s / 14.92 s |
| Input files | 100 × 10,000 records |
| Bronze / Silver / quarantine | 1,000,000 / 998,976 / 1,024 |
| White wins / Black wins / draws | 497,294 / 463,906 / 37,776 |
| Opening labels / opening-context groups | 2,675 / 42,618 |
| Gold marts / total groups | 7 / 45,332 |
| Spark + receipt checks / full independent reconciliation | 192.74 s / 300.13 s |
| Second independent export | 117.37 s; identical snapshot SHA-256 |
| Aggregate JSON / locally gzipped size | 3,783,288 / 338,923 bytes |

The actual source is the October 2026 Lichess rated standard archive prefix, covering validated games from 1 October 2026, 00:00:00–10:45:58 UTC. It is not a representative sample of the full month. The decompressed PGN SHA-256 is `6050321afa4d758518168b63f58b5845f2dec4947a1ce92c6d6b25b6c70905a0`; the aggregate snapshot SHA-256 is `0bc49de7258a0424657a469074be3963c207f7ef5f7d1b110d8b764177988c5a`.

All 100 chunk SHA-256 values and attempted counts matched the [manifest](docs/assets/sample-chunks-1m-manifest.json); the first ten entries match the verified historical 100,000-game manifest. The independently streamed Python pass matched all seven Gold marts and their exact half-even rates; each mart sums to 998,976 games. A second independent export produced the same snapshot bytes. The final 100,000-game checkpoint also reproduced the historical dashboard SHA-256 exactly. All checkpoint results, including the 250,000-game run, are recorded in the [local run report](docs/assets/scale-run-report.json) and [run evidence](docs/run-evidence.md).

The original Python JSONL pipeline and Databricks job were not rewritten. The downloader now writes and hashes games incrementally, with staged publication to protect successful previous downloads. The local Spark runner writes only Gold CSVs, the public aggregate snapshot and a report; temporary analytics data is removed. It does not produce full persistent Delta tables. The local test machine has 16 GiB RAM; Spark used two local cores, a 4 GiB driver heap and disk-backed scratch space. The lowest sampled free disk was about 28 GiB, above the 15 GiB stop threshold.

## Verify locally

The portable reproduction commands are in [README](README.md#reproduce-the-snapshot) and [operations notes](DEV_HANDOFF.md#local-spark-scale-check). The local checkpoints used the original 100,000-game chunk directory and the expanded chunk directory, respectively:

```bash
HOST_SITE_PACKAGES=$(python3.12 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
export PYTHONPATH="$PWD/src:$HOST_SITE_PACKAGES"
SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv/bin/python" \
  .venv/bin/python -m scripts.scale_sample \
  --chunks output/dashboard-100000/chunks --games 100000 \
  --output output/scale-1m/checkpoint-100k-final
SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv/bin/python" \
  .venv/bin/python -m scripts.scale_sample \
  --chunks output/scale-1m/chunks --games 250000 \
  --output output/scale-1m/checkpoint-250k
SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/.venv/bin/python" \
  .venv/bin/python -m scripts.scale_sample \
  --chunks output/scale-1m/chunks --games 1000000 \
  --output output/scale-1m/checkpoint-1m
```

The measured PySpark installation used the host PySpark 3.5.3 and Java 21 route in the operations notes. Use new output directories when repeating these commands. Only the parent PGN, chunk files and aggregate evidence are needed; all raw files stay ignored by Git.

| Verification | Result |
| --- | --- |
| `python -m pytest -ra` | 40 passed; optional Spark module skipped |
| Full local Spark/Delta tests with real 100-game fixture | 51 passed in 78.94 s |
| `ruff check .` / `ruff format --check src tests scripts` | Passed |
| `node --check` for dashboard and browser test | Passed |
| Markdown links and anchors | All 41 local targets passed after cloud-status update |
| Playwright against one-million-game snapshot | Passed filters, exact counts and rates, reset, empty states, six viewport widths, no browser errors |
| Screenshots | [Desktop](docs/assets/dashboard-preview.png) and [filtered opening study](docs/assets/opening-study.png) captured from the actual one-million-game site; 320 px mobile also inspected |

To repeat the browser checks, install Playwright and serve the docs folder locally:

```bash
npm --prefix output/dashboard-browser install --no-save playwright@1.65.1
mkdir -p output/dashboard-preview-server
ln -sfn "$PWD/docs" output/dashboard-preview-server/chess-data-lakehouse
.venv/bin/python -m http.server 18767 --bind 127.0.0.1 --directory output/dashboard-preview-server
```

In another terminal:

```bash
NODE_PATH="$PWD/output/dashboard-browser/node_modules" \
DASHBOARD_BROWSER_CHANNEL=chrome \
DASHBOARD_URL=http://127.0.0.1:18767/chess-data-lakehouse/ \
node scripts/test_dashboard.cjs
```

## Databricks verification

One incremental serverless job completed successfully in 324.55 s using the existing v0.3 job, schema and volume. Validation and deployment passed. All 100 files were present at the exact manifest sizes before the run; the first ten files were retained from the earlier cloud sample and 90 files were added.

| Direct SQL check | Result |
| --- | ---: |
| Bronze / distinct Bronze | 1,000,000 / 1,000,000 |
| Silver / quarantine | 998,976 / 1,024 |
| Latest attempted / inserted / duplicates | 1,000,000 / 900,000 / 100,000 |
| Newly accepted / quarantined | 899,101 / 899 |
| Latest file receipt hashes and counts matching manifest | 100 / 100 |
| Ingestion history rows | 120 (20 historical + 100 latest) |
| Managed Delta tables | 11 |
| Gold groups matching local CSVs | 45,332 / 45,332 |

All seven Gold marts each sum to 998,976 games, with outcomes, exact half-even rates and length totals matching local results. The earlier 100,000-game same-input replay remains verified: two successful serverless runs, 100,000 unique Bronze rows, 99,875 Silver, 125 quarantine, 20 history rows and zero inserts / 100,000 duplicate attempts on replay. The latest one-million-game run verified incremental overlap handling; no full replay at one million games was performed. An independent check also matched the SQL receipts and every Gold row against the manifest and independently validated local results.

See the [operations notes](DEV_HANDOFF.md#verified-incremental-expansion-to-1000000-games) and [measured SQL results](docs/run-evidence.md#databricks-cloud-verification). The dashboard remains a static snapshot, not live ingestion or full-archive analysis. No paid upgrade was used.
