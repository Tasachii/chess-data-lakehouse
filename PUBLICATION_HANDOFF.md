# Publication notes — v0.3 opening analytics

The enriched 100,000-game dashboard is ready for independent review and publication from `main:/docs`. The previous live release and cloud results remain separate: the [earlier dashboard](https://tasachii.github.io/chess-data-lakehouse/) and [hosted CI](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046) were verified, and Databricks ran two 10,000-game v0.2 jobs. The new v0.3 site, CI job and Databricks deployment have not been run remotely.

## What changed

- The 100,000-game v0.2 baseline is preserved at `4753c4a`. Version 0.3 retains stable IDs, validation and the original Gold groupings, adds ECO, termination, signed rating gap and nullable lexical mainline plies to Silver, and adds four Gold marts. Older schemas are rejected before writes; rebuild the same source in a fresh schema or output directory.
- The opening table shows White wins, Black wins and draws with counts and rates. Opening-family/name/ECO search, White/Black perspective, result, average Elo, speed and minimum game filters work against the same opening-context mart. The remaining charts show the full sample. No player-level data is published.
- The half-even rounding contract matches Python and Spark, including the 77/160 exact tie. Rates are computed from integer counts, not rounded rates.

## Data evidence

| Check | Result |
| --- | --- |
| Actual source | 100,000 records / 234,164,347 bytes, original receipt and SHA unchanged |
| v0.3 local Bronze / Silver / quarantine | 100,000 / 99,875 / 125 |
| Local first/replay inserts | 100,000 / 0 |
| Local first/replay wall time | 12.87 s / 13.87 s |
| Opening-context rows | 18,342 |
| Known ECO / termination / mainline length | 99,633 / 99,875 / 99,633 accepted games |
| Full 100,000-game local Spark glob check | All 20,399 rows across 7 Gold marts match Python counts, length metrics and rounded rates; 33.42 s wall clock |
| Current snapshot SHA-256 | `71c078b99c9ec5f2177166704fd5c8a99512cca8082d190c9b2bbbcca94bb239` (identical on local replay) |

The source prefix covers 1 October 2026, 00:00:00–01:22:41 UTC. All 125 quarantined records have invalid time controls; 25 also have invalid results. Unknown ECO and mainline length remain explicit categories. Clock and eval tags were audited, not used as elapsed-time or engine-quality metrics. [Run evidence](docs/run-evidence.md) records actual field coverage, sample counts and limitations. The compact aggregate JSON is 1,652,287 bytes uncompressed (146,953 bytes when locally gzipped); it contains no usernames, game IDs or PGN.

## Cloud preparation (not executed)

The [chunk manifest](docs/assets/sample-chunks-manifest.json) records ten 10,000-game files and exact SHA-256 values. Each file is about 23–24 MB. The splitter verified that the reconstructed parent matches the original source. Prepare the files with `scripts/split_sample.py` and use a single `part-*.pgn` glob Job with a fresh v0.3 schema; leave the single-file expected SHA blank, check the per-file bound and independently match all ten SQL `ingestion_runs` hashes/counts with the manifest. See [operations notes](DEV_HANDOFF.md#databricks-run-instructions). Local managed Delta fixture tests verified multi-file ingestion, replay, and the legacy-schema guard. The prior 10,000-game cloud schema is untouched; a v0.3 100,000-game cloud run remains to be verified.

## Local verification

- Fresh editable v0.3.0 install and base Python suite: 34 passed (optional Spark module skipped), `pip check` passed; full local Spark/Delta suite: 44 passed in 96.63 s with the separate 100-game validation fixture. Full 100,000-game Spark transformation parity tested independently as described above.
- `ruff check .`, `ruff format --check src tests scripts`, Python compilation, JavaScript syntax, `actionlint` and all 28 local Markdown links passed.
- Playwright compared actual table labels, counts and rates for Italian/Ruy Lopez/Black defenses, ECO search, rating and speed slices, minimum counts, empty/reset states, all outcome filters and all six viewport widths from 320 to 1,440 px. No console errors. Real desktop and mobile screenshots were inspected.
- [Dashboard screenshot](docs/assets/dashboard-preview.png) and [filtered opening table](docs/assets/opening-study.png) are captured from the actual v0.3 UI.

To run the browser check, use Node.js, Playwright and local Chrome:

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

### Developer smoke review

| Angle | Assessment | Evidence and remaining limits |
| --- | ---: | --- |
| Product/CTO | 8/10 | Focused bounded analysis, CC0 attribution, private raw data, repeatable source, chunk strategy; new cloud resource behavior is unverified. |
| Tech lead | 9/10 | Shared contracts, exact count/rate parity, version guards, clean lint, repeatable tests; no hosted CI result for this revision yet. |
| UX/UI | 8.5/10 | Clear color perspective, exact context filters, explicit results/denominators, mobile table and empty states; no formal screen-reader audit. |
| QA | 9/10 | Actual sample, local Delta/glob fixture, tie/unknown/receipt regressions, exact browser checks, stable export; independent release QA and live retest remain pending. |

These are developer smoke-review ratings, not independent release sign-off. No push or cloud execution was performed.
