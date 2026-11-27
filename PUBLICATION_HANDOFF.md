# Publication notes

The 100,000-game dashboard update is ready for publication from `main:/docs`. It has passed local verification. The [previous 10,000-game release](https://tasachii.github.io/chess-data-lakehouse/) remains the verified live site. The dashboard publishes aggregate statistics only, with no real player names, game IDs, raw PGN or private workspace details.

## 100,000-game update

Downloaded and processed the first 100,000 real records from the October 2026 Lichess archive. The receipt and source SHA-256 were verified; 99,875 records were valid and 125 were quarantined. The replay inserted zero records, and both aggregate exports were byte-identical. An independent streaming raw-PGN check matched all 2,037 Gold groups and the valid/quarantined game ID sets. The previous 10,000-game snapshot and source evidence are retained locally and in Git history.

Added search shortcuts for actual opening labels, including Italian Game (3,883 games across 93 labels) and Ruy Lopez (1,722 games across 100 labels), plus a result selector for White wins, Black wins and draws. Rates retain all games in the opening as their denominator. These are game-level opening labels and descriptive outcomes, not proof one opening is stronger. Exact counts, checksums and times are in [run evidence](docs/run-evidence.md#current-100000-game-dashboard-sample).

## Responsive layout verification

In the previous 10,000-game release, an independent browser check found the white and black outcome captions overlapping at 320 px. Text-range measurements reproduced the same result before the fix. The narrow-screen grid now allows caption wrapping rather than forcing text past the column boundary.

| Viewport | Before: white-to-black gap | After | Result |
| --- | ---: | ---: | --- |
| 320 px | −1.23 px | 29.41 px | Pass |
| 360 px | 12.09 px | 42.73 px | Pass |
| 390 px | 22.09 px | 22.09 px | Pass |
| 560 px | 78.77 px | 78.77 px | Pass |
| 768 px | 126.11 px | 126.11 px | Pass |
| 1,440 px | 276.78 px | 276.78 px | Pass |

The regression checks every outcome caption's text bounds against its own column and adjacent captions. It still passes at all six widths on the larger 100,000-game snapshot, alongside the new opening-shortcut and result-filter tests.

To reproduce (requires Node.js and installed Google Chrome):

```bash
npm --prefix output/dashboard-browser install --no-save playwright@1.65.1
mkdir -p output/dashboard-preview-server
ln -sfn "$PWD/docs" output/dashboard-preview-server/chess-data-lakehouse
.venv/bin/python -m http.server 18766 --bind 127.0.0.1 --directory output/dashboard-preview-server
```

In a second terminal at the project root:

```bash
NODE_PATH="$PWD/output/dashboard-browser/node_modules" \
DASHBOARD_BROWSER_CHANNEL=chrome \
DASHBOARD_URL=http://127.0.0.1:18766/chess-data-lakehouse/ \
node scripts/test_dashboard.cjs
```

## Databricks verification

Official Databricks CLI bundle validation passed. The serverless job ran successfully twice on the same 10,000-game October 2026 sample. Direct SQL checks confirmed seven managed Delta tables, 10,000 Bronze rows and distinct IDs, 9,996 Silver rows, four quarantined, 9,996 games summed in each of three Gold tables, two run-history rows and zero inserted / 10,000 duplicates on the last run. These cloud results are for the original 10,000-game sample only. The 100,000-game dashboard was processed locally. Continuous ingestion and the full archive were not tested.

[Run evidence](docs/run-evidence.md) records source checksums and measurements. [Operations notes](DEV_HANDOFF.md#databricks-run-instructions) include the example deployment and SQL checks.

## Local checks

- `.venv/bin/python -m pytest -ra`: 22 passed, one optional Spark module skipped.
- Full local PySpark/Delta suite with the 100-game validation sample and explicit host/JAR setup: 30 passed in 42.05 s (42.33 s wall clock). See [run evidence](docs/run-evidence.md).
- Playwright browser checks on the 100,000-game snapshot: passed all six widths, actual Italian/Ruy Lopez matches, White/Black/draw result counts, unfiltered rate denominators and reset.
- Python lint and formatting, Python compilation, JavaScript syntax and local Markdown links: passed.
- Current snapshot SHA-256 is `690ce5d3c295703d92082b33ce540c09dbbb8a81a5e3cca2434c45bfe64176e1`. The prior snapshot `b299b5a7b654f0be438787cdb76f04ae7be9f78a825e1080a4b02ed76a6a8165` is preserved locally.

## Continuous integration

The [Chess Lakehouse CI workflow](.github/workflows/ci.yml) uses standard GitHub-hosted Ubuntu runners with read-only repository access. Pushes, pull requests and manual dispatch run the base Python and JavaScript checks plus a separate local Spark/Delta fixture job. Both jobs completed successfully for the previous release in [the hosted CI run](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046). No cloud credentials or real archive downloads are required by CI. The workflow also passed `actionlint`. A fresh local Python install ran 21 base tests (one optional Spark module skipped), Ruff checks and both JavaScript syntax checks. The local Spark/Delta fixture run passed six tests (two real-data tests skipped). Real-data parity is tested locally; browser interactions were also verified on the live site.

## GitHub Pages deployment

The prior 10,000-game GitHub Pages release built commit `b6387d4` from `main:/docs`. The [live dashboard](https://tasachii.github.io/chess-data-lakehouse/) returned HTTP 200, and independent live checks confirmed that the deployed assets matched the committed files byte-for-byte. That release passed its full live checks. The 100,000-game revision has not been pushed or deployed; its local checks are documented here. It remains a static archive sample, not a live ingestion service.
