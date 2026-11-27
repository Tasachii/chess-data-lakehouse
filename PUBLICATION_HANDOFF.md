# Publication notes

The static dashboard is ready for GitHub Pages from `main:/docs`. The outcome captions now wrap within their columns on narrow screens. No dashboard data, pipeline logic or Databricks configuration changed in this revision. Published material contains aggregate statistics only, with no real player names, game IDs, raw PGN or private workspace details.

## Responsive layout verification

An independent browser check found the white and black outcome captions overlapping at 320 px. Text-range measurements reproduced the same result before the fix. The narrow-screen grid now allows caption wrapping rather than forcing text past the column boundary.

| Viewport | Before: white-to-black gap | After | Result |
| --- | ---: | ---: | --- |
| 320 px | −1.23 px | 29.41 px | Pass |
| 360 px | 12.09 px | 42.73 px | Pass |
| 390 px | 22.09 px | 22.09 px | Pass |
| 560 px | 78.77 px | 78.77 px | Pass |
| 768 px | 126.11 px | 126.11 px | Pass |
| 1,440 px | 276.78 px | 276.78 px | Pass |

The new browser regression failed on the old layout and passed after the fix. It checks every outcome caption's text bounds against its own column and adjacent captions, as well as the existing interaction and page-width checks.

To reproduce (requires Node.js and installed Google Chrome):

```bash
npm --prefix output/dashboard-browser install --no-save playwright@1.65.1
mkdir -p output/dashboard-preview-server
ln -sfn "$PWD/docs" output/dashboard-preview-server/chess-data-lakehouse
.venv/bin/python -m http.server 18765 --bind 127.0.0.1 --directory output/dashboard-preview-server
```

In a second terminal at the project root:

```bash
NODE_PATH="$PWD/output/dashboard-browser/node_modules" \
DASHBOARD_BROWSER_CHANNEL=chrome \
DASHBOARD_URL=http://127.0.0.1:18765/chess-data-lakehouse/ \
node scripts/test_dashboard.cjs
```

## Databricks verification

Official Databricks CLI bundle validation passed. The serverless job ran successfully twice on the same 10,000-game October 2026 sample. Direct SQL checks confirmed seven managed Delta tables, 10,000 Bronze rows and distinct IDs, 9,996 Silver rows, four quarantined, 9,996 games summed in each of three Gold tables, two run-history rows and zero inserted / 10,000 duplicates on the last run. The dashboard remains a static sample; continuous ingestion and the full archive were not tested.

[Run evidence](docs/run-evidence.md) records source checksums and measurements. [Operations notes](DEV_HANDOFF.md#databricks-run-instructions) include the example deployment and SQL checks.

## Local checks

- `.venv/bin/python -m pytest -ra`: 21 passed, one optional Spark module skipped.
- Full local PySpark/Delta suite with the 100-game validation sample and explicit host/JAR setup: 29 passed in 45.18 s (45.53 s wall clock). See [run evidence](docs/run-evidence.md).
- Playwright browser interaction and outcome-caption checks: passed at all six widths.
- Python lint and formatting, Python compilation, JavaScript syntax and local Markdown links: passed.
- Snapshot SHA-256 remains `b299b5a7b654f0be438787cdb76f04ae7be9f78a825e1080a4b02ed76a6a8165`.

## GitHub Pages deployment

Select the `main` branch and `/docs` folder in GitHub Pages settings. The intended URL is [https://tasachii.github.io/chess-data-lakehouse/](https://tasachii.github.io/chess-data-lakehouse/). The final live URL must be checked after publication. This revision was committed locally without pushing or changing cloud configuration.
