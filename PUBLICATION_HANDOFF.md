# Publication notes

The dashboard is ready to serve from `main:/docs`. No repository remote was created, no push or GitHub Pages deployment was performed, and no cloud or billing settings were changed. The approved pipeline code and Databricks configuration are unchanged.

## Changes

- `docs/index.html`, `docs/assets/`: responsive, static analytics dashboard with opening filters, outcome and speed charts, selectable rating buckets, data quality, source and pipeline summary. Relative asset paths and `.nojekyll` support a GitHub Pages project subdirectory.
- `scripts/export_dashboard.py`: exports aggregate-only JSON from verified Gold CSVs and checks every group against Silver and the input receipt. No actual player names, game IDs or raw PGN are exported.
- `scripts/test_dashboard.cjs`, `tests/test_dashboard_export.py`: browser interaction and aggregate export checks.
- All existing Markdown notes were rewritten in plain English. The README links to the intended Pages URL, dashboard source, screenshot and reproduction commands.

Actual sample: 10,000 Lichess archive records (October 2026), 9,996 accepted, four quarantined, zero inserted on replay. Source time range for valid games: 1 October 2026, 00:00:00–00:08:08 UTC. No full archive claim. [Run evidence](docs/run-evidence.md) gives the counts, checksums and timings.

## Local checks

| Check | Result |
| --- | --- |
| `.venv/bin/python -m pytest -ra` | 21 passed, optional Spark module skipped |
| Full local Spark/Delta suite, using the documented host/JAR setup and 100-game validation sample | 29 passed in 47.33 s |
| `ruff check .`, `ruff format --check src tests scripts`, `node --check` for both JS files | Passed |
| Export after the 10,000-game replay | Byte-identical SHA-256 `b299b5a7b654f0be438787cdb76f04ae7be9f78a825e1080a4b02ed76a6a8165` |
| Playwright 1.65.1 with local Chrome | Passed snapshot totals, opening search/minimum/sort/limit/reset, rating metric and bucket selection, 1,440/390/320 px widths, and no console errors |
| Pipeline comparison against commit `765bd91` | No differences in `src`, `pyproject.toml`, `databricks.yml` or `resources/chess_lakehouse.job.yml` |

To reproduce the browser check (requires Node.js and installed Google Chrome):

```bash
npm --prefix output/dashboard-browser install --no-save playwright@1.65.1
mkdir -p output/dashboard-preview-server
ln -sfn "$PWD/docs" output/dashboard-preview-server/chess-data-lakehouse
.venv/bin/python -m http.server 8765 --bind 127.0.0.1 --directory output/dashboard-preview-server
```

While the server runs, in a second terminal at the project root:

```bash
NODE_PATH="$PWD/output/dashboard-browser/node_modules" \
DASHBOARD_BROWSER_CHANNEL=chrome \
DASHBOARD_URL=http://127.0.0.1:8765/chess-data-lakehouse/ \
node scripts/test_dashboard.cjs
```

## Deployment

In the repository's GitHub Pages settings, select the `main` branch and `/docs` folder. The intended URL is [https://tasachii.github.io/chess-data-lakehouse/](https://tasachii.github.io/chess-data-lakehouse/). After publication, confirm the HTML, CSS, JavaScript and snapshot JSON load at that URL. The live site was not tested during local development. Databricks runtime and bundle CLI execution also remain unverified.
