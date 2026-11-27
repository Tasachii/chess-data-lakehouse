# Run evidence

The following first-run evidence is historical v0.1.0 evidence. QA subsequently found four integrity/lineage defects; see the v0.2.0 remediation below and [DEV_HANDOFF.md](../DEV_HANDOFF.md) for current verification.

Captured on 2026-11-27 (Asia/Bangkok host, Python 3.12). Commands were run from the project root.

## Real bounded source

Source: `https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst`

```text
$ /usr/bin/time -p .venv/bin/chess-lakehouse download ... --max-games 100
games_written: 100
bytes_written: 234489
real: 2.16 s

$ /usr/bin/time -p .venv/bin/chess-lakehouse run ...
input_games: 100
inserted_bronze: 100
valid_silver: 100
quarantined: 0
real: 0.06 s

$ /usr/bin/time -p .venv/bin/chess-lakehouse run ...  # identical replay
input_games: 100
inserted_bronze: 0
valid_silver: 100
quarantined: 0
real: 0.06 s
```

Decompressed sample SHA-256:
`d2675dd2381bcf736422367ea8800474939a067f05e7a949aa3b52fbd41b60f6`.

Observed speed aggregate after the run:

| speed | games | white wins | draws |
|---|---:|---:|---:|
| blitz | 56 | 31 | 3 |
| bullet | 27 | 11 | 2 |
| rapid | 17 | 10 | 1 |

These are sample observations only. Network and runtime measurements will vary.

## Cloud status

`src/databricks_job.py` and the Asset Bundle configuration were checked locally. They were not run
against Databricks because no existing authorized workspace session was used. Therefore this
repository makes no claim about a successful cloud run or cloud performance.

## Dev1 verification (same date, fresh independent runs)

A second bounded download wrote 100 games / 234,489 bytes in 2.77 s and reproduced the exact
SHA-256 above. A fresh local output (`output/dev1-real`) inserted 100 games initially and zero on
replay (0.06 s each). The synthetic fixture inserted 2 distinct games from 3 records, 1 valid and
1 quarantined, then inserted zero on replay (0.05 s each). A new isolated environment installed
`.[dev]` successfully; 9 tests passed, with only the optional Spark module skipped. The six
data-layer outputs from that environment and the fresh local run were byte-identical.

Real Gold contains 82 opening groups, 10 rating groups, 3 speed groups. Each dimension totals
100 games, 52 white wins, 6 draws. All records and group metrics matched the local Spark path.
With PySpark 3.5.3, Delta Lake 3.3.2 and Java 21.0.2, the full optional suite passed **15 tests in
30.33 s** (30.66 s wall clock), including locally managed Delta tables created and replayed twice:
real Bronze/Silver/quarantine remained 100/100/0; synthetic remained 2/1/1. Distinct key counts
and persisted Gold counts were verified. No Databricks run or CLI validation was performed.
See [DEV_HANDOFF.md](../DEV_HANDOFF.md) for exact commands and limitations.

## v0.2.0 — TEST1-01–04 remediation (2026-11-27)

New bounded public download: 100 games / 234,489 bytes / 1.89 s wall clock, identical SHA-256 to the earlier sample. Real Bronze now uses 100 canonical eight-character Lichess IDs and zero content fallbacks. The exact Test1 metadata-alternate first game replays to one Bronze / one Silver / one draw in Gold. The exact header-only input attempts two records, quarantines one and produces `Unknown` opening for the valid sibling. All three 1/10/30-byte compressed truncations exit 1 without publishing new output.

Fresh developer-run copy of both unmodified QA scripts: eight independent cases passed, no failures, three truncated inputs rejected, one logical game retained. Clean base environment: 19 passed and only the optional module skipped. Full PySpark/Delta suite: 27 passed in 47.22 s (47.61 s wall clock), including real and fixture managed Delta replay, logical ID replay, empty receipt and per-run lineage. Real ordinary Gold group counts remain 82/10/3, 100 games in every dimension. Source receipt and durable run-history counts reconcile. No cloud run or CLI validation performed. Exact commands and all batch counts: [DEV_HANDOFF.md](../DEV_HANDOFF.md). QA retest remains pending.
