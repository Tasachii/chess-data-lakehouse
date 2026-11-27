# Measured run evidence

Local measurements were recorded on 27 November 2026 with Python 3.12. Databricks SQL results are listed separately below. The publisher is the [Lichess Open Database](https://database.lichess.org/), and the source is the [October 2026 rated standard archive](https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst), licensed CC0 1.0. Only bounded prefixes were downloaded; the full archive was not processed.

## Current v0.3 opening analytics (100,000 games)

Version 0.3 rebuilds the same verified October 2026 prefix in a fresh local output directory. The original file SHA-256 and receipt are unchanged. The first run inserted 100,000, and replay inserted zero. Bronze / Silver / quarantine = 100,000 / 99,875 / 125. The final local run took 12.87 s; replay took 13.87 s. Both exports matched byte-for-byte. Current [aggregate snapshot](assets/snapshot.json) SHA-256: `71c078b99c9ec5f2177166704fd5c8a99512cca8082d190c9b2bbbcca94bb239`. No player-level rows are exported.

### PGN field audit

| Field or annotation | Nonempty or known, raw (100,000) | Nonempty or known, accepted (99,875) | Use |
| --- | ---: | ---: | --- |
| Opening, WhiteElo, BlackElo, TimeControl, UTCDate/UTCTime | 100,000 each | 99,875 each | Validated group dimensions and pre-game rating gap |
| ECO tag | 100,000 nonempty | 99,633 valid standard codes | 242 accepted records are `Unknown` |
| Termination | 100,000 | 99,875 recognized | Grouped exactly as reported; unknown category supported |
| WhiteRatingDiff / BlackRatingDiff | 99,642 each | 99,562 each | Post-game rating changes, not the pre-game Elo gap; not exported |
| Mainline ply count | — | 99,633 known | 242 result-only games have unknown length |
| Games with clock annotations | 99,633 | 99,633 | Coverage audited; no move-time metrics |
| Games with eval annotations | 10,455 | 10,441 | Sparse and partially annotated; no eval metrics |

The lexical mainline parser found 6,638,521 half-moves in 99,633 accepted games, from 1 to 548 plies; the mean is 66.6 plies. Braced comments are removed, and only records with SAN-shaped tokens and a matching final result are counted. Variations or unsupported tokens return null. This is not a chess legality check. The source has clock tags for all of those known plies, but a clock reading is not elapsed move time; the sample does not justify a move-time or engine-quality metric. The other 242 accepted games remain in outcome statistics and the Unknown length bucket.

| Termination | Games |
| --- | ---: |
| Normal | 66,293 |
| Time forfeit | 33,188 |
| Abandoned | 373 |
| Insufficient material | 20 |
| Rules infraction | 1 |

### New marts and parity

| Gold mart | Group rows |
| --- | ---: |
| Opening, average rating and speed | 2,018 / 15 / 4 |
| Opening context (opening × ECO × rating × speed) | 18,342 |
| Signed rating gap | 7 |
| Termination | 5 |
| Mainline length | 8 |

All seven marts reconcile to 99,875 games / 49,550 White wins / 46,550 Black wins / 3,775 draws. The exporter validates each group against Silver, including total plies and known length counts. The 18,342 context rows also roll up exactly to the original three marginal groupings. A separate local Spark run read the actual ten files via a glob and matched every one of the 20,399 Gold rows, including count fields, length totals and rounded rates (33.42 s wall clock). The Spark check was transformation parity, not a 100,000-game managed Delta write or cloud run.

The original binary-float Python rounding gave 0.4813 for 77/160; Spark half-even gave 0.4812. Version 0.3 aligns stored rates with Decimal half-even at four decimals and includes an exact tie regression. All counts are unchanged and UI rates are derived from integer counts, not rounded rates.

The [chunk manifest](assets/sample-chunks-manifest.json) was generated from the original source: ten files of exactly 10,000 games, 23,193,716–23,614,481 bytes each, with matching per-file receipts. Recombining them with a separating newline gives the original source hash. A local managed Delta fixture verified the multi-file glob, per-file hashes, replay and fresh-schema guard. Both subsequent Databricks runs used these same ten files; the SQL checks are recorded below.

## Previous 100,000-game v0.2 dashboard sample

The sample was downloaded and processed locally using the same bounded pipeline. This is a prefix of the October 2026 archive, not a random sample of all games.

| Measurement | Value |
| --- | --- |
| Requested and retrieved | 100,000 records |
| Decompressed data size | 234,164,347 bytes |
| Retrieval (UTC) | 2026-11-27 15:19:51 |
| Validated game time range (UTC) | 2026-10-01 00:00:00–01:22:41 |
| Source SHA-256 | `96e4ef9dd84f2e646f1604da2e47b4f686a2725186a3d026534ca9e742a3b031` |
| Download / first run / replay | 56.79 s / 5.35 s / 6.79 s wall clock |
| Bronze / Silver / quarantine | 100,000 / 99,875 / 125 |
| First insert / replay insert | 100,000 / 0 |
| Gold opening / rating / speed groups | 2,018 / 15 / 4 |
| White wins / black wins / draws | 49,550 / 46,550 / 3,775 |

All 125 quarantined records have an invalid time control; 25 also have an invalid result. An independent streaming parse of the raw PGN confirmed 100,000 distinct canonical game IDs, the exact valid/quarantined ID sets, timestamp range, outcomes and all 2,037 Gold group rows. Each dimension totals 99,875 validated games. The original 10,000-game decompressed input is a byte-identical prefix of the expanded sample. The first and replay exports matched byte for byte, with SHA-256 `690ce5d3c295703d92082b33ce540c09dbbb8a81a5e3cca2434c45bfe64176e1`. That v0.2 snapshot is preserved in Git history and ignored local evidence. The current v0.3 dashboard uses the same source bytes, with additional fields and aggregate marts.

Opening search shortcuts use literal name matches in the real Lichess opening labels, including variations. Results are not causal estimates of opening strength.

| Search label | Matching opening labels | Games | White wins | Black wins | Draws |
| --- | ---: | ---: | ---: | ---: | ---: |
| Italian Game | 93 | 3,883 | 1,939 | 1,806 | 138 |
| Ruy Lopez | 100 | 1,722 | 838 | 794 | 90 |
| Queen's Gambit | 112 | 3,421 | 1,730 | 1,570 | 121 |
| London System | 12 | 3,820 | 1,944 | 1,709 | 167 |
| English Opening | 89 | 3,048 | 1,561 | 1,354 | 133 |
| Scotch Game | 35 | 1,888 | 955 | 870 | 63 |

The result selector filters these matches by White wins, Black wins or draws while retaining all games in each opening as the win-rate denominator. Playwright passed exact shortcut, result-count, rate-denominator and reset checks, plus all six responsive widths. The base suite passed 22 tests with one optional Spark module skipped; the full local suite (with the separate 100-game Spark validation input) passed 30 tests in 42.05 s. This v0.2 expanded sample was tested locally; the subsequent v0.3 cloud verification is recorded below.

## Original 10,000-game dashboard sample

| Measurement | Value |
| --- | --- |
| Requested and retrieved | 10,000 records |
| Decompressed data size | 23,614,481 bytes |
| Retrieval (UTC) | 2026-11-27 14:06:34 |
| Validated game time range (UTC) | 2026-10-01 00:00:00–00:08:08 |
| Source SHA-256 | `24a50c38d31691401b5ef3b5fa246c4941e56d4223c54d397d0ef6c9b13c3030` |
| Download / local first run / local replay | 10.97 s / 0.57 s / 0.68 s wall clock |
| Bronze / Silver / quarantine | 10,000 / 9,996 / 4 |
| First insert / replay insert | 10,000 / 0 |

The four quarantined records have invalid time-control fields. The three Gold dimensions each sum to 9,996 valid games, with 5,026 white wins, 4,571 black wins and 399 draws. The exporter reconciled every group and white-win-rate value with Silver, and a second export after replay was byte-identical.

| Gold dimension | Groups |
| --- | ---: |
| Opening | 1,107 |
| Average rating bucket | 14 |
| Speed | 4 |

The original snapshot (SHA-256 `b299b5a7b654f0be438787cdb76f04ae7be9f78a825e1080a4b02ed76a6a8165`) is retained in Git history and in ignored local evidence. It is the sample used for the historical v0.2 Databricks runs, not the current dashboard dataset.

## Pipeline validation sample (100 games)

The local pipeline and PySpark/Delta implementation were validated on a smaller prefix before the dashboard was built. The download wrote 100 records and 234,489 bytes; repeated downloads matched SHA-256 `d2675dd2381bcf736422367ea8800474939a067f05e7a949aa3b52fbd41b60f6`. The first corrected local run inserted 100 games; replay inserted zero. All 100 games were valid and had canonical eight-character Lichess IDs.

| Speed | Games | White wins | Draws |
| --- | ---: | ---: | ---: |
| Blitz | 56 | 31 | 3 |
| Bullet | 27 | 11 | 2 |
| Rapid | 17 | 10 | 1 |

There were 82 opening groups, 10 rating buckets and three speeds. Local Spark matched all 100 Silver records and all 95 Gold groups. The managed Delta test created and replayed tables for this real sample and the synthetic fixture, with no duplicate keys. With PySpark 3.5.3, Delta 3.3.2 and Java 21, the corrected full suite passed 27 tests in 47.22 s. The 100-game sample is not the dashboard sample.

Independent testing confirmed the local pipeline at commit `94f58a11304cc11c92e102f3894aabcbdcc80ce9`. The regression cases covered alternate game URLs, incomplete PGN boundaries, truncated Zstandard input, missing fields, overlap and empty delivery, and run lineage. Original independent reproduction scripts passed eight cases; three deliberately truncated files were rejected without publication. The local fixture and real replays preserved all aggregate results.

## Previous 10,000-game dashboard release verification

The local base suite, including two aggregate-export tests, passed 21 tests with the optional Spark module skipped. The full local Spark/Delta suite passed 29 tests in 45.18 s (45.53 s wall clock) on the final layout revision, using the original 100-game validation sample. Browser checks exercised the 10,000-game dashboard at 320, 360, 390, 560, 768 and 1,440 pixel widths. Opening search, filters, sorting and rating selection passed with no JavaScript errors. A 320 px outcome-caption overlap was reproduced and fixed: the measured horizontal gap between white and black captions improved from −1.23 px to 29.41 px. All six widths now pass the caption containment and overlap checks. Pipeline behavior and Databricks configuration are unchanged.

## GitHub Pages and CI verification

The previous 10,000-game GitHub Pages release built commit `b6387d4`. The [live dashboard](https://tasachii.github.io/chess-data-lakehouse/) returned HTTP 200 and passed independent live checks, including byte-for-byte comparison of deployed assets with the committed files. Both the Python/dashboard and Spark/Delta fixture jobs completed successfully in [Chess Lakehouse CI run 46021313046](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046). The hosted jobs did not require cloud credentials or real archive downloads. The 100,000-game dashboard has been verified locally; its new GitHub Pages deployment and hosted CI are still awaiting verification.

## Databricks cloud verification

### Current v0.3 (100,000 games)

Official Databricks CLI bundle validation and deployment passed. Two actual serverless job runs completed successfully in a fresh v0.3 schema. Each run read the same ten 10,000-game files through a single glob. Direct SQL queries after the second run confirmed:

| Check | Observed |
| --- | ---: |
| Bronze rows / distinct IDs | 100,000 / 100,000 |
| Silver rows | 99,875 |
| Quarantine rows | 125 |
| Managed Delta tables | 11 |
| Sum of `games` in each of the seven Gold marts | 99,875 |
| Total Gold group rows | 20,399 |
| File SHA-256 and attempted counts matching the [chunk manifest](assets/sample-chunks-manifest.json) | 10 of 10 in each run |
| `ingestion_runs` rows | 20 (10 per run) |
| Replay `inserted` (sum) | 0 |
| Replay `duplicates` (sum) | 100,000 |

The replay added no new game records. These SQL checks confirm the bounded cloud run and its replay; the dashboard file itself was exported locally.

### Historical v0.2 (10,000 games)

Official Databricks CLI bundle validation passed. The deployed serverless job ran twice successfully on the same bounded 10,000-game October 2026 sample and created seven managed Delta tables. SQL queries after the second execution verified:

| Check | Observed |
| --- | ---: |
| Bronze rows / distinct IDs | 10,000 / 10,000 |
| Silver rows | 9,996 |
| Quarantine rows | 4 |
| Sum of `games` in each of the three Gold tables | 9,996 |
| `ingestion_runs` rows | 2 |
| Last run `inserted` | 0 |
| Last run `duplicates` | 10,000 |

This earlier result remains separate from the v0.3 cloud verification above. Both releases confirmed replay without duplicate game records.

## Limitations

Runtime depends on the machine and network; these measurements are not a scale benchmark. The archive prefix is not a random or representative sample. The dashboard is a static snapshot, and the serverless job was verified only for the bounded samples described above, not continuous ingestion or the full archive.
