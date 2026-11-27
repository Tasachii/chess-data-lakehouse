# Measured run evidence

Local measurements were recorded on 27 November 2026 with Python 3.12. Databricks SQL results are listed separately below. The publisher is the [Lichess Open Database](https://database.lichess.org/), and the source is the [October 2026 rated standard archive](https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst), licensed CC0 1.0. Only bounded prefixes were downloaded; the full archive was not processed.

## Current 100,000-game dashboard sample

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

All 125 quarantined records have an invalid time control; 25 also have an invalid result. An independent streaming parse of the raw PGN confirmed 100,000 distinct canonical game IDs, the exact valid/quarantined ID sets, timestamp range, outcomes and all 2,037 Gold group rows. Each dimension totals 99,875 validated games. The original 10,000-game decompressed input is a byte-identical prefix of the expanded sample. The first and replay exports matched byte for byte, with SHA-256 `690ce5d3c295703d92082b33ce540c09dbbb8a81a5e3cca2434c45bfe64176e1`. The [current snapshot](assets/snapshot.json) contains aggregates only. Reproduce it with the [README commands](../README.md#reproduce-the-snapshot).

Opening search shortcuts use literal name matches in the real Lichess opening labels, including variations. Results are not causal estimates of opening strength.

| Search label | Matching opening labels | Games | White wins | Black wins | Draws |
| --- | ---: | ---: | ---: | ---: | ---: |
| Italian Game | 93 | 3,883 | 1,939 | 1,806 | 138 |
| Ruy Lopez | 100 | 1,722 | 838 | 794 | 90 |
| Queen's Gambit | 112 | 3,421 | 1,730 | 1,570 | 121 |
| London System | 12 | 3,820 | 1,944 | 1,709 | 167 |
| English Opening | 89 | 3,048 | 1,561 | 1,354 | 133 |
| Scotch Game | 35 | 1,888 | 955 | 870 | 63 |

The result selector filters these matches by White wins, Black wins or draws while retaining all games in each opening as the win-rate denominator. Playwright passed exact shortcut, result-count, rate-denominator and reset checks, plus all six responsive widths. The base suite passed 22 tests with one optional Spark module skipped; the full local suite (with the separate 100-game Spark validation input) passed 30 tests in 42.05 s. The cloud job has only been verified on the smaller 10,000-game sample.

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

The original snapshot (SHA-256 `b299b5a7b654f0be438787cdb76f04ae7be9f78a825e1080a4b02ed76a6a8165`) is retained in Git history and in ignored local evidence. It is the sample used for the verified Databricks runs, not the current dashboard dataset.

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

The previous 10,000-game GitHub Pages release built commit `b6387d4`. The [live dashboard](https://tasachii.github.io/chess-data-lakehouse/) returned HTTP 200 and passed independent live checks, including byte-for-byte comparison of deployed assets with the committed files. Both the Python/dashboard and Spark/Delta fixture jobs completed successfully in [Chess Lakehouse CI run 46021313046](https://github.com/Tasachii/chess-data-lakehouse/actions/runs/46021313046). The hosted jobs did not require cloud credentials or real archive downloads. The 100,000-game update is verified locally and has not yet been pushed or deployed.

## Databricks cloud verification

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

The cloud result confirms that the same input can be replayed without duplicate records. It does not demonstrate continuous ingestion or processing of the full archive.

## Limitations

Runtime depends on the machine and network; these measurements are not a scale benchmark. The archive prefix is not a random or representative sample. The dashboard is a static snapshot, and the serverless job was verified only for the bounded sample described above.
