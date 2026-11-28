# Measured run evidence

Local measurements were recorded on 27 November 2026 with Python 3.12. Databricks SQL results are listed separately below. The publisher is the [Lichess Open Database](https://database.lichess.org/), and the source is the [October 2026 rated standard archive](https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst), licensed CC0 1.0. Only bounded prefixes were downloaded; the full archive was not processed.

## Current local Spark scale check (1,000,000 games)

The first 1,000,000 records of the October 2026 rated standard archive were downloaded on 27 November at 16:25:22 UTC. The bounded decompressed PGN contains 2,321,066,081 bytes with SHA-256 `6050321afa4d758518168b63f58b5845f2dec4947a1ce92c6d6b25b6c70905a0`. The streaming downloader took 221.06 s; process maximum RSS was 93,028,352 bytes. Splitting into 100 files of exactly 10,000 records took 14.92 s; each file contains 22,595,909–23,614,481 bytes. The [chunk manifest](assets/sample-chunks-1m-manifest.json) verifies the exact reconstructed parent hash and all file receipts. The first ten entries match the earlier 100,000-game cloud sample byte-for-byte.

The scale runner uses PySpark 3.5.3 / Java 21 on a 16 GiB machine with `local[2]`, a 4 GiB driver heap and disk-backed temporary storage. It checks every selected file’s SHA-256 and attempted record count in Spark, deduplicates by canonical ID, and independently streams the PGN with Python to verify every Gold group and exact half-even rate. The full sample uses one glob; the 250,000-game checkpoint uses the first 25 files. Minimum sampled free disk space was about 28 GiB; no out-of-memory failure occurred.

| Checkpoint | Files | Unique Bronze | Silver | Quarantine | Gold groups | Spark + receipts (s) | Total with independent check (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 10 | 100,000 | 99,875 | 125 | 20,399 | 30.02 | 41.16 |
| 250,000 | 25 | 250,000 | 249,745 | 255 | 28,955 | 61.45 | 88.95 |
| 1,000,000 | 100 | 1,000,000 | 998,976 | 1,024 | 45,332 | 192.74 | 300.13 |

The final 100,000-game checkpoint used the original receipt and reproduced the previous dashboard SHA-256 exactly. A second independent streamed verification and export of the one-million-game Gold results took 117.37 s and produced an identical snapshot; the Spark transformation itself was not rerun at one million games. The [machine-readable run report](assets/scale-run-report.json) includes all checkpoint source hashes, outcomes, field coverage and timings. The one-million-game snapshot is 3,783,288 bytes (338,923 bytes when locally gzipped), with SHA-256 `0bc49de7258a0424657a469074be3963c207f7ef5f7d1b110d8b764177988c5a`.

| Gold mart | Group rows |
| --- | ---: |
| Opening | 2,675 |
| Average rating | 15 |
| Speed | 4 |
| Opening context | 42,618 |
| Signed rating gap | 7 |
| Termination | 5 |
| Mainline length | 8 |

Each mart sums to 998,976 validated games: 497,294 White wins, 463,906 Black wins and 37,776 draws. There are 997,485 known ECO codes and mainline lengths, with 65,810,996 total known half-moves. The accepted game time range is 00:00:00–10:45:58 UTC on October 1. The extract is a prefix, not a representative sample of the full month. No individual game or player records are included in the [dashboard snapshot](assets/snapshot.json).

The base Python suite passed 40 tests with one optional Spark module skipped. The full local Spark/Delta suite passed 51 tests in 78.94 s, including the separate 100-game real validation sample and the multi-file scale fixture. Ruff lint and formatting, JavaScript syntax, and the browser test all passed. The browser checked the actual one-million-game snapshot at 320, 360, 390, 560, 768 and 1,440 px, including both color perspectives, opening names and ECO, rating and speed slices, minimum counts, exact outcomes, reset and empty states.

These measurements cover the local Spark transformation and export, not managed Delta writes. The separate one-million-game Databricks incremental run and direct SQL verification are recorded below.

## Historical v0.3 opening analytics (100,000 games)

Version 0.3 rebuilds the same verified October 2026 prefix in a fresh local output directory. The original file SHA-256 and receipt are unchanged. The first run inserted 100,000, and replay inserted zero. Bronze / Silver / quarantine = 100,000 / 99,875 / 125. The final local run took 12.87 s; replay took 13.87 s. Both exports matched byte-for-byte. The historical aggregate snapshot (preserved in Git at `4e2c3e2`) has SHA-256: `71c078b99c9ec5f2177166704fd5c8a99512cca8082d190c9b2bbbcca94bb239`. No player-level rows are exported.

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

The previous 10,000-game GitHub Pages release built commit `b6387d4`. The [live dashboard](https://tasachii.github.io/Chess-Data-Lakehouse/) returned HTTP 200 and passed independent live checks, including byte-for-byte comparison of deployed assets with the committed files. Both the Python/dashboard and Spark/Delta fixture jobs completed successfully in [Chess Lakehouse CI run 46021313046](https://github.com/Tasachii/Chess-Data-Lakehouse/actions/runs/46021313046). The hosted jobs did not require cloud credentials or real archive downloads. The one-million-game dashboard was published at commit `7f45e0e`. The [Pages build](https://github.com/Tasachii/Chess-Data-Lakehouse/actions/runs/46038266738) and both jobs in [Chess Lakehouse CI run 46038266966](https://github.com/Tasachii/Chess-Data-Lakehouse/actions/runs/46038266966) succeeded. The served aggregate snapshot matched the local committed bytes.

## Databricks cloud verification

### Current v0.3 incremental run (1,000,000 games)

The existing v0.3 serverless job and schema were expanded without replacing the original ten 10,000-game files. Before the run, direct SQL confirmed the 100,000-game counts and 20 historical ingestion records. After adding the remaining 90 files, all 100 uploaded sizes matched the source manifest. Databricks CLI bundle validation and deployment succeeded, with the same job ID and the all-file glob. One incremental job completed successfully in 324.55 s.

| Direct SQL check | Observed |
| --- | ---: |
| Bronze rows / distinct IDs | 1,000,000 / 1,000,000 |
| Silver rows / distinct IDs | 998,976 / 998,976 |
| Quarantine rows / distinct IDs | 1,024 / 1,024 |
| Managed Delta tables | 11 |
| Latest attempted / inserted / duplicate attempts | 1,000,000 / 900,000 / 100,000 |
| Latest accepted / quarantined | 899,101 / 899 |
| Latest file SHA-256 and attempted counts matching manifest | 100 of 100 |
| Ingestion history rows | 120 (20 prior + 100 latest) |
| Sum of `games` in each of the seven Gold marts | 998,976 |
| Gold rows matching local CSVs | 45,332 of 45,332 |

All 100 latest source receipts matched the [one-million-game chunk manifest](assets/sample-chunks-1m-manifest.json), including 10,000 attempted records per file, batch identities and the parent receipt timestamp. The first ten files produced 100,000 duplicate attempts with no inserts; the remaining 90 produced 900,000 inserts. Prior receipts were preserved. SQL rows in all seven Gold tables matched the local CSVs field-for-field, including 497,294 White wins, 463,906 Black wins, 37,776 draws per mart and exact rounded rates and length totals. This confirms incremental ingestion against an overlapping input, not a full one-million-game replay. No further job run was performed.

The public dashboard is the same locally exported snapshot, now matched against the cloud results; it does not query Databricks live.

### Historical v0.3 replay (100,000 games)

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

The replay added no new game records. These are the historical 100,000-game results before the incremental expansion described above.

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
