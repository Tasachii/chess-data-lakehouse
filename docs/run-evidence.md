# Measured run evidence

All measurements below are from local runs on 27 November 2026 with Python 3.12. The publisher is the [Lichess Open Database](https://database.lichess.org/), and the source is the [October 2026 rated standard archive](https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst), licensed CC0 1.0. Only bounded prefixes were downloaded; the full archive was not processed.

## Dashboard sample

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

The [dashboard snapshot](assets/snapshot.json) includes the aggregate rows, receipt checksum, source retrieval timestamp and validated UTC time range. Its byte SHA-256 is `b299b5a7b654f0be438787cdb76f04ae7be9f78a825e1080a4b02ed76a6a8165`. The exact reproduction commands are in [README.md](../README.md#reproduce-the-snapshot).

## Pipeline validation sample (100 games)

The local pipeline and PySpark/Delta implementation were validated on a smaller prefix before the dashboard was built. The download wrote 100 records and 234,489 bytes; repeated downloads matched SHA-256 `d2675dd2381bcf736422367ea8800474939a067f05e7a949aa3b52fbd41b60f6`. The first corrected local run inserted 100 games; replay inserted zero. All 100 games were valid and had canonical eight-character Lichess IDs.

| Speed | Games | White wins | Draws |
| --- | ---: | ---: | ---: |
| Blitz | 56 | 31 | 3 |
| Bullet | 27 | 11 | 2 |
| Rapid | 17 | 10 | 1 |

There were 82 opening groups, 10 rating buckets and three speeds. Local Spark matched all 100 Silver records and all 95 Gold groups. The managed Delta test created and replayed tables for this real sample and the synthetic fixture, with no duplicate keys. With PySpark 3.5.3, Delta 3.3.2 and Java 21, the corrected full suite passed 27 tests in 47.22 s. The 100-game sample is not the dashboard sample.

Independent QA approved the local pipeline at commit `94f58a11304cc11c92e102f3894aabcbdcc80ce9`. The regression cases covered alternate game URLs, incomplete PGN boundaries, truncated Zstandard input, missing fields, overlap and empty delivery, and run lineage. Original independent reproduction scripts passed eight cases; three deliberately truncated files were rejected without publication. The local fixture and real replays preserved all aggregate results.

## Dashboard release verification

The local base suite, including two aggregate-export tests, passed 21 tests with the optional Spark module skipped. The full local Spark/Delta suite passed 29 tests in 47.33 s (47.65 s wall clock), using the original 100-game validation sample. Browser checks exercised the 10,000-game dashboard at 1,440, 390 and 320 pixel widths, including opening search, filters, sorting, rating selection and absence of JavaScript errors. The pipeline source and Databricks configuration are unchanged from the independently approved version.

## Limitations

Runtime depends on the machine and network; these measurements are not a scale benchmark. The archive prefix is not a random or representative sample. Databricks bundle YAML and script references were checked locally, but the CLI and workspace execution were not verified. No cloud services or billing changes were used.
