# Dev1 handoff v0.2.0 — TEST1-01 through TEST1-04 remediation

**Developer status: fixes verified locally; ready for Test1 fresh-copy retest, NOT independent QA acceptance yet.** Captured 2026-11-27 ICT on macOS arm64, Python 3.12.0, Java 21.0.2, PySpark 3.5.3 / Delta 3.3.2. Scope was only the Chess Data Lakehouse. The lead's own project Git repository is used; no home-repo, sibling-project or QA-file changes, and no cloud account/credentials/billing/deployment changes. Earlier v0.1.0 evidence remains in `docs/run-evidence.md` for history, not as proof of the corrected ID contract.

## Findings and fixes

| Finding | Root cause and remediation | Observed retest |
|---|---|---|
| TEST1-01 HIGH | Both ID regexes excluded actual Lichess `/game/<id>` URLs. Both now recognize exact host, optional `game/` prefix, and 8-character ID; non-Lichess content hash is retained. | All 100 real Bronze IDs are 8 characters, zero fallbacks. Actual first game `Q8ku5wgM` with changed Event text: first insert 1, replay 0; 1 Bronze / 1 Silver, one Gold draw. Local Delta logical replays also retain 1/1/0. |
| TEST1-02 HIGH | Parser required `saw_moves` before recognizing next Event, causing tag leakage. The local parser and streaming downloader now share an Event-boundary parser; Spark splits identically. Empty move text is quarantined even if tags are otherwise valid. | Exact QA header-only + valid sibling: 2 attempted, 2 Bronze, 1 Silver, 1 quarantine. `qa000002` quarantined for missing Elo and move text; valid `qa000001` has `Opening=Unknown`; Gold opening `Unknown,1,0,1,0.0`. Spark regression validates IDs, quarantine and sibling opening. |
| TEST1-03 HIGH | Zstd stream reader silently accepted physical EOF in an incomplete frame. A frame-aware incremental decoder checks `.eof`, handles concatenated frames, and rejects incomplete/corrupt local `.zst` before any layer writes. Bounded downloader fails when EOF precedes requested records; deliberate bound stop does not read/validate the archive suffix. | QA exact 1/10/30-byte truncations all exit 1, no fresh output. Regression also checks existing-output bytes unchanged, short complete source, early EOF, and valid concatenated frames. |
| TEST1-04 MEDIUM | Latest manifest overwrote lineage, omitted receipt metadata and mixed batch/cumulative counts. v0.2.0 adds deterministic `batch_id`, Bronze source hash, verified downloader receipt, version, per-run history and reconciled counters in local JSON and Delta `ingestion_runs`. | All checked histories reconcile; local actual sample receipt bound=100, retrieved timestamp, CC0-1.0, version 0.2.0. Delta real/fixture/logical/empty runs append lineage and preserve keys/counts. |

**Migration:** old Bronze contains incorrect real fallback IDs and lacks batch lineage; the new code refuses populated legacy Bronze to prevent mixed-key double counting. Rebuild original source into a fresh `output/*-v2` directory or separate fresh Delta schema. Actual legacy `output/real` check returned exit 1 with `Legacy Bronze IDs/lineage`; all 7 original artifacts' SHA-256 values were unchanged. No old output overwritten or deleted.

## 1. Actual source and local baseline

From the project root (`/Users/tasachi/Projects/de-portfolio/chess-data-lakehouse`):

```bash
SOURCE_URL=https://database.lichess.org/standard/lichess_db_standard_rated_2026-10.pgn.zst
/usr/bin/time -p .venv/bin/chess-lakehouse download \
  --url "$SOURCE_URL" --output data/remediated_sample_100.pgn --max-games 100
shasum -a 256 data/remediated_sample_100.pgn data/real_sample_100.pgn
cat data/remediated_sample_100.pgn.receipt.json
```

Measured: 100 complete source records / 234,489 decompressed bytes, wall clock 1.89 s, SHA-256 `d2675dd2381bcf736422367ea8800474939a067f05e7a949aa3b52fbd41b60f6` on both files (unchanged from initial sample). Actual receipt retrieval `2026-11-27T10:19:17.039637+00:00`; bound=100; license CC0-1.0. Publisher [Lichess Open Database](https://database.lichess.org/) is a CC0 source; only this prefix is processed (not the 31 GB archive).

Real first run: 100 input / 100 inserted / 100 Bronze / 100 Silver / 0 quarantine. Exact replay: 100 input / 0 inserted / same cumulative totals. All real Gold dimensions preserve 100 games, 52 white wins, 42 black wins and 6 draws. 82 distinct opening groups, 10 average-rating groups, 3 speed groups. Observed speeds:

| Speed | Games | White wins | Draws | White win rate |
|---|---:|---:|---:|---:|
| Blitz | 56 | 31 | 3 | 0.5536 |
| Bullet | 27 | 11 | 2 | 0.4074 |
| Rapid | 17 | 10 | 1 | 0.5882 |

The synthetic fixture still yields 3 attempts / 2 Bronze / 1 Silver / 1 quarantine and exact replay inserts zero. Independent ordinary test replay/fresh rebuild compares all six data-layer files byte-for-byte between each v0.2.0 run; manifest history is intentionally run-specific. The v0.1.0 IDs and Bronze schema differ deliberately; analytical metrics are unchanged.

## 2. Exact independent QA reproduction, developer-run on fresh copy

Read original QA report `../qa/test1-report.md` and original scripts at `../qa/test1-evidence/run.CVt9IY/`. No QA files were edited. Copied the scripts unmodified into an ignored new evidence directory in the project, copied only the project sources/config/tests and newly downloaded sample/receipt, removed no existing outputs. Commands actually executed (choose a new destination if repeating):

```bash
mkdir -p output/test1-reproduction-final/project/data
rsync -a --exclude='.git' --exclude='.venv' --exclude='output' \
  --exclude='.pytest_cache' --exclude='.ruff_cache' --exclude='__pycache__' \
  --exclude='data/*.pgn' --exclude='data/*.receipt.json' ./ output/test1-reproduction-final/project/
cp ../qa/test1-evidence/run.CVt9IY/check.py \
   ../qa/test1-evidence/run.CVt9IY/check-source-corruption.py output/test1-reproduction-final/
cp data/remediated_sample_100.pgn output/test1-reproduction-final/project/data/real_sample_100.pgn
cp data/remediated_sample_100.pgn.receipt.json \
   output/test1-reproduction-final/project/data/real_sample_100.pgn.receipt.json
cd output/test1-reproduction-final/project
python3.12 -m venv .venv
.venv/bin/python -m pip install --no-cache-dir -e '.[dev]' > ../install.log 2>&1
.venv/bin/python -m pytest -ra --basetemp ../base-pytest > ../base-test.log 2>&1
.venv/bin/python ../check.py > ../independent-cases.log 2>&1
.venv/bin/python ../check-source-corruption.py > ../source-corruption.log 2>&1
```

Fresh package metadata version 0.2.0; **19 passed, one optional Spark module skipped, 0 failures in 0.68 s**. QA script separately reports **8/8 PASS, 0 FAIL** (ordinary fixture and real clean rebuild/replay, overlap, empty, corrupt random bytes, malformed boundary, unavailable source, reordered input). `check-source-corruption.py` reports 100 unique source URLs, all 100 complete terminal markers, zero fallback IDs (formerly 100), 3/3 truncated files rejected with exit 1 and `published False`, and a logical duplicate case with 1 Bronze and 1 Silver instead of 2. Independently asserted canonical ID `Q8ku5wgM` and Gold `blitz,1,0,1,0.0` from the exact case output. Logs and artifacts in `output/test1-reproduction-final/` are ignored, not repository deliverables. This is developer re-execution of the QA scripts, not Test1 sign-off.

### Machine-readable lineage verification

Loaded `manifests/runs/*.json` and Bronze JSONL for each of the freshly reproduced QA cases with a separate Python inspection; checked `batch_id == sha256(source_url + '\n' + source_sha256)`, Bronze-to-batch hash and URL equality, `pipeline_version=0.2.0`, `attempted=duplicates+inserted`, `inserted=accepted+quarantined`, and `bronze=silver+quarantine`. All assertions passed. Exact observed rows:

| Delivery | Attempted | Duplicates | Inserted | Accepted new | Quarantined new | Cumulative B/S/Q |
|---|---:|---:|---:|---:|---:|---|
| Fixture first | 3 | 1 | 2 | 1 | 1 | 2/1/1 |
| Fixture replay | 3 | 3 | 0 | 0 | 0 | 2/1/1 |
| Overlap first | 4 | 1 | 3 | 1 | 2 | 5/2/3 |
| Overlap replay | 4 | 4 | 0 | 0 | 0 | 5/2/3 |
| Empty delivery into existing fixture state | 0 | 0 | 0 | 0 | 0 | 5/2/3 |
| Real first | 100 | 0 | 100 | 100 | 0 | 100/100/0 |
| Real replay | 100 | 100 | 0 | 0 | 0 | 100/100/0 |
| Malformed boundary | 2 | 0 | 2 | 1 | 1 | 2/1/1 |
| Logical first | 1 | 0 | 1 | 1 | 0 | 1/1/0 |
| Logical alternate delivery | 1 | 1 | 0 | 0 | 0 | 1/1/0 |
| Empty fresh | 0 | 0 | 0 | 0 | 0 | 0/0/0 |

Fixture output retained 5 run history files / 3 deterministic batch identities after the empty run; real retained 2 runs / 1 batch; logical variant retained 2 runs / 2 content batches for one game key. Actual real manifests contain receipt-derived `retrieved_at_utc` and `requested_max_games`; manually supplied fixture/external files deliberately use `null` for unknown retrieval/bound. Unknown external licenses are `null`; README describes the distinctions. Legacy four-field CLI `counts` is unchanged for compatibility; new `batch_counts` and `cumulative_counts` disambiguate the receipt vs persisted state.

## 3. Full local PySpark and Delta suite

Original Spark baseline used preinstalled host PySpark 3.5.3; matching Delta 3.3.2 Python package and JARs reside in ignored `output/spark-deps`. Portable installation is `pip install -e '.[dev,spark]'` and a JDK; Maven auto-resolution is not separately tested here. To recreate the tested environment if needed:

```bash
mkdir -p output/spark-deps/jars
.venv/bin/python -m pip install --no-cache-dir --no-deps --target output/spark-deps/python 'delta-spark==3.3.2'
curl -fLsS --max-time 60 https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar -o output/spark-deps/jars/delta-spark_2.12-3.3.2.jar
curl -fLsS --max-time 60 https://repo.maven.apache.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar -o output/spark-deps/jars/delta-storage-3.3.2.jar
```

Executed final full suite command from the production project root (use new `--basetemp` directory on QA rerun):

```bash
SPARK_LOCAL_IP=127.0.0.1 \
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
PYTHONPATH="$PWD/output/spark-deps/python:/Library/Frameworks/Python.framework/Versions/3.12/lib/python3.12/site-packages" \
CHESS_TEST_DELTA=1 \
CHESS_DELTA_JARS="$PWD/output/spark-deps/jars/delta-spark_2.12-3.3.2.jar,$PWD/output/spark-deps/jars/delta-storage-3.3.2.jar" \
CHESS_REAL_SAMPLE="$PWD/data/remediated_sample_100.pgn" \
/usr/bin/time -p .venv/bin/python -m pytest -s -ra tests --basetemp output/test1-fix-spark-pytest-v4
```

**27 passed, 0 failed, 0 skipped in 47.22 s; wall clock 47.61 s.** Spark ANSI mode on, UTC session, actual local managed Delta tables. Actual first/replay counts:

| Delta input | Incoming distinct | Bronze | Silver | Quarantine |
|---|---:|---:|---:|---:|
| Synthetic fixture (both executions) | 2 | 2 | 1 | 1 |
| Real 100-game sample (both executions) | 100 | 100 | 100 | 0 |
| Synthetic actual-URL logical variant (both executions) | 1 | 1 | 1 | 0 |
| Empty PGN following logical replay | 0 | 1 | 1 | 0 |

All 100 real Silver rows match the local transform field-for-field; all 95 Gold dimension rows match expected metrics. Actual Delta persisted Gold tables match the real local reference. Spark history tests assert two real receipts with the same deterministic batch ID and correct 100/0 inserted counts, real source license and receipt metadata, two logical content batches with one game key, and a third empty delivery which appends history without inserting a game. Spark binary-file scans omitted zero-byte files; fixed by verifying omitted indexed local/Volume paths have zero bytes and recording the empty receipt (the earlier intermediate full run 26 passed/1 failed exposed this; final 27 pass confirms correction). No fabricated retrieval timestamp or guessed empty state accepted. Spark only accepts decompressed `.pgn` input; supplied expected SHA-256 and record bound are checked before any table writes.

## 4. Static checks and repository status

```bash
.venv/bin/ruff check .
.venv/bin/ruff format --check src tests
.venv/bin/python -m compileall -q src tests
python3.12 - <<'PY'
import yaml
from pathlib import Path
bundle=yaml.safe_load(Path('databricks.yml').read_text())
job=yaml.safe_load(Path('resources/chess_lakehouse.job.yml').read_text())['resources']['jobs']['chess_lakehouse']
assert set(bundle['variables'])=={x['name'] for x in job['parameters']}
assert (Path('resources')/job['tasks'][0]['spark_python_task']['python_file']).exists()
PY
```

Ruff PASS, 13 Python files formatted, compileall exit 0, both YAML files parsed and seven bundle variables matched Job parameters; Python script reference exists. Project own Git root confirmed `.../de-portfolio/chess-data-lakehouse`; ignored raw PGNs, receipts, compressed files in `data/`, venvs, and local output/test/JAR evidence. No Git operation in the home repository.

## 5. Remaining limitations/status

- No Databricks CLI execution, bundle schema validation or actual Databricks workspace execution; CLI unavailable and authorized workspace not established. Bundle changes add receipt parameters/metadata, Job history and a single concurrent run, but cloud behavior is not claimed verified. README includes upload, receipt parameters, deployment and post-run SQL instructions for an existing authorized workspace only.
- Bounded micro-batch MVP: source files must remain unchanged while a run reads them; serial processing, no cross-table atomic transaction. Manifest/Delta history records successful runs, not interrupted attempts. Stage interruption/recovery has not been independently simulated. Unread suffix of a bounded download isn't certified intact. Header/move text checks are not chess legality validation or arbitrary PGN grammar.
- Unknown retrieval time/bound for a manually supplied file stays null; no inference from run time. Legacy v0.1.0 datasets require rebuilding (intentional safety behavior); preserved old outputs are historical, not trustworthy for new IDs. No full archive or scale/performance claims.
- Fresh basic install passed; full Spark/Delta tests passed using the documented actual host/JAR route, not fresh portable Maven dependency installation. Resume-safe claim remains a bounded 100-game real sample and locally verified Python/PySpark/Delta lakehouse, not deployed Databricks.

**Test1 independent retest pending.** The updated README and regressions cover all four returned issues. Lead should use this handoff and independently execute Test1 against the fresh project snapshot.
