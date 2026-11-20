# TASK REPORT — M1-T01 v1.1

**Status: DONE FOR REVIEW.** Recorded 2026-11-20 (Asia/Bangkok). This report is an implementer claim for independent Tech Lead review, not merge approval or completion of M1 / AC-01–AC-10.

## A. Authorization, scope and implementation

The Owner explicitly authorized M1-T01 v1.1 in this chat: workspace `D:\retail-order-to-cash`, creation/work on `feature/m1-t01-foundation`, scoped file writes, local Docker/PostgreSQL/Airflow/dbt/tests and local commits after testing. No push, PR, merge, deployment, cloud resources, other projects or subsequent tasks were authorized or performed.

Preflight found the workspace parent is not a Git repository. The existing Retail checkout is `D:\retail-order-to-cash\Retail-Order-to-Cash`, inside that workspace, initially clean on `main`. `git switch -c feature/m1-t01-foundation` returned 0. All implementation files are in that checkout. Parent `AGENTS.md` and both project skills were read and applied. The only parent-file edit was the specifically authorized exact status-label replacement `OWNER-REVIEW DRAFT` → `OWNER-APPROVED`; that file is outside this checkout and cannot be included in its Git commit. No governance rules/skills were rewritten. The unchanged parent `README-OWNER.md` remains a historical draft.

### Files created/modified

| File | Change and purpose |
|---|---|
| `README.md` | Replace placeholder with foundation scope and runbook link |
| `compose.yml` | Local PostgreSQL, Airflow init/scheduler/webserver and on-demand dbt |
| `.env.example` | Blank, non-secret environment template |
| `.gitignore` | Exclude local secrets, evidence, generated targets/logs |
| `.gitattributes` | LF for shell, SQL and YAML container inputs |
| `docker/postgres/init.sh` | Separate database owners, credentials and CONNECT grants |
| `docker/airflow/init.sh` | Metadata migration and idempotent local admin creation |
| `docker/dbt/Dockerfile` | Pinned Python base, Git dependency and dbt build with pip check |
| `docker/dbt/requirements.txt` | All 54 resolved Python dependency versions |
| `dbt/dbt_project.yml`, `dbt/profiles.yml` | Connectivity only; no business models |
| `scripts/New-LocalEnv.ps1` | Generate ignored disposable local secrets; refuse overwrite |
| `sql/001_foundation.sql` | Transactional, repeatable eight-table schema and deferred-invariant comments |
| `tests/schema_smoke.sql` | Independent native expected/actual assertions; synthetic fixtures roll back |
| `tests/runtime_smoke.ps1` | T01-01–14 harness, exact command/exit/output logs, restart sentinel cleanup |
| `docs/LOCAL_RUNBOOK.md` | Prerequisites, exact setup/migrate/test/restart/shutdown procedures |
| `docs/TASK_REPORT_M1-T01.md` | This review handoff |
| Parent `..\AGENTS.md` | Authorized status-label change only; outside Git checkout |

### Runtime versions and layout

Observed host: PowerShell **7.6.9**, Docker Desktop **4.99.0**, Docker client/server **29.9.3**, Compose **5.7.0**, Linux/amd64 engine; Docker had 16 CPUs and 8,177,139,712 memory bytes available at preflight.

| Component | Selected and observed version |
|---|---|
| PostgreSQL | `postgres:16.11-bookworm`; server `16.11 (Debian 16.11-1.pgdg12+1)` |
| Airflow | `apache/airflow:2.11.0-python3.12`; native CLI `2.11.0` |
| dbt base | `python:3.12.12-slim-bookworm`, pinned digest in Dockerfile; native Python `3.12.12` |
| dbt Core / adapter | `1.10.15` / `dbt-postgres 1.9.1`, confirmed by native dbt debug |
| Git in dbt image | Debian `1:2.39.5-0+deb12u3` |

Downloaded service image digests (recorded in `artifacts/evidence/setup/image-digests.txt`):

- PostgreSQL: `sha256:a2420e9555e2224583fe84d0bb3f0b967e69354ae3a0be55a9c14e251388c4eb`.
- Airflow: `sha256:5c688e0df5359950f8901955a807dec2510d9f6ce6e56b23a8946108a61aba3e`.
- Python base: `sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c`.

One PostgreSQL service stores `retail` owned by login `retail` and `airflow` owned by separate login `airflow_meta`. PUBLIC CONNECT is revoked on both; application roles cannot connect to each other's database. App roles are not superusers. Retail migrations and assertions connect over TCP with the Retail credential. Only host loopback is published: PostgreSQL `127.0.0.1:55432`, Airflow `127.0.0.1:8080`. Named volumes preserve database data and Airflow logs. dbt is a tools-profile one-shot container; its temporary logs/targets are discarded. No continuously running dbt service, business DAG, ingestion, reconciliation logic or dbt model was added.

Airflow 2.11 uses init, scheduler and webserver for this task. Configured executor is `LocalExecutor`, with parallelism 2, active tasks per DAG 2 and active runs per DAG 1; one web worker and parsing process. Examples are disabled. The API returned zero DAG entries. No triggerer is required by this foundation with no deferred business tasks. This is a tested local version baseline, not a claim of current production support or security hardening. Compatibility references: [Airflow 2.11 prerequisites](https://airflow.apache.org/docs/apache-airflow/2.11.0/installation/prerequisites.html), [Airflow LocalExecutor deployment](https://airflow.apache.org/docs/apache-airflow/2.11.0/administration-and-deployment/production-deployment.html), [dbt PostgreSQL profile](https://docs.getdbt.com/docs/local/connect-data-platform/postgres-setup).

### Schema and design decisions

- `bronze`: `input_batches`, `orders_source`, `payments_source`, `refunds_source`.
- `core`: `evaluation_runs`, `reconciliation_results`, `reconciliation_findings`, `dq_findings`.
- Globally unique BIGINT identity `batch_sequence`; nonnegative row counts including zero; explicit technical batch status/commit time and readiness flag. No business source-status restriction.
- Technical row IDs, unique batch/source-row occurrence keys and technical batch foreign keys. No unique business IDs or cross-source business foreign keys; conflicting versions and orphan records remain representable.
- Raw payload is TEXT to preserve original representations, including malformed JSON. Parsed business identifiers/statuses/versions and normalized amounts/timestamps remain permissive/nullable where appropriate. Additional `source_updated_at` and technical `ingested_at` distinguish provenance times; insertion time is not business precedence.
- Seven nullable money fields are BIGINT cents without zero defaults. Validated timestamps use TIMESTAMPTZ. No float money, automatic conversion, rounding, deduplication, net calculation or business classification exists.
- Results have `(evaluation_key,order_id)` primary key; findings add reason code and reference the result grain. Processed status/reason values are constrained. DQ findings may lack an order, batch or source-row assignment; a source-row reference is polymorphic, not a rejecting cross-source FK.
- Multiple attempts can share an evaluation key. No ninth table, canonical-state store or business calculation was introduced.

## B. Native test evidence

Final command, run from the checkout:

```powershell
pwsh -NoProfile -File tests\runtime_smoke.ps1 -EvidenceDirectory artifacts\evidence\native-run-2
```

**Actual harness exit: 0; all 14 checks PASS; cleanup succeeded.** Execution occurred approximately 16:05–16:06 Asia/Bangkok. No mocks were used. The full expected/observed matrix, exact command argv, timestamps, actual exit codes and log references are in [`summary.json`](../artifacts/evidence/native-run-2/summary.json). All evidence paths below are relative to `artifacts/evidence/native-run-2/`. Runtime-generated logs remain ignored and are available locally for inspection.

| Check | Procedure / expected result | Actual observed result | Exit / evidence |
|---|---|---|---|
| T01-01 | `docker compose config --quiet`; `up -d --wait --wait-timeout 180 postgres airflow-scheduler airflow-webserver`; `ps -a`; services healthy | Config valid; postgres, scheduler and webserver healthy; init exited 0 | All 0; `001`–`003-T01-01.log` |
| T01-02 | Native config assertion, `airflow jobs check --job-type SchedulerJob`, health + authenticated API GET | Executor LocalExecutor, limits 2/2/1; live SchedulerJob; metadata/scheduler healthy; API HTTP 200, zero DAG entries | All 0; `004`–`006-T01-02.log` |
| T01-03 | Native role logins, database ownership/CONNECT assertions and rejected cross-login | `retail|retail`, `airflow|airflow_meta`; cross privileges false; Retail → Airflow rejected with `permission denied for database` | Positive 0; explicitly expected/observed cross-login **2**; `007`–`009-T01-03.log` |
| T01-04 | `docker compose --profile tools run --rm dbt debug`; all checks pass | `Connection test: [OK connection ok]`; `All checks passed!` | 0; `010-T01-04.log` |
| T01-05 | Native psql SQL assertions compare independently specified table-name array | Exactly eight approved tables, no additional Retail tables | 0; `011-T01-05.log` |
| T01-06 | Attempt duplicate `(batch_id,source_row_number)` for each of three sources | All rejected; expected/observed SQLSTATE **23505**, exact occurrence constraint names asserted | 0; shared `011-T01-05.log` |
| T01-07 | Attempt duplicate global batch sequence | Rejected, SQLSTATE **23505**, `input_batches_batch_sequence_key` | 0; shared SQL log |
| T01-08 | Insert repeated identities and versions as distinct occurrences | Three same-business-ID occurrences accepted in each source | 0; shared SQL log |
| T01-09 | Insert malformed raw money/payload/time, missing/unknown status and orphan references | Raw `001.2300`, malformed JSON/date text and source version `001` retained; parsed fields NULL; orphan order/payment IDs and optional refund payment ID accepted | 0; shared SQL log |
| T01-10 | Compare seven expected monetary columns to database catalog | Seven nullable BIGINT columns without zero defaults; temporal fields also TIMESTAMPTZ | 0; shared SQL log |
| T01-11 | Duplicate result/finding insert, distinct reason insert, DQ key and processed-value boundary checks | Duplicate keys **23505**; four distinct reasons allowed for one order; unassignable DQ retained; invalid processed status/reason **23514** | 0; shared SQL log |
| T01-12 | Insert two run attempts with same key/context | Two attempts accepted; future conflicting-context invariant explicitly deferred | 0; shared SQL log |
| T01-13 | Commit unique synthetic batch, fingerprint table OIDs/data, rerun `001_foundation.sql`, compare | DDL rerun preserved all eight OIDs and sentinel; existing-relation notices expected | All 0; `012`–`015-T01-13.log` |
| T01-14 | `docker compose restart postgres airflow-scheduler airflow-webserver`; wait healthy; compare fingerprint | Identical OIDs and persisted synthetic sentinel after ordinary restart; no volume deletion | All 0; `016`–`018-T01-14.log` |

The shared SQL command is:

```powershell
docker compose exec -T postgres sh -c 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -f /workspace/tests/schema_smoke.sql'
```

Expected rejections are caught and asserted by native PostgreSQL exception handlers; the test raises an exception if an insert unexpectedly succeeds. Therefore psql exit 0 denotes successful assertions, not ignored insert failures. SQL additionally confirmed duplicate batch IDs reject with 23505, missing technical batches reject with 23503, a committed empty readiness batch survives, and unknown result money stays NULL. Synthetic SQL records were rolled back. Restart sentinel cleanup log `019-cleanup.log` shows **`DELETE 1` then `0`**, exit 0. Identity sequence gaps are expected and harmless.

### Startup evidence and recovered failures

| Command/procedure | Actual exit and result | Local evidence |
|---|---|---|
| `docker version` in sandbox, then outside sandbox | Initial pipe permission denial; authorized retry 0, Docker engine available | Tool preflight transcript; Docker Desktop 4.99.0 / engine29.9.3 |
| `scripts/New-LocalEnv.ps1`, `docker compose config --quiet` | 0; local random credentials generated without printing; config valid | Tool transcript; ignored `.env` |
| `docker compose pull postgres airflow-init airflow-scheduler airflow-webserver` | First 1 due registry CDN EOF; retry 0, pinned images downloaded | `setup/pull.log`, `setup/pull-retry.log` |
| `docker compose --profile tools build dbt` | Initial build 0; full-lock rebuild 0; pip check passed | `setup/dbt-build.log`, `setup/dbt-build-locked.log` |
| `docker compose up -d --wait --wait-timeout 120 postgres` | 0; fresh project named volume initialized, service healthy | `setup/postgres-start.log` |
| Initial schema psql command using `/workspace/sql/001_foundation.sql` | 0; eight CREATE TABLE operations, COMMIT | `setup/schema-initial.log` |
| Native harness, `-EvidenceDirectory artifacts\evidence\native-run-1` | **1**; T01-01–03 PASS, T01-04 FAIL because Git absent; later checks NOT VERIFIED in this run | `native-run-1/summary.json`, `010-T01-04.log` |
| Add pinned Git dependency; rebuild dbt | 0; required Git installed, all requirements valid | `setup/git-package.log`, `setup/dbt-build-with-git.log` |
| Final native harness, `native-run-2` | **0**, T01-01–14 PASS and cleanup 0 | Final evidence matrix above |
| Host `Invoke-RestMethod http://127.0.0.1:8080/health` after restart | Successful host connection; metadata and scheduler healthy | `setup/host-health.json` |

The initial failure is preserved, not reclassified as PASS. Successful final native evidence supersedes its outstanding verification gaps. No tests were weakened to recover.

## C. Correctness limits, safety and Git handoff

Deferred as required by the task: committed-batch content/child-row immutability, content-digest identity, legal writer transitions, complete source/batch consistency, deterministic ingestion/replay, evaluation-key derivation and conflicting-context rejection, result atomicity/publication, and all ingestion/reconciliation/DQ business rules. Keys alone do not prove these invariants. This task validates storage capabilities and technical constraints only; it does not demonstrate AC-01–AC-10 or financial reconciliation correctness.

DDL uses `CREATE ... IF NOT EXISTS` transactionally. It preserves a correct existing schema and its data; it does not repair arbitrary historical drift. The schema is rerunnable, not a general migration framework. Credentials persist in ignored `.env`; replacing them does not rotate users in an existing database volume. These local version pins require registry/package availability and are not a byte-identical OS dependency snapshot or package-hash lock.

Only synthetic isolated SQL test data was used. No real transaction data/credentials, cloud resources or external publication APIs were used. Network access was limited to public documentation, image registries and build package repositories. No volume was deleted or destructively reinitialized. Runtime data/logs/credentials remain outside tracked files. No business DAG, ingestion, business dbt model, reconciliation logic, CI/CD expansion or next task was implemented. Internal agent reviews are implementation self-review, not independent Tech Lead review.

Pre-commit audit completed: `git diff --cached --check` returned **0**; `git diff --cached --stat` and `git status --short --branch` showed exactly **17** scoped source/documentation files on `feature/m1-t01-foundation`. A source scan of all 17 staged/unignored files against the generated local secret values found no matches (exit **0**). `git check-ignore` confirmed `.env` and native evidence are ignored (exit **0**). An inspection of the final summary confirmed 14 PASS statuses and no recorded failure (exit **0**). No unexpected existing user changes were present. Only the explicit foundation paths are committed after native test success. Local commit IDs and final clean status are recorded in the delivery message and ignored `artifacts/evidence/finalization.json`; this report deliberately does not embed its own commit hash. Parent `AGENTS.md` remains the one authorized change outside the repository commit. Independent review must consider that parent label change separately.

## D. Reproduction and review

Follow [LOCAL_RUNBOOK.md](LOCAL_RUNBOOK.md) for exact prerequisite, startup, migration, test, restart and shutdown commands. In this environment Docker commands need permission to reach Docker Desktop outside the tool sandbox; that is a tool access requirement, not an application dependency.

Re-run the harness with a new evidence directory to preserve earlier results:

```powershell
Set-Location 'D:\retail-order-to-cash\Retail-Order-to-Cash'
pwsh -NoProfile -File tests\runtime_smoke.ps1
$LASTEXITCODE
```

Services are left healthy for local review; use `docker compose down` when finished to stop/remove project containers and network while retaining named volumes. Never use `down -v` or destructive cleanup for this task. No unresolved native test failure remains in the final run. Remaining work is independent Tech Lead review and Owner disposition; no next task is authorized.
