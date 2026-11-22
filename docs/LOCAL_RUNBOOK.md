# Local foundation runbook — M1-T01 v1.1

Run these commands in PowerShell 7 from `D:\retail-order-to-cash\Retail-Order-to-Cash`, on the branch authorized for the current task. This runbook covers the local foundation; [INGESTION_RUNBOOK.md](INGESTION_RUNBOOK.md) covers the on-demand M1-T02 runner. Documentation does not grant execution, remote Git or deployment authorization.

## Prerequisites and pinned baseline

Use Docker Desktop with its Linux engine running, Docker Compose v2 or newer with `--wait` support, and PowerShell 7. Allow at least 4 GB for Airflow plus PostgreSQL/build overhead; 8 GB available to Docker is a practical starting point for this small local runtime. Image downloads and the initial dbt build require registry/PyPI access and disk space. Host ports `8080` and `55432` must be free.

| Component | Pin |
|---|---|
| PostgreSQL image | `postgres:16.11-bookworm` |
| Airflow build base | `apache/airflow:2.11.0-python3.12` |
| Local Airflow image | `retail-m1-t04-airflow:2.11.0-dbt1.10.15` |
| dbt build base | `python:3.12.12-slim-bookworm` |
| dbt Core | `1.10.15` |
| dbt PostgreSQL adapter | `1.9.1` |
| Git in dbt image | Debian `1:2.39.5-0+deb12u3` |

`docker/dbt/requirements.txt` pins the 54 resolved Python packages for the dbt image. M1-T04 also installs these unchanged pins in an isolated venv inside the derived Airflow image, without replacing Airflow's packages; see the [Airflow runbook](AIRFLOW_RUNBOOK.md). Both image builds run `pip check` for the dbt environment. The standalone Python base is pinned by tag and digest; the other service bases use exact version tags. Python packages are version-pinned without hash locks. Git is required by `dbt debug`; both builds include the same pinned Debian package. Rebuilding requires those packages to remain available from their registries.

Airflow 2.11's [documented compatibility](https://airflow.apache.org/docs/apache-airflow/2.11.0/installation/prerequisites.html) includes Python 3.12 and PostgreSQL 16. Its [LocalExecutor runs task subprocesses with the scheduler](https://airflow.apache.org/docs/apache-airflow/2.11.0/administration-and-deployment/production-deployment.html), so this version-specific layout uses an init job, scheduler and webserver. It does not reuse the official Celery Compose layout. This pinned local foundation is not a production hardening or ongoing support guarantee.

## First setup

Check the engine and branch before creating local runtime state:

```powershell
Set-Location 'D:\retail-order-to-cash\Retail-Order-to-Cash'
git status --short --branch
docker version
docker compose version
```

Generate local credentials once. The script refuses to overwrite an existing `.env`; it does not print secret values. `.env.example` contains blank non-secret templates only.

```powershell
pwsh -NoProfile -File .\scripts\New-LocalEnv.ps1
docker compose config --quiet
docker compose pull postgres
docker compose build airflow-scheduler
docker compose --profile tools build dbt
docker compose up -d --wait --wait-timeout 180 postgres
```

Apply the Retail schema through the non-superuser Retail role. The single-quoted PowerShell argument preserves the container's environment-variable expansion without putting a password in command text:

```powershell
docker compose exec -T postgres sh -c 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -f /workspace/sql/001_foundation.sql'
docker compose up -d --wait --wait-timeout 180 airflow-scheduler airflow-webserver
docker compose ps -a
```

Check each command's exit code (`$LASTEXITCODE`) and stop on an unexpected nonzero result. `airflow-init` migrates metadata and creates the local `admin` user if absent; successful one-shot completion is expected. Reapplying the foundation SQL preserves existing objects and records. It does not repair arbitrary schema drift.

The PostgreSQL entrypoint creates the two databases/roles only when its named data volume is first initialized. Preserve `.env` alongside that volume. Regenerating credentials or changing `.env` does not rotate database passwords or an existing Airflow admin password.

## Native smoke tests and evidence

Run the complete native harness after setup:

```powershell
pwsh -NoProfile -File .\tests\runtime_smoke.ps1
$LASTEXITCODE
```

The script records T01-01 through T01-14 under `artifacts/evidence/<timestamp>/`. Inspect `summary.json` for each expected/observed result and each command log for arguments, output and actual exit code. A nonzero harness exit means the run failed; checks not reached remain `NOT VERIFIED`. Tests run against real local Docker/PostgreSQL/Airflow/dbt, without substituting mocks.

Checks cover Compose startup, the configured `LocalExecutor` and concurrency limits, scheduler/UI/authenticated API health, separate database roles, `dbt debug`, the eight-table contract, asserted duplicate-key SQLSTATEs, permissive invalid/orphan source evidence, BIGINT money, retry attempts, idempotent schema setup and ordinary restart persistence.

SQL fixtures are synthetic and rolled back. The restart test temporarily commits a uniquely named synthetic batch, verifies its survival and table object IDs, then deletes that batch in `finally`. Identity sequence gaps after rolled-back tests are expected. Read the cleanup log if a run fails during shutdown or database unavailability. Tests never remove a volume.

To run only connectivity or the SQL assertions during diagnosis:

```powershell
docker compose --profile tools run --rm dbt debug
docker compose exec -T postgres sh -c 'export PGPASSWORD="$RETAIL_DB_PASSWORD"; exec psql -X -h 127.0.0.1 -U retail -d retail -v ON_ERROR_STOP=1 -f /workspace/tests/schema_smoke.sql'
```

The dbt profile uses environment credentials in the [documented PostgreSQL profile format](https://docs.getdbt.com/docs/local/connect-data-platform/postgres-setup). dbt runs on demand, with temporary logs/targets inside its disposable container and anonymous usage reporting disabled. M1-T03 adds the ephemeral reconciliation business model described in the [reconciliation runbook](RECONCILIATION_RUNBOOK.md); M1-T04 orchestrates it through the manually triggered [Airflow DAG](AIRFLOW_RUNBOOK.md).

## Local access and storage

| Destination | Address or identity |
|---|---|
| Airflow UI/API | `http://127.0.0.1:8080`, username `admin` |
| Host PostgreSQL | `127.0.0.1:55432` |
| Retail data | Database `retail`, role `retail` |
| Airflow metadata | Database `airflow`, role `airflow_meta` |

Look up passwords privately in ignored `.env`. Do not paste expanded Compose configuration, container environment dumps or credentials into evidence or Git. PostgreSQL superuser credentials exist for local initialization; ordinary Retail schema/test operations use `retail`. The two application roles have their own database credentials and no cross-database `CONNECT` grant. Both exposed ports bind to loopback only.

Compose project `retail-m1-t01` uses named `postgres_data` and `airflow_logs` volumes. PostgreSQL data, Airflow metadata and logs remain outside tracked source files. Evidence lives in ignored `artifacts/`. Airflow examples are disabled. Parallelism is `2`, active tasks per DAG `2`, active runs per DAG `1`, with one web worker and one parsing process. These bounds are local resource choices, not production SLAs.

## Schema and deferred contracts

| Schema | Tables |
|---|---|
| `bronze` | `input_batches`, `orders_source`, `payments_source`, `refunds_source` |
| `core` | `evaluation_runs`, `reconciliation_results`, `reconciliation_findings`, `dq_findings` |

The separate Airflow database contains Airflow's own metadata tables, excluded from the eight Retail tables. Raw source business identities/statuses/versions stay permissive; technical occurrence keys and batch foreign keys preserve provenance. Original monetary text and full raw payload are retained as text, including malformed records. Parsed money is nullable BIGINT cents and validated timestamps are TIMESTAMPTZ, using a UTC convention. M1-T02 adds bounded Python parsing into those existing columns; no business deduplication, effective-state selection or reconciliation calculation is implemented.

The M1-T02 workflow enforces bounded batch identity, atomic commitment, source/row lineage, explicit completeness metadata and unchanged committed-batch replay. Primary keys alone do not establish immutability; direct SQL access remains outside that workflow guarantee. Business input readiness, effective states and broader replay/backfill remain deferred. Future evaluation writers must derive/validate stable evaluation keys, reject conflicting snapshot/as-of/rules context reuse and publish atomically. Multiple attempts may share an evaluation key now; the schema alone does not prove context consistency.

## Restart and shutdown

For an ordinary restart, retain containers' named volumes and wait for health again:

```powershell
docker compose restart postgres airflow-scheduler airflow-webserver
docker compose up -d --wait --wait-timeout 180 postgres airflow-scheduler airflow-webserver
docker compose ps -a
```

To remove this project's service containers/network while preserving named data/log volumes:

```powershell
docker compose down
```

Do not add `-v`, delete volumes or destructively reinitialize databases for test cleanup. Start again with the same `.env` and the documented `up` command. A runtime blocker or failed native test must be reported with its actual output; missing evidence is not a PASS.

## Governance location

The authorized workspace is `D:\retail-order-to-cash`; its actual Git checkout is the nested `Retail-Order-to-Cash` directory. Owner-approved `AGENTS.md` and both project skills reside in the workspace parent and apply to this checkout. The approved status-label correction is outside this Git repository and cannot appear in its commit. The parent `README-OWNER.md` is an unchanged historical draft; task authority comes from the Owner's explicit M1-T01 v1.1 authorization and current `AGENTS.md`.

Task summaries, test evidence and limitations are recorded in the relevant PR description under the Owner-approved delivery workflow. Independent Tech Lead review and a separate Owner merge decision are required. Test success does not authorize deployment or the next task.
