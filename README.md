# Retail Order-to-Cash

M1-T01 v1.1 establishes a local Docker runtime and PostgreSQL source/evaluation schema for a synthetic Retail reconciliation project. It includes PostgreSQL, Airflow LocalExecutor, one-shot dbt connectivity checks, and native smoke tests. All test records are synthetic.

The foundation contains exactly eight Retail tables across `bronze` and `core`. It does not implement CSV ingestion, reconciliation calculations, business DAGs, dbt business models, publication, or cloud deployment.

Follow [the local runbook](docs/LOCAL_RUNBOOK.md) for prerequisites, pinned versions, setup, testing, restart and shutdown. The smoke harness writes inspectable native evidence under ignored `artifacts/evidence/`; its presence alone does not establish a passing result or independent approval.

This checkout is `D:\retail-order-to-cash\Retail-Order-to-Cash`, on the authorized `feature/m1-t01-foundation` branch. The Owner-approved `AGENTS.md` and two project skills are inherited from `D:\retail-order-to-cash`; those governance files are outside this nested Git repository. The parent `README-OWNER.md` remains a historical draft and was not edited. Task execution authorization is supplied separately by the Owner.
