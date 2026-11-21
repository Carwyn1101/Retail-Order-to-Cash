# Retail Order-to-Cash

This synthetic Retail project includes the M1-T01 local Docker/PostgreSQL/Airflow/dbt foundation and M1-T02 on-demand three-source JSONL batch ingestion. All sample records and test data are synthetic.

The foundation contains exactly eight Retail tables across `bronze` and `core`. Ingestion preserves source occurrences and raw lines, parses exact integer cents and explicit-offset timestamps, and commits each source batch atomically. It does not calculate reconciliation, choose effective source versions, schedule business DAGs, publish data or deploy.

Follow [the foundation runbook](docs/LOCAL_RUNBOOK.md) for setup and [the ingestion runbook](docs/INGESTION_RUNBOOK.md) for fixtures, commands and ingestion contracts. Test harnesses write inspectable evidence under ignored `artifacts/evidence/`. Task summaries, results and limitations are delivered in the relevant GitHub PR description for independent review.

This checkout is `D:\retail-order-to-cash\Retail-Order-to-Cash`. Use the branch authorized for the current task. The Owner-approved `AGENTS.md` and two project skills are inherited from `D:\retail-order-to-cash`; those governance files are outside this nested Git repository. The parent `README-OWNER.md` remains a historical draft. Task execution authorization is supplied separately by the Owner.
