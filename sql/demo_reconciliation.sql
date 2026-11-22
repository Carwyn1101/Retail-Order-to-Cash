\set ON_ERROR_STOP on
-- Read-only demonstration. Supply -v evaluation_key=<key>; optionally
-- -v comparison_key=<before-due-key>. All money below is stored BIGINT cents.
\if :{?evaluation_key}
\else
\echo 'An explicit evaluation_key is required.'
SELECT 1/0;
\endif
\if :{?comparison_key}
\else
\set comparison_key :evaluation_key
\endif

\echo 'Canonical results and independent finding codes (one row per order)'
SELECT r.evaluation_key, r.order_id,
       r.expected_cents AS "O", r.payments_cents AS "P",
       r.refunds_cents AS "R", r.net_cents AS "N", r.overall_status,
       COALESCE(f.reasons, '') AS reason_codes
FROM core.reconciliation_results r
LEFT JOIN LATERAL (
    SELECT string_agg(reason_code, ',' ORDER BY reason_code) AS reasons
    FROM core.reconciliation_findings
    WHERE evaluation_key=r.evaluation_key AND order_id=r.order_id
) f ON TRUE
WHERE r.evaluation_key=:'evaluation_key'
ORDER BY r.order_id;

\echo 'Evaluation context and attempts; replays share a canonical key'
SELECT evaluation_key, snapshot_cutoff, evaluation_as_of, rules_version,
       count(*) AS attempt_count,
       count(*) FILTER (WHERE status='SUCCEEDED') AS succeeded,
       count(*) FILTER (WHERE status='FAILED') AS failed
FROM core.evaluation_runs
WHERE evaluation_key IN (:'evaluation_key', :'comparison_key')
GROUP BY evaluation_key,snapshot_cutoff,evaluation_as_of,rules_version
ORDER BY evaluation_as_of,evaluation_key;

\echo 'Before/after context comparison; no additional classification logic'
SELECT COALESCE(b.order_id,a.order_id) AS order_id,
       b.overall_status AS comparison_status, a.overall_status AS evaluation_status,
       b.payments_cents AS comparison_p, a.payments_cents AS evaluation_p,
       b.net_cents AS comparison_n, a.net_cents AS evaluation_n
FROM (SELECT * FROM core.reconciliation_results WHERE evaluation_key=:'comparison_key') b
FULL JOIN (SELECT * FROM core.reconciliation_results WHERE evaluation_key=:'evaluation_key') a
ON a.order_id=b.order_id
ORDER BY 1;

\echo 'Exception evidence references joined back to actual source occurrences'
WITH refs AS (
    SELECT f.order_id,f.reason_code,e.ref
    FROM core.reconciliation_findings f
    CROSS JOIN LATERAL jsonb_array_elements(f.evidence_refs) AS e(ref)
    WHERE f.evaluation_key=:'evaluation_key'
), raw AS (
    SELECT 'orders' AS source_name,row_id,batch_id,source_row_number,source_record_id,
           order_id,source_version,amount_raw,order_status AS source_status
    FROM bronze.orders_source
    UNION ALL
    SELECT 'payments',row_id,batch_id,source_row_number,source_record_id,
           order_id,source_version,amount_raw,payment_status FROM bronze.payments_source
    UNION ALL
    SELECT 'refunds',row_id,batch_id,source_row_number,source_record_id,
           order_id,source_version,amount_raw,refund_status FROM bronze.refunds_source
)
SELECT refs.order_id AS evaluated_order,refs.reason_code,raw.*
FROM refs JOIN raw ON raw.source_name=refs.ref->>'source_name'
 AND raw.row_id=(refs.ref->>'row_id')::BIGINT
 AND raw.batch_id=refs.ref->>'batch_id'
ORDER BY refs.order_id,raw.source_name,raw.row_id;

\echo 'All related visible source occurrences, including matched-order lineage'
-- This is provenance reporting, not a second effective-state calculator.
WITH ctx AS (
    SELECT DISTINCT snapshot_cutoff,evaluation_as_of FROM core.evaluation_runs
    WHERE evaluation_key=:'evaluation_key' AND status='SUCCEEDED'
), raw AS (
    SELECT 'orders' AS source_name,row_id,batch_id,source_row_number,source_record_id,
           order_id,source_version,event_at,source_updated_at FROM bronze.orders_source
    UNION ALL
    SELECT 'payments',row_id,batch_id,source_row_number,source_record_id,
           order_id,source_version,event_at,source_updated_at FROM bronze.payments_source
    UNION ALL
    SELECT 'refunds',row_id,batch_id,source_row_number,source_record_id,
           order_id,source_version,event_at,source_updated_at FROM bronze.refunds_source
)
SELECT r.order_id,raw.source_name,raw.row_id,raw.batch_id,raw.source_row_number,
       raw.source_record_id,raw.source_version
FROM core.reconciliation_results r
CROSS JOIN ctx
JOIN raw ON raw.order_id=r.order_id
JOIN bronze.input_batches b ON b.batch_id=raw.batch_id
WHERE r.evaluation_key=:'evaluation_key' AND b.status='COMMITTED'
  AND b.source_name=raw.source_name AND b.batch_sequence<=ctx.snapshot_cutoff
  AND (raw.event_at IS NULL OR raw.event_at<=ctx.evaluation_as_of)
  AND (raw.source_updated_at IS NULL OR raw.source_updated_at<=ctx.evaluation_as_of)
ORDER BY r.order_id,raw.source_name,raw.row_id;
