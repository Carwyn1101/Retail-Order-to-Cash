{{ config(materialized='ephemeral') }}

-- M1-T03: bounded F01/F03/F08 only. No source-version precedence is inferred.
-- The caller executes this compiled SELECT in its publication snapshot/transaction.
with context as (
    select {{ var('snapshot_cutoff') | int }}::bigint as snapshot_cutoff,
           '{{ var("evaluation_as_of") | replace("'", "''") }}'::timestamptz as evaluation_as_of,
           '{{ var("rules_version") | replace("'", "''") }}'::text as rules_version
), batches as (
    select b.* from bronze.input_batches b cross join context c
    where b.status = 'COMMITTED' and b.committed_at is not null
      and b.batch_sequence <= c.snapshot_cutoff
), occurrence_counts as (
    select batch_id, count(*) filter (where source_name = 'orders') as orders_count,
           count(*) filter (where source_name = 'payments') as payments_count,
           count(*) filter (where source_name = 'refunds') as refunds_count
    from (
        select batch_id, 'orders'::text as source_name from bronze.orders_source
        union all select batch_id, 'payments' from bronze.payments_source
        union all select batch_id, 'refunds' from bronze.refunds_source
    ) occurrences group by batch_id
), batch_checks as (
    select b.*, coalesce(n.orders_count, 0) as orders_count,
           coalesce(n.payments_count, 0) as payments_count,
           coalesce(n.refunds_count, 0) as refunds_count,
           case b.source_name
             when 'orders' then b.row_count = coalesce(n.orders_count, 0)
               and coalesce(n.payments_count, 0) = 0 and coalesce(n.refunds_count, 0) = 0
             when 'payments' then b.row_count = coalesce(n.payments_count, 0)
               and coalesce(n.orders_count, 0) = 0 and coalesce(n.refunds_count, 0) = 0
             when 'refunds' then b.row_count = coalesce(n.refunds_count, 0)
               and coalesce(n.orders_count, 0) = 0 and coalesce(n.payments_count, 0) = 0
             else false
           end as lineage_consistent
    from batches b left join occurrence_counts n using (batch_id)
), readiness as (
    select count(distinct source_name) = 3
             and coalesce(bool_and(scope_complete and lineage_consistent), false) as ready,
           coalesce(jsonb_agg(jsonb_build_object(
               'batch_id', batch_id, 'source_name', source_name,
               'batch_sequence', batch_sequence, 'row_count', row_count,
               'scope_complete', scope_complete, 'lineage_consistent', lineage_consistent
           ) order by batch_sequence, batch_id), '[]'::jsonb) as batch_evidence
    from batch_checks
), orders_visible as (
    select s.* from bronze.orders_source s join batches b using (batch_id)
    cross join context c
    where (s.event_at is null or s.event_at <= c.evaluation_as_of)
      and (s.source_updated_at is null or s.source_updated_at <= c.evaluation_as_of)
), payments_visible as (
    select s.* from bronze.payments_source s join batches b using (batch_id)
    cross join context c
    where (s.event_at is null or s.event_at <= c.evaluation_as_of)
      and (s.source_updated_at is null or s.source_updated_at <= c.evaluation_as_of)
), refunds_visible as (
    select s.* from bronze.refunds_source s join batches b using (batch_id)
    cross join context c
    where (s.event_at is null or s.event_at <= c.evaluation_as_of)
      and (s.source_updated_at is null or s.source_updated_at <= c.evaluation_as_of)
), orders_grouped as (
    select order_id, min(amount_cents) as candidate_expected,
           min(payment_due_at) as candidate_due,
           bool_and(coalesce(
               nullif(btrim(source_record_id), '') is not null
               and nullif(btrim(source_version), '') is not null
               and event_at is not null and source_updated_at is not null
               and order_status = 'CONFIRMED' and currency = 'USD'
               and amount_cents > 0 and payment_due_at is not null, false
           )) and count(distinct jsonb_build_array(
               order_status, amount_cents, currency, payment_due_at
           )) = 1 as order_valid
    from orders_visible where nullif(btrim(order_id), '') is not null
    group by order_id
), payment_rows as (
    select p.*, coalesce(
        nullif(btrim(p.payment_id), '') is not null
        and nullif(btrim(p.order_id), '') is not null
        and nullif(btrim(p.external_charge_ref), '') is not null
        and nullif(btrim(p.source_record_id), '') is not null
        and nullif(btrim(p.source_version), '') is not null
        and p.event_at is not null and p.source_updated_at is not null
        and p.currency = 'USD' and p.amount_cents > 0
        and p.payment_status in ('PENDING', 'SUCCEEDED', 'FAILED')
        and (p.payment_status <> 'SUCCEEDED'
             or (p.financial_effective_at is not null
                 and p.financial_effective_at <= c.evaluation_as_of)), false
    ) as row_valid
    from payments_visible p cross join context c
), payment_entities as (
    select payment_id, min(order_id) as order_id,
           min(external_charge_ref) as external_charge_ref,
           min(payment_status) as payment_status,
           bool_and(row_valid) and count(distinct jsonb_build_array(
               order_id, external_charge_ref, payment_status, amount_cents,
               currency, financial_effective_at
           )) = 1 as entity_valid
    from payment_rows where nullif(btrim(payment_id), '') is not null
    group by payment_id
), charge_states as (
    select p.external_charge_ref, min(p.order_id) as order_id,
           min(p.payment_status) as payment_status, min(p.amount_cents) as amount_cents,
           bool_and(p.row_valid and coalesce(e.entity_valid, false))
             and count(distinct jsonb_build_array(
                 p.order_id, p.payment_status, p.amount_cents,
                 p.currency, p.financial_effective_at
             )) = 1 as charge_valid
    from payment_rows p left join payment_entities e using (payment_id)
    where nullif(btrim(p.external_charge_ref), '') is not null
    group by p.external_charge_ref
), payment_order_links as (
    select p.row_id, p.order_id from payment_rows p
    where nullif(btrim(p.order_id), '') is not null
    union
    select p.row_id, q.order_id from payment_rows p join payment_rows q
      on (nullif(btrim(p.payment_id), '') is not null and p.payment_id = q.payment_id)
      or (nullif(btrim(p.external_charge_ref), '') is not null
          and p.external_charge_ref = q.external_charge_ref)
    where nullif(btrim(q.order_id), '') is not null
), payment_quality as (
    select l.order_id, bool_or(not p.row_valid
        or not coalesce(e.entity_valid, false)
        or not coalesce(s.charge_valid, false)) as payment_bad
    from payment_order_links l join payment_rows p using (row_id)
    left join payment_entities e on e.payment_id = p.payment_id
    left join charge_states s on s.external_charge_ref = p.external_charge_ref
    group by l.order_id
), payment_totals as (
    select order_id, sum(case when payment_status = 'SUCCEEDED'
                             then amount_cents::numeric else 0::numeric end) as amount
    from charge_states where charge_valid group by order_id
), refund_rows as (
    select r.*, coalesce(
        nullif(btrim(r.refund_id), '') is not null
        and nullif(btrim(r.order_id), '') is not null
        and nullif(btrim(r.source_record_id), '') is not null
        and nullif(btrim(r.source_version), '') is not null
        and r.event_at is not null and r.source_updated_at is not null
        and r.currency = 'USD' and r.amount_cents > 0
        and r.refund_status in ('PENDING', 'COMPLETED', 'FAILED')
        and (r.refund_status <> 'COMPLETED'
             or (r.financial_effective_at is not null
                 and r.financial_effective_at <= c.evaluation_as_of))
        and (r.payment_id is null or (
            nullif(btrim(r.payment_id), '') is not null and e.entity_valid
            and e.order_id = r.order_id and s.charge_valid
            and (r.refund_status <> 'COMPLETED' or e.payment_status = 'SUCCEEDED')
        )), false
    ) as row_valid
    from refunds_visible r cross join context c
    left join payment_entities e using (payment_id)
    left join charge_states s on s.external_charge_ref = e.external_charge_ref
), refund_entities as (
    select refund_id, min(order_id) as order_id, min(payment_id) as payment_id,
           min(refund_status) as refund_status, min(amount_cents) as amount_cents,
           bool_and(row_valid) and count(distinct jsonb_build_array(
               order_id, payment_id, refund_status, amount_cents,
               currency, financial_effective_at
           )) = 1 as entity_valid
    from refund_rows where nullif(btrim(refund_id), '') is not null group by refund_id
), refund_order_links as (
    select r.row_id, r.order_id from refund_rows r
    where nullif(btrim(r.order_id), '') is not null
    union
    select r.row_id, q.order_id from refund_rows r join refund_rows q
      on nullif(btrim(r.refund_id), '') is not null and r.refund_id = q.refund_id
    where nullif(btrim(q.order_id), '') is not null
    union
    select r.row_id, p.order_id from refund_rows r join payment_rows p
      on r.payment_id = p.payment_id
    where nullif(btrim(p.order_id), '') is not null
), refund_quality as (
    select l.order_id, bool_or(not r.row_valid
        or not coalesce(e.entity_valid, false)) as refund_bad
    from refund_order_links l join refund_rows r using (row_id)
    left join refund_entities e using (refund_id) group by l.order_id
), refund_totals as (
    select order_id, sum(case when refund_status = 'COMPLETED'
                             then amount_cents::numeric else 0::numeric end) as amount
    from refund_entities where entity_valid group by order_id
), linked_refund_limits as (
    -- A referenced charge cannot validate refunds larger than that charge.
    -- Unsupported cases stay UNDETERMINED; no refund mismatch rule is invented.
    select r.order_id, e.external_charge_ref,
           sum(r.amount_cents::numeric) > max(s.amount_cents)::numeric as exceeds_charge
    from refund_entities r join payment_entities e using (payment_id)
    join charge_states s using (external_charge_ref)
    where r.entity_valid and r.refund_status = 'COMPLETED'
    group by r.order_id, e.external_charge_ref
), limits_by_order as (
    select order_id, bool_or(exceeds_charge) as exceeds_charge
    from linked_refund_limits group by order_id
), values_and_quality as (
    select o.order_id, o.order_valid, o.candidate_due,
           case when o.order_valid then o.candidate_expected end as expected_cents,
           case when not coalesce(pq.payment_bad, false)
                  and coalesce(p.amount, 0) between 0 and 9223372036854775807::numeric
                then coalesce(p.amount, 0)::bigint end as payments_cents,
           case when not coalesce(rq.refund_bad, false)
                  and not coalesce(l.exceeds_charge, false)
                  and coalesce(r.amount, 0) between 0 and 9223372036854775807::numeric
                then coalesce(r.amount, 0)::bigint end as refunds_cents,
           coalesce(pq.payment_bad, false) as payment_bad,
           coalesce(rq.refund_bad, false) or coalesce(l.exceeds_charge, false) as refund_bad,
           d.ready, d.batch_evidence, c.snapshot_cutoff, c.evaluation_as_of, c.rules_version
    from orders_grouped o cross join readiness d cross join context c
    left join payment_quality pq using (order_id)
    left join payment_totals p using (order_id)
    left join refund_quality rq using (order_id)
    left join refund_totals r using (order_id)
    left join limits_by_order l using (order_id)
), classified as (
    select v.*, case
        when not ready or rules_version <> 'm1-thin-v1' or not order_valid
          or payments_cents is null or refunds_cents is null then 'UNDETERMINED'
        when payments_cents = expected_cents and refunds_cents >= 0
          and refunds_cents < payments_cents then 'MATCHED'
        when payments_cents = 0 and refunds_cents = 0
          and evaluation_as_of > candidate_due then 'EXCEPTION'
        when payments_cents = 0 and refunds_cents = 0
          and evaluation_as_of <= candidate_due then 'PENDING'
        else 'UNDETERMINED'
    end as overall_status
    from values_and_quality v
), all_lineage as (
    select o.order_id, 'orders'::text as source_name, o.row_id,
           jsonb_build_object('source_name', 'orders', 'row_id', o.row_id,
             'batch_id', o.batch_id, 'source_row_number', o.source_row_number,
             'source_record_id', o.source_record_id, 'source_version', o.source_version,
             'order_id', o.order_id) as evidence
    from orders_visible o where nullif(btrim(o.order_id), '') is not null
    union all
    select l.order_id, 'payments', p.row_id,
           jsonb_build_object('source_name', 'payments', 'row_id', p.row_id,
             'batch_id', p.batch_id, 'source_row_number', p.source_row_number,
             'source_record_id', p.source_record_id, 'source_version', p.source_version,
             'order_id', p.order_id, 'payment_id', p.payment_id,
             'external_charge_ref', p.external_charge_ref)
    from payment_order_links l join payment_rows p using (row_id)
    union all
    select l.order_id, 'refunds', r.row_id,
           jsonb_build_object('source_name', 'refunds', 'row_id', r.row_id,
             'batch_id', r.batch_id, 'source_row_number', r.source_row_number,
             'source_record_id', r.source_record_id, 'source_version', r.source_version,
             'order_id', r.order_id, 'refund_id', r.refund_id, 'payment_id', r.payment_id)
    from refund_order_links l join refund_rows r using (row_id)
), lineage as (
    select order_id, jsonb_agg(evidence order by source_name, row_id) as evidence_refs
    from all_lineage group by order_id
)
select c.order_id, c.expected_cents, c.payments_cents, c.refunds_cents,
       case when c.payments_cents is not null and c.refunds_cents is not null
            then c.payments_cents - c.refunds_cents end::bigint as net_cents,
       c.overall_status,
       case when c.overall_status = 'EXCEPTION' then 'MISSING_PAYMENT' end::text as finding_reason,
       coalesce(l.evidence_refs, '[]'::jsonb) as evidence_refs,
       jsonb_build_object(
           'rules_version', c.rules_version, 'snapshot_cutoff', c.snapshot_cutoff,
           'evaluation_as_of', to_char(c.evaluation_as_of at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
           'source_ready', c.ready, 'source_batches', c.batch_evidence,
           'order_valid', c.order_valid, 'payment_ambiguous_or_invalid', c.payment_bad,
           'refund_ambiguous_or_invalid', c.refund_bad,
           'payment_sum_out_of_range', c.payments_cents is null and not c.payment_bad,
           'refund_sum_out_of_range', c.refunds_cents is null and not c.refund_bad,
           'payment_due_at', c.candidate_due, 'bounded_classification', true
       ) as details
from classified c left join lineage l using (order_id)
order by c.order_id
