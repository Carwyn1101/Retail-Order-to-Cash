\set ON_ERROR_STOP on
-- Native PostgreSQL assertions against independently specified expected values.
-- All fixtures are synthetic and rolled back. Identity sequences may acquire harmless gaps.
BEGIN;
SET LOCAL TIME ZONE 'UTC';

DO $smoke$
DECLARE
    prefix TEXT := 'synthetic-m1-t01-' || txid_current()::TEXT;
    batch TEXT;
    first_sequence BIGINT;
    table_name TEXT;
    id_column TEXT;
    status_column TEXT;
    expected_constraint TEXT;
    observed_constraint TEXT;
    observed_count BIGINT;
    observed_tables TEXT[];
    observed_payload TEXT;
    observed_amount TEXT;
    observed_version TEXT;
    bad_payload CONSTANT TEXT := E'{not-json: amount="001.2300",event_at="not-a-date"}\n';
    expected_tables CONSTANT TEXT[] := ARRAY[
        'bronze.input_batches', 'bronze.orders_source', 'bronze.payments_source',
        'bronze.refunds_source', 'core.dq_findings', 'core.evaluation_runs',
        'core.reconciliation_findings', 'core.reconciliation_results'
    ];
BEGIN
    SELECT array_agg(schemaname || '.' || tablename ORDER BY schemaname, tablename)
      INTO observed_tables
      FROM pg_tables
     WHERE schemaname !~ '^pg_' AND schemaname <> 'information_schema';
    IF observed_tables IS DISTINCT FROM expected_tables THEN
        RAISE EXCEPTION 'T01-05 expected exactly %, observed %', expected_tables, observed_tables;
    END IF;
    RAISE NOTICE 'T01-05 PASS expected exactly 8 approved tables; observed %', observed_tables;

    -- Three source batches, plus a committed empty readiness declaration.
    FOREACH table_name IN ARRAY ARRAY['orders', 'payments', 'refunds'] LOOP
        INSERT INTO bronze.input_batches
            (batch_id, source_name, content_digest, input_reference, row_count, status, scope_complete)
        VALUES (prefix || '-' || table_name, table_name, 'synthetic-digest',
                'synthetic://schema-smoke/' || table_name, 3, 'REGISTERED', FALSE);
    END LOOP;
    INSERT INTO bronze.input_batches
        (batch_id, source_name, content_digest, input_reference, row_count, status,
         scope_complete, committed_at)
    VALUES (prefix || '-empty', 'payments', 'synthetic-empty-digest',
            'synthetic://schema-smoke/empty', 0, 'COMMITTED', TRUE, '2026-01-01T00:00:00Z');
    SELECT count(*) INTO observed_count FROM bronze.input_batches
     WHERE batch_id = prefix || '-empty' AND row_count = 0 AND scope_complete
       AND status = 'COMMITTED' AND committed_at IS NOT NULL;
    IF observed_count <> 1 THEN RAISE EXCEPTION 'Committed zero-row readiness lost'; END IF;
    RAISE NOTICE 'Batch boundary PASS committed zero-row scope_complete batch retained';

    SELECT batch_sequence INTO first_sequence FROM bronze.input_batches
     WHERE batch_id = prefix || '-orders';
    BEGIN
        INSERT INTO bronze.input_batches
            (batch_id, source_name, content_digest, input_reference, row_count)
        VALUES (prefix || '-orders', 'orders', 'synthetic', 'synthetic://duplicate-id', 0);
        RAISE EXCEPTION 'Duplicate batch_id unexpectedly accepted';
    EXCEPTION WHEN unique_violation THEN
        GET STACKED DIAGNOSTICS observed_constraint = CONSTRAINT_NAME;
        IF observed_constraint <> 'input_batches_pkey' THEN RAISE; END IF;
        RAISE NOTICE 'Batch PK PASS expected/observed SQLSTATE %, constraint %', SQLSTATE, observed_constraint;
    END;

    BEGIN
        INSERT INTO bronze.input_batches
            (batch_id, source_name, batch_sequence, content_digest, input_reference, row_count)
        VALUES (prefix || '-duplicate-sequence', 'refunds', first_sequence,
                'synthetic', 'synthetic://duplicate-sequence', 0);
        RAISE EXCEPTION 'T01-07 duplicate batch_sequence unexpectedly accepted';
    EXCEPTION WHEN unique_violation THEN
        GET STACKED DIAGNOSTICS observed_constraint = CONSTRAINT_NAME;
        IF observed_constraint <> 'input_batches_batch_sequence_key' THEN RAISE; END IF;
        RAISE NOTICE 'T01-07 PASS expected/observed SQLSTATE %, constraint %', SQLSTATE, observed_constraint;
    END;

    FOREACH table_name IN ARRAY ARRAY['orders', 'payments', 'refunds'] LOOP
        batch := prefix || '-' || table_name;
        id_column := CASE table_name WHEN 'orders' THEN 'order_id'
                     WHEN 'payments' THEN 'payment_id' ELSE 'refund_id' END;
        status_column := CASE table_name WHEN 'orders' THEN 'order_status'
                         WHEN 'payments' THEN 'payment_status' ELSE 'refund_status' END;
        -- Same business identity and same/conflicting version all remain representable.
        EXECUTE format(
            'INSERT INTO bronze.%I (batch_id, source_row_number, %I, source_record_id,
             source_version, %I, amount_raw, raw_payload)
             VALUES ($1,1,$2,$3,''001'',''UNRECOGNIZED'',''001.2300'',$4),
                    ($1,2,$2,$3,''001'',NULL,''not-money'',$4),
                    ($1,3,$2,$3,''malformed-version'',''UNRECOGNIZED'',NULL,$4)',
            table_name || '_source', id_column, status_column
        ) USING batch, prefix || '-same-business-id', prefix || '-same-record-id', bad_payload;

        BEGIN
            EXECUTE format('INSERT INTO bronze.%I (batch_id, source_row_number, raw_payload)
                            VALUES ($1,1,''synthetic duplicate occurrence'')', table_name || '_source')
                USING batch;
            RAISE EXCEPTION 'T01-06 duplicate occurrence unexpectedly accepted in %', table_name;
        EXCEPTION WHEN unique_violation THEN
            GET STACKED DIAGNOSTICS observed_constraint = CONSTRAINT_NAME;
            expected_constraint := table_name || '_source_batch_id_source_row_number_key';
            IF observed_constraint <> expected_constraint THEN RAISE; END IF;
            RAISE NOTICE 'T01-06 PASS % expected/observed SQLSTATE %, constraint %',
                table_name, SQLSTATE, observed_constraint;
        END;

        EXECUTE format('SELECT count(*) FROM bronze.%I WHERE batch_id=$1 AND %I=$2',
                       table_name || '_source', id_column)
            INTO observed_count USING batch, prefix || '-same-business-id';
        IF observed_count <> 3 THEN
            RAISE EXCEPTION 'T01-08 % expected 3 same-ID occurrences, observed %', table_name, observed_count;
        END IF;
        RAISE NOTICE 'T01-08 PASS % expected/observed 3 same-business-ID occurrences', table_name;

        EXECUTE format('SELECT raw_payload, amount_raw, source_version FROM bronze.%I
                        WHERE batch_id=$1 AND source_row_number=1', table_name || '_source')
            INTO observed_payload, observed_amount, observed_version USING batch;
        IF observed_payload IS DISTINCT FROM bad_payload OR observed_amount IS DISTINCT FROM '001.2300'
            OR observed_version IS DISTINCT FROM '001' THEN
            RAISE EXCEPTION 'T01-09 % raw representations changed', table_name;
        END IF;
        EXECUTE format('SELECT count(*) FROM bronze.%I WHERE batch_id=$1 AND
                        amount_cents IS NULL AND event_at IS NULL AND source_updated_at IS NULL
                        AND (%I IS NULL OR %I=''UNRECOGNIZED'')',
                        table_name || '_source', status_column, status_column)
            INTO observed_count USING batch;
        IF observed_count <> 3 THEN RAISE EXCEPTION 'T01-09 % lost malformed evidence', table_name; END IF;
        RAISE NOTICE 'T01-09 PASS % lossless malformed payload/amount, unknown/missing status, NULL parsed fields', table_name;

        -- Technical provenance FK exists even though business FKs intentionally do not.
        BEGIN
            EXECUTE format('INSERT INTO bronze.%I (batch_id, source_row_number, raw_payload)
                            VALUES ($1,1,''synthetic missing batch'')', table_name || '_source')
                USING prefix || '-nonexistent-batch';
            RAISE EXCEPTION 'Missing technical batch unexpectedly accepted in %', table_name;
        EXCEPTION WHEN foreign_key_violation THEN
            RAISE NOTICE 'Technical batch FK PASS % expected/observed SQLSTATE %', table_name, SQLSTATE;
        END;
    END LOOP;

    UPDATE bronze.payments_source SET order_id = prefix || '-orphan-order',
        external_charge_ref = 'same-synthetic-charge', financial_effective_at = NULL
     WHERE batch_id = prefix || '-payments';
    UPDATE bronze.refunds_source SET order_id = prefix || '-orphan-order',
        payment_id = prefix || '-orphan-payment', financial_effective_at = NULL
     WHERE batch_id = prefix || '-refunds' AND source_row_number <> 3;
    SELECT count(*) INTO observed_count FROM bronze.refunds_source
     WHERE batch_id = prefix || '-refunds' AND payment_id IS NULL;
    IF observed_count <> 1 THEN RAISE EXCEPTION 'Optional refund payment_id not preserved'; END IF;
    RAISE NOTICE 'T01-09 PASS orphan order/payment references and optional refund payment_id accepted';

    SELECT count(*) INTO observed_count
      FROM (VALUES ('bronze','orders_source','amount_cents'),
                   ('bronze','payments_source','amount_cents'),
                   ('bronze','refunds_source','amount_cents'),
                   ('core','reconciliation_results','expected_cents'),
                   ('core','reconciliation_results','payments_cents'),
                   ('core','reconciliation_results','refunds_cents'),
                   ('core','reconciliation_results','net_cents')) AS expected(s,t,c)
      JOIN information_schema.columns actual
        ON actual.table_schema=expected.s AND actual.table_name=expected.t AND actual.column_name=expected.c
     WHERE actual.data_type='bigint' AND actual.is_nullable='YES' AND actual.column_default IS NULL;
    IF observed_count <> 7 THEN RAISE EXCEPTION 'T01-10 expected 7 nullable BIGINT money columns without defaults, observed %', observed_count; END IF;
    RAISE NOTICE 'T01-10 PASS expected/observed 7 nullable BIGINT monetary columns without zero defaults';

    SELECT count(*) INTO observed_count FROM information_schema.columns
     WHERE table_schema IN ('bronze','core')
       AND column_name IN ('event_at','source_updated_at','financial_effective_at','payment_due_at',
                           'ingested_at','registered_at','committed_at','evaluation_as_of','started_at','finished_at')
       AND data_type <> 'timestamp with time zone';
    IF observed_count <> 0 THEN RAISE EXCEPTION 'Validated timestamps must all use TIMESTAMPTZ'; END IF;
    RAISE NOTICE 'Timestamp types PASS all declared temporal fields are TIMESTAMPTZ';

    INSERT INTO core.reconciliation_results (evaluation_key, order_id, overall_status)
    VALUES (prefix, prefix || '-order', 'UNDETERMINED');
    SELECT count(*) INTO observed_count FROM core.reconciliation_results
     WHERE evaluation_key=prefix AND expected_cents IS NULL AND payments_cents IS NULL
       AND refunds_cents IS NULL AND net_cents IS NULL;
    IF observed_count <> 1 THEN RAISE EXCEPTION 'Unknown result money must remain NULL'; END IF;
    BEGIN
        INSERT INTO core.reconciliation_results (evaluation_key,order_id,overall_status)
        VALUES (prefix,prefix || '-order','MATCHED');
        RAISE EXCEPTION 'T01-11 duplicate result key accepted';
    EXCEPTION WHEN unique_violation THEN
        GET STACKED DIAGNOSTICS observed_constraint=CONSTRAINT_NAME;
        IF observed_constraint <> 'reconciliation_results_pkey' THEN RAISE; END IF;
        RAISE NOTICE 'T01-11 PASS duplicate result expected/observed SQLSTATE %, constraint %', SQLSTATE, observed_constraint;
    END;

    INSERT INTO core.reconciliation_findings (evaluation_key,order_id,reason_code,evidence_refs,details)
    VALUES (prefix,prefix || '-order','MISSING_PAYMENT','[]','{"synthetic":true}'),
           (prefix,prefix || '-order','DUPLICATE_PAYMENT','[]','{"synthetic":true}'),
           (prefix,prefix || '-order','AMOUNT_MISMATCH','[]','{"synthetic":true}'),
           (prefix,prefix || '-order','REFUND_MISMATCH','[]','{"synthetic":true}');
    BEGIN
        INSERT INTO core.reconciliation_findings (evaluation_key,order_id,reason_code,evidence_refs,details)
        VALUES (prefix,prefix || '-order','MISSING_PAYMENT','[]','{"synthetic":true}');
        RAISE EXCEPTION 'T01-11 duplicate finding key accepted';
    EXCEPTION WHEN unique_violation THEN
        GET STACKED DIAGNOSTICS observed_constraint=CONSTRAINT_NAME;
        IF observed_constraint <> 'reconciliation_findings_pkey' THEN RAISE; END IF;
        RAISE NOTICE 'T01-11 PASS duplicate finding expected/observed SQLSTATE %, constraint %', SQLSTATE, observed_constraint;
    END;
    SELECT count(*) INTO observed_count FROM core.reconciliation_findings WHERE evaluation_key=prefix;
    IF observed_count <> 4 THEN RAISE EXCEPTION 'T01-11 expected 4 distinct reasons, observed %', observed_count; END IF;
    RAISE NOTICE 'T01-11 PASS expected/observed 4 distinct reasons on one order';

    INSERT INTO core.dq_findings (evaluation_key,finding_key,source_name,dq_code,details)
    VALUES (prefix,'synthetic-unassignable','orders','SYNTHETIC_SMOKE','{"synthetic":true}');
    BEGIN
        INSERT INTO core.dq_findings (evaluation_key,finding_key,source_name,dq_code,details)
        VALUES (prefix,'synthetic-unassignable','orders','SYNTHETIC_SMOKE','{"synthetic":true}');
        RAISE EXCEPTION 'Duplicate DQ key accepted';
    EXCEPTION WHEN unique_violation THEN
        GET STACKED DIAGNOSTICS observed_constraint=CONSTRAINT_NAME;
        IF observed_constraint <> 'dq_findings_pkey' THEN RAISE; END IF;
        RAISE NOTICE 'DQ key PASS expected/observed SQLSTATE %, NULL source_row_id/batch_id accepted', SQLSTATE;
    END;

    BEGIN
        INSERT INTO core.reconciliation_results (evaluation_key,order_id,overall_status)
        VALUES (prefix,prefix || '-bad-status','UNKNOWN');
        RAISE EXCEPTION 'Unknown processed result status accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'Processed status constraint PASS expected/observed SQLSTATE %', SQLSTATE;
    END;
    BEGIN
        INSERT INTO core.reconciliation_findings (evaluation_key,order_id,reason_code,evidence_refs,details)
        VALUES (prefix,prefix || '-order','UNAPPROVED_REASON','[]','{"synthetic":true}');
        RAISE EXCEPTION 'Unknown processed reason accepted';
    EXCEPTION WHEN check_violation THEN
        RAISE NOTICE 'Processed reason constraint PASS expected/observed SQLSTATE %', SQLSTATE;
    END;

    INSERT INTO core.evaluation_runs (run_id,evaluation_key,snapshot_cutoff,evaluation_as_of,rules_version,status)
    VALUES (prefix || '-attempt-1',prefix,first_sequence,'2026-01-01T00:00:00Z','synthetic-v1','FAILED'),
           (prefix || '-attempt-2',prefix,first_sequence,'2026-01-01T00:00:00Z','synthetic-v1','SUCCEEDED');
    SELECT count(*) INTO observed_count FROM core.evaluation_runs WHERE evaluation_key=prefix;
    IF observed_count <> 2 THEN RAISE EXCEPTION 'T01-12 expected 2 attempts, observed %', observed_count; END IF;
    RAISE NOTICE 'T01-12 PASS expected/observed 2 run attempts sharing one stable evaluation key';
    RAISE NOTICE 'DEFERRED: conflicting evaluation-key context reuse and committed-batch immutability are future writer invariants, not proven here';
END
$smoke$;

ROLLBACK;
\echo 'Schema smoke completed; all synthetic fixture rows rolled back.'
