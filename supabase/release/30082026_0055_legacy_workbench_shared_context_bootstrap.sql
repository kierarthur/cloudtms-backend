\set ON_ERROR_STOP on

BEGIN;

SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '2min';

DO $$
DECLARE
    v_required_column_count integer;
    v_duplicate_group_count bigint;
BEGIN
    IF to_regclass('public.banking_pay_workbench_sessions') IS NULL THEN
        RAISE EXCEPTION
            'Required legacy table public.banking_pay_workbench_sessions does not exist; refusing shared-context index bootstrap';
    END IF;

    SELECT count(*)::integer
    INTO v_required_column_count
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'banking_pay_workbench_sessions'
      AND column_name IN (
          'session_signature',
          'pay_date',
          'week_ending_cutoff',
          'status',
          'discarded_at_utc'
      );

    IF v_required_column_count <> 5 THEN
        RAISE EXCEPTION
            'Legacy workbench table is missing one or more shared-context index columns; refusing bootstrap';
    END IF;

    IF to_regclass('public.ux_bpay_workbench_sessions_shared_context_open') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_index AS i
           JOIN pg_class AS idx
             ON idx.oid = i.indexrelid
           JOIN pg_class AS tbl
             ON tbl.oid = i.indrelid
           JOIN pg_namespace AS ns
             ON ns.oid = tbl.relnamespace
           WHERE ns.nspname = 'public'
             AND tbl.relname = 'banking_pay_workbench_sessions'
             AND idx.relname = 'ux_bpay_workbench_sessions_shared_context_open'
             AND i.indisunique = true
             AND i.indisvalid = true
             AND pg_get_indexdef(i.indexrelid, 1, true) = 'session_signature'
             AND pg_get_indexdef(i.indexrelid, 2, true) = 'pay_date'
             AND pg_get_indexdef(i.indexrelid, 3, true) = 'week_ending_cutoff'
             AND regexp_replace(
                   pg_get_expr(i.indpred, i.indrelid, true),
                   '[[:space:]()]',
                   '',
                   'g'
                 ) = 'status=''OPEN''::textANDdiscarded_at_utcISNULL'
       ) THEN
        RAISE EXCEPTION
            'Existing public.ux_bpay_workbench_sessions_shared_context_open has an unexpected definition; refusing bootstrap';
    END IF;

    SELECT count(*)
    INTO v_duplicate_group_count
    FROM (
        SELECT 1
        FROM public.banking_pay_workbench_sessions
        WHERE status = 'OPEN'
          AND discarded_at_utc IS NULL
        GROUP BY session_signature, pay_date, week_ending_cutoff
        HAVING count(*) > 1
    ) AS duplicate_groups;

    IF v_duplicate_group_count <> 0 THEN
        RAISE EXCEPTION
            'Legacy workbench contains % duplicate open shared-context groups; refusing unique-index bootstrap',
            v_duplicate_group_count;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_bpay_workbench_sessions_shared_context_open
    ON public.banking_pay_workbench_sessions (
        session_signature,
        pay_date,
        week_ending_cutoff
    )
    WHERE status = 'OPEN'
      AND discarded_at_utc IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_index AS i
        JOIN pg_class AS idx
          ON idx.oid = i.indexrelid
        JOIN pg_class AS tbl
          ON tbl.oid = i.indrelid
        JOIN pg_namespace AS ns
          ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'public'
          AND tbl.relname = 'banking_pay_workbench_sessions'
          AND idx.relname = 'ux_bpay_workbench_sessions_shared_context_open'
          AND i.indisunique = true
          AND i.indisvalid = true
          AND pg_get_indexdef(i.indexrelid, 1, true) = 'session_signature'
          AND pg_get_indexdef(i.indexrelid, 2, true) = 'pay_date'
          AND pg_get_indexdef(i.indexrelid, 3, true) = 'week_ending_cutoff'
          AND regexp_replace(
                pg_get_expr(i.indpred, i.indrelid, true),
                '[[:space:]()]',
                '',
                'g'
              ) = 'status=''OPEN''::textANDdiscarded_at_utcISNULL'
    ) THEN
        RAISE EXCEPTION
            'Shared-context unique index is missing or invalid after legacy bootstrap';
    END IF;
END $$;

COMMIT;
