BEGIN;

SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '2min';

DO $$
BEGIN
    IF to_regclass('public.banking_pay_workbench_sessions') IS NULL THEN
        RAISE EXCEPTION
            'Required table public.banking_pay_workbench_sessions does not exist; refusing to drop workbench session index';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_index i
        JOIN pg_class idx
            ON idx.oid = i.indexrelid
        JOIN pg_class tbl
            ON tbl.oid = i.indrelid
        JOIN pg_namespace ns
            ON ns.oid = tbl.relnamespace
        WHERE ns.nspname = 'public'
          AND tbl.relname = 'banking_pay_workbench_sessions'
          AND idx.relname = 'ux_bpay_workbench_sessions_shared_context_open'
          AND i.indisunique = true
          AND i.indisvalid = true
    ) THEN
        RAISE EXCEPTION
            'Required shared-context unique index public.ux_bpay_workbench_sessions_shared_context_open is missing or invalid; refusing to drop actor-scoped index';
    END IF;
END $$;

DROP INDEX IF EXISTS public.ux_bpay_workbench_sessions_actor_signature_open;

DO $$
BEGIN
    IF to_regclass('public.ux_bpay_workbench_sessions_actor_signature_open') IS NOT NULL THEN
        RAISE EXCEPTION
            'Index public.ux_bpay_workbench_sessions_actor_signature_open still exists after DROP INDEX';
    END IF;

    IF to_regclass('public.ux_bpay_workbench_sessions_shared_context_open') IS NULL THEN
        RAISE EXCEPTION
            'Shared-context index public.ux_bpay_workbench_sessions_shared_context_open is missing after migration';
    END IF;
END $$;

COMMIT;