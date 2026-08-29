BEGIN;

DO $$
DECLARE
    v_column_exists boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns AS c
        WHERE c.table_schema = 'public'
          AND c.table_name = 'banking_pay_workbench_sessions'
          AND c.column_name = 'server_selected_preview_row_ids_provided'
    )
    INTO v_column_exists;

    IF NOT v_column_exists THEN
        ALTER TABLE public.banking_pay_workbench_sessions
            ADD COLUMN server_selected_preview_row_ids_provided boolean;

        UPDATE public.banking_pay_workbench_sessions AS s
        SET server_selected_preview_row_ids_provided =
            CASE
                WHEN jsonb_typeof(s.server_selected_preview_row_ids) = 'array'
                 AND jsonb_array_length(s.server_selected_preview_row_ids) > 0
                THEN true
                ELSE false
            END;

        ALTER TABLE public.banking_pay_workbench_sessions
            ALTER COLUMN server_selected_preview_row_ids_provided SET DEFAULT false,
            ALTER COLUMN server_selected_preview_row_ids_provided SET NOT NULL;
    END IF;
END
$$;

COMMIT;
