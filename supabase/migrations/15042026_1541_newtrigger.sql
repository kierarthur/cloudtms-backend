BEGIN;

DO $$
DECLARE
  v_table_oid oid;
  v_function_oid oid;
  v_trigger_exists boolean;
BEGIN
  -- Verify target table exists
  SELECT c.oid
  INTO v_table_oid
  FROM pg_class c
  JOIN pg_namespace n
    ON n.oid = c.relnamespace
  WHERE n.nspname = 'public'
    AND c.relname = 'timesheet_pay_state'
    AND c.relkind = 'r';

  IF v_table_oid IS NULL THEN
    RAISE EXCEPTION 'Table public.timesheet_pay_state does not exist';
  END IF;

  -- Verify trigger function exists with no args
  SELECT p.oid
  INTO v_function_oid
  FROM pg_proc p
  JOIN pg_namespace n
    ON n.oid = p.pronamespace
  WHERE n.nspname = 'public'
    AND p.proname = 'pay_workbench_mark_candidate_dirty'
    AND pg_get_function_identity_arguments(p.oid) = '';

  IF v_function_oid IS NULL THEN
    RAISE EXCEPTION 'Function public.pay_workbench_mark_candidate_dirty() does not exist';
  END IF;

  -- Check whether the trigger already exists on this table
  SELECT EXISTS (
    SELECT 1
    FROM pg_trigger t
    WHERE t.tgrelid = v_table_oid
      AND t.tgname = 'trg_pay_workbench_mark_candidate_dirty__timesheet_pay_state'
      AND NOT t.tgisinternal
  )
  INTO v_trigger_exists;

  -- Create only if missing
  IF NOT v_trigger_exists THEN
    EXECUTE $sql$
      CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__timesheet_pay_state
      AFTER INSERT OR UPDATE OR DELETE
      ON public.timesheet_pay_state
      FOR EACH ROW
      EXECUTE FUNCTION public.pay_workbench_mark_candidate_dirty()
    $sql$;
  END IF;
END
$$;

COMMIT;
