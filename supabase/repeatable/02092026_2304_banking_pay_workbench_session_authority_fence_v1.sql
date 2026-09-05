-- One Workbench-session authority fence for certificate build, seal and Draft admission.
-- Metadata-only timestamps are deliberately excluded: they cannot change who/what is payable.

ALTER TABLE public.banking_pay_workbench_sessions
  ADD COLUMN IF NOT EXISTS authority_fence_generation bigint NOT NULL DEFAULT 1;

DO $verification$
DECLARE
  v_type text;
  v_not_null boolean;
  v_default text;
BEGIN
  SELECT pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
         attribute.attnotnull,
         pg_catalog.pg_get_expr(default_value.adbin, default_value.adrelid)
  INTO v_type, v_not_null, v_default
  FROM pg_catalog.pg_attribute attribute
  LEFT JOIN pg_catalog.pg_attrdef default_value
    ON default_value.adrelid = attribute.attrelid
   AND default_value.adnum = attribute.attnum
  WHERE attribute.attrelid = 'public.banking_pay_workbench_sessions'::pg_catalog.regclass
    AND attribute.attname = 'authority_fence_generation'
    AND NOT attribute.attisdropped;
  IF v_type IS DISTINCT FROM 'bigint'
     OR v_not_null IS NOT TRUE
     OR v_default IS DISTINCT FROM '1' THEN
    RAISE EXCEPTION 'WORKBENCH_AUTHORITY_FENCE_COLUMN_CONTRACT_INVALID';
  END IF;
END;
$verification$;

DO $constraint$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_constraint constraint_row
    WHERE constraint_row.conrelid = 'public.banking_pay_workbench_sessions'::pg_catalog.regclass
      AND constraint_row.conname = 'banking_pay_workbench_sessions_authority_fence_generation_chk'
  ) THEN
    ALTER TABLE public.banking_pay_workbench_sessions
      ADD CONSTRAINT banking_pay_workbench_sessions_authority_fence_generation_chk
      CHECK (authority_fence_generation >= 1);
  END IF;
END;
$constraint$;

CREATE OR REPLACE FUNCTION private.pay_workbench_session_authority_fence_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = ''
AS $function$
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.authority_fence_generation := 1;
  ELSE
    NEW.authority_fence_generation := OLD.authority_fence_generation + 1;
  END IF;
  RETURN NEW;
END;
$function$;

ALTER FUNCTION private.pay_workbench_session_authority_fence_v1() OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_session_authority_fence_v1()
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_session_authority_fence_v1() TO postgres;

DROP TRIGGER IF EXISTS banking_pay_workbench_session_authority_fence_insert_v1
  ON public.banking_pay_workbench_sessions;
CREATE TRIGGER banking_pay_workbench_session_authority_fence_insert_v1
BEFORE INSERT ON public.banking_pay_workbench_sessions
FOR EACH ROW
EXECUTE FUNCTION private.pay_workbench_session_authority_fence_v1();

DROP TRIGGER IF EXISTS banking_pay_workbench_session_authority_fence_update_v1
  ON public.banking_pay_workbench_sessions;
CREATE TRIGGER banking_pay_workbench_session_authority_fence_update_v1
BEFORE UPDATE OF
  actor_user_id,
  pay_date,
  week_ending_cutoff,
  filters_json,
  session_signature,
  source_snapshot_run_id,
  status,
  version,
  server_selected_preview_row_ids,
  discarded_at_utc,
  server_selected_preview_row_ids_provided,
  scope_seed_complete,
  scope_total_count,
  scope_seeded_count,
  scope_ready_count,
  scope_pending_count,
  scope_failed_count,
  line_units_total,
  line_units_ready,
  line_units_pending,
  line_units_failed,
  preview_row_count,
  selected_row_count,
  section_counts_json,
  progress_state,
  progress_json,
  progress_counter_version,
  scope_candidate_ids,
  replacement_session_id,
  replacement_idempotency_key,
  scope_change_generation_target,
  scope_change_generation_applied,
  scope_change_generation_shadow_checked
ON public.banking_pay_workbench_sessions
FOR EACH ROW
EXECUTE FUNCTION private.pay_workbench_session_authority_fence_v1();

-- The shared wrapper is the only service-callable session-open boundary.
REVOKE EXECUTE ON FUNCTION public.pay_workbench_session_open(
  uuid,date,date,jsonb,text,boolean,boolean,uuid,jsonb,text,jsonb,jsonb,jsonb
) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_open_shared_v2(uuid,date,date,jsonb,text)
  TO service_role;

NOTIFY pgrst, 'reload schema';
