-- Banking Pay bounded-scope V1.2.4: preserve the installed public sync contract
-- while requiring one exact owner-created Workbench build/attempt context.

CREATE OR REPLACE FUNCTION public.pay_sync_overpayments_from_preview(
  p_pay_date date,
  p_week_ending_cutoff date,
  p_actor_user_id uuid,
  p_pay_channel_scope text,
  p_candidate_ids uuid[],
  p_mismatch_choices jsonb DEFAULT '{}'::jsonb,
  p_client_filter_single uuid DEFAULT NULL::uuid,
  p_force_include_timesheet_ids uuid[] DEFAULT NULL::uuid[],
  p_exclude_timesheet_ids uuid[] DEFAULT NULL::uuid[]
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_context regclass:=pg_catalog.to_regclass('pg_temp._bpay_wb_sync_context_v1');
  v_owner oid;
  v_context_count integer;
  v_job_id uuid;
  v_build_id uuid;
  v_attempt_id uuid;
  v_attempt_nonce uuid;
  v_result jsonb;
BEGIN
  IF v_context IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_BUILD_REQUIRED' USING ERRCODE='42501';
  END IF;
  SELECT relowner INTO v_owner FROM pg_catalog.pg_class WHERE oid=v_context;
  IF v_owner IS DISTINCT FROM current_user::regrole::oid THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_BUILD_REQUIRED' USING ERRCODE='42501';
  END IF;
  SELECT count(*) INTO v_context_count FROM pg_catalog.pg_attribute
  WHERE attrelid=v_context AND attnum>0 AND NOT attisdropped;
  IF v_context_count<>10 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_BUILD_REQUIRED' USING ERRCODE='42501';
  END IF;

  SELECT context.job_id,context.build_id,context.attempt_id,context.attempt_nonce
  INTO v_job_id,v_build_id,v_attempt_id,v_attempt_nonce
  FROM pg_temp._bpay_wb_sync_context_v1 AS context;
  IF NOT FOUND OR EXISTS(SELECT 1 FROM pg_temp._bpay_wb_sync_context_v1 OFFSET 1) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_BUILD_REQUIRED' USING ERRCODE='42501';
  END IF;

  v_result:=private.pay_sync_overpayments_from_workbench_workspace_v1(
    v_build_id,v_job_id,v_attempt_id,v_attempt_nonce,
    p_pay_date,p_week_ending_cutoff,p_actor_user_id,p_pay_channel_scope,
    p_candidate_ids,p_mismatch_choices,p_client_filter_single,
    p_force_include_timesheet_ids,p_exclude_timesheet_ids
  );
  IF jsonb_typeof(v_result)<>'object' OR jsonb_typeof(v_result->'public_result_json')<>'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECONCILIATION_RESULT_INVALID' USING ERRCODE='22023';
  END IF;
  RETURN v_result->'public_result_json';
END;
$function$;

ALTER FUNCTION public.pay_sync_overpayments_from_preview(
  date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[]
) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_sync_overpayments_from_preview(
  date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[]
) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.pay_sync_overpayments_from_preview(
  date,date,uuid,text,uuid[],jsonb,uuid,uuid[],uuid[]
) TO postgres,service_role;
