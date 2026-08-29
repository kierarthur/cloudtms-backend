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
SET plpgsql_check.mode TO 'disabled'
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
  v_empty_finalize jsonb;
  v_capture_mode boolean:=lower(COALESCE(
    pg_catalog.current_setting('cloudtms.pay_workbench_effect_capture_mode',true),''
  ))='capture';
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

  IF jsonb_typeof(v_result)='object'
     AND COALESCE((v_result->>'authoritative_timesheet_scope')::boolean,false)
     AND COALESCE((v_result->>'explicit_empty_timesheet_scope')::boolean,false)
     AND v_result->>'preview_scope_strategy'='AUTHORITATIVE_EMPTY'
     AND jsonb_typeof(v_result->'scope_timesheet_ids')='array'
     AND jsonb_array_length(v_result->'scope_timesheet_ids')=0 THEN
    IF v_capture_mode THEN
      RETURN v_result||jsonb_build_object(
        'effect_plan_capture',true,
        'captured_effects','[]'::jsonb,
        '_internal_reconcile_timing','{}'::jsonb
      );
    END IF;
    v_empty_finalize:=private.pay_workbench_reconcile_empty_scope_v1(
      v_job_id,v_build_id,v_attempt_id,v_attempt_nonce
    );
    IF jsonb_typeof(v_empty_finalize) IS DISTINCT FROM 'object'
       OR COALESCE((v_empty_finalize->>'ok')::boolean,false) IS NOT TRUE THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_EMPTY_SCOPE_RECONCILIATION_INVALID'
        USING ERRCODE='23514';
    END IF;
    RETURN v_result;
  END IF;

  IF jsonb_typeof(v_result) IS DISTINCT FROM 'object'
     OR jsonb_typeof(v_result->'public_result_json') IS DISTINCT FROM 'object' THEN
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
