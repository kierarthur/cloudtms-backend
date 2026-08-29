-- Read-only presentation adapter over both existing Workbench readiness owners.
-- No Draft is constructed. Existing Draft preparation and UI security/review
-- checks remain mandatory; this is not another eligibility calculation.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_draft_gate_v2(
  p_session_id uuid, p_selected_ready_count bigint
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_progress jsonb; v_scope jsonb; v_blockers jsonb; v_gate jsonb;
BEGIN
  IF p_session_id IS NULL OR p_selected_ready_count IS NULL OR p_selected_ready_count<0 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  v_progress:=public.pay_workbench_session_recompute_progress_counters(
    p_session_id, false, 'BANKING_PAY_MODAL_STRUCTURE_V2', false
  );
  IF v_progress->>'code'='WORKBENCH_SESSION_NOT_FOUND' THEN
    RAISE EXCEPTION 'WORKBENCH_SESSION_NOT_FOUND' USING ERRCODE='P0001';
  END IF;
  IF v_progress->'ok' IS DISTINCT FROM 'true'::jsonb
     OR v_progress->>'session_id' IS DISTINCT FROM p_session_id::text
     OR v_progress->'applied' IS DISTINCT FROM 'false'::jsonb
     OR v_progress->'progress_json_written' IS DISTINCT FROM 'false'::jsonb
     OR jsonb_typeof(v_progress->'can_create_draft') IS DISTINCT FROM 'boolean'
     OR jsonb_typeof(v_progress->'session_ready') IS DISTINCT FROM 'boolean'
     OR jsonb_typeof(v_progress->'read_only') IS DISTINCT FROM 'boolean'
     OR jsonb_typeof(v_progress->'work_queued') IS DISTINCT FROM 'boolean'
     OR jsonb_typeof(v_progress->'selected_row_count') IS DISTINCT FROM 'number'
     OR (v_progress->>'selected_row_count') !~ '^(0|[1-9][0-9]*)$'
     OR jsonb_typeof(v_progress->'selected_eligible_ready_row_count') IS DISTINCT FROM 'number'
     OR (v_progress->>'selected_eligible_ready_row_count') !~ '^(0|[1-9][0-9]*)$'
     OR jsonb_typeof(v_progress->'draft_blocker_codes') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  -- The existing screen ALSO consumes this original continuous-scope owner.
  -- Counter readiness alone cannot prove that background changes are applied.
  v_scope:=public.pay_workbench_scope_progress_v1(p_session_id);
  IF v_scope->'ok' IS DISTINCT FROM 'true'::jsonb
     OR jsonb_typeof(v_scope->'display_ready') IS DISTINCT FROM 'boolean'
     OR jsonb_typeof(v_scope->'draft_safe') IS DISTINCT FROM 'boolean'
     OR NOT (v_scope ? 'draft_block_reason_code')
     OR jsonb_typeof(v_scope->'draft_block_reason_code') NOT IN ('string','null')
     OR (v_scope->'draft_safe'='true'::jsonb AND (v_scope->'display_ready'='false'::jsonb
       OR v_scope->'draft_block_reason_code' IS DISTINCT FROM 'null'::jsonb)) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  v_blockers:=v_progress->'draft_blocker_codes';
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_blockers) AS b(value) WHERE jsonb_typeof(value)<>'string') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  IF v_scope->'draft_safe'='false'::jsonb AND NULLIF(v_scope->>'draft_block_reason_code','') IS NOT NULL
     AND NOT v_blockers ? (v_scope->>'draft_block_reason_code') THEN
    v_blockers:=v_blockers || jsonb_build_array(v_scope->'draft_block_reason_code');
  END IF;
  -- The original owner considers the complete session. A narrower channel/filter
  -- with no selected Ready rows cannot become draftable because another scope is.
  IF p_selected_ready_count=0 AND NOT v_blockers ? 'NO_SELECTED_ROWS' THEN
    v_blockers:=v_blockers || jsonb_build_array('NO_SELECTED_ROWS');
  END IF;
  v_gate:=jsonb_build_object(
    'can_create_draft',v_progress->'can_create_draft'='true'::jsonb AND p_selected_ready_count>0
      AND v_progress->'read_only'='false'::jsonb AND v_scope->'draft_safe'='true'::jsonb
      AND jsonb_array_length(v_blockers)=0,
    'blocker_codes',v_blockers,
    'display_ready',v_scope->'display_ready',
    'draft_safe',v_scope->'draft_safe',
    'draft_block_reason_code',v_scope->'draft_block_reason_code',
    'session_selected_row_count',v_progress->'selected_row_count',
    'session_selected_eligible_ready_row_count',v_progress->'selected_eligible_ready_row_count',
    'session_ready',v_progress->'session_ready',
    'read_only',v_progress->'read_only',
    'work_queued',v_progress->'work_queued',
    'progress_state',v_progress->'progress_state',
    'next_recommended_action',v_progress->'next_recommended_action'
  );
  IF octet_length(v_gate::text)>2048 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  RETURN v_gate;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_draft_gate_v2(uuid,bigint) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_draft_gate_v2(uuid,bigint) FROM PUBLIC, anon, authenticated, service_role;

commit;
