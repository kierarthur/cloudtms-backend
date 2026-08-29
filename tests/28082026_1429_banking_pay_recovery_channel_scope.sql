\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
DO $channel_scope$
DECLARE
  v_session uuid:='10000000-0000-4000-8000-000000000005';
  v_candidate uuid:='10000000-0000-4000-8000-000000000002';
  v_other jsonb; v_result jsonb; v_error text; v_one jsonb; v_two jsonb;
BEGIN
  SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO v_other FROM public.banking_pay_workbench_preview_rows r
  WHERE session_id=v_session AND (candidate_id<>v_candidate OR row_json->>'pay_channel'='UMBRELLA');
  v_result:=public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(v_session,v_candidate,'{"pay_channel_scope":"PAYE"}'::jsonb);
  IF v_result->>'ok'<>'true' OR v_result->>'action'<>'SELECTION_DEPENDENT_RECOVERY_HEADROOM_APPLIED' THEN RAISE EXCEPTION 'SCOPED_OWNER_FAILED'; END IF;
  IF v_other IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r
    WHERE session_id=v_session AND (candidate_id<>v_candidate OR row_json->>'pay_channel'='UMBRELLA')) THEN
    RAISE EXCEPTION 'OTHER_CHANNEL_OR_CANDIDATE_WAS_WRITTEN';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows WHERE row_key='selection-recovery:1'
    AND row_json->>'amount_ex_vat'='-15.00' AND selected=false AND selection_state='UNSELECTED'
    AND private.pay_workbench_preview_effective_section_v1(section,row_json)='canonical_preview_lines') THEN
    RAISE EXCEPTION 'SCOPED_RECOVERY_ALLOCATION_OR_EXPLICIT_OVERRIDE_CHANGED';
  END IF;
  -- Original two-argument owner still revalidates both channels. Its allocation
  -- and the additive ALL overload must agree (ignore only audit timestamps).
  v_result:=public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(v_session,v_candidate);
  SELECT jsonb_agg(jsonb_build_object('id',id,'selected',selected,'state',selection_state,
    'section',private.pay_workbench_preview_effective_section_v1(section,row_json),'amount',row_json->'amount_ex_vat',
    'components',row_json->'case_components','digest',row_json#>'{selection_recovery_headroom_v1,overlay_digest}') ORDER BY id)
    INTO v_one FROM public.banking_pay_workbench_preview_rows WHERE session_id=v_session AND candidate_id=v_candidate;
  v_result:=public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(v_session,v_candidate,'{"pay_channel_scope":"ALL"}'::jsonb);
  SELECT jsonb_agg(jsonb_build_object('id',id,'selected',selected,'state',selection_state,
    'section',private.pay_workbench_preview_effective_section_v1(section,row_json),'amount',row_json->'amount_ex_vat',
    'components',row_json->'case_components','digest',row_json#>'{selection_recovery_headroom_v1,overlay_digest}') ORDER BY id)
    INTO v_two FROM public.banking_pay_workbench_preview_rows WHERE session_id=v_session AND candidate_id=v_candidate;
  IF v_one IS DISTINCT FROM v_two THEN RAISE EXCEPTION 'ALL_OVERLOAD_CHANGED_ORIGINAL_RESULT'; END IF;
  BEGIN
    PERFORM public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(v_session,v_candidate,'{"pay_channel_scope":"PAYE OR true"}'::jsonb);
    RAISE EXCEPTION 'UNKNOWN_CHANNEL_ACCEPTED';
  EXCEPTION WHEN SQLSTATE '22023' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE; END IF;
  END;
  UPDATE public.banking_pay_workbench_session_scope SET certified_preview_publication_attestation_json='{}'::jsonb
    WHERE session_id=v_session AND candidate_id=v_candidate;
  BEGIN
    PERFORM public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(v_session,v_candidate,'{"pay_channel_scope":"PAYE"}'::jsonb);
    RAISE EXCEPTION 'UNPARTITIONED_LEGACY_SCOPE_WAS_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' THEN RAISE; END IF;
  END;
END $channel_scope$;
ROLLBACK;
