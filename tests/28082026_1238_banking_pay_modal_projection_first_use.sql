-- Data-free first-use checks, always rolled back on the disposable local DB.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout = '10s';
DO $projection_first_use$
DECLARE
  v_row public.banking_pay_workbench_preview_rows;
  v_payload jsonb;
  v_section text;
BEGIN
  IF current_database() <> 'banking_modal_v2_test' THEN
    RAISE EXCEPTION 'LOCAL_BANKING_MODAL_FIXTURE_ONLY';
  END IF;
  FOREACH v_section IN ARRAY ARRAY['canonical_preview_lines','cases_resolutions','blocked_for_pay'] LOOP
    IF EXISTS (SELECT 1 FROM private.pay_workbench_modal_eligible_rows_v2(
      '00000000-0000-4000-8000-000000000099'::uuid, 1, v_section
    )) THEN RAISE EXCEPTION 'EMPTY_SESSION_PROJECTION_INVALID'; END IF;
  END LOOP;
  PERFORM 1 FROM private.pay_workbench_modal_eligible_rows_v2(
    '00000000-0000-4000-8000-000000000099'::uuid, 2147483648::bigint, 'canonical_preview_lines'
  );
  v_row := jsonb_populate_record(NULL::public.banking_pay_workbench_preview_rows, '{
    "id":"00000000-0000-4000-8000-000000000001",
    "candidate_id":"00000000-0000-4000-8000-000000000002",
    "section":"canonical_preview_lines","row_key":"fixture:row:1",
    "row_ordinal":1,"session_version":1,"status":"READY","selected":true,"selection_state":"SELECTED",
    "key_type":"TS_DAY","key_value":"2026-08-28",
    "row_json":{"candidate_name":"Fixture candidate","amount_display":"100.20","section_amount_display":"120.24",
      "selection_allowed":true,"draftable":true,"is_ready_for_draft":true,
      "existing_action_metadata":{"must_remain":true},"is_recognised_finance_deduction":false}
  }'::jsonb);
  v_payload := private.pay_workbench_modal_row_payload_v2(v_row);
  IF v_payload->>'amount_display' <> '100.20' OR v_payload->>'section_amount_display' <> '120.24'
     OR v_payload#>>'{existing_action_metadata,must_remain}' <> 'true'
     OR v_payload->>'key_value' <> '2026-08-28'
     OR v_payload->>'selection_allowed' <> 'true'
     OR v_payload->>'effective_section' <> 'canonical_preview_lines' THEN
    RAISE EXCEPTION 'READY_PAYLOAD_PARITY_FAILED';
  END IF;
  v_row.row_json := v_row.row_json || '{"selection_recovery_headroom_v1":{"contract_version":1,"effective_section":"blocked_for_pay"}}'::jsonb;
  v_payload := private.pay_workbench_modal_row_payload_v2(v_row);
  IF v_payload->>'effective_section' <> 'blocked_for_pay'
     OR v_payload->>'physical_section' <> 'canonical_preview_lines'
     OR v_payload->>'selected' <> 'false' OR v_payload->>'selection_allowed' <> 'false' THEN
    RAISE EXCEPTION 'BLOCKED_OVERLAY_PAYLOAD_PARITY_FAILED';
  END IF;
  v_row.row_json := (v_row.row_json - 'selection_recovery_headroom_v1') || '{"post_draft_unavailable":true}'::jsonb;
  v_payload := private.pay_workbench_modal_row_payload_v2(v_row);
  IF v_payload->>'draftable' <> 'false' OR v_payload#>>'{preview_contract,ok}' <> 'false' THEN
    RAISE EXCEPTION 'POST_DRAFT_PAYLOAD_FENCE_FAILED';
  END IF;
END
$projection_first_use$;
ROLLBACK;
