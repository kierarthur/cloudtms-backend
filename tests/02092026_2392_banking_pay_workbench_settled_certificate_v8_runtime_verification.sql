\set ON_ERROR_STOP on

-- Rollback-contained first-use proof for the H1 Workbench settled-certificate
-- producer. No Draft, payment, provider, settlement or remittance owner runs.
BEGIN;
SET LOCAL statement_timeout = '60s';
SET LOCAL lock_timeout = '5s';
SET LOCAL jit = off;

CREATE OR REPLACE FUNCTION pg_temp.h1_v8_clone_due_session(
  p_source_session_id uuid,
  p_new_session_id uuid,
  p_new_preview_row_id uuid,
  p_new_candidate_state_id uuid,
  p_updated_at_utc timestamptz
)
RETURNS void
LANGUAGE plpgsql
SET search_path = ''
AS $clone$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_preview public.banking_pay_workbench_preview_rows%ROWTYPE;
  v_state public.banking_pay_workbench_session_candidate_state%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
BEGIN
  SELECT * INTO STRICT v_session
  FROM public.banking_pay_workbench_sessions
  WHERE id=p_source_session_id;
  v_session.id:=p_new_session_id;
  v_session.session_signature:=v_session.session_signature||':'||p_new_session_id::text;
  v_session.server_selected_preview_row_ids:=pg_catalog.jsonb_build_array(p_new_preview_row_id::text);
  v_session.created_at_utc:=p_updated_at_utc;
  v_session.updated_at_utc:=p_updated_at_utc;
  v_session.progress_updated_at_utc:=p_updated_at_utc;
  INSERT INTO public.banking_pay_workbench_sessions SELECT v_session.*;

  SELECT * INTO STRICT v_preview
  FROM public.banking_pay_workbench_preview_rows
  WHERE session_id=p_source_session_id AND selected
  ORDER BY row_ordinal,id LIMIT 1;
  v_preview.id:=p_new_preview_row_id;
  v_preview.session_id:=p_new_session_id;
  v_preview.row_json:=pg_catalog.jsonb_set(
    v_preview.row_json,'{preview_row_id}',pg_catalog.to_jsonb(p_new_preview_row_id::text),true
  );
  v_preview.created_at_utc:=p_updated_at_utc;
  v_preview.updated_at_utc:=p_updated_at_utc;
  INSERT INTO public.banking_pay_workbench_preview_rows SELECT v_preview.*;

  SELECT * INTO STRICT v_state
  FROM public.banking_pay_workbench_session_candidate_state
  WHERE session_id=p_source_session_id;
  v_state.id:=p_new_candidate_state_id;
  v_state.session_id:=p_new_session_id;
  v_state.created_at_utc:=p_updated_at_utc;
  v_state.updated_at_utc:=p_updated_at_utc;
  v_state.last_recomputed_at_utc:=p_updated_at_utc;
  INSERT INTO public.banking_pay_workbench_session_candidate_state SELECT v_state.*;

  SELECT * INTO STRICT v_scope
  FROM public.banking_pay_workbench_session_scope
  WHERE session_id=p_source_session_id;
  v_scope.id:=pg_catalog.gen_random_uuid();
  v_scope.session_id:=p_new_session_id;
  v_scope.created_at_utc:=p_updated_at_utc;
  v_scope.updated_at_utc:=p_updated_at_utc;
  INSERT INTO public.banking_pay_workbench_session_scope SELECT v_scope.*;
END;
$clone$;

DO $verification$
DECLARE
  v_snapshot_id uuid := gen_random_uuid();
  v_actor_id uuid := gen_random_uuid();
  v_candidate_id uuid := gen_random_uuid();
  v_timesheet_id uuid := gen_random_uuid();
  v_session_id uuid := gen_random_uuid();
  v_due_session_b uuid := gen_random_uuid();
  v_due_session_c uuid := gen_random_uuid();
  v_due_session_d uuid := gen_random_uuid();
  v_due_preview_b uuid := gen_random_uuid();
  v_due_preview_c uuid := gen_random_uuid();
  v_due_preview_d uuid := gen_random_uuid();
  v_due_state_b uuid := gen_random_uuid();
  v_due_state_c uuid := gen_random_uuid();
  v_due_state_d uuid := gen_random_uuid();
  v_due_certificate_b uuid;
  v_due_certificate_c uuid;
  v_due_certificate_d uuid;
  v_candidate_state_id uuid := gen_random_uuid();
  v_source_line_id uuid := gen_random_uuid();
  v_source_build_run_id uuid := gen_random_uuid();
  v_source_publication_id uuid := gen_random_uuid();
  v_preview_row_id uuid := gen_random_uuid();
  v_client_id uuid := gen_random_uuid();
  v_economic_build_id uuid := gen_random_uuid();
  v_finance_case_id uuid := gen_random_uuid();
  v_finance_component_id uuid := gen_random_uuid();
  v_reservation_id uuid := gen_random_uuid();
  v_sibling_finance_component_id uuid := gen_random_uuid();
  v_sibling_reservation_id uuid := gen_random_uuid();
  v_superseded_source_line_id uuid := gen_random_uuid();
  v_superseded_source_build_run_id uuid := gen_random_uuid();
  v_superseded_source_publication_id uuid := gen_random_uuid();
  v_prefix text := 'H1-V8-FIRST-USE:' || gen_random_uuid()::text;
  v_result jsonb;
  v_replay jsonb;
  v_certificate_uuid uuid;
  v_claim_lease_owner text;
  v_iteration integer;
  v_expected_partition_digest text;
  v_header private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_channels jsonb;
  v_completeness jsonb;
  v_policy jsonb;
  v_publication_set jsonb;
  v_selected_partitions jsonb;
  v_expected_overall_payload jsonb;
  v_expected_overall_digest text;
  v_sealed_overall_digest text;
  v_operation_id uuid;
  v_second_operation_id uuid;
  v_reference_envelope jsonb;
  v_certificate_reference jsonb;
  v_caught boolean;
BEGIN
  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot_id,DATE '2099-04-03',DATE '2099-03-29',DATE '2099-03-23',
    DATE '2099-03-01',DATE '2099-03-29','OPEN',false
  );
  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (v_actor_id,replace(v_actor_id::text,'-','')||'@example.invalid','UNUSABLE','admin',true);
  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
  VALUES (v_candidate_id,v_prefix,v_prefix,'PAYE');
  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date
  ) VALUES (v_timesheet_id,v_prefix,v_prefix,'H1','H1','H1',DATE '2099-03-29');

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,filters_json,session_signature,
    source_snapshot_run_id,status,version,server_selected_preview_row_ids,
    server_selected_preview_row_ids_provided,scope_seed_complete,scope_total_count,
    scope_seeded_count,scope_ready_count,scope_pending_count,scope_failed_count,
    line_units_total,line_units_ready,line_units_pending,line_units_failed,
    preview_row_count,selected_row_count,progress_state,progress_counter_version,
    scope_candidate_ids,scope_change_generation_target,scope_change_generation_applied,
    scope_change_generation_shadow_checked
  ) VALUES (
    v_session_id,v_actor_id,DATE '2099-04-03',DATE '2099-03-29','{}'::jsonb,v_prefix,
    v_snapshot_id,'OPEN',1,pg_catalog.jsonb_build_array(v_preview_row_id::text),
    true,true,1,1,1,0,0,1,1,0,0,1,1,'READY',1,ARRAY[v_candidate_id],0,0,0
  );
  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,source_build_run_id,
    captured_candidate_generation,source_change_seq,status,private_stage,
    scope_cursor_json,closure_cursor_json,seed_scope_count,seed_scope_digest,
    seed_scope_sealed_at_utc,scope_count,dependency_edge_stream_complete,
    edge_tag_stream_complete,unit_digest,edge_tag_digest,scope_digest,dependency_digest,
    sealed_fingerprint_digest,row_seal_count,last_stable_ordinal,
    dependency_closure_sealed_at_utc,canonical_digest,attestation_json,
    reconciled_at_utc,completed_at_utc
  ) VALUES (
    v_economic_build_id,v_candidate_id,v_session_id,1,v_snapshot_id,v_source_build_run_id,
    0,1,'COMPLETE','COMPLETE',
    '{"terminal":true}'::jsonb,'{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb,
    1,md5(v_prefix||':SEED'),clock_timestamp(),1,true,true,
    md5(v_prefix||':UNIT'),md5(v_prefix||':EDGE-TAG'),md5(v_prefix||':SCOPE'),
    md5(v_prefix||':DEPENDENCY'),md5(v_prefix||':FINGERPRINT'),1,1,
    clock_timestamp(),md5(v_prefix||':CANONICAL'),'{"fixture":"H1_V8"}'::jsonb,
    clock_timestamp(),clock_timestamp()
  );
  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
    truth_ex_vat,truth_inc_vat,baseline_ex_vat,baseline_inc_vat,financial_digest
  ) VALUES (
    v_economic_build_id,'ENTITLEMENT_COMPONENT',v_prefix||':ENTITLEMENT',v_candidate_id,
    v_timesheet_id,ARRAY[v_timesheet_id],v_prefix||':UNIT','H1_V8_FIXTURE',v_timesheet_id,
    'TS_DAY','2099-03-28',100,100,40,40,md5(v_prefix||':ENTITLEMENT')
  );
  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
    reserved_source_amount,finance_case_id,finance_component_id,reservation_id,financial_digest
  ) VALUES (
    v_economic_build_id,'RESERVATION_COMPONENT',v_prefix||':RESERVATION',v_candidate_id,
    v_timesheet_id,ARRAY[v_timesheet_id],'GLOBAL','H1_V8_FIXTURE',v_reservation_id,
    'TS_DAY','2099-03-28',10,v_finance_case_id,v_finance_component_id,v_reservation_id,
    md5(v_prefix||':RESERVATION')
  );
  -- Same case, Timesheet and economic key but a different component: it must
  -- not leak into this constituent's exact source-reservation evidence.
  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
    reserved_source_amount,finance_case_id,finance_component_id,reservation_id,financial_digest
  ) VALUES (
    v_economic_build_id,'RESERVATION_COMPONENT',v_prefix||':RESERVATION-SIBLING',v_candidate_id,
    v_timesheet_id,ARRAY[v_timesheet_id],'GLOBAL','H1_V8_FIXTURE',v_sibling_reservation_id,
    'TS_DAY','2099-03-28',999,v_finance_case_id,v_sibling_finance_component_id,
    v_sibling_reservation_id,md5(v_prefix||':RESERVATION-SIBLING')
  );
  INSERT INTO public.banking_pay_workbench_session_candidate_state(
    id,session_id,candidate_id,status,source_change_seq,session_version,last_recomputed_at_utc
  ) VALUES (v_candidate_state_id,v_session_id,v_candidate_id,'READY',1,1,clock_timestamp());
  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,
    certified_preview_publication_required,certified_preview_publication_parity_ok,
    certified_preview_publication_session_version,
    certified_preview_publication_source_change_seq,
    certified_preview_publication_source_build_run_id,
    certified_preview_publication_source_publication_id,
    certified_preview_publication_attestation_json,
    certified_preview_publication_attested_at_utc
  ) VALUES (
    v_session_id,v_candidate_id,0,'READY',true,false,true,true,1,1,
    v_source_build_run_id,v_source_publication_id,
    pg_catalog.jsonb_build_object(
      'attestation_version','CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3',
      'contract_version','3','semantic_contract_version','READY_TO_PAY_SEMANTIC_V2',
      'authority_kind','BOUNDED_FULL_SOURCE_BUILD','final_state','READY',
      'semantic_ready','true','parity_complete','true','invalid_selectable_row_count',0,
      'candidate_ready_amount',100,'semantic_proof_digest','H1-V8-PROOF',
      'source_publication_id',v_source_publication_id::text,
      'economic_build_id',v_economic_build_id::text
    ),clock_timestamp()
  );
  INSERT INTO public.banking_pay_workbench_candidate_source_lines(
    id,session_id,candidate_id,session_version,source_change_seq,source_build_run_id,
    source_ordinal,line_key,timesheet_id,section,source_row_json,economic_key_json,
    contract_json,pay_channel_scope,refresh_scope_kind,status,source_publication_id
  ) VALUES (
    v_source_line_id,v_session_id,v_candidate_id,1,1,v_source_build_run_id,1,
    v_prefix||':LINE',v_timesheet_id,'canonical_preview_lines','{}',
    pg_catalog.jsonb_build_object('key_type','TS_DAY','key_value','2099-03-28'),'{}',
    'ALL','CANDIDATE_FULL_LIVE','CURRENT',v_source_publication_id
  );
  INSERT INTO public.banking_pay_workbench_candidate_source_lines(
    id,session_id,candidate_id,session_version,source_change_seq,source_build_run_id,
    source_ordinal,line_key,timesheet_id,section,source_row_json,economic_key_json,
    contract_json,pay_channel_scope,refresh_scope_kind,status,source_publication_id
  ) VALUES (
    v_superseded_source_line_id,v_session_id,v_candidate_id,1,0,
    v_superseded_source_build_run_id,1,v_prefix||':LINE',v_timesheet_id,
    'canonical_preview_lines','{}',
    pg_catalog.jsonb_build_object('key_type','TS_DAY','key_value','2099-03-28'),'{}',
    'ALL','CANDIDATE_FULL_LIVE','SUPERSEDED',v_superseded_source_publication_id
  );
  INSERT INTO public.banking_pay_workbench_preview_rows(
    id,session_id,candidate_id,timesheet_id,section,row_key,row_ordinal,row_json,
    key_type,key_value,selected,selection_state,status,session_version
  ) VALUES (
    v_preview_row_id,v_session_id,v_candidate_id,v_timesheet_id,'canonical_preview_lines',
    v_prefix||':ROW',1,
    pg_catalog.jsonb_build_object(
      'source_line_id',v_source_line_id::text,'source_change_seq',1,
      'source_build_run_id',v_source_build_run_id::text,
      'preview_row_id',v_preview_row_id::text,'line_id',v_prefix||':LINE',
      'client_id',v_client_id::text,'timesheet_id',v_timesheet_id::text,
      'pay_channel','PAYE','current_target_pay_method','PAYE','source_pay_method','PAYE',
      'line_type','TIMESHEET_PAYMENT','amount_ex_vat',50,'draftable',true,
      'finance_case_id',v_finance_case_id::text,
      'finance_component_id',v_finance_component_id::text,
      'is_ready_for_draft',true,'is_excluded_from_allocation',false,
      'selection_allowed',true,
      'preview_contract',pg_catalog.jsonb_build_object('ok',true,'selection_allowed',true),
      'case_components',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'component_fingerprint','ordinary-1','component_key_type','TS_DAY',
          'component_key_value','2099-03-28','classification','ORDINARY',
          'authoritative_truth_ex_vat',100,'authoritative_baseline_ex_vat',40,
          'authoritative_reserved_ex_vat',10,'authoritative_outstanding_ex_vat',50,
          'component_amount_ex_vat',50,'source_pay_ex_vat',50,
          'source_charge_ex_vat',0,'source_pay_method','PAYE',
          'current_target_pay_method','PAYE'
        )
      )
    ),'TS_DAY','2099-03-28',true,'SELECTED','READY',1
  );

  v_result := public.pay_workbench_settled_certificate_status_v8(v_session_id,v_actor_id);
  IF v_result->>'certificate_lifecycle'<>'MISSING'
     OR v_result->'certificate_ready_for_draft' IS DISTINCT FROM 'false'::jsonb
     OR v_result->'certificate_continue_polling' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'H1_V8_STATUS_MISSING_FAILED: %',v_result;
  END IF;

  -- One server sweep is enough to admit the certificate.  Everything below
  -- completes after that single interactive opportunity, proving that later
  -- bounded sweep calls can continue without a browser poll.
  v_result := public.pay_workbench_settled_certificate_due_claim_v8(1);
  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR (v_result->>'processed')::integer<>1
     OR v_result#>>'{claims,0,session_id}'<>v_session_id::text
     OR v_result#>>'{claims,0,lifecycle}'<>'BUILDING' THEN
    RAISE EXCEPTION 'H1_V8_DUE_UNSTARTED_CLAIM_FAILED: %',v_result;
  END IF;
  v_certificate_uuid := (v_result#>>'{claims,0,certificate_uuid}')::uuid;
  v_claim_lease_owner := v_result#>>'{claims,0,lease_owner}';
  v_result := public.pay_workbench_settled_certificate_status_v8(v_session_id,v_actor_id);
  IF v_result->>'certificate_lifecycle'<>'BUILDING'
     OR v_result->'certificate_ready_for_draft' IS DISTINCT FROM 'false'::jsonb
     OR v_result->'certificate_continue_polling' IS DISTINCT FROM 'true'::jsonb
     OR (v_result->>'certificate_uuid')::uuid IS DISTINCT FROM v_certificate_uuid THEN
    RAISE EXCEPTION 'H1_V8_STATUS_BUILDING_FAILED: %',v_result;
  END IF;
  v_result := public.pay_workbench_settled_certificate_build_start_v8(
    v_session_id,v_actor_id,'WORKBENCH_SETTLED_CERTIFICATE_V8:'||v_session_id::text
  );
  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR v_result->>'lifecycle' <> 'BUILDING'
     OR v_result->'replayed' IS DISTINCT FROM 'true'::jsonb
     OR (v_result->>'selected_constituent_count')::integer <> 1 THEN
    RAISE EXCEPTION 'H1_V8_BUILD_START_FIRST_USE_FAILED: %',v_result;
  END IF;
  IF (v_result->>'certificate_uuid')::uuid IS DISTINCT FROM v_certificate_uuid THEN
    RAISE EXCEPTION 'H1_V8_DUE_BUILD_IDENTITY_CHANGED';
  END IF;
  v_result := public.pay_workbench_settled_certificate_build_append_page_v8(
    v_certificate_uuid,NULL,256,NULL,v_claim_lease_owner
  );
  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR v_result->'replayed' IS DISTINCT FROM 'false'::jsonb
     OR v_result->'has_more' IS DISTINCT FROM 'false'::jsonb
     OR (v_result->>'row_count')::integer <> 1 THEN
    RAISE EXCEPTION 'H1_V8_BUILD_APPEND_FIRST_USE_FAILED: %',v_result;
  END IF;
  v_replay := public.pay_workbench_settled_certificate_build_append_page_v8(
    v_certificate_uuid,NULL,256,NULL,v_claim_lease_owner
  );
  IF v_replay->'replayed' IS DISTINCT FROM 'true'::jsonb
     OR v_replay->>'page_receipt_sha256' IS DISTINCT FROM v_result->>'page_receipt_sha256' THEN
    RAISE EXCEPTION 'H1_V8_BUILD_APPEND_RESPONSE_LOSS_REPLAY_FAILED: %',v_replay;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
    WHERE entry.certificate_uuid=v_certificate_uuid AND entry.constituent_ordinal=0
      AND entry.all_same_key_count=1 AND entry.signed_match_count=0
      AND entry.decisive_signed_count=0 AND entry.canonical_amount_ex_vat='50.00'
      AND entry.prior_paid_amount_ex_vat='40.00'
      AND entry.prior_payment_treatment='ACTIVE_SETTLED_COMPONENT_BASELINE_APPLIED'
      AND entry.source_reservation_amount_ex_vat='10.00'
      AND entry.supersession_treatment='CURRENT_CERTIFIED_SOURCE_REPLACES_PROVED_LINEAGE'
  ) OR NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
    WHERE member.certificate_uuid=v_certificate_uuid AND member.stream_ordinal=0
      AND member.member_ordinal=0 AND member.constituent_ordinal=0
  ) OR NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_cert_source_reservations_v8 reservation
    WHERE reservation.certificate_uuid=v_certificate_uuid
      AND reservation.constituent_ordinal=0
      AND reservation.source_reservation_id=v_reservation_id::text
  ) OR NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_superseded_sources_v8 superseded
    WHERE superseded.certificate_uuid=v_certificate_uuid
      AND superseded.constituent_ordinal=0
      AND superseded.superseded_source_id=v_superseded_source_line_id::text
  ) THEN
    RAISE EXCEPTION 'H1_V8_BUILD_APPEND_NORMALISED_EVIDENCE_FAILED';
  END IF;
  FOR v_iteration IN 1..200 LOOP
    v_result:=public.pay_workbench_settled_certificate_seal_v8(v_certificate_uuid,v_actor_id);
    EXIT WHEN v_result->'sealed'='true'::jsonb;
  END LOOP;
  IF v_result->'sealed' IS DISTINCT FROM 'true'::jsonb
     OR v_result->>'lifecycle'<>'SEALED_CURRENT'
     OR COALESCE(v_result->>'certification_id','') NOT LIKE 'WORKBENCH_SETTLED_CERTIFICATION_V2:%'
     OR (SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8
         WHERE certificate_uuid=v_certificate_uuid)<>3 THEN
    RAISE EXCEPTION 'H1_V8_BOUNDED_SEAL_PHASES_FAILED: %',v_result;
  END IF;
  v_sealed_overall_digest:=v_result->>'overall_digest_sha256';
  v_result := public.pay_workbench_settled_certificate_status_v8(v_session_id,v_actor_id);
  IF v_result->>'certificate_lifecycle'<>'SEALED_CURRENT'
     OR v_result->'certificate_ready_for_draft' IS DISTINCT FROM 'true'::jsonb
     OR v_result->'certificate_continue_polling' IS DISTINCT FROM 'false'::jsonb
     OR v_result->>'overall_digest_sha256' IS DISTINCT FROM v_sealed_overall_digest
     OR COALESCE(v_result->>'certification_id','') NOT LIKE 'WORKBENCH_SETTLED_CERTIFICATION_V2:%' THEN
    RAISE EXCEPTION 'H1_V8_STATUS_SEALED_CURRENT_FAILED: %',v_result;
  END IF;

  -- A sealed oldest session cannot hide a newer unstarted one.
  PERFORM pg_temp.h1_v8_clone_due_session(
    v_session_id,v_due_session_b,v_due_preview_b,v_due_state_b,clock_timestamp()+interval '1 second'
  );
  v_result:=public.pay_workbench_settled_certificate_due_claim_v8(1);
  IF v_result#>>'{claims,0,session_id}'<>v_due_session_b::text
     OR v_result#>>'{claims,0,replayed}'<>'false' THEN
    RAISE EXCEPTION 'H1_V8_SEALED_OLDEST_UNSTARTED_NEWER_FAILED: %',v_result;
  END IF;
  v_due_certificate_b:=(v_result#>>'{claims,0,certificate_uuid}')::uuid;

  -- The same unfinished identity resumes after its lease expires; it is not
  -- replaced and the sealed older session remains ignored.
  UPDATE private.banking_pay_workbench_settled_certificates_v8
  SET lease_expires_at_utc=clock_timestamp()-interval '1 second'
  WHERE certificate_uuid=v_due_certificate_b;
  v_result:=public.pay_workbench_settled_certificate_due_claim_v8(1);
  IF v_result#>>'{claims,0,session_id}'<>v_due_session_b::text
     OR (v_result#>>'{claims,0,certificate_uuid}')::uuid IS DISTINCT FROM v_due_certificate_b
     OR v_result#>>'{claims,0,replayed}'<>'true' THEN
    RAISE EXCEPTION 'H1_V8_SEALED_OLDEST_INCOMPLETE_NEWER_FAILED: %',v_result;
  END IF;

  -- BUILDING work has priority over a later unstarted session.
  PERFORM pg_temp.h1_v8_clone_due_session(
    v_session_id,v_due_session_c,v_due_preview_c,v_due_state_c,clock_timestamp()+interval '2 seconds'
  );
  UPDATE private.banking_pay_workbench_settled_certificates_v8
  SET lease_expires_at_utc=clock_timestamp()-interval '1 second'
  WHERE certificate_uuid=v_due_certificate_b;
  v_result:=public.pay_workbench_settled_certificate_due_claim_v8(1);
  IF v_result#>>'{claims,0,session_id}'<>v_due_session_b::text THEN
    RAISE EXCEPTION 'H1_V8_INCOMPLETE_PRIORITY_FAILED: %',v_result;
  END IF;

  -- A second sweep cannot take the actively leased BUILDING identity.  It can
  -- claim other valid work, and a response-lost first claim resumes unchanged
  -- after the bounded lease expires.
  v_result:=public.pay_workbench_settled_certificate_due_claim_v8(1);
  IF v_result#>>'{claims,0,session_id}'<>v_due_session_c::text THEN
    RAISE EXCEPTION 'H1_V8_CONCURRENT_SWEEP_LEASE_FAILED: %',v_result;
  END IF;
  v_due_certificate_c:=(v_result#>>'{claims,0,certificate_uuid}')::uuid;
  UPDATE private.banking_pay_workbench_settled_certificates_v8
  SET lease_expires_at_utc=clock_timestamp()-interval '1 second'
  WHERE certificate_uuid=v_due_certificate_b;
  v_result:=public.pay_workbench_settled_certificate_due_claim_v8(1);
  IF v_result#>>'{claims,0,session_id}'<>v_due_session_b::text
     OR (v_result#>>'{claims,0,certificate_uuid}')::uuid IS DISTINCT FROM v_due_certificate_b THEN
    RAISE EXCEPTION 'H1_V8_RESPONSE_LOSS_RESUME_FAILED: %',v_result;
  END IF;

  -- A terminal deterministic generation remains audit history but cannot
  -- starve a valid newer session.  Only a real authority-identity change makes
  -- that same Workbench session eligible again.
  UPDATE private.banking_pay_workbench_settled_certificates_v8
  SET lifecycle='BUILD_FAILED',build_failure_code='H1_V8_FIXTURE_DETERMINISTIC',
      build_failure_message='fixture terminal cause',build_failed_at_utc=clock_timestamp(),
      lease_owner=NULL,lease_expires_at_utc=NULL
  WHERE certificate_uuid=v_due_certificate_b;
  PERFORM pg_temp.h1_v8_clone_due_session(
    v_session_id,v_due_session_d,v_due_preview_d,v_due_state_d,clock_timestamp()+interval '3 seconds'
  );
  v_result:=public.pay_workbench_settled_certificate_due_claim_v8(1);
  IF v_result#>>'{claims,0,session_id}'<>v_due_session_d::text THEN
    RAISE EXCEPTION 'H1_V8_TERMINAL_HISTORY_STARVATION_FAILED: %',v_result;
  END IF;
  v_due_certificate_d:=(v_result#>>'{claims,0,certificate_uuid}')::uuid;
  IF v_due_certificate_c IS NULL OR v_due_certificate_d IS NULL THEN
    RAISE EXCEPTION 'H1_V8_DUE_CERTIFICATE_IDENTITY_MISSING';
  END IF;
  SELECT private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(
      pg_catalog.jsonb_build_object(
        'partition_ordinal',partition.partition_ordinal,
        'candidate_id',partition.candidate_id,
        'resolved_pay_channel',partition.resolved_pay_channel,
        'ordered_constituent_ordinals',pg_catalog.jsonb_build_array(0),
        'ordered_constituent_identity_digests',pg_catalog.jsonb_build_array(entry.constituent_digest_sha256),
        'constituent_count',partition.constituent_count,
        'canonical_amount_ex_vat_total',partition.canonical_amount_ex_vat_total
      )
    )
  ) INTO v_expected_partition_digest
  FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition
  JOIN private.banking_pay_workbench_settled_certificate_entries_v8 entry
    ON entry.certificate_uuid=partition.certificate_uuid
  WHERE partition.certificate_uuid=v_certificate_uuid AND partition.partition_ordinal=0;
  IF v_expected_partition_digest IS DISTINCT FROM (
    SELECT partition_digest_sha256 FROM private.banking_pay_workbench_settled_certificate_partitions_v8
    WHERE certificate_uuid=v_certificate_uuid AND partition_ordinal=0
  ) THEN
    RAISE EXCEPTION 'H1_V8_PARTITION_V1_DIGEST_MISMATCH';
  END IF;

  -- At one row, it is safe for the fixture to materialise the complete object.
  -- This independently proves that the production paged stream is byte-for-byte
  -- the accepted stableStringify contract rather than a merely similar digest.
  SELECT * INTO STRICT v_header
  FROM private.banking_pay_workbench_settled_certificates_v8
  WHERE certificate_uuid=v_certificate_uuid;
  SELECT pg_catalog.jsonb_object_agg(manifest.pay_channel_scope,
           pg_catalog.jsonb_build_object(
             'pay_channel_scope',manifest.pay_channel_scope,
             'constituent_count',manifest.constituent_count,
             'partition_count',manifest.partition_count,
             'canonical_amount_ex_vat_total',manifest.canonical_amount_ex_vat_total,
             'selected_constituents_digest_sha256',manifest.selected_constituents_digest_sha256,
             'selected_partitions_digest_sha256',manifest.selected_partitions_digest_sha256,
             'manifest_digest_sha256',manifest.manifest_digest_sha256
           )) || pg_catalog.jsonb_build_object('manifests_digest_sha256',v_header.manifests_digest_sha256)
  INTO v_channels
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
  WHERE manifest.certificate_uuid=v_certificate_uuid;

  SELECT pg_catalog.jsonb_build_object(
    'active_draft_rows_excluded',true,
    'all_selected_rows_loaded',true,
    'create_draft_gate',pg_catalog.jsonb_build_object(
      'can_create_draft',v_header.can_create_draft,
      'selected_eligible_ready_row_count',v_header.selected_eligible_ready_row_count,
      'blocking_reason_count',v_header.blocking_reason_count,
      'gate_digest_sha256',v_header.gate_digest_sha256),
    'exclusion_universes',pg_catalog.jsonb_build_object(
      'active_draft',(SELECT pg_catalog.jsonb_build_object(
        'ordered_stable_identity_digests',COALESCE((SELECT pg_catalog.jsonb_agg(member.stable_identity_digest_sha256 ORDER BY member.member_ordinal)
          FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
          WHERE member.certificate_uuid=v_certificate_uuid AND member.universe_kind='ACTIVE_DRAFT'),'[]'::jsonb),
        'row_count',universe.row_count,'universe_digest_sha256',universe.universe_digest_sha256)
        FROM private.banking_pay_workbench_settled_certificate_universes_v8 universe
        WHERE universe.certificate_uuid=v_certificate_uuid AND universe.universe_kind='ACTIVE_DRAFT'),
      'ineligible',(SELECT pg_catalog.jsonb_build_object(
        'ordered_stable_identity_digests',COALESCE((SELECT pg_catalog.jsonb_agg(member.stable_identity_digest_sha256 ORDER BY member.member_ordinal)
          FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
          WHERE member.certificate_uuid=v_certificate_uuid AND member.universe_kind='INELIGIBLE'),'[]'::jsonb),
        'row_count',universe.row_count,'universe_digest_sha256',universe.universe_digest_sha256)
        FROM private.banking_pay_workbench_settled_certificate_universes_v8 universe
        WHERE universe.certificate_uuid=v_certificate_uuid AND universe.universe_kind='INELIGIBLE'),
      'snoozed',(SELECT pg_catalog.jsonb_build_object(
        'ordered_stable_identity_digests',COALESCE((SELECT pg_catalog.jsonb_agg(member.stable_identity_digest_sha256 ORDER BY member.member_ordinal)
          FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
          WHERE member.certificate_uuid=v_certificate_uuid AND member.universe_kind='SNOOZED'),'[]'::jsonb),
        'row_count',universe.row_count,'universe_digest_sha256',universe.universe_digest_sha256)
        FROM private.banking_pay_workbench_settled_certificate_universes_v8 universe
        WHERE universe.certificate_uuid=v_certificate_uuid AND universe.universe_kind='SNOOZED')),
    'historical_terminal_rows_are_not_current_authority',true,
    'historical_terminal_rows_retained',true,
    'ineligible_rows_excluded',true,
    'invalid_current_job_pointer_count',0,
    'line_units_failed',0,'line_units_pending',0,
    'line_units_ready',v_header.line_units_ready,'line_units_total',v_header.line_units_total,
    'queued_current_job_count',0,
    'ready_action_required_blocked_pairwise_disjoint',true,
    'running_current_job_count',0,
    'scope_failed_count',0,'scope_pending_count',0,
    'scope_ready_count',v_header.scope_ready_count,'scope_seeded_count',v_header.scope_seeded_count,
    'scope_total_count',v_header.scope_total_count,
    'section_universes',pg_catalog.jsonb_build_object(
      'action_required',(SELECT pg_catalog.jsonb_build_object(
        'ordered_stable_identity_digests',COALESCE((SELECT pg_catalog.jsonb_agg(member.stable_identity_digest_sha256 ORDER BY member.member_ordinal)
          FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
          WHERE member.certificate_uuid=v_certificate_uuid AND member.universe_kind='ACTION_REQUIRED'),'[]'::jsonb),
        'row_count',universe.row_count,'universe_digest_sha256',universe.universe_digest_sha256)
        FROM private.banking_pay_workbench_settled_certificate_universes_v8 universe
        WHERE universe.certificate_uuid=v_certificate_uuid AND universe.universe_kind='ACTION_REQUIRED'),
      'blocked',(SELECT pg_catalog.jsonb_build_object(
        'ordered_stable_identity_digests',COALESCE((SELECT pg_catalog.jsonb_agg(member.stable_identity_digest_sha256 ORDER BY member.member_ordinal)
          FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
          WHERE member.certificate_uuid=v_certificate_uuid AND member.universe_kind='BLOCKED'),'[]'::jsonb),
        'row_count',universe.row_count,'universe_digest_sha256',universe.universe_digest_sha256)
        FROM private.banking_pay_workbench_settled_certificate_universes_v8 universe
        WHERE universe.certificate_uuid=v_certificate_uuid AND universe.universe_kind='BLOCKED'),
      'ordering','stable row ordinal asc, stable identity asc',
      'ready',(SELECT pg_catalog.jsonb_build_object(
        'ordered_stable_identity_digests',COALESCE((SELECT pg_catalog.jsonb_agg(member.stable_identity_digest_sha256 ORDER BY member.member_ordinal)
          FROM private.banking_pay_workbench_settled_certificate_universe_members_v8 member
          WHERE member.certificate_uuid=v_certificate_uuid AND member.universe_kind='READY'),'[]'::jsonb),
        'row_count',universe.row_count,'universe_digest_sha256',universe.universe_digest_sha256)
        FROM private.banking_pay_workbench_settled_certificate_universes_v8 universe
        WHERE universe.certificate_uuid=v_certificate_uuid AND universe.universe_kind='READY')),
    'selected_page_order',v_header.selected_page_order,
    'selected_page_size_max',v_header.selected_page_size_max,
    'selected_pages_fetched',v_header.selected_pages_fetched,
    'selected_row_count',v_header.selected_row_count,
    'selected_sentinel_overflow',false,
    'selected_terminal_sentinel_seen',true,
    'server_selected_ids_equal_materialised_selected_ids',true,
    'server_selected_preview_row_ids_provided',true,
    'snoozed_rows_excluded',true,
    'unloaded_selection_gap_count',0,
    'unresolved_current_job_count',0
  ) INTO v_completeness;

  SELECT pg_catalog.jsonb_build_object(
    'contract_version',v_header.policy_contract_version,
    'logical_owner_identities',COALESCE((SELECT pg_catalog.jsonb_agg(owner.logical_owner_identity ORDER BY owner.owner_ordinal)
      FROM private.banking_pay_workbench_settled_certificate_policy_owners_v8 owner
      WHERE owner.certificate_uuid=v_certificate_uuid),'[]'::jsonb),
    'before_policy_projection_digest_sha256',v_header.before_policy_projection_digest_sha256,
    'after_policy_projection_digest_sha256',v_header.after_policy_projection_digest_sha256,
    'digests_equal',v_header.policy_digests_equal,
    'execution_recovery_delta_only',v_header.execution_recovery_delta_only,
    'forbidden_policy_delta_count',v_header.forbidden_policy_delta_count,
    'compared_surfaces',COALESCE((SELECT pg_catalog.jsonb_agg(surface.compared_surface ORDER BY surface.surface_ordinal)
      FROM private.banking_pay_workbench_settled_certificate_policy_surfaces_v8 surface
      WHERE surface.certificate_uuid=v_certificate_uuid),'[]'::jsonb),
    'no_payment_policy_change',v_header.no_payment_policy_change
  ) INTO v_policy;

  SELECT pg_catalog.jsonb_build_object(
    'ordering','scope_ordinal asc, candidate_id asc',
    'publication_count',v_header.publication_count,
    'publications',COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'scope_ordinal',publication.scope_ordinal,'candidate_id',publication.candidate_id,
      'candidate_state_id',publication.candidate_state_id,'candidate_state_status',publication.candidate_state_status,
      'source_change_seq',publication.source_change_seq,'source_build_run_id',publication.source_build_run_id,
      'source_publication_id',publication.source_publication_id,
      'certified_publication_session_version',publication.certified_publication_session_version,
      'publication_attestation_version',publication.publication_attestation_version,
      'publication_attestation_digest_sha256',publication.publication_attestation_digest_sha256,
      'publication_parity_ok',publication.publication_parity_ok,
      'publication_attested_at_utc',publication.publication_attested_at_utc
    ) ORDER BY publication.scope_ordinal),'[]'::jsonb),
    'publications_digest_sha256',v_header.publications_digest_sha256
  ) INTO v_publication_set
  FROM private.banking_pay_workbench_settled_certificate_publications_v8 publication
  WHERE publication.certificate_uuid=v_certificate_uuid;

  SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'partition_ordinal',partition.partition_ordinal,'candidate_id',partition.candidate_id,
    'resolved_pay_channel',partition.resolved_pay_channel,
    'ordered_constituent_ordinals',(SELECT pg_catalog.jsonb_agg(member.constituent_ordinal ORDER BY member.member_ordinal)
      FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
      WHERE member.certificate_uuid=partition.certificate_uuid AND member.partition_ordinal=partition.partition_ordinal),
    'ordered_constituent_identity_digests',(SELECT pg_catalog.jsonb_agg(member.stable_identity_digest_sha256 ORDER BY member.member_ordinal)
      FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
      WHERE member.certificate_uuid=partition.certificate_uuid AND member.partition_ordinal=partition.partition_ordinal),
    'constituent_count',partition.constituent_count,
    'canonical_amount_ex_vat_total',partition.canonical_amount_ex_vat_total,
    'partition_digest_sha256',partition.partition_digest_sha256
  ) ORDER BY partition.partition_ordinal),'[]'::jsonb)
  INTO v_selected_partitions
  FROM private.banking_pay_workbench_settled_certificate_partitions_v8 partition
  WHERE partition.certificate_uuid=v_certificate_uuid;

  v_expected_overall_payload:=pg_catalog.jsonb_build_object(
    'authority',pg_catalog.jsonb_build_object(
      'session_id',v_header.workbench_session_id,'session_version',v_header.session_version,
      'progress_counter_version',v_header.progress_counter_version,'progress_state',v_header.progress_state,
      'source_snapshot_run_id',v_header.source_snapshot_run_id,'session_signature',v_header.session_signature,
      'pay_date',v_header.pay_date,'week_ending_cutoff',v_header.week_ending_cutoff,
      'filters_digest_sha256',v_header.filters_digest_sha256,
      'scope_change_generation_target',v_header.scope_change_generation_target,
      'scope_change_generation_applied',v_header.scope_change_generation_applied,
      'scope_change_generation_shadow_checked',v_header.scope_change_generation_shadow_checked,
      'authority_fence_generation',v_header.authority_fence_generation),
    'certified_at_utc',v_header.certified_at_utc,
    'channel_manifests',v_channels,
    'completeness',v_completeness,
    'payment_policy_parity',v_policy,
    'publication_set',v_publication_set,
    'selected_canonical_amount_ex_vat_total',v_header.selected_canonical_amount_ex_vat_total,
    'selected_constituent_count',v_header.selected_constituent_count,
    'selected_constituents',(SELECT pg_catalog.jsonb_agg(
      private.pay_workbench_settled_certificate_constituent_json_v8(entry.certificate_uuid,entry.constituent_ordinal)
      ORDER BY entry.constituent_ordinal)
      FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
      WHERE entry.certificate_uuid=v_certificate_uuid),
    'selected_constituents_digest_sha256',v_header.selected_constituents_digest_sha256,
    'selected_partition_count',v_header.selected_partition_count,
    'selected_partitions',v_selected_partitions,
    'selected_partitions_digest_sha256',v_header.selected_partitions_digest_sha256,
    'selected_partitions_ordering',v_header.selected_partitions_ordering
  );
  v_expected_overall_digest:=private.pay_workbench_settled_certificate_sha256_text_v8(
    private.pay_workbench_settled_certificate_stable_stringify_v8(v_expected_overall_payload));
  IF v_expected_overall_digest IS DISTINCT FROM v_header.overall_digest_sha256
     OR v_sealed_overall_digest IS DISTINCT FROM v_expected_overall_digest THEN
    RAISE EXCEPTION 'H1_V8_OVERALL_STABLE_STRINGIFY_DIGEST_MISMATCH expected %, got %, expected_bytes %, streamed_bytes %',
      v_expected_overall_digest,v_header.overall_digest_sha256,
      pg_catalog.octet_length(private.pay_workbench_settled_certificate_stable_stringify_v8(v_expected_overall_payload)),
      (SELECT (current_state_json->>'total_bytes')::bigint
      FROM private.banking_pay_workbench_settled_certificate_digest_runs_v8
       WHERE certificate_uuid=v_certificate_uuid AND stream_kind='OVERALL_PAYLOAD');
  END IF;

  v_reference_envelope:=public.pay_workbench_settled_certificate_current_reference_issue_v8(
    v_header.workbench_session_id,v_header.session_version,v_header.progress_counter_version,
    'ALL',v_prefix||':OP-1',
    pg_catalog.jsonb_build_object(
      'continue',false,'verified',false,'used',false,
      'pay_date','2099-04-03','pay_week_start','2099-03-30','pay_week_end','2099-04-05',
      'reason',NULL,'verified_by_user_id',NULL,'verified_at_utc',NULL,
      'reauth_purpose',NULL,'guardrail_code',NULL));
  v_certificate_reference:=v_reference_envelope->'certificate_reference';
  IF (SELECT COUNT(*) FROM pg_catalog.jsonb_object_keys(v_certificate_reference))<>9
     OR v_certificate_reference->>'overall_digest_sha256' IS DISTINCT FROM v_header.overall_digest_sha256
     OR v_certificate_reference->>'filter_context_digest_sha256' IS DISTINCT FROM v_header.filter_context_digest_sha256
     OR v_reference_envelope->'pre_admission_scope_facts'->>'scope_facts_contract'
          IS DISTINCT FROM 'WORKBENCH_SETTLED_CERTIFICATE_PRE_ADMISSION_SCOPE_FACTS_V1'
     OR (v_reference_envelope->'pre_admission_scope_facts'->>'certificate_uuid')::uuid
          IS DISTINCT FROM v_certificate_uuid
     OR (v_reference_envelope->'pre_admission_scope_facts'->>'workbench_session_id')::uuid
          IS DISTINCT FROM v_header.workbench_session_id
     OR (v_reference_envelope->'pre_admission_scope_facts'->>'selected_ready_total')::integer<>1
     OR (v_reference_envelope->'pre_admission_scope_facts'->>'selected_ready_for_request')::integer<>1
     OR (v_reference_envelope->'pre_admission_scope_facts'->>'selected_ready_paye')::integer<>1
     OR (v_reference_envelope->'pre_admission_scope_facts'->>'selected_ready_umbrella')::integer<>0
     OR v_reference_envelope->'pre_admission_scope_facts'->>'pay_date'<>'2099-04-03'
     OR v_reference_envelope->'pre_admission_scope_facts'->>'pay_week_start'<>'2099-03-30'
     OR v_reference_envelope->'pre_admission_scope_facts'->>'pay_week_end'<>'2099-04-05'
     OR v_reference_envelope->'pre_admission_scope_facts'->>'week_ending_cutoff'<>'2099-03-29'
     OR v_reference_envelope->'pre_admission_scope_facts'->>'manifest_digest_sha256'
          IS DISTINCT FROM v_certificate_reference->>'manifest_digest_sha256' THEN
    RAISE EXCEPTION 'H1_V8_REFERENCE_ISSUER_FAILED: %',v_reference_envelope;
  END IF;

  v_replay:=public.pay_workbench_settled_certificate_current_reference_issue_v8(
    v_header.workbench_session_id,v_header.session_version,v_header.progress_counter_version,
    'ALL',v_prefix||':OP-1',v_certificate_reference->'same_week_paye_override');
  IF v_replay IS DISTINCT FROM v_reference_envelope THEN
    RAISE EXCEPTION 'H1_V8_REFERENCE_ISSUER_REPLAY_MISMATCH: %',v_replay;
  END IF;

  v_caught:=false;
  BEGIN
    PERFORM public.pay_workbench_settled_certificate_current_reference_issue_v8(
      v_header.workbench_session_id,v_header.session_version,v_header.progress_counter_version+1,
      'ALL',v_prefix||':STALE',
      v_certificate_reference->'same_week_paye_override');
  EXCEPTION WHEN OTHERS THEN
    v_caught:=SQLERRM='WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED';
  END;
  IF NOT v_caught THEN
    RAISE EXCEPTION 'H1_V8_STALE_CURRENT_REFERENCE_DID_NOT_FAIL_CLOSED';
  END IF;

  v_caught:=false;
  BEGIN
    PERFORM public.pay_workbench_settled_certificate_current_reference_issue_v8(
      v_header.workbench_session_id,v_header.session_version,v_header.progress_counter_version,
      'ALL',v_prefix||':BAD-OVERRIDE',
      pg_catalog.jsonb_set(v_certificate_reference->'same_week_paye_override','{verified}','true'::jsonb));
  EXCEPTION WHEN OTHERS THEN
    v_caught:=SQLERRM='WORKBENCH_CERTIFICATE_OVERRIDE_CONTEXT_INVALID';
  END;
  IF NOT v_caught THEN
    RAISE EXCEPTION 'H1_V8_INVALID_OVERRIDE_REFERENCE_DID_NOT_FAIL_CLOSED';
  END IF;

  v_caught:=false;
  BEGIN
    PERFORM public.banking_pay_draft_certified_operation_start_v8(
      pg_catalog.jsonb_set(v_certificate_reference,'{manifest_digest_sha256}',
        pg_catalog.to_jsonb(repeat('0',64))),v_actor_id,v_prefix||':OP-1');
  EXCEPTION WHEN OTHERS THEN
    v_caught:=SQLERRM='WORKBENCH_CERTIFICATE_IDEMPOTENCY_CONTEXT_MISMATCH'
      OR SQLERRM='WORKBENCH_CERTIFICATE_MANIFEST_MISMATCH';
  END;
  IF NOT v_caught THEN
    RAISE EXCEPTION 'H1_V8_TAMPERED_REFERENCE_DID_NOT_FAIL_CLOSED';
  END IF;

  v_result:=public.banking_pay_draft_certified_operation_start_v8(
    v_certificate_reference,v_actor_id,v_prefix||':OP-1');
  v_operation_id:=(v_result->>'operation_id')::uuid;
  IF v_result->>'contract' IS DISTINCT FROM 'WORKBENCH_SETTLED_CERTIFICATE_OPERATION_ADMISSION_V8'
     OR v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR (v_result->>'certificate_uuid')::uuid IS DISTINCT FROM v_certificate_uuid
     OR v_result->>'freeze_state'<>'STAGING'
     OR v_result->'replayed' IS DISTINCT FROM 'false'::jsonb
     OR v_result->'operation_is_existing' IS DISTINCT FROM 'false'::jsonb
     OR v_result->>'compact_operation_projection_contract'
          IS DISTINCT FROM 'WORKBENCH_SETTLED_CERTIFICATE_OPERATION_PROJECTION_V1'
     OR NOT EXISTS (
       SELECT 1 FROM public.banking_pay_operations operation
       WHERE operation.id=v_operation_id
         AND operation.input_json->>'pay_channel_scope'='ALL'
         AND operation.input_json->>'draft_scope'='ALL'
         AND operation.input_json->'same_week_paye_override'
               IS NOT DISTINCT FROM v_certificate_reference->'same_week_paye_override'
         AND operation.input_json->'workbench_settled_certificate_reference_v8'
               IS NOT DISTINCT FROM v_certificate_reference
     ) THEN
    RAISE EXCEPTION 'H1_V8_CERTIFIED_OPERATION_ADMISSION_FAILED: %',v_result;
  END IF;
  v_replay:=public.banking_pay_draft_certified_operation_start_v8(
    v_certificate_reference,v_actor_id,v_prefix||':OP-1');
  IF (v_replay->>'operation_id')::uuid IS DISTINCT FROM v_operation_id
     OR v_replay->'replayed' IS DISTINCT FROM 'true'::jsonb
     OR v_replay->'operation_is_existing' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'H1_V8_CERTIFIED_OPERATION_ADMISSION_REPLAY_FAILED: %',v_replay;
  END IF;

  v_caught:=false;
  BEGIN
    UPDATE public.banking_pay_operations operation
    SET input_json=pg_catalog.jsonb_set(operation.input_json,'{draft_scope}','"UMBRELLA"'::jsonb)
    WHERE operation.id=v_operation_id;
    PERFORM private.pay_workbench_settled_certificate_operation_admit_v8(v_operation_id);
  EXCEPTION WHEN OTHERS THEN
    v_caught:=SQLERRM='WORKBENCH_CERTIFICATE_IDEMPOTENCY_CONTEXT_MISMATCH';
  END;
  IF NOT v_caught THEN
    RAISE EXCEPTION 'H1_V8_COMPACT_OPERATION_PROJECTION_TAMPER_DID_NOT_FAIL_CLOSED';
  END IF;

  v_caught:=false;
  BEGIN
    UPDATE public.banking_pay_operations operation
    SET input_json=operation.input_json-'same_week_paye_override'
    WHERE operation.id=v_operation_id;
    PERFORM private.pay_workbench_settled_certificate_operation_admit_v8(v_operation_id);
  EXCEPTION WHEN OTHERS THEN
    v_caught:=SQLERRM='WORKBENCH_CERTIFICATE_IDEMPOTENCY_CONTEXT_MISMATCH';
  END;
  IF NOT v_caught THEN
    RAISE EXCEPTION 'H1_V8_COMPACT_OPERATION_PROJECTION_MISSING_DID_NOT_FAIL_CLOSED';
  END IF;

  v_caught:=false;
  BEGIN
    UPDATE public.banking_pay_operations operation
    SET input_json=pg_catalog.jsonb_set(operation.input_json,'{pay_channel_scope}','"UMBRELLA"'::jsonb)
    WHERE operation.id=v_operation_id;
    PERFORM private.pay_workbench_settled_certificate_operation_admit_v8(v_operation_id);
  EXCEPTION WHEN OTHERS THEN
    v_caught:=SQLERRM='WORKBENCH_CERTIFICATE_IDEMPOTENCY_CONTEXT_MISMATCH';
  END;
  IF NOT v_caught THEN
    RAISE EXCEPTION 'H1_V8_COMPACT_OPERATION_PROJECTION_CHANGED_DID_NOT_FAIL_CLOSED';
  END IF;

  v_result:=public.pay_workbench_settled_certificate_filter_manifest_v8(v_operation_id,v_header.certification_id);
  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR (v_result->>'certificate_uuid')::uuid IS DISTINCT FROM v_certificate_uuid
     OR (v_result->>'constituent_count')::integer<>1
     OR v_result->>'selected_constituents_digest_sha256' IS DISTINCT FROM v_header.selected_constituents_digest_sha256 THEN
    RAISE EXCEPTION 'H1_V8_FILTER_MANIFEST_READER_FAILED: %',v_result;
  END IF;
  v_result:=public.pay_workbench_settled_certificate_entry_page_v8(
    v_operation_id,v_header.certification_id,NULL,256,NULL);
  IF v_result->'replayed' IS DISTINCT FROM 'false'::jsonb
     OR (v_result->>'row_count')::integer<>1 OR v_result->'has_more' IS DISTINCT FROM 'false'::jsonb
     OR v_result->'terminal_sentinel_present' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'H1_V8_ENTRY_PAGE_FAILED: %',v_result;
  END IF;
  v_replay:=public.pay_workbench_settled_certificate_entry_page_v8(
    v_operation_id,v_header.certification_id,NULL,256,NULL);
  IF v_replay->'replayed' IS DISTINCT FROM 'true'::jsonb
     OR v_replay->>'page_receipt_digest_sha256' IS DISTINCT FROM v_result->>'page_receipt_digest_sha256'
     OR v_replay->'rows' IS DISTINCT FROM v_result->'rows' THEN
    RAISE EXCEPTION 'H1_V8_ENTRY_PAGE_RESPONSE_LOSS_REPLAY_FAILED: %',v_replay;
  END IF;
  v_result:=public.pay_workbench_settled_certificate_partition_page_v8(
    v_operation_id,v_header.certification_id,NULL,256,NULL);
  IF (v_result->>'row_count')::integer<>1
     OR (v_result->>'member_stream_total_count')::integer<>1
     OR (v_result->>'partition_count')::integer<>1
     OR v_result->>'selected_partitions_digest_sha256' IS DISTINCT FROM v_header.selected_partitions_digest_sha256
     OR v_result->'rows'->0->>'constituent_ordinal'<>'0'
     OR v_result->'rows'->0->>'partition_ordinal'<>'0' THEN
    RAISE EXCEPTION 'H1_V8_PARTITION_PAGE_FAILED: %',v_result;
  END IF;
  v_result:=public.pay_workbench_settled_certificate_component_page_v8(
    v_operation_id,v_header.certification_id,'ALL_SAME_ECONOMIC_KEY',NULL,256,NULL);
  IF (v_result->>'row_count')::integer<>1
     OR (v_result->'rows'->0->>'expected_count')::integer<>1
     OR pg_catalog.jsonb_array_length(v_result->'rows'->0->'evidence_rows')<>1 THEN
    RAISE EXCEPTION 'H1_V8_COMPONENT_PAGE_FAILED: %',v_result;
  END IF;
  UPDATE public.banking_pay_operations operation
  SET status='FAILED',runner_state='FAILED',failed_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp()
  WHERE operation.id=v_operation_id;
  v_certificate_reference:=pg_catalog.jsonb_set(
    v_certificate_reference,'{idempotency_key}',pg_catalog.to_jsonb(v_prefix||':OP-2'));
  v_result:=public.banking_pay_draft_certified_operation_start_v8(
    v_certificate_reference,v_actor_id,v_prefix||':OP-2');
  v_second_operation_id:=(v_result->>'operation_id')::uuid;
  IF v_second_operation_id IS NULL OR v_second_operation_id=v_operation_id
     OR v_result->'replayed' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION 'H1_V8_SECOND_CERTIFIED_OPERATION_ADMISSION_FAILED: %',v_result;
  END IF;
  v_replay:=public.pay_workbench_settled_certificate_entry_page_v8(
    v_second_operation_id,v_header.certification_id,NULL,256,NULL);
  IF v_replay->'replayed' IS DISTINCT FROM 'false'::jsonb
     OR (SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_page_receipts_v8 receipt
         WHERE receipt.certificate_uuid=v_certificate_uuid AND receipt.page_kind='CERTIFICATE_ENTRIES')<>2 THEN
    RAISE EXCEPTION 'H1_V8_OPERATION_SCOPED_RECEIPT_ISOLATION_FAILED: %',v_replay;
  END IF;

  v_result:=public.pay_workbench_settled_certificate_build_start_v8(
    v_session_id,v_actor_id,v_prefix||':DIFFERENT-CALLER-KEY');
  IF v_result->'replayed' IS DISTINCT FROM 'true'::jsonb
     OR (v_result->>'certificate_uuid')::uuid IS DISTINCT FROM v_certificate_uuid
     OR v_result->>'lifecycle'<>'SEALED_CURRENT' THEN
    RAISE EXCEPTION 'H1_V8_CURRENT_AUTHORITY_CANONICAL_REPLAY_FAILED: %',v_result;
  END IF;

  v_result:=public.pay_workbench_settled_certificate_lifecycle_v8(
    v_header.certification_id,'REVOKE_CORRUPT_OR_SECURITY','H1_V8_TEST_ONLY_REVOCATION',v_actor_id);
  IF v_result->>'lifecycle'<>'REVOKED_CORRUPT_OR_SECURITY' OR v_result->'replayed' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION 'H1_V8_REVOCATION_FAILED: %',v_result;
  END IF;
  v_replay:=public.pay_workbench_settled_certificate_lifecycle_v8(
    v_header.certification_id,'REVOKE_CORRUPT_OR_SECURITY','H1_V8_TEST_ONLY_REVOCATION',v_actor_id);
  IF v_replay->'replayed' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'H1_V8_REVOCATION_REPLAY_FAILED: %',v_replay;
  END IF;
  v_caught:=false;
  BEGIN
    PERFORM public.pay_workbench_settled_certificate_entry_page_v8(
      v_operation_id,v_header.certification_id,NULL,256,NULL);
  EXCEPTION WHEN OTHERS THEN
    v_caught:=SQLERRM='WORKBENCH_CERTIFICATE_REVOKED';
  END;
  IF NOT v_caught THEN
    RAISE EXCEPTION 'H1_V8_REVOKED_CERTIFICATE_DID_NOT_FAIL_CLOSED';
  END IF;
END;
$verification$;

ROLLBACK;
