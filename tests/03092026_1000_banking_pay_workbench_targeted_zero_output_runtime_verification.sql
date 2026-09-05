\set ON_ERROR_STOP on

-- Rollback-contained first-use regression for the real targeted publication
-- defect: a requested revoked Timesheet may correctly resolve to its current
-- rotation-family member in the completed build while producing zero rows.
-- No Draft, payment, provider, settlement or remittance owner is invoked.
BEGIN;
SET LOCAL statement_timeout='60s';
SET LOCAL lock_timeout='5s';
SET LOCAL jit=off;

DO $verification$
DECLARE
  v_now timestamptz:=clock_timestamp();
  v_actor_id uuid:=gen_random_uuid();
  v_candidate_id uuid:=gen_random_uuid();
  v_snapshot_id uuid:=gen_random_uuid();
  v_session_id uuid:=gen_random_uuid();
  v_job_id uuid:=gen_random_uuid();
  v_build_id uuid:=gen_random_uuid();
  v_source_build_run_id uuid:=gen_random_uuid();
  v_old_timesheet_id uuid:=gen_random_uuid();
  v_current_timesheet_id uuid:=gen_random_uuid();
  v_unrelated_timesheet_id uuid:=gen_random_uuid();
  v_booking_id text:='H1-ZERO-OUTPUT:'||gen_random_uuid()::text;
  v_result jsonb;
  v_detail text;
  v_caught boolean:=false;
BEGIN
  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot_id,DATE '2099-09-04',DATE '2099-08-30',DATE '2099-08-31',
    DATE '2099-01-01',DATE '2099-08-30','OPEN',false
  );

  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES(v_actor_id,replace(v_actor_id::text,'-','')||'@example.invalid','UNUSABLE','admin',true);
  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
  VALUES(v_candidate_id,'H1 zero-output targeted fixture',v_booking_id,'PAYE');

  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,status,version,is_current,revoked_at,revoked_reason,revoked_by
  ) VALUES (
    v_old_timesheet_id,v_booking_id,v_booking_id,'H1','H1','H1',DATE '2099-08-30',
    'REVOKED',1,false,v_now,'H1_ROLLBACK_FIXTURE','H1'
  ),(
    v_current_timesheet_id,v_booking_id,v_booking_id,'H1','H1','H1',DATE '2099-08-30',
    'RECEIVED',2,true,NULL,NULL,NULL
  ),(
    v_unrelated_timesheet_id,v_booking_id||':OTHER',v_booking_id||':OTHER','H1','H1','H1',
    DATE '2099-08-30','RECEIVED',1,true,NULL,NULL,NULL
  );

  INSERT INTO public.banking_pay_workbench_sessions(
    id,actor_user_id,pay_date,week_ending_cutoff,session_signature,
    source_snapshot_run_id,status,version,scope_candidate_ids,
    scope_seed_complete,scope_total_count,scope_seeded_count,scope_ready_count,
    line_units_total,line_units_ready,progress_state,progress_counter_version
  ) VALUES (
    v_session_id,v_actor_id,DATE '2099-09-04',DATE '2099-08-30',v_booking_id,
    v_snapshot_id,'OPEN',1,ARRAY[v_candidate_id],true,1,1,0,0,0,'REFRESHING_CANDIDATES',1
  );
  INSERT INTO public.banking_pay_workbench_jobs(
    id,job_type,status,dedupe_key,snapshot_run_id,session_id,candidate_id,payload_json
  ) VALUES(
    v_job_id,'WORKBENCH_CANDIDATE_SOURCE_BUILD','RUNNING',v_booking_id,
    v_snapshot_id,v_session_id,v_candidate_id,
    pg_catalog.jsonb_build_object(
      'session_version',1,'source_change_seq',1,
      'source_build_run_id',v_source_build_run_id::text,
      'refresh_scope_kind','TARGETED_TIMESHEETS')
  );
  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,
    source_build_run_id,source_job_id,captured_candidate_generation,
    source_change_seq,status,private_stage,seed_scope_count,seed_scope_digest,
    seed_scope_sealed_at_utc,scope_cursor_json,closure_cursor_json,
    scope_count,dependency_node_count,
    dependency_edge_count,dependency_edge_stream_digest,
    dependency_edge_stream_complete,tagged_edge_count,edge_tag_digest,
    edge_tag_stream_complete,unit_count,unit_digest,row_seal_count,
    last_stable_ordinal,sealed_fingerprint_digest,fact_count,canonical_count,
    scope_digest,dependency_digest,canonical_digest,attestation_json,
    dependency_closure_sealed_at_utc,created_at_utc,updated_at_utc,
    ready_at_utc,reconciled_at_utc,completed_at_utc
  ) VALUES (
    v_build_id,v_candidate_id,v_session_id,1,v_snapshot_id,
    v_source_build_run_id,v_job_id,1,1,'COMPLETE','COMPLETE',1,md5('seed'),
    v_now,'{"terminal":true}'::jsonb,'{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb,
    1,1,0,md5('edges'),true,0,md5('tags'),true,1,md5('unit'),1,1,
    md5('fingerprint'),0,0,md5('scope'),md5('dependency'),md5(''),
    pg_catalog.jsonb_build_object(
      'effect_plan_sealed',true,'effect_plan_digest','H1-EFFECT-PLAN',
      'observed_finance_effect_digest','H1-OBSERVED-EFFECT'),
    v_now,v_now,v_now,v_now,v_now,v_now
  );
  UPDATE public.banking_pay_workbench_jobs
  SET economic_build_id=v_build_id,
      private_stage='SOURCE_PUBLISH',
      private_cursor_kind='SOURCE_PUBLISH',
      private_stage_version=1,
      private_cursor_json=pg_catalog.jsonb_build_object(
        'build_id',v_build_id,'source_build_run_id',v_source_build_run_id)
  WHERE id=v_job_id;
  v_now:=clock_timestamp();
  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,pending_job_id
  ) VALUES(v_session_id,v_candidate_id,0,'PENDING',true,false,v_job_id);
  INSERT INTO public.banking_pay_workbench_session_candidate_state(
    session_id,candidate_id,status,source_change_seq,session_version,pending_job_id
  ) VALUES(v_session_id,v_candidate_id,'PENDING',1,1,v_job_id);
  UPDATE private.banking_pay_workbench_candidate_scope_registry registry
  SET initialisation_status='READY',dirty_generation=1,evaluated_generation=1,
      current_source_change_seq=1,current_build_id=v_build_id,
      last_dirty_reason='H1_ROLLBACK_FIXTURE',last_dirtied_at_utc=v_now,
      last_evaluated_at_utc=v_now,initialised_at_utc=v_now,
      failure_json='{}'::jsonb,updated_at_utc=v_now
  WHERE registry.candidate_id=v_candidate_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'H1_TARGETED_REGISTRY_FIXTURE_MISSING';
  END IF;
  INSERT INTO public.app_change_counters(entity_key,seq)
  VALUES('pay_candidate:'||v_candidate_id::text,1)
  ON CONFLICT(entity_key) DO UPDATE SET seq=EXCLUDED.seq;

  INSERT INTO private.banking_pay_workbench_economic_build_scope(
    build_id,timesheet_id,candidate_id,stable_ordinal,root_timesheet_id,
    dependency_unit_anchor_timesheet_id,dependency_unit_key,
    dependency_unit_digest,seed_reasons,dependency_reasons,
    captured_dirty_generation,captured_input_fingerprint,closure_status,
    closure_family_ordinal,required_fact_families,completed_fact_families,
    fact_row_count,fact_digest,seal_prepared_at_utc
  ) VALUES (
    v_build_id,v_current_timesheet_id,v_candidate_id,1,v_current_timesheet_id,
    v_current_timesheet_id,'H1-ZERO-OUTPUT-UNIT',md5('unit'),
    ARRAY['TARGETED_TIMESHEET'],ARRAY['ROTATION_FAMILY'],1,md5('input'),'SEALED',11,
    ARRAY[]::text[],ARRAY[]::text[],0,md5('facts-empty'),v_now
  );

  -- A genuinely unrelated Timesheet remains rejected before publication.
  BEGIN
    PERFORM private.pay_workbench_publish_certified_source_preview_v1(
      v_session_id,v_candidate_id,v_build_id,v_source_build_run_id,1,1,v_job_id,
      'TARGETED_TIMESHEETS',pg_catalog.jsonb_build_array(v_unrelated_timesheet_id::text),'[]'::jsonb,'{}'::jsonb);
  EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_detail=PG_EXCEPTION_DETAIL;
    v_caught:=SQLERRM='CERTIFIED_SOURCE_PREVIEW_SCOPE_MISSING'
      AND COALESCE(v_detail,'') LIKE '%TARGETED_SCOPE_NOT_IN_COMPLETED_BUILD%';
  END;
  IF NOT v_caught THEN
    RAISE EXCEPTION 'H1_TARGETED_UNRELATED_SCOPE_DID_NOT_FAIL_CLOSED detail=%',v_detail;
  END IF;

  -- The revoked predecessor and its current replacement are both covered by
  -- the one sealed rotation-family member. Zero output rows are correct.
  v_result:=private.pay_workbench_publish_certified_source_preview_v1(
    v_session_id,v_candidate_id,v_build_id,v_source_build_run_id,1,1,v_job_id,
    'TARGETED_TIMESHEETS',
    pg_catalog.jsonb_build_array(v_old_timesheet_id::text,v_current_timesheet_id::text),
    '[]'::jsonb,'{}'::jsonb);

  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR (v_result->>'source_row_count')::integer<>0
     OR (v_result->>'preview_row_count')::integer<>0
     OR (SELECT COUNT(*) FROM public.banking_pay_workbench_preview_rows preview
         WHERE preview.session_id=v_session_id AND preview.candidate_id=v_candidate_id)<>0
     OR NOT EXISTS(
       SELECT 1 FROM public.banking_pay_workbench_session_scope scope
       WHERE scope.session_id=v_session_id AND scope.candidate_id=v_candidate_id
         AND scope.certified_preview_publication_parity_ok IS TRUE) THEN
    RAISE EXCEPTION 'H1_TARGETED_ZERO_OUTPUT_PUBLICATION_FAILED: %',v_result;
  END IF;
END;
$verification$;

ROLLBACK;
