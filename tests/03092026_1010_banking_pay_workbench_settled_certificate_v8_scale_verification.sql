\set ON_ERROR_STOP on

-- Bounded, rollback-contained H1 V8 scale proof. Invoke with
--   psql -v target_count=101 -f <this file>
-- and repeat for 1001 and the representative 2000/5000-row measurements.
-- The configured 50,000 ceiling is checked by lightweight source/contract guards;
-- this runtime fixture deliberately never constructs a 50,000-row data set. The synthetic
-- setup has a generous
-- fixture-only budget; every measured interface call is asserted against the
-- unchanged production routine budget (15s/1.5s, or 6s/1s for admission).
\if :{?target_count}
\else
\set target_count 101
\endif
\if :{?worker_seed_only}
\else
\set worker_seed_only false
\endif
\if :{?h2_transport}
\else
\set h2_transport false
\endif

BEGIN;
SET LOCAL statement_timeout = '60min';
SET LOCAL lock_timeout = '5s';
SET LOCAL jit = off;
SET LOCAL client_min_messages = warning;

CREATE TEMP TABLE h1_v8_scale_config(
  target_count integer NOT NULL,
  worker_seed_only boolean NOT NULL,
  h2_transport boolean NOT NULL
);
INSERT INTO h1_v8_scale_config
VALUES (:target_count,:'worker_seed_only'::boolean,:'h2_transport'::boolean);

CREATE TEMP TABLE h1_v8_scale_result(
  certificate_uuid uuid,
  certification_id text,
  operation_id uuid,
  target_count integer NOT NULL,
  candidate_count integer NOT NULL,
  build_start_ms numeric NOT NULL DEFAULT 0,
  append_call_count integer NOT NULL DEFAULT 0,
  append_max_ms numeric NOT NULL DEFAULT 0,
  seal_call_count integer NOT NULL DEFAULT 0,
  seal_max_ms numeric NOT NULL DEFAULT 0,
  reference_issue_ms numeric NOT NULL DEFAULT 0,
  operation_start_ms numeric NOT NULL DEFAULT 0,
  entry_page_call_count integer NOT NULL DEFAULT 0,
  partition_page_call_count integer NOT NULL DEFAULT 0,
  component_page_call_count integer NOT NULL DEFAULT 0,
  read_page_max_ms numeric NOT NULL DEFAULT 0
);

DO $verification$
DECLARE
  v_target integer := (SELECT target_count FROM h1_v8_scale_config);
  v_worker_seed_only boolean := (SELECT worker_seed_only FROM h1_v8_scale_config);
  v_h2_transport boolean := (SELECT h2_transport FROM h1_v8_scale_config);
  v_candidate_count integer;
  v_snapshot_id uuid := '30000000-0000-0000-0000-000000000001';
  v_actor_id uuid := '10000000-0000-4000-8000-000000000001';
  v_timesheet_id uuid := '40000000-0000-0000-0000-000000000001';
  v_session_id uuid := '20000000-0000-4000-8000-000000000001';
  v_client_id uuid := '50000000-0000-0000-0000-000000000001';
  v_prefix text := 'H1-V8-SCALE-' || v_target::text;
  v_result jsonb;
  v_reference_envelope jsonb;
  v_reference jsonb;
  v_certificate_uuid uuid;
  v_certification_id text;
  v_operation_id uuid;
  v_after integer;
  v_receipt text;
  v_has_more boolean;
  v_started timestamptz;
  v_elapsed numeric;
  v_build_start_ms numeric := 0;
  v_append_max_ms numeric := 0;
  v_seal_max_ms numeric := 0;
  v_reference_ms numeric := 0;
  v_operation_ms numeric := 0;
  v_read_max_ms numeric := 0;
  v_append_calls integer := 0;
  v_seal_calls integer := 0;
  v_entry_calls integer := 0;
  v_partition_calls integer := 0;
  v_component_calls integer := 0;
  v_rows_seen integer;
  v_expected_paye integer;
  v_expected_umbrella integer;
  v_kind text;
  v_caught boolean;
  v_update_count integer;
BEGIN
  IF v_target NOT IN (101,1001,2000,5000) THEN
    RAISE EXCEPTION 'H1_V8_SCALE_TARGET_UNSUPPORTED: %',v_target;
  END IF;
  -- Four interleaved partitions exercise both PAYE and UMBRELLA subset
  -- manifests without making the benchmark measure per-Candidate fixture
  -- setup. Independent 101-Candidate Draft staging belongs to H2's boundary.
  v_candidate_count:=LEAST(v_target,4);

  CREATE TEMP TABLE h1_v8_scale_candidates AS
  SELECT candidate_seq,
         format('60000000-0000-0000-0000-%s',lpad(candidate_seq::text,12,'0'))::uuid AS candidate_id,
         format('61000000-0000-0000-0000-%s',lpad(candidate_seq::text,12,'0'))::uuid AS candidate_state_id,
         format('62000000-0000-0000-0000-%s',lpad(candidate_seq::text,12,'0'))::uuid AS source_build_run_id,
         format('63000000-0000-0000-0000-%s',lpad(candidate_seq::text,12,'0'))::uuid AS source_publication_id,
         format('64000000-0000-0000-0000-%s',lpad(candidate_seq::text,12,'0'))::uuid AS economic_build_id,
         CASE WHEN candidate_seq % 2 = 1 THEN 'PAYE' ELSE 'UMBRELLA' END::text AS pay_channel
  FROM pg_catalog.generate_series(1,v_candidate_count) candidate_seq;

  CREATE TEMP TABLE h1_v8_scale_rows AS
  SELECT row_ordinal,
         ((row_ordinal-1) % v_candidate_count)+1 AS candidate_seq,
         format('70000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS source_line_id,
         format('80000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS preview_row_id
  FROM pg_catalog.generate_series(1,v_target) row_ordinal;

  INSERT INTO public.banking_pay_snapshot_runs(
    id,pay_date,week_ending_cutoff,pay_week_start,eligibility_from_date,
    eligibility_to_date,status,is_active
  ) VALUES (
    v_snapshot_id,DATE '2099-04-03',DATE '2099-03-29',DATE '2099-03-23',
    DATE '2099-03-01',DATE '2099-03-29','OPEN',false
  );
  INSERT INTO public.tms_users(id,email,password_hash,role,is_active)
  VALUES (v_actor_id,'h1-v8-scale-'||v_target::text||'@example.invalid','UNUSABLE','admin',true);
  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
  SELECT candidate_id,v_prefix||':CANDIDATE:'||candidate_seq::text,
         'CCR-'||(99000000+candidate_seq)::text,pay_channel
  FROM h1_v8_scale_candidates ORDER BY candidate_seq;
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
  )
  SELECT v_session_id,v_actor_id,DATE '2099-04-03',DATE '2099-03-29','{}'::jsonb,v_prefix,
         v_snapshot_id,'OPEN',1,
         (SELECT pg_catalog.jsonb_agg(row.preview_row_id::text ORDER BY row.row_ordinal)
          FROM h1_v8_scale_rows row),
         true,true,v_candidate_count,v_candidate_count,v_candidate_count,0,0,
         v_target,v_target,0,0,v_target,v_target,'READY',1,
         (SELECT pg_catalog.array_agg(candidate.candidate_id ORDER BY candidate.candidate_seq)
           FROM h1_v8_scale_candidates candidate),0,0,0;

  INSERT INTO private.banking_pay_workbench_economic_builds(
    id,candidate_id,session_id,session_version,source_snapshot_run_id,source_build_run_id,
    captured_candidate_generation,source_change_seq,status,private_stage,
    scope_cursor_json,closure_cursor_json,seed_scope_count,seed_scope_digest,
    seed_scope_sealed_at_utc,scope_count,dependency_edge_stream_complete,
    edge_tag_stream_complete,unit_digest,edge_tag_digest,scope_digest,dependency_digest,
    sealed_fingerprint_digest,row_seal_count,last_stable_ordinal,
    dependency_closure_sealed_at_utc,canonical_digest,attestation_json,
    reconciled_at_utc,completed_at_utc,created_at_utc,updated_at_utc
  )
  SELECT candidate.economic_build_id,candidate.candidate_id,v_session_id,1,v_snapshot_id,
         candidate.source_build_run_id,0,1,'COMPLETE','COMPLETE',
         '{"terminal":true}'::jsonb,'{"terminal":true,"seal_phase":"COMPLETE"}'::jsonb,
         1,md5(v_prefix||':SEED:'||candidate.candidate_seq::text),pg_catalog.statement_timestamp(),
         1,true,true,md5(v_prefix||':UNIT:'||candidate.candidate_seq::text),
         md5(v_prefix||':EDGE-TAG:'||candidate.candidate_seq::text),
         md5(v_prefix||':SCOPE:'||candidate.candidate_seq::text),
         md5(v_prefix||':DEPENDENCY:'||candidate.candidate_seq::text),
         md5(v_prefix||':FINGERPRINT:'||candidate.candidate_seq::text),
         1,1,
         pg_catalog.statement_timestamp(),md5(v_prefix||':CANONICAL:'||candidate.candidate_seq::text),
         pg_catalog.jsonb_build_object('fixture','H1_V8_SCALE'),
         pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp(),
         pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  FROM h1_v8_scale_candidates candidate;

  INSERT INTO private.banking_pay_workbench_economic_build_facts(
    build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
    dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
    truth_ex_vat,truth_inc_vat,baseline_ex_vat,baseline_inc_vat,financial_digest
  )
  SELECT candidate.economic_build_id,'ENTITLEMENT_COMPONENT',
         v_prefix||':ENTITLEMENT:'||row.row_ordinal::text,candidate.candidate_id,
         v_timesheet_id,ARRAY[v_timesheet_id],v_prefix||':UNIT:'||row.row_ordinal::text,
         'H1_V8_SCALE_FIXTURE',row.preview_row_id,'ADJUSTMENT_CODE',
         'H1SCALE:'||row.row_ordinal::text,1,1,0,0,
         md5(v_prefix||':ENTITLEMENT:'||row.row_ordinal::text)
  FROM h1_v8_scale_rows row
  JOIN h1_v8_scale_candidates candidate USING(candidate_seq);

  INSERT INTO public.banking_pay_workbench_session_candidate_state(
    id,session_id,candidate_id,status,source_change_seq,session_version,last_recomputed_at_utc
  )
  SELECT candidate_state_id,v_session_id,candidate_id,'READY',1,1,pg_catalog.clock_timestamp()
  FROM h1_v8_scale_candidates ORDER BY candidate_seq;
  INSERT INTO public.banking_pay_workbench_session_scope(
    session_id,candidate_id,scope_ordinal,status,seeded,dirty,
    certified_preview_publication_required,certified_preview_publication_parity_ok,
    certified_preview_publication_session_version,certified_preview_publication_source_change_seq,
    certified_preview_publication_source_build_run_id,
    certified_preview_publication_source_publication_id,
    certified_preview_publication_attestation_json,
    certified_preview_publication_attested_at_utc
  )
  SELECT v_session_id,candidate.candidate_id,candidate.candidate_seq-1,'READY',true,false,true,true,1,1,
         candidate.source_build_run_id,candidate.source_publication_id,
         pg_catalog.jsonb_build_object(
           'attestation_version','CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3',
           'contract_version','3','semantic_contract_version','READY_TO_PAY_SEMANTIC_V2',
           'authority_kind','BOUNDED_FULL_SOURCE_BUILD','final_state','READY',
           'semantic_ready','true','parity_complete','true','invalid_selectable_row_count',0,
           'candidate_ready_amount',(SELECT COUNT(*) FROM h1_v8_scale_rows row
                                     WHERE row.candidate_seq=candidate.candidate_seq),
           'semantic_proof_digest','H1-V8-SCALE-PROOF',
           'source_publication_id',candidate.source_publication_id::text,
           'economic_build_id',candidate.economic_build_id::text),
         pg_catalog.clock_timestamp()
  FROM h1_v8_scale_candidates candidate ORDER BY candidate.candidate_seq;

  INSERT INTO public.banking_pay_workbench_candidate_source_lines(
    id,session_id,candidate_id,session_version,source_change_seq,source_build_run_id,
    source_ordinal,line_key,timesheet_id,section,source_row_json,economic_key_json,
    contract_json,pay_channel_scope,refresh_scope_kind,status,source_publication_id
  )
  SELECT row.source_line_id,v_session_id,candidate.candidate_id,1,1,candidate.source_build_run_id,
         ROW_NUMBER() OVER (PARTITION BY row.candidate_seq ORDER BY row.row_ordinal),
         v_prefix||':LINE:'||row.row_ordinal::text,v_timesheet_id,'canonical_preview_lines',
         '{}'::jsonb,
         pg_catalog.jsonb_build_object('key_type','ADJUSTMENT_CODE',
           'key_value','H1SCALE:'||row.row_ordinal::text),
         '{}'::jsonb,'ALL','CANDIDATE_FULL_LIVE','CURRENT',
         candidate.source_publication_id
  FROM h1_v8_scale_rows row
  JOIN h1_v8_scale_candidates candidate USING (candidate_seq)
  ORDER BY row.row_ordinal;

  INSERT INTO public.banking_pay_workbench_preview_rows(
    id,session_id,candidate_id,timesheet_id,section,row_key,row_ordinal,row_json,
    key_type,key_value,selected,selection_state,status,session_version
  )
  SELECT row.preview_row_id,v_session_id,candidate.candidate_id,v_timesheet_id,
         'canonical_preview_lines',v_prefix||':ROW:'||row.row_ordinal::text,row.row_ordinal,
         pg_catalog.jsonb_build_object(
           'source_line_id',row.source_line_id::text,'source_change_seq',1,
           'source_build_run_id',candidate.source_build_run_id::text,
           'preview_row_id',row.preview_row_id::text,'line_id',v_prefix||':LINE:'||row.row_ordinal::text,
           'client_id',v_client_id::text,'timesheet_id',v_timesheet_id::text,
           'pay_channel',candidate.pay_channel,'current_target_pay_method',candidate.pay_channel,
           'source_pay_method',candidate.pay_channel,'line_type','TIMESHEET_PAYMENT',
           'amount_ex_vat',1,'draftable',true,'is_ready_for_draft',true,
           'is_excluded_from_allocation',false,'selection_allowed',true,
           'preview_contract',pg_catalog.jsonb_build_object('ok',true,'selection_allowed',true),
           'case_components',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
             'component_fingerprint','ordinary-'||row.row_ordinal::text,
             'component_key_type','ADJUSTMENT_CODE',
             'component_key_value','H1SCALE:'||row.row_ordinal::text,
             'classification','ORDINARY','component_amount_ex_vat',1,
             'source_pay_ex_vat',1,'source_charge_ex_vat',0,
             'source_pay_method',candidate.pay_channel,
             'current_target_pay_method',candidate.pay_channel))) AS row_json,
         'ADJUSTMENT_CODE','H1SCALE:'||row.row_ordinal::text,true,'SELECTED','READY',1
  FROM h1_v8_scale_rows row
  JOIN h1_v8_scale_candidates candidate USING (candidate_seq)
  ORDER BY row.row_ordinal;

  PERFORM pg_catalog.set_config('client_min_messages','notice',true);
  RAISE NOTICE 'H1_V8_SCALE_PHASE setup_complete target=%',v_target;
  IF v_worker_seed_only THEN
    RAISE NOTICE 'H1_V8_WORKER_POSTGREST_SEED_READY target=% session_id=%',v_target,v_session_id;
    RETURN;
  END IF;

  v_started:=pg_catalog.clock_timestamp();
  v_result:=public.pay_workbench_settled_certificate_build_start_v8(
    v_session_id,v_actor_id,v_prefix||':BUILD');
  v_build_start_ms:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
  IF v_build_start_ms>15000 THEN
    RAISE EXCEPTION 'H1_V8_BUILD_START_BUDGET_EXCEEDED: % ms',v_build_start_ms;
  END IF;
  IF v_result->'ok' IS DISTINCT FROM 'true'::jsonb
     OR v_result->>'lifecycle'<>'BUILDING'
     OR (v_result->>'selected_constituent_count')::integer<>v_target THEN
    RAISE EXCEPTION 'H1_V8_SCALE_BUILD_START_FAILED: %',v_result;
  END IF;
  v_certificate_uuid:=(v_result->>'certificate_uuid')::uuid;
  RAISE NOTICE 'H1_V8_SCALE_PHASE build_start_complete target=% ms=%',v_target,ROUND(v_build_start_ms,3);

  v_after:=NULL;v_receipt:=NULL;
  LOOP
    v_started:=pg_catalog.clock_timestamp();
    v_result:=public.pay_workbench_settled_certificate_build_append_page_v8(
      v_certificate_uuid,v_after,128,v_receipt,v_actor_id::text);
    v_elapsed:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
    v_append_max_ms:=GREATEST(v_append_max_ms,v_elapsed);v_append_calls:=v_append_calls+1;
    IF v_elapsed>15000 OR v_result->'ok' IS DISTINCT FROM 'true'::jsonb THEN
      RAISE EXCEPTION 'H1_V8_APPEND_FAILED_OR_OVER_BUDGET: % ms %',v_elapsed,v_result;
    END IF;
    v_has_more:=(v_result->>'has_more')::boolean;
    EXIT WHEN NOT v_has_more;
    v_after:=(v_result->>'next_after_ordinal')::integer;
    v_receipt:=v_result->>'page_receipt_sha256';
  END LOOP;
  IF (SELECT COUNT(*) FROM private.banking_pay_workbench_settled_certificate_entries_v8
      WHERE certificate_uuid=v_certificate_uuid)<>v_target THEN
    RAISE EXCEPTION 'H1_V8_APPEND_COUNT_MISMATCH';
  END IF;
  RAISE NOTICE 'H1_V8_SCALE_PHASE append_complete target=% calls=% max_ms=%',v_target,v_append_calls,ROUND(v_append_max_ms,3);

  LOOP
    v_seal_calls:=v_seal_calls+1;
    IF v_seal_calls>10000 THEN RAISE EXCEPTION 'H1_V8_SEAL_DID_NOT_TERMINATE'; END IF;
    v_started:=pg_catalog.clock_timestamp();
    v_result:=public.pay_workbench_settled_certificate_seal_v8(v_certificate_uuid,v_actor_id);
    v_elapsed:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
    v_seal_max_ms:=GREATEST(v_seal_max_ms,v_elapsed);
    IF v_elapsed>15000 OR v_result->'ok' IS DISTINCT FROM 'true'::jsonb THEN
      RAISE EXCEPTION 'H1_V8_SEAL_FAILED_OR_OVER_BUDGET: % ms %',v_elapsed,v_result;
    END IF;
    IF v_seal_calls % 100 = 0 THEN
      RAISE NOTICE 'H1_V8_SCALE_PHASE seal_progress target=% calls=% stage=% max_ms=%',
        v_target,v_seal_calls,v_result->>'stage',ROUND(v_seal_max_ms,3);
    END IF;
    EXIT WHEN v_result->'sealed'='true'::jsonb;
  END LOOP;
  v_certification_id:=v_result->>'certification_id';
  RAISE NOTICE 'H1_V8_SCALE_PHASE seal_complete target=% calls=% max_ms=%',v_target,v_seal_calls,ROUND(v_seal_max_ms,3);
  SELECT COUNT(*) FILTER (WHERE candidate.pay_channel='PAYE'),
         COUNT(*) FILTER (WHERE candidate.pay_channel='UMBRELLA')
  INTO v_expected_paye,v_expected_umbrella
  FROM h1_v8_scale_rows row JOIN h1_v8_scale_candidates candidate USING(candidate_seq);
  IF NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
    WHERE manifest.certificate_uuid=v_certificate_uuid AND manifest.pay_channel_scope='ALL'
      AND manifest.constituent_count=v_target AND manifest.partition_count=v_candidate_count
      AND manifest.canonical_amount_ex_vat_total::numeric=v_target::numeric
      AND manifest.selected_constituents_digest_sha256 ~ '^[0-9a-f]{64}$'
      AND manifest.selected_partitions_digest_sha256 ~ '^[0-9a-f]{64}$'
  ) OR NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
    WHERE manifest.certificate_uuid=v_certificate_uuid AND manifest.pay_channel_scope='PAYE'
      AND manifest.constituent_count=v_expected_paye
  ) OR NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 manifest
    WHERE manifest.certificate_uuid=v_certificate_uuid AND manifest.pay_channel_scope='UMBRELLA'
      AND manifest.constituent_count=v_expected_umbrella
  ) THEN
    RAISE EXCEPTION 'H1_V8_CHANNEL_MANIFEST_SCALE_MISMATCH';
  END IF;

  v_started:=pg_catalog.clock_timestamp();
  v_reference_envelope:=public.pay_workbench_settled_certificate_current_reference_issue_v8(
    v_session_id,1,1,'ALL',v_prefix||':OPERATION',
    pg_catalog.jsonb_build_object(
      'continue',false,'verified',false,'used',false,
      'pay_date','2099-04-03','pay_week_start','2099-03-30','pay_week_end','2099-04-05',
      'reason',NULL,'verified_by_user_id',NULL,'verified_at_utc',NULL,
      'reauth_purpose',NULL,'guardrail_code',NULL));
  v_reference_ms:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
  IF v_reference_ms>6000 THEN RAISE EXCEPTION 'H1_V8_REFERENCE_BUDGET_EXCEEDED: %',v_reference_ms; END IF;
  v_reference:=v_reference_envelope->'certificate_reference';
  v_started:=pg_catalog.clock_timestamp();
  v_result:=public.banking_pay_draft_certified_operation_start_v8(
    v_reference,v_actor_id,v_prefix||':OPERATION');
  v_operation_ms:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
  IF v_operation_ms>6000 OR v_result->>'freeze_state'<>'STAGING' THEN
    RAISE EXCEPTION 'H1_V8_OPERATION_START_FAILED_OR_OVER_BUDGET: % ms %',v_operation_ms,v_result;
  END IF;
  v_operation_id:=(v_result->>'operation_id')::uuid;

  IF v_h2_transport THEN
    INSERT INTO h1_v8_scale_result(
      certificate_uuid,certification_id,operation_id,target_count,candidate_count,
      build_start_ms,append_call_count,append_max_ms,seal_call_count,seal_max_ms,
      reference_issue_ms,operation_start_ms,entry_page_call_count,
      partition_page_call_count,component_page_call_count,read_page_max_ms
    ) VALUES (
      v_certificate_uuid,v_certification_id,v_operation_id,v_target,v_candidate_count,
      v_build_start_ms,v_append_calls,v_append_max_ms,v_seal_calls,v_seal_max_ms,
      v_reference_ms,v_operation_ms,0,0,0,0
    );
    RETURN;
  END IF;

  v_after:=NULL;v_receipt:=NULL;v_rows_seen:=0;
  LOOP
    v_started:=pg_catalog.clock_timestamp();
    v_result:=public.pay_workbench_settled_certificate_entry_page_v8(
      v_operation_id,v_certification_id,v_after,256,v_receipt);
    v_elapsed:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
    v_read_max_ms:=GREATEST(v_read_max_ms,v_elapsed);v_entry_calls:=v_entry_calls+1;
    IF v_elapsed>15000 THEN RAISE EXCEPTION 'H1_V8_ENTRY_PAGE_BUDGET_EXCEEDED: %',v_elapsed; END IF;
    v_rows_seen:=v_rows_seen+(v_result->>'row_count')::integer;
    IF v_entry_calls % 50 = 0 THEN
      RAISE NOTICE 'H1_V8_SCALE_PHASE entry_progress target=% calls=% rows=% max_ms=%',
        v_target,v_entry_calls,v_rows_seen,ROUND(v_read_max_ms,3);
    END IF;
    EXIT WHEN v_result->'has_more'='false'::jsonb;
    v_after:=(v_result->>'next_after_ordinal')::integer;v_receipt:=v_result->>'page_receipt_digest_sha256';
  END LOOP;
  IF v_rows_seen<>v_target THEN RAISE EXCEPTION 'H1_V8_ENTRY_STREAM_COUNT_MISMATCH'; END IF;
  RAISE NOTICE 'H1_V8_SCALE_PHASE entry_pages_complete target=% calls=%',v_target,v_entry_calls;

  v_after:=NULL;v_receipt:=NULL;v_rows_seen:=0;
  LOOP
    v_started:=pg_catalog.clock_timestamp();
    v_result:=public.pay_workbench_settled_certificate_partition_page_v8(
      v_operation_id,v_certification_id,v_after,256,v_receipt);
    v_elapsed:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
    v_read_max_ms:=GREATEST(v_read_max_ms,v_elapsed);v_partition_calls:=v_partition_calls+1;
    IF v_elapsed>15000 THEN RAISE EXCEPTION 'H1_V8_PARTITION_PAGE_BUDGET_EXCEEDED: %',v_elapsed; END IF;
    v_rows_seen:=v_rows_seen+(v_result->>'row_count')::integer;
    IF v_partition_calls % 50 = 0 THEN
      RAISE NOTICE 'H1_V8_SCALE_PHASE partition_progress target=% calls=% rows=% max_ms=%',
        v_target,v_partition_calls,v_rows_seen,ROUND(v_read_max_ms,3);
    END IF;
    EXIT WHEN v_result->'has_more'='false'::jsonb;
    v_after:=(v_result->>'next_after_ordinal')::integer;v_receipt:=v_result->>'page_receipt_digest_sha256';
  END LOOP;
  IF v_rows_seen<>v_target THEN RAISE EXCEPTION 'H1_V8_PARTITION_STREAM_COUNT_MISMATCH'; END IF;
  RAISE NOTICE 'H1_V8_SCALE_PHASE partition_pages_complete target=% calls=%',v_target,v_partition_calls;

  FOREACH v_kind IN ARRAY ARRAY['ALL_SAME_ECONOMIC_KEY','FULL_SIGNED_PRE_SIGNATURE','DECISIVE_SIGNED_EVIDENCE'] LOOP
    v_after:=NULL;v_receipt:=NULL;v_rows_seen:=0;
    LOOP
      v_started:=pg_catalog.clock_timestamp();
      v_result:=public.pay_workbench_settled_certificate_component_page_v8(
        v_operation_id,v_certification_id,v_kind,v_after,256,v_receipt);
      v_elapsed:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
      v_read_max_ms:=GREATEST(v_read_max_ms,v_elapsed);v_component_calls:=v_component_calls+1;
      IF v_elapsed>15000 THEN RAISE EXCEPTION 'H1_V8_COMPONENT_PAGE_BUDGET_EXCEEDED: %',v_elapsed; END IF;
      v_rows_seen:=v_rows_seen+(v_result->>'row_count')::integer;
      IF v_component_calls % 50 = 0 THEN
        RAISE NOTICE 'H1_V8_SCALE_PHASE component_progress target=% kind=% cumulative_calls=% rows=% max_ms=%',
          v_target,v_kind,v_component_calls,v_rows_seen,ROUND(v_read_max_ms,3);
      END IF;
      EXIT WHEN v_result->'has_more'='false'::jsonb;
      v_after:=(v_result->>'next_after_ordinal')::integer;v_receipt:=v_result->>'page_receipt_digest_sha256';
    END LOOP;
    IF v_rows_seen<>v_target THEN RAISE EXCEPTION 'H1_V8_COMPONENT_STREAM_COUNT_MISMATCH: %',v_kind; END IF;
    RAISE NOTICE 'H1_V8_SCALE_PHASE component_pages_complete target=% kind=% cumulative_calls=%',
      v_target,v_kind,v_component_calls;
  END LOOP;

  UPDATE private.banking_pay_workbench_settled_certificate_operation_links_v8 link
  SET link_state='FROZEN'
  WHERE link.operation_id=v_operation_id AND link.certificate_uuid=v_certificate_uuid
    AND link.certification_id=v_certification_id AND link.link_state='STAGING';
  IF NOT FOUND THEN RAISE EXCEPTION 'H1_V8_FINAL_FREEZE_EXACT_UPDATE_FAILED'; END IF;
  UPDATE private.banking_pay_workbench_settled_certificate_operation_links_v8 link
  SET link_state='FROZEN'
  WHERE link.operation_id=v_operation_id AND link.certificate_uuid=v_certificate_uuid
    AND link.certification_id=v_certification_id AND link.link_state='STAGING';
  GET DIAGNOSTICS v_update_count = ROW_COUNT;
  IF v_update_count<>0 THEN
    RAISE EXCEPTION 'H1_V8_FINAL_FREEZE_REPLAY_MUTATED';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 link
    WHERE link.operation_id=v_operation_id AND link.certificate_uuid=v_certificate_uuid
      AND link.certification_id=v_certification_id AND link.link_state='FROZEN'
  ) THEN RAISE EXCEPTION 'H1_V8_FINAL_FREEZE_REPLAY_IDENTITY_MISMATCH'; END IF;
  v_caught:=false;
  BEGIN
    PERFORM public.pay_workbench_settled_certificate_filter_manifest_v8(v_operation_id,v_certification_id);
  EXCEPTION WHEN OTHERS THEN
    v_caught:=SQLERRM='WORKBENCH_CERTIFICATE_NOT_CURRENT';
  END;
  IF NOT v_caught THEN RAISE EXCEPTION 'H1_V8_POST_FROZEN_READER_DID_NOT_FAIL_CLOSED'; END IF;

  INSERT INTO h1_v8_scale_result(
    certificate_uuid,certification_id,operation_id,target_count,candidate_count,
    build_start_ms,append_call_count,append_max_ms,seal_call_count,seal_max_ms,
    reference_issue_ms,operation_start_ms,entry_page_call_count,
    partition_page_call_count,component_page_call_count,read_page_max_ms
  ) VALUES (
    v_certificate_uuid,v_certification_id,v_operation_id,v_target,v_candidate_count,
    v_build_start_ms,v_append_calls,v_append_max_ms,v_seal_calls,v_seal_max_ms,
    v_reference_ms,v_operation_ms,v_entry_calls,v_partition_calls,v_component_calls,v_read_max_ms
  );
  RAISE NOTICE 'H1_V8_SCALE_RESULT %',(
    SELECT pg_catalog.to_jsonb(result)-'certificate_uuid'-'certification_id'-'operation_id'
    FROM h1_v8_scale_result result WHERE result.target_count=v_target);
END;
$verification$;

\if :h2_transport
DO $h2_transport$
DECLARE
  v_operation_id uuid := (SELECT operation_id FROM h1_v8_scale_result);
  v_target integer := (SELECT target_count FROM h1_v8_scale_result);
  v_expected_partitions integer := (SELECT candidate_count FROM h1_v8_scale_result);
  v_admission jsonb;
  v_stage jsonb;
  v_replay jsonb;
  v_started timestamptz;
  v_elapsed numeric;
  v_max_call_ms numeric := 0;
  v_calls integer := 0;
  v_phase text;
BEGIN
  v_started:=pg_catalog.clock_timestamp();
  v_admission:=private.pay_workbench_settled_certificate_operation_admit_v8(v_operation_id);
  v_elapsed:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
  v_max_call_ms:=GREATEST(v_max_call_ms,v_elapsed);
  IF v_elapsed>6000
     OR v_admission->'ok' IS DISTINCT FROM 'true'::jsonb
     OR v_admission->>'freeze_state'<>'STAGING' THEN
    RAISE EXCEPTION 'H2_V8_SCALE_ADMISSION_FAILED_OR_OVER_BUDGET:%:%',v_elapsed,v_admission;
  END IF;

  LOOP
    v_calls:=v_calls+1;
    IF v_calls>100 THEN RAISE EXCEPTION 'H2_V8_SCALE_STAGING_DID_NOT_TERMINATE'; END IF;
    v_started:=pg_catalog.clock_timestamp();
    v_stage:=public.banking_pay_draft_certificate_stage_advance_v8(
      v_operation_id,'H2_V8_SCALE_TRANSPORT');
    v_elapsed:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
    v_max_call_ms:=GREATEST(v_max_call_ms,v_elapsed);
    IF v_elapsed>15000 THEN
      RAISE EXCEPTION 'H2_V8_SCALE_STAGE_CALL_OVER_BUDGET:%:%',v_elapsed,v_stage;
    END IF;
    SELECT phase INTO STRICT v_phase FROM public.banking_pay_operations WHERE id=v_operation_id;
    EXIT WHEN v_phase='DRAIN_TSFIN';
  END LOOP;

  IF (SELECT count(*) FROM private.banking_pay_draft_frozen_constituent_refs_v8
      WHERE operation_id=v_operation_id)<>v_target
     OR (SELECT count(*) FROM private.banking_pay_draft_frozen_constituent_payloads_v8
         WHERE operation_id=v_operation_id)<>v_target
     OR (SELECT count(*) FROM private.banking_pay_draft_frozen_candidate_scope_members_v8
         WHERE operation_id=v_operation_id)<>v_target
     OR (SELECT count(*) FROM private.banking_pay_draft_frozen_candidate_scopes_v8
         WHERE operation_id=v_operation_id AND scope_state='FROZEN')<>v_expected_partitions
     OR EXISTS (
       SELECT 1 FROM public.banking_pay_operation_candidate_scope AS candidate_scope
       WHERE candidate_scope.operation_id=v_operation_id
         AND (
           candidate_scope.selected_preview_row_ids_json<>'[]'::jsonb
           OR candidate_scope.selected_timesheet_ids_json<>'[]'::jsonb
           OR candidate_scope.selected_finance_case_ids_json<>'[]'::jsonb
           OR candidate_scope.selected_canonical_preview_lines_json<>'[]'::jsonb
           OR candidate_scope.effective_canonical_preview_lines_json<>'[]'::jsonb
         )
     ) THEN
    RAISE EXCEPTION 'H2_V8_SCALE_TRANSPORT_COUNT_OR_ARRAY_MISMATCH';
  END IF;

  v_replay:=public.banking_pay_draft_certificate_stage_advance_v8(
    v_operation_id,'H2_V8_SCALE_TRANSPORT');
  IF v_replay->>'work_kind'<>'CERTIFICATE_STAGE_ALREADY_COMPLETE'
     OR v_replay->'replayed' IS DISTINCT FROM 'true'::jsonb THEN
    RAISE EXCEPTION 'H2_V8_SCALE_RESPONSE_LOSS_REPLAY_CHANGED:%',v_replay;
  END IF;

  RAISE NOTICE 'H2_V8_SCALE_TRANSPORT_RESULT %',pg_catalog.jsonb_build_object(
    'target_count',v_target,
    'partition_count',v_expected_partitions,
    'stage_call_count',v_calls,
    'max_stage_or_admission_ms',ROUND(v_max_call_ms,3),
    'final_phase',v_phase,
    'response_loss_replay',true,
    'expanded_candidate_scope_arrays',false,
    'transaction_outcome','ROLLBACK'
  );
END;
$h2_transport$;
\endif

\if :worker_seed_only
-- The scope-generation bump is a deferred transaction authority.  Force it
-- before committing a seed that will be consumed by a separate PostgREST
-- transaction, then record the exact settled generation on the fixture
-- session.  The ordinary rollback benchmark remains byte-for-byte unchanged.
SET CONSTRAINTS ALL IMMEDIATE;
DELETE FROM public.banking_pay_workbench_jobs job
USING public.candidates candidate
WHERE job.candidate_id=candidate.id
  AND candidate.display_name LIKE 'H1-V8-SCALE-%';
UPDATE public.banking_pay_workbench_sessions
SET scope_change_generation_target=public.pay_workbench_scope_current_generation_v1(),
    scope_change_generation_applied=public.pay_workbench_scope_current_generation_v1(),
    scope_change_generation_shadow_checked=public.pay_workbench_scope_current_generation_v1()
WHERE id='20000000-0000-4000-8000-000000000001'::uuid;
\endif

-- These plans are evidence only. At scale they must use the certificate/keyset,
-- channel-leading partition-member and exact operation-link indexes.
EXPLAIN (COSTS OFF)
SELECT entry.constituent_ordinal
FROM private.banking_pay_workbench_settled_certificate_entries_v8 entry
WHERE entry.certificate_uuid=(SELECT certificate_uuid FROM h1_v8_scale_result)
  AND entry.constituent_ordinal>0
ORDER BY entry.constituent_ordinal
LIMIT 257;

EXPLAIN (COSTS OFF)
SELECT member.stream_ordinal
FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 member
JOIN private.banking_pay_workbench_settled_certificate_partitions_v8 partition
  ON partition.certificate_uuid=member.certificate_uuid
 AND partition.partition_ordinal=member.partition_ordinal
WHERE member.certificate_uuid=(SELECT certificate_uuid FROM h1_v8_scale_result)
  AND partition.resolved_pay_channel='PAYE'
  AND member.stream_ordinal>0
ORDER BY member.stream_ordinal
LIMIT 257;

EXPLAIN (COSTS OFF)
SELECT link.link_state
FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 link
WHERE link.operation_id=(SELECT operation_id FROM h1_v8_scale_result);

\if :worker_seed_only
COMMIT;
\else
ROLLBACK;
\endif
