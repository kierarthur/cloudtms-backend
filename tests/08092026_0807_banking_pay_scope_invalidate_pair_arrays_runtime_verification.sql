\set ON_ERROR_STOP on

-- Local-owner rollback-only proof for the pair-array invalidator. This fixture
-- deliberately uses SET ROLE and session_replication_role and therefore is not
-- a release verifier. The existing 0521 fixture remains the full Candidate
-- cohort matrix; this file is bounded to the transitive pg_temp boundary and
-- exact invalidator compatibility.
BEGIN;
SET LOCAL statement_timeout = '15s';
SET LOCAL lock_timeout = '1500ms';

CREATE TEMP TABLE pg_temp._bpay_pair_array_runtime_state_v1(
  fixture_prefix text NOT NULL,
  tms_ref_base integer NOT NULL,
  main_candidate_id uuid NOT NULL,
  trigger_candidate_id uuid NOT NULL,
  null_scope_candidate_id uuid NOT NULL,
  alignment_candidate_low_id uuid NOT NULL,
  alignment_candidate_high_id uuid NOT NULL,
  main_timesheet_id uuid NOT NULL,
  trigger_timesheet_id uuid NOT NULL,
  alignment_timesheet_for_low_id uuid NOT NULL,
  alignment_timesheet_for_high_id uuid NOT NULL,
  trigger_advance_id uuid NOT NULL,
  route_job_id uuid NULL,
  raw_job_id uuid NOT NULL,
  poison_oid oid NULL,
  cohort_token uuid NULL,
  cohort_generation bigint NULL,
  cohort_source_seq bigint NULL
) ON COMMIT DROP;

INSERT INTO pg_temp._bpay_pair_array_runtime_state_v1(
  fixture_prefix,tms_ref_base,main_candidate_id,trigger_candidate_id,
  null_scope_candidate_id,
  alignment_candidate_low_id,alignment_candidate_high_id,
  main_timesheet_id,trigger_timesheet_id,
  alignment_timesheet_for_low_id,alignment_timesheet_for_high_id,
  trigger_advance_id,raw_job_id
)
SELECT 'BPAY-SCOPE-PAIR-0807:'||pg_catalog.gen_random_uuid()::text,
       1700000000+(pg_catalog.floor(pg_catalog.random()*300000000))::integer,
       pg_catalog.gen_random_uuid(),pg_catalog.gen_random_uuid(),
       pg_catalog.gen_random_uuid(),
       (pg_catalog.substr(identity_seed.uuid_text,1,32)||'0101')::uuid,
       (pg_catalog.substr(identity_seed.uuid_text,1,32)||'0102')::uuid,
       pg_catalog.gen_random_uuid(),pg_catalog.gen_random_uuid(),
       (pg_catalog.substr(identity_seed.uuid_text,1,32)||'0202')::uuid,
       (pg_catalog.substr(identity_seed.uuid_text,1,32)||'0201')::uuid,
       pg_catalog.gen_random_uuid(),pg_catalog.gen_random_uuid()
FROM (SELECT pg_catalog.gen_random_uuid()::text AS uuid_text) AS identity_seed;

GRANT SELECT,UPDATE ON pg_temp._bpay_pair_array_runtime_state_v1 TO service_role;

CREATE TEMP TABLE pg_temp._bpay_pair_array_scale_candidates_v1(
  ordinal integer PRIMARY KEY,
  candidate_id uuid NOT NULL UNIQUE,
  timesheet_id uuid NOT NULL UNIQUE
) ON COMMIT DROP;

INSERT INTO pg_temp._bpay_pair_array_scale_candidates_v1(
  ordinal,candidate_id,timesheet_id
)
SELECT scale_ordinal,pg_catalog.gen_random_uuid(),pg_catalog.gen_random_uuid()
FROM pg_catalog.generate_series(1,5000) AS scale_ordinal;

-- The poison relation is owned by the lower-privileged caller. The vulnerable
-- historical invalidator selects this fixed pg_temp name and fails at TRUNCATE.
SET LOCAL ROLE service_role;

CREATE TEMP TABLE pg_temp._bpay_pair_array_poison_observed_v1(
  operation text NOT NULL
) ON COMMIT DROP;

CREATE OR REPLACE FUNCTION pg_temp._bpay_pair_array_poison_trigger_v1()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO pg_temp._bpay_pair_array_poison_observed_v1(operation)
  VALUES (TG_OP);
  IF TG_LEVEL = 'ROW' THEN
    RETURN NEW;
  END IF;
  RETURN NULL;
END;
$function$;

CREATE TEMP TABLE pg_temp._bpay_wb_invalidation_pairs_v1(
  hostile_payload text NOT NULL
) ON COMMIT DROP;

INSERT INTO pg_temp._bpay_wb_invalidation_pairs_v1(hostile_payload)
VALUES ('CALLER_OWNED_SENTINEL');

CREATE TRIGGER hostile_pair_insert
BEFORE INSERT ON pg_temp._bpay_wb_invalidation_pairs_v1
FOR EACH ROW EXECUTE FUNCTION pg_temp._bpay_pair_array_poison_trigger_v1();

CREATE TRIGGER hostile_pair_truncate
BEFORE TRUNCATE ON pg_temp._bpay_wb_invalidation_pairs_v1
FOR EACH STATEMENT EXECUTE FUNCTION pg_temp._bpay_pair_array_poison_trigger_v1();

UPDATE pg_temp._bpay_pair_array_runtime_state_v1
SET poison_oid=pg_catalog.to_regclass(
  'pg_temp._bpay_wb_invalidation_pairs_v1'
)::oid;

DO $poison_setup$
DECLARE
  v_poison_oid oid;
BEGIN
  SELECT poison_oid INTO STRICT v_poison_oid
  FROM pg_temp._bpay_pair_array_runtime_state_v1;
  IF v_poison_oid IS NULL
     OR pg_catalog.pg_get_userbyid((
          SELECT relation.relowner
          FROM pg_catalog.pg_class AS relation
          WHERE relation.oid=v_poison_oid
        )) IS DISTINCT FROM 'service_role'
     OR (SELECT count(*) FROM pg_temp._bpay_wb_invalidation_pairs_v1)
          IS DISTINCT FROM 1
     OR (SELECT count(*) FROM pg_temp._bpay_pair_array_poison_observed_v1)
          IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'CALLER_OWNED_POISON_SETUP_FAILED';
  END IF;
END;
$poison_setup$;

RESET ROLE;

-- Seed only local rollback fixtures. Replica mode prevents unrelated trigger
-- fanout while the factual Candidate/timesheet ownership rows are established.
SET LOCAL session_replication_role = 'replica';

INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
SELECT fixture.main_candidate_id,
       fixture.fixture_prefix||':MAIN',
       'CCR-'||(fixture.tms_ref_base-2)::text,
       'PAYE'
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.trigger_candidate_id,
       fixture.fixture_prefix||':TRIGGER',
       'CCR-'||(fixture.tms_ref_base-1)::text,
       'PAYE'
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.null_scope_candidate_id,
       fixture.fixture_prefix||':NULL_SCOPE',
       'CCR-'||(fixture.tms_ref_base-3)::text,
       'PAYE'
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.alignment_candidate_low_id,
       fixture.fixture_prefix||':ALIGNMENT_LOW',
       'CCR-'||(fixture.tms_ref_base-4)::text,
       'PAYE'
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.alignment_candidate_high_id,
       fixture.fixture_prefix||':ALIGNMENT_HIGH',
       'CCR-'||(fixture.tms_ref_base-5)::text,
       'PAYE'
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT scale_candidate.candidate_id,
       fixture.fixture_prefix||':SCALE:'||scale_candidate.ordinal::text,
       'CCR-'||(fixture.tms_ref_base+scale_candidate.ordinal)::text,
       'PAYE'
FROM pg_temp._bpay_pair_array_scale_candidates_v1 AS scale_candidate
CROSS JOIN pg_temp._bpay_pair_array_runtime_state_v1 AS fixture;

INSERT INTO public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
  job_title_norm,week_ending_date,status,is_current,version
)
SELECT fixture.main_timesheet_id,(fixture.tms_ref_base-2)::text,
       fixture.fixture_prefix,'PAIR_ARRAY','PAIR_ARRAY','PAIR_ARRAY',
       DATE '2099-09-06','RECEIVED'::public.timesheet_status_enum,true,1
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.trigger_timesheet_id,(fixture.tms_ref_base-1)::text,
       fixture.fixture_prefix,'PAIR_ARRAY','PAIR_ARRAY','PAIR_ARRAY',
       DATE '2099-09-06','RECEIVED'::public.timesheet_status_enum,true,1
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.alignment_timesheet_for_low_id,(fixture.tms_ref_base-4)::text,
       fixture.fixture_prefix,'PAIR_ARRAY','PAIR_ARRAY','PAIR_ARRAY',
       DATE '2099-09-06','RECEIVED'::public.timesheet_status_enum,true,1
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.alignment_timesheet_for_high_id,(fixture.tms_ref_base-5)::text,
       fixture.fixture_prefix,'PAIR_ARRAY','PAIR_ARRAY','PAIR_ARRAY',
       DATE '2099-09-06','RECEIVED'::public.timesheet_status_enum,true,1
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT scale_candidate.timesheet_id,
       (fixture.tms_ref_base+scale_candidate.ordinal)::text,
       fixture.fixture_prefix,'PAIR_ARRAY','PAIR_ARRAY','PAIR_ARRAY',
       DATE '2099-09-06','RECEIVED'::public.timesheet_status_enum,true,1
FROM pg_temp._bpay_pair_array_scale_candidates_v1 AS scale_candidate
CROSS JOIN pg_temp._bpay_pair_array_runtime_state_v1 AS fixture;

INSERT INTO public.timesheets_financials(
  timesheet_id,timesheet_version,is_current,candidate_id,
  candidate_assignment,processing_status,pay_on_hold
)
SELECT fixture.main_timesheet_id,1,true,fixture.main_candidate_id,
       'ASSIGNED'::public.candidate_assignment_enum,
       'READY_FOR_HR'::public.ts_fin_processing_status_enum,false
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.trigger_timesheet_id,1,true,fixture.trigger_candidate_id,
       'ASSIGNED'::public.candidate_assignment_enum,
       'READY_FOR_HR'::public.ts_fin_processing_status_enum,false
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.alignment_timesheet_for_low_id,1,true,
       fixture.alignment_candidate_low_id,
       'ASSIGNED'::public.candidate_assignment_enum,
       'READY_FOR_HR'::public.ts_fin_processing_status_enum,false
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT fixture.alignment_timesheet_for_high_id,1,true,
       fixture.alignment_candidate_high_id,
       'ASSIGNED'::public.candidate_assignment_enum,
       'READY_FOR_HR'::public.ts_fin_processing_status_enum,false
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT scale_candidate.timesheet_id,1,true,scale_candidate.candidate_id,
       'ASSIGNED'::public.candidate_assignment_enum,
       'READY_FOR_HR'::public.ts_fin_processing_status_enum,false
FROM pg_temp._bpay_pair_array_scale_candidates_v1 AS scale_candidate;

INSERT INTO public.app_change_counters(entity_key,seq,scope_change_generation)
SELECT 'pay_candidate:'||fixture.main_candidate_id::text,1000,0
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT 'pay_candidate:'||fixture.trigger_candidate_id::text,1000,0
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT 'pay_candidate:'||fixture.null_scope_candidate_id::text,1000,0
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT 'pay_candidate:'||fixture.alignment_candidate_low_id::text,1000,0
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
UNION ALL
SELECT 'pay_candidate:'||fixture.alignment_candidate_high_id::text,1000,0
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
ON CONFLICT(entity_key) DO UPDATE
SET seq=GREATEST(public.app_change_counters.seq,EXCLUDED.seq);

SET LOCAL session_replication_role = 'origin';

-- Exercise both installed SECURITY DEFINER statement-trigger families through
-- their table owner while the lower caller continues to own the poison. The
-- actual public invalidator and processor routes below remain service_role.

INSERT INTO public.pay_advances(
  id,candidate_id,reason,original_amount,outstanding_amount,status,
  advance_kind,linked_timesheet_id,case_type
)
SELECT fixture.trigger_advance_id,fixture.trigger_candidate_id,
       'MANUAL_ADVANCE',10.00,10.00,'ACTIVE','LEGACY_ADVANCE',
       fixture.trigger_timesheet_id,'PAYMENT_ADVANCE'
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture;

DO $financial_trigger_assertion$
DECLARE
  v_candidate_id uuid;
BEGIN
  SELECT trigger_candidate_id INTO STRICT v_candidate_id
  FROM pg_temp._bpay_pair_array_runtime_state_v1;
  IF (SELECT registry.last_dirty_reason
      FROM private.banking_pay_workbench_candidate_scope_registry AS registry
      WHERE registry.candidate_id=v_candidate_id)
       IS DISTINCT FROM 'PAY_ADVANCES_INSERT' THEN
    RAISE EXCEPTION 'FINANCIAL_TRANSITION_TRIGGER_FAMILY_DID_NOT_INVALIDATE';
  END IF;
END;
$financial_trigger_assertion$;

UPDATE public.timesheets_financials AS financial
SET pay_on_hold=true
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
WHERE financial.timesheet_id=fixture.trigger_timesheet_id
  AND financial.is_current;

SET LOCAL ROLE service_role;

DO $service_poison_after_triggers$
DECLARE
  v_poison_oid oid;
BEGIN
  SELECT poison_oid INTO STRICT v_poison_oid
  FROM pg_temp._bpay_pair_array_runtime_state_v1;
  IF pg_catalog.to_regclass('pg_temp._bpay_wb_invalidation_pairs_v1')::oid
       IS DISTINCT FROM v_poison_oid
     OR (SELECT hostile_payload
         FROM pg_temp._bpay_wb_invalidation_pairs_v1)
          IS DISTINCT FROM 'CALLER_OWNED_SENTINEL'
     OR (SELECT count(*) FROM pg_temp._bpay_pair_array_poison_observed_v1)
          IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'CALLER_OWNED_POISON_TOUCHED_BY_TRIGGER_FAMILY';
  END IF;
END;
$service_poison_after_triggers$;

RESET ROLE;

DO $summary_trigger_assertion$
DECLARE
  v_candidate_id uuid;
BEGIN
  SELECT trigger_candidate_id INTO STRICT v_candidate_id
  FROM pg_temp._bpay_pair_array_runtime_state_v1;
  IF (SELECT registry.last_dirty_reason
      FROM private.banking_pay_workbench_candidate_scope_registry AS registry
      WHERE registry.candidate_id=v_candidate_id)
       IS DISTINCT FROM 'DIRTY_TRIGGER:TIMESHEETS_FINANCIALS:UPDATE' THEN
    RAISE EXCEPTION 'SUMMARY_TRIGGER_FAMILY_DID_NOT_INVALIDATE';
  END IF;
END;
$summary_trigger_assertion$;

SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE;
SELECT pg_catalog.set_config('cloudtms.scope_generation_finalising','false',true);
SELECT pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token','',true);
SELECT pg_catalog.set_config('cloudtms.bpay_scope_invalidator_active','false',true);
SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED;

UPDATE public.banking_pay_workbench_jobs AS job
SET status='SUCCEEDED',completed_at_utc=pg_catalog.clock_timestamp(),
    updated_at_utc=pg_catalog.clock_timestamp()
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
WHERE job.candidate_id=fixture.trigger_candidate_id
  AND job.status IN ('QUEUED','RUNNING');

-- Snapshot exact economic table multisets after the deliberate trigger-family
-- fixture mutations. All following invalidator/processor calls are Policy X.
CREATE TEMP TABLE pg_temp._bpay_pair_array_economic_fingerprints_v1(
  relation_name text PRIMARY KEY,
  row_count bigint NOT NULL,
  row_digest text NOT NULL
) ON COMMIT DROP;

DO $economic_snapshot$
DECLARE
  v_relation regclass;
  v_count bigint;
  v_digest text;
BEGIN
  FOREACH v_relation IN ARRAY ARRAY[
    'public.banking_pay_operations'::regclass,
    'public.pay_batches'::regclass,
    'public.pay_batch_items'::regclass,
    'public.banking_pay_operation_provider_attempts'::regclass,
    'public.banking_pay_operation_settlement_scope'::regclass,
    'public.banking_pay_operation_remittance_scope'::regclass,
    'public.pay_advances'::regclass,
    'public.pay_finance_case_components'::regclass,
    'public.pay_bank_transfers'::regclass,
    'public.pay_bank_transfer_events'::regclass,
    'public.timesheets_financials'::regclass
  ] LOOP
    EXECUTE pg_catalog.format(
      'SELECT count(*)::bigint,pg_catalog.md5(COALESCE(pg_catalog.string_agg(pg_catalog.to_jsonb(economic_row)::text,E''\n'' ORDER BY pg_catalog.to_jsonb(economic_row)::text),'''')) FROM %s AS economic_row',
      v_relation
    ) INTO v_count,v_digest;
    INSERT INTO pg_temp._bpay_pair_array_economic_fingerprints_v1(
      relation_name,row_count,row_digest
    ) VALUES (v_relation::text,v_count,v_digest);
  END LOOP;
END;
$economic_snapshot$;

-- Exact direct semantics: paired duplicate collapse, null Candidate filtering,
-- nullable full-scope member retention, same-token replay and typed failures.
DO $direct_semantics$
DECLARE
  v_candidate_id uuid;
  v_other_candidate_id uuid;
  v_null_scope_candidate_id uuid;
  v_alignment_candidate_low_id uuid;
  v_alignment_candidate_high_id uuid;
  v_timesheet_id uuid;
  v_other_timesheet_id uuid;
  v_alignment_timesheet_for_low_id uuid;
  v_alignment_timesheet_for_high_id uuid;
  v_result jsonb;
  v_replay jsonb;
  v_token uuid;
  v_job_count_before bigint;
  v_registry_count_before bigint;
  v_state_count_before bigint;
  v_error_detail text;
  v_rows integer;
BEGIN
  SELECT main_candidate_id,trigger_candidate_id,null_scope_candidate_id,
         alignment_candidate_low_id,alignment_candidate_high_id,
         main_timesheet_id,trigger_timesheet_id,
         alignment_timesheet_for_low_id,alignment_timesheet_for_high_id
  INTO STRICT v_candidate_id,v_other_candidate_id,v_null_scope_candidate_id,
              v_alignment_candidate_low_id,v_alignment_candidate_high_id,
              v_timesheet_id,v_other_timesheet_id,
              v_alignment_timesheet_for_low_id,
              v_alignment_timesheet_for_high_id
  FROM pg_temp._bpay_pair_array_runtime_state_v1;

  SELECT count(*) INTO STRICT v_job_count_before
  FROM public.banking_pay_workbench_jobs;
  SELECT count(*) INTO STRICT v_registry_count_before
  FROM private.banking_pay_workbench_candidate_scope_registry;
  SELECT count(*) INTO STRICT v_state_count_before
  FROM private.banking_pay_workbench_timesheet_scope_state;

  v_result:=private.pay_workbench_scope_invalidate_v1(
    NULL::uuid[],NULL::uuid[],'PAIR_ARRAY_NULL_ARRAYS',NULL::uuid,
    pg_catalog.jsonb_build_object('skip_candidate_job_enqueue',true)
  );
  IF v_result IS DISTINCT FROM pg_catalog.jsonb_build_object(
       'ok',true,'candidate_count',0,'timesheet_count',0,
       'registry_inserted_count',0,'registry_updated_count',0,
       'state_inserted_count',0,'state_updated_count',0,
       'job_inserted_count',0,'job_coalesced_count',0,
       'scope_change_tx_token',NULL,'reason','PAIR_ARRAY_NULL_ARRAYS'
     ) THEN
    RAISE EXCEPTION 'PAIR_ARRAY_NULL_ARRAYS_ZERO_SHAPE_FAILED'
      USING DETAIL=v_result::text;
  END IF;

  v_result:=private.pay_workbench_scope_invalidate_v1(
    ARRAY[]::uuid[],ARRAY[]::uuid[],'PAIR_ARRAY_EMPTY_ARRAYS',NULL::uuid,
    pg_catalog.jsonb_build_object('skip_candidate_job_enqueue',true)
  );
  IF v_result IS DISTINCT FROM pg_catalog.jsonb_build_object(
       'ok',true,'candidate_count',0,'timesheet_count',0,
       'registry_inserted_count',0,'registry_updated_count',0,
       'state_inserted_count',0,'state_updated_count',0,
       'job_inserted_count',0,'job_coalesced_count',0,
       'scope_change_tx_token',NULL,'reason','PAIR_ARRAY_EMPTY_ARRAYS'
     )
     OR (SELECT count(*) FROM public.banking_pay_workbench_jobs)
          IS DISTINCT FROM v_job_count_before
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_candidate_scope_registry)
          IS DISTINCT FROM v_registry_count_before
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_timesheet_scope_state)
          IS DISTINCT FROM v_state_count_before THEN
    RAISE EXCEPTION 'PAIR_ARRAY_EMPTY_ARRAYS_ZERO_SHAPE_FAILED'
      USING DETAIL=v_result::text;
  END IF;

  v_result:=private.pay_workbench_scope_invalidate_v1(
    ARRAY[
      v_candidate_id,v_other_candidate_id,v_null_scope_candidate_id,
      NULL::uuid,v_candidate_id
    ],
    ARRAY[
      v_timesheet_id,v_other_timesheet_id,NULL::uuid,
      v_timesheet_id,v_timesheet_id
    ],
    'PAIR_ARRAY_DUPLICATE_NULL',NULL::uuid,
    pg_catalog.jsonb_build_object(
      'skip_candidate_job_enqueue',true,
      'latest_source_change_seq',1000
    )
  );
  v_token:=(v_result->>'scope_change_tx_token')::uuid;

  IF COALESCE((v_result->>'ok')::boolean,false) IS NOT TRUE
     OR (v_result->>'candidate_count')::integer IS DISTINCT FROM 3
     OR (v_result->>'timesheet_count')::integer IS DISTINCT FROM 2
     OR (v_result->>'registry_inserted_count')::integer IS DISTINCT FROM 2
     OR (v_result->>'registry_updated_count')::integer IS DISTINCT FROM 1
     OR (v_result->>'state_inserted_count')::integer IS DISTINCT FROM 1
     OR (v_result->>'state_updated_count')::integer IS DISTINCT FROM 1
     OR (v_result->>'job_inserted_count')::integer IS DISTINCT FROM 0
     OR (v_result->>'job_coalesced_count')::integer IS DISTINCT FROM 0
     OR v_token IS NULL
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_candidate_scope_registry AS registry
         WHERE registry.candidate_id IN (
           v_candidate_id,v_other_candidate_id,v_null_scope_candidate_id
         )
           AND registry.last_scope_change_tx_token=v_token)
          IS DISTINCT FROM 3
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
         WHERE (scope_state.candidate_id,scope_state.timesheet_id) IN (
           (v_candidate_id,v_timesheet_id),
           (v_other_candidate_id,v_other_timesheet_id)
         )
           AND scope_state.last_scope_change_tx_token=v_token)
          IS DISTINCT FROM 2
     OR EXISTS (
          SELECT 1
          FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
          WHERE scope_state.candidate_id=v_null_scope_candidate_id
        )
     OR (SELECT count(*) FROM public.banking_pay_workbench_jobs)
          IS DISTINCT FROM v_job_count_before THEN
    RAISE EXCEPTION 'PAIR_ARRAY_DUPLICATE_NULL_SEMANTICS_FAILED'
      USING DETAIL=v_result::text;
  END IF;

  v_replay:=private.pay_workbench_scope_invalidate_v1(
    ARRAY[
      v_candidate_id,v_other_candidate_id,v_null_scope_candidate_id,
      NULL::uuid,v_candidate_id
    ],
    ARRAY[
      v_timesheet_id,v_other_timesheet_id,NULL::uuid,
      v_timesheet_id,v_timesheet_id
    ],
    'PAIR_ARRAY_DUPLICATE_NULL',v_token,
    pg_catalog.jsonb_build_object(
      'skip_candidate_job_enqueue',true,
      'latest_source_change_seq',1000
    )
  );

  IF (v_replay->>'scope_change_tx_token')::uuid IS DISTINCT FROM v_token
     OR (v_replay->>'candidate_count')::integer IS DISTINCT FROM 3
     OR (v_replay->>'timesheet_count')::integer IS DISTINCT FROM 2
     OR (v_replay->>'registry_inserted_count')::integer IS DISTINCT FROM 0
     OR (v_replay->>'registry_updated_count')::integer IS DISTINCT FROM 3
     OR (v_replay->>'state_inserted_count')::integer IS DISTINCT FROM 0
     OR (v_replay->>'state_updated_count')::integer IS DISTINCT FROM 2
     OR (v_replay->>'job_inserted_count')::integer IS DISTINCT FROM 0
     OR (SELECT count(*) FROM public.banking_pay_workbench_jobs)
          IS DISTINCT FROM v_job_count_before THEN
    RAISE EXCEPTION 'PAIR_ARRAY_PENDING_TOKEN_REPLAY_FAILED'
      USING DETAIL=v_replay::text;
  END IF;

  v_result:=private.pay_workbench_scope_invalidate_v1(
    ARRAY[NULL::uuid],ARRAY[v_timesheet_id],
    'PAIR_ARRAY_NULL_CANDIDATE',NULL::uuid,
    pg_catalog.jsonb_build_object('skip_candidate_job_enqueue',true)
  );
  IF v_result IS DISTINCT FROM pg_catalog.jsonb_build_object(
       'ok',true,'candidate_count',0,'timesheet_count',0,
       'registry_inserted_count',0,'registry_updated_count',0,
       'state_inserted_count',0,'state_updated_count',0,
       'job_inserted_count',0,'job_coalesced_count',0,
       'scope_change_tx_token',NULL,'reason','PAIR_ARRAY_NULL_CANDIDATE'
     ) THEN
    RAISE EXCEPTION 'PAIR_ARRAY_NULL_CANDIDATE_FILTER_FAILED'
      USING DETAIL=v_result::text;
  END IF;

  BEGIN
    PERFORM private.pay_workbench_scope_invalidate_v1(
      ARRAY[v_candidate_id],ARRAY[]::uuid[],
      'PAIR_ARRAY_LENGTH_MISMATCH',NULL::uuid,'{}'::jsonb
    );
    RAISE EXCEPTION 'EXPECTED_LENGTH_MISMATCH_FAILURE';
  EXCEPTION WHEN SQLSTATE '22023' THEN
    IF SQLERRM IS DISTINCT FROM 'PAY_WORKBENCH_SCOPE_INVALIDATION_INPUT_INVALID' THEN
      RAISE;
    END IF;
  END;

  BEGIN
    PERFORM private.pay_workbench_scope_invalidate_v1(
      ARRAY[pg_catalog.gen_random_uuid()],ARRAY[NULL::uuid],
      'PAIR_ARRAY_MISSING_CANDIDATE',NULL::uuid,'{}'::jsonb
    );
    RAISE EXCEPTION 'EXPECTED_MISSING_CANDIDATE_FAILURE';
  EXCEPTION WHEN SQLSTATE '23503' THEN
    GET STACKED DIAGNOSTICS v_error_detail=PG_EXCEPTION_DETAIL;
    IF SQLERRM IS DISTINCT FROM 'PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH'
       OR COALESCE(v_error_detail,'{}')::jsonb->>'kind'
            IS DISTINCT FROM 'CANDIDATE_NOT_FOUND' THEN
      RAISE;
    END IF;
  END;

  SELECT count(*) INTO STRICT v_job_count_before
  FROM public.banking_pay_workbench_jobs;
  SELECT count(*) INTO STRICT v_registry_count_before
  FROM private.banking_pay_workbench_candidate_scope_registry;
  SELECT count(*) INTO STRICT v_state_count_before
  FROM private.banking_pay_workbench_timesheet_scope_state;

  BEGIN
    PERFORM private.pay_workbench_scope_invalidate_v1(
      ARRAY[v_candidate_id],ARRAY[pg_catalog.gen_random_uuid()],
      'PAIR_ARRAY_MISSING_TIMESHEET',NULL::uuid,'{}'::jsonb
    );
    RAISE EXCEPTION 'EXPECTED_MISSING_TIMESHEET_FAILURE';
  EXCEPTION WHEN SQLSTATE '23503' THEN
    GET STACKED DIAGNOSTICS v_error_detail=PG_EXCEPTION_DETAIL;
    IF SQLERRM IS DISTINCT FROM 'PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH'
       OR COALESCE(v_error_detail,'{}')::jsonb->>'kind'
            IS DISTINCT FROM 'TIMESHEET_CANDIDATE_MISMATCH' THEN
      RAISE;
    END IF;
  END;

  IF (SELECT count(*) FROM public.banking_pay_workbench_jobs)
       IS DISTINCT FROM v_job_count_before
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_candidate_scope_registry)
          IS DISTINCT FROM v_registry_count_before
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_timesheet_scope_state)
          IS DISTINCT FROM v_state_count_before THEN
    RAISE EXCEPTION 'PAIR_ARRAY_MISSING_TIMESHEET_SIDE_EFFECT';
  END IF;

  BEGIN
    PERFORM private.pay_workbench_scope_invalidate_v1(
      ARRAY[v_other_candidate_id],ARRAY[v_timesheet_id],
      'PAIR_ARRAY_OWNERSHIP_MISMATCH',NULL::uuid,'{}'::jsonb
    );
    RAISE EXCEPTION 'EXPECTED_TIMESHEET_OWNERSHIP_FAILURE';
  EXCEPTION WHEN SQLSTATE '23503' THEN
    GET STACKED DIAGNOSTICS v_error_detail=PG_EXCEPTION_DETAIL;
    IF SQLERRM IS DISTINCT FROM 'PAY_WORKBENCH_SCOPE_INVALIDATION_OWNERSHIP_MISMATCH'
       OR COALESCE(v_error_detail,'{}')::jsonb->>'kind'
            IS DISTINCT FROM 'TIMESHEET_CANDIDATE_MISMATCH' THEN
      RAISE;
    END IF;
  END;

  BEGIN
    PERFORM private.pay_workbench_scope_invalidate_v1(
      ARRAY[v_candidate_id],ARRAY[v_timesheet_id],
      'PAIR_ARRAY_INVALID_TOKEN',pg_catalog.gen_random_uuid(),'{}'::jsonb
    );
    RAISE EXCEPTION 'EXPECTED_INVALID_TOKEN_FAILURE';
  EXCEPTION WHEN SQLSTATE '22023' THEN
    IF SQLERRM IS DISTINCT FROM 'PAY_WORKBENCH_SCOPE_TRANSACTION_TOKEN_INVALID' THEN
      RAISE;
    END IF;
  END;

  IF v_alignment_candidate_low_id >= v_alignment_candidate_high_id
     OR v_alignment_timesheet_for_high_id >=
          v_alignment_timesheet_for_low_id THEN
    RAISE EXCEPTION 'PAIR_ARRAY_ALIGNMENT_FIXTURE_ORDER_INVALID';
  END IF;

  SELECT count(*) INTO STRICT v_job_count_before
  FROM public.banking_pay_workbench_jobs;
  v_result:=private.pay_workbench_scope_invalidate_v1(
    ARRAY[v_alignment_candidate_high_id,v_alignment_candidate_low_id],
    ARRAY[
      v_alignment_timesheet_for_high_id,
      v_alignment_timesheet_for_low_id
    ],
    'PAIR_ARRAY_CROSSED_ALIGNMENT',NULL::uuid,
    pg_catalog.jsonb_build_object('latest_source_change_seq',1000)
  );
  v_token:=(v_result->>'scope_change_tx_token')::uuid;

  IF COALESCE((v_result->>'ok')::boolean,false) IS NOT TRUE
     OR (v_result->>'candidate_count')::integer IS DISTINCT FROM 2
     OR (v_result->>'timesheet_count')::integer IS DISTINCT FROM 2
     OR (v_result->>'registry_inserted_count')::integer IS DISTINCT FROM 2
     OR (v_result->>'registry_updated_count')::integer IS DISTINCT FROM 0
     OR (v_result->>'state_inserted_count')::integer IS DISTINCT FROM 2
     OR (v_result->>'state_updated_count')::integer IS DISTINCT FROM 0
     OR (v_result->>'job_inserted_count')::integer IS DISTINCT FROM 2
     OR (v_result->>'job_coalesced_count')::integer IS DISTINCT FROM 0
     OR v_token IS NULL
     OR (SELECT count(*) FROM public.banking_pay_workbench_jobs)
          IS DISTINCT FROM v_job_count_before+2
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
         WHERE (scope_state.candidate_id,scope_state.timesheet_id) IN (
           (v_alignment_candidate_low_id,v_alignment_timesheet_for_low_id),
           (v_alignment_candidate_high_id,v_alignment_timesheet_for_high_id)
         )
           AND scope_state.last_scope_change_tx_token=v_token)
          IS DISTINCT FROM 2
     OR (SELECT count(*)
         FROM public.banking_pay_workbench_jobs AS job
         WHERE job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
           AND job.status='QUEUED'
           AND (
             (job.candidate_id=v_alignment_candidate_low_id
              AND job.payload_json->'targeted_timesheet_ids'
                   =pg_catalog.to_jsonb(
                     ARRAY[v_alignment_timesheet_for_low_id]
                   ))
             OR
             (job.candidate_id=v_alignment_candidate_high_id
              AND job.payload_json->'targeted_timesheet_ids'
                   =pg_catalog.to_jsonb(
                     ARRAY[v_alignment_timesheet_for_high_id]
                   ))
           )) IS DISTINCT FROM 2 THEN
    RAISE EXCEPTION 'PAIR_ARRAY_CROSSED_ALIGNMENT_FAILED'
      USING DETAIL=v_result::text;
  END IF;

  UPDATE public.banking_pay_workbench_jobs AS job
  SET status='RUNNING',attempt_count=1,
      started_at_utc=pg_catalog.clock_timestamp(),
      updated_at_utc=pg_catalog.clock_timestamp()
  WHERE job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
    AND job.candidate_id IN (
      v_alignment_candidate_low_id,v_alignment_candidate_high_id
    )
    AND job.status='QUEUED';
  GET DIAGNOSTICS v_rows=ROW_COUNT;
  IF v_rows IS DISTINCT FROM 2 THEN
    RAISE EXCEPTION 'PAIR_ARRAY_CROSSED_ALIGNMENT_RUNNING_STAGE_FAILED';
  END IF;

  v_replay:=private.pay_workbench_scope_invalidate_v1(
    ARRAY[v_alignment_candidate_high_id,v_alignment_candidate_low_id],
    ARRAY[
      v_alignment_timesheet_for_high_id,
      v_alignment_timesheet_for_low_id
    ],
    'PAIR_ARRAY_CROSSED_ALIGNMENT',v_token,
    pg_catalog.jsonb_build_object('latest_source_change_seq',1000)
  );
  IF (v_replay->>'scope_change_tx_token')::uuid IS DISTINCT FROM v_token
     OR (v_replay->>'candidate_count')::integer IS DISTINCT FROM 2
     OR (v_replay->>'timesheet_count')::integer IS DISTINCT FROM 2
     OR (v_replay->>'registry_inserted_count')::integer IS DISTINCT FROM 0
     OR (v_replay->>'registry_updated_count')::integer IS DISTINCT FROM 2
     OR (v_replay->>'state_inserted_count')::integer IS DISTINCT FROM 0
     OR (v_replay->>'state_updated_count')::integer IS DISTINCT FROM 2
     OR (v_replay->>'job_inserted_count')::integer IS DISTINCT FROM 0
     OR (v_replay->>'job_coalesced_count')::integer IS DISTINCT FROM 2
     OR (SELECT count(*) FROM public.banking_pay_workbench_jobs)
          IS DISTINCT FROM v_job_count_before+2
     OR (SELECT count(*)
         FROM public.banking_pay_workbench_jobs AS job
         WHERE job.candidate_id IN (
           v_alignment_candidate_low_id,v_alignment_candidate_high_id
         )
           AND job.status='RUNNING'
           AND COALESCE((job.payload_json->>'rerun_required')::boolean,false))
          IS DISTINCT FROM 2 THEN
    RAISE EXCEPTION 'PAIR_ARRAY_CROSSED_ALIGNMENT_REPLAY_FAILED'
      USING DETAIL=v_replay::text;
  END IF;
END;
$direct_semantics$;

SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE;
SELECT pg_catalog.set_config('cloudtms.scope_generation_finalising','false',true);
SELECT pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token','',true);
SELECT pg_catalog.set_config('cloudtms.bpay_scope_invalidator_active','false',true);
SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED;

-- Direct private EXECUTE stays denied, while the actual public SECURITY DEFINER
-- route succeeds in the same caller session that owns the hostile temp table.
SET LOCAL ROLE service_role;

DO $public_route$
DECLARE
  v_candidate_id uuid;
  v_timesheet_id uuid;
  v_result jsonb;
BEGIN
  SELECT main_candidate_id,main_timesheet_id
  INTO STRICT v_candidate_id,v_timesheet_id
  FROM pg_temp._bpay_pair_array_runtime_state_v1;

  BEGIN
    PERFORM private.pay_workbench_scope_invalidate_v1(
      ARRAY[v_candidate_id],ARRAY[v_timesheet_id],
      'DIRECT_PRIVATE_ACL_NEGATIVE',NULL::uuid,'{}'::jsonb
    );
    RAISE EXCEPTION 'EXPECTED_PRIVATE_INVALIDATOR_ACL_FAILURE';
  EXCEPTION WHEN SQLSTATE '42501' THEN
    NULL;
  END;

  v_result:=public.pay_workbench_dirty_event_enqueue(
    p_job_type=>'WORKBENCH_CANDIDATE_DIRTY_APPLY',
    p_scope_kind=>'CANDIDATE',
    p_scope_id=>v_candidate_id::text,
    p_candidate_id=>v_candidate_id,
    p_targeted_timesheet_ids=>ARRAY[v_timesheet_id],
    p_linked_timesheet_ids=>ARRAY[]::uuid[],
    p_payload_json=>pg_catalog.jsonb_build_object(
      'trigger_table','PAIR_ARRAY_PUBLIC_ROUTE',
      'trigger_operation','UPDATE'
    ),
    p_reason=>'DIRTY_TRIGGER:PAIR_ARRAY_PUBLIC_ROUTE:UPDATE',
    p_priority=>-1000,
    p_run_at_utc=>pg_catalog.clock_timestamp()
  );

  IF COALESCE((v_result->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE(v_result->>'job_id','') !~
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR v_result->>'status' IS DISTINCT FROM 'QUEUED' THEN
    RAISE EXCEPTION 'PAIR_ARRAY_PUBLIC_ROUTE_FAILED'
      USING DETAIL=v_result::text;
  END IF;

  UPDATE pg_temp._bpay_pair_array_runtime_state_v1
  SET route_job_id=(v_result->>'job_id')::uuid;

  IF pg_catalog.to_regclass('pg_temp._bpay_wb_invalidation_pairs_v1')::oid
       IS DISTINCT FROM (
         SELECT poison_oid FROM pg_temp._bpay_pair_array_runtime_state_v1
       )
     OR (SELECT hostile_payload
         FROM pg_temp._bpay_wb_invalidation_pairs_v1)
          IS DISTINCT FROM 'CALLER_OWNED_SENTINEL'
     OR (SELECT count(*) FROM pg_temp._bpay_pair_array_poison_observed_v1)
          IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'CALLER_OWNED_POISON_TOUCHED_BY_PUBLIC_ROUTE';
  END IF;
END;
$public_route$;

RESET ROLE;

SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE;
SELECT pg_catalog.set_config('cloudtms.scope_generation_finalising','false',true);
SELECT pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token','',true);
SELECT pg_catalog.set_config('cloudtms.bpay_scope_invalidator_active','false',true);
SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED;

-- Add one raw distinct-key sibling without trigger staging. Processing it must
-- enter the cohort helper and transitively call the corrected invalidator.
SET LOCAL session_replication_role = 'replica';

INSERT INTO public.banking_pay_workbench_jobs(
  id,job_type,status,priority,run_at_utc,attempt_count,max_attempts,
  dedupe_key,candidate_id,payload_json,created_at_utc,updated_at_utc
)
SELECT fixture.raw_job_id,'WORKBENCH_CANDIDATE_DIRTY_APPLY','RUNNING',-1000,
       pg_catalog.clock_timestamp(),1,8,
       fixture.fixture_prefix||':RAW_COHORT_SIBLING',fixture.main_candidate_id,
       pg_catalog.jsonb_build_object(
         'candidate_id',fixture.main_candidate_id::text,
         'targeted_timesheet_ids',pg_catalog.to_jsonb(ARRAY[fixture.main_timesheet_id]),
         'linked_timesheet_ids','[]'::jsonb,
         'reason','DIRTY_TRIGGER:PAIR_ARRAY_RAW:UPDATE',
         'reasons',pg_catalog.jsonb_build_array('DIRTY_TRIGGER:PAIR_ARRAY_RAW:UPDATE'),
         'latest_source_change_seq',counter.seq,
         'source_change_seq',counter.seq
       ),
       pg_catalog.clock_timestamp(),pg_catalog.clock_timestamp()
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
JOIN public.app_change_counters AS counter
  ON counter.entity_key='pay_candidate:'||fixture.main_candidate_id::text;

SET LOCAL session_replication_role = 'origin';
SET LOCAL ROLE service_role;

DO $cohort_route$
DECLARE
  v_job_id uuid;
  v_result jsonb;
BEGIN
  SELECT raw_job_id INTO STRICT v_job_id
  FROM pg_temp._bpay_pair_array_runtime_state_v1;
  v_result:=public.pay_workbench_candidate_dirty_apply_job_process(v_job_id,100);

  IF v_result->>'dirty_apply_cohort_action'
       IS DISTINCT FROM 'COHORT_REISSUED_PENDING_FINALIZATION'
     OR (v_result->>'dirty_apply_cohort_member_count')::integer
          IS DISTINCT FROM 2
     OR v_result->>'dirty_apply_cohort_authority_scope'
          IS DISTINCT FROM 'TARGETED_UNION' THEN
    RAISE EXCEPTION 'PAIR_ARRAY_COHORT_HELPER_ROUTE_FAILED'
      USING DETAIL=v_result::text;
  END IF;

  IF pg_catalog.to_regclass('pg_temp._bpay_wb_invalidation_pairs_v1')::oid
       IS DISTINCT FROM (
         SELECT poison_oid FROM pg_temp._bpay_pair_array_runtime_state_v1
       )
     OR (SELECT hostile_payload
         FROM pg_temp._bpay_wb_invalidation_pairs_v1)
          IS DISTINCT FROM 'CALLER_OWNED_SENTINEL'
     OR (SELECT count(*) FROM pg_temp._bpay_pair_array_poison_observed_v1)
          IS DISTINCT FROM 0 THEN
    RAISE EXCEPTION 'CALLER_OWNED_POISON_TOUCHED_BY_COHORT_HELPER';
  END IF;
END;
$cohort_route$;

RESET ROLE;

SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 IMMEDIATE;
SELECT pg_catalog.set_config('cloudtms.scope_generation_finalising','false',true);
SELECT pg_catalog.set_config('cloudtms.banking_pay_scope_tx_token','',true);
SELECT pg_catalog.set_config('cloudtms.bpay_scope_invalidator_active','false',true);
SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1 DEFERRED;

UPDATE pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
SET cohort_token=(job.payload_json->>'scope_change_tx_token')::uuid,
    cohort_generation=job.scope_change_generation,
    cohort_source_seq=counter.seq
FROM public.banking_pay_workbench_jobs AS job,
     public.app_change_counters AS counter
WHERE job.id=fixture.raw_job_id
  AND counter.entity_key='pay_candidate:'||fixture.main_candidate_id::text;

UPDATE public.banking_pay_workbench_jobs AS job
SET status='RUNNING',attempt_count=attempt_count+1,
    started_at_utc=pg_catalog.clock_timestamp(),
    updated_at_utc=pg_catalog.clock_timestamp()
FROM pg_temp._bpay_pair_array_runtime_state_v1 AS fixture
WHERE job.id=fixture.raw_job_id;

DO $cohort_replay$
DECLARE
  v_fixture pg_temp._bpay_pair_array_runtime_state_v1%ROWTYPE;
  v_result jsonb;
BEGIN
  SELECT * INTO STRICT v_fixture
  FROM pg_temp._bpay_pair_array_runtime_state_v1;
  EXECUTE 'SET LOCAL ROLE service_role';
  v_result:=public.pay_workbench_candidate_dirty_apply_job_process(
    v_fixture.raw_job_id,100
  );
  EXECUTE 'RESET ROLE';
  IF (v_result->>'effective_scope_change_generation')::bigint
       IS DISTINCT FROM v_fixture.cohort_generation
     OR v_result->>'refresh_scope_kind' IS DISTINCT FROM 'TARGETED_TIMESHEETS'
     OR (SELECT payload_json->>'scope_change_tx_token'
         FROM public.banking_pay_workbench_jobs
         WHERE id=v_fixture.raw_job_id)
          IS DISTINCT FROM v_fixture.cohort_token::text
     OR (SELECT seq FROM public.app_change_counters
         WHERE entity_key='pay_candidate:'||v_fixture.main_candidate_id::text)
          IS DISTINCT FROM v_fixture.cohort_source_seq THEN
    RAISE EXCEPTION 'PAIR_ARRAY_COHORT_REPLAY_REISSUED_AUTHORITY'
      USING DETAIL=v_result::text;
  END IF;
END;
$cohort_replay$;

-- 5,000 DISTINCT accepted Candidate-full pairs. Enqueue is explicitly skipped:
-- this measures canonicalization/ownership/registry work, never 5,000 jobs.
SET LOCAL statement_timeout = '15s';

DO $scale_5000$
DECLARE
  v_candidate_ids uuid[];
  v_timesheet_ids uuid[];
  v_result jsonb;
  v_started_at timestamptz:=pg_catalog.clock_timestamp();
  v_elapsed_ms numeric;
BEGIN
  SELECT pg_catalog.array_agg(scale_candidate.candidate_id ORDER BY scale_candidate.ordinal),
         pg_catalog.array_agg(scale_candidate.timesheet_id ORDER BY scale_candidate.ordinal)
  INTO STRICT v_candidate_ids,v_timesheet_ids
  FROM pg_temp._bpay_pair_array_scale_candidates_v1 AS scale_candidate;

  IF cardinality(v_candidate_ids) IS DISTINCT FROM 5000
     OR cardinality(v_timesheet_ids) IS DISTINCT FROM 5000
     OR (SELECT count(DISTINCT candidate_id)
         FROM pg_temp._bpay_pair_array_scale_candidates_v1)
          IS DISTINCT FROM 5000
     OR (SELECT count(DISTINCT timesheet_id)
         FROM pg_temp._bpay_pair_array_scale_candidates_v1)
          IS DISTINCT FROM 5000 THEN
    RAISE EXCEPTION 'PAIR_ARRAY_5000_DISTINCT_FIXTURE_INVALID';
  END IF;

  v_result:=private.pay_workbench_scope_invalidate_v1(
    v_candidate_ids,v_timesheet_ids,'PAIR_ARRAY_5000_DISTINCT',NULL::uuid,
    pg_catalog.jsonb_build_object(
      'skip_candidate_job_enqueue',true,
      'latest_source_change_seq',1000
    )
  );
  v_elapsed_ms:=1000*EXTRACT(
    epoch FROM pg_catalog.clock_timestamp()-v_started_at
  );

  IF (v_result->>'candidate_count')::integer IS DISTINCT FROM 5000
     OR (v_result->>'timesheet_count')::integer IS DISTINCT FROM 5000
     OR (v_result->>'registry_inserted_count')::integer IS DISTINCT FROM 5000
     OR (v_result->>'registry_updated_count')::integer IS DISTINCT FROM 0
     OR (v_result->>'state_inserted_count')::integer IS DISTINCT FROM 5000
     OR (v_result->>'state_updated_count')::integer IS DISTINCT FROM 0
     OR (v_result->>'job_inserted_count')::integer IS DISTINCT FROM 0
     OR (v_result->>'job_coalesced_count')::integer IS DISTINCT FROM 0
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_candidate_scope_registry AS registry
         JOIN pg_temp._bpay_pair_array_scale_candidates_v1 AS scale_candidate
           ON scale_candidate.candidate_id=registry.candidate_id
         WHERE registry.last_scope_change_tx_token=
               (v_result->>'scope_change_tx_token')::uuid)
          IS DISTINCT FROM 5000
     OR (SELECT count(*)
         FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
         JOIN pg_temp._bpay_pair_array_scale_candidates_v1 AS scale_candidate
           ON scale_candidate.timesheet_id=scope_state.timesheet_id
          AND scale_candidate.candidate_id=scope_state.candidate_id
         WHERE scope_state.last_scope_change_tx_token=
               (v_result->>'scope_change_tx_token')::uuid)
          IS DISTINCT FROM 5000
     OR v_elapsed_ms >= 15000
     OR v_result::text ~* 'CAPACITY_EXHAUSTED|TOO_MANY|CARDINALITY' THEN
    RAISE EXCEPTION 'PAIR_ARRAY_5000_DISTINCT_FAILED'
      USING DETAIL=pg_catalog.jsonb_build_object(
        'result',v_result,'elapsed_ms',v_elapsed_ms
      )::text;
  END IF;

  RAISE NOTICE 'PAIR_ARRAY_5000_DISTINCT elapsed_ms=%',
    pg_catalog.round(v_elapsed_ms,2);
END;
$scale_5000$;

-- Exact multiset comparison detects UPDATE and delete+insert drift, not merely
-- net row-count stability. The poison relation must also remain byte/schema
-- stable after every direct, trigger, public and cohort-helper path above.
DO $policy_x_no_money_effect$
DECLARE
  v_fingerprint record;
  v_count bigint;
  v_digest text;
BEGIN
  FOR v_fingerprint IN
    SELECT *
    FROM pg_temp._bpay_pair_array_economic_fingerprints_v1
    ORDER BY relation_name
  LOOP
    EXECUTE pg_catalog.format(
      'SELECT count(*)::bigint,pg_catalog.md5(COALESCE(pg_catalog.string_agg(pg_catalog.to_jsonb(economic_row)::text,E''\n'' ORDER BY pg_catalog.to_jsonb(economic_row)::text),'''')) FROM %s AS economic_row',
      v_fingerprint.relation_name::regclass
    ) INTO v_count,v_digest;
    IF v_count IS DISTINCT FROM v_fingerprint.row_count
       OR v_digest IS DISTINCT FROM v_fingerprint.row_digest THEN
      RAISE EXCEPTION 'POLICY_X_NO_MONEY_EFFECT_FAILED'
        USING DETAIL=pg_catalog.jsonb_build_object(
          'relation',v_fingerprint.relation_name,
          'before_count',v_fingerprint.row_count,
          'after_count',v_count,
          'before_digest',v_fingerprint.row_digest,
          'after_digest',v_digest
        )::text;
    END IF;
  END LOOP;

  IF (SELECT count(DISTINCT trigger_proc.proname)
      FROM pg_catalog.pg_trigger AS trigger_row
      JOIN pg_catalog.pg_proc AS trigger_proc
        ON trigger_proc.oid=trigger_row.tgfoid
      WHERE NOT trigger_row.tgisinternal
        AND trigger_row.tgenabled IS DISTINCT FROM 'D'
        AND trigger_proc.proname IN (
          'pay_workbench_financial_scope_dirty_transition_v1',
          'pay_timesheet_summary_pay_state_refresh_trigger'
        )) IS DISTINCT FROM 2 THEN
    RAISE EXCEPTION 'PAIR_ARRAY_TRIGGER_FAMILY_CLOSURE_MISSING';
  END IF;
END;
$policy_x_no_money_effect$;

SET LOCAL ROLE service_role;

DO $final_poison_identity$
DECLARE
  v_poison_oid oid;
BEGIN
  SELECT poison_oid INTO STRICT v_poison_oid
  FROM pg_temp._bpay_pair_array_runtime_state_v1;
  IF pg_catalog.to_regclass('pg_temp._bpay_wb_invalidation_pairs_v1')::oid
       IS DISTINCT FROM v_poison_oid
     OR pg_catalog.pg_get_userbyid((
          SELECT relation.relowner FROM pg_catalog.pg_class AS relation
          WHERE relation.oid=v_poison_oid
        )) IS DISTINCT FROM 'service_role'
     OR (SELECT pg_catalog.jsonb_agg(
           pg_catalog.jsonb_build_object(
             'name',attribute.attname,
             'type',pg_catalog.format_type(attribute.atttypid,attribute.atttypmod),
             'not_null',attribute.attnotnull
           ) ORDER BY attribute.attnum
         )
         FROM pg_catalog.pg_attribute AS attribute
         WHERE attribute.attrelid=v_poison_oid
           AND attribute.attnum>0
           AND NOT attribute.attisdropped)
          IS DISTINCT FROM '[{"name":"hostile_payload","type":"text","not_null":true}]'::jsonb
     OR (SELECT count(*) FROM pg_temp._bpay_wb_invalidation_pairs_v1)
          IS DISTINCT FROM 1
     OR (SELECT hostile_payload FROM pg_temp._bpay_wb_invalidation_pairs_v1)
          IS DISTINCT FROM 'CALLER_OWNED_SENTINEL'
     OR (SELECT count(*) FROM pg_temp._bpay_pair_array_poison_observed_v1)
          IS DISTINCT FROM 0
     OR (SELECT count(*) FROM pg_catalog.pg_trigger
         WHERE tgrelid=v_poison_oid AND NOT tgisinternal)
          IS DISTINCT FROM 2 THEN
    RAISE EXCEPTION 'CALLER_OWNED_POISON_FINAL_IDENTITY_CHANGED';
  END IF;
END;
$final_poison_identity$;

RESET ROLE;

DO $completed$
BEGIN
  RAISE NOTICE 'PASS: pair-array invalidator preserves direct semantics, typed failures, both trigger families, the public/cohort routes, 5,000 distinct pairs and Policy X while leaving caller-owned poison untouched.';
END;
$completed$;

ROLLBACK;
