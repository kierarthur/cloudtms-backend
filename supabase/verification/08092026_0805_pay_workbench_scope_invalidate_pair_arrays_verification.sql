\set ON_ERROR_STOP on

-- Data-free installed-authority proof for the pair-array scope invalidator.
BEGIN;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '5s';

DO $verification$
DECLARE
  v_invalidator_oid oid :=
    'private.pay_workbench_scope_invalidate_v1(uuid[],uuid[],text,uuid,jsonb)'::regprocedure;
  v_invalidator record;
  v_definition text;
  v_execute_grantees text[] := ARRAY[]::text[];
  v_expected_execute_grantees text[] := ARRAY[]::text[];
  v_execute_grant_option boolean := false;
  v_direct_callers text[] := ARRAY[]::text[];
  v_expected_direct_callers text[] := ARRAY[
    'private.pay_workbench_candidate_dirty_cohort_stage_v1(p_job_id uuid, p_candidate_id uuid, p_now_utc timestamp with time zone)',
    'private.pay_workbench_financial_scope_dirty_transition_v1()',
    'public.candidate_pay_method_change_refresh_scope_v1(p_candidate_id uuid, p_source_method text, p_target_method text)',
    'public.pay_timesheet_summary_pay_state_refresh_trigger()',
    'public.pay_workbench_contract_client_dirty_fanout_chunk(p_job_id uuid, p_cursor_json jsonb, p_limit integer)',
    'public.pay_workbench_dirty_event_enqueue(p_job_type text, p_scope_kind text, p_scope_id text, p_candidate_id uuid, p_targeted_timesheet_ids uuid[], p_linked_timesheet_ids uuid[], p_payload_json jsonb, p_reason text, p_priority integer, p_run_at_utc timestamp with time zone)',
    'public.pay_workbench_enqueue_candidate_refresh(p_snapshot_run_id uuid, p_candidate_id uuid, p_reason text, p_actor_user_id uuid, p_payload_json jsonb)',
    'public.pay_workbench_repair_invalid_dirty_apply_jobs_v1(p_session_id uuid, p_candidate_id uuid, p_limit integer, p_reason text)'
  ]::text[];
  v_rows_from_count integer := 0;
  v_pair_candidate_unnest_count integer := 0;
  v_pair_timesheet_unnest_count integer := 0;
  v_trigger_family_count integer := 0;
BEGIN
  SELECT proc.prosecdef,
         proc.provolatile,
         proc.proparallel,
         proc.proconfig,
         proc.pronargdefaults,
         proc.prorettype,
         language_row.lanname,
         pg_catalog.pg_get_userbyid(proc.proowner) AS owner_name
  INTO STRICT v_invalidator
  FROM pg_catalog.pg_proc AS proc
  JOIN pg_catalog.pg_language AS language_row
    ON language_row.oid = proc.prolang
  WHERE proc.oid = v_invalidator_oid;

  SELECT COALESCE(
           pg_catalog.array_agg(
             normalized_acl.grantee_name
             ORDER BY normalized_acl.grantee_name
           ),
           ARRAY[]::text[]
         ),
         COALESCE(pg_catalog.bool_or(normalized_acl.is_grantable), false)
  INTO v_execute_grantees,v_execute_grant_option
  FROM (
    SELECT DISTINCT
           CASE WHEN function_acl.grantee = 0 THEN 'PUBLIC'
             ELSE pg_catalog.pg_get_userbyid(function_acl.grantee) END
             AS grantee_name,
           function_acl.is_grantable
    FROM pg_catalog.pg_proc AS proc
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
      proc.proacl,
      pg_catalog.acldefault('f'::"char",proc.proowner)
    )) AS function_acl
    WHERE proc.oid = v_invalidator_oid
      AND function_acl.privilege_type = 'EXECUTE'
  ) AS normalized_acl;

  v_expected_execute_grantees := ARRAY[v_invalidator.owner_name]::text[];

  IF v_invalidator.prosecdef IS TRUE
     OR v_invalidator.provolatile IS DISTINCT FROM 'v'::"char"
     OR v_invalidator.proparallel IS DISTINCT FROM 'u'::"char"
     OR v_invalidator.proconfig IS DISTINCT FROM ARRAY['search_path=""']::text[]
     OR v_invalidator.pronargdefaults IS DISTINCT FROM 2
     OR v_invalidator.prorettype IS DISTINCT FROM 'jsonb'::regtype
     OR v_invalidator.lanname IS DISTINCT FROM 'plpgsql'
     OR v_invalidator.owner_name IS DISTINCT FROM current_user
     OR v_execute_grantees IS DISTINCT FROM v_expected_execute_grantees
     OR v_execute_grant_option
     OR pg_catalog.has_function_privilege('anon',v_invalidator_oid,'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated',v_invalidator_oid,'EXECUTE')
     OR pg_catalog.has_function_privilege('service_role',v_invalidator_oid,'EXECUTE') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATOR_PAIR_ARRAY_METADATA_INVALID'
      USING ERRCODE='55000',DETAIL=pg_catalog.jsonb_build_object(
        'metadata',pg_catalog.to_jsonb(v_invalidator),
        'execute_grantees',v_execute_grantees,
        'execute_grant_option',v_execute_grant_option
      )::text;
  END IF;

  SELECT pg_catalog.pg_get_functiondef(v_invalidator_oid)
  INTO STRICT v_definition;

  v_rows_from_count := (
    length(v_definition)-length(replace(v_definition,'ROWS FROM',''))
  )/length('ROWS FROM');
  v_pair_candidate_unnest_count := (
    length(v_definition)-length(replace(
      v_definition,'pg_catalog.unnest(v_pair_candidate_ids)',''
    ))
  )/length('pg_catalog.unnest(v_pair_candidate_ids)');
  v_pair_timesheet_unnest_count := (
    length(v_definition)-length(replace(
      v_definition,'pg_catalog.unnest(v_pair_timesheet_ids)',''
    ))
  )/length('pg_catalog.unnest(v_pair_timesheet_ids)');

  IF v_definition ~* 'pg_temp|_bpay_wb_invalidation_pairs_v1|CREATE[[:space:]]+TEMP|TRUNCATE'
     OR v_definition !~ 'v_pair_candidate_ids uuid\[\] := ARRAY\[\]::uuid\[\]'
     OR v_definition !~ 'v_pair_timesheet_ids uuid\[\] := ARRAY\[\]::uuid\[\]'
     OR v_definition !~ 'SELECT DISTINCT input_pair.candidate_id,input_pair.timesheet_id'
     OR v_definition !~ 'ORDER BY canonical_pair.candidate_id,[[:space:]]+canonical_pair.timesheet_id NULLS FIRST'
     OR v_definition !~ 'v_pair_count := cardinality\(v_pair_candidate_ids\)'
     OR v_rows_from_count <> 9
     OR v_pair_candidate_unnest_count <> 8
     OR v_pair_timesheet_unnest_count <> 8 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATOR_PAIR_ARRAY_DEFINITION_INVALID'
      USING ERRCODE='55000',DETAIL=pg_catalog.jsonb_build_object(
        'rows_from_count',v_rows_from_count,
        'pair_candidate_unnest_count',v_pair_candidate_unnest_count,
        'pair_timesheet_unnest_count',v_pair_timesheet_unnest_count
      )::text;
  END IF;

  SELECT COALESCE(
           pg_catalog.array_agg(
             caller_namespace.nspname||'.'||caller_proc.proname||'('||
             pg_catalog.pg_get_function_identity_arguments(caller_proc.oid)||')'
             ORDER BY caller_namespace.nspname,caller_proc.proname,
                      pg_catalog.pg_get_function_identity_arguments(caller_proc.oid)
           ),
           ARRAY[]::text[]
         )
  INTO v_direct_callers
  FROM pg_catalog.pg_proc AS caller_proc
  JOIN pg_catalog.pg_namespace AS caller_namespace
    ON caller_namespace.oid = caller_proc.pronamespace
  WHERE caller_proc.prokind = 'f'
    AND caller_proc.oid <> v_invalidator_oid
    AND pg_catalog.pg_get_functiondef(caller_proc.oid)
          LIKE '%private.pay_workbench_scope_invalidate_v1(%';

  IF v_direct_callers IS DISTINCT FROM v_expected_direct_callers THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATOR_DIRECT_CALLER_CLOSURE_INVALID'
      USING ERRCODE='55000',DETAIL=pg_catalog.jsonb_build_object(
        'expected',v_expected_direct_callers,
        'actual',v_direct_callers
      )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS caller_proc
    JOIN pg_catalog.pg_namespace AS caller_namespace
      ON caller_namespace.oid = caller_proc.pronamespace
    WHERE caller_proc.prokind = 'f'
      AND caller_proc.oid <> v_invalidator_oid
      AND pg_catalog.pg_get_functiondef(caller_proc.oid)
            LIKE '%private.pay_workbench_scope_invalidate_v1(%'
      AND CASE
            WHEN caller_namespace.nspname = 'private'
             AND caller_proc.proname =
                   'pay_workbench_candidate_dirty_cohort_stage_v1'
              THEN caller_proc.prosecdef IS DISTINCT FROM FALSE
            ELSE caller_proc.prosecdef IS DISTINCT FROM TRUE
          END
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATOR_CALLER_SECURITY_INVALID'
      USING ERRCODE='55000';
  END IF;

  SELECT count(DISTINCT trigger_proc.oid)::integer
  INTO v_trigger_family_count
  FROM pg_catalog.pg_trigger AS trigger_row
  JOIN pg_catalog.pg_proc AS trigger_proc
    ON trigger_proc.oid = trigger_row.tgfoid
  WHERE NOT trigger_row.tgisinternal
    AND trigger_row.tgenabled <> 'D'
    AND trigger_proc.proname IN (
      'pay_workbench_financial_scope_dirty_transition_v1',
      'pay_timesheet_summary_pay_state_refresh_trigger'
    );

  IF v_trigger_family_count <> 2 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SCOPE_INVALIDATOR_TRIGGER_CLOSURE_INVALID'
      USING ERRCODE='55000',DETAIL=pg_catalog.jsonb_build_object(
        'trigger_family_count',v_trigger_family_count
      )::text;
  END IF;

  RAISE NOTICE 'PASS: pair-array invalidator metadata, ACL, definition, direct callers and trigger families are exact.';
END;
$verification$;

ROLLBACK;
