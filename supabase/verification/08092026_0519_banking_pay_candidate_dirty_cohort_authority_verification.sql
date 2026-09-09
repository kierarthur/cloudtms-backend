\set ON_ERROR_STOP on

-- Data-free installed-authority proof for the Candidate DIRTY_APPLY cohort
-- owner. Runtime/state-machine cases live in the rollback-only 0521 fixture.
BEGIN;
SET LOCAL statement_timeout = '30s';

DO $verification$
DECLARE
  v_helper_oid oid := 'private.pay_workbench_candidate_dirty_cohort_stage_v1(uuid,uuid,timestamptz)'::regprocedure;
  v_processor_oid oid := 'public.pay_workbench_candidate_dirty_apply_job_process(uuid,integer)'::regprocedure;
  v_helper record;
  v_processor record;
  v_helper_definition text;
  v_processor_definition text;
  v_index_count integer := 0;
  v_helper_public_execute boolean := false;
  v_processor_public_execute boolean := false;
  v_helper_execute_grantees text[] := ARRAY[]::text[];
  v_processor_execute_grantees text[] := ARRAY[]::text[];
  v_helper_expected_execute_grantees text[] := ARRAY[]::text[];
  v_processor_expected_execute_grantees text[] := ARRAY[]::text[];
  v_helper_execute_grant_option boolean := false;
  v_processor_execute_grant_option boolean := false;
BEGIN
  SELECT proc.prosecdef,
         proc.provolatile,
         proc.proparallel,
         proc.proconfig,
         pg_catalog.pg_get_userbyid(proc.proowner) AS owner_name
  INTO STRICT v_helper
  FROM pg_catalog.pg_proc AS proc
  WHERE proc.oid = v_helper_oid;

  SELECT proc.prosecdef,
         proc.provolatile,
         proc.proparallel,
         proc.proconfig,
         pg_catalog.pg_get_userbyid(proc.proowner) AS owner_name
  INTO STRICT v_processor
  FROM pg_catalog.pg_proc AS proc
  WHERE proc.oid = v_processor_oid;

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS proc
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
      proc.proacl,
      pg_catalog.acldefault('f'::"char", proc.proowner)
    )) AS function_acl
    WHERE proc.oid = v_helper_oid
      AND function_acl.grantee = 0
      AND function_acl.privilege_type = 'EXECUTE'
  ) INTO v_helper_public_execute;

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_proc AS proc
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
      proc.proacl,
      pg_catalog.acldefault('f'::"char", proc.proowner)
    )) AS function_acl
    WHERE proc.oid = v_processor_oid
      AND function_acl.grantee = 0
      AND function_acl.privilege_type = 'EXECUTE'
  ) INTO v_processor_public_execute;

  SELECT COALESCE(
           pg_catalog.array_agg(
             normalized_acl.grantee_name
             ORDER BY normalized_acl.grantee_name
           ),
           ARRAY[]::text[]
         ),
         COALESCE(pg_catalog.bool_or(normalized_acl.is_grantable), false)
  INTO v_helper_execute_grantees, v_helper_execute_grant_option
  FROM (
    SELECT DISTINCT
           CASE WHEN function_acl.grantee = 0 THEN 'PUBLIC'
             ELSE pg_catalog.pg_get_userbyid(function_acl.grantee) END
             AS grantee_name,
           function_acl.is_grantable
    FROM pg_catalog.pg_proc AS proc
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
      proc.proacl,
      pg_catalog.acldefault('f'::"char", proc.proowner)
    )) AS function_acl
    WHERE proc.oid = v_helper_oid
      AND function_acl.privilege_type = 'EXECUTE'
  ) AS normalized_acl;

  SELECT COALESCE(
           pg_catalog.array_agg(
             normalized_acl.grantee_name
             ORDER BY normalized_acl.grantee_name
           ),
           ARRAY[]::text[]
         ),
         COALESCE(pg_catalog.bool_or(normalized_acl.is_grantable), false)
  INTO v_processor_execute_grantees, v_processor_execute_grant_option
  FROM (
    SELECT DISTINCT
           CASE WHEN function_acl.grantee = 0 THEN 'PUBLIC'
             ELSE pg_catalog.pg_get_userbyid(function_acl.grantee) END
             AS grantee_name,
           function_acl.is_grantable
    FROM pg_catalog.pg_proc AS proc
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
      proc.proacl,
      pg_catalog.acldefault('f'::"char", proc.proowner)
    )) AS function_acl
    WHERE proc.oid = v_processor_oid
      AND function_acl.privilege_type = 'EXECUTE'
  ) AS normalized_acl;

  v_helper_expected_execute_grantees := ARRAY[v_helper.owner_name]::text[];
  SELECT pg_catalog.array_agg(expected_name ORDER BY expected_name)
  INTO v_processor_expected_execute_grantees
  FROM (
    SELECT DISTINCT expected_name
    FROM pg_catalog.unnest(
      ARRAY[v_processor.owner_name, 'service_role']::text[]
    ) AS expected_acl(expected_name)
  ) AS expected_grantee;

  IF v_helper.prosecdef IS TRUE
     OR v_helper.provolatile IS DISTINCT FROM 'v'::"char"
     OR v_helper.proparallel IS DISTINCT FROM 'u'::"char"
     OR v_helper.proconfig IS DISTINCT FROM ARRAY['search_path=""']::text[]
     OR v_helper.owner_name IS DISTINCT FROM current_user
     OR v_helper_public_execute
     OR v_helper_execute_grantees IS DISTINCT FROM
          v_helper_expected_execute_grantees
     OR v_helper_execute_grant_option
     OR pg_catalog.has_function_privilege(
          'anon', v_helper_oid, 'EXECUTE'
        )
     OR pg_catalog.has_function_privilege(
          'authenticated', v_helper_oid, 'EXECUTE'
        )
     OR pg_catalog.has_function_privilege(
          'service_role', v_helper_oid, 'EXECUTE'
        ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_HELPER_AUTHORITY_INVALID'
      USING ERRCODE = '55000', DETAIL = pg_catalog.to_jsonb(v_helper)::text;
  END IF;

  IF v_processor.prosecdef IS NOT TRUE
     OR v_processor.provolatile IS DISTINCT FROM 'v'::"char"
     OR v_processor.proparallel IS DISTINCT FROM 'u'::"char"
     OR v_processor.proconfig IS DISTINCT FROM ARRAY['search_path=public']::text[]
     OR v_processor.owner_name IS DISTINCT FROM current_user
     OR v_processor_public_execute
     OR v_processor_execute_grantees IS DISTINCT FROM
          v_processor_expected_execute_grantees
     OR v_processor_execute_grant_option
     OR pg_catalog.has_function_privilege(
          'anon', v_processor_oid, 'EXECUTE'
        )
     OR pg_catalog.has_function_privilege(
          'authenticated', v_processor_oid, 'EXECUTE'
        )
     OR NOT pg_catalog.has_function_privilege(
          'service_role', v_processor_oid, 'EXECUTE'
        ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_PROCESSOR_AUTHORITY_INVALID'
      USING ERRCODE = '55000', DETAIL = pg_catalog.to_jsonb(v_processor)::text;
  END IF;

  v_helper_definition := pg_catalog.pg_get_functiondef(v_helper_oid);
  v_processor_definition := pg_catalog.pg_get_functiondef(v_processor_oid);

  IF v_helper_definition !~ 'DIRTY_APPLY_COHORT_AUTHORITY_V1'
     OR v_helper_definition !~ 'REQUEST_OWNED_CORRECTION_UNFINISHED'
     OR v_helper_definition !~ 'COHORT_EMPTY_EFFECTIVE_TIMESHEET_SCOPE'
     OR v_helper_definition !~ 'COHORT_REISSUED_PENDING_FINALIZATION'
     OR v_helper_definition !~ 'REUSE_FINALIZED_AUTHORITY'
     OR v_helper_definition ~* '\mpg_temp\M'
     OR v_helper_definition ~ 'v_member_count = 1[[:space:]]+OR v_all_full_markers_match'
     OR v_processor_definition !~ 'FAST_PATH_FINALIZED_COHORT'
     OR v_processor_definition !~ 'v_dirty_cohort_authority_scope = ''CANDIDATE_FULL_LIVE'''
     OR v_processor_definition !~ 'cardinality\(v_effective_bounded_timesheet_ids\)>0[[:space:]]+AND COALESCE\(v_payload->>''dirty_apply_cohort_authority_scope'', ''''\) =[[:space:]]+''TARGETED_UNION'''
     OR v_processor_definition ~ 'DIRTY_APPLY_EFFECTIVE_SCOPE_REISSUE'
     OR v_processor_definition ~ 'v_scope_reissue_result' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_DEFINITION_INVALID'
      USING ERRCODE = '55000';
  END IF;

  SELECT count(*)::integer
  INTO v_index_count
  FROM pg_catalog.pg_class AS index_class
  JOIN pg_catalog.pg_namespace AS index_namespace
    ON index_namespace.oid = index_class.relnamespace
  JOIN pg_catalog.pg_index AS index_catalog
    ON index_catalog.indexrelid = index_class.oid
  WHERE index_namespace.nspname = 'public'
    AND index_class.relname IN (
      'idx_bpay_wb_jobs_candidate_dirty_active_cohort_v1',
      'idx_bpay_wb_jobs_legacy_candidate_dirty_active_cohort_v1'
    )
    AND index_catalog.indisvalid
    AND index_catalog.indisready;

  IF v_index_count <> 2
     OR pg_catalog.pg_get_indexdef(
          'public.idx_bpay_wb_jobs_candidate_dirty_active_cohort_v1'::regclass
        ) !~ '\(candidate_id, id\) WHERE .*job_type = ''WORKBENCH_CANDIDATE_DIRTY_APPLY''.*status = ANY.*candidate_id IS NOT NULL'
     OR pg_catalog.pg_get_indexdef(
          'public.idx_bpay_wb_jobs_legacy_candidate_dirty_active_cohort_v1'::regclass
        ) !~ 'lower\(btrim\(COALESCE\(\(payload_json ->> ''candidate_id''::text\), ''''::text\)\)\), id\) WHERE .*job_type = ''WORKBENCH_CANDIDATE_DIRTY_APPLY''.*status = ANY.*candidate_id IS NULL' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_DIRTY_COHORT_INDEX_CONTRACT_INVALID'
      USING ERRCODE = '55000', DETAIL = pg_catalog.jsonb_build_object(
        'valid_ready_index_count', v_index_count,
        'candidate_index', pg_catalog.pg_get_indexdef(
          'public.idx_bpay_wb_jobs_candidate_dirty_active_cohort_v1'::regclass
        ),
        'legacy_index', pg_catalog.pg_get_indexdef(
          'public.idx_bpay_wb_jobs_legacy_candidate_dirty_active_cohort_v1'::regclass
        )
      )::text;
  END IF;

  RAISE NOTICE 'PASS: Candidate DIRTY_APPLY cohort routines, ACLs, exact finalized fast path and active-cohort indexes are installed.';
END;
$verification$;

ROLLBACK;
