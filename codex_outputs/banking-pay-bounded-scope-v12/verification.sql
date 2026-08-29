-- Banking Pay bounded-scope Version 1.2.4: bounded read-only verification.
-- Safe to run only against the explicitly approved TEST project. No DDL/DML.

SELECT current_database() AS database_name,
  current_setting('server_version') AS server_version,
  clock_timestamp() AS observed_at_utc;

-- Exact private table surface.
SELECT count(*)::integer AS private_table_count
FROM pg_catalog.pg_class relation
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
WHERE namespace.nspname='private' AND relation.relkind='r'
  AND relation.relname IN (
    'banking_pay_workbench_candidate_scope_registry',
    'banking_pay_workbench_timesheet_scope_state',
    'banking_pay_workbench_economic_builds',
    'banking_pay_workbench_economic_build_scope',
    'banking_pay_workbench_economic_build_facts',
    'banking_pay_workbench_economic_build_fact_pages',
    'banking_pay_workbench_stage_attempts',
    'banking_pay_workbench_canonical_stage_lines'
  );

-- Named constraints created by the migration: expected 88.
SELECT count(*)::integer AS bounded_scope_named_constraint_count
FROM pg_catalog.pg_constraint constraint_row
JOIN pg_catalog.pg_class table_relation
  ON table_relation.oid=constraint_row.conrelid
JOIN pg_catalog.pg_namespace namespace
  ON namespace.oid=table_relation.relnamespace
WHERE constraint_row.contype<>'n'
  AND (
    (namespace.nspname='private' AND table_relation.relname IN (
      'banking_pay_workbench_candidate_scope_registry',
      'banking_pay_workbench_timesheet_scope_state',
      'banking_pay_workbench_economic_builds',
      'banking_pay_workbench_economic_build_scope',
      'banking_pay_workbench_economic_build_facts',
      'banking_pay_workbench_economic_build_fact_pages',
      'banking_pay_workbench_stage_attempts',
      'banking_pay_workbench_canonical_stage_lines'
    ))
    OR (namespace.nspname='public'
      AND table_relation.relname='banking_pay_workbench_jobs'
      AND constraint_row.conname IN (
        'bpay_wb_jobs_economic_build_fk',
        'bpay_wb_jobs_private_cursor_chk',
        'bpay_wb_jobs_private_stage_cursor_chk',
        'bpay_wb_jobs_build_identity_chk'
      ))
    OR (namespace.nspname='public'
      AND table_relation.relname='settings_defaults'
      AND constraint_row.conname IN (
        'settings_bpay_wb_reconciliation_envelope_version_chk',
        'settings_bpay_wb_reconciliation_envelope_json_chk'
      ))
  );

-- All planned indexes: expected 47 rows. Constraint-owned indexes are included.
SELECT count(*)::integer AS bounded_scope_index_count
FROM pg_catalog.pg_class index_relation
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=index_relation.relnamespace
WHERE index_relation.relkind='i'
  AND (
    (namespace.nspname='private' AND EXISTS (
      SELECT 1
      FROM pg_catalog.pg_index index_meta
      JOIN pg_catalog.pg_class table_relation
        ON table_relation.oid=index_meta.indrelid
      JOIN pg_catalog.pg_namespace table_namespace
        ON table_namespace.oid=table_relation.relnamespace
      WHERE index_meta.indexrelid=index_relation.oid
        AND table_namespace.nspname='private'
        AND table_relation.relname IN (
          'banking_pay_workbench_candidate_scope_registry',
          'banking_pay_workbench_timesheet_scope_state',
          'banking_pay_workbench_economic_builds',
          'banking_pay_workbench_economic_build_scope',
          'banking_pay_workbench_economic_build_facts',
          'banking_pay_workbench_economic_build_fact_pages',
          'banking_pay_workbench_stage_attempts',
          'banking_pay_workbench_canonical_stage_lines'
        )
    ))
    OR index_relation.relname IN (
      'bpay_wb_jobs_source_claim_idx',
      'bpay_advances_candidate_open_timesheet_idx',
      'bpay_finance_components_candidate_open_timesheet_idx',
      'bpay_timesheet_overrides_candidate_active_idx',
      'bpay_ts_adjustments_candidate_unpaid_idx',
      'bpay_wb_jobs_economic_build_idx'
    )
  );

-- DB-CONSTRAINT-079/DB-INDEX-035: real table constraint, constraint-owned
-- backing index, and PostgreSQL NULLS NOT DISTINCT semantics.
SELECT constraint_row.conname,constraint_row.contype,
  index_relation.relname AS backing_index,index_meta.indnullsnotdistinct,
  pg_catalog.pg_get_constraintdef(constraint_row.oid) AS definition
FROM pg_catalog.pg_constraint constraint_row
JOIN pg_catalog.pg_class table_relation ON table_relation.oid=constraint_row.conrelid
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=table_relation.relnamespace
JOIN pg_catalog.pg_class index_relation ON index_relation.oid=constraint_row.conindid
JOIN pg_catalog.pg_index index_meta ON index_meta.indexrelid=index_relation.oid
WHERE namespace.nspname='private'
  AND table_relation.relname='banking_pay_workbench_canonical_stage_lines'
  AND constraint_row.conname='bpay_wb_canonical_stage_identity_uq';

-- Private table ACL must expose no browser/service-role privilege.
SELECT grantee,table_name,privilege_type
FROM information_schema.role_table_grants
WHERE table_schema='private'
  AND table_name LIKE 'banking_pay_workbench_%'
  AND grantee IN ('PUBLIC','anon','authenticated','service_role')
ORDER BY table_name,grantee,privilege_type;

-- New function identities and hardening. Expected nine identities.
SELECT namespace.nspname,function_row.proname,
  pg_catalog.pg_get_function_identity_arguments(function_row.oid) AS identity_args,
  owner_role.rolname AS owner,function_row.prosecdef,function_row.provolatile,
  function_row.proparallel,function_row.proconfig,function_row.proacl
FROM pg_catalog.pg_proc function_row
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=function_row.pronamespace
JOIN pg_catalog.pg_roles owner_role ON owner_role.oid=function_row.proowner
WHERE (namespace.nspname='private' AND function_row.proname IN (
    'pay_workbench_scope_invalidate_v1',
    'pay_workbench_financial_scope_dirty_transition_v1',
    'pay_workbench_candidate_bounded_scope_v1',
    'pay_workbench_timesheet_dependency_closure_v2',
    'pay_workbench_timesheet_input_fingerprint_v1',
    'pay_current_timesheet_entitlement_components_from_build_v1',
    'pay_sync_overpayments_from_workbench_workspace_v1'
  )) OR (namespace.nspname='public' AND function_row.proname IN (
    'pay_workbench_source_build_attempt_claim_start_v1',
    'pay_workbench_source_build_attempt_execute_v1'
  ))
ORDER BY namespace.nspname,function_row.proname,identity_args;

-- No accidental overloads for any new identity.
SELECT namespace.nspname,function_row.proname,count(*)::integer AS overload_count
FROM pg_catalog.pg_proc function_row
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=function_row.pronamespace
WHERE function_row.proname IN (
  'pay_workbench_scope_invalidate_v1',
  'pay_workbench_financial_scope_dirty_transition_v1',
  'pay_workbench_candidate_bounded_scope_v1',
  'pay_workbench_timesheet_dependency_closure_v2',
  'pay_workbench_timesheet_input_fingerprint_v1',
  'pay_current_timesheet_entitlement_components_from_build_v1',
  'pay_sync_overpayments_from_workbench_workspace_v1',
  'pay_workbench_source_build_attempt_claim_start_v1',
  'pay_workbench_source_build_attempt_execute_v1'
)
GROUP BY namespace.nspname,function_row.proname
HAVING count(*)<>1;

-- New statement trigger surface. Expected 16; all are statement-level.
SELECT count(*)::integer AS trigger_count,
  count(*) FILTER (WHERE trigger_row.tgtype & 1 = 0)::integer
    AS statement_trigger_count
FROM pg_catalog.pg_trigger trigger_row
JOIN pg_catalog.pg_proc function_row ON function_row.oid=trigger_row.tgfoid
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=function_row.pronamespace
WHERE NOT trigger_row.tgisinternal
  AND namespace.nspname='private'
  AND function_row.proname='pay_workbench_financial_scope_dirty_transition_v1';

-- The three retained row triggers must be BEFORE ROW so exact transition
-- digests are declared before authorised Workbench finance DML.
SELECT table_relation.relname,trigger_row.tgname,
  CASE WHEN trigger_row.tgtype & 2 = 2 THEN 'BEFORE' ELSE 'AFTER' END AS timing,
  CASE WHEN trigger_row.tgtype & 1 = 1 THEN 'ROW' ELSE 'STATEMENT' END AS trigger_level
FROM pg_catalog.pg_trigger trigger_row
JOIN pg_catalog.pg_class table_relation ON table_relation.oid=trigger_row.tgrelid
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=table_relation.relnamespace
WHERE NOT trigger_row.tgisinternal AND namespace.nspname='public'
  AND trigger_row.tgname IN (
    'trg_pay_workbench_mark_candidate_dirty__pay_advances',
    'trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_com',
    'trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_eve'
  )
ORDER BY table_relation.relname,trigger_row.tgname;

-- No malformed committed private-stage job may exist.
SELECT count(*)::integer AS malformed_source_job_count
FROM public.banking_pay_workbench_jobs job
WHERE job.job_type='WORKBENCH_CANDIDATE_SOURCE_BUILD'
  AND NOT (
    (job.economic_build_id IS NULL AND job.private_stage IS NULL
      AND job.private_cursor_kind IS NULL AND job.private_stage_version IS NULL
      AND job.private_cursor_json='{}'::jsonb)
    OR (job.economic_build_id IS NULL AND job.status='QUEUED'
      AND job.private_stage='BUILD_INITIALISE'
      AND job.private_cursor_kind='BUILD_INITIALISE'
      AND job.private_stage_version=1 AND job.attempt_count=0)
    OR (job.economic_build_id IS NOT NULL
      AND job.private_stage IS NOT NULL
      AND job.private_stage<>'BUILD_INITIALISE'
      AND job.private_cursor_kind IS NOT NULL
      AND job.private_stage_version IS NOT NULL)
  );

-- One active build/candidate and one STARTED attempt/job are also enforced by
-- partial unique indexes; these queries independently prove no duplicate.
SELECT candidate_id,count(*)::integer AS active_build_count
FROM private.banking_pay_workbench_economic_builds
WHERE status IN ('COLLECTING','READY_FOR_RECONCILIATION','RECONCILING',
  'RECONCILED','PUBLISHING','BLOCKED_UNVALIDATED_RECONCILIATION_SCALE')
GROUP BY candidate_id HAVING count(*)>1;

SELECT job_id,count(*)::integer AS started_attempt_count
FROM private.banking_pay_workbench_stage_attempts
WHERE attempt_status='STARTED'
GROUP BY job_id HAVING count(*)>1;

-- No dangling private graph or detached public job reference.
SELECT
  count(*) FILTER (WHERE build_row.id IS NULL)::integer AS missing_builds,
  count(*) FILTER (WHERE job.id IS NULL)::integer AS missing_jobs
FROM private.banking_pay_workbench_stage_attempts attempt
LEFT JOIN private.banking_pay_workbench_economic_builds build_row
  ON build_row.id=attempt.build_id
LEFT JOIN public.banking_pay_workbench_jobs job ON job.id=attempt.job_id;

-- Ordinary selector authority: report only active-state counts, never closed
-- history. This shape must use bpay_wb_timesheet_scope_active_idx.
EXPLAIN (COSTS,VERBOSE)
SELECT timesheet_id,dirty_generation
FROM private.banking_pay_workbench_timesheet_scope_state
WHERE candidate_id='00000000-0000-0000-0000-000000000000'::uuid
  AND economic_state IN ('DIRTY','LIVE')
ORDER BY dirty_generation,timesheet_id;

-- Metadata-only COMPLETE success probe. Empty on a fully sealed build and
-- backed by bpay_wb_economic_build_scope_incomplete_idx.
EXPLAIN (COSTS,VERBOSE)
SELECT 1
FROM private.banking_pay_workbench_economic_build_scope
WHERE build_id='00000000-0000-0000-0000-000000000000'::uuid
  AND closure_status<>'SEALED'
LIMIT 1;

-- Bootstrap/READY safety gaps. All counts must be zero before cutover.
SELECT
  count(*) FILTER (WHERE registry.initialisation_status='READY'
    AND registry.evaluated_generation<>registry.dirty_generation)::integer
      AS ready_generation_mismatch,
  count(*) FILTER (WHERE registry.initialisation_status='READY'
    AND EXISTS (
      SELECT 1 FROM private.banking_pay_workbench_timesheet_scope_state state_row
      WHERE state_row.candidate_id=registry.candidate_id
        AND state_row.economic_state='DIRTY'
    ))::integer AS ready_with_dirty_state,
  count(*) FILTER (WHERE registry.current_build_id IS NOT NULL
    AND build_row.id IS NULL)::integer AS dangling_current_build
FROM private.banking_pay_workbench_candidate_scope_registry registry
LEFT JOIN private.banking_pay_workbench_economic_builds build_row
  ON build_row.id=registry.current_build_id;

-- Source lines retain the installed public lifecycle. STAGED must not exist.
SELECT pg_catalog.pg_get_constraintdef(constraint_row.oid) AS status_constraint
FROM pg_catalog.pg_constraint constraint_row
JOIN pg_catalog.pg_class table_relation ON table_relation.oid=constraint_row.conrelid
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=table_relation.relnamespace
WHERE namespace.nspname='public'
  AND table_relation.relname='banking_pay_workbench_candidate_source_lines'
  AND pg_catalog.pg_get_constraintdef(constraint_row.oid) ILIKE '%status%';

-- Policy X source audit: no post-draft helper was replaced by this change.
SELECT function_row.proname,md5(pg_catalog.pg_get_functiondef(function_row.oid))
  AS installed_md5
FROM pg_catalog.pg_proc function_row
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=function_row.pronamespace
WHERE namespace.nspname='public' AND function_row.proname IN (
  '_pay_batch_item_economic_components',
  '_pay_batch_item_source_reservation_amount_ex_vat',
  '_pay_policy_x_resolve_post_draft_economic_key'
)
ORDER BY function_row.proname;
