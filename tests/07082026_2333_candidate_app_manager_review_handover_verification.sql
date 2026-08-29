-- CloudTMS Candidate App manager-review DB/RPC handover verification.
-- SELECT-only. Run only after installing the bundled migration and repeatable SQL
-- into an approved TEST or disposable PostgreSQL database.

with expected(table_name) as (
  values
    ('candidate_app_accounts'),
    ('candidate_app_sessions'),
    ('candidate_auth_challenges'),
    ('candidate_submission_workflows'),
    ('candidate_submission_components'),
    ('candidate_approval_requests'),
    ('candidate_notifications')
), actual as (
  select c.relname::text as table_name
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public'
    and c.relkind='r'
    and c.relname in (select table_name from expected)
)
select
  'candidate_tables'::text as check_name,
  count(*)::integer as expected_count,
  count(a.table_name)::integer as actual_count,
  coalesce(jsonb_agg(e.table_name order by e.table_name) filter (where a.table_name is null),'[]'::jsonb) as missing
from expected e
left join actual a using (table_name);

with expected(function_name) as (
  values
    ('candidate_auth_account_transition_v1'),
    ('candidate_auth_challenge_transition_v1'),
    ('candidate_app_bootstrap_v1'),
    ('candidate_app_timesheet_page_v1'),
    ('candidate_app_timesheet_detail_v1'),
    ('candidate_missing_week_options_v1'),
    ('candidate_contract_week_add_missing_atomic_v1'),
    ('expense_placement_resolve_v1'),
    ('expense_carrier_resolve_or_create_atomic_v1'),
    ('timesheet_expense_apply_atomic_v1'),
    ('candidate_workflow_transition_atomic_v1'),
    ('candidate_submission_finalize_atomic_v1'),
    ('candidate_submission_reject_atomic_v1'),
    ('candidate_no_work_atomic_v1')
), actual as (
  select p.proname::text as function_name
  from pg_catalog.pg_proc p
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public'
    and p.proname in (select function_name from expected)
)
select
  'candidate_public_rpcs'::text as check_name,
  count(*)::integer as expected_count,
  count(a.function_name)::integer as actual_count,
  coalesce(jsonb_agg(e.function_name order by e.function_name) filter (where a.function_name is null),'[]'::jsonb) as missing
from expected e
left join actual a using (function_name);

with expected(column_name) as (
  values
    ('immutable_submission_json'),
    ('immutable_submission_sha256'),
    ('policy_snapshot_sha256'),
    ('candidate_signature_component_id'),
    ('candidate_signature_sha256'),
    ('candidate_signed_at_utc'),
    ('review_manifest_json'),
    ('review_manifest_sha256'),
    ('manager_signature_component_id'),
    ('manager_signature_sha256'),
    ('manager_approved_at_utc')
), actual as (
  select c.column_name::text
  from information_schema.columns c
  where c.table_schema='public'
    and c.table_name='candidate_submission_workflows'
    and c.column_name in (select column_name from expected)
)
select
  'workflow_manager_review_columns'::text as check_name,
  count(*)::integer as expected_count,
  count(a.column_name)::integer as actual_count,
  coalesce(jsonb_agg(e.column_name order by e.column_name) filter (where a.column_name is null),'[]'::jsonb) as missing
from expected e
left join actual a using (column_name);

with expected(column_name) as (
  values
    ('required'),
    ('review_ordinal'),
    ('review_storage_key'),
    ('review_content_sha256'),
    ('review_page_count'),
    ('review_render_input_sha256'),
    ('review_renderer_contract_version'),
    ('review_renderer_receipt_json'),
    ('review_render_state'),
    ('final_signed_storage_key'),
    ('final_signed_content_sha256'),
    ('final_signed_page_count'),
    ('final_signed_render_input_sha256'),
    ('final_signed_renderer_contract_version'),
    ('final_signed_renderer_receipt_json'),
    ('final_signed_render_state')
), actual as (
  select c.column_name::text
  from information_schema.columns c
  where c.table_schema='public'
    and c.table_name='candidate_submission_components'
    and c.column_name in (select column_name from expected)
)
select
  'component_review_final_lineage_columns'::text as check_name,
  count(*)::integer as expected_count,
  count(a.column_name)::integer as actual_count,
  coalesce(jsonb_agg(e.column_name order by e.column_name) filter (where a.column_name is null),'[]'::jsonb) as missing
from expected e
left join actual a using (column_name);

select
  'electronic_signature_constraint'::text as check_name,
  1::integer as expected_count,
  count(*)::integer as actual_count,
  coalesce(jsonb_agg(pg_get_constraintdef(c.oid)),'[]'::jsonb) as definitions
from pg_catalog.pg_constraint c
join pg_catalog.pg_class r on r.oid=c.conrelid
join pg_catalog.pg_namespace n on n.oid=r.relnamespace
where n.nspname='public'
  and r.relname='timesheets'
  and c.conname='chk_ts_signatures_for_electronic';

select
  'candidate_integrity_indexes'::text as check_name,
  2::integer as expected_count,
  count(*)::integer as actual_count,
  coalesce(jsonb_agg(indexname order by indexname),'[]'::jsonb) as indexes
from pg_catalog.pg_indexes
where schemaname='public'
  and indexname in (
    'candidate_submission_components_hours_review_uq',
    'timesheet_evidence_one_active_timesheet_uq'
  );

select
  'candidate_security_posture'::text as check_name,
  7::integer as expected_count,
  count(*) filter (where c.relrowsecurity and c.relforcerowsecurity)::integer as actual_count,
  coalesce(jsonb_agg(c.relname order by c.relname) filter (where not (c.relrowsecurity and c.relforcerowsecurity)),'[]'::jsonb) as noncompliant
from pg_catalog.pg_class c
join pg_catalog.pg_namespace n on n.oid=c.relnamespace
where n.nspname='public'
  and c.relname in (
    'candidate_app_accounts',
    'candidate_app_sessions',
    'candidate_auth_challenges',
    'candidate_submission_workflows',
    'candidate_submission_components',
    'candidate_approval_requests',
    'candidate_notifications'
  );

select
  'browser_role_function_privileges'::text as check_name,
  0::integer as expected_count,
  count(*)::integer as actual_count,
  coalesce(jsonb_agg(distinct jsonb_build_object('role',r.rolname,'function',p.proname)),'[]'::jsonb) as unexpected
from pg_catalog.pg_proc p
join pg_catalog.pg_namespace n on n.oid=p.pronamespace
join pg_catalog.pg_roles r on r.rolname in ('anon','authenticated')
where n.nspname='public'
  and p.proname in (
    'candidate_auth_account_transition_v1',
    'candidate_auth_challenge_transition_v1',
    'candidate_app_bootstrap_v1',
    'candidate_app_timesheet_page_v1',
    'candidate_app_timesheet_detail_v1',
    'candidate_missing_week_options_v1',
    'candidate_contract_week_add_missing_atomic_v1',
    'expense_placement_resolve_v1',
    'expense_carrier_resolve_or_create_atomic_v1',
    'timesheet_expense_apply_atomic_v1',
    'candidate_workflow_transition_atomic_v1',
    'candidate_submission_finalize_atomic_v1',
    'candidate_submission_reject_atomic_v1',
    'candidate_no_work_atomic_v1'
  )
  and has_function_privilege(r.oid,p.oid,'EXECUTE');
