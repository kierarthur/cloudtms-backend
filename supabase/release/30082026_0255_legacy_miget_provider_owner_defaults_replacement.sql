-- LEGACY_UPGRADE-only provider-neutral replacement for the immutable Miget
-- provider-owner ACL migration. Historical LIVE lacks TEST-era routines that the
-- generated TEST ACL snapshot assumes already exist. This replacement operates
-- only on existing catalogue objects, installs the current public-relation
-- service deny list, closes browser access, and establishes safe owner defaults.
-- Later repository migrations/repeatables install their reviewed object-specific
-- routine grants. No application row, function body, view definition, finance,
-- Banking Pay economic rule, or Policy X authority is changed.

set local lock_timeout = '5s';
set local statement_timeout = '120s';

revoke all privileges on all tables in schema public from public, anon, authenticated;
revoke all privileges on all sequences in schema public from public, anon, authenticated;
grant all privileges on all tables in schema public to service_role;
grant all privileges on all sequences in schema public to service_role;

do $service_relation_deny_list$
declare
  v_name text;
  v_relation regclass;
begin
  foreach v_name in array array[
    'banking_pay_scope_change_transactions',
    'banking_pay_workbench_case_resolution_carry_registrations',
    'banking_pay_workbench_selection_carry_registrations',
    'candidate_daily_availability_days',
    'candidate_daily_command_receipts',
    'candidate_daily_rota_days',
    'candidate_daily_rota_generations',
    'candidate_daily_sheet_projection_outbox',
    'candidate_home_announcement_versions',
    'candidate_manager_authoriser_policy_receipts',
    'candidate_manager_email_route_receipts',
    'candidate_manager_email_template_versions',
    'hr_issue_email_deliveries',
    'hr_issue_email_delivery_items',
    'import_review_action_outcomes',
    'import_review_daily_timesheet_resolutions',
    'import_review_decisions',
    'import_review_events',
    'import_review_scope_candidates',
    'import_review_scope_clients',
    'import_review_states',
    'import_review_weekly_validation_resolutions',
    'pay_payment_correction_request_candidates',
    'timesheet_archive_transition_capability',
    'timesheet_financial_retention',
    'timesheet_r2_cleanup_queue'
  ] loop
    v_relation := to_regclass(format('public.%I',v_name));
    if v_relation is not null then
      execute format('revoke all privileges on table public.%I from service_role',v_name);
    end if;
  end loop;

  v_relation := to_regclass('public.import_review_events_id_seq');
  if v_relation is not null then
    revoke all privileges on sequence public.import_review_events_id_seq from service_role;
  end if;
end
$service_relation_deny_list$;

do $existing_function_acl$
declare
  v_signature regprocedure;
begin
  for v_signature in
    select p.oid::regprocedure
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
    order by p.oid
  loop
    execute format(
      'revoke all privileges on function %s from PUBLIC, anon, authenticated',
      v_signature
    );
  end loop;
end
$existing_function_acl$;

revoke all privileges on all functions in schema private
  from public, anon, authenticated;

revoke all privileges on table public.candidate_manager_email_route_receipts
  from public, anon, authenticated, service_role;
revoke all privileges on table public.candidate_manager_email_template_versions
  from public, anon, authenticated, service_role;

revoke all privileges on function public.cloudtms_data_api_mfa_gate()
  from public, anon, authenticated, service_role;
grant execute on function public.cloudtms_data_api_mfa_gate()
  to anon, authenticated, service_role;

alter default privileges for role current_user in schema public
  revoke all on tables from public, anon, authenticated;
alter default privileges for role current_user in schema public
  grant all on tables to service_role;
alter default privileges for role current_user in schema public
  revoke all on sequences from public, anon, authenticated;
alter default privileges for role current_user in schema public
  grant all on sequences to service_role;
alter default privileges for role current_user in schema public
  revoke all on functions from public, anon, authenticated;
alter default privileges for role current_user in schema public
  grant all on functions to service_role;

do $verification$
declare
  v_expected_denied text[] := array[
    'banking_pay_scope_change_transactions',
    'banking_pay_workbench_case_resolution_carry_registrations',
    'banking_pay_workbench_selection_carry_registrations',
    'candidate_daily_availability_days',
    'candidate_daily_command_receipts',
    'candidate_daily_rota_days',
    'candidate_daily_rota_generations',
    'candidate_daily_sheet_projection_outbox',
    'candidate_home_announcement_versions',
    'candidate_manager_authoriser_policy_receipts',
    'candidate_manager_email_route_receipts',
    'candidate_manager_email_template_versions',
    'hr_issue_email_deliveries',
    'hr_issue_email_delivery_items',
    'import_review_action_outcomes',
    'import_review_daily_timesheet_resolutions',
    'import_review_decisions',
    'import_review_events',
    'import_review_scope_candidates',
    'import_review_scope_clients',
    'import_review_states',
    'import_review_weekly_validation_resolutions',
    'pay_payment_correction_request_candidates',
    'timesheet_archive_transition_capability',
    'timesheet_financial_retention',
    'timesheet_r2_cleanup_queue'
  ];
begin
  if exists(
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and (
        has_function_privilege('anon',p.oid,'EXECUTE')
        or has_function_privilege('authenticated',p.oid,'EXECUTE')
      )
  ) then
    raise exception 'LEGACY_PROVIDER_OWNER_PUBLIC_SECURITY_DEFINER_EXPOSURE';
  end if;

  if exists(
    select 1
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private'
      and (
        has_function_privilege('anon',p.oid,'EXECUTE')
        or has_function_privilege('authenticated',p.oid,'EXECUTE')
      )
  ) then
    raise exception 'LEGACY_PROVIDER_OWNER_PRIVATE_FUNCTION_EXPOSURE';
  end if;

  if exists(
    select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind in('r','p')
      and c.relname=any(v_expected_denied)
      and has_table_privilege('service_role',c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
  ) then
    raise exception 'LEGACY_PROVIDER_OWNER_SERVICE_DENY_LIST_FAILED';
  end if;

  if not has_function_privilege('anon','public.cloudtms_data_api_mfa_gate()','EXECUTE')
     or not has_function_privilege('authenticated','public.cloudtms_data_api_mfa_gate()','EXECUTE')
     or not has_function_privilege('service_role','public.cloudtms_data_api_mfa_gate()','EXECUTE') then
    raise exception 'LEGACY_PROVIDER_OWNER_MFA_GATE_UNAVAILABLE';
  end if;

  if 3<>(
    select count(*) from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    where n.nspname='public'
      and pg_catalog.pg_get_userbyid(d.defaclrole)=current_user
      and d.defaclobjtype in('r','S','f')
  ) then
    raise exception 'LEGACY_PROVIDER_OWNER_DEFAULT_ACL_MISSING';
  end if;

  if exists(
    select 1 from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    cross join lateral pg_catalog.aclexplode(d.defaclacl) acl
    left join pg_catalog.pg_roles grantee on grantee.oid=acl.grantee
    where n.nspname='public'
      and pg_catalog.pg_get_userbyid(d.defaclrole)=current_user
      and d.defaclobjtype in('r','S','f')
      and (acl.grantee=0 or grantee.rolname in('anon','authenticated'))
  ) then
    raise exception 'LEGACY_PROVIDER_OWNER_BROWSER_DEFAULT_ACL_PRESENT';
  end if;
end
$verification$;
