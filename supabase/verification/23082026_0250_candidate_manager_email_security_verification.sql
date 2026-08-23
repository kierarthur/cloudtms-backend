do $verification$
declare
  v_count integer;
  v_settings jsonb;
begin
  if not exists (
    select 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relname='candidate_manager_email_route_receipts'
      and c.relrowsecurity and c.relforcerowsecurity
  ) or not exists (
    select 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relname='candidate_manager_email_template_versions'
      and c.relrowsecurity and c.relforcerowsecurity
  ) then raise exception 'CANDIDATE_MANAGER_EMAIL_RLS_NOT_FORCED'; end if;

  if exists (
    select 1 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    cross join lateral pg_catalog.aclexplode(
      coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))
    ) acl
    where n.nspname='public' and c.relname in (
      'candidate_manager_email_route_receipts','candidate_manager_email_template_versions'
    ) and acl.privilege_type in ('SELECT','INSERT','UPDATE','DELETE')
      and (acl.grantee=0 or acl.grantee in (
        select oid from pg_catalog.pg_roles where rolname in ('anon','authenticated','service_role')
      ))
  ) then raise exception 'CANDIDATE_MANAGER_EMAIL_TABLE_PRIVILEGE_INVALID'; end if;

  select pg_catalog.count(*)::integer into v_count
  from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  where n.nspname in ('public','private')
    and p.proname in (
      '_candidate_manager_terminal_mail_payload_v1',
      'candidate_manager_email_route_receipt_commit_v1',
      'candidate_manager_email_route_receipt_retire_v1',
      'candidate_manager_email_settings_get_v1',
      'candidate_manager_email_settings_set_v1',
      'candidate_manager_email_settings_reset_v1'
    )
    and (
      not p.prosecdef or pg_catalog.pg_get_userbyid(p.proowner)<>'postgres'
      or not exists (
        select 1 from pg_catalog.unnest(coalesce(p.proconfig,array[]::text[])) cfg(setting)
        where cfg.setting like 'search_path=%'
      )
    );
  if v_count<>0 then raise exception 'CANDIDATE_MANAGER_EMAIL_FUNCTION_SECURITY_INVALID %',v_count; end if;

  if exists (
    select 1 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    cross join lateral pg_catalog.aclexplode(
      coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))
    ) acl
    where n.nspname in ('public','private')
      and (p.proname like '%candidate_manager_email%'
        or p.proname='_candidate_manager_terminal_mail_payload_v1')
      and acl.privilege_type='EXECUTE'
      and (acl.grantee=0 or acl.grantee in (
        select oid from pg_catalog.pg_roles where rolname in ('anon','authenticated')
      ))
  ) then raise exception 'CANDIDATE_MANAGER_EMAIL_BROWSER_FUNCTION_PRIVILEGE'; end if;

  if exists(select 1 from public.candidate_manager_email_route_receipts)
     or exists(select 1 from public.candidate_approval_requests where current_manager_route_receipt_id is not null)
     or exists(select 1 from public.candidate_submission_components where manager_signature_capture_method is not null)
     or exists(
       select 1 from public.mail_outbox
       where context_kind='CANDIDATE_WORKFLOW' and payment_scope_json ? 'candidate_manager_mail_kind'
     )
  then raise exception 'CANDIDATE_MANAGER_EMAIL_INSTALL_NOT_INERT'; end if;

  if (select candidate_app_environment from public.settings_defaults where id=1)<>'TEST'
     or exists (
       select 1 from public.settings_defaults s,
         lateral pg_catalog.jsonb_each(s.candidate_app_feature_flags_json) f
       where s.id=1 and f.value='true'::jsonb
     )
  then raise exception 'CANDIDATE_MANAGER_EMAIL_DISABLED_STATE_INVALID'; end if;

  v_settings:=public.candidate_manager_email_settings_get_v1();
  if v_settings->>'ok'<>'true'
     or v_settings->>'sanitizer_policy_version'<>'MANAGER_EMAIL_SAFE_HTML_V1'
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_settings->'templates'->'TIMESHEET'))<>5
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_settings->'templates'->'EXPENSE_CLAIM'))<>5
     or (select pg_catalog.count(*) from public.candidate_manager_email_template_versions)<>1
  then raise exception 'CANDIDATE_MANAGER_EMAIL_TEMPLATE_AUTHORITY_INVALID'; end if;
end
$verification$;

select 'PASS'::text as candidate_manager_email_security_verification;
