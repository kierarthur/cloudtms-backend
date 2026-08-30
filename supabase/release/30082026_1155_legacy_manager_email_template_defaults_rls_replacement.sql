-- Provider-safe LEGACY_UPGRADE replacement for
-- 23082026_1337_manager_email_candidate_identity_defaults.sql.
--
-- The preceding immutable migration intentionally FORCEs RLS on the template
-- history table and exposes no policies. Supabase's provider owner could still
-- execute the later seed insert, while Miget's non-superuser owner cannot. This
-- replacement preserves the original data-conditional update and restores the
-- exact forced-RLS state inside the same atomic release transaction.

do $guard$
declare
  v_table pg_catalog.pg_class%rowtype;
begin
  select c.* into strict v_table
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public'
    and c.relname='candidate_manager_email_template_versions'
    and c.relkind='r';

  if pg_catalog.pg_get_userbyid(v_table.relowner)<>current_user
     or not v_table.relrowsecurity
     or not v_table.relforcerowsecurity
     or exists (
       select 1
       from pg_catalog.pg_policy p
       where p.polrelid=v_table.oid
     )
     or pg_catalog.pg_get_userbyid(
       (select c.relowner from pg_catalog.pg_class c where c.oid='public.settings_defaults'::regclass)
     )<>current_user
  then
    raise exception using
      errcode='check_violation',
      message='LEGACY_MANAGER_EMAIL_TEMPLATE_RLS_SHAPE_MISMATCH';
  end if;
end
$guard$;

alter table public.candidate_manager_email_template_versions no force row level security;

do $migration$
declare
  v_settings public.settings_defaults%rowtype;
  v_templates jsonb;
  v_version bigint;
  v_hash bytea;
  v_changed boolean:=false;
  v_body constant text:='The below candidate has submitted a timesheet or expenses for approval. You can approve or refuse the complete submission using the secure link below.';
  v_html constant text:='<p>The below candidate has submitted a timesheet or expenses for approval. You can approve or refuse the complete submission using the secure link below.</p>';
begin
  select * into strict v_settings from public.settings_defaults where id=1 for update;
  v_templates:=v_settings.candidate_manager_email_templates_json;

  if v_templates#>>'{TIMESHEET,INITIAL,body_text}'=
      'Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.' then
    v_templates:=jsonb_set(v_templates,'{TIMESHEET,INITIAL,body_text}',to_jsonb(v_body),false);
    v_templates:=jsonb_set(v_templates,'{TIMESHEET,INITIAL,body_html}',to_jsonb(v_html),false);
    v_changed:=true;
  end if;
  if v_templates#>>'{EXPENSE_CLAIM,INITIAL,body_text}'=
      'Please review every page of the submitted documents. You can approve or refuse the complete submission using the secure link below.' then
    v_templates:=jsonb_set(v_templates,'{EXPENSE_CLAIM,INITIAL,body_text}',to_jsonb(v_body),false);
    v_templates:=jsonb_set(v_templates,'{EXPENSE_CLAIM,INITIAL,body_html}',to_jsonb(v_html),false);
    v_changed:=true;
  end if;

  if v_changed then
    v_version:=v_settings.candidate_manager_email_templates_version+1;
    v_hash:=extensions.digest(pg_catalog.convert_to(v_templates::text,'UTF8'),'sha256');
    update public.settings_defaults set
      candidate_manager_email_templates_json=v_templates,
      candidate_manager_email_templates_version=v_version,
      candidate_manager_email_templates_sha256=v_hash,
      candidate_manager_email_templates_updated_at_utc=pg_catalog.transaction_timestamp(),
      candidate_manager_email_templates_updated_by_hmac=null
    where id=1;
    insert into public.candidate_manager_email_template_versions(
      version,templates_json,sanitizer_policy_version,semantic_sha256,
      actor_identity_hmac,idempotency_key,reason_code,recorded_at_utc
    ) values (
      v_version,v_templates,v_settings.candidate_manager_email_sanitizer_policy_version,v_hash,
      null,'manager-email-candidate-identity-defaults-20260823','OFFICE_SAVE',
      pg_catalog.transaction_timestamp()
    );
  end if;
end
$migration$;

alter table public.candidate_manager_email_template_versions force row level security;

do $verification$
begin
  if not exists (
    select 1
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public'
      and c.relname='candidate_manager_email_template_versions'
      and c.relkind='r'
      and c.relrowsecurity
      and c.relforcerowsecurity
  ) then
    raise exception using
      errcode='check_violation',
      message='LEGACY_MANAGER_EMAIL_TEMPLATE_FORCE_RLS_NOT_RESTORED';
  end if;
end
$verification$;
