-- Reconcile the historical QR refusal helper with the current Worker-only
-- database boundary. The Office Worker is its only HTTP caller and invokes it
-- with the service role after enforcing the admin and stale-record guards.
-- This changes no Timesheet lifecycle rule or function body.

\set ON_ERROR_STOP on

begin;

alter function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
  owner to postgres;
alter function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
  set search_path = pg_catalog, public, private, extensions, pg_temp;

revoke all on function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
  from public,anon,authenticated,service_role;

do $acl$
begin
  if exists (
    select 1 from pg_catalog.pg_roles where rolname='authenticator'
  ) then
    execute 'revoke all on function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid) from authenticator';
  end if;
  if exists (
    select 1 from pg_catalog.pg_roles where rolname='supabase_admin'
  ) then
    execute 'revoke all on function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid) from supabase_admin';
  end if;
end;
$acl$;

grant execute on function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
  to postgres,service_role;

-- A later replay of the historical function definition can restore its old
-- browser grant while this repeatable remains unchanged in the release
-- ledger.  Reassert and prove the effective role privileges whenever this
-- closure is selected for release.
do $verify_acl$
begin
  if has_function_privilege(
       'anon',
       'public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)',
       'EXECUTE'
     )
     or has_function_privilege(
       'authenticated',
       'public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)',
       'EXECUTE'
     )
     or not has_function_privilege(
       'service_role',
       'public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)',
       'EXECUTE'
     ) then
    raise exception 'TIMESHEET_QR_REFUSE_SERVICE_ACL_INVALID';
  end if;
end;
$verify_acl$;

commit;
