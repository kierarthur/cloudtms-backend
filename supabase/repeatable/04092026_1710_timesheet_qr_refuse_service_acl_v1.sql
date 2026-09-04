-- Reconcile the historical QR refusal helper with the current Worker-only
-- database boundary. The Office Worker is its only HTTP caller and invokes it
-- with the service role after enforcing the admin and stale-record guards.
-- This changes no Timesheet lifecycle rule or function body.

\set ON_ERROR_STOP on

begin;

alter function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
  owner to postgres;

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

commit;
