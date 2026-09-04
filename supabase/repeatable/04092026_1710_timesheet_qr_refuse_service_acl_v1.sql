-- Reconcile the historical QR refusal helper with the current Worker-only
-- database boundary. The Office Worker is its only HTTP caller and invokes it
-- with the service role after enforcing the admin and stale-record guards.
-- This changes no Timesheet lifecycle rule or function body.

\set ON_ERROR_STOP on

begin;

alter function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
  owner to postgres;

revoke all on function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
  from public,anon,authenticated,service_role,authenticator,supabase_admin;

grant execute on function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid)
  to postgres,service_role;

commit;
