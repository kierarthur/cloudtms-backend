-- Purpose-bound service read for pending Cancel and applied same-draft Return.
-- PREPARE requires finalized P0 evidence and is deliberately not reused here.
\set ON_ERROR_STOP on
begin;
create or replace function public.timesheet_correction_request_authority_v1(p_request_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare request private.timesheet_correction_requests%rowtype;
begin
  if private.timesheet_correction_actor_allowed_v1(p_actor_user_id) is distinct from true then
    raise exception 'CORRECTION_ACTOR_NOT_ALLOWED' using errcode='42501';
  end if;
  if p_request_id is null then raise exception 'CORRECTION_REQUEST_ID_REQUIRED' using errcode='22023'; end if;
  select r.* into request from private.timesheet_correction_requests r where r.id=p_request_id;
  if not found then raise exception 'CORRECTION_REQUEST_NOT_FOUND' using errcode='P0002'; end if;
  return jsonb_build_object('contract_version','TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1',
    'request_id',request.id,'state',request.state,'state_revision',request.state_revision::text,
    'replacement_timesheet_id',request.replacement_timesheet_id,'return_invoice_id',request.return_invoice_id);
end;
$function$;
alter function public.timesheet_correction_request_authority_v1(uuid,uuid) owner to postgres;
revoke all on function public.timesheet_correction_request_authority_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_request_authority_v1(uuid,uuid) to service_role;
notify pgrst,'reload schema';
commit;
