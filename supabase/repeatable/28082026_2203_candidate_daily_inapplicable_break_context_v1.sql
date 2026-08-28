-- Repeatable CloudTMS function/view authority: candidate_daily_inapplicable_break_context_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_daily_break_entry_v1(
  p_booking_id text,p_work_date date,p_client_id uuid default null,
  p_contract_id uuid default null,p_applicable boolean default true
) returns jsonb language plpgsql stable security definer set search_path=''
as $function$
declare
  v_resolution jsonb;
  v_applicable boolean:=coalesce(p_applicable,false);
  v_identity text;
  v_reason text;
begin
  if nullif(btrim(p_booking_id),'') is null or p_work_date is null then
    raise exception 'CANDIDATE_DAILY_IDENTITY_INVALID' using errcode='22023';
  end if;
  -- A view-only Manual/import/protected record has no break-entry controls.
  -- Its historical Client settings must not prevent the Candidate reading it
  -- (or every other row on the same page). Editable records retain the exact
  -- existing precedence/error checks; this does not invent missing settings.
  if v_applicable and p_client_id is not null then
    v_resolution:=private._timesheet_break_entry_precedence_v1(
      p_client_id,p_contract_id,p_work_date);
  else
    v_resolution:=jsonb_build_object('mode','START_END_TIMES','source','DEFAULT',
      'settings_as_of_date',p_work_date);
  end if;
  v_applicable:=v_applicable and not coalesce((v_resolution->>'is_nhsp')::boolean,false)
    and not coalesce((v_resolution->>'no_timesheet_required')::boolean,false);
  v_reason:=case when v_applicable then 'CANDIDATE_EDITABLE_ELECTRONIC'
    else 'NOT_CANDIDATE_EDITABLE' end;
  v_identity:=concat_ws('|','CANDIDATE_DAILY_BREAK_ENTRY_V1',p_booking_id,p_work_date,
    p_client_id,p_contract_id,v_resolution->>'client_settings_id',
    v_resolution->>'mode',v_resolution->>'source',v_applicable,v_reason);
  return jsonb_build_object('applicable',v_applicable,
    'mode',case when v_applicable then v_resolution->'mode' else 'null'::jsonb end,
    'source',case when v_applicable then v_resolution->'source'
      else to_jsonb('NOT_APPLICABLE'::text) end,'reason',v_reason,
    'context_version','CANDIDATE_BREAK_ENTRY_V1',
    'context_token',encode(extensions.digest(v_identity,'sha256'),'hex'));
end;
$function$;

alter function private._candidate_daily_break_entry_v1(text,date,uuid,uuid,boolean) owner to postgres;
revoke all on function private._candidate_daily_break_entry_v1(text,date,uuid,uuid,boolean)
  from public,anon,authenticated,service_role;

commit;
