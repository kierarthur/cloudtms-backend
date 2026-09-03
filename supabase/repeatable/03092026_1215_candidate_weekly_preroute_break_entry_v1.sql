-- Expose the configured break-entry mode while an editable Weekly Timesheet is
-- still waiting for its final submission route to be selected. The selected
-- Electronic/Paper/QR routes retain the same context identity and every
-- protected/import-authoritative/unrelated route remains inapplicable.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_break_entry_context_core_v1(
  p_timesheet_id uuid,
  p_contract_week_id uuid,
  p_as_of_date date,
  p_capabilities jsonb
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_timesheet public.timesheets%rowtype;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_resolution jsonb;
  v_applicable boolean;
  v_reason text;
  v_context_identity text;
begin
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current=true and archived_at_utc is null;
  end if;
  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  elsif v_timesheet.timesheet_id is not null then
    select * into v_week from public.contract_weeks
    where timesheet_id=v_timesheet.timesheet_id
    order by updated_at desc,id desc limit 1;
  end if;
  if v_week.id is not null then
    select * into v_contract from public.contracts where id=v_week.contract_id;
  elsif v_timesheet.contract_id is not null then
    select * into v_contract from public.contracts where id=v_timesheet.contract_id;
  end if;
  if not found or v_contract.id is null then
    raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002';
  end if;

  v_resolution:=private._timesheet_break_entry_precedence_v1(
    v_contract.client_id,v_contract.id,p_as_of_date
  );
  v_applicable:=coalesce((p_capabilities->>'can_edit_hours')::boolean,false)
    and not coalesce((p_capabilities->>'import_authoritative')::boolean,false)
    and coalesce(p_capabilities->>'route_family','') in ('','ELECTRONIC','PAPER','QR')
    and not coalesce((v_resolution->>'is_nhsp')::boolean,false)
    and not coalesce((v_resolution->>'no_timesheet_required')::boolean,false);
  v_reason:=case
    when v_applicable then 'CANDIDATE_EDITABLE_ELECTRONIC'
    when coalesce((p_capabilities->>'import_authoritative')::boolean,false)
      then 'IMPORT_AUTHORITATIVE'
    when coalesce((v_resolution->>'is_nhsp')::boolean,false) then 'NHSP'
    when coalesce((v_resolution->>'no_timesheet_required')::boolean,false)
      then 'NO_TIMESHEET_REQUIRED'
    when coalesce(p_capabilities->>'route_family','') not in ('','ELECTRONIC','PAPER','QR')
      then 'NON_ELECTRONIC_ROUTE'
    else 'NOT_CANDIDATE_EDITABLE'
  end;
  v_context_identity:=concat_ws('|',
    'CANDIDATE_BREAK_ENTRY_V1',v_contract.client_id,v_contract.id,
    coalesce(v_timesheet.timesheet_id::text,''),coalesce(v_week.id::text,''),
    v_resolution->>'settings_as_of_date',v_resolution->>'client_settings_id',
    v_resolution->>'mode',v_resolution->>'source',v_applicable,v_reason
  );
  return jsonb_build_object(
    'applicable',v_applicable,
    'mode',case when v_applicable then v_resolution->'mode' else 'null'::jsonb end,
    'source',case when v_applicable then v_resolution->'source'
      else to_jsonb('NOT_APPLICABLE'::text) end,
    'reason',v_reason,
    'context_version','CANDIDATE_BREAK_ENTRY_V1',
    'context_token',encode(extensions.digest(v_context_identity,'sha256'),'hex')
  );
end
$function$;

alter function private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb)
  owner to postgres;
revoke all on function private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb)
  from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';

commit;
