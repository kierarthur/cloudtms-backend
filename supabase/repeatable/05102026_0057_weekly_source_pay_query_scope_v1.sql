-- Query V3 factual ScopeV2 only. This certifies identity, not authorisation,
-- approved money, missing-source zero, or query clearance. The complete gate
-- and earliest writer closure must be installed/tested together before use.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_pay_query_scope_v1(
  p_root_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_identity jsonb;
  v_booking text;
  v_source boolean;
  v_count integer;
begin
  if p_root_timesheet_id is null then
    raise exception 'WEEKLY_SOURCE_PAY_QUERY_ROOT_REQUIRED' using errcode='22023';
  end if;
  -- This existing identity owner refuses raw/trimmed collisions, multiple
  -- current roots and duplicate family versions. Do not guess a successor.
  v_identity:=private.weekly_source_resolve_root_identity_v1(p_root_timesheet_id);
  if (v_identity->>'ok') is distinct from 'true'
     or (v_identity->>'canonical_timesheet_id')::uuid is distinct from p_root_timesheet_id
     or (v_identity->>'requested_is_canonical') is distinct from 'true'
     or (v_identity->>'family_is_current') is distinct from 'true' then return null; end if;
  select t.* into v_root from public.timesheets t where t.timesheet_id=p_root_timesheet_id;
  if not found or v_root.is_current is distinct from true
     or v_root.revoked_at is not null or v_root.archived_at_utc is not null
     or v_root.sheet_scope is distinct from 'WEEKLY'
     or v_root.line_type is distinct from 'HOURS'
     or v_root.is_adjustment is distinct from false
     or v_root.week_ending_date is null or v_root.version is null or v_root.version<1
     or nullif(btrim(v_root.booking_id),'') is null then return null; end if;
  v_booking:=btrim(v_root.booking_id);
  if btrim(v_identity->>'family_booking_id') is distinct from v_booking then return null; end if;
  select c.* into v_contract from public.contracts c where c.id=v_root.contract_id;
  if not found or v_contract.candidate_id is null or v_contract.client_id is null then return null; end if;
  -- Preserve the established Source route's self-bill domain, including real
  -- first-authorisation transitions. Always execute the installed classifier:
  -- its integrity/permission errors must not be short-circuited or swallowed.
  v_source:=private._candidate_expense_source_family_v1(p_root_timesheet_id);
  if v_contract.self_bill is distinct from true and v_source is distinct from true then
    return null;
  end if;
  select count(*) into v_count from public.contract_weeks cw
    where cw.contract_id=v_root.contract_id and cw.week_ending_date=v_root.week_ending_date
      and cw.additional_seq=0 and cw.is_adjustment is false
      and cw.timesheet_id=p_root_timesheet_id;
  if v_count<>1 then return null; end if;
  select count(*) into v_count from public.weekly_exceptional_pay_target_families f
    where btrim(f.root_family_booking_id)=v_booking;
  if v_count>1 then return null; end if;
  if v_count=1 then
    select f.* into strict v_family from public.weekly_exceptional_pay_target_families f
      where btrim(f.root_family_booking_id)=v_booking;
    if v_family.root_timesheet_id is distinct from p_root_timesheet_id
       or v_family.root_family_booking_id is distinct from v_root.booking_id
       or v_family.candidate_id is distinct from v_contract.candidate_id
       or v_family.contract_id is distinct from v_root.contract_id
       or v_family.week_ending_date is distinct from v_root.week_ending_date
       or v_family.week_start_date is distinct from (v_root.week_ending_date-6)
       or v_family.bound_version is null or v_family.bound_version<1 then return null; end if;
  end if;
  -- Ordinary Source publication does not create an exceptional target family.
  -- Its absence remains NULL on both genuine initial TSFIN and normal HEAD
  -- paths. Never manufacture protected identity to satisfy a query reader.
  return jsonb_build_object('root_timesheet_id',v_root.timesheet_id,
    'family_booking_id',v_root.booking_id,'target_family_id',v_family.id,
    'candidate_id',v_contract.candidate_id,'client_id',v_contract.client_id,
    'contract_id',v_contract.id,'week_ending_date',v_root.week_ending_date,
    'root_version',v_root.version::text,'family_bound_version',v_family.bound_version::text);
end;
$function$;
alter function private.weekly_source_pay_query_scope_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_pay_query_scope_v1(uuid)
  from public,anon,authenticated,service_role;

commit;
