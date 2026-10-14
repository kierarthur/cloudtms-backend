create or replace function private._timesheet_settings_authority_frozen_v1(
  p_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_snapshot jsonb; v_version text; v_fingerprint text; v_expected text;
begin
  select t.settings_authority_json,t.settings_authority_version,
    t.settings_authority_fingerprint
  into v_snapshot,v_version,v_fingerprint
  from public.timesheets t
  where t.timesheet_id=p_timesheet_id;
  if not found then
    raise exception 'CONTRACT_SETTINGS_TIMESHEET_NOT_FOUND' using errcode='P0002';
  end if;
  if coalesce(v_snapshot,'{}'::jsonb)='{}'::jsonb
     or v_version is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
     or coalesce(v_fingerprint,'') !~ '^[0-9a-f]{64}$' then
    raise exception 'CONTRACT_SETTINGS_TIMESHEET_AUTHORITY_NOT_FROZEN' using errcode='55000';
  end if;
  v_expected:=encode(digest(convert_to(
    (v_snapshot-'authority_fingerprint'-'resolved_at_utc')::text,'UTF8'
  ),'sha256'),'hex');
  if v_expected is distinct from v_fingerprint
     or v_snapshot->>'authority_fingerprint' is distinct from v_fingerprint then
    raise exception 'CONTRACT_SETTINGS_TIMESHEET_AUTHORITY_INVALID' using errcode='22023';
  end if;
  return v_snapshot;
end
$function$;