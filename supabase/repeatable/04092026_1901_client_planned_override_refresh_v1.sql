-- Repeatable CloudTMS function/view authority: client_planned_override_refresh_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Client changes must revisit every remaining planned Contract week.  The
-- central resolver, rather than this row-selection helper, owns the
-- setting-by-setting Client/Contract precedence.  Excluding an entire Contract
-- merely because its broad override is enabled would leave its still
-- Client-owned values stale.
create or replace function private._contract_settings_refresh_planned_for_client_v1(
  p_client_id uuid
)
returns integer
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_row record; v_count integer:=0;
begin
  for v_row in
    select cw.id
    from public.contract_weeks cw
    join public.contracts c on c.id=cw.contract_id
    where c.client_id=p_client_id
      and cw.timesheet_id is null
    order by c.id,cw.week_ending_date,cw.id
  loop
    if private._contract_settings_apply_week_snapshot_v1(v_row.id) then
      v_count:=v_count+1;
    end if;
  end loop;
  return v_count;
end
$function$;

alter function private._contract_settings_refresh_planned_for_client_v1(uuid)
  owner to postgres;
revoke all on function private._contract_settings_refresh_planned_for_client_v1(uuid)
  from public,anon,authenticated,service_role;

commit;
