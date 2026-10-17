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