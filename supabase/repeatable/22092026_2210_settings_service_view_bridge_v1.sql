begin;

-- The Office summary and invoice precheck are service-only security-invoker
-- views.  Their historical definitions pre-date the dated settings authority
-- and call its private helpers directly.  Keep those helpers private and route
-- the views through service-only SECURITY DEFINER wrappers instead.
create or replace function public.timesheet_settings_authority_frozen_get_v1(
  p_timesheet_id uuid
)
returns jsonb
language sql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
  select private._timesheet_settings_authority_frozen_v1(p_timesheet_id)
$function$;

alter function public.timesheet_settings_authority_frozen_get_v1(uuid) owner to postgres;
revoke all on function public.timesheet_settings_authority_frozen_get_v1(uuid)
  from public,anon,authenticated;
grant execute on function public.timesheet_settings_authority_frozen_get_v1(uuid)
  to service_role;

do $settings_service_view_bridge$
declare
  v_row record;
  v_definition text;
  v_core_count integer;
  v_frozen_count integer;
begin
  for v_row in
    select *
    from (values
      ('public','v_timesheets_summary_base',2,0),
      ('public','v_ts_invoice_precheck',1,1)
    ) as expected(schema_name,view_name,core_count,frozen_count)
  loop
    select pg_get_viewdef(c.oid,true),
           coalesce((length(pg_get_viewdef(c.oid,true))
             -length(replace(pg_get_viewdef(c.oid,true),
               'private._contract_settings_effective_core_v1','')))
             /nullif(length('private._contract_settings_effective_core_v1'),0),0),
           coalesce((length(pg_get_viewdef(c.oid,true))
             -length(replace(pg_get_viewdef(c.oid,true),
               'private._timesheet_settings_authority_frozen_v1','')))
             /nullif(length('private._timesheet_settings_authority_frozen_v1'),0),0)
      into strict v_definition,v_core_count,v_frozen_count
    from pg_class c
    join pg_namespace n on n.oid=c.relnamespace
    where n.nspname=v_row.schema_name
      and c.relname=v_row.view_name
      and c.relkind='v';

    if v_core_count<>v_row.core_count or v_frozen_count<>v_row.frozen_count then
      raise exception 'SETTINGS_SERVICE_VIEW_BRIDGE_SOURCE_DRIFT:%:CORE_%_EXPECTED_%:FROZEN_%_EXPECTED_%',
        v_row.view_name,v_core_count,v_row.core_count,v_frozen_count,v_row.frozen_count
        using errcode='55000';
    end if;

    v_definition:=replace(
      v_definition,
      'private._contract_settings_effective_core_v1',
      'public.contract_settings_effective_get_v1'
    );
    v_definition:=replace(
      v_definition,
      'private._timesheet_settings_authority_frozen_v1',
      'public.timesheet_settings_authority_frozen_get_v1'
    );

    if position('private._contract_settings_effective_core_v1' in v_definition)>0
       or position('private._timesheet_settings_authority_frozen_v1' in v_definition)>0 then
      raise exception 'SETTINGS_SERVICE_VIEW_BRIDGE_REWRITE_INCOMPLETE:%',v_row.view_name
        using errcode='55000';
    end if;

    execute format(
      'create or replace view %I.%I with (security_invoker=true) as %s',
      v_row.schema_name,v_row.view_name,v_definition
    );
  end loop;
end
$settings_service_view_bridge$;

revoke all on public.v_timesheets_summary_base from public,anon,authenticated;
grant select on public.v_timesheets_summary_base to service_role;
revoke all on public.v_ts_invoice_precheck from public,anon,authenticated;
grant select on public.v_ts_invoice_precheck to service_role;

comment on function public.timesheet_settings_authority_frozen_get_v1(uuid) is
  'Service-only wrapper for the immutable settings authority already frozen on one Timesheet. It changes no settings, hours, pay, invoice or expense policy.';

commit;
