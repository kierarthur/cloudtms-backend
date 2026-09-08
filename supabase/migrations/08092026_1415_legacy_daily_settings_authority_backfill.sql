\set ON_ERROR_STOP on

begin;

-- Historical Daily records created before frozen settings authority was added
-- can still be discovered by the invoice candidate classifier.  They are not
-- changed functionally here: this one-time repair only records the dated rules
-- which already applied to each past Daily record.  New Daily records continue
-- to freeze their authority through the normal processing trigger.
create or replace function private._legacy_daily_settings_authority_backfill_before_v1()
returns trigger
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_client_id uuid;
  v_relevant_date date;
  v_snapshot jsonb;
begin
  if new.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum
     or new.is_current is not true
     or new.revoked_at is not null
     or new.settings_authority_json <> '{}'::jsonb
     or new.week_ending_date >= date '2026-09-08' then
    return new;
  end if;

  select financial.client_id
  into v_client_id
  from public.timesheets_financials financial
  where financial.timesheet_id=new.timesheet_id
    and financial.is_current=true
    and financial.client_id is not null
  order by financial.updated_at desc,financial.id desc
  limit 1;

  if not found then
    return new;
  end if;

  v_relevant_date:=coalesce(
    (new.worked_start_iso at time zone 'Europe/London')::date,
    (new.scheduled_start_iso at time zone 'Europe/London')::date,
    new.week_ending_date
  );
  v_snapshot:=private._contract_settings_effective_core_v1(
    v_client_id,new.contract_id,v_relevant_date,'DAILY',null
  );

  new.settings_authority_json:=v_snapshot;
  new.settings_authority_version:='CONTRACT_SETTINGS_AUTHORITY_V1';
  new.settings_authority_fingerprint:=v_snapshot->>'authority_fingerprint';
  new.settings_authority_resolved_at:=(v_snapshot->>'resolved_at_utc')::timestamptz;
  return new;
end
$function$;

alter function private._legacy_daily_settings_authority_backfill_before_v1()
  owner to postgres;
revoke all on function private._legacy_daily_settings_authority_backfill_before_v1()
  from public,anon,authenticated,service_role;

drop trigger if exists zz_legacy_daily_settings_authority_backfill_before_v1
  on public.timesheets;
create trigger zz_legacy_daily_settings_authority_backfill_before_v1
before update on public.timesheets
for each row execute function private._legacy_daily_settings_authority_backfill_before_v1();

update public.timesheets timesheet
set updated_at=timesheet.updated_at
where timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum
  and timesheet.is_current=true
  and timesheet.revoked_at is null
  and timesheet.settings_authority_json='{}'::jsonb
  and timesheet.week_ending_date < date '2026-09-08'
  and exists(
    select 1
    from public.timesheets_financials financial
    where financial.timesheet_id=timesheet.timesheet_id
      and financial.is_current=true
      and financial.client_id is not null
  );

drop trigger zz_legacy_daily_settings_authority_backfill_before_v1
  on public.timesheets;
drop function private._legacy_daily_settings_authority_backfill_before_v1();

commit;
