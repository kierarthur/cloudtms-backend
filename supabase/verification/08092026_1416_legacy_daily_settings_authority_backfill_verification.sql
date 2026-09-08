\set ON_ERROR_STOP on

do $verification$
begin
  if exists(
    select 1
    from public.timesheets timesheet
    join public.timesheets_financials financial
      on financial.timesheet_id=timesheet.timesheet_id
     and financial.is_current=true
     and financial.client_id is not null
    where timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum
      and timesheet.is_current=true
      and timesheet.revoked_at is null
      and timesheet.settings_authority_json='{}'::jsonb
      and timesheet.week_ending_date < date '2026-09-08'
  ) then
    raise exception 'LEGACY_DAILY_SETTINGS_AUTHORITY_BACKFILL_INCOMPLETE';
  end if;

  if to_regprocedure(
    'private._legacy_daily_settings_authority_backfill_before_v1()'
  ) is not null then
    raise exception 'LEGACY_DAILY_SETTINGS_AUTHORITY_BACKFILL_HELPER_RETAINED';
  end if;

  if exists(
    select 1
    from pg_catalog.pg_trigger trigger_row
    join pg_catalog.pg_class relation
      on relation.oid=trigger_row.tgrelid
    join pg_catalog.pg_namespace namespace
      on namespace.oid=relation.relnamespace
    where namespace.nspname='public'
      and relation.relname='timesheets'
      and trigger_row.tgname='zz_legacy_daily_settings_authority_backfill_before_v1'
      and not trigger_row.tgisinternal
  ) then
    raise exception 'LEGACY_DAILY_SETTINGS_AUTHORITY_BACKFILL_TRIGGER_RETAINED';
  end if;
end
$verification$;
