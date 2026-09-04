-- One-time TEST compatibility repair for processed Daily Timesheets that
-- pre-date their Client's first effective-dated settings canvas.  Daily rows
-- can legitimately have no Contract, so the earlier earliest-Contract repair
-- cannot establish authority for them.  Backdate only the first settings row;
-- preserve every setting value and every later settings-history row.

\set ON_ERROR_STOP on

begin;

do $migration$
declare
  v_updated integer := 0;
begin
  with processed_daily_origin as (
    select
      tf.client_id,
      min(coalesce(
        (t.worked_start_iso at time zone 'Europe/London')::date,
        (t.scheduled_start_iso at time zone 'Europe/London')::date,
        t.week_ending_date
      )) as earliest_processed_daily_date
    from public.timesheets t
    join public.timesheets_financials tf
      on tf.timesheet_id=t.timesheet_id
     and tf.is_current=true
     and tf.processed_at_utc is not null
    where t.sheet_scope='DAILY'::public.timesheet_scope_enum
      and t.settings_authority_json='{}'::jsonb
      and tf.client_id is not null
    group by tf.client_id
  ), first_settings as (
    select ranked.id,ranked.client_id,ranked.effective_from
    from (
      select
        cs.id,
        cs.client_id,
        cs.effective_from,
        row_number() over(
          partition by cs.client_id
          order by cs.effective_from asc nulls first,cs.created_at asc,cs.id asc
        ) as row_no
      from public.client_settings cs
    ) ranked
    where ranked.row_no=1
  )
  update public.client_settings cs
  set effective_from=pdo.earliest_processed_daily_date
  from first_settings fs
  join processed_daily_origin pdo on pdo.client_id=fs.client_id
  where cs.id=fs.id
    and fs.effective_from is not null
    and pdo.earliest_processed_daily_date is not null
    and fs.effective_from>pdo.earliest_processed_daily_date;

  get diagnostics v_updated = row_count;

  if exists(
    select 1
    from public.timesheets t
    join public.timesheets_financials tf
      on tf.timesheet_id=t.timesheet_id
     and tf.is_current=true
     and tf.processed_at_utc is not null
    where t.sheet_scope='DAILY'::public.timesheet_scope_enum
      and t.settings_authority_json='{}'::jsonb
      and tf.client_id is not null
      and not exists(
        select 1
        from public.client_settings cs
        where cs.client_id=tf.client_id
          and (
            cs.effective_from is null
            or cs.effective_from<=coalesce(
              (t.worked_start_iso at time zone 'Europe/London')::date,
              (t.scheduled_start_iso at time zone 'Europe/London')::date,
              t.week_ending_date
            )
          )
      )
  ) then
    raise exception 'CLIENT_SETTINGS_PROCESSED_DAILY_ORIGIN_REPAIR_INCOMPLETE';
  end if;

  raise notice 'Backdated the first Client settings canvas for % historical processed Daily Client records.',v_updated;
end
$migration$;

commit;
