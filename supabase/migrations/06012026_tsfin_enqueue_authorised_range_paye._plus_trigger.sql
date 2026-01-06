create or replace function public.trg_tsfin_finance_windows_erni_wakeup()
returns trigger
language plpgsql
as $$
declare
  v_from date;
  v_to   date;
  v_end_of_year date;
begin
  -- Only care if ERNI-related values/range actually changed on UPDATE
  if tg_op = 'UPDATE' then
    if new.erni_pct      is not distinct from old.erni_pct
       and new.apply_erni_to is not distinct from old.apply_erni_to
       and new.date_from = old.date_from
       and new.date_to   is not distinct from old.date_to
    then
      return new;
    end if;
  end if;

  -- End of the current year in Europe/London
  v_end_of_year :=
    (
      date_trunc('year', (now() at time zone 'Europe/London'))::date
      + interval '1 year'
      - interval '1 day'
    )::date;

  -- Affected range start: window start
  v_from := new.date_from;

  -- Affected range end: window end clamped to end-of-year
  v_to := least(coalesce(new.date_to, v_end_of_year), v_end_of_year);

  -- If window starts after end-of-year, nothing to do
  if v_from is null or v_from > v_end_of_year then
    return new;
  end if;

  -- Enqueue PAYE authorised processed unlocked timesheets in range
  perform public.enqueue_tsfin_for_authorised_range(
    p_from   => v_from,
    p_to     => v_to,
    p_reason => 'CONTEXT_CHANGED'::public.ts_fin_reason_enum,
    p_limit  => 20000
  );

  return new;
end;
$$;


create or replace function public.trg_tsfin_finance_windows_erni_wakeup_all()
returns trigger
language plpgsql
as $$
declare
  v_now timestamptz := now();
begin
  -- Only run if ERNI-related fields actually changed (for UPDATE)
  if tg_op = 'UPDATE' then
    if new.erni_pct is not distinct from old.erni_pct
       and new.apply_erni_to is not distinct from old.apply_erni_to
       and new.date_from = old.date_from
       and new.date_to is not distinct from old.date_to
    then
      return new;
    end if;
  end if;

  with picked as (
    select distinct ts.timesheet_id
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    where ts.is_current = true
      and ts.revoked_at is null

      -- authorised OR processed
      and (ts.authorised_at_server is not null or tf.timesheet_id is not null)

      -- safety: skip locked/paid when a current TSFIN exists
      and (
        tf.timesheet_id is null
        or (tf.locked_by_invoice_id is null and tf.paid_at_utc is null)
      )
  )
  insert into public.ts_financials_outbox
    (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select
    p.timesheet_id,
    'CONTEXT_CHANGED'::public.ts_fin_reason_enum,
    0,
    null,
    null,
    v_now
  from picked p
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = excluded.created_at;

  return new;
end;
$$;


drop trigger if exists trg_tsfin_finance_windows_erni_wakeup_all_aiu
on public.settings_finance_windows;

create trigger trg_tsfin_finance_windows_erni_wakeup_all_aiu
after insert or update of erni_pct, apply_erni_to, date_from, date_to
on public.settings_finance_windows
for each row
execute function public.trg_tsfin_finance_windows_erni_wakeup_all();



