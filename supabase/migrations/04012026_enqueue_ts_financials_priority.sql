create or replace function public.enqueue_ts_financials_priority(
  _timesheet_ids uuid[],
  _reason ts_fin_reason_enum
)
returns integer
language plpgsql
as $function$
declare
  v_count int := 0;
  v_priority_ts timestamptz := now() - interval '100 years';
begin
  if _timesheet_ids is null or array_length(_timesheet_ids, 1) is null then
    return 0;
  end if;

  with ids as (
    select distinct t as timesheet_id
    from unnest(_timesheet_ids) as t
    where t is not null
  )
  insert into public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select timesheet_id, _reason, 0, null, null, v_priority_ts
  from ids
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = v_priority_ts;

  get diagnostics v_count = row_count;
  return v_count;
end;
$function$;
