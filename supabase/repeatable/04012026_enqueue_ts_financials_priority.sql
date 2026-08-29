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

  if cardinality(_timesheet_ids) > 5000 then
    raise exception 'TSFIN_PRIORITY_ENQUEUE_TARGET_LIMIT_EXCEEDED'
      using errcode = '22023';
  end if;

  with requested as (
    select distinct t as timesheet_id
    from unnest(_timesheet_ids) as t
    where t is not null
  ), expanded as (
    select r.timesheet_id
    from requested r

    union

    -- A TSFIN write for an import-authoritative correction is atomic at the
    -- correction-unit boundary. If either reversal/replacement member is
    -- requested, enqueue every current member of that same unit. This does
    -- not alter either leg's economics; it completes the guarded batch.
    select partner.timesheet_id
    from requested r
    join public.timesheets seed
      on seed.timesheet_id = r.timesheet_id
     and seed.is_current = true
     and seed.correction_id is not null
     and upper(btrim(coalesce(seed.adjustment_origin, ''))) in (
       'IMPORT_CORRECTION', 'IMPORT_CANCELLATION',
       'HEALTHROSTER_CHANGED_HOURS', 'NHSP_CHANGED_HOURS',
       'HEALTHROSTER_CANCELLATION', 'NHSP_CANCELLATION'
     )
    join public.timesheets partner
      on partner.correction_id = seed.correction_id
     and partner.is_current = true
     and upper(btrim(coalesce(partner.adjustment_origin, ''))) in (
       'IMPORT_CORRECTION', 'IMPORT_CANCELLATION',
       'HEALTHROSTER_CHANGED_HOURS', 'NHSP_CHANGED_HOURS',
       'HEALTHROSTER_CANCELLATION', 'NHSP_CANCELLATION'
     )
  ), ids as (
    select distinct timesheet_id from expanded
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
