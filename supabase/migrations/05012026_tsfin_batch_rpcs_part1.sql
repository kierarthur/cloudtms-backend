-- 05012026_tsfin_batch_rpcs_part1.sql
-- Implements:
-- 2.3 tsfin_dequeue_specific
-- 2.4 tsfin_dequeue_batch_ids
-- 2.7 enqueue_tsfin_for_occ_key
-- 2.7 enqueue_tsfin_for_hospital_norm
--
-- Assumptions used here are NOT guesses:
-- - tables exist: ts_financials_outbox, timesheets, timesheets_financials
-- - columns exist (confirmed by your indexes output):
--   timesheets: timesheet_id, occupant_key_norm, hospital_norm, is_current, authorised_at_server, revoked_at
--   timesheets_financials: timesheet_id, is_current, locked_by_invoice_id, paid_at_utc
--   outbox: id, timesheet_id, reason, attempt_count, next_attempt_at, created_at
-- - unique constraint exists: uq_tsfin_outbox(timesheet_id, reason)

-- ============================================================
-- 2.3: Deterministic targeted dequeue: only lease specific IDs
-- ============================================================
create or replace function public.tsfin_dequeue_specific(
  p_timesheet_ids uuid[],
  p_limit integer default null
)
returns setof public.ts_financials_outbox
language plpgsql
as $function$
declare
  v_now timestamptz := now();
  v_lim integer;
begin
  if p_timesheet_ids is null or array_length(p_timesheet_ids, 1) is null then
    return;
  end if;

  v_lim := coalesce(p_limit, array_length(p_timesheet_ids, 1));

  return query
  with wanted as (
    select distinct unnest(p_timesheet_ids) as timesheet_id
  ),
  picked as (
    select o.id
    from public.ts_financials_outbox o
    join wanted w on w.timesheet_id = o.timesheet_id
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit v_lim
    for update skip locked
  )
  update public.ts_financials_outbox o
  set attempt_count   = o.attempt_count + 1,
      next_attempt_at = v_now + interval '5 minutes'
  where o.id in (select id from picked)
  returning o.*;
end;
$function$;

-- ============================================================
-- 2.4: Generic batch dequeue, but return stable "IDs list" shape
-- ============================================================
create or replace function public.tsfin_dequeue_batch_ids(
  p_limit integer default 50
)
returns table (
  outbox_id uuid,
  timesheet_id uuid,
  reason public.ts_fin_reason_enum,
  attempt_count integer,
  next_attempt_at timestamptz,
  created_at timestamptz
)
language plpgsql
as $function$
declare
  v_now timestamptz := now();
begin
  return query
  with picked as (
    select o.id
    from public.ts_financials_outbox o
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit p_limit
    for update skip locked
  ),
  leased as (
    update public.ts_financials_outbox o
    set attempt_count   = o.attempt_count + 1,
        next_attempt_at = v_now + interval '5 minutes'
    where o.id in (select id from picked)
    returning o.*
  )
  select
    l.id           as outbox_id,
    l.timesheet_id as timesheet_id,
    l.reason       as reason,
    l.attempt_count,
    l.next_attempt_at,
    l.created_at
  from leased l;
end;
$function$;

-- ============================================================
-- 2.7A: Bulk enqueue by occupant_key_norm (GCK) with lock/paid safety
-- ============================================================
create or replace function public.enqueue_tsfin_for_occ_key(
  p_occ_key_norm text,
  p_reason public.ts_fin_reason_enum default 'CONTEXT_CHANGED',
  p_priority boolean default true,
  p_limit integer default 500
)
returns integer
language plpgsql
as $function$
declare
  v_norm text := lower(btrim(coalesce(p_occ_key_norm, '')));
  v_now  timestamptz := now();
  v_cnt  integer := 0;
begin
  if v_norm = '' then
    return 0;
  end if;

  with picked as (
    select ts.timesheet_id
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    where ts.is_current = true
      and ts.revoked_at is null
      and ts.occupant_key_norm = v_norm
      -- safety: don't enqueue if current TSFIN is locked/paid
      and (
        tf.timesheet_id is null
        or (tf.locked_by_invoice_id is null and tf.paid_at_utc is null)
      )
    limit p_limit
  )
  select
    case
      when p_priority then public.enqueue_ts_financials_priority(array_agg(timesheet_id), p_reason)
      else null
    end
  into v_cnt
  from picked;

  if p_priority then
    return coalesce(v_cnt, 0);
  end if;

  insert into public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select p.timesheet_id, p_reason, 0, null, null, v_now
  from picked p
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = excluded.created_at;

  get diagnostics v_cnt = row_count;
  return v_cnt;
end;
$function$;


-- ============================================================
-- 2.7B: Bulk enqueue by hospital_norm alias with lock/paid safety
-- ============================================================
create or replace function public.enqueue_tsfin_for_hospital_norm(
  p_hospital_norm text,
  p_reason public.ts_fin_reason_enum default 'CONTEXT_CHANGED',
  p_priority boolean default true,
  p_limit integer default 500
)
returns integer
language plpgsql
as $function$
declare
  v_norm text := lower(btrim(coalesce(p_hospital_norm, '')));
  v_now  timestamptz := now();
  v_cnt  integer := 0;
begin
  if v_norm = '' then
    return 0;
  end if;

  with picked as (
    select ts.timesheet_id
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    where ts.is_current = true
      and ts.revoked_at is null
      and ts.authorised_at_server is not null
      and ts.hospital_norm = v_norm
      -- safety: don't enqueue if current TSFIN is locked/paid
      and (
        tf.timesheet_id is null
        or (tf.locked_by_invoice_id is null and tf.paid_at_utc is null)
      )
    limit p_limit
  )
  select
    case
      when p_priority then public.enqueue_ts_financials_priority(array_agg(timesheet_id), p_reason)
      else null
    end
  into v_cnt
  from picked;

  if p_priority then
    return coalesce(v_cnt, 0);
  end if;

  insert into public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select p.timesheet_id, p_reason, 0, null, null, v_now
  from picked p
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = excluded.created_at;

  get diagnostics v_cnt = row_count;
  return v_cnt;
end;
$function$;
