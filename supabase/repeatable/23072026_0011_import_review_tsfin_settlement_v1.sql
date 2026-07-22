-- Bounded durable settlement proof for import-review TSFIN follow-up.
-- This is read-only evidence: it never enqueues, leases, writes or authorises.

create or replace function public.tsfin_follow_up_target_summary_v1(
  p_timesheet_ids uuid[],
  p_not_before_utc timestamptz
)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare
  v_target_count integer := 0;
  v_current_target_count integer := 0;
  v_fresh_target_count integer := 0;
  v_pending_total integer := 0;
  v_pending_ready integer := 0;
  v_latest_computed_at timestamptz := null;
  v_next_attempt_at_min timestamptz := null;
  v_now timestamptz := now();
begin
  if p_not_before_utc is null then
    raise exception 'TSFIN_FOLLOW_UP_COMMIT_FENCE_REQUIRED' using errcode = '22023';
  end if;

  if cardinality(coalesce(p_timesheet_ids, array[]::uuid[])) > 5000 then
    raise exception 'TSFIN_FOLLOW_UP_TARGET_LIMIT_EXCEEDED' using errcode = '22023';
  end if;

  select count(distinct input_id)::integer
  into v_target_count
  from unnest(coalesce(p_timesheet_ids, array[]::uuid[])) as requested(input_id)
  where input_id is not null;

  if v_target_count > 5000 then
    raise exception 'TSFIN_FOLLOW_UP_TARGET_LIMIT_EXCEEDED' using errcode = '22023';
  end if;

  if v_target_count = 0 then
    return jsonb_build_object(
      'ok', true,
      'target_count', 0,
      'current_target_count', 0,
      'fresh_target_count', 0,
      'stale_or_missing_count', 0,
      'pending_total', 0,
      'pending_ready', 0,
      'next_attempt_at_min', null,
      'latest_computed_at', null,
      'not_before_utc', p_not_before_utc,
      'all_targets_fresh', true,
      'all_targets_settled', true
    );
  end if;

  with wanted as (
    select distinct input_id as timesheet_id
    from unnest(p_timesheet_ids) as requested(input_id)
    where input_id is not null
  ),
  target_state as (
    select
      w.timesheet_id,
      (
        ts.timesheet_id is not null
        and ts.is_current is true
        and ts.revoked_at is null
      ) as is_current_target,
      (
        ts.timesheet_id is not null
        and ts.is_current is true
        and ts.revoked_at is null
        and tf.timesheet_id is not null
        and tf.is_current is true
        and coalesce(tf.is_stale, false) is false
        and tf.timesheet_version is not distinct from ts.version
        and coalesce(tf.computed_at_utc, tf.updated_at, tf.created_at) >= p_not_before_utc
      ) as is_fresh_target,
      coalesce(tf.computed_at_utc, tf.updated_at, tf.created_at) as computed_at_utc
    from wanted w
    left join public.timesheets ts
      on ts.timesheet_id = w.timesheet_id
    left join lateral (
      select tf0.*
      from public.timesheets_financials tf0
      where tf0.timesheet_id = w.timesheet_id
        and tf0.is_current is true
      order by
        tf0.computed_at_utc desc nulls last,
        tf0.updated_at desc nulls last,
        tf0.created_at desc nulls last,
        tf0.id desc
      limit 1
    ) tf on true
  ),
  pending_state as (
    select o.*
    from public.ts_financials_outbox o
    join wanted w on w.timesheet_id = o.timesheet_id
  )
  select
    count(*) filter (where target_state.is_current_target)::integer,
    count(*) filter (where target_state.is_fresh_target)::integer,
    max(target_state.computed_at_utc),
    (select count(*)::integer from pending_state),
    (select count(*)::integer from pending_state where next_attempt_at is null or next_attempt_at <= v_now),
    (select min(next_attempt_at) from pending_state where next_attempt_at is not null and next_attempt_at > v_now)
  into
    v_current_target_count,
    v_fresh_target_count,
    v_latest_computed_at,
    v_pending_total,
    v_pending_ready,
    v_next_attempt_at_min
  from target_state;

  return jsonb_build_object(
    'ok', true,
    'target_count', v_target_count,
    'current_target_count', coalesce(v_current_target_count, 0),
    'fresh_target_count', coalesce(v_fresh_target_count, 0),
    'stale_or_missing_count', greatest(v_target_count - coalesce(v_fresh_target_count, 0), 0),
    'pending_total', coalesce(v_pending_total, 0),
    'pending_ready', coalesce(v_pending_ready, 0),
    'next_attempt_at_min', v_next_attempt_at_min,
    'latest_computed_at', v_latest_computed_at,
    'not_before_utc', p_not_before_utc,
    'all_targets_fresh', coalesce(v_fresh_target_count, 0) = v_target_count,
    'all_targets_settled',
      coalesce(v_fresh_target_count, 0) = v_target_count
      and coalesce(v_pending_total, 0) = 0
  );
end;
$function$;

alter function public.tsfin_follow_up_target_summary_v1(uuid[], timestamptz) owner to postgres;
revoke all on function public.tsfin_follow_up_target_summary_v1(uuid[], timestamptz) from public, anon, authenticated;
grant execute on function public.tsfin_follow_up_target_summary_v1(uuid[], timestamptz) to service_role;
