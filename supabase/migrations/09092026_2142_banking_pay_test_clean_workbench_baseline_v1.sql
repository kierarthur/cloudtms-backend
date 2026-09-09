-- One-time TEST data cleanup for the pre-acceptance Banking Pay Workbench state.
--
-- This migration does not make old Workbench work compatible with current
-- code. It discards the one observed unfinished TEST session, retires the
-- three obsolete source builds still owned by its discarded predecessor, and
-- terminalises the 25 unfinished jobs belonging to that session. Historical
-- rows and audit evidence are retained. No Draft, payment, provider,
-- settlement, remittance, eligibility, selection or economic policy is
-- changed.

\set ON_ERROR_STOP on

begin;

set local statement_timeout = '6000ms';
set local lock_timeout = '1000ms';

do $required_authority$
begin
  if pg_catalog.to_regclass('private.cloudtms_database_identity') is null
     or pg_catalog.to_regclass(
       'private.banking_pay_workbench_candidate_scope_registry'
     ) is null
     or pg_catalog.to_regclass(
       'private.banking_pay_workbench_economic_builds'
     ) is null
     or pg_catalog.to_regclass(
       'private.banking_pay_workbench_stage_attempts'
     ) is null
     or pg_catalog.to_regclass(
       'public.banking_pay_workbench_sessions'
     ) is null
     or pg_catalog.to_regclass(
       'public.banking_pay_workbench_session_scope'
     ) is null
     or pg_catalog.to_regclass(
       'public.banking_pay_workbench_session_candidate_state'
     ) is null
     or pg_catalog.to_regclass('public.banking_pay_workbench_jobs') is null
     or pg_catalog.to_regclass('public.banking_pay_operations') is null
     or pg_catalog.to_regclass('public.candidates') is null
     or pg_catalog.to_regprocedure(
       'public.pay_workbench_session_discard(uuid,uuid)'
     ) is null
     or pg_catalog.to_regprocedure(
       'public._pay_workbench_candidate_serial_key(uuid)'
     ) is null then
    raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_REQUIRED_AUTHORITY_MISSING';
  end if;
end
$required_authority$;

create temporary table _banking_pay_test_clean_workbench_targets (
  candidate_id uuid primary key,
  stale_build_id uuid not null unique,
  stale_session_id uuid not null,
  current_session_id uuid not null,
  current_job_id uuid not null unique,
  actor_user_id uuid not null
) on commit drop;

insert into _banking_pay_test_clean_workbench_targets (
  candidate_id,
  stale_build_id,
  stale_session_id,
  current_session_id,
  current_job_id,
  actor_user_id
)
select
  registry.candidate_id,
  stale_build.id,
  stale_session.id,
  current_session.id,
  current_job.id,
  current_session.actor_user_id
from private.banking_pay_workbench_candidate_scope_registry as registry
join private.banking_pay_workbench_economic_builds as stale_build
  on stale_build.id = registry.current_build_id
join public.banking_pay_workbench_sessions as stale_session
  on stale_session.id = stale_build.session_id
join public.banking_pay_workbench_sessions as current_session
  on current_session.id = stale_session.replacement_session_id
join public.banking_pay_workbench_session_scope as current_scope
  on current_scope.session_id = current_session.id
 and current_scope.candidate_id = registry.candidate_id
join public.banking_pay_workbench_session_candidate_state as current_state
  on current_state.session_id = current_session.id
 and current_state.candidate_id = registry.candidate_id
join public.banking_pay_workbench_jobs as current_job
  on current_job.id = current_scope.pending_job_id
where stale_build.status in (
    'COLLECTING',
    'READY_FOR_RECONCILIATION',
    'RECONCILING',
    'RECONCILED',
    'PUBLISHING',
    'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
  )
  and stale_session.status = 'DISCARDED'
  and stale_session.discarded_at_utc is not null
  and current_session.status = 'OPEN'
  and current_session.discarded_at_utc is null
  and current_session.replacement_session_id is null
  and current_scope.status = 'SOURCE_BUILD_PENDING'
  and current_scope.dirty is true
  and current_scope.error_json is null
  and current_state.status = 'PENDING'
  and current_state.pending_job_id = current_job.id
  and current_state.session_version = current_session.version
  and current_job.session_id = current_session.id
  and current_job.candidate_id = registry.candidate_id
  and current_job.job_type = 'WORKBENCH_CANDIDATE_SOURCE_BUILD'
  and current_job.status = 'QUEUED'
  and current_job.economic_build_id is null
  and current_job.private_stage = 'BUILD_INITIALISE'
  and current_job.private_cursor_kind = 'BUILD_INITIALISE'
  and current_job.private_stage_version = 1
  and current_job.attempt_count = 0
  and current_job.payload_json->>'created_by_helper' =
    'pay_workbench_enqueue_candidate_refresh'
  and current_job.payload_json->>'session_version' =
    current_session.version::text
  and current_job.payload_json->>'source_change_seq' =
    registry.current_source_change_seq::text
  and current_job.payload_json->>'authority_fingerprint_version' = '3'
  and coalesce(
    current_job.payload_json->>'authority_fingerprint',
    ''
  ) ~ '^[0-9a-f]{64}$'
  and coalesce(
    current_job.payload_json->>'source_build_run_id',
    ''
  ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  and not exists (
    select 1
    from public.banking_pay_workbench_jobs as active_stale_job
    where active_stale_job.economic_build_id = stale_build.id
      and active_stale_job.status in ('QUEUED', 'RUNNING')
  )
  and not exists (
    select 1
    from private.banking_pay_workbench_stage_attempts as active_attempt
    where active_attempt.build_id = stale_build.id
      and active_attempt.attempt_status = 'STARTED'
  )
  and exists (
    select 1
    from public.banking_pay_workbench_jobs as terminal_continuation
    where terminal_continuation.economic_build_id = stale_build.id
      and terminal_continuation.candidate_id = registry.candidate_id
      and terminal_continuation.session_id = stale_session.id
      and terminal_continuation.job_type =
        'WORKBENCH_CANDIDATE_SOURCE_BUILD'
      and terminal_continuation.status = 'DEAD'
      and terminal_continuation.last_error_json->>'code' =
        'REPLACED_SESSION_QUEUE_REPLAYED'
      and terminal_continuation.payload_json->>'replacement_session_id' =
        current_session.id::text
  );

do $test_clean_workbench_baseline$
declare
  v_environment text;
  v_open_session_count integer := 0;
  v_target_session_count integer := 0;
  v_target_build_count integer := 0;
  v_target_active_job_count integer := 0;
  v_current_session_id uuid;
  v_actor_user_id uuid;
  v_target record;
  v_discard jsonb;
  v_changed integer := 0;
  v_retired_build_count integer := 0;
  v_retired_attempt_count integer := 0;
  v_retired_job_count integer := 0;
begin
  select identity_row.environment
  into strict v_environment
  from private.cloudtms_database_identity as identity_row
  where identity_row.singleton;

  if v_environment is distinct from 'TEST' then
    return;
  end if;

  select pg_catalog.count(*)::integer
  into v_open_session_count
  from public.banking_pay_workbench_sessions as session_row
  where session_row.status = 'OPEN'
    and session_row.discarded_at_utc is null;

  select
    pg_catalog.count(distinct target.current_session_id)::integer,
    pg_catalog.count(*)::integer
  into v_target_session_count, v_target_build_count
  from _banking_pay_test_clean_workbench_targets as target;

  select pg_catalog.count(*)::integer
  into v_target_active_job_count
  from public.banking_pay_workbench_jobs as current_job
  where current_job.status in ('QUEUED', 'RUNNING')
    and exists (
      select 1
      from _banking_pay_test_clean_workbench_targets as target
      where target.current_session_id = current_job.session_id
    );

  -- A clean NEW database and an already-cleaned retry are intentional no-ops.
  if v_open_session_count = 0
     and v_target_session_count = 0
     and v_target_build_count = 0
     and v_target_active_job_count = 0 then
    return;
  end if;

  if v_open_session_count is distinct from 1
     or v_target_session_count is distinct from 1
     or v_target_build_count is distinct from 3
     or v_target_active_job_count is distinct from 25 then
    raise exception using
      errcode = 'P0001',
      message = 'BANKING_PAY_TEST_CLEAN_BASELINE_SCOPE_MISMATCH',
      detail = pg_catalog.format(
        'open_sessions=%s target_sessions=%s stale_builds=%s active_jobs=%s',
        v_open_session_count,
        v_target_session_count,
        v_target_build_count,
        v_target_active_job_count
      );
  end if;

  select target.current_session_id, target.actor_user_id
  into strict v_current_session_id, v_actor_user_id
  from _banking_pay_test_clean_workbench_targets as target
  order by target.current_session_id, target.actor_user_id
  limit 1;

  if exists (
    select 1
    from public.banking_pay_operations as operation_row
    where operation_row.workbench_session_id = v_current_session_id
  ) then
    raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_DRAFT_OPERATION_FOUND';
  end if;

  perform 1
  from public.banking_pay_workbench_sessions as current_session
  where current_session.id = v_current_session_id
    and current_session.status = 'OPEN'
    and current_session.discarded_at_utc is null
  for update;
  if not found then
    raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_SESSION_CHANGED';
  end if;

  perform 1
  from public.banking_pay_workbench_jobs as current_job
  where current_job.session_id = v_current_session_id
    and current_job.status in ('QUEUED', 'RUNNING')
  order by current_job.id
  for update;

  select pg_catalog.count(*)::integer
  into v_target_active_job_count
  from public.banking_pay_workbench_jobs as current_job
  where current_job.session_id = v_current_session_id
    and current_job.status in ('QUEUED', 'RUNNING');

  if v_target_active_job_count is distinct from 25 then
    raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_JOB_SET_CHANGED';
  end if;

  v_discard := public.pay_workbench_session_discard(
    v_current_session_id,
    v_actor_user_id
  );

  if coalesce((v_discard->>'ok')::boolean, false) is not true
     or v_discard->>'status' is distinct from 'DISCARDED' then
    raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_SESSION_NOT_DISCARDED';
  end if;

  for v_target in
    select target.*
    from _banking_pay_test_clean_workbench_targets as target
    order by target.candidate_id
  loop
    if not pg_catalog.pg_try_advisory_xact_lock(
      pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_target.candidate_id),
        24062027
      )
    ) then
      raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_CANDIDATE_BUSY';
    end if;

    perform 1
    from public.candidates as candidate_row
    where candidate_row.id = v_target.candidate_id
    for update;
    if not found then
      raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_CANDIDATE_MISSING';
    end if;

    perform 1
    from private.banking_pay_workbench_candidate_scope_registry as registry
    where registry.candidate_id = v_target.candidate_id
      and registry.current_build_id = v_target.stale_build_id
    for update;
    if not found then
      raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_REGISTRY_CHANGED';
    end if;

    perform 1
    from public.banking_pay_workbench_session_scope as current_scope
    where current_scope.session_id = v_target.current_session_id
      and current_scope.candidate_id = v_target.candidate_id
      and current_scope.pending_job_id = v_target.current_job_id
      and current_scope.status = 'SOURCE_BUILD_PENDING'
    for update;
    if not found then
      raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_SCOPE_CHANGED';
    end if;

    perform 1
    from private.banking_pay_workbench_economic_builds as stale_build
    where stale_build.id = v_target.stale_build_id
      and stale_build.candidate_id = v_target.candidate_id
      and stale_build.session_id = v_target.stale_session_id
      and stale_build.status in (
        'COLLECTING',
        'READY_FOR_RECONCILIATION',
        'RECONCILING',
        'RECONCILED',
        'PUBLISHING',
        'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
      )
    for update;
    if not found then
      raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_BUILD_CHANGED';
    end if;

    perform 1
    from public.banking_pay_workbench_jobs as build_job
    where build_job.economic_build_id = v_target.stale_build_id
       or build_job.id = v_target.current_job_id
    order by build_job.id
    for update;

    perform 1
    from private.banking_pay_workbench_stage_attempts as build_attempt
    where build_attempt.build_id = v_target.stale_build_id
    order by build_attempt.id
    for update;

    if exists (
      select 1
      from public.banking_pay_workbench_jobs as active_stale_job
      where active_stale_job.economic_build_id = v_target.stale_build_id
        and active_stale_job.status in ('QUEUED', 'RUNNING')
    ) or exists (
      select 1
      from private.banking_pay_workbench_stage_attempts as active_attempt
      where active_attempt.build_id = v_target.stale_build_id
        and active_attempt.attempt_status = 'STARTED'
    ) then
      raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_ACTIVE_WORK_FOUND';
    end if;

    update private.banking_pay_workbench_economic_builds as stale_build
    set status = 'OBSOLETE',
        obsolete_at_utc = pg_catalog.clock_timestamp(),
        failure_json = pg_catalog.jsonb_build_object(
          'code', 'TEST_LEGACY_REPLACED_SESSION_BUILD_RETIRED',
          'message',
          'Obsolete TEST Workbench build was retired before clean acceptance.',
          'replacement_session_id', v_target.current_session_id::text,
          'payment_policy_changed', false,
          'financial_rows_changed', false
        ),
        updated_at_utc = pg_catalog.clock_timestamp()
    where stale_build.id = v_target.stale_build_id;
    get diagnostics v_changed = row_count;
    if v_changed is distinct from 1 then
      raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_BUILD_NOT_RETIRED';
    end if;
    v_retired_build_count := v_retired_build_count + v_changed;

    update private.banking_pay_workbench_candidate_scope_registry as registry
    set current_build_id = null,
        updated_at_utc = pg_catalog.clock_timestamp()
    where registry.candidate_id = v_target.candidate_id
      and registry.current_build_id = v_target.stale_build_id;
    get diagnostics v_changed = row_count;
    if v_changed is distinct from 1 then
      raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_REGISTRY_NOT_CLEARED';
    end if;

    perform public._audit_insert(
      'banking_pay_workbench_session_scope',
      v_target.candidate_id::text,
      'TEST_LEGACY_REPLACED_SESSION_BUILD_RETIRED',
      pg_catalog.jsonb_build_object(
        'discarded_session_id', v_target.stale_session_id::text,
        'stale_build_id', v_target.stale_build_id::text
      ),
      pg_catalog.jsonb_build_object(
        'current_session_id', v_target.current_session_id::text,
        'current_job_id', v_target.current_job_id::text,
        'stale_build_retired', true,
        'current_job_retired', true,
        'payment_policy_changed', false,
        'financial_rows_changed', false
      ),
      'TEST_CLEAN_BASELINE_RETIRE_LEGACY_REPLACED_SESSION_BUILD',
      v_target.actor_user_id
    );
  end loop;

  update private.banking_pay_workbench_stage_attempts as current_attempt
  set attempt_status = 'OBSOLETE',
      obsolete_at_utc = pg_catalog.clock_timestamp(),
      result_code = 'TEST_CLEAN_BASELINE_SESSION_DISCARDED',
      error_class = 'TEST_CLEAN_BASELINE_SESSION_DISCARDED',
      error_json = pg_catalog.jsonb_build_object(
        'code', 'TEST_CLEAN_BASELINE_SESSION_DISCARDED',
        'payment_policy_changed', false,
        'financial_rows_changed', false
      ),
      updated_at_utc = pg_catalog.clock_timestamp()
  from public.banking_pay_workbench_jobs as current_job
  where current_job.id = current_attempt.job_id
    and current_job.session_id = v_current_session_id
    and current_attempt.attempt_status = 'STARTED';
  get diagnostics v_retired_attempt_count = row_count;

  if v_retired_attempt_count is distinct from 0 then
    raise exception using
      errcode = 'P0001',
      message = 'BANKING_PAY_TEST_CLEAN_BASELINE_ACTIVE_ATTEMPT_FOUND',
      detail = pg_catalog.format(
        'started_attempts=%s',
        v_retired_attempt_count
      );
  end if;

  update public.banking_pay_workbench_jobs as current_job
  set status = 'DEAD',
      failed_at_utc = coalesce(
        current_job.failed_at_utc,
        pg_catalog.clock_timestamp()
      ),
      completed_at_utc = null,
      private_stage = case
        when current_job.economic_build_id is null then null
        else current_job.private_stage
      end,
      private_cursor_kind = case
        when current_job.economic_build_id is null then null
        else current_job.private_cursor_kind
      end,
      private_cursor_json = case
        when current_job.economic_build_id is null then '{}'::jsonb
        else current_job.private_cursor_json
      end,
      private_stage_version = case
        when current_job.economic_build_id is null then null
        else current_job.private_stage_version
      end,
      last_error_json = pg_catalog.jsonb_build_object(
        'code', 'TEST_CLEAN_BASELINE_SESSION_DISCARDED',
        'message',
        'Pre-acceptance TEST Workbench work was retired before a fresh session.',
        'payment_policy_changed', false,
        'financial_rows_changed', false
      ),
      updated_at_utc = pg_catalog.clock_timestamp()
  where current_job.session_id = v_current_session_id
    and current_job.status in ('QUEUED', 'RUNNING');
  get diagnostics v_retired_job_count = row_count;

  if v_retired_build_count is distinct from 3
     or v_retired_job_count is distinct from 25 then
    raise exception using
      errcode = 'P0001',
      message = 'BANKING_PAY_TEST_CLEAN_BASELINE_WRITE_COUNT_MISMATCH',
      detail = pg_catalog.format(
        'retired_builds=%s retired_jobs=%s',
        v_retired_build_count,
        v_retired_job_count
      );
  end if;

  if exists (
    select 1
    from public.banking_pay_workbench_sessions as remaining_session
    where remaining_session.id = v_current_session_id
      and (
        remaining_session.status is distinct from 'DISCARDED'
        or remaining_session.discarded_at_utc is null
      )
  ) or exists (
    select 1
    from public.banking_pay_workbench_jobs as remaining_job
    where remaining_job.session_id = v_current_session_id
      and remaining_job.status in ('QUEUED', 'RUNNING')
  ) or exists (
    select 1
    from private.banking_pay_workbench_candidate_scope_registry as registry
    join private.banking_pay_workbench_economic_builds as remaining_build
      on remaining_build.id = registry.current_build_id
    join public.banking_pay_workbench_sessions as owner_session
      on owner_session.id = remaining_build.session_id
    where owner_session.status = 'DISCARDED'
      and remaining_build.status in (
        'COLLECTING',
        'READY_FOR_RECONCILIATION',
        'RECONCILING',
        'RECONCILED',
        'PUBLISHING',
        'BLOCKED_UNVALIDATED_RECONCILIATION_SCALE'
      )
  ) then
    raise exception 'BANKING_PAY_TEST_CLEAN_BASELINE_POSTCONDITION_FAILED';
  end if;
end
$test_clean_workbench_baseline$;

commit;
