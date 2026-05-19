-- ============================================================
-- NEW - Banking Pay  operation RPCs 1-5
-- CloudTMS scalable Banking Pay operations foundation
--
-- Functions included only:
--   1. public.banking_pay_operation_config_get
--   2. public.banking_pay_operation_start
--   3. public.banking_pay_operation_get
--   4. public.banking_pay_operation_claim_next
--   5. public.banking_pay_operation_save_progress
--
-- Checked against current uploaded schema plus confirmed applied DB-1 to DB-9 migration.
-- No unsafe MIN/MAX aggregate/scalar usage is used.
-- ============================================================

-- ============================================================
-- 1. banking_pay_operation_config_get
-- ============================================================

create or replace function public.banking_pay_operation_config_get(
    p_operation_type text,
    p_phase text,
    p_chunk_type text
)
returns table (
    chunk_size integer,
    min_chunk_size integer,
    max_chunk_size integer,
    max_advance_ms integer,
    lock_seconds integer
)
language plpgsql
security definer
stable
set search_path = public, pg_temp
as $$
declare
    v_operation_type text;
    v_phase text;
    v_chunk_type text;
    v_default_chunk_size integer;
    v_default_min_chunk_size integer;
    v_default_max_chunk_size integer;
    v_default_max_advance_ms integer;
    v_default_lock_seconds integer;
begin
    v_operation_type := coalesce(nullif(btrim(p_operation_type), ''), 'ALL');
    v_phase := coalesce(nullif(btrim(p_phase), ''), 'ALL');
    v_chunk_type := coalesce(nullif(btrim(p_chunk_type), ''), 'CANDIDATE_SCOPE');

    if v_chunk_type in ('TRANSFER_SUBMIT', 'PAYEE_READINESS', 'FRESHNESS_VALIDATE') then
        v_default_chunk_size := 50;
        v_default_min_chunk_size := 1;
        v_default_max_chunk_size := 250;
    else
        v_default_chunk_size := 100;
        v_default_min_chunk_size := 1;
        v_default_max_chunk_size := 500;
    end if;

    v_default_max_advance_ms := 15000;
    v_default_lock_seconds := 60;

    return query
    with candidate_config as (
        select
            config_exact.default_chunk_size,
            config_exact.min_chunk_size,
            config_exact.max_chunk_size,
            config_exact.max_advance_ms,
            config_exact.lock_seconds,
            1 as priority_order
        from public.banking_pay_operation_config as config_exact
        where config_exact.enabled is true
          and config_exact.operation_type = v_operation_type
          and config_exact.phase = v_phase
          and config_exact.chunk_type = v_chunk_type

        union all

        select
            config_operation_default.default_chunk_size,
            config_operation_default.min_chunk_size,
            config_operation_default.max_chunk_size,
            config_operation_default.max_advance_ms,
            config_operation_default.lock_seconds,
            2 as priority_order
        from public.banking_pay_operation_config as config_operation_default
        where config_operation_default.enabled is true
          and config_operation_default.operation_type = v_operation_type
          and config_operation_default.phase = 'ALL'
          and config_operation_default.chunk_type = v_chunk_type

        union all

        select
            config_global_default.default_chunk_size,
            config_global_default.min_chunk_size,
            config_global_default.max_chunk_size,
            config_global_default.max_advance_ms,
            config_global_default.lock_seconds,
            3 as priority_order
        from public.banking_pay_operation_config as config_global_default
        where config_global_default.enabled is true
          and config_global_default.operation_type = 'ALL'
          and config_global_default.phase = 'ALL'
          and config_global_default.chunk_type = v_chunk_type
    )
    select
        selected_config.default_chunk_size,
        selected_config.min_chunk_size,
        selected_config.max_chunk_size,
        selected_config.max_advance_ms,
        selected_config.lock_seconds
    from candidate_config as selected_config
    order by selected_config.priority_order asc
    limit 1;

    if found then
        return;
    end if;

    return query
    select
        v_default_chunk_size,
        v_default_min_chunk_size,
        v_default_max_chunk_size,
        v_default_max_advance_ms,
        v_default_lock_seconds;
end;
$$;


-- ============================================================
-- 3. banking_pay_operation_get
-- ============================================================

create or replace function public.banking_pay_operation_get(
    p_operation_id uuid,
    p_actor_user_id uuid default null
)
returns table (
    operation_id uuid,
    operation_type text,
    status text,
    phase text,
    actor_user_id uuid,
    workbench_session_id uuid,
    pay_batch_id uuid,
    root_operation_id uuid,
    idempotency_key text,
    input_json jsonb,
    config_json jsonb,
    progress_json jsonb,
    result_json jsonb,
    error_json jsonb,
    total_units integer,
    completed_units integer,
    failed_units integer,
    current_chunk_index integer,
    chunk_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    updated_at_utc timestamptz,
    completed_at_utc timestamptz,
    failed_at_utc timestamptz,
    terminal boolean
)
language plpgsql
security definer
stable
set search_path = public, pg_temp
as $$
begin
    return query
    select
        operation_row.id,
        operation_row.operation_type,
        operation_row.status,
        operation_row.phase,
        operation_row.actor_user_id,
        operation_row.workbench_session_id,
        operation_row.pay_batch_id,
        operation_row.root_operation_id,
        operation_row.idempotency_key,
        operation_row.input_json,
        operation_row.config_json,
        operation_row.progress_json,
        operation_row.result_json,
        operation_row.error_json,
        operation_row.total_units,
        operation_row.completed_units,
        operation_row.failed_units,
        operation_row.current_chunk_index,
        operation_row.chunk_count,
        operation_row.locked_by,
        operation_row.lock_expires_at_utc,
        operation_row.created_at_utc,
        operation_row.started_at_utc,
        operation_row.updated_at_utc,
        operation_row.completed_at_utc,
        operation_row.failed_at_utc,
        operation_row.status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED')
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
      and (
          p_actor_user_id is null
          or operation_row.actor_user_id is null
          or operation_row.actor_user_id = p_actor_user_id
      );
end;
$$;


-- ============================================================
-- 4. banking_pay_operation_claim_next
-- ============================================================

create or replace function public.banking_pay_operation_claim_next(
    p_operation_id uuid,
    p_actor_user_id uuid,
    p_lock_owner text,
    p_lock_seconds integer default 60
)
returns table (
    claimed boolean,
    not_claimed_reason text,
    operation_id uuid,
    operation_type text,
    status text,
    phase text,
    actor_user_id uuid,
    workbench_session_id uuid,
    pay_batch_id uuid,
    root_operation_id uuid,
    idempotency_key text,
    input_json jsonb,
    config_json jsonb,
    progress_json jsonb,
    result_json jsonb,
    error_json jsonb,
    total_units integer,
    completed_units integer,
    failed_units integer,
    current_chunk_index integer,
    chunk_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    updated_at_utc timestamptz,
    completed_at_utc timestamptz,
    failed_at_utc timestamptz
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_operation public.banking_pay_operations%rowtype;
    v_visible_operation public.banking_pay_operations%rowtype;
    v_lock_owner text;
    v_lock_seconds integer;
begin
    v_lock_owner := coalesce(nullif(btrim(p_lock_owner), ''), 'unknown');
    v_lock_seconds := coalesce(p_lock_seconds, 60);

    if v_lock_seconds < 5 then
        v_lock_seconds := 5;
    elsif v_lock_seconds > 3600 then
        v_lock_seconds := 3600;
    end if;

    select operation_row.*
    into v_operation
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
      and (
          p_actor_user_id is null
          or operation_row.actor_user_id is null
          or operation_row.actor_user_id = p_actor_user_id
      )
    for update skip locked;

    if not found then
        select visible_operation_row.*
        into v_visible_operation
        from public.banking_pay_operations as visible_operation_row
        where visible_operation_row.id = p_operation_id
          and (
              p_actor_user_id is null
              or visible_operation_row.actor_user_id is null
              or visible_operation_row.actor_user_id = p_actor_user_id
          );

        if found then
            return query
            select
                false,
                'ROW_LOCKED_OR_UNAVAILABLE'::text,
                v_visible_operation.id,
                v_visible_operation.operation_type,
                v_visible_operation.status,
                v_visible_operation.phase,
                v_visible_operation.actor_user_id,
                v_visible_operation.workbench_session_id,
                v_visible_operation.pay_batch_id,
                v_visible_operation.root_operation_id,
                v_visible_operation.idempotency_key,
                v_visible_operation.input_json,
                v_visible_operation.config_json,
                v_visible_operation.progress_json,
                v_visible_operation.result_json,
                v_visible_operation.error_json,
                v_visible_operation.total_units,
                v_visible_operation.completed_units,
                v_visible_operation.failed_units,
                v_visible_operation.current_chunk_index,
                v_visible_operation.chunk_count,
                v_visible_operation.locked_by,
                v_visible_operation.lock_expires_at_utc,
                v_visible_operation.created_at_utc,
                v_visible_operation.started_at_utc,
                v_visible_operation.updated_at_utc,
                v_visible_operation.completed_at_utc,
                v_visible_operation.failed_at_utc;
            return;
        end if;

        return query
        select
            false,
            'NOT_FOUND_OR_NOT_AUTHORISED'::text,
            p_operation_id,
            null::text,
            null::text,
            null::text,
            null::uuid,
            null::uuid,
            null::uuid,
            null::uuid,
            null::text,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::integer,
            null::integer,
            null::integer,
            null::integer,
            null::integer,
            null::text,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz;
        return;
    end if;

    if v_operation.status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        return query
        select
            false,
            'TERMINAL'::text,
            v_operation.id,
            v_operation.operation_type,
            v_operation.status,
            v_operation.phase,
            v_operation.actor_user_id,
            v_operation.workbench_session_id,
            v_operation.pay_batch_id,
            v_operation.root_operation_id,
            v_operation.idempotency_key,
            v_operation.input_json,
            v_operation.config_json,
            v_operation.progress_json,
            v_operation.result_json,
            v_operation.error_json,
            v_operation.total_units,
            v_operation.completed_units,
            v_operation.failed_units,
            v_operation.current_chunk_index,
            v_operation.chunk_count,
            v_operation.locked_by,
            v_operation.lock_expires_at_utc,
            v_operation.created_at_utc,
            v_operation.started_at_utc,
            v_operation.updated_at_utc,
            v_operation.completed_at_utc,
            v_operation.failed_at_utc;
        return;
    end if;

    if v_operation.status not in ('QUEUED', 'RUNNING', 'WAITING') then
        return query
        select
            false,
            'NOT_CLAIMABLE_STATUS'::text,
            v_operation.id,
            v_operation.operation_type,
            v_operation.status,
            v_operation.phase,
            v_operation.actor_user_id,
            v_operation.workbench_session_id,
            v_operation.pay_batch_id,
            v_operation.root_operation_id,
            v_operation.idempotency_key,
            v_operation.input_json,
            v_operation.config_json,
            v_operation.progress_json,
            v_operation.result_json,
            v_operation.error_json,
            v_operation.total_units,
            v_operation.completed_units,
            v_operation.failed_units,
            v_operation.current_chunk_index,
            v_operation.chunk_count,
            v_operation.locked_by,
            v_operation.lock_expires_at_utc,
            v_operation.created_at_utc,
            v_operation.started_at_utc,
            v_operation.updated_at_utc,
            v_operation.completed_at_utc,
            v_operation.failed_at_utc;
        return;
    end if;

    if v_operation.locked_by is not null
       and v_operation.lock_expires_at_utc is not null
       and v_operation.lock_expires_at_utc > now() then
        return query
        select
            false,
            'LOCK_ACTIVE'::text,
            v_operation.id,
            v_operation.operation_type,
            v_operation.status,
            v_operation.phase,
            v_operation.actor_user_id,
            v_operation.workbench_session_id,
            v_operation.pay_batch_id,
            v_operation.root_operation_id,
            v_operation.idempotency_key,
            v_operation.input_json,
            v_operation.config_json,
            v_operation.progress_json,
            v_operation.result_json,
            v_operation.error_json,
            v_operation.total_units,
            v_operation.completed_units,
            v_operation.failed_units,
            v_operation.current_chunk_index,
            v_operation.chunk_count,
            v_operation.locked_by,
            v_operation.lock_expires_at_utc,
            v_operation.created_at_utc,
            v_operation.started_at_utc,
            v_operation.updated_at_utc,
            v_operation.completed_at_utc,
            v_operation.failed_at_utc;
        return;
    end if;

    update public.banking_pay_operations as operation_update
    set
        status = 'RUNNING',
        locked_by = v_lock_owner,
        lock_expires_at_utc = now() + make_interval(secs => v_lock_seconds),
        started_at_utc = coalesce(operation_update.started_at_utc, now()),
        updated_at_utc = now()
    where operation_update.id = v_operation.id
    returning operation_update.* into v_operation;

    return query
    select
        true,
        null::text,
        v_operation.id,
        v_operation.operation_type,
        v_operation.status,
        v_operation.phase,
        v_operation.actor_user_id,
        v_operation.workbench_session_id,
        v_operation.pay_batch_id,
        v_operation.root_operation_id,
        v_operation.idempotency_key,
        v_operation.input_json,
        v_operation.config_json,
        v_operation.progress_json,
        v_operation.result_json,
        v_operation.error_json,
        v_operation.total_units,
        v_operation.completed_units,
        v_operation.failed_units,
        v_operation.current_chunk_index,
        v_operation.chunk_count,
        v_operation.locked_by,
        v_operation.lock_expires_at_utc,
        v_operation.created_at_utc,
        v_operation.started_at_utc,
        v_operation.updated_at_utc,
        v_operation.completed_at_utc,
        v_operation.failed_at_utc;
end;
$$;

create or replace function public.banking_pay_operation_save_progress(
    p_operation_id uuid,
    p_status text default null,
    p_phase text default null,
    p_total_units integer default null,
    p_completed_units integer default null,
    p_failed_units integer default null,
    p_current_chunk_index integer default null,
    p_chunk_count integer default null,
    p_progress_json jsonb default '{}'::jsonb,
    p_extend_lock_seconds integer default null
)
returns table (
    saved boolean,
    not_saved_reason text,
    operation_id uuid,
    operation_type text,
    status text,
    phase text,
    actor_user_id uuid,
    workbench_session_id uuid,
    pay_batch_id uuid,
    root_operation_id uuid,
    idempotency_key text,
    input_json jsonb,
    config_json jsonb,
    progress_json jsonb,
    result_json jsonb,
    error_json jsonb,
    total_units integer,
    completed_units integer,
    failed_units integer,
    current_chunk_index integer,
    chunk_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    updated_at_utc timestamptz,
    completed_at_utc timestamptz,
    failed_at_utc timestamptz
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_operation public.banking_pay_operations%rowtype;
    v_status text;
    v_phase text;
    v_total_units integer;
    v_completed_units integer;
    v_failed_units integer;
    v_current_chunk_index integer;
    v_chunk_count integer;
    v_progress_json jsonb;
    v_extend_lock_seconds integer;
    v_locked_by text;
    v_lock_expires_at_utc timestamptz;
begin
    v_progress_json := coalesce(p_progress_json, '{}'::jsonb);

    if jsonb_typeof(v_progress_json) <> 'object' then
        raise exception 'banking_pay_operation_save_progress requires p_progress_json to be a JSON object';
    end if;

    select operation_row.*
    into v_operation
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
        return query
        select
            false,
            'NOT_FOUND'::text,
            p_operation_id,
            null::text,
            null::text,
            null::text,
            null::uuid,
            null::uuid,
            null::uuid,
            null::uuid,
            null::text,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::integer,
            null::integer,
            null::integer,
            null::integer,
            null::integer,
            null::text,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz;
        return;
    end if;

    if v_operation.status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        return query
        select
            false,
            'TERMINAL'::text,
            v_operation.id,
            v_operation.operation_type,
            v_operation.status,
            v_operation.phase,
            v_operation.actor_user_id,
            v_operation.workbench_session_id,
            v_operation.pay_batch_id,
            v_operation.root_operation_id,
            v_operation.idempotency_key,
            v_operation.input_json,
            v_operation.config_json,
            v_operation.progress_json,
            v_operation.result_json,
            v_operation.error_json,
            v_operation.total_units,
            v_operation.completed_units,
            v_operation.failed_units,
            v_operation.current_chunk_index,
            v_operation.chunk_count,
            v_operation.locked_by,
            v_operation.lock_expires_at_utc,
            v_operation.created_at_utc,
            v_operation.started_at_utc,
            v_operation.updated_at_utc,
            v_operation.completed_at_utc,
            v_operation.failed_at_utc;
        return;
    end if;

    v_status := coalesce(nullif(btrim(p_status), ''), v_operation.status);
    v_phase := coalesce(nullif(btrim(p_phase), ''), v_operation.phase);

    if v_status not in ('QUEUED', 'RUNNING', 'WAITING', 'COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        raise exception 'Unsupported banking pay operation status: %', v_status;
    end if;

    if v_status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        return query
        select
            false,
            'TERMINAL_STATUS_REQUIRES_FINISH'::text,
            v_operation.id,
            v_operation.operation_type,
            v_operation.status,
            v_operation.phase,
            v_operation.actor_user_id,
            v_operation.workbench_session_id,
            v_operation.pay_batch_id,
            v_operation.root_operation_id,
            v_operation.idempotency_key,
            v_operation.input_json,
            v_operation.config_json,
            v_operation.progress_json,
            v_operation.result_json,
            v_operation.error_json,
            v_operation.total_units,
            v_operation.completed_units,
            v_operation.failed_units,
            v_operation.current_chunk_index,
            v_operation.chunk_count,
            v_operation.locked_by,
            v_operation.lock_expires_at_utc,
            v_operation.created_at_utc,
            v_operation.started_at_utc,
            v_operation.updated_at_utc,
            v_operation.completed_at_utc,
            v_operation.failed_at_utc;
        return;
    end if;

    v_total_units := coalesce(p_total_units, v_operation.total_units);
    v_completed_units := coalesce(p_completed_units, v_operation.completed_units);
    v_failed_units := coalesce(p_failed_units, v_operation.failed_units);
    v_current_chunk_index := coalesce(p_current_chunk_index, v_operation.current_chunk_index);
    v_chunk_count := coalesce(p_chunk_count, v_operation.chunk_count);

    if v_total_units < 0 then
        v_total_units := 0;
    end if;

    if v_completed_units < 0 then
        v_completed_units := 0;
    end if;

    if v_failed_units < 0 then
        v_failed_units := 0;
    end if;

    if v_current_chunk_index < 0 then
        v_current_chunk_index := 0;
    end if;

    if v_chunk_count < 0 then
        v_chunk_count := 0;
    end if;

    v_locked_by := null;
    v_lock_expires_at_utc := null;

    if p_extend_lock_seconds is not null and p_extend_lock_seconds > 0 then
        v_extend_lock_seconds := p_extend_lock_seconds;

        if v_extend_lock_seconds < 5 then
            v_extend_lock_seconds := 5;
        elsif v_extend_lock_seconds > 3600 then
            v_extend_lock_seconds := 3600;
        end if;

        v_locked_by := v_operation.locked_by;
        v_lock_expires_at_utc := now() + make_interval(secs => v_extend_lock_seconds);
    end if;

    update public.banking_pay_operations as operation_update
    set
        status = v_status,
        phase = v_phase,
        total_units = v_total_units,
        completed_units = v_completed_units,
        failed_units = v_failed_units,
        current_chunk_index = v_current_chunk_index,
        chunk_count = v_chunk_count,
        progress_json = coalesce(operation_update.progress_json, '{}'::jsonb) || v_progress_json,
        locked_by = v_locked_by,
        lock_expires_at_utc = v_lock_expires_at_utc,
        updated_at_utc = now()
    where operation_update.id = v_operation.id
    returning operation_update.* into v_operation;

    return query
    select
        true,
        null::text,
        v_operation.id,
        v_operation.operation_type,
        v_operation.status,
        v_operation.phase,
        v_operation.actor_user_id,
        v_operation.workbench_session_id,
        v_operation.pay_batch_id,
        v_operation.root_operation_id,
        v_operation.idempotency_key,
        v_operation.input_json,
        v_operation.config_json,
        v_operation.progress_json,
        v_operation.result_json,
        v_operation.error_json,
        v_operation.total_units,
        v_operation.completed_units,
        v_operation.failed_units,
        v_operation.current_chunk_index,
        v_operation.chunk_count,
        v_operation.locked_by,
        v_operation.lock_expires_at_utc,
        v_operation.created_at_utc,
        v_operation.started_at_utc,
        v_operation.updated_at_utc,
        v_operation.completed_at_utc,
        v_operation.failed_at_utc;
end;
$$;
create or replace function public.banking_pay_operation_start(
    p_operation_type text,
    p_actor_user_id uuid,
    p_idempotency_key text,
    p_workbench_session_id uuid default null,
    p_pay_batch_id uuid default null,
    p_root_operation_id uuid default null,
    p_input_json jsonb default '{}'::jsonb,
    p_config_json jsonb default '{}'::jsonb
)
returns table (
    operation_id uuid,
    operation_type text,
    status text,
    phase text,
    actor_user_id uuid,
    workbench_session_id uuid,
    pay_batch_id uuid,
    root_operation_id uuid,
    idempotency_key text,
    input_json jsonb,
    config_json jsonb,
    progress_json jsonb,
    result_json jsonb,
    error_json jsonb,
    total_units integer,
    completed_units integer,
    failed_units integer,
    current_chunk_index integer,
    chunk_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    updated_at_utc timestamptz,
    completed_at_utc timestamptz,
    failed_at_utc timestamptz,
    is_existing boolean
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_operation_type text;
    v_idempotency_key text;
    v_input_json jsonb;
    v_config_json jsonb;
    v_operation public.banking_pay_operations%rowtype;
begin
    v_operation_type := nullif(btrim(p_operation_type), '');
    v_idempotency_key := nullif(btrim(p_idempotency_key), '');
    v_input_json := coalesce(p_input_json, '{}'::jsonb);
    v_config_json := coalesce(p_config_json, '{}'::jsonb);

    if v_operation_type is null then
        raise exception 'banking_pay_operation_start requires p_operation_type';
    end if;

    if v_operation_type not in (
        'DRAFT_CREATE',
        'PAYMENT_EXECUTE',
        'PAYMENT_RETRY_BLOCKED_FUNDS',
        'PAYMENT_SETTLEMENT',
        'REMITTANCE_QUEUE',
        'PREVIEW_REFRESH'
    ) then
        raise exception 'Unsupported banking pay operation_type: %', v_operation_type;
    end if;

    if v_idempotency_key is null then
        raise exception 'banking_pay_operation_start requires p_idempotency_key';
    end if;

    if jsonb_typeof(v_input_json) <> 'object' then
        raise exception 'banking_pay_operation_start requires p_input_json to be a JSON object';
    end if;

    if jsonb_typeof(v_config_json) <> 'object' then
        raise exception 'banking_pay_operation_start requires p_config_json to be a JSON object';
    end if;

    perform pg_advisory_xact_lock(pg_catalog.hashtextextended('banking_pay_operation_start:' || v_idempotency_key, 0));

    select existing_operation.*
    into v_operation
    from public.banking_pay_operations as existing_operation
    where existing_operation.idempotency_key = v_idempotency_key
    order by
        case
            when existing_operation.status not in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then 0
            else 1
        end asc,
        existing_operation.created_at_utc desc,
        existing_operation.id desc
    limit 1
    for update;

    if found then
        if v_operation.operation_type <> v_operation_type then
            raise exception 'Existing banking pay operation idempotency key belongs to a different operation type';
        end if;

        if p_actor_user_id is not null
           and v_operation.actor_user_id is not null
           and v_operation.actor_user_id <> p_actor_user_id then
            raise exception 'Existing banking pay operation idempotency key belongs to a different actor';
        end if;

        if p_workbench_session_id is not null
           and v_operation.workbench_session_id is distinct from p_workbench_session_id then
            raise exception 'Existing banking pay operation idempotency key belongs to a different workbench session';
        end if;

        if p_pay_batch_id is not null
           and v_operation.pay_batch_id is distinct from p_pay_batch_id then
            raise exception 'Existing banking pay operation idempotency key belongs to a different pay batch';
        end if;

        if p_root_operation_id is not null
           and v_operation.root_operation_id is distinct from p_root_operation_id then
            raise exception 'Existing banking pay operation idempotency key belongs to a different root operation';
        end if;

        return query
        select
            v_operation.id,
            v_operation.operation_type,
            v_operation.status,
            v_operation.phase,
            v_operation.actor_user_id,
            v_operation.workbench_session_id,
            v_operation.pay_batch_id,
            v_operation.root_operation_id,
            v_operation.idempotency_key,
            v_operation.input_json,
            v_operation.config_json,
            v_operation.progress_json,
            v_operation.result_json,
            v_operation.error_json,
            v_operation.total_units,
            v_operation.completed_units,
            v_operation.failed_units,
            v_operation.current_chunk_index,
            v_operation.chunk_count,
            v_operation.locked_by,
            v_operation.lock_expires_at_utc,
            v_operation.created_at_utc,
            v_operation.started_at_utc,
            v_operation.updated_at_utc,
            v_operation.completed_at_utc,
            v_operation.failed_at_utc,
            true;
        return;
    end if;

    insert into public.banking_pay_operations (
        operation_type,
        status,
        phase,
        actor_user_id,
        workbench_session_id,
        pay_batch_id,
        root_operation_id,
        idempotency_key,
        input_json,
        config_json,
        progress_json,
        result_json,
        error_json,
        total_units,
        completed_units,
        failed_units,
        current_chunk_index,
        chunk_count
    )
    values (
        v_operation_type,
        'QUEUED',
        'INITIALISE',
        p_actor_user_id,
        p_workbench_session_id,
        p_pay_batch_id,
        p_root_operation_id,
        v_idempotency_key,
        v_input_json,
        v_config_json,
        '{}'::jsonb,
        null,
        null,
        0,
        0,
        0,
        0,
        0
    )
    returning * into v_operation;

    return query
    select
        v_operation.id,
        v_operation.operation_type,
        v_operation.status,
        v_operation.phase,
        v_operation.actor_user_id,
        v_operation.workbench_session_id,
        v_operation.pay_batch_id,
        v_operation.root_operation_id,
        v_operation.idempotency_key,
        v_operation.input_json,
        v_operation.config_json,
        v_operation.progress_json,
        v_operation.result_json,
        v_operation.error_json,
        v_operation.total_units,
        v_operation.completed_units,
        v_operation.failed_units,
        v_operation.current_chunk_index,
        v_operation.chunk_count,
        v_operation.locked_by,
        v_operation.lock_expires_at_utc,
        v_operation.created_at_utc,
        v_operation.started_at_utc,
        v_operation.updated_at_utc,
        v_operation.completed_at_utc,
        v_operation.failed_at_utc,
        false;
end;
$$;


create or replace function public.banking_pay_operation_finish(
    p_operation_id uuid,
    p_status text,
    p_result_json jsonb default null,
    p_error_json jsonb default null
)
returns table (
    finished boolean,
    not_finished_reason text,
    operation_id uuid,
    operation_type text,
    status text,
    phase text,
    actor_user_id uuid,
    workbench_session_id uuid,
    pay_batch_id uuid,
    root_operation_id uuid,
    idempotency_key text,
    input_json jsonb,
    config_json jsonb,
    progress_json jsonb,
    result_json jsonb,
    error_json jsonb,
    total_units integer,
    completed_units integer,
    failed_units integer,
    current_chunk_index integer,
    chunk_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    updated_at_utc timestamptz,
    completed_at_utc timestamptz,
    failed_at_utc timestamptz
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_operation public.banking_pay_operations%rowtype;
    v_status text;
    v_result_json jsonb;
    v_error_json jsonb;
begin
    v_status := nullif(btrim(p_status), '');
    v_result_json := p_result_json;
    v_error_json := p_error_json;

    if v_status is null then
        raise exception 'banking_pay_operation_finish requires p_status';
    end if;

    if v_status not in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        raise exception 'banking_pay_operation_finish requires a terminal status, got: %', v_status;
    end if;

    if v_result_json is not null and jsonb_typeof(v_result_json) <> 'object' then
        raise exception 'banking_pay_operation_finish requires p_result_json to be null or a JSON object';
    end if;

    if v_error_json is not null and jsonb_typeof(v_error_json) <> 'object' then
        raise exception 'banking_pay_operation_finish requires p_error_json to be null or a JSON object';
    end if;

    select operation_row.*
    into v_operation
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
        return query
        select
            false,
            'NOT_FOUND'::text,
            p_operation_id,
            null::text,
            null::text,
            null::text,
            null::uuid,
            null::uuid,
            null::uuid,
            null::uuid,
            null::text,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::integer,
            null::integer,
            null::integer,
            null::integer,
            null::integer,
            null::text,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz;
        return;
    end if;

    if v_operation.status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        return query
        select
            false,
            'ALREADY_TERMINAL'::text,
            v_operation.id,
            v_operation.operation_type,
            v_operation.status,
            v_operation.phase,
            v_operation.actor_user_id,
            v_operation.workbench_session_id,
            v_operation.pay_batch_id,
            v_operation.root_operation_id,
            v_operation.idempotency_key,
            v_operation.input_json,
            v_operation.config_json,
            v_operation.progress_json,
            v_operation.result_json,
            v_operation.error_json,
            v_operation.total_units,
            v_operation.completed_units,
            v_operation.failed_units,
            v_operation.current_chunk_index,
            v_operation.chunk_count,
            v_operation.locked_by,
            v_operation.lock_expires_at_utc,
            v_operation.created_at_utc,
            v_operation.started_at_utc,
            v_operation.updated_at_utc,
            v_operation.completed_at_utc,
            v_operation.failed_at_utc;
        return;
    end if;

    update public.banking_pay_operations as operation_update
    set
        status = v_status,
        result_json = v_result_json,
        error_json = v_error_json,
        locked_by = null,
        lock_expires_at_utc = null,
        completed_at_utc = case
            when v_status in ('COMPLETE', 'CANCELLED', 'REVIEW_REQUIRED') then coalesce(operation_update.completed_at_utc, now())
            else operation_update.completed_at_utc
        end,
        failed_at_utc = case
            when v_status = 'FAILED' then coalesce(operation_update.failed_at_utc, now())
            else operation_update.failed_at_utc
        end,
        updated_at_utc = now()
    where operation_update.id = v_operation.id
    returning operation_update.* into v_operation;

    return query
    select
        true,
        null::text,
        v_operation.id,
        v_operation.operation_type,
        v_operation.status,
        v_operation.phase,
        v_operation.actor_user_id,
        v_operation.workbench_session_id,
        v_operation.pay_batch_id,
        v_operation.root_operation_id,
        v_operation.idempotency_key,
        v_operation.input_json,
        v_operation.config_json,
        v_operation.progress_json,
        v_operation.result_json,
        v_operation.error_json,
        v_operation.total_units,
        v_operation.completed_units,
        v_operation.failed_units,
        v_operation.current_chunk_index,
        v_operation.chunk_count,
        v_operation.locked_by,
        v_operation.lock_expires_at_utc,
        v_operation.created_at_utc,
        v_operation.started_at_utc,
        v_operation.updated_at_utc,
        v_operation.completed_at_utc,
        v_operation.failed_at_utc;
end;
$$;


create or replace function public.banking_pay_operation_claim_chunk(
    p_operation_id uuid,
    p_phase text,
    p_chunk_type text,
    p_lock_owner text,
    p_lock_seconds integer default 60
)
returns table (
    chunk_id uuid,
    operation_id uuid,
    phase text,
    chunk_type text,
    sequence_no integer,
    status text,
    payload_json jsonb,
    result_json jsonb,
    error_json jsonb,
    unit_count integer,
    completed_count integer,
    failed_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    completed_at_utc timestamptz,
    updated_at_utc timestamptz
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_phase text;
    v_chunk_type text;
    v_lock_owner text;
    v_lock_seconds integer;
    v_chunk public.banking_pay_operation_chunks%rowtype;
begin
    v_phase := nullif(btrim(p_phase), '');
    v_chunk_type := nullif(btrim(p_chunk_type), '');
    v_lock_owner := coalesce(nullif(btrim(p_lock_owner), ''), 'unknown');
    v_lock_seconds := coalesce(p_lock_seconds, 60);

    if v_phase is null then
        raise exception 'banking_pay_operation_claim_chunk requires p_phase';
    end if;

    if v_chunk_type is null then
        raise exception 'banking_pay_operation_claim_chunk requires p_chunk_type';
    end if;

    if v_chunk_type not in (
        'CANDIDATE_SCOPE',
        'TSFIN',
        'PAYEE_READINESS',
        'TRANSFER_GROUP',
        'TRANSFER_SUBMIT',
        'RAIL_UPDATE',
        'SETTLEMENT',
        'REMITTANCE',
        'PAYOUT_NOTICE',
        'PREVIEW_PAGE',
        'FRESHNESS_VALIDATE'
    ) then
        raise exception 'Unsupported banking pay operation chunk_type: %', v_chunk_type;
    end if;

    if v_chunk_type = 'FRESHNESS_VALIDATE' and v_phase <> 'VALIDATE_FRESHNESS' then
        raise exception 'FRESHNESS_VALIDATE chunks must be claimed with phase VALIDATE_FRESHNESS';
    end if;

    if v_lock_seconds < 5 then
        v_lock_seconds := 5;
    elsif v_lock_seconds > 3600 then
        v_lock_seconds := 3600;
    end if;

    with claimable_chunk as (
        select chunk_row.id
        from public.banking_pay_operation_chunks as chunk_row
        where chunk_row.operation_id = p_operation_id
          and chunk_row.phase = v_phase
          and chunk_row.chunk_type = v_chunk_type
          and (
              chunk_row.status = 'PENDING'
              or (
                  chunk_row.status = 'RUNNING'
                  and (
                      chunk_row.lock_expires_at_utc is null
                      or chunk_row.lock_expires_at_utc <= now()
                  )
              )
          )
        order by chunk_row.sequence_no asc
        for update skip locked
        limit 1
    )
    update public.banking_pay_operation_chunks as chunk_update
    set
        status = 'RUNNING',
        locked_by = v_lock_owner,
        lock_expires_at_utc = now() + make_interval(secs => v_lock_seconds),
        started_at_utc = coalesce(chunk_update.started_at_utc, now()),
        updated_at_utc = now()
    from claimable_chunk
    where chunk_update.id = claimable_chunk.id
    returning chunk_update.* into v_chunk;

    if not found then
        return;
    end if;

    return query
    select
        v_chunk.id,
        v_chunk.operation_id,
        v_chunk.phase,
        v_chunk.chunk_type,
        v_chunk.sequence_no,
        v_chunk.status,
        v_chunk.payload_json,
        v_chunk.result_json,
        v_chunk.error_json,
        v_chunk.unit_count,
        v_chunk.completed_count,
        v_chunk.failed_count,
        v_chunk.locked_by,
        v_chunk.lock_expires_at_utc,
        v_chunk.created_at_utc,
        v_chunk.started_at_utc,
        v_chunk.completed_at_utc,
        v_chunk.updated_at_utc;
end;
$$;



create or replace function public.banking_pay_operation_finish_chunk(
    p_chunk_id uuid,
    p_status text,
    p_completed_count integer default null,
    p_failed_count integer default null,
    p_result_json jsonb default null,
    p_error_json jsonb default null
)
returns table (
    finished boolean,
    not_finished_reason text,
    chunk_id uuid,
    operation_id uuid,
    phase text,
    chunk_type text,
    sequence_no integer,
    status text,
    payload_json jsonb,
    result_json jsonb,
    error_json jsonb,
    unit_count integer,
    completed_count integer,
    failed_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    completed_at_utc timestamptz,
    updated_at_utc timestamptz
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_chunk public.banking_pay_operation_chunks%rowtype;
    v_status text;
    v_completed_count integer;
    v_failed_count integer;
    v_result_json jsonb;
    v_error_json jsonb;
begin
    v_status := nullif(btrim(p_status), '');
    v_result_json := p_result_json;
    v_error_json := p_error_json;

    if v_status is null then
        raise exception 'banking_pay_operation_finish_chunk requires p_status';
    end if;

    if v_status not in ('COMPLETE', 'FAILED', 'SKIPPED') then
        raise exception 'banking_pay_operation_finish_chunk requires COMPLETE, FAILED, or SKIPPED status, got: %', v_status;
    end if;

    if v_result_json is not null and jsonb_typeof(v_result_json) <> 'object' then
        raise exception 'banking_pay_operation_finish_chunk requires p_result_json to be null or a JSON object';
    end if;

    if v_error_json is not null and jsonb_typeof(v_error_json) <> 'object' then
        raise exception 'banking_pay_operation_finish_chunk requires p_error_json to be null or a JSON object';
    end if;

    select chunk_row.*
    into v_chunk
    from public.banking_pay_operation_chunks as chunk_row
    where chunk_row.id = p_chunk_id
    for update;

    if not found then
        return query
        select
            false,
            'NOT_FOUND'::text,
            p_chunk_id,
            null::uuid,
            null::text,
            null::text,
            null::integer,
            null::text,
            null::jsonb,
            null::jsonb,
            null::jsonb,
            null::integer,
            null::integer,
            null::integer,
            null::text,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz,
            null::timestamptz;
        return;
    end if;

    if v_chunk.status in ('COMPLETE', 'FAILED', 'SKIPPED') then
        return query
        select
            false,
            'ALREADY_TERMINAL'::text,
            v_chunk.id,
            v_chunk.operation_id,
            v_chunk.phase,
            v_chunk.chunk_type,
            v_chunk.sequence_no,
            v_chunk.status,
            v_chunk.payload_json,
            v_chunk.result_json,
            v_chunk.error_json,
            v_chunk.unit_count,
            v_chunk.completed_count,
            v_chunk.failed_count,
            v_chunk.locked_by,
            v_chunk.lock_expires_at_utc,
            v_chunk.created_at_utc,
            v_chunk.started_at_utc,
            v_chunk.completed_at_utc,
            v_chunk.updated_at_utc;
        return;
    end if;

    if v_status = 'COMPLETE' then
        v_completed_count := coalesce(p_completed_count, v_chunk.unit_count);
        v_failed_count := coalesce(p_failed_count, 0);
    elsif v_status = 'FAILED' then
        v_completed_count := coalesce(p_completed_count, 0);
        v_failed_count := coalesce(p_failed_count, v_chunk.unit_count);
    else
        v_completed_count := coalesce(p_completed_count, v_chunk.unit_count);
        v_failed_count := coalesce(p_failed_count, 0);
    end if;

    if v_completed_count < 0 then
        v_completed_count := 0;
    end if;

    if v_failed_count < 0 then
        v_failed_count := 0;
    end if;

    if v_completed_count > v_chunk.unit_count then
        v_completed_count := v_chunk.unit_count;
    end if;

    if v_failed_count > (v_chunk.unit_count - v_completed_count) then
        v_failed_count := v_chunk.unit_count - v_completed_count;
    end if;

    update public.banking_pay_operation_chunks as chunk_update
    set
        status = v_status,
        completed_count = v_completed_count,
        failed_count = v_failed_count,
        result_json = v_result_json,
        error_json = v_error_json,
        locked_by = null,
        lock_expires_at_utc = null,
        completed_at_utc = coalesce(chunk_update.completed_at_utc, now()),
        updated_at_utc = now()
    where chunk_update.id = v_chunk.id
    returning chunk_update.* into v_chunk;

    with chunk_progress as (
        select
            count(*)::integer as chunk_count,
            count(*) filter (where progress_chunk.status in ('COMPLETE', 'FAILED', 'SKIPPED'))::integer as completed_chunk_count,
            coalesce(sum(progress_chunk.unit_count), 0)::integer as total_units,
            coalesce(sum(progress_chunk.completed_count), 0)::integer as completed_units,
            coalesce(sum(progress_chunk.failed_count), 0)::integer as failed_units
        from public.banking_pay_operation_chunks as progress_chunk
        where progress_chunk.operation_id = v_chunk.operation_id
          and progress_chunk.phase = v_chunk.phase
          and progress_chunk.chunk_type = v_chunk.chunk_type
    )
    update public.banking_pay_operations as operation_update
    set
        phase = v_chunk.phase,
        total_units = chunk_progress.total_units,
        completed_units = chunk_progress.completed_units,
        failed_units = chunk_progress.failed_units,
        current_chunk_index = chunk_progress.completed_chunk_count,
        chunk_count = chunk_progress.chunk_count,
        progress_json = coalesce(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
            'chunk_type', v_chunk.chunk_type,
            'last_chunk_id', v_chunk.id,
            'last_chunk_status', v_chunk.status,
            'last_chunk_sequence_no', v_chunk.sequence_no
        ),
        updated_at_utc = now()
    from chunk_progress
    where operation_update.id = v_chunk.operation_id
      and operation_update.status not in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED');

    return query
    select
        true,
        null::text,
        v_chunk.id,
        v_chunk.operation_id,
        v_chunk.phase,
        v_chunk.chunk_type,
        v_chunk.sequence_no,
        v_chunk.status,
        v_chunk.payload_json,
        v_chunk.result_json,
        v_chunk.error_json,
        v_chunk.unit_count,
        v_chunk.completed_count,
        v_chunk.failed_count,
        v_chunk.locked_by,
        v_chunk.lock_expires_at_utc,
        v_chunk.created_at_utc,
        v_chunk.started_at_utc,
        v_chunk.completed_at_utc,
        v_chunk.updated_at_utc;
end;
$$;
create or replace function public.banking_pay_operation_find_active(
    p_operation_type text default null,
    p_workbench_session_id uuid default null,
    p_pay_batch_id uuid default null,
    p_actor_user_id uuid default null
)
returns table (
    operation_id uuid,
    operation_type text,
    status text,
    phase text,
    actor_user_id uuid,
    workbench_session_id uuid,
    pay_batch_id uuid,
    root_operation_id uuid,
    idempotency_key text,
    progress_json jsonb,
    total_units integer,
    completed_units integer,
    failed_units integer,
    current_chunk_index integer,
    chunk_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    updated_at_utc timestamptz
)
language plpgsql
security definer
stable
set search_path = public, pg_temp
as $$
declare
    v_operation_type text;
begin
    v_operation_type := nullif(btrim(p_operation_type), '');

    if p_workbench_session_id is null and p_pay_batch_id is null then
        raise exception 'banking_pay_operation_find_active requires p_workbench_session_id or p_pay_batch_id';
    end if;

    if v_operation_type is not null
       and v_operation_type not in (
           'DRAFT_CREATE',
           'PAYMENT_EXECUTE',
           'PAYMENT_RETRY_BLOCKED_FUNDS',
           'PAYMENT_SETTLEMENT',
           'REMITTANCE_QUEUE',
           'PREVIEW_REFRESH'
       ) then
        raise exception 'Unsupported banking pay operation_type: %', v_operation_type;
    end if;

    return query
    select
        operation_row.id,
        operation_row.operation_type,
        operation_row.status,
        operation_row.phase,
        operation_row.actor_user_id,
        operation_row.workbench_session_id,
        operation_row.pay_batch_id,
        operation_row.root_operation_id,
        operation_row.idempotency_key,
        operation_row.progress_json,
        operation_row.total_units,
        operation_row.completed_units,
        operation_row.failed_units,
        operation_row.current_chunk_index,
        operation_row.chunk_count,
        operation_row.locked_by,
        operation_row.lock_expires_at_utc,
        operation_row.created_at_utc,
        operation_row.started_at_utc,
        operation_row.updated_at_utc
    from public.banking_pay_operations as operation_row
    where operation_row.status not in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED')
      and (v_operation_type is null or operation_row.operation_type = v_operation_type)
      and (p_workbench_session_id is null or operation_row.workbench_session_id = p_workbench_session_id)
      and (p_pay_batch_id is null or operation_row.pay_batch_id = p_pay_batch_id)
      and (
          p_actor_user_id is null
          or operation_row.actor_user_id is null
          or operation_row.actor_user_id = p_actor_user_id
      )
    order by operation_row.updated_at_utc desc, operation_row.created_at_utc desc, operation_row.id desc
    limit 1;
end;
$$;
create or replace function public.banking_pay_operation_seed_chunks(
    p_operation_id uuid,
    p_phase text,
    p_chunk_type text,
    p_chunk_size integer,
    p_units_json jsonb
)
returns table (
    total_units integer,
    chunk_count integer,
    existing_chunk_count integer,
    new_chunk_count integer
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_phase text;
    v_chunk_type text;
    v_chunk_size integer;
    v_units_json jsonb;
    v_total_units integer;
    v_chunk_count integer;
    v_existing_chunk_count integer;
    v_new_chunk_count integer;
    v_mismatched_chunk_count integer;
    v_extra_chunk_count integer;
    v_operation_status text;
begin
    v_phase := nullif(btrim(p_phase), '');
    v_chunk_type := nullif(btrim(p_chunk_type), '');
    v_chunk_size := coalesce(p_chunk_size, 100);
    v_units_json := coalesce(p_units_json, '[]'::jsonb);

    if v_phase is null then
        raise exception 'banking_pay_operation_seed_chunks requires p_phase';
    end if;

    if v_chunk_type is null then
        raise exception 'banking_pay_operation_seed_chunks requires p_chunk_type';
    end if;

    if v_chunk_type not in (
        'CANDIDATE_SCOPE',
        'TSFIN',
        'PAYEE_READINESS',
        'TRANSFER_GROUP',
        'TRANSFER_SUBMIT',
        'RAIL_UPDATE',
        'SETTLEMENT',
        'REMITTANCE',
        'PAYOUT_NOTICE',
        'PREVIEW_PAGE',
        'FRESHNESS_VALIDATE'
    ) then
        raise exception 'Unsupported banking pay operation chunk_type: %', v_chunk_type;
    end if;

    if jsonb_typeof(v_units_json) <> 'array' then
        raise exception 'banking_pay_operation_seed_chunks requires p_units_json to be a JSON array';
    end if;

    select operation_row.status
    into v_operation_status
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
        raise exception 'banking_pay_operation_seed_chunks operation not found: %', p_operation_id;
    end if;

    if v_operation_status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        raise exception 'banking_pay_operation_seed_chunks cannot seed chunks for terminal operation % with status %', p_operation_id, v_operation_status;
    end if;

    if v_chunk_size < 1 then
        v_chunk_size := 1;
    elsif v_chunk_size > 10000 then
        v_chunk_size := 10000;
    end if;

    v_total_units := jsonb_array_length(v_units_json);

    if v_total_units = 0 then
        if exists (
            select 1
            from public.banking_pay_operation_chunks as existing_empty_check_chunk
            where existing_empty_check_chunk.operation_id = p_operation_id
              and existing_empty_check_chunk.phase = v_phase
              and existing_empty_check_chunk.chunk_type = v_chunk_type
        ) then
            raise exception 'CHUNK_SCOPE_MISMATCH: empty unit list does not match existing chunks for operation %, phase %, chunk_type %', p_operation_id, v_phase, v_chunk_type
              using errcode = 'P0001',
                    detail = jsonb_build_object(
                        'code', 'CHUNK_SCOPE_MISMATCH',
                        'operation_id', p_operation_id::text,
                        'phase', v_phase,
                        'chunk_type', v_chunk_type
                    )::text;
        end if;

        return query
        select
            0::integer,
            0::integer,
            0::integer,
            0::integer;
        return;
    end if;

    if v_chunk_type = 'CANDIDATE_SCOPE' then
        if exists (
            select 1
            from jsonb_array_elements(v_units_json) as candidate_scope_unit(unit_value)
            where not ((candidate_scope_unit.unit_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        ) then
            raise exception 'CANDIDATE_SCOPE chunks require p_units_json to contain candidate scope UUID strings';
        end if;

        if exists (
            select 1
            from jsonb_array_elements(v_units_json) as candidate_scope_unit(unit_value)
            where not exists (
                select 1
                from public.banking_pay_operation_candidate_scope as candidate_scope_row
                where candidate_scope_row.operation_id = p_operation_id
                  and candidate_scope_row.id = (candidate_scope_unit.unit_value #>> '{}')::uuid
            )
        ) then
            raise exception 'CANDIDATE_SCOPE chunks contain a candidate scope id that does not belong to the operation';
        end if;
    end if;

    if v_chunk_type = 'FRESHNESS_VALIDATE' then
        if v_phase <> 'VALIDATE_FRESHNESS' then
            raise exception 'FRESHNESS_VALIDATE chunks must use phase VALIDATE_FRESHNESS';
        end if;

        if exists (
            select 1
            from jsonb_array_elements(v_units_json) as freshness_unit(unit_value)
            where jsonb_typeof(freshness_unit.unit_value) <> 'object'
               or (
                    not (freshness_unit.unit_value ? 'timesheet_id')
                and not (freshness_unit.unit_value ? 'snapshot_id')
                and not (freshness_unit.unit_value ? 'pay_batch_item_ids')
               )
        ) then
            raise exception 'FRESHNESS_VALIDATE chunks require bounded frozen batch unit objects containing timesheet_id, snapshot_id, or pay_batch_item_ids';
        end if;

        if exists (
            select 1
            from jsonb_array_elements(v_units_json) as freshness_unit(unit_value)
            where (freshness_unit.unit_value ? 'pay_batch_item_ids')
              and jsonb_typeof(freshness_unit.unit_value->'pay_batch_item_ids') <> 'array'
        ) then
            raise exception 'FRESHNESS_VALIDATE unit pay_batch_item_ids must be a JSON array when supplied';
        end if;
    end if;

    v_chunk_count := (v_total_units + v_chunk_size - 1) / v_chunk_size;

    with ordered_units as (
        select
            unit_items.unit_value,
            unit_items.unit_ordinal,
            (((unit_items.unit_ordinal - 1) / v_chunk_size) + 1)::integer as sequence_no
        from jsonb_array_elements(v_units_json) with ordinality as unit_items(unit_value, unit_ordinal)
    ),
    expected_chunk_payloads as (
        select
            ordered_units.sequence_no,
            jsonb_build_object(
                'units', jsonb_agg(ordered_units.unit_value order by ordered_units.unit_ordinal asc),
                'unit_count', count(*)::integer
            ) as payload_json,
            count(*)::integer as unit_count
        from ordered_units
        group by ordered_units.sequence_no
    )
    select count(*)::integer
    into v_mismatched_chunk_count
    from expected_chunk_payloads as expected_chunk
    join public.banking_pay_operation_chunks as existing_chunk
      on existing_chunk.operation_id = p_operation_id
     and existing_chunk.phase = v_phase
     and existing_chunk.chunk_type = v_chunk_type
     and existing_chunk.sequence_no = expected_chunk.sequence_no
    where existing_chunk.unit_count <> expected_chunk.unit_count
       or existing_chunk.payload_json is distinct from expected_chunk.payload_json;

    if v_mismatched_chunk_count > 0 then
        raise exception 'CHUNK_SCOPE_MISMATCH: existing chunks differ for operation %, phase %, chunk_type %', p_operation_id, v_phase, v_chunk_type
          using errcode = 'P0001',
                detail = jsonb_build_object(
                    'code', 'CHUNK_SCOPE_MISMATCH',
                    'operation_id', p_operation_id::text,
                    'phase', v_phase,
                    'chunk_type', v_chunk_type,
                    'mismatched_chunk_count', v_mismatched_chunk_count
                )::text;
    end if;

    select count(*)::integer
    into v_extra_chunk_count
    from public.banking_pay_operation_chunks as extra_chunk
    where extra_chunk.operation_id = p_operation_id
      and extra_chunk.phase = v_phase
      and extra_chunk.chunk_type = v_chunk_type
      and extra_chunk.sequence_no > v_chunk_count;

    if v_extra_chunk_count > 0 then
        raise exception 'CHUNK_SCOPE_MISMATCH: extra existing chunks for operation %, phase %, chunk_type %', p_operation_id, v_phase, v_chunk_type
          using errcode = 'P0001',
                detail = jsonb_build_object(
                    'code', 'CHUNK_SCOPE_MISMATCH',
                    'operation_id', p_operation_id::text,
                    'phase', v_phase,
                    'chunk_type', v_chunk_type,
                    'extra_chunk_count', v_extra_chunk_count
                )::text;
    end if;

    with ordered_units as (
        select
            unit_items.unit_value,
            unit_items.unit_ordinal,
            (((unit_items.unit_ordinal - 1) / v_chunk_size) + 1)::integer as sequence_no
        from jsonb_array_elements(v_units_json) with ordinality as unit_items(unit_value, unit_ordinal)
    ),
    chunk_payloads as (
        select
            ordered_units.sequence_no,
            jsonb_build_object(
                'units', jsonb_agg(ordered_units.unit_value order by ordered_units.unit_ordinal asc),
                'unit_count', count(*)::integer
            ) as payload_json,
            count(*)::integer as unit_count
        from ordered_units
        group by ordered_units.sequence_no
    ),
    inserted_chunks as (
        insert into public.banking_pay_operation_chunks (
            operation_id,
            phase,
            chunk_type,
            sequence_no,
            status,
            payload_json,
            result_json,
            error_json,
            unit_count,
            completed_count,
            failed_count
        )
        select
            p_operation_id,
            v_phase,
            v_chunk_type,
            chunk_payloads.sequence_no,
            'PENDING',
            chunk_payloads.payload_json,
            null::jsonb,
            null::jsonb,
            chunk_payloads.unit_count,
            0,
            0
        from chunk_payloads
        order by chunk_payloads.sequence_no asc
        on conflict (operation_id, phase, chunk_type, sequence_no) do nothing
        returning 1
    )
    select count(*)::integer
    into v_new_chunk_count
    from inserted_chunks;

    v_existing_chunk_count := v_chunk_count - v_new_chunk_count;

    return query
    select
        v_total_units,
        v_chunk_count,
        v_existing_chunk_count,
        v_new_chunk_count;
end;
$$;




create or replace function public.pay_batch_stage_operation_candidate_chunk_context(
    p_operation_id uuid,
    p_pay_batch_id uuid,
    p_candidate_scope_ids jsonb,
    p_actor_user_id uuid
)
returns table (
    candidate_count integer,
    selected_row_count integer,
    timesheet_snapshot_count integer,
    finance_component_count integer
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_operation public.banking_pay_operations%rowtype;
    v_pay_batch public.pay_batches%rowtype;
    v_scope_ids jsonb;
    v_candidate_count integer;
    v_selected_row_count integer;
    v_timesheet_snapshot_count integer;
    v_finance_component_count integer;
    v_settings_bank_system text;
    v_settings_external_paye_system text;
    v_settings_rail_provider text;
    v_settings_rail_env text;
    v_need_name_check boolean;
    v_requires_payee_map boolean;
    v_vat_rate_pct numeric;
    v_erni_pct numeric;
    v_week_start date;
    v_scope text;
begin
    v_scope_ids := coalesce(p_candidate_scope_ids, '[]'::jsonb);

    if p_actor_user_id is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context requires p_actor_user_id';
    end if;

    perform 1
    from public.tms_users as actor_user
    where actor_user.id = p_actor_user_id;

    if not found then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context tms_users row not found: %', p_actor_user_id;
    end if;

    if jsonb_typeof(v_scope_ids) <> 'array' then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context requires p_candidate_scope_ids to be a JSON array';
    end if;

    if jsonb_array_length(v_scope_ids) = 0 then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context requires at least one candidate scope id';
    end if;

    if exists (
        select 1
        from jsonb_array_elements(v_scope_ids) as supplied_scope(scope_value)
        where not ((supplied_scope.scope_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    ) then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context requires candidate scope ids to be UUID strings';
    end if;

    select operation_row.*
    into v_operation
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context operation not found: %', p_operation_id;
    end if;

    if v_operation.operation_type <> 'DRAFT_CREATE' then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context expected DRAFT_CREATE operation %, got %', p_operation_id, v_operation.operation_type;
    end if;

    if v_operation.status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context cannot stage terminal operation % with status %', p_operation_id, v_operation.status;
    end if;

    if v_operation.actor_user_id is not null and v_operation.actor_user_id <> p_actor_user_id then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context operation % belongs to a different actor', p_operation_id;
    end if;

    select batch_row.*
    into v_pay_batch
    from public.pay_batches as batch_row
    where batch_row.id = p_pay_batch_id
    for update;

    if not found then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context pay batch not found: %', p_pay_batch_id;
    end if;

    if v_pay_batch.status <> 'DRAFT' then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context pay batch % is not DRAFT: %', p_pay_batch_id, v_pay_batch.status;
    end if;

    if v_pay_batch.source_workbench_session_id is not null
       and v_operation.workbench_session_id is not null
       and v_pay_batch.source_workbench_session_id <> v_operation.workbench_session_id then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context pay batch % belongs to a different workbench session', p_pay_batch_id;
    end if;

    drop table if exists pg_temp.tmp_pay_build_operation_candidate_scope;
    create temporary table pg_temp.tmp_pay_build_operation_candidate_scope as
    select scope_row.*
    from public.banking_pay_operation_candidate_scope as scope_row
    where scope_row.operation_id = p_operation_id
      and scope_row.pay_batch_id = p_pay_batch_id
      and scope_row.id in (
          select (supplied_scope.scope_value #>> '{}')::uuid
          from jsonb_array_elements(v_scope_ids) as supplied_scope(scope_value)
      )
    order by scope_row.chunk_sequence nulls last, scope_row.pay_channel, scope_row.candidate_id;

    select count(*)::integer
    into v_candidate_count
    from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope;

    if coalesce(v_candidate_count, 0) = 0 then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context found no candidate scope rows for operation %, batch %', p_operation_id, p_pay_batch_id;
    end if;

    if coalesce(v_candidate_count, 0) <> jsonb_array_length(v_scope_ids) then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context one or more candidate scope ids do not belong to operation % and batch %', p_operation_id, p_pay_batch_id;
    end if;

    select upper(btrim(coalesce(staged_scope.pay_channel, '')))
    into v_scope
    from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
    where upper(btrim(coalesce(staged_scope.pay_channel, ''))) in ('PAYE', 'UMBRELLA')
    group by upper(btrim(coalesce(staged_scope.pay_channel, '')))
    order by upper(btrim(coalesce(staged_scope.pay_channel, '')))
    limit 1;

    if v_scope is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context cannot determine pay channel scope for operation %', p_operation_id;
    end if;

    if (
        select count(*)::integer
        from (
            select distinct upper(btrim(coalesce(staged_scope.pay_channel, ''))) as pay_channel_scope
            from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
            where upper(btrim(coalesce(staged_scope.pay_channel, ''))) in ('PAYE', 'UMBRELLA')
        ) as pay_channel_scope_rows
    ) <> 1 then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context candidate scope chunk must contain exactly one pay channel scope';
    end if;

    v_week_start := public._pay_week_start_monday(v_pay_batch.pay_date);

    select coalesce(nullif(v_pay_batch.banking_system_snapshot, ''), settings_row.banking_system),
           coalesce(nullif(v_pay_batch.external_paye_system_snapshot, ''), settings_row.external_paye_system),
           coalesce(nullif(v_pay_batch.rail_provider_snapshot, ''), settings_row.rail_provider_default),
           coalesce(nullif(v_pay_batch.rail_env_snapshot, ''), settings_row.rail_env_default),
           (coalesce(settings_row.rail_supports_name_check, false) = true and upper(btrim(coalesce(coalesce(nullif(v_pay_batch.rail_provider_snapshot, ''), settings_row.rail_provider_default), ''))) <> 'CSV') as need_name_check,
           (upper(btrim(coalesce(coalesce(nullif(v_pay_batch.rail_provider_snapshot, ''), settings_row.rail_provider_default), ''))) <> 'CSV') as requires_payee_map
    into v_settings_bank_system,
         v_settings_external_paye_system,
         v_settings_rail_provider,
         v_settings_rail_env,
         v_need_name_check,
         v_requires_payee_map
    from public.settings_defaults as settings_row
    order by settings_row.id asc
    limit 1;

    if v_settings_bank_system is null or v_settings_external_paye_system is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context settings_defaults missing banking_system/external_paye_system';
    end if;

    if v_settings_rail_provider is null or v_settings_rail_env is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context settings_defaults missing rail provider/environment';
    end if;

    select finance_window.vat_rate_pct,
           finance_window.erni_pct
    into v_vat_rate_pct,
         v_erni_pct
    from public.settings_finance_windows as finance_window
    where v_pay_batch.pay_date >= finance_window.date_from
      and v_pay_batch.pay_date <= coalesce(finance_window.date_to, 'infinity'::date)
    order by finance_window.date_from desc
    limit 1;

    if v_vat_rate_pct is null or v_erni_pct is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context no matching finance window for pay date %', v_pay_batch.pay_date;
    end if;

    create temporary table if not exists pg_temp.tmp_pay_build_settings_context (
        banking_system text,
        external_paye_system text,
        rail_provider_default text,
        rail_env_default text,
        need_name_check boolean,
        requires_payee_map boolean,
        vat_rate_pct numeric,
        erni_pct numeric,
        pay_week_start date
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_settings_context;

    insert into pg_temp.tmp_pay_build_settings_context (
        banking_system,
        external_paye_system,
        rail_provider_default,
        rail_env_default,
        need_name_check,
        requires_payee_map,
        vat_rate_pct,
        erni_pct,
        pay_week_start
    ) values (
        v_settings_bank_system,
        v_settings_external_paye_system,
        v_settings_rail_provider,
        v_settings_rail_env,
        v_need_name_check,
        v_requires_payee_map,
        v_vat_rate_pct,
        v_erni_pct,
        v_week_start
    );

    create temporary table if not exists pg_temp.tmp_pay_build_preview_candidates (
        cand jsonb not null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_preview_candidates;

    insert into pg_temp.tmp_pay_build_preview_candidates(cand)
    select candidate_json.cand
    from (
        select staged_scope.effective_paye_candidate_json as cand
        from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
        where jsonb_typeof(staged_scope.effective_paye_candidate_json) = 'object'
          and nullif(btrim(coalesce(staged_scope.effective_paye_candidate_json->>'candidate_id', '')), '') is not null
        union all
        select staged_scope.effective_non_paye_payee_json as cand
        from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
        where jsonb_typeof(staged_scope.effective_non_paye_payee_json) = 'object'
          and nullif(btrim(coalesce(staged_scope.effective_non_paye_payee_json->>'candidate_id', '')), '') is not null
    ) as candidate_json;

    create temporary table if not exists pg_temp.tmp_pay_build_selected_preview_rows (
        preview_row_id text primary key,
        candidate_id uuid not null,
        finance_case_id uuid null,
        timesheet_id uuid null,
        client_id uuid null,
        line_type text null,
        case_type text null,
        pay_channel text null,
        paye_treatment text null,
        routing_kind text null,
        destination_label text null,
        taxability text null,
        beneficiary_name text null,
        masked_bank_account text null,
        bank_details_hash text null,
        item_direction text null,
        item_type_label text null,
        source_ref text null,
        preview_amount_ex_vat numeric(12,2) null,
        draftable boolean not null,
        snooze_allowed boolean not null,
        is_candidate_directed_oneoff_payout boolean not null,
        appears_on_umbrella_remittance boolean not null,
        generates_candidate_payment_advice boolean not null,
        case_components_json jsonb null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_selected_preview_rows;

    insert into pg_temp.tmp_pay_build_selected_preview_rows (
        preview_row_id,
        candidate_id,
        finance_case_id,
        timesheet_id,
        client_id,
        line_type,
        case_type,
        pay_channel,
        paye_treatment,
        routing_kind,
        destination_label,
        taxability,
        beneficiary_name,
        masked_bank_account,
        bank_details_hash,
        item_direction,
        item_type_label,
        source_ref,
        preview_amount_ex_vat,
        draftable,
        snooze_allowed,
        is_candidate_directed_oneoff_payout,
        appears_on_umbrella_remittance,
        generates_candidate_payment_advice,
        case_components_json
    )
    select distinct on (line_rows.preview_row_id)
        line_rows.preview_row_id,
        line_rows.candidate_id,
        line_rows.finance_case_id,
        line_rows.timesheet_id,
        line_rows.client_id,
        line_rows.line_type,
        line_rows.case_type,
        line_rows.pay_channel,
        line_rows.paye_treatment,
        line_rows.routing_kind,
        line_rows.destination_label,
        line_rows.taxability,
        line_rows.beneficiary_name,
        line_rows.masked_bank_account,
        line_rows.bank_details_hash,
        line_rows.item_direction,
        line_rows.item_type_label,
        line_rows.source_ref,
        line_rows.preview_amount_ex_vat,
        line_rows.draftable,
        line_rows.snooze_allowed,
        line_rows.is_candidate_directed_oneoff_payout,
        line_rows.appears_on_umbrella_remittance,
        line_rows.generates_candidate_payment_advice,
        line_rows.case_components_json
    from (
        select
            coalesce(nullif(btrim(coalesce(line_element.value->>'preview_row_id', '')), ''), nullif(btrim(coalesce(line_element.value->>'line_id', '')), ''), nullif(btrim(coalesce(line_element.value->>'row_id', '')), ''), nullif(btrim(coalesce(line_element.value->>'id', '')), '')) as preview_row_id,
            case when coalesce(line_element.value->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'candidate_id')::uuid else staged_scope.candidate_id end as candidate_id,
            case when coalesce(line_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'finance_case_id')::uuid else null::uuid end as finance_case_id,
            case when coalesce(line_element.value->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'timesheet_id')::uuid else null::uuid end as timesheet_id,
            case when coalesce(line_element.value->>'client_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'client_id')::uuid else null::uuid end as client_id,
            nullif(btrim(coalesce(line_element.value->>'line_type', '')), '') as line_type,
            nullif(btrim(coalesce(line_element.value->>'case_type', '')), '') as case_type,
            upper(btrim(coalesce(line_element.value->>'pay_channel', staged_scope.pay_channel, ''))) as pay_channel,
            upper(btrim(coalesce(line_element.value->>'paye_treatment', ''))) as paye_treatment,
            nullif(btrim(coalesce(line_element.value->>'routing_kind', '')), '') as routing_kind,
            nullif(btrim(coalesce(line_element.value->>'destination_label', '')), '') as destination_label,
            nullif(btrim(coalesce(line_element.value->>'taxability', '')), '') as taxability,
            nullif(btrim(coalesce(line_element.value->>'beneficiary_name', '')), '') as beneficiary_name,
            nullif(btrim(coalesce(line_element.value->>'masked_bank_account', '')), '') as masked_bank_account,
            nullif(btrim(coalesce(line_element.value->>'bank_details_hash', '')), '') as bank_details_hash,
            nullif(btrim(coalesce(line_element.value->>'item_direction', '')), '') as item_direction,
            nullif(btrim(coalesce(line_element.value->>'item_type_label', '')), '') as item_type_label,
            nullif(btrim(coalesce(line_element.value->>'source_ref', '')), '') as source_ref,
            round(coalesce(
                case when coalesce(line_element.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (line_element.value->>'amount_ex_vat')::numeric else null::numeric end,
                case when coalesce(line_element.value->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (line_element.value->>'preview_amount_ex_vat')::numeric else null::numeric end,
                0::numeric
            ), 2)::numeric(12,2) as preview_amount_ex_vat,
            coalesce(case when lower(coalesce(line_element.value->>'draftable', '')) in ('true', 'false') then (line_element.value->>'draftable')::boolean else null::boolean end, true) as draftable,
            coalesce(case when lower(coalesce(line_element.value->>'snooze_allowed', '')) in ('true', 'false') then (line_element.value->>'snooze_allowed')::boolean else null::boolean end, false) as snooze_allowed,
            coalesce(case when lower(coalesce(line_element.value->>'is_candidate_directed_oneoff_payout', '')) in ('true', 'false') then (line_element.value->>'is_candidate_directed_oneoff_payout')::boolean else null::boolean end, false) as is_candidate_directed_oneoff_payout,
            coalesce(case when lower(coalesce(line_element.value->>'appears_on_umbrella_remittance', '')) in ('true', 'false') then (line_element.value->>'appears_on_umbrella_remittance')::boolean else null::boolean end, false) as appears_on_umbrella_remittance,
            coalesce(case when lower(coalesce(line_element.value->>'generates_candidate_payment_advice', '')) in ('true', 'false') then (line_element.value->>'generates_candidate_payment_advice')::boolean else null::boolean end, false) as generates_candidate_payment_advice,
            coalesce(line_element.value->'case_components', line_element.value->'components', '[]'::jsonb) as case_components_json,
            line_element.ord as line_ordinal
        from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
        cross join lateral jsonb_array_elements(staged_scope.selected_canonical_preview_lines_json) with ordinality as line_element(value, ord)
        where jsonb_typeof(line_element.value) = 'object'
    ) as line_rows
    where line_rows.preview_row_id is not null
      and line_rows.candidate_id is not null
      and line_rows.draftable = true
    order by line_rows.preview_row_id, line_rows.line_ordinal;

    select count(*)::integer
    into v_selected_row_count
    from pg_temp.tmp_pay_build_selected_preview_rows as selected_row;

    if coalesce(v_selected_row_count, 0) = 0 then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context staged no selected preview rows for operation %, batch %', p_operation_id, p_pay_batch_id;
    end if;

    create temporary table if not exists pg_temp.tmp_pay_build_hidden_template_input_rows (
        candidate_id uuid null,
        finance_case_id uuid null,
        recovery_family text null,
        paye_treatment text null,
        raw_pay_channel text null,
        normalized_pay_channel text null,
        umbrella_id uuid null,
        source_ref text null,
        finance_component_id uuid null,
        frozen_component_key_type text null,
        frozen_component_key_value text null,
        frozen_component_snapshot_json jsonb null,
        frozen_component_classification public.pay_finance_component_classification_enum null,
        frozen_source_basis_json jsonb null,
        frozen_source_pay_method text null,
        frozen_target_pay_method text null,
        frozen_resolution_mode public.pay_finance_component_resolution_mode_enum null,
        frozen_resolution_payload_json jsonb null,
        frozen_resolution_result_json jsonb null,
        frozen_source_amount numeric(12,2) null,
        frozen_outstanding_amount numeric(12,2) null,
        weekly_due numeric(12,2) null,
        minimum_earnings_threshold numeric null,
        take_home_floor_override numeric null,
        default_take_home_floor numeric null,
        payout_status text null,
        next_due_week_start date null,
        sort_order integer null,
        required_identifiers_present boolean not null,
        candidate_selected boolean not null,
        pay_channel_matches_scope boolean not null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_hidden_template_input_rows;

    insert into pg_temp.tmp_pay_build_hidden_template_input_rows (
        candidate_id,
        finance_case_id,
        recovery_family,
        paye_treatment,
        raw_pay_channel,
        normalized_pay_channel,
        umbrella_id,
        source_ref,
        finance_component_id,
        frozen_component_key_type,
        frozen_component_key_value,
        frozen_component_snapshot_json,
        frozen_component_classification,
        frozen_source_basis_json,
        frozen_source_pay_method,
        frozen_target_pay_method,
        frozen_resolution_mode,
        frozen_resolution_payload_json,
        frozen_resolution_result_json,
        frozen_source_amount,
        frozen_outstanding_amount,
        weekly_due,
        minimum_earnings_threshold,
        take_home_floor_override,
        default_take_home_floor,
        payout_status,
        next_due_week_start,
        sort_order,
        required_identifiers_present,
        candidate_selected,
        pay_channel_matches_scope
    )
    select
        case when coalesce(template_element.value->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'candidate_id')::uuid else staged_scope.candidate_id end,
        case when coalesce(template_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'finance_case_id')::uuid else null::uuid end,
        upper(btrim(coalesce(template_element.value->>'recovery_family', ''))),
        upper(nullif(btrim(coalesce(template_element.value->>'paye_treatment', '')), '')),
        upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), '')),
        case
            when upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), '')) in ('PAYE', 'UMBRELLA') then upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), ''))
            when coalesce(btrim(coalesce(template_element.value->>'pay_channel', '')), '') = '' and v_scope = 'PAYE' then 'PAYE'
            else null::text
        end,
        case when coalesce(template_element.value->>'umbrella_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'umbrella_id')::uuid else null::uuid end,
        nullif(btrim(coalesce(template_element.value->>'source_ref', '')), ''),
        case when coalesce(template_element.value->>'finance_component_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'finance_component_id')::uuid else null::uuid end,
        nullif(btrim(coalesce(template_element.value->>'frozen_component_key_type', '')), ''),
        nullif(btrim(coalesce(template_element.value->>'frozen_component_key_value', '')), ''),
        case when jsonb_typeof(template_element.value->'frozen_component_snapshot_json') = 'object' then template_element.value->'frozen_component_snapshot_json' else null::jsonb end,
        case
            when upper(btrim(coalesce(template_element.value->>'frozen_component_classification', ''))) = 'TAXABLE_CHANNEL_SENSITIVE' then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            when upper(btrim(coalesce(template_element.value->>'frozen_component_classification', ''))) = 'REIMBURSEMENT_GROSS_FIXED' then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
            when upper(btrim(coalesce(template_element.value->>'frozen_component_classification', ''))) = 'NET_PAY_FIXED_RECOVERY' then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
            else null::public.pay_finance_component_classification_enum
        end,
        case when jsonb_typeof(template_element.value->'frozen_source_basis_json') = 'object' then template_element.value->'frozen_source_basis_json' else null::jsonb end,
        upper(nullif(btrim(coalesce(template_element.value->>'frozen_source_pay_method', '')), '')),
        upper(nullif(btrim(coalesce(template_element.value->>'frozen_target_pay_method', '')), '')),
        case
            when upper(btrim(coalesce(template_element.value->>'frozen_resolution_mode', ''))) = 'SUGGESTED_EQUIVALENT_BASIS' then 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum
            when upper(btrim(coalesce(template_element.value->>'frozen_resolution_mode', ''))) = 'MANUAL_REPLACEMENT_RATE' then 'MANUAL_REPLACEMENT_RATE'::public.pay_finance_component_resolution_mode_enum
            when upper(btrim(coalesce(template_element.value->>'frozen_resolution_mode', ''))) = 'MANUAL_AMOUNT' then 'MANUAL_AMOUNT'::public.pay_finance_component_resolution_mode_enum
            else null::public.pay_finance_component_resolution_mode_enum
        end,
        case when jsonb_typeof(template_element.value->'frozen_resolution_payload_json') = 'object' then template_element.value->'frozen_resolution_payload_json' else null::jsonb end,
        case when jsonb_typeof(template_element.value->'frozen_resolution_result_json') = 'object' then template_element.value->'frozen_resolution_result_json' else null::jsonb end,
        case when coalesce(template_element.value->>'frozen_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((template_element.value->>'frozen_source_amount')::numeric, 2)::numeric(12,2) else null::numeric(12,2) end,
        case when coalesce(template_element.value->>'frozen_outstanding_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((template_element.value->>'frozen_outstanding_amount')::numeric, 2)::numeric(12,2) else null::numeric(12,2) end,
        case when coalesce(template_element.value->>'weekly_due', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((template_element.value->>'weekly_due')::numeric, 2)::numeric(12,2) else null::numeric(12,2) end,
        case when coalesce(template_element.value->>'minimum_earnings_threshold', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (template_element.value->>'minimum_earnings_threshold')::numeric else null::numeric end,
        case when coalesce(template_element.value->>'take_home_floor_override', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (template_element.value->>'take_home_floor_override')::numeric else null::numeric end,
        case when coalesce(template_element.value->>'default_take_home_floor', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (template_element.value->>'default_take_home_floor')::numeric else null::numeric end,
        nullif(btrim(coalesce(template_element.value->>'payout_status', '')), ''),
        case when coalesce(template_element.value->>'next_due_week_start', '') ~ '^\d{4}-\d{2}-\d{2}$' then (template_element.value->>'next_due_week_start')::date else null::date end,
        case when coalesce(template_element.value->>'sort_order', '') ~ '^-?[0-9]+$' then (template_element.value->>'sort_order')::integer else template_element.ord::integer end,
        (
            (case when coalesce(template_element.value->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'candidate_id')::uuid else staged_scope.candidate_id end) is not null
            and coalesce(template_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            and upper(btrim(coalesce(template_element.value->>'recovery_family', ''))) in ('MANUAL_DEBT_RECOVERY', 'LOAN_REPAYMENT', 'OVERPAYMENT_RECOVERY')
        ),
        true,
        case
            when upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), '')) in ('PAYE', 'UMBRELLA') then upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), '')) = v_scope
            when coalesce(btrim(coalesce(template_element.value->>'pay_channel', '')), '') = '' and v_scope = 'PAYE' then true
            else false
        end
    from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
    cross join lateral jsonb_array_elements(staged_scope.hidden_recovery_template_lines_json) with ordinality as template_element(value, ord)
    where jsonb_typeof(template_element.value) = 'object';

    create temporary table if not exists pg_temp.tmp_pay_build_recovery_template_rows (
        candidate_id uuid not null,
        finance_case_id uuid not null,
        recovery_family text not null,
        paye_treatment text null,
        pay_channel text null,
        umbrella_id uuid null,
        source_ref text null,
        finance_component_id uuid null,
        frozen_component_key_type text null,
        frozen_component_key_value text null,
        frozen_component_snapshot_json jsonb null,
        frozen_component_classification public.pay_finance_component_classification_enum null,
        frozen_source_basis_json jsonb null,
        frozen_source_pay_method text null,
        frozen_target_pay_method text null,
        frozen_resolution_mode public.pay_finance_component_resolution_mode_enum null,
        frozen_resolution_payload_json jsonb null,
        frozen_resolution_result_json jsonb null,
        frozen_source_amount numeric(12,2) null,
        frozen_outstanding_amount numeric(12,2) null,
        weekly_due numeric(12,2) null,
        minimum_earnings_threshold numeric null,
        take_home_floor_override numeric null,
        default_take_home_floor numeric null,
        payout_status text null,
        next_due_week_start date null,
        sort_order integer null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_recovery_template_rows;

    insert into pg_temp.tmp_pay_build_recovery_template_rows (
        candidate_id,
        finance_case_id,
        recovery_family,
        paye_treatment,
        pay_channel,
        umbrella_id,
        source_ref,
        finance_component_id,
        frozen_component_key_type,
        frozen_component_key_value,
        frozen_component_snapshot_json,
        frozen_component_classification,
        frozen_source_basis_json,
        frozen_source_pay_method,
        frozen_target_pay_method,
        frozen_resolution_mode,
        frozen_resolution_payload_json,
        frozen_resolution_result_json,
        frozen_source_amount,
        frozen_outstanding_amount,
        weekly_due,
        minimum_earnings_threshold,
        take_home_floor_override,
        default_take_home_floor,
        payout_status,
        next_due_week_start,
        sort_order
    )
    select distinct on (
        hidden_input.candidate_id,
        hidden_input.finance_case_id,
        hidden_input.recovery_family,
        coalesce(hidden_input.finance_component_id, '00000000-0000-0000-0000-000000000000'::uuid),
        coalesce(hidden_input.source_ref, ''),
        coalesce(hidden_input.normalized_pay_channel, '')
    )
        hidden_input.candidate_id,
        hidden_input.finance_case_id,
        hidden_input.recovery_family,
        hidden_input.paye_treatment,
        hidden_input.normalized_pay_channel,
        hidden_input.umbrella_id,
        hidden_input.source_ref,
        hidden_input.finance_component_id,
        hidden_input.frozen_component_key_type,
        hidden_input.frozen_component_key_value,
        hidden_input.frozen_component_snapshot_json,
        hidden_input.frozen_component_classification,
        hidden_input.frozen_source_basis_json,
        hidden_input.frozen_source_pay_method,
        hidden_input.frozen_target_pay_method,
        hidden_input.frozen_resolution_mode,
        hidden_input.frozen_resolution_payload_json,
        hidden_input.frozen_resolution_result_json,
        hidden_input.frozen_source_amount,
        hidden_input.frozen_outstanding_amount,
        hidden_input.weekly_due,
        hidden_input.minimum_earnings_threshold,
        hidden_input.take_home_floor_override,
        hidden_input.default_take_home_floor,
        hidden_input.payout_status,
        hidden_input.next_due_week_start,
        hidden_input.sort_order
    from pg_temp.tmp_pay_build_hidden_template_input_rows as hidden_input
    where hidden_input.candidate_selected = true
      and hidden_input.required_identifiers_present = true
      and hidden_input.pay_channel_matches_scope = true
    order by
        hidden_input.candidate_id,
        hidden_input.finance_case_id,
        hidden_input.recovery_family,
        coalesce(hidden_input.finance_component_id, '00000000-0000-0000-0000-000000000000'::uuid),
        coalesce(hidden_input.source_ref, ''),
        coalesce(hidden_input.normalized_pay_channel, ''),
        hidden_input.sort_order;

    create temporary table if not exists pg_temp.tmp_pay_build_candidates_ctx (
        id uuid primary key,
        tms_ref text,
        display_name text,
        pay_method text,
        umbrella_id uuid,
        first_name text,
        last_name text,
        account_holder text,
        sort_code text,
        account_number text,
        bank_details_hash text,
        payee_id text,
        payee_account_id text,
        umbrella_vat_chargeable boolean
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_candidates_ctx;

    create temporary table if not exists pg_temp.tmp_pay_build_umbrellas_ctx (
        id uuid primary key,
        name text,
        vat_chargeable boolean,
        sort_code text,
        account_number text,
        bank_details_hash text,
        payee_id text,
        payee_account_id text
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_umbrellas_ctx;

    insert into pg_temp.tmp_pay_build_candidates_ctx (
        id,
        tms_ref,
        display_name,
        pay_method,
        umbrella_id,
        first_name,
        last_name,
        account_holder,
        sort_code,
        account_number,
        bank_details_hash,
        payee_id,
        payee_account_id,
        umbrella_vat_chargeable
    )
    with preview_candidate_rows as (
        select distinct on (preview_candidate_data.preview_candidate_id)
            preview_candidate_data.preview_candidate_id,
            preview_candidate_data.preview_tms_ref,
            preview_candidate_data.preview_display_name,
            preview_candidate_data.preview_pay_method,
            preview_candidate_data.preview_umbrella_id,
            preview_candidate_data.preview_umbrella_vat_chargeable,
            preview_candidate_data.preview_candidate_bank_hash
        from (
            select
                case when coalesce(preview_candidate.cand->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (preview_candidate.cand->>'candidate_id')::uuid else null::uuid end as preview_candidate_id,
                nullif(btrim(coalesce(preview_candidate.cand->>'tms_ref', '')), '') as preview_tms_ref,
                nullif(btrim(coalesce(preview_candidate.cand->>'display_name', '')), '') as preview_display_name,
                upper(nullif(btrim(coalesce(preview_candidate.cand->>'current_pay_method', '')), '')) as preview_pay_method,
                case when coalesce(preview_candidate.cand->>'umbrella_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (preview_candidate.cand->>'umbrella_id')::uuid else null::uuid end as preview_umbrella_id,
                coalesce(case when lower(coalesce(preview_candidate.cand->>'umbrella_vat_chargeable', '')) in ('true', 'false') then (preview_candidate.cand->>'umbrella_vat_chargeable')::boolean else null::boolean end, false) as preview_umbrella_vat_chargeable,
                nullif(btrim(coalesce(preview_candidate.cand->>'candidate_bank_hash', '')), '') as preview_candidate_bank_hash
            from pg_temp.tmp_pay_build_preview_candidates as preview_candidate
        ) as preview_candidate_data
        where preview_candidate_data.preview_candidate_id is not null
        order by preview_candidate_data.preview_candidate_id
    )
    select
        candidate_row.id,
        coalesce(preview_candidate_rows.preview_tms_ref, candidate_row.tms_ref),
        coalesce(preview_candidate_rows.preview_display_name, candidate_row.display_name),
        coalesce(preview_candidate_rows.preview_pay_method, upper(coalesce(candidate_row.pay_method, ''))),
        coalesce(preview_candidate_rows.preview_umbrella_id, candidate_row.umbrella_id),
        candidate_row.first_name,
        candidate_row.last_name,
        candidate_row.account_holder,
        regexp_replace(coalesce(candidate_row.sort_code, ''), '[^0-9]', '', 'g'),
        nullif(regexp_replace(coalesce(candidate_row.account_number, ''), '[^0-9]', '', 'g'), ''),
        coalesce(preview_candidate_rows.preview_candidate_bank_hash, candidate_row.bank_details_hash),
        candidate_payee_map.payee_id,
        candidate_payee_map.payee_account_id,
        coalesce(preview_candidate_rows.preview_umbrella_vat_chargeable, umbrella_row.vat_chargeable, false)
    from (
        select distinct selected_row.candidate_id
        from pg_temp.tmp_pay_build_selected_preview_rows as selected_row
    ) as selected_candidate
    join public.candidates as candidate_row
      on candidate_row.id = selected_candidate.candidate_id
    left join preview_candidate_rows
      on preview_candidate_rows.preview_candidate_id = candidate_row.id
    left join public.umbrellas as umbrella_row
      on umbrella_row.id = coalesce(preview_candidate_rows.preview_umbrella_id, candidate_row.umbrella_id)
    left join public.bank_payee_map as candidate_payee_map
      on upper(coalesce(candidate_payee_map.rail_provider, '')) = upper(btrim(coalesce(v_settings_rail_provider, '')))
     and upper(coalesce(candidate_payee_map.rail_env, '')) = upper(btrim(coalesce(v_settings_rail_env, '')))
     and upper(coalesce(candidate_payee_map.entity_kind, '')) = 'CANDIDATE'
     and candidate_payee_map.entity_id = candidate_row.id
     and candidate_payee_map.bank_details_hash = coalesce(preview_candidate_rows.preview_candidate_bank_hash, candidate_row.bank_details_hash);

    insert into pg_temp.tmp_pay_build_umbrellas_ctx (
        id,
        name,
        vat_chargeable,
        sort_code,
        account_number,
        bank_details_hash,
        payee_id,
        payee_account_id
    )
    select
        umbrella_row.id,
        umbrella_row.name,
        coalesce(umbrella_row.vat_chargeable, false),
        regexp_replace(coalesce(umbrella_row.sort_code, ''), '[^0-9]', '', 'g'),
        nullif(regexp_replace(coalesce(umbrella_row.account_number, ''), '[^0-9]', '', 'g'), ''),
        umbrella_row.bank_details_hash,
        umbrella_payee_map.payee_id,
        umbrella_payee_map.payee_account_id
    from public.umbrellas as umbrella_row
    join (
        select distinct candidate_ctx.umbrella_id
        from pg_temp.tmp_pay_build_candidates_ctx as candidate_ctx
        where candidate_ctx.umbrella_id is not null
    ) as selected_umbrella
      on selected_umbrella.umbrella_id = umbrella_row.id
    left join public.bank_payee_map as umbrella_payee_map
      on upper(coalesce(umbrella_payee_map.rail_provider, '')) = upper(btrim(coalesce(v_settings_rail_provider, '')))
     and upper(coalesce(umbrella_payee_map.rail_env, '')) = upper(btrim(coalesce(v_settings_rail_env, '')))
     and upper(coalesce(umbrella_payee_map.entity_kind, '')) = 'UMBRELLA'
     and umbrella_payee_map.entity_id = umbrella_row.id
     and umbrella_payee_map.bank_details_hash = umbrella_row.bank_details_hash;

    create temporary table if not exists pg_temp.tmp_pay_build_timesheet_snapshots_ctx (
        timesheet_id uuid primary key,
        candidate_id uuid not null,
        client_id uuid null,
        week_ending_date date null,
        source_pay_method text null,
        candidate_pay_method text null,
        baseline_signature text null,
        base_snapshot_json jsonb not null,
        target_snapshot_json jsonb not null,
        target_signature text null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_timesheet_snapshots_ctx;

    insert into pg_temp.tmp_pay_build_timesheet_snapshots_ctx (
        timesheet_id,
        candidate_id,
        client_id,
        week_ending_date,
        source_pay_method,
        candidate_pay_method,
        baseline_signature,
        base_snapshot_json,
        target_snapshot_json,
        target_signature
    )
    select distinct on (snapshot_row.timesheet_id)
        snapshot_row.timesheet_id,
        snapshot_row.candidate_id,
        snapshot_row.client_id,
        snapshot_row.week_ending_date,
        snapshot_row.source_pay_method,
        snapshot_row.candidate_pay_method,
        snapshot_row.baseline_signature,
        snapshot_row.base_snapshot_json,
        snapshot_row.target_snapshot_json,
        snapshot_row.target_signature
    from (
        select
            case when coalesce(snapshot_element.value->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (snapshot_element.value->>'timesheet_id')::uuid else null::uuid end as timesheet_id,
            case when coalesce(snapshot_element.value->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (snapshot_element.value->>'candidate_id')::uuid else staged_scope.candidate_id end as candidate_id,
            case when coalesce(snapshot_element.value->>'client_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (snapshot_element.value->>'client_id')::uuid else null::uuid end as client_id,
            case when coalesce(snapshot_element.value->>'week_ending_date', '') ~ '^\d{4}-\d{2}-\d{2}$' then (snapshot_element.value->>'week_ending_date')::date else null::date end as week_ending_date,
            nullif(btrim(coalesce(snapshot_element.value->>'source_pay_method', '')), '') as source_pay_method,
            nullif(btrim(coalesce(snapshot_element.value->>'candidate_pay_method', '')), '') as candidate_pay_method,
            nullif(btrim(coalesce(snapshot_element.value->>'baseline_signature', '')), '') as baseline_signature,
            coalesce(snapshot_element.value->'base_snapshot_json', '{}'::jsonb) as base_snapshot_json,
            coalesce(snapshot_element.value->'target_snapshot_json', '{}'::jsonb) as target_snapshot_json,
            nullif(btrim(coalesce(snapshot_element.value->>'target_signature', '')), '') as target_signature
        from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
        cross join lateral jsonb_array_elements(
            case
                when jsonb_typeof(staged_scope.effective_candidate_fragment_json->'timesheet_snapshots_json') = 'array' then coalesce(staged_scope.effective_candidate_fragment_json->'timesheet_snapshots_json', '[]'::jsonb)
                when jsonb_typeof(staged_scope.effective_candidate_fragment_json->'timesheet_snapshots') = 'array' then coalesce(staged_scope.effective_candidate_fragment_json->'timesheet_snapshots', '[]'::jsonb)
                else '[]'::jsonb
            end
        ) as snapshot_element(value)
        where jsonb_typeof(snapshot_element.value) = 'object'
    ) as snapshot_row
    where snapshot_row.timesheet_id is not null
      and exists (
          select 1
          from pg_temp.tmp_pay_build_selected_preview_rows as selected_row
          where selected_row.timesheet_id = snapshot_row.timesheet_id
            and selected_row.candidate_id = snapshot_row.candidate_id
      )
    order by snapshot_row.timesheet_id, snapshot_row.week_ending_date desc nulls last;

    create temporary table if not exists pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx (
        finance_case_id uuid primary key,
        beneficiary_name text,
        sort_code text,
        account_number text,
        bank_details_hash text,
        created_by_user_id uuid,
        updated_by_user_id uuid,
        note text
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx;

    insert into pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx (
        finance_case_id,
        beneficiary_name,
        sort_code,
        account_number,
        bank_details_hash,
        created_by_user_id,
        updated_by_user_id,
        note
    )
    select
        oneoff_details.finance_case_id,
        oneoff_details.beneficiary_name,
        regexp_replace(coalesce(oneoff_details.sort_code, ''), '[^0-9]', '', 'g'),
        nullif(regexp_replace(coalesce(oneoff_details.account_number, ''), '[^0-9]', '', 'g'), ''),
        oneoff_details.bank_details_hash,
        oneoff_details.created_by_user_id,
        oneoff_details.updated_by_user_id,
        oneoff_details.note
    from public.pay_finance_case_oneoff_payout_bank_details as oneoff_details
    where oneoff_details.finance_case_id in (
        select distinct selected_row.finance_case_id
        from pg_temp.tmp_pay_build_selected_preview_rows as selected_row
        where selected_row.finance_case_id is not null
    );

    create temporary table if not exists pg_temp.tmp_pay_build_finance_case_components_ctx (
        id uuid primary key,
        finance_case_id uuid not null,
        candidate_id uuid null,
        linked_timesheet_id uuid null,
        component_key_type text null,
        component_key_value text null,
        classification public.pay_finance_component_classification_enum null,
        source_pay_method text null,
        source_basis_json jsonb null,
        saved_target_pay_method text null,
        saved_resolution_mode public.pay_finance_component_resolution_mode_enum null,
        saved_resolution_payload_json jsonb null,
        saved_resolution_result_json jsonb null,
        source_amount numeric(12,2) null,
        remaining_source_amount numeric(12,2) null,
        allocation_priority_group integer null,
        allocation_priority_order integer null,
        created_at_utc timestamptz null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_finance_case_components_ctx;

    -- Operation-mode staging must use the frozen operation candidate/allocation scope.
    -- Do not rebuild finance component identity from live public.pay_finance_case_components here.
    -- The child batch-builder functions read this temp table, so expose allocation rows in the same shape.
    insert into pg_temp.tmp_pay_build_finance_case_components_ctx (
        id,
        finance_case_id,
        candidate_id,
        linked_timesheet_id,
        component_key_type,
        component_key_value,
        classification,
        source_pay_method,
        source_basis_json,
        saved_target_pay_method,
        saved_resolution_mode,
        saved_resolution_payload_json,
        saved_resolution_result_json,
        source_amount,
        remaining_source_amount,
        allocation_priority_group,
        allocation_priority_order,
        created_at_utc
    )
    select distinct on (coalesce(allocation_row.finance_component_id, allocation_row.id))
        coalesce(allocation_row.finance_component_id, allocation_row.id) as id,
        allocation_row.finance_case_id,
        allocation_row.candidate_id,
        case
            when coalesce(allocation_row.allocation_basis_json #>> '{line,timesheet_id}', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                then (allocation_row.allocation_basis_json #>> '{line,timesheet_id}')::uuid
            else null::uuid
        end as linked_timesheet_id,
        coalesce(
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,frozen_component_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,component_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,economic_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{component,frozen_component_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{component,component_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_type), '')
        ) as component_key_type,
        coalesce(
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,frozen_component_key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,component_key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,economic_key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{component,frozen_component_key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{component,component_key_value}'), ''),
            allocation_row.operation_source_key
        ) as component_key_value,
        case
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_component_classification}', allocation_row.allocation_basis_json #>> '{component,classification}', '')) = 'TAXABLE_CHANNEL_SENSITIVE'
                then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_component_classification}', allocation_row.allocation_basis_json #>> '{component,classification}', '')) = 'REIMBURSEMENT_GROSS_FIXED'
                then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_component_classification}', allocation_row.allocation_basis_json #>> '{component,classification}', '')) = 'NET_PAY_FIXED_RECOVERY'
                then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
            else null::public.pay_finance_component_classification_enum
        end as classification,
        upper(nullif(btrim(coalesce(
            allocation_row.allocation_basis_json #>> '{line,frozen_source_pay_method}',
            allocation_row.allocation_basis_json #>> '{line,source_pay_method}',
            allocation_row.allocation_basis_json #>> '{component,source_pay_method}',
            allocation_row.pay_channel
        )), '')) as source_pay_method,
        coalesce(
            nullif(allocation_row.allocation_basis_json->'line'->'frozen_source_basis_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'line'->'source_basis_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'component'->'source_basis_json', 'null'::jsonb),
            allocation_row.allocation_basis_json
        ) as source_basis_json,
        upper(nullif(btrim(coalesce(
            allocation_row.allocation_basis_json #>> '{line,frozen_target_pay_method}',
            allocation_row.allocation_basis_json #>> '{line,target_pay_method}',
            allocation_row.allocation_basis_json #>> '{component,saved_target_pay_method}',
            allocation_row.pay_channel
        )), '')) as saved_target_pay_method,
        case
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_resolution_mode}', allocation_row.allocation_basis_json #>> '{component,saved_resolution_mode}', '')) = 'SUGGESTED_EQUIVALENT_BASIS'
                then 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_resolution_mode}', allocation_row.allocation_basis_json #>> '{component,saved_resolution_mode}', '')) = 'MANUAL_REPLACEMENT_RATE'
                then 'MANUAL_REPLACEMENT_RATE'::public.pay_finance_component_resolution_mode_enum
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_resolution_mode}', allocation_row.allocation_basis_json #>> '{component,saved_resolution_mode}', '')) = 'MANUAL_AMOUNT'
                then 'MANUAL_AMOUNT'::public.pay_finance_component_resolution_mode_enum
            else null::public.pay_finance_component_resolution_mode_enum
        end as saved_resolution_mode,
        coalesce(
            nullif(allocation_row.allocation_basis_json->'line'->'frozen_resolution_payload_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'line'->'resolution_payload_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'component'->'saved_resolution_payload_json', 'null'::jsonb),
            '{}'::jsonb
        ) as saved_resolution_payload_json,
        coalesce(
            nullif(allocation_row.allocation_basis_json->'line'->'frozen_resolution_result_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'line'->'resolution_result_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'component'->'saved_resolution_result_json', 'null'::jsonb),
            jsonb_build_object('operation_source_key', allocation_row.operation_source_key, 'allocated_amount', allocation_row.allocated_amount)
        ) as saved_resolution_result_json,
        round(abs(coalesce(allocation_row.allocated_amount, 0)), 2)::numeric(12,2) as source_amount,
        round(abs(coalesce(allocation_row.allocated_amount, 0)), 2)::numeric(12,2) as remaining_source_amount,
        0::integer as allocation_priority_group,
        coalesce(allocation_row.sort_order, 0)::integer as allocation_priority_order,
        coalesce(allocation_row.created_at_utc, now()) as created_at_utc
    from public.banking_pay_operation_candidate_allocation_rows as allocation_row
    where allocation_row.operation_id = p_operation_id
      and allocation_row.pay_batch_id = p_pay_batch_id
      and allocation_row.candidate_scope_id in (
          select staged_scope.id
          from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
      )
      and allocation_row.finance_case_id is not null
    order by coalesce(allocation_row.finance_component_id, allocation_row.id), allocation_row.sort_order nulls last, allocation_row.id;

    select count(*)::integer
    into v_timesheet_snapshot_count
    from pg_temp.tmp_pay_build_timesheet_snapshots_ctx as snapshot_ctx;

    select count(*)::integer
    into v_finance_component_count
    from pg_temp.tmp_pay_build_finance_case_components_ctx as component_ctx;

    return query
    select
        coalesce(v_candidate_count, 0),
        coalesce(v_selected_row_count, 0),
        coalesce(v_timesheet_snapshot_count, 0),
        coalesce(v_finance_component_count, 0);
end;
$$;






create or replace function public.pay_workbench_prepare_draft_allocation_rows_seed(
    p_operation_id uuid,
    p_candidate_scope_ids jsonb default null
)
returns table (
    candidate_scopes_processed integer,
    allocation_rows_inserted integer,
    allocation_rows_reused integer,
    failures integer
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_operation_status text;
    v_scope_ids jsonb;
    v_candidate_scopes_processed integer;
    v_inserted integer;
    v_reused integer;
    v_mismatch_count integer;
begin
    v_scope_ids := coalesce(p_candidate_scope_ids, '[]'::jsonb);

    if p_candidate_scope_ids is not null and jsonb_typeof(p_candidate_scope_ids) <> 'array' then
        raise exception 'pay_workbench_prepare_draft_allocation_rows_seed requires p_candidate_scope_ids to be null or a JSON array';
    end if;

    select operation_row.status
    into v_operation_status
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
        raise exception 'pay_workbench_prepare_draft_allocation_rows_seed operation not found: %', p_operation_id;
    end if;

    if v_operation_status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        raise exception 'pay_workbench_prepare_draft_allocation_rows_seed cannot seed allocation rows for terminal operation % with status %', p_operation_id, v_operation_status;
    end if;

    if p_candidate_scope_ids is not null then
        if exists (
            select 1
            from jsonb_array_elements(v_scope_ids) as supplied_scope(scope_value)
            where not ((supplied_scope.scope_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        ) then
            raise exception 'pay_workbench_prepare_draft_allocation_rows_seed requires candidate scope ids to be UUID strings';
        end if;
    end if;

    drop table if exists pg_temp.tmp_pay_workbench_allocation_scope_rows;
    create temporary table pg_temp.tmp_pay_workbench_allocation_scope_rows as
    select scope_row.*
    from public.banking_pay_operation_candidate_scope as scope_row
    where scope_row.operation_id = p_operation_id
      and (
          p_candidate_scope_ids is null
          or scope_row.id in (
              select (supplied_scope.scope_value #>> '{}')::uuid
              from jsonb_array_elements(v_scope_ids) as supplied_scope(scope_value)
          )
      )
    for update;

    select count(*)::integer
    into v_candidate_scopes_processed
    from pg_temp.tmp_pay_workbench_allocation_scope_rows as selected_scope;

    if p_candidate_scope_ids is not null
       and coalesce(v_candidate_scopes_processed, 0) <> jsonb_array_length(v_scope_ids) then
        raise exception 'pay_workbench_prepare_draft_allocation_rows_seed one or more candidate scope ids do not belong to operation %', p_operation_id;
    end if;

    if coalesce(v_candidate_scopes_processed, 0) = 0 then
        return query
        select 0::integer, 0::integer, 0::integer, 0::integer;
        return;
    end if;

    drop table if exists pg_temp.tmp_pay_workbench_allocation_expected_rows;
    create temporary table pg_temp.tmp_pay_workbench_allocation_expected_rows as
    with selected_lines as (
        select
            selected_scope.id as candidate_scope_id,
            selected_scope.operation_id,
            selected_scope.pay_batch_id,
            selected_scope.candidate_id,
            selected_scope.pay_channel,
            selected_scope.baseline_component_rows_json,
            selected_scope.hidden_recovery_template_lines_json,
            line_element.value as line_json,
            line_element.ord as line_ordinal,
            btrim(coalesce(
                line_element.value->>'preview_row_id',
                line_element.value->>'line_id',
                line_element.value->>'row_id',
                line_element.value->>'id',
                ''
            )) as preview_row_id,
            case
                when coalesce(line_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'finance_case_id')::uuid
                else null::uuid
            end as finance_case_id,
            case
                when coalesce(line_element.value->>'finance_component_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'finance_component_id')::uuid
                when jsonb_typeof(line_element.value->'case_components') = 'array' then (
                    select case
                        when coalesce(component_element.value->>'finance_component_id', component_element.value->>'id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                            then coalesce(component_element.value->>'finance_component_id', component_element.value->>'id')::uuid
                        else null::uuid
                    end
                    from jsonb_array_elements(coalesce(line_element.value->'case_components', '[]'::jsonb)) as component_element(value)
                    where jsonb_typeof(component_element.value) = 'object'
                      and coalesce(component_element.value->>'finance_component_id', component_element.value->>'id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    order by coalesce(component_element.value->>'finance_component_id', component_element.value->>'id')
                    limit 1
                )
                else null::uuid
            end as finance_component_id,
            upper(coalesce(nullif(btrim(coalesce(line_element.value->>'line_type', '')), ''), nullif(btrim(coalesce(line_element.value->>'case_type', '')), ''), nullif(btrim(coalesce(line_element.value->>'item_type_label', '')), ''), 'FINANCE_ADJUSTMENT')) as allocation_type,
            nullif(btrim(coalesce(line_element.value->>'source_ref', '')), '') as source_ref,
            round(coalesce(
                case when coalesce(line_element.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (line_element.value->>'amount_ex_vat')::numeric else null::numeric end,
                case when coalesce(line_element.value->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (line_element.value->>'preview_amount_ex_vat')::numeric else null::numeric end,
                case when coalesce(line_element.value->>'amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (line_element.value->>'amount')::numeric else null::numeric end,
                0::numeric
            ), 2) as allocated_amount
        from pg_temp.tmp_pay_workbench_allocation_scope_rows as selected_scope
        cross join lateral jsonb_array_elements(selected_scope.selected_canonical_preview_lines_json) with ordinality as line_element(value, ord)
        where jsonb_typeof(line_element.value) = 'object'
    )
    select
        selected_lines.operation_id,
        selected_lines.candidate_scope_id,
        selected_lines.pay_batch_id,
        selected_lines.candidate_id,
        selected_lines.pay_channel,
        selected_lines.finance_case_id,
        selected_lines.finance_component_id,
        selected_lines.allocation_type,
        selected_lines.source_ref,
        (
            selected_lines.operation_id::text
            || ':allocation:' || selected_lines.candidate_scope_id::text
            || ':' || coalesce(nullif(selected_lines.preview_row_id, ''), 'line_' || selected_lines.line_ordinal::text)
            || ':' || coalesce(selected_lines.finance_component_id::text, 'no_component')
        ) as operation_source_key,
        selected_lines.allocated_amount,
        jsonb_build_object(
            'source', 'selected_canonical_preview_lines_json',
            'line', selected_lines.line_json,
            'baseline_component_rows', selected_lines.baseline_component_rows_json,
            'hidden_recovery_template_lines', selected_lines.hidden_recovery_template_lines_json
        ) as allocation_basis_json,
        selected_lines.line_ordinal::integer as sort_order
    from selected_lines
    where selected_lines.finance_case_id is not null
       or selected_lines.finance_component_id is not null
       or selected_lines.allocation_type in (
           'MANUAL_DEBT_RECOVERY',
           'LOAN_REPAYMENT',
           'OVERPAYMENT_RECOVERY',
           'PAYMENT_ADVANCE',
           'MANUAL_DEBT_ADJUSTMENT',
           'MANUAL_CREDIT_ADJUSTMENT',
           'UNDERPAYMENT',
           'FINANCE_ADJUSTMENT'
       );

    select count(*)::integer
    into v_mismatch_count
    from pg_temp.tmp_pay_workbench_allocation_expected_rows as expected_row
    join public.banking_pay_operation_candidate_allocation_rows as existing_row
      on existing_row.operation_id = expected_row.operation_id
     and existing_row.operation_source_key = expected_row.operation_source_key
    where existing_row.candidate_scope_id <> expected_row.candidate_scope_id
       or existing_row.candidate_id <> expected_row.candidate_id
       or existing_row.pay_channel <> expected_row.pay_channel
       or existing_row.finance_case_id is distinct from expected_row.finance_case_id
       or existing_row.finance_component_id is distinct from expected_row.finance_component_id
       or round(existing_row.allocated_amount, 2) <> round(expected_row.allocated_amount, 2)
       or existing_row.allocation_basis_json is distinct from expected_row.allocation_basis_json;

    if coalesce(v_mismatch_count, 0) > 0 then
        raise exception 'pay_workbench_prepare_draft_allocation_rows_seed found existing allocation rows with different payloads for operation %', p_operation_id;
    end if;

    with inserted_rows as (
        insert into public.banking_pay_operation_candidate_allocation_rows (
            operation_id,
            candidate_scope_id,
            pay_batch_id,
            candidate_id,
            pay_channel,
            finance_case_id,
            finance_component_id,
            allocation_type,
            source_ref,
            operation_source_key,
            allocated_amount,
            allocation_basis_json,
            sort_order,
            status
        )
        select
            expected_row.operation_id,
            expected_row.candidate_scope_id,
            expected_row.pay_batch_id,
            expected_row.candidate_id,
            expected_row.pay_channel,
            expected_row.finance_case_id,
            expected_row.finance_component_id,
            expected_row.allocation_type,
            expected_row.source_ref,
            expected_row.operation_source_key,
            expected_row.allocated_amount,
            expected_row.allocation_basis_json,
            expected_row.sort_order,
            'PENDING'
        from pg_temp.tmp_pay_workbench_allocation_expected_rows as expected_row
        on conflict (operation_id, operation_source_key) do nothing
        returning 1
    )
    select count(*)::integer
    into v_inserted
    from inserted_rows;

    select count(*)::integer
    into v_reused
    from pg_temp.tmp_pay_workbench_allocation_expected_rows as expected_row
    join public.banking_pay_operation_candidate_allocation_rows as existing_row
      on existing_row.operation_id = expected_row.operation_id
     and existing_row.operation_source_key = expected_row.operation_source_key;

    update public.banking_pay_operation_candidate_scope as scope_update
    set
        status = case when scope_update.status in ('PENDING', 'SCOPED') then 'ALLOCATED' else scope_update.status end,
        updated_at_utc = now()
    where scope_update.operation_id = p_operation_id
      and exists (
          select 1
          from pg_temp.tmp_pay_workbench_allocation_scope_rows as selected_scope
          where selected_scope.id = scope_update.id
      );

    return query
    select
        coalesce(v_candidate_scopes_processed, 0),
        coalesce(v_inserted, 0),
        greatest(coalesce(v_reused, 0) - coalesce(v_inserted, 0), 0),
        0::integer;
end;
$$;


create or replace function public.pay_batch_shell_ensure_from_operation(
    p_operation_id uuid,
    p_workbench_session_id uuid,
    p_actor_user_id uuid,
    p_batch_kind text,
    p_pay_channel text,
    p_input_json jsonb default '{}'::jsonb
)
returns table (
    pay_batch_id uuid,
    created boolean,
    scope jsonb
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_operation public.banking_pay_operations%rowtype;
    v_session public.banking_pay_workbench_sessions%rowtype;
    v_input_json jsonb;
    v_batch_kind text;
    v_pay_channel text;
    v_existing_pay_batch_id uuid;
    v_existing_pay_batch public.pay_batches%rowtype;
    v_existing_pay_batch_count integer;
    v_new_pay_batch_id uuid;
    v_scope_candidate_count integer;
    v_settings_bank_system text;
    v_settings_external_paye_system text;
    v_settings_rail_provider text;
    v_settings_rail_env text;
    v_same_week_override_json jsonb;
    v_same_week_override_used boolean;
    v_same_week_override_reason text;
    v_same_week_override_verified_at_utc timestamptz;
    v_same_week_override_verified_by_user_id uuid;
begin
    v_input_json := coalesce(p_input_json, '{}'::jsonb);
    v_batch_kind := upper(coalesce(nullif(btrim(p_batch_kind), ''), nullif(btrim(p_pay_channel), ''), 'MIXED'));
    v_pay_channel := upper(coalesce(nullif(btrim(p_pay_channel), ''), nullif(v_batch_kind, 'STANDARD_PAYRUN'), 'MIXED'));

    if v_batch_kind = 'STANDARD_PAYRUN' then
        v_batch_kind := v_pay_channel;
    end if;

    if jsonb_typeof(v_input_json) <> 'object' then
        raise exception 'pay_batch_shell_ensure_from_operation requires p_input_json to be a JSON object';
    end if;

    if v_batch_kind not in ('PAYE', 'UMBRELLA', 'MIXED', 'LOANS') then
        raise exception 'pay_batch_shell_ensure_from_operation unsupported batch kind: %', v_batch_kind;
    end if;

    if v_pay_channel not in ('PAYE', 'UMBRELLA', 'MIXED', 'LOANS') then
        raise exception 'pay_batch_shell_ensure_from_operation unsupported pay channel: %', v_pay_channel;
    end if;

    if p_actor_user_id is null then
        raise exception 'pay_batch_shell_ensure_from_operation requires p_actor_user_id';
    end if;

    perform 1
    from public.tms_users as actor_user
    where actor_user.id = p_actor_user_id;

    if not found then
        raise exception 'pay_batch_shell_ensure_from_operation tms_users row not found: %', p_actor_user_id;
    end if;

    select operation_row.*
    into v_operation
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
        raise exception 'pay_batch_shell_ensure_from_operation operation not found: %', p_operation_id;
    end if;

    if v_operation.operation_type <> 'DRAFT_CREATE' then
        raise exception 'pay_batch_shell_ensure_from_operation expected DRAFT_CREATE operation %, got %', p_operation_id, v_operation.operation_type;
    end if;

    if v_operation.status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        raise exception 'pay_batch_shell_ensure_from_operation cannot create shell for terminal operation % with status %', p_operation_id, v_operation.status;
    end if;

    if v_operation.actor_user_id is not null and v_operation.actor_user_id <> p_actor_user_id then
        raise exception 'pay_batch_shell_ensure_from_operation operation % belongs to a different actor', p_operation_id;
    end if;

    if v_operation.workbench_session_id is not null and v_operation.workbench_session_id <> p_workbench_session_id then
        raise exception 'pay_batch_shell_ensure_from_operation operation % belongs to a different workbench session', p_operation_id;
    end if;

    select session_row.*
    into v_session
    from public.banking_pay_workbench_sessions as session_row
    where session_row.id = p_workbench_session_id
    for update;

    if not found then
        raise exception 'pay_batch_shell_ensure_from_operation workbench session not found: %', p_workbench_session_id;
    end if;

    if v_session.status <> 'OPEN' then
        raise exception 'pay_batch_shell_ensure_from_operation workbench session % is not OPEN: %', p_workbench_session_id, v_session.status;
    end if;

    if v_session.discarded_at_utc is not null then
        raise exception 'pay_batch_shell_ensure_from_operation workbench session % has been discarded', p_workbench_session_id;
    end if;

    if v_session.actor_user_id <> p_actor_user_id then
        raise exception 'pay_batch_shell_ensure_from_operation workbench session % belongs to a different actor', p_workbench_session_id;
    end if;

    select count(*)::integer
    into v_scope_candidate_count
    from public.banking_pay_operation_candidate_scope as scope_row
    where scope_row.operation_id = p_operation_id
      and scope_row.workbench_session_id = p_workbench_session_id
      and (
          v_pay_channel in ('MIXED', 'LOANS')
          or scope_row.pay_channel = v_pay_channel
      );

    if coalesce(v_scope_candidate_count, 0) = 0 then
        raise exception 'pay_batch_shell_ensure_from_operation found no candidate scope rows for operation %, pay channel %', p_operation_id, v_pay_channel;
    end if;

    select count(*)::integer
    into v_existing_pay_batch_count
    from (
        select distinct scope_row.pay_batch_id
        from public.banking_pay_operation_candidate_scope as scope_row
        where scope_row.operation_id = p_operation_id
          and scope_row.workbench_session_id = p_workbench_session_id
          and scope_row.pay_batch_id is not null
          and (
              v_pay_channel in ('MIXED', 'LOANS')
              or scope_row.pay_channel = v_pay_channel
          )
    ) as existing_batch_ids;

    select distinct scope_row.pay_batch_id
    into v_existing_pay_batch_id
    from public.banking_pay_operation_candidate_scope as scope_row
    where scope_row.operation_id = p_operation_id
      and scope_row.workbench_session_id = p_workbench_session_id
      and scope_row.pay_batch_id is not null
      and (
          v_pay_channel in ('MIXED', 'LOANS')
          or scope_row.pay_channel = v_pay_channel
      )
    order by scope_row.pay_batch_id
    limit 1;

    if coalesce(v_existing_pay_batch_count, 0) > 1 then
        raise exception 'pay_batch_shell_ensure_from_operation found multiple pay batches for operation %, pay channel %', p_operation_id, v_pay_channel;
    end if;

    if v_existing_pay_batch_id is not null then
        select existing_batch_row.*
        into v_existing_pay_batch
        from public.pay_batches as existing_batch_row
        where existing_batch_row.id = v_existing_pay_batch_id
        for update;

        if not found then
            raise exception 'pay_batch_shell_ensure_from_operation candidate scope references missing pay batch %', v_existing_pay_batch_id;
        end if;

        if v_existing_pay_batch.status <> 'DRAFT' then
            raise exception 'pay_batch_shell_ensure_from_operation existing pay batch % is not DRAFT: %', v_existing_pay_batch_id, v_existing_pay_batch.status;
        end if;

        if v_existing_pay_batch.source_workbench_session_id is distinct from p_workbench_session_id then
            raise exception 'pay_batch_shell_ensure_from_operation existing pay batch % belongs to a different workbench session', v_existing_pay_batch_id;
        end if;

        if v_existing_pay_batch.batch_kind_fixed is not null
           and upper(btrim(v_existing_pay_batch.batch_kind_fixed)) is distinct from v_batch_kind then
            raise exception 'pay_batch_shell_ensure_from_operation existing pay batch % has batch kind %, expected %', v_existing_pay_batch_id, v_existing_pay_batch.batch_kind_fixed, v_batch_kind;
        end if;

        if v_existing_pay_batch.source_snapshot_run_id is distinct from v_session.source_snapshot_run_id then
            raise exception 'pay_batch_shell_ensure_from_operation existing pay batch % belongs to a different snapshot run', v_existing_pay_batch_id;
        end if;

        if v_existing_pay_batch.source_session_version is distinct from v_session.version then
            raise exception 'pay_batch_shell_ensure_from_operation existing pay batch % belongs to a different session version', v_existing_pay_batch_id;
        end if;

        update public.banking_pay_operation_candidate_scope as scope_update
        set
            pay_batch_id = v_existing_pay_batch_id,
            updated_at_utc = now()
        where scope_update.operation_id = p_operation_id
          and scope_update.workbench_session_id = p_workbench_session_id
          and scope_update.pay_batch_id is distinct from v_existing_pay_batch_id
          and (
              v_pay_channel in ('MIXED', 'LOANS')
              or scope_update.pay_channel = v_pay_channel
          );

        update public.banking_pay_operation_candidate_allocation_rows as allocation_update
        set
            pay_batch_id = v_existing_pay_batch_id,
            updated_at_utc = now()
        where allocation_update.operation_id = p_operation_id
          and allocation_update.pay_batch_id is distinct from v_existing_pay_batch_id
          and exists (
              select 1
              from public.banking_pay_operation_candidate_scope as scope_row
              where scope_row.id = allocation_update.candidate_scope_id
                and scope_row.operation_id = p_operation_id
                and scope_row.workbench_session_id = p_workbench_session_id
                and scope_row.pay_batch_id = v_existing_pay_batch_id
                and (
                    v_pay_channel in ('MIXED', 'LOANS')
                    or scope_row.pay_channel = v_pay_channel
                )
          );

        return query
        select
            v_existing_pay_batch_id,
            false,
            jsonb_build_object(
                'operation_id', p_operation_id::text,
                'workbench_session_id', p_workbench_session_id::text,
                'pay_channel', v_pay_channel,
                'batch_kind', v_batch_kind,
                'candidate_scope_count', v_scope_candidate_count,
                'source_snapshot_run_id', case when v_existing_pay_batch.source_snapshot_run_id is null then null else v_existing_pay_batch.source_snapshot_run_id::text end,
                'source_session_version', v_existing_pay_batch.source_session_version
            );
        return;
    end if;

    select settings_row.banking_system,
           settings_row.external_paye_system,
           settings_row.rail_provider_default,
           settings_row.rail_env_default
    into v_settings_bank_system,
         v_settings_external_paye_system,
         v_settings_rail_provider,
         v_settings_rail_env
    from public.settings_defaults as settings_row
    order by settings_row.id asc
    limit 1;

    if v_settings_bank_system is null or v_settings_external_paye_system is null then
        raise exception 'pay_batch_shell_ensure_from_operation settings_defaults missing banking_system/external_paye_system';
    end if;

    if v_settings_rail_provider is null or v_settings_rail_env is null then
        raise exception 'pay_batch_shell_ensure_from_operation settings_defaults missing rail provider/environment';
    end if;

    v_same_week_override_json := case
        when jsonb_typeof(v_input_json->'same_week_paye_override') = 'object' then coalesce(v_input_json->'same_week_paye_override', '{}'::jsonb)
        else '{}'::jsonb
    end;

    v_same_week_override_used := coalesce(
        case when lower(coalesce(v_input_json->>'same_week_paye_override_used', '')) in ('true', 'false') then (v_input_json->>'same_week_paye_override_used')::boolean else null::boolean end,
        case when lower(coalesce(v_same_week_override_json->>'used', '')) in ('true', 'false') then (v_same_week_override_json->>'used')::boolean else null::boolean end,
        false
    );

    v_same_week_override_reason := coalesce(
        nullif(btrim(coalesce(v_input_json->>'same_week_paye_override_reason', '')), ''),
        nullif(btrim(coalesce(v_same_week_override_json->>'reason', '')), '')
    );

    v_same_week_override_verified_by_user_id := case
        when coalesce(v_input_json->>'same_week_paye_override_verified_by_user_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (v_input_json->>'same_week_paye_override_verified_by_user_id')::uuid
        when coalesce(v_same_week_override_json->>'verified_by_user_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (v_same_week_override_json->>'verified_by_user_id')::uuid
        else null::uuid
    end;

    v_same_week_override_verified_at_utc := case
        when coalesce(v_input_json->>'same_week_paye_override_verified_at_utc', '') ~ '^\d{4}-\d{2}-\d{2}' then (v_input_json->>'same_week_paye_override_verified_at_utc')::timestamptz
        when coalesce(v_same_week_override_json->>'verified_at_utc', '') ~ '^\d{4}-\d{2}-\d{2}' then (v_same_week_override_json->>'verified_at_utc')::timestamptz
        else null::timestamptz
    end;

    insert into public.pay_batches (
        pay_date,
        created_at_utc,
        created_by_user_id,
        status,
        banking_system_snapshot,
        external_paye_system_snapshot,
        rail_provider_snapshot,
        rail_env_snapshot,
        batch_kind_fixed,
        source_workbench_session_id,
        source_snapshot_run_id,
        source_session_version,
        execution_commit_state,
        same_week_paye_override_used,
        same_week_paye_override_reason,
        same_week_paye_override_verified_at_utc,
        same_week_paye_override_verified_by_user_id
    )
    values (
        v_session.pay_date,
        now(),
        p_actor_user_id,
        'DRAFT',
        v_settings_bank_system,
        v_settings_external_paye_system,
        v_settings_rail_provider,
        v_settings_rail_env,
        v_batch_kind,
        p_workbench_session_id,
        v_session.source_snapshot_run_id,
        v_session.version,
        'NOT_SUBMITTED',
        v_same_week_override_used,
        v_same_week_override_reason,
        v_same_week_override_verified_at_utc,
        v_same_week_override_verified_by_user_id
    )
    returning public.pay_batches.id into v_new_pay_batch_id;

    update public.banking_pay_operation_candidate_scope as scope_update
    set
        pay_batch_id = v_new_pay_batch_id,
        updated_at_utc = now()
    where scope_update.operation_id = p_operation_id
      and scope_update.workbench_session_id = p_workbench_session_id
      and (
          v_pay_channel in ('MIXED', 'LOANS')
          or scope_update.pay_channel = v_pay_channel
      );

    update public.banking_pay_operation_candidate_allocation_rows as allocation_update
    set
        pay_batch_id = v_new_pay_batch_id,
        updated_at_utc = now()
    where allocation_update.operation_id = p_operation_id
      and allocation_update.pay_batch_id is distinct from v_new_pay_batch_id
      and exists (
          select 1
          from public.banking_pay_operation_candidate_scope as scope_row
          where scope_row.id = allocation_update.candidate_scope_id
            and scope_row.operation_id = p_operation_id
            and scope_row.workbench_session_id = p_workbench_session_id
            and scope_row.pay_batch_id = v_new_pay_batch_id
            and (
                v_pay_channel in ('MIXED', 'LOANS')
                or scope_row.pay_channel = v_pay_channel
            )
      );

    return query
    select
        v_new_pay_batch_id,
        true,
        jsonb_build_object(
            'operation_id', p_operation_id::text,
            'workbench_session_id', p_workbench_session_id::text,
            'pay_channel', v_pay_channel,
            'batch_kind', v_batch_kind,
            'candidate_scope_count', v_scope_candidate_count,
            'source_snapshot_run_id', case when v_session.source_snapshot_run_id is null then null else v_session.source_snapshot_run_id::text end,
            'source_session_version', v_session.version
        );
end;
$$;



DROP FUNCTION IF EXISTS public.pay_remittance_maybe_queue_for_trigger(uuid, text, text, uuid, boolean);
DROP FUNCTION IF EXISTS public.pay_remittance_maybe_queue_for_trigger(uuid, text, text, uuid, boolean, uuid, boolean);

DROP FUNCTION IF EXISTS public.pay_remittance_maybe_queue_for_trigger(uuid, text, text, uuid);
DROP FUNCTION IF EXISTS public.pay_remittance_maybe_queue_for_trigger(uuid, text, text, uuid, boolean);
DROP FUNCTION IF EXISTS public.pay_remittance_maybe_queue_for_trigger(uuid, text, text, uuid, boolean, uuid, boolean);


CREATE OR REPLACE FUNCTION public.pay_remittance_maybe_queue_for_trigger(
  p_pay_batch_id uuid,
  p_trigger text,
  p_scope text DEFAULT 'ALL'::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_only_confirmed boolean DEFAULT false,
  p_root_operation_id uuid DEFAULT NULL::uuid,
  p_operation_mode boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_batch public.pay_batches%rowtype;
  v_trigger text;
  v_scope text;
  v_timing_setting text := 'ON_EXECUTION';
  v_effective_actor_user_id uuid;
  v_is_loans_batch boolean := false;
  v_suppress_remittances boolean := false;
  v_suppress_reasons jsonb := '[]'::jsonb;
  v_auth_suppress_count integer := 0;
  v_queue_result jsonb := '{}'::jsonb;
  v_has_remittance_operation_overload boolean := false;
  v_has_finance_operation_overload boolean := false;
  v_has_remittance_confirmed_overload boolean := false;
  v_has_finance_confirmed_overload boolean := false;
  v_has_remittance_legacy_function boolean := false;
  v_has_finance_legacy_function boolean := false;
  v_dispatch_required boolean := false;
  v_job_count integer := 0;
  v_operation_start record;
  v_operation_kind text := null;
  v_operation_idempotency_key text := null;
  v_operation_input_json jsonb := '{}'::jsonb;
  v_operation_config_json jsonb := '{}'::jsonb;
  v_operation_config_snapshot_status text := 'created_or_reused';
  v_batch_candidate_count integer := 0;
  v_large_batch_threshold integer := 100;
BEGIN
  v_trigger := upper(nullif(btrim(COALESCE(p_trigger, '')), ''));
  v_scope := upper(nullif(btrim(COALESCE(p_scope, 'ALL')), ''));

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAY_REMITTANCE_MAYBE_QUEUE_FOR_TRIGGER_START',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'trigger', v_trigger,
      'scope', v_scope,
      'actor_user_id', p_actor_user_id,
      'only_confirmed', COALESCE(p_only_confirmed, false)
    ),
    'pay_remittance',
    COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  IF v_trigger NOT IN ('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED') THEN
    RAISE EXCEPTION 'UNSUPPORTED_REMITTANCE_QUEUE_TRIGGER'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UNSUPPORTED_REMITTANCE_QUEUE_TRIGGER',
              'trigger', p_trigger,
              'supported_triggers', jsonb_build_array('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED')
            )::text;
  END IF;

  IF v_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'UNSUPPORTED_REMITTANCE_QUEUE_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'UNSUPPORTED_REMITTANCE_QUEUE_SCOPE',
              'scope', p_scope,
              'supported_scopes', jsonb_build_array('ALL', 'PAYE', 'UMBRELLA')
            )::text;
  END IF;

  SELECT public.pay_batches.*
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_batch.created_by_user_id);
  v_is_loans_batch := upper(btrim(COALESCE(v_batch.batch_kind_fixed, ''))) IN ('LOANS', 'LOAN', 'FINANCE_PAYOUTS', 'PAYOUTS');

  SELECT COALESCE(NULLIF(btrim(public.settings_defaults.payment_remittance_send_timing), ''), 'ON_EXECUTION')
  INTO v_timing_setting
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_timing_setting := upper(COALESCE(NULLIF(btrim(v_timing_setting), ''), 'ON_EXECUTION'));

  IF v_timing_setting NOT IN ('ON_EXECUTION', 'ON_PAYMENT_CONFIRMED') THEN
    v_timing_setting := 'ON_EXECUTION';
  END IF;

  IF v_timing_setting <> v_trigger THEN
    RETURN jsonb_build_object(
      'ok', true,
      'queued', false,
      'deferred', true,
      'suppressed', false,
      'dispatch_required', false,
      'requires_remittance_operation', false,
      'trigger_status', 'DEFERRED_BY_CONFIGURED_TIMING',
      'operation_mode', COALESCE(p_operation_mode, false),
      'operation_idempotency_key', NULL::text,
      'already_exists', false,
      'operation_created', false,
      'operation_reused', false,
      'child_operation_id', NULL::text,
      'operation_id', NULL::text,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'pay_batch_id', p_pay_batch_id,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'message', 'Remittance/payout notice queueing deferred because configured timing does not match this trigger.'
    );
  END IF;

  IF lower(btrim(COALESCE(v_batch.execution_intent_json->>'suppress_remittances', 'false'))) IN ('true', '1', 'yes', 'y', 'on') THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('batch.execution_intent_json.suppress_remittances');
  END IF;

  IF lower(btrim(COALESCE(v_batch.settlement_confirmation_json->>'suppress_remittances', 'false'))) IN ('true', '1', 'yes', 'y', 'on') THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('batch.settlement_confirmation_json.suppress_remittances');
  END IF;

  IF lower(btrim(COALESCE(v_batch.execution_intent_json->>'suppress_remittances_pending', 'false'))) IN ('true', '1', 'yes', 'y', 'on') THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('batch.execution_intent_json.suppress_remittances_pending');
  END IF;

  IF lower(btrim(COALESCE(v_batch.settlement_confirmation_json->>'suppress_remittances_pending', 'false'))) IN ('true', '1', 'yes', 'y', 'on') THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('batch.settlement_confirmation_json.suppress_remittances_pending');
  END IF;

  SELECT count(*)::integer
  INTO v_auth_suppress_count
  FROM public.pay_batch_auth_requests AS active_auth_requests
  WHERE active_auth_requests.pay_batch_id = p_pay_batch_id
    AND active_auth_requests.finalised_at_utc IS NULL
    AND (
      lower(btrim(COALESCE(active_auth_requests.execution_intent_json->>'suppress_remittances', 'false'))) IN ('true', '1', 'yes', 'y', 'on')
      OR lower(btrim(COALESCE(active_auth_requests.execution_intent_json->>'suppress_remittances_pending', 'false'))) IN ('true', '1', 'yes', 'y', 'on')
    );

  IF v_auth_suppress_count > 0 THEN
    v_suppress_remittances := true;
    v_suppress_reasons := v_suppress_reasons || jsonb_build_array('active_auth_request.execution_intent_json.suppress_remittances');
  END IF;

  IF v_suppress_remittances THEN
    RETURN jsonb_build_object(
      'ok', true,
      'queued', false,
      'deferred', false,
      'suppressed', true,
      'dispatch_required', false,
      'requires_remittance_operation', false,
      'trigger_status', 'SUPPRESSED_BY_EXECUTION_OR_SETTLEMENT_INTENT',
      'operation_mode', COALESCE(p_operation_mode, false),
      'operation_idempotency_key', NULL::text,
      'already_exists', false,
      'operation_created', false,
      'operation_reused', false,
      'child_operation_id', NULL::text,
      'operation_id', NULL::text,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'pay_batch_id', p_pay_batch_id,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'suppress_reasons', v_suppress_reasons,
      'message', 'Remittance/payout notice queueing suppressed by execution or settlement intent.'
    );
  END IF;

  IF v_effective_actor_user_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'queued', false,
      'deferred', false,
      'suppressed', false,
      'dispatch_required', false,
      'requires_remittance_operation', false,
      'trigger_status', 'ACTOR_USER_ID_REQUIRED_FOR_QUEUE_STAGE',
      'operation_mode', COALESCE(p_operation_mode, false),
      'operation_idempotency_key', NULL::text,
      'already_exists', false,
      'operation_created', false,
      'operation_reused', false,
      'child_operation_id', NULL::text,
      'operation_id', NULL::text,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'pay_batch_id', p_pay_batch_id,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'error', 'ACTOR_USER_ID_REQUIRED_FOR_QUEUE_STAGE',
      'message', 'Queue-stage RPC requires an actor user id and none was supplied or available from the batch creator.'
    );
  END IF;

  IF COALESCE(p_operation_mode, false) THEN
    v_operation_kind := CASE WHEN v_is_loans_batch THEN 'PAYOUT_NOTICE' ELSE 'REMITTANCE' END;
    v_operation_idempotency_key := 'remittance-queue:batch:' || p_pay_batch_id::text
      || ':trigger:' || v_trigger
      || ':scope:' || v_scope
      || ':only-confirmed:' || CASE WHEN COALESCE(p_only_confirmed, false) THEN 'true' ELSE 'false' END
      || ':kind:' || v_operation_kind
      || ':root:' || COALESCE(p_root_operation_id::text, 'none');

    v_operation_input_json := jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'message_kind', v_operation_kind,
      'is_loans_batch', v_is_loans_batch,
      'root_operation_id', CASE WHEN p_root_operation_id IS NULL THEN NULL ELSE p_root_operation_id::text END,
      'source_rpc', 'pay_remittance_maybe_queue_for_trigger'
    );

    WITH operation_config_plan AS (
      SELECT *
      FROM (
        VALUES
          ('queue_remittance_chunks', 'REMITTANCE_QUEUE', 'QUEUE_REMITTANCE_CHUNKS', 'REMITTANCE'),
          ('queue_payout_notice_chunks', 'REMITTANCE_QUEUE', 'QUEUE_PAYOUT_NOTICE_CHUNKS', 'PAYOUT_NOTICE')
      ) AS config_plan(config_key, operation_type, phase, chunk_type)
    ), operation_config_rows AS (
      SELECT
        operation_config_plan.config_key,
        operation_config_plan.operation_type,
        operation_config_plan.phase,
        operation_config_plan.chunk_type,
        operation_config_get.chunk_size,
        operation_config_get.min_chunk_size,
        operation_config_get.max_chunk_size,
        operation_config_get.max_advance_ms,
        operation_config_get.lock_seconds
      FROM operation_config_plan
      CROSS JOIN LATERAL public.banking_pay_operation_config_get(
        p_operation_type => operation_config_plan.operation_type,
        p_phase => operation_config_plan.phase,
        p_chunk_type => operation_config_plan.chunk_type
      ) AS operation_config_get
    )
    SELECT jsonb_build_object(
      'source', 'pay_remittance_maybe_queue_for_trigger',
      'version', 1,
      'operation_type', 'REMITTANCE_QUEUE',
      'snapshotted_at_utc', now()::text,
      'lock_seconds', COALESCE(max(operation_config_rows.lock_seconds), 60),
      'max_advance_ms', COALESCE(max(operation_config_rows.max_advance_ms), 15000),
      'chunks', COALESCE(jsonb_object_agg(
        operation_config_rows.config_key,
        jsonb_build_object(
          'operation_type', operation_config_rows.operation_type,
          'phase', operation_config_rows.phase,
          'chunk_type', operation_config_rows.chunk_type,
          'chunk_size', operation_config_rows.chunk_size,
          'min_chunk_size', operation_config_rows.min_chunk_size,
          'max_chunk_size', operation_config_rows.max_chunk_size,
          'max_advance_ms', operation_config_rows.max_advance_ms,
          'lock_seconds', operation_config_rows.lock_seconds
        )
        ORDER BY operation_config_rows.config_key
      ), '{}'::jsonb)
    )
    INTO v_operation_config_json
    FROM operation_config_rows;

    v_operation_config_json := COALESCE(v_operation_config_json, '{}'::jsonb) || jsonb_build_object(
      'remittance_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,queue_remittance_chunks,chunk_size}', '')::integer, 100),
      'payout_notice_chunk_size', COALESCE(NULLIF(v_operation_config_json #>> '{chunks,queue_payout_notice_chunks,chunk_size}', '')::integer, 100)
    );

    SELECT operation_start_row.*
    INTO v_operation_start
    FROM public.banking_pay_operation_start(
      p_operation_type => 'REMITTANCE_QUEUE',
      p_actor_user_id => v_effective_actor_user_id,
      p_idempotency_key => v_operation_idempotency_key,
      p_workbench_session_id => NULL::uuid,
      p_pay_batch_id => p_pay_batch_id,
      p_root_operation_id => p_root_operation_id,
      p_input_json => v_operation_input_json,
      p_config_json => v_operation_config_json
    ) AS operation_start_row;

    v_operation_config_snapshot_status := CASE
      WHEN v_operation_start.config_json IS NULL OR v_operation_start.config_json = '{}'::jsonb THEN 'repaired'
      WHEN v_operation_start.is_existing THEN 'reused'
      ELSE 'created'
    END;

    IF v_operation_config_snapshot_status = 'repaired' THEN
      UPDATE public.banking_pay_operations AS operation_update
      SET config_json = v_operation_config_json,
          progress_json = COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
            'config_snapshot_status', 'repaired',
            'config_snapshot_repaired_at_utc', now()::text,
            'config_snapshot_source', 'pay_remittance_maybe_queue_for_trigger'
          ),
          updated_at_utc = now()
      WHERE operation_update.id = v_operation_start.operation_id
        AND (operation_update.config_json IS NULL OR operation_update.config_json = '{}'::jsonb);
    END IF;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAY_REMITTANCE_MAYBE_QUEUE_FOR_TRIGGER_OPERATION_STARTED',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'trigger', v_trigger,
        'configured_timing', v_timing_setting,
        'scope', v_scope,
        'only_confirmed', COALESCE(p_only_confirmed, false),
        'is_loans_batch', v_is_loans_batch,
        'message_kind', v_operation_kind,
        'operation_id', v_operation_start.operation_id,
        'operation_status', v_operation_start.status,
        'operation_phase', v_operation_start.phase,
        'operation_config_snapshot_status', v_operation_config_snapshot_status,
        'operation_idempotency_key', v_operation_idempotency_key,
        'already_exists', COALESCE(v_operation_start.is_existing, false),
        'operation_created', COALESCE(v_operation_start.is_existing, false) = false,
        'operation_reused', COALESCE(v_operation_start.is_existing, false) = true,
        'root_operation_id', p_root_operation_id
      ),
      'pay_remittance',
      p_pay_batch_id::text,
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'queued', false,
      'operation_queued', true,
      'deferred', false,
      'suppressed', false,
      'dispatch_required', false,
      'requires_remittance_operation', true,
      'trigger_status', CASE WHEN COALESCE(v_operation_start.is_existing, false) THEN 'REMITTANCE_QUEUE_OPERATION_REUSED' ELSE 'REMITTANCE_QUEUE_OPERATION_STARTED' END,
      'operation_mode', true,
      'operation_idempotency_key', v_operation_idempotency_key,
      'already_exists', COALESCE(v_operation_start.is_existing, false),
      'operation_created', COALESCE(v_operation_start.is_existing, false) = false,
      'operation_reused', COALESCE(v_operation_start.is_existing, false) = true,
      'child_operation_id', v_operation_start.operation_id::text,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'pay_batch_id', p_pay_batch_id,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'is_loans_batch', v_is_loans_batch,
      'message_kind', v_operation_kind,
      'effective_actor_user_id', v_effective_actor_user_id,
      'operation_id', v_operation_start.operation_id::text,
      'operation_type', v_operation_start.operation_type,
      'operation_status', v_operation_start.status,
      'operation_phase', v_operation_start.phase,
      'operation_config_snapshot_status', v_operation_config_snapshot_status,
      'root_operation_id', CASE WHEN p_root_operation_id IS NULL THEN NULL ELSE p_root_operation_id::text END,
      'queue_result', jsonb_build_object(
        'ok', true,
        'operation_mode', true,
        'trigger', v_trigger,
        'configured_timing', v_timing_setting,
        'scope', v_scope,
        'only_confirmed', COALESCE(p_only_confirmed, false),
        'operation_idempotency_key', v_operation_idempotency_key,
        'already_exists', COALESCE(v_operation_start.is_existing, false),
        'operation_created', COALESCE(v_operation_start.is_existing, false) = false,
        'operation_reused', COALESCE(v_operation_start.is_existing, false) = true,
        'child_operation_id', v_operation_start.operation_id::text,
        'operation_id', v_operation_start.operation_id::text,
        'operation_type', v_operation_start.operation_type,
        'status', v_operation_start.status,
        'phase', v_operation_start.phase,
        'operation_config_snapshot_status', v_operation_config_snapshot_status,
        'pay_batch_id', p_pay_batch_id::text,
        'message_kind', v_operation_kind,
        'trigger_status', CASE WHEN COALESCE(v_operation_start.is_existing, false) THEN 'REMITTANCE_QUEUE_OPERATION_REUSED' ELSE 'REMITTANCE_QUEUE_OPERATION_STARTED' END,
        'dispatch_required', false,
        'job_count', 0,
        'jobs', '[]'::jsonb
      )
    );
  END IF;

  SELECT count(*)::integer
  INTO v_batch_candidate_count
  FROM public.pay_batch_candidates AS batch_candidate_count_row
  WHERE batch_candidate_count_row.pay_batch_id = p_pay_batch_id;

  IF v_batch_candidate_count > v_large_batch_threshold THEN
    v_operation_kind := CASE WHEN v_is_loans_batch THEN 'PAYOUT_NOTICE' ELSE 'REMITTANCE' END;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAY_REMITTANCE_MAYBE_QUEUE_FOR_TRIGGER_OPERATION_REQUIRED_FOR_LARGE_BATCH',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'trigger', v_trigger,
        'configured_timing', v_timing_setting,
        'scope', v_scope,
        'only_confirmed', COALESCE(p_only_confirmed, false),
        'is_loans_batch', v_is_loans_batch,
        'message_kind', v_operation_kind,
        'batch_candidate_count', v_batch_candidate_count,
        'large_batch_threshold', v_large_batch_threshold
      ),
      'pay_remittance',
      p_pay_batch_id::text,
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'queued', false,
      'operation_queued', false,
      'deferred', false,
      'suppressed', false,
      'dispatch_required', false,
      'requires_remittance_operation', true,
      'trigger_status', 'REMITTANCE_QUEUE_OPERATION_REQUIRED_FOR_LARGE_BATCH',
      'operation_mode', false,
      'operation_idempotency_key', NULL::text,
      'already_exists', false,
      'operation_created', false,
      'operation_reused', false,
      'child_operation_id', NULL::text,
      'operation_id', NULL::text,
      'operation_status', NULL::text,
      'operation_phase', NULL::text,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'pay_batch_id', p_pay_batch_id,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'is_loans_batch', v_is_loans_batch,
      'message_kind', v_operation_kind,
      'effective_actor_user_id', v_effective_actor_user_id,
      'batch_candidate_count', v_batch_candidate_count,
      'large_batch_threshold', v_large_batch_threshold,
      'message', 'Large payment batches must queue remittances through a REMITTANCE_QUEUE operation.',
      'queue_result', jsonb_build_object(
        'ok', true,
        'operation_required', true,
        'operation_mode', false,
        'operation_idempotency_key', NULL::text,
        'already_exists', false,
        'operation_created', false,
        'operation_reused', false,
        'child_operation_id', NULL::text,
        'trigger_status', 'REMITTANCE_QUEUE_OPERATION_REQUIRED_FOR_LARGE_BATCH',
        'message_kind', v_operation_kind,
        'dispatch_required', false,
        'job_count', 0
      )
    );
  END IF;

  v_has_remittance_operation_overload := to_regprocedure('public.pay_remittance_queue_commit_stage(uuid,text,uuid,boolean,uuid,jsonb)') IS NOT NULL;
  v_has_finance_operation_overload := to_regprocedure('public.pay_finance_payout_notice_queue_commit_stage(uuid,uuid,boolean,uuid,jsonb)') IS NOT NULL;
  v_has_remittance_confirmed_overload := to_regprocedure('public.pay_remittance_queue_commit_stage(uuid,text,uuid,boolean)') IS NOT NULL;
  v_has_finance_confirmed_overload := to_regprocedure('public.pay_finance_payout_notice_queue_commit_stage(uuid,uuid,boolean)') IS NOT NULL;
  v_has_remittance_legacy_function := to_regprocedure('public.pay_remittance_queue_commit_stage(uuid,text,uuid)') IS NOT NULL;
  v_has_finance_legacy_function := to_regprocedure('public.pay_finance_payout_notice_queue_commit_stage(uuid,uuid)') IS NOT NULL;

  IF v_is_loans_batch THEN
    IF v_has_finance_operation_overload THEN
      EXECUTE 'SELECT public.pay_finance_payout_notice_queue_commit_stage($1, $2, $3, NULL::uuid, NULL::jsonb)'
      INTO v_queue_result
      USING p_pay_batch_id, v_effective_actor_user_id, COALESCE(p_only_confirmed, false);
    ELSIF v_has_finance_confirmed_overload THEN
      EXECUTE 'SELECT public.pay_finance_payout_notice_queue_commit_stage($1, $2, $3)'
      INTO v_queue_result
      USING p_pay_batch_id, v_effective_actor_user_id, COALESCE(p_only_confirmed, false);
    ELSIF COALESCE(p_only_confirmed, false) THEN
      v_queue_result := jsonb_build_object(
        'ok', true,
        'trigger_status', 'CONFIRMED_ONLY_QUEUE_STAGE_OVERLOAD_NOT_INSTALLED',
        'message_kind', 'PAYOUT_NOTICE',
        'automatic_commit_stage', true,
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id,
        'job_count', 0,
        'jobs', '[]'::jsonb,
        'message', 'Confirmed-only finance payout notice queue-stage overload is not installed yet.'
      );
    ELSIF v_has_finance_legacy_function THEN
      EXECUTE 'SELECT public.pay_finance_payout_notice_queue_commit_stage($1, $2)'
      INTO v_queue_result
      USING p_pay_batch_id, v_effective_actor_user_id;
    ELSE
      v_queue_result := jsonb_build_object(
        'ok', false,
        'trigger_status', 'PAYOUT_NOTICE_QUEUE_STAGE_FUNCTION_MISSING',
        'message_kind', 'PAYOUT_NOTICE',
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id,
        'job_count', 0,
        'jobs', '[]'::jsonb,
        'error', 'pay_finance_payout_notice_queue_commit_stage function not found'
      );
    END IF;
  ELSE
    IF v_has_remittance_operation_overload THEN
      EXECUTE 'SELECT public.pay_remittance_queue_commit_stage($1, $2, $3, $4, NULL::uuid, NULL::jsonb)'
      INTO v_queue_result
      USING p_pay_batch_id, v_scope, v_effective_actor_user_id, COALESCE(p_only_confirmed, false);
    ELSIF v_has_remittance_confirmed_overload THEN
      EXECUTE 'SELECT public.pay_remittance_queue_commit_stage($1, $2, $3, $4)'
      INTO v_queue_result
      USING p_pay_batch_id, v_scope, v_effective_actor_user_id, COALESCE(p_only_confirmed, false);
    ELSIF COALESCE(p_only_confirmed, false) THEN
      v_queue_result := jsonb_build_object(
        'ok', true,
        'trigger_status', 'CONFIRMED_ONLY_QUEUE_STAGE_OVERLOAD_NOT_INSTALLED',
        'message_kind', 'REMITTANCE',
        'automatic_commit_stage', true,
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id,
        'scope', v_scope,
        'job_count', 0,
        'jobs', '[]'::jsonb,
        'message', 'Confirmed-only remittance queue-stage overload is not installed yet.'
      );
    ELSIF v_has_remittance_legacy_function THEN
      EXECUTE 'SELECT public.pay_remittance_queue_commit_stage($1, $2, $3)'
      INTO v_queue_result
      USING p_pay_batch_id, v_scope, v_effective_actor_user_id;
    ELSE
      v_queue_result := jsonb_build_object(
        'ok', false,
        'trigger_status', 'REMITTANCE_QUEUE_STAGE_FUNCTION_MISSING',
        'message_kind', 'REMITTANCE',
        'dispatch_required', false,
        'pay_batch_id', p_pay_batch_id,
        'scope', v_scope,
        'job_count', 0,
        'jobs', '[]'::jsonb,
        'error', 'pay_remittance_queue_commit_stage function not found'
      );
    END IF;
  END IF;

  v_dispatch_required := COALESCE((v_queue_result->>'dispatch_required')::boolean, false);
  v_job_count := COALESCE((v_queue_result->>'job_count')::integer, 0);

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAY_REMITTANCE_MAYBE_QUEUE_FOR_TRIGGER_RESULT',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id,
      'trigger', v_trigger,
      'configured_timing', v_timing_setting,
      'scope', v_scope,
      'only_confirmed', COALESCE(p_only_confirmed, false),
      'is_loans_batch', v_is_loans_batch,
      'dispatch_required', v_dispatch_required,
      'job_count', v_job_count,
      'queue_result_status', v_queue_result->>'trigger_status'
    ),
    'pay_remittance',
    p_pay_batch_id::text,
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', COALESCE((v_queue_result->>'ok')::boolean, true),
    'queued', v_dispatch_required,
    'deferred', false,
    'suppressed', false,
    'dispatch_required', v_dispatch_required,
    'requires_remittance_operation', false,
    'trigger_status', COALESCE(NULLIF(btrim(COALESCE(v_queue_result->>'trigger_status', '')), ''), 'REMITTANCE_QUEUE_STAGE_PROCESSED'),
    'operation_mode', false,
    'operation_idempotency_key', NULL::text,
    'already_exists', false,
    'operation_created', false,
    'operation_reused', false,
    'child_operation_id', NULL::text,
    'operation_id', NULL::text,
    'trigger', v_trigger,
    'configured_timing', v_timing_setting,
    'pay_batch_id', p_pay_batch_id,
    'scope', v_scope,
    'only_confirmed', COALESCE(p_only_confirmed, false),
    'is_loans_batch', v_is_loans_batch,
    'effective_actor_user_id', v_effective_actor_user_id,
    'queue_result', COALESCE(v_queue_result, '{}'::jsonb)
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAY_REMITTANCE_MAYBE_QUEUE_FOR_TRIGGER_ERROR',
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id,
        'trigger', p_trigger,
        'scope', p_scope,
        'only_confirmed', COALESCE(p_only_confirmed, false),
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_remittance',
      COALESCE(p_pay_batch_id::text, 'NO_BATCH_ID'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;







DROP FUNCTION IF EXISTS public.pay_workbench_enqueue_candidate_refresh_many(uuid, jsonb, text, uuid);



CREATE OR REPLACE FUNCTION public.pay_workbench_enqueue_candidate_refresh_many(
  p_session_id uuid,
  p_candidate_ids jsonb,
  p_reason text DEFAULT NULL::text,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_candidate_ids uuid[] := ARRAY[]::uuid[];
  v_requested_count integer := 0;
  v_scope_count integer := 0;
  v_queued_count integer := 0;
  v_reused_count integer := 0;
  v_candidate_ids_jsonb jsonb := '[]'::jsonb;
  v_job_ids_jsonb jsonb := '[]'::jsonb;
  v_projection_contract_json jsonb := '{}'::jsonb;
  v_required_projection_version integer := 0;
  v_required_hidden_recovery_template_projection_version integer := 0;
  v_requires_hidden_recovery_templates boolean := false;
  v_reason text := COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'BULK_SESSION_CANDIDATE_RECOMPUTE');
  v_reason_upper text := UPPER(COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'BULK_SESSION_CANDIDATE_RECOMPUTE'));
  v_is_batch_mutation boolean := false;
  v_candidate_id uuid := NULL::uuid;
  v_snapshot_refresh jsonb := '{}'::jsonb;
  v_snapshot_refresh_job_ids_jsonb jsonb := '[]'::jsonb;
  v_session_recompute_job_ids_jsonb jsonb := '[]'::jsonb;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  IF p_candidate_ids IS NULL OR jsonb_typeof(p_candidate_ids) <> 'array' OR jsonb_array_length(p_candidate_ids) = 0 THEN
    RAISE EXCEPTION 'p_candidate_ids must be a non-empty JSON array';
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', p_session_id;
  END IF;

  IF v_session_row.status <> 'OPEN' OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'banking_pay_workbench_session % is not open', p_session_id;
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'tms_users row % not found', p_actor_user_id;
  END IF;

  SELECT COALESCE(array_agg(candidate_input.candidate_id ORDER BY candidate_input.candidate_id), ARRAY[]::uuid[]),
         count(*)::integer
  INTO v_candidate_ids, v_requested_count
  FROM (
    SELECT DISTINCT (candidate_element.value #>> '{}')::uuid AS candidate_id
    FROM jsonb_array_elements(p_candidate_ids) AS candidate_element(value)
    WHERE (candidate_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS candidate_input;

  IF COALESCE(v_requested_count, 0) <> jsonb_array_length(p_candidate_ids) THEN
    RAISE EXCEPTION 'p_candidate_ids contains invalid candidate id values';
  END IF;

  SELECT count(*)::integer
  INTO v_scope_count
  FROM unnest(v_candidate_ids) AS requested_candidate(candidate_id)
  WHERE requested_candidate.candidate_id = ANY(COALESCE(v_session_row.scope_candidate_ids, ARRAY[]::uuid[]));

  IF v_scope_count <> v_requested_count THEN
    RAISE EXCEPTION 'one or more candidate ids do not belong to session % scope', p_session_id;
  END IF;

  v_is_batch_mutation := v_reason_upper IN ('DRAFT_CREATED', 'DRAFT_CANCELLED', 'PAY_BATCH_CANCELLED', 'BATCH_MUTATION');

  IF v_is_batch_mutation THEN
    FOREACH v_candidate_id IN ARRAY v_candidate_ids
    LOOP
      PERFORM public._change_bump('pay_candidate:' || v_candidate_id::text);

      IF v_session_row.source_snapshot_run_id IS NOT NULL THEN
        v_snapshot_refresh := public.pay_workbench_enqueue_candidate_refresh(
          p_snapshot_run_id => v_session_row.source_snapshot_run_id,
          p_candidate_id => v_candidate_id,
          p_reason => v_reason,
          p_actor_user_id => p_actor_user_id,
          p_payload_json => jsonb_build_object(
            'source_session_id', p_session_id::text,
            'source_session_signature', v_session_row.session_signature,
            'source_session_version', COALESCE(v_session_row.version, 0),
            'mutation_reason', v_reason,
            'refresh_reason', v_reason,
            'requires_new_session', true,
            'source_session_discard_required', true
          )
        );

        IF NULLIF(BTRIM(COALESCE(v_snapshot_refresh->>'job_id', '')), '') IS NOT NULL THEN
          v_snapshot_refresh_job_ids_jsonb := v_snapshot_refresh_job_ids_jsonb || to_jsonb(v_snapshot_refresh->>'job_id');
        END IF;
      END IF;
    END LOOP;

    SELECT COALESCE(jsonb_agg(to_jsonb(candidate_id_text.candidate_id) ORDER BY candidate_id_text.candidate_id), '[]'::jsonb)
    INTO v_candidate_ids_jsonb
    FROM (
      SELECT unnest(v_candidate_ids)::text AS candidate_id
    ) AS candidate_id_text;

    UPDATE public.banking_pay_workbench_session_candidate_state AS state_row
    SET status = 'PENDING',
        effective_candidate_fragment_json = '{}'::jsonb,
        effective_summary_fragment_json = '{}'::jsonb,
        effective_paye_candidate_json = NULL::jsonb,
        effective_non_paye_payee_json = NULL::jsonb,
        effective_payees_json = '[]'::jsonb,
        effective_case_resolution_states_json = '[]'::jsonb,
        effective_canonical_preview_lines_json = '[]'::jsonb,
        updated_at_utc = v_now,
        last_recomputed_at_utc = NULL::timestamptz,
        last_error_json = NULL::jsonb
    WHERE state_row.session_id = p_session_id
      AND state_row.candidate_id = ANY(v_candidate_ids);

    RETURN jsonb_build_object(
      'ok', true,
      'session_id', p_session_id::text,
      'source_session_id', p_session_id::text,
      'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
      'source_snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
      'reason', v_reason,
      'mutation_reason', v_reason,
      'requires_new_session', true,
      'source_session_discard_required', true,
      'dirty_candidate_ids', COALESCE(v_candidate_ids_jsonb, '[]'::jsonb),
      'candidate_ids', COALESCE(v_candidate_ids_jsonb, '[]'::jsonb),
      'candidate_count', COALESCE(array_length(v_candidate_ids, 1), 0),
      'snapshot_refresh_job_ids', COALESCE(v_snapshot_refresh_job_ids_jsonb, '[]'::jsonb),
      'refresh_job_ids', COALESCE(v_snapshot_refresh_job_ids_jsonb, '[]'::jsonb),
      'job_ids', COALESCE(v_snapshot_refresh_job_ids_jsonb, '[]'::jsonb),
      'session_recompute_job_ids', COALESCE(v_session_recompute_job_ids_jsonb, '[]'::jsonb),
      'queued_count', COALESCE(jsonb_array_length(v_snapshot_refresh_job_ids_jsonb), 0),
      'reused_count', 0
    );
  END IF;

  v_projection_contract_json := public._pay_workbench_candidate_projection_contract();
  v_required_projection_version := COALESCE(
    CASE
      WHEN COALESCE(v_projection_contract_json->>'projection_version', '') ~ '^[0-9]+$'
        THEN (v_projection_contract_json->>'projection_version')::integer
      ELSE NULL::integer
    END,
    0
  );
  v_required_hidden_recovery_template_projection_version := COALESCE(
    CASE
      WHEN COALESCE(v_projection_contract_json->>'hidden_recovery_template_projection_version', '') ~ '^[0-9]+$'
        THEN (v_projection_contract_json->>'hidden_recovery_template_projection_version')::integer
      ELSE NULL::integer
    END,
    0
  );
  v_requires_hidden_recovery_templates := COALESCE(
    CASE
      WHEN lower(COALESCE(v_projection_contract_json->>'requires_hidden_recovery_templates', '')) IN ('true', 'false')
        THEN (v_projection_contract_json->>'requires_hidden_recovery_templates')::boolean
      ELSE NULL::boolean
    END,
    false
  );

  WITH candidate_scope AS (
    SELECT requested_candidate.candidate_id,
           COALESCE(change_counter.seq, 0) AS source_change_seq
    FROM unnest(v_candidate_ids) AS requested_candidate(candidate_id)
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key = 'pay_candidate:' || requested_candidate.candidate_id::text
  ),
  job_source AS (
    SELECT
      candidate_scope.candidate_id,
      candidate_scope.source_change_seq,
      'SESSION_CANDIDATE_RECOMPUTE:'
        || p_session_id::text
        || ':' || candidate_scope.candidate_id::text
        || ':v' || COALESCE(v_session_row.version, 0)::text
        || ':s' || candidate_scope.source_change_seq::text
        || ':pv' || v_required_projection_version::text
        || ':ht' || v_required_hidden_recovery_template_projection_version::text AS dedupe_key,
      jsonb_build_object(
        'reason', v_reason,
        'actor_user_id', p_actor_user_id::text,
        'session_id', p_session_id::text,
        'session_signature', v_session_row.session_signature,
        'session_version', COALESCE(v_session_row.version, 0),
        'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
        'candidate_id', candidate_scope.candidate_id::text,
        'source_change_seq', candidate_scope.source_change_seq,
        'projection_version', v_required_projection_version,
        'hidden_recovery_template_projection_version', v_required_hidden_recovery_template_projection_version,
        'requires_hidden_recovery_templates', v_requires_hidden_recovery_templates,
        'refresh_reason', v_reason,
        'job_type', 'SESSION_CANDIDATE_RECOMPUTE'
      ) AS payload_json
    FROM candidate_scope
  ),
  upserted_jobs AS (
    INSERT INTO public.banking_pay_workbench_jobs (
      job_type,
      status,
      priority,
      run_at_utc,
      attempt_count,
      max_attempts,
      dedupe_key,
      snapshot_run_id,
      session_id,
      candidate_id,
      payload_json,
      created_at_utc,
      updated_at_utc,
      started_at_utc,
      completed_at_utc,
      failed_at_utc,
      last_error_json
    )
    SELECT
      'SESSION_CANDIDATE_RECOMPUTE',
      'QUEUED',
      45,
      v_now,
      0,
      8,
      job_source.dedupe_key,
      v_session_row.source_snapshot_run_id,
      p_session_id,
      job_source.candidate_id,
      job_source.payload_json,
      v_now,
      v_now,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::jsonb
    FROM job_source
    ON CONFLICT (dedupe_key) WHERE status IN ('QUEUED', 'RUNNING')
    DO UPDATE
    SET priority = LEAST(public.banking_pay_workbench_jobs.priority, EXCLUDED.priority),
        run_at_utc = LEAST(public.banking_pay_workbench_jobs.run_at_utc, EXCLUDED.run_at_utc),
        payload_json = public._pay_workbench_merge_targeted_scope_payload(
          COALESCE(public.banking_pay_workbench_jobs.payload_json, '{}'::jsonb),
          COALESCE(EXCLUDED.payload_json, '{}'::jsonb)
        ),
        updated_at_utc = v_now
    RETURNING
      public.banking_pay_workbench_jobs.id,
      public.banking_pay_workbench_jobs.candidate_id,
      (xmax = 0) AS was_inserted
  ),
  state_upsert AS (
    INSERT INTO public.banking_pay_workbench_session_candidate_state (
      session_id,
      candidate_id,
      status,
      effective_candidate_fragment_json,
      effective_summary_fragment_json,
      effective_paye_candidate_json,
      effective_non_paye_payee_json,
      effective_payees_json,
      effective_case_resolution_states_json,
      effective_canonical_preview_lines_json,
      source_change_seq,
      session_version,
      pending_job_id,
      created_at_utc,
      updated_at_utc,
      last_recomputed_at_utc,
      last_error_json
    )
    SELECT
      p_session_id,
      job_source.candidate_id,
      'PENDING',
      '{}'::jsonb,
      '{}'::jsonb,
      NULL::jsonb,
      NULL::jsonb,
      '[]'::jsonb,
      '[]'::jsonb,
      '[]'::jsonb,
      job_source.source_change_seq,
      COALESCE(v_session_row.version, 0),
      upserted_jobs.id,
      v_now,
      v_now,
      NULL::timestamptz,
      NULL::jsonb
    FROM job_source
    JOIN upserted_jobs
      ON upserted_jobs.candidate_id = job_source.candidate_id
    ON CONFLICT (session_id, candidate_id)
    DO UPDATE
    SET status = 'PENDING',
        effective_candidate_fragment_json = '{}'::jsonb,
        effective_summary_fragment_json = '{}'::jsonb,
        effective_paye_candidate_json = NULL::jsonb,
        effective_non_paye_payee_json = NULL::jsonb,
        effective_payees_json = '[]'::jsonb,
        effective_case_resolution_states_json = '[]'::jsonb,
        effective_canonical_preview_lines_json = '[]'::jsonb,
        source_change_seq = GREATEST(public.banking_pay_workbench_session_candidate_state.source_change_seq, EXCLUDED.source_change_seq),
        session_version = GREATEST(public.banking_pay_workbench_session_candidate_state.session_version, EXCLUDED.session_version),
        pending_job_id = EXCLUDED.pending_job_id,
        updated_at_utc = v_now,
        last_recomputed_at_utc = NULL::timestamptz,
        last_error_json = NULL::jsonb
    RETURNING public.banking_pay_workbench_session_candidate_state.candidate_id
  )
  SELECT
    count(*) FILTER (WHERE upserted_jobs.was_inserted)::integer,
    count(*) FILTER (WHERE upserted_jobs.was_inserted IS NOT TRUE)::integer,
    COALESCE(jsonb_agg(to_jsonb(upserted_jobs.candidate_id::text) ORDER BY upserted_jobs.candidate_id::text), '[]'::jsonb),
    COALESCE(jsonb_agg(to_jsonb(upserted_jobs.id::text) ORDER BY upserted_jobs.id::text), '[]'::jsonb)
  INTO
    v_queued_count,
    v_reused_count,
    v_candidate_ids_jsonb,
    v_job_ids_jsonb
  FROM upserted_jobs;

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END,
    'reason', p_reason,
    'queued_count', COALESCE(v_queued_count, 0),
    'reused_count', COALESCE(v_reused_count, 0),
    'candidate_ids', COALESCE(v_candidate_ids_jsonb, '[]'::jsonb),
    'job_ids', COALESCE(v_job_ids_jsonb, '[]'::jsonb),
    'session_recompute_job_ids', COALESCE(v_job_ids_jsonb, '[]'::jsonb),
    'requires_new_session', false,
    'source_session_discard_required', false
  );
END;
$function$;








DROP FUNCTION IF EXISTS public.pay_workbench_session_get_preview_page(uuid, text, jsonb, integer);



CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_preview_page(
  p_session_id uuid,
  p_section text,
  p_cursor_json jsonb DEFAULT '{}'::jsonb,
  p_limit integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_section text := lower(btrim(coalesce(p_section, '')));
  v_cursor_json jsonb := CASE WHEN jsonb_typeof(COALESCE(p_cursor_json, '{}'::jsonb)) = 'object' THEN COALESCE(p_cursor_json, '{}'::jsonb) ELSE '{}'::jsonb END;
  v_limit integer := 100;
  v_items jsonb := '[]'::jsonb;
  v_known_count integer := NULL;
  v_returned_count integer := 0;
  v_page_row_count integer := 0;
  v_next_cursor jsonb := NULL::jsonb;
  v_cursor_candidate_id uuid := NULL::uuid;
  v_cursor_item_order integer := 0;
  v_cursor_ordinality bigint := 0;
  v_last_candidate_id uuid := NULL::uuid;
  v_last_item_order integer := 0;
  v_last_ordinality bigint := 0;
  v_has_more boolean := false;
  v_state_row record;
  v_candidate_item_json jsonb := '{}'::jsonb;
  v_candidate_items jsonb := '[]'::jsonb;
  v_item_record record;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'session_id is required';
  END IF;

  IF v_section NOT IN ('candidates', 'canonical_preview_lines', 'itemisation', 'blocked_items', 'do_not_pay_items', 'snoozed_items', 'baseline_component_rows') THEN
    RAISE EXCEPTION 'unsupported preview page section: %', p_section;
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'WORKBENCH_SESSION_NOT_FOUND',
      'code', 'WORKBENCH_SESSION_NOT_FOUND',
      'rebase_required', true,
      'requires_new_session', true,
      'session_id', p_session_id::text,
      'section', v_section,
      'items', '[]'::jsonb,
      'rows', '[]'::jsonb,
      'next_cursor', NULL::jsonb,
      'returned_count', 0,
      'ready', false,
      'ready_flag', false,
      'phase', 'REBASE_REQUIRED',
      'status_text', 'Payment preview needs refreshing.',
      'message', 'Payment preview needs refreshing.'
    );
  END IF;

  IF UPPER(COALESCE(v_session_row.status, '')) <> 'OPEN' OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', 'OBSOLETE_SESSION',
      'code', 'OBSOLETE_SESSION',
      'rebase_required', true,
      'requires_new_session', true,
      'session_id', p_session_id::text,
      'section', v_section,
      'status', v_session_row.status,
      'session_status', v_session_row.status,
      'discarded_at_utc', v_session_row.discarded_at_utc,
      'items', '[]'::jsonb,
      'rows', '[]'::jsonb,
      'next_cursor', NULL::jsonb,
      'returned_count', 0,
      'ready', false,
      'ready_flag', false,
      'phase', 'REBASE_REQUIRED',
      'status_text', 'Payment preview needs refreshing.',
      'message', 'Payment preview needs refreshing.'
    );
  END IF;

  v_limit := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 500);

  IF COALESCE(v_cursor_json->>'last_candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_cursor_candidate_id := (v_cursor_json->>'last_candidate_id')::uuid;
  ELSIF COALESCE(v_cursor_json->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_cursor_candidate_id := (v_cursor_json->>'candidate_id')::uuid;
  END IF;

  IF COALESCE(v_cursor_json->>'last_item_order', '') ~ '^[0-9]+$' THEN
    v_cursor_item_order := (v_cursor_json->>'last_item_order')::integer;
  ELSIF COALESCE(v_cursor_json->>'item_order', '') ~ '^[0-9]+$' THEN
    v_cursor_item_order := (v_cursor_json->>'item_order')::integer;
  END IF;

  IF COALESCE(v_cursor_json->>'last_ordinality', '') ~ '^[0-9]+$' THEN
    v_cursor_ordinality := (v_cursor_json->>'last_ordinality')::bigint;
  ELSIF COALESCE(v_cursor_json->>'ordinality', '') ~ '^[0-9]+$' THEN
    v_cursor_ordinality := (v_cursor_json->>'ordinality')::bigint;
  END IF;

  FOR v_state_row IN
    SELECT state_row.candidate_id,
           state_row.effective_paye_candidate_json,
           state_row.effective_non_paye_payee_json,
           state_row.effective_candidate_fragment_json,
           state_row.effective_canonical_preview_lines_json
    FROM public.banking_pay_workbench_session_candidate_state AS state_row
    WHERE state_row.session_id = p_session_id
      AND state_row.status = 'READY'
      AND (v_cursor_candidate_id IS NULL OR state_row.candidate_id >= v_cursor_candidate_id)
    ORDER BY state_row.candidate_id ASC
  LOOP
    IF v_section = 'candidates' THEN
      IF jsonb_typeof(v_state_row.effective_paye_candidate_json) = 'object'
         AND (
           v_cursor_candidate_id IS NULL
           OR v_state_row.candidate_id > v_cursor_candidate_id
           OR (v_state_row.candidate_id = v_cursor_candidate_id AND 1 > v_cursor_item_order)
         ) THEN
        v_candidate_item_json := jsonb_strip_nulls(
          COALESCE(v_state_row.effective_paye_candidate_json, '{}'::jsonb)
          || jsonb_build_object('candidate_id', v_state_row.candidate_id::text, 'preview_section', 'paye_candidates')
        );

        v_page_row_count := v_page_row_count + 1;
        IF v_page_row_count <= v_limit THEN
          v_items := COALESCE(v_items, '[]'::jsonb) || jsonb_build_array(v_candidate_item_json);
          v_returned_count := v_returned_count + 1;
          v_last_candidate_id := v_state_row.candidate_id;
          v_last_item_order := 1;
          v_last_ordinality := 0;
        ELSE
          v_has_more := true;
          EXIT;
        END IF;
      END IF;

      IF jsonb_typeof(v_state_row.effective_non_paye_payee_json) = 'object'
         AND (
           v_cursor_candidate_id IS NULL
           OR v_state_row.candidate_id > v_cursor_candidate_id
           OR (v_state_row.candidate_id = v_cursor_candidate_id AND 2 > v_cursor_item_order)
         ) THEN
        v_candidate_item_json := jsonb_strip_nulls(
          COALESCE(v_state_row.effective_non_paye_payee_json, '{}'::jsonb)
          || jsonb_build_object('candidate_id', v_state_row.candidate_id::text, 'preview_section', 'non_paye_payees')
        );

        v_page_row_count := v_page_row_count + 1;
        IF v_page_row_count <= v_limit THEN
          v_items := COALESCE(v_items, '[]'::jsonb) || jsonb_build_array(v_candidate_item_json);
          v_returned_count := v_returned_count + 1;
          v_last_candidate_id := v_state_row.candidate_id;
          v_last_item_order := 2;
          v_last_ordinality := 0;
        ELSE
          v_has_more := true;
          EXIT;
        END IF;
      END IF;
    ELSE
      v_candidate_items := CASE
        WHEN v_section = 'canonical_preview_lines' AND jsonb_typeof(COALESCE(v_state_row.effective_canonical_preview_lines_json, '[]'::jsonb)) = 'array'
          THEN COALESCE(v_state_row.effective_canonical_preview_lines_json, '[]'::jsonb)
        WHEN v_section = 'itemisation' AND jsonb_typeof(COALESCE(v_state_row.effective_candidate_fragment_json->'itemisation', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_state_row.effective_candidate_fragment_json->'itemisation', '[]'::jsonb)
        WHEN v_section = 'blocked_items' AND jsonb_typeof(COALESCE(v_state_row.effective_candidate_fragment_json->'blocked_items', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_state_row.effective_candidate_fragment_json->'blocked_items', '[]'::jsonb)
        WHEN v_section = 'do_not_pay_items' AND jsonb_typeof(COALESCE(v_state_row.effective_candidate_fragment_json->'do_not_pay_items', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_state_row.effective_candidate_fragment_json->'do_not_pay_items', '[]'::jsonb)
        WHEN v_section = 'snoozed_items' AND jsonb_typeof(COALESCE(v_state_row.effective_candidate_fragment_json->'snoozed_items', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_state_row.effective_candidate_fragment_json->'snoozed_items', '[]'::jsonb)
        WHEN v_section = 'baseline_component_rows' AND jsonb_typeof(COALESCE(v_state_row.effective_candidate_fragment_json->'baseline_component_rows', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_state_row.effective_candidate_fragment_json->'baseline_component_rows', '[]'::jsonb)
        ELSE '[]'::jsonb
      END;

      IF jsonb_typeof(v_candidate_items) = 'array' THEN
        FOR v_item_record IN
          SELECT item_element.value AS item_json,
                 item_element.ordinality::bigint AS ordinality
          FROM jsonb_array_elements(v_candidate_items) WITH ORDINALITY AS item_element(value, ordinality)
          WHERE jsonb_typeof(item_element.value) = 'object'
            AND (
              v_cursor_candidate_id IS NULL
              OR v_state_row.candidate_id > v_cursor_candidate_id
              OR (v_state_row.candidate_id = v_cursor_candidate_id AND item_element.ordinality::bigint > v_cursor_ordinality)
            )
          ORDER BY item_element.ordinality ASC
        LOOP
          v_page_row_count := v_page_row_count + 1;
          IF v_page_row_count <= v_limit THEN
            v_items := COALESCE(v_items, '[]'::jsonb) || jsonb_build_array(v_item_record.item_json);
            v_returned_count := v_returned_count + 1;
            v_last_candidate_id := v_state_row.candidate_id;
            v_last_item_order := 0;
            v_last_ordinality := v_item_record.ordinality;
          ELSE
            v_has_more := true;
            EXIT;
          END IF;
        END LOOP;
      END IF;
    END IF;

    IF v_has_more THEN
      EXIT;
    END IF;
  END LOOP;

  IF v_has_more AND v_last_candidate_id IS NOT NULL THEN
    IF v_section = 'candidates' THEN
      v_next_cursor := jsonb_build_object(
        'last_candidate_id', v_last_candidate_id::text,
        'last_item_order', COALESCE(v_last_item_order, 0)
      );
    ELSE
      v_next_cursor := jsonb_build_object(
        'last_candidate_id', v_last_candidate_id::text,
        'last_ordinality', COALESCE(v_last_ordinality, 0)
      );
    END IF;
  ELSE
    v_next_cursor := NULL::jsonb;
  END IF;

  RETURN jsonb_build_object(
    'ok', TRUE,
    'session_id', p_session_id::text,
    'section', v_section,
    'items', COALESCE(v_items, '[]'::jsonb),
    'known_count', v_known_count,
    'returned_count', COALESCE(v_returned_count, 0),
    'next_cursor', v_next_cursor,
    'session_version', v_session_row.version,
    'session_signature', v_session_row.session_signature,
    'ready', TRUE,
    'paging_mode', 'plpgsql_keyset_limit_plus_one'
  );
END;
$function$;



DROP FUNCTION IF EXISTS public.pay_batch_execution_summary_get(uuid, uuid);




CREATE OR REPLACE FUNCTION public.pay_batch_execution_summary_get(
  p_pay_batch_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_batch_row public.pay_batches%ROWTYPE;
  v_candidate_count integer := 0;
  v_item_count integer := 0;
  v_active_payment_count integer := 0;
  v_total_amount numeric := 0;
  v_transfer_count integer := 0;
  v_prepared_transfer_count integer := 0;
  v_provider_submitted_transfer_count integer := 0;
  v_local_only_transfer_count integer := 0;
  v_pending_transfer_count integer := 0;
  v_evidence_pending_transfer_count integer := 0;
  v_authorisation_ready_transfer_count integer := 0;
  v_unattempted_submit_eligible_transfer_count integer := 0;
  v_provider_submit_ready_transfer_count integer := 0;
  v_authorised_without_provider_submission_transfer_count integer := 0;
  v_authorised_but_not_submit_ready_transfer_count integer := 0;
  v_remaining_provider_submit_ready_transfer_count integer := 0;
  v_provider_submit_phase_active boolean := false;
  v_authorisation_phase_active boolean := false;
  v_remaining_submit_attempt_required_phase text := NULL::text;
  v_safe_local_cleanup_transfer_count integer := 0;
  v_provider_attempt_or_evidence_transfer_count integer := 0;
  v_provider_or_ambiguous_evidence_transfer_count integer := 0;
  v_canonical_pending_status_transfer_count integer := 0;
  v_failed_transfer_count integer := 0;
  v_ambiguous_transfer_count integer := 0;
  v_attempted_but_unproven_transfer_count integer := 0;
  v_provider_attempt_without_external_id_count integer := 0;
  v_requires_provider_poll_count integer := 0;
  v_remaining_submit_attempt_required integer := 0;
  v_remaining_unattempted_submit_required integer := 0;
  v_remaining_evidence_unresolved_count integer := 0;
  v_remaining_provider_evidence_required integer := 0;
  v_remaining_provider_submission_required integer := 0;
  v_blocked_transfer_count integer := 0;
  v_settled_candidate_count integer := 0;
  v_remittance_sent_count integer := 0;
  v_active_operation_id uuid := NULL::uuid;
  v_active_operation_type text := NULL;
  v_active_operation_status text := NULL;
  v_submission_evidence_json jsonb := '{}'::jsonb;
  v_freshness_result_json jsonb := '{}'::jsonb;
  v_freshness_is_stale boolean := false;
  v_active_operation_pay_channel_scope text := 'ALL';
  v_operation_scope_count integer := 0;
  v_operation_scope_prepared_count integer := 0;
  v_operation_scope_pending_count integer := 0;
  v_operation_scope_failed_count integer := 0;
  v_operation_scope_skipped_count integer := 0;
  v_operation_scope_without_transfer_count integer := 0;
  v_operation_scope_authorisation_ready_count integer := 0;
  v_all_operation_scopes_authorisation_ready boolean := false;
  v_cancellable_local_auth_request_count integer := 0;
  v_non_cancellable_auth_request_count integer := 0;
  v_auth_request_retry_blocker_count integer := 0;
  v_provider_submit_diagnostic jsonb := '{}'::jsonb;
  v_provider_submission_status text := NULL::text;
  v_provider_submit_review_reason_code text := NULL::text;
  v_provider_acceptance_evidence_count integer := 0;
  v_provider_response_present_count integer := 0;
  v_provider_request_sent_count integer := 0;
  v_provider_submission_unknown_count integer := 0;
  v_stale_unresolved_submit_chunk_count integer := 0;
  v_unfinalised_submit_chunk_count integer := 0;
  v_provider_manual_resolution_required boolean := false;
  v_provider_safe_retry_available boolean := false;
  v_provider_recommended_action text := NULL::text;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_actor_user_id IS NOT NULL THEN
    PERFORM 1
    FROM public.tms_users AS actor_user
    WHERE actor_user.id = p_actor_user_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'tms_users row % not found', p_actor_user_id;
    END IF;
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batches row % not found', p_pay_batch_id;
  END IF;

  v_freshness_result_json := coalesce(v_batch_row.freshness_result_json, '{}'::jsonb);
  v_freshness_is_stale := upper(coalesce(v_batch_row.freshness_validation_status, '')) = 'STALE'
    OR lower(btrim(coalesce(v_freshness_result_json->>'is_stale', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT count(*)::integer,
         count(*) FILTER (WHERE upper(coalesce(batch_candidate.settlement_status, '')) IN ('SETTLED', 'PAID', 'CONFIRMED'))::integer,
         count(*) FILTER (WHERE batch_candidate.remittance_sent_at_utc IS NOT NULL)::integer
  INTO v_candidate_count, v_settled_candidate_count, v_remittance_sent_count
  FROM public.pay_batch_candidates AS batch_candidate
  WHERE batch_candidate.pay_batch_id = p_pay_batch_id;

  SELECT count(*)::integer,
         count(*) FILTER (
           WHERE coalesce(batch_item.is_voided, false) = false
             AND coalesce(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0) <> 0
         )::integer,
         round(coalesce(sum(CASE WHEN coalesce(batch_item.is_voided, false) = false THEN coalesce(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0) ELSE 0 END), 0), 2)
  INTO v_item_count, v_active_payment_count, v_total_amount
  FROM public.pay_batch_items AS batch_item
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id = batch_item.pay_batch_candidate_id
  WHERE batch_candidate.pay_batch_id = p_pay_batch_id;

  -- Use counts-only submission evidence for execution summary so progress checks
  -- remain lightweight for large batches with many transfer events. The returned
  -- payload intentionally avoids evidence samples/detail arrays.
  v_submission_evidence_json := public.pay_batch_submission_evidence(p_pay_batch_id, true);
  v_transfer_count := coalesce(nullif(v_submission_evidence_json->>'transfer_count', '')::integer, 0);
  v_provider_submitted_transfer_count := coalesce(nullif(v_submission_evidence_json->>'provider_submitted_count', '')::integer, 0);
  v_local_only_transfer_count := coalesce(nullif(v_submission_evidence_json->>'local_only_count', '')::integer, coalesce(nullif(v_submission_evidence_json->>'local_idempotency_only_count', '')::integer, 0));
  v_pending_transfer_count := coalesce(nullif(v_submission_evidence_json->>'pending_count', '')::integer, 0);
  v_evidence_pending_transfer_count := v_pending_transfer_count;
  v_authorisation_ready_transfer_count := coalesce(nullif(v_submission_evidence_json->>'authorisation_ready_transfer_count', '')::integer, coalesce(nullif(v_submission_evidence_json->>'authorisation_ready_count', '')::integer, 0));
  v_unattempted_submit_eligible_transfer_count := coalesce(nullif(v_submission_evidence_json->>'unattempted_submit_eligible_count', '')::integer, 0);
  v_provider_submit_ready_transfer_count := coalesce(nullif(v_submission_evidence_json->>'provider_submit_ready_transfer_count', '')::integer, coalesce(nullif(v_submission_evidence_json->>'provider_submit_ready_count', '')::integer, 0));
  v_authorised_without_provider_submission_transfer_count := coalesce(nullif(v_submission_evidence_json->>'authorised_without_provider_submission_count', '')::integer, 0);
  v_authorised_but_not_submit_ready_transfer_count := coalesce(nullif(v_submission_evidence_json->>'authorised_but_not_submit_ready_count', '')::integer, 0);
  v_remaining_provider_submit_ready_transfer_count := v_provider_submit_ready_transfer_count;
  v_provider_submit_phase_active := coalesce((v_submission_evidence_json->>'provider_submit_phase_active')::boolean, false);
  v_authorisation_phase_active := coalesce((v_submission_evidence_json->>'authorisation_phase_active')::boolean, false);
  v_remaining_submit_attempt_required_phase := nullif(btrim(coalesce(v_submission_evidence_json->>'remaining_submit_attempt_required_phase', '')), '');
  v_safe_local_cleanup_transfer_count := coalesce(nullif(v_submission_evidence_json->>'safe_local_cleanup_count', '')::integer, 0);
  v_provider_attempt_or_evidence_transfer_count := coalesce(nullif(v_submission_evidence_json->>'provider_attempt_or_evidence_count', '')::integer, 0);
  v_provider_or_ambiguous_evidence_transfer_count := coalesce(nullif(v_submission_evidence_json->>'provider_or_ambiguous_evidence_count', '')::integer, 0);
  v_cancellable_local_auth_request_count := coalesce(nullif(v_submission_evidence_json->>'cancellable_local_auth_request_count', '')::integer, 0);
  v_non_cancellable_auth_request_count := coalesce(nullif(v_submission_evidence_json->>'non_cancellable_auth_request_count', '')::integer, 0);
  v_auth_request_retry_blocker_count := coalesce(nullif(v_submission_evidence_json->>'auth_request_retry_blocker_count', '')::integer, 0);
  v_failed_transfer_count := coalesce(nullif(v_submission_evidence_json->>'failed_count', '')::integer, 0);
  v_ambiguous_transfer_count := coalesce(nullif(v_submission_evidence_json->>'ambiguous_count', '')::integer, 0);
  v_attempted_but_unproven_transfer_count := coalesce(nullif(v_submission_evidence_json->>'attempted_but_unproven_count', '')::integer, 0);
  v_provider_attempt_without_external_id_count := coalesce(nullif(v_submission_evidence_json->>'provider_attempt_without_external_id_count', '')::integer, 0);
  v_requires_provider_poll_count := coalesce(nullif(v_submission_evidence_json->>'requires_provider_poll_count', '')::integer, 0);
  v_remaining_unattempted_submit_required := coalesce(nullif(v_submission_evidence_json->>'remaining_unattempted_submit_required', '')::integer, coalesce(nullif(v_submission_evidence_json->>'unattempted_submit_eligible_count', '')::integer, 0));
  v_remaining_evidence_unresolved_count := coalesce(nullif(v_submission_evidence_json->>'remaining_evidence_unresolved_count', '')::integer, coalesce(v_pending_transfer_count, 0) + coalesce(v_local_only_transfer_count, 0));
  v_remaining_submit_attempt_required := coalesce(nullif(v_submission_evidence_json->>'remaining_submit_attempt_required', '')::integer, v_remaining_unattempted_submit_required);
  v_remaining_provider_evidence_required := coalesce(nullif(v_submission_evidence_json->>'remaining_provider_evidence_required', '')::integer, 0);
  v_remaining_provider_submission_required := coalesce(nullif(v_submission_evidence_json->>'remaining_provider_submission_required', '')::integer, v_remaining_unattempted_submit_required);
  v_provider_submit_diagnostic := COALESCE(v_submission_evidence_json->'provider_submit_diagnostic', '{}'::jsonb);
  v_provider_submission_status := NULLIF(BTRIM(COALESCE(v_submission_evidence_json->>'provider_submission_status', v_provider_submit_diagnostic->>'provider_submission_status', '')), '');
  v_provider_submit_review_reason_code := NULLIF(BTRIM(COALESCE(v_submission_evidence_json->>'review_reason_code', v_provider_submit_diagnostic->>'review_reason_code', '')), '');
  v_provider_acceptance_evidence_count := coalesce(nullif(v_submission_evidence_json->>'provider_acceptance_evidence_count', '')::integer, 0);
  v_provider_response_present_count := coalesce(nullif(v_submission_evidence_json->>'provider_response_present_count', '')::integer, 0);
  v_provider_request_sent_count := coalesce(nullif(v_submission_evidence_json->>'provider_request_sent_count', '')::integer, 0);
  v_provider_submission_unknown_count := coalesce(nullif(v_submission_evidence_json->>'provider_submission_unknown_count', '')::integer, 0);
  v_stale_unresolved_submit_chunk_count := coalesce(nullif(v_submission_evidence_json->>'stale_unresolved_submit_chunk_count', '')::integer, coalesce(nullif(v_submission_evidence_json->>'stale_empty_submit_chunk_count', '')::integer, 0));
  v_unfinalised_submit_chunk_count := coalesce(nullif(v_submission_evidence_json->>'unfinalised_submit_chunk_count', '')::integer, 0);
  v_provider_manual_resolution_required := lower(BTRIM(COALESCE(v_submission_evidence_json->>'manual_resolution_required', v_provider_submit_diagnostic->>'manual_resolution_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_provider_safe_retry_available := lower(BTRIM(COALESCE(v_submission_evidence_json->>'safe_retry_available', v_provider_submit_diagnostic->>'safe_retry_available', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_provider_recommended_action := NULLIF(BTRIM(COALESCE(v_submission_evidence_json->>'recommended_action', v_provider_submit_diagnostic->>'recommended_action', '')), '');

  SELECT count(*)::integer
  INTO v_canonical_pending_status_transfer_count
  FROM public.pay_bank_transfers AS canonical_pending_transfer
  WHERE canonical_pending_transfer.pay_batch_id = p_pay_batch_id
    AND upper(btrim(coalesce(canonical_pending_transfer.status, ''))) = 'PENDING';

  SELECT
    count(*) FILTER (
      WHERE coalesce(transfer_summary.amount, 0) > 0
        AND upper(btrim(coalesce(transfer_summary.status, ''))) NOT IN ('FAILED', 'DECLINED', 'REJECTED', 'RETURNED', 'REVERTED', 'CANCELLED', 'CANCELED', 'BLOCKED', 'SKIPPED', 'COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED')
        AND upper(btrim(coalesce(transfer_summary.rail_state, ''))) NOT IN ('FAILED', 'DECLINED', 'REJECTED', 'RETURNED', 'REVERTED', 'CANCELLED', 'CANCELED', 'BLOCKED', 'SKIPPED', 'COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED')
        AND transfer_summary.completed_at_utc IS NULL
        AND NULLIF(btrim(coalesce(transfer_summary.failed_reason, '')), '') IS NULL
        AND (
          (transfer_summary.payee_entity_kind IS NOT NULL AND transfer_summary.payee_entity_id IS NOT NULL)
          OR (
            NULLIF(btrim(coalesce(transfer_summary.payee_name, '')), '') IS NOT NULL
            AND NULLIF(btrim(coalesce(transfer_summary.sort_code, '')), '') IS NOT NULL
            AND NULLIF(btrim(coalesce(transfer_summary.account_number, '')), '') IS NOT NULL
          )
          OR NULLIF(btrim(coalesce(transfer_summary.bank_details_hash_snapshot, '')), '') IS NOT NULL
        )
    )::integer,
    count(*) FILTER (
      WHERE upper(btrim(coalesce(transfer_summary.status, ''))) = 'BLOCKED'
         OR upper(btrim(coalesce(transfer_summary.rail_state, ''))) = 'BLOCKED'
    )::integer
  INTO v_prepared_transfer_count, v_blocked_transfer_count
  FROM public.pay_bank_transfers AS transfer_summary
  WHERE transfer_summary.pay_batch_id = p_pay_batch_id;

  SELECT operation_row.id,
         operation_row.operation_type,
         operation_row.status
  INTO v_active_operation_id, v_active_operation_type, v_active_operation_status
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.pay_batch_id = p_pay_batch_id
    AND upper(btrim(coalesce(operation_row.status, ''))) NOT IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED')
  ORDER BY operation_row.updated_at_utc DESC NULLS LAST, operation_row.created_at_utc DESC NULLS LAST, operation_row.id DESC
  LIMIT 1;

  IF v_active_operation_id IS NOT NULL THEN
    SELECT upper(btrim(coalesce(
             nullif(btrim(coalesce(operation_scope_row.input_json->>'pay_channel_scope', '')), ''),
             nullif(btrim(coalesce(operation_scope_row.input_json->>'payChannelScope', '')), ''),
             nullif(btrim(coalesce(operation_scope_row.config_json->>'pay_channel_scope', '')), ''),
             nullif(btrim(coalesce(operation_scope_row.config_json->>'payChannelScope', '')), ''),
             'ALL'
           )))
    INTO v_active_operation_pay_channel_scope
    FROM public.banking_pay_operations AS operation_scope_row
    WHERE operation_scope_row.id = v_active_operation_id;

    IF v_active_operation_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'LOANS', 'ALL') THEN
      v_active_operation_pay_channel_scope := 'ALL';
    END IF;

    SELECT count(*)::integer,
           count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'PREPARED')::integer,
           count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'PENDING')::integer,
           count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'FAILED')::integer,
           count(*) FILTER (WHERE upper(btrim(coalesce(operation_scope.status, ''))) = 'SKIPPED')::integer,
           count(*) FILTER (WHERE operation_scope.pay_bank_transfer_id IS NULL)::integer
    INTO v_operation_scope_count,
         v_operation_scope_prepared_count,
         v_operation_scope_pending_count,
         v_operation_scope_failed_count,
         v_operation_scope_skipped_count,
         v_operation_scope_without_transfer_count
    FROM public.banking_pay_operation_transfer_scope AS operation_scope
    WHERE operation_scope.pay_batch_id = p_pay_batch_id
      AND operation_scope.operation_id = v_active_operation_id
      AND (
        v_active_operation_pay_channel_scope IN ('ALL', 'ANY', '*')
        OR upper(btrim(coalesce(operation_scope.pay_channel, ''))) = v_active_operation_pay_channel_scope
      );

    SELECT count(*) FILTER (WHERE operation_classification.is_authorisation_ready)::integer
    INTO v_operation_scope_authorisation_ready_count
    FROM public.pay_bank_transfer_execution_classify(
      p_pay_batch_id => p_pay_batch_id,
      p_pay_channel_scope => v_active_operation_pay_channel_scope,
      p_operation_id => v_active_operation_id,
      p_include_unscoped_transfers => false
    ) AS operation_classification;

    v_all_operation_scopes_authorisation_ready := (
      COALESCE(v_operation_scope_count, 0) > 0
      AND COALESCE(v_operation_scope_prepared_count, 0) = COALESCE(v_operation_scope_count, 0)
      AND COALESCE(v_operation_scope_pending_count, 0) = 0
      AND COALESCE(v_operation_scope_failed_count, 0) = 0
      AND COALESCE(v_operation_scope_skipped_count, 0) = 0
      AND COALESCE(v_operation_scope_without_transfer_count, 0) = 0
      AND COALESCE(v_operation_scope_authorisation_ready_count, 0) = COALESCE(v_operation_scope_count, 0)
    );
  END IF;

  RETURN (
  jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'batch_id', p_pay_batch_id::text,
    'batch_status', v_batch_row.status,
    'status', v_batch_row.status,
    'batch_kind_fixed', v_batch_row.batch_kind_fixed,
    'pay_channel', v_batch_row.batch_kind_fixed,
    'provider', v_batch_row.rail_provider_snapshot,
    'provider_environment', v_batch_row.rail_env_snapshot,
    'rail_provider_snapshot', v_batch_row.rail_provider_snapshot,
    'rail_env_snapshot', v_batch_row.rail_env_snapshot,
    'execution_commit_state', v_batch_row.execution_commit_state,
    'execution_mode', coalesce(nullif(btrim(coalesce(v_batch_row.execution_intent_json->>'execution_mode', '')), ''), nullif(btrim(coalesce(v_batch_row.execution_intent_json->>'mode', '')), ''), case when upper(coalesce(v_batch_row.rail_provider_snapshot, '')) = 'CSV' then 'CSV' else 'BANK' end),
    'freshness_validation_status', v_batch_row.freshness_validation_status,
    'freshness_checked_at_utc', v_batch_row.freshness_checked_at_utc,
    'freshness_result_hash', v_batch_row.freshness_result_hash,
    'freshness_scope_hash', v_batch_row.freshness_scope_hash,
    'freshness_operation_id', case when v_batch_row.freshness_operation_id is null then null else v_batch_row.freshness_operation_id::text end,
    'freshness_is_stale', coalesce(v_freshness_is_stale, false),
    'freshness_stale_reason_counts', coalesce(v_freshness_result_json->'stale_reason_counts', '{}'::jsonb),
    'schedule_state', jsonb_build_object(
      'schedule_kind', v_batch_row.schedule_kind,
      'scheduled_at_utc', v_batch_row.scheduled_at_utc,
      'scheduled_by_user_id', case when v_batch_row.scheduled_by_user_id is null then null else v_batch_row.scheduled_by_user_id::text end
    ),
    'authorisation_state', jsonb_build_object(
      'execution_commit_state', v_batch_row.execution_commit_state,
      'execution_commit_ref', v_batch_row.execution_commit_ref,
      'execution_committed_at_utc', v_batch_row.execution_committed_at_utc
    ),
    'candidate_count', coalesce(v_candidate_count, 0),
    'item_count', coalesce(v_item_count, 0),
    'active_payment_count', coalesce(v_active_payment_count, 0),
    'total_amount', coalesce(v_total_amount, 0),
    'transfer_count', coalesce(v_transfer_count, 0),
    'prepared_transfer_count', coalesce(v_prepared_transfer_count, 0),
    'submitted_transfer_count', coalesce(v_provider_submitted_transfer_count, 0),
    'provider_submitted_transfer_count', coalesce(v_provider_submitted_transfer_count, 0),
    'local_only_transfer_count', coalesce(v_local_only_transfer_count, 0),
    'pending_transfer_count', coalesce(v_pending_transfer_count, 0)
  )
  ||
  jsonb_build_object(
    'evidence_pending_transfer_count', coalesce(v_evidence_pending_transfer_count, 0),
    'canonical_pending_status_transfer_count', coalesce(v_canonical_pending_status_transfer_count, 0),
    'authorisation_ready_transfer_count', coalesce(v_authorisation_ready_transfer_count, 0),
    'operation_scope_count', coalesce(v_operation_scope_count, 0),
    'operation_scope_prepared_count', coalesce(v_operation_scope_prepared_count, 0),
    'operation_scope_pending_count', coalesce(v_operation_scope_pending_count, 0),
    'operation_scope_failed_count', coalesce(v_operation_scope_failed_count, 0),
    'operation_scope_skipped_count', coalesce(v_operation_scope_skipped_count, 0),
    'operation_scope_without_transfer_count', coalesce(v_operation_scope_without_transfer_count, 0),
    'operation_scope_authorisation_ready_count', coalesce(v_operation_scope_authorisation_ready_count, 0),
    'all_operation_scopes_authorisation_ready', coalesce(v_all_operation_scopes_authorisation_ready, false),
    'cancellable_local_auth_request_count', coalesce(v_cancellable_local_auth_request_count, 0),
    'non_cancellable_auth_request_count', coalesce(v_non_cancellable_auth_request_count, 0),
    'auth_request_retry_blocker_count', coalesce(v_auth_request_retry_blocker_count, 0),
    'pending_transfer_count_semantics', 'evidence-level pending classification; not an authorisation or provider-submit gate',
    'pending_transfer_count_definition', 'evidence_level_pending_count',
    'authorisation_ready_transfer_count_definition', 'pre_authorisation_ready_local_transfers',
    'provider_submit_ready_transfer_count_definition', 'same_operation_authorised_local_transfers_safe_to_submit',
    'unattempted_submit_eligible_transfer_count', coalesce(v_unattempted_submit_eligible_transfer_count, 0),
    'provider_submit_ready_transfer_count', coalesce(v_provider_submit_ready_transfer_count, 0),
    'authorised_without_provider_submission_transfer_count', coalesce(v_authorised_without_provider_submission_transfer_count, 0),
    'authorised_but_not_submit_ready_transfer_count', coalesce(v_authorised_but_not_submit_ready_transfer_count, 0),
    'remaining_provider_submit_ready_transfer_count', coalesce(v_remaining_provider_submit_ready_transfer_count, 0),
    'provider_submit_phase_active', coalesce(v_provider_submit_phase_active, false),
    'authorisation_phase_active', coalesce(v_authorisation_phase_active, false),
    'safe_local_cleanup_transfer_count', coalesce(v_safe_local_cleanup_transfer_count, 0),
    'provider_attempt_or_evidence_transfer_count', coalesce(v_provider_attempt_or_evidence_transfer_count, 0),
    'provider_or_ambiguous_evidence_transfer_count', coalesce(v_provider_or_ambiguous_evidence_transfer_count, 0),
    'failed_transfer_count', coalesce(v_failed_transfer_count, 0),
    'ambiguous_transfer_count', coalesce(v_ambiguous_transfer_count, 0),
    'attempted_but_unproven_transfer_count', coalesce(v_attempted_but_unproven_transfer_count, 0),
    'attempted_but_unproven_count', coalesce(v_attempted_but_unproven_transfer_count, 0)
  )
  ||
  jsonb_build_object(
    'provider_attempt_without_external_id_count', coalesce(v_provider_attempt_without_external_id_count, 0),
    'requires_provider_poll_count', coalesce(v_requires_provider_poll_count, 0),
    'remaining_submit_attempt_required', coalesce(v_remaining_submit_attempt_required, 0),
    'remaining_unattempted_submit_required', coalesce(v_remaining_unattempted_submit_required, 0),
    'remaining_evidence_unresolved_count', coalesce(v_remaining_evidence_unresolved_count, 0),
    'remaining_submit_attempt_required_definition', 'phase_dependent_provider_submit_ready_count',
    'remaining_submit_attempt_required_phase', COALESCE(v_remaining_submit_attempt_required_phase, CASE WHEN COALESCE(v_provider_submit_phase_active, false) THEN 'PROVIDER_SUBMIT' WHEN COALESCE(v_authorisation_phase_active, false) THEN 'AUTHORISATION' ELSE 'NOT_PROVIDER_SUBMIT_PHASE' END),
    'remaining_provider_evidence_required', coalesce(v_remaining_provider_evidence_required, 0),
    'remaining_provider_submission_required', coalesce(v_remaining_provider_submission_required, 0),
    'blocked_transfer_count', coalesce(v_blocked_transfer_count, 0),
    'submission_evidence_counts_only', true,
    'submission_evidence', coalesce(v_submission_evidence_json, '{}'::jsonb),
    'provider_submit_diagnostic', COALESCE(v_provider_submit_diagnostic, '{}'::jsonb),
    'provider_submission_status', v_provider_submission_status,
    'review_reason_code', v_provider_submit_review_reason_code,
    'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
    'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
    'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
    'provider_submission_unknown_count', COALESCE(v_provider_submission_unknown_count, 0),
    'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
    'unfinalised_submit_chunk_count', COALESCE(v_unfinalised_submit_chunk_count, 0),
    'manual_resolution_required', COALESCE(v_provider_manual_resolution_required, false),
    'safe_retry_available', COALESCE(v_provider_safe_retry_available, false),
    'recommended_action', v_provider_recommended_action,
    'blocked_funds_state', jsonb_build_object(
      'is_blocked_funds', upper(coalesce(v_batch_row.status, '')) = 'BLOCKED_FUNDS',
      'last_funds_check_at_utc', v_batch_row.last_funds_check_at_utc,
      'last_funds_check_json', coalesce(v_batch_row.last_funds_check_json, '{}'::jsonb),
      'funding_account_ref', v_batch_row.funding_account_ref
    ),
    'settlement_state', jsonb_build_object(
      'settled_candidate_count', coalesce(v_settled_candidate_count, 0),
      'candidate_count', coalesce(v_candidate_count, 0),
      'settlement_confirmation_json', coalesce(v_batch_row.settlement_confirmation_json, '{}'::jsonb)
    ),
    'remittance_state', jsonb_build_object(
      'remittance_sent_count', coalesce(v_remittance_sent_count, 0),
      'candidate_count', coalesce(v_candidate_count, 0)
    ),
    'active_operation_id', case when v_active_operation_id is null then null else v_active_operation_id::text end,
    'active_operation_type', v_active_operation_type,
    'active_operation_status', v_active_operation_status
  )
  );
END;
$function$;





DROP FUNCTION IF EXISTS public.pay_execute_bank_transfer_chunk_prepare(uuid, uuid, jsonb, uuid);


DROP FUNCTION IF EXISTS public.pay_execute_bank_transfer_chunk_prepare(uuid, uuid, jsonb, uuid);

CREATE OR REPLACE FUNCTION public.pay_execute_bank_transfer_chunk_prepare(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_transfer_scope_ids jsonb,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_requested_count integer := 0;
  v_scope_count integer := 0;
  v_prepared_count integer := 0;
  v_reused_count integer := 0;
  v_skipped_count integer := 0;
  v_existing_skipped_count integer := 0;
  v_new_skipped_count integer := 0;
  v_failed_count integer := 0;
  v_remaining_count integer := 0;
  v_existing_prepared_reused_count integer := 0;
  v_prepared_link_missing_count integer := 0;
  v_authorisation_ready_count integer := 0;
  v_not_authorisation_ready_count integer := 0;
  v_provider_evidence_blocked_count integer := 0;
  v_conflict_provider_evidence_blocked_count integer := 0;
  v_conflict_failed_count integer := 0;
  v_normalised_transfer_count integer := 0;
  v_unsafe_transfer_count integer := 0;
  v_final_scope_prepared_count integer := 0;
  v_final_scope_failed_count integer := 0;
  v_final_scope_skipped_count integer := 0;
  v_final_scope_without_transfer_count integer := 0;
  v_all_requested_scopes_authorisation_ready boolean := false;
  v_hard_blocker boolean := false;
  v_result_code text := NULL::text;
  v_unsafe_transfer_ids jsonb := '[]'::jsonb;
  v_unsafe_reasons jsonb := '[]'::jsonb;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'operation_id is required';
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  IF p_transfer_scope_ids IS NULL OR jsonb_typeof(p_transfer_scope_ids) <> 'array' OR jsonb_array_length(p_transfer_scope_ids) = 0 THEN
    RAISE EXCEPTION 'p_transfer_scope_ids must be a non-empty JSON array';
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_operations row % not found', p_operation_id;
  END IF;

  IF v_operation_row.operation_type NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'operation % is not a payment execution operation', p_operation_id;
  END IF;

  IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> p_pay_batch_id THEN
    RAISE EXCEPTION 'operation % is for pay batch %, not %', p_operation_id, v_operation_row.pay_batch_id, p_pay_batch_id;
  END IF;

  IF v_operation_row.actor_user_id IS NOT NULL AND v_operation_row.actor_user_id <> p_actor_user_id THEN
    RAISE EXCEPTION 'operation % belongs to a different actor', p_operation_id;
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id
    AND coalesce(actor_user.is_active, false) = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'active tms_users row % not found', p_actor_user_id;
  END IF;

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_transfer_scope_request (
    transfer_scope_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  TRUNCATE TABLE pg_temp.tmp_transfer_scope_request;

  INSERT INTO pg_temp.tmp_transfer_scope_request(transfer_scope_id)
  SELECT DISTINCT (scope_element.value #>> '{}')::uuid AS transfer_scope_id
  FROM jsonb_array_elements(p_transfer_scope_ids) AS scope_element(value)
  WHERE (scope_element.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  SELECT count(*)::integer
  INTO v_requested_count
  FROM pg_temp.tmp_transfer_scope_request AS requested_scope;

  IF v_requested_count <> jsonb_array_length(p_transfer_scope_ids) THEN
    RAISE EXCEPTION 'p_transfer_scope_ids contains invalid uuid values';
  END IF;

  WITH scope_rows AS (
    SELECT transfer_scope.*
    FROM public.banking_pay_operation_transfer_scope AS transfer_scope
    JOIN pg_temp.tmp_transfer_scope_request AS requested_scope
      ON requested_scope.transfer_scope_id = transfer_scope.id
    WHERE transfer_scope.operation_id = p_operation_id
      AND transfer_scope.pay_batch_id = p_pay_batch_id
    FOR UPDATE OF transfer_scope
  )
  SELECT count(*)::integer,
         count(*) FILTER (WHERE scope_rows.status = 'PREPARED'
                           AND scope_rows.pay_bank_transfer_id IS NOT NULL
                           AND EXISTS (
                             SELECT 1
                             FROM public.pay_bank_transfer_execution_classify(
                               p_pay_batch_id => p_pay_batch_id,
                               p_pay_channel_scope => scope_rows.pay_channel,
                               p_operation_id => p_operation_id,
                               p_include_unscoped_transfers => true
                             ) AS prepared_classification
                             WHERE prepared_classification.pay_bank_transfer_id = scope_rows.pay_bank_transfer_id
                               AND prepared_classification.pay_batch_id = scope_rows.pay_batch_id
                               AND prepared_classification.pay_channel = scope_rows.pay_channel
                               AND prepared_classification.transfer_group_key = scope_rows.transfer_group_key
                               AND prepared_classification.is_authorisation_ready
                           ))::integer,
         count(*) FILTER (WHERE scope_rows.status = 'PREPARED'
                           AND (scope_rows.pay_bank_transfer_id IS NULL
                                OR NOT EXISTS (
                                  SELECT 1
                                  FROM public.pay_bank_transfers AS prepared_transfer
                                  WHERE prepared_transfer.id = scope_rows.pay_bank_transfer_id
                                    AND prepared_transfer.pay_batch_id = scope_rows.pay_batch_id
                                    AND prepared_transfer.pay_channel = scope_rows.pay_channel
                                    AND prepared_transfer.transfer_group_key = scope_rows.transfer_group_key
                                )))::integer,
         count(*) FILTER (WHERE scope_rows.status = 'SKIPPED')::integer,
         count(*) FILTER (WHERE scope_rows.status = 'FAILED')::integer
  INTO v_scope_count,
       v_existing_prepared_reused_count,
       v_prepared_link_missing_count,
       v_existing_skipped_count,
       v_failed_count
  FROM scope_rows;

  IF v_scope_count <> v_requested_count THEN
    RAISE EXCEPTION 'one or more transfer scope ids do not belong to operation % and batch %', p_operation_id, p_pay_batch_id;
  END IF;

  IF coalesce(v_prepared_link_missing_count, 0) > 0 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_BANK_TRANSFER_PREPARED_LINK_MISSING',
      'message', 'One or more transfer scope rows are marked prepared but do not have a valid linked bank transfer.',
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'count', v_prepared_link_missing_count
    )::text;
  END IF;

  v_skipped_count := COALESCE(v_existing_skipped_count, 0);

  DROP TABLE IF EXISTS pg_temp.tmp_prepare_existing_transfer_classification;
  CREATE TEMPORARY TABLE pg_temp.tmp_prepare_existing_transfer_classification (
    transfer_scope_id uuid PRIMARY KEY,
    pay_bank_transfer_id uuid NOT NULL,
    status_upper text,
    evidence_classification text,
    is_authorisation_ready boolean NOT NULL DEFAULT false,
    is_safe_local_cleanup boolean NOT NULL DEFAULT false,
    is_canonical_pending_status boolean NOT NULL DEFAULT false,
    has_provider_submission_evidence boolean NOT NULL DEFAULT false,
    has_provider_event_evidence boolean NOT NULL DEFAULT false,
    has_provider_attempt_without_external_id boolean NOT NULL DEFAULT false,
    has_operation_submit_attempt boolean NOT NULL DEFAULT false,
    has_ambiguous_external_evidence boolean NOT NULL DEFAULT false,
    is_failed_or_blocked boolean NOT NULL DEFAULT false,
    is_terminal_or_completed boolean NOT NULL DEFAULT false,
    has_different_operation_scope boolean NOT NULL DEFAULT false,
    has_stale_auth_request_evidence boolean NOT NULL DEFAULT false,
    allow_prepare_update boolean NOT NULL DEFAULT false,
    unsafe_reason text
  ) ON COMMIT DROP;

  DROP TABLE IF EXISTS pg_temp.tmp_prepare_unsafe_transfer_reasons;
  CREATE TEMPORARY TABLE pg_temp.tmp_prepare_unsafe_transfer_reasons (
    transfer_scope_id uuid,
    pay_bank_transfer_id uuid,
    pay_channel text,
    transfer_group_key text,
    unsafe_reason text
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_prepare_existing_transfer_classification (
    transfer_scope_id,
    pay_bank_transfer_id,
    status_upper,
    evidence_classification,
    is_authorisation_ready,
    is_safe_local_cleanup,
    is_canonical_pending_status,
    has_provider_submission_evidence,
    has_provider_event_evidence,
    has_provider_attempt_without_external_id,
    has_operation_submit_attempt,
    has_ambiguous_external_evidence,
    is_failed_or_blocked,
    is_terminal_or_completed,
    has_different_operation_scope,
    has_stale_auth_request_evidence,
    allow_prepare_update,
    unsafe_reason
  )
  SELECT transfer_scope.id AS transfer_scope_id,
         bank_transfer.id AS pay_bank_transfer_id,
         classified_transfer.status_upper,
         classified_transfer.evidence_classification,
         COALESCE(classified_transfer.is_authorisation_ready, false) AS is_authorisation_ready,
         COALESCE(classified_transfer.is_safe_local_cleanup, false) AS is_safe_local_cleanup,
         COALESCE(classified_transfer.is_canonical_pending_status, false) AS is_canonical_pending_status,
         COALESCE(classified_transfer.has_provider_submission_evidence, false) AS has_provider_submission_evidence,
         COALESCE(classified_transfer.has_provider_event_evidence, false) AS has_provider_event_evidence,
         COALESCE(classified_transfer.has_provider_attempt_without_external_id, false) AS has_provider_attempt_without_external_id,
         COALESCE(classified_transfer.has_operation_submit_attempt, false) AS has_operation_submit_attempt,
         COALESCE(classified_transfer.has_ambiguous_external_evidence, false) AS has_ambiguous_external_evidence,
         COALESCE(classified_transfer.is_failed_or_blocked, false) AS is_failed_or_blocked,
         COALESCE(classified_transfer.is_terminal_or_completed, false) AS is_terminal_or_completed,
         COALESCE(classified_transfer.has_different_operation_scope, false) AS has_different_operation_scope,
         COALESCE(classified_transfer.has_stale_auth_request_evidence, false) AS has_stale_auth_request_evidence,
         (
           COALESCE(classified_transfer.is_authorisation_ready, false) IS TRUE
           OR (
             COALESCE(classified_transfer.is_safe_local_cleanup, false) IS TRUE
             AND COALESCE(classified_transfer.has_provider_submission_evidence, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_provider_event_evidence, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_provider_attempt_without_external_id, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_operation_submit_attempt, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_ambiguous_external_evidence, false) IS NOT TRUE
             AND COALESCE(classified_transfer.is_failed_or_blocked, false) IS NOT TRUE
             AND COALESCE(classified_transfer.is_terminal_or_completed, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_different_operation_scope, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_stale_auth_request_evidence, false) IS NOT TRUE
           )
         ) AS allow_prepare_update,
         COALESCE(classified_transfer.unsafe_reason, 'TRANSFER_CONFLICT_NOT_AUTHORISATION_READY_OR_SAFE_TO_REPAIR') AS unsafe_reason
  FROM public.banking_pay_operation_transfer_scope AS transfer_scope
  JOIN pg_temp.tmp_transfer_scope_request AS requested_scope
    ON requested_scope.transfer_scope_id = transfer_scope.id
  JOIN public.pay_bank_transfers AS bank_transfer
    ON bank_transfer.pay_batch_id = transfer_scope.pay_batch_id
   AND bank_transfer.pay_channel = transfer_scope.pay_channel
   AND bank_transfer.transfer_group_key = transfer_scope.transfer_group_key
  LEFT JOIN LATERAL (
    SELECT classified_inner.*
    FROM public.pay_bank_transfer_execution_classify(
      p_pay_batch_id => p_pay_batch_id,
      p_pay_channel_scope => transfer_scope.pay_channel,
      p_operation_id => p_operation_id,
      p_include_unscoped_transfers => true
    ) AS classified_inner
    WHERE classified_inner.pay_bank_transfer_id = bank_transfer.id
    ORDER BY
      CASE WHEN classified_inner.scope_operation_id = p_operation_id THEN 0 ELSE 1 END,
      classified_inner.scope_id NULLS LAST,
      classified_inner.pay_bank_transfer_id
    LIMIT 1
  ) AS classified_transfer ON true
  WHERE transfer_scope.operation_id = p_operation_id
    AND transfer_scope.pay_batch_id = p_pay_batch_id
    AND transfer_scope.status = 'PENDING';

  INSERT INTO pg_temp.tmp_prepare_unsafe_transfer_reasons (
    transfer_scope_id,
    pay_bank_transfer_id,
    pay_channel,
    transfer_group_key,
    unsafe_reason
  )
  SELECT transfer_scope.id,
         classified_conflict.pay_bank_transfer_id,
         transfer_scope.pay_channel,
         transfer_scope.transfer_group_key,
         COALESCE(classified_conflict.unsafe_reason, 'TRANSFER_CONFLICT_NOT_AUTHORISATION_READY_OR_SAFE_TO_REPAIR')
  FROM pg_temp.tmp_prepare_existing_transfer_classification AS classified_conflict
  JOIN public.banking_pay_operation_transfer_scope AS transfer_scope
    ON transfer_scope.id = classified_conflict.transfer_scope_id
  WHERE classified_conflict.allow_prepare_update IS NOT TRUE;

  WITH blocked_conflict_scope AS (
    UPDATE public.banking_pay_operation_transfer_scope AS transfer_scope_update
    SET status = 'FAILED',
        updated_at_utc = v_now
    FROM pg_temp.tmp_prepare_existing_transfer_classification AS classified_conflict
    WHERE transfer_scope_update.id = classified_conflict.transfer_scope_id
      AND transfer_scope_update.operation_id = p_operation_id
      AND transfer_scope_update.pay_batch_id = p_pay_batch_id
      AND transfer_scope_update.status = 'PENDING'
      AND classified_conflict.allow_prepare_update IS NOT TRUE
    RETURNING transfer_scope_update.id
  )
  SELECT COALESCE(COUNT(*), 0)::integer
  INTO v_conflict_failed_count
  FROM blocked_conflict_scope;

  SELECT COALESCE(COUNT(*), 0)::integer
  INTO v_conflict_provider_evidence_blocked_count
  FROM pg_temp.tmp_prepare_existing_transfer_classification AS classified_conflict
  WHERE classified_conflict.allow_prepare_update IS NOT TRUE
    AND (
      classified_conflict.has_provider_submission_evidence
      OR classified_conflict.has_provider_event_evidence
      OR classified_conflict.has_provider_attempt_without_external_id
      OR classified_conflict.has_operation_submit_attempt
      OR classified_conflict.has_ambiguous_external_evidence
      OR classified_conflict.has_stale_auth_request_evidence
    );

  SELECT COALESCE(COUNT(*), 0)::integer
  INTO v_normalised_transfer_count
  FROM pg_temp.tmp_prepare_existing_transfer_classification AS classified_conflict
  WHERE classified_conflict.allow_prepare_update IS TRUE
    AND COALESCE(classified_conflict.is_canonical_pending_status, false) IS NOT TRUE;

  v_failed_count := COALESCE(v_failed_count, 0) + COALESCE(v_conflict_failed_count, 0);

  WITH scope_rows AS (
    SELECT transfer_scope.*,
           coalesce(
             nullif(btrim(coalesce(transfer_scope.request_id, '')), ''),
             'cltms-' || md5(transfer_scope.pay_batch_id::text || '|' || transfer_scope.pay_channel || '|' || transfer_scope.transfer_group_key)
           ) AS deterministic_request_id
    FROM public.banking_pay_operation_transfer_scope AS transfer_scope
    JOIN pg_temp.tmp_transfer_scope_request AS requested_scope
      ON requested_scope.transfer_scope_id = transfer_scope.id
    WHERE transfer_scope.operation_id = p_operation_id
      AND transfer_scope.pay_batch_id = p_pay_batch_id
      AND transfer_scope.status = 'PENDING'
    FOR UPDATE OF transfer_scope
  ), upserted_transfers AS (
    INSERT INTO public.pay_bank_transfers (
      id,
      pay_batch_id,
      candidate_id,
      umbrella_id,
      pay_channel,
      amount,
      currency,
      status,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      created_at_utc,
      completed_at_utc,
      failed_reason,
      rail_provider,
      rail_env,
      request_id,
      rail_tx_id,
      rail_state,
      rail_meta_json,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used,
      week_ending_bucket
    )
    SELECT
      gen_random_uuid(),
      transfer_scope.pay_batch_id,
      transfer_scope.candidate_id,
      transfer_scope.umbrella_id,
      transfer_scope.pay_channel,
      transfer_scope.amount,
      COALESCE(NULLIF(BTRIM(COALESCE(transfer_scope.currency, '')), ''), 'GBP'),
      'PENDING',
      transfer_scope.payment_reference,
      transfer_scope.payee_name,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(transfer_scope.sort_code, '')), '') IS NULL THEN NULL::text
        WHEN BTRIM(transfer_scope.sort_code) ~ '^[0-9]{2}-[0-9]{2}-[0-9]{2}$' THEN BTRIM(transfer_scope.sort_code)
        WHEN regexp_replace(transfer_scope.sort_code, '[^0-9]', '', 'g') ~ '^[0-9]{6}$' THEN
          substring(regexp_replace(transfer_scope.sort_code, '[^0-9]', '', 'g') from 1 for 2)
          || '-' || substring(regexp_replace(transfer_scope.sort_code, '[^0-9]', '', 'g') from 3 for 2)
          || '-' || substring(regexp_replace(transfer_scope.sort_code, '[^0-9]', '', 'g') from 5 for 2)
        ELSE BTRIM(transfer_scope.sort_code)
      END,
      regexp_replace(COALESCE(transfer_scope.account_number, ''), '[^0-9]', '', 'g'),
      transfer_scope.account_type,
      v_now,
      NULL::timestamptz,
      NULL::text,
      pay_batch.rail_provider_snapshot,
      pay_batch.rail_env_snapshot,
      transfer_scope.deterministic_request_id,
      NULL::text,
      NULL::text,
      jsonb_strip_nulls(jsonb_build_object(
        'operation_id', p_operation_id::text,
        'transfer_scope_id', transfer_scope.id::text,
        'request_id', transfer_scope.deterministic_request_id,
        'transfer_group_key', transfer_scope.transfer_group_key,
        'grouping_mode_used', transfer_scope.grouping_mode_used,
        'prepared_at_utc', v_now
      )),
      transfer_scope.bank_details_hash_snapshot,
      transfer_scope.payee_entity_kind,
      transfer_scope.payee_entity_id,
      transfer_scope.transfer_group_key,
      transfer_scope.grouping_mode_used,
      transfer_scope.week_ending_bucket
    FROM scope_rows AS transfer_scope
    JOIN public.pay_batches AS pay_batch
      ON pay_batch.id = transfer_scope.pay_batch_id
    ON CONFLICT (pay_batch_id, pay_channel, transfer_group_key)
    DO UPDATE
    SET candidate_id = EXCLUDED.candidate_id,
        umbrella_id = EXCLUDED.umbrella_id,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency,
        status = EXCLUDED.status,
        payment_reference = EXCLUDED.payment_reference,
        payee_name = EXCLUDED.payee_name,
        sort_code = EXCLUDED.sort_code,
        account_number = EXCLUDED.account_number,
        account_type = EXCLUDED.account_type,
        rail_provider = EXCLUDED.rail_provider,
        rail_env = EXCLUDED.rail_env,
        request_id = COALESCE(NULLIF(public.pay_bank_transfers.request_id, ''), EXCLUDED.request_id),
        rail_tx_id = NULL::text,
        rail_state = NULL::text,
        completed_at_utc = NULL::timestamptz,
        failed_reason = NULL::text,
        rail_meta_json = jsonb_strip_nulls(COALESCE(public.pay_bank_transfers.rail_meta_json, '{}'::jsonb) || COALESCE(EXCLUDED.rail_meta_json, '{}'::jsonb)),
        bank_details_hash_snapshot = EXCLUDED.bank_details_hash_snapshot,
        payee_entity_kind = EXCLUDED.payee_entity_kind,
        payee_entity_id = EXCLUDED.payee_entity_id,
        grouping_mode_used = EXCLUDED.grouping_mode_used,
        week_ending_bucket = EXCLUDED.week_ending_bucket
    WHERE EXISTS (
      SELECT 1
      FROM pg_temp.tmp_prepare_existing_transfer_classification AS classified_conflict
      WHERE classified_conflict.pay_bank_transfer_id = public.pay_bank_transfers.id
        AND classified_conflict.allow_prepare_update IS TRUE
    )
    RETURNING public.pay_bank_transfers.id,
              public.pay_bank_transfers.pay_batch_id,
              public.pay_bank_transfers.pay_channel,
              public.pay_bank_transfers.transfer_group_key,
              (xmax = 0) AS was_inserted
  ), linked_scope AS (
    UPDATE public.banking_pay_operation_transfer_scope AS transfer_scope_update
    SET status = 'PREPARED',
        pay_bank_transfer_id = upserted_transfers.id,
        updated_at_utc = v_now
    FROM upserted_transfers
    WHERE transfer_scope_update.operation_id = p_operation_id
      AND transfer_scope_update.pay_batch_id = upserted_transfers.pay_batch_id
      AND transfer_scope_update.pay_channel = upserted_transfers.pay_channel
      AND transfer_scope_update.transfer_group_key = upserted_transfers.transfer_group_key
      AND transfer_scope_update.status = 'PENDING'
    RETURNING transfer_scope_update.id,
              transfer_scope_update.pay_bank_transfer_id,
              upserted_transfers.was_inserted
  ), item_links AS (
    UPDATE public.pay_batch_items AS batch_item_update
    SET pay_bank_transfer_id = linked_scope.pay_bank_transfer_id,
        bank_reference = COALESCE(batch_item_update.bank_reference, bank_transfer.payment_reference),
        updated_at = v_now
    FROM linked_scope
    JOIN public.banking_pay_operation_transfer_scope AS transfer_scope
      ON transfer_scope.id = linked_scope.id
    JOIN public.pay_bank_transfers AS bank_transfer
      ON bank_transfer.id = linked_scope.pay_bank_transfer_id
    WHERE batch_item_update.id IN (
        SELECT (item_id.value #>> '{}')::uuid
        FROM jsonb_array_elements(COALESCE(transfer_scope.pay_batch_item_ids_json, '[]'::jsonb)) AS item_id(value)
        WHERE (item_id.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      )
      AND EXISTS (
        SELECT 1
        FROM public.pay_batch_candidates AS batch_candidate
        WHERE batch_candidate.id = batch_item_update.pay_batch_candidate_id
          AND batch_candidate.pay_batch_id = p_pay_batch_id
      )
    RETURNING batch_item_update.id
  )
  SELECT count(*) FILTER (WHERE linked_scope.was_inserted)::integer,
         count(*) FILTER (WHERE linked_scope.was_inserted IS NOT TRUE)::integer
  INTO v_prepared_count, v_reused_count
  FROM linked_scope;

  WITH failed_requested_scope AS (
    UPDATE public.banking_pay_operation_transfer_scope AS transfer_scope_update
    SET status = 'FAILED',
        updated_at_utc = v_now
    FROM pg_temp.tmp_transfer_scope_request AS requested_scope
    WHERE transfer_scope_update.id = requested_scope.transfer_scope_id
      AND transfer_scope_update.operation_id = p_operation_id
      AND transfer_scope_update.pay_batch_id = p_pay_batch_id
      AND transfer_scope_update.status = 'PENDING'
    RETURNING transfer_scope_update.id
  )
  SELECT coalesce(v_failed_count, 0) + coalesce(count(*), 0)::integer
  INTO v_failed_count
  FROM failed_requested_scope;

  v_reused_count := COALESCE(v_existing_prepared_reused_count, 0) + COALESCE(v_reused_count, 0);

  WITH requested_prepared_scope AS (
    SELECT prepared_scope.id,
           prepared_scope.pay_bank_transfer_id,
           prepared_scope.pay_channel
    FROM public.banking_pay_operation_transfer_scope AS prepared_scope
    JOIN pg_temp.tmp_transfer_scope_request AS requested_scope
      ON requested_scope.transfer_scope_id = prepared_scope.id
    WHERE prepared_scope.operation_id = p_operation_id
      AND prepared_scope.pay_batch_id = p_pay_batch_id
      AND prepared_scope.status = 'PREPARED'
      AND prepared_scope.pay_bank_transfer_id IS NOT NULL
  ), classified_prepared_scope AS (
    SELECT requested_prepared_scope.id AS transfer_scope_id,
           requested_prepared_scope.pay_bank_transfer_id,
           COALESCE(classified_transfer.is_authorisation_ready, false) AS is_authorisation_ready,
           COALESCE(classified_transfer.has_provider_submission_evidence, false) AS has_provider_submission_evidence,
           COALESCE(classified_transfer.has_provider_event_evidence, false) AS has_provider_event_evidence,
           COALESCE(classified_transfer.has_provider_attempt_without_external_id, false) AS has_provider_attempt_without_external_id,
           COALESCE(classified_transfer.has_operation_submit_attempt, false) AS has_operation_submit_attempt,
           COALESCE(classified_transfer.has_ambiguous_external_evidence, false) AS has_ambiguous_external_evidence,
           COALESCE(classified_transfer.has_stale_auth_request_evidence, false) AS has_stale_auth_request_evidence,
           classified_transfer.unsafe_reason
    FROM requested_prepared_scope
    LEFT JOIN LATERAL public.pay_bank_transfer_execution_classify(
      p_pay_batch_id => p_pay_batch_id,
      p_pay_channel_scope => requested_prepared_scope.pay_channel,
      p_operation_id => p_operation_id,
      p_include_unscoped_transfers => true
    ) AS classified_transfer
      ON classified_transfer.pay_bank_transfer_id = requested_prepared_scope.pay_bank_transfer_id
  ), unsafe_not_ready_scope AS (
    INSERT INTO pg_temp.tmp_prepare_unsafe_transfer_reasons (
      transfer_scope_id,
      pay_bank_transfer_id,
      pay_channel,
      transfer_group_key,
      unsafe_reason
    )
    SELECT transfer_scope.id,
           classified_prepared_scope.pay_bank_transfer_id,
           transfer_scope.pay_channel,
           transfer_scope.transfer_group_key,
           COALESCE(classified_prepared_scope.unsafe_reason, 'PREPARED_TRANSFER_NOT_AUTHORISATION_READY')
    FROM classified_prepared_scope
    JOIN public.banking_pay_operation_transfer_scope AS transfer_scope
      ON transfer_scope.id = classified_prepared_scope.transfer_scope_id
    WHERE classified_prepared_scope.is_authorisation_ready IS NOT TRUE
    ON CONFLICT DO NOTHING
    RETURNING transfer_scope_id
  ), failed_not_ready_scope AS (
    UPDATE public.banking_pay_operation_transfer_scope AS transfer_scope_update
    SET status = 'FAILED',
        updated_at_utc = v_now
    FROM classified_prepared_scope
    WHERE transfer_scope_update.id = classified_prepared_scope.transfer_scope_id
      AND classified_prepared_scope.is_authorisation_ready IS NOT TRUE
    RETURNING transfer_scope_update.id
  )
  SELECT
    count(*) FILTER (WHERE classified_prepared_scope.is_authorisation_ready)::integer,
    count(*) FILTER (WHERE classified_prepared_scope.is_authorisation_ready IS NOT TRUE)::integer,
    count(*) FILTER (
      WHERE classified_prepared_scope.is_authorisation_ready IS NOT TRUE
        AND (
          classified_prepared_scope.has_provider_submission_evidence
          OR classified_prepared_scope.has_provider_event_evidence
          OR classified_prepared_scope.has_provider_attempt_without_external_id
          OR classified_prepared_scope.has_operation_submit_attempt
          OR classified_prepared_scope.has_ambiguous_external_evidence
          OR classified_prepared_scope.has_stale_auth_request_evidence
        )
    )::integer
  INTO v_authorisation_ready_count,
       v_not_authorisation_ready_count,
       v_provider_evidence_blocked_count
  FROM classified_prepared_scope;

  v_failed_count := COALESCE(v_failed_count, 0) + COALESCE(v_not_authorisation_ready_count, 0);
  v_provider_evidence_blocked_count := COALESCE(v_provider_evidence_blocked_count, 0) + COALESCE(v_conflict_provider_evidence_blocked_count, 0);

  SELECT COALESCE(COUNT(DISTINCT unsafe_reason_row.pay_bank_transfer_id) FILTER (WHERE unsafe_reason_row.pay_bank_transfer_id IS NOT NULL), 0)::integer,
         COALESCE(jsonb_agg(DISTINCT to_jsonb(unsafe_reason_row.pay_bank_transfer_id::text)) FILTER (WHERE unsafe_reason_row.pay_bank_transfer_id IS NOT NULL), '[]'::jsonb),
         COALESCE(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
           'transfer_scope_id', CASE WHEN unsafe_reason_row.transfer_scope_id IS NULL THEN NULL ELSE unsafe_reason_row.transfer_scope_id::text END,
           'pay_bank_transfer_id', CASE WHEN unsafe_reason_row.pay_bank_transfer_id IS NULL THEN NULL ELSE unsafe_reason_row.pay_bank_transfer_id::text END,
           'pay_channel', unsafe_reason_row.pay_channel,
           'transfer_group_key', unsafe_reason_row.transfer_group_key,
           'reason', unsafe_reason_row.unsafe_reason
         )) ORDER BY unsafe_reason_row.pay_channel NULLS LAST, unsafe_reason_row.transfer_group_key NULLS LAST, unsafe_reason_row.pay_bank_transfer_id NULLS LAST, unsafe_reason_row.transfer_scope_id NULLS LAST), '[]'::jsonb)
  INTO v_unsafe_transfer_count,
       v_unsafe_transfer_ids,
       v_unsafe_reasons
  FROM pg_temp.tmp_prepare_unsafe_transfer_reasons AS unsafe_reason_row;

  SELECT count(*)::integer
  INTO v_remaining_count
  FROM public.banking_pay_operation_transfer_scope AS remaining_scope
  WHERE remaining_scope.operation_id = p_operation_id
    AND remaining_scope.pay_batch_id = p_pay_batch_id
    AND remaining_scope.status = 'PENDING';

  SELECT count(*) FILTER (WHERE upper(btrim(coalesce(final_scope.status, ''))) = 'PREPARED')::integer,
         count(*) FILTER (WHERE upper(btrim(coalesce(final_scope.status, ''))) = 'FAILED')::integer,
         count(*) FILTER (WHERE upper(btrim(coalesce(final_scope.status, ''))) = 'SKIPPED')::integer,
         count(*) FILTER (WHERE final_scope.pay_bank_transfer_id IS NULL)::integer
  INTO v_final_scope_prepared_count,
       v_final_scope_failed_count,
       v_final_scope_skipped_count,
       v_final_scope_without_transfer_count
  FROM public.banking_pay_operation_transfer_scope AS final_scope
  JOIN pg_temp.tmp_transfer_scope_request AS requested_scope
    ON requested_scope.transfer_scope_id = final_scope.id
  WHERE final_scope.operation_id = p_operation_id
    AND final_scope.pay_batch_id = p_pay_batch_id;

  v_all_requested_scopes_authorisation_ready := (
    COALESCE(v_requested_count, 0) > 0
    AND COALESCE(v_authorisation_ready_count, 0) = COALESCE(v_requested_count, 0)
    AND COALESCE(v_not_authorisation_ready_count, 0) = 0
    AND COALESCE(v_provider_evidence_blocked_count, 0) = 0
    AND COALESCE(v_unsafe_transfer_count, 0) = 0
    AND COALESCE(v_failed_count, 0) = 0
    AND COALESCE(v_final_scope_failed_count, 0) = 0
    AND COALESCE(v_final_scope_skipped_count, 0) = 0
    AND COALESCE(v_final_scope_without_transfer_count, 0) = 0
    AND COALESCE(v_remaining_count, 0) = 0
  );

  v_hard_blocker := COALESCE(v_all_requested_scopes_authorisation_ready, false) IS NOT TRUE;

  v_result_code := CASE
    WHEN COALESCE(v_all_requested_scopes_authorisation_ready, false) IS TRUE THEN NULL::text
    WHEN COALESCE(v_provider_evidence_blocked_count, 0) > 0 THEN 'TRANSFER_PREPARATION_BLOCKED_BY_PROVIDER_EVIDENCE'
    WHEN COALESCE(v_unsafe_transfer_count, 0) > 0 THEN 'TRANSFER_PREPARATION_UNSAFE_TRANSFER'
    WHEN COALESCE(v_final_scope_without_transfer_count, 0) > 0 THEN 'TRANSFER_PREPARATION_SCOPE_WITHOUT_TRANSFER'
    WHEN COALESCE(v_failed_count, 0) > 0 OR COALESCE(v_final_scope_failed_count, 0) > 0 THEN 'TRANSFER_PREPARATION_SCOPE_FAILED'
    WHEN COALESCE(v_final_scope_skipped_count, 0) > 0 THEN 'TRANSFER_PREPARATION_SCOPE_SKIPPED'
    WHEN COALESCE(v_not_authorisation_ready_count, 0) > 0 THEN 'TRANSFER_PREPARATION_NOT_AUTHORISATION_READY'
    WHEN COALESCE(v_remaining_count, 0) > 0 THEN 'TRANSFER_PREPARATION_SCOPE_REMAINING'
    ELSE 'TRANSFER_PREPARATION_NOT_AUTHORISATION_READY'
  END;

  RETURN jsonb_build_object(
    'ok', COALESCE(v_all_requested_scopes_authorisation_ready, false),
    'hard_blocker', COALESCE(v_hard_blocker, false),
    'code', v_result_code,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'requested_scope_count', COALESCE(v_requested_count, 0),
    'prepared_scope_count', COALESCE(v_final_scope_prepared_count, 0),
    'scope_failed_count', COALESCE(v_final_scope_failed_count, 0),
    'scope_skipped_count', COALESCE(v_final_scope_skipped_count, 0),
    'scope_without_transfer_count', COALESCE(v_final_scope_without_transfer_count, 0),
    'all_requested_scopes_authorisation_ready', COALESCE(v_all_requested_scopes_authorisation_ready, false),
    'prepared_count', COALESCE(v_authorisation_ready_count, 0),
    'linked_transfer_count', COALESCE(v_prepared_count, 0) + COALESCE(v_reused_count, 0),
    'authorisation_ready_count', COALESCE(v_authorisation_ready_count, 0),
    'not_authorisation_ready_count', COALESCE(v_not_authorisation_ready_count, 0),
    'provider_evidence_blocked_count', COALESCE(v_provider_evidence_blocked_count, 0),
    'normalised_transfer_count', COALESCE(v_normalised_transfer_count, 0),
    'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
    'unsafe_transfer_ids', COALESCE(v_unsafe_transfer_ids, '[]'::jsonb),
    'unsafe_reasons', COALESCE(v_unsafe_reasons, '[]'::jsonb),
    'reused_count', COALESCE(v_reused_count, 0),
    'skipped_count', COALESCE(v_skipped_count, 0),
    'failed_count', COALESCE(v_failed_count, 0),
    'remaining_count', COALESCE(v_remaining_count, 0)
  );
END;
$function$;




DROP FUNCTION IF EXISTS public.pay_execute_bank_transfer_scope_seed(uuid, uuid, text, uuid, boolean);

CREATE OR REPLACE FUNCTION public.pay_execute_bank_transfer_scope_seed(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_pay_channel_scope text,
  p_actor_user_id uuid,
  p_retry_blocked_funds boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_scope text := upper(btrim(coalesce(p_pay_channel_scope, 'ALL')));
  v_now timestamptz := now();
  v_provider text := NULL::text;
  v_env text := NULL::text;
  v_bulk_ref_num integer := NULL::integer;
  v_bulk_ref_date date := NULL::date;
  v_bulk_reference text := NULL::text;
  v_pay_week_start date := NULL::date;
  v_pay_week_end date := NULL::date;
  v_tax_year_start date := NULL::date;
  v_tax_week integer := 1;
  v_batch_kind_fixed text := NULL::text;
  v_do_paye boolean := false;
  v_do_umbrella boolean := false;
  v_do_loans boolean := false;
  v_group_count integer := 0;
  v_created_count integer := 0;
  v_reused_count integer := 0;
  v_blocked_count integer := 0;
  v_total_amount numeric := 0;
  v_invalid_loans_items integer := 0;
  v_prepared_scope_mismatch_count integer := 0;
  v_prepared_scope_mismatch_details jsonb := '[]'::jsonb;
  v_stale_scope_skipped_count integer := 0;
  v_stale_previous_operation_cleanup_attempted_count integer := 0;
  v_stale_previous_operation_scope_cleaned_count integer := 0;
  v_stale_previous_retry_operation_scope_cleaned_count integer := 0;
  v_stale_previous_operation_transfer_cleaned_count integer := 0;
  v_stale_previous_operation_auth_requests_cancelled_count integer := 0;
  v_stale_previous_operation_batch_execution_intent_cleared_count integer := 0;
  v_stale_previous_operation_scope_blocked_count integer := 0;
  v_cleanup_retry_blocked_count integer := 0;
  v_stale_previous_operation_ids jsonb := '[]'::jsonb;
  v_blocked_transfer_group_keys jsonb := '[]'::jsonb;
  v_blocked_previous_operation_types jsonb := '[]'::jsonb;
  v_blocked_previous_operation_statuses jsonb := '[]'::jsonb;
  v_blocked_cleanup_reasons jsonb := '[]'::jsonb;
  v_operation_freshness_status text := NULL::text;
  v_operation_freshness_result_hash text := NULL::text;
  v_operation_freshness_scope_hash text := NULL::text;
  v_batch_freshness_status text := NULL::text;
  v_freshness_status text := NULL::text;
  v_freshness_result_hash_used text := NULL::text;
  v_freshness_scope_hash_used text := NULL::text;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'operation_id is required';
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  IF v_scope NOT IN ('PAYE', 'UMBRELLA', 'ALL', 'LOANS') THEN
    RAISE EXCEPTION 'pay_channel_scope must be PAYE, UMBRELLA, ALL, or LOANS';
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'tms_users row % not found', p_actor_user_id;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_operations row % not found', p_operation_id;
  END IF;

  IF v_operation_row.operation_type NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'operation % is not a payment execution operation', p_operation_id;
  END IF;

  IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> p_pay_batch_id THEN
    RAISE EXCEPTION 'operation % is for pay batch %, not %', p_operation_id, v_operation_row.pay_batch_id, p_pay_batch_id;
  END IF;

  IF v_operation_row.actor_user_id IS NOT NULL AND v_operation_row.actor_user_id <> p_actor_user_id THEN
    RAISE EXCEPTION 'operation % belongs to a different actor', p_operation_id;
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batches row % not found', p_pay_batch_id;
  END IF;

  IF upper(coalesce(v_batch_row.status, '')) = 'BLOCKED_FUNDS' AND COALESCE(p_retry_blocked_funds, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'pay batch % is BLOCKED_FUNDS; retry flag is required', p_pay_batch_id;
  END IF;

  v_batch_freshness_status := upper(nullif(btrim(coalesce(v_batch_row.freshness_validation_status, '')), ''));
  v_operation_freshness_status := upper(coalesce(
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness_validation_status}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness,status}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness,result,status}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness_validation_status}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness,status}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness,result,status}', '')), '')
  ));
  v_operation_freshness_result_hash := coalesce(
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness_result_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness,result_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness,result,result_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness_result_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness,result_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness,result,result_hash}', '')), '')
  );
  v_operation_freshness_scope_hash := coalesce(
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness_scope_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness,scope_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.progress_json #>> '{freshness,result,scope_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness_scope_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness,scope_hash}', '')), ''),
    nullif(btrim(coalesce(v_operation_row.result_json #>> '{freshness,result,scope_hash}', '')), '')
  );

  IF v_batch_freshness_status IS NOT NULL AND v_batch_freshness_status <> 'PASSED' THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_BATCH_FRESHNESS_NOT_PASSED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_EXECUTE_TRANSFER_SCOPE_BATCH_FRESHNESS_NOT_PASSED',
              'pay_batch_id', p_pay_batch_id::text,
              'operation_id', p_operation_id::text,
              'batch_freshness_validation_status', v_batch_freshness_status,
              'operation_freshness_validation_status', v_operation_freshness_status
            )::text;
  END IF;

  v_freshness_status := coalesce(v_operation_freshness_status, v_batch_freshness_status);
  v_freshness_result_hash_used := coalesce(v_operation_freshness_result_hash, nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), ''));
  v_freshness_scope_hash_used := coalesce(v_operation_freshness_scope_hash, nullif(btrim(coalesce(v_batch_row.freshness_scope_hash, '')), ''));

  IF coalesce(v_freshness_status, '') <> 'PASSED' THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_FRESHNESS_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_EXECUTE_TRANSFER_SCOPE_FRESHNESS_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text,
              'operation_id', p_operation_id::text,
              'freshness_validation_status', v_freshness_status
            )::text;
  END IF;

  IF v_freshness_result_hash_used IS NULL THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_FRESHNESS_HASH_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_EXECUTE_TRANSFER_SCOPE_FRESHNESS_HASH_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text,
              'operation_id', p_operation_id::text
            )::text;
  END IF;

  IF v_operation_freshness_result_hash IS NOT NULL
     AND nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') IS NOT NULL
     AND v_operation_freshness_result_hash <> nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), '') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_FRESHNESS_HASH_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_EXECUTE_TRANSFER_SCOPE_FRESHNESS_HASH_MISMATCH',
              'pay_batch_id', p_pay_batch_id::text,
              'operation_id', p_operation_id::text,
              'operation_freshness_result_hash', v_operation_freshness_result_hash,
              'batch_freshness_result_hash', v_batch_row.freshness_result_hash
            )::text;
  END IF;

  IF v_operation_freshness_scope_hash IS NOT NULL
     AND nullif(btrim(coalesce(v_batch_row.freshness_scope_hash, '')), '') IS NOT NULL
     AND v_operation_freshness_scope_hash <> nullif(btrim(coalesce(v_batch_row.freshness_scope_hash, '')), '') THEN
    RAISE EXCEPTION 'PAY_EXECUTE_TRANSFER_SCOPE_FRESHNESS_SCOPE_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_EXECUTE_TRANSFER_SCOPE_FRESHNESS_SCOPE_MISMATCH',
              'pay_batch_id', p_pay_batch_id::text,
              'operation_id', p_operation_id::text,
              'operation_freshness_scope_hash', v_operation_freshness_scope_hash,
              'batch_freshness_scope_hash', v_batch_row.freshness_scope_hash
            )::text;
  END IF;

  v_provider := v_batch_row.rail_provider_snapshot;
  v_env := v_batch_row.rail_env_snapshot;
  v_batch_kind_fixed := upper(btrim(coalesce(v_batch_row.batch_kind_fixed, '')));

  IF v_batch_row.pay_date IS NULL THEN
    RAISE EXCEPTION 'pay_batch pay_date is required';
  END IF;

  IF v_batch_row.bulk_reference IS NULL THEN
    v_bulk_ref_num := COALESCE(v_batch_row.bulk_ref_num, nextval('public.pay_bulk_ref_seq')::integer);
    v_bulk_ref_date := COALESCE(v_batch_row.bulk_ref_date, v_batch_row.pay_date);
    v_bulk_reference := to_char(v_bulk_ref_date, 'DDMMYYYY') || lpad(v_bulk_ref_num::text, 7, '0');

    UPDATE public.pay_batches AS batch_update
    SET bulk_ref_num = v_bulk_ref_num,
        bulk_ref_date = v_bulk_ref_date,
        bulk_reference = v_bulk_reference
    WHERE batch_update.id = p_pay_batch_id;
  ELSE
    v_bulk_reference := v_batch_row.bulk_reference;
  END IF;

  v_pay_week_start := public._pay_week_start_monday(v_batch_row.pay_date);
  v_pay_week_end := v_pay_week_start + 6;
  v_tax_year_start := make_date(extract(year from v_batch_row.pay_date)::integer, 4, 6);
  IF v_batch_row.pay_date < v_tax_year_start THEN
    v_tax_year_start := make_date((extract(year from v_batch_row.pay_date)::integer) - 1, 4, 6);
  END IF;
  v_tax_week := ((v_batch_row.pay_date - v_tax_year_start) / 7) + 1;
  IF v_tax_week < 1 THEN
    v_tax_week := 1;
  END IF;

  v_do_loans := (v_batch_kind_fixed = 'LOANS');
  IF v_do_loans THEN
    v_do_paye := false;
    v_do_umbrella := false;
  ELSE
    v_do_paye := (v_scope IN ('ALL', 'PAYE'));
    v_do_umbrella := (v_scope IN ('ALL', 'UMBRELLA'));
  END IF;

  IF v_do_loans = true THEN
    SELECT count(*)::integer
    INTO v_invalid_loans_items
    FROM public.pay_batch_items AS loan_item_check
    JOIN public.pay_batch_candidates AS loan_candidate_check
      ON loan_candidate_check.id = loan_item_check.pay_batch_candidate_id
    WHERE loan_candidate_check.pay_batch_id = p_pay_batch_id
      AND coalesce(loan_item_check.is_voided, false) = false
      AND loan_item_check.item_type = 'LOAN_PAYOUT'
      AND (
        loan_item_check.source_ref IS NULL
        OR btrim(loan_item_check.source_ref) = ''
        OR btrim(loan_item_check.source_ref) !~ '^advance:[0-9a-fA-F-]{36}$'
      );

    IF coalesce(v_invalid_loans_items, 0) > 0 THEN
      RAISE EXCEPTION 'LOANS_PAYOUT_INVALID: one or more frozen LOAN_PAYOUT items are missing a valid frozen advance source reference';
    END IF;
  END IF;

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_operation_transfer_item_groups (
    pay_batch_item_id uuid NOT NULL,
    pay_channel text NOT NULL,
    candidate_id uuid NOT NULL,
    umbrella_id uuid NULL,
    week_ending_bucket date NULL,
    amount numeric NOT NULL,
    currency text NOT NULL,
    status text NOT NULL,
    payment_reference text NULL,
    payee_name text NULL,
    sort_code text NULL,
    account_number text NULL,
    account_type text NULL,
    bank_details_hash_snapshot text NULL,
    payee_entity_kind text NOT NULL,
    payee_entity_id uuid NULL,
    transfer_group_key text NOT NULL,
    grouping_mode_used text NULL
  ) ON COMMIT DROP;
  TRUNCATE TABLE pg_temp.tmp_operation_transfer_item_groups;

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_operation_transfer_groups (
    pay_channel text NOT NULL,
    candidate_id uuid NOT NULL,
    umbrella_id uuid NULL,
    week_ending_bucket date NULL,
    amount numeric NOT NULL,
    currency text NOT NULL,
    status text NOT NULL,
    payment_reference text NULL,
    payee_name text NULL,
    sort_code text NULL,
    account_number text NULL,
    account_type text NULL,
    bank_details_hash_snapshot text NULL,
    payee_entity_kind text NOT NULL,
    payee_entity_id uuid NULL,
    transfer_group_key text NOT NULL,
    grouping_mode_used text NULL,
    pay_batch_item_ids_json jsonb NOT NULL,
    candidate_ids_json jsonb NOT NULL
  ) ON COMMIT DROP;
  TRUNCATE TABLE pg_temp.tmp_operation_transfer_groups;

  IF v_do_loans THEN
    INSERT INTO pg_temp.tmp_operation_transfer_groups (
      pay_channel,
      candidate_id,
      umbrella_id,
      week_ending_bucket,
      amount,
      currency,
      status,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used,
      pay_batch_item_ids_json,
      candidate_ids_json
    )
    WITH loan_item_rows AS (
      SELECT
        batch_candidate.candidate_id,
        batch_item.id AS pay_batch_item_id,
        COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0)::numeric AS item_amount,
        (batch_item.payout_instruction_snapshot_json IS NULL) AS is_missing_snapshot,
        upper(coalesce(batch_item.payout_instruction_snapshot_json->>'routing_kind', 'NORMAL_PAY_ROUTE')) AS routing_kind_txt,
        upper(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_kind', 'CANDIDATE')) AS payee_entity_kind_txt,
        CASE
          WHEN NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')), '') IS NULL THEN NULL::uuid
          ELSE (batch_item.payout_instruction_snapshot_json->>'payee_entity_id')::uuid
        END AS snapshot_payee_entity_id,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'beneficiary_name', '')), '') AS snapshot_beneficiary_name,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'sort_code', '')), '') AS snapshot_sort_code,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_number', '')), '') AS snapshot_account_number,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_type', '')), '') AS snapshot_account_type,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), '') AS snapshot_bank_details_hash,
        concat_ws(
          '|',
          upper(coalesce(batch_item.payout_instruction_snapshot_json->>'routing_kind', 'NORMAL_PAY_ROUTE')),
          upper(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_kind', 'CANDIDATE')),
          coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', ''),
          coalesce(NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'beneficiary_name', '')), ''), ''),
          coalesce(NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'sort_code', '')), ''), ''),
          coalesce(NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_number', '')), ''), ''),
          coalesce(NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_type', '')), ''), ''),
          coalesce(NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''), '')
        ) AS destination_signature
      FROM public.pay_batch_candidates AS batch_candidate
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.pay_batch_candidate_id = batch_candidate.id
       AND batch_item.item_type = 'LOAN_PAYOUT'
       AND coalesce(batch_item.is_voided, false) = false
      WHERE batch_candidate.pay_batch_id = p_pay_batch_id
    ),
    grouped_loan_rows AS (
      SELECT
        loan_item_rows.candidate_id,
        round(sum(loan_item_rows.item_amount), 2) AS sum_amount,
        bool_or(loan_item_rows.is_missing_snapshot) AS has_missing_snapshot,
        count(DISTINCT loan_item_rows.destination_signature) FILTER (WHERE loan_item_rows.is_missing_snapshot = false) AS destination_variant_count,
        min(loan_item_rows.routing_kind_txt) FILTER (WHERE loan_item_rows.is_missing_snapshot = false) AS routing_kind_txt,
        min(loan_item_rows.payee_entity_kind_txt) FILTER (WHERE loan_item_rows.is_missing_snapshot = false) AS payee_entity_kind_txt,
        (array_agg(loan_item_rows.snapshot_payee_entity_id ORDER BY loan_item_rows.snapshot_payee_entity_id::text NULLS LAST) FILTER (WHERE loan_item_rows.is_missing_snapshot = false))[1] AS snapshot_payee_entity_id,
        min(loan_item_rows.snapshot_beneficiary_name) FILTER (WHERE loan_item_rows.is_missing_snapshot = false) AS snapshot_beneficiary_name,
        min(loan_item_rows.snapshot_sort_code) FILTER (WHERE loan_item_rows.is_missing_snapshot = false) AS snapshot_sort_code,
        min(loan_item_rows.snapshot_account_number) FILTER (WHERE loan_item_rows.is_missing_snapshot = false) AS snapshot_account_number,
        min(loan_item_rows.snapshot_account_type) FILTER (WHERE loan_item_rows.is_missing_snapshot = false) AS snapshot_account_type,
        min(loan_item_rows.snapshot_bank_details_hash) FILTER (WHERE loan_item_rows.is_missing_snapshot = false) AS snapshot_bank_details_hash,
        coalesce(jsonb_agg(to_jsonb(loan_item_rows.pay_batch_item_id::text) ORDER BY loan_item_rows.pay_batch_item_id::text), '[]'::jsonb) AS pay_batch_item_ids_json
      FROM loan_item_rows
      GROUP BY loan_item_rows.candidate_id
      HAVING round(greatest(sum(loan_item_rows.item_amount), 0), 2) > 0
    )
    SELECT
      'PAYE',
      grouped_loan_rows.candidate_id,
      NULL::uuid,
      NULL::date,
      round(grouped_loan_rows.sum_amount, 2),
      'GBP',
      CASE
        WHEN grouped_loan_rows.has_missing_snapshot THEN 'FAILED'
        WHEN coalesce(grouped_loan_rows.destination_variant_count, 0) <> 1 THEN 'FAILED'
        WHEN coalesce(grouped_loan_rows.routing_kind_txt, '') <> 'NORMAL_PAY_ROUTE' THEN 'FAILED'
        WHEN coalesce(grouped_loan_rows.payee_entity_kind_txt, '') <> 'CANDIDATE' THEN 'FAILED'
        WHEN grouped_loan_rows.snapshot_payee_entity_id IS DISTINCT FROM grouped_loan_rows.candidate_id THEN 'FAILED'
        WHEN grouped_loan_rows.snapshot_bank_details_hash IS NULL OR btrim(coalesce(grouped_loan_rows.snapshot_bank_details_hash, '')) = '' THEN 'FAILED'
        WHEN grouped_loan_rows.snapshot_beneficiary_name IS NULL THEN 'FAILED'
        WHEN length(regexp_replace(coalesce(grouped_loan_rows.snapshot_sort_code, ''), '[^0-9]', '', 'g')) <> 6 THEN 'FAILED'
        WHEN NULLIF(regexp_replace(coalesce(grouped_loan_rows.snapshot_account_number, ''), '[^0-9]', '', 'g'), '') IS NULL THEN 'FAILED'
        WHEN NULLIF(btrim(coalesce(grouped_loan_rows.snapshot_account_type, '')), '') IS NULL THEN 'FAILED'
        ELSE 'PENDING'
      END,
      'Loan payout - week ' || v_tax_week::text,
      grouped_loan_rows.snapshot_beneficiary_name,
      grouped_loan_rows.snapshot_sort_code,
      grouped_loan_rows.snapshot_account_number,
      grouped_loan_rows.snapshot_account_type,
      grouped_loan_rows.snapshot_bank_details_hash,
      'CANDIDATE',
      grouped_loan_rows.candidate_id,
      grouped_loan_rows.candidate_id::text,
      'CANDIDATE',
      grouped_loan_rows.pay_batch_item_ids_json,
      jsonb_build_array(grouped_loan_rows.candidate_id::text)
    FROM grouped_loan_rows;
  END IF;

  IF v_do_paye THEN
    INSERT INTO pg_temp.tmp_operation_transfer_item_groups (
      pay_batch_item_id,
      pay_channel,
      candidate_id,
      umbrella_id,
      week_ending_bucket,
      amount,
      currency,
      status,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used
    )
    SELECT
      batch_item.id,
      'PAYE',
      batch_candidate.candidate_id,
      NULL::uuid,
      NULL::date,
      COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0)::numeric,
      'GBP',
      CASE
        WHEN batch_item.payout_instruction_snapshot_json IS NULL THEN 'FAILED'
        WHEN upper(coalesce(batch_item.payout_instruction_snapshot_json->>'routing_kind', 'NORMAL_PAY_ROUTE')) <> 'NORMAL_PAY_ROUTE' THEN 'FAILED'
        WHEN upper(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_kind', 'CANDIDATE')) <> 'CANDIDATE' THEN 'FAILED'
        WHEN (
          CASE
            WHEN NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')), '') IS NULL THEN NULL::uuid
            ELSE (batch_item.payout_instruction_snapshot_json->>'payee_entity_id')::uuid
          END
        ) IS DISTINCT FROM batch_candidate.candidate_id THEN 'FAILED'
        WHEN NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), '') IS NULL THEN 'FAILED'
        WHEN NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'beneficiary_name', '')), '') IS NULL THEN 'FAILED'
        WHEN length(regexp_replace(coalesce(batch_item.payout_instruction_snapshot_json->>'sort_code', ''), '[^0-9]', '', 'g')) <> 6 THEN 'FAILED'
        WHEN NULLIF(regexp_replace(coalesce(batch_item.payout_instruction_snapshot_json->>'account_number', ''), '[^0-9]', '', 'g'), '') IS NULL THEN 'FAILED'
        WHEN NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_type', '')), '') IS NULL THEN 'FAILED'
        ELSE 'PENDING'
      END,
      'Pay - week ' || v_tax_week::text,
      NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'beneficiary_name', '')), ''),
      NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'sort_code', '')), ''),
      NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_number', '')), ''),
      NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_type', '')), ''),
      NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''),
      'CANDIDATE',
      batch_candidate.candidate_id,
      batch_candidate.candidate_id::text || '|NORMAL_PAY_ROUTE|' || coalesce(NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), ''), ''),
      'CANDIDATE_DESTINATION'
    FROM public.pay_batch_candidates AS batch_candidate
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.pay_batch_candidate_id = batch_candidate.id
    WHERE batch_candidate.pay_batch_id = p_pay_batch_id
      AND batch_item.pay_channel = 'PAYE'
      AND batch_item.item_type <> 'DEBT_CREATED'
      AND coalesce(batch_item.is_voided, false) = false;

    INSERT INTO pg_temp.tmp_operation_transfer_groups (
      pay_channel,
      candidate_id,
      umbrella_id,
      week_ending_bucket,
      amount,
      currency,
      status,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used,
      pay_batch_item_ids_json,
      candidate_ids_json
    )
    WITH paye_destination_guard AS (
      SELECT
        item_group.candidate_id,
        count(*) FILTER (WHERE round(greatest(coalesce(item_group.amount, 0), 0), 2) > 0)::integer AS positive_item_count,
        count(*) FILTER (WHERE round(greatest(coalesce(item_group.amount, 0), 0), 2) > 0 AND item_group.status = 'FAILED')::integer AS blocked_positive_item_count,
        count(DISTINCT concat_ws('|', upper(coalesce(item_group.payee_entity_kind, '')), coalesce(item_group.payee_entity_id::text, ''), coalesce(item_group.payee_name, ''), regexp_replace(coalesce(item_group.sort_code, ''), '[^0-9]', '', 'g'), regexp_replace(coalesce(item_group.account_number, ''), '[^0-9]', '', 'g'), upper(coalesce(item_group.account_type, '')), coalesce(item_group.bank_details_hash_snapshot, ''))) FILTER (WHERE round(greatest(coalesce(item_group.amount, 0), 0), 2) > 0)::integer AS destination_variant_count,
        count(DISTINCT concat_ws('|', upper(coalesce(item_group.payee_entity_kind, '')), coalesce(item_group.payee_entity_id::text, ''), coalesce(item_group.payee_name, ''), regexp_replace(coalesce(item_group.sort_code, ''), '[^0-9]', '', 'g'), regexp_replace(coalesce(item_group.account_number, ''), '[^0-9]', '', 'g'), upper(coalesce(item_group.account_type, '')), coalesce(item_group.bank_details_hash_snapshot, ''))) FILTER (WHERE round(greatest(coalesce(item_group.amount, 0), 0), 2) > 0 AND item_group.status <> 'FAILED')::integer AS valid_destination_variant_count
      FROM pg_temp.tmp_operation_transfer_item_groups AS item_group
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.pay_batch_id = p_pay_batch_id
       AND batch_candidate.candidate_id = item_group.candidate_id
      WHERE item_group.pay_channel = 'PAYE'
        AND round(greatest(coalesce(item_group.amount, 0), 0), 2) > 0
        AND round(coalesce(batch_candidate.net_bank_amount, 0), 2) > 0
      GROUP BY item_group.candidate_id
    )
    SELECT
      item_group.pay_channel,
      item_group.candidate_id,
      NULL::uuid,
      NULL::date,
      CASE
        WHEN coalesce(destination_guard.positive_item_count, 0) > 0
         AND coalesce(destination_guard.blocked_positive_item_count, 0) = 0
         AND coalesce(destination_guard.destination_variant_count, 0) = 1
         AND coalesce(destination_guard.valid_destination_variant_count, 0) = 1
          THEN round(max(coalesce(batch_candidate.net_bank_amount, 0)), 2)
        ELSE round(sum(greatest(coalesce(item_group.amount, 0), 0)), 2)
      END,
      max(item_group.currency),
      CASE
        WHEN coalesce(destination_guard.positive_item_count, 0) = 0 THEN 'FAILED'
        WHEN coalesce(destination_guard.blocked_positive_item_count, 0) <> 0 THEN 'FAILED'
        WHEN coalesce(destination_guard.destination_variant_count, 0) <> 1 THEN 'FAILED'
        WHEN coalesce(destination_guard.valid_destination_variant_count, 0) <> 1 THEN 'FAILED'
        WHEN bool_or(item_group.status = 'FAILED') THEN 'FAILED'
        ELSE 'PENDING'
      END,
      min(item_group.payment_reference),
      min(item_group.payee_name),
      min(item_group.sort_code),
      min(item_group.account_number),
      min(item_group.account_type),
      min(item_group.bank_details_hash_snapshot),
      min(item_group.payee_entity_kind),
      (array_agg(item_group.payee_entity_id ORDER BY item_group.payee_entity_id::text NULLS LAST))[1],
      item_group.transfer_group_key,
      min(item_group.grouping_mode_used),
      coalesce(jsonb_agg(to_jsonb(item_group.pay_batch_item_id::text) ORDER BY item_group.pay_batch_item_id::text), '[]'::jsonb),
      jsonb_build_array(item_group.candidate_id::text)
    FROM pg_temp.tmp_operation_transfer_item_groups AS item_group
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.pay_batch_id = p_pay_batch_id
     AND batch_candidate.candidate_id = item_group.candidate_id
    LEFT JOIN paye_destination_guard AS destination_guard
      ON destination_guard.candidate_id = item_group.candidate_id
    WHERE item_group.pay_channel = 'PAYE'
      AND round(greatest(coalesce(item_group.amount, 0), 0), 2) > 0
      AND round(coalesce(batch_candidate.net_bank_amount, 0), 2) > 0
    GROUP BY item_group.pay_channel,
             item_group.candidate_id,
             item_group.transfer_group_key,
             destination_guard.positive_item_count,
             destination_guard.blocked_positive_item_count,
             destination_guard.destination_variant_count,
             destination_guard.valid_destination_variant_count
    HAVING round(max(coalesce(batch_candidate.net_bank_amount, 0)), 2) > 0;
  END IF;

  IF v_do_umbrella THEN
    INSERT INTO pg_temp.tmp_operation_transfer_item_groups (
      pay_batch_item_id,
      pay_channel,
      candidate_id,
      umbrella_id,
      week_ending_bucket,
      amount,
      currency,
      status,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used
    )
    WITH umbrella_item_rows AS (
      SELECT
        batch_item.id AS pay_batch_item_id,
        'UMBRELLA'::text AS pay_channel,
        batch_candidate.candidate_id,
        batch_item.umbrella_id,
        batch_item.finance_case_id,
        batch_item.item_type,
        COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0)::numeric AS item_amount,
        batch_item.payout_instruction_snapshot_json,
        batch_candidate.candidate_display_name AS frozen_candidate_display_name,
        (batch_item.item_type IN ('LOAN_PAYOUT', 'MANUAL_CREDIT_PAYOUT', 'MANUAL_DEBT_RECOVERY', 'LOAN_REPAYMENT', 'OVERPAYMENT_RECOVERY')) AS is_finance_item,
        (batch_item.payout_instruction_snapshot_json IS NULL) AS is_missing_snapshot,
        upper(coalesce(batch_item.payout_instruction_snapshot_json->>'routing_kind', 'UMBRELLA_COMPANY')) AS routing_kind_txt,
        upper(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_kind', 'UMBRELLA')) AS payee_entity_kind_txt,
        CASE
          WHEN NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'payee_entity_id', '')), '') IS NULL THEN NULL::uuid
          ELSE (batch_item.payout_instruction_snapshot_json->>'payee_entity_id')::uuid
        END AS snapshot_payee_entity_id,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'bank_details_hash', '')), '') AS snapshot_bank_details_hash,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'beneficiary_name', '')), '') AS snapshot_beneficiary_name,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'sort_code', '')), '') AS snapshot_sort_code,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_number', '')), '') AS snapshot_account_number,
        NULLIF(btrim(coalesce(batch_item.payout_instruction_snapshot_json->>'account_type', '')), '') AS snapshot_account_type,
        CASE
          WHEN coalesce(batch_item.payout_instruction_snapshot_json->>'week_ending_bucket', '') ~ '^\d{4}-\d{2}-\d{2}$'
            THEN (batch_item.payout_instruction_snapshot_json->>'week_ending_bucket')::date
          ELSE NULL::date
        END AS snapshot_week_ending_bucket
      FROM public.pay_batch_candidates AS batch_candidate
      JOIN public.pay_batch_items AS batch_item
        ON batch_item.pay_batch_candidate_id = batch_candidate.id
      WHERE batch_candidate.pay_batch_id = p_pay_batch_id
        AND batch_item.pay_channel = 'UMBRELLA'
        AND batch_item.item_type <> 'DEBT_CREATED'
        AND coalesce(batch_item.is_voided, false) = false
    ),
    umbrella_item_final AS (
      SELECT
        umbrella_item_rows.pay_batch_item_id,
        umbrella_item_rows.pay_channel,
        umbrella_item_rows.candidate_id,
        CASE
          WHEN upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' THEN NULL::uuid
          ELSE umbrella_item_rows.snapshot_payee_entity_id
        END AS umbrella_id,
        CASE
          WHEN upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' THEN NULL::date
          WHEN umbrella_item_rows.is_finance_item = true THEN umbrella_item_rows.snapshot_week_ending_bucket
          ELSE umbrella_item_rows.snapshot_week_ending_bucket
        END AS week_ending_bucket,
        umbrella_item_rows.item_amount AS amount,
        'GBP'::text AS currency,
        CASE
          WHEN umbrella_item_rows.is_missing_snapshot THEN 'FAILED'
          WHEN upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
           AND (
             upper(coalesce(umbrella_item_rows.payee_entity_kind_txt, '')) <> 'CANDIDATE'
             OR umbrella_item_rows.snapshot_payee_entity_id IS DISTINCT FROM umbrella_item_rows.candidate_id
           ) THEN 'FAILED'
          WHEN upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'UMBRELLA_COMPANY'
           AND (
             upper(coalesce(umbrella_item_rows.payee_entity_kind_txt, '')) <> 'UMBRELLA'
             OR umbrella_item_rows.snapshot_payee_entity_id IS NULL
           ) THEN 'FAILED'
          WHEN upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) NOT IN ('ONE_OFF_SPECIFIED_BANK_ACCOUNT', 'UMBRELLA_COMPANY') THEN 'FAILED'
          WHEN umbrella_item_rows.snapshot_bank_details_hash IS NULL OR btrim(coalesce(umbrella_item_rows.snapshot_bank_details_hash, '')) = '' THEN 'FAILED'
          WHEN umbrella_item_rows.snapshot_beneficiary_name IS NULL THEN 'FAILED'
          WHEN length(regexp_replace(coalesce(umbrella_item_rows.snapshot_sort_code, ''), '[^0-9]', '', 'g')) <> 6 THEN 'FAILED'
          WHEN NULLIF(regexp_replace(coalesce(umbrella_item_rows.snapshot_account_number, ''), '[^0-9]', '', 'g'), '') IS NULL THEN 'FAILED'
          WHEN NULLIF(btrim(coalesce(umbrella_item_rows.snapshot_account_type, '')), '') IS NULL THEN 'FAILED'
          WHEN umbrella_item_rows.is_finance_item = false AND upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'UMBRELLA_COMPANY' AND umbrella_item_rows.snapshot_week_ending_bucket IS NULL THEN 'FAILED'
          ELSE 'PENDING'
        END AS status,
        left(btrim(coalesce(NULLIF(btrim(coalesce(umbrella_item_rows.frozen_candidate_display_name, '')), ''), umbrella_item_rows.candidate_id::text)), 18) AS payment_reference,
        umbrella_item_rows.snapshot_beneficiary_name AS payee_name,
        umbrella_item_rows.snapshot_sort_code AS sort_code,
        umbrella_item_rows.snapshot_account_number AS account_number,
        umbrella_item_rows.snapshot_account_type AS account_type,
        umbrella_item_rows.snapshot_bank_details_hash AS bank_details_hash_snapshot,
        CASE
          WHEN upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' THEN 'CANDIDATE'
          ELSE 'UMBRELLA'
        END AS payee_entity_kind,
        CASE
          WHEN upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' THEN umbrella_item_rows.candidate_id
          ELSE umbrella_item_rows.snapshot_payee_entity_id
        END AS payee_entity_id,
        CASE
          WHEN umbrella_item_rows.is_finance_item = true AND upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' THEN
            umbrella_item_rows.candidate_id::text || '|ONEOFF|' || coalesce(umbrella_item_rows.snapshot_bank_details_hash, '')
          WHEN upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) <> 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
            AND EXISTS (
              SELECT 1
              FROM umbrella_item_rows AS umbrella_net_adjustment
              WHERE umbrella_net_adjustment.candidate_id = umbrella_item_rows.candidate_id
                AND upper(coalesce(umbrella_net_adjustment.routing_kind_txt, '')) = upper(coalesce(umbrella_item_rows.routing_kind_txt, ''))
                AND upper(coalesce(umbrella_net_adjustment.payee_entity_kind_txt, '')) = upper(coalesce(umbrella_item_rows.payee_entity_kind_txt, ''))
                AND umbrella_net_adjustment.snapshot_payee_entity_id IS NOT DISTINCT FROM umbrella_item_rows.snapshot_payee_entity_id
                AND coalesce(umbrella_net_adjustment.snapshot_bank_details_hash, '') = coalesce(umbrella_item_rows.snapshot_bank_details_hash, '')
                AND umbrella_net_adjustment.is_finance_item = true
                AND round(coalesce(umbrella_net_adjustment.item_amount, 0), 2) < 0
            ) THEN
            umbrella_item_rows.candidate_id::text || '|NET_UMBRELLA|' || upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) || '|' || coalesce(umbrella_item_rows.payee_entity_kind_txt, '') || '|' || coalesce(umbrella_item_rows.snapshot_payee_entity_id::text, '') || '|' || coalesce(umbrella_item_rows.snapshot_bank_details_hash, '')
          WHEN umbrella_item_rows.is_finance_item = true THEN
            umbrella_item_rows.candidate_id::text || '|' || upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) || '|' || coalesce(umbrella_item_rows.payee_entity_kind_txt, '') || '|' || coalesce(umbrella_item_rows.snapshot_payee_entity_id::text, '') || '|' || coalesce(umbrella_item_rows.snapshot_bank_details_hash, '')
          ELSE
            umbrella_item_rows.candidate_id::text || '|' || umbrella_item_rows.snapshot_week_ending_bucket::text || '|UMBRELLA|' || coalesce(umbrella_item_rows.snapshot_payee_entity_id::text, '') || '|' || coalesce(umbrella_item_rows.snapshot_bank_details_hash, '')
        END AS transfer_group_key,
        CASE
          WHEN umbrella_item_rows.is_finance_item = true AND upper(coalesce(umbrella_item_rows.routing_kind_txt, '')) = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT' THEN 'CANDIDATE_DESTINATION'
          WHEN umbrella_item_rows.is_finance_item = true THEN 'FINANCE_DESTINATION'
          ELSE 'CANDIDATE_WEEK_DESTINATION'
        END AS grouping_mode_used
      FROM umbrella_item_rows
    )
    SELECT
      umbrella_item_final.pay_batch_item_id,
      umbrella_item_final.pay_channel,
      umbrella_item_final.candidate_id,
      umbrella_item_final.umbrella_id,
      umbrella_item_final.week_ending_bucket,
      umbrella_item_final.amount,
      umbrella_item_final.currency,
      umbrella_item_final.status,
      umbrella_item_final.payment_reference,
      umbrella_item_final.payee_name,
      umbrella_item_final.sort_code,
      umbrella_item_final.account_number,
      umbrella_item_final.account_type,
      umbrella_item_final.bank_details_hash_snapshot,
      umbrella_item_final.payee_entity_kind,
      umbrella_item_final.payee_entity_id,
      umbrella_item_final.transfer_group_key,
      umbrella_item_final.grouping_mode_used
    FROM umbrella_item_final;

    INSERT INTO pg_temp.tmp_operation_transfer_groups (
      pay_channel,
      candidate_id,
      umbrella_id,
      week_ending_bucket,
      amount,
      currency,
      status,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      payee_entity_kind,
      payee_entity_id,
      transfer_group_key,
      grouping_mode_used,
      pay_batch_item_ids_json,
      candidate_ids_json
    )
    SELECT
      item_group.pay_channel,
      (array_agg(item_group.candidate_id ORDER BY item_group.candidate_id::text NULLS LAST))[1],
      (array_agg(item_group.umbrella_id ORDER BY item_group.umbrella_id::text NULLS LAST))[1],
      min(item_group.week_ending_bucket),
      round(sum(item_group.amount), 2),
      max(item_group.currency),
      CASE WHEN bool_or(item_group.status = 'FAILED') THEN 'FAILED' ELSE 'PENDING' END,
      min(item_group.payment_reference),
      min(item_group.payee_name),
      min(item_group.sort_code),
      min(item_group.account_number),
      min(item_group.account_type),
      min(item_group.bank_details_hash_snapshot),
      min(item_group.payee_entity_kind),
      (array_agg(item_group.payee_entity_id ORDER BY item_group.payee_entity_id::text NULLS LAST))[1],
      item_group.transfer_group_key,
      min(item_group.grouping_mode_used),
      coalesce(jsonb_agg(to_jsonb(item_group.pay_batch_item_id::text) ORDER BY item_group.pay_batch_item_id::text), '[]'::jsonb),
      coalesce(jsonb_agg(DISTINCT to_jsonb(item_group.candidate_id::text)), '[]'::jsonb)
    FROM pg_temp.tmp_operation_transfer_item_groups AS item_group
    WHERE item_group.pay_channel = 'UMBRELLA'
    GROUP BY item_group.pay_channel, item_group.transfer_group_key
    HAVING round(sum(coalesce(item_group.amount, 0)), 2) > 0
       AND count(*) FILTER (WHERE round(greatest(coalesce(item_group.amount, 0), 0), 2) > 0) > 0;
  END IF;

  WITH incoming_scope AS (
    SELECT
      transfer_group.pay_channel,
      transfer_group.transfer_group_key,
      transfer_group.candidate_id,
      transfer_group.umbrella_id,
      transfer_group.payee_entity_kind,
      transfer_group.payee_entity_id,
      transfer_group.pay_batch_item_ids_json,
      transfer_group.candidate_ids_json,
      transfer_group.currency,
      transfer_group.amount,
      transfer_group.payment_reference,
      transfer_group.payee_name,
      transfer_group.sort_code,
      transfer_group.account_number,
      transfer_group.account_type,
      transfer_group.bank_details_hash_snapshot,
      transfer_group.grouping_mode_used,
      transfer_group.week_ending_bucket,
      'op:' || p_operation_id::text || ':batch:' || p_pay_batch_id::text || ':group:' || md5(transfer_group.pay_channel || ':' || transfer_group.transfer_group_key) AS request_id,
      transfer_group.status
    FROM pg_temp.tmp_operation_transfer_groups AS transfer_group
  ),
  mismatched_prepared_scope AS (
    SELECT
      existing_scope.id AS transfer_scope_id,
      existing_scope.pay_channel,
      existing_scope.transfer_group_key,
      existing_scope.status AS existing_status,
      jsonb_strip_nulls(jsonb_build_object(
        'transfer_scope_id', existing_scope.id::text,
        'pay_channel', existing_scope.pay_channel,
        'transfer_group_key', existing_scope.transfer_group_key,
        'existing_status', existing_scope.status,
        'existing_pay_bank_transfer_id', CASE WHEN existing_scope.pay_bank_transfer_id IS NULL THEN NULL ELSE existing_scope.pay_bank_transfer_id::text END,
        'mismatch_reason', CASE WHEN incoming_scope.transfer_group_key IS NULL THEN 'PREPARED_SCOPE_NOT_IN_CURRENT_FULL_GROUPING' ELSE 'PREPARED_SCOPE_CHANGED' END,
        'incoming_amount', incoming_scope.amount,
        'existing_amount', existing_scope.amount,
        'incoming_pay_batch_item_ids_json', incoming_scope.pay_batch_item_ids_json,
        'existing_pay_batch_item_ids_json', existing_scope.pay_batch_item_ids_json,
        'incoming_candidate_ids_json', incoming_scope.candidate_ids_json,
        'existing_candidate_ids_json', existing_scope.candidate_ids_json
      )) AS mismatch_detail
    FROM public.banking_pay_operation_transfer_scope AS existing_scope
    LEFT JOIN incoming_scope
      ON incoming_scope.pay_channel = existing_scope.pay_channel
     AND incoming_scope.transfer_group_key = existing_scope.transfer_group_key
    WHERE existing_scope.operation_id = p_operation_id
      AND existing_scope.pay_batch_id = p_pay_batch_id
      AND existing_scope.status IN ('PREPARED', 'SUBMITTED')
      AND (
        incoming_scope.transfer_group_key IS NULL
        OR existing_scope.candidate_id IS DISTINCT FROM incoming_scope.candidate_id
        OR existing_scope.umbrella_id IS DISTINCT FROM incoming_scope.umbrella_id
        OR existing_scope.payee_entity_kind IS DISTINCT FROM incoming_scope.payee_entity_kind
        OR existing_scope.payee_entity_id IS DISTINCT FROM incoming_scope.payee_entity_id
        OR COALESCE(existing_scope.pay_batch_item_ids_json, '[]'::jsonb) IS DISTINCT FROM COALESCE(incoming_scope.pay_batch_item_ids_json, '[]'::jsonb)
        OR COALESCE(existing_scope.candidate_ids_json, '[]'::jsonb) IS DISTINCT FROM COALESCE(incoming_scope.candidate_ids_json, '[]'::jsonb)
        OR COALESCE(existing_scope.currency, '') IS DISTINCT FROM COALESCE(incoming_scope.currency, '')
        OR ROUND(COALESCE(existing_scope.amount, 0), 2) IS DISTINCT FROM ROUND(COALESCE(incoming_scope.amount, 0), 2)
        OR COALESCE(existing_scope.payment_reference, '') IS DISTINCT FROM COALESCE(incoming_scope.payment_reference, '')
        OR COALESCE(existing_scope.payee_name, '') IS DISTINCT FROM COALESCE(incoming_scope.payee_name, '')
        OR COALESCE(existing_scope.sort_code, '') IS DISTINCT FROM COALESCE(incoming_scope.sort_code, '')
        OR COALESCE(existing_scope.account_number, '') IS DISTINCT FROM COALESCE(incoming_scope.account_number, '')
        OR COALESCE(existing_scope.account_type, '') IS DISTINCT FROM COALESCE(incoming_scope.account_type, '')
        OR COALESCE(existing_scope.bank_details_hash_snapshot, '') IS DISTINCT FROM COALESCE(incoming_scope.bank_details_hash_snapshot, '')
        OR COALESCE(existing_scope.grouping_mode_used, '') IS DISTINCT FROM COALESCE(incoming_scope.grouping_mode_used, '')
        OR existing_scope.week_ending_bucket IS DISTINCT FROM incoming_scope.week_ending_bucket
        OR COALESCE(existing_scope.request_id, '') IS DISTINCT FROM COALESCE(incoming_scope.request_id, '')
      )
  )
  SELECT
    COUNT(*)::integer,
    COALESCE(
      jsonb_agg(mismatched_prepared_scope.mismatch_detail ORDER BY mismatched_prepared_scope.pay_channel, mismatched_prepared_scope.transfer_group_key),
      '[]'::jsonb
    )
  INTO v_prepared_scope_mismatch_count, v_prepared_scope_mismatch_details
  FROM mismatched_prepared_scope;

  IF COALESCE(v_prepared_scope_mismatch_count, 0) > 0 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_TRANSFER_SCOPE_ALREADY_PREPARED_MISMATCH',
      'message', 'Prepared or submitted transfer scope rows cannot be re-seeded with different grouping, item, amount, or payee details.',
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'mismatch_count', v_prepared_scope_mismatch_count,
      'mismatch_details', v_prepared_scope_mismatch_details
    )::text;
  END IF;


  WITH incoming_scope AS (
    SELECT transfer_group.pay_channel,
           transfer_group.transfer_group_key
    FROM pg_temp.tmp_operation_transfer_groups AS transfer_group
  ),
  stale_scope AS (
    UPDATE public.banking_pay_operation_transfer_scope AS scope_update
    SET status = 'SKIPPED',
        updated_at_utc = v_now
    WHERE scope_update.operation_id = p_operation_id
      AND scope_update.pay_batch_id = p_pay_batch_id
      AND scope_update.status IN ('PENDING', 'FAILED')
      AND NOT EXISTS (
        SELECT 1
        FROM incoming_scope
        WHERE incoming_scope.pay_channel = scope_update.pay_channel
          AND incoming_scope.transfer_group_key = scope_update.transfer_group_key
      )
    RETURNING scope_update.id
  )
  SELECT count(*)::integer
  INTO v_stale_scope_skipped_count
  FROM stale_scope;


  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_transfer_scope_previous_conflicts (
    transfer_scope_id uuid PRIMARY KEY,
    previous_operation_id uuid NOT NULL,
    previous_operation_status text,
    previous_operation_type text,
    pay_bank_transfer_id uuid,
    pay_channel text NOT NULL,
    transfer_group_key text NOT NULL
  ) ON COMMIT DROP;

  TRUNCATE TABLE pg_temp.tmp_transfer_scope_previous_conflicts;

  INSERT INTO pg_temp.tmp_transfer_scope_previous_conflicts (
    transfer_scope_id,
    previous_operation_id,
    previous_operation_status,
    previous_operation_type,
    pay_bank_transfer_id,
    pay_channel,
    transfer_group_key
  )
  SELECT previous_scope.id,
         previous_scope.operation_id,
         upper(btrim(coalesce(previous_operation.status, ''))),
         upper(btrim(coalesce(previous_operation.operation_type, ''))),
         previous_scope.pay_bank_transfer_id,
         previous_scope.pay_channel,
         previous_scope.transfer_group_key
  FROM public.banking_pay_operation_transfer_scope AS previous_scope
  JOIN pg_temp.tmp_operation_transfer_groups AS incoming_group
    ON incoming_group.pay_channel = previous_scope.pay_channel
   AND incoming_group.transfer_group_key = previous_scope.transfer_group_key
  LEFT JOIN public.banking_pay_operations AS previous_operation
    ON previous_operation.id = previous_scope.operation_id
  WHERE previous_scope.pay_batch_id = p_pay_batch_id
    AND previous_scope.operation_id <> p_operation_id;

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_transfer_scope_previous_cleanup_results (
    previous_operation_id uuid PRIMARY KEY,
    cleanup_result jsonb NOT NULL
  ) ON COMMIT DROP;

  TRUNCATE TABLE pg_temp.tmp_transfer_scope_previous_cleanup_results;

  INSERT INTO pg_temp.tmp_transfer_scope_previous_cleanup_results (
    previous_operation_id,
    cleanup_result
  )
  SELECT cleanup_operation.previous_operation_id,
         public.pay_execute_operation_cleanup_failed_local_artifacts(
           p_operation_id => cleanup_operation.previous_operation_id,
           p_actor_user_id => p_actor_user_id,
           p_failure_phase => 'PREPARE_TRANSFER_SCOPE_RETRY_CONFLICT',
           p_failure_error_json => jsonb_build_object(
             'code', 'TRANSFER_SCOPE_RETRY_CONFLICT_CLEANUP_REQUESTED',
             'requested_by_operation_id', p_operation_id::text,
             'pay_batch_id', p_pay_batch_id::text
           ),
           p_dry_run => false
         ) AS cleanup_result
  FROM (
    SELECT DISTINCT previous_conflict.previous_operation_id
    FROM pg_temp.tmp_transfer_scope_previous_conflicts AS previous_conflict
    WHERE previous_conflict.previous_operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
      AND previous_conflict.previous_operation_status IN ('FAILED', 'CANCELLED', 'CANCELED')
  ) AS cleanup_operation;

  SELECT COUNT(*)::integer,
         COALESCE((COUNT(*) FILTER (
           WHERE COALESCE((cleanup_result_row.cleanup_result->>'retry_blocked')::boolean, false) IS TRUE
              OR COALESCE((cleanup_result_row.cleanup_result->>'review_required')::boolean, false) IS TRUE
              OR COALESCE((cleanup_result_row.cleanup_result->>'safe_to_retry')::boolean, false) IS NOT TRUE
         )), 0)::integer
  INTO v_stale_previous_operation_cleanup_attempted_count,
       v_cleanup_retry_blocked_count
  FROM pg_temp.tmp_transfer_scope_previous_cleanup_results AS cleanup_result_row;

  SELECT COALESCE(SUM(COALESCE(NULLIF(cleanup_result_row.cleanup_result->>'transfer_rows_deleted', '')::integer, 0)), 0)::integer,
         COALESCE(SUM(COALESCE(NULLIF(cleanup_result_row.cleanup_result->>'auth_requests_cancelled', '')::integer, 0)), 0)::integer,
         COALESCE(SUM(COALESCE(NULLIF(cleanup_result_row.cleanup_result->>'batch_execution_intent_cleared', '')::integer, 0)), 0)::integer
  INTO v_stale_previous_operation_transfer_cleaned_count,
       v_stale_previous_operation_auth_requests_cancelled_count,
       v_stale_previous_operation_batch_execution_intent_cleared_count
  FROM pg_temp.tmp_transfer_scope_previous_cleanup_results AS cleanup_result_row;

  SELECT COUNT(*)::integer
  INTO v_stale_previous_operation_scope_cleaned_count
  FROM pg_temp.tmp_transfer_scope_previous_conflicts AS previous_conflict
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_operation_transfer_scope AS remaining_previous_scope
    WHERE remaining_previous_scope.id = previous_conflict.transfer_scope_id
  );

  SELECT COUNT(*)::integer
  INTO v_stale_previous_retry_operation_scope_cleaned_count
  FROM pg_temp.tmp_transfer_scope_previous_conflicts AS previous_conflict
  WHERE previous_conflict.previous_operation_type = 'PAYMENT_RETRY_BLOCKED_FUNDS'
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_transfer_scope AS remaining_previous_scope
      WHERE remaining_previous_scope.id = previous_conflict.transfer_scope_id
    );

  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(DISTINCT to_jsonb(remaining_conflict.previous_operation_id::text)), '[]'::jsonb),
         COALESCE(jsonb_agg(DISTINCT to_jsonb(remaining_conflict.transfer_group_key)), '[]'::jsonb),
         COALESCE(jsonb_agg(DISTINCT to_jsonb(remaining_conflict.previous_operation_type)) FILTER (WHERE remaining_conflict.previous_operation_type IS NOT NULL), '[]'::jsonb),
         COALESCE(jsonb_agg(DISTINCT to_jsonb(remaining_conflict.previous_operation_status)) FILTER (WHERE remaining_conflict.previous_operation_status IS NOT NULL), '[]'::jsonb)
  INTO v_stale_previous_operation_scope_blocked_count,
       v_stale_previous_operation_ids,
       v_blocked_transfer_group_keys,
       v_blocked_previous_operation_types,
       v_blocked_previous_operation_statuses
  FROM (
    SELECT previous_scope.operation_id AS previous_operation_id,
           previous_scope.transfer_group_key AS transfer_group_key,
           upper(btrim(coalesce(previous_operation.operation_type, ''))) AS previous_operation_type,
           upper(btrim(coalesce(previous_operation.status, ''))) AS previous_operation_status
    FROM public.banking_pay_operation_transfer_scope AS previous_scope
    LEFT JOIN public.banking_pay_operations AS previous_operation
      ON previous_operation.id = previous_scope.operation_id
    JOIN pg_temp.tmp_operation_transfer_groups AS incoming_group
      ON incoming_group.pay_channel = previous_scope.pay_channel
     AND incoming_group.transfer_group_key = previous_scope.transfer_group_key
    WHERE previous_scope.pay_batch_id = p_pay_batch_id
      AND previous_scope.operation_id <> p_operation_id
  ) AS remaining_conflict;

  IF COALESCE(v_cleanup_retry_blocked_count, 0) > 0
     AND COALESCE(v_stale_previous_operation_scope_blocked_count, 0) = 0 THEN
    SELECT COALESCE(jsonb_agg(DISTINCT to_jsonb(cleanup_result_row.previous_operation_id::text)), '[]'::jsonb)
    INTO v_stale_previous_operation_ids
    FROM pg_temp.tmp_transfer_scope_previous_cleanup_results AS cleanup_result_row
    WHERE COALESCE((cleanup_result_row.cleanup_result->>'retry_blocked')::boolean, false) IS TRUE
       OR COALESCE((cleanup_result_row.cleanup_result->>'review_required')::boolean, false) IS TRUE
       OR COALESCE((cleanup_result_row.cleanup_result->>'safe_to_retry')::boolean, false) IS NOT TRUE;

    SELECT COALESCE(jsonb_agg(DISTINCT to_jsonb(previous_conflict.transfer_group_key)), '[]'::jsonb)
    INTO v_blocked_transfer_group_keys
    FROM pg_temp.tmp_transfer_scope_previous_conflicts AS previous_conflict
    WHERE EXISTS (
      SELECT 1
      FROM pg_temp.tmp_transfer_scope_previous_cleanup_results AS cleanup_result_row
      WHERE cleanup_result_row.previous_operation_id = previous_conflict.previous_operation_id
        AND (
          COALESCE((cleanup_result_row.cleanup_result->>'retry_blocked')::boolean, false) IS TRUE
          OR COALESCE((cleanup_result_row.cleanup_result->>'review_required')::boolean, false) IS TRUE
          OR COALESCE((cleanup_result_row.cleanup_result->>'safe_to_retry')::boolean, false) IS NOT TRUE
        )
    );

    SELECT COALESCE(jsonb_agg(DISTINCT to_jsonb(NULLIF(btrim(coalesce(cleanup_result_row.cleanup_result->>'retry_blocked_reason', '')), '')))
                    FILTER (WHERE NULLIF(btrim(coalesce(cleanup_result_row.cleanup_result->>'retry_blocked_reason', '')), '') IS NOT NULL), '[]'::jsonb)
    INTO v_blocked_cleanup_reasons
    FROM pg_temp.tmp_transfer_scope_previous_cleanup_results AS cleanup_result_row
    WHERE COALESCE((cleanup_result_row.cleanup_result->>'retry_blocked')::boolean, false) IS TRUE
       OR COALESCE((cleanup_result_row.cleanup_result->>'review_required')::boolean, false) IS TRUE
       OR COALESCE((cleanup_result_row.cleanup_result->>'safe_to_retry')::boolean, false) IS NOT TRUE;

    v_stale_previous_operation_scope_blocked_count := COALESCE(v_cleanup_retry_blocked_count, 0);
  END IF;

  IF COALESCE(v_stale_previous_operation_scope_blocked_count, 0) > 0 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_TRANSFER_SCOPE_SEED',
      'code', 'TRANSFER_SCOPE_GROUP_HELD_BY_ACTIVE_OR_UNSAFE_OPERATION',
      'message', 'A previous payment execution operation still owns one or more transfer groups for this batch and cannot be cleaned safely for retry.',
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'blocked_count', v_stale_previous_operation_scope_blocked_count,
      'stale_previous_operation_cleanup_attempted_count', COALESCE(v_stale_previous_operation_cleanup_attempted_count, 0),
      'stale_previous_operation_scope_cleaned_count', COALESCE(v_stale_previous_operation_scope_cleaned_count, 0),
      'stale_previous_retry_operation_scope_cleaned_count', COALESCE(v_stale_previous_retry_operation_scope_cleaned_count, 0),
      'stale_previous_operation_transfer_cleaned_count', COALESCE(v_stale_previous_operation_transfer_cleaned_count, 0),
      'stale_previous_operation_auth_requests_cancelled_count', COALESCE(v_stale_previous_operation_auth_requests_cancelled_count, 0),
      'stale_previous_operation_batch_execution_intent_cleared_count', COALESCE(v_stale_previous_operation_batch_execution_intent_cleared_count, 0),
      'cleanup_retry_blocked_count', COALESCE(v_cleanup_retry_blocked_count, 0),
      'blocked_previous_operation_ids', v_stale_previous_operation_ids,
      'stale_previous_operation_ids', v_stale_previous_operation_ids,
      'blocked_previous_operation_types', v_blocked_previous_operation_types,
      'blocked_previous_operation_statuses', v_blocked_previous_operation_statuses,
      'blocked_transfer_group_keys', v_blocked_transfer_group_keys,
      'blocked_cleanup_reasons', v_blocked_cleanup_reasons
    )::text USING ERRCODE = 'P0001';
  END IF;

  WITH existing_scope AS (
    SELECT scope_row.id,
           scope_row.operation_id,
           scope_row.pay_channel,
           scope_row.transfer_group_key
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND scope_row.pay_batch_id = p_pay_batch_id
  ),
  upserted_scope AS (
    INSERT INTO public.banking_pay_operation_transfer_scope (
      operation_id,
      pay_batch_id,
      pay_channel,
      transfer_group_key,
      candidate_id,
      umbrella_id,
      payee_entity_kind,
      payee_entity_id,
      pay_batch_item_ids_json,
      candidate_ids_json,
      currency,
      amount,
      payment_reference,
      payee_name,
      sort_code,
      account_number,
      account_type,
      bank_details_hash_snapshot,
      grouping_mode_used,
      week_ending_bucket,
      request_id,
      status,
      pay_bank_transfer_id,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      p_operation_id,
      p_pay_batch_id,
      transfer_group.pay_channel,
      transfer_group.transfer_group_key,
      transfer_group.candidate_id,
      transfer_group.umbrella_id,
      transfer_group.payee_entity_kind,
      transfer_group.payee_entity_id,
      transfer_group.pay_batch_item_ids_json,
      transfer_group.candidate_ids_json,
      transfer_group.currency,
      transfer_group.amount,
      transfer_group.payment_reference,
      transfer_group.payee_name,
      transfer_group.sort_code,
      transfer_group.account_number,
      transfer_group.account_type,
      transfer_group.bank_details_hash_snapshot,
      transfer_group.grouping_mode_used,
      transfer_group.week_ending_bucket,
      'op:' || p_operation_id::text || ':batch:' || p_pay_batch_id::text || ':group:' || md5(transfer_group.pay_channel || ':' || transfer_group.transfer_group_key),
      transfer_group.status,
      NULL::uuid,
      v_now,
      v_now
    FROM pg_temp.tmp_operation_transfer_groups AS transfer_group
    ON CONFLICT (operation_id, pay_channel, transfer_group_key)
    DO UPDATE
    SET candidate_id = EXCLUDED.candidate_id,
        umbrella_id = EXCLUDED.umbrella_id,
        payee_entity_kind = EXCLUDED.payee_entity_kind,
        payee_entity_id = EXCLUDED.payee_entity_id,
        pay_batch_item_ids_json = EXCLUDED.pay_batch_item_ids_json,
        candidate_ids_json = EXCLUDED.candidate_ids_json,
        currency = EXCLUDED.currency,
        amount = EXCLUDED.amount,
        payment_reference = EXCLUDED.payment_reference,
        payee_name = EXCLUDED.payee_name,
        sort_code = EXCLUDED.sort_code,
        account_number = EXCLUDED.account_number,
        account_type = EXCLUDED.account_type,
        bank_details_hash_snapshot = EXCLUDED.bank_details_hash_snapshot,
        grouping_mode_used = EXCLUDED.grouping_mode_used,
        week_ending_bucket = EXCLUDED.week_ending_bucket,
        request_id = EXCLUDED.request_id,
        status = CASE WHEN public.banking_pay_operation_transfer_scope.status IN ('PREPARED', 'SUBMITTED') THEN public.banking_pay_operation_transfer_scope.status ELSE EXCLUDED.status END,
        updated_at_utc = v_now
    RETURNING public.banking_pay_operation_transfer_scope.id,
              public.banking_pay_operation_transfer_scope.pay_channel,
              public.banking_pay_operation_transfer_scope.transfer_group_key,
              (xmax = 0) AS was_inserted,
              public.banking_pay_operation_transfer_scope.status,
              public.banking_pay_operation_transfer_scope.amount
  )
  SELECT count(*)::integer,
         count(*) FILTER (WHERE upserted_scope.was_inserted)::integer,
         count(*) FILTER (WHERE upserted_scope.was_inserted IS NOT TRUE)::integer,
         count(*) FILTER (WHERE upserted_scope.status IN ('FAILED', 'SKIPPED'))::integer,
         round(COALESCE(sum(CASE WHEN upserted_scope.status NOT IN ('FAILED', 'SKIPPED') THEN upserted_scope.amount ELSE 0 END), 0), 2)
  INTO v_group_count, v_created_count, v_reused_count, v_blocked_count, v_total_amount
  FROM upserted_scope;

  UPDATE public.banking_pay_operations AS operation_update
  SET pay_batch_id = p_pay_batch_id,
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id
    AND operation_update.pay_batch_id IS NULL;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'pay_channel_scope', v_scope,
    'group_count', COALESCE(v_group_count, 0),
    'total_amount', COALESCE(v_total_amount, 0),
    'created_count', COALESCE(v_created_count, 0),
    'reused_count', COALESCE(v_reused_count, 0),
    'blocked_invalid_count', COALESCE(v_blocked_count, 0) + COALESCE(v_stale_scope_skipped_count, 0),
    'stale_scope_skipped_count', COALESCE(v_stale_scope_skipped_count, 0),
    'stale_previous_operation_cleanup_attempted_count', COALESCE(v_stale_previous_operation_cleanup_attempted_count, 0),
    'stale_previous_operation_scope_cleaned_count', COALESCE(v_stale_previous_operation_scope_cleaned_count, 0),
    'stale_previous_retry_operation_scope_cleaned_count', COALESCE(v_stale_previous_retry_operation_scope_cleaned_count, 0),
    'stale_previous_operation_transfer_cleaned_count', COALESCE(v_stale_previous_operation_transfer_cleaned_count, 0),
    'stale_previous_operation_auth_requests_cancelled_count', COALESCE(v_stale_previous_operation_auth_requests_cancelled_count, 0),
    'stale_previous_operation_batch_execution_intent_cleared_count', COALESCE(v_stale_previous_operation_batch_execution_intent_cleared_count, 0),
    'stale_previous_operation_scope_blocked_count', COALESCE(v_stale_previous_operation_scope_blocked_count, 0),
    'cleanup_retry_blocked_count', COALESCE(v_cleanup_retry_blocked_count, 0),
    'blocked_previous_operation_ids', COALESCE(v_stale_previous_operation_ids, '[]'::jsonb),
    'stale_previous_operation_ids', COALESCE(v_stale_previous_operation_ids, '[]'::jsonb),
    'blocked_previous_operation_types', COALESCE(v_blocked_previous_operation_types, '[]'::jsonb),
    'blocked_previous_operation_statuses', COALESCE(v_blocked_previous_operation_statuses, '[]'::jsonb),
    'blocked_transfer_group_keys', COALESCE(v_blocked_transfer_group_keys, '[]'::jsonb),
    'blocked_cleanup_reasons', COALESCE(v_blocked_cleanup_reasons, '[]'::jsonb),
    'freshness_validation_status', v_freshness_status,
    'freshness_result_hash_used', v_freshness_result_hash_used,
    'freshness_scope_hash_used', v_freshness_scope_hash_used
  );
END;
$function$;


DROP FUNCTION IF EXISTS public.pay_bank_transfers_claim_provider_submit_chunk(uuid, uuid, integer, text, integer);



DROP FUNCTION IF EXISTS public.pay_operation_remittance_scope_seed(uuid, uuid, text, uuid);

CREATE OR REPLACE FUNCTION public.pay_bank_transfers_claim_provider_submit_chunk(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_limit integer DEFAULT 50,
  p_lock_owner text DEFAULT NULL::text,
  p_lock_seconds integer DEFAULT 60
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_lock_owner text := NULL::text;
  v_lock_seconds integer := 60;
  v_limit integer := 50;
  v_sequence_no integer := 1;
  v_chunk_id uuid := NULL::uuid;
  v_transfer_ids jsonb := '[]'::jsonb;
  v_transfer_rows jsonb := '[]'::jsonb;
  v_transfer_count integer := 0;
  v_remaining_count integer := 0;
  v_retry_mode boolean := false;
  v_pay_channel_scope text := 'ALL';
  v_unattempted_eligible_count integer := 0;
  v_provider_submit_ready_count integer := 0;
  v_same_operation_authorised_auth_count integer := 0;
  v_authorised_without_provider_submission_count integer := 0;
  v_authorised_but_not_submit_ready_count integer := 0;
  v_auth_request_state text := NULL::text;
  v_auth_request_unsafe_reason text := NULL::text;
  v_claim_blocker_code text := NULL::text;
  v_attempted_but_unproven_count integer := 0;
  v_provider_evidence_present_count integer := 0;
  v_provider_attempt_or_evidence_count integer := 0;
  v_unsafe_transfer_count integer := 0;
  v_failed_or_retryable_count integer := 0;
  v_terminal_count integer := 0;
  v_remaining_provider_evidence_required integer := 0;
  v_remaining_unattempted_submit_required integer := 0;
  v_classification_source text := 'pay_bank_transfer_execution_classify.is_unattempted_submit_eligible';
  v_has_unproven_attempts boolean := false;
  v_diagnostic_result jsonb := '{}'::jsonb;
  v_provider_submit_diagnostic jsonb := '{}'::jsonb;
  v_provider_submission_status text := NULL::text;
  v_review_reason_code text := NULL::text;
  v_provider_acceptance_evidence_count integer := 0;
  v_provider_response_present_count integer := 0;
  v_provider_request_sent_count integer := 0;
  v_stale_empty_submit_chunk_count integer := 0;
  v_stale_unresolved_submit_chunk_count integer := 0;
  v_unfinalised_submit_chunk_count integer := 0;
  v_provider_submission_unknown_count integer := 0;
  v_claim_blocked boolean := false;
  v_claim_blocked_chunk_ids jsonb := '[]'::jsonb;
  v_claim_blocked_transfer_ids jsonb := '[]'::jsonb;
  v_local_submit_chunk_claimed_count integer := 0;
  v_operation_submit_attempt_count integer := 0;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'operation_id is required';
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  v_limit := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 500);
  v_lock_seconds := LEAST(GREATEST(COALESCE(p_lock_seconds, 60), 10), 3600);
  v_lock_owner := COALESCE(NULLIF(BTRIM(COALESCE(p_lock_owner, '')), ''), 'provider-submit:' || p_operation_id::text);

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_operations row % not found', p_operation_id;
  END IF;

  IF v_operation_row.operation_type NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'operation % is not a payment execution operation', p_operation_id;
  END IF;

  IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> p_pay_batch_id THEN
    RAISE EXCEPTION 'operation % is for pay batch %, not %', p_operation_id, v_operation_row.pay_batch_id, p_pay_batch_id;
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batches row % not found', p_pay_batch_id;
  END IF;

  v_retry_mode := (v_operation_row.operation_type = 'PAYMENT_RETRY_BLOCKED_FUNDS');
  v_classification_source := CASE
    WHEN v_retry_mode IS TRUE THEN 'pay_bank_transfer_execution_classify.blocked_funds_retry_conservative'
    ELSE 'pay_bank_transfer_execution_classify.is_provider_submit_ready'
  END;
  v_pay_channel_scope := upper(btrim(coalesce(
    nullif(btrim(coalesce(v_operation_row.input_json->>'pay_channel_scope', '')), ''),
    nullif(btrim(coalesce(v_operation_row.input_json->>'payChannelScope', '')), ''),
    nullif(btrim(coalesce(v_operation_row.config_json->>'pay_channel_scope', '')), ''),
    nullif(btrim(coalesce(v_operation_row.config_json->>'payChannelScope', '')), ''),
    'ALL'
  )));
  IF v_pay_channel_scope NOT IN ('PAYE', 'UMBRELLA', 'LOANS', 'ALL') THEN
    v_pay_channel_scope := 'ALL';
  END IF;

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_provider_submit_classified_transfers (
    transfer_id uuid PRIMARY KEY,
    evidence_classification text NOT NULL,
    is_eligible_for_submit boolean NOT NULL DEFAULT false,
    is_provider_submit_ready boolean NOT NULL DEFAULT false,
    has_same_operation_authorised_auth_request boolean NOT NULL DEFAULT false,
    has_authorised_auth_without_provider_submission boolean NOT NULL DEFAULT false,
    has_provider_submit_blocker boolean NOT NULL DEFAULT false,
    auth_request_state text NULL,
    auth_request_unsafe_reason text NULL,
    has_provider_attempt_or_evidence boolean NOT NULL DEFAULT false,
    local_submit_chunk_claimed boolean NOT NULL DEFAULT false,
    is_unsafe_transfer boolean NOT NULL DEFAULT false
  ) ON COMMIT DROP;
  TRUNCATE TABLE pg_temp.tmp_provider_submit_classified_transfers;

  INSERT INTO pg_temp.tmp_provider_submit_classified_transfers (
    transfer_id,
    evidence_classification,
    is_eligible_for_submit,
    is_provider_submit_ready,
    has_same_operation_authorised_auth_request,
    has_authorised_auth_without_provider_submission,
    has_provider_submit_blocker,
    auth_request_state,
    auth_request_unsafe_reason,
    has_provider_attempt_or_evidence,
    local_submit_chunk_claimed,
    is_unsafe_transfer
  )
  SELECT classified_transfer.pay_bank_transfer_id,
         CASE
           WHEN classified_transfer.has_provider_submission_evidence OR classified_transfer.has_provider_event_evidence THEN 'provider_evidence_present'
           WHEN classified_transfer.is_terminal_or_completed THEN 'terminal'
           WHEN classified_transfer.is_failed_or_blocked AND v_retry_mode IS NOT TRUE THEN 'failed_or_retryable'
           WHEN classified_transfer.has_ambiguous_external_evidence
             OR classified_transfer.has_provider_attempt_without_external_id THEN 'attempted_but_unproven'
           WHEN v_retry_mode IS FALSE AND COALESCE(classified_transfer.is_provider_submit_ready, false) THEN 'unattempted_and_eligible_for_submit'
           WHEN v_retry_mode IS TRUE
            AND COALESCE(classified_transfer.has_route_ready, false) IS TRUE
            AND upper(btrim(coalesce(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
            AND NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NULL
            AND v_batch_row.execution_committed_at_utc IS NULL
            AND classified_transfer.status_upper IN ('PENDING', 'BLOCKED')
            AND COALESCE(classified_transfer.is_terminal_or_completed, false) IS NOT TRUE
            AND COALESCE(classified_transfer.has_provider_submission_evidence, false) IS NOT TRUE
            AND COALESCE(classified_transfer.has_provider_event_evidence, false) IS NOT TRUE
            AND COALESCE(classified_transfer.has_provider_attempt_without_external_id, false) IS NOT TRUE
            AND COALESCE(classified_transfer.has_operation_submit_attempt, false) IS NOT TRUE
            AND COALESCE(classified_transfer.has_ambiguous_external_evidence, false) IS NOT TRUE
            AND COALESCE(classified_transfer.has_different_operation_scope, false) IS NOT TRUE
            AND COALESCE(classified_transfer.has_stale_auth_request_evidence, false) IS NOT TRUE
            AND (p_operation_id IS NULL OR COALESCE(classified_transfer.has_auth_prepared_scope, false) IS TRUE) THEN 'unattempted_and_eligible_for_submit'
           ELSE 'unsafe_or_not_submit_eligible'
         END AS evidence_classification,
         CASE
           WHEN v_retry_mode IS FALSE THEN COALESCE(classified_transfer.is_provider_submit_ready, false)
           ELSE (
             COALESCE(classified_transfer.has_route_ready, false) IS TRUE
             AND upper(btrim(coalesce(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
             AND NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NULL
             AND v_batch_row.execution_committed_at_utc IS NULL
             AND classified_transfer.status_upper IN ('PENDING', 'BLOCKED')
             AND COALESCE(classified_transfer.is_terminal_or_completed, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_provider_submission_evidence, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_provider_event_evidence, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_provider_attempt_without_external_id, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_operation_submit_attempt, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_ambiguous_external_evidence, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_different_operation_scope, false) IS NOT TRUE
             AND COALESCE(classified_transfer.has_stale_auth_request_evidence, false) IS NOT TRUE
             AND (p_operation_id IS NULL OR COALESCE(classified_transfer.has_auth_prepared_scope, false) IS TRUE)
           )
         END AS is_eligible_for_submit,
         COALESCE(classified_transfer.is_provider_submit_ready, false) AS is_provider_submit_ready,
         COALESCE(classified_transfer.has_same_operation_authorised_auth_request, false) AS has_same_operation_authorised_auth_request,
         COALESCE(classified_transfer.has_authorised_auth_without_provider_submission, false) AS has_authorised_auth_without_provider_submission,
         COALESCE(classified_transfer.has_provider_submit_blocker, false) AS has_provider_submit_blocker,
         classified_transfer.auth_request_state AS auth_request_state,
         classified_transfer.auth_request_unsafe_reason AS auth_request_unsafe_reason,
         (
           COALESCE(classified_transfer.has_provider_submission_evidence, false)
           OR COALESCE(classified_transfer.has_provider_event_evidence, false)
           OR COALESCE(classified_transfer.has_provider_attempt_without_external_id, false)
           OR COALESCE(classified_transfer.has_ambiguous_external_evidence, false)
         ) AS has_provider_attempt_or_evidence,
         COALESCE(classified_transfer.has_operation_submit_attempt, false) AS local_submit_chunk_claimed,
         (
           CASE
             WHEN v_retry_mode IS FALSE THEN COALESCE(classified_transfer.is_provider_submit_ready, false) IS NOT TRUE
             ELSE NOT (
               COALESCE(classified_transfer.has_route_ready, false) IS TRUE
               AND upper(btrim(coalesce(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
               AND NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NULL
               AND v_batch_row.execution_committed_at_utc IS NULL
               AND classified_transfer.status_upper IN ('PENDING', 'BLOCKED')
               AND COALESCE(classified_transfer.is_terminal_or_completed, false) IS NOT TRUE
               AND COALESCE(classified_transfer.has_provider_submission_evidence, false) IS NOT TRUE
               AND COALESCE(classified_transfer.has_provider_event_evidence, false) IS NOT TRUE
               AND COALESCE(classified_transfer.has_provider_attempt_without_external_id, false) IS NOT TRUE
               AND COALESCE(classified_transfer.has_operation_submit_attempt, false) IS NOT TRUE
               AND COALESCE(classified_transfer.has_ambiguous_external_evidence, false) IS NOT TRUE
               AND COALESCE(classified_transfer.has_different_operation_scope, false) IS NOT TRUE
               AND COALESCE(classified_transfer.has_stale_auth_request_evidence, false) IS NOT TRUE
               AND (p_operation_id IS NULL OR COALESCE(classified_transfer.has_auth_prepared_scope, false) IS TRUE)
             )
           END
         ) AS is_unsafe_transfer
  FROM public.pay_bank_transfer_execution_classify(
    p_pay_batch_id => p_pay_batch_id,
    p_pay_channel_scope => v_pay_channel_scope,
    p_operation_id => p_operation_id,
    p_include_unscoped_transfers => true
  ) AS classified_transfer
  WHERE classified_transfer.pay_bank_transfer_id IS NOT NULL;

  SELECT
    COUNT(*) FILTER (WHERE classified.evidence_classification = 'unattempted_and_eligible_for_submit' AND classified.is_eligible_for_submit = true)::integer,
    COUNT(*) FILTER (WHERE classified.is_provider_submit_ready = true)::integer,
    COUNT(*) FILTER (WHERE classified.has_same_operation_authorised_auth_request = true)::integer,
    COUNT(*) FILTER (WHERE classified.has_authorised_auth_without_provider_submission = true)::integer,
    COUNT(*) FILTER (WHERE classified.has_authorised_auth_without_provider_submission = true AND classified.is_provider_submit_ready IS NOT TRUE)::integer,
    MIN(NULLIF(BTRIM(COALESCE(classified.auth_request_state, '')), '')) FILTER (WHERE NULLIF(BTRIM(COALESCE(classified.auth_request_state, '')), '') IS NOT NULL),
    MIN(NULLIF(BTRIM(COALESCE(classified.auth_request_unsafe_reason, '')), '')) FILTER (WHERE NULLIF(BTRIM(COALESCE(classified.auth_request_unsafe_reason, '')), '') IS NOT NULL),
    COUNT(*) FILTER (WHERE classified.evidence_classification = 'attempted_but_unproven')::integer,
    COUNT(*) FILTER (WHERE classified.evidence_classification = 'provider_evidence_present')::integer,
    COUNT(*) FILTER (WHERE classified.evidence_classification = 'failed_or_retryable')::integer,
    COUNT(*) FILTER (WHERE classified.evidence_classification = 'terminal')::integer,
    COUNT(*) FILTER (WHERE classified.evidence_classification = 'attempted_but_unproven')::integer,
    COUNT(*) FILTER (WHERE classified.has_provider_attempt_or_evidence)::integer,
    COUNT(*) FILTER (WHERE classified.is_unsafe_transfer)::integer,
    COUNT(*) FILTER (WHERE classified.is_eligible_for_submit = true)::integer,
    BOOL_OR(classified.evidence_classification = 'attempted_but_unproven')
  INTO v_unattempted_eligible_count,
       v_provider_submit_ready_count,
       v_same_operation_authorised_auth_count,
       v_authorised_without_provider_submission_count,
       v_authorised_but_not_submit_ready_count,
       v_auth_request_state,
       v_auth_request_unsafe_reason,
       v_attempted_but_unproven_count,
       v_provider_evidence_present_count,
       v_failed_or_retryable_count,
       v_terminal_count,
       v_remaining_provider_evidence_required,
       v_provider_attempt_or_evidence_count,
       v_unsafe_transfer_count,
       v_remaining_unattempted_submit_required,
       v_has_unproven_attempts
  FROM pg_temp.tmp_provider_submit_classified_transfers AS classified;

  SELECT COUNT(*) FILTER (WHERE classified.local_submit_chunk_claimed)::integer,
         COUNT(*) FILTER (WHERE classified.local_submit_chunk_claimed)::integer
  INTO v_local_submit_chunk_claimed_count,
       v_operation_submit_attempt_count
  FROM pg_temp.tmp_provider_submit_classified_transfers AS classified;

  v_diagnostic_result := public.pay_provider_submit_diagnostic_get(
    p_pay_batch_id := p_pay_batch_id,
    p_operation_id := p_operation_id,
    p_transfer_id := NULL::uuid,
    p_chunk_id := NULL::uuid,
    p_counts_only := true
  );
  v_provider_submit_diagnostic := COALESCE(v_diagnostic_result->'provider_submit_diagnostic', '{}'::jsonb);
  v_provider_submission_status := NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'provider_submission_status', v_diagnostic_result->>'provider_submission_status', '')), '');
  v_review_reason_code := NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'review_reason_code', v_diagnostic_result->>'review_reason_code', '')), '');
  IF COALESCE(v_diagnostic_result #>> '{counts,provider_acceptance_evidence_count}', '') ~ '^[0-9]+$' THEN
    v_provider_acceptance_evidence_count := (v_diagnostic_result #>> '{counts,provider_acceptance_evidence_count}')::integer;
  ELSE
    v_provider_acceptance_evidence_count := 0;
  END IF;
  IF COALESCE(v_diagnostic_result #>> '{counts,provider_response_present_count}', '') ~ '^[0-9]+$' THEN
    v_provider_response_present_count := (v_diagnostic_result #>> '{counts,provider_response_present_count}')::integer;
  ELSE
    v_provider_response_present_count := 0;
  END IF;
  IF COALESCE(v_diagnostic_result #>> '{counts,provider_request_sent_count}', '') ~ '^[0-9]+$' THEN
    v_provider_request_sent_count := (v_diagnostic_result #>> '{counts,provider_request_sent_count}')::integer;
  ELSE
    v_provider_request_sent_count := 0;
  END IF;
  IF COALESCE(v_diagnostic_result #>> '{counts,stale_unresolved_submit_chunk_count}', v_diagnostic_result #>> '{counts,stale_empty_submit_chunk_count}', '') ~ '^[0-9]+$' THEN
    v_stale_unresolved_submit_chunk_count := COALESCE(v_diagnostic_result #>> '{counts,stale_unresolved_submit_chunk_count}', v_diagnostic_result #>> '{counts,stale_empty_submit_chunk_count}')::integer;
  ELSE
    v_stale_unresolved_submit_chunk_count := 0;
  END IF;
  v_stale_empty_submit_chunk_count := COALESCE(v_stale_unresolved_submit_chunk_count, 0);
  v_claim_blocked_chunk_ids := COALESCE(v_diagnostic_result #> '{ids,chunk_ids}', '[]'::jsonb);
  v_claim_blocked_transfer_ids := COALESCE(v_diagnostic_result #> '{ids,transfer_ids}', '[]'::jsonb);
  IF COALESCE(v_diagnostic_result #>> '{counts,unfinalised_submit_chunk_count}', '') ~ '^[0-9]+$' THEN
    v_unfinalised_submit_chunk_count := (v_diagnostic_result #>> '{counts,unfinalised_submit_chunk_count}')::integer;
  ELSE
    v_unfinalised_submit_chunk_count := 0;
  END IF;
  IF COALESCE(v_diagnostic_result #>> '{counts,provider_submission_unknown_count}', '') ~ '^[0-9]+$' THEN
    v_provider_submission_unknown_count := (v_diagnostic_result #>> '{counts,provider_submission_unknown_count}')::integer;
  ELSE
    v_provider_submission_unknown_count := 0;
  END IF;
  IF COALESCE(v_stale_empty_submit_chunk_count, 0) > 0 THEN
    v_claim_blocked := true;
    IF COALESCE(v_provider_request_sent_count, 0) > 0 AND COALESCE(v_provider_response_present_count, 0) = 0 THEN
      v_claim_blocker_code := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
      v_provider_submission_status := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
      v_review_reason_code := 'PROVIDER_REQUEST_SENT_NO_RESPONSE';
    ELSE
      v_claim_blocker_code := 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK';
      v_provider_submission_status := 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK';
      v_review_reason_code := 'STALE_RUNNING_PROVIDER_SUBMIT_CHUNK';
    END IF;
    v_provider_submit_diagnostic := jsonb_strip_nulls(COALESCE(v_provider_submit_diagnostic, '{}'::jsonb) || jsonb_build_object(
      'diagnostic_version', 1,
      'generated_at_utc', v_now::text,
      'provider_submission_status', v_provider_submission_status,
      'review_reason_code', v_review_reason_code,
      'manual_resolution_required', true,
      'safe_retry_available', false,
      'provider_acceptance_evidence_present', false,
      'provider_response_present', false,
      'stale_submit_chunk', true,
      'unfinalised_submit_chunk', true,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text
    ));
  END IF;

  IF v_claim_blocked IS TRUE THEN
    RETURN jsonb_build_object(
      'ok', false,
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'chunk_id', NULL,
      'chunk_type', 'TRANSFER_SUBMIT',
      'phase', 'SUBMIT_PROVIDER_TRANSFERS',
      'claim_blocked', true,
      'claim_blocker_code', v_claim_blocker_code,
      'requires_review', true,
      'provider_submit_diagnostic', v_provider_submit_diagnostic,
      'provider_submission_status', v_provider_submission_status,
      'review_reason_code', v_review_reason_code,
      'chunk_ids', COALESCE(v_claim_blocked_chunk_ids, '[]'::jsonb),
      'transfer_ids', COALESCE(v_claim_blocked_transfer_ids, '[]'::jsonb),
      'manual_resolution_required', true,
      'safe_retry_available', false,
      'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
      'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
      'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
      'stale_empty_submit_chunk_count', COALESCE(v_stale_empty_submit_chunk_count, 0),
      'unfinalised_submit_chunk_count', COALESCE(v_unfinalised_submit_chunk_count, 0),
      'provider_submission_unknown_count', COALESCE(v_provider_submission_unknown_count, 0),
      'provider_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_evidence_present_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_attempt_or_evidence_count', COALESCE(v_provider_request_sent_count, 0) + COALESCE(v_provider_response_present_count, 0) + COALESCE(v_provider_acceptance_evidence_count, 0),
      'local_submit_chunk_claimed_count', COALESCE(v_local_submit_chunk_claimed_count, 0),
      'operation_submit_attempt_count', COALESCE(v_operation_submit_attempt_count, 0)
    );
  END IF;

  v_provider_evidence_present_count := COALESCE(v_provider_acceptance_evidence_count, 0);
  v_provider_attempt_or_evidence_count := COALESCE(v_provider_request_sent_count, 0) + COALESCE(v_provider_response_present_count, 0) + COALESCE(v_provider_acceptance_evidence_count, 0);
  v_attempted_but_unproven_count := COALESCE(v_provider_submission_unknown_count, 0);

  IF v_retry_mode IS NOT TRUE THEN
    IF COALESCE(v_provider_submit_ready_count, 0) = 0
       AND COALESCE(v_authorised_without_provider_submission_count, 0) > 0
       AND COALESCE(v_provider_attempt_or_evidence_count, 0) = 0 THEN
      v_claim_blocker_code := 'AUTHORISED_TRANSFER_NOT_PROVIDER_SUBMIT_READY';
    ELSIF COALESCE(v_provider_submit_ready_count, 0) = 0
       AND COALESCE(v_same_operation_authorised_auth_count, 0) > 0 THEN
      v_claim_blocker_code := 'PROVIDER_SUBMIT_NO_ELIGIBLE_TRANSFERS';
    ELSE
      v_claim_blocker_code := NULL::text;
    END IF;
  ELSE
    v_claim_blocker_code := NULL::text;
  END IF;

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.tmp_provider_submit_eligible_transfer_ids (
    transfer_id uuid PRIMARY KEY
  ) ON COMMIT DROP;
  TRUNCATE TABLE pg_temp.tmp_provider_submit_eligible_transfer_ids;

  INSERT INTO pg_temp.tmp_provider_submit_eligible_transfer_ids (transfer_id)
  SELECT classified.transfer_id
  FROM pg_temp.tmp_provider_submit_classified_transfers AS classified
  WHERE classified.is_eligible_for_submit = true;

  WITH claimable_chunk AS (
    SELECT operation_chunk.id
    FROM public.banking_pay_operation_chunks AS operation_chunk
    WHERE operation_chunk.operation_id = p_operation_id
      AND operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS'
      AND operation_chunk.chunk_type = 'TRANSFER_SUBMIT'
      AND (
        (
          operation_chunk.status = 'RUNNING'
          AND operation_chunk.locked_by = v_lock_owner
          AND operation_chunk.lock_expires_at_utc IS NOT NULL
          AND operation_chunk.lock_expires_at_utc > v_now
        )
        OR operation_chunk.status = 'PENDING'
      )
    ORDER BY
      CASE
        WHEN operation_chunk.status = 'RUNNING'
         AND operation_chunk.locked_by = v_lock_owner
         AND operation_chunk.lock_expires_at_utc IS NOT NULL
         AND operation_chunk.lock_expires_at_utc > v_now THEN 0
        WHEN operation_chunk.status = 'PENDING' THEN 1
        ELSE 2
      END,
      operation_chunk.sequence_no,
      operation_chunk.created_at_utc NULLS FIRST,
      operation_chunk.id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  ), claimed_chunk AS (
    UPDATE public.banking_pay_operation_chunks AS operation_chunk_update
    SET status = 'RUNNING',
        locked_by = v_lock_owner,
        lock_expires_at_utc = v_now + make_interval(secs => v_lock_seconds),
        started_at_utc = COALESCE(operation_chunk_update.started_at_utc, v_now),
        updated_at_utc = v_now
    FROM claimable_chunk
    WHERE operation_chunk_update.id = claimable_chunk.id
    RETURNING operation_chunk_update.id,
              COALESCE(operation_chunk_update.payload_json->'transfer_ids', '[]'::jsonb) AS transfer_ids_json,
              COALESCE(operation_chunk_update.unit_count, 0) AS unit_count,
              operation_chunk_update.sequence_no
  )
  SELECT claimed_chunk.id,
         claimed_chunk.transfer_ids_json,
         claimed_chunk.unit_count,
         claimed_chunk.sequence_no
  INTO v_chunk_id,
       v_transfer_ids,
       v_transfer_count,
       v_sequence_no
  FROM claimed_chunk;

  IF v_chunk_id IS NOT NULL THEN
    WITH claimed_transfer_ids AS (
      SELECT transfer_id_value.value::uuid AS transfer_id,
             transfer_id_value.ordinality::integer AS transfer_order
      FROM jsonb_array_elements_text(COALESCE(v_transfer_ids, '[]'::jsonb)) WITH ORDINALITY AS transfer_id_value(value, ordinality)
      WHERE transfer_id_value.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND transfer_id_value.ordinality <= v_limit
    ), claimed_transfer_rows AS (
      SELECT transfer_row.*,
             claimed_transfer_ids.transfer_order
      FROM claimed_transfer_ids
      JOIN pg_temp.tmp_provider_submit_eligible_transfer_ids AS eligible_transfer
        ON eligible_transfer.transfer_id = claimed_transfer_ids.transfer_id
      JOIN public.pay_bank_transfers AS transfer_row
        ON transfer_row.id = claimed_transfer_ids.transfer_id
       AND transfer_row.pay_batch_id = p_pay_batch_id
    ), claimed_payload AS (
      SELECT
        COALESCE(jsonb_agg(to_jsonb(claimed_transfer_rows.id::text) ORDER BY claimed_transfer_rows.transfer_order, claimed_transfer_rows.id), '[]'::jsonb) AS transfer_ids_json,
        COALESCE(jsonb_agg(
          jsonb_strip_nulls(jsonb_build_object(
            'pay_bank_transfer_id', claimed_transfer_rows.id::text,
            'transfer_id', claimed_transfer_rows.id::text,
            'pay_batch_id', claimed_transfer_rows.pay_batch_id::text,
            'candidate_id', CASE WHEN claimed_transfer_rows.candidate_id IS NULL THEN NULL ELSE claimed_transfer_rows.candidate_id::text END,
            'umbrella_id', CASE WHEN claimed_transfer_rows.umbrella_id IS NULL THEN NULL ELSE claimed_transfer_rows.umbrella_id::text END,
            'pay_channel', claimed_transfer_rows.pay_channel,
            'amount', claimed_transfer_rows.amount,
            'currency', COALESCE(NULLIF(BTRIM(COALESCE(claimed_transfer_rows.currency, '')), ''), 'GBP'),
            'payment_reference', claimed_transfer_rows.payment_reference,
            'payee_name', claimed_transfer_rows.payee_name,
            'sort_code', claimed_transfer_rows.sort_code,
            'account_number', claimed_transfer_rows.account_number,
            'account_type', claimed_transfer_rows.account_type,
            'bank_details_hash_snapshot', claimed_transfer_rows.bank_details_hash_snapshot,
            'payee_entity_kind', claimed_transfer_rows.payee_entity_kind,
            'payee_entity_id', CASE WHEN claimed_transfer_rows.payee_entity_id IS NULL THEN NULL ELSE claimed_transfer_rows.payee_entity_id::text END,
            'request_id', claimed_transfer_rows.request_id,
            'idempotency_key', COALESCE(NULLIF(BTRIM(COALESCE(claimed_transfer_rows.request_id, '')), ''), 'transfer:' || claimed_transfer_rows.id::text),
            'funding_account_ref', v_batch_row.funding_account_ref,
            'rail_provider', claimed_transfer_rows.rail_provider,
            'rail_env', claimed_transfer_rows.rail_env,
            'transfer_group_key', claimed_transfer_rows.transfer_group_key
          ))
          ORDER BY claimed_transfer_rows.transfer_order, claimed_transfer_rows.id
        ), '[]'::jsonb) AS transfer_rows_json,
        COUNT(*)::integer AS transfer_count_value
      FROM claimed_transfer_rows
    )
    SELECT claimed_payload.transfer_ids_json,
           claimed_payload.transfer_rows_json,
           claimed_payload.transfer_count_value
    INTO v_transfer_ids,
         v_transfer_rows,
         v_transfer_count
    FROM claimed_payload;

    v_provider_submit_diagnostic := jsonb_strip_nulls(COALESCE(v_provider_submit_diagnostic, '{}'::jsonb) || jsonb_build_object(
      'diagnostic_version', 1,
      'generated_at_utc', v_now::text,
      'provider_call_stage', 'PROVIDER_SUBMIT_CHUNK_CLAIMED',
      'provider_submission_status', 'NO_PROVIDER_SUBMISSION_ATTEMPTED',
      'provider_submission_attempted', false,
      'provider_request_sent', false,
      'provider_response_received', false,
      'provider_response_present', false,
      'provider_acceptance_evidence_present', false,
      'crash_safety_status_if_lock_expires', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK',
      'provider_request_impossible', false,
      'stale_submit_chunk', false,
      'unfinalised_submit_chunk', true,
      'manual_resolution_required', false,
      'safe_retry_available', false,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'chunk_id', v_chunk_id::text,
      'transfer_ids', COALESCE(v_transfer_ids, '[]'::jsonb)
    ));
    v_provider_submission_status := 'NO_PROVIDER_SUBMISSION_ATTEMPTED';
    v_review_reason_code := NULL::text;

    UPDATE public.banking_pay_operation_chunks AS operation_chunk_update
    SET payload_json = jsonb_set(
          jsonb_set(
            COALESCE(operation_chunk_update.payload_json, '{}'::jsonb),
            '{transfer_ids}',
            COALESCE(v_transfer_ids, '[]'::jsonb),
            true
          ),
          '{transfers}',
          COALESCE(v_transfer_rows, '[]'::jsonb),
          true
        ),
        unit_count = COALESCE(v_transfer_count, 0),
        result_json = jsonb_strip_nulls(COALESCE(operation_chunk_update.result_json, '{}'::jsonb) || jsonb_build_object('provider_submit_diagnostic', v_provider_submit_diagnostic)),
        updated_at_utc = v_now
    WHERE operation_chunk_update.id = v_chunk_id;

    SELECT COUNT(*)::integer
    INTO v_remaining_count
    FROM pg_temp.tmp_provider_submit_eligible_transfer_ids AS eligible_remaining
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_chunks AS active_chunk
      CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(active_chunk.payload_json->'transfer_ids', '[]'::jsonb)) AS active_transfer_id(value)
      WHERE active_chunk.operation_id = p_operation_id
        AND active_chunk.chunk_type = 'TRANSFER_SUBMIT'
        AND active_chunk.status = 'RUNNING'
        AND active_chunk.lock_expires_at_utc IS NOT NULL
        AND active_chunk.lock_expires_at_utc > v_now
        AND active_transfer_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND active_transfer_id.value::uuid = eligible_remaining.transfer_id
    );

    RETURN jsonb_build_object(
      'ok', true,
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'chunk_id', v_chunk_id::text,
      'chunk_type', 'TRANSFER_SUBMIT',
      'phase', 'SUBMIT_PROVIDER_TRANSFERS',
      'sequence_no', v_sequence_no,
      'lock_owner', v_lock_owner,
      'lock_expires_at_utc', (v_now + make_interval(secs => v_lock_seconds))::text,
      'transfer_ids', COALESCE(v_transfer_ids, '[]'::jsonb),
      'transfers', COALESCE(v_transfer_rows, '[]'::jsonb),
      'unit_count', COALESCE(v_transfer_count, 0),
      'claimed_count', COALESCE(v_transfer_count, 0),
      'unattempted_eligible_count', COALESCE(v_unattempted_eligible_count, 0),
      'provider_submit_ready_count', COALESCE(v_provider_submit_ready_count, 0),
      'same_operation_authorised_auth_count', COALESCE(v_same_operation_authorised_auth_count, 0),
      'authorised_without_provider_submission_count', COALESCE(v_authorised_without_provider_submission_count, 0),
      'authorised_but_not_submit_ready_count', COALESCE(v_authorised_but_not_submit_ready_count, 0),
      'auth_request_state', v_auth_request_state,
      'auth_request_unsafe_reason', v_auth_request_unsafe_reason,
      'claim_blocker_code', v_claim_blocker_code,
      'attempted_but_unproven_count', COALESCE(v_attempted_but_unproven_count, 0),
      'provider_evidence_present_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_attempt_or_evidence_count', COALESCE(v_provider_attempt_or_evidence_count, 0),
      'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
      'classification_source', v_classification_source,
      'retry_mode', COALESCE(v_retry_mode, false),
      'failed_or_retryable_count', COALESCE(v_failed_or_retryable_count, 0),
      'terminal_count', COALESCE(v_terminal_count, 0),
      'remaining_count', COALESCE(v_remaining_count, 0),
      'remaining_submit_attempt_required', COALESCE(v_remaining_count, 0),
      'remaining_unattempted_submit_required', COALESCE(v_remaining_count, 0),
      'remaining_provider_submission_required', COALESCE(v_remaining_count, 0),
      'remaining_provider_evidence_required', COALESCE(v_remaining_provider_evidence_required, 0),
      'has_more', COALESCE(v_remaining_count, 0) > 0,
      'has_more_submit_attempts', COALESCE(v_remaining_count, 0) > 0,
      'provider_submit_diagnostic', v_provider_submit_diagnostic,
      'provider_submission_status', v_provider_submission_status,
      'review_reason_code', v_review_reason_code,
      'chunk_ids', jsonb_build_array(v_chunk_id::text),
      'manual_resolution_required', false,
      'safe_retry_available', false,
      'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
      'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
      'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
      'stale_empty_submit_chunk_count', COALESCE(v_stale_empty_submit_chunk_count, 0),
      'unfinalised_submit_chunk_count', COALESCE(v_unfinalised_submit_chunk_count, 0),
      'provider_submission_unknown_count', COALESCE(v_provider_submission_unknown_count, 0),
      'provider_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'local_submit_chunk_claimed_count', COALESCE(v_local_submit_chunk_claimed_count, 0),
      'operation_submit_attempt_count', COALESCE(v_operation_submit_attempt_count, 0),
      'has_unproven_attempts', COALESCE(v_has_unproven_attempts, false),
      'idempotent_reuse', true
    );
  END IF;

  WITH candidate_transfers AS (
    SELECT transfer_row.*
    FROM pg_temp.tmp_provider_submit_eligible_transfer_ids AS eligible_transfer
    JOIN public.pay_bank_transfers AS transfer_row
      ON transfer_row.id = eligible_transfer.transfer_id
     AND transfer_row.pay_batch_id = p_pay_batch_id
    WHERE NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_chunks AS active_chunk
      CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(active_chunk.payload_json->'transfer_ids', '[]'::jsonb)) AS active_transfer_id(value)
      WHERE active_chunk.operation_id = p_operation_id
        AND active_chunk.chunk_type = 'TRANSFER_SUBMIT'
        AND active_chunk.status = 'RUNNING'
        AND active_chunk.lock_expires_at_utc IS NOT NULL
        AND active_chunk.lock_expires_at_utc > v_now
        AND active_transfer_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND active_transfer_id.value::uuid = transfer_row.id
    )
    ORDER BY transfer_row.created_at_utc NULLS FIRST, transfer_row.id
    LIMIT v_limit
    FOR UPDATE SKIP LOCKED
  ), selected_payload AS (
    SELECT
      COALESCE(jsonb_agg(to_jsonb(candidate_transfers.id::text) ORDER BY candidate_transfers.created_at_utc NULLS FIRST, candidate_transfers.id), '[]'::jsonb) AS transfer_ids_json,
      COALESCE(jsonb_agg(
        jsonb_strip_nulls(jsonb_build_object(
          'pay_bank_transfer_id', candidate_transfers.id::text,
          'transfer_id', candidate_transfers.id::text,
          'pay_batch_id', candidate_transfers.pay_batch_id::text,
          'candidate_id', CASE WHEN candidate_transfers.candidate_id IS NULL THEN NULL ELSE candidate_transfers.candidate_id::text END,
          'umbrella_id', CASE WHEN candidate_transfers.umbrella_id IS NULL THEN NULL ELSE candidate_transfers.umbrella_id::text END,
          'pay_channel', candidate_transfers.pay_channel,
          'amount', candidate_transfers.amount,
          'currency', COALESCE(NULLIF(BTRIM(COALESCE(candidate_transfers.currency, '')), ''), 'GBP'),
          'payment_reference', candidate_transfers.payment_reference,
          'payee_name', candidate_transfers.payee_name,
          'sort_code', candidate_transfers.sort_code,
          'account_number', candidate_transfers.account_number,
          'account_type', candidate_transfers.account_type,
          'bank_details_hash_snapshot', candidate_transfers.bank_details_hash_snapshot,
          'payee_entity_kind', candidate_transfers.payee_entity_kind,
          'payee_entity_id', CASE WHEN candidate_transfers.payee_entity_id IS NULL THEN NULL ELSE candidate_transfers.payee_entity_id::text END,
          'request_id', candidate_transfers.request_id,
          'idempotency_key', COALESCE(NULLIF(BTRIM(COALESCE(candidate_transfers.request_id, '')), ''), 'transfer:' || candidate_transfers.id::text),
          'funding_account_ref', v_batch_row.funding_account_ref,
          'rail_provider', candidate_transfers.rail_provider,
          'rail_env', candidate_transfers.rail_env,
          'transfer_group_key', candidate_transfers.transfer_group_key
        ))
        ORDER BY candidate_transfers.created_at_utc NULLS FIRST, candidate_transfers.id
      ), '[]'::jsonb) AS transfer_rows_json,
      COUNT(*)::integer AS transfer_count_value
    FROM candidate_transfers
  )
  SELECT selected_payload.transfer_ids_json,
         selected_payload.transfer_rows_json,
         selected_payload.transfer_count_value
  INTO v_transfer_ids,
       v_transfer_rows,
       v_transfer_count
  FROM selected_payload;

  IF COALESCE(v_transfer_count, 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'chunk_id', NULL,
      'chunk_type', 'TRANSFER_SUBMIT',
      'phase', 'SUBMIT_PROVIDER_TRANSFERS',
      'transfer_ids', '[]'::jsonb,
      'transfers', '[]'::jsonb,
      'unit_count', 0,
      'claimed_count', 0,
      'unattempted_eligible_count', COALESCE(v_unattempted_eligible_count, 0),
      'provider_submit_ready_count', COALESCE(v_provider_submit_ready_count, 0),
      'same_operation_authorised_auth_count', COALESCE(v_same_operation_authorised_auth_count, 0),
      'authorised_without_provider_submission_count', COALESCE(v_authorised_without_provider_submission_count, 0),
      'authorised_but_not_submit_ready_count', COALESCE(v_authorised_but_not_submit_ready_count, 0),
      'auth_request_state', v_auth_request_state,
      'auth_request_unsafe_reason', v_auth_request_unsafe_reason,
      'claim_blocker_code', v_claim_blocker_code,
      'attempted_but_unproven_count', COALESCE(v_attempted_but_unproven_count, 0),
      'provider_evidence_present_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_attempt_or_evidence_count', COALESCE(v_provider_attempt_or_evidence_count, 0),
      'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
      'classification_source', v_classification_source,
      'retry_mode', COALESCE(v_retry_mode, false),
      'failed_or_retryable_count', COALESCE(v_failed_or_retryable_count, 0),
      'terminal_count', COALESCE(v_terminal_count, 0),
      'remaining_count', 0,
      'remaining_submit_attempt_required', 0,
      'remaining_unattempted_submit_required', 0,
      'remaining_provider_submission_required', 0,
      'remaining_provider_evidence_required', COALESCE(v_remaining_provider_evidence_required, 0),
      'has_more', false,
      'has_more_submit_attempts', false,
      'provider_submit_diagnostic', v_provider_submit_diagnostic,
      'provider_submission_status', v_provider_submission_status,
      'review_reason_code', v_review_reason_code,
      'chunk_ids', '[]'::jsonb,
      'manual_resolution_required', false,
      'safe_retry_available', false,
      'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
      'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
      'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
      'stale_empty_submit_chunk_count', COALESCE(v_stale_empty_submit_chunk_count, 0),
      'unfinalised_submit_chunk_count', COALESCE(v_unfinalised_submit_chunk_count, 0),
      'provider_submission_unknown_count', COALESCE(v_provider_submission_unknown_count, 0),
      'provider_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'local_submit_chunk_claimed_count', COALESCE(v_local_submit_chunk_claimed_count, 0),
      'operation_submit_attempt_count', COALESCE(v_operation_submit_attempt_count, 0),
      'has_unproven_attempts', COALESCE(v_has_unproven_attempts, false)
    );
  END IF;

  SELECT (COUNT(*) + 1)::integer
  INTO v_sequence_no
  FROM public.banking_pay_operation_chunks AS operation_chunk
  WHERE operation_chunk.operation_id = p_operation_id
    AND operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS'
    AND operation_chunk.chunk_type = 'TRANSFER_SUBMIT';

  v_chunk_id := gen_random_uuid();
  v_provider_submit_diagnostic := jsonb_strip_nulls(COALESCE(v_provider_submit_diagnostic, '{}'::jsonb) || jsonb_build_object(
    'diagnostic_version', 1,
    'generated_at_utc', v_now::text,
    'provider_call_stage', 'PROVIDER_SUBMIT_CHUNK_CLAIMED',
    'provider_submission_status', 'NO_PROVIDER_SUBMISSION_ATTEMPTED',
    'provider_submission_attempted', false,
    'provider_request_sent', false,
    'provider_response_received', false,
    'provider_response_present', false,
    'provider_acceptance_evidence_present', false,
    'crash_safety_status_if_lock_expires', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK',
    'provider_request_impossible', false,
    'stale_submit_chunk', false,
    'unfinalised_submit_chunk', true,
    'manual_resolution_required', false,
    'safe_retry_available', false,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', p_operation_id::text,
    'chunk_id', v_chunk_id::text,
    'transfer_ids', COALESCE(v_transfer_ids, '[]'::jsonb)
  ));
  v_provider_submission_status := 'NO_PROVIDER_SUBMISSION_ATTEMPTED';
  v_review_reason_code := NULL::text;

  INSERT INTO public.banking_pay_operation_chunks (
    id,
    operation_id,
    phase,
    chunk_type,
    sequence_no,
    status,
    payload_json,
    result_json,
    error_json,
    unit_count,
    completed_count,
    failed_count,
    locked_by,
    lock_expires_at_utc,
    created_at_utc,
    started_at_utc,
    completed_at_utc,
    updated_at_utc
  )
  VALUES (
    v_chunk_id,
    p_operation_id,
    'SUBMIT_PROVIDER_TRANSFERS',
    'TRANSFER_SUBMIT',
    v_sequence_no,
    'RUNNING',
    jsonb_build_object(
      'pay_batch_id', p_pay_batch_id::text,
      'transfer_ids', v_transfer_ids,
      'transfers', v_transfer_rows,
      'claimed_at_utc', v_now,
      'lock_owner', v_lock_owner
    ),
    jsonb_build_object('provider_submit_diagnostic', v_provider_submit_diagnostic),
    NULL::jsonb,
    v_transfer_count,
    0,
    0,
    v_lock_owner,
    v_now + make_interval(secs => v_lock_seconds),
    v_now,
    v_now,
    NULL::timestamptz,
    v_now
  )
  RETURNING id INTO v_chunk_id;

  SELECT COUNT(*)::integer
  INTO v_remaining_count
  FROM pg_temp.tmp_provider_submit_eligible_transfer_ids AS eligible_remaining
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_operation_chunks AS active_chunk
    CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(active_chunk.payload_json->'transfer_ids', '[]'::jsonb)) AS active_transfer_id(value)
    WHERE active_chunk.operation_id = p_operation_id
      AND active_chunk.chunk_type = 'TRANSFER_SUBMIT'
      AND active_chunk.status = 'RUNNING'
      AND active_chunk.lock_expires_at_utc IS NOT NULL
      AND active_chunk.lock_expires_at_utc > v_now
      AND active_transfer_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND active_transfer_id.value::uuid = eligible_remaining.transfer_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'chunk_id', v_chunk_id::text,
    'chunk_type', 'TRANSFER_SUBMIT',
    'phase', 'SUBMIT_PROVIDER_TRANSFERS',
    'sequence_no', v_sequence_no,
    'lock_owner', v_lock_owner,
    'lock_expires_at_utc', (v_now + make_interval(secs => v_lock_seconds))::text,
    'transfer_ids', COALESCE(v_transfer_ids, '[]'::jsonb),
    'transfers', COALESCE(v_transfer_rows, '[]'::jsonb),
    'unit_count', COALESCE(v_transfer_count, 0),
    'claimed_count', COALESCE(v_transfer_count, 0),
    'unattempted_eligible_count', COALESCE(v_unattempted_eligible_count, 0),
    'provider_submit_ready_count', COALESCE(v_provider_submit_ready_count, 0),
    'same_operation_authorised_auth_count', COALESCE(v_same_operation_authorised_auth_count, 0),
    'authorised_without_provider_submission_count', COALESCE(v_authorised_without_provider_submission_count, 0),
    'authorised_but_not_submit_ready_count', COALESCE(v_authorised_but_not_submit_ready_count, 0),
    'auth_request_state', v_auth_request_state,
    'auth_request_unsafe_reason', v_auth_request_unsafe_reason,
    'claim_blocker_code', v_claim_blocker_code,
    'attempted_but_unproven_count', COALESCE(v_attempted_but_unproven_count, 0),
    'provider_evidence_present_count', COALESCE(v_provider_acceptance_evidence_count, 0),
    'provider_attempt_or_evidence_count', COALESCE(v_provider_attempt_or_evidence_count, 0),
    'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
    'classification_source', v_classification_source,
    'retry_mode', COALESCE(v_retry_mode, false),
    'failed_or_retryable_count', COALESCE(v_failed_or_retryable_count, 0),
    'terminal_count', COALESCE(v_terminal_count, 0),
    'remaining_count', COALESCE(v_remaining_count, 0),
    'remaining_submit_attempt_required', COALESCE(v_remaining_count, 0),
    'remaining_unattempted_submit_required', COALESCE(v_remaining_count, 0),
    'remaining_provider_submission_required', COALESCE(v_remaining_count, 0),
    'remaining_provider_evidence_required', COALESCE(v_remaining_provider_evidence_required, 0),
    'has_more', COALESCE(v_remaining_count, 0) > 0,
    'has_more_submit_attempts', COALESCE(v_remaining_count, 0) > 0,
    'provider_submit_diagnostic', v_provider_submit_diagnostic,
      'provider_submission_status', v_provider_submission_status,
      'review_reason_code', v_review_reason_code,
      'chunk_ids', jsonb_build_array(v_chunk_id::text),
      'manual_resolution_required', false,
      'safe_retry_available', false,
      'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
      'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
      'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
      'stale_empty_submit_chunk_count', COALESCE(v_stale_empty_submit_chunk_count, 0),
      'unfinalised_submit_chunk_count', COALESCE(v_unfinalised_submit_chunk_count, 0),
      'provider_submission_unknown_count', COALESCE(v_provider_submission_unknown_count, 0),
      'provider_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'local_submit_chunk_claimed_count', COALESCE(v_local_submit_chunk_claimed_count, 0),
      'operation_submit_attempt_count', COALESCE(v_operation_submit_attempt_count, 0),
      'has_unproven_attempts', COALESCE(v_has_unproven_attempts, false)
  );
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_operation_remittance_scope_seed(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_scope text DEFAULT 'ALL'::text,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_scope text := upper(BTRIM(COALESCE(p_scope, 'ALL')));
  v_created_count integer := 0;
  v_reused_count integer := 0;
  v_recipient_count integer := 0;
  v_stale_scope_skipped_count integer := 0;
  v_trigger text;
  v_only_confirmed boolean := false;
  v_configured_timing text := 'ON_EXECUTION';
  v_batch_status text := null;
  v_batch_schedule_kind text := null;
  v_execution_intent_json jsonb := '{}'::jsonb;
  v_settlement_confirmation_json jsonb := '{}'::jsonb;
  v_suppress_remittances boolean := false;
  v_scheduled_execution_eligible boolean := false;
  v_deferred_by_timing boolean := false;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'operation_id is required';
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'actor_user_id is required';
  END IF;

  IF v_scope NOT IN ('ALL', 'CANDIDATE', 'PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'p_scope must be ALL, CANDIDATE, PAYE, or UMBRELLA';
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'tms_users row % not found', p_actor_user_id;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_operations row % not found', p_operation_id;
  END IF;

  IF v_operation_row.operation_type NOT IN ('REMITTANCE_QUEUE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLEMENT') THEN
    RAISE EXCEPTION 'operation % is not a remittance-capable operation', p_operation_id;
  END IF;

  IF v_operation_row.pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id <> p_pay_batch_id THEN
    RAISE EXCEPTION 'operation % is for pay batch %, not %', p_operation_id, v_operation_row.pay_batch_id, p_pay_batch_id;
  END IF;

  IF v_operation_row.actor_user_id IS NOT NULL AND v_operation_row.actor_user_id <> p_actor_user_id THEN
    RAISE EXCEPTION 'operation % belongs to a different actor', p_operation_id;
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_batches row % not found', p_pay_batch_id;
  END IF;


  v_trigger := upper(btrim(coalesce(
    nullif(btrim(coalesce(v_operation_row.input_json->>'trigger', '')), ''),
    CASE
      WHEN v_operation_row.operation_type = 'PAYMENT_SETTLEMENT' THEN 'ON_PAYMENT_CONFIRMED'
      ELSE 'ON_EXECUTION'
    END
  )));

  v_only_confirmed := lower(btrim(coalesce(v_operation_row.input_json->>'only_confirmed', 'false'))) IN ('true','1','yes','y','on');

  IF v_trigger NOT IN ('ON_EXECUTION','ON_PAYMENT_CONFIRMED') THEN
    RAISE EXCEPTION 'unsupported remittance trigger %', v_trigger;
  END IF;

  SELECT COALESCE(NULLIF(btrim(public.settings_defaults.payment_remittance_send_timing), ''), 'ON_EXECUTION')
  INTO v_configured_timing
  FROM public.settings_defaults
  ORDER BY public.settings_defaults.id
  LIMIT 1;

  v_configured_timing := upper(coalesce(nullif(btrim(v_configured_timing), ''), 'ON_EXECUTION'));

  IF v_configured_timing NOT IN ('ON_EXECUTION','ON_PAYMENT_CONFIRMED') THEN
    v_configured_timing := 'ON_EXECUTION';
  END IF;

  v_deferred_by_timing := v_configured_timing <> v_trigger;

  IF v_batch_row.execution_intent_json IS NOT NULL AND jsonb_typeof(v_batch_row.execution_intent_json) = 'object' THEN
    v_execution_intent_json := v_batch_row.execution_intent_json;
  ELSE
    v_execution_intent_json := '{}'::jsonb;
  END IF;

  IF v_batch_row.settlement_confirmation_json IS NOT NULL AND jsonb_typeof(v_batch_row.settlement_confirmation_json) = 'object' THEN
    v_settlement_confirmation_json := v_batch_row.settlement_confirmation_json;
  ELSE
    v_settlement_confirmation_json := '{}'::jsonb;
  END IF;

  v_batch_status := upper(btrim(coalesce(v_batch_row.status, '')));
  v_batch_schedule_kind := upper(btrim(coalesce(v_batch_row.schedule_kind, v_execution_intent_json->>'schedule_kind', '')));

  IF v_deferred_by_timing THEN
    RETURN jsonb_build_object(
      'ok', true,
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'scope', v_scope,
      'trigger', v_trigger,
      'configured_timing', v_configured_timing,
      'only_confirmed', v_only_confirmed,
      'deferred', true,
      'suppressed', false,
      'scope_rows_created', 0,
      'scope_rows_reused', 0,
      'recipient_count', 0,
      'stale_scope_skipped_count', 0,
      'scheduled_execution_eligible', false,
      'trigger_status', 'REMITTANCE_SCOPE_DEFERRED_BY_CONFIGURED_TIMING'
    );
  END IF;

  v_suppress_remittances :=
    lower(btrim(coalesce(v_execution_intent_json->>'suppress_remittances', 'false'))) IN ('true','1','yes','y','on')
    OR lower(btrim(coalesce(v_execution_intent_json->>'suppress_remittances_pending', 'false'))) IN ('true','1','yes','y','on')
    OR lower(btrim(coalesce(v_settlement_confirmation_json->>'suppress_remittances', 'false'))) IN ('true','1','yes','y','on')
    OR lower(btrim(coalesce(v_settlement_confirmation_json->>'suppress_remittances_pending', 'false'))) IN ('true','1','yes','y','on')
    OR lower(btrim(coalesce(v_operation_row.input_json->>'suppress_remittances', 'false'))) IN ('true','1','yes','y','on');

  IF v_suppress_remittances THEN
    RETURN jsonb_build_object(
      'ok', true,
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text,
      'scope', v_scope,
      'trigger', v_trigger,
      'configured_timing', v_configured_timing,
      'only_confirmed', v_only_confirmed,
      'deferred', false,
      'suppressed', true,
      'scope_rows_created', 0,
      'scope_rows_reused', 0,
      'recipient_count', 0,
      'stale_scope_skipped_count', 0,
      'scheduled_execution_eligible', false,
      'trigger_status', 'REMITTANCE_SCOPE_SUPPRESSED'
    );
  END IF;

  v_scheduled_execution_eligible :=
    v_operation_row.operation_type = 'REMITTANCE_QUEUE'
    AND v_trigger = 'ON_EXECUTION'
    AND v_configured_timing = 'ON_EXECUTION'
    AND COALESCE(v_only_confirmed, false) = false
    AND v_batch_schedule_kind = 'SCHEDULED'
    AND v_batch_row.scheduled_at_utc IS NOT NULL
    AND v_batch_status NOT IN (
      'BLOCKED_FUNDS',
      'FAILED',
      'CANCELLED',
      'CANCELED',
      'REJECTED',
      'DECLINED',
      'RETURNED',
      'VOID',
      'DELETED'
    )
    AND lower(btrim(coalesce(v_execution_intent_json->>'suppress_remittances', 'false'))) NOT IN ('true','1','yes','y','on')
    AND (
      v_operation_row.root_operation_id IS NULL
      OR nullif(btrim(coalesce(v_execution_intent_json->>'operation_id', '')), '') IS NULL
      OR v_execution_intent_json->>'operation_id' = v_operation_row.root_operation_id::text
    );

  WITH eligible_item_rows AS (
    SELECT
      batch_candidate.id AS pay_batch_candidate_id,
      batch_candidate.candidate_id,
      upper(BTRIM(COALESCE(batch_item.pay_channel, ''))) AS pay_channel,
      batch_item.umbrella_id,
      batch_item.id AS pay_batch_item_id,
      batch_item.pay_bank_transfer_id,
      transfer_row.transfer_group_key,
      COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0)::numeric AS item_amount,
      CASE
        WHEN batch_item.pay_bank_transfer_id IS NOT NULL THEN 'transfer:' || batch_item.pay_bank_transfer_id::text
        ELSE 'batch_candidate:' || batch_candidate.id::text || ':channel:' || upper(BTRIM(COALESCE(batch_item.pay_channel, '')))
      END AS payment_scope_key
    FROM public.pay_batch_candidates AS batch_candidate
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.pay_batch_candidate_id = batch_candidate.id
    LEFT JOIN public.pay_bank_transfers AS transfer_row
      ON transfer_row.id = batch_item.pay_bank_transfer_id
    WHERE batch_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(batch_item.is_voided, false) = false
      AND COALESCE(batch_item.item_type, '') <> 'DEBT_CREATED'
      AND (
        v_scheduled_execution_eligible
        OR upper(COALESCE(batch_candidate.settlement_status, '')) IN ('SETTLED', 'PAID', 'CONFIRMED')
        OR upper(COALESCE(transfer_row.status, '')) IN ('SUBMITTED', 'COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED')
        OR upper(COALESCE(transfer_row.rail_state, '')) IN ('SUBMITTED', 'QUEUED', 'ACCEPTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'PENDING_SETTLEMENT', 'PENDING_CONFIRMATION', 'PENDING_SUBMISSION', 'COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED')
        OR NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json->>'provider_submission_id', '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json->>'submission_id', '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json->>'provider_transfer_id', '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json->>'transfer_id', '')), '') IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json->>'provider_payment_id', '')), '') IS NOT NULL
      )
      AND (
        v_scope = 'ALL'
        OR (v_scope IN ('CANDIDATE', 'PAYE') AND upper(BTRIM(COALESCE(batch_item.pay_channel, ''))) = 'PAYE')
        OR (v_scope = 'UMBRELLA' AND upper(BTRIM(COALESCE(batch_item.pay_channel, ''))) = 'UMBRELLA')
      )
  ), candidate_scope AS (
    SELECT
      eligible_item_rows.pay_batch_candidate_id,
      eligible_item_rows.candidate_id,
      'CANDIDATE'::text AS recipient_kind,
      eligible_item_rows.candidate_id AS recipient_id,
      'CANDIDATE_REMITTANCE'::text AS remittance_type,
      'remittance:batch:' || p_pay_batch_id::text || ':candidate:' || eligible_item_rows.candidate_id::text || ':scope:' || eligible_item_rows.payment_scope_key || ':type:CANDIDATE_REMITTANCE' AS deterministic_outbox_key,
      eligible_item_rows.payment_scope_key,
      eligible_item_rows.pay_bank_transfer_id,
      eligible_item_rows.transfer_group_key,
      COALESCE(jsonb_agg(to_jsonb(eligible_item_rows.pay_batch_item_id::text) ORDER BY eligible_item_rows.pay_batch_item_id::text), '[]'::jsonb) AS pay_batch_item_ids_json,
      COUNT(*)::integer AS item_count,
      ROUND(COALESCE(SUM(eligible_item_rows.item_amount), 0), 2) AS total_amount
    FROM eligible_item_rows
    WHERE eligible_item_rows.pay_channel = 'PAYE'
      AND v_scope IN ('ALL', 'CANDIDATE', 'PAYE')
    GROUP BY eligible_item_rows.pay_batch_candidate_id,
             eligible_item_rows.candidate_id,
             eligible_item_rows.payment_scope_key,
             eligible_item_rows.pay_bank_transfer_id,
             eligible_item_rows.transfer_group_key
    HAVING ROUND(COALESCE(SUM(eligible_item_rows.item_amount), 0), 2) <> 0
  ), umbrella_scope AS (
    SELECT
      eligible_item_rows.pay_batch_candidate_id,
      eligible_item_rows.candidate_id,
      'UMBRELLA'::text AS recipient_kind,
      eligible_item_rows.umbrella_id AS recipient_id,
      'UMBRELLA_REMITTANCE'::text AS remittance_type,
      'remittance:batch:' || p_pay_batch_id::text || ':candidate:' || eligible_item_rows.candidate_id::text || ':umbrella:' || eligible_item_rows.umbrella_id::text || ':scope:' || eligible_item_rows.payment_scope_key || ':type:UMBRELLA_REMITTANCE' AS deterministic_outbox_key,
      eligible_item_rows.payment_scope_key,
      eligible_item_rows.pay_bank_transfer_id,
      eligible_item_rows.transfer_group_key,
      COALESCE(jsonb_agg(to_jsonb(eligible_item_rows.pay_batch_item_id::text) ORDER BY eligible_item_rows.pay_batch_item_id::text), '[]'::jsonb) AS pay_batch_item_ids_json,
      COUNT(*)::integer AS item_count,
      ROUND(COALESCE(SUM(eligible_item_rows.item_amount), 0), 2) AS total_amount
    FROM eligible_item_rows
    WHERE eligible_item_rows.pay_channel = 'UMBRELLA'
      AND eligible_item_rows.umbrella_id IS NOT NULL
      AND v_scope IN ('ALL', 'UMBRELLA')
    GROUP BY eligible_item_rows.pay_batch_candidate_id,
             eligible_item_rows.candidate_id,
             eligible_item_rows.umbrella_id,
             eligible_item_rows.payment_scope_key,
             eligible_item_rows.pay_bank_transfer_id,
             eligible_item_rows.transfer_group_key
    HAVING ROUND(COALESCE(SUM(eligible_item_rows.item_amount), 0), 2) <> 0
  ), scope_rows AS (
    SELECT candidate_scope.pay_batch_candidate_id,
           candidate_scope.candidate_id,
           candidate_scope.recipient_kind,
           candidate_scope.recipient_id,
           candidate_scope.remittance_type,
           candidate_scope.deterministic_outbox_key,
           jsonb_build_object(
             'pay_batch_id', p_pay_batch_id::text,
             'pay_batch_candidate_id', candidate_scope.pay_batch_candidate_id::text,
             'candidate_id', candidate_scope.candidate_id::text,
             'recipient_kind', candidate_scope.recipient_kind,
             'recipient_id', candidate_scope.recipient_id::text,
             'remittance_type', candidate_scope.remittance_type,
             'payment_scope_json', jsonb_strip_nulls(jsonb_build_object(
               'scope_key', candidate_scope.payment_scope_key,
               'pay_bank_transfer_id', CASE WHEN candidate_scope.pay_bank_transfer_id IS NULL THEN NULL ELSE candidate_scope.pay_bank_transfer_id::text END,
               'transfer_group_key', candidate_scope.transfer_group_key,
               'remittance_trigger', CASE WHEN v_scheduled_execution_eligible THEN v_trigger ELSE NULL END,
               'configured_timing', CASE WHEN v_scheduled_execution_eligible THEN v_configured_timing ELSE NULL END,
               'scheduled_execution_eligible', CASE WHEN v_scheduled_execution_eligible THEN true ELSE NULL END,
               'scheduled_at_utc', CASE WHEN v_scheduled_execution_eligible AND v_batch_row.scheduled_at_utc IS NOT NULL THEN v_batch_row.scheduled_at_utc::text ELSE NULL END,
               'schedule_kind', CASE WHEN v_scheduled_execution_eligible THEN v_batch_schedule_kind ELSE NULL END
             )),
             'pay_batch_item_ids', candidate_scope.pay_batch_item_ids_json,
             'item_count', candidate_scope.item_count,
             'total_amount', candidate_scope.total_amount,
             'scope', v_scope
           ) AS payload_json
    FROM candidate_scope

    UNION ALL

    SELECT umbrella_scope.pay_batch_candidate_id,
           umbrella_scope.candidate_id,
           umbrella_scope.recipient_kind,
           umbrella_scope.recipient_id,
           umbrella_scope.remittance_type,
           umbrella_scope.deterministic_outbox_key,
           jsonb_build_object(
             'pay_batch_id', p_pay_batch_id::text,
             'pay_batch_candidate_id', umbrella_scope.pay_batch_candidate_id::text,
             'candidate_id', umbrella_scope.candidate_id::text,
             'recipient_kind', umbrella_scope.recipient_kind,
             'recipient_id', umbrella_scope.recipient_id::text,
             'remittance_type', umbrella_scope.remittance_type,
             'payment_scope_json', jsonb_strip_nulls(jsonb_build_object(
               'scope_key', umbrella_scope.payment_scope_key,
               'pay_bank_transfer_id', CASE WHEN umbrella_scope.pay_bank_transfer_id IS NULL THEN NULL ELSE umbrella_scope.pay_bank_transfer_id::text END,
               'transfer_group_key', umbrella_scope.transfer_group_key,
               'remittance_trigger', CASE WHEN v_scheduled_execution_eligible THEN v_trigger ELSE NULL END,
               'configured_timing', CASE WHEN v_scheduled_execution_eligible THEN v_configured_timing ELSE NULL END,
               'scheduled_execution_eligible', CASE WHEN v_scheduled_execution_eligible THEN true ELSE NULL END,
               'scheduled_at_utc', CASE WHEN v_scheduled_execution_eligible AND v_batch_row.scheduled_at_utc IS NOT NULL THEN v_batch_row.scheduled_at_utc::text ELSE NULL END,
               'schedule_kind', CASE WHEN v_scheduled_execution_eligible THEN v_batch_schedule_kind ELSE NULL END
             )),
             'pay_batch_item_ids', umbrella_scope.pay_batch_item_ids_json,
             'item_count', umbrella_scope.item_count,
             'total_amount', umbrella_scope.total_amount,
             'scope', v_scope
           ) AS payload_json
    FROM umbrella_scope
  ), stale_scope AS (
    UPDATE public.banking_pay_operation_remittance_scope AS scope_update
    SET status = 'SKIPPED',
        updated_at_utc = v_now
    WHERE scope_update.operation_id = p_operation_id
      AND scope_update.pay_batch_id = p_pay_batch_id
      AND scope_update.status IN ('PENDING', 'FAILED')
      AND NOT EXISTS (
        SELECT 1
        FROM scope_rows
        WHERE scope_rows.deterministic_outbox_key = scope_update.deterministic_outbox_key
      )
    RETURNING scope_update.id
  ), upserted_scope AS (
    INSERT INTO public.banking_pay_operation_remittance_scope (
      operation_id,
      pay_batch_id,
      pay_batch_candidate_id,
      candidate_id,
      recipient_kind,
      recipient_id,
      remittance_type,
      deterministic_outbox_key,
      payload_json,
      status,
      outbox_id,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      p_operation_id,
      p_pay_batch_id,
      scope_rows.pay_batch_candidate_id,
      scope_rows.candidate_id,
      scope_rows.recipient_kind,
      scope_rows.recipient_id,
      scope_rows.remittance_type,
      scope_rows.deterministic_outbox_key,
      scope_rows.payload_json,
      'PENDING',
      NULL::uuid,
      v_now,
      v_now
    FROM scope_rows
    ON CONFLICT (operation_id, deterministic_outbox_key)
    DO UPDATE
    SET pay_batch_candidate_id = CASE WHEN public.banking_pay_operation_remittance_scope.status = 'QUEUED' THEN public.banking_pay_operation_remittance_scope.pay_batch_candidate_id ELSE EXCLUDED.pay_batch_candidate_id END,
        candidate_id = CASE WHEN public.banking_pay_operation_remittance_scope.status = 'QUEUED' THEN public.banking_pay_operation_remittance_scope.candidate_id ELSE EXCLUDED.candidate_id END,
        recipient_kind = CASE WHEN public.banking_pay_operation_remittance_scope.status = 'QUEUED' THEN public.banking_pay_operation_remittance_scope.recipient_kind ELSE EXCLUDED.recipient_kind END,
        recipient_id = CASE WHEN public.banking_pay_operation_remittance_scope.status = 'QUEUED' THEN public.banking_pay_operation_remittance_scope.recipient_id ELSE EXCLUDED.recipient_id END,
        remittance_type = CASE WHEN public.banking_pay_operation_remittance_scope.status = 'QUEUED' THEN public.banking_pay_operation_remittance_scope.remittance_type ELSE EXCLUDED.remittance_type END,
        payload_json = CASE WHEN public.banking_pay_operation_remittance_scope.status = 'QUEUED' THEN public.banking_pay_operation_remittance_scope.payload_json ELSE EXCLUDED.payload_json END,
        status = CASE WHEN public.banking_pay_operation_remittance_scope.status = 'QUEUED' THEN public.banking_pay_operation_remittance_scope.status ELSE EXCLUDED.status END,
        updated_at_utc = v_now
    RETURNING public.banking_pay_operation_remittance_scope.id,
              public.banking_pay_operation_remittance_scope.recipient_kind,
              public.banking_pay_operation_remittance_scope.recipient_id,
              (xmax = 0) AS was_inserted
  )
  SELECT COUNT(*) FILTER (WHERE upserted_scope.was_inserted)::integer,
         COUNT(*) FILTER (WHERE upserted_scope.was_inserted IS NOT TRUE)::integer,
         COUNT(DISTINCT upserted_scope.recipient_kind || ':' || upserted_scope.recipient_id::text)::integer,
         COALESCE((SELECT COUNT(*)::integer FROM stale_scope), 0)
  INTO v_created_count,
       v_reused_count,
       v_recipient_count,
       v_stale_scope_skipped_count
  FROM upserted_scope;

  UPDATE public.banking_pay_operations AS operation_update
  SET pay_batch_id = p_pay_batch_id,
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id
    AND operation_update.pay_batch_id IS NULL;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'scope', v_scope,
    'trigger', v_trigger,
    'configured_timing', v_configured_timing,
    'only_confirmed', v_only_confirmed,
    'scheduled_execution_eligible', v_scheduled_execution_eligible,
    'deferred', false,
    'suppressed', false,
    'trigger_status', CASE WHEN v_scheduled_execution_eligible THEN 'SCHEDULED_ON_EXECUTION_SCOPE_SEEDED' ELSE 'STANDARD_REMITTANCE_SCOPE_SEEDED' END,
    'scope_rows_created', COALESCE(v_created_count, 0),
    'scope_rows_reused', COALESCE(v_reused_count, 0),
    'recipient_count', COALESCE(v_recipient_count, 0),
    'stale_scope_skipped_count', COALESCE(v_stale_scope_skipped_count, 0)
  );
END;
$function$;













create or replace function public.pay_batch_freshness_scope_seed(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_actor_user_id uuid default null::uuid,
  p_chunk_size integer default null::integer
)
returns jsonb
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
  v_operation public.banking_pay_operations%rowtype;
  v_batch public.pay_batches%rowtype;
  v_actor_is_valid boolean := true;
  v_chunk_size integer;
  v_units_json jsonb := '[]'::jsonb;
  v_unit_count integer := 0;
  v_scope_hash text := null;
  v_existing_scope_hash text := null;
  v_existing_batch_scope_hash text := null;
  v_existing_batch_freshness_status text := null;
  v_existing_batch_freshness_result_hash text := null;
  v_existing_batch_freshness_operation_id uuid := null;
  v_batch_execution_boundary_crossed boolean := false;
  v_seed_result record;
  v_config_candidate text := null;
begin
  if p_operation_id is null then
    raise exception 'PAY_BATCH_FRESHNESS_SCOPE_SEED_OPERATION_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_FRESHNESS_SCOPE_SEED_OPERATION_ID_REQUIRED')::text;
  end if;

  if p_pay_batch_id is null then
    raise exception 'PAY_BATCH_FRESHNESS_SCOPE_SEED_PAY_BATCH_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_FRESHNESS_SCOPE_SEED_PAY_BATCH_ID_REQUIRED')::text;
  end if;

  select operation_row.*
  into v_operation
  from public.banking_pay_operations as operation_row
  where operation_row.id = p_operation_id
  for update;

  if not found then
    raise exception 'PAY_BATCH_FRESHNESS_SCOPE_SEED_OPERATION_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_SCOPE_SEED_OPERATION_NOT_FOUND',
              'operation_id', p_operation_id::text
            )::text;
  end if;

  if v_operation.operation_type not in ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') then
    raise exception 'PAY_BATCH_FRESHNESS_SCOPE_SEED_UNSUPPORTED_OPERATION_TYPE'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_SCOPE_SEED_UNSUPPORTED_OPERATION_TYPE',
              'operation_id', p_operation_id::text,
              'operation_type', v_operation.operation_type
            )::text;
  end if;

  if v_operation.pay_batch_id is not null and v_operation.pay_batch_id <> p_pay_batch_id then
    raise exception 'PAY_BATCH_FRESHNESS_SCOPE_SEED_OPERATION_BATCH_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_SCOPE_SEED_OPERATION_BATCH_MISMATCH',
              'operation_id', p_operation_id::text,
              'operation_pay_batch_id', v_operation.pay_batch_id::text,
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  select pay_batch_row.*
  into v_batch
  from public.pay_batches as pay_batch_row
  where pay_batch_row.id = p_pay_batch_id;

  if not found then
    raise exception 'PAY_BATCH_FRESHNESS_SCOPE_SEED_BATCH_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_SCOPE_SEED_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  if p_actor_user_id is not null then
    select exists (
      select 1
      from public.tms_users as actor_user
      where actor_user.id = p_actor_user_id
        and coalesce(actor_user.is_active, false) = true
    )
    into v_actor_is_valid;

    if coalesce(v_actor_is_valid, false) is not true then
      raise exception 'PAY_BATCH_FRESHNESS_SCOPE_SEED_ACTOR_NOT_ALLOWED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'code', 'PAY_BATCH_FRESHNESS_SCOPE_SEED_ACTOR_NOT_ALLOWED',
                'actor_user_id', p_actor_user_id::text
              )::text;
    end if;
  end if;

  v_config_candidate := coalesce(
    nullif(btrim(v_operation.config_json #>> '{VALIDATE_FRESHNESS,FRESHNESS_VALIDATE,chunk_size}'), ''),
    nullif(btrim(v_operation.config_json #>> '{freshness,chunk_size}'), ''),
    nullif(btrim(v_operation.config_json ->> 'freshness_chunk_size'), ''),
    nullif(btrim(v_operation.config_json ->> 'BANKING_PAY_FRESHNESS_CHUNK_SIZE'), '')
  );

  if p_chunk_size is not null then
    v_chunk_size := p_chunk_size;
  elsif v_config_candidate is not null and v_config_candidate ~ '^[0-9]+$' then
    v_chunk_size := v_config_candidate::integer;
  else
    v_chunk_size := 50;
  end if;

  if v_chunk_size < 1 then
    v_chunk_size := 1;
  elsif v_chunk_size > 250 then
    v_chunk_size := 250;
  end if;

  with item_scope as (
    select
      pay_batch_item_scope.id as pay_batch_item_id,
      pay_batch_candidate_scope.id as pay_batch_candidate_id,
      pay_batch_candidate_scope.candidate_id,
      pay_batch_item_scope.pay_channel,
      pay_batch_item_scope.timesheet_id,
      pay_batch_item_scope.frozen_component_key_type,
      pay_batch_item_scope.frozen_component_key_value,
      md5(coalesce(pay_batch_item_scope.frozen_source_basis_json::text, 'null')) as frozen_source_basis_hash
    from public.pay_batch_items as pay_batch_item_scope
    join public.pay_batch_candidates as pay_batch_candidate_scope
      on pay_batch_candidate_scope.id = pay_batch_item_scope.pay_batch_candidate_id
    where pay_batch_candidate_scope.pay_batch_id = p_pay_batch_id
      and coalesce(pay_batch_item_scope.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as correction_scope_exclusion
        where correction_scope_exclusion.pay_batch_item_id = pay_batch_item_scope.id
          and correction_scope_exclusion.status = 'APPLIED'
          and correction_scope_exclusion.correction_item_kind in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
      )
  ),
  snapshot_scope as (
    select
      timesheet_snapshot_scope.id as snapshot_id,
      timesheet_snapshot_scope.pay_batch_id,
      timesheet_snapshot_scope.timesheet_id,
      timesheet_snapshot_scope.candidate_id,
      timesheet_snapshot_scope.pay_channel
    from public.pay_batch_timesheet_snapshots as timesheet_snapshot_scope
    where timesheet_snapshot_scope.pay_batch_id = p_pay_batch_id
  ),
  unit_base as (
    select
      coalesce(snapshot_scope.timesheet_id, item_scope.timesheet_id) as timesheet_id,
      snapshot_scope.snapshot_id,
      coalesce(snapshot_scope.candidate_id, item_scope.candidate_id) as candidate_id,
      item_scope.pay_batch_candidate_id,
      coalesce(snapshot_scope.pay_channel, item_scope.pay_channel) as pay_channel,
      coalesce(
        jsonb_agg(item_scope.pay_batch_item_id::text order by item_scope.pay_batch_item_id) filter (where item_scope.pay_batch_item_id is not null),
        '[]'::jsonb
      ) as pay_batch_item_ids,
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'pay_batch_item_id', item_scope.pay_batch_item_id::text,
            'frozen_component_key_type', item_scope.frozen_component_key_type,
            'frozen_component_key_value', item_scope.frozen_component_key_value,
            'frozen_source_basis_hash', item_scope.frozen_source_basis_hash
          )
          order by item_scope.pay_batch_item_id
        ) filter (where item_scope.pay_batch_item_id is not null),
        '[]'::jsonb
      ) as frozen_key_summary
    from snapshot_scope
    full join item_scope
      on item_scope.timesheet_id = snapshot_scope.timesheet_id
     and item_scope.candidate_id = snapshot_scope.candidate_id
     and coalesce(item_scope.pay_channel, '') = coalesce(snapshot_scope.pay_channel, '')
    group by
      coalesce(snapshot_scope.timesheet_id, item_scope.timesheet_id),
      snapshot_scope.snapshot_id,
      coalesce(snapshot_scope.candidate_id, item_scope.candidate_id),
      item_scope.pay_batch_candidate_id,
      coalesce(snapshot_scope.pay_channel, item_scope.pay_channel)
  ),
  ordered_units as (
    select
      unit_base.timesheet_id,
      unit_base.snapshot_id,
      unit_base.candidate_id,
      unit_base.pay_batch_candidate_id,
      unit_base.pay_channel,
      unit_base.pay_batch_item_ids,
      unit_base.frozen_key_summary,
      row_number() over (
        order by
          unit_base.candidate_id nulls last,
          unit_base.timesheet_id nulls last,
          unit_base.snapshot_id nulls last,
          unit_base.pay_batch_candidate_id nulls last,
          unit_base.pay_channel nulls last
      ) as unit_ordinal
    from unit_base
  )
  select
    coalesce(jsonb_agg(
      jsonb_build_object(
        'unit_ordinal', ordered_units.unit_ordinal,
        'timesheet_id', case when ordered_units.timesheet_id is null then null else ordered_units.timesheet_id::text end,
        'snapshot_id', case when ordered_units.snapshot_id is null then null else ordered_units.snapshot_id::text end,
        'pay_channel', ordered_units.pay_channel,
        'candidate_id', case when ordered_units.candidate_id is null then null else ordered_units.candidate_id::text end,
        'pay_batch_candidate_id', case when ordered_units.pay_batch_candidate_id is null then null else ordered_units.pay_batch_candidate_id::text end,
        'pay_batch_item_ids', ordered_units.pay_batch_item_ids,
        'frozen_key_summary', ordered_units.frozen_key_summary
      )
      order by ordered_units.unit_ordinal
    ), '[]'::jsonb)
  into v_units_json
  from ordered_units;

  v_unit_count := jsonb_array_length(coalesce(v_units_json, '[]'::jsonb));

  v_scope_hash := md5(jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'units', coalesce(v_units_json, '[]'::jsonb)
  )::text);

  v_existing_scope_hash := nullif(btrim(coalesce(v_operation.progress_json->>'freshness_scope_hash', '')), '');
  v_existing_batch_scope_hash := nullif(btrim(coalesce(v_batch.freshness_scope_hash, '')), '');
  v_existing_batch_freshness_status := nullif(upper(btrim(coalesce(v_batch.freshness_validation_status, ''))), '');
  v_existing_batch_freshness_result_hash := nullif(btrim(coalesce(v_batch.freshness_result_hash, '')), '');
  v_existing_batch_freshness_operation_id := v_batch.freshness_operation_id;
  v_batch_execution_boundary_crossed := (
    upper(btrim(coalesce(v_batch.execution_commit_state, 'NOT_SUBMITTED'))) NOT IN ('', 'NOT_SUBMITTED', 'NOT_STARTED', 'DRAFT', 'PENDING')
    OR upper(btrim(coalesce(v_batch.status, ''))) IN ('EXECUTING', 'EXECUTED', 'COMPLETED', 'SETTLED', 'PAID')
    OR v_batch.execution_committed_at_utc IS NOT NULL
  );

  if v_existing_scope_hash is not null and v_existing_scope_hash <> v_scope_hash then
    raise exception 'CHUNK_SCOPE_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'CHUNK_SCOPE_MISMATCH',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'existing_freshness_scope_hash', v_existing_scope_hash,
              'requested_freshness_scope_hash', v_scope_hash
            )::text;
  end if;

  if v_existing_batch_scope_hash is not null and v_existing_batch_scope_hash <> v_scope_hash then
    if v_batch_execution_boundary_crossed then
      raise exception 'FRESHNESS_SCOPE_CHANGED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'code', 'FRESHNESS_SCOPE_CHANGED',
                'operation_id', p_operation_id::text,
                'pay_batch_id', p_pay_batch_id::text,
                'existing_freshness_scope_hash', v_existing_batch_scope_hash,
                'requested_freshness_scope_hash', v_scope_hash,
                'freshness_validation_status', v_existing_batch_freshness_status,
                'freshness_result_hash', v_existing_batch_freshness_result_hash,
                'freshness_operation_id', case when v_existing_batch_freshness_operation_id is null then null else v_existing_batch_freshness_operation_id::text end,
                'message', 'Stored freshness scope no longer matches the frozen batch scope after the execution boundary.'
              )::text;
    else
      update public.pay_batches as stale_freshness_reset
      set freshness_operation_id = null,
          freshness_validation_status = null,
          freshness_checked_at_utc = null,
          freshness_result_hash = null,
          freshness_scope_hash = null,
          freshness_result_json = null
      where stale_freshness_reset.id = p_pay_batch_id
        and nullif(btrim(coalesce(stale_freshness_reset.freshness_scope_hash, '')), '') = v_existing_batch_scope_hash;
    end if;
  end if;

  select seed_result.total_units,
         seed_result.chunk_count,
         seed_result.existing_chunk_count,
         seed_result.new_chunk_count
  into v_seed_result
  from public.banking_pay_operation_seed_chunks(
    p_operation_id,
    'VALIDATE_FRESHNESS',
    'FRESHNESS_VALIDATE',
    v_chunk_size,
    coalesce(v_units_json, '[]'::jsonb)
  ) as seed_result;

  update public.banking_pay_operations as operation_update
  set progress_json = coalesce(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'freshness_scope_hash', v_scope_hash,
          'freshness_unit_count', v_unit_count,
          'freshness_chunk_count', coalesce(v_seed_result.chunk_count, 0),
          'freshness_seeded_at_utc', to_jsonb(now())
        ),
      total_units = case when coalesce(operation_update.total_units, 0) = 0 then coalesce(v_seed_result.total_units, 0) else operation_update.total_units end,
      chunk_count = case when coalesce(operation_update.chunk_count, 0) = 0 then coalesce(v_seed_result.chunk_count, 0) else operation_update.chunk_count end,
      updated_at_utc = now()
  where operation_update.id = p_operation_id;

  update public.pay_batches as pay_batch_update
  set freshness_operation_id = p_operation_id,
      freshness_validation_status = 'PENDING',
      freshness_checked_at_utc = null,
      freshness_result_hash = null,
      freshness_scope_hash = v_scope_hash,
      freshness_result_json = jsonb_build_object(
        'validation_complete', false,
        'is_stale', false,
        'operation_id', p_operation_id::text,
        'freshness_scope_hash', v_scope_hash,
        'seeded_at_utc', now()
      )
  where pay_batch_update.id = p_pay_batch_id;

  return jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', p_operation_id::text,
    'unit_count', coalesce(v_seed_result.total_units, 0),
    'chunk_count', coalesce(v_seed_result.chunk_count, 0),
    'existing_chunk_count', coalesce(v_seed_result.existing_chunk_count, 0),
    'new_chunk_count', coalesce(v_seed_result.new_chunk_count, 0),
    'freshness_scope_hash', v_scope_hash,
    'chunk_size', v_chunk_size
  );
end;
$$;





create or replace function public.pay_batch_validate_freshness_chunk(
  p_operation_id uuid,
  p_chunk_id uuid,
  p_pay_batch_id uuid,
  p_actor_user_id uuid default null::uuid,
  p_diff_limit integer default 50
)
returns jsonb
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
  v_operation public.banking_pay_operations%rowtype;
  v_chunk public.banking_pay_operation_chunks%rowtype;
  v_batch public.pay_batches%rowtype;
  v_actor_is_valid boolean := true;
  v_diff_limit integer;
  v_timesheet_ids uuid[] := array[]::uuid[];
  v_item_ids uuid[] := array[]::uuid[];
  v_candidate_ids uuid[] := array[]::uuid[];
  v_checked_units integer := 0;
  v_checked_item_count integer := 0;
  v_checked_timesheet_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_key_diff_count integer := 0;
  v_other_reservation_count integer := 0;
  v_finance_reservation_diff_count integer := 0;
  v_snooze_count integer := 0;
  v_restructure_or_writeoff_count integer := 0;
  v_timesheet_override_count integer := 0;
  v_deduction_diff_count integer := 0;
  v_candidate_scope_deduction_diff_count integer := 0;
  v_stale_count integer := 0;
  v_is_stale boolean := false;
  v_reasons text[] := array[]::text[];
  v_stale_reason_counts jsonb := '{}'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;
  v_chunk_result_hash text := null;
  v_result jsonb := '{}'::jsonb;
begin
  if p_operation_id is null then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_ID_REQUIRED')::text;
  end if;

  if p_chunk_id is null then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_CHUNK_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_CHUNK_ID_REQUIRED')::text;
  end if;

  if p_pay_batch_id is null then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_PAY_BATCH_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_PAY_BATCH_ID_REQUIRED')::text;
  end if;

  v_diff_limit := coalesce(p_diff_limit, 50);
  if v_diff_limit < 0 then
    v_diff_limit := 0;
  elsif v_diff_limit > 250 then
    v_diff_limit := 250;
  end if;

  select operation_row.*
  into v_operation
  from public.banking_pay_operations as operation_row
  where operation_row.id = p_operation_id;

  if not found then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_NOT_FOUND',
              'operation_id', p_operation_id::text
            )::text;
  end if;

  if v_operation.pay_batch_id is not null and v_operation.pay_batch_id <> p_pay_batch_id then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_BATCH_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_BATCH_MISMATCH',
              'operation_id', p_operation_id::text,
              'operation_pay_batch_id', v_operation.pay_batch_id::text,
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  select pay_batch_row.*
  into v_batch
  from public.pay_batches as pay_batch_row
  where pay_batch_row.id = p_pay_batch_id;

  if not found then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_BATCH_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  if p_actor_user_id is not null then
    select exists (
      select 1
      from public.tms_users as actor_user
      where actor_user.id = p_actor_user_id
        and coalesce(actor_user.is_active, false) = true
    )
    into v_actor_is_valid;

    if coalesce(v_actor_is_valid, false) is not true then
      raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_ACTOR_NOT_ALLOWED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_ACTOR_NOT_ALLOWED',
                'actor_user_id', p_actor_user_id::text
              )::text;
    end if;
  end if;

  select chunk_row.*
  into v_chunk
  from public.banking_pay_operation_chunks as chunk_row
  where chunk_row.id = p_chunk_id;

  if not found then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_NOT_FOUND',
              'chunk_id', p_chunk_id::text
            )::text;
  end if;

  if v_chunk.operation_id <> p_operation_id then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_OPERATION_MISMATCH',
              'chunk_id', p_chunk_id::text,
              'chunk_operation_id', v_chunk.operation_id::text,
              'operation_id', p_operation_id::text
            )::text;
  end if;

  if v_chunk.phase <> 'VALIDATE_FRESHNESS' or v_chunk.chunk_type <> 'FRESHNESS_VALIDATE' then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_INVALID_CHUNK_KIND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_INVALID_CHUNK_KIND',
              'chunk_id', p_chunk_id::text,
              'phase', v_chunk.phase,
              'chunk_type', v_chunk.chunk_type
            )::text;
  end if;

  if jsonb_typeof(v_chunk.payload_json) <> 'object' or jsonb_typeof(v_chunk.payload_json->'units') <> 'array' then
    raise exception 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_INVALID_PAYLOAD'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_VALIDATE_FRESHNESS_CHUNK_INVALID_PAYLOAD',
              'chunk_id', p_chunk_id::text
            )::text;
  end if;

  select coalesce(array_agg(distinct (unit_value->>'timesheet_id')::uuid) filter (where nullif(unit_value->>'timesheet_id', '') is not null), array[]::uuid[]),
         coalesce(array_agg(distinct (item_id_text.item_id)::uuid) filter (where item_id_text.item_id is not null), array[]::uuid[]),
         coalesce(array_agg(distinct (unit_value->>'candidate_id')::uuid) filter (where nullif(unit_value->>'candidate_id', '') is not null), array[]::uuid[]),
         count(*)::integer
  into v_timesheet_ids, v_item_ids, v_candidate_ids, v_checked_units
  from jsonb_array_elements(v_chunk.payload_json->'units') as unit_items(unit_value)
  left join lateral jsonb_array_elements_text(coalesce(unit_items.unit_value->'pay_batch_item_ids', '[]'::jsonb)) as item_id_text(item_id)
    on true;

  v_checked_timesheet_count := coalesce(array_length(v_timesheet_ids, 1), 0);
  v_checked_item_count := coalesce(array_length(v_item_ids, 1), 0);

  create temporary table if not exists pg_temp.tmp_validate_freshness_chunk_diffs (
    reason text not null,
    timesheet_id uuid null,
    pay_batch_item_id uuid null,
    candidate_id uuid null,
    key_type text null,
    key_value text null,
    expected jsonb null,
    actual jsonb null,
    ord integer not null
  ) on commit drop;

  truncate table pg_temp.tmp_validate_freshness_chunk_diffs;

  if v_checked_item_count > 0 then
  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
      reason,
      timesheet_id,
      pay_batch_item_id,
      candidate_id,
      key_type,
      key_value,
      expected,
      actual,
      ord
    )
    select
      'KEY_RESOLUTION_FAILED',
      batch_component.timesheet_id,
      batch_component.pay_batch_item_id,
      null::uuid,
      batch_component.key_type,
      batch_component.key_value,
      jsonb_build_object('batch_component', batch_component.pay_batch_item_id::text),
      jsonb_build_object('failure_reason', batch_component.key_resolution_failure_reason),
      10
    from public._pay_batch_item_economic_components(null::uuid, v_item_ids) as batch_component
    where nullif(btrim(coalesce(batch_component.key_resolution_failure_reason, '')), '') is not null;
  
    get diagnostics v_key_resolution_failure_count = row_count;
  
    insert into pg_temp.tmp_validate_freshness_chunk_diffs (
      reason,
      timesheet_id,
      pay_batch_item_id,
      candidate_id,
      key_type,
      key_value,
      expected,
      actual,
      ord
    )
    with batch_components as (
      select
        batch_component.timesheet_id,
        upper(nullif(btrim(coalesce(batch_component.key_type, '')), '')) as key_type,
        nullif(btrim(coalesce(batch_component.key_value, '')), '') as key_value,
        round(sum(coalesce(batch_component.source_amount_ex_vat, 0)), 2)::numeric as batch_source_ex_vat
      from public._pay_batch_item_economic_components(null::uuid, v_item_ids) as batch_component
      where batch_component.item_type in ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
        and nullif(btrim(coalesce(batch_component.key_type, '')), '') is not null
        and nullif(btrim(coalesce(batch_component.key_value, '')), '') is not null
      group by batch_component.timesheet_id, upper(nullif(btrim(coalesce(batch_component.key_type, '')), '')), nullif(btrim(coalesce(batch_component.key_value, '')), '')
    ),
    current_components as (
      select
        current_component.timesheet_id,
        upper(nullif(btrim(coalesce(current_component.key_type, '')), '')) as key_type,
        nullif(btrim(coalesce(current_component.key_value, '')), '') as key_value,
        round(sum(coalesce(current_component.truth_ex_vat, 0) - coalesce(current_component.baseline_ex_vat, 0)), 2)::numeric as current_available_ex_vat
      from public._pay_current_timesheet_entitlement_components(v_timesheet_ids) as current_component
      where nullif(btrim(coalesce(current_component.key_type, '')), '') is not null
        and nullif(btrim(coalesce(current_component.key_value, '')), '') is not null
      group by current_component.timesheet_id, upper(nullif(btrim(coalesce(current_component.key_type, '')), '')), nullif(btrim(coalesce(current_component.key_value, '')), '')
    ),
    comparison_keys as (
      select batch_components.timesheet_id, batch_components.key_type, batch_components.key_value
      from batch_components
      union
      select current_components.timesheet_id, current_components.key_type, current_components.key_value
      from current_components
    )
    select
      'ECONOMIC_KEY_CHANGED',
      comparison_keys.timesheet_id,
      null::uuid,
      null::uuid,
      comparison_keys.key_type,
      comparison_keys.key_value,
      jsonb_build_object('batch_source_ex_vat', round(coalesce(batch_components.batch_source_ex_vat, 0), 2)),
      jsonb_build_object('current_available_ex_vat', round(coalesce(current_components.current_available_ex_vat, 0), 2)),
      20
    from comparison_keys
    left join batch_components
      on batch_components.timesheet_id is not distinct from comparison_keys.timesheet_id
     and batch_components.key_type = comparison_keys.key_type
     and batch_components.key_value = comparison_keys.key_value
    left join current_components
      on current_components.timesheet_id is not distinct from comparison_keys.timesheet_id
     and current_components.key_type = comparison_keys.key_type
     and current_components.key_value = comparison_keys.key_value
    where round(coalesce(batch_components.batch_source_ex_vat, 0), 2) <> round(coalesce(current_components.current_available_ex_vat, 0), 2);
  
    get diagnostics v_key_diff_count = row_count;
  
  
  else
    v_key_resolution_failure_count := 0;
    v_key_diff_count := 0;
  end if;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  select
    'OTHER_ACTIVE_RESERVATION',
    reserved_item.timesheet_id,
    reserved_item.id,
    reserved_candidate.candidate_id,
    reserved_component.key_type,
    reserved_component.key_value,
    jsonb_build_object('this_pay_batch_id', p_pay_batch_id::text),
    jsonb_build_object('other_pay_batch_id', reserved_candidate.pay_batch_id::text, 'reserved_ex_vat', reserved_component.source_amount_ex_vat),
    30
  from public.pay_batch_items as reserved_item
  join public.pay_batch_candidates as reserved_candidate
    on reserved_candidate.id = reserved_item.pay_batch_candidate_id
  join public.pay_batches as reserved_batch
    on reserved_batch.id = reserved_candidate.pay_batch_id
  join public._pay_batch_item_economic_components(null::uuid, array[reserved_item.id]) as reserved_component
    on reserved_component.pay_batch_item_id = reserved_item.id
  where reserved_item.timesheet_id = any(v_timesheet_ids)
    and reserved_candidate.pay_batch_id <> p_pay_batch_id
    and coalesce(reserved_item.is_voided, false) = false
    and public._pay_batch_status_is_active_reservation(reserved_batch.status)
    and reserved_item.item_type in ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
    and not exists (
      select 1
      from public.pay_payment_correction_items as reserved_correction_exclusion
      where reserved_correction_exclusion.pay_batch_item_id = reserved_item.id
        and reserved_correction_exclusion.status = 'APPLIED'
        and reserved_correction_exclusion.correction_item_kind in ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
    );

  get diagnostics v_other_reservation_count = row_count;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  select
    'FINANCE_RESERVATION_CHANGED',
    batch_item.timesheet_id,
    batch_item.id,
    batch_candidate.candidate_id,
    'FINANCE_RESERVATION',
    coalesce(batch_item.finance_case_id::text, batch_item.reservation_id::text, batch_item.finance_component_id::text, batch_item.id::text),
    jsonb_build_object('expected_status', case when upper(coalesce(v_batch.status, '')) in ('AUTHORISED_FOR_PAYMENT', 'SCHEDULED', 'EXECUTING') then 'COMMITTED' else 'RESERVED' end),
    jsonb_build_object('actual_status', reservation_row.status, 'reservation_id', reservation_row.id::text),
    40
  from public.pay_batch_items as batch_item
  join public.pay_batch_candidates as batch_candidate
    on batch_candidate.id = batch_item.pay_batch_candidate_id
  left join public.pay_advance_reservations as reservation_row
    on reservation_row.pay_batch_item_id = batch_item.id
  where batch_item.id = any(v_item_ids)
    and batch_item.finance_case_id is not null
    and (
      reservation_row.id is null
      or upper(coalesce(reservation_row.status, '')) <> case when upper(coalesce(v_batch.status, '')) in ('AUTHORISED_FOR_PAYMENT', 'SCHEDULED', 'EXECUTING') then 'COMMITTED' else 'RESERVED' end
    );

  get diagnostics v_finance_reservation_diff_count = row_count;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  with chunk_item_scope as (
    select
      batch_item.id as pay_batch_item_id,
      batch_item.timesheet_id,
      batch_candidate.candidate_id,
      nullif(btrim(coalesce(batch_item.source_ref, '')), '') as source_ref,
      nullif(btrim(coalesce(batch_item.segment_key, '')), '') as segment_key,
      batch_item.finance_case_id,
      batch_item.finance_component_id,
      upper(btrim(coalesce(batch_item.item_type, ''))) as item_type
    from public.pay_batch_items as batch_item
    join public.pay_batch_candidates as batch_candidate
      on batch_candidate.id = batch_item.pay_batch_candidate_id
    where batch_item.id = any(v_item_ids)
      and batch_candidate.pay_batch_id = p_pay_batch_id
  ), chunk_timesheet_scope as (
    select
      timesheet_row.timesheet_id,
      nullif(btrim(coalesce(timesheet_row.booking_id, '')), '') as booking_id
    from public.timesheets as timesheet_row
    where timesheet_row.timesheet_id = any(v_timesheet_ids)
  ), scoped_snoozes as (
    select distinct
      snooze_row.id,
      snooze_row.timesheet_id,
      snooze_row.candidate_id,
      snooze_row.source_ref,
      snooze_row.segment_stable_key,
      snooze_row.segment_id,
      snooze_row.booking_id,
      snooze_row.snooze_kind,
      snooze_row.snooze_until_date
    from public.pay_item_snoozes as snooze_row
    where snooze_row.cleared_at_utc is null
      and snooze_row.cancelled_at_utc is null
      and (
        (snooze_row.timesheet_id is not null and snooze_row.timesheet_id = any(v_timesheet_ids))
        or exists (
          select 1
          from chunk_item_scope as source_ref_scope
          where source_ref_scope.source_ref is not null
            and nullif(btrim(coalesce(snooze_row.source_ref, '')), '') = source_ref_scope.source_ref
        )
        or exists (
          select 1
          from chunk_item_scope as segment_scope
          where segment_scope.segment_key is not null
            and (
              nullif(btrim(coalesce(snooze_row.segment_id, '')), '') = segment_scope.segment_key
              or nullif(btrim(coalesce(snooze_row.segment_stable_key, '')), '') = segment_scope.segment_key
            )
        )
        or exists (
          select 1
          from chunk_timesheet_scope as booking_scope
          where booking_scope.booking_id is not null
            and nullif(btrim(coalesce(snooze_row.booking_id, '')), '') = booking_scope.booking_id
        )
        or (
          snooze_row.candidate_id = any(v_candidate_ids)
          and snooze_row.timesheet_id is null
          and nullif(btrim(coalesce(snooze_row.source_ref, '')), '') is null
          and nullif(btrim(coalesce(snooze_row.segment_id, '')), '') is null
          and nullif(btrim(coalesce(snooze_row.segment_stable_key, '')), '') is null
          and nullif(btrim(coalesce(snooze_row.booking_id, '')), '') is null
          and upper(btrim(coalesce(snooze_row.snooze_kind, ''))) in ('FINANCE', 'FINANCE_CASE', 'LOAN', 'OVERPAYMENT', 'MANUAL_DEBT', 'PAY_ADVANCE', 'RECOVERY')
          and exists (
            select 1
            from chunk_item_scope as finance_scope
            where finance_scope.candidate_id = snooze_row.candidate_id
              and (finance_scope.finance_case_id is not null or finance_scope.finance_component_id is not null)
          )
        )
      )
  )
  select
    'ACTIVE_SNOOZE_CHANGED',
    scoped_snoozes.timesheet_id,
    null::uuid,
    scoped_snoozes.candidate_id,
    'SNOOZE',
    coalesce(scoped_snoozes.source_ref, scoped_snoozes.segment_stable_key, scoped_snoozes.segment_id, scoped_snoozes.booking_id, scoped_snoozes.id::text),
    jsonb_build_object('expected', 'no_active_snooze_affecting_batch_scope'),
    jsonb_build_object('snooze_id', scoped_snoozes.id::text, 'snooze_kind', scoped_snoozes.snooze_kind, 'snooze_until_date', scoped_snoozes.snooze_until_date),
    50
  from scoped_snoozes;

  get diagnostics v_snooze_count = row_count;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  select
    'FINANCE_CASE_RESTRUCTURE_OR_WRITEOFF_CHANGED',
    batch_item.timesheet_id,
    batch_item.id,
    batch_candidate.candidate_id,
    'FINANCE_CASE',
    batch_item.finance_case_id::text,
    jsonb_build_object('expected', 'finance_case_open_and_not_written_off'),
    jsonb_build_object('advance_status', advance_row.status, 'written_off_at_utc', advance_row.written_off_at_utc),
    60
  from public.pay_batch_items as batch_item
  join public.pay_batch_candidates as batch_candidate
    on batch_candidate.id = batch_item.pay_batch_candidate_id
  join public.pay_advances as advance_row
    on advance_row.id = batch_item.finance_case_id
  where batch_item.id = any(v_item_ids)
    and batch_item.finance_case_id is not null
    and (
      advance_row.written_off_at_utc is not null
      or upper(coalesce(advance_row.status::text, '')) in ('CANCELLED', 'PAID_OFF')
    );

  get diagnostics v_restructure_or_writeoff_count = row_count;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  select
    'TIMESHEET_PAYMENT_OVERRIDE_CHANGED',
    current_financials.timesheet_id,
    null::uuid,
    current_financials.candidate_id,
    'TIMESHEET_PAYMENT_OVERRIDE',
    current_financials.timesheet_id::text,
    jsonb_build_object('expected', 'not_on_hold_or_already_paid_after_batch'),
    jsonb_build_object('pay_on_hold', current_financials.pay_on_hold, 'pay_on_hold_reason', current_financials.pay_on_hold_reason, 'paid_at_utc', current_financials.paid_at_utc),
    70
  from public.timesheets_financials as current_financials
  where current_financials.timesheet_id = any(v_timesheet_ids)
    and current_financials.is_current = true
    and (
      coalesce(current_financials.pay_on_hold, false) = true
      or current_financials.paid_at_utc is not null
    );

  get diagnostics v_timesheet_override_count = row_count;

  insert into pg_temp.tmp_validate_freshness_chunk_diffs (
    reason,
    timesheet_id,
    pay_batch_item_id,
    candidate_id,
    key_type,
    key_value,
    expected,
    actual,
    ord
  )
  with candidate_finance_items as (
    select
      batch_candidate.candidate_id,
      batch_item.id as pay_batch_item_id,
      batch_item.finance_case_id,
      batch_item.finance_component_id,
      round(abs(coalesce(batch_item.amount_ex_vat, 0)), 2)::numeric as item_amount_ex_vat
    from public.pay_batch_items as batch_item
    join public.pay_batch_candidates as batch_candidate
      on batch_candidate.id = batch_item.pay_batch_candidate_id
    where batch_candidate.pay_batch_id = p_pay_batch_id
      and batch_candidate.candidate_id = any(v_candidate_ids)
      and coalesce(batch_item.is_voided, false) = false
      and batch_item.item_type in ('LOAN_REPAYMENT', 'OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY')
      and (batch_item.finance_case_id is not null or batch_item.finance_component_id is not null)
  ), finance_item_totals as (
    select
      candidate_finance_items.candidate_id,
      candidate_finance_items.finance_case_id,
      candidate_finance_items.finance_component_id,
      round(sum(candidate_finance_items.item_amount_ex_vat), 2)::numeric as item_total_ex_vat
    from candidate_finance_items
    group by candidate_finance_items.candidate_id, candidate_finance_items.finance_case_id, candidate_finance_items.finance_component_id
  ), reservation_totals as (
    select
      candidate_finance_items.candidate_id,
      reservation_row.finance_case_id,
      reservation_row.finance_component_id,
      round(sum(abs(coalesce(reservation_row.reserved_amount, reservation_row.frozen_rounded_target_amount, 0))), 2)::numeric as reservation_total_ex_vat
    from candidate_finance_items
    join public.pay_advance_reservations as reservation_row
      on reservation_row.pay_batch_item_id = candidate_finance_items.pay_batch_item_id
    group by candidate_finance_items.candidate_id, reservation_row.finance_case_id, reservation_row.finance_component_id
  ), mismatches as (
    select
      finance_item_totals.candidate_id,
      finance_item_totals.finance_case_id,
      finance_item_totals.finance_component_id,
      finance_item_totals.item_total_ex_vat,
      coalesce(reservation_totals.reservation_total_ex_vat, 0)::numeric as reservation_total_ex_vat
    from finance_item_totals
    left join reservation_totals
      on reservation_totals.candidate_id is not distinct from finance_item_totals.candidate_id
     and reservation_totals.finance_case_id is not distinct from finance_item_totals.finance_case_id
     and reservation_totals.finance_component_id is not distinct from finance_item_totals.finance_component_id
    where finance_item_totals.item_total_ex_vat <> coalesce(reservation_totals.reservation_total_ex_vat, 0)
  )
  select
    'CANDIDATE_SCOPE_DEDUCTION_MISMATCH',
    null::uuid,
    null::uuid,
    mismatches.candidate_id,
    'DEDUCTION_RECOVERY',
    coalesce(mismatches.finance_case_id::text, mismatches.finance_component_id::text),
    jsonb_build_object('candidate_batch_finance_item_total_ex_vat', mismatches.item_total_ex_vat),
    jsonb_build_object('candidate_batch_reservation_total_ex_vat', mismatches.reservation_total_ex_vat),
    80
  from mismatches;

  get diagnostics v_candidate_scope_deduction_diff_count = row_count;
  v_deduction_diff_count := v_candidate_scope_deduction_diff_count;

  select count(*)::integer
  into v_stale_count
  from pg_temp.tmp_validate_freshness_chunk_diffs as diff_count_row;

  v_is_stale := coalesce(v_stale_count, 0) > 0;

  if v_key_resolution_failure_count > 0 then
    v_reasons := array_append(v_reasons, 'KEY_RESOLUTION_FAILED');
  end if;
  if v_key_diff_count > 0 then
    v_reasons := array_append(v_reasons, 'ECONOMIC_KEY_CHANGED');
  end if;
  if v_other_reservation_count > 0 then
    v_reasons := array_append(v_reasons, 'OTHER_ACTIVE_RESERVATION');
  end if;
  if v_finance_reservation_diff_count > 0 then
    v_reasons := array_append(v_reasons, 'FINANCE_RESERVATION_CHANGED');
  end if;
  if v_snooze_count > 0 then
    v_reasons := array_append(v_reasons, 'ACTIVE_SNOOZE_CHANGED');
  end if;
  if v_restructure_or_writeoff_count > 0 then
    v_reasons := array_append(v_reasons, 'FINANCE_CASE_RESTRUCTURE_OR_WRITEOFF_CHANGED');
  end if;
  if v_timesheet_override_count > 0 then
    v_reasons := array_append(v_reasons, 'TIMESHEET_PAYMENT_OVERRIDE_CHANGED');
  end if;
  if v_deduction_diff_count > 0 then
    v_reasons := array_append(v_reasons, 'DEDUCTION_RECOVERY_CHANGED');
  end if;
  if v_candidate_scope_deduction_diff_count > 0 then
    v_reasons := array_append(v_reasons, 'CANDIDATE_SCOPE_DEDUCTION_MISMATCH');
  end if;

  select coalesce(jsonb_object_agg(reason_counts.reason, reason_counts.reason_count order by reason_counts.reason), '{}'::jsonb)
  into v_stale_reason_counts
  from (
    select diff_rows.reason, count(*)::integer as reason_count
    from pg_temp.tmp_validate_freshness_chunk_diffs as diff_rows
    group by diff_rows.reason
  ) as reason_counts;

  if coalesce(v_deduction_diff_count, 0) > 0 then
    v_stale_reason_counts := coalesce(v_stale_reason_counts, '{}'::jsonb)
      || jsonb_build_object('DEDUCTION_RECOVERY_CHANGED', v_deduction_diff_count);
  end if;

  select coalesce(jsonb_agg(
    jsonb_build_object(
      'reason', diff_sample_rows.reason,
      'timesheet_id', case when diff_sample_rows.timesheet_id is null then null else diff_sample_rows.timesheet_id::text end,
      'pay_batch_item_id', case when diff_sample_rows.pay_batch_item_id is null then null else diff_sample_rows.pay_batch_item_id::text end,
      'candidate_id', case when diff_sample_rows.candidate_id is null then null else diff_sample_rows.candidate_id::text end,
      'key_type', diff_sample_rows.key_type,
      'key_value', diff_sample_rows.key_value,
      'expected', diff_sample_rows.expected,
      'actual', diff_sample_rows.actual
    )
    order by diff_sample_rows.ord, diff_sample_rows.reason, diff_sample_rows.timesheet_id nulls last, diff_sample_rows.pay_batch_item_id nulls last
  ), '[]'::jsonb)
  into v_diff_sample
  from (
    select diff_rows.*
    from pg_temp.tmp_validate_freshness_chunk_diffs as diff_rows
    order by diff_rows.ord, diff_rows.reason, diff_rows.timesheet_id nulls last, diff_rows.pay_batch_item_id nulls last
    limit v_diff_limit
  ) as diff_sample_rows;

  if array_length(v_reasons, 1) is not null then
    select array_agg(distinct reason_value order by reason_value)
    into v_reasons
    from unnest(v_reasons) as reason_values(reason_value);
  end if;

  v_result := jsonb_build_object(
    'ok', true,
    'is_stale', v_is_stale,
    'checked_units', coalesce(v_checked_units, 0),
    'checked_count', coalesce(v_checked_units, 0),
    'checked_item_count', coalesce(v_checked_item_count, 0),
    'checked_timesheet_count', coalesce(v_checked_timesheet_count, 0),
    'stale_count', coalesce(v_stale_count, 0),
    'stale_reasons', coalesce(to_jsonb(v_reasons), '[]'::jsonb),
    'stale_reason_counts', coalesce(v_stale_reason_counts, '{}'::jsonb),
    'key_resolution_failure_count', coalesce(v_key_resolution_failure_count, 0),
    'diff_sample', coalesce(v_diff_sample, '[]'::jsonb),
    'counts', jsonb_build_object(
      'economic_key_changed', coalesce(v_key_diff_count, 0),
      'other_active_reservation', coalesce(v_other_reservation_count, 0),
      'finance_reservation_changed', coalesce(v_finance_reservation_diff_count, 0),
      'active_snooze_changed', coalesce(v_snooze_count, 0),
      'finance_case_restructure_or_writeoff_changed', coalesce(v_restructure_or_writeoff_count, 0),
      'timesheet_payment_override_changed', coalesce(v_timesheet_override_count, 0),
      'deduction_recovery_changed', coalesce(v_deduction_diff_count, 0),
      'candidate_scope_deduction_mismatch', coalesce(v_candidate_scope_deduction_diff_count, 0)
    ),
    'operation_id', p_operation_id::text,
    'chunk_id', p_chunk_id::text,
    'pay_batch_id', p_pay_batch_id::text
  );

  v_chunk_result_hash := md5(v_result::text);
  v_result := v_result || jsonb_build_object('chunk_result_hash', v_chunk_result_hash);

  update public.banking_pay_operation_chunks as chunk_update
  set result_json = v_result,
      error_json = null,
      completed_count = coalesce(v_checked_units, 0),
      failed_count = case when v_is_stale then coalesce(v_stale_count, 0) else 0 end,
      updated_at_utc = now()
  where chunk_update.id = p_chunk_id;

    return jsonb_build_object(
    'ok', true,
    'is_stale', v_is_stale,
    'checked_count', coalesce(v_checked_units, 0),
    'stale_count', coalesce(v_stale_count, 0),
    'key_resolution_failure_count', coalesce(v_key_resolution_failure_count, 0),
    'chunk_result_hash', v_chunk_result_hash,
    'result', v_result
  );
end;
$$;




create or replace function public.pay_batch_get_section_page(
  p_pay_batch_id uuid,
  p_section text,
  p_cursor_json jsonb default null::jsonb,
  p_limit integer default 100,
  p_actor_user_id uuid default null::uuid,
  p_filters_json jsonb default '{}'::jsonb,
  p_sort_json jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
stable
set search_path = public
as $$
declare
  v_batch public.pay_batches%rowtype;
  v_section text;
  v_limit integer;
  v_cursor_id uuid := null;
  v_items jsonb := '[]'::jsonb;
  v_next_cursor jsonb := null;
  v_returned_count integer := 0;
  v_known_total_count integer := null;
  v_last_id uuid := null;
  v_freshness_summary jsonb := '{}'::jsonb;
  v_batch_status text := null;
  v_execution_commit_state text := null;
  v_actor_is_valid boolean := true;
begin
  if p_pay_batch_id is null then
    raise exception 'PAY_BATCH_GET_SECTION_PAGE_PAY_BATCH_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_GET_SECTION_PAGE_PAY_BATCH_ID_REQUIRED')::text;
  end if;

  v_section := lower(btrim(coalesce(p_section, '')));

  if v_section not in (
    'candidates',
    'items',
    'item_breakdowns',
    'transfers',
    'finance_case_groups',
    'remittances',
    'communications',
    'auth_history',
    'events'
  ) then
    raise exception 'PAY_BATCH_GET_SECTION_PAGE_UNSUPPORTED_SECTION'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_GET_SECTION_PAGE_UNSUPPORTED_SECTION',
              'section', coalesce(p_section, null),
              'supported_sections', jsonb_build_array(
                'candidates',
                'items',
                'item_breakdowns',
                'transfers',
                'finance_case_groups',
                'remittances',
                'communications',
                'auth_history',
                'events'
              )
            )::text;
  end if;

  v_limit := coalesce(p_limit, 100);
  if v_limit < 1 then
    v_limit := 1;
  elsif v_limit > 250 then
    v_limit := 250;
  end if;

  if p_cursor_json is not null
     and jsonb_typeof(p_cursor_json) = 'object'
     and nullif(btrim(coalesce(p_cursor_json->>'last_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_cursor_id := (p_cursor_json->>'last_id')::uuid;
  end if;

  select pay_batch_row.*
  into v_batch
  from public.pay_batches as pay_batch_row
  where pay_batch_row.id = p_pay_batch_id;

  if not found then
    raise exception 'PAY_BATCH_GET_SECTION_PAGE_BATCH_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_GET_SECTION_PAGE_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  if p_actor_user_id is not null then
    select exists (
      select 1
      from public.tms_users as actor_user
      where actor_user.id = p_actor_user_id
        and coalesce(actor_user.is_active, false) = true
    )
    into v_actor_is_valid;

    if coalesce(v_actor_is_valid, false) is not true then
      raise exception 'PAY_BATCH_GET_SECTION_PAGE_ACTOR_NOT_ALLOWED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'code', 'PAY_BATCH_GET_SECTION_PAGE_ACTOR_NOT_ALLOWED',
                'actor_user_id', p_actor_user_id::text
              )::text;
    end if;
  end if;

  v_batch_status := coalesce(v_batch.status, null);
  v_execution_commit_state := coalesce(v_batch.execution_commit_state, null);

  v_freshness_summary := jsonb_build_object(
    'freshness_operation_id', case when v_batch.freshness_operation_id is null then null else v_batch.freshness_operation_id::text end,
    'freshness_validation_status', v_batch.freshness_validation_status,
    'freshness_checked_at_utc', case when v_batch.freshness_checked_at_utc is null then null else to_jsonb(v_batch.freshness_checked_at_utc) end,
    'freshness_result_hash', v_batch.freshness_result_hash,
    'freshness_scope_hash', v_batch.freshness_scope_hash,
    'freshness_is_stale', upper(coalesce(v_batch.freshness_validation_status, '')) = 'STALE',
    'freshness_result_summary', coalesce(v_batch.freshness_result_json, '{}'::jsonb)
  );

  if v_section = 'candidates' then
    v_known_total_count := null;

    with page_rows as (
      select
        pay_batch_candidate_page.id,
        pay_batch_candidate_page.pay_batch_id,
        pay_batch_candidate_page.candidate_id,
        pay_batch_candidate_page.candidate_tms_ref,
        pay_batch_candidate_page.candidate_display_name,
        pay_batch_candidate_page.paye_state,
        pay_batch_candidate_page.mismatch_settlement_choice,
        pay_batch_candidate_page.gross_preview,
        pay_batch_candidate_page.net_bank_amount,
        pay_batch_candidate_page.debt_created,
        pay_batch_candidate_page.loan_repayment_taken,
        pay_batch_candidate_page.overpayment_recovery_taken,
        pay_batch_candidate_page.awaiting_net_amount,
        pay_batch_candidate_page.settlement_status,
        pay_batch_candidate_page.settled_at_utc,
        pay_batch_candidate_page.settled_via,
        pay_batch_candidate_page.remittance_trigger_status,
        pay_batch_candidate_page.remittance_sent_at_utc,
        pay_batch_candidate_page.last_remittance_error,
        pay_batch_candidate_page.updated_at
      from public.pay_batch_candidates as pay_batch_candidate_page
      where pay_batch_candidate_page.pay_batch_id = p_pay_batch_id
        and (v_cursor_id is null or pay_batch_candidate_page.id > v_cursor_id)
      order by pay_batch_candidate_page.id asc
      limit (v_limit + 1)
    )
    select
      coalesce(jsonb_agg(
        jsonb_build_object(
          'id', page_rows.id::text,
          'pay_batch_id', page_rows.pay_batch_id::text,
          'candidate_id', page_rows.candidate_id::text,
          'candidate_tms_ref', page_rows.candidate_tms_ref,
          'candidate_display_name', page_rows.candidate_display_name,
          'paye_state', page_rows.paye_state,
          'mismatch_settlement_choice', page_rows.mismatch_settlement_choice,
          'gross_preview', page_rows.gross_preview,
          'net_bank_amount', page_rows.net_bank_amount,
          'debt_created', page_rows.debt_created,
          'loan_repayment_taken', page_rows.loan_repayment_taken,
          'overpayment_recovery_taken', page_rows.overpayment_recovery_taken,
          'awaiting_net_amount', page_rows.awaiting_net_amount,
          'settlement_status', page_rows.settlement_status,
          'settled_at_utc', page_rows.settled_at_utc,
          'settled_via', page_rows.settled_via,
          'remittance_trigger_status', page_rows.remittance_trigger_status,
          'remittance_sent_at_utc', page_rows.remittance_sent_at_utc,
          'last_remittance_error', page_rows.last_remittance_error,
          'updated_at', page_rows.updated_at
        )
        order by page_rows.id asc
      ), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'items' then
    v_known_total_count := null;

    with page_rows as (
      select
        pay_batch_item_page.id,
        pay_batch_item_page.pay_batch_candidate_id,
        pay_batch_candidate_for_item_page.candidate_id,
        pay_batch_candidate_for_item_page.candidate_display_name,
        pay_batch_item_page.item_type,
        pay_batch_item_page.timesheet_id,
        pay_batch_item_page.segment_key,
        pay_batch_item_page.source_ref,
        pay_batch_item_page.description,
        pay_batch_item_page.amount_ex_vat,
        pay_batch_item_page.amount_vat,
        pay_batch_item_page.amount_inc_vat,
        pay_batch_item_page.pay_channel,
        pay_batch_item_page.umbrella_id,
        pay_batch_item_page.bank_reference,
        pay_batch_item_page.pay_bank_transfer_id,
        pay_batch_item_page.repayment_week_start,
        pay_batch_item_page.is_voided,
        pay_batch_item_page.is_mismatch,
        pay_batch_item_page.finance_case_id,
        pay_batch_item_page.finance_component_id,
        pay_batch_item_page.frozen_component_key_type,
        pay_batch_item_page.frozen_component_key_value,
        pay_batch_item_page.operation_source_key,
        pay_batch_item_page.created_at,
        pay_batch_item_page.updated_at
      from public.pay_batch_items as pay_batch_item_page
      join public.pay_batch_candidates as pay_batch_candidate_for_item_page
        on pay_batch_candidate_for_item_page.id = pay_batch_item_page.pay_batch_candidate_id
      where pay_batch_candidate_for_item_page.pay_batch_id = p_pay_batch_id
        and (v_cursor_id is null or pay_batch_item_page.id > v_cursor_id)
      order by pay_batch_item_page.id asc
      limit (v_limit + 1)
    )
    select
      coalesce(jsonb_agg(
        jsonb_build_object(
          'id', page_rows.id::text,
          'pay_batch_candidate_id', page_rows.pay_batch_candidate_id::text,
          'candidate_id', page_rows.candidate_id::text,
          'candidate_display_name', page_rows.candidate_display_name,
          'item_type', page_rows.item_type,
          'timesheet_id', case when page_rows.timesheet_id is null then null else page_rows.timesheet_id::text end,
          'segment_key', page_rows.segment_key,
          'source_ref', page_rows.source_ref,
          'description', page_rows.description,
          'amount_ex_vat', page_rows.amount_ex_vat,
          'amount_vat', page_rows.amount_vat,
          'amount_inc_vat', page_rows.amount_inc_vat,
          'pay_channel', page_rows.pay_channel,
          'umbrella_id', case when page_rows.umbrella_id is null then null else page_rows.umbrella_id::text end,
          'bank_reference', page_rows.bank_reference,
          'pay_bank_transfer_id', case when page_rows.pay_bank_transfer_id is null then null else page_rows.pay_bank_transfer_id::text end,
          'repayment_week_start', case when page_rows.repayment_week_start is null then null else page_rows.repayment_week_start::text end,
          'is_voided', page_rows.is_voided,
          'is_mismatch', page_rows.is_mismatch,
          'finance_case_id', case when page_rows.finance_case_id is null then null else page_rows.finance_case_id::text end,
          'finance_component_id', case when page_rows.finance_component_id is null then null else page_rows.finance_component_id::text end,
          'frozen_component_key_type', page_rows.frozen_component_key_type,
          'frozen_component_key_value', page_rows.frozen_component_key_value,
          'operation_source_key', page_rows.operation_source_key,
          'created_at', page_rows.created_at,
          'updated_at', page_rows.updated_at
        )
        order by page_rows.id asc
      ), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'item_breakdowns' then
    v_known_total_count := null;

    with page_rows as (
      select
        pay_batch_breakdown_page.id,
        pay_batch_breakdown_page.pay_batch_item_id,
        pay_batch_item_for_breakdown_page.pay_batch_candidate_id,
        pay_batch_candidate_for_breakdown_page.candidate_id,
        pay_batch_breakdown_page.line_kind,
        pay_batch_breakdown_page.bucket_code,
        pay_batch_breakdown_page.unit_name,
        pay_batch_breakdown_page.units,
        pay_batch_breakdown_page.rate,
        pay_batch_breakdown_page.amount_ex_vat,
        pay_batch_breakdown_page.amount_vat,
        pay_batch_breakdown_page.amount_inc_vat,
        pay_batch_breakdown_page.meta_json,
        pay_batch_breakdown_page.operation_source_key,
        pay_batch_breakdown_page.created_at_utc
      from public.pay_batch_item_breakdowns as pay_batch_breakdown_page
      join public.pay_batch_items as pay_batch_item_for_breakdown_page
        on pay_batch_item_for_breakdown_page.id = pay_batch_breakdown_page.pay_batch_item_id
      join public.pay_batch_candidates as pay_batch_candidate_for_breakdown_page
        on pay_batch_candidate_for_breakdown_page.id = pay_batch_item_for_breakdown_page.pay_batch_candidate_id
      where pay_batch_candidate_for_breakdown_page.pay_batch_id = p_pay_batch_id
        and (v_cursor_id is null or pay_batch_breakdown_page.id > v_cursor_id)
      order by pay_batch_breakdown_page.id asc
      limit (v_limit + 1)
    )
    select
      coalesce(jsonb_agg(
        jsonb_build_object(
          'id', page_rows.id::text,
          'pay_batch_item_id', page_rows.pay_batch_item_id::text,
          'pay_batch_candidate_id', page_rows.pay_batch_candidate_id::text,
          'candidate_id', page_rows.candidate_id::text,
          'line_kind', page_rows.line_kind,
          'bucket_code', page_rows.bucket_code,
          'unit_name', page_rows.unit_name,
          'units', page_rows.units,
          'rate', page_rows.rate,
          'amount_ex_vat', page_rows.amount_ex_vat,
          'amount_vat', page_rows.amount_vat,
          'amount_inc_vat', page_rows.amount_inc_vat,
          'meta_json', coalesce(page_rows.meta_json, '{}'::jsonb),
          'operation_source_key', page_rows.operation_source_key,
          'created_at_utc', page_rows.created_at_utc
        )
        order by page_rows.id asc
      ), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'transfers' then
    v_known_total_count := null;

    with page_rows as (
      select
        transfer_page.id,
        transfer_page.pay_batch_id,
        transfer_page.candidate_id,
        transfer_page.umbrella_id,
        transfer_page.pay_channel,
        transfer_page.amount,
        transfer_page.currency,
        transfer_page.status,
        transfer_page.payment_reference,
        transfer_page.payee_name,
        transfer_page.sort_code,
        transfer_page.account_number,
        transfer_page.account_type,
        transfer_page.created_at_utc,
        transfer_page.completed_at_utc,
        transfer_page.failed_reason,
        transfer_page.rail_provider,
        transfer_page.rail_env,
        transfer_page.request_id,
        transfer_page.rail_tx_id,
        transfer_page.rail_state,
        transfer_page.rail_meta_json,
        transfer_page.bank_details_hash_snapshot,
        transfer_page.payee_entity_kind,
        transfer_page.payee_entity_id,
        transfer_page.transfer_group_key,
        transfer_page.grouping_mode_used,
        transfer_page.week_ending_bucket,
        (
          (
            lower(btrim(coalesce(transfer_page.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
            and exists (
              select 1
              from (values
                (transfer_page.rail_tx_id),
                (transfer_page.rail_meta_json #>> '{provider_event_id}'),
                (transfer_page.rail_meta_json #>> '{provider_reference}'),
                (transfer_page.rail_meta_json #>> '{provider_submission_id}'),
                (transfer_page.rail_meta_json #>> '{submission_id}'),
                (transfer_page.rail_meta_json #>> '{rail_submission_id}'),
                (transfer_page.rail_meta_json #>> '{provider_payment_id}'),
                (transfer_page.rail_meta_json #>> '{payment_id}'),
                (transfer_page.rail_meta_json #>> '{external_payment_id}'),
                (transfer_page.rail_meta_json #>> '{revolut_payment_id}'),
                (transfer_page.rail_meta_json #>> '{provider_transfer_id}'),
                (transfer_page.rail_meta_json #>> '{transfer_id}'),
                (transfer_page.rail_meta_json #>> '{external_transfer_id}'),
                (transfer_page.rail_meta_json #>> '{provider_transaction_id}'),
                (transfer_page.rail_meta_json #>> '{transaction_id}')
              ) as transfer_identifier(identifier_value)
              where nullif(btrim(coalesce(transfer_identifier.identifier_value, '')), '') is not null
                and not (
                  nullif(btrim(coalesce(transfer_identifier.identifier_value, '')), '') = any(
                    array_remove(array[
                      transfer_page.id::text,
                      nullif(btrim(coalesce(transfer_page.request_id, '')), ''),
                      nullif(btrim(coalesce(transfer_page.payment_reference, '')), ''),
                      nullif(btrim(coalesce(transfer_page.transfer_group_key, '')), ''),
                      nullif(btrim(coalesce(v_batch.bulk_reference, '')), ''),
                      nullif(btrim(coalesce(transfer_page.rail_meta_json #>> '{request_id}', '')), ''),
                      nullif(btrim(coalesce(transfer_page.rail_meta_json #>> '{idempotency_key}', '')), ''),
                      nullif(btrim(coalesce(transfer_page.rail_meta_json #>> '{payment_reference}', '')), ''),
                      nullif(btrim(coalesce(transfer_page.rail_meta_json #>> '{bulk_reference}', '')), '')
                    ]::text[], null::text)
                  )
                )
            )
          )
          or exists (
            select 1
            from public.pay_bank_transfer_events as provider_event_page
            where provider_event_page.pay_batch_id = p_pay_batch_id
              and provider_event_page.pay_bank_transfer_id = transfer_page.id
              and upper(btrim(coalesce(provider_event_page.event_source, ''))) in ('PROVIDER_RESPONSE','PROVIDER_POLL','PROVIDER_WEBHOOK','WEBHOOK','POLL','RAIL_PROVIDER','PROVIDER','PROVIDER_SETTLEMENT')
              and exists (
                select 1
                from (values
                  (provider_event_page.provider_event_id),
                  (provider_event_page.provider_reference),
                  (provider_event_page.raw_payload #>> '{provider_event_id}'),
                  (provider_event_page.raw_payload #>> '{provider_reference}'),
                  (provider_event_page.raw_payload #>> '{provider_submission_id}'),
                  (provider_event_page.raw_payload #>> '{submission_id}'),
                  (provider_event_page.raw_payload #>> '{rail_submission_id}'),
                  (provider_event_page.raw_payload #>> '{provider_payment_id}'),
                  (provider_event_page.raw_payload #>> '{payment_id}'),
                  (provider_event_page.raw_payload #>> '{external_payment_id}'),
                  (provider_event_page.raw_payload #>> '{revolut_payment_id}'),
                  (provider_event_page.raw_payload #>> '{provider_transfer_id}'),
                  (provider_event_page.raw_payload #>> '{transfer_id}'),
                  (provider_event_page.raw_payload #>> '{external_transfer_id}'),
                  (provider_event_page.raw_payload #>> '{provider_transaction_id}'),
                  (provider_event_page.raw_payload #>> '{transaction_id}')
                ) as provider_identifier(identifier_value)
                where nullif(btrim(coalesce(provider_identifier.identifier_value, '')), '') is not null
                  and not (
                    nullif(btrim(coalesce(provider_identifier.identifier_value, '')), '') = any(
                      array_remove(array[
                        transfer_page.id::text,
                        nullif(btrim(coalesce(transfer_page.request_id, '')), ''),
                        nullif(btrim(coalesce(transfer_page.payment_reference, '')), ''),
                        nullif(btrim(coalesce(transfer_page.transfer_group_key, '')), ''),
                        nullif(btrim(coalesce(v_batch.bulk_reference, '')), ''),
                        nullif(btrim(coalesce(transfer_page.rail_meta_json #>> '{request_id}', '')), ''),
                        nullif(btrim(coalesce(transfer_page.rail_meta_json #>> '{idempotency_key}', '')), ''),
                        nullif(btrim(coalesce(transfer_page.rail_meta_json #>> '{payment_reference}', '')), ''),
                        nullif(btrim(coalesce(transfer_page.rail_meta_json #>> '{bulk_reference}', '')), '')
                      ]::text[], null::text)
                    )
                  )
              )
          )
        ) as has_provider_evidence
      from public.pay_bank_transfers as transfer_page
      where transfer_page.pay_batch_id = p_pay_batch_id
        and (v_cursor_id is null or transfer_page.id > v_cursor_id)
      order by transfer_page.id asc
      limit (v_limit + 1)
    )
    select
      coalesce(jsonb_agg(
        jsonb_build_object(
          'id', page_rows.id::text,
          'pay_batch_id', page_rows.pay_batch_id::text,
          'candidate_id', case when page_rows.candidate_id is null then null else page_rows.candidate_id::text end,
          'umbrella_id', case when page_rows.umbrella_id is null then null else page_rows.umbrella_id::text end,
          'pay_channel', page_rows.pay_channel,
          'amount', page_rows.amount,
          'currency', page_rows.currency,
          'status', page_rows.status,
          'payment_reference', page_rows.payment_reference,
          'payee_name', page_rows.payee_name,
          'sort_code', page_rows.sort_code,
          'account_number', page_rows.account_number,
          'account_type', page_rows.account_type,
          'created_at_utc', page_rows.created_at_utc,
          'completed_at_utc', page_rows.completed_at_utc,
          'failed_reason', page_rows.failed_reason,
          'rail_provider', page_rows.rail_provider,
          'rail_env', page_rows.rail_env,
          'request_id', page_rows.request_id,
          'rail_tx_id', page_rows.rail_tx_id,
          'rail_state', page_rows.rail_state,
          'rail_meta_json', coalesce(page_rows.rail_meta_json, '{}'::jsonb),
          'bank_details_hash_snapshot', page_rows.bank_details_hash_snapshot,
          'payee_entity_kind', page_rows.payee_entity_kind,
          'payee_entity_id', case when page_rows.payee_entity_id is null then null else page_rows.payee_entity_id::text end,
          'transfer_group_key', page_rows.transfer_group_key,
          'grouping_mode_used', page_rows.grouping_mode_used,
          'week_ending_bucket', case when page_rows.week_ending_bucket is null then null else page_rows.week_ending_bucket::text end,
          'has_provider_evidence', page_rows.has_provider_evidence
        )
        order by page_rows.id asc
      ), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'finance_case_groups' then
    v_known_total_count := null;

    with page_group_keys as (
      select finance_group_keys.group_id
      from (
        select coalesce(pay_batch_item_key.finance_case_id, pay_batch_item_key.finance_component_id) as group_id
        from public.pay_batch_items as pay_batch_item_key
        join public.pay_batch_candidates as pay_batch_candidate_key
          on pay_batch_candidate_key.id = pay_batch_item_key.pay_batch_candidate_id
        where pay_batch_candidate_key.pay_batch_id = p_pay_batch_id
          and (pay_batch_item_key.finance_case_id is not null or pay_batch_item_key.finance_component_id is not null)
        group by coalesce(pay_batch_item_key.finance_case_id, pay_batch_item_key.finance_component_id)
      ) as finance_group_keys
      where v_cursor_id is null or finance_group_keys.group_id > v_cursor_id
      order by finance_group_keys.group_id asc
      limit (v_limit + 1)
    ),
    page_rows as (
      select
        page_group_keys.group_id,
        (array_agg(pay_batch_item_finance_page.finance_case_id ORDER BY pay_batch_item_finance_page.finance_case_id::text) FILTER (WHERE pay_batch_item_finance_page.finance_case_id IS NOT NULL))[1] AS finance_case_id,
        (array_agg(pay_batch_item_finance_page.finance_component_id ORDER BY pay_batch_item_finance_page.finance_component_id::text) FILTER (WHERE pay_batch_item_finance_page.finance_component_id IS NOT NULL))[1] AS finance_component_id,
        count(*)::integer as item_count,
        round(sum(coalesce(pay_batch_item_finance_page.amount_ex_vat, 0)), 2)::numeric as total_amount_ex_vat,
        coalesce(jsonb_agg(distinct pay_batch_item_finance_page.item_type) filter (where pay_batch_item_finance_page.item_type is not null), '[]'::jsonb) as item_types,
        coalesce(jsonb_agg(distinct pay_batch_candidate_finance_page.candidate_id::text) filter (where pay_batch_candidate_finance_page.candidate_id is not null), '[]'::jsonb) as candidate_ids
      from page_group_keys
      join public.pay_batch_items as pay_batch_item_finance_page
        on coalesce(pay_batch_item_finance_page.finance_case_id, pay_batch_item_finance_page.finance_component_id) = page_group_keys.group_id
      join public.pay_batch_candidates as pay_batch_candidate_finance_page
        on pay_batch_candidate_finance_page.id = pay_batch_item_finance_page.pay_batch_candidate_id
      where pay_batch_candidate_finance_page.pay_batch_id = p_pay_batch_id
      group by page_group_keys.group_id
    )
    select
      coalesce(jsonb_agg(
        jsonb_build_object(
          'group_id', page_rows.group_id::text,
          'finance_case_id', case when page_rows.finance_case_id is null then null else page_rows.finance_case_id::text end,
          'finance_component_id', case when page_rows.finance_component_id is null then null else page_rows.finance_component_id::text end,
          'item_count', page_rows.item_count,
          'total_amount_ex_vat', page_rows.total_amount_ex_vat,
          'item_types', page_rows.item_types,
          'candidate_ids', page_rows.candidate_ids
        )
        order by page_rows.group_id asc
      ), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.group_id order by page_rows.group_id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'remittances' then
    v_known_total_count := null;

    with page_rows as (
      select
        mail_outbox_page.id,
        mail_outbox_page.type,
        mail_outbox_page.to,
        mail_outbox_page.cc,
        mail_outbox_page.bcc,
        mail_outbox_page.subject,
        mail_outbox_page.status,
        mail_outbox_page.provider_status,
        mail_outbox_page.recipient_kind,
        mail_outbox_page.recipient_id,
        mail_outbox_page.context_kind,
        mail_outbox_page.context_id,
        mail_outbox_page.reference,
        mail_outbox_page.created_at_utc,
        mail_outbox_page.sent_at,
        mail_outbox_page.failed_at,
        mail_outbox_page.last_error,
        mail_outbox_page.deterministic_outbox_key
      from public.mail_outbox as mail_outbox_page
      where (
           mail_outbox_page.payment_scope_json->>'pay_batch_id' = p_pay_batch_id::text
        or (lower(coalesce(mail_outbox_page.context_kind, '')) in ('pay_batch', 'pay_batches') and mail_outbox_page.context_id = p_pay_batch_id)
        or mail_outbox_page.reference = p_pay_batch_id::text
      )
        and (v_cursor_id is null or mail_outbox_page.id > v_cursor_id)
      order by mail_outbox_page.id asc
      limit (v_limit + 1)
    )
    select
      coalesce(jsonb_agg(to_jsonb(page_rows) order by page_rows.id asc), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'communications' then
    v_known_total_count := null;

    with page_rows as (
      select
        comms_outbox_page.id,
        comms_outbox_page.channel,
        comms_outbox_page.status,
        comms_outbox_page.to_address,
        comms_outbox_page.provider_key,
        comms_outbox_page.provider_message_id,
        comms_outbox_page.recipient_kind,
        comms_outbox_page.recipient_id,
        comms_outbox_page.context_kind,
        comms_outbox_page.context_id,
        comms_outbox_page.created_at_utc,
        comms_outbox_page.sent_at,
        comms_outbox_page.delivered_at,
        comms_outbox_page.read_at,
        comms_outbox_page.failed_at,
        comms_outbox_page.last_error,
        comms_outbox_page.deterministic_outbox_key
      from public.comms_outbox as comms_outbox_page
      where (
           (lower(coalesce(comms_outbox_page.context_kind, '')) in ('pay_batch', 'pay_batches') and comms_outbox_page.context_id = p_pay_batch_id)
        or comms_outbox_page.provider_payload_json->>'pay_batch_id' = p_pay_batch_id::text
        or comms_outbox_page.provider_response_json->>'pay_batch_id' = p_pay_batch_id::text
      )
        and (v_cursor_id is null or comms_outbox_page.id > v_cursor_id)
      order by comms_outbox_page.id asc
      limit (v_limit + 1)
    )
    select
      coalesce(jsonb_agg(to_jsonb(page_rows) order by page_rows.id asc), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'auth_history' then
    v_known_total_count := null;

    with page_rows as (
      select
        auth_request_page.id,
        auth_request_page.pay_batch_id,
        auth_request_page.requested_by_user_id,
        auth_request_page.required_quantity,
        auth_request_page.schedule_kind,
        auth_request_page.scheduled_at_utc,
        auth_request_page.funding_account_ref,
        auth_request_page.state,
        auth_request_page.golden_key_used,
        auth_request_page.golden_key_user_id,
        auth_request_page.created_at_utc,
        auth_request_page.finalised_at_utc,
        auth_request_page.finalised_by_user_id,
        auth_request_page.execution_intent_json
      from public.pay_batch_auth_requests as auth_request_page
      where auth_request_page.pay_batch_id = p_pay_batch_id
        and (v_cursor_id is null or auth_request_page.id > v_cursor_id)
      order by auth_request_page.id asc
      limit (v_limit + 1)
    )
    select
      coalesce(jsonb_agg(to_jsonb(page_rows) order by page_rows.id asc), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'events' then
    v_known_total_count := null;

    with page_rows as (
      select
        transfer_event_page.id,
        transfer_event_page.pay_batch_id,
        transfer_event_page.pay_bank_transfer_id,
        transfer_event_page.candidate_id,
        transfer_event_page.umbrella_id,
        transfer_event_page.provider_key,
        transfer_event_page.provider_event_id,
        transfer_event_page.provider_reference,
        transfer_event_page.provider_state,
        transfer_event_page.normalised_state,
        transfer_event_page.event_source,
        transfer_event_page.event_time_utc,
        transfer_event_page.received_at_utc,
        transfer_event_page.amount,
        transfer_event_page.currency,
        transfer_event_page.mapping_status,
        transfer_event_page.movement_classification,
        transfer_event_page.correction_disposition,
        transfer_event_page.mapping_method,
        transfer_event_page.created_at_utc
      from public.pay_bank_transfer_events as transfer_event_page
      where transfer_event_page.pay_batch_id = p_pay_batch_id
        and (v_cursor_id is null or transfer_event_page.id > v_cursor_id)
      order by transfer_event_page.id asc
      limit (v_limit + 1)
    )
    select
      coalesce(jsonb_agg(to_jsonb(page_rows) order by page_rows.id asc), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;
  end if;

  if coalesce(v_returned_count, 0) > v_limit then
    v_next_cursor := jsonb_build_object(
      'last_id',
      coalesce(
        v_items -> (v_limit - 1) ->> 'id',
        v_items -> (v_limit - 1) ->> 'group_id'
      )
    );

    select coalesce(jsonb_agg(trimmed_items.value order by trimmed_items.ordinality), '[]'::jsonb)
    into v_items
    from jsonb_array_elements(coalesce(v_items, '[]'::jsonb)) with ordinality as trimmed_items(value, ordinality)
    where trimmed_items.ordinality <= v_limit;

    v_returned_count := v_limit;
  else
    v_next_cursor := null;
    v_returned_count := coalesce(v_returned_count, 0);
  end if;

  return jsonb_build_object(
    'section', v_section,
    'items', coalesce(v_items, '[]'::jsonb),
    'next_cursor', v_next_cursor,
    'returned_count', coalesce(v_returned_count, 0),
    'known_total_count', v_known_total_count,
    'pay_batch_id', p_pay_batch_id::text,
    'batch_status', v_batch_status,
    'execution_commit_state', v_execution_commit_state,
    'freshness', v_freshness_summary,
    'limit', v_limit
  );
end;
$$;





create or replace function public.pay_batch_freshness_result_get(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_actor_user_id uuid default null::uuid
)
returns jsonb
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
  v_operation public.banking_pay_operations%rowtype;
  v_batch public.pay_batches%rowtype;
  v_actor_is_valid boolean := true;
  v_total_chunk_count integer := 0;
  v_pending_chunk_count integer := 0;
  v_running_chunk_count integer := 0;
  v_failed_chunk_count integer := 0;
  v_complete_chunk_count integer := 0;
  v_validation_complete boolean := false;
  v_is_stale boolean := false;
  v_checked_count integer := 0;
  v_stale_count integer := 0;
  v_failed_count integer := 0;
  v_key_resolution_failure_count integer := 0;
  v_stale_reasons jsonb := '[]'::jsonb;
  v_stale_reason_counts jsonb := '{}'::jsonb;
  v_diff_sample jsonb := '[]'::jsonb;
  v_checked_at_utc timestamptz := now();
  v_scope_hash text := null;
  v_result_hash text := null;
  v_status text := 'PENDING';
  v_result jsonb := '{}'::jsonb;
  v_hash_basis jsonb := '{}'::jsonb;
  v_chunk_result_hashes jsonb := '[]'::jsonb;
  v_failed_sample jsonb := '[]'::jsonb;
begin
  if p_operation_id is null then
    raise exception 'PAY_BATCH_FRESHNESS_RESULT_GET_OPERATION_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_FRESHNESS_RESULT_GET_OPERATION_ID_REQUIRED')::text;
  end if;

  if p_pay_batch_id is null then
    raise exception 'PAY_BATCH_FRESHNESS_RESULT_GET_PAY_BATCH_ID_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('code', 'PAY_BATCH_FRESHNESS_RESULT_GET_PAY_BATCH_ID_REQUIRED')::text;
  end if;

  select operation_row.*
  into v_operation
  from public.banking_pay_operations as operation_row
  where operation_row.id = p_operation_id
  for update;

  if not found then
    raise exception 'PAY_BATCH_FRESHNESS_RESULT_GET_OPERATION_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_RESULT_GET_OPERATION_NOT_FOUND',
              'operation_id', p_operation_id::text
            )::text;
  end if;

  if v_operation.pay_batch_id is not null and v_operation.pay_batch_id <> p_pay_batch_id then
    raise exception 'PAY_BATCH_FRESHNESS_RESULT_GET_OPERATION_BATCH_MISMATCH'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_RESULT_GET_OPERATION_BATCH_MISMATCH',
              'operation_id', p_operation_id::text,
              'operation_pay_batch_id', v_operation.pay_batch_id::text,
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  select pay_batch_row.*
  into v_batch
  from public.pay_batches as pay_batch_row
  where pay_batch_row.id = p_pay_batch_id
  for update;

  if not found then
    raise exception 'PAY_BATCH_FRESHNESS_RESULT_GET_BATCH_NOT_FOUND'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_RESULT_GET_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  end if;

  if p_actor_user_id is not null then
    select exists (
      select 1
      from public.tms_users as actor_user
      where actor_user.id = p_actor_user_id
        and coalesce(actor_user.is_active, false) = true
    )
    into v_actor_is_valid;

    if coalesce(v_actor_is_valid, false) is not true then
      raise exception 'PAY_BATCH_FRESHNESS_RESULT_GET_ACTOR_NOT_ALLOWED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'code', 'PAY_BATCH_FRESHNESS_RESULT_GET_ACTOR_NOT_ALLOWED',
                'actor_user_id', p_actor_user_id::text
              )::text;
    end if;
  end if;

  select
    count(*)::integer,
    count(*) filter (where chunk_row.status = 'PENDING')::integer,
    count(*) filter (where chunk_row.status = 'RUNNING')::integer,
    count(*) filter (where chunk_row.status = 'FAILED')::integer,
    count(*) filter (where chunk_row.status = 'COMPLETE')::integer
  into
    v_total_chunk_count,
    v_pending_chunk_count,
    v_running_chunk_count,
    v_failed_chunk_count,
    v_complete_chunk_count
  from public.banking_pay_operation_chunks as chunk_row
  where chunk_row.operation_id = p_operation_id
    and chunk_row.phase = 'VALIDATE_FRESHNESS'
    and chunk_row.chunk_type = 'FRESHNESS_VALIDATE';

  if coalesce(v_total_chunk_count, 0) = 0 then
    v_result := jsonb_build_object(
      'validation_complete', false,
      'is_stale', false,
      'status', 'PENDING',
      'reason', 'NO_FRESHNESS_CHUNKS',
      'operation_id', p_operation_id::text,
      'pay_batch_id', p_pay_batch_id::text
    );

    return v_result;
  end if;

  v_validation_complete := coalesce(v_pending_chunk_count, 0) = 0
    and coalesce(v_running_chunk_count, 0) = 0;

  v_scope_hash := nullif(btrim(coalesce(v_operation.progress_json->>'freshness_scope_hash', v_batch.freshness_scope_hash, '')), '');

  if v_validation_complete is not true then
    v_result := jsonb_build_object(
      'validation_complete', false,
      'is_stale', false,
      'status', 'PENDING',
      'pending_count', coalesce(v_pending_chunk_count, 0),
      'running_count', coalesce(v_running_chunk_count, 0),
      'complete_count', coalesce(v_complete_chunk_count, 0),
      'failed_count', coalesce(v_failed_chunk_count, 0),
      'checked_count', 0,
      'stale_count', 0,
      'key_resolution_failure_count', 0,
      'diff_sample', '[]'::jsonb,
      'freshness_checked_at_utc', null,
      'freshness_result_hash', null,
      'freshness_scope_hash', v_scope_hash,
      'operation_id', p_operation_id::text
    );

    return v_result;
  end if;

  with chunk_results as (
    select
      chunk_row.id,
      chunk_row.status,
      coalesce(chunk_row.result_json, '{}'::jsonb) as result_json,
      coalesce(chunk_row.error_json, '{}'::jsonb) as error_json
    from public.banking_pay_operation_chunks as chunk_row
    where chunk_row.operation_id = p_operation_id
      and chunk_row.phase = 'VALIDATE_FRESHNESS'
      and chunk_row.chunk_type = 'FRESHNESS_VALIDATE'
  ),
  counts as (
    select
      sum(coalesce((chunk_results.result_json->>'checked_count')::integer, 0))::integer as checked_count,
      sum(coalesce((chunk_results.result_json->>'stale_count')::integer, 0))::integer as stale_count,
      sum(coalesce((chunk_results.result_json->>'key_resolution_failure_count')::integer, 0))::integer as key_resolution_failure_count,
      count(*) filter (where chunk_results.status = 'FAILED')::integer as failed_count,
      bool_or(coalesce((chunk_results.result_json->>'is_stale')::boolean, false)) as any_stale,
      bool_or(chunk_results.status = 'FAILED') as any_failed
    from chunk_results
  )
  select
    coalesce(counts.checked_count, 0),
    coalesce(counts.stale_count, 0),
    coalesce(counts.key_resolution_failure_count, 0),
    coalesce(counts.failed_count, 0),
    coalesce(counts.any_stale, false) or coalesce(counts.any_failed, false) or coalesce(counts.key_resolution_failure_count, 0) > 0
  into
    v_checked_count,
    v_stale_count,
    v_key_resolution_failure_count,
    v_failed_count,
    v_is_stale
  from counts;

  with reason_values as (
    select distinct reason_text.reason_value
    from public.banking_pay_operation_chunks as chunk_row
    cross join lateral jsonb_array_elements_text(coalesce(chunk_row.result_json->'stale_reasons', '[]'::jsonb)) as reason_text(reason_value)
    where chunk_row.operation_id = p_operation_id
      and chunk_row.phase = 'VALIDATE_FRESHNESS'
      and chunk_row.chunk_type = 'FRESHNESS_VALIDATE'
      and chunk_row.result_json is not null
  )
  select coalesce(jsonb_agg(reason_values.reason_value order by reason_values.reason_value), '[]'::jsonb)
  into v_stale_reasons
  from reason_values;

  with reason_pairs as (
    select
      reason_entry.key as reason_key,
      sum(coalesce(reason_entry.value::text::integer, 0))::integer as reason_count
    from public.banking_pay_operation_chunks as chunk_row
    cross join lateral jsonb_each(coalesce(chunk_row.result_json->'stale_reason_counts', '{}'::jsonb)) as reason_entry(key, value)
    where chunk_row.operation_id = p_operation_id
      and chunk_row.phase = 'VALIDATE_FRESHNESS'
      and chunk_row.chunk_type = 'FRESHNESS_VALIDATE'
      and chunk_row.result_json is not null
    group by reason_entry.key
  )
  select coalesce(jsonb_object_agg(reason_pairs.reason_key, reason_pairs.reason_count order by reason_pairs.reason_key), '{}'::jsonb)
  into v_stale_reason_counts
  from reason_pairs;

  with diff_items as (
    select
      chunk_row.sequence_no,
      diff_item.diff_value
    from public.banking_pay_operation_chunks as chunk_row
    cross join lateral jsonb_array_elements(coalesce(chunk_row.result_json->'diff_sample', '[]'::jsonb)) as diff_item(diff_value)
    where chunk_row.operation_id = p_operation_id
      and chunk_row.phase = 'VALIDATE_FRESHNESS'
      and chunk_row.chunk_type = 'FRESHNESS_VALIDATE'
      and chunk_row.result_json is not null
    order by chunk_row.sequence_no, diff_item.diff_value::text
    limit 50
  )
  select coalesce(jsonb_agg(diff_items.diff_value order by diff_items.sequence_no, diff_items.diff_value::text), '[]'::jsonb)
  into v_diff_sample
  from diff_items;

  with chunk_hash_rows as (
    select
      chunk_row.sequence_no,
      nullif(btrim(coalesce(chunk_row.result_json->>'chunk_result_hash', '')), '') as chunk_result_hash
    from public.banking_pay_operation_chunks as chunk_row
    where chunk_row.operation_id = p_operation_id
      and chunk_row.phase = 'VALIDATE_FRESHNESS'
      and chunk_row.chunk_type = 'FRESHNESS_VALIDATE'
      and nullif(btrim(coalesce(chunk_row.result_json->>'chunk_result_hash', '')), '') is not null
    order by chunk_row.sequence_no
  )
  select coalesce(jsonb_agg(chunk_hash_rows.chunk_result_hash order by chunk_hash_rows.sequence_no), '[]'::jsonb)
  into v_chunk_result_hashes
  from chunk_hash_rows;

  with failed_rows as (
    select
      chunk_row.sequence_no,
      jsonb_build_object(
        'chunk_id', chunk_row.id::text,
        'sequence_no', chunk_row.sequence_no,
        'error', coalesce(chunk_row.error_json, '{}'::jsonb)
      ) as failed_json
    from public.banking_pay_operation_chunks as chunk_row
    where chunk_row.operation_id = p_operation_id
      and chunk_row.phase = 'VALIDATE_FRESHNESS'
      and chunk_row.chunk_type = 'FRESHNESS_VALIDATE'
      and chunk_row.status = 'FAILED'
    order by chunk_row.sequence_no
    limit 20
  )
  select coalesce(jsonb_agg(failed_rows.failed_json order by failed_rows.sequence_no), '[]'::jsonb)
  into v_failed_sample
  from failed_rows;

  if coalesce(v_failed_count, 0) > 0 and not (coalesce(v_stale_reasons, '[]'::jsonb) ? 'CHUNK_FAILED') then
    v_stale_reasons := coalesce(v_stale_reasons, '[]'::jsonb) || jsonb_build_array('CHUNK_FAILED');
    v_stale_reason_counts := coalesce(v_stale_reason_counts, '{}'::jsonb) || jsonb_build_object('CHUNK_FAILED', coalesce(v_failed_count, 0));
  end if;

  if coalesce(v_key_resolution_failure_count, 0) > 0 and not (coalesce(v_stale_reasons, '[]'::jsonb) ? 'KEY_RESOLUTION_FAILED') then
    v_stale_reasons := coalesce(v_stale_reasons, '[]'::jsonb) || jsonb_build_array('KEY_RESOLUTION_FAILED');
    v_stale_reason_counts := coalesce(v_stale_reason_counts, '{}'::jsonb) || jsonb_build_object('KEY_RESOLUTION_FAILED', coalesce(v_key_resolution_failure_count, 0));
  end if;

  if coalesce(v_failed_count, 0) > 0 then
    v_status := 'FAILED';
  elsif v_is_stale then
    v_status := 'STALE';
  else
    v_status := 'PASSED';
  end if;

  v_hash_basis := jsonb_build_object(
    'validation_complete', true,
    'is_stale', v_is_stale,
    'status', v_status,
    'stale_reasons', coalesce(v_stale_reasons, '[]'::jsonb),
    'stale_reason_counts', coalesce(v_stale_reason_counts, '{}'::jsonb),
    'checked_count', coalesce(v_checked_count, 0),
    'stale_count', coalesce(v_stale_count, 0),
    'failed_count', coalesce(v_failed_count, 0),
    'key_resolution_failure_count', coalesce(v_key_resolution_failure_count, 0),
    'diff_sample', coalesce(v_diff_sample, '[]'::jsonb),
    'failed_sample', coalesce(v_failed_sample, '[]'::jsonb),
    'freshness_scope_hash', v_scope_hash,
    'chunk_count', coalesce(v_total_chunk_count, 0),
    'complete_chunk_count', coalesce(v_complete_chunk_count, 0),
    'chunk_result_hashes', coalesce(v_chunk_result_hashes, '[]'::jsonb)
  );

  v_result_hash := md5(v_hash_basis::text);

  if v_batch.freshness_operation_id = p_operation_id
     and nullif(btrim(coalesce(v_batch.freshness_result_hash, '')), '') = v_result_hash
     and v_batch.freshness_checked_at_utc is not null then
    v_checked_at_utc := v_batch.freshness_checked_at_utc;
  end if;

  v_result := v_hash_basis || jsonb_build_object(
    'freshness_checked_at_utc', to_jsonb(v_checked_at_utc),
    'freshness_result_hash', v_result_hash,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text
  );

  update public.pay_batches as pay_batch_update
  set freshness_operation_id = p_operation_id,
      freshness_validation_status = v_status,
      freshness_checked_at_utc = v_checked_at_utc,
      freshness_result_hash = v_result_hash,
      freshness_scope_hash = v_scope_hash,
      freshness_result_json = jsonb_build_object(
        'validation_complete', true,
        'is_stale', v_is_stale,
        'status', v_status,
        'stale_reasons', coalesce(v_stale_reasons, '[]'::jsonb),
        'stale_reason_counts', coalesce(v_stale_reason_counts, '{}'::jsonb),
        'checked_count', coalesce(v_checked_count, 0),
        'stale_count', coalesce(v_stale_count, 0),
        'failed_count', coalesce(v_failed_count, 0),
        'key_resolution_failure_count', coalesce(v_key_resolution_failure_count, 0),
        'diff_sample', coalesce(v_diff_sample, '[]'::jsonb),
        'failed_sample', coalesce(v_failed_sample, '[]'::jsonb),
        'freshness_result_hash', v_result_hash,
        'freshness_scope_hash', v_scope_hash,
        'freshness_checked_at_utc', to_jsonb(v_checked_at_utc)
      )
  where pay_batch_update.id = p_pay_batch_id;

  update public.banking_pay_operations as operation_update
  set progress_json = coalesce(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'freshness_validation_status', v_status,
          'freshness_result_hash', v_result_hash,
          'freshness_scope_hash', v_scope_hash,
          'freshness_checked_at_utc', to_jsonb(v_checked_at_utc),
          'freshness_is_stale', v_is_stale
        ),
      result_json = coalesce(operation_update.result_json, '{}'::jsonb)
        || jsonb_build_object('freshness', v_result),
      updated_at_utc = now()
  where operation_update.id = p_operation_id;

  return v_result;
end;
$$;

CREATE OR REPLACE FUNCTION public.pay_bank_csv_export_summary_get(
    p_pay_batch_id uuid,
    p_scope text DEFAULT 'ALL'::text,
    p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
    v_scope text := upper(btrim(coalesce(p_scope, 'ALL')));
    v_batch_row public.pay_batches%ROWTYPE;
    v_batch_status_upper text := NULL;
    v_execution_commit_state text := 'NOT_SUBMITTED';
    v_execution_commit_ref text := NULL;
    v_execution_committed_at_utc timestamptz := NULL;
    v_pre_execution_export boolean := false;

    v_row_count integer := 0;
    v_total_amount numeric := 0;
    v_current_transfer_hash text := NULL;
    v_current_transfer_hash_basis text := NULL;

    v_pending_row_count integer := 0;
    v_pending_total_amount numeric := 0;
    v_pending_transfer_hash text := NULL;

    v_stable_row_count integer := 0;
    v_stable_total_amount numeric := 0;
    v_stable_transfer_hash text := NULL;

    v_blocked_row_count integer := 0;
    v_all_transfer_count integer := 0;
    v_pay_channels_included jsonb := '[]'::jsonb;
    v_status_counts jsonb := '{}'::jsonb;
    v_provider_counts jsonb := '{}'::jsonb;
    v_rail_env_counts jsonb := '{}'::jsonb;
    v_exportable_status_filter text := NULL;
    v_bank_csv_export_hash text := NULL;
BEGIN
    IF p_pay_batch_id IS NULL THEN
        RAISE EXCEPTION '%', jsonb_build_object(
            'error', 'PAY_BANK_CSV_EXPORT_SUMMARY_GET',
            'code', 'PAY_BATCH_ID_REQUIRED',
            'message', 'pay_bank_csv_export_summary_get: pay_batch_id is required'
        )::text;
    END IF;

    IF v_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA') THEN
        RAISE EXCEPTION '%', jsonb_build_object(
            'error', 'PAY_BANK_CSV_EXPORT_SUMMARY_GET',
            'code', 'INVALID_SCOPE',
            'message', 'pay_bank_csv_export_summary_get: scope must be ALL, PAYE, or UMBRELLA',
            'scope', p_scope
        )::text;
    END IF;

    IF p_actor_user_id IS NOT NULL THEN
        PERFORM 1
        FROM public.tms_users AS actor_user
        WHERE actor_user.id = p_actor_user_id;

        IF NOT FOUND THEN
            RAISE EXCEPTION '%', jsonb_build_object(
                'error', 'PAY_BANK_CSV_EXPORT_SUMMARY_GET',
                'code', 'ACTOR_USER_NOT_FOUND',
                'message', 'pay_bank_csv_export_summary_get: actor user was not found',
                'actor_user_id', p_actor_user_id::text,
                'pay_batch_id', p_pay_batch_id::text
            )::text;
        END IF;
    END IF;

    SELECT pay_batch.*
    INTO v_batch_row
    FROM public.pay_batches AS pay_batch
    WHERE pay_batch.id = p_pay_batch_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION '%', jsonb_build_object(
            'error', 'PAY_BANK_CSV_EXPORT_SUMMARY_GET',
            'code', 'PAY_BATCH_NOT_FOUND',
            'message', 'pay_bank_csv_export_summary_get: pay batch was not found',
            'pay_batch_id', p_pay_batch_id::text
        )::text;
    END IF;

    v_batch_status_upper := upper(btrim(coalesce(v_batch_row.status, '')));
    v_execution_commit_state := upper(btrim(coalesce(v_batch_row.execution_commit_state, 'NOT_SUBMITTED')));

    IF v_execution_commit_state NOT IN ('NOT_SUBMITTED', 'SUBMITTED_NOT_COMMITTED', 'COMMITTED') THEN
        v_execution_commit_state := 'NOT_SUBMITTED';
    END IF;

    v_execution_commit_ref := nullif(btrim(coalesce(v_batch_row.execution_commit_ref, '')), '');
    v_execution_committed_at_utc := v_batch_row.execution_committed_at_utc;
    v_bank_csv_export_hash := nullif(btrim(coalesce(v_batch_row.bank_csv_export_json->>'transfer_hash', '')), '');

    v_pre_execution_export := (
        v_batch_row.cancelled_at_utc IS NULL
        AND v_batch_status_upper NOT IN ('CANCELLED', 'CANCELED')
        AND v_execution_commit_state = 'NOT_SUBMITTED'
        AND v_execution_commit_ref IS NULL
        AND v_execution_committed_at_utc IS NULL
    );

    SELECT
        count(*)::integer,
        round(coalesce(sum(CASE WHEN upper(coalesce(pending_transfer.status, '')) = 'PENDING' THEN pending_transfer.amount ELSE 0 END), 0), 2),
        md5(coalesce(jsonb_agg(
            jsonb_build_object(
                'id', pending_transfer.id,
                'candidate_id', pending_transfer.candidate_id,
                'umbrella_id', pending_transfer.umbrella_id,
                'pay_channel', pending_transfer.pay_channel,
                'amount', pending_transfer.amount,
                'currency', pending_transfer.currency,
                'payment_reference', pending_transfer.payment_reference,
                'payee_name', pending_transfer.payee_name,
                'sort_code_digits', pending_transfer.sort_code_digits,
                'account_number_digits', pending_transfer.account_number_digits,
                'account_type', pending_transfer.account_type,
                'bank_details_hash_snapshot', pending_transfer.bank_details_hash_snapshot,
                'payee_entity_kind', pending_transfer.payee_entity_kind,
                'payee_entity_id', pending_transfer.payee_entity_id,
                'transfer_group_key', pending_transfer.transfer_group_key
            )
            ORDER BY pending_transfer.id
        )::text, '[]'))
    INTO
        v_pending_row_count,
        v_pending_total_amount,
        v_pending_transfer_hash
    FROM (
        SELECT
            transfer_row.id::text AS id,
            CASE WHEN transfer_row.candidate_id IS NULL THEN NULL ELSE transfer_row.candidate_id::text END AS candidate_id,
            CASE WHEN transfer_row.umbrella_id IS NULL THEN NULL ELSE transfer_row.umbrella_id::text END AS umbrella_id,
            transfer_row.pay_channel AS pay_channel,
            round(transfer_row.amount, 2) AS amount,
            transfer_row.currency AS currency,
            transfer_row.payment_reference AS payment_reference,
            transfer_row.payee_name AS payee_name,
            regexp_replace(coalesce(transfer_row.sort_code, ''), '[^0-9]', '', 'g') AS sort_code_digits,
            regexp_replace(coalesce(transfer_row.account_number, ''), '[^0-9]', '', 'g') AS account_number_digits,
            transfer_row.account_type AS account_type,
            transfer_row.bank_details_hash_snapshot AS bank_details_hash_snapshot,
            transfer_row.payee_entity_kind AS payee_entity_kind,
            CASE WHEN transfer_row.payee_entity_id IS NULL THEN NULL ELSE transfer_row.payee_entity_id::text END AS payee_entity_id,
            transfer_row.transfer_group_key AS transfer_group_key,
            transfer_row.status AS status
        FROM public.pay_bank_transfers AS transfer_row
        WHERE transfer_row.pay_batch_id = p_pay_batch_id
          AND (v_scope = 'ALL' OR upper(coalesce(transfer_row.pay_channel, '')) = v_scope)
          AND upper(coalesce(transfer_row.status, '')) = 'PENDING'
        ORDER BY transfer_row.id
    ) AS pending_transfer;

    SELECT
        count(*)::integer,
        round(coalesce(sum(stable_transfer.amount), 0), 2),
        md5(coalesce(jsonb_agg(
            jsonb_build_object(
                'id', stable_transfer.id,
                'candidate_id', stable_transfer.candidate_id,
                'umbrella_id', stable_transfer.umbrella_id,
                'pay_channel', stable_transfer.pay_channel,
                'amount', stable_transfer.amount,
                'currency', stable_transfer.currency,
                'payment_reference', stable_transfer.payment_reference,
                'payee_name', stable_transfer.payee_name,
                'sort_code_digits', stable_transfer.sort_code_digits,
                'account_number_digits', stable_transfer.account_number_digits,
                'account_type', stable_transfer.account_type,
                'bank_details_hash_snapshot', stable_transfer.bank_details_hash_snapshot,
                'payee_entity_kind', stable_transfer.payee_entity_kind,
                'payee_entity_id', stable_transfer.payee_entity_id,
                'transfer_group_key', stable_transfer.transfer_group_key
            )
            ORDER BY stable_transfer.id
        )::text, '[]'))
    INTO
        v_stable_row_count,
        v_stable_total_amount,
        v_stable_transfer_hash
    FROM (
        SELECT
            transfer_row.id::text AS id,
            CASE WHEN transfer_row.candidate_id IS NULL THEN NULL ELSE transfer_row.candidate_id::text END AS candidate_id,
            CASE WHEN transfer_row.umbrella_id IS NULL THEN NULL ELSE transfer_row.umbrella_id::text END AS umbrella_id,
            transfer_row.pay_channel AS pay_channel,
            round(transfer_row.amount, 2) AS amount,
            transfer_row.currency AS currency,
            transfer_row.payment_reference AS payment_reference,
            transfer_row.payee_name AS payee_name,
            regexp_replace(coalesce(transfer_row.sort_code, ''), '[^0-9]', '', 'g') AS sort_code_digits,
            regexp_replace(coalesce(transfer_row.account_number, ''), '[^0-9]', '', 'g') AS account_number_digits,
            transfer_row.account_type AS account_type,
            transfer_row.bank_details_hash_snapshot AS bank_details_hash_snapshot,
            transfer_row.payee_entity_kind AS payee_entity_kind,
            CASE WHEN transfer_row.payee_entity_id IS NULL THEN NULL ELSE transfer_row.payee_entity_id::text END AS payee_entity_id,
            transfer_row.transfer_group_key AS transfer_group_key
        FROM public.pay_bank_transfers AS transfer_row
        WHERE transfer_row.pay_batch_id = p_pay_batch_id
          AND (v_scope = 'ALL' OR upper(coalesce(transfer_row.pay_channel, '')) = v_scope)
          AND upper(coalesce(transfer_row.status, '')) <> 'BLOCKED'
        ORDER BY transfer_row.id
    ) AS stable_transfer;

    IF v_pre_execution_export THEN
        v_row_count := coalesce(v_pending_row_count, 0);
        v_total_amount := coalesce(v_pending_total_amount, 0);
        v_current_transfer_hash := v_pending_transfer_hash;
        v_current_transfer_hash_basis := 'PENDING_TRANSFER_HASH';
        v_exportable_status_filter := 'PENDING';
    ELSE
        v_row_count := coalesce(v_stable_row_count, 0);
        v_total_amount := coalesce(v_stable_total_amount, 0);
        v_current_transfer_hash := v_stable_transfer_hash;
        v_current_transfer_hash_basis := 'STABLE_ALL_NON_BLOCKED_TRANSFER_HASH';
        v_exportable_status_filter := 'ALL_NON_BLOCKED';
    END IF;

    SELECT count(*)::integer
    INTO v_all_transfer_count
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.pay_batch_id = p_pay_batch_id
      AND (v_scope = 'ALL' OR upper(coalesce(transfer_row.pay_channel, '')) = v_scope);

    SELECT count(*)::integer
    INTO v_blocked_row_count
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.pay_batch_id = p_pay_batch_id
      AND (v_scope = 'ALL' OR upper(coalesce(transfer_row.pay_channel, '')) = v_scope)
      AND upper(coalesce(transfer_row.status, '')) = 'BLOCKED';

    SELECT coalesce(jsonb_agg(to_jsonb(pay_channel_values.pay_channel) ORDER BY pay_channel_values.pay_channel), '[]'::jsonb)
    INTO v_pay_channels_included
    FROM (
        SELECT DISTINCT upper(coalesce(transfer_row.pay_channel, '')) AS pay_channel
        FROM public.pay_bank_transfers AS transfer_row
        WHERE transfer_row.pay_batch_id = p_pay_batch_id
          AND (v_scope = 'ALL' OR upper(coalesce(transfer_row.pay_channel, '')) = v_scope)
          AND (
              (v_pre_execution_export = true AND upper(coalesce(transfer_row.status, '')) = 'PENDING')
              OR (v_pre_execution_export = false AND upper(coalesce(transfer_row.status, '')) <> 'BLOCKED')
          )
    ) AS pay_channel_values
    WHERE pay_channel_values.pay_channel <> '';

    SELECT coalesce(jsonb_object_agg(status_counts.status_key, status_counts.status_count ORDER BY status_counts.status_key), '{}'::jsonb)
    INTO v_status_counts
    FROM (
        SELECT
            upper(coalesce(transfer_row.status, '')) AS status_key,
            count(*)::integer AS status_count
        FROM public.pay_bank_transfers AS transfer_row
        WHERE transfer_row.pay_batch_id = p_pay_batch_id
          AND (v_scope = 'ALL' OR upper(coalesce(transfer_row.pay_channel, '')) = v_scope)
        GROUP BY upper(coalesce(transfer_row.status, ''))
    ) AS status_counts
    WHERE status_counts.status_key <> '';

    SELECT coalesce(jsonb_object_agg(provider_counts.provider_key, provider_counts.provider_count ORDER BY provider_counts.provider_key), '{}'::jsonb)
    INTO v_provider_counts
    FROM (
        SELECT
            upper(coalesce(transfer_row.rail_provider, '')) AS provider_key,
            count(*)::integer AS provider_count
        FROM public.pay_bank_transfers AS transfer_row
        WHERE transfer_row.pay_batch_id = p_pay_batch_id
          AND (v_scope = 'ALL' OR upper(coalesce(transfer_row.pay_channel, '')) = v_scope)
          AND (
              (v_pre_execution_export = true AND upper(coalesce(transfer_row.status, '')) = 'PENDING')
              OR (v_pre_execution_export = false AND upper(coalesce(transfer_row.status, '')) <> 'BLOCKED')
          )
        GROUP BY upper(coalesce(transfer_row.rail_provider, ''))
    ) AS provider_counts
    WHERE provider_counts.provider_key <> '';

    SELECT coalesce(jsonb_object_agg(env_counts.env_key, env_counts.env_count ORDER BY env_counts.env_key), '{}'::jsonb)
    INTO v_rail_env_counts
    FROM (
        SELECT
            upper(coalesce(transfer_row.rail_env, '')) AS env_key,
            count(*)::integer AS env_count
        FROM public.pay_bank_transfers AS transfer_row
        WHERE transfer_row.pay_batch_id = p_pay_batch_id
          AND (v_scope = 'ALL' OR upper(coalesce(transfer_row.pay_channel, '')) = v_scope)
          AND (
              (v_pre_execution_export = true AND upper(coalesce(transfer_row.status, '')) = 'PENDING')
              OR (v_pre_execution_export = false AND upper(coalesce(transfer_row.status, '')) <> 'BLOCKED')
          )
        GROUP BY upper(coalesce(transfer_row.rail_env, ''))
    ) AS env_counts
    WHERE env_counts.env_key <> '';

    RETURN jsonb_strip_nulls(jsonb_build_object(
        'ok', true,
        'pay_batch_id', p_pay_batch_id::text,
        'scope', v_scope,
        'pay_channel_scope', v_scope,
        'pre_execution_export', v_pre_execution_export,
        'exportable_status_filter', v_exportable_status_filter,
        'row_count', coalesce(v_row_count, 0),
        'total_amount', coalesce(v_total_amount, 0),
        'transfer_hash', v_current_transfer_hash,
        'current_transfer_hash', v_current_transfer_hash,
        'current_transfer_hash_basis', v_current_transfer_hash_basis,
        'pending_transfer_hash', v_pending_transfer_hash,
        'stable_transfer_hash', v_stable_transfer_hash,
        'provider_snapshot', nullif(btrim(coalesce(v_batch_row.rail_provider_snapshot, '')), ''),
        'rail_env_snapshot', nullif(btrim(coalesce(v_batch_row.rail_env_snapshot, '')), ''),
        'freshness_validation_status', nullif(upper(btrim(coalesce(v_batch_row.freshness_validation_status, ''))), ''),
        'freshness_checked_at_utc', CASE WHEN v_batch_row.freshness_checked_at_utc IS NULL THEN NULL ELSE v_batch_row.freshness_checked_at_utc::text END,
        'freshness_result_hash', nullif(btrim(coalesce(v_batch_row.freshness_result_hash, '')), ''),
        'freshness_scope_hash', nullif(btrim(coalesce(v_batch_row.freshness_scope_hash, '')), ''),
        'freshness_operation_id', CASE WHEN v_batch_row.freshness_operation_id IS NULL THEN NULL ELSE v_batch_row.freshness_operation_id::text END,
        'bank_csv_export_hash', v_bank_csv_export_hash,
        'csv_export_matches_current_transfers', CASE WHEN v_bank_csv_export_hash IS NULL OR v_current_transfer_hash IS NULL THEN NULL ELSE v_bank_csv_export_hash = v_current_transfer_hash END,
        'pending_row_count', coalesce(v_pending_row_count, 0),
        'pending_total_amount', coalesce(v_pending_total_amount, 0),
        'stable_row_count', coalesce(v_stable_row_count, 0),
        'stable_total_amount', coalesce(v_stable_total_amount, 0),
        'blocked_row_count', coalesce(v_blocked_row_count, 0),
        'all_transfer_count', coalesce(v_all_transfer_count, 0),
        'pay_channels_included', coalesce(v_pay_channels_included, '[]'::jsonb),
        'status_counts', coalesce(v_status_counts, '{}'::jsonb),
        'provider_counts', coalesce(v_provider_counts, '{}'::jsonb),
        'rail_env_counts', coalesce(v_rail_env_counts, '{}'::jsonb),
        'summary_source', 'pay_bank_transfers',
        'policy_x_authority', 'FROZEN_MATERIALISED_TRANSFER_ROWS',
        'generated_at_utc', now()::text
    ));
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_scope_seed(p_operation_id uuid, p_workbench_session_id uuid, p_actor_user_id uuid, p_selected_preview_row_ids jsonb DEFAULT NULL::jsonb, p_pay_channel_scope text DEFAULT NULL::text, p_same_week_paye_override_json jsonb DEFAULT '{}'::jsonb)
 RETURNS TABLE(candidate_scope_count integer, selected_row_count integer, timesheet_count integer, finance_case_count integer, pay_channel_count integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
    v_operation public.banking_pay_operations%rowtype;
    v_session public.banking_pay_workbench_sessions%rowtype;
    v_scope_filter text;
    v_selected_input jsonb;
    v_selected_row_count integer;
    v_existing_mismatch_count integer;
    v_same_week_paye_override_json jsonb;
    v_candidate_scope_count integer;
    v_selected_count integer;
    v_timesheet_count integer;
    v_finance_case_count integer;
    v_pay_channel_count integer;
begin
    v_scope_filter := upper(coalesce(nullif(btrim(p_pay_channel_scope), ''), 'ALL'));
    v_same_week_paye_override_json := coalesce(p_same_week_paye_override_json, '{}'::jsonb);

    if v_scope_filter not in ('ALL', 'PAYE', 'UMBRELLA') then
        raise exception 'pay_workbench_prepare_draft_scope_seed unsupported pay channel scope: %', v_scope_filter;
    end if;

    if jsonb_typeof(v_same_week_paye_override_json) <> 'object' then
        raise exception 'pay_workbench_prepare_draft_scope_seed requires p_same_week_paye_override_json to be a JSON object';
    end if;

    select operation_row.*
    into v_operation
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
        raise exception 'pay_workbench_prepare_draft_scope_seed operation not found: %', p_operation_id;
    end if;

    if v_operation.operation_type <> 'DRAFT_CREATE' then
        raise exception 'pay_workbench_prepare_draft_scope_seed expected DRAFT_CREATE operation %, got %', p_operation_id, v_operation.operation_type;
    end if;

    if v_operation.status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        raise exception 'pay_workbench_prepare_draft_scope_seed cannot seed terminal operation % with status %', p_operation_id, v_operation.status;
    end if;

    if v_operation.workbench_session_id is not null and v_operation.workbench_session_id <> p_workbench_session_id then
        raise exception 'pay_workbench_prepare_draft_scope_seed operation % belongs to a different workbench session', p_operation_id;
    end if;

    if p_actor_user_id is not null and v_operation.actor_user_id is not null and v_operation.actor_user_id <> p_actor_user_id then
        raise exception 'pay_workbench_prepare_draft_scope_seed operation % belongs to a different actor', p_operation_id;
    end if;

    select session_row.*
    into v_session
    from public.banking_pay_workbench_sessions as session_row
    where session_row.id = p_workbench_session_id
    for update;

    if not found then
        raise exception 'pay_workbench_prepare_draft_scope_seed workbench session not found: %', p_workbench_session_id;
    end if;

    if v_session.status <> 'OPEN' then
        raise exception 'pay_workbench_prepare_draft_scope_seed workbench session % is not OPEN: %', p_workbench_session_id, v_session.status;
    end if;

    if v_session.discarded_at_utc is not null then
        raise exception 'pay_workbench_prepare_draft_scope_seed workbench session % has been discarded', p_workbench_session_id;
    end if;

    if p_actor_user_id is not null and v_session.actor_user_id <> p_actor_user_id then
        raise exception 'pay_workbench_prepare_draft_scope_seed workbench session % belongs to a different actor', p_workbench_session_id;
    end if;

    if p_selected_preview_row_ids is not null then
        if jsonb_typeof(p_selected_preview_row_ids) <> 'array' then
            raise exception 'pay_workbench_prepare_draft_scope_seed requires p_selected_preview_row_ids to be null or a JSON array';
        end if;

        select coalesce(jsonb_agg(to_jsonb(normalized_selection.preview_row_id) order by normalized_selection.ord), '[]'::jsonb), count(*)::integer
        into v_selected_input, v_selected_row_count
        from (
            select distinct on (raw_selection.preview_row_id)
                raw_selection.preview_row_id,
                raw_selection.ord
            from (
                select btrim(selection_element.value) as preview_row_id,
                       selection_element.ord
                from jsonb_array_elements_text(p_selected_preview_row_ids) with ordinality as selection_element(value, ord)
                where btrim(selection_element.value) <> ''
            ) as raw_selection
            order by raw_selection.preview_row_id, raw_selection.ord
        ) as normalized_selection;
    else
        if coalesce(v_session.server_selected_preview_row_ids_provided, false) is not true then
            raise exception 'pay_workbench_prepare_draft_scope_seed requires server-selected preview rows for session %', p_workbench_session_id;
        end if;

        if jsonb_typeof(coalesce(v_session.server_selected_preview_row_ids, '[]'::jsonb)) <> 'array' then
            raise exception 'pay_workbench_prepare_draft_scope_seed session % has invalid server_selected_preview_row_ids', p_workbench_session_id;
        end if;

        select coalesce(jsonb_agg(to_jsonb(normalized_selection.preview_row_id) order by normalized_selection.ord), '[]'::jsonb), count(*)::integer
        into v_selected_input, v_selected_row_count
        from (
            select distinct on (raw_selection.preview_row_id)
                raw_selection.preview_row_id,
                raw_selection.ord
            from (
                select btrim(selection_element.value) as preview_row_id,
                       selection_element.ord
                from jsonb_array_elements_text(coalesce(v_session.server_selected_preview_row_ids, '[]'::jsonb)) with ordinality as selection_element(value, ord)
                where btrim(selection_element.value) <> ''
            ) as raw_selection
            order by raw_selection.preview_row_id, raw_selection.ord
        ) as normalized_selection;
    end if;

    if coalesce(v_selected_row_count, 0) = 0 then
        raise exception 'pay_workbench_prepare_draft_scope_seed requires at least one selected preview row';
    end if;

    if coalesce(v_session.server_selected_preview_row_ids_provided, false) is not true then
        raise exception 'pay_workbench_prepare_draft_scope_seed session % does not have persisted server-selected preview rows', p_workbench_session_id;
    end if;

    if v_selected_input is distinct from coalesce(v_session.server_selected_preview_row_ids, '[]'::jsonb) then
        raise exception 'pay_workbench_prepare_draft_scope_seed supplied selected preview rows do not match session server-selected preview rows for session %', p_workbench_session_id;
    end if;

    drop table if exists pg_temp.tmp_pay_workbench_scope_seed_requested_rows;
    create temporary table pg_temp.tmp_pay_workbench_scope_seed_requested_rows (
        preview_row_id text primary key,
        ord bigint not null
    ) on commit drop;

    insert into pg_temp.tmp_pay_workbench_scope_seed_requested_rows(preview_row_id, ord)
    select normalized_selection.preview_row_id,
           normalized_selection.ord
    from (
        select distinct on (raw_selection.preview_row_id)
            raw_selection.preview_row_id,
            raw_selection.ord
        from (
            select btrim(selection_element.value) as preview_row_id,
                   selection_element.ord
            from jsonb_array_elements_text(v_selected_input) with ordinality as selection_element(value, ord)
            where btrim(selection_element.value) <> ''
        ) as raw_selection
        order by raw_selection.preview_row_id, raw_selection.ord
    ) as normalized_selection;

    drop table if exists pg_temp.tmp_pay_workbench_scope_seed_ready_state;
    create temporary table pg_temp.tmp_pay_workbench_scope_seed_ready_state as
    select distinct on (state_row.candidate_id)
        state_row.id as candidate_state_id,
        state_row.session_id,
        state_row.candidate_id,
        state_row.status,
        state_row.effective_candidate_fragment_json,
        state_row.effective_summary_fragment_json,
        state_row.effective_paye_candidate_json,
        state_row.effective_non_paye_payee_json,
        state_row.effective_payees_json,
        state_row.effective_case_resolution_states_json,
        state_row.effective_canonical_preview_lines_json,
        state_row.session_version,
        state_row.updated_at_utc
    from public.banking_pay_workbench_session_candidate_state as state_row
    where state_row.session_id = p_workbench_session_id
      and state_row.status = 'READY'
    order by state_row.candidate_id, state_row.updated_at_utc desc, state_row.id desc;

    drop table if exists pg_temp.tmp_pay_workbench_scope_seed_selected_lines;
    create temporary table pg_temp.tmp_pay_workbench_scope_seed_selected_lines as
    select
        ready_state.candidate_state_id,
        ready_state.candidate_id,
        upper(btrim(coalesce(line_element.value->>'pay_channel', ''))) as pay_channel,
        btrim(coalesce(
            line_element.value->>'preview_row_id',
            line_element.value->>'line_id',
            line_element.value->>'row_id',
            line_element.value->>'id',
            ''
        )) as preview_row_id,
        requested_rows.ord,
        line_element.value as line_json,
        ready_state.effective_candidate_fragment_json,
        ready_state.effective_summary_fragment_json,
        ready_state.effective_paye_candidate_json,
        ready_state.effective_non_paye_payee_json,
        ready_state.effective_payees_json,
        ready_state.effective_case_resolution_states_json,
        ready_state.effective_canonical_preview_lines_json,
        ready_state.session_version
    from pg_temp.tmp_pay_workbench_scope_seed_ready_state as ready_state
    cross join lateral jsonb_array_elements(
        case
            when jsonb_typeof(coalesce(ready_state.effective_canonical_preview_lines_json, '[]'::jsonb)) = 'array' then coalesce(ready_state.effective_canonical_preview_lines_json, '[]'::jsonb)
            else '[]'::jsonb
        end
    ) as line_element(value)
    join pg_temp.tmp_pay_workbench_scope_seed_requested_rows as requested_rows
      on requested_rows.preview_row_id = btrim(coalesce(
             line_element.value->>'preview_row_id',
             line_element.value->>'line_id',
             line_element.value->>'row_id',
             line_element.value->>'id',
             ''
         ))
    where jsonb_typeof(line_element.value) = 'object'
      and btrim(coalesce(
             line_element.value->>'preview_row_id',
             line_element.value->>'line_id',
             line_element.value->>'row_id',
             line_element.value->>'id',
             ''
         )) <> ''
      and upper(btrim(coalesce(line_element.value->>'pay_channel', ''))) in ('PAYE', 'UMBRELLA')
      and (v_scope_filter = 'ALL' or upper(btrim(coalesce(line_element.value->>'pay_channel', ''))) = v_scope_filter);

    select count(*)::integer
    into v_selected_count
    from pg_temp.tmp_pay_workbench_scope_seed_selected_lines as selected_line;

    if coalesce(v_selected_count, 0) = 0 then
        raise exception 'pay_workbench_prepare_draft_scope_seed found no READY selected rows for session %, scope %', p_workbench_session_id, v_scope_filter;
    end if;

    select count(*)::integer
    into v_existing_mismatch_count
    from (
        select requested_rows.preview_row_id
        from pg_temp.tmp_pay_workbench_scope_seed_requested_rows as requested_rows
        where not exists (
            select 1
            from pg_temp.tmp_pay_workbench_scope_seed_selected_lines as selected_line
            where selected_line.preview_row_id = requested_rows.preview_row_id
        )
    ) as unresolved_rows
    where v_scope_filter = 'ALL';

    if coalesce(v_existing_mismatch_count, 0) > 0 then
        raise exception 'pay_workbench_prepare_draft_scope_seed selected rows did not resolve against READY session state for session %', p_workbench_session_id;
    end if;

    drop table if exists pg_temp.tmp_pay_workbench_scope_seed_scopes;
    create temporary table pg_temp.tmp_pay_workbench_scope_seed_scopes as
    select
        p_operation_id as operation_id,
        p_workbench_session_id as workbench_session_id,
        v_session.source_snapshot_run_id as source_snapshot_run_id,
        v_session.version as source_session_version,
        grouped_lines.candidate_state_id,
        grouped_lines.candidate_id,
        grouped_lines.pay_channel,
        coalesce(jsonb_agg(to_jsonb(grouped_lines.preview_row_id) order by grouped_lines.ord), '[]'::jsonb) as selected_preview_row_ids_json,
        coalesce(jsonb_agg(distinct to_jsonb(grouped_lines.timesheet_id_text)) filter (where grouped_lines.timesheet_id_text is not null), '[]'::jsonb) as selected_timesheet_ids_json,
        coalesce(jsonb_agg(distinct to_jsonb(grouped_lines.finance_case_id_text)) filter (where grouped_lines.finance_case_id_text is not null), '[]'::jsonb) as selected_finance_case_ids_json,
        grouped_lines.effective_candidate_fragment_json,
        grouped_lines.effective_summary_fragment_json,
        grouped_lines.effective_paye_candidate_json,
        grouped_lines.effective_non_paye_payee_json,
        grouped_lines.effective_payees_json,
        grouped_lines.effective_case_resolution_states_json,
        grouped_lines.effective_canonical_preview_lines_json,
        coalesce(jsonb_agg(grouped_lines.line_json order by grouped_lines.ord), '[]'::jsonb) as selected_canonical_preview_lines_json,
        case
            when jsonb_typeof(grouped_lines.effective_candidate_fragment_json->'baseline_component_rows') = 'array' then coalesce(grouped_lines.effective_candidate_fragment_json->'baseline_component_rows', '[]'::jsonb)
            else '[]'::jsonb
        end as baseline_component_rows_json,
        case
            when jsonb_typeof(grouped_lines.effective_candidate_fragment_json->'hidden_recovery_template_lines') = 'array' then coalesce(grouped_lines.effective_candidate_fragment_json->'hidden_recovery_template_lines', '[]'::jsonb)
            else '[]'::jsonb
        end as hidden_recovery_template_lines_json,
        jsonb_build_object(
            'selected_preview_row_count', count(*)::integer,
            'selected_timesheet_count', count(distinct grouped_lines.timesheet_id_text) filter (where grouped_lines.timesheet_id_text is not null),
            'selected_finance_case_count', count(distinct grouped_lines.finance_case_id_text) filter (where grouped_lines.finance_case_id_text is not null),
            'selected_amount_ex_vat', round(coalesce(sum(grouped_lines.amount_ex_vat), 0), 2),
            'pay_channel', grouped_lines.pay_channel
        ) as candidate_totals_json,
        jsonb_build_object(
            'source', 'banking_pay_workbench_session_candidate_state',
            'same_week_paye_override', v_same_week_paye_override_json,
            'pay_channel_scope', v_scope_filter,
            'session_signature', v_session.session_signature,
            'session_version', v_session.version
        ) as allocation_basis_json
    from (
        select
            selected_line.candidate_state_id,
            selected_line.candidate_id,
            selected_line.pay_channel,
            selected_line.preview_row_id,
            selected_line.ord,
            selected_line.line_json,
            selected_line.effective_candidate_fragment_json,
            selected_line.effective_summary_fragment_json,
            case when jsonb_typeof(selected_line.effective_paye_candidate_json) = 'object' then selected_line.effective_paye_candidate_json else '{}'::jsonb end as effective_paye_candidate_json,
            case when jsonb_typeof(selected_line.effective_non_paye_payee_json) = 'object' then selected_line.effective_non_paye_payee_json else '{}'::jsonb end as effective_non_paye_payee_json,
            case when jsonb_typeof(selected_line.effective_payees_json) = 'array' then selected_line.effective_payees_json else '[]'::jsonb end as effective_payees_json,
            case
                when jsonb_typeof(selected_line.effective_case_resolution_states_json) = 'array' then jsonb_build_object('rows', selected_line.effective_case_resolution_states_json)
                when jsonb_typeof(selected_line.effective_case_resolution_states_json) = 'object' then selected_line.effective_case_resolution_states_json
                else jsonb_build_object('rows', '[]'::jsonb)
            end as effective_case_resolution_states_json,
            case when jsonb_typeof(selected_line.effective_canonical_preview_lines_json) = 'array' then selected_line.effective_canonical_preview_lines_json else '[]'::jsonb end as effective_canonical_preview_lines_json,
            case
                when coalesce(selected_line.line_json->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then selected_line.line_json->>'timesheet_id'
                else null::text
            end as timesheet_id_text,
            case
                when coalesce(selected_line.line_json->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then selected_line.line_json->>'finance_case_id'
                else null::text
            end as finance_case_id_text,
            case
                when coalesce(selected_line.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (selected_line.line_json->>'amount_ex_vat')::numeric
                when coalesce(selected_line.line_json->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (selected_line.line_json->>'preview_amount_ex_vat')::numeric
                when coalesce(selected_line.line_json->>'amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (selected_line.line_json->>'amount')::numeric
                else 0::numeric
            end as amount_ex_vat
        from pg_temp.tmp_pay_workbench_scope_seed_selected_lines as selected_line
    ) as grouped_lines
    group by
        grouped_lines.candidate_state_id,
        grouped_lines.candidate_id,
        grouped_lines.pay_channel,
        grouped_lines.effective_candidate_fragment_json,
        grouped_lines.effective_summary_fragment_json,
        grouped_lines.effective_paye_candidate_json,
        grouped_lines.effective_non_paye_payee_json,
        grouped_lines.effective_payees_json,
        grouped_lines.effective_case_resolution_states_json,
        grouped_lines.effective_canonical_preview_lines_json;

    drop table if exists pg_temp.tmp_pay_workbench_scope_seed_required_payees;
    create temporary table pg_temp.tmp_pay_workbench_scope_seed_required_payees as
    with selected_line_routes as (
        select
            proposed_scope.operation_id,
            proposed_scope.workbench_session_id,
            proposed_scope.candidate_id,
            proposed_scope.pay_channel,
            selected_line_element.ord as line_ord,
            selected_line_element.value as line_json,
            case
                when upper(btrim(coalesce(selected_line_element.value->>'payee_entity_kind', selected_line_element.value->>'line_payee_entity_kind', selected_line_element.value->>'entity_kind', ''))) = 'UMBRELLA_COMPANY' then 'UMBRELLA'
                else upper(btrim(coalesce(selected_line_element.value->>'payee_entity_kind', selected_line_element.value->>'line_payee_entity_kind', selected_line_element.value->>'entity_kind', '')))
            end as line_payee_entity_kind,
            case
                when btrim(coalesce(selected_line_element.value->>'payee_entity_id', selected_line_element.value->>'line_payee_entity_id', selected_line_element.value->>'entity_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    then btrim(coalesce(selected_line_element.value->>'payee_entity_id', selected_line_element.value->>'line_payee_entity_id', selected_line_element.value->>'entity_id', ''))::uuid
                else null::uuid
            end as line_payee_entity_id,
            case
                when btrim(coalesce(proposed_scope.effective_non_paye_payee_json->>'payee_entity_id', proposed_scope.effective_non_paye_payee_json->>'umbrella_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                    then btrim(coalesce(proposed_scope.effective_non_paye_payee_json->>'payee_entity_id', proposed_scope.effective_non_paye_payee_json->>'umbrella_id', ''))::uuid
                else null::uuid
            end as non_paye_payee_entity_id
        from pg_temp.tmp_pay_workbench_scope_seed_scopes as proposed_scope
        cross join lateral jsonb_array_elements(
            case
                when jsonb_typeof(coalesce(proposed_scope.selected_canonical_preview_lines_json, '[]'::jsonb)) = 'array' then coalesce(proposed_scope.selected_canonical_preview_lines_json, '[]'::jsonb)
                else '[]'::jsonb
            end
        ) with ordinality as selected_line_element(value, ord)
        where proposed_scope.operation_id = p_operation_id
          and proposed_scope.workbench_session_id = p_workbench_session_id
          and jsonb_typeof(selected_line_element.value) = 'object'
    ),
    selected_required_routes as (
        select
            selected_line_routes.operation_id,
            selected_line_routes.workbench_session_id,
            selected_line_routes.candidate_id,
            selected_line_routes.pay_channel,
            selected_line_routes.line_ord,
            case
                when selected_line_routes.line_payee_entity_kind in ('CANDIDATE', 'UMBRELLA') then selected_line_routes.line_payee_entity_kind
                when lower(btrim(coalesce(selected_line_routes.line_json->>'is_candidate_directed_oneoff_payout', ''))) in ('true', 't', '1', 'yes', 'y') then 'CANDIDATE'
                when upper(btrim(coalesce(selected_line_routes.line_json->>'routing_kind', selected_line_routes.line_json->>'route_type', ''))) in ('CANDIDATE', 'CANDIDATE_DIRECT', 'CANDIDATE_DIRECTED', 'CANDIDATE_ONEOFF', 'CANDIDATE_ONE_OFF') then 'CANDIDATE'
                when selected_line_routes.pay_channel = 'PAYE' then 'CANDIDATE'
                when selected_line_routes.pay_channel = 'UMBRELLA' then 'UMBRELLA'
                else null::text
            end as required_payee_entity_kind,
            case
                when selected_line_routes.line_payee_entity_kind = 'CANDIDATE' then coalesce(selected_line_routes.line_payee_entity_id, selected_line_routes.candidate_id)
                when selected_line_routes.line_payee_entity_kind = 'UMBRELLA' then coalesce(selected_line_routes.line_payee_entity_id, selected_line_routes.non_paye_payee_entity_id)
                when lower(btrim(coalesce(selected_line_routes.line_json->>'is_candidate_directed_oneoff_payout', ''))) in ('true', 't', '1', 'yes', 'y') then selected_line_routes.candidate_id
                when upper(btrim(coalesce(selected_line_routes.line_json->>'routing_kind', selected_line_routes.line_json->>'route_type', ''))) in ('CANDIDATE', 'CANDIDATE_DIRECT', 'CANDIDATE_DIRECTED', 'CANDIDATE_ONEOFF', 'CANDIDATE_ONE_OFF') then selected_line_routes.candidate_id
                when selected_line_routes.pay_channel = 'PAYE' then selected_line_routes.candidate_id
                when selected_line_routes.pay_channel = 'UMBRELLA' then selected_line_routes.non_paye_payee_entity_id
                else null::uuid
            end as required_payee_entity_id
        from selected_line_routes
    )
    select distinct
        selected_required_routes.operation_id,
        selected_required_routes.workbench_session_id,
        selected_required_routes.candidate_id,
        selected_required_routes.pay_channel,
        selected_required_routes.required_payee_entity_kind,
        selected_required_routes.required_payee_entity_id
    from selected_required_routes
    where selected_required_routes.required_payee_entity_kind in ('CANDIDATE', 'UMBRELLA');

    update pg_temp.tmp_pay_workbench_scope_seed_scopes as proposed_scope_update
    set effective_payees_json = coalesce((
        select jsonb_agg(payee_element.value order by payee_element.ord)
        from jsonb_array_elements(
            case
                when jsonb_typeof(coalesce(proposed_scope_update.effective_payees_json, '[]'::jsonb)) = 'array' then coalesce(proposed_scope_update.effective_payees_json, '[]'::jsonb)
                else '[]'::jsonb
            end
        ) with ordinality as payee_element(value, ord)
        where jsonb_typeof(payee_element.value) = 'object'
          and exists (
              select 1
              from pg_temp.tmp_pay_workbench_scope_seed_required_payees as required_payee
              where required_payee.operation_id = proposed_scope_update.operation_id
                and required_payee.workbench_session_id = proposed_scope_update.workbench_session_id
                and required_payee.candidate_id = proposed_scope_update.candidate_id
                and required_payee.pay_channel = proposed_scope_update.pay_channel
                and required_payee.required_payee_entity_kind = case
                    when upper(btrim(coalesce(payee_element.value->>'payee_entity_kind', payee_element.value->>'entity_kind', ''))) = 'UMBRELLA_COMPANY' then 'UMBRELLA'
                    else upper(btrim(coalesce(payee_element.value->>'payee_entity_kind', payee_element.value->>'entity_kind', '')))
                end
                and (
                    required_payee.required_payee_entity_id is null
                    or (
                        btrim(coalesce(payee_element.value->>'payee_entity_id', payee_element.value->>'entity_id', '')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        and btrim(coalesce(payee_element.value->>'payee_entity_id', payee_element.value->>'entity_id', ''))::uuid = required_payee.required_payee_entity_id
                    )
                )
          )
    ), '[]'::jsonb)
    where proposed_scope_update.operation_id = p_operation_id
      and proposed_scope_update.workbench_session_id = p_workbench_session_id
      and exists (
          select 1
          from pg_temp.tmp_pay_workbench_scope_seed_required_payees as required_payee
          where required_payee.operation_id = proposed_scope_update.operation_id
            and required_payee.workbench_session_id = proposed_scope_update.workbench_session_id
            and required_payee.candidate_id = proposed_scope_update.candidate_id
            and required_payee.pay_channel = proposed_scope_update.pay_channel
      );

    update pg_temp.tmp_pay_workbench_scope_seed_scopes as proposed_scope_update
    set
        selected_timesheet_ids_json = (
            select coalesce(jsonb_agg(to_jsonb(timesheet_ids.timesheet_id_text) order by timesheet_ids.timesheet_id_text), '[]'::jsonb)
            from (
                select distinct line_element.value->>'timesheet_id' as timesheet_id_text
                from jsonb_array_elements(proposed_scope_update.selected_canonical_preview_lines_json) as line_element(value)
                where coalesce(line_element.value->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            ) as timesheet_ids
        ),
        selected_finance_case_ids_json = (
            select coalesce(jsonb_agg(to_jsonb(finance_case_ids.finance_case_id_text) order by finance_case_ids.finance_case_id_text), '[]'::jsonb)
            from (
                select distinct line_element.value->>'finance_case_id' as finance_case_id_text
                from jsonb_array_elements(proposed_scope_update.selected_canonical_preview_lines_json) as line_element(value)
                where coalesce(line_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            ) as finance_case_ids
        )
    where proposed_scope_update.operation_id = p_operation_id
      and proposed_scope_update.workbench_session_id = p_workbench_session_id;

    if exists (
        select 1
        from pg_temp.tmp_pay_workbench_scope_seed_scopes as proposed_scope
        join public.banking_pay_operation_candidate_scope as existing_scope
          on existing_scope.operation_id = proposed_scope.operation_id
         and existing_scope.candidate_id = proposed_scope.candidate_id
         and existing_scope.pay_channel = proposed_scope.pay_channel
        where existing_scope.scope_hash is distinct from encode(extensions.digest(convert_to(jsonb_build_object(
            'operation_id', proposed_scope.operation_id::text,
            'workbench_session_id', proposed_scope.workbench_session_id::text,
            'source_snapshot_run_id', case when proposed_scope.source_snapshot_run_id is null then null else proposed_scope.source_snapshot_run_id::text end,
            'source_session_version', proposed_scope.source_session_version,
            'candidate_state_id', case when proposed_scope.candidate_state_id is null then null else proposed_scope.candidate_state_id::text end,
            'candidate_id', proposed_scope.candidate_id::text,
            'pay_channel', proposed_scope.pay_channel,
            'selected_preview_row_ids_json', proposed_scope.selected_preview_row_ids_json,
            'selected_timesheet_ids_json', proposed_scope.selected_timesheet_ids_json,
            'selected_finance_case_ids_json', proposed_scope.selected_finance_case_ids_json,
            'selected_canonical_preview_lines_json', proposed_scope.selected_canonical_preview_lines_json,
            'candidate_totals_json', proposed_scope.candidate_totals_json,
            'allocation_basis_json', proposed_scope.allocation_basis_json
        )::text, 'UTF8'), 'sha256'::text), 'hex')
    ) then
        raise exception 'pay_workbench_prepare_draft_scope_seed found existing candidate scope rows with a different scope hash for operation %', p_operation_id;
    end if;

    insert into public.banking_pay_operation_candidate_scope (
        operation_id,
        workbench_session_id,
        source_snapshot_run_id,
        source_session_version,
        candidate_state_id,
        candidate_id,
        pay_channel,
        selected_preview_row_ids_json,
        selected_timesheet_ids_json,
        selected_finance_case_ids_json,
        effective_candidate_fragment_json,
        effective_summary_fragment_json,
        effective_paye_candidate_json,
        effective_non_paye_payee_json,
        effective_payees_json,
        effective_case_resolution_states_json,
        effective_canonical_preview_lines_json,
        selected_canonical_preview_lines_json,
        baseline_component_rows_json,
        hidden_recovery_template_lines_json,
        candidate_totals_json,
        allocation_basis_json,
        scope_hash,
        chunk_sequence,
        status
    )
    select
        proposed_scope.operation_id,
        proposed_scope.workbench_session_id,
        proposed_scope.source_snapshot_run_id,
        proposed_scope.source_session_version,
        proposed_scope.candidate_state_id,
        proposed_scope.candidate_id,
        proposed_scope.pay_channel,
        proposed_scope.selected_preview_row_ids_json,
        proposed_scope.selected_timesheet_ids_json,
        proposed_scope.selected_finance_case_ids_json,
        proposed_scope.effective_candidate_fragment_json,
        proposed_scope.effective_summary_fragment_json,
        proposed_scope.effective_paye_candidate_json,
        proposed_scope.effective_non_paye_payee_json,
        proposed_scope.effective_payees_json,
        proposed_scope.effective_case_resolution_states_json,
        proposed_scope.effective_canonical_preview_lines_json,
        proposed_scope.selected_canonical_preview_lines_json,
        proposed_scope.baseline_component_rows_json,
        proposed_scope.hidden_recovery_template_lines_json,
        proposed_scope.candidate_totals_json,
        proposed_scope.allocation_basis_json,
        encode(extensions.digest(convert_to(jsonb_build_object(
            'operation_id', proposed_scope.operation_id::text,
            'workbench_session_id', proposed_scope.workbench_session_id::text,
            'source_snapshot_run_id', case when proposed_scope.source_snapshot_run_id is null then null else proposed_scope.source_snapshot_run_id::text end,
            'source_session_version', proposed_scope.source_session_version,
            'candidate_state_id', case when proposed_scope.candidate_state_id is null then null else proposed_scope.candidate_state_id::text end,
            'candidate_id', proposed_scope.candidate_id::text,
            'pay_channel', proposed_scope.pay_channel,
            'selected_preview_row_ids_json', proposed_scope.selected_preview_row_ids_json,
            'selected_timesheet_ids_json', proposed_scope.selected_timesheet_ids_json,
            'selected_finance_case_ids_json', proposed_scope.selected_finance_case_ids_json,
            'selected_canonical_preview_lines_json', proposed_scope.selected_canonical_preview_lines_json,
            'candidate_totals_json', proposed_scope.candidate_totals_json,
            'allocation_basis_json', proposed_scope.allocation_basis_json
        )::text, 'UTF8'), 'sha256'::text), 'hex'),
        row_number() over (order by proposed_scope.pay_channel, proposed_scope.candidate_id),
        'SCOPED'
    from pg_temp.tmp_pay_workbench_scope_seed_scopes as proposed_scope
    on conflict (operation_id, candidate_id, pay_channel) do update
    set
        selected_preview_row_ids_json = excluded.selected_preview_row_ids_json,
        selected_timesheet_ids_json = excluded.selected_timesheet_ids_json,
        selected_finance_case_ids_json = excluded.selected_finance_case_ids_json,
        effective_candidate_fragment_json = excluded.effective_candidate_fragment_json,
        effective_summary_fragment_json = excluded.effective_summary_fragment_json,
        effective_paye_candidate_json = excluded.effective_paye_candidate_json,
        effective_non_paye_payee_json = excluded.effective_non_paye_payee_json,
        effective_payees_json = excluded.effective_payees_json,
        effective_case_resolution_states_json = excluded.effective_case_resolution_states_json,
        effective_canonical_preview_lines_json = excluded.effective_canonical_preview_lines_json,
        selected_canonical_preview_lines_json = excluded.selected_canonical_preview_lines_json,
        baseline_component_rows_json = excluded.baseline_component_rows_json,
        hidden_recovery_template_lines_json = excluded.hidden_recovery_template_lines_json,
        candidate_totals_json = excluded.candidate_totals_json,
        allocation_basis_json = excluded.allocation_basis_json,
        scope_hash = excluded.scope_hash,
        chunk_sequence = excluded.chunk_sequence,
        status = case
            when public.banking_pay_operation_candidate_scope.status = 'PENDING' then 'SCOPED'
            else public.banking_pay_operation_candidate_scope.status
        end,
        updated_at_utc = now();

    select count(*)::integer
    into v_candidate_scope_count
    from public.banking_pay_operation_candidate_scope as scope_row
    where scope_row.operation_id = p_operation_id
      and exists (
          select 1
          from pg_temp.tmp_pay_workbench_scope_seed_scopes as proposed_scope
          where proposed_scope.candidate_id = scope_row.candidate_id
            and proposed_scope.pay_channel = scope_row.pay_channel
      );

    select count(distinct selected_line.preview_row_id)::integer
    into v_selected_count
    from pg_temp.tmp_pay_workbench_scope_seed_selected_lines as selected_line;

    select count(distinct selected_line.line_json->>'timesheet_id')::integer
    into v_timesheet_count
    from pg_temp.tmp_pay_workbench_scope_seed_selected_lines as selected_line
    where coalesce(selected_line.line_json->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    select count(distinct selected_line.line_json->>'finance_case_id')::integer
    into v_finance_case_count
    from pg_temp.tmp_pay_workbench_scope_seed_selected_lines as selected_line
    where coalesce(selected_line.line_json->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    select count(distinct selected_line.pay_channel)::integer
    into v_pay_channel_count
    from pg_temp.tmp_pay_workbench_scope_seed_selected_lines as selected_line;

    return query
    select
        coalesce(v_candidate_scope_count, 0),
        coalesce(v_selected_count, 0),
        coalesce(v_timesheet_count, 0),
        coalesce(v_finance_case_count, 0),
        coalesce(v_pay_channel_count, 0);
end;
$function$;


DROP FUNCTION IF EXISTS public.pay_bank_transfer_execution_classify(uuid, text, uuid, boolean);



DROP FUNCTION IF EXISTS public.pay_bank_transfer_execution_classify(uuid, text, uuid, boolean);



DROP FUNCTION IF EXISTS public.pay_bank_transfer_execution_classify(uuid, text, uuid, boolean);

DROP FUNCTION IF EXISTS public.pay_bank_transfer_execution_classify(uuid, text, uuid, boolean);







CREATE OR REPLACE FUNCTION public.pay_bank_transfer_execution_classify(
  p_pay_batch_id uuid,
  p_pay_channel_scope text DEFAULT 'ALL'::text,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_include_unscoped_transfers boolean DEFAULT true
)
RETURNS TABLE (
  pay_bank_transfer_id uuid,
  pay_batch_id uuid,
  pay_channel text,
  transfer_group_key text,
  scope_id uuid,
  scope_operation_id uuid,
  scope_status text,
  scope_request_id text,
  status_upper text,
  rail_state_upper text,
  amount numeric,
  currency text,
  has_route_ready boolean,
  has_local_prepare_identity boolean,
  has_provider_submission_evidence boolean,
  has_provider_event_evidence boolean,
  has_provider_attempt_without_external_id boolean,
  has_operation_submit_attempt boolean,
  has_ambiguous_external_evidence boolean,
  is_failed_or_blocked boolean,
  is_terminal_or_completed boolean,
  evidence_classification text,
  is_authorisation_ready boolean,
  is_unattempted_submit_eligible boolean,
  is_safe_local_cleanup boolean,
  is_canonical_pending_status boolean,
  has_auth_prepared_scope boolean,
  has_different_operation_scope boolean,
  has_stale_auth_request_evidence boolean,
  auth_request_id uuid,
  auth_request_state text,
  auth_request_operation_id uuid,
  has_same_operation_active_auth_request boolean,
  has_other_operation_active_auth_request boolean,
  has_cancellable_local_auth_request boolean,
  has_non_cancellable_auth_request boolean,
  has_authorised_auth_without_provider_submission boolean,
  has_auth_request_provider_risk boolean,
  auth_request_unsafe_reason text,
  unsafe_reason text,
  has_same_operation_authorised_auth_request boolean,
  has_pending_authorisation_auth_request boolean,
  is_provider_submit_ready boolean,
  provider_submit_unsafe_reason text,
  has_provider_submit_blocker boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_scope text := 'ALL';
BEGIN
  v_scope := upper(btrim(coalesce(p_pay_channel_scope, 'ALL')));
  IF v_scope = '' THEN
    v_scope := 'ALL';
  END IF;

  RETURN QUERY
  WITH batch_context AS (
    SELECT
      batch_row.id AS batch_id,
      batch_row.bulk_reference AS batch_bulk_reference,
      upper(btrim(coalesce(batch_row.execution_commit_state, 'NOT_SUBMITTED'))) AS execution_commit_state_upper,
      batch_row.execution_commit_ref AS execution_commit_ref,
      batch_row.execution_committed_at_utc AS execution_committed_at_utc,
      (
        upper(btrim(coalesce(batch_row.execution_commit_state, 'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
        OR nullif(btrim(coalesce(batch_row.execution_commit_ref, '')), '') IS NOT NULL
        OR batch_row.execution_committed_at_utc IS NOT NULL
      ) AS batch_execution_boundary_crossed
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = p_pay_batch_id
  ), transfer_source AS (
    SELECT
      transfer_row.id AS transfer_id,
      transfer_row.pay_batch_id AS transfer_pay_batch_id,
      transfer_row.pay_channel AS transfer_pay_channel,
      transfer_row.transfer_group_key AS transfer_group_key,
      transfer_row.status AS transfer_status,
      transfer_row.rail_state AS transfer_rail_state,
      transfer_row.amount AS transfer_amount,
      transfer_row.currency AS transfer_currency,
      transfer_row.payee_name AS transfer_payee_name,
      transfer_row.sort_code AS transfer_sort_code,
      transfer_row.account_number AS transfer_account_number,
      transfer_row.account_type AS transfer_account_type,
      transfer_row.payment_reference AS transfer_payment_reference,
      transfer_row.request_id AS transfer_request_id,
      transfer_row.rail_tx_id AS transfer_rail_tx_id,
      transfer_row.rail_meta_json AS transfer_rail_meta_json,
      transfer_row.completed_at_utc AS transfer_completed_at_utc,
      transfer_row.failed_reason AS transfer_failed_reason,
      transfer_row.bank_details_hash_snapshot AS transfer_bank_details_hash_snapshot,
      transfer_row.payee_entity_kind AS transfer_payee_entity_kind,
      transfer_row.payee_entity_id AS transfer_payee_entity_id,
      transfer_row.grouping_mode_used AS transfer_grouping_mode_used,
      transfer_row.week_ending_bucket AS transfer_week_ending_bucket,
      scope_pick.scope_id,
      scope_pick.scope_operation_id,
      scope_pick.scope_status,
      scope_pick.scope_request_id,
      scope_pick.scope_payment_reference,
      scope_pick.scope_currency,
      scope_pick.scope_amount,
      scope_pick.scope_payee_name,
      scope_pick.scope_sort_code,
      scope_pick.scope_account_number,
      scope_pick.scope_account_type,
      scope_pick.scope_bank_details_hash_snapshot
    FROM public.pay_bank_transfers AS transfer_row
    JOIN batch_context AS batch_context_row
      ON batch_context_row.batch_id = transfer_row.pay_batch_id
    LEFT JOIN LATERAL (
      SELECT
        scope_row.id AS scope_id,
        scope_row.operation_id AS scope_operation_id,
        scope_row.status AS scope_status,
        scope_row.request_id AS scope_request_id,
        scope_row.payment_reference AS scope_payment_reference,
        scope_row.currency AS scope_currency,
        scope_row.amount AS scope_amount,
        scope_row.payee_name AS scope_payee_name,
        scope_row.sort_code AS scope_sort_code,
        scope_row.account_number AS scope_account_number,
        scope_row.account_type AS scope_account_type,
        scope_row.bank_details_hash_snapshot AS scope_bank_details_hash_snapshot
      FROM public.banking_pay_operation_transfer_scope AS scope_row
      WHERE scope_row.pay_batch_id = transfer_row.pay_batch_id
        AND scope_row.pay_channel = transfer_row.pay_channel
        AND (
          scope_row.pay_bank_transfer_id = transfer_row.id
          OR (
            scope_row.pay_bank_transfer_id IS NULL
            AND nullif(btrim(coalesce(scope_row.transfer_group_key, '')), '') IS NOT NULL
            AND scope_row.transfer_group_key = transfer_row.transfer_group_key
          )
        )
      ORDER BY
        CASE WHEN p_operation_id IS NOT NULL AND scope_row.operation_id = p_operation_id THEN 0 ELSE 1 END,
        CASE WHEN scope_row.pay_bank_transfer_id = transfer_row.id THEN 0 ELSE 1 END,
        scope_row.updated_at_utc DESC NULLS LAST,
        scope_row.created_at_utc DESC NULLS LAST,
        scope_row.id DESC
      LIMIT 1
    ) AS scope_pick ON true
    WHERE transfer_row.pay_batch_id = p_pay_batch_id
      AND (v_scope IN ('ALL', 'ANY', '*') OR upper(btrim(coalesce(transfer_row.pay_channel, ''))) = v_scope)
      AND (
        coalesce(p_include_unscoped_transfers, true) IS TRUE
        OR p_operation_id IS NULL
        OR scope_pick.scope_operation_id = p_operation_id
        OR btrim(coalesce(transfer_row.rail_meta_json #>> '{operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(transfer_row.rail_meta_json #>> '{created_by_operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(transfer_row.rail_meta_json #>> '{payment_execute_operation_id}', '')) = p_operation_id::text
      )
  ), scope_without_transfer_source AS (
    SELECT
      NULL::uuid AS transfer_id,
      scope_row.pay_batch_id AS transfer_pay_batch_id,
      scope_row.pay_channel AS transfer_pay_channel,
      scope_row.transfer_group_key AS transfer_group_key,
      NULL::text AS transfer_status,
      NULL::text AS transfer_rail_state,
      scope_row.amount AS transfer_amount,
      scope_row.currency AS transfer_currency,
      scope_row.payee_name AS transfer_payee_name,
      scope_row.sort_code AS transfer_sort_code,
      scope_row.account_number AS transfer_account_number,
      scope_row.account_type AS transfer_account_type,
      scope_row.payment_reference AS transfer_payment_reference,
      scope_row.request_id AS transfer_request_id,
      NULL::text AS transfer_rail_tx_id,
      '{}'::jsonb AS transfer_rail_meta_json,
      NULL::timestamptz AS transfer_completed_at_utc,
      NULL::text AS transfer_failed_reason,
      scope_row.bank_details_hash_snapshot AS transfer_bank_details_hash_snapshot,
      scope_row.payee_entity_kind AS transfer_payee_entity_kind,
      scope_row.payee_entity_id AS transfer_payee_entity_id,
      scope_row.grouping_mode_used AS transfer_grouping_mode_used,
      scope_row.week_ending_bucket AS transfer_week_ending_bucket,
      scope_row.id AS scope_id,
      scope_row.operation_id AS scope_operation_id,
      scope_row.status AS scope_status,
      scope_row.request_id AS scope_request_id,
      scope_row.payment_reference AS scope_payment_reference,
      scope_row.currency AS scope_currency,
      scope_row.amount AS scope_amount,
      scope_row.payee_name AS scope_payee_name,
      scope_row.sort_code AS scope_sort_code,
      scope_row.account_number AS scope_account_number,
      scope_row.account_type AS scope_account_type,
      scope_row.bank_details_hash_snapshot AS scope_bank_details_hash_snapshot
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    JOIN batch_context AS batch_context_row
      ON batch_context_row.batch_id = scope_row.pay_batch_id
    WHERE scope_row.pay_batch_id = p_pay_batch_id
      AND (v_scope IN ('ALL', 'ANY', '*') OR upper(btrim(coalesce(scope_row.pay_channel, ''))) = v_scope)
      AND (p_operation_id IS NULL OR scope_row.operation_id = p_operation_id)
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfers AS transfer_row
        WHERE transfer_row.pay_batch_id = scope_row.pay_batch_id
          AND transfer_row.pay_channel = scope_row.pay_channel
          AND (
            transfer_row.id = scope_row.pay_bank_transfer_id
            OR (
              scope_row.pay_bank_transfer_id IS NULL
              AND nullif(btrim(coalesce(scope_row.transfer_group_key, '')), '') IS NOT NULL
              AND transfer_row.transfer_group_key = scope_row.transfer_group_key
            )
          )
      )
  ), combined_source AS (
    SELECT transfer_source.*
    FROM transfer_source
    UNION ALL
    SELECT scope_without_transfer_source.*
    FROM scope_without_transfer_source
  ), normalised_rows AS (
    SELECT
      combined_source.transfer_id,
      combined_source.transfer_pay_batch_id,
      combined_source.transfer_pay_channel,
      combined_source.transfer_group_key,
      combined_source.scope_id,
      combined_source.scope_operation_id,
      combined_source.scope_status,
      combined_source.scope_request_id,
      upper(btrim(coalesce(combined_source.transfer_status, ''))) AS status_upper,
      upper(btrim(coalesce(combined_source.transfer_rail_state, ''))) AS rail_state_upper,
      combined_source.transfer_amount,
      combined_source.transfer_currency,
      combined_source.transfer_payee_name,
      combined_source.transfer_sort_code,
      combined_source.transfer_account_number,
      combined_source.transfer_account_type,
      combined_source.transfer_payment_reference,
      combined_source.transfer_request_id,
      combined_source.transfer_rail_tx_id,
      coalesce(combined_source.transfer_rail_meta_json, '{}'::jsonb) AS transfer_rail_meta_json,
      upper(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) AS provider_submit_status_upper,
      lower(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_request_sent}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS provider_submit_request_sent,
      lower(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_response_received}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS provider_submit_response_received,
      lower(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_response_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS provider_submit_response_present,
      lower(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_acceptance_evidence_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS provider_submit_acceptance_evidence_present,
      upper(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) = 'MANUAL_RESOLVED_NO_PAYMENT_MADE' AS provider_submit_manual_resolved_no_payment,
      combined_source.transfer_completed_at_utc,
      combined_source.transfer_failed_reason,
      combined_source.transfer_bank_details_hash_snapshot,
      combined_source.transfer_payee_entity_kind,
      combined_source.transfer_payee_entity_id,
      batch_context_row.batch_bulk_reference,
      batch_context_row.batch_execution_boundary_crossed,
      ARRAY_REMOVE(ARRAY[
        combined_source.transfer_id::text,
        NULLIF(btrim(coalesce(combined_source.transfer_request_id, '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_payment_reference, '')), ''),
        NULLIF(btrim(coalesce(combined_source.scope_request_id, '')), ''),
        NULLIF(btrim(coalesce(combined_source.scope_payment_reference, '')), ''),
        NULLIF(btrim(coalesce(batch_context_row.batch_bulk_reference, '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_group_key, '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{request_id}', '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{idempotency_key}', '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{payment_reference}', '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{bulk_reference}', '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{operation_id}', '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{created_by_operation_id}', '')), ''),
        NULLIF(btrim(coalesce(combined_source.transfer_rail_meta_json #>> '{payment_execute_operation_id}', '')), '')
      ]::text[], NULL::text) AS local_identity_values
    FROM combined_source
    JOIN batch_context AS batch_context_row
      ON batch_context_row.batch_id = combined_source.transfer_pay_batch_id
  ), classified_rows AS (
    SELECT
      normalised_rows.*,
      (
        coalesce(normalised_rows.transfer_amount, 0) > 0
        AND nullif(btrim(coalesce(normalised_rows.transfer_currency, '')), '') IS NOT NULL
        AND nullif(btrim(coalesce(normalised_rows.transfer_payee_name, '')), '') IS NOT NULL
        AND nullif(btrim(coalesce(normalised_rows.transfer_sort_code, '')), '') IS NOT NULL
        AND nullif(btrim(coalesce(normalised_rows.transfer_account_number, '')), '') IS NOT NULL
      ) AS calc_has_route_ready,
      (
        nullif(btrim(coalesce(normalised_rows.transfer_request_id, '')), '') IS NOT NULL
        OR nullif(btrim(coalesce(normalised_rows.transfer_payment_reference, '')), '') IS NOT NULL
        OR nullif(btrim(coalesce(normalised_rows.scope_request_id, '')), '') IS NOT NULL
        OR nullif(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{request_id}', '')), '') IS NOT NULL
        OR nullif(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{idempotency_key}', '')), '') IS NOT NULL
        OR nullif(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{payment_reference}', '')), '') IS NOT NULL
        OR nullif(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{operation_id}', '')), '') IS NOT NULL
        OR nullif(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{created_by_operation_id}', '')), '') IS NOT NULL
        OR nullif(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{payment_execute_operation_id}', '')), '') IS NOT NULL
      ) AS calc_has_local_prepare_identity,
      EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS event_row
        WHERE event_row.pay_batch_id = normalised_rows.transfer_pay_batch_id
          AND normalised_rows.transfer_id IS NOT NULL
          AND (
            event_row.pay_bank_transfer_id = normalised_rows.transfer_id
            OR (
              event_row.pay_bank_transfer_id IS NULL
              AND (
                NULLIF(BTRIM(COALESCE(event_row.provider_reference, '')), '') = ANY(normalised_rows.local_identity_values)
                OR NULLIF(BTRIM(COALESCE(event_row.provider_event_id, '')), '') = ANY(normalised_rows.local_identity_values)
                OR NULLIF(BTRIM(COALESCE(event_row.idempotency_key, '')), '') = ANY(normalised_rows.local_identity_values)
                OR EXISTS (
                  SELECT 1
                  FROM unnest(normalised_rows.local_identity_values) AS local_event_identity(identity_value)
                  WHERE LENGTH(NULLIF(BTRIM(COALESCE(local_event_identity.identity_value, '')), '')) >= 8
                    AND POSITION(lower(NULLIF(BTRIM(COALESCE(local_event_identity.identity_value, '')), '')) IN lower(COALESCE(event_row.raw_payload::text, ''))) > 0
                )
              )
            )
          )
          AND upper(btrim(coalesce(event_row.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL', 'RAIL_PROVIDER', 'PROVIDER', 'PROVIDER_SETTLEMENT')
          AND upper(btrim(coalesce(event_row.normalised_state, event_row.provider_state, event_row.raw_payload #>> '{provider_submit_diagnostic,provider_state}', ''))) NOT IN ('REJECTED', 'FAILED', 'ERROR', 'DECLINED', 'CANCELLED', 'CANCELED', 'MALFORMED', 'UNKNOWN', 'TIMEOUT', 'TIMED_OUT')
          AND upper(btrim(coalesce(event_row.raw_payload #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN (
            'PROVIDER_SUBMISSION_REJECTED',
            'PROVIDER_SUBMISSION_FAILED',
            'PROVIDER_SUBMISSION_MALFORMED_RESPONSE',
            'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME',
            'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK',
            'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL',
            'NO_PROVIDER_SUBMISSION_ATTEMPTED',
            'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID',
            'MANUAL_RESOLVED_NO_PAYMENT_MADE'
          )
          AND (
            EXISTS (
              SELECT 1
              FROM (VALUES
                (event_row.provider_event_id),
                (event_row.provider_reference),
                (event_row.raw_payload #>> '{provider_event_id}'),
                (event_row.raw_payload #>> '{provider_reference}'),
                (event_row.raw_payload #>> '{provider_submission_id}'),
                (event_row.raw_payload #>> '{submission_id}'),
                (event_row.raw_payload #>> '{rail_submission_id}'),
                (event_row.raw_payload #>> '{provider_payment_id}'),
                (event_row.raw_payload #>> '{payment_id}'),
                (event_row.raw_payload #>> '{external_payment_id}'),
                (event_row.raw_payload #>> '{revolut_payment_id}'),
                (event_row.raw_payload #>> '{provider_transfer_id}'),
                (event_row.raw_payload #>> '{external_transfer_id}'),
                (event_row.raw_payload #>> '{provider_transaction_id}'),
                (event_row.raw_payload #>> '{transaction_id}'),
                (event_row.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}'),
                (event_row.raw_payload #>> '{provider_submit_diagnostic,provider_payment_id}'),
                (event_row.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}'),
                (event_row.raw_payload #>> '{provider_submit_diagnostic,provider_reference}')
              ) AS provider_identifier(identifier_value)
              WHERE nullif(btrim(coalesce(provider_identifier.identifier_value, '')), '') IS NOT NULL
                AND NOT (nullif(btrim(coalesce(provider_identifier.identifier_value, '')), '') = ANY(normalised_rows.local_identity_values))
            )
          )
      ) AS calc_has_provider_event_evidence,
      EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS event_row
        WHERE event_row.pay_batch_id = normalised_rows.transfer_pay_batch_id
          AND normalised_rows.transfer_id IS NOT NULL
          AND (
            event_row.pay_bank_transfer_id = normalised_rows.transfer_id
            OR (
              event_row.pay_bank_transfer_id IS NULL
              AND (
                NULLIF(BTRIM(COALESCE(event_row.provider_reference, '')), '') = ANY(normalised_rows.local_identity_values)
                OR NULLIF(BTRIM(COALESCE(event_row.provider_event_id, '')), '') = ANY(normalised_rows.local_identity_values)
                OR NULLIF(BTRIM(COALESCE(event_row.idempotency_key, '')), '') = ANY(normalised_rows.local_identity_values)
                OR EXISTS (
                  SELECT 1
                  FROM unnest(normalised_rows.local_identity_values) AS local_event_identity(identity_value)
                  WHERE LENGTH(NULLIF(BTRIM(COALESCE(local_event_identity.identity_value, '')), '')) >= 8
                    AND POSITION(lower(NULLIF(BTRIM(COALESCE(local_event_identity.identity_value, '')), '')) IN lower(COALESCE(event_row.raw_payload::text, ''))) > 0
                )
              )
            )
          )
          AND (
            upper(btrim(coalesce(event_row.mapping_status, ''))) IN ('AMBIGUOUS', 'UNMATCHED', 'NO_MATCH', 'MULTIPLE_MATCHES')
            OR upper(btrim(coalesce(event_row.normalised_state, ''))) IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'PENDING_REVIEW')
            OR upper(btrim(coalesce(event_row.provider_state, ''))) IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'PENDING_REVIEW')
          )
      ) AS calc_has_ambiguous_event,
      EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_chunks AS chunk_row
        JOIN public.banking_pay_operations AS chunk_operation_row
          ON chunk_operation_row.id = chunk_row.operation_id
        WHERE chunk_operation_row.pay_batch_id = p_pay_batch_id
          AND chunk_row.phase = 'SUBMIT_PROVIDER_TRANSFERS'
          AND chunk_row.chunk_type = 'TRANSFER_SUBMIT'
          AND (
            upper(btrim(coalesce(chunk_row.status, ''))) IN ('RUNNING', 'COMPLETE', 'FAILED')
            OR chunk_row.started_at_utc IS NOT NULL
            OR (chunk_row.result_json IS NOT NULL AND chunk_row.result_json <> '{}'::jsonb)
            OR (chunk_row.error_json IS NOT NULL AND chunk_row.error_json <> '{}'::jsonb)
          )
          AND normalised_rows.transfer_id IS NOT NULL
          AND position(
            lower(normalised_rows.transfer_id::text) IN lower(
              coalesce(chunk_row.payload_json::text, '')
              || coalesce(chunk_row.result_json::text, '')
              || coalesce(chunk_row.error_json::text, '')
            )
          ) > 0
      ) AS calc_has_operation_submit_attempt,
      EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_chunks AS stale_chunk_row
        JOIN public.banking_pay_operations AS stale_chunk_operation_row
          ON stale_chunk_operation_row.id = stale_chunk_row.operation_id
        WHERE stale_chunk_operation_row.pay_batch_id = p_pay_batch_id
          AND stale_chunk_row.phase = 'SUBMIT_PROVIDER_TRANSFERS'
          AND stale_chunk_row.chunk_type = 'TRANSFER_SUBMIT'
          AND upper(btrim(coalesce(stale_chunk_row.status, ''))) = 'RUNNING'
          AND (stale_chunk_row.lock_expires_at_utc IS NULL OR stale_chunk_row.lock_expires_at_utc <= now())
          AND normalised_rows.transfer_id IS NOT NULL
          AND normalised_rows.provider_submit_manual_resolved_no_payment IS NOT TRUE
          AND position(lower(normalised_rows.transfer_id::text) IN lower(coalesce(stale_chunk_row.payload_json::text, '') || coalesce(stale_chunk_row.result_json::text, '') || coalesce(stale_chunk_row.error_json::text, ''))) > 0
          AND (
            stale_chunk_row.result_json IS NULL
            OR stale_chunk_row.result_json = '{}'::jsonb
            OR jsonb_typeof(stale_chunk_row.result_json->'provider_submit_diagnostic') IS DISTINCT FROM 'object'
            OR upper(btrim(coalesce(stale_chunk_row.result_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) IN ('', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'CLAIMED_NOT_PROVIDER_CALLED_YET', 'PROVIDER_SUBMIT_CHUNK_CLAIMED', 'PROVIDER_PRECALL_VALIDATION_STARTED', 'PROVIDER_PAYMENT_CREATE_STARTED', 'PROVIDER_PAYMENT_CREATE_REQUEST_SENDING', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL')
          )
          AND (stale_chunk_row.error_json IS NULL OR stale_chunk_row.error_json = '{}'::jsonb OR jsonb_typeof(stale_chunk_row.error_json->'provider_submit_diagnostic') IS DISTINCT FROM 'object')
          AND lower(btrim(coalesce(stale_chunk_row.result_json #>> '{provider_submit_diagnostic,provider_request_impossible}', stale_chunk_row.result_json #>> '{provider_submit_diagnostic,durable_provider_request_impossible}', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
      ) AS calc_has_stale_provider_submit_chunk,
      (
        normalised_rows.provider_submit_manual_resolved_no_payment IS NOT TRUE
        AND normalised_rows.provider_submit_status_upper NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID', 'MANUAL_RESOLVED_NO_PAYMENT_MADE')
        AND EXISTS (
        SELECT 1
        FROM (VALUES
          (normalised_rows.transfer_rail_meta_json #>> '{rail_tx_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_event_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_reference}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_submission_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{submission_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{rail_submission_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_payment_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{payment_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{external_payment_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{revolut_payment_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_transfer_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{transfer_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{external_transfer_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_transaction_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{transaction_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}'),
          (normalised_rows.transfer_rail_meta_json #>> '{external_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{provider_id}'),
          (normalised_rows.transfer_rail_meta_json #>> '{bank_transfer_id}')
        ) AS rail_identifier(identifier_value)
        WHERE nullif(btrim(coalesce(rail_identifier.identifier_value, '')), '') IS NOT NULL
          AND NOT (nullif(btrim(coalesce(rail_identifier.identifier_value, '')), '') = ANY(normalised_rows.local_identity_values))
      )
      ) AS calc_has_non_local_rail_meta_identifier,
      (
        normalised_rows.provider_submit_manual_resolved_no_payment IS NOT TRUE
        AND normalised_rows.provider_submit_status_upper NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID', 'MANUAL_RESOLVED_NO_PAYMENT_MADE')
        AND nullif(btrim(coalesce(normalised_rows.transfer_rail_tx_id, '')), '') IS NOT NULL
        AND NOT (nullif(btrim(coalesce(normalised_rows.transfer_rail_tx_id, '')), '') = ANY(normalised_rows.local_identity_values))
      ) AS calc_has_non_local_rail_tx_id,
      (
        normalised_rows.provider_submit_manual_resolved_no_payment IS NOT TRUE
        AND NOT (
          (
            normalised_rows.provider_submit_status_upper NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID', 'MANUAL_RESOLVED_NO_PAYMENT_MADE')
            AND nullif(btrim(coalesce(normalised_rows.transfer_rail_tx_id, '')), '') IS NOT NULL
            AND NOT (nullif(btrim(coalesce(normalised_rows.transfer_rail_tx_id, '')), '') = ANY(normalised_rows.local_identity_values))
          )
          OR (
            normalised_rows.provider_submit_status_upper NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID', 'MANUAL_RESOLVED_NO_PAYMENT_MADE')
            AND EXISTS (
              SELECT 1
              FROM (VALUES
                (normalised_rows.transfer_rail_meta_json #>> '{rail_tx_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_event_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_reference}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_submission_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{submission_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{rail_submission_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_payment_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{payment_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{external_payment_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{revolut_payment_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_transfer_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{external_transfer_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_transaction_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{transaction_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}'),
                (normalised_rows.transfer_rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}')
              ) AS strict_meta_identifier(identifier_value)
              WHERE nullif(btrim(coalesce(strict_meta_identifier.identifier_value, '')), '') IS NOT NULL
                AND NOT (nullif(btrim(coalesce(strict_meta_identifier.identifier_value, '')), '') = ANY(normalised_rows.local_identity_values))
            )
          )
          OR EXISTS (
            SELECT 1
            FROM public.pay_bank_transfer_events AS strict_provider_event
            WHERE strict_provider_event.pay_batch_id = normalised_rows.transfer_pay_batch_id
              AND normalised_rows.transfer_id IS NOT NULL
              AND strict_provider_event.pay_bank_transfer_id = normalised_rows.transfer_id
              AND upper(btrim(coalesce(strict_provider_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL', 'RAIL_PROVIDER', 'PROVIDER', 'PROVIDER_SETTLEMENT')
              AND upper(btrim(coalesce(strict_provider_event.normalised_state, strict_provider_event.provider_state, strict_provider_event.raw_payload #>> '{provider_submit_diagnostic,provider_state}', ''))) NOT IN ('REJECTED', 'FAILED', 'ERROR', 'DECLINED', 'CANCELLED', 'CANCELED', 'MALFORMED', 'UNKNOWN', 'TIMEOUT', 'TIMED_OUT')
              AND upper(btrim(coalesce(strict_provider_event.raw_payload #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID', 'MANUAL_RESOLVED_NO_PAYMENT_MADE')
              AND EXISTS (
                SELECT 1
                FROM (VALUES
                  (strict_provider_event.provider_event_id),
                  (strict_provider_event.provider_reference),
                  (strict_provider_event.raw_payload #>> '{provider_event_id}'),
                  (strict_provider_event.raw_payload #>> '{provider_reference}'),
                  (strict_provider_event.raw_payload #>> '{provider_payment_id}'),
                  (strict_provider_event.raw_payload #>> '{payment_id}'),
                  (strict_provider_event.raw_payload #>> '{external_payment_id}'),
                  (strict_provider_event.raw_payload #>> '{revolut_payment_id}'),
                  (strict_provider_event.raw_payload #>> '{provider_transaction_id}'),
                  (strict_provider_event.raw_payload #>> '{transaction_id}'),
                  (strict_provider_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}'),
                  (strict_provider_event.raw_payload #>> '{provider_submit_diagnostic,provider_payment_id}'),
                  (strict_provider_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}'),
                  (strict_provider_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}')
                ) AS strict_provider_event_identifier(identifier_value)
                WHERE nullif(btrim(coalesce(strict_provider_event_identifier.identifier_value, '')), '') IS NOT NULL
                  AND NOT (nullif(btrim(coalesce(strict_provider_event_identifier.identifier_value, '')), '') = ANY(normalised_rows.local_identity_values))
              )
          )
        )
        AND (
          lower(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{provider_attempt_without_external_id}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR normalised_rows.provider_submit_request_sent IS TRUE
          OR normalised_rows.provider_submit_response_received IS TRUE
          OR normalised_rows.provider_submit_response_present IS TRUE
          OR lower(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{last_update_provider_source}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR normalised_rows.provider_submit_status_upper IN ('PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME')
          OR EXISTS (
            SELECT 1
            FROM public.pay_bank_transfer_events AS provider_attempt_event
            WHERE provider_attempt_event.pay_batch_id = normalised_rows.transfer_pay_batch_id
              AND normalised_rows.transfer_id IS NOT NULL
              AND (
                provider_attempt_event.pay_bank_transfer_id = normalised_rows.transfer_id
                OR (
                  provider_attempt_event.pay_bank_transfer_id IS NULL
                  AND (
                    NULLIF(BTRIM(COALESCE(provider_attempt_event.provider_reference, '')), '') = ANY(normalised_rows.local_identity_values)
                    OR NULLIF(BTRIM(COALESCE(provider_attempt_event.provider_event_id, '')), '') = ANY(normalised_rows.local_identity_values)
                    OR NULLIF(BTRIM(COALESCE(provider_attempt_event.idempotency_key, '')), '') = ANY(normalised_rows.local_identity_values)
                  )
                )
              )
              AND upper(btrim(coalesce(provider_attempt_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL', 'RAIL_PROVIDER', 'PROVIDER', 'PROVIDER_SETTLEMENT')
              AND NOT EXISTS (
                SELECT 1
                FROM (VALUES
                  (provider_attempt_event.provider_event_id),
                  (provider_attempt_event.provider_reference),
                  (provider_attempt_event.raw_payload #>> '{provider_event_id}'),
                  (provider_attempt_event.raw_payload #>> '{provider_reference}'),
                  (provider_attempt_event.raw_payload #>> '{provider_submission_id}'),
                  (provider_attempt_event.raw_payload #>> '{submission_id}'),
                  (provider_attempt_event.raw_payload #>> '{rail_submission_id}'),
                  (provider_attempt_event.raw_payload #>> '{provider_payment_id}'),
                  (provider_attempt_event.raw_payload #>> '{payment_id}'),
                  (provider_attempt_event.raw_payload #>> '{external_payment_id}'),
                  (provider_attempt_event.raw_payload #>> '{revolut_payment_id}'),
                  (provider_attempt_event.raw_payload #>> '{provider_transfer_id}'),
                  (provider_attempt_event.raw_payload #>> '{external_transfer_id}'),
                  (provider_attempt_event.raw_payload #>> '{provider_transaction_id}'),
                  (provider_attempt_event.raw_payload #>> '{transaction_id}'),
                  (provider_attempt_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}'),
                  (provider_attempt_event.raw_payload #>> '{provider_submit_diagnostic,provider_payment_id}'),
                  (provider_attempt_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}'),
                  (provider_attempt_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}')
                ) AS provider_attempt_identifier(identifier_value)
                WHERE nullif(btrim(coalesce(provider_attempt_identifier.identifier_value, '')), '') IS NOT NULL
                  AND NOT (nullif(btrim(coalesce(provider_attempt_identifier.identifier_value, '')), '') = ANY(normalised_rows.local_identity_values))
              )
          )
        )
      ) AS calc_has_provider_attempt_without_external_id,
      (
        normalised_rows.provider_submit_manual_resolved_no_payment IS NOT TRUE
        AND (
          lower(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR lower(btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{last_update_provider_source}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        )
      ) AS calc_has_provider_source_flag,
      (
        normalised_rows.status_upper IN ('FAILED', 'REJECTED', 'DECLINED', 'BLOCKED', 'ERROR')
        OR normalised_rows.rail_state_upper IN ('FAILED', 'REJECTED', 'DECLINED', 'BLOCKED', 'ERROR')
        OR nullif(btrim(coalesce(normalised_rows.transfer_failed_reason, '')), '') IS NOT NULL
      ) AS calc_is_failed_or_blocked,
      (
        normalised_rows.status_upper IN ('COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED')
        OR normalised_rows.rail_state_upper IN ('COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED')
        OR normalised_rows.transfer_completed_at_utc IS NOT NULL
      ) AS calc_is_terminal_or_completed,
      (
        normalised_rows.status_upper IN ('SUBMITTED', 'QUEUED', 'ACCEPTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'PENDING_SETTLEMENT', 'PENDING_CONFIRMATION', 'PENDING_SUBMISSION', 'COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'RETURNED', 'REVERTED')
        OR normalised_rows.rail_state_upper IN ('SUBMITTED', 'QUEUED', 'ACCEPTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'PENDING_SETTLEMENT', 'PENDING_CONFIRMATION', 'PENDING_SUBMISSION', 'COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'RETURNED', 'REVERTED')
      ) AS calc_has_provider_like_state,
      (
        normalised_rows.status_upper IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'PENDING_REVIEW')
        OR normalised_rows.rail_state_upper IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'PENDING_REVIEW')
      ) AS calc_has_unknown_or_review_state,
      (
        p_operation_id IS NOT NULL
        AND (
          normalised_rows.scope_operation_id = p_operation_id
          OR btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{operation_id}', '')) = p_operation_id::text
          OR btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{created_by_operation_id}', '')) = p_operation_id::text
          OR btrim(coalesce(normalised_rows.transfer_rail_meta_json #>> '{payment_execute_operation_id}', '')) = p_operation_id::text
        )
      ) AS calc_is_operation_owned,
      EXISTS (
        SELECT 1
        FROM public.banking_pay_operations AS operation_row
        WHERE operation_row.id = normalised_rows.scope_operation_id
          AND operation_row.status IN ('FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED')
      ) AS calc_scope_owner_is_terminal_cleanup_candidate,
      false AS calc_has_stale_auth_request_evidence
    FROM normalised_rows
  ), labelled_rows AS (
    SELECT
      classified_rows.*,
      (
        classified_rows.provider_submit_manual_resolved_no_payment IS NOT TRUE
        AND (
          classified_rows.calc_has_provider_event_evidence
          OR classified_rows.calc_has_non_local_rail_tx_id
          OR classified_rows.calc_has_non_local_rail_meta_identifier
          OR (
            classified_rows.provider_submit_acceptance_evidence_present IS TRUE
            AND (
              classified_rows.calc_has_provider_event_evidence
              OR classified_rows.calc_has_non_local_rail_tx_id
              OR classified_rows.calc_has_non_local_rail_meta_identifier
            )
          )
        )
      ) AS calc_has_provider_submission_evidence,
      (
        classified_rows.provider_submit_manual_resolved_no_payment IS NOT TRUE
        AND (
          classified_rows.calc_has_ambiguous_event
          OR classified_rows.calc_has_unknown_or_review_state
          OR classified_rows.calc_has_stale_provider_submit_chunk
          OR classified_rows.provider_submit_status_upper IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
        )
      ) AS calc_has_ambiguous_external_evidence
    FROM classified_rows
  ), final_rows AS (
    SELECT
      labelled_rows.*,
      CASE
        WHEN labelled_rows.transfer_id IS NULL THEN 'scope_without_transfer'
        WHEN labelled_rows.calc_is_failed_or_blocked THEN 'failed'
        WHEN labelled_rows.calc_is_terminal_or_completed THEN 'terminal'
        WHEN labelled_rows.calc_has_ambiguous_external_evidence THEN 'ambiguous'
        WHEN labelled_rows.calc_has_provider_submission_evidence THEN 'provider_evidence_present'
        WHEN labelled_rows.calc_has_local_prepare_identity THEN 'local_only_evidence'
        WHEN labelled_rows.status_upper = 'PENDING' THEN 'pending'
        ELSE 'unclassified_local'
      END AS calc_evidence_classification
    FROM labelled_rows
  ), auth_request_rows AS (
    SELECT
      auth_request.id AS auth_request_id,
      upper(btrim(coalesce(auth_request.state, ''))) AS auth_request_state_upper,
      auth_intent.auth_operation_id_text,
      CASE
        WHEN auth_intent.auth_operation_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN auth_intent.auth_operation_id_text::uuid
        ELSE NULL::uuid
      END AS auth_request_operation_id,
      auth_intent.auth_pay_channel_scope_upper,
      upper(btrim(coalesce(auth_owner_operation.status, ''))) AS auth_owner_operation_status_upper,
      auth_request.created_at_utc AS auth_request_created_at_utc,
      (
        p_operation_id IS NOT NULL
        AND auth_intent.auth_operation_id_text = p_operation_id::text
      ) AS is_same_operation_auth_request,
      (
        p_operation_id IS NULL
        OR auth_intent.auth_operation_id_text IS NULL
        OR auth_intent.auth_operation_id_text <> p_operation_id::text
      ) AS is_other_operation_auth_request,
      (
        upper(btrim(coalesce(auth_owner_operation.status, ''))) IN ('FAILED', 'CANCELLED', 'CANCELED')
      ) AS owner_operation_is_terminal_cleanup_candidate
    FROM public.pay_batch_auth_requests AS auth_request
    LEFT JOIN LATERAL (
      SELECT
        nullif(btrim(coalesce(auth_request.execution_intent_json->>'operation_id', '')), '') AS auth_operation_id_text,
        upper(btrim(coalesce(
          nullif(btrim(coalesce(auth_request.execution_intent_json->>'pay_channel_scope', '')), ''),
          nullif(btrim(coalesce(auth_request.execution_intent_json->>'payChannelScope', '')), ''),
          'ALL'
        ))) AS auth_pay_channel_scope_upper
    ) AS auth_intent ON true
    LEFT JOIN public.banking_pay_operations AS auth_owner_operation
      ON auth_owner_operation.id = CASE
        WHEN auth_intent.auth_operation_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN auth_intent.auth_operation_id_text::uuid
        ELSE NULL::uuid
      END
    WHERE auth_request.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(auth_request.state, ''))) IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED')
      AND (
        v_scope IN ('ALL', 'ANY', '*')
        OR auth_intent.auth_pay_channel_scope_upper IN ('', 'ALL', 'ANY', '*')
        OR auth_intent.auth_pay_channel_scope_upper = v_scope
      )
  ), auth_request_ranked AS (
    SELECT
      auth_request_rows.*,
      row_number() OVER (
        ORDER BY
          CASE WHEN auth_request_rows.is_same_operation_auth_request THEN 0 ELSE 1 END,
          CASE
            WHEN auth_request_rows.auth_request_state_upper = 'AWAITING' THEN 0
            WHEN auth_request_rows.auth_request_state_upper = 'AUTHORISED' THEN 1
            WHEN auth_request_rows.auth_request_state_upper = 'PENDING_AUTHORISATION' THEN 2
            ELSE 3
          END,
          auth_request_rows.auth_request_created_at_utc DESC NULLS LAST,
          auth_request_rows.auth_request_id DESC
      ) AS auth_request_rank
    FROM auth_request_rows
  ), auth_context AS (
    SELECT
      (SELECT auth_request_ranked.auth_request_id FROM auth_request_ranked WHERE auth_request_ranked.auth_request_rank = 1) AS auth_request_id,
      (SELECT auth_request_ranked.auth_request_state_upper FROM auth_request_ranked WHERE auth_request_ranked.auth_request_rank = 1) AS auth_request_state,
      (SELECT auth_request_ranked.auth_request_operation_id FROM auth_request_ranked WHERE auth_request_ranked.auth_request_rank = 1) AS auth_request_operation_id,
      EXISTS (SELECT 1 FROM auth_request_rows) AS has_active_auth_request,
      EXISTS (SELECT 1 FROM auth_request_rows WHERE auth_request_rows.is_same_operation_auth_request) AS has_same_operation_active_auth_request,
      EXISTS (SELECT 1 FROM auth_request_rows WHERE auth_request_rows.is_other_operation_auth_request) AS has_other_operation_active_auth_request,
      EXISTS (SELECT 1 FROM auth_request_rows WHERE auth_request_rows.is_same_operation_auth_request AND auth_request_rows.auth_request_state_upper = 'AWAITING') AS has_same_operation_awaiting_auth_request,
      EXISTS (SELECT 1 FROM auth_request_rows WHERE auth_request_rows.is_same_operation_auth_request AND auth_request_rows.auth_request_state_upper = 'AUTHORISED') AS has_same_operation_authorised_auth_request,
      EXISTS (SELECT 1 FROM auth_request_rows WHERE auth_request_rows.auth_request_state_upper = 'PENDING_AUTHORISATION') AS has_pending_authorisation_auth_request,
      EXISTS (
        SELECT 1
        FROM auth_request_rows
        WHERE auth_request_rows.is_other_operation_auth_request
          AND auth_request_rows.owner_operation_is_terminal_cleanup_candidate IS NOT TRUE
      ) AS has_non_terminal_other_operation_auth_request,
      EXISTS (
        SELECT 1
        FROM auth_request_rows
        WHERE auth_request_rows.is_other_operation_auth_request
          AND auth_request_rows.owner_operation_is_terminal_cleanup_candidate IS TRUE
      ) AS has_terminal_other_operation_auth_request
  )
  SELECT
    final_rows.transfer_id AS pay_bank_transfer_id,
    final_rows.transfer_pay_batch_id AS pay_batch_id,
    final_rows.transfer_pay_channel AS pay_channel,
    final_rows.transfer_group_key AS transfer_group_key,
    final_rows.scope_id AS scope_id,
    final_rows.scope_operation_id AS scope_operation_id,
    final_rows.scope_status AS scope_status,
    final_rows.scope_request_id AS scope_request_id,
    final_rows.status_upper AS status_upper,
    final_rows.rail_state_upper AS rail_state_upper,
    final_rows.transfer_amount AS amount,
    final_rows.transfer_currency AS currency,
    final_rows.calc_has_route_ready AS has_route_ready,
    final_rows.calc_has_local_prepare_identity AS has_local_prepare_identity,
    final_rows.calc_has_provider_submission_evidence AS has_provider_submission_evidence,
    final_rows.calc_has_provider_event_evidence AS has_provider_event_evidence,
    (
      final_rows.calc_has_provider_attempt_without_external_id
      AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
    ) AS has_provider_attempt_without_external_id,
    final_rows.calc_has_operation_submit_attempt AS has_operation_submit_attempt,
    final_rows.calc_has_ambiguous_external_evidence AS has_ambiguous_external_evidence,
    final_rows.calc_is_failed_or_blocked AS is_failed_or_blocked,
    final_rows.calc_is_terminal_or_completed AS is_terminal_or_completed,
    final_rows.calc_evidence_classification AS evidence_classification,
    (
      final_rows.transfer_id IS NOT NULL
      AND final_rows.transfer_pay_batch_id = p_pay_batch_id
      AND (v_scope IN ('ALL', 'ANY', '*') OR upper(btrim(coalesce(final_rows.transfer_pay_channel, ''))) = v_scope)
      AND final_rows.status_upper = 'PENDING'
      AND final_rows.calc_has_route_ready IS TRUE
      AND (
        p_operation_id IS NULL
        OR upper(btrim(coalesce(final_rows.scope_status, ''))) = 'PREPARED'
      )
      AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
      AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
      AND final_rows.calc_is_failed_or_blocked IS NOT TRUE
      AND final_rows.calc_is_terminal_or_completed IS NOT TRUE
      AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
      AND auth_context.has_active_auth_request IS NOT TRUE
      AND final_rows.calc_has_stale_auth_request_evidence IS NOT TRUE
      AND NOT (
        p_operation_id IS NOT NULL
        AND final_rows.scope_operation_id IS NOT NULL
        AND final_rows.scope_operation_id <> p_operation_id
      )
      AND (
        p_operation_id IS NULL
        OR final_rows.scope_operation_id = p_operation_id
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{created_by_operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{payment_execute_operation_id}', '')) = p_operation_id::text
      )
    ) AS is_authorisation_ready,
    (
      final_rows.transfer_id IS NOT NULL
      AND final_rows.transfer_pay_batch_id = p_pay_batch_id
      AND (v_scope IN ('ALL', 'ANY', '*') OR upper(btrim(coalesce(final_rows.transfer_pay_channel, ''))) = v_scope)
      AND final_rows.status_upper = 'PENDING'
      AND final_rows.calc_has_route_ready IS TRUE
      AND (
        p_operation_id IS NULL
        OR upper(btrim(coalesce(final_rows.scope_status, ''))) = 'PREPARED'
      )
      AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
      AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
      AND final_rows.calc_is_failed_or_blocked IS NOT TRUE
      AND final_rows.calc_is_terminal_or_completed IS NOT TRUE
      AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
      AND auth_context.has_active_auth_request IS NOT TRUE
      AND final_rows.calc_has_stale_auth_request_evidence IS NOT TRUE
      AND NOT (
        p_operation_id IS NOT NULL
        AND final_rows.scope_operation_id IS NOT NULL
        AND final_rows.scope_operation_id <> p_operation_id
      )
      AND (
        p_operation_id IS NULL
        OR final_rows.scope_operation_id = p_operation_id
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{created_by_operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{payment_execute_operation_id}', '')) = p_operation_id::text
      )
    ) AS is_unattempted_submit_eligible,
    (
      final_rows.calc_is_operation_owned IS TRUE
      AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
      AND (
        auth_context.has_active_auth_request IS NOT TRUE
        OR (
          (auth_context.has_same_operation_awaiting_auth_request OR auth_context.has_same_operation_authorised_auth_request)
          AND auth_context.has_pending_authorisation_auth_request IS NOT TRUE
          AND auth_context.has_other_operation_active_auth_request IS NOT TRUE
          AND (
            final_rows.calc_has_provider_submission_evidence IS NOT TRUE
            AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
            AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
                  AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
            AND final_rows.calc_has_non_local_rail_tx_id IS NOT TRUE
            AND final_rows.calc_has_non_local_rail_meta_identifier IS NOT TRUE
            AND final_rows.calc_has_provider_source_flag IS NOT TRUE
          )
        )
      )
      AND (
        (
          final_rows.transfer_id IS NOT NULL
          AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
          AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
          AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
              AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
          AND final_rows.calc_is_failed_or_blocked IS NOT TRUE
          AND final_rows.calc_is_terminal_or_completed IS NOT TRUE
        )
        OR (
          final_rows.transfer_id IS NULL
          AND final_rows.calc_scope_owner_is_terminal_cleanup_candidate IS TRUE
        )
      )
    ) AS is_safe_local_cleanup,
    (
      final_rows.transfer_id IS NOT NULL
      AND final_rows.status_upper = 'PENDING'
    ) AS is_canonical_pending_status,
    (upper(btrim(coalesce(final_rows.scope_status, ''))) = 'PREPARED') AS has_auth_prepared_scope,
    (
      p_operation_id IS NOT NULL
      AND final_rows.scope_operation_id IS NOT NULL
      AND final_rows.scope_operation_id <> p_operation_id
    ) AS has_different_operation_scope,
    (
      auth_context.has_active_auth_request IS TRUE
      AND NOT (
        (auth_context.has_same_operation_awaiting_auth_request OR auth_context.has_same_operation_authorised_auth_request)
        AND auth_context.has_pending_authorisation_auth_request IS NOT TRUE
        AND auth_context.has_other_operation_active_auth_request IS NOT TRUE
        AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
        AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
        AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
        AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
          AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
        AND final_rows.calc_has_non_local_rail_tx_id IS NOT TRUE
        AND final_rows.calc_has_non_local_rail_meta_identifier IS NOT TRUE
        AND final_rows.calc_has_provider_source_flag IS NOT TRUE
      )
    ) AS has_stale_auth_request_evidence,
    auth_context.auth_request_id AS auth_request_id,
    auth_context.auth_request_state AS auth_request_state,
    auth_context.auth_request_operation_id AS auth_request_operation_id,
    auth_context.has_same_operation_active_auth_request AS has_same_operation_active_auth_request,
    auth_context.has_other_operation_active_auth_request AS has_other_operation_active_auth_request,
    (
      auth_context.has_active_auth_request IS TRUE
      AND (auth_context.has_same_operation_awaiting_auth_request OR auth_context.has_same_operation_authorised_auth_request)
      AND auth_context.has_pending_authorisation_auth_request IS NOT TRUE
      AND auth_context.has_other_operation_active_auth_request IS NOT TRUE
      AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
      AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
      AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
      AND final_rows.calc_has_non_local_rail_tx_id IS NOT TRUE
      AND final_rows.calc_has_non_local_rail_meta_identifier IS NOT TRUE
      AND final_rows.calc_has_provider_source_flag IS NOT TRUE
    ) AS has_cancellable_local_auth_request,
    (
      auth_context.has_active_auth_request IS TRUE
      AND NOT (
        (auth_context.has_same_operation_awaiting_auth_request OR auth_context.has_same_operation_authorised_auth_request)
        AND auth_context.has_pending_authorisation_auth_request IS NOT TRUE
        AND auth_context.has_other_operation_active_auth_request IS NOT TRUE
        AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
        AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
        AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
        AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
          AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
        AND final_rows.calc_has_non_local_rail_tx_id IS NOT TRUE
        AND final_rows.calc_has_non_local_rail_meta_identifier IS NOT TRUE
        AND final_rows.calc_has_provider_source_flag IS NOT TRUE
      )
    ) AS has_non_cancellable_auth_request,
    (
      auth_context.has_same_operation_authorised_auth_request IS TRUE
      AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
      AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
      AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
      AND final_rows.calc_has_non_local_rail_tx_id IS NOT TRUE
      AND final_rows.calc_has_non_local_rail_meta_identifier IS NOT TRUE
      AND final_rows.calc_has_provider_source_flag IS NOT TRUE
    ) AS has_authorised_auth_without_provider_submission,
    (
      auth_context.has_active_auth_request IS TRUE
      AND (
        final_rows.batch_execution_boundary_crossed IS TRUE
        OR final_rows.calc_has_provider_submission_evidence IS TRUE
        OR final_rows.calc_has_provider_event_evidence IS TRUE
        OR final_rows.calc_has_provider_attempt_without_external_id IS TRUE
        OR final_rows.calc_has_ambiguous_external_evidence IS TRUE
        OR final_rows.calc_has_non_local_rail_tx_id IS TRUE
        OR final_rows.calc_has_non_local_rail_meta_identifier IS TRUE
        OR final_rows.calc_has_provider_source_flag IS TRUE
      )
    ) AS has_auth_request_provider_risk,
    CASE
      WHEN auth_context.has_active_auth_request IS NOT TRUE THEN NULL::text
      WHEN final_rows.batch_execution_boundary_crossed THEN 'AUTH_REQUEST_BATCH_EXECUTION_BOUNDARY_CROSSED'
      WHEN final_rows.calc_has_provider_event_evidence THEN 'AUTH_REQUEST_PROVIDER_EVENT_EVIDENCE_PRESENT'
      WHEN final_rows.calc_has_provider_submission_evidence THEN 'AUTH_REQUEST_PROVIDER_SUBMISSION_EVIDENCE_PRESENT'
      WHEN final_rows.calc_has_stale_provider_submit_chunk THEN 'AUTH_REQUEST_PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
      WHEN final_rows.calc_has_ambiguous_external_evidence THEN 'AUTH_REQUEST_AMBIGUOUS_EXTERNAL_EVIDENCE'
      WHEN final_rows.calc_has_provider_attempt_without_external_id THEN 'AUTH_REQUEST_PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID'
      WHEN final_rows.calc_has_non_local_rail_tx_id THEN 'AUTH_REQUEST_NON_LOCAL_RAIL_TX_ID_PRESENT'
      WHEN final_rows.calc_has_non_local_rail_meta_identifier THEN 'AUTH_REQUEST_NON_LOCAL_PROVIDER_IDENTIFIER_PRESENT'
      WHEN final_rows.calc_has_provider_source_flag THEN 'AUTH_REQUEST_PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID'
      WHEN auth_context.has_pending_authorisation_auth_request THEN 'AUTH_REQUEST_PENDING_AUTHORISATION'
      WHEN auth_context.has_other_operation_active_auth_request THEN 'AUTH_REQUEST_OWNED_BY_PREVIOUS_OPERATION'
      WHEN auth_context.has_same_operation_awaiting_auth_request OR auth_context.has_same_operation_authorised_auth_request THEN NULL::text
      ELSE 'ACTIVE_AUTH_REQUEST_NOT_CANCELLABLE'
    END AS auth_request_unsafe_reason,
    CASE
      WHEN final_rows.batch_execution_boundary_crossed THEN 'BATCH_EXECUTION_BOUNDARY_CROSSED'
      WHEN p_operation_id IS NOT NULL AND final_rows.transfer_id IS NOT NULL AND upper(btrim(coalesce(final_rows.scope_status, ''))) <> 'PREPARED' THEN 'AUTH_PREPARED_SCOPE_REQUIRED'
      WHEN final_rows.calc_has_provider_event_evidence THEN 'PROVIDER_EVENT_EVIDENCE_PRESENT'
      WHEN final_rows.calc_has_non_local_rail_tx_id THEN 'NON_LOCAL_RAIL_TX_ID_PRESENT'
      WHEN final_rows.calc_has_non_local_rail_meta_identifier THEN 'NON_LOCAL_PROVIDER_IDENTIFIER_PRESENT'
      WHEN final_rows.calc_has_provider_source_flag THEN 'PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID'
      WHEN final_rows.calc_has_stale_provider_submit_chunk THEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
      WHEN final_rows.calc_has_ambiguous_external_evidence THEN COALESCE(NULLIF(final_rows.provider_submit_status_upper, ''), 'AMBIGUOUS_EXTERNAL_EVIDENCE')
      WHEN final_rows.calc_has_provider_attempt_without_external_id THEN 'PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID'
      WHEN final_rows.calc_is_terminal_or_completed THEN 'TERMINAL_OR_COMPLETED_TRANSFER_STATE'
      WHEN final_rows.calc_is_failed_or_blocked THEN 'FAILED_OR_BLOCKED_TRANSFER_STATE'
      WHEN auth_context.has_active_auth_request IS TRUE
        AND NOT (
          (auth_context.has_same_operation_awaiting_auth_request OR auth_context.has_same_operation_authorised_auth_request)
          AND auth_context.has_pending_authorisation_auth_request IS NOT TRUE
          AND auth_context.has_other_operation_active_auth_request IS NOT TRUE
          AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
          AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
          AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
          AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
              AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
          AND final_rows.calc_has_non_local_rail_tx_id IS NOT TRUE
          AND final_rows.calc_has_non_local_rail_meta_identifier IS NOT TRUE
          AND final_rows.calc_has_provider_source_flag IS NOT TRUE
        ) THEN CASE
          WHEN final_rows.batch_execution_boundary_crossed THEN 'AUTH_REQUEST_BATCH_EXECUTION_BOUNDARY_CROSSED'
          WHEN final_rows.calc_has_provider_event_evidence THEN 'AUTH_REQUEST_PROVIDER_EVENT_EVIDENCE_PRESENT'
          WHEN final_rows.calc_has_provider_submission_evidence THEN 'AUTH_REQUEST_PROVIDER_SUBMISSION_EVIDENCE_PRESENT'
          WHEN final_rows.calc_has_stale_provider_submit_chunk THEN 'AUTH_REQUEST_PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
          WHEN final_rows.calc_has_ambiguous_external_evidence THEN 'AUTH_REQUEST_AMBIGUOUS_EXTERNAL_EVIDENCE'
          WHEN final_rows.calc_has_provider_attempt_without_external_id THEN 'AUTH_REQUEST_PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID'
              WHEN final_rows.calc_has_non_local_rail_tx_id THEN 'AUTH_REQUEST_NON_LOCAL_RAIL_TX_ID_PRESENT'
          WHEN final_rows.calc_has_non_local_rail_meta_identifier THEN 'AUTH_REQUEST_NON_LOCAL_PROVIDER_IDENTIFIER_PRESENT'
          WHEN final_rows.calc_has_provider_source_flag THEN 'AUTH_REQUEST_PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID'
          WHEN auth_context.has_pending_authorisation_auth_request THEN 'AUTH_REQUEST_PENDING_AUTHORISATION'
          WHEN auth_context.has_other_operation_active_auth_request THEN 'AUTH_REQUEST_OWNED_BY_PREVIOUS_OPERATION'
          ELSE 'ACTIVE_AUTH_REQUEST_NOT_CANCELLABLE'
        END
      WHEN final_rows.calc_has_stale_auth_request_evidence THEN 'STALE_AUTH_REQUEST_EVIDENCE_PRESENT'
      WHEN p_operation_id IS NOT NULL AND final_rows.scope_operation_id IS NOT NULL AND final_rows.scope_operation_id <> p_operation_id THEN 'TRANSFER_SCOPE_OWNED_BY_DIFFERENT_OPERATION'
      WHEN final_rows.transfer_id IS NULL AND final_rows.calc_scope_owner_is_terminal_cleanup_candidate IS NOT TRUE THEN 'SCOPE_WITHOUT_TRANSFER_NOT_TERMINAL_OPERATION'
      WHEN final_rows.transfer_id IS NOT NULL AND final_rows.calc_has_route_ready IS NOT TRUE THEN 'TRANSFER_ROUTE_NOT_READY'
      WHEN final_rows.transfer_id IS NOT NULL AND final_rows.status_upper <> 'PENDING' THEN 'TRANSFER_NOT_PENDING'
      ELSE NULL::text
    END AS unsafe_reason,
    auth_context.has_same_operation_authorised_auth_request AS has_same_operation_authorised_auth_request,
    auth_context.has_pending_authorisation_auth_request AS has_pending_authorisation_auth_request,
    (
      p_operation_id IS NOT NULL
      AND final_rows.transfer_id IS NOT NULL
      AND final_rows.transfer_pay_batch_id = p_pay_batch_id
      AND (v_scope IN ('ALL', 'ANY', '*') OR upper(btrim(coalesce(final_rows.transfer_pay_channel, ''))) = v_scope)
      AND final_rows.status_upper = 'PENDING'
      AND final_rows.calc_has_route_ready IS TRUE
      AND upper(btrim(coalesce(final_rows.scope_status, ''))) = 'PREPARED'
      AND (
        final_rows.scope_operation_id = p_operation_id
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{created_by_operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{payment_execute_operation_id}', '')) = p_operation_id::text
      )
      AND auth_context.has_same_operation_authorised_auth_request IS TRUE
      AND auth_context.has_pending_authorisation_auth_request IS NOT TRUE
      AND auth_context.has_other_operation_active_auth_request IS NOT TRUE
      AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
      AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
      AND final_rows.calc_has_non_local_rail_tx_id IS NOT TRUE
      AND final_rows.calc_has_non_local_rail_meta_identifier IS NOT TRUE
      AND final_rows.calc_has_provider_source_flag IS NOT TRUE
      AND final_rows.calc_is_failed_or_blocked IS NOT TRUE
      AND final_rows.calc_is_terminal_or_completed IS NOT TRUE
      AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
      AND final_rows.calc_has_stale_auth_request_evidence IS NOT TRUE
      AND NOT (
        final_rows.scope_operation_id IS NOT NULL
        AND final_rows.scope_operation_id <> p_operation_id
      )
    ) AS is_provider_submit_ready,
    CASE
      WHEN p_operation_id IS NULL THEN 'PROVIDER_SUBMIT_OPERATION_ID_REQUIRED'
      WHEN final_rows.transfer_id IS NULL THEN 'PROVIDER_SUBMIT_TRANSFER_REQUIRED'
      WHEN final_rows.batch_execution_boundary_crossed THEN 'PROVIDER_SUBMIT_BATCH_EXECUTION_BOUNDARY_CROSSED'
      WHEN final_rows.calc_has_provider_event_evidence THEN 'PROVIDER_SUBMIT_PROVIDER_EVENT_EVIDENCE_PRESENT'
      WHEN final_rows.calc_has_provider_submission_evidence THEN 'PROVIDER_SUBMIT_PROVIDER_SUBMISSION_EVIDENCE_PRESENT'
      WHEN final_rows.calc_has_stale_provider_submit_chunk THEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
      WHEN final_rows.calc_has_ambiguous_external_evidence THEN COALESCE(NULLIF(final_rows.provider_submit_status_upper, ''), 'PROVIDER_SUBMIT_AMBIGUOUS_EXTERNAL_EVIDENCE')
      WHEN final_rows.calc_has_provider_attempt_without_external_id THEN 'PROVIDER_SUBMIT_PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID'
      WHEN final_rows.calc_has_non_local_rail_tx_id THEN 'PROVIDER_SUBMIT_NON_LOCAL_RAIL_TX_ID_PRESENT'
      WHEN final_rows.calc_has_non_local_rail_meta_identifier THEN 'PROVIDER_SUBMIT_NON_LOCAL_PROVIDER_IDENTIFIER_PRESENT'
      WHEN final_rows.calc_has_provider_source_flag THEN 'PROVIDER_SUBMIT_PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID'
      WHEN final_rows.calc_is_terminal_or_completed THEN 'PROVIDER_SUBMIT_TERMINAL_OR_COMPLETED_TRANSFER_STATE'
      WHEN final_rows.calc_is_failed_or_blocked THEN 'PROVIDER_SUBMIT_FAILED_OR_BLOCKED_TRANSFER_STATE'
      WHEN final_rows.calc_has_stale_auth_request_evidence THEN 'PROVIDER_SUBMIT_STALE_AUTH_REQUEST_EVIDENCE_PRESENT'
      WHEN final_rows.status_upper <> 'PENDING' THEN 'PROVIDER_SUBMIT_TRANSFER_NOT_PENDING'
      WHEN final_rows.calc_has_route_ready IS NOT TRUE THEN 'PROVIDER_SUBMIT_TRANSFER_ROUTE_NOT_READY'
      WHEN upper(btrim(coalesce(final_rows.scope_status, ''))) <> 'PREPARED' THEN 'PROVIDER_SUBMIT_PREPARED_SCOPE_REQUIRED'
      WHEN (
        final_rows.scope_operation_id IS NULL
        AND btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{operation_id}', '')) <> p_operation_id::text
        AND btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{created_by_operation_id}', '')) <> p_operation_id::text
        AND btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{payment_execute_operation_id}', '')) <> p_operation_id::text
      ) THEN 'PROVIDER_SUBMIT_OPERATION_OWNERSHIP_REQUIRED'
      WHEN final_rows.scope_operation_id IS NOT NULL AND final_rows.scope_operation_id <> p_operation_id THEN 'PROVIDER_SUBMIT_SCOPE_OWNED_BY_DIFFERENT_OPERATION'
      WHEN auth_context.has_same_operation_authorised_auth_request IS NOT TRUE THEN 'PROVIDER_SUBMIT_SAME_OPERATION_AUTHORISED_AUTH_REQUIRED'
      WHEN auth_context.has_pending_authorisation_auth_request THEN 'PROVIDER_SUBMIT_PENDING_AUTHORISATION_AUTH_REQUEST_PRESENT'
      WHEN auth_context.has_other_operation_active_auth_request THEN 'PROVIDER_SUBMIT_OTHER_OPERATION_ACTIVE_AUTH_REQUEST_PRESENT'
      ELSE NULL::text
    END AS provider_submit_unsafe_reason,
    (
      p_operation_id IS NOT NULL
      AND final_rows.transfer_id IS NOT NULL
      AND auth_context.has_same_operation_authorised_auth_request IS TRUE
      AND (
      p_operation_id IS NOT NULL
      AND final_rows.transfer_id IS NOT NULL
      AND final_rows.transfer_pay_batch_id = p_pay_batch_id
      AND (v_scope IN ('ALL', 'ANY', '*') OR upper(btrim(coalesce(final_rows.transfer_pay_channel, ''))) = v_scope)
      AND final_rows.status_upper = 'PENDING'
      AND final_rows.calc_has_route_ready IS TRUE
      AND upper(btrim(coalesce(final_rows.scope_status, ''))) = 'PREPARED'
      AND (
        final_rows.scope_operation_id = p_operation_id
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{created_by_operation_id}', '')) = p_operation_id::text
        OR btrim(coalesce(final_rows.transfer_rail_meta_json #>> '{payment_execute_operation_id}', '')) = p_operation_id::text
      )
      AND auth_context.has_same_operation_authorised_auth_request IS TRUE
      AND auth_context.has_pending_authorisation_auth_request IS NOT TRUE
      AND auth_context.has_other_operation_active_auth_request IS NOT TRUE
      AND final_rows.calc_has_provider_submission_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_event_evidence IS NOT TRUE
      AND final_rows.calc_has_provider_attempt_without_external_id IS NOT TRUE
      AND final_rows.calc_has_ambiguous_external_evidence IS NOT TRUE
      AND final_rows.calc_has_non_local_rail_tx_id IS NOT TRUE
      AND final_rows.calc_has_non_local_rail_meta_identifier IS NOT TRUE
      AND final_rows.calc_has_provider_source_flag IS NOT TRUE
      AND final_rows.calc_is_failed_or_blocked IS NOT TRUE
      AND final_rows.calc_is_terminal_or_completed IS NOT TRUE
      AND final_rows.batch_execution_boundary_crossed IS NOT TRUE
      AND final_rows.calc_has_stale_auth_request_evidence IS NOT TRUE
      AND NOT (
        final_rows.scope_operation_id IS NOT NULL
        AND final_rows.scope_operation_id <> p_operation_id
      )
    ) IS NOT TRUE
    ) AS has_provider_submit_blocker
  FROM final_rows
  CROSS JOIN auth_context AS auth_context
  ORDER BY
    final_rows.transfer_pay_channel,
    final_rows.transfer_group_key NULLS LAST,
    final_rows.transfer_id NULLS LAST,
    final_rows.scope_id NULLS LAST;
END;
$function$;








CREATE OR REPLACE FUNCTION public.pay_execute_operation_cleanup_failed_local_artifacts(
  p_operation_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_failure_phase text DEFAULT NULL::text,
  p_failure_error_json jsonb DEFAULT '{}'::jsonb,
  p_dry_run boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_batch_row public.pay_batches%ROWTYPE;
  v_operation_type_upper text := NULL::text;
  v_operation_type_cleanup_mode text := NULL::text;
  v_retry_blocked_reason text := NULL::text;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_failure_phase text := NULL::text;
  v_failure_error_json jsonb := '{}'::jsonb;
  v_batch_execution_boundary_crossed boolean := false;
  v_active_auth_request_count integer := 0;
  v_initial_active_auth_request_count integer := 0;
  v_operation_active_auth_request_count integer := 0;
  v_active_auth_request_ids jsonb := '[]'::jsonb;
  v_auth_requests_cancelled integer := 0;
  v_awaiting_auth_requests_cancelled integer := 0;
  v_authorised_auth_requests_cancelled integer := 0;
  v_auth_tokens_voided integer := 0;
  v_batch_execution_intent_cleared integer := 0;
  v_active_auth_request_blocker_count integer := 0;
  v_authorised_auth_request_review_count integer := 0;
  v_cancellable_auth_request_count integer := 0;
  v_non_cancellable_auth_request_count integer := 0;
  v_cancelled_auth_request_ids jsonb := '[]'::jsonb;
  v_provider_submit_chunk_attempt_count integer := 0;
  v_stale_empty_provider_submit_chunk_count integer := 0;
  v_provider_submit_unknown_chunk_count integer := 0;
  v_deletion_allowed boolean := false;
  v_scope_rows_considered integer := 0;
  v_transfer_rows_considered integer := 0;
  v_safe_scope_candidate_count integer := 0;
  v_safe_transfer_candidate_count integer := 0;
  v_scope_rows_deleted integer := 0;
  v_transfer_rows_deleted integer := 0;
  v_item_links_cleared integer := 0;
  v_bank_references_cleared integer := 0;
  v_chunks_marked_failed integer := 0;
  v_chunks_marked_skipped integer := 0;
  v_locks_released integer := 0;
  v_provider_evidence_count integer := 0;
  v_provider_review_risk_count integer := 0;
  v_unsafe_transfer_count integer := 0;
  v_unsafe_scope_count integer := 0;
  v_remaining_operation_scope_count integer := 0;
  v_remaining_operation_transfer_count integer := 0;
  v_retry_blocked boolean := false;
  v_safe_to_retry boolean := false;
  v_review_required boolean := false;
  v_cleanup_mode text := 'NOTHING_TO_CLEAN';
  v_safe_scope_ids jsonb := '[]'::jsonb;
  v_safe_transfer_ids jsonb := '[]'::jsonb;
  v_deleted_scope_ids jsonb := '[]'::jsonb;
  v_deleted_transfer_ids jsonb := '[]'::jsonb;
  v_unsafe_transfer_ids jsonb := '[]'::jsonb;
  v_unsafe_reasons jsonb := '[]'::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_provider_submit_finalise_result jsonb := '{}'::jsonb;
  v_provider_submit_diagnostic_result jsonb := '{}'::jsonb;
  v_provider_submit_diagnostic jsonb := '{}'::jsonb;
  v_provider_submission_status text := NULL::text;
  v_provider_submit_review_reason_code text := NULL::text;
  v_provider_acceptance_evidence_count integer := 0;
  v_provider_response_present_count integer := 0;
  v_provider_request_sent_count integer := 0;
  v_provider_submission_unknown_count integer := 0;
  v_stale_unresolved_submit_chunk_count integer := 0;
  v_unfinalised_submit_chunk_count integer := 0;
  v_provider_manual_resolution_required boolean := false;
  v_provider_safe_retry_available boolean := false;
  v_provider_recommended_action text := NULL::text;
  v_provider_submit_requires_manual_review boolean := false;
  v_provider_submit_allows_local_cleanup boolean := false;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_ID_REQUIRED',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: operation_id is required'
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_failure_phase := NULLIF(BTRIM(COALESCE(p_failure_phase, '')), '');

  IF p_failure_error_json IS NOT NULL AND jsonb_typeof(p_failure_error_json) = 'object' THEN
    v_failure_error_json := p_failure_error_json;
  ELSE
    v_failure_error_json := '{}'::jsonb;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF v_operation_row.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_NOT_FOUND',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: operation not found',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_operation_row.actor_user_id);

  IF v_effective_actor_user_id IS NOT NULL
     AND NOT EXISTS (
       SELECT 1
       FROM public.tms_users AS actor_user
       WHERE actor_user.id = v_effective_actor_user_id
     ) THEN
    v_effective_actor_user_id := NULL::uuid;
  END IF;

  v_operation_type_upper := upper(BTRIM(COALESCE(v_operation_row.operation_type, '')));
  v_operation_type_cleanup_mode := CASE
    WHEN v_operation_type_upper = 'PAYMENT_EXECUTE' THEN 'STANDARD_PAYMENT_EXECUTE_LOCAL_ARTIFACT_CLEANUP'
    WHEN v_operation_type_upper = 'PAYMENT_RETRY_BLOCKED_FUNDS' THEN 'CONDITIONAL_BLOCKED_FUNDS_RETRY_LOCAL_ARTIFACT_CLEANUP'
    ELSE 'UNSUPPORTED_OPERATION_TYPE'
  END;

  IF v_operation_type_upper NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'OPERATION_TYPE_NOT_SUPPORTED',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: only PAYMENT_EXECUTE and proven local PAYMENT_RETRY_BLOCKED_FUNDS operations are supported',
      'operation_id', p_operation_id::text,
      'operation_type', v_operation_row.operation_type,
      'operation_types_supported', jsonb_build_array('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF upper(BTRIM(COALESCE(v_operation_row.status, ''))) = 'COMPLETE' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'COMPLETE_OPERATION_CANNOT_BE_CLEANED',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: completed payment execution operations cannot be cleaned as failed local artefacts',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_row.pay_batch_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: operation has no pay_batch_id',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_failure_phase IS NULL THEN
    v_failure_phase := NULLIF(BTRIM(COALESCE(v_operation_row.phase, '')), '');
  END IF;

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = v_operation_row.pay_batch_id
  FOR UPDATE;

  IF v_batch_row.id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_OPERATION_CLEANUP_FAILED_LOCAL_ARTIFACTS',
      'code', 'PAY_BATCH_NOT_FOUND',
      'message', 'pay_execute_operation_cleanup_failed_local_artifacts: pay batch not found',
      'operation_id', p_operation_id::text,
      'pay_batch_id', v_operation_row.pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_batch_execution_boundary_crossed := (
    upper(BTRIM(COALESCE(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
    OR NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
    OR v_batch_row.execution_committed_at_utc IS NOT NULL
  );

  IF COALESCE(p_dry_run, false) IS NOT TRUE THEN
    BEGIN
      v_provider_submit_finalise_result := public.pay_provider_submit_chunk_diagnostic_finalise(
        p_operation_id := p_operation_id,
        p_pay_batch_id := v_operation_row.pay_batch_id,
        p_chunk_id := NULL::uuid,
        p_actor_user_id := v_effective_actor_user_id,
        p_reason_code := 'FAILED_EXECUTION_CLEANUP_PROVIDER_SUBMIT_DIAGNOSTIC',
        p_failure_error_json := v_failure_error_json
      );
    EXCEPTION
      WHEN undefined_function THEN
        v_provider_submit_finalise_result := '{}'::jsonb;
      WHEN OTHERS THEN
        v_provider_submit_finalise_result := jsonb_build_object(
          'ok', false,
          'error', SQLERRM,
          'reason', 'PROVIDER_SUBMIT_DIAGNOSTIC_FINALISE_FAILED'
        );
    END;
  END IF;

  BEGIN
    v_provider_submit_diagnostic_result := public.pay_provider_submit_diagnostic_get(
      p_pay_batch_id := v_operation_row.pay_batch_id,
      p_operation_id := p_operation_id,
      p_transfer_id := NULL::uuid,
      p_chunk_id := NULL::uuid,
      p_counts_only := true
    );
  EXCEPTION
    WHEN undefined_function THEN
      v_provider_submit_diagnostic_result := '{}'::jsonb;
    WHEN OTHERS THEN
      v_provider_submit_diagnostic_result := jsonb_build_object(
        'ok', false,
        'error', SQLERRM,
        'reason', 'PROVIDER_SUBMIT_DIAGNOSTIC_GET_FAILED'
      );
  END;

  v_provider_submit_diagnostic := COALESCE(v_provider_submit_diagnostic_result->'provider_submit_diagnostic', '{}'::jsonb);
  v_provider_submission_status := NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'provider_submission_status', v_provider_submit_diagnostic_result->>'provider_submission_status', '')), '');
  v_provider_submit_review_reason_code := NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'review_reason_code', v_provider_submit_diagnostic_result->>'review_reason_code', '')), '');
  v_provider_recommended_action := NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'recommended_action', v_provider_submit_diagnostic_result->>'recommended_action', '')), '');
  v_provider_manual_resolution_required := lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'manual_resolution_required', v_provider_submit_diagnostic_result->>'manual_resolution_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_provider_safe_retry_available := lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'safe_retry_available', v_provider_submit_diagnostic_result->>'safe_retry_available', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF COALESCE(v_provider_submit_diagnostic_result #>> '{counts,provider_acceptance_evidence_count}', '') ~ '^[0-9]+$' THEN
    v_provider_acceptance_evidence_count := (v_provider_submit_diagnostic_result #>> '{counts,provider_acceptance_evidence_count}')::integer;
  END IF;
  IF COALESCE(v_provider_submit_diagnostic_result #>> '{counts,provider_response_present_count}', '') ~ '^[0-9]+$' THEN
    v_provider_response_present_count := (v_provider_submit_diagnostic_result #>> '{counts,provider_response_present_count}')::integer;
  END IF;
  IF COALESCE(v_provider_submit_diagnostic_result #>> '{counts,provider_request_sent_count}', '') ~ '^[0-9]+$' THEN
    v_provider_request_sent_count := (v_provider_submit_diagnostic_result #>> '{counts,provider_request_sent_count}')::integer;
  END IF;
  IF COALESCE(v_provider_submit_diagnostic_result #>> '{counts,provider_submission_unknown_count}', '') ~ '^[0-9]+$' THEN
    v_provider_submission_unknown_count := (v_provider_submit_diagnostic_result #>> '{counts,provider_submission_unknown_count}')::integer;
  END IF;
  IF COALESCE(v_provider_submit_diagnostic_result #>> '{counts,stale_unresolved_submit_chunk_count}', v_provider_submit_diagnostic_result #>> '{counts,stale_empty_submit_chunk_count}', '') ~ '^[0-9]+$' THEN
    v_stale_unresolved_submit_chunk_count := COALESCE(v_provider_submit_diagnostic_result #>> '{counts,stale_unresolved_submit_chunk_count}', v_provider_submit_diagnostic_result #>> '{counts,stale_empty_submit_chunk_count}')::integer;
  END IF;
  IF COALESCE(v_provider_submit_diagnostic_result #>> '{counts,unfinalised_submit_chunk_count}', '') ~ '^[0-9]+$' THEN
    v_unfinalised_submit_chunk_count := (v_provider_submit_diagnostic_result #>> '{counts,unfinalised_submit_chunk_count}')::integer;
  END IF;

  v_provider_submit_requires_manual_review := COALESCE(v_provider_acceptance_evidence_count, 0) > 0
    OR COALESCE(v_stale_unresolved_submit_chunk_count, 0) > 0
    OR COALESCE(v_provider_submission_unknown_count, 0) > 0
    OR COALESCE(v_unfinalised_submit_chunk_count, 0) > 0
    OR COALESCE(v_provider_submission_status, '') IN (
      'PROVIDER_SUBMISSION_ACCEPTED',
      'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME',
      'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK',
      'PROVIDER_SUBMISSION_MALFORMED_RESPONSE',
      'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID'
    );

  v_provider_submit_allows_local_cleanup := COALESCE(v_provider_acceptance_evidence_count, 0) = 0
    AND COALESCE(v_provider_request_sent_count, 0) = 0
    AND COALESCE(v_provider_response_present_count, 0) = 0
    AND COALESCE(v_provider_submission_unknown_count, 0) = 0
    AND COALESCE(v_stale_unresolved_submit_chunk_count, 0) = 0
    AND COALESCE(v_unfinalised_submit_chunk_count, 0) = 0
    AND COALESCE(v_provider_submission_status, 'NO_PROVIDER_SUBMISSION_ATTEMPTED') IN ('', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'MANUAL_RESOLVED_NO_PAYMENT_MADE');

  SELECT COUNT(*)::integer,
         (COUNT(*) FILTER (WHERE auth_request.execution_intent_json->>'operation_id' = p_operation_id::text))::integer,
         COALESCE(jsonb_agg(to_jsonb(auth_request.id::text) ORDER BY auth_request.created_at_utc NULLS LAST, auth_request.id), '[]'::jsonb)
  INTO v_active_auth_request_count,
       v_operation_active_auth_request_count,
       v_active_auth_request_ids
  FROM public.pay_batch_auth_requests AS auth_request
  WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
    AND auth_request.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

  v_initial_active_auth_request_count := COALESCE(v_active_auth_request_count, 0);

  SELECT COUNT(*) FILTER (
           WHERE lower(BTRIM(COALESCE(provider_submit_chunk_diagnostic.provider_submit_diagnostic->>'provider_request_sent', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR lower(BTRIM(COALESCE(provider_submit_chunk_diagnostic.provider_submit_diagnostic->>'provider_response_received', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR lower(BTRIM(COALESCE(provider_submit_chunk_diagnostic.provider_submit_diagnostic->>'provider_response_present', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR lower(BTRIM(COALESCE(provider_submit_chunk_diagnostic.provider_submit_diagnostic->>'provider_acceptance_evidence_present', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR COALESCE(provider_submit_chunk_diagnostic.provider_submit_diagnostic->>'provider_submission_status', '') IN (
                'PROVIDER_SUBMISSION_ACCEPTED',
                'PROVIDER_SUBMISSION_REJECTED',
                'PROVIDER_SUBMISSION_MALFORMED_RESPONSE',
                'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME'
              )
         )::integer,
         COUNT(*) FILTER (
           WHERE upper(BTRIM(COALESCE(provider_submit_chunk.status, ''))) = 'RUNNING'
             AND (provider_submit_chunk.lock_expires_at_utc IS NULL OR provider_submit_chunk.lock_expires_at_utc <= v_now)
             AND COALESCE(provider_submit_chunk.result_json, '{}'::jsonb) = '{}'::jsonb
             AND COALESCE(provider_submit_chunk.error_json, '{}'::jsonb) = '{}'::jsonb
         )::integer,
         COUNT(*) FILTER (
           WHERE COALESCE(provider_submit_chunk_diagnostic.provider_submit_diagnostic->>'provider_submission_status', '') IN (
             'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME',
             'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK',
             'PROVIDER_SUBMISSION_MALFORMED_RESPONSE'
           )
           OR (
             upper(BTRIM(COALESCE(provider_submit_chunk.status, ''))) = 'RUNNING'
             AND (provider_submit_chunk.lock_expires_at_utc IS NULL OR provider_submit_chunk.lock_expires_at_utc <= v_now)
             AND COALESCE(provider_submit_chunk.result_json, '{}'::jsonb) = '{}'::jsonb
             AND COALESCE(provider_submit_chunk.error_json, '{}'::jsonb) = '{}'::jsonb
           )
         )::integer
  INTO v_provider_submit_chunk_attempt_count,
       v_stale_empty_provider_submit_chunk_count,
       v_provider_submit_unknown_chunk_count
  FROM public.banking_pay_operation_chunks AS provider_submit_chunk
  CROSS JOIN LATERAL (
    SELECT COALESCE(
      CASE WHEN jsonb_typeof(provider_submit_chunk.error_json->'provider_submit_diagnostic') = 'object' THEN provider_submit_chunk.error_json->'provider_submit_diagnostic' ELSE NULL::jsonb END,
      CASE WHEN jsonb_typeof(provider_submit_chunk.result_json->'provider_submit_diagnostic') = 'object' THEN provider_submit_chunk.result_json->'provider_submit_diagnostic' ELSE NULL::jsonb END,
      '{}'::jsonb
    ) AS provider_submit_diagnostic
  ) AS provider_submit_chunk_diagnostic
  WHERE provider_submit_chunk.operation_id = p_operation_id
    AND (
      provider_submit_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS'
      OR provider_submit_chunk.chunk_type = 'TRANSFER_SUBMIT'
    )
    AND (
      provider_submit_chunk.status IN ('RUNNING', 'COMPLETE', 'FAILED')
      OR provider_submit_chunk.started_at_utc IS NOT NULL
      OR (provider_submit_chunk.result_json IS NOT NULL AND provider_submit_chunk.result_json <> '{}'::jsonb)
      OR (provider_submit_chunk.error_json IS NOT NULL AND provider_submit_chunk.error_json <> '{}'::jsonb)
    );

  v_provider_submit_chunk_attempt_count := COALESCE(v_provider_request_sent_count, 0) + COALESCE(v_provider_response_present_count, 0);
  v_stale_empty_provider_submit_chunk_count := COALESCE(v_stale_unresolved_submit_chunk_count, 0);
  v_provider_submit_unknown_chunk_count := COALESCE(v_provider_submission_unknown_count, 0);

  v_deletion_allowed := false;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_execute_cleanup_scope_classified;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_execute_cleanup_scope_classified (
    scope_id uuid PRIMARY KEY,
    pay_bank_transfer_id uuid,
    pay_channel text NOT NULL,
    transfer_group_key text NOT NULL,
    scope_status text,
    scope_request_id text,
    scope_payment_reference text,
    classifier_found boolean NOT NULL DEFAULT false,
    classifier_is_safe_local_cleanup boolean NOT NULL DEFAULT false,
    has_provider_submission_evidence boolean NOT NULL DEFAULT false,
    has_provider_event_evidence boolean NOT NULL DEFAULT false,
    has_provider_attempt_without_external_id boolean NOT NULL DEFAULT false,
    has_operation_submit_attempt boolean NOT NULL DEFAULT false,
    has_ambiguous_external_evidence boolean NOT NULL DEFAULT false,
    is_failed_or_blocked boolean NOT NULL DEFAULT false,
    is_terminal_or_completed boolean NOT NULL DEFAULT false,
    evidence_classification text,
    unsafe_reason text
  ) ON COMMIT DROP;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_execute_cleanup_transfer_classified;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_execute_cleanup_transfer_classified (
    pay_bank_transfer_id uuid PRIMARY KEY,
    pay_channel text NOT NULL,
    transfer_group_key text,
    payment_reference text,
    request_id text,
    classifier_found boolean NOT NULL DEFAULT false,
    is_safe_local_cleanup boolean NOT NULL DEFAULT false,
    has_provider_submission_evidence boolean NOT NULL DEFAULT false,
    has_provider_event_evidence boolean NOT NULL DEFAULT false,
    has_provider_attempt_without_external_id boolean NOT NULL DEFAULT false,
    has_operation_submit_attempt boolean NOT NULL DEFAULT false,
    has_ambiguous_external_evidence boolean NOT NULL DEFAULT false,
    is_failed_or_blocked boolean NOT NULL DEFAULT false,
    is_terminal_or_completed boolean NOT NULL DEFAULT false,
    evidence_classification text,
    unsafe_reason text
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_pay_execute_cleanup_scope_classified (
    scope_id,
    pay_bank_transfer_id,
    pay_channel,
    transfer_group_key,
    scope_status,
    scope_request_id,
    scope_payment_reference,
    classifier_found,
    classifier_is_safe_local_cleanup,
    has_provider_submission_evidence,
    has_provider_event_evidence,
    has_provider_attempt_without_external_id,
    has_operation_submit_attempt,
    has_ambiguous_external_evidence,
    is_failed_or_blocked,
    is_terminal_or_completed,
    evidence_classification,
    unsafe_reason
  )
  SELECT scope_row.id,
         scope_row.pay_bank_transfer_id,
         scope_row.pay_channel,
         scope_row.transfer_group_key,
         scope_row.status,
         scope_row.request_id,
         scope_row.payment_reference,
         (classifier_row.pay_bank_transfer_id IS NOT NULL OR classifier_row.scope_id IS NOT NULL),
         COALESCE(classifier_row.is_safe_local_cleanup, false),
         COALESCE(classifier_row.has_provider_submission_evidence, false),
         COALESCE(classifier_row.has_provider_event_evidence, false),
         COALESCE(classifier_row.has_provider_attempt_without_external_id, false),
         COALESCE(classifier_row.has_operation_submit_attempt, false),
         COALESCE(classifier_row.has_ambiguous_external_evidence, false),
         COALESCE(classifier_row.is_failed_or_blocked, false),
         COALESCE(classifier_row.is_terminal_or_completed, false),
         classifier_row.evidence_classification,
         CASE
           WHEN v_batch_execution_boundary_crossed THEN 'BATCH_EXECUTION_BOUNDARY_CROSSED'
           WHEN scope_row.pay_bank_transfer_id IS NULL THEN NULL::text
           ELSE classifier_row.unsafe_reason
         END
  FROM public.banking_pay_operation_transfer_scope AS scope_row
  LEFT JOIN LATERAL (
    SELECT classifier_inner.*
    FROM public.pay_bank_transfer_execution_classify(
      p_pay_batch_id => v_operation_row.pay_batch_id,
      p_pay_channel_scope => scope_row.pay_channel,
      p_operation_id => p_operation_id,
      p_include_unscoped_transfers => true
    ) AS classifier_inner
    WHERE classifier_inner.scope_id = scope_row.id
       OR (
         scope_row.pay_bank_transfer_id IS NOT NULL
         AND classifier_inner.pay_bank_transfer_id = scope_row.pay_bank_transfer_id
       )
    ORDER BY
      CASE WHEN classifier_inner.scope_id = scope_row.id THEN 0 ELSE 1 END,
      CASE WHEN classifier_inner.pay_bank_transfer_id = scope_row.pay_bank_transfer_id THEN 0 ELSE 1 END,
      classifier_inner.pay_bank_transfer_id NULLS LAST,
      classifier_inner.scope_id NULLS LAST
    LIMIT 1
  ) AS classifier_row ON true
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.pay_batch_id = v_operation_row.pay_batch_id;

  INSERT INTO pg_temp.tmp_pay_execute_cleanup_transfer_classified (
    pay_bank_transfer_id,
    pay_channel,
    transfer_group_key,
    payment_reference,
    request_id,
    classifier_found,
    is_safe_local_cleanup,
    has_provider_submission_evidence,
    has_provider_event_evidence,
    has_provider_attempt_without_external_id,
    has_operation_submit_attempt,
    has_ambiguous_external_evidence,
    is_failed_or_blocked,
    is_terminal_or_completed,
    evidence_classification,
    unsafe_reason
  )
  SELECT transfer_candidate.pay_bank_transfer_id,
         transfer_candidate.pay_channel,
         transfer_candidate.transfer_group_key,
         transfer_candidate.payment_reference,
         transfer_candidate.request_id,
         (classifier_row.pay_bank_transfer_id IS NOT NULL),
         COALESCE(classifier_row.is_safe_local_cleanup, false),
         COALESCE(classifier_row.has_provider_submission_evidence, false),
         COALESCE(classifier_row.has_provider_event_evidence, false),
         COALESCE(classifier_row.has_provider_attempt_without_external_id, false),
         COALESCE(classifier_row.has_operation_submit_attempt, false),
         COALESCE(classifier_row.has_ambiguous_external_evidence, false),
         COALESCE(classifier_row.is_failed_or_blocked, false),
         COALESCE(classifier_row.is_terminal_or_completed, false),
         classifier_row.evidence_classification,
         CASE
           WHEN v_batch_execution_boundary_crossed THEN 'BATCH_EXECUTION_BOUNDARY_CROSSED'
           WHEN classifier_row.pay_bank_transfer_id IS NULL THEN 'TRANSFER_CLASSIFICATION_MISSING'
           ELSE classifier_row.unsafe_reason
         END
  FROM (
    SELECT DISTINCT transfer_row.id AS pay_bank_transfer_id,
           transfer_row.pay_channel AS pay_channel,
           transfer_row.transfer_group_key AS transfer_group_key,
           transfer_row.payment_reference AS payment_reference,
           transfer_row.request_id AS request_id
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.pay_batch_id = v_operation_row.pay_batch_id
      AND (
        EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_transfer_scope AS linked_scope
          WHERE linked_scope.operation_id = p_operation_id
            AND linked_scope.pay_batch_id = v_operation_row.pay_batch_id
            AND linked_scope.pay_bank_transfer_id = transfer_row.id
        )
        OR BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{operation_id}', '')) = p_operation_id::text
        OR BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{created_by_operation_id}', '')) = p_operation_id::text
        OR BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_execute_operation_id}', '')) = p_operation_id::text
        OR transfer_row.request_id LIKE ('op:' || p_operation_id::text || ':%')
        OR BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{request_id}', '')) LIKE ('op:' || p_operation_id::text || ':%')
        OR BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')) LIKE ('op:' || p_operation_id::text || ':%')
      )
  ) AS transfer_candidate
  LEFT JOIN LATERAL (
    SELECT classifier_inner.*
    FROM public.pay_bank_transfer_execution_classify(
      p_pay_batch_id => v_operation_row.pay_batch_id,
      p_pay_channel_scope => transfer_candidate.pay_channel,
      p_operation_id => p_operation_id,
      p_include_unscoped_transfers => true
    ) AS classifier_inner
    WHERE classifier_inner.pay_bank_transfer_id = transfer_candidate.pay_bank_transfer_id
    ORDER BY
      CASE WHEN classifier_inner.scope_operation_id = p_operation_id THEN 0 ELSE 1 END,
      classifier_inner.scope_id NULLS LAST,
      classifier_inner.pay_bank_transfer_id
    LIMIT 1
  ) AS classifier_row ON true;

  UPDATE pg_temp.tmp_pay_execute_cleanup_transfer_classified AS classified_transfer_update
  SET has_provider_event_evidence = true,
      is_safe_local_cleanup = false,
      evidence_classification = CASE
        WHEN NULLIF(BTRIM(COALESCE(classified_transfer_update.evidence_classification, '')), '') IS NULL
          OR classified_transfer_update.evidence_classification IN ('local_only_evidence', 'pending', 'unclassified_local') THEN 'provider_evidence_present'
        ELSE classified_transfer_update.evidence_classification
      END,
      unsafe_reason = CASE
        WHEN NULLIF(BTRIM(COALESCE(classified_transfer_update.unsafe_reason, '')), '') IS NULL
          OR classified_transfer_update.unsafe_reason = 'TRANSFER_CLASSIFICATION_MISSING' THEN 'UNLINKED_PROVIDER_EVENT_EVIDENCE_PRESENT'
        ELSE classified_transfer_update.unsafe_reason
      END
  FROM public.pay_bank_transfers AS transfer_row
  WHERE transfer_row.id = classified_transfer_update.pay_bank_transfer_id
    AND transfer_row.pay_batch_id = v_operation_row.pay_batch_id
    AND EXISTS (
      SELECT 1
      FROM public.pay_bank_transfer_events AS transfer_event
      WHERE transfer_event.pay_batch_id = v_operation_row.pay_batch_id
        AND upper(BTRIM(COALESCE(transfer_event.event_source, ''))) IN (
          'PROVIDER_RESPONSE',
          'PROVIDER_POLL',
          'PROVIDER_WEBHOOK',
          'WEBHOOK',
          'POLL',
          'RAIL_PROVIDER',
          'PROVIDER',
          'PROVIDER_SETTLEMENT',
          'RAIL_PROVIDER_SETTLEMENT'
        )
        AND EXISTS (
          SELECT 1
          FROM (VALUES
            (transfer_event.provider_event_id),
            (transfer_event.provider_reference),
            (transfer_event.raw_payload #>> '{provider_event_id}'),
            (transfer_event.raw_payload #>> '{provider_reference}'),
            (transfer_event.raw_payload #>> '{provider_submission_id}'),
            (transfer_event.raw_payload #>> '{submission_id}'),
            (transfer_event.raw_payload #>> '{rail_submission_id}'),
            (transfer_event.raw_payload #>> '{provider_payment_id}'),
            (transfer_event.raw_payload #>> '{payment_id}'),
            (transfer_event.raw_payload #>> '{external_payment_id}'),
            (transfer_event.raw_payload #>> '{revolut_payment_id}'),
            (transfer_event.raw_payload #>> '{provider_transfer_id}'),
            (transfer_event.raw_payload #>> '{transfer_id}'),
            (transfer_event.raw_payload #>> '{external_transfer_id}'),
            (transfer_event.raw_payload #>> '{provider_transaction_id}'),
            (transfer_event.raw_payload #>> '{transaction_id}')
          ) AS provider_event_identifier(identifier_value)
          WHERE NULLIF(BTRIM(COALESCE(provider_event_identifier.identifier_value, '')), '') IS NOT NULL
            AND NOT (
              NULLIF(BTRIM(COALESCE(provider_event_identifier.identifier_value, '')), '') = ANY(
                ARRAY_REMOVE(ARRAY[
                  transfer_row.id::text,
                  NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''),
                  NULLIF(BTRIM(COALESCE(v_batch_row.bulk_reference, '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{local_provider_request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,local_provider_request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{bulk_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{idempotency_key}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{local_provider_request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,idempotency_key}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,local_provider_request_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{payment_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{bulk_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_event.idempotency_key, '')), '')
                ]::text[], NULL::text)
              )
            )
            AND (
              transfer_event.pay_bank_transfer_id = transfer_row.id
              OR NULLIF(BTRIM(COALESCE(provider_event_identifier.identifier_value, '')), '') = ANY(
                ARRAY_REMOVE(ARRAY[
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_event_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_reference}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submission_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{submission_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{rail_submission_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_payment_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{external_payment_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{revolut_payment_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_transfer_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{external_transfer_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_transaction_id}', '')), ''),
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{transaction_id}', '')), '')
                ]::text[], NULL::text)
              )
            )
        )
    );

  SELECT COUNT(*)::integer
  INTO v_scope_rows_considered
  FROM pg_temp.tmp_pay_execute_cleanup_scope_classified AS classified_scope;

  SELECT COUNT(*)::integer
  INTO v_transfer_rows_considered
  FROM pg_temp.tmp_pay_execute_cleanup_transfer_classified AS classified_transfer;

  SELECT COUNT(*)::integer
  INTO v_provider_evidence_count
  FROM pg_temp.tmp_pay_execute_cleanup_transfer_classified AS classified_transfer
  WHERE classified_transfer.has_provider_submission_evidence
     OR classified_transfer.has_provider_event_evidence;

  SELECT COUNT(*)::integer
  INTO v_provider_review_risk_count
  FROM pg_temp.tmp_pay_execute_cleanup_transfer_classified AS classified_transfer
  WHERE classified_transfer.has_provider_submission_evidence
     OR classified_transfer.has_provider_event_evidence
     OR classified_transfer.has_provider_attempt_without_external_id
     OR classified_transfer.has_ambiguous_external_evidence;

  v_provider_evidence_count := COALESCE(v_provider_acceptance_evidence_count, 0);
  v_provider_review_risk_count := CASE
    WHEN COALESCE(v_provider_submit_requires_manual_review, false) IS TRUE THEN
      COALESCE(v_provider_acceptance_evidence_count, 0)
      + COALESCE(v_provider_response_present_count, 0)
      + COALESCE(v_provider_request_sent_count, 0)
      + COALESCE(v_provider_submission_unknown_count, 0)
      + COALESCE(v_stale_unresolved_submit_chunk_count, 0)
      + COALESCE(v_unfinalised_submit_chunk_count, 0)
    ELSE 0
  END;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_execute_cleanup_auth_requests;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_execute_cleanup_auth_requests (
    auth_request_id uuid PRIMARY KEY,
    auth_request_state text NOT NULL,
    auth_request_operation_id uuid,
    owner_operation_status text,
    is_same_operation boolean NOT NULL DEFAULT false,
    is_other_operation boolean NOT NULL DEFAULT false,
    has_auth_request_provider_risk boolean NOT NULL DEFAULT false,
    is_cancellable_local_auth_request boolean NOT NULL DEFAULT false,
    is_non_cancellable_auth_request boolean NOT NULL DEFAULT false,
    unsafe_reason text
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_pay_execute_cleanup_auth_requests (
    auth_request_id,
    auth_request_state,
    auth_request_operation_id,
    owner_operation_status,
    is_same_operation,
    is_other_operation,
    has_auth_request_provider_risk,
    is_cancellable_local_auth_request,
    is_non_cancellable_auth_request,
    unsafe_reason
  )
  SELECT auth_request.id,
         upper(btrim(coalesce(auth_request.state, ''))) AS auth_request_state,
         CASE
           WHEN auth_intent.auth_operation_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
             THEN auth_intent.auth_operation_id_text::uuid
           ELSE NULL::uuid
         END AS auth_request_operation_id,
         upper(btrim(coalesce(owner_operation.status, ''))) AS owner_operation_status,
         (
           auth_intent.auth_operation_id_text = p_operation_id::text
         ) AS is_same_operation,
         (
           auth_intent.auth_operation_id_text IS NULL
           OR auth_intent.auth_operation_id_text <> p_operation_id::text
         ) AS is_other_operation,
         (
           v_batch_execution_boundary_crossed IS TRUE
           OR COALESCE(v_provider_review_risk_count, 0) > 0
         ) AS has_auth_request_provider_risk,
         COALESCE((
           upper(btrim(coalesce(auth_request.state, ''))) IN ('AWAITING', 'AUTHORISED')
           AND v_batch_execution_boundary_crossed IS NOT TRUE
           AND COALESCE(v_provider_review_risk_count, 0) = 0
           AND (
             auth_intent.auth_operation_id_text = p_operation_id::text
             OR (
               auth_intent.auth_operation_id_text IS NOT NULL
               AND auth_intent.auth_operation_id_text <> p_operation_id::text
               AND upper(btrim(coalesce(owner_operation.status, ''))) IN ('FAILED', 'CANCELLED', 'CANCELED')
             )
           )
         ), false) AS is_cancellable_local_auth_request,
         NOT COALESCE((
           upper(btrim(coalesce(auth_request.state, ''))) IN ('AWAITING', 'AUTHORISED')
           AND v_batch_execution_boundary_crossed IS NOT TRUE
           AND COALESCE(v_provider_review_risk_count, 0) = 0
           AND (
             auth_intent.auth_operation_id_text = p_operation_id::text
             OR (
               auth_intent.auth_operation_id_text IS NOT NULL
               AND auth_intent.auth_operation_id_text <> p_operation_id::text
               AND upper(btrim(coalesce(owner_operation.status, ''))) IN ('FAILED', 'CANCELLED', 'CANCELED')
             )
           )
         ), false) AS is_non_cancellable_auth_request,
         CASE
           WHEN v_batch_execution_boundary_crossed THEN 'AUTH_REQUEST_BATCH_EXECUTION_BOUNDARY_CROSSED'
           WHEN COALESCE(v_provider_evidence_count, 0) > 0 THEN 'AUTH_REQUEST_PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT'
           WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' OR COALESCE(v_stale_empty_provider_submit_chunk_count, 0) > 0 THEN 'AUTH_REQUEST_PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
           WHEN COALESCE(v_provider_submission_status, '') = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' OR COALESCE(v_provider_submit_unknown_chunk_count, 0) > 0 THEN 'AUTH_REQUEST_PROVIDER_SUBMISSION_UNKNOWN'
           WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'AUTH_REQUEST_PROVIDER_SUBMISSION_MALFORMED_RESPONSE'
           WHEN upper(btrim(coalesce(auth_request.state, ''))) = 'PENDING_AUTHORISATION' THEN 'AUTH_REQUEST_PENDING_AUTHORISATION'
           WHEN auth_intent.auth_operation_id_text IS NULL THEN 'AUTH_REQUEST_OPERATION_ID_MISSING'
           WHEN auth_intent.auth_operation_id_text <> p_operation_id::text
             AND upper(btrim(coalesce(owner_operation.status, ''))) IN ('FAILED', 'CANCELLED', 'CANCELED') THEN NULL::text
           WHEN auth_intent.auth_operation_id_text <> p_operation_id::text THEN 'AUTH_REQUEST_OWNED_BY_DIFFERENT_OPERATION'
           ELSE NULL::text
         END AS unsafe_reason
  FROM public.pay_batch_auth_requests AS auth_request
  LEFT JOIN LATERAL (
    SELECT nullif(btrim(coalesce(auth_request.execution_intent_json->>'operation_id', '')), '') AS auth_operation_id_text
  ) AS auth_intent ON true
  LEFT JOIN public.banking_pay_operations AS owner_operation
    ON owner_operation.id = CASE
      WHEN auth_intent.auth_operation_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN auth_intent.auth_operation_id_text::uuid
      ELSE NULL::uuid
    END
  WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
    AND upper(btrim(coalesce(auth_request.state, ''))) IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

  SELECT COUNT(*) FILTER (WHERE auth_classification.is_cancellable_local_auth_request)::integer,
         COUNT(*) FILTER (WHERE auth_classification.is_non_cancellable_auth_request)::integer,
         COUNT(*) FILTER (WHERE auth_classification.auth_request_state = 'AUTHORISED' AND auth_classification.is_non_cancellable_auth_request)::integer
  INTO v_cancellable_auth_request_count,
       v_non_cancellable_auth_request_count,
       v_authorised_auth_request_review_count
  FROM pg_temp.tmp_pay_execute_cleanup_auth_requests AS auth_classification;

  SELECT COUNT(*) FILTER (WHERE auth_classification.is_cancellable_local_auth_request AND auth_classification.auth_request_state = 'AWAITING')::integer,
         COUNT(*) FILTER (WHERE auth_classification.is_cancellable_local_auth_request AND auth_classification.auth_request_state = 'AUTHORISED')::integer
  INTO v_awaiting_auth_requests_cancelled,
       v_authorised_auth_requests_cancelled
  FROM pg_temp.tmp_pay_execute_cleanup_auth_requests AS auth_classification;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_execute_cleanup_cancelled_auth_request_ids;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_execute_cleanup_cancelled_auth_request_ids (
    auth_request_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  IF COALESCE(p_dry_run, false) IS TRUE THEN
    INSERT INTO pg_temp.tmp_pay_execute_cleanup_cancelled_auth_request_ids (auth_request_id)
    SELECT auth_classification.auth_request_id
    FROM pg_temp.tmp_pay_execute_cleanup_auth_requests AS auth_classification
    WHERE auth_classification.is_cancellable_local_auth_request IS TRUE
    ON CONFLICT DO NOTHING;

    v_auth_requests_cancelled := COALESCE(v_cancellable_auth_request_count, 0);
  ELSE
    WITH cancellable_auth_requests AS (
      SELECT auth_classification.auth_request_id
      FROM pg_temp.tmp_pay_execute_cleanup_auth_requests AS auth_classification
      WHERE auth_classification.is_cancellable_local_auth_request IS TRUE
    ), cancelled_auth_requests AS (
      UPDATE public.pay_batch_auth_requests AS auth_request_update
      SET state = 'CANCELLED',
          finalised_at_utc = COALESCE(auth_request_update.finalised_at_utc, v_now),
          finalised_by_user_id = COALESCE(auth_request_update.finalised_by_user_id, v_effective_actor_user_id),
          execution_intent_json = jsonb_strip_nulls(
            COALESCE(auth_request_update.execution_intent_json, '{}'::jsonb)
            || jsonb_build_object(
              'cancelled_by_failed_execution_cleanup', true,
              'cleanup_operation_id', p_operation_id::text,
              'cleanup_at_utc', v_now::text,
              'failure_phase', v_failure_phase
            )
          )
      FROM cancellable_auth_requests
      WHERE auth_request_update.id = cancellable_auth_requests.auth_request_id
        AND auth_request_update.pay_batch_id = v_operation_row.pay_batch_id
        AND upper(btrim(coalesce(auth_request_update.state, ''))) IN ('AWAITING', 'AUTHORISED')
      RETURNING auth_request_update.id
    ), inserted_cancelled AS (
      INSERT INTO pg_temp.tmp_pay_execute_cleanup_cancelled_auth_request_ids (auth_request_id)
      SELECT cancelled_auth_requests.id
      FROM cancelled_auth_requests
      ON CONFLICT DO NOTHING
      RETURNING auth_request_id
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(to_jsonb(inserted_cancelled.auth_request_id::text) ORDER BY inserted_cancelled.auth_request_id), '[]'::jsonb)
    INTO v_auth_requests_cancelled,
         v_cancelled_auth_request_ids
    FROM inserted_cancelled;

    WITH voided_tokens AS (
      UPDATE public.pay_batch_auth_tokens AS auth_token_update
      SET used_at_utc = COALESCE(auth_token_update.used_at_utc, v_now),
          expires_at_utc = CASE
            WHEN auth_token_update.expires_at_utc > v_now THEN v_now
            ELSE auth_token_update.expires_at_utc
          END
      FROM pg_temp.tmp_pay_execute_cleanup_cancelled_auth_request_ids AS cancelled_auth_request
      WHERE auth_token_update.auth_request_id = cancelled_auth_request.auth_request_id
        AND (auth_token_update.used_at_utc IS NULL OR auth_token_update.expires_at_utc > v_now)
      RETURNING auth_token_update.token
    )
    SELECT COUNT(*)::integer
    INTO v_auth_tokens_voided
    FROM voided_tokens;

    WITH cleared_batch_intent AS (
      UPDATE public.pay_batches AS batch_update
      SET schedule_kind = NULL::text,
          scheduled_at_utc = NULL::timestamptz,
          scheduled_by_user_id = NULL::uuid,
          funding_account_ref = NULL::text,
          funds_warning_hours_json = NULL::jsonb,
          execution_intent_json = NULL::jsonb
      WHERE batch_update.id = v_operation_row.pay_batch_id
        AND v_batch_execution_boundary_crossed IS NOT TRUE
        AND COALESCE(v_provider_review_risk_count, 0) = 0
        AND (
          NULLIF(BTRIM(COALESCE(batch_update.execution_intent_json->>'operation_id', '')), '') = p_operation_id::text
          OR EXISTS (
            SELECT 1
            FROM pg_temp.tmp_pay_execute_cleanup_cancelled_auth_request_ids AS cancelled_auth_request
            WHERE NULLIF(BTRIM(COALESCE(batch_update.execution_intent_json->>'auth_request_id', '')), '') = cancelled_auth_request.auth_request_id::text
          )
        )
      RETURNING batch_update.id
    )
    SELECT COUNT(*)::integer
    INTO v_batch_execution_intent_cleared
    FROM cleared_batch_intent;
  END IF;

  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(to_jsonb(cancelled_auth_request.auth_request_id::text) ORDER BY cancelled_auth_request.auth_request_id), '[]'::jsonb)
  INTO v_auth_requests_cancelled,
       v_cancelled_auth_request_ids
  FROM pg_temp.tmp_pay_execute_cleanup_cancelled_auth_request_ids AS cancelled_auth_request;

  SELECT COUNT(*) FILTER (WHERE auth_classification.auth_request_state = 'AWAITING')::integer,
         COUNT(*) FILTER (WHERE auth_classification.auth_request_state = 'AUTHORISED')::integer
  INTO v_awaiting_auth_requests_cancelled,
       v_authorised_auth_requests_cancelled
  FROM pg_temp.tmp_pay_execute_cleanup_cancelled_auth_request_ids AS cancelled_auth_request
  JOIN pg_temp.tmp_pay_execute_cleanup_auth_requests AS auth_classification
    ON auth_classification.auth_request_id = cancelled_auth_request.auth_request_id;

  SELECT COUNT(*)::integer,
         (COUNT(*) FILTER (WHERE auth_request.execution_intent_json->>'operation_id' = p_operation_id::text))::integer,
         COALESCE(jsonb_agg(to_jsonb(auth_request.id::text) ORDER BY auth_request.created_at_utc NULLS LAST, auth_request.id), '[]'::jsonb)
  INTO v_active_auth_request_count,
       v_operation_active_auth_request_count,
       v_active_auth_request_ids
  FROM public.pay_batch_auth_requests AS auth_request
  WHERE auth_request.pay_batch_id = v_operation_row.pay_batch_id
    AND auth_request.state IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED');

  IF COALESCE(p_dry_run, false) IS TRUE THEN
    v_active_auth_request_blocker_count := COALESCE(v_non_cancellable_auth_request_count, 0);
  ELSE
    v_active_auth_request_blocker_count := COALESCE(v_active_auth_request_count, 0);
  END IF;

  v_deletion_allowed := (
    v_batch_execution_boundary_crossed IS NOT TRUE
    AND COALESCE(v_provider_review_risk_count, 0) = 0
    AND COALESCE(v_active_auth_request_blocker_count, 0) = 0
  );

  IF v_deletion_allowed IS TRUE THEN
    UPDATE pg_temp.tmp_pay_execute_cleanup_transfer_classified AS classified_transfer_update
    SET is_safe_local_cleanup = true,
        unsafe_reason = NULL::text
    WHERE classified_transfer_update.has_provider_submission_evidence IS NOT TRUE
      AND classified_transfer_update.has_provider_event_evidence IS NOT TRUE
      AND classified_transfer_update.has_provider_attempt_without_external_id IS NOT TRUE
      AND classified_transfer_update.has_ambiguous_external_evidence IS NOT TRUE
      AND classified_transfer_update.is_failed_or_blocked IS NOT TRUE
      AND classified_transfer_update.is_terminal_or_completed IS NOT TRUE;

    UPDATE pg_temp.tmp_pay_execute_cleanup_scope_classified AS classified_scope_update
    SET classifier_is_safe_local_cleanup = true,
        unsafe_reason = NULL::text
    WHERE (
        classified_scope_update.pay_bank_transfer_id IS NULL
        OR EXISTS (
          SELECT 1
          FROM pg_temp.tmp_pay_execute_cleanup_transfer_classified AS classified_transfer
          WHERE classified_transfer.pay_bank_transfer_id = classified_scope_update.pay_bank_transfer_id
            AND classified_transfer.is_safe_local_cleanup IS TRUE
        )
      )
      AND classified_scope_update.has_provider_submission_evidence IS NOT TRUE
      AND classified_scope_update.has_provider_event_evidence IS NOT TRUE
      AND classified_scope_update.has_provider_attempt_without_external_id IS NOT TRUE
      AND classified_scope_update.has_ambiguous_external_evidence IS NOT TRUE
      AND classified_scope_update.is_failed_or_blocked IS NOT TRUE
      AND classified_scope_update.is_terminal_or_completed IS NOT TRUE;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids (
    pay_bank_transfer_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids (pay_bank_transfer_id)
  SELECT classified_transfer.pay_bank_transfer_id
  FROM pg_temp.tmp_pay_execute_cleanup_transfer_classified AS classified_transfer
  JOIN public.pay_bank_transfers AS transfer_row
    ON transfer_row.id = classified_transfer.pay_bank_transfer_id
   AND transfer_row.pay_batch_id = v_operation_row.pay_batch_id
  WHERE v_deletion_allowed IS TRUE
    AND classified_transfer.is_safe_local_cleanup IS TRUE
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_bank_transfer_events AS transfer_event
      WHERE transfer_event.pay_batch_id = v_operation_row.pay_batch_id
        AND (
          transfer_event.pay_bank_transfer_id = transfer_row.id
          OR (
            transfer_event.pay_bank_transfer_id IS NULL
            AND (
              NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') = ANY(ARRAY_REMOVE(ARRAY[
                transfer_row.id::text,
                NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{request_id}', '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_reference}', '')), '')
              ]::text[], NULL::text))
              OR NULLIF(BTRIM(COALESCE(transfer_event.provider_event_id, '')), '') = ANY(ARRAY_REMOVE(ARRAY[
                transfer_row.id::text,
                NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{request_id}', '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_reference}', '')), '')
              ]::text[], NULL::text))
              OR NULLIF(BTRIM(COALESCE(transfer_event.idempotency_key, '')), '') = ANY(ARRAY_REMOVE(ARRAY[
                transfer_row.id::text,
                NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{request_id}', '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), ''),
                NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_reference}', '')), '')
              ]::text[], NULL::text))
            )
          )
          OR EXISTS (
            SELECT 1
            FROM (VALUES
              (transfer_row.id::text),
              (NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '')),
              (NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), '')),
              (NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), '')),
              (NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{request_id}', '')), '')),
              (NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), '')),
              (NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_reference}', '')), ''))
            ) AS provider_event_identity(identity_value)
            WHERE LENGTH(NULLIF(BTRIM(COALESCE(provider_event_identity.identity_value, '')), '')) >= 8
              AND POSITION(lower(NULLIF(BTRIM(COALESCE(provider_event_identity.identity_value, '')), '')) IN lower(COALESCE(transfer_event.raw_payload::text, ''))) > 0
          )
        )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_chunks AS operation_chunk
      JOIN public.banking_pay_operations AS chunk_operation
        ON chunk_operation.id = operation_chunk.operation_id
      WHERE chunk_operation.pay_batch_id = v_operation_row.pay_batch_id
        AND (
          operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS'
          OR operation_chunk.chunk_type = 'TRANSFER_SUBMIT'
        )
        AND (
          operation_chunk.status IN ('RUNNING', 'COMPLETE', 'FAILED')
          OR operation_chunk.started_at_utc IS NOT NULL
          OR (operation_chunk.result_json IS NOT NULL AND operation_chunk.result_json <> '{}'::jsonb)
          OR (operation_chunk.error_json IS NOT NULL AND operation_chunk.error_json <> '{}'::jsonb)
        )
        AND POSITION(lower(transfer_row.id::text) IN lower(COALESCE(operation_chunk.payload_json::text, '') || COALESCE(operation_chunk.result_json::text, '') || COALESCE(operation_chunk.error_json::text, ''))) > 0
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_transfer_scope AS other_scope
      WHERE other_scope.pay_bank_transfer_id = transfer_row.id
        AND other_scope.operation_id <> p_operation_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS correction_item
      WHERE correction_item.pay_bank_transfer_id = transfer_row.id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS correction_work_item
      WHERE correction_work_item.pay_bank_transfer_id = transfer_row.id
    );

  DROP TABLE IF EXISTS pg_temp.tmp_pay_execute_cleanup_safe_scope_ids;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_execute_cleanup_safe_scope_ids (
    scope_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_pay_execute_cleanup_safe_scope_ids (scope_id)
  SELECT classified_scope.scope_id
  FROM pg_temp.tmp_pay_execute_cleanup_scope_classified AS classified_scope
  WHERE v_deletion_allowed IS TRUE
    AND (
      classified_scope.pay_bank_transfer_id IS NULL
      OR EXISTS (
        SELECT 1
        FROM pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids AS safe_transfer
        WHERE safe_transfer.pay_bank_transfer_id = classified_scope.pay_bank_transfer_id
      )
    );

  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(to_jsonb(safe_scope.scope_id::text) ORDER BY safe_scope.scope_id), '[]'::jsonb)
  INTO v_safe_scope_candidate_count,
       v_safe_scope_ids
  FROM pg_temp.tmp_pay_execute_cleanup_safe_scope_ids AS safe_scope;

  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(to_jsonb(safe_transfer.pay_bank_transfer_id::text) ORDER BY safe_transfer.pay_bank_transfer_id), '[]'::jsonb)
  INTO v_safe_transfer_candidate_count,
       v_safe_transfer_ids
  FROM pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids AS safe_transfer;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_execute_cleanup_unsafe_artifacts;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_execute_cleanup_unsafe_artifacts (
    scope_id uuid,
    pay_bank_transfer_id uuid,
    pay_channel text,
    transfer_group_key text,
    unsafe_reason text
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_pay_execute_cleanup_unsafe_artifacts (
    scope_id,
    pay_bank_transfer_id,
    pay_channel,
    transfer_group_key,
    unsafe_reason
  )
  SELECT classified_scope.scope_id,
         classified_scope.pay_bank_transfer_id,
         classified_scope.pay_channel,
         classified_scope.transfer_group_key,
         COALESCE(
           CASE WHEN v_batch_execution_boundary_crossed THEN 'BATCH_EXECUTION_BOUNDARY_CROSSED' ELSE NULL::text END,
           CASE
             WHEN COALESCE(v_provider_evidence_count, 0) > 0 THEN 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT'
             WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' OR COALESCE(v_stale_empty_provider_submit_chunk_count, 0) > 0 THEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
             WHEN COALESCE(v_provider_submission_status, '') = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' OR COALESCE(v_provider_submit_unknown_chunk_count, 0) > 0 THEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME'
             WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE'
             ELSE NULL::text
           END,
           CASE WHEN COALESCE(v_active_auth_request_blocker_count, 0) > 0 THEN 'ACTIVE_AUTH_REQUEST_PRESENT' ELSE NULL::text END,
           CASE WHEN classified_scope.has_provider_event_evidence THEN 'PROVIDER_EVENT_EVIDENCE_PRESENT' ELSE NULL::text END,
           CASE WHEN classified_scope.has_provider_submission_evidence THEN 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT' ELSE NULL::text END,
           CASE WHEN classified_scope.has_provider_attempt_without_external_id THEN 'PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID' ELSE NULL::text END,
           CASE WHEN classified_scope.has_ambiguous_external_evidence THEN COALESCE(NULLIF(BTRIM(COALESCE(classified_scope.unsafe_reason, '')), ''), 'PROVIDER_SUBMISSION_UNKNOWN') ELSE NULL::text END,
           CASE WHEN classified_scope.is_terminal_or_completed THEN 'TERMINAL_OR_COMPLETED_TRANSFER_STATE' ELSE NULL::text END,
           CASE WHEN classified_scope.is_failed_or_blocked THEN 'FAILED_OR_BLOCKED_TRANSFER_STATE' ELSE NULL::text END,
           classified_scope.unsafe_reason,
           'UNSAFE_LOCAL_SCOPE_CLEANUP_NOT_CONFIRMED'
         )
  FROM pg_temp.tmp_pay_execute_cleanup_scope_classified AS classified_scope
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_execute_cleanup_safe_scope_ids AS safe_scope
    WHERE safe_scope.scope_id = classified_scope.scope_id
  );

  INSERT INTO pg_temp.tmp_pay_execute_cleanup_unsafe_artifacts (
    scope_id,
    pay_bank_transfer_id,
    pay_channel,
    transfer_group_key,
    unsafe_reason
  )
  SELECT NULL::uuid,
         classified_transfer.pay_bank_transfer_id,
         classified_transfer.pay_channel,
         classified_transfer.transfer_group_key,
         COALESCE(
           CASE WHEN v_batch_execution_boundary_crossed THEN 'BATCH_EXECUTION_BOUNDARY_CROSSED' ELSE NULL::text END,
           CASE
             WHEN COALESCE(v_provider_evidence_count, 0) > 0 THEN 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT'
             WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' OR COALESCE(v_stale_empty_provider_submit_chunk_count, 0) > 0 THEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
             WHEN COALESCE(v_provider_submission_status, '') = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' OR COALESCE(v_provider_submit_unknown_chunk_count, 0) > 0 THEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME'
             WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE'
             ELSE NULL::text
           END,
           CASE WHEN COALESCE(v_active_auth_request_blocker_count, 0) > 0 THEN 'ACTIVE_AUTH_REQUEST_PRESENT' ELSE NULL::text END,
           CASE WHEN classified_transfer.has_provider_event_evidence THEN 'PROVIDER_EVENT_EVIDENCE_PRESENT' ELSE NULL::text END,
           CASE WHEN classified_transfer.has_provider_submission_evidence THEN 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT' ELSE NULL::text END,
           CASE WHEN classified_transfer.has_provider_attempt_without_external_id THEN 'PROVIDER_ATTEMPT_WITHOUT_EXTERNAL_ID' ELSE NULL::text END,
           CASE WHEN classified_transfer.has_ambiguous_external_evidence THEN COALESCE(NULLIF(BTRIM(COALESCE(classified_transfer.unsafe_reason, '')), ''), 'PROVIDER_SUBMISSION_UNKNOWN') ELSE NULL::text END,
           CASE WHEN classified_transfer.is_terminal_or_completed THEN 'TERMINAL_OR_COMPLETED_TRANSFER_STATE' ELSE NULL::text END,
           CASE WHEN classified_transfer.is_failed_or_blocked THEN 'FAILED_OR_BLOCKED_TRANSFER_STATE' ELSE NULL::text END,
           CASE
             WHEN EXISTS (
               SELECT 1
               FROM public.pay_payment_correction_items AS correction_item
               WHERE correction_item.pay_bank_transfer_id = classified_transfer.pay_bank_transfer_id
             ) THEN 'PAYMENT_CORRECTION_REFERENCE_PRESENT'
             ELSE NULL::text
           END,
           CASE
             WHEN EXISTS (
               SELECT 1
               FROM public.pay_payment_correction_work_items AS correction_work_item
               WHERE correction_work_item.pay_bank_transfer_id = classified_transfer.pay_bank_transfer_id
             ) THEN 'PAYMENT_CORRECTION_WORK_REFERENCE_PRESENT'
             ELSE NULL::text
           END,
           classified_transfer.unsafe_reason,
           'UNSAFE_LOCAL_TRANSFER_CLEANUP_NOT_CONFIRMED'
         )
  FROM pg_temp.tmp_pay_execute_cleanup_transfer_classified AS classified_transfer
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids AS safe_transfer
    WHERE safe_transfer.pay_bank_transfer_id = classified_transfer.pay_bank_transfer_id
  );

  SELECT COALESCE((COUNT(DISTINCT unsafe_artifact.pay_bank_transfer_id) FILTER (WHERE unsafe_artifact.pay_bank_transfer_id IS NOT NULL)), 0)::integer,
         COALESCE((COUNT(DISTINCT unsafe_artifact.scope_id) FILTER (WHERE unsafe_artifact.pay_bank_transfer_id IS NULL)), 0)::integer
  INTO v_unsafe_transfer_count,
       v_unsafe_scope_count
  FROM pg_temp.tmp_pay_execute_cleanup_unsafe_artifacts AS unsafe_artifact;

  SELECT COALESCE(jsonb_agg(to_jsonb(unsafe_transfer_ids.pay_bank_transfer_id::text) ORDER BY unsafe_transfer_ids.pay_bank_transfer_id), '[]'::jsonb)
  INTO v_unsafe_transfer_ids
  FROM (
    SELECT DISTINCT unsafe_artifact.pay_bank_transfer_id
    FROM pg_temp.tmp_pay_execute_cleanup_unsafe_artifacts AS unsafe_artifact
    WHERE unsafe_artifact.pay_bank_transfer_id IS NOT NULL
  ) AS unsafe_transfer_ids;

  SELECT COALESCE(jsonb_agg(
           jsonb_strip_nulls(jsonb_build_object(
             'scope_id', CASE WHEN unsafe_artifact.scope_id IS NULL THEN NULL ELSE unsafe_artifact.scope_id::text END,
             'pay_bank_transfer_id', CASE WHEN unsafe_artifact.pay_bank_transfer_id IS NULL THEN NULL ELSE unsafe_artifact.pay_bank_transfer_id::text END,
             'pay_channel', unsafe_artifact.pay_channel,
             'transfer_group_key', unsafe_artifact.transfer_group_key,
             'reason', unsafe_artifact.unsafe_reason
           ))
           ORDER BY unsafe_artifact.pay_channel NULLS LAST,
                    unsafe_artifact.transfer_group_key NULLS LAST,
                    unsafe_artifact.pay_bank_transfer_id NULLS LAST,
                    unsafe_artifact.scope_id NULLS LAST
         ), '[]'::jsonb)
  INTO v_unsafe_reasons
  FROM pg_temp.tmp_pay_execute_cleanup_unsafe_artifacts AS unsafe_artifact;

  IF COALESCE(p_dry_run, false) IS TRUE THEN
    SELECT COUNT(*)::integer,
           COALESCE((COUNT(*) FILTER (
             WHERE item_row.bank_reference = transfer_row.payment_reference
               AND NULLIF(BTRIM(COALESCE(item_row.bank_reference, '')), '') IS NOT NULL
           )), 0)::integer
    INTO v_item_links_cleared,
         v_bank_references_cleared
    FROM public.pay_batch_items AS item_row
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = item_row.pay_batch_candidate_id
     AND batch_candidate.pay_batch_id = v_operation_row.pay_batch_id
    JOIN pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids AS safe_transfer
      ON safe_transfer.pay_bank_transfer_id = item_row.pay_bank_transfer_id
    JOIN public.pay_bank_transfers AS transfer_row
      ON transfer_row.id = safe_transfer.pay_bank_transfer_id;

    SELECT COALESCE((COUNT(*) FILTER (WHERE operation_chunk.status = 'RUNNING')), 0)::integer,
           COALESCE((COUNT(*) FILTER (WHERE operation_chunk.status = 'PENDING')), 0)::integer,
           COALESCE((COUNT(*) FILTER (WHERE operation_chunk.locked_by IS NOT NULL OR operation_chunk.lock_expires_at_utc IS NOT NULL)), 0)::integer
    INTO v_chunks_marked_failed,
         v_chunks_marked_skipped,
         v_locks_released
    FROM public.banking_pay_operation_chunks AS operation_chunk
    WHERE operation_chunk.operation_id = p_operation_id
      AND operation_chunk.status IN ('PENDING', 'RUNNING')
      AND NOT (
        operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS'
        OR operation_chunk.chunk_type = 'TRANSFER_SUBMIT'
      );

    v_scope_rows_deleted := v_safe_scope_candidate_count;
    v_transfer_rows_deleted := v_safe_transfer_candidate_count;
  ELSE
    WITH item_rows_to_clear AS (
      SELECT item_row.id AS pay_batch_item_id,
             item_row.bank_reference AS previous_bank_reference,
             transfer_row.payment_reference AS transfer_payment_reference
      FROM public.pay_batch_items AS item_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = item_row.pay_batch_candidate_id
       AND batch_candidate.pay_batch_id = v_operation_row.pay_batch_id
      JOIN pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids AS safe_transfer
        ON safe_transfer.pay_bank_transfer_id = item_row.pay_bank_transfer_id
      JOIN public.pay_bank_transfers AS transfer_row
        ON transfer_row.id = safe_transfer.pay_bank_transfer_id
    ), cleared_item_links AS (
      UPDATE public.pay_batch_items AS item_update
      SET pay_bank_transfer_id = NULL,
          bank_reference = CASE
            WHEN item_update.bank_reference = item_rows_to_clear.transfer_payment_reference THEN NULL
            ELSE item_update.bank_reference
          END,
          updated_at = v_now
      FROM item_rows_to_clear
      WHERE item_update.id = item_rows_to_clear.pay_batch_item_id
      RETURNING item_update.id,
                item_rows_to_clear.previous_bank_reference,
                item_rows_to_clear.transfer_payment_reference
    )
    SELECT COUNT(*)::integer,
           COALESCE((COUNT(*) FILTER (
             WHERE cleared_item_links.previous_bank_reference = cleared_item_links.transfer_payment_reference
               AND NULLIF(BTRIM(COALESCE(cleared_item_links.previous_bank_reference, '')), '') IS NOT NULL
           )), 0)::integer
    INTO v_item_links_cleared,
         v_bank_references_cleared
    FROM cleared_item_links;

    WITH deleted_scope_rows AS (
      DELETE FROM public.banking_pay_operation_transfer_scope AS scope_delete
      USING pg_temp.tmp_pay_execute_cleanup_safe_scope_ids AS safe_scope
      WHERE scope_delete.id = safe_scope.scope_id
        AND scope_delete.operation_id = p_operation_id
        AND scope_delete.pay_batch_id = v_operation_row.pay_batch_id
      RETURNING scope_delete.id
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(to_jsonb(deleted_scope_rows.id::text) ORDER BY deleted_scope_rows.id), '[]'::jsonb)
    INTO v_scope_rows_deleted,
         v_deleted_scope_ids
    FROM deleted_scope_rows;

    WITH deleted_transfer_rows AS (
      DELETE FROM public.pay_bank_transfers AS transfer_delete
      USING pg_temp.tmp_pay_execute_cleanup_safe_transfer_ids AS safe_transfer
      WHERE transfer_delete.id = safe_transfer.pay_bank_transfer_id
        AND transfer_delete.pay_batch_id = v_operation_row.pay_batch_id
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_transfer_scope AS remaining_scope
          WHERE remaining_scope.pay_bank_transfer_id = transfer_delete.id
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_batch_items AS remaining_item
          WHERE remaining_item.pay_bank_transfer_id = transfer_delete.id
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS transfer_event
          WHERE transfer_event.pay_bank_transfer_id = transfer_delete.id
             OR (
               transfer_event.pay_batch_id = v_operation_row.pay_batch_id
               AND transfer_event.pay_bank_transfer_id IS NULL
               AND (
                 NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') = ANY(ARRAY_REMOVE(ARRAY[
                   transfer_delete.id::text,
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_tx_id, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.request_id, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.payment_reference, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{request_id}', '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{idempotency_key}', '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{payment_reference}', '')), '')
                 ]::text[], NULL::text))
                 OR NULLIF(BTRIM(COALESCE(transfer_event.provider_event_id, '')), '') = ANY(ARRAY_REMOVE(ARRAY[
                   transfer_delete.id::text,
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_tx_id, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.request_id, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.payment_reference, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{request_id}', '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{idempotency_key}', '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{payment_reference}', '')), '')
                 ]::text[], NULL::text))
                 OR NULLIF(BTRIM(COALESCE(transfer_event.idempotency_key, '')), '') = ANY(ARRAY_REMOVE(ARRAY[
                   transfer_delete.id::text,
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_tx_id, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.request_id, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.payment_reference, '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{request_id}', '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{idempotency_key}', '')), ''),
                   NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{payment_reference}', '')), '')
                 ]::text[], NULL::text))
               )
             )
             OR (
               transfer_event.pay_batch_id = v_operation_row.pay_batch_id
               AND EXISTS (
                 SELECT 1
                 FROM (VALUES
                   (transfer_delete.id::text),
                   (NULLIF(BTRIM(COALESCE(transfer_delete.rail_tx_id, '')), '')),
                   (NULLIF(BTRIM(COALESCE(transfer_delete.request_id, '')), '')),
                   (NULLIF(BTRIM(COALESCE(transfer_delete.payment_reference, '')), '')),
                   (NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{request_id}', '')), '')),
                   (NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{idempotency_key}', '')), '')),
                   (NULLIF(BTRIM(COALESCE(transfer_delete.rail_meta_json #>> '{payment_reference}', '')), ''))
                 ) AS provider_event_identity(identity_value)
                 WHERE LENGTH(NULLIF(BTRIM(COALESCE(provider_event_identity.identity_value, '')), '')) >= 8
                   AND POSITION(lower(NULLIF(BTRIM(COALESCE(provider_event_identity.identity_value, '')), '')) IN lower(COALESCE(transfer_event.raw_payload::text, ''))) > 0
               )
             )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_chunks AS operation_chunk
          JOIN public.banking_pay_operations AS chunk_operation
            ON chunk_operation.id = operation_chunk.operation_id
          WHERE chunk_operation.pay_batch_id = v_operation_row.pay_batch_id
            AND (
              operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS'
              OR operation_chunk.chunk_type = 'TRANSFER_SUBMIT'
            )
            AND (
              operation_chunk.status IN ('RUNNING', 'COMPLETE', 'FAILED')
              OR operation_chunk.started_at_utc IS NOT NULL
              OR (operation_chunk.result_json IS NOT NULL AND operation_chunk.result_json <> '{}'::jsonb)
              OR (operation_chunk.error_json IS NOT NULL AND operation_chunk.error_json <> '{}'::jsonb)
            )
            AND POSITION(lower(transfer_delete.id::text) IN lower(COALESCE(operation_chunk.payload_json::text, '') || COALESCE(operation_chunk.result_json::text, '') || COALESCE(operation_chunk.error_json::text, ''))) > 0
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_items AS correction_item
          WHERE correction_item.pay_bank_transfer_id = transfer_delete.id
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_work_items AS correction_work_item
          WHERE correction_work_item.pay_bank_transfer_id = transfer_delete.id
        )
      RETURNING transfer_delete.id
    )
    SELECT COUNT(*)::integer,
           COALESCE(jsonb_agg(to_jsonb(deleted_transfer_rows.id::text) ORDER BY deleted_transfer_rows.id), '[]'::jsonb)
    INTO v_transfer_rows_deleted,
         v_deleted_transfer_ids
    FROM deleted_transfer_rows;

    WITH chunks_to_mutate AS (
      SELECT operation_chunk.id AS operation_chunk_id,
             operation_chunk.status AS previous_status,
             (operation_chunk.locked_by IS NOT NULL OR operation_chunk.lock_expires_at_utc IS NOT NULL) AS had_lock
      FROM public.banking_pay_operation_chunks AS operation_chunk
      WHERE operation_chunk.operation_id = p_operation_id
        AND operation_chunk.status IN ('PENDING', 'RUNNING')
        AND NOT (
          operation_chunk.phase IN ('SUBMIT_PROVIDER_TRANSFERS', 'APPLY_RAIL_UPDATES', 'APPLY_SETTLEMENT_CHUNKS', 'QUEUE_REMITTANCE_CHUNKS', 'QUEUE_PAYOUT_NOTICE_CHUNKS')
          OR operation_chunk.chunk_type IN ('TRANSFER_SUBMIT', 'RAIL_UPDATE', 'SETTLEMENT_APPLY', 'REMITTANCE_QUEUE', 'PAYOUT_NOTICE_QUEUE')
        )
    ), mutated_chunks AS (
      UPDATE public.banking_pay_operation_chunks AS operation_chunk_update
      SET status = CASE WHEN chunks_to_mutate.previous_status = 'RUNNING' THEN 'FAILED' ELSE 'SKIPPED' END,
          result_json = CASE
            WHEN chunks_to_mutate.previous_status = 'PENDING' THEN jsonb_strip_nulls(
              COALESCE(operation_chunk_update.result_json, '{}'::jsonb) || jsonb_build_object(
                'skipped_by_failed_execution_cleanup', true,
                'cleanup_operation_id', p_operation_id::text,
                'cleanup_at_utc', v_now::text,
                'failure_phase', v_failure_phase
              )
            )
            ELSE operation_chunk_update.result_json
          END,
          error_json = CASE
            WHEN chunks_to_mutate.previous_status = 'RUNNING' THEN jsonb_strip_nulls(jsonb_build_object(
              'code', 'PAYMENT_EXECUTE_FAILED_LOCAL_ARTIFACT_CLEANUP',
              'message', 'Chunk was still running when failed payment execution cleanup ran.',
              'operation_id', p_operation_id::text,
              'failure_phase', v_failure_phase,
              'cleanup_at_utc', v_now::text,
              'failure_code', NULLIF(BTRIM(COALESCE(v_failure_error_json->>'code', v_failure_error_json->>'error', '')), '')
            ))
            ELSE operation_chunk_update.error_json
          END,
          failed_count = CASE
            WHEN chunks_to_mutate.previous_status = 'RUNNING' THEN
              CASE
                WHEN COALESCE(operation_chunk_update.unit_count, 0) > 0 THEN GREATEST(COALESCE(operation_chunk_update.failed_count, 0), COALESCE(operation_chunk_update.unit_count, 0) - COALESCE(operation_chunk_update.completed_count, 0))
                ELSE GREATEST(COALESCE(operation_chunk_update.failed_count, 0), 1)
              END
            ELSE operation_chunk_update.failed_count
          END,
          locked_by = NULL::text,
          lock_expires_at_utc = NULL::timestamptz,
          completed_at_utc = COALESCE(operation_chunk_update.completed_at_utc, v_now),
          updated_at_utc = v_now
      FROM chunks_to_mutate
      WHERE operation_chunk_update.id = chunks_to_mutate.operation_chunk_id
      RETURNING chunks_to_mutate.previous_status,
                chunks_to_mutate.had_lock
    )
    SELECT COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.previous_status = 'RUNNING')), 0)::integer,
           COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.previous_status = 'PENDING')), 0)::integer,
           COALESCE((COUNT(*) FILTER (WHERE mutated_chunks.had_lock)), 0)::integer
    INTO v_chunks_marked_failed,
         v_chunks_marked_skipped,
         v_locks_released
    FROM mutated_chunks;

    WITH terminal_lock_release AS (
      UPDATE public.banking_pay_operation_chunks AS terminal_chunk_update
      SET locked_by = NULL::text,
          lock_expires_at_utc = NULL::timestamptz,
          updated_at_utc = v_now
      WHERE terminal_chunk_update.operation_id = p_operation_id
        AND terminal_chunk_update.status IN ('COMPLETE', 'FAILED', 'SKIPPED')
        AND (terminal_chunk_update.locked_by IS NOT NULL OR terminal_chunk_update.lock_expires_at_utc IS NOT NULL)
        AND NOT (
          terminal_chunk_update.phase IN ('SUBMIT_PROVIDER_TRANSFERS', 'APPLY_RAIL_UPDATES', 'APPLY_SETTLEMENT_CHUNKS', 'QUEUE_REMITTANCE_CHUNKS', 'QUEUE_PAYOUT_NOTICE_CHUNKS')
          OR terminal_chunk_update.chunk_type IN ('TRANSFER_SUBMIT', 'RAIL_UPDATE', 'SETTLEMENT_APPLY', 'REMITTANCE_QUEUE', 'PAYOUT_NOTICE_QUEUE')
        )
      RETURNING terminal_chunk_update.id
    )
    SELECT COALESCE(v_locks_released, 0) + COUNT(*)::integer
    INTO v_locks_released
    FROM terminal_lock_release;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_remaining_operation_scope_count
  FROM public.banking_pay_operation_transfer_scope AS remaining_scope
  WHERE remaining_scope.operation_id = p_operation_id
    AND remaining_scope.pay_batch_id = v_operation_row.pay_batch_id;

  SELECT COUNT(*)::integer
  INTO v_remaining_operation_transfer_count
  FROM public.pay_bank_transfers AS remaining_transfer
  WHERE remaining_transfer.pay_batch_id = v_operation_row.pay_batch_id
    AND (
      BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{operation_id}', '')) = p_operation_id::text
      OR BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{created_by_operation_id}', '')) = p_operation_id::text
      OR BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{payment_execute_operation_id}', '')) = p_operation_id::text
      OR remaining_transfer.request_id LIKE ('op:' || p_operation_id::text || ':%')
      OR BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{request_id}', '')) LIKE ('op:' || p_operation_id::text || ':%')
      OR BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{idempotency_key}', '')) LIKE ('op:' || p_operation_id::text || ':%')
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_transfer_scope AS remaining_scope
        WHERE remaining_scope.operation_id = p_operation_id
          AND remaining_scope.pay_batch_id = v_operation_row.pay_batch_id
          AND remaining_scope.pay_bank_transfer_id = remaining_transfer.id
      )
    );

  IF COALESCE(p_dry_run, false) IS TRUE THEN
    v_retry_blocked := (
      v_batch_execution_boundary_crossed IS TRUE
      OR COALESCE(v_active_auth_request_blocker_count, 0) > 0
      OR COALESCE(v_provider_review_risk_count, 0) > 0
      OR COALESCE(v_unsafe_transfer_count, 0) > 0
      OR COALESCE(v_unsafe_scope_count, 0) > 0
    );
  ELSE
    v_retry_blocked := (
      v_batch_execution_boundary_crossed IS TRUE
      OR COALESCE(v_active_auth_request_blocker_count, 0) > 0
      OR COALESCE(v_provider_review_risk_count, 0) > 0
      OR COALESCE(v_unsafe_transfer_count, 0) > 0
      OR COALESCE(v_unsafe_scope_count, 0) > 0
      OR COALESCE(v_remaining_operation_scope_count, 0) > 0
      OR COALESCE(v_remaining_operation_transfer_count, 0) > 0
    );
  END IF;

  v_review_required := v_retry_blocked;
  v_safe_to_retry := (v_retry_blocked IS NOT TRUE);

  v_cleanup_mode := CASE
    WHEN COALESCE(p_dry_run, false) IS TRUE THEN 'DRY_RUN'
    WHEN v_batch_execution_boundary_crossed THEN 'REVIEW_REQUIRED_BATCH_EXECUTION_BOUNDARY'
    WHEN COALESCE(v_provider_evidence_count, 0) > 0 THEN 'REVIEW_REQUIRED_PROVIDER_ACCEPTANCE_EVIDENCE'
    WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' OR COALESCE(v_stale_empty_provider_submit_chunk_count, 0) > 0 THEN 'REVIEW_REQUIRED_STALE_PROVIDER_SUBMIT_CHUNK'
    WHEN COALESCE(v_provider_submission_status, '') = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' OR COALESCE(v_provider_submit_unknown_chunk_count, 0) > 0 THEN 'REVIEW_REQUIRED_UNKNOWN_PROVIDER_SUBMISSION_OUTCOME'
    WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'REVIEW_REQUIRED_PROVIDER_MALFORMED_RESPONSE'
    WHEN COALESCE(v_active_auth_request_blocker_count, 0) > 0 THEN 'REVIEW_REQUIRED_ACTIVE_AUTH_REQUEST'
    WHEN COALESCE(v_provider_review_risk_count, 0) > 0 THEN 'REVIEW_REQUIRED_PROVIDER_SUBMISSION_REVIEW'
    WHEN COALESCE(v_unsafe_transfer_count, 0) > 0 OR COALESCE(v_unsafe_scope_count, 0) > 0 THEN 'REVIEW_REQUIRED_UNSAFE_LOCAL_ARTIFACTS'
    WHEN COALESCE(v_remaining_operation_scope_count, 0) > 0 OR COALESCE(v_remaining_operation_transfer_count, 0) > 0 THEN 'REVIEW_REQUIRED_RETRY_BLOCKER_REMAINS'
    WHEN COALESCE(v_scope_rows_deleted, 0) > 0 OR COALESCE(v_transfer_rows_deleted, 0) > 0 OR COALESCE(v_item_links_cleared, 0) > 0 OR COALESCE(v_chunks_marked_failed, 0) > 0 OR COALESCE(v_chunks_marked_skipped, 0) > 0 THEN CASE
      WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'CLEANED_LOCAL_ARTIFACTS_PROVIDER_BLOCKED_PRE_CALL'
      ELSE 'CLEANED_LOCAL_ARTIFACTS_NO_PROVIDER_CALL'
    END
    ELSE 'NO_LOCAL_ARTIFACTS_TO_CLEAN'
  END;

  v_retry_blocked_reason := CASE
    WHEN v_retry_blocked IS NOT TRUE THEN NULL::text
    WHEN v_operation_type_upper = 'PAYMENT_RETRY_BLOCKED_FUNDS'
      AND COALESCE(v_provider_evidence_count, 0) > 0 THEN 'PAYMENT_RETRY_BLOCKED_FUNDS_CLEANUP_NOT_SAFE_PROVIDER_ACCEPTANCE_EVIDENCE'
    WHEN v_operation_type_upper = 'PAYMENT_RETRY_BLOCKED_FUNDS'
      AND (COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' OR COALESCE(v_stale_empty_provider_submit_chunk_count, 0) > 0) THEN 'PAYMENT_RETRY_BLOCKED_FUNDS_CLEANUP_NOT_SAFE_PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
    WHEN v_operation_type_upper = 'PAYMENT_RETRY_BLOCKED_FUNDS'
      AND (COALESCE(v_provider_submission_status, '') = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' OR COALESCE(v_provider_submit_unknown_chunk_count, 0) > 0) THEN 'PAYMENT_RETRY_BLOCKED_FUNDS_CLEANUP_NOT_SAFE_PROVIDER_SUBMISSION_UNKNOWN'
    WHEN v_operation_type_upper = 'PAYMENT_RETRY_BLOCKED_FUNDS'
      AND COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'PAYMENT_RETRY_BLOCKED_FUNDS_CLEANUP_NOT_SAFE_PROVIDER_SUBMISSION_MALFORMED_RESPONSE'
    WHEN v_operation_type_upper = 'PAYMENT_RETRY_BLOCKED_FUNDS'
      AND COALESCE(v_unsafe_transfer_count, 0) > 0 THEN 'PAYMENT_RETRY_BLOCKED_FUNDS_CLEANUP_NOT_SAFE_UNSAFE_TRANSFER'
    WHEN v_operation_type_upper = 'PAYMENT_RETRY_BLOCKED_FUNDS'
      AND COALESCE(v_unsafe_scope_count, 0) > 0 THEN 'PAYMENT_RETRY_BLOCKED_FUNDS_CLEANUP_NOT_SAFE_UNSAFE_SCOPE'
    WHEN v_operation_type_upper = 'PAYMENT_RETRY_BLOCKED_FUNDS' THEN 'PAYMENT_RETRY_BLOCKED_FUNDS_CLEANUP_NOT_SAFE'
    WHEN v_batch_execution_boundary_crossed THEN 'BATCH_EXECUTION_BOUNDARY_CROSSED'
    WHEN COALESCE(v_provider_evidence_count, 0) > 0 THEN 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT'
    WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' OR COALESCE(v_stale_empty_provider_submit_chunk_count, 0) > 0 THEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
    WHEN COALESCE(v_provider_submission_status, '') = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' OR COALESCE(v_provider_submit_unknown_chunk_count, 0) > 0 THEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME'
    WHEN COALESCE(v_provider_submission_status, '') = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE'
    WHEN COALESCE(v_active_auth_request_blocker_count, 0) > 0 THEN 'ACTIVE_AUTH_REQUEST_PRESENT'
    WHEN COALESCE(v_provider_review_risk_count, 0) > 0 THEN 'PROVIDER_SUBMISSION_REVIEW_REQUIRED'
    WHEN COALESCE(v_unsafe_transfer_count, 0) > 0 OR COALESCE(v_unsafe_scope_count, 0) > 0 THEN 'UNSAFE_LOCAL_ARTIFACTS_REMAIN'
    WHEN COALESCE(v_remaining_operation_scope_count, 0) > 0 OR COALESCE(v_remaining_operation_transfer_count, 0) > 0 THEN 'RETRY_BLOCKER_REMAINS'
    ELSE 'RETRY_BLOCKED'
  END;

  v_result := jsonb_strip_nulls(
    jsonb_build_object(
      'ok', true,
      'dry_run', COALESCE(p_dry_run, false),
      'dry_run_counts_are_prospective', COALESCE(p_dry_run, false),
      'operation_id', p_operation_id::text,
      'pay_batch_id', v_operation_row.pay_batch_id::text,
      'failure_phase', v_failure_phase,
      'cleanup_mode', v_cleanup_mode,
      'operation_types_supported', jsonb_build_array('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS'),
      'operation_type_cleanup_mode', v_operation_type_cleanup_mode,
      'retry_blocked_reason', v_retry_blocked_reason,
      'safe_to_retry', v_safe_to_retry,
      'retry_blocked', v_retry_blocked,
      'review_required', v_review_required,
      'scope_rows_considered', COALESCE(v_scope_rows_considered, 0),
      'transfer_rows_considered', COALESCE(v_transfer_rows_considered, 0),
      'safe_scope_candidate_count', COALESCE(v_safe_scope_candidate_count, 0),
      'safe_transfer_candidate_count', COALESCE(v_safe_transfer_candidate_count, 0),
      'safe_local_transfer_count', COALESCE(v_safe_transfer_candidate_count, 0),
      'scope_rows_deleted', COALESCE(v_scope_rows_deleted, 0),
      'transfer_rows_deleted', COALESCE(v_transfer_rows_deleted, 0),
      'item_links_cleared', COALESCE(v_item_links_cleared, 0),
      'bank_references_cleared', COALESCE(v_bank_references_cleared, 0),
      'chunks_marked_failed', COALESCE(v_chunks_marked_failed, 0),
      'chunks_marked_skipped', COALESCE(v_chunks_marked_skipped, 0),
      'locks_released', COALESCE(v_locks_released, 0),
      'provider_evidence_count', COALESCE(v_provider_evidence_count, 0),
      'provider_review_risk_count', COALESCE(v_provider_review_risk_count, 0),
      'provider_submit_diagnostic', COALESCE(v_provider_submit_diagnostic, '{}'::jsonb),
      'provider_submit_diagnostic_finalise_result', COALESCE(v_provider_submit_finalise_result, '{}'::jsonb),
      'provider_submission_status', v_provider_submission_status,
      'review_reason_code', v_provider_submit_review_reason_code,
      'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
      'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
      'provider_submission_unknown_count', COALESCE(v_provider_submission_unknown_count, 0),
      'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
      'unfinalised_submit_chunk_count', COALESCE(v_unfinalised_submit_chunk_count, 0),
      'manual_resolution_required', COALESCE(v_provider_manual_resolution_required, false),
      'recommended_action', v_provider_recommended_action,
      'safe_retry_available', COALESCE(v_safe_to_retry, false),
      'unsafe_transfer_count', COALESCE(v_unsafe_transfer_count, 0),
      'unsafe_scope_count', COALESCE(v_unsafe_scope_count, 0),
      'unsafe_transfer_ids', COALESCE(v_unsafe_transfer_ids, '[]'::jsonb),
      'unsafe_reasons', COALESCE(v_unsafe_reasons, '[]'::jsonb)
    )
    ||
    jsonb_build_object(
      'safe_scope_ids', CASE WHEN COALESCE(p_dry_run, false) THEN COALESCE(v_safe_scope_ids, '[]'::jsonb) ELSE COALESCE(v_deleted_scope_ids, '[]'::jsonb) END,
      'safe_transfer_ids', CASE WHEN COALESCE(p_dry_run, false) THEN COALESCE(v_safe_transfer_ids, '[]'::jsonb) ELSE COALESCE(v_deleted_transfer_ids, '[]'::jsonb) END,
      'initial_active_auth_request_count', COALESCE(v_initial_active_auth_request_count, 0),
      'active_auth_request_count', COALESCE(v_active_auth_request_count, 0),
      'operation_active_auth_request_count', COALESCE(v_operation_active_auth_request_count, 0),
      'active_auth_request_ids', COALESCE(v_active_auth_request_ids, '[]'::jsonb),
      'auth_requests_cancelled', COALESCE(v_auth_requests_cancelled, 0),
      'awaiting_auth_requests_cancelled', COALESCE(v_awaiting_auth_requests_cancelled, 0),
      'authorised_auth_requests_cancelled', COALESCE(v_authorised_auth_requests_cancelled, 0),
      'auth_request_cleanup_mode', CASE WHEN COALESCE(v_auth_requests_cancelled, 0) > 0 THEN 'CANCELLED_SAFE_LOCAL_AUTH_REQUESTS' WHEN COALESCE(v_non_cancellable_auth_request_count, 0) > 0 THEN 'AUTH_REQUEST_REVIEW_REQUIRED' ELSE 'NO_AUTH_REQUEST_CLEANUP_REQUIRED' END,
      'auth_tokens_voided', COALESCE(v_auth_tokens_voided, 0),
      'batch_execution_intent_cleared', COALESCE(v_batch_execution_intent_cleared, 0),
      'active_auth_request_blocker_count', COALESCE(v_active_auth_request_blocker_count, 0),
      'authorised_auth_request_review_count', COALESCE(v_authorised_auth_request_review_count, 0),
      'cancellable_auth_request_count', COALESCE(v_cancellable_auth_request_count, 0),
      'non_cancellable_auth_request_count', COALESCE(v_non_cancellable_auth_request_count, 0),
      'cancelled_auth_request_ids', COALESCE(v_cancelled_auth_request_ids, '[]'::jsonb),
      'provider_submit_chunk_attempt_count', COALESCE(v_provider_submit_chunk_attempt_count, 0),
      'stale_empty_provider_submit_chunk_count', COALESCE(v_stale_empty_provider_submit_chunk_count, 0),
      'provider_submit_unknown_chunk_count', COALESCE(v_provider_submit_unknown_chunk_count, 0),
      'batch_execution_boundary_crossed', v_batch_execution_boundary_crossed,
      'execution_commit_state', upper(BTRIM(COALESCE(v_batch_row.execution_commit_state, 'NOT_SUBMITTED'))),
      'execution_commit_ref_present', NULLIF(BTRIM(COALESCE(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL,
      'execution_committed_at_utc_present', v_batch_row.execution_committed_at_utc IS NOT NULL,
      'remaining_operation_scope_count', COALESCE(v_remaining_operation_scope_count, 0),
      'remaining_operation_transfer_count', COALESCE(v_remaining_operation_transfer_count, 0)
    )
  );

  IF COALESCE(p_dry_run, false) IS NOT TRUE THEN
    BEGIN
      PERFORM public._audit_insert(
        'banking_pay_operations',
        p_operation_id::text,
        'PAYMENT_EXECUTE_FAILED_LOCAL_ARTIFACT_CLEANUP',
        NULL::jsonb,
        v_result,
        'payment_execute_failed_local_artifact_cleanup',
        v_effective_actor_user_id
      );
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  END IF;

  RETURN v_result;
END;
$function$;





CREATE OR REPLACE FUNCTION public.pay_provider_submit_diagnostic_get(
  p_pay_batch_id uuid DEFAULT NULL::uuid,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_transfer_id uuid DEFAULT NULL::uuid,
  p_chunk_id uuid DEFAULT NULL::uuid,
  p_counts_only boolean DEFAULT false
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_uuid_regex text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_effective_pay_batch_id uuid := p_pay_batch_id;
  v_batch_id_from_scope uuid := NULL::uuid;
  v_operation_count integer := 0;
  v_chunk_count integer := 0;
  v_transfer_count integer := 0;
  v_active_auth_request_count integer := 0;
  v_provider_acceptance_evidence_count integer := 0;
  v_provider_response_present_count integer := 0;
  v_provider_request_sent_count integer := 0;
  v_provider_submission_unknown_count integer := 0;
  v_stale_empty_submit_chunk_count integer := 0;
  v_stale_unresolved_submit_chunk_count integer := 0;
  v_request_sent_no_response_count integer := 0;
  v_unfinalised_submit_chunk_count integer := 0;
  v_provider_submission_rejected_count integer := 0;
  v_provider_submission_accepted_count integer := 0;
  v_provider_submission_blocked_pre_call_count integer := 0;
  v_provider_submission_malformed_response_count integer := 0;
  v_provider_submission_status text := 'NO_PROVIDER_SUBMISSION_ATTEMPTED';
  v_review_reason_code text := 'NO_PROVIDER_SUBMIT_ATTEMPT';
  v_provider_call_stage text := 'NO_PROVIDER_SUBMIT_ATTEMPT';
  v_provider_submission_attempted boolean := false;
  v_provider_request_sent boolean := false;
  v_provider_response_received boolean := false;
  v_provider_response_present boolean := false;
  v_provider_submission_accepted boolean := false;
  v_provider_submission_rejected boolean := false;
  v_provider_submission_failed boolean := false;
  v_provider_submission_unknown boolean := false;
  v_provider_acceptance_evidence_present boolean := false;
  v_stale_submit_chunk boolean := false;
  v_unfinalised_submit_chunk boolean := false;
  v_chunk_lock_expired boolean := false;
  v_manual_resolution_required boolean := false;
  v_safe_retry_available boolean := false;
  v_automatic_retry_blocked boolean := false;
  v_retry_blocked_reason text := NULL::text;
  v_recommended_action text := 'No provider submission attempt has been recorded for this scope.';
  v_operation_ids jsonb := '[]'::jsonb;
  v_chunk_ids jsonb := '[]'::jsonb;
  v_transfer_ids jsonb := '[]'::jsonb;
  v_transfer_scope_ids jsonb := '[]'::jsonb;
  v_auth_request_ids jsonb := '[]'::jsonb;
  v_primary_operation_id text := NULL::text;
  v_primary_chunk_id text := NULL::text;
  v_primary_transfer_id text := NULL::text;
  v_primary_transfer_scope_id text := NULL::text;
  v_primary_auth_request_id text := NULL::text;
  v_primary_rail_provider text := NULL::text;
  v_primary_rail_env text := NULL::text;
  v_primary_rail_tx_id text := NULL::text;
  v_primary_rail_state text := NULL::text;
  v_primary_provider_transaction_id text := NULL::text;
  v_primary_provider_reference text := NULL::text;
  v_primary_provider_state text := NULL::text;
  v_primary_request_id text := NULL::text;
  v_primary_idempotency_key text := NULL::text;
  v_primary_local_provider_request_id text := NULL::text;
  v_primary_provider_http_status text := NULL::text;
  v_primary_provider_error_code text := NULL::text;
  v_primary_provider_error_message_redacted text := NULL::text;
  v_primary_provider_response_redacted jsonb := NULL::jsonb;
  v_primary_provider_error_redacted jsonb := NULL::jsonb;
  v_chunk_started_at_utc text := NULL::text;
  v_chunk_completed_at_utc text := NULL::text;
  v_chunk_lock_expires_at_utc text := NULL::text;
  v_provider_submit_diagnostic jsonb := '{}'::jsonb;
  v_counts jsonb := '{}'::jsonb;
  v_ids jsonb := '{}'::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL AND p_operation_id IS NULL AND p_transfer_id IS NULL AND p_chunk_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_DIAGNOSTIC_GET',
      'code', 'IDENTIFIER_REQUIRED',
      'message', 'pay_provider_submit_diagnostic_get: at least one identifier is required'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_pay_batch_id IS NOT NULL THEN
    PERFORM 1 FROM public.pay_batches AS batch_check WHERE batch_check.id = p_pay_batch_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_PROVIDER_SUBMIT_DIAGNOSTIC_GET',
        'code', 'PAY_BATCH_NOT_FOUND',
        'message', 'pay_provider_submit_diagnostic_get: pay batch not found',
        'pay_batch_id', p_pay_batch_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF p_operation_id IS NOT NULL THEN
    SELECT operation_row.pay_batch_id
    INTO v_batch_id_from_scope
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_PROVIDER_SUBMIT_DIAGNOSTIC_GET',
        'code', 'OPERATION_NOT_FOUND',
        'message', 'pay_provider_submit_diagnostic_get: operation not found',
        'operation_id', p_operation_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;
    IF v_effective_pay_batch_id IS NULL THEN
      v_effective_pay_batch_id := v_batch_id_from_scope;
    ELSIF v_batch_id_from_scope IS NOT NULL AND v_effective_pay_batch_id <> v_batch_id_from_scope THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_PROVIDER_SUBMIT_DIAGNOSTIC_GET',
        'code', 'OPERATION_BATCH_MISMATCH',
        'message', 'pay_provider_submit_diagnostic_get: operation does not belong to supplied pay batch',
        'operation_id', p_operation_id::text,
        'operation_pay_batch_id', v_batch_id_from_scope::text,
        'pay_batch_id', v_effective_pay_batch_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF p_transfer_id IS NOT NULL THEN
    SELECT transfer_row.pay_batch_id
    INTO v_batch_id_from_scope
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.id = p_transfer_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_PROVIDER_SUBMIT_DIAGNOSTIC_GET',
        'code', 'TRANSFER_NOT_FOUND',
        'message', 'pay_provider_submit_diagnostic_get: transfer not found',
        'transfer_id', p_transfer_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;
    IF v_effective_pay_batch_id IS NULL THEN
      v_effective_pay_batch_id := v_batch_id_from_scope;
    ELSIF v_effective_pay_batch_id <> v_batch_id_from_scope THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_PROVIDER_SUBMIT_DIAGNOSTIC_GET',
        'code', 'TRANSFER_BATCH_MISMATCH',
        'message', 'pay_provider_submit_diagnostic_get: transfer does not belong to supplied pay batch',
        'transfer_id', p_transfer_id::text,
        'transfer_pay_batch_id', v_batch_id_from_scope::text,
        'pay_batch_id', v_effective_pay_batch_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  IF p_chunk_id IS NOT NULL THEN
    SELECT operation_row.pay_batch_id
    INTO v_batch_id_from_scope
    FROM public.banking_pay_operation_chunks AS chunk_row
    JOIN public.banking_pay_operations AS operation_row
      ON operation_row.id = chunk_row.operation_id
    WHERE chunk_row.id = p_chunk_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_PROVIDER_SUBMIT_DIAGNOSTIC_GET',
        'code', 'CHUNK_NOT_FOUND',
        'message', 'pay_provider_submit_diagnostic_get: chunk not found',
        'chunk_id', p_chunk_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;
    IF v_effective_pay_batch_id IS NULL THEN
      v_effective_pay_batch_id := v_batch_id_from_scope;
    ELSIF v_batch_id_from_scope IS NOT NULL AND v_effective_pay_batch_id <> v_batch_id_from_scope THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_PROVIDER_SUBMIT_DIAGNOSTIC_GET',
        'code', 'CHUNK_BATCH_MISMATCH',
        'message', 'pay_provider_submit_diagnostic_get: chunk operation does not belong to supplied pay batch',
        'chunk_id', p_chunk_id::text,
        'chunk_pay_batch_id', v_batch_id_from_scope::text,
        'pay_batch_id', v_effective_pay_batch_id::text
      )::text USING ERRCODE = 'P0001';
    END IF;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_diagnostic_operations;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_diagnostic_chunks;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_diagnostic_chunk_transfer_ids;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_diagnostic_transfers;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_diagnostic_sources;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_diagnostic_transfer_flags;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_diagnostic_chunk_flags;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_diagnostic_operations (
    operation_id uuid PRIMARY KEY,
    pay_batch_id uuid,
    operation_status text,
    operation_phase text,
    progress_json jsonb,
    result_json jsonb,
    error_json jsonb,
    created_at_utc timestamptz,
    updated_at_utc timestamptz
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_provider_submit_diagnostic_operations (
    operation_id,
    pay_batch_id,
    operation_status,
    operation_phase,
    progress_json,
    result_json,
    error_json,
    created_at_utc,
    updated_at_utc
  )
  SELECT operation_row.id,
         operation_row.pay_batch_id,
         operation_row.status,
         operation_row.phase,
         COALESCE(operation_row.progress_json, '{}'::jsonb),
         operation_row.result_json,
         operation_row.error_json,
         operation_row.created_at_utc,
         operation_row.updated_at_utc
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
    AND (
      (p_operation_id IS NOT NULL AND operation_row.id = p_operation_id)
      OR (v_effective_pay_batch_id IS NOT NULL AND operation_row.pay_batch_id = v_effective_pay_batch_id)
      OR (p_chunk_id IS NOT NULL AND EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_chunks AS chunk_filter
        WHERE chunk_filter.id = p_chunk_id
          AND chunk_filter.operation_id = operation_row.id
      ))
    )
  ON CONFLICT (operation_id) DO NOTHING;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_diagnostic_chunks (
    chunk_id uuid PRIMARY KEY,
    operation_id uuid NOT NULL,
    pay_batch_id uuid,
    phase text NOT NULL,
    chunk_type text NOT NULL,
    sequence_no integer NOT NULL,
    status text NOT NULL,
    payload_json jsonb NOT NULL,
    result_json jsonb,
    error_json jsonb,
    unit_count integer NOT NULL,
    completed_count integer NOT NULL,
    failed_count integer NOT NULL,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz NOT NULL,
    started_at_utc timestamptz,
    completed_at_utc timestamptz,
    updated_at_utc timestamptz NOT NULL
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_provider_submit_diagnostic_chunks (
    chunk_id,
    operation_id,
    pay_batch_id,
    phase,
    chunk_type,
    sequence_no,
    status,
    payload_json,
    result_json,
    error_json,
    unit_count,
    completed_count,
    failed_count,
    locked_by,
    lock_expires_at_utc,
    created_at_utc,
    started_at_utc,
    completed_at_utc,
    updated_at_utc
  )
  SELECT chunk_row.id,
         chunk_row.operation_id,
         operation_row.pay_batch_id,
         chunk_row.phase,
         chunk_row.chunk_type,
         chunk_row.sequence_no,
         chunk_row.status,
         COALESCE(chunk_row.payload_json, '{}'::jsonb),
         chunk_row.result_json,
         chunk_row.error_json,
         chunk_row.unit_count,
         chunk_row.completed_count,
         chunk_row.failed_count,
         chunk_row.locked_by,
         chunk_row.lock_expires_at_utc,
         chunk_row.created_at_utc,
         chunk_row.started_at_utc,
         chunk_row.completed_at_utc,
         chunk_row.updated_at_utc
  FROM public.banking_pay_operation_chunks AS chunk_row
  JOIN public.banking_pay_operations AS operation_row
    ON operation_row.id = chunk_row.operation_id
  WHERE operation_row.operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
    AND (chunk_row.phase = 'SUBMIT_PROVIDER_TRANSFERS' OR chunk_row.chunk_type = 'TRANSFER_SUBMIT')
    AND (
      (p_chunk_id IS NOT NULL AND chunk_row.id = p_chunk_id)
      OR (p_operation_id IS NOT NULL AND chunk_row.operation_id = p_operation_id)
      OR EXISTS (
        SELECT 1
        FROM pg_temp.tmp_provider_submit_diagnostic_operations AS selected_operation
        WHERE selected_operation.operation_id = chunk_row.operation_id
      )
    )
  ON CONFLICT (chunk_id) DO NOTHING;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_diagnostic_chunk_transfer_ids (
    chunk_id uuid NOT NULL,
    transfer_id uuid NOT NULL,
    PRIMARY KEY (chunk_id, transfer_id)
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_provider_submit_diagnostic_chunk_transfer_ids (chunk_id, transfer_id)
  SELECT DISTINCT selected_chunk.chunk_id,
                  transfer_id_text.value::uuid
  FROM pg_temp.tmp_provider_submit_diagnostic_chunks AS selected_chunk
  CROSS JOIN LATERAL jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(selected_chunk.payload_json->'transfer_ids') = 'array' THEN selected_chunk.payload_json->'transfer_ids'
      ELSE '[]'::jsonb
    END
  ) AS transfer_id_text(value)
  WHERE transfer_id_text.value ~* v_uuid_regex
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_diagnostic_chunk_transfer_ids (chunk_id, transfer_id)
  SELECT DISTINCT selected_chunk.chunk_id,
                  transfer_id_text.value::uuid
  FROM pg_temp.tmp_provider_submit_diagnostic_chunks AS selected_chunk
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN jsonb_typeof(selected_chunk.payload_json->'transfers') = 'array' THEN selected_chunk.payload_json->'transfers'
      ELSE '[]'::jsonb
    END
  ) AS transfer_payload(value)
  CROSS JOIN LATERAL (
    SELECT COALESCE(
      NULLIF(BTRIM(COALESCE(transfer_payload.value->>'pay_bank_transfer_id', '')), ''),
      NULLIF(BTRIM(COALESCE(transfer_payload.value->>'transfer_id', '')), '')
    ) AS value
  ) AS transfer_id_text
  WHERE transfer_id_text.value ~* v_uuid_regex
  ON CONFLICT DO NOTHING;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_diagnostic_transfers (
    transfer_id uuid PRIMARY KEY,
    pay_batch_id uuid NOT NULL,
    status text NOT NULL,
    rail_provider text NOT NULL,
    rail_env text NOT NULL,
    request_id text,
    rail_tx_id text,
    rail_state text,
    rail_meta_json jsonb,
    payment_reference text,
    created_at_utc timestamptz NOT NULL
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_provider_submit_diagnostic_transfers (
    transfer_id,
    pay_batch_id,
    status,
    rail_provider,
    rail_env,
    request_id,
    rail_tx_id,
    rail_state,
    rail_meta_json,
    payment_reference,
    created_at_utc
  )
  SELECT transfer_row.id,
         transfer_row.pay_batch_id,
         transfer_row.status,
         transfer_row.rail_provider,
         transfer_row.rail_env,
         transfer_row.request_id,
         transfer_row.rail_tx_id,
         transfer_row.rail_state,
         transfer_row.rail_meta_json,
         transfer_row.payment_reference,
         transfer_row.created_at_utc
  FROM public.pay_bank_transfers AS transfer_row
  WHERE (p_transfer_id IS NOT NULL AND transfer_row.id = p_transfer_id)
     OR EXISTS (
       SELECT 1
       FROM pg_temp.tmp_provider_submit_diagnostic_chunk_transfer_ids AS selected_chunk_transfer
       WHERE selected_chunk_transfer.transfer_id = transfer_row.id
     )
     OR (
       p_transfer_id IS NULL
       AND p_chunk_id IS NULL
       AND v_effective_pay_batch_id IS NOT NULL
       AND transfer_row.pay_batch_id = v_effective_pay_batch_id
     )
  ON CONFLICT (transfer_id) DO NOTHING;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_diagnostic_sources (
    source_type text NOT NULL,
    operation_id uuid,
    chunk_id uuid,
    transfer_id uuid,
    event_id uuid,
    diagnostic jsonb NOT NULL,
    provider_submission_status text,
    review_reason_code text,
    provider_call_stage text,
    provider_submission_attempted boolean NOT NULL DEFAULT false,
    provider_request_sent boolean NOT NULL DEFAULT false,
    provider_response_received boolean NOT NULL DEFAULT false,
    provider_response_present boolean NOT NULL DEFAULT false,
    provider_submission_accepted boolean NOT NULL DEFAULT false,
    provider_submission_rejected boolean NOT NULL DEFAULT false,
    provider_submission_failed boolean NOT NULL DEFAULT false,
    provider_submission_unknown boolean NOT NULL DEFAULT false,
    provider_acceptance_evidence_present boolean NOT NULL DEFAULT false,
    manual_resolution_required boolean NOT NULL DEFAULT false,
    safe_retry_available boolean NOT NULL DEFAULT false,
    provider_http_status text,
    provider_error_code text,
    provider_error_message_redacted text,
    provider_transaction_id text,
    provider_reference text,
    provider_state text,
    rail_tx_id text,
    rail_state text,
    request_id text,
    idempotency_key text,
    local_provider_request_id text,
    provider_response_redacted jsonb,
    provider_error_redacted jsonb
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_provider_submit_diagnostic_sources (
    source_type,
    operation_id,
    chunk_id,
    transfer_id,
    event_id,
    diagnostic,
    provider_submission_status,
    review_reason_code,
    provider_call_stage,
    provider_submission_attempted,
    provider_request_sent,
    provider_response_received,
    provider_response_present,
    provider_submission_accepted,
    provider_submission_rejected,
    provider_submission_failed,
    provider_submission_unknown,
    provider_acceptance_evidence_present,
    manual_resolution_required,
    safe_retry_available,
    provider_http_status,
    provider_error_code,
    provider_error_message_redacted,
    provider_transaction_id,
    provider_reference,
    provider_state,
    rail_tx_id,
    rail_state,
    request_id,
    idempotency_key,
    local_provider_request_id,
    provider_response_redacted,
    provider_error_redacted
  )
  WITH raw_diagnostics AS (
    SELECT 'operation_progress'::text AS source_type,
           selected_operation.operation_id,
           NULL::uuid AS chunk_id,
           NULL::uuid AS transfer_id,
           NULL::uuid AS event_id,
           selected_operation.progress_json->'provider_submit_diagnostic' AS diagnostic
    FROM pg_temp.tmp_provider_submit_diagnostic_operations AS selected_operation
    WHERE jsonb_typeof(selected_operation.progress_json->'provider_submit_diagnostic') = 'object'
    UNION ALL
    SELECT 'operation_result'::text,
           selected_operation.operation_id,
           NULL::uuid,
           NULL::uuid,
           NULL::uuid,
           selected_operation.result_json->'provider_submit_diagnostic'
    FROM pg_temp.tmp_provider_submit_diagnostic_operations AS selected_operation
    WHERE jsonb_typeof(selected_operation.result_json->'provider_submit_diagnostic') = 'object'
    UNION ALL
    SELECT 'operation_error'::text,
           selected_operation.operation_id,
           NULL::uuid,
           NULL::uuid,
           NULL::uuid,
           selected_operation.error_json->'provider_submit_diagnostic'
    FROM pg_temp.tmp_provider_submit_diagnostic_operations AS selected_operation
    WHERE jsonb_typeof(selected_operation.error_json->'provider_submit_diagnostic') = 'object'
    UNION ALL
    SELECT 'chunk_result'::text,
           selected_chunk.operation_id,
           selected_chunk.chunk_id,
           selected_chunk_transfer.transfer_id,
           NULL::uuid,
           selected_chunk.result_json->'provider_submit_diagnostic'
    FROM pg_temp.tmp_provider_submit_diagnostic_chunks AS selected_chunk
    LEFT JOIN pg_temp.tmp_provider_submit_diagnostic_chunk_transfer_ids AS selected_chunk_transfer
      ON selected_chunk_transfer.chunk_id = selected_chunk.chunk_id
    WHERE jsonb_typeof(selected_chunk.result_json->'provider_submit_diagnostic') = 'object'
    UNION ALL
    SELECT 'chunk_error'::text,
           selected_chunk.operation_id,
           selected_chunk.chunk_id,
           selected_chunk_transfer.transfer_id,
           NULL::uuid,
           selected_chunk.error_json->'provider_submit_diagnostic'
    FROM pg_temp.tmp_provider_submit_diagnostic_chunks AS selected_chunk
    LEFT JOIN pg_temp.tmp_provider_submit_diagnostic_chunk_transfer_ids AS selected_chunk_transfer
      ON selected_chunk_transfer.chunk_id = selected_chunk.chunk_id
    WHERE jsonb_typeof(selected_chunk.error_json->'provider_submit_diagnostic') = 'object'
    UNION ALL
    SELECT 'transfer_meta'::text,
           NULL::uuid,
           NULL::uuid,
           selected_transfer.transfer_id,
           NULL::uuid,
           selected_transfer.rail_meta_json->'provider_submit_diagnostic'
    FROM pg_temp.tmp_provider_submit_diagnostic_transfers AS selected_transfer
    WHERE jsonb_typeof(selected_transfer.rail_meta_json->'provider_submit_diagnostic') = 'object'
    UNION ALL
    SELECT 'transfer_event'::text,
           NULL::uuid,
           NULL::uuid,
           transfer_event.pay_bank_transfer_id,
           transfer_event.id,
           transfer_event.raw_payload->'provider_submit_diagnostic'
    FROM public.pay_bank_transfer_events AS transfer_event
    WHERE jsonb_typeof(transfer_event.raw_payload->'provider_submit_diagnostic') = 'object'
      AND EXISTS (
        SELECT 1
        FROM pg_temp.tmp_provider_submit_diagnostic_transfers AS selected_transfer
        WHERE selected_transfer.transfer_id = transfer_event.pay_bank_transfer_id
      )
  )
  SELECT raw_diagnostics.source_type,
         raw_diagnostics.operation_id,
         raw_diagnostics.chunk_id,
         raw_diagnostics.transfer_id,
         raw_diagnostics.event_id,
         raw_diagnostics.diagnostic,
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_submission_status', raw_diagnostics.diagnostic->>'outcome_code', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'review_reason_code', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_call_stage', '')), ''),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_submission_attempted', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_request_sent', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_response_received', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_response_present', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_submission_accepted', raw_diagnostics.diagnostic->>'provider_accepted', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_submission_rejected', raw_diagnostics.diagnostic->>'provider_rejected', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_submission_failed', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_submission_unknown', raw_diagnostics.diagnostic->>'provider_unknown', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_acceptance_evidence_present', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'manual_resolution_required', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         lower(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'safe_retry_available', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_http_status', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_error_code', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_error_message_redacted', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_transaction_id', raw_diagnostics.diagnostic->>'provider_payment_id', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_reference', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'provider_state', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'rail_tx_id', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'rail_state', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'request_id', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'idempotency_key', '')), ''),
         NULLIF(BTRIM(COALESCE(raw_diagnostics.diagnostic->>'local_provider_request_id', '')), ''),
         raw_diagnostics.diagnostic->'provider_response_redacted',
         raw_diagnostics.diagnostic->'provider_error_redacted'
  FROM raw_diagnostics
  WHERE jsonb_typeof(raw_diagnostics.diagnostic) = 'object'
    AND raw_diagnostics.diagnostic <> '{}'::jsonb;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_diagnostic_transfer_flags (
    transfer_id uuid PRIMARY KEY,
    has_provider_acceptance_evidence boolean NOT NULL DEFAULT false,
    has_provider_response boolean NOT NULL DEFAULT false,
    has_provider_request_sent boolean NOT NULL DEFAULT false,
    has_provider_rejection boolean NOT NULL DEFAULT false,
    has_provider_unknown boolean NOT NULL DEFAULT false,
    has_provider_blocked_pre_call boolean NOT NULL DEFAULT false,
    has_provider_malformed_response boolean NOT NULL DEFAULT false
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_provider_submit_diagnostic_transfer_flags (
    transfer_id,
    has_provider_acceptance_evidence,
    has_provider_response,
    has_provider_request_sent,
    has_provider_rejection,
    has_provider_unknown,
    has_provider_blocked_pre_call,
    has_provider_malformed_response
  )
  SELECT selected_transfer.transfer_id,
         (
           (
             NULLIF(BTRIM(COALESCE(selected_transfer.rail_tx_id, '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(selected_transfer.rail_tx_id, '')), '') <> selected_transfer.transfer_id::text
             AND NULLIF(BTRIM(COALESCE(selected_transfer.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_transfer.request_id, '')), ''), '__NO_REQUEST_ID__')
             AND NULLIF(BTRIM(COALESCE(selected_transfer.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_transfer.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
             AND UPPER(BTRIM(COALESCE(selected_transfer.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
           )
           OR COALESCE(BOOL_OR(
             NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> selected_transfer.transfer_id::text
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_transfer.request_id, '')), ''), '__NO_REQUEST_ID__')
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
             AND UPPER(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
             AND selected_source.provider_submission_rejected IS NOT TRUE
             AND selected_source.provider_submission_failed IS NOT TRUE
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_transfer.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
           ), false)
           OR COALESCE(BOOL_OR(
             NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') IS NOT NULL
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> selected_transfer.transfer_id::text
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_transfer.request_id, '')), ''), '__NO_REQUEST_ID__')
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
             AND UPPER(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
             AND selected_source.provider_submission_rejected IS NOT TRUE
             AND selected_source.provider_submission_failed IS NOT TRUE
             AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_transfer.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
           ), false)
           OR COALESCE(BOOL_OR(
                upper(BTRIM(COALESCE(transfer_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')
                AND upper(BTRIM(COALESCE(transfer_event.normalised_state, transfer_event.provider_state, ''))) NOT IN ('REJECTED', 'FAILED', 'ERROR', 'DECLINED', 'CANCELLED', 'CANCELED', 'MALFORMED', 'UNKNOWN')
                AND EXISTS (
                  SELECT 1
                  FROM (VALUES
                    (transfer_event.provider_event_id),
                    (transfer_event.provider_reference),
                    (transfer_event.raw_payload #>> '{provider_transaction_id}'),
                    (transfer_event.raw_payload #>> '{provider_payment_id}'),
                    (transfer_event.raw_payload #>> '{payment_id}'),
                    (transfer_event.raw_payload #>> '{external_payment_id}'),
                    (transfer_event.raw_payload #>> '{revolut_payment_id}'),
                    (transfer_event.raw_payload #>> '{rail_tx_id}'),
                    (transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}'),
                    (transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_payment_id}'),
                    (transfer_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}'),
                    (transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}')
                  ) AS provider_identifier(identifier_value)
                  WHERE NULLIF(BTRIM(COALESCE(provider_identifier.identifier_value, '')), '') IS NOT NULL
                    AND NULLIF(BTRIM(COALESCE(provider_identifier.identifier_value, '')), '') <> selected_transfer.transfer_id::text
                    AND NULLIF(BTRIM(COALESCE(provider_identifier.identifier_value, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_transfer.request_id, '')), ''), '__NO_REQUEST_ID__')
                    AND NULLIF(BTRIM(COALESCE(provider_identifier.identifier_value, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_transfer.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
                    AND NULLIF(BTRIM(COALESCE(provider_identifier.identifier_value, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_event.idempotency_key, '')), ''), '__NO_EVENT_IDEMPOTENCY_KEY__')
                )
              ), false)
         ),
         (
           COALESCE(BOOL_OR(selected_source.provider_response_received OR selected_source.provider_response_present), false)
           OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('PROVIDER_SUBMISSION_ACCEPTED', 'PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')), false)
           OR COALESCE(BOOL_OR(NULLIF(BTRIM(COALESCE(selected_source.provider_http_status, selected_source.provider_error_code, '')), '') IS NOT NULL), false)
           OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(transfer_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')), false)
         ),
         (
           COALESCE(BOOL_OR(selected_source.provider_request_sent), false)
           OR COALESCE(BOOL_OR(selected_source.provider_response_received OR selected_source.provider_response_present), false)
           OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('PROVIDER_SUBMISSION_ACCEPTED', 'PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME')), false)
           OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(transfer_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')), false)
           OR NULLIF(BTRIM(COALESCE(selected_transfer.rail_tx_id, '')), '') IS NOT NULL
         ),
         (
           COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED')), false)
           OR COALESCE(BOOL_OR(selected_source.provider_submission_rejected OR selected_source.provider_submission_failed), false)
           OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(transfer_event.normalised_state, transfer_event.provider_state, ''))) IN ('REJECTED', 'FAILED', 'ERROR', 'DECLINED', 'CANCELLED', 'CANCELED')), false)
           OR COALESCE(BOOL_OR(CASE WHEN NULLIF(BTRIM(COALESCE(selected_source.provider_http_status, '')), '') ~ '^[0-9]+$' THEN selected_source.provider_http_status::integer >= 400 ELSE false END), false)
         ),
         (
           COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')), false)
           OR COALESCE(BOOL_OR(selected_source.provider_submission_unknown), false)
         ),
         (
           COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL'), false)
           OR COALESCE(BOOL_OR(
             upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) = 'NO_PROVIDER_SUBMISSION_ATTEMPTED'
             AND selected_source.provider_submission_attempted IS FALSE
             AND selected_source.provider_request_sent IS FALSE
             AND selected_source.provider_response_received IS FALSE
             AND selected_source.provider_response_present IS FALSE
           ), false)
         ),
         (
           COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE'), false)
         )
  FROM pg_temp.tmp_provider_submit_diagnostic_transfers AS selected_transfer
  LEFT JOIN pg_temp.tmp_provider_submit_diagnostic_sources AS selected_source
    ON selected_source.transfer_id = selected_transfer.transfer_id
  LEFT JOIN public.pay_bank_transfer_events AS transfer_event
    ON transfer_event.pay_bank_transfer_id = selected_transfer.transfer_id
  GROUP BY selected_transfer.transfer_id,
           selected_transfer.request_id,
           selected_transfer.payment_reference,
           selected_transfer.rail_tx_id
  ON CONFLICT (transfer_id) DO NOTHING;

  UPDATE pg_temp.tmp_provider_submit_diagnostic_transfer_flags AS transfer_flag_update
  SET has_provider_unknown = true
  WHERE transfer_flag_update.has_provider_request_sent IS TRUE
    AND transfer_flag_update.has_provider_response IS NOT TRUE
    AND transfer_flag_update.has_provider_acceptance_evidence IS NOT TRUE;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_diagnostic_chunk_flags (
    chunk_id uuid PRIMARY KEY,
    has_provider_acceptance_evidence boolean NOT NULL DEFAULT false,
    has_provider_response boolean NOT NULL DEFAULT false,
    has_provider_request_sent boolean NOT NULL DEFAULT false,
    has_provider_rejection boolean NOT NULL DEFAULT false,
    has_provider_unknown boolean NOT NULL DEFAULT false,
    has_provider_blocked_pre_call boolean NOT NULL DEFAULT false,
    has_provider_malformed_response boolean NOT NULL DEFAULT false,
    has_durable_provider_request_impossible boolean NOT NULL DEFAULT false,
    is_stale_unresolved boolean NOT NULL DEFAULT false,
    is_stale_request_sent_no_response boolean NOT NULL DEFAULT false,
    is_stale_claim_or_pre_call_only boolean NOT NULL DEFAULT false
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_provider_submit_diagnostic_chunk_flags (
    chunk_id,
    has_provider_acceptance_evidence,
    has_provider_response,
    has_provider_request_sent,
    has_provider_rejection,
    has_provider_unknown,
    has_provider_blocked_pre_call,
    has_provider_malformed_response,
    has_durable_provider_request_impossible,
    is_stale_unresolved,
    is_stale_request_sent_no_response,
    is_stale_claim_or_pre_call_only
  )
  SELECT chunk_eval.chunk_id,
         chunk_eval.has_provider_acceptance_evidence,
         chunk_eval.has_provider_response,
         chunk_eval.has_provider_request_sent,
         chunk_eval.has_provider_rejection,
         chunk_eval.has_provider_unknown OR (chunk_eval.is_stale_unresolved AND chunk_eval.has_provider_request_sent),
         chunk_eval.has_provider_blocked_pre_call,
         chunk_eval.has_provider_malformed_response,
         chunk_eval.has_durable_provider_request_impossible,
         chunk_eval.is_stale_unresolved,
         chunk_eval.is_stale_unresolved AND chunk_eval.has_provider_request_sent,
         chunk_eval.is_stale_unresolved AND chunk_eval.has_provider_request_sent IS NOT TRUE
  FROM (
    SELECT selected_chunk.chunk_id,
           (
             COALESCE(BOOL_OR(transfer_flag.has_provider_acceptance_evidence), false)
             OR COALESCE(BOOL_OR(
               NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(selected_source.chunk_id::text, '__NO_CHUNK_ID__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.request_id, '')), ''), '__NO_REQUEST_ID__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
               AND UPPER(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
               AND selected_source.provider_submission_rejected IS NOT TRUE
               AND selected_source.provider_submission_failed IS NOT TRUE
             ), false)
             OR COALESCE(BOOL_OR(
               NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(selected_source.chunk_id::text, '__NO_CHUNK_ID__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.request_id, '')), ''), '__NO_REQUEST_ID__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
               AND UPPER(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
               AND selected_source.provider_submission_rejected IS NOT TRUE
               AND selected_source.provider_submission_failed IS NOT TRUE
             ), false)
           ) AS has_provider_acceptance_evidence,
           (
             COALESCE(BOOL_OR(transfer_flag.has_provider_response), false)
             OR COALESCE(BOOL_OR(selected_source.provider_response_received OR selected_source.provider_response_present), false)
             OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('PROVIDER_SUBMISSION_ACCEPTED', 'PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')), false)
             OR COALESCE(BOOL_OR(NULLIF(BTRIM(COALESCE(selected_source.provider_http_status, selected_source.provider_error_code, '')), '') IS NOT NULL), false)
           ) AS has_provider_response,
           (
             COALESCE(BOOL_OR(transfer_flag.has_provider_request_sent), false)
             OR COALESCE(BOOL_OR(selected_source.provider_request_sent), false)
             OR COALESCE(BOOL_OR(selected_source.provider_response_received OR selected_source.provider_response_present), false)
             OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID', 'PROVIDER_SUBMISSION_ACCEPTED', 'PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE')), false)
             OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_call_stage, ''))) IN ('PROVIDER_PAYMENT_CREATE_REQUEST_SENT', 'PROVIDER_PAYMENT_CREATE_RESPONSE_RECEIVED', 'PROVIDER_PAYMENT_CREATE_ACCEPTED', 'PROVIDER_PAYMENT_CREATE_REJECTED', 'PROVIDER_PAYMENT_CREATE_UNKNOWN', 'PROVIDER_PAYMENT_CREATE_MALFORMED', 'RAIL_UPDATE_APPLY_STARTED', 'RAIL_UPDATE_APPLY_FAILED')), false)
           ) AS has_provider_request_sent,
           (
             COALESCE(BOOL_OR(transfer_flag.has_provider_rejection), false)
             OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED')), false)
             OR COALESCE(BOOL_OR(selected_source.provider_submission_rejected OR selected_source.provider_submission_failed), false)
             OR COALESCE(BOOL_OR(CASE WHEN NULLIF(BTRIM(COALESCE(selected_source.provider_http_status, '')), '') ~ '^[0-9]+$' THEN selected_source.provider_http_status::integer >= 400 ELSE false END), false)
           ) AS has_provider_rejection,
           (
             COALESCE(BOOL_OR(transfer_flag.has_provider_unknown), false)
             OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')), false)
             OR COALESCE(BOOL_OR(selected_source.provider_submission_unknown), false)
           ) AS has_provider_unknown,
           (
             COALESCE(BOOL_OR(transfer_flag.has_provider_blocked_pre_call), false)
             OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL'), false)
           ) AS has_provider_blocked_pre_call,
           (
             COALESCE(BOOL_OR(transfer_flag.has_provider_malformed_response), false)
             OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE'), false)
           ) AS has_provider_malformed_response,
           COALESCE(BOOL_OR(
             lower(BTRIM(COALESCE(selected_source.diagnostic->>'provider_request_impossible', selected_source.diagnostic->>'durable_provider_request_impossible', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
           ), false) AS has_durable_provider_request_impossible,
           (
             upper(BTRIM(COALESCE(selected_chunk.status, ''))) = 'RUNNING'
             AND upper(BTRIM(COALESCE(selected_chunk.chunk_type, ''))) = 'TRANSFER_SUBMIT'
             AND selected_chunk.completed_at_utc IS NULL
             AND (selected_chunk.lock_expires_at_utc IS NULL OR selected_chunk.lock_expires_at_utc < v_now)
             AND NOT (
               COALESCE(BOOL_OR(transfer_flag.has_provider_acceptance_evidence), false)
               OR COALESCE(BOOL_OR(
                 NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') IS NOT NULL
                 AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(selected_source.chunk_id::text, '__NO_CHUNK_ID__')
                 AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.request_id, '')), ''), '__NO_REQUEST_ID__')
                 AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
                 AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
                 AND UPPER(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
                 AND selected_source.provider_submission_rejected IS NOT TRUE
                 AND selected_source.provider_submission_failed IS NOT TRUE
               ), false)
               OR COALESCE(BOOL_OR(
                 NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') IS NOT NULL
                 AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(selected_source.chunk_id::text, '__NO_CHUNK_ID__')
                 AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.request_id, '')), ''), '__NO_REQUEST_ID__')
                 AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
                 AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
                 AND UPPER(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
                 AND selected_source.provider_submission_rejected IS NOT TRUE
                 AND selected_source.provider_submission_failed IS NOT TRUE
               ), false)
             )
             AND NOT (
               COALESCE(BOOL_OR(transfer_flag.has_provider_response), false)
               OR COALESCE(BOOL_OR(selected_source.provider_response_received OR selected_source.provider_response_present), false)
               OR COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) IN ('PROVIDER_SUBMISSION_ACCEPTED', 'PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')), false)
               OR COALESCE(BOOL_OR(NULLIF(BTRIM(COALESCE(selected_source.provider_http_status, selected_source.provider_error_code, '')), '') IS NOT NULL), false)
             )
             AND COALESCE(BOOL_OR(
               lower(BTRIM(COALESCE(selected_source.diagnostic->>'provider_request_impossible', selected_source.diagnostic->>'durable_provider_request_impossible', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
             ), false) IS NOT TRUE
           ) AS is_stale_unresolved
    FROM pg_temp.tmp_provider_submit_diagnostic_chunks AS selected_chunk
    LEFT JOIN pg_temp.tmp_provider_submit_diagnostic_chunk_transfer_ids AS selected_chunk_transfer
      ON selected_chunk_transfer.chunk_id = selected_chunk.chunk_id
    LEFT JOIN pg_temp.tmp_provider_submit_diagnostic_transfer_flags AS transfer_flag
      ON transfer_flag.transfer_id = selected_chunk_transfer.transfer_id
    LEFT JOIN pg_temp.tmp_provider_submit_diagnostic_sources AS selected_source
      ON selected_source.chunk_id = selected_chunk.chunk_id
    GROUP BY selected_chunk.chunk_id,
             selected_chunk.status,
             selected_chunk.chunk_type,
             selected_chunk.completed_at_utc,
             selected_chunk.lock_expires_at_utc
  ) AS chunk_eval
  ON CONFLICT (chunk_id) DO NOTHING;

  SELECT COUNT(*)::integer INTO v_operation_count FROM pg_temp.tmp_provider_submit_diagnostic_operations AS selected_operation;
  SELECT COUNT(*)::integer INTO v_chunk_count FROM pg_temp.tmp_provider_submit_diagnostic_chunks AS selected_chunk;
  SELECT COUNT(*)::integer INTO v_transfer_count FROM pg_temp.tmp_provider_submit_diagnostic_transfers AS selected_transfer;

  SELECT COUNT(*) FILTER (WHERE transfer_flag.has_provider_acceptance_evidence)::integer,
         COUNT(*) FILTER (WHERE transfer_flag.has_provider_response)::integer,
         COUNT(*) FILTER (WHERE transfer_flag.has_provider_request_sent)::integer,
         COUNT(*) FILTER (WHERE transfer_flag.has_provider_unknown)::integer,
         COUNT(*) FILTER (WHERE transfer_flag.has_provider_rejection)::integer,
         COUNT(*) FILTER (WHERE transfer_flag.has_provider_acceptance_evidence)::integer,
         COUNT(*) FILTER (WHERE transfer_flag.has_provider_blocked_pre_call)::integer,
         COUNT(*) FILTER (WHERE transfer_flag.has_provider_malformed_response)::integer
  INTO v_provider_acceptance_evidence_count,
       v_provider_response_present_count,
       v_provider_request_sent_count,
       v_provider_submission_unknown_count,
       v_provider_submission_rejected_count,
       v_provider_submission_accepted_count,
       v_provider_submission_blocked_pre_call_count,
       v_provider_submission_malformed_response_count
  FROM pg_temp.tmp_provider_submit_diagnostic_transfer_flags AS transfer_flag;

  SELECT GREATEST(COALESCE(v_provider_acceptance_evidence_count, 0), COALESCE(COUNT(*) FILTER (WHERE chunk_flag.has_provider_acceptance_evidence), 0))::integer,
         GREATEST(COALESCE(v_provider_response_present_count, 0), COALESCE(COUNT(*) FILTER (WHERE chunk_flag.has_provider_response), 0))::integer,
         GREATEST(COALESCE(v_provider_request_sent_count, 0), COALESCE(COUNT(*) FILTER (WHERE chunk_flag.has_provider_request_sent), 0))::integer,
         GREATEST(COALESCE(v_provider_submission_unknown_count, 0), COALESCE(COUNT(*) FILTER (WHERE chunk_flag.has_provider_unknown OR chunk_flag.is_stale_unresolved), 0))::integer,
         GREATEST(COALESCE(v_provider_submission_rejected_count, 0), COALESCE(COUNT(*) FILTER (WHERE chunk_flag.has_provider_rejection), 0))::integer,
         GREATEST(COALESCE(v_provider_submission_accepted_count, 0), COALESCE(COUNT(*) FILTER (WHERE chunk_flag.has_provider_acceptance_evidence), 0))::integer,
         GREATEST(COALESCE(v_provider_submission_blocked_pre_call_count, 0), COALESCE(COUNT(*) FILTER (WHERE chunk_flag.has_provider_blocked_pre_call), 0))::integer,
         GREATEST(COALESCE(v_provider_submission_malformed_response_count, 0), COALESCE(COUNT(*) FILTER (WHERE chunk_flag.has_provider_malformed_response), 0))::integer,
         COALESCE(COUNT(*) FILTER (WHERE chunk_flag.is_stale_unresolved), 0)::integer,
         COALESCE(COUNT(*) FILTER (WHERE chunk_flag.is_stale_request_sent_no_response), 0)::integer,
         COALESCE(COUNT(*) FILTER (WHERE upper(BTRIM(COALESCE(selected_chunk.status, ''))) = 'RUNNING'), 0)::integer,
         COALESCE(BOOL_OR(upper(BTRIM(COALESCE(selected_chunk.status, ''))) = 'RUNNING' AND (selected_chunk.lock_expires_at_utc IS NULL OR selected_chunk.lock_expires_at_utc < v_now)), false)
  INTO v_provider_acceptance_evidence_count,
       v_provider_response_present_count,
       v_provider_request_sent_count,
       v_provider_submission_unknown_count,
       v_provider_submission_rejected_count,
       v_provider_submission_accepted_count,
       v_provider_submission_blocked_pre_call_count,
       v_provider_submission_malformed_response_count,
       v_stale_unresolved_submit_chunk_count,
       v_request_sent_no_response_count,
       v_unfinalised_submit_chunk_count,
       v_chunk_lock_expired
  FROM pg_temp.tmp_provider_submit_diagnostic_chunks AS selected_chunk
  LEFT JOIN pg_temp.tmp_provider_submit_diagnostic_chunk_flags AS chunk_flag
    ON chunk_flag.chunk_id = selected_chunk.chunk_id;

  v_stale_empty_submit_chunk_count := COALESCE(v_stale_unresolved_submit_chunk_count, 0);

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_operation.operation_id::text) ORDER BY selected_operation.operation_id), '[]'::jsonb),
         MIN(selected_operation.operation_id)::text
  INTO v_operation_ids,
       v_primary_operation_id
  FROM pg_temp.tmp_provider_submit_diagnostic_operations AS selected_operation;

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_chunk.chunk_id::text) ORDER BY selected_chunk.chunk_id), '[]'::jsonb),
         MIN(selected_chunk.chunk_id)::text,
         MIN(selected_chunk.started_at_utc)::text,
         MIN(selected_chunk.completed_at_utc)::text,
         MIN(selected_chunk.lock_expires_at_utc)::text
  INTO v_chunk_ids,
       v_primary_chunk_id,
       v_chunk_started_at_utc,
       v_chunk_completed_at_utc,
       v_chunk_lock_expires_at_utc
  FROM pg_temp.tmp_provider_submit_diagnostic_chunks AS selected_chunk;

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_transfer.transfer_id::text) ORDER BY selected_transfer.transfer_id), '[]'::jsonb),
         MIN(selected_transfer.transfer_id)::text,
         MIN(selected_transfer.rail_provider),
         MIN(selected_transfer.rail_env),
         MIN(NULLIF(BTRIM(COALESCE(selected_transfer.rail_tx_id, '')), '')),
         MIN(NULLIF(BTRIM(COALESCE(selected_transfer.rail_state, '')), '')),
         MIN(NULLIF(BTRIM(COALESCE(selected_transfer.request_id, '')), ''))
  INTO v_transfer_ids,
       v_primary_transfer_id,
       v_primary_rail_provider,
       v_primary_rail_env,
       v_primary_rail_tx_id,
       v_primary_rail_state,
       v_primary_request_id
  FROM pg_temp.tmp_provider_submit_diagnostic_transfers AS selected_transfer;

  SELECT selected_source.provider_transaction_id,
         selected_source.provider_reference,
         selected_source.provider_state,
         COALESCE(selected_source.request_id, v_primary_request_id),
         COALESCE(selected_source.idempotency_key, v_primary_request_id),
         selected_source.local_provider_request_id,
         selected_source.provider_http_status,
         selected_source.provider_error_code,
         selected_source.provider_error_message_redacted,
         selected_source.provider_response_redacted,
         selected_source.provider_error_redacted,
         COALESCE(selected_source.provider_call_stage, v_provider_call_stage)
  INTO v_primary_provider_transaction_id,
       v_primary_provider_reference,
       v_primary_provider_state,
       v_primary_request_id,
       v_primary_idempotency_key,
       v_primary_local_provider_request_id,
       v_primary_provider_http_status,
       v_primary_provider_error_code,
       v_primary_provider_error_message_redacted,
       v_primary_provider_response_redacted,
       v_primary_provider_error_redacted,
       v_provider_call_stage
  FROM pg_temp.tmp_provider_submit_diagnostic_sources AS selected_source
  WHERE NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.provider_reference, selected_source.provider_state, selected_source.provider_error_code, selected_source.request_id, selected_source.idempotency_key, selected_source.provider_call_stage, '')), '') IS NOT NULL
  ORDER BY CASE
             WHEN (
               NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.request_id, '')), ''), '__NO_REQUEST_ID__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_transaction_id, selected_source.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
               AND UPPER(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
               AND selected_source.provider_submission_rejected IS NOT TRUE
               AND selected_source.provider_submission_failed IS NOT TRUE
             ) OR (
               NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.request_id, '')), ''), '__NO_REQUEST_ID__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
               AND NULLIF(BTRIM(COALESCE(selected_source.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(selected_source.local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
               AND UPPER(BTRIM(COALESCE(selected_source.provider_submission_status, ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
               AND selected_source.provider_submission_rejected IS NOT TRUE
               AND selected_source.provider_submission_failed IS NOT TRUE
             ) THEN 0
             WHEN selected_source.provider_response_present OR selected_source.provider_response_received THEN 1
             WHEN selected_source.provider_request_sent THEN 2
             ELSE 3
           END,
           selected_source.source_type
  LIMIT 1;

  IF v_primary_idempotency_key IS NULL THEN
    v_primary_idempotency_key := v_primary_request_id;
  END IF;

  IF v_primary_provider_reference IS NOT NULL
     AND v_primary_provider_reference IN (
       COALESCE(v_primary_request_id, '__NO_REQUEST_ID__'),
       COALESCE(v_primary_idempotency_key, '__NO_IDEMPOTENCY_KEY__'),
       COALESCE(v_primary_local_provider_request_id, '__NO_LOCAL_PROVIDER_REQUEST_ID__'),
       COALESCE(v_primary_transfer_id, '__NO_TRANSFER_ID__')
     ) THEN
    v_primary_provider_reference := NULL::text;
  END IF;

  IF v_primary_provider_transaction_id IS NOT NULL
     AND v_primary_provider_transaction_id IN (
       COALESCE(v_primary_request_id, '__NO_REQUEST_ID__'),
       COALESCE(v_primary_idempotency_key, '__NO_IDEMPOTENCY_KEY__'),
       COALESCE(v_primary_local_provider_request_id, '__NO_LOCAL_PROVIDER_REQUEST_ID__'),
       COALESCE(v_primary_transfer_id, '__NO_TRANSFER_ID__')
     ) THEN
    v_primary_provider_transaction_id := NULL::text;
  END IF;

  SELECT COALESCE(jsonb_agg(to_jsonb(scope_values.transfer_scope_id::text) ORDER BY scope_values.transfer_scope_id), '[]'::jsonb),
         MIN(scope_values.transfer_scope_id)::text
  INTO v_transfer_scope_ids,
       v_primary_transfer_scope_id
  FROM (
    SELECT DISTINCT transfer_scope.id AS transfer_scope_id
    FROM public.banking_pay_operation_transfer_scope AS transfer_scope
    WHERE (
        EXISTS (
          SELECT 1
          FROM pg_temp.tmp_provider_submit_diagnostic_operations AS selected_operation
          WHERE selected_operation.operation_id = transfer_scope.operation_id
        )
        OR (v_effective_pay_batch_id IS NOT NULL AND transfer_scope.pay_batch_id = v_effective_pay_batch_id)
      )
      AND (p_transfer_id IS NULL OR transfer_scope.pay_bank_transfer_id = p_transfer_id)
  ) AS scope_values;

  SELECT COUNT(*)::integer,
         COALESCE(jsonb_agg(to_jsonb(auth_values.auth_request_id::text) ORDER BY auth_values.auth_request_id), '[]'::jsonb),
         MIN(auth_values.auth_request_id)::text
  INTO v_active_auth_request_count,
       v_auth_request_ids,
       v_primary_auth_request_id
  FROM (
    SELECT DISTINCT auth_request.id AS auth_request_id
    FROM public.pay_batch_auth_requests AS auth_request
    WHERE v_effective_pay_batch_id IS NOT NULL
      AND auth_request.pay_batch_id = v_effective_pay_batch_id
      AND upper(BTRIM(COALESCE(auth_request.state, ''))) IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED')
      AND (p_operation_id IS NULL OR auth_request.execution_intent_json->>'operation_id' = p_operation_id::text)
  ) AS auth_values;

  IF COALESCE(v_provider_acceptance_evidence_count, 0) > 0 THEN
    v_provider_submission_status := 'PROVIDER_SUBMISSION_ACCEPTED';
    v_review_reason_code := 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT';
    v_provider_call_stage := COALESCE(v_provider_call_stage, 'PROVIDER_RESPONSE_RECEIVED');
    v_provider_submission_attempted := true;
    v_provider_request_sent := true;
    v_provider_response_received := true;
    v_provider_response_present := true;
    v_provider_submission_accepted := true;
    v_provider_acceptance_evidence_present := true;
    v_stale_submit_chunk := COALESCE(v_stale_empty_submit_chunk_count, 0) > 0;
    v_unfinalised_submit_chunk := COALESCE(v_unfinalised_submit_chunk_count, 0) > 0;
    v_manual_resolution_required := false;
    v_safe_retry_available := false;
    v_automatic_retry_blocked := true;
    v_retry_blocked_reason := 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT';
    v_recommended_action := 'Provider acceptance evidence exists. Do not retry unless reconciled.';
  ELSIF COALESCE(v_provider_submission_malformed_response_count, 0) > 0 THEN
    v_provider_submission_status := 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE';
    v_review_reason_code := 'PROVIDER_RESPONSE_MALFORMED';
    v_provider_submission_attempted := true;
    v_provider_request_sent := true;
    v_provider_response_received := true;
    v_provider_response_present := true;
    v_provider_submission_unknown := true;
    v_manual_resolution_required := true;
    v_safe_retry_available := false;
    v_automatic_retry_blocked := true;
    v_retry_blocked_reason := 'PROVIDER_RESPONSE_MALFORMED';
    v_recommended_action := 'Provider returned an unusable response. Check Revolut/bank records before retry.';
  ELSIF COALESCE(v_provider_submission_unknown_count, 0) > 0
     AND COALESCE(v_stale_unresolved_submit_chunk_count, 0) = 0 THEN
    v_provider_submission_status := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
    v_review_reason_code := 'PROVIDER_REQUEST_SENT_NO_RESPONSE';
    v_provider_submission_attempted := true;
    v_provider_request_sent := true;
    v_provider_response_received := false;
    v_provider_response_present := false;
    v_provider_submission_unknown := true;
    v_manual_resolution_required := true;
    v_safe_retry_available := false;
    v_automatic_retry_blocked := true;
    v_retry_blocked_reason := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
    v_recommended_action := 'Provider request may have been sent, but no usable response was recorded. Check Revolut/bank records before retry.';
  ELSIF COALESCE(v_request_sent_no_response_count, 0) > 0
     OR (
       COALESCE(v_provider_request_sent_count, 0) > 0
       AND COALESCE(v_provider_response_present_count, 0) = 0
       AND COALESCE(v_provider_acceptance_evidence_count, 0) = 0
     ) THEN
    v_provider_submission_status := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
    v_review_reason_code := 'PROVIDER_REQUEST_SENT_NO_RESPONSE';
    v_provider_submission_attempted := true;
    v_provider_request_sent := true;
    v_provider_response_received := false;
    v_provider_response_present := false;
    v_provider_submission_unknown := true;
    v_stale_submit_chunk := COALESCE(v_stale_unresolved_submit_chunk_count, 0) > 0;
    v_unfinalised_submit_chunk := COALESCE(v_unfinalised_submit_chunk_count, 0) > 0;
    v_manual_resolution_required := true;
    v_safe_retry_available := false;
    v_automatic_retry_blocked := true;
    v_retry_blocked_reason := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
    v_recommended_action := 'Provider request may have been sent, but no usable response was recorded. Check Revolut/bank records before retry.';
  ELSIF COALESCE(v_stale_unresolved_submit_chunk_count, 0) > 0 THEN
    v_provider_submission_status := 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK';
    v_review_reason_code := 'STALE_RUNNING_PROVIDER_SUBMIT_CHUNK';
    v_provider_call_stage := COALESCE(v_provider_call_stage, 'STALE_RUNNING_PROVIDER_SUBMIT_CHUNK');
    v_provider_submission_attempted := NULL::boolean;
    v_provider_request_sent := NULL::boolean;
    v_provider_response_received := false;
    v_provider_response_present := false;
    v_provider_submission_unknown := true;
    v_stale_submit_chunk := true;
    v_unfinalised_submit_chunk := true;
    v_manual_resolution_required := true;
    v_safe_retry_available := false;
    v_automatic_retry_blocked := true;
    v_retry_blocked_reason := 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK';
    v_recommended_action := 'Check Revolut/bank records before retry. If no payment was made, record manual no-payment confirmation and reset for retry.';
  ELSIF COALESCE(v_provider_submission_rejected_count, 0) > 0 THEN
    v_provider_submission_status := 'PROVIDER_SUBMISSION_REJECTED';
    v_review_reason_code := 'PROVIDER_REJECTED_PAYMENT';
    v_provider_submission_attempted := true;
    v_provider_request_sent := true;
    v_provider_response_received := true;
    v_provider_response_present := true;
    v_provider_submission_rejected := true;
    v_provider_submission_failed := true;
    v_manual_resolution_required := false;
    v_safe_retry_available := false;
    v_recommended_action := 'Review the provider rejection/error and retry only after correction.';
  ELSIF COALESCE(v_provider_submission_blocked_pre_call_count, 0) > 0 THEN
    v_provider_submission_status := 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL';
    v_review_reason_code := 'PROVIDER_SUBMIT_BLOCKED_PRE_CALL';
    v_provider_submission_attempted := false;
    v_provider_request_sent := false;
    v_provider_response_received := false;
    v_provider_response_present := false;
    v_provider_submission_failed := true;
    v_recommended_action := 'Provider was not called. Fix the local blocker and retry if cleanup allows.';
  END IF;

  v_counts := jsonb_build_object(
    'operation_count', COALESCE(v_operation_count, 0),
    'chunk_count', COALESCE(v_chunk_count, 0),
    'transfer_count', COALESCE(v_transfer_count, 0),
    'active_auth_request_count', COALESCE(v_active_auth_request_count, 0),
    'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
    'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
    'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
    'provider_submission_unknown_count', COALESCE(v_provider_submission_unknown_count, 0),
    'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
    'stale_empty_submit_chunk_count', COALESCE(v_stale_empty_submit_chunk_count, 0),
    'request_sent_no_response_count', COALESCE(v_request_sent_no_response_count, 0),
    'unfinalised_submit_chunk_count', COALESCE(v_unfinalised_submit_chunk_count, 0),
    'provider_submission_rejected_count', COALESCE(v_provider_submission_rejected_count, 0),
    'provider_submission_accepted_count', COALESCE(v_provider_submission_accepted_count, 0),
    'provider_submission_blocked_pre_call_count', COALESCE(v_provider_submission_blocked_pre_call_count, 0),
    'provider_submission_malformed_response_count', COALESCE(v_provider_submission_malformed_response_count, 0),
    'provider_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
    'provider_attempt_or_evidence_count', COALESCE(v_provider_request_sent_count, 0) + COALESCE(v_provider_response_present_count, 0) + COALESCE(v_provider_acceptance_evidence_count, 0),
    'attempted_but_unproven_count', COALESCE(v_provider_submission_unknown_count, 0)
  );

  v_ids := jsonb_build_object(
    'operation_ids', COALESCE(v_operation_ids, '[]'::jsonb),
    'chunk_ids', COALESCE(v_chunk_ids, '[]'::jsonb),
    'transfer_ids', COALESCE(v_transfer_ids, '[]'::jsonb),
    'transfer_scope_ids', COALESCE(v_transfer_scope_ids, '[]'::jsonb),
    'auth_request_ids', COALESCE(v_auth_request_ids, '[]'::jsonb)
  );

  v_provider_submit_diagnostic := jsonb_build_object(
    'diagnostic_version', 1,
    'generated_at_utc', v_now::text,
    'review_reason_code', v_review_reason_code,
    'provider_submission_status', v_provider_submission_status,
    'provider_call_stage', v_provider_call_stage,
    'provider_submission_attempted', v_provider_submission_attempted,
    'provider_request_sent', v_provider_request_sent,
    'provider_response_received', v_provider_response_received,
    'provider_response_present', v_provider_response_present,
    'provider_submission_accepted', v_provider_submission_accepted,
    'provider_submission_rejected', v_provider_submission_rejected,
    'provider_submission_failed', v_provider_submission_failed,
    'provider_submission_unknown', v_provider_submission_unknown,
    'provider_acceptance_evidence_present', v_provider_acceptance_evidence_present,
    'stale_submit_chunk', v_stale_submit_chunk,
    'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
    'stale_empty_submit_chunk_count', COALESCE(v_stale_empty_submit_chunk_count, 0),
    'request_sent_no_response_count', COALESCE(v_request_sent_no_response_count, 0),
    'unfinalised_submit_chunk', v_unfinalised_submit_chunk,
    'chunk_lock_expired', COALESCE(v_chunk_lock_expired, false),
    'chunk_started_at_utc', v_chunk_started_at_utc,
    'chunk_completed_at_utc', v_chunk_completed_at_utc,
    'chunk_lock_expires_at_utc', v_chunk_lock_expires_at_utc,
    'manual_resolution_required', v_manual_resolution_required,
    'safe_retry_available', v_safe_retry_available,
    'automatic_retry_blocked', v_automatic_retry_blocked,
    'retry_blocked_reason', v_retry_blocked_reason,
    'recommended_action', v_recommended_action,
    'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
    'operation_id', v_primary_operation_id,
    'operation_ids', v_operation_ids,
    'chunk_id', v_primary_chunk_id,
    'chunk_ids', v_chunk_ids,
    'transfer_id', v_primary_transfer_id,
    'transfer_ids', v_transfer_ids,
    'transfer_scope_id', v_primary_transfer_scope_id,
    'transfer_scope_ids', v_transfer_scope_ids,
    'auth_request_id', v_primary_auth_request_id,
    'auth_request_ids', v_auth_request_ids,
    'rail_provider', v_primary_rail_provider,
    'rail_env', v_primary_rail_env,
    'rail_tx_id', v_primary_rail_tx_id,
    'rail_state', v_primary_rail_state,
    'provider_transaction_id', v_primary_provider_transaction_id,
    'provider_reference', v_primary_provider_reference,
    'provider_state', v_primary_provider_state,
    'request_id', v_primary_request_id,
    'idempotency_key', v_primary_idempotency_key,
    'local_provider_request_id', v_primary_local_provider_request_id,
    'provider_http_status', v_primary_provider_http_status,
    'provider_error_code', v_primary_provider_error_code,
    'provider_error_message_redacted', v_primary_provider_error_message_redacted,
    'provider_response_redacted', v_primary_provider_response_redacted,
    'provider_error_redacted', v_primary_provider_error_redacted
  );

  RETURN jsonb_build_object(
    'ok', true,
    'provider_submit_diagnostic', v_provider_submit_diagnostic,
    'provider_submission_status', v_provider_submission_status,
    'review_reason_code', v_review_reason_code,
    'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
    'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
    'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
    'provider_submission_unknown_count', COALESCE(v_provider_submission_unknown_count, 0),
    'stale_unresolved_submit_chunk_count', COALESCE(v_stale_unresolved_submit_chunk_count, 0),
    'stale_empty_submit_chunk_count', COALESCE(v_stale_empty_submit_chunk_count, 0),
    'unfinalised_submit_chunk_count', COALESCE(v_unfinalised_submit_chunk_count, 0),
    'manual_resolution_required', v_manual_resolution_required,
    'safe_retry_available', v_safe_retry_available,
    'recommended_action', v_recommended_action,
    'counts', v_counts,
    'ids', v_ids,
    'provider_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
    'provider_attempt_or_evidence_count', COALESCE(v_provider_request_sent_count, 0) + COALESCE(v_provider_response_present_count, 0) + COALESCE(v_provider_acceptance_evidence_count, 0),
    'attempted_but_unproven_count', COALESCE(v_provider_submission_unknown_count, 0),
    'counts_only', COALESCE(p_counts_only, false)
  );

END;
$function$;









CREATE OR REPLACE FUNCTION public.pay_provider_submit_chunk_diagnostic_finalise(
  p_operation_id uuid,
  p_pay_batch_id uuid DEFAULT NULL::uuid,
  p_chunk_id uuid DEFAULT NULL::uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_reason_code text DEFAULT NULL::text,
  p_failure_error_json jsonb DEFAULT '{}'::jsonb
)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_effective_pay_batch_id uuid := NULL::uuid;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_reason_code text := NULL::text;
  v_failure_error_json jsonb := '{}'::jsonb;
  v_diagnostic_result jsonb := '{}'::jsonb;
  v_provider_submit_diagnostic jsonb := '{}'::jsonb;
  v_provider_submission_status text := NULL::text;
  v_review_reason_code text := NULL::text;
  v_target_chunk_row public.banking_pay_operation_chunks%ROWTYPE;
  v_after_chunk_row public.banking_pay_operation_chunks%ROWTYPE;
  v_finish_status text := 'FAILED';
  v_completed_count integer := 0;
  v_failed_count integer := 0;
  v_affected_transfer_count integer := 0;
  v_result_json jsonb := NULL::jsonb;
  v_error_json jsonb := NULL::jsonb;
  v_finished boolean := false;
  v_not_finished_reason text := NULL::text;
  v_finalised_chunk_count integer := 0;
  v_already_terminal_chunk_count integer := 0;
  v_finalised_chunk_ids jsonb := '[]'::jsonb;
  v_already_terminal_chunk_ids jsonb := '[]'::jsonb;
  v_before_json jsonb := '{}'::jsonb;
  v_after_json jsonb := '{}'::jsonb;
  v_message text := NULL::text;
  v_chunk_diagnostic_result jsonb := '{}'::jsonb;
  v_chunk_provider_submit_diagnostic jsonb := '{}'::jsonb;
  v_chunk_provider_submission_status text := NULL::text;
  v_chunk_review_reason_code text := NULL::text;
  v_chunk_provider_acceptance_evidence_count integer := 0;
  v_chunk_provider_response_present_count integer := 0;
  v_chunk_provider_request_sent_count integer := 0;
  v_chunk_stale_unresolved_submit_chunk_count integer := 0;
  v_chunk_unfinalised_submit_chunk_count integer := 0;
  v_chunk_transfer_ids jsonb := '[]'::jsonb;
  v_direct_provider_acceptance_evidence_count integer := 0;
  v_direct_provider_response_present_count integer := 0;
  v_direct_provider_request_sent_count integer := 0;
  v_direct_stale_unresolved boolean := false;
  v_diagnostic_before jsonb := '{}'::jsonb;
  v_diagnostic_after jsonb := '{}'::jsonb;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_DIAGNOSTIC_FINALISE',
      'code', 'OPERATION_ID_REQUIRED',
      'message', 'pay_provider_submit_chunk_diagnostic_finalise requires p_operation_id'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_failure_error_json IS NOT NULL AND jsonb_typeof(p_failure_error_json) = 'object' THEN
    v_failure_error_json := p_failure_error_json;
  ELSE
    v_failure_error_json := '{}'::jsonb;
  END IF;

  v_reason_code := COALESCE(NULLIF(BTRIM(COALESCE(p_reason_code, '')), ''), 'PROVIDER_SUBMIT_CHUNK_DIAGNOSTIC_FINALISE');

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_DIAGNOSTIC_FINALISE',
      'code', 'OPERATION_NOT_FOUND',
      'message', 'pay_provider_submit_chunk_diagnostic_finalise: operation not found',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_effective_pay_batch_id := COALESCE(p_pay_batch_id, v_operation_row.pay_batch_id);
  v_effective_actor_user_id := COALESCE(p_actor_user_id, v_operation_row.actor_user_id);

  IF p_pay_batch_id IS NOT NULL AND v_operation_row.pay_batch_id IS NOT NULL AND p_pay_batch_id <> v_operation_row.pay_batch_id THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_DIAGNOSTIC_FINALISE',
      'code', 'OPERATION_BATCH_MISMATCH',
      'message', 'pay_provider_submit_chunk_diagnostic_finalise: operation belongs to a different pay batch',
      'operation_id', p_operation_id::text,
      'operation_pay_batch_id', v_operation_row.pay_batch_id::text,
      'pay_batch_id', p_pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_row.operation_type NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RETURN jsonb_build_object(
      'ok', true,
      'operation_id', p_operation_id::text,
      'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
      'finalised_chunk_count', 0,
      'finalised_chunk_ids', '[]'::jsonb,
      'reason', 'NOT_PROVIDER_SUBMIT_OPERATION',
      'operation_type', v_operation_row.operation_type
    );
  END IF;

  v_diagnostic_result := public.pay_provider_submit_diagnostic_get(
    p_pay_batch_id := v_effective_pay_batch_id,
    p_operation_id := p_operation_id,
    p_transfer_id := NULL::uuid,
    p_chunk_id := p_chunk_id,
    p_counts_only := false
  );

  v_provider_submit_diagnostic := COALESCE(v_diagnostic_result->'provider_submit_diagnostic', '{}'::jsonb);

  IF jsonb_typeof(v_failure_error_json->'provider_submit_diagnostic') = 'object' THEN
    v_provider_submit_diagnostic := jsonb_strip_nulls(
      v_provider_submit_diagnostic
      || (v_failure_error_json->'provider_submit_diagnostic')
      || jsonb_build_object(
        'diagnostic_version', 1,
        'generated_at_utc', v_now::text,
        'operation_id', p_operation_id::text,
        'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END
      )
    );
  END IF;

  v_provider_submission_status := NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'provider_submission_status', v_diagnostic_result->>'provider_submission_status', '')), '');
  v_review_reason_code := NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'review_reason_code', v_diagnostic_result->>'review_reason_code', '')), '');

  IF v_provider_submission_status IS NULL THEN
    v_provider_submission_status := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
  END IF;

  IF v_review_reason_code IS NULL THEN
    v_review_reason_code := CASE
      WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'STALE_RUNNING_PROVIDER_SUBMIT_CHUNK'
      WHEN v_provider_submission_status = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'PROVIDER_REQUEST_SENT_NO_RESPONSE'
      WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_REJECTED' THEN 'PROVIDER_REJECTED_PAYMENT'
      WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'PROVIDER_RESPONSE_MALFORMED'
      WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'PROVIDER_SUBMIT_BLOCKED_PRE_CALL'
      WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT'
      ELSE 'MANUAL_BANK_CHECK_REQUIRED'
    END;
  END IF;

  IF COALESCE(v_diagnostic_result #>> '{counts,transfer_count}', '') ~ '^[0-9]+$' THEN
    v_affected_transfer_count := (v_diagnostic_result #>> '{counts,transfer_count}')::integer;
  ELSE
    v_affected_transfer_count := 0;
  END IF;

  FOR v_target_chunk_row IN
    SELECT operation_chunk.*
    FROM public.banking_pay_operation_chunks AS operation_chunk
    WHERE operation_chunk.operation_id = p_operation_id
      AND (operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS' OR operation_chunk.chunk_type = 'TRANSFER_SUBMIT')
      AND (p_chunk_id IS NULL OR operation_chunk.id = p_chunk_id)
      AND operation_chunk.status = 'RUNNING'
    ORDER BY operation_chunk.sequence_no, operation_chunk.created_at_utc NULLS FIRST, operation_chunk.id
    FOR UPDATE
  LOOP
    v_before_json := to_jsonb(v_target_chunk_row);
    v_diagnostic_before := COALESCE(v_target_chunk_row.result_json->'provider_submit_diagnostic', v_target_chunk_row.error_json->'provider_submit_diagnostic', '{}'::jsonb);

    v_chunk_diagnostic_result := public.pay_provider_submit_diagnostic_get(
      p_pay_batch_id := v_effective_pay_batch_id,
      p_operation_id := p_operation_id,
      p_transfer_id := NULL::uuid,
      p_chunk_id := v_target_chunk_row.id,
      p_counts_only := false
    );

    v_chunk_provider_submit_diagnostic := COALESCE(v_chunk_diagnostic_result->'provider_submit_diagnostic', '{}'::jsonb);
    v_chunk_provider_submission_status := NULLIF(BTRIM(COALESCE(v_chunk_provider_submit_diagnostic->>'provider_submission_status', v_chunk_diagnostic_result->>'provider_submission_status', '')), '');
    v_chunk_review_reason_code := NULLIF(BTRIM(COALESCE(v_chunk_provider_submit_diagnostic->>'review_reason_code', v_chunk_diagnostic_result->>'review_reason_code', '')), '');

    IF COALESCE(v_chunk_diagnostic_result #>> '{counts,provider_acceptance_evidence_count}', '') ~ '^[0-9]+$' THEN
      v_chunk_provider_acceptance_evidence_count := (v_chunk_diagnostic_result #>> '{counts,provider_acceptance_evidence_count}')::integer;
    ELSE
      v_chunk_provider_acceptance_evidence_count := 0;
    END IF;

    IF COALESCE(v_chunk_diagnostic_result #>> '{counts,provider_response_present_count}', '') ~ '^[0-9]+$' THEN
      v_chunk_provider_response_present_count := (v_chunk_diagnostic_result #>> '{counts,provider_response_present_count}')::integer;
    ELSE
      v_chunk_provider_response_present_count := 0;
    END IF;

    IF COALESCE(v_chunk_diagnostic_result #>> '{counts,provider_request_sent_count}', '') ~ '^[0-9]+$' THEN
      v_chunk_provider_request_sent_count := (v_chunk_diagnostic_result #>> '{counts,provider_request_sent_count}')::integer;
    ELSE
      v_chunk_provider_request_sent_count := 0;
    END IF;

    IF COALESCE(v_chunk_diagnostic_result #>> '{counts,stale_unresolved_submit_chunk_count}', v_chunk_diagnostic_result #>> '{counts,stale_empty_submit_chunk_count}', '') ~ '^[0-9]+$' THEN
      v_chunk_stale_unresolved_submit_chunk_count := COALESCE(v_chunk_diagnostic_result #>> '{counts,stale_unresolved_submit_chunk_count}', v_chunk_diagnostic_result #>> '{counts,stale_empty_submit_chunk_count}')::integer;
    ELSE
      v_chunk_stale_unresolved_submit_chunk_count := 0;
    END IF;

    IF COALESCE(v_chunk_diagnostic_result #>> '{counts,unfinalised_submit_chunk_count}', '') ~ '^[0-9]+$' THEN
      v_chunk_unfinalised_submit_chunk_count := (v_chunk_diagnostic_result #>> '{counts,unfinalised_submit_chunk_count}')::integer;
    ELSE
      v_chunk_unfinalised_submit_chunk_count := 0;
    END IF;

    SELECT COALESCE(jsonb_agg(to_jsonb(chunk_transfer.transfer_id::text) ORDER BY chunk_transfer.transfer_id), '[]'::jsonb),
           COUNT(*) FILTER (
             WHERE (
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NOT NULL
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') <> transfer_row.id::text
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
                  AND UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
                )
                OR (
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') IS NOT NULL
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') <> transfer_row.id::text
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
                  AND UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
                )
                OR (
                  NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}', '')), '') IS NOT NULL
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}', '')), '') <> transfer_row.id::text
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
                  AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
                  AND UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
                )
          )::integer,
           COUNT(*) FILTER (
             WHERE lower(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_response_present}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_response_received}', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
                OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_http_status}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_error_code}', '')), '') IS NOT NULL
                OR EXISTS (
                  SELECT 1
                  FROM public.pay_bank_transfer_events AS transfer_event_check
                  WHERE transfer_event_check.pay_bank_transfer_id = transfer_row.id
                    AND UPPER(BTRIM(COALESCE(transfer_event_check.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')
                )
           )::integer,
           COUNT(*) FILTER (
             WHERE lower(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_request_sent}', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
                OR NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NOT NULL
                OR EXISTS (
                  SELECT 1
                  FROM public.pay_bank_transfer_events AS transfer_event_check
                  WHERE transfer_event_check.pay_bank_transfer_id = transfer_row.id
                    AND UPPER(BTRIM(COALESCE(transfer_event_check.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')
                )
           )::integer
    INTO v_chunk_transfer_ids,
         v_direct_provider_acceptance_evidence_count,
         v_direct_provider_response_present_count,
         v_direct_provider_request_sent_count
    FROM (
      SELECT DISTINCT transfer_text.transfer_id_text::uuid AS transfer_id
      FROM (
        SELECT transfer_id_element.value AS transfer_id_text
        FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_target_chunk_row.payload_json->'transfer_ids') = 'array' THEN v_target_chunk_row.payload_json->'transfer_ids' ELSE '[]'::jsonb END) AS transfer_id_element(value)
        UNION ALL
        SELECT COALESCE(transfer_payload.value->>'pay_bank_transfer_id', transfer_payload.value->>'transfer_id', transfer_payload.value->>'id') AS transfer_id_text
        FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_target_chunk_row.payload_json->'transfers') = 'array' THEN v_target_chunk_row.payload_json->'transfers' ELSE '[]'::jsonb END) AS transfer_payload(value)
      ) AS transfer_text
      WHERE NULLIF(BTRIM(COALESCE(transfer_text.transfer_id_text, '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) AS chunk_transfer
    JOIN public.pay_bank_transfers AS transfer_row
      ON transfer_row.id = chunk_transfer.transfer_id
     AND (v_effective_pay_batch_id IS NULL OR transfer_row.pay_batch_id = v_effective_pay_batch_id);

    v_direct_provider_acceptance_evidence_count := GREATEST(COALESCE(v_direct_provider_acceptance_evidence_count, 0), COALESCE(v_chunk_provider_acceptance_evidence_count, 0));
    v_direct_provider_response_present_count := GREATEST(COALESCE(v_direct_provider_response_present_count, 0), COALESCE(v_chunk_provider_response_present_count, 0));
    v_direct_provider_request_sent_count := GREATEST(COALESCE(v_direct_provider_request_sent_count, 0), COALESCE(v_chunk_provider_request_sent_count, 0));
    v_direct_stale_unresolved := UPPER(BTRIM(COALESCE(v_target_chunk_row.chunk_type, ''))) = 'TRANSFER_SUBMIT'
      AND UPPER(BTRIM(COALESCE(v_target_chunk_row.status, ''))) = 'RUNNING'
      AND v_target_chunk_row.completed_at_utc IS NULL
      AND (v_target_chunk_row.lock_expires_at_utc IS NULL OR v_target_chunk_row.lock_expires_at_utc < v_now)
      AND COALESCE(v_direct_provider_acceptance_evidence_count, 0) = 0
      AND COALESCE(v_direct_provider_response_present_count, 0) = 0
      AND lower(BTRIM(COALESCE(v_chunk_provider_submit_diagnostic->>'provider_request_impossible', v_chunk_provider_submit_diagnostic->>'durable_provider_request_impossible', v_provider_submit_diagnostic->>'provider_request_impossible', v_provider_submit_diagnostic->>'durable_provider_request_impossible', ''))) NOT IN ('true', 't', '1', 'yes', 'y', 'on');

    IF v_chunk_provider_submit_diagnostic <> '{}'::jsonb THEN
      v_provider_submit_diagnostic := jsonb_strip_nulls(v_provider_submit_diagnostic || v_chunk_provider_submit_diagnostic);
      v_provider_submission_status := COALESCE(v_chunk_provider_submission_status, v_provider_submission_status);
      v_review_reason_code := COALESCE(v_chunk_review_reason_code, v_review_reason_code);
    END IF;

    IF v_direct_stale_unresolved IS TRUE THEN
      IF COALESCE(v_direct_provider_request_sent_count, 0) > 0
         OR lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'provider_request_sent', ''))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
        v_provider_submission_status := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
        v_review_reason_code := 'PROVIDER_REQUEST_SENT_NO_RESPONSE';
      ELSE
        v_provider_submission_status := 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK';
        v_review_reason_code := 'STALE_RUNNING_PROVIDER_SUBMIT_CHUNK';
      END IF;
      v_provider_submit_diagnostic := jsonb_strip_nulls(
        v_provider_submit_diagnostic || jsonb_build_object(
          'diagnostic_version', 1,
          'generated_at_utc', v_now::text,
          'provider_submission_status', v_provider_submission_status,
          'review_reason_code', v_review_reason_code,
          'provider_response_present', false,
          'provider_acceptance_evidence_present', false,
          'provider_submission_unknown', true,
          'stale_submit_chunk', true,
          'unfinalised_submit_chunk', true,
          'chunk_lock_expired', true,
          'manual_resolution_required', true,
          'safe_retry_available', false,
          'automatic_retry_blocked', true,
          'retry_blocked_reason', v_provider_submission_status,
          'recommended_action', 'Check Revolut/bank records before retry. If no payment was made, record manual no-payment confirmation and reset for retry.',
          'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
          'operation_id', p_operation_id::text,
          'chunk_id', v_target_chunk_row.id::text,
          'chunk_ids', jsonb_build_array(v_target_chunk_row.id::text),
          'transfer_ids', COALESCE(v_chunk_transfer_ids, '[]'::jsonb),
          'provider_acceptance_evidence_count', COALESCE(v_direct_provider_acceptance_evidence_count, 0),
          'provider_response_present_count', COALESCE(v_direct_provider_response_present_count, 0),
          'provider_request_sent_count', COALESCE(v_direct_provider_request_sent_count, 0),
          'stale_unresolved_submit_chunk_count', GREATEST(COALESCE(v_chunk_stale_unresolved_submit_chunk_count, 0), 1),
          'stale_empty_submit_chunk_count', GREATEST(COALESCE(v_chunk_stale_unresolved_submit_chunk_count, 0), 1),
          'unfinalised_submit_chunk_count', GREATEST(COALESCE(v_chunk_unfinalised_submit_chunk_count, 0), 1)
        )
      );
    END IF;

    IF v_provider_submission_status = 'PROVIDER_SUBMISSION_ACCEPTED'
       AND COALESCE(v_direct_provider_acceptance_evidence_count, 0) <= 0 THEN
      v_provider_submission_status := 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID';
      v_review_reason_code := 'PROVIDER_RESPONSE_PRESENT_NO_EXTERNAL_ID';
      v_provider_submit_diagnostic := jsonb_strip_nulls(
        v_provider_submit_diagnostic || jsonb_build_object(
          'provider_submission_status', v_provider_submission_status,
          'review_reason_code', v_review_reason_code,
          'provider_submission_accepted', false,
          'provider_acceptance_evidence_present', false,
          'provider_submission_unknown', true,
          'manual_resolution_required', true,
          'safe_retry_available', false,
          'automatic_retry_blocked', true,
          'retry_blocked_reason', v_provider_submission_status,
          'recommended_action', 'Provider response/status did not include usable external acceptance evidence. Manually reconcile before retry.'
        )
      );
    END IF;

    IF v_provider_submission_status = 'PROVIDER_SUBMISSION_ACCEPTED' THEN
      v_finish_status := 'COMPLETE';
      v_completed_count := COALESCE(NULLIF(v_target_chunk_row.completed_count, 0), NULLIF(v_target_chunk_row.unit_count, 0), NULLIF(v_affected_transfer_count, 0), 0);
      v_failed_count := 0;
      v_message := 'Provider acceptance evidence was found while finalising the provider-submit chunk diagnostic.';
      v_result_json := COALESCE(v_target_chunk_row.result_json, '{}'::jsonb)
        || jsonb_build_object(
          'code', v_provider_submission_status,
          'message', v_message,
          'provider_submit_diagnostic', v_provider_submit_diagnostic,
          'diagnostic_finalised_at_utc', v_now,
          'diagnostic_finalise_reason_code', v_reason_code
        );
      v_error_json := v_target_chunk_row.error_json;
    ELSE
      v_finish_status := 'FAILED';
      v_completed_count := COALESCE(v_target_chunk_row.completed_count, 0);
      v_failed_count := GREATEST(
        COALESCE(v_target_chunk_row.failed_count, 0),
        COALESCE(v_affected_transfer_count, 0),
        COALESCE(v_target_chunk_row.unit_count, 0),
        1
      );
      v_message := CASE
        WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Provider submit chunk became stale before any usable provider response was recorded.'
        WHEN v_provider_submission_status = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'Provider request may have been sent, but no usable provider response was recorded.'
        WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider returned an unusable response. Manual reconciliation is required before retry.'
        WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider rejected the payment submission.'
        WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider submission was blocked before the provider payment request was sent.'
        ELSE 'Provider-submit chunk was finalised with a provider-submit diagnostic.'
      END;
      v_result_json := COALESCE(v_target_chunk_row.result_json, '{}'::jsonb);
      v_error_json := COALESCE(v_target_chunk_row.error_json, '{}'::jsonb)
        || jsonb_build_object(
          'code', COALESCE(v_review_reason_code, v_provider_submission_status),
          'provider_submission_status', v_provider_submission_status,
          'message', v_message,
          'provider_submit_diagnostic', v_provider_submit_diagnostic,
          'recommended_action', COALESCE(v_provider_submit_diagnostic->>'recommended_action', 'Check Revolut/bank records before retry. If no payment was made, record manual no-payment confirmation and reset for retry.'),
          'manual_resolution_required', lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'manual_resolution_required', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'safe_retry_available', lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'safe_retry_available', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
          'failure_error_json', v_failure_error_json,
          'diagnostic_finalised_at_utc', v_now,
          'diagnostic_finalise_reason_code', v_reason_code
        );
    END IF;

    SELECT finish_result.finished,
           finish_result.not_finished_reason
    INTO v_finished,
         v_not_finished_reason
    FROM public.banking_pay_operation_finish_chunk(
      p_chunk_id := v_target_chunk_row.id,
      p_status := v_finish_status,
      p_completed_count := v_completed_count,
      p_failed_count := v_failed_count,
      p_result_json := v_result_json,
      p_error_json := v_error_json
    ) AS finish_result
    LIMIT 1;

    SELECT operation_chunk_after.*
    INTO v_after_chunk_row
    FROM public.banking_pay_operation_chunks AS operation_chunk_after
    WHERE operation_chunk_after.id = v_target_chunk_row.id;

    v_after_json := to_jsonb(v_after_chunk_row);

    IF COALESCE(v_finished, false) IS TRUE THEN
      v_finalised_chunk_count := COALESCE(v_finalised_chunk_count, 0) + 1;
      v_finalised_chunk_ids := COALESCE(v_finalised_chunk_ids, '[]'::jsonb) || jsonb_build_array(v_target_chunk_row.id::text);
    ELSE
      IF COALESCE(v_not_finished_reason, '') = 'ALREADY_TERMINAL' THEN
        v_already_terminal_chunk_count := COALESCE(v_already_terminal_chunk_count, 0) + 1;
        v_already_terminal_chunk_ids := COALESCE(v_already_terminal_chunk_ids, '[]'::jsonb) || jsonb_build_array(v_target_chunk_row.id::text);
      END IF;
    END IF;

    BEGIN
      PERFORM public._audit_insert(
        'banking_pay_operation_chunks',
        v_target_chunk_row.id::text,
        'PAYMENT_PROVIDER_SUBMIT_CHUNK_DIAGNOSTIC_FINALISED',
        v_before_json,
        jsonb_build_object(
          'chunk', v_after_json,
          'previous_chunk_status', v_target_chunk_row.status,
          'new_chunk_status', CASE WHEN v_after_chunk_row.id IS NULL THEN NULL ELSE v_after_chunk_row.status END,
          'previous_lock_expires_at_utc', CASE WHEN v_target_chunk_row.lock_expires_at_utc IS NULL THEN NULL ELSE v_target_chunk_row.lock_expires_at_utc::text END,
          'provider_evidence_counts', jsonb_build_object(
            'provider_acceptance_evidence_count', COALESCE(v_direct_provider_acceptance_evidence_count, 0),
            'provider_response_present_count', COALESCE(v_direct_provider_response_present_count, 0),
            'provider_request_sent_count', COALESCE(v_direct_provider_request_sent_count, 0),
            'stale_unresolved_submit_chunk_count', COALESCE(v_chunk_stale_unresolved_submit_chunk_count, 0),
            'unfinalised_submit_chunk_count', COALESCE(v_chunk_unfinalised_submit_chunk_count, 0)
          ),
          'diagnostic_before', v_diagnostic_before,
          'diagnostic_after', v_provider_submit_diagnostic,
          'affected_transfer_ids', COALESCE(v_chunk_transfer_ids, '[]'::jsonb),
          'provider_submit_diagnostic', v_provider_submit_diagnostic,
          'provider_submission_status', v_provider_submission_status,
          'review_reason_code', v_review_reason_code,
          'finish_status', v_finish_status,
          'reason_code', v_reason_code
        ),
        'payment_provider_submit_chunk_diagnostic_finalised',
        v_effective_actor_user_id
      );
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  END LOOP;

  IF COALESCE(v_finalised_chunk_count, 0) > 0 THEN
    UPDATE public.banking_pay_operations AS operation_update
    SET progress_json = COALESCE(operation_update.progress_json, '{}'::jsonb)
          || jsonb_build_object(
            'provider_submit_diagnostic', v_provider_submit_diagnostic,
            'provider_submission_status', v_provider_submission_status,
            'provider_submit_diagnostic_finalised_at_utc', v_now,
            'provider_submit_diagnostic_finalise_reason_code', v_reason_code
          ),
        result_json = CASE
          WHEN v_provider_submission_status = 'PROVIDER_SUBMISSION_ACCEPTED' THEN
            COALESCE(operation_update.result_json, '{}'::jsonb)
            || jsonb_build_object(
              'provider_submit_diagnostic', v_provider_submit_diagnostic,
              'provider_submission_status', v_provider_submission_status,
              'provider_submit_diagnostic_finalised_at_utc', v_now
            )
          ELSE operation_update.result_json
        END,
        error_json = CASE
          WHEN v_provider_submission_status <> 'PROVIDER_SUBMISSION_ACCEPTED' THEN
            COALESCE(operation_update.error_json, '{}'::jsonb)
            || jsonb_build_object(
              'provider_submit_diagnostic', v_provider_submit_diagnostic,
              'provider_submission_status', v_provider_submission_status,
              'review_reason_code', v_review_reason_code,
              'provider_submit_diagnostic_finalised_at_utc', v_now,
              'provider_submit_diagnostic_finalise_reason_code', v_reason_code
            )
          ELSE operation_update.error_json
        END,
        updated_at_utc = v_now
    WHERE operation_update.id = p_operation_id;

    BEGIN
      PERFORM public._audit_insert(
        'banking_pay_operations',
        p_operation_id::text,
        'PAYMENT_PROVIDER_SUBMIT_DIAGNOSTIC_FINALISED',
        NULL::jsonb,
        jsonb_build_object(
          'operation_id', p_operation_id::text,
          'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
          'finalised_chunk_count', COALESCE(v_finalised_chunk_count, 0),
          'finalised_chunk_ids', COALESCE(v_finalised_chunk_ids, '[]'::jsonb),
          'provider_submit_diagnostic', v_provider_submit_diagnostic,
          'provider_submission_status', v_provider_submission_status,
          'review_reason_code', v_review_reason_code,
          'provider_evidence_counts', jsonb_build_object(
            'provider_acceptance_evidence_count', COALESCE(v_diagnostic_result #>> '{counts,provider_acceptance_evidence_count}', '0'),
            'provider_response_present_count', COALESCE(v_diagnostic_result #>> '{counts,provider_response_present_count}', '0'),
            'provider_request_sent_count', COALESCE(v_diagnostic_result #>> '{counts,provider_request_sent_count}', '0'),
            'stale_unresolved_submit_chunk_count', COALESCE(v_diagnostic_result #>> '{counts,stale_unresolved_submit_chunk_count}', v_diagnostic_result #>> '{counts,stale_empty_submit_chunk_count}', '0'),
            'unfinalised_submit_chunk_count', COALESCE(v_diagnostic_result #>> '{counts,unfinalised_submit_chunk_count}', '0')
          ),
          'reason_code', v_reason_code
        ),
        'payment_provider_submit_diagnostic_finalised',
        v_effective_actor_user_id
      );
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
    'finalised_chunk_count', COALESCE(v_finalised_chunk_count, 0),
    'finalised_chunk_ids', COALESCE(v_finalised_chunk_ids, '[]'::jsonb),
    'already_terminal_chunk_count', COALESCE(v_already_terminal_chunk_count, 0),
    'already_terminal_chunk_ids', COALESCE(v_already_terminal_chunk_ids, '[]'::jsonb),
    'provider_submit_diagnostic', v_provider_submit_diagnostic,
    'provider_submission_status', v_provider_submission_status,
    'review_reason_code', v_review_reason_code,
    'provider_acceptance_evidence_count', COALESCE(v_diagnostic_result #>> '{counts,provider_acceptance_evidence_count}', '0'),
    'provider_response_present_count', COALESCE(v_diagnostic_result #>> '{counts,provider_response_present_count}', '0'),
    'provider_request_sent_count', COALESCE(v_diagnostic_result #>> '{counts,provider_request_sent_count}', '0'),
    'stale_unresolved_submit_chunk_count', COALESCE(v_diagnostic_result #>> '{counts,stale_unresolved_submit_chunk_count}', v_diagnostic_result #>> '{counts,stale_empty_submit_chunk_count}', '0'),
    'unfinalised_submit_chunk_count', COALESCE(v_diagnostic_result #>> '{counts,unfinalised_submit_chunk_count}', '0'),
    'reason_code', v_reason_code,
    'message', CASE
      WHEN COALESCE(v_finalised_chunk_count, 0) > 0 THEN 'Provider-submit chunk diagnostic finalisation completed.'
      ELSE 'No running provider-submit chunks required diagnostic finalisation.'
    END
  );
END;
$function$;







CREATE OR REPLACE FUNCTION public.pay_execute_provider_submit_review_resolve(
  p_pay_batch_id uuid,
  p_operation_id uuid,
  p_resolution_action text,
  p_confirmation_json jsonb,
  p_actor_user_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_uuid_regex text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_batch_row public.pay_batches%ROWTYPE;
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_resolution_action text := NULL::text;
  v_confirmation_json jsonb := '{}'::jsonb;
  v_checked_provider_or_bank boolean := false;
  v_confirmed_no_payment_made boolean := false;
  v_provider_checked text := NULL::text;
  v_checked_at_text text := NULL::text;
  v_checked_at_utc timestamptz := NULL::timestamptz;
  v_notes text := NULL::text;
  v_confirmation_redacted jsonb := '{}'::jsonb;
  v_operation_type text := NULL::text;
  v_operation_status text := NULL::text;
  v_operation_phase text := NULL::text;
  v_batch_execution_commit_state text := NULL::text;
  v_diagnostic_before_result jsonb := '{}'::jsonb;
  v_diagnostic_after_finalise_result jsonb := '{}'::jsonb;
  v_provider_submit_diagnostic_before jsonb := '{}'::jsonb;
  v_provider_submit_diagnostic_after_finalise jsonb := '{}'::jsonb;
  v_manual_provider_submit_diagnostic jsonb := '{}'::jsonb;
  v_finalise_result jsonb := '{}'::jsonb;
  v_provider_submission_status text := NULL::text;
  v_manual_resolution_required boolean := false;
  v_provider_acceptance_evidence_count integer := 0;
  v_direct_provider_acceptance_evidence_count integer := 0;
  v_after_provider_acceptance_evidence_count integer := 0;
  v_chunk_ids jsonb := '[]'::jsonb;
  v_transfer_ids jsonb := '[]'::jsonb;
  v_transfer_scope_ids jsonb := '[]'::jsonb;
  v_auth_request_ids jsonb := '[]'::jsonb;
  v_resolved_chunk_ids jsonb := '[]'::jsonb;
  v_resolved_transfer_ids jsonb := '[]'::jsonb;
  v_resolved_transfer_scope_ids jsonb := '[]'::jsonb;
  v_cancelled_auth_request_ids jsonb := '[]'::jsonb;
  v_cleared_pay_batch_item_ids jsonb := '[]'::jsonb;
  v_manual_event_ids jsonb := '[]'::jsonb;
  v_auth_tokens_voided integer := 0;
  v_batch_rows_updated integer := 0;
  v_before_json jsonb := '{}'::jsonb;
  v_after_json jsonb := '{}'::jsonb;
  v_result jsonb := '{}'::jsonb;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'message', 'pay_execute_provider_submit_review_resolve requires p_pay_batch_id'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'OPERATION_ID_REQUIRED',
      'message', 'pay_execute_provider_submit_review_resolve requires p_operation_id'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'ACTOR_USER_ID_REQUIRED',
      'message', 'pay_execute_provider_submit_review_resolve requires p_actor_user_id'
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_resolution_action := upper(btrim(coalesce(p_resolution_action, '')));
  IF v_resolution_action <> 'CONFIRM_NO_PAYMENT_MADE_AND_RESET_FOR_RETRY' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'UNSUPPORTED_RESOLUTION_ACTION',
      'message', 'Unsupported provider-submit review resolution action',
      'resolution_action', coalesce(p_resolution_action, '')
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_confirmation_json IS NULL OR jsonb_typeof(p_confirmation_json) <> 'object' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'CONFIRMATION_JSON_REQUIRED',
      'message', 'p_confirmation_json must be a JSON object'
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_confirmation_json := p_confirmation_json;
  v_checked_provider_or_bank := lower(btrim(coalesce(v_confirmation_json->>'checked_provider_or_bank', ''))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_confirmed_no_payment_made := lower(btrim(coalesce(v_confirmation_json->>'confirmed_no_payment_made', ''))) IN ('true', 't', '1', 'yes', 'y', 'on');
  v_provider_checked := upper(btrim(coalesce(v_confirmation_json->>'provider_checked', '')));
  v_checked_at_text := NULLIF(btrim(coalesce(v_confirmation_json->>'checked_at_utc', '')), '');
  v_notes := NULLIF(btrim(coalesce(v_confirmation_json->>'notes', '')), '');

  IF v_checked_provider_or_bank IS NOT TRUE THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'CHECKED_PROVIDER_OR_BANK_REQUIRED',
      'message', 'checked_provider_or_bank must be true'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_confirmed_no_payment_made IS NOT TRUE THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'CONFIRMED_NO_PAYMENT_MADE_REQUIRED',
      'message', 'confirmed_no_payment_made must be true'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_provider_checked NOT IN ('REVOLUT', 'BANK', 'BOTH') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'PROVIDER_CHECKED_INVALID',
      'message', 'provider_checked must be REVOLUT, BANK, or BOTH',
      'provider_checked', v_provider_checked
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_checked_at_text IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'CHECKED_AT_UTC_REQUIRED',
      'message', 'checked_at_utc is required'
    )::text USING ERRCODE = 'P0001';
  END IF;

  BEGIN
    v_checked_at_utc := v_checked_at_text::timestamptz;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
        'code', 'CHECKED_AT_UTC_INVALID',
        'message', 'checked_at_utc must be a valid timestamp',
        'checked_at_utc', v_checked_at_text
      )::text USING ERRCODE = 'P0001';
  END;

  v_confirmation_redacted := jsonb_strip_nulls(jsonb_build_object(
    'checked_provider_or_bank', true,
    'confirmed_no_payment_made', true,
    'provider_checked', v_provider_checked,
    'checked_at_utc', v_checked_at_utc::text,
    'notes_present', v_notes IS NOT NULL,
    'notes_length', CASE WHEN v_notes IS NULL THEN NULL ELSE char_length(v_notes) END,
    'recorded_at_utc', v_now::text,
    'recorded_by_user_id', p_actor_user_id::text
  ));

  SELECT batch_row.*
  INTO v_batch_row
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'PAY_BATCH_NOT_FOUND',
      'message', 'Pay batch not found',
      'pay_batch_id', p_pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'OPERATION_NOT_FOUND',
      'message', 'Operation not found',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_row.pay_batch_id IS NULL OR v_operation_row.pay_batch_id <> p_pay_batch_id THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'OPERATION_BATCH_MISMATCH',
      'message', 'Operation does not belong to supplied pay batch',
      'operation_id', p_operation_id::text,
      'operation_pay_batch_id', CASE WHEN v_operation_row.pay_batch_id IS NULL THEN NULL ELSE v_operation_row.pay_batch_id::text END,
      'pay_batch_id', p_pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_operation_type := upper(btrim(coalesce(v_operation_row.operation_type, '')));
  v_operation_status := upper(btrim(coalesce(v_operation_row.status, '')));
  v_operation_phase := upper(btrim(coalesce(v_operation_row.phase, '')));

  IF v_operation_type NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'OPERATION_TYPE_NOT_SUPPORTED',
      'message', 'Operation is not a payment execution operation',
      'operation_type', v_operation_row.operation_type
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_status <> 'REVIEW_REQUIRED' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'OPERATION_NOT_REVIEW_REQUIRED',
      'message', 'Operation must be REVIEW_REQUIRED before manual no-payment resolution',
      'operation_status', v_operation_row.status
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_phase NOT IN ('SUBMIT_PROVIDER_TRANSFERS', 'APPLY_RAIL_UPDATES', 'COMPLETE') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_EXECUTE_PROVIDER_SUBMIT_REVIEW_RESOLVE',
      'code', 'OPERATION_PHASE_NOT_PROVIDER_SUBMIT_REVIEW',
      'message', 'Operation phase is not a provider-submit review phase',
      'operation_phase', v_operation_row.phase
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_batch_execution_commit_state := upper(btrim(coalesce(v_batch_row.execution_commit_state, 'NOT_SUBMITTED')));
  IF v_batch_execution_commit_state <> 'NOT_SUBMITTED'
     OR NULLIF(btrim(coalesce(v_batch_row.execution_commit_ref, '')), '') IS NOT NULL
     OR v_batch_row.execution_committed_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'resolved', false,
      'reason', 'BATCH_EXECUTION_BOUNDARY_CROSSED',
      'safe_retry_available', false,
      'manual_resolution_recorded', false,
      'message', 'Batch execution boundary has been crossed. Manual reconciliation is required before retry.',
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text
    );
  END IF;

  v_diagnostic_before_result := public.pay_provider_submit_diagnostic_get(
    p_pay_batch_id := p_pay_batch_id,
    p_operation_id := p_operation_id,
    p_transfer_id := NULL::uuid,
    p_chunk_id := NULL::uuid,
    p_counts_only := false
  );

  v_provider_submit_diagnostic_before := COALESCE(v_diagnostic_before_result->'provider_submit_diagnostic', '{}'::jsonb);
  v_provider_submission_status := upper(btrim(coalesce(v_provider_submit_diagnostic_before->>'provider_submission_status', v_diagnostic_before_result->>'provider_submission_status', '')));
  v_manual_resolution_required := lower(btrim(coalesce(v_provider_submit_diagnostic_before->>'manual_resolution_required', v_diagnostic_before_result->>'manual_resolution_required', ''))) IN ('true', 't', '1', 'yes', 'y', 'on');

  IF COALESCE(v_diagnostic_before_result #>> '{counts,provider_acceptance_evidence_count}', '') ~ '^[0-9]+$' THEN
    v_provider_acceptance_evidence_count := (v_diagnostic_before_result #>> '{counts,provider_acceptance_evidence_count}')::integer;
  ELSIF COALESCE(v_diagnostic_before_result->>'provider_evidence_count', '') ~ '^[0-9]+$' THEN
    v_provider_acceptance_evidence_count := (v_diagnostic_before_result->>'provider_evidence_count')::integer;
  ELSE
    v_provider_acceptance_evidence_count := 0;
  END IF;

  IF v_manual_resolution_required IS NOT TRUE
     OR v_provider_submission_status NOT IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE') THEN
    RETURN jsonb_build_object(
      'ok', false,
      'resolved', false,
      'reason', 'PROVIDER_SUBMIT_REVIEW_STATE_NOT_ELIGIBLE',
      'safe_retry_available', false,
      'manual_resolution_recorded', false,
      'provider_submit_diagnostic', v_provider_submit_diagnostic_before,
      'provider_submission_status', v_provider_submission_status,
      'manual_resolution_required', v_manual_resolution_required,
      'message', 'Provider-submit review state is not eligible for no-payment manual resolution.',
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_review_chunks;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_review_transfers;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_review_scopes;
  DROP TABLE IF EXISTS pg_temp.tmp_provider_submit_review_auth_requests;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_review_chunks (
    chunk_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_review_transfers (
    transfer_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_review_scopes (
    scope_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  CREATE TEMPORARY TABLE pg_temp.tmp_provider_submit_review_auth_requests (
    auth_request_id uuid PRIMARY KEY
  ) ON COMMIT DROP;

  INSERT INTO pg_temp.tmp_provider_submit_review_chunks (chunk_id)
  SELECT operation_chunk.id
  FROM public.banking_pay_operation_chunks AS operation_chunk
  WHERE operation_chunk.operation_id = p_operation_id
    AND (operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS' OR operation_chunk.chunk_type = 'TRANSFER_SUBMIT')
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_review_transfers (transfer_id)
  SELECT DISTINCT transfer_id_text.value::uuid
  FROM public.banking_pay_operation_chunks AS operation_chunk
  JOIN pg_temp.tmp_provider_submit_review_chunks AS selected_chunk
    ON selected_chunk.chunk_id = operation_chunk.id
  CROSS JOIN LATERAL jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(COALESCE(operation_chunk.payload_json, '{}'::jsonb)->'transfer_ids') = 'array' THEN COALESCE(operation_chunk.payload_json, '{}'::jsonb)->'transfer_ids'
      ELSE '[]'::jsonb
    END
  ) AS transfer_id_text(value)
  WHERE transfer_id_text.value ~* v_uuid_regex
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_review_transfers (transfer_id)
  SELECT DISTINCT transfer_id_text.value::uuid
  FROM public.banking_pay_operation_chunks AS operation_chunk
  JOIN pg_temp.tmp_provider_submit_review_chunks AS selected_chunk
    ON selected_chunk.chunk_id = operation_chunk.id
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN jsonb_typeof(COALESCE(operation_chunk.payload_json, '{}'::jsonb)->'transfers') = 'array' THEN COALESCE(operation_chunk.payload_json, '{}'::jsonb)->'transfers'
      ELSE '[]'::jsonb
    END
  ) AS transfer_payload(value)
  CROSS JOIN LATERAL (
    SELECT COALESCE(
      NULLIF(btrim(coalesce(transfer_payload.value->>'pay_bank_transfer_id', '')), ''),
      NULLIF(btrim(coalesce(transfer_payload.value->>'transfer_id', '')), '')
    ) AS value
  ) AS transfer_id_text
  WHERE transfer_id_text.value ~* v_uuid_regex
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_review_transfers (transfer_id)
  SELECT DISTINCT transfer_scope.pay_bank_transfer_id
  FROM public.banking_pay_operation_transfer_scope AS transfer_scope
  WHERE transfer_scope.operation_id = p_operation_id
    AND transfer_scope.pay_batch_id = p_pay_batch_id
    AND transfer_scope.pay_bank_transfer_id IS NOT NULL
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_review_transfers (transfer_id)
  SELECT DISTINCT transfer_id_text.value::uuid
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_provider_submit_diagnostic_before->'transfer_ids') = 'array' THEN v_provider_submit_diagnostic_before->'transfer_ids'
      ELSE '[]'::jsonb
    END
  ) AS transfer_id_text(value)
  WHERE transfer_id_text.value ~* v_uuid_regex
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_review_scopes (scope_id)
  SELECT transfer_scope.id
  FROM public.banking_pay_operation_transfer_scope AS transfer_scope
  WHERE transfer_scope.operation_id = p_operation_id
    AND transfer_scope.pay_batch_id = p_pay_batch_id
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_review_scopes (scope_id)
  SELECT DISTINCT scope_id_text.value::uuid
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_provider_submit_diagnostic_before->'transfer_scope_ids') = 'array' THEN v_provider_submit_diagnostic_before->'transfer_scope_ids'
      ELSE '[]'::jsonb
    END
  ) AS scope_id_text(value)
  WHERE scope_id_text.value ~* v_uuid_regex
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_review_auth_requests (auth_request_id)
  SELECT auth_request.id
  FROM public.pay_batch_auth_requests AS auth_request
  WHERE auth_request.pay_batch_id = p_pay_batch_id
    AND upper(btrim(coalesce(auth_request.state, ''))) IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED')
    AND auth_request.execution_intent_json->>'operation_id' = p_operation_id::text
  ON CONFLICT DO NOTHING;

  INSERT INTO pg_temp.tmp_provider_submit_review_auth_requests (auth_request_id)
  SELECT DISTINCT auth_id_text.value::uuid
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(v_provider_submit_diagnostic_before->'auth_request_ids') = 'array' THEN v_provider_submit_diagnostic_before->'auth_request_ids'
      ELSE '[]'::jsonb
    END
  ) AS auth_id_text(value)
  WHERE auth_id_text.value ~* v_uuid_regex
  ON CONFLICT DO NOTHING;

  SELECT COUNT(*)::integer
  INTO v_direct_provider_acceptance_evidence_count
  FROM public.pay_bank_transfers AS transfer_row
  WHERE transfer_row.pay_batch_id = p_pay_batch_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp.tmp_provider_submit_review_transfers AS selected_transfer
      WHERE selected_transfer.transfer_id = transfer_row.id
    )
    AND (
      (
        NULLIF(btrim(coalesce(transfer_row.rail_tx_id, '')), '') IS NOT NULL
        AND NOT (
          NULLIF(btrim(coalesce(transfer_row.rail_tx_id, '')), '') = ANY(
            ARRAY_REMOVE(ARRAY[
              transfer_row.id::text,
              NULLIF(btrim(coalesce(transfer_row.request_id, '')), ''),
              NULLIF(btrim(coalesce(transfer_row.payment_reference, '')), ''),
              NULLIF(btrim(coalesce(v_batch_row.bulk_reference, '')), ''),
              NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{request_id}', '')), ''),
              NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), ''),
              NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{local_provider_request_id}', '')), ''),
              NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,local_provider_request_id}', '')), ''),
              NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{payment_reference}', '')), ''),
              NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{bulk_reference}', '')), '')
            ]::text[], NULL::text)
          )
        )
      )
      OR (
        upper(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) = 'PROVIDER_SUBMISSION_ACCEPTED'
        AND (
          NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') IS NOT NULL
          OR upper(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_state}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_state}', ''))) IN ('ACCEPTED', 'SUBMITTED', 'PROCESSING', 'SENT', 'COMPLETED', 'COMPLETE', 'APPROVED', 'EXECUTED', 'PAID')
        )
      )
      OR EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS transfer_event
        WHERE transfer_event.pay_batch_id = p_pay_batch_id
          AND transfer_event.pay_bank_transfer_id = transfer_row.id
          AND upper(btrim(coalesce(transfer_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')
          AND upper(btrim(coalesce(transfer_event.normalised_state, transfer_event.provider_state, transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_state}', ''))) NOT IN ('REJECTED', 'FAILED', 'ERROR', 'DECLINED', 'CANCELLED', 'CANCELED', 'MALFORMED', 'UNKNOWN')
          AND upper(btrim(coalesce(transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
          AND (
            EXISTS (
              SELECT 1
              FROM (VALUES
                (transfer_event.provider_event_id),
                (transfer_event.provider_reference),
                (transfer_event.raw_payload #>> '{provider_event_id}'),
                (transfer_event.raw_payload #>> '{provider_reference}'),
                (transfer_event.raw_payload #>> '{provider_submission_id}'),
                (transfer_event.raw_payload #>> '{submission_id}'),
                (transfer_event.raw_payload #>> '{rail_submission_id}'),
                (transfer_event.raw_payload #>> '{provider_payment_id}'),
                (transfer_event.raw_payload #>> '{payment_id}'),
                (transfer_event.raw_payload #>> '{external_payment_id}'),
                (transfer_event.raw_payload #>> '{revolut_payment_id}'),
                (transfer_event.raw_payload #>> '{provider_transfer_id}'),
                (transfer_event.raw_payload #>> '{external_transfer_id}'),
                (transfer_event.raw_payload #>> '{provider_transaction_id}'),
                (transfer_event.raw_payload #>> '{transaction_id}'),
                (transfer_event.raw_payload #>> '{rail_tx_id}'),
                (transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}'),
                (transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_payment_id}'),
                (transfer_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}'),
                (transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}')
              ) AS provider_identifier(identifier_value)
              WHERE NULLIF(btrim(coalesce(provider_identifier.identifier_value, '')), '') IS NOT NULL
                AND NOT (
                  NULLIF(btrim(coalesce(provider_identifier.identifier_value, '')), '') = ANY(
                    ARRAY_REMOVE(ARRAY[
                      transfer_row.id::text,
                      NULLIF(btrim(coalesce(transfer_row.request_id, '')), ''),
                      NULLIF(btrim(coalesce(transfer_row.payment_reference, '')), ''),
                      NULLIF(btrim(coalesce(v_batch_row.bulk_reference, '')), ''),
                      NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{request_id}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{local_provider_request_id}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,local_provider_request_id}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_event.raw_payload #>> '{request_id}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_event.raw_payload #>> '{idempotency_key}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_event.raw_payload #>> '{local_provider_request_id}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_event.raw_payload #>> '{provider_submit_diagnostic,request_id}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_event.raw_payload #>> '{provider_submit_diagnostic,idempotency_key}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_event.raw_payload #>> '{provider_submit_diagnostic,local_provider_request_id}', '')), ''),
                      NULLIF(btrim(coalesce(transfer_event.idempotency_key, '')), '')
                    ]::text[], NULL::text)
                  )
                )
            )
            OR upper(btrim(coalesce(transfer_event.provider_state, transfer_event.normalised_state, transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_state}', ''))) IN ('ACCEPTED', 'SUBMITTED', 'PROCESSING', 'SENT', 'COMPLETED', 'COMPLETE', 'APPROVED', 'EXECUTED', 'PAID')
          )
      )
    );

  IF COALESCE(v_provider_acceptance_evidence_count, 0) > 0 OR COALESCE(v_direct_provider_acceptance_evidence_count, 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'resolved', false,
      'reason', 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT',
      'safe_retry_available', false,
      'manual_resolution_recorded', false,
      'message', 'Provider acceptance evidence exists. Manual reconciliation is required before retry.',
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'provider_submit_diagnostic', v_provider_submit_diagnostic_before,
      'provider_acceptance_evidence_count', GREATEST(COALESCE(v_provider_acceptance_evidence_count, 0), COALESCE(v_direct_provider_acceptance_evidence_count, 0))
    );
  END IF;

  v_before_json := jsonb_build_object(
    'pay_batch', to_jsonb(v_batch_row),
    'operation', to_jsonb(v_operation_row),
    'chunks', COALESCE((
      SELECT jsonb_agg(to_jsonb(chunk_before) ORDER BY chunk_before.id::text)
      FROM public.banking_pay_operation_chunks AS chunk_before
      JOIN pg_temp.tmp_provider_submit_review_chunks AS selected_chunk
        ON selected_chunk.chunk_id = chunk_before.id
    ), '[]'::jsonb),
    'transfers', COALESCE((
      SELECT jsonb_agg(to_jsonb(transfer_before) ORDER BY transfer_before.id::text)
      FROM public.pay_bank_transfers AS transfer_before
      JOIN pg_temp.tmp_provider_submit_review_transfers AS selected_transfer
        ON selected_transfer.transfer_id = transfer_before.id
    ), '[]'::jsonb),
    'transfer_scopes', COALESCE((
      SELECT jsonb_agg(to_jsonb(scope_before) ORDER BY scope_before.id::text)
      FROM public.banking_pay_operation_transfer_scope AS scope_before
      JOIN pg_temp.tmp_provider_submit_review_scopes AS selected_scope
        ON selected_scope.scope_id = scope_before.id
    ), '[]'::jsonb),
    'auth_requests', COALESCE((
      SELECT jsonb_agg(to_jsonb(auth_before) ORDER BY auth_before.id::text)
      FROM public.pay_batch_auth_requests AS auth_before
      JOIN pg_temp.tmp_provider_submit_review_auth_requests AS selected_auth
        ON selected_auth.auth_request_id = auth_before.id
    ), '[]'::jsonb),
    'provider_submit_diagnostic_before', v_provider_submit_diagnostic_before,
    'operator_confirmation', v_confirmation_redacted
  );

  v_finalise_result := public.pay_provider_submit_chunk_diagnostic_finalise(
    p_operation_id := p_operation_id,
    p_pay_batch_id := p_pay_batch_id,
    p_chunk_id := NULL::uuid,
    p_actor_user_id := p_actor_user_id,
    p_reason_code := 'MANUAL_NO_PAYMENT_RESOLUTION_FINALISE_PROVIDER_SUBMIT_CHUNKS',
    p_failure_error_json := jsonb_build_object(
      'provider_submit_diagnostic', v_provider_submit_diagnostic_before,
      'manual_resolution_action', v_resolution_action,
      'operator_confirmation', v_confirmation_redacted
    )
  );

  v_diagnostic_after_finalise_result := public.pay_provider_submit_diagnostic_get(
    p_pay_batch_id := p_pay_batch_id,
    p_operation_id := p_operation_id,
    p_transfer_id := NULL::uuid,
    p_chunk_id := NULL::uuid,
    p_counts_only := false
  );

  v_provider_submit_diagnostic_after_finalise := COALESCE(v_diagnostic_after_finalise_result->'provider_submit_diagnostic', v_provider_submit_diagnostic_before, '{}'::jsonb);

  IF COALESCE(v_diagnostic_after_finalise_result #>> '{counts,provider_acceptance_evidence_count}', '') ~ '^[0-9]+$' THEN
    v_after_provider_acceptance_evidence_count := (v_diagnostic_after_finalise_result #>> '{counts,provider_acceptance_evidence_count}')::integer;
  ELSIF COALESCE(v_diagnostic_after_finalise_result->>'provider_evidence_count', '') ~ '^[0-9]+$' THEN
    v_after_provider_acceptance_evidence_count := (v_diagnostic_after_finalise_result->>'provider_evidence_count')::integer;
  ELSE
    v_after_provider_acceptance_evidence_count := 0;
  END IF;

  IF COALESCE(v_after_provider_acceptance_evidence_count, 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', false,
      'resolved', false,
      'reason', 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT',
      'safe_retry_available', false,
      'manual_resolution_recorded', false,
      'message', 'Provider acceptance evidence exists. Manual reconciliation is required before retry.',
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'provider_submit_diagnostic', v_provider_submit_diagnostic_after_finalise,
      'provider_acceptance_evidence_count', v_after_provider_acceptance_evidence_count
    );
  END IF;

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_chunk.chunk_id::text) ORDER BY selected_chunk.chunk_id::text), '[]'::jsonb)
  INTO v_chunk_ids
  FROM pg_temp.tmp_provider_submit_review_chunks AS selected_chunk;

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_transfer.transfer_id::text) ORDER BY selected_transfer.transfer_id::text), '[]'::jsonb)
  INTO v_transfer_ids
  FROM pg_temp.tmp_provider_submit_review_transfers AS selected_transfer;

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_scope.scope_id::text) ORDER BY selected_scope.scope_id::text), '[]'::jsonb)
  INTO v_transfer_scope_ids
  FROM pg_temp.tmp_provider_submit_review_scopes AS selected_scope;

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_auth.auth_request_id::text) ORDER BY selected_auth.auth_request_id::text), '[]'::jsonb)
  INTO v_auth_request_ids
  FROM pg_temp.tmp_provider_submit_review_auth_requests AS selected_auth;

  v_manual_provider_submit_diagnostic := jsonb_strip_nulls(
    COALESCE(v_provider_submit_diagnostic_after_finalise, '{}'::jsonb)
    || jsonb_build_object(
      'diagnostic_version', 1,
      'generated_at_utc', v_now::text,
      'review_reason_code', 'MANUAL_NO_PAYMENT_CONFIRMATION_RECORDED',
      'provider_submission_status', 'MANUAL_RESOLVED_NO_PAYMENT_MADE',
      'provider_submission_unknown', false,
      'provider_acceptance_evidence_present', false,
      'manual_resolution_required', false,
      'safe_retry_available', true,
      'automatic_retry_blocked', false,
      'retry_blocked_reason', NULL::text,
      'recommended_action', 'Manual no-payment confirmation recorded. The batch can now be retried using the existing frozen batch artefacts.',
      'manual_resolution_recorded', true,
      'resolved_at_utc', v_now::text,
      'resolved_by_user_id', p_actor_user_id::text,
      'operator_confirmation', v_confirmation_redacted,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'chunk_ids', v_chunk_ids,
      'transfer_ids', v_transfer_ids,
      'transfer_scope_ids', v_transfer_scope_ids,
      'auth_request_ids', v_auth_request_ids
    )
  );

  WITH updated_chunks AS (
    UPDATE public.banking_pay_operation_chunks AS chunk_update
    SET result_json = jsonb_strip_nulls(
          COALESCE(chunk_update.result_json, '{}'::jsonb)
          || jsonb_build_object(
            'provider_submit_diagnostic', v_manual_provider_submit_diagnostic,
            'provider_submission_status', 'MANUAL_RESOLVED_NO_PAYMENT_MADE',
            'manual_resolution_recorded', true,
            'manual_resolution_recorded_at_utc', v_now::text
          )
        ),
        error_json = jsonb_strip_nulls(
          COALESCE(chunk_update.error_json, '{}'::jsonb)
          || jsonb_build_object(
            'provider_submit_diagnostic', v_manual_provider_submit_diagnostic,
            'provider_submission_status', 'MANUAL_RESOLVED_NO_PAYMENT_MADE',
            'review_reason_code', 'MANUAL_NO_PAYMENT_CONFIRMATION_RECORDED',
            'manual_resolution_required', false,
            'safe_retry_available', true,
            'manual_resolution_recorded_at_utc', v_now::text
          )
        ),
        locked_by = NULL::text,
        lock_expires_at_utc = NULL::timestamptz,
        updated_at_utc = v_now
    FROM pg_temp.tmp_provider_submit_review_chunks AS selected_chunk
    WHERE chunk_update.id = selected_chunk.chunk_id
      AND chunk_update.operation_id = p_operation_id
    RETURNING chunk_update.id
  )
  SELECT COALESCE(jsonb_agg(to_jsonb(updated_chunks.id::text) ORDER BY updated_chunks.id::text), '[]'::jsonb)
  INTO v_resolved_chunk_ids
  FROM updated_chunks;

  WITH updated_transfers AS (
    UPDATE public.pay_bank_transfers AS transfer_update
    SET status = 'CANCELLED',
        failed_reason = 'MANUAL_RESOLVED_NO_PAYMENT_MADE',
        rail_meta_json = jsonb_strip_nulls(
          COALESCE(transfer_update.rail_meta_json, '{}'::jsonb)
          || jsonb_build_object(
            'provider_submit_diagnostic', v_manual_provider_submit_diagnostic,
            'manual_resolution_recorded', true,
            'manual_resolution_recorded_at_utc', v_now::text,
            'manual_resolution_operation_id', p_operation_id::text,
            'operator_confirmation', v_confirmation_redacted
          )
        )
    FROM pg_temp.tmp_provider_submit_review_transfers AS selected_transfer
    WHERE transfer_update.id = selected_transfer.transfer_id
      AND transfer_update.pay_batch_id = p_pay_batch_id
      AND NULLIF(btrim(coalesce(transfer_update.rail_tx_id, '')), '') IS NULL
    RETURNING transfer_update.id
  )
  SELECT COALESCE(jsonb_agg(to_jsonb(updated_transfers.id::text) ORDER BY updated_transfers.id::text), '[]'::jsonb)
  INTO v_resolved_transfer_ids
  FROM updated_transfers;

  WITH inserted_events AS (
    INSERT INTO public.pay_bank_transfer_events (
      pay_batch_id,
      pay_bank_transfer_id,
      candidate_id,
      umbrella_id,
      provider_key,
      provider_event_id,
      provider_reference,
      provider_state,
      normalised_state,
      event_source,
      event_time_utc,
      amount,
      currency,
      mapping_status,
      movement_classification,
      correction_disposition,
      raw_payload,
      idempotency_key,
      mapping_method
    )
    SELECT p_pay_batch_id,
           transfer_row.id,
           transfer_row.candidate_id,
           transfer_row.umbrella_id,
           transfer_row.rail_provider,
           NULL::text,
           NULL::text,
           'MANUAL_RESOLVED_NO_PAYMENT_MADE',
           'CANCELLED',
           'LOCAL_STATE',
           v_now,
           transfer_row.amount,
           COALESCE(NULLIF(btrim(coalesce(transfer_row.currency, '')), ''), 'GBP'),
           'MANUAL_RESOLVED_NO_PAYMENT_MADE',
           'NO_MONEY_MOVED',
           'NO_PAYMENT_MADE_CONFIRMED',
           jsonb_strip_nulls(jsonb_build_object(
             'event_kind', 'PROVIDER_SUBMIT_MANUAL_NO_PAYMENT_RESOLUTION',
             'provider_submit_diagnostic', v_manual_provider_submit_diagnostic,
             'operator_confirmation', v_confirmation_redacted,
             'operation_id', p_operation_id::text,
             'resolved_at_utc', v_now::text,
             'resolved_by_user_id', p_actor_user_id::text
           )),
           'MANUAL_NO_PAYMENT_RESOLUTION|' || p_operation_id::text || '|' || transfer_row.id::text,
           'MANUAL_PROVIDER_SUBMIT_REVIEW_RESOLUTION'
    FROM public.pay_bank_transfers AS transfer_row
    JOIN pg_temp.tmp_provider_submit_review_transfers AS selected_transfer
      ON selected_transfer.transfer_id = transfer_row.id
    WHERE transfer_row.pay_batch_id = p_pay_batch_id
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING id
  )
  SELECT COALESCE(jsonb_agg(to_jsonb(inserted_events.id::text) ORDER BY inserted_events.id::text), '[]'::jsonb)
  INTO v_manual_event_ids
  FROM inserted_events;

  WITH updated_scopes AS (
    UPDATE public.banking_pay_operation_transfer_scope AS scope_update
    SET status = 'CANCELLED',
        updated_at_utc = v_now
    FROM pg_temp.tmp_provider_submit_review_scopes AS selected_scope
    WHERE scope_update.id = selected_scope.scope_id
      AND scope_update.pay_batch_id = p_pay_batch_id
      AND scope_update.operation_id = p_operation_id
    RETURNING scope_update.id
  )
  SELECT COALESCE(jsonb_agg(to_jsonb(updated_scopes.id::text) ORDER BY updated_scopes.id::text), '[]'::jsonb)
  INTO v_resolved_transfer_scope_ids
  FROM updated_scopes;

  WITH cleared_items AS (
    UPDATE public.pay_batch_items AS item_update
    SET pay_bank_transfer_id = NULL::uuid,
        bank_reference = NULL::text,
        updated_at = v_now
    FROM pg_temp.tmp_provider_submit_review_transfers AS selected_transfer
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.pay_batch_id = p_pay_batch_id
    WHERE item_update.pay_batch_candidate_id = batch_candidate.id
      AND item_update.pay_bank_transfer_id = selected_transfer.transfer_id
      AND COALESCE(item_update.is_voided, false) IS NOT TRUE
    RETURNING item_update.id
  )
  SELECT COALESCE(jsonb_agg(to_jsonb(cleared_items.id::text) ORDER BY cleared_items.id::text), '[]'::jsonb)
  INTO v_cleared_pay_batch_item_ids
  FROM cleared_items;

  WITH cancelled_auth_requests AS (
    UPDATE public.pay_batch_auth_requests AS auth_request_update
    SET state = 'CANCELLED',
        finalised_at_utc = COALESCE(auth_request_update.finalised_at_utc, v_now),
        finalised_by_user_id = COALESCE(auth_request_update.finalised_by_user_id, p_actor_user_id),
        execution_intent_json = jsonb_strip_nulls(
          COALESCE(auth_request_update.execution_intent_json, '{}'::jsonb)
          || jsonb_build_object(
            'manual_provider_submit_review_resolution', true,
            'manual_resolution_action', v_resolution_action,
            'manual_resolution_operation_id', p_operation_id::text,
            'manual_resolution_at_utc', v_now::text,
            'manual_resolution_by_user_id', p_actor_user_id::text,
            'operator_confirmation', v_confirmation_redacted,
            'provider_submit_diagnostic', v_manual_provider_submit_diagnostic
          )
        )
    FROM pg_temp.tmp_provider_submit_review_auth_requests AS selected_auth
    WHERE auth_request_update.id = selected_auth.auth_request_id
      AND auth_request_update.pay_batch_id = p_pay_batch_id
      AND upper(btrim(coalesce(auth_request_update.state, ''))) IN ('AWAITING', 'PENDING_AUTHORISATION', 'AUTHORISED')
    RETURNING auth_request_update.id
  )
  SELECT COALESCE(jsonb_agg(to_jsonb(cancelled_auth_requests.id::text) ORDER BY cancelled_auth_requests.id::text), '[]'::jsonb)
  INTO v_cancelled_auth_request_ids
  FROM cancelled_auth_requests;

  WITH voided_auth_tokens AS (
    UPDATE public.pay_batch_auth_tokens AS auth_token_update
    SET used_at_utc = COALESCE(auth_token_update.used_at_utc, v_now),
        expires_at_utc = CASE
          WHEN auth_token_update.expires_at_utc > v_now THEN v_now
          ELSE auth_token_update.expires_at_utc
        END
    FROM pg_temp.tmp_provider_submit_review_auth_requests AS selected_auth
    WHERE auth_token_update.auth_request_id = selected_auth.auth_request_id
      AND (auth_token_update.used_at_utc IS NULL OR auth_token_update.expires_at_utc > v_now)
    RETURNING auth_token_update.token
  )
  SELECT COUNT(*)::integer
  INTO v_auth_tokens_voided
  FROM voided_auth_tokens;

  UPDATE public.pay_batches AS batch_update
  SET schedule_kind = NULL::text,
      scheduled_at_utc = NULL::timestamptz,
      scheduled_by_user_id = NULL::uuid,
      funding_account_ref = NULL::text,
      funds_warning_hours_json = NULL::jsonb,
      execution_intent_json = NULL::jsonb
  WHERE batch_update.id = p_pay_batch_id;

  GET DIAGNOSTICS v_batch_rows_updated = ROW_COUNT;

  UPDATE public.banking_pay_operations AS operation_update
  SET status = 'FAILED',
      progress_json = jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'provider_submit_diagnostic', v_manual_provider_submit_diagnostic,
          'provider_submission_status', 'MANUAL_RESOLVED_NO_PAYMENT_MADE',
          'manual_resolution_recorded', true,
          'manual_resolution_recorded_at_utc', v_now::text,
          'manual_resolution_by_user_id', p_actor_user_id::text
        )
      ),
      result_json = jsonb_strip_nulls(
        COALESCE(operation_update.result_json, '{}'::jsonb)
        || jsonb_build_object(
          'provider_submit_diagnostic', v_manual_provider_submit_diagnostic,
          'provider_submission_status', 'MANUAL_RESOLVED_NO_PAYMENT_MADE',
          'manual_provider_submit_review_resolution', true,
          'manual_resolution_recorded_at_utc', v_now::text,
          'manual_resolution_by_user_id', p_actor_user_id::text,
          'safe_retry_available', true
        )
      ),
      error_json = jsonb_strip_nulls(
        COALESCE(operation_update.error_json, '{}'::jsonb)
        || jsonb_build_object(
          'provider_submit_diagnostic', v_manual_provider_submit_diagnostic,
          'provider_submission_status', 'MANUAL_RESOLVED_NO_PAYMENT_MADE',
          'review_reason_code', 'MANUAL_NO_PAYMENT_CONFIRMATION_RECORDED',
          'manual_resolution_recorded', true,
          'manual_resolution_recorded_at_utc', v_now::text,
          'manual_resolution_by_user_id', p_actor_user_id::text,
          'safe_retry_available', true
        )
      ),
      failed_at_utc = COALESCE(operation_update.failed_at_utc, v_now),
      completed_at_utc = COALESCE(operation_update.completed_at_utc, v_now),
      locked_by = NULL::text,
      lock_expires_at_utc = NULL::timestamptz,
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  SELECT jsonb_build_object(
    'pay_batch', (SELECT to_jsonb(batch_after) FROM public.pay_batches AS batch_after WHERE batch_after.id = p_pay_batch_id),
    'operation', (SELECT to_jsonb(operation_after) FROM public.banking_pay_operations AS operation_after WHERE operation_after.id = p_operation_id),
    'chunks', COALESCE((
      SELECT jsonb_agg(to_jsonb(chunk_after) ORDER BY chunk_after.id::text)
      FROM public.banking_pay_operation_chunks AS chunk_after
      JOIN pg_temp.tmp_provider_submit_review_chunks AS selected_chunk
        ON selected_chunk.chunk_id = chunk_after.id
    ), '[]'::jsonb),
    'transfers', COALESCE((
      SELECT jsonb_agg(to_jsonb(transfer_after) ORDER BY transfer_after.id::text)
      FROM public.pay_bank_transfers AS transfer_after
      JOIN pg_temp.tmp_provider_submit_review_transfers AS selected_transfer
        ON selected_transfer.transfer_id = transfer_after.id
    ), '[]'::jsonb),
    'transfer_scopes', COALESCE((
      SELECT jsonb_agg(to_jsonb(scope_after) ORDER BY scope_after.id::text)
      FROM public.banking_pay_operation_transfer_scope AS scope_after
      JOIN pg_temp.tmp_provider_submit_review_scopes AS selected_scope
        ON selected_scope.scope_id = scope_after.id
    ), '[]'::jsonb),
    'auth_requests', COALESCE((
      SELECT jsonb_agg(to_jsonb(auth_after) ORDER BY auth_after.id::text)
      FROM public.pay_batch_auth_requests AS auth_after
      JOIN pg_temp.tmp_provider_submit_review_auth_requests AS selected_auth
        ON selected_auth.auth_request_id = auth_after.id
    ), '[]'::jsonb),
    'manual_provider_submit_diagnostic', v_manual_provider_submit_diagnostic
  )
  INTO v_after_json;

  v_result := jsonb_build_object(
    'ok', true,
    'resolved', true,
    'resolution_status', 'COMPLETE',
    'manual_resolution_recorded', true,
    'safe_retry_available', true,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', p_operation_id::text,
    'provider_submit_diagnostic', v_manual_provider_submit_diagnostic,
    'provider_submission_status', 'MANUAL_RESOLVED_NO_PAYMENT_MADE',
    'review_reason_code', 'MANUAL_NO_PAYMENT_CONFIRMATION_RECORDED',
    'resolved_chunk_ids', COALESCE(v_resolved_chunk_ids, '[]'::jsonb),
    'resolved_transfer_ids', COALESCE(v_resolved_transfer_ids, '[]'::jsonb),
    'resolved_transfer_scope_ids', COALESCE(v_resolved_transfer_scope_ids, '[]'::jsonb),
    'cancelled_auth_request_ids', COALESCE(v_cancelled_auth_request_ids, '[]'::jsonb),
    'cleared_pay_batch_item_ids', COALESCE(v_cleared_pay_batch_item_ids, '[]'::jsonb),
    'manual_resolution_event_ids', COALESCE(v_manual_event_ids, '[]'::jsonb),
    'auth_tokens_voided', COALESCE(v_auth_tokens_voided, 0),
    'batch_rows_updated', COALESCE(v_batch_rows_updated, 0),
    'provider_submit_finalise_result', COALESCE(v_finalise_result, '{}'::jsonb),
    'message', 'Manual no-payment confirmation recorded. The batch can now be retried.'
  );

  BEGIN
    PERFORM public._audit_insert(
      'banking_pay_operations',
      p_operation_id::text,
      'PAYMENT_PROVIDER_SUBMIT_REVIEW_RESOLVED_NO_PAYMENT_MADE',
      v_before_json,
      v_after_json || jsonb_build_object('result', v_result),
      'payment_provider_submit_review_resolved_no_payment_made',
      p_actor_user_id
    );
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END;

  RETURN v_result;
END;
$function$;


CREATE OR REPLACE FUNCTION public.pay_provider_submit_chunk_stage_record(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_chunk_id uuid,
  p_transfer_ids jsonb DEFAULT '[]'::jsonb,
  p_stage text DEFAULT NULL::text,
  p_provider_submit_diagnostic jsonb DEFAULT '{}'::jsonb,
  p_actor_user_id uuid DEFAULT NULL::uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_uuid_regex text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_chunk_row public.banking_pay_operation_chunks%ROWTYPE;
  v_after_chunk_row public.banking_pay_operation_chunks%ROWTYPE;

  v_actor_user_id uuid := NULL::uuid;
  v_stage text := NULL::text;
  v_stage_upper text := NULL::text;
  v_input_diagnostic jsonb := '{}'::jsonb;
  v_helper_result jsonb := '{}'::jsonb;

  v_transfer_ids uuid[] := ARRAY[]::uuid[];
  v_transfer_ids_json jsonb := '[]'::jsonb;
  v_transfer_count integer := 0;
  v_invalid_transfer_text_count integer := 0;
  v_missing_transfer_count integer := 0;

  v_primary_transfer_id text := NULL::text;
  v_primary_rail_provider text := NULL::text;
  v_primary_rail_env text := NULL::text;
  v_primary_rail_tx_id text := NULL::text;
  v_primary_rail_state text := NULL::text;
  v_primary_request_id text := NULL::text;
  v_primary_payment_reference text := NULL::text;

  v_direct_acceptance_evidence_count integer := 0;
  v_direct_response_present_count integer := 0;
  v_direct_request_sent_count integer := 0;
  v_direct_rejection_count integer := 0;

  v_helper_acceptance_evidence_count integer := 0;
  v_helper_response_present_count integer := 0;
  v_helper_request_sent_count integer := 0;
  v_helper_unknown_count integer := 0;
  v_helper_rejected_count integer := 0;
  v_helper_malformed_count integer := 0;
  v_helper_blocked_pre_call_count integer := 0;

  v_provider_acceptance_evidence_count integer := 0;
  v_provider_response_present_count integer := 0;
  v_provider_request_sent_count integer := 0;
  v_provider_rejection_count integer := 0;
  v_provider_unknown_count integer := 0;
  v_provider_malformed_count integer := 0;
  v_provider_blocked_pre_call_count integer := 0;

  v_existing_diagnostic jsonb := '{}'::jsonb;
  v_existing_diagnostic_source text := NULL::text;
  v_existing_status text := NULL::text;
  v_existing_review_reason_code text := NULL::text;
  v_existing_rank integer := 0;

  v_incoming_status text := NULL::text;
  v_incoming_review_reason_code text := NULL::text;
  v_incoming_rank integer := 0;
  v_incoming_diagnostic jsonb := '{}'::jsonb;

  v_final_status text := NULL::text;
  v_final_review_reason_code text := NULL::text;
  v_final_rank integer := 0;
  v_final_diagnostic jsonb := '{}'::jsonb;

  v_provider_call_stage text := NULL::text;
  v_provider_submission_attempted boolean := false;
  v_provider_request_sent boolean := false;
  v_provider_response_received boolean := false;
  v_provider_response_present boolean := false;
  v_provider_submission_accepted boolean := false;
  v_provider_submission_rejected boolean := false;
  v_provider_submission_failed boolean := false;
  v_provider_submission_unknown boolean := false;
  v_provider_acceptance_evidence_present boolean := false;
  v_manual_resolution_required boolean := false;
  v_safe_retry_available boolean := false;
  v_automatic_retry_blocked boolean := false;
  v_provider_request_impossible boolean := false;
  v_retry_blocked_reason text := NULL::text;
  v_recommended_action text := NULL::text;
  v_crash_safety_status_if_lock_expires text := NULL::text;

  v_provider_transaction_id text := NULL::text;
  v_provider_reference text := NULL::text;
  v_provider_state text := NULL::text;
  v_rail_tx_id text := NULL::text;
  v_rail_state text := NULL::text;
  v_request_id text := NULL::text;
  v_idempotency_key text := NULL::text;
  v_local_provider_request_id text := NULL::text;
  v_provider_http_status text := NULL::text;
  v_provider_error_code text := NULL::text;
  v_provider_error_message_redacted text := NULL::text;
  v_provider_response_redacted jsonb := NULL::jsonb;
  v_provider_error_redacted jsonb := NULL::jsonb;

  v_chunk_lock_expired boolean := false;
  v_unfinalised_submit_chunk boolean := false;
  v_apply_incoming boolean := true;
  v_preserved_stronger_provider_evidence boolean := false;

  v_chunk_result_before jsonb := '{}'::jsonb;
  v_chunk_result_after jsonb := '{}'::jsonb;
  v_operation_progress_after jsonb := '{}'::jsonb;
  v_stage_history jsonb := '[]'::jsonb;
  v_stage_history_entry jsonb := '{}'::jsonb;
  v_before_json jsonb := '{}'::jsonb;
  v_after_json jsonb := '{}'::jsonb;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'OPERATION_ID_REQUIRED',
      'message', 'pay_provider_submit_chunk_stage_record requires p_operation_id'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'PAY_BATCH_ID_REQUIRED',
      'message', 'pay_provider_submit_chunk_stage_record requires p_pay_batch_id'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_chunk_id IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'CHUNK_ID_REQUIRED',
      'message', 'pay_provider_submit_chunk_stage_record requires p_chunk_id'
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF p_provider_submit_diagnostic IS NULL THEN
    v_input_diagnostic := '{}'::jsonb;
  ELSIF jsonb_typeof(p_provider_submit_diagnostic) = 'object' THEN
    v_input_diagnostic := p_provider_submit_diagnostic;
  ELSE
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'PROVIDER_SUBMIT_DIAGNOSTIC_MUST_BE_OBJECT',
      'message', 'pay_provider_submit_chunk_stage_record requires p_provider_submit_diagnostic to be a JSON object when supplied',
      'json_type', jsonb_typeof(p_provider_submit_diagnostic)
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_input_diagnostic := v_input_diagnostic
    - 'access_token'
    - 'authorization'
    - 'Authorization'
    - 'bearer_token'
    - 'token'
    - 'account_number'
    - 'sort_code'
    - 'iban'
    - 'source_account_number'
    - 'beneficiary_account_number';

  v_stage := NULLIF(BTRIM(COALESCE(p_stage, v_input_diagnostic->>'provider_call_stage', v_input_diagnostic->>'stage', '')), '');
  IF v_stage IS NULL THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'STAGE_REQUIRED',
      'message', 'pay_provider_submit_chunk_stage_record requires p_stage or provider_submit_diagnostic.provider_call_stage'
    )::text USING ERRCODE = 'P0001';
  END IF;
  v_stage_upper := UPPER(v_stage);

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'OPERATION_NOT_FOUND',
      'message', 'pay_provider_submit_chunk_stage_record: operation not found',
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_row.operation_type NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'NOT_PROVIDER_SUBMIT_OPERATION',
      'message', 'pay_provider_submit_chunk_stage_record can only be used for payment execution provider-submit operations',
      'operation_id', p_operation_id::text,
      'operation_type', v_operation_row.operation_type
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_operation_row.pay_batch_id IS NULL OR v_operation_row.pay_batch_id <> p_pay_batch_id THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'OPERATION_BATCH_MISMATCH',
      'message', 'pay_provider_submit_chunk_stage_record: operation does not belong to supplied pay batch',
      'operation_id', p_operation_id::text,
      'operation_pay_batch_id', CASE WHEN v_operation_row.pay_batch_id IS NULL THEN NULL ELSE v_operation_row.pay_batch_id::text END,
      'pay_batch_id', p_pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  PERFORM 1
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'PAY_BATCH_NOT_FOUND',
      'message', 'pay_provider_submit_chunk_stage_record: pay batch not found',
      'pay_batch_id', p_pay_batch_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  SELECT chunk_row.*
  INTO v_chunk_row
  FROM public.banking_pay_operation_chunks AS chunk_row
  WHERE chunk_row.id = p_chunk_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'CHUNK_NOT_FOUND',
      'message', 'pay_provider_submit_chunk_stage_record: chunk not found',
      'chunk_id', p_chunk_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF v_chunk_row.operation_id <> p_operation_id THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'CHUNK_OPERATION_MISMATCH',
      'message', 'pay_provider_submit_chunk_stage_record: chunk does not belong to supplied operation',
      'chunk_id', p_chunk_id::text,
      'chunk_operation_id', v_chunk_row.operation_id::text,
      'operation_id', p_operation_id::text
    )::text USING ERRCODE = 'P0001';
  END IF;

  IF NOT (UPPER(BTRIM(COALESCE(v_chunk_row.phase, ''))) = 'SUBMIT_PROVIDER_TRANSFERS' OR UPPER(BTRIM(COALESCE(v_chunk_row.chunk_type, ''))) = 'TRANSFER_SUBMIT') THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'NOT_PROVIDER_SUBMIT_CHUNK',
      'message', 'pay_provider_submit_chunk_stage_record: chunk is not a provider-submit transfer chunk',
      'chunk_id', p_chunk_id::text,
      'phase', v_chunk_row.phase,
      'chunk_type', v_chunk_row.chunk_type
    )::text USING ERRCODE = 'P0001';
  END IF;

  v_actor_user_id := COALESCE(p_actor_user_id, v_operation_row.actor_user_id);
  v_chunk_lock_expired := UPPER(BTRIM(COALESCE(v_chunk_row.status, ''))) = 'RUNNING'
    AND v_chunk_row.completed_at_utc IS NULL
    AND (v_chunk_row.lock_expires_at_utc IS NULL OR v_chunk_row.lock_expires_at_utc < v_now);
  v_unfinalised_submit_chunk := UPPER(BTRIM(COALESCE(v_chunk_row.status, ''))) = 'RUNNING'
    AND v_chunk_row.completed_at_utc IS NULL;

  WITH raw_items AS (
    SELECT transfer_element.value AS item
    FROM jsonb_array_elements(CASE WHEN p_transfer_ids IS NOT NULL AND jsonb_typeof(p_transfer_ids) = 'array' THEN p_transfer_ids ELSE '[]'::jsonb END) AS transfer_element(value)
    UNION ALL
    SELECT transfer_element.value AS item
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_input_diagnostic->'transfer_ids') = 'array' THEN v_input_diagnostic->'transfer_ids' ELSE '[]'::jsonb END) AS transfer_element(value)
    UNION ALL
    SELECT to_jsonb(v_input_diagnostic->>'transfer_id') AS item
    WHERE NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'transfer_id', '')), '') IS NOT NULL
    UNION ALL
    SELECT transfer_element.value AS item
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_chunk_row.payload_json->'transfer_ids') = 'array' THEN v_chunk_row.payload_json->'transfer_ids' ELSE '[]'::jsonb END) AS transfer_element(value)
    UNION ALL
    SELECT transfer_element.value AS item
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_chunk_row.payload_json->'transfers') = 'array' THEN v_chunk_row.payload_json->'transfers' ELSE '[]'::jsonb END) AS transfer_element(value)
  ), raw_texts AS (
    SELECT NULLIF(BTRIM(COALESCE(
      CASE
        WHEN jsonb_typeof(raw_items.item) = 'string' THEN raw_items.item #>> '{}'
        WHEN jsonb_typeof(raw_items.item) = 'object' THEN COALESCE(
          raw_items.item->>'pay_bank_transfer_id',
          raw_items.item->>'transfer_id',
          raw_items.item->>'id'
        )
        ELSE NULL::text
      END,
      ''
    )), '') AS transfer_id_text
    FROM raw_items
  ), valid_ids AS (
    SELECT DISTINCT raw_texts.transfer_id_text::uuid AS transfer_id
    FROM raw_texts
    WHERE raw_texts.transfer_id_text ~* v_uuid_regex
  )
  SELECT COALESCE(array_agg(valid_ids.transfer_id ORDER BY valid_ids.transfer_id), ARRAY[]::uuid[]),
         COALESCE(jsonb_agg(to_jsonb(valid_ids.transfer_id::text) ORDER BY valid_ids.transfer_id), '[]'::jsonb),
         COUNT(*)::integer
  INTO v_transfer_ids,
       v_transfer_ids_json,
       v_transfer_count
  FROM valid_ids;

  WITH raw_items AS (
    SELECT transfer_element.value AS item
    FROM jsonb_array_elements(CASE WHEN p_transfer_ids IS NOT NULL AND jsonb_typeof(p_transfer_ids) = 'array' THEN p_transfer_ids ELSE '[]'::jsonb END) AS transfer_element(value)
    UNION ALL
    SELECT transfer_element.value AS item
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_input_diagnostic->'transfer_ids') = 'array' THEN v_input_diagnostic->'transfer_ids' ELSE '[]'::jsonb END) AS transfer_element(value)
    UNION ALL
    SELECT to_jsonb(v_input_diagnostic->>'transfer_id') AS item
    WHERE NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'transfer_id', '')), '') IS NOT NULL
    UNION ALL
    SELECT transfer_element.value AS item
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_chunk_row.payload_json->'transfer_ids') = 'array' THEN v_chunk_row.payload_json->'transfer_ids' ELSE '[]'::jsonb END) AS transfer_element(value)
    UNION ALL
    SELECT transfer_element.value AS item
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(v_chunk_row.payload_json->'transfers') = 'array' THEN v_chunk_row.payload_json->'transfers' ELSE '[]'::jsonb END) AS transfer_element(value)
  ), raw_texts AS (
    SELECT NULLIF(BTRIM(COALESCE(
      CASE
        WHEN jsonb_typeof(raw_items.item) = 'string' THEN raw_items.item #>> '{}'
        WHEN jsonb_typeof(raw_items.item) = 'object' THEN COALESCE(
          raw_items.item->>'pay_bank_transfer_id',
          raw_items.item->>'transfer_id',
          raw_items.item->>'id'
        )
        ELSE NULL::text
      END,
      ''
    )), '') AS transfer_id_text
    FROM raw_items
  )
  SELECT COUNT(*)::integer
  INTO v_invalid_transfer_text_count
  FROM raw_texts
  WHERE raw_texts.transfer_id_text IS NOT NULL
    AND raw_texts.transfer_id_text !~* v_uuid_regex;

  IF COALESCE(v_invalid_transfer_text_count, 0) > 0 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'INVALID_TRANSFER_ID',
      'message', 'pay_provider_submit_chunk_stage_record: one or more supplied transfer IDs are not UUIDs',
      'invalid_transfer_id_count', v_invalid_transfer_text_count
    )::text USING ERRCODE = 'P0001';
  END IF;

  SELECT COUNT(*)::integer
  INTO v_missing_transfer_count
  FROM unnest(v_transfer_ids) AS selected_transfer(transfer_id)
  LEFT JOIN public.pay_bank_transfers AS transfer_check
    ON transfer_check.id = selected_transfer.transfer_id
   AND transfer_check.pay_batch_id = p_pay_batch_id
  WHERE transfer_check.id IS NULL;

  IF COALESCE(v_missing_transfer_count, 0) > 0 THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_PROVIDER_SUBMIT_CHUNK_STAGE_RECORD',
      'code', 'TRANSFER_BATCH_MISMATCH_OR_NOT_FOUND',
      'message', 'pay_provider_submit_chunk_stage_record: one or more transfer IDs do not exist or do not belong to the supplied pay batch',
      'pay_batch_id', p_pay_batch_id::text,
      'transfer_ids', COALESCE(v_transfer_ids_json, '[]'::jsonb),
      'missing_or_wrong_batch_transfer_count', v_missing_transfer_count
    )::text USING ERRCODE = 'P0001';
  END IF;

  BEGIN
    v_helper_result := public.pay_provider_submit_diagnostic_get(
      p_pay_batch_id := p_pay_batch_id,
      p_operation_id := p_operation_id,
      p_transfer_id := NULL::uuid,
      p_chunk_id := p_chunk_id,
      p_counts_only := true
    );
  EXCEPTION
    WHEN undefined_function THEN
      v_helper_result := '{}'::jsonb;
    WHEN OTHERS THEN
      v_helper_result := '{}'::jsonb;
  END;

  IF COALESCE(v_helper_result #>> '{counts,provider_acceptance_evidence_count}', '') ~ '^[0-9]+$' THEN
    v_helper_acceptance_evidence_count := (v_helper_result #>> '{counts,provider_acceptance_evidence_count}')::integer;
  END IF;
  IF COALESCE(v_helper_result #>> '{counts,provider_response_present_count}', '') ~ '^[0-9]+$' THEN
    v_helper_response_present_count := (v_helper_result #>> '{counts,provider_response_present_count}')::integer;
  END IF;
  IF COALESCE(v_helper_result #>> '{counts,provider_request_sent_count}', '') ~ '^[0-9]+$' THEN
    v_helper_request_sent_count := (v_helper_result #>> '{counts,provider_request_sent_count}')::integer;
  END IF;
  IF COALESCE(v_helper_result #>> '{counts,provider_submission_unknown_count}', '') ~ '^[0-9]+$' THEN
    v_helper_unknown_count := (v_helper_result #>> '{counts,provider_submission_unknown_count}')::integer;
  END IF;
  IF COALESCE(v_helper_result #>> '{counts,provider_submission_rejected_count}', '') ~ '^[0-9]+$' THEN
    v_helper_rejected_count := (v_helper_result #>> '{counts,provider_submission_rejected_count}')::integer;
  END IF;
  IF COALESCE(v_helper_result #>> '{counts,provider_submission_malformed_response_count}', '') ~ '^[0-9]+$' THEN
    v_helper_malformed_count := (v_helper_result #>> '{counts,provider_submission_malformed_response_count}')::integer;
  END IF;
  IF COALESCE(v_helper_result #>> '{counts,provider_submission_blocked_pre_call_count}', '') ~ '^[0-9]+$' THEN
    v_helper_blocked_pre_call_count := (v_helper_result #>> '{counts,provider_submission_blocked_pre_call_count}')::integer;
  END IF;

  SELECT COUNT(DISTINCT transfer_row.id) FILTER (
          WHERE (
               NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') <> transfer_row.id::text
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
               AND UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
             )
             OR (
               NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') <> transfer_row.id::text
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_payment_id}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,rail_tx_id}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
               AND UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
             )
             OR (
               NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}', '')), '') IS NOT NULL
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}', '')), '') <> transfer_row.id::text
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
               AND NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_reference}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
               AND UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
             )
             OR COALESCE(event_flags.has_provider_acceptance_evidence, false) IS TRUE
        )::integer,
        COUNT(DISTINCT transfer_row.id) FILTER (
           WHERE lower(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_response_present}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_response_received}', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_http_status}', transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_error_code}', '')), '') IS NOT NULL
              OR COALESCE(event_flags.has_provider_response, false) IS TRUE
         )::integer,
         COUNT(DISTINCT transfer_row.id) FILTER (
           WHERE lower(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_request_sent}', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
              OR COALESCE(event_flags.has_provider_response, false) IS TRUE
              OR NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NOT NULL
         )::integer,
         COUNT(DISTINCT transfer_row.id) FILTER (
           WHERE UPPER(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED')
              OR COALESCE(event_flags.has_provider_rejection, false) IS TRUE
              OR CASE WHEN NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_http_status}', '')), '') ~ '^[0-9]+$' THEN (transfer_row.rail_meta_json #>> '{provider_submit_diagnostic,provider_http_status}')::integer >= 400 ELSE false END
         )::integer
  INTO v_direct_acceptance_evidence_count,
       v_direct_response_present_count,
       v_direct_request_sent_count,
       v_direct_rejection_count
  FROM public.pay_bank_transfers AS transfer_row
  JOIN unnest(v_transfer_ids) AS selected_transfer(transfer_id)
    ON selected_transfer.transfer_id = transfer_row.id
  LEFT JOIN LATERAL (
    SELECT COALESCE(BOOL_OR(UPPER(BTRIM(COALESCE(transfer_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')), false) AS has_provider_response,
           COALESCE(BOOL_OR(
             UPPER(BTRIM(COALESCE(transfer_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')
             AND (
               (
                 NULLIF(BTRIM(COALESCE(transfer_event.provider_event_id, '')), '') IS NOT NULL
                 AND NULLIF(BTRIM(COALESCE(transfer_event.provider_event_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
                 AND NULLIF(BTRIM(COALESCE(transfer_event.provider_event_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
               )
               OR (
                 NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') IS NOT NULL
                 AND NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
                 AND NULLIF(BTRIM(COALESCE(transfer_event.provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
               )
               OR (
                 NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}', transfer_event.raw_payload #>> '{provider_transaction_id}', transfer_event.raw_payload #>> '{provider_payment_id}', transfer_event.raw_payload #>> '{payment_id}', transfer_event.raw_payload #>> '{external_payment_id}', transfer_event.raw_payload #>> '{revolut_payment_id}', transfer_event.raw_payload #>> '{provider_reference}', transfer_event.raw_payload #>> '{rail_tx_id}', '')), '') IS NOT NULL
                 AND NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}', transfer_event.raw_payload #>> '{provider_transaction_id}', transfer_event.raw_payload #>> '{provider_payment_id}', transfer_event.raw_payload #>> '{payment_id}', transfer_event.raw_payload #>> '{external_payment_id}', transfer_event.raw_payload #>> '{revolut_payment_id}', transfer_event.raw_payload #>> '{provider_reference}', transfer_event.raw_payload #>> '{rail_tx_id}', '')), '') <> transfer_row.id::text
                 AND NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}', transfer_event.raw_payload #>> '{provider_transaction_id}', transfer_event.raw_payload #>> '{provider_payment_id}', transfer_event.raw_payload #>> '{payment_id}', transfer_event.raw_payload #>> '{external_payment_id}', transfer_event.raw_payload #>> '{revolut_payment_id}', transfer_event.raw_payload #>> '{provider_reference}', transfer_event.raw_payload #>> '{rail_tx_id}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__NO_REQUEST_ID__')
                 AND NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}', transfer_event.raw_payload #>> '{provider_transaction_id}', transfer_event.raw_payload #>> '{provider_payment_id}', transfer_event.raw_payload #>> '{payment_id}', transfer_event.raw_payload #>> '{external_payment_id}', transfer_event.raw_payload #>> '{revolut_payment_id}', transfer_event.raw_payload #>> '{provider_reference}', transfer_event.raw_payload #>> '{rail_tx_id}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
                 AND NULLIF(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_transaction_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,rail_tx_id}', transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_reference}', transfer_event.raw_payload #>> '{provider_transaction_id}', transfer_event.raw_payload #>> '{provider_payment_id}', transfer_event.raw_payload #>> '{payment_id}', transfer_event.raw_payload #>> '{external_payment_id}', transfer_event.raw_payload #>> '{revolut_payment_id}', transfer_event.raw_payload #>> '{provider_reference}', transfer_event.raw_payload #>> '{rail_tx_id}', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(transfer_event.idempotency_key, '')), ''), '__NO_EVENT_IDEMPOTENCY_KEY__')
               )
             )
             AND UPPER(BTRIM(COALESCE(transfer_event.normalised_state, transfer_event.provider_state, ''))) NOT IN ('REJECTED', 'FAILED', 'ERROR', 'DECLINED', 'CANCELLED', 'CANCELED')
           ), false) AS has_provider_acceptance_evidence,
           COALESCE(BOOL_OR(
             UPPER(BTRIM(COALESCE(transfer_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK')
             AND (
               UPPER(BTRIM(COALESCE(transfer_event.normalised_state, ''))) IN ('REJECTED', 'FAILED', 'ERROR', 'DECLINED', 'CANCELLED', 'CANCELED')
               OR UPPER(BTRIM(COALESCE(transfer_event.provider_state, ''))) IN ('REJECTED', 'FAILED', 'ERROR', 'DECLINED', 'CANCELLED', 'CANCELED')
               OR UPPER(BTRIM(COALESCE(transfer_event.raw_payload #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED')
             )
           ), false) AS has_provider_rejection
    FROM public.pay_bank_transfer_events AS transfer_event
    WHERE transfer_event.pay_bank_transfer_id = transfer_row.id
  ) AS event_flags ON true;

  v_provider_acceptance_evidence_count := GREATEST(COALESCE(v_helper_acceptance_evidence_count, 0), COALESCE(v_direct_acceptance_evidence_count, 0));
  v_provider_response_present_count := GREATEST(COALESCE(v_helper_response_present_count, 0), COALESCE(v_direct_response_present_count, 0));
  v_provider_request_sent_count := GREATEST(COALESCE(v_helper_request_sent_count, 0), COALESCE(v_direct_request_sent_count, 0));
  v_provider_rejection_count := GREATEST(COALESCE(v_helper_rejected_count, 0), COALESCE(v_direct_rejection_count, 0));
  v_provider_unknown_count := COALESCE(v_helper_unknown_count, 0);
  v_provider_malformed_count := COALESCE(v_helper_malformed_count, 0);
  v_provider_blocked_pre_call_count := COALESCE(v_helper_blocked_pre_call_count, 0);

  SELECT transfer_row.id::text,
         transfer_row.rail_provider,
         transfer_row.rail_env,
         NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), ''),
         NULLIF(BTRIM(COALESCE(transfer_row.rail_state, '')), ''),
         NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''),
         NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), '')
  INTO v_primary_transfer_id,
       v_primary_rail_provider,
       v_primary_rail_env,
       v_primary_rail_tx_id,
       v_primary_rail_state,
       v_primary_request_id,
       v_primary_payment_reference
  FROM public.pay_bank_transfers AS transfer_row
  JOIN unnest(v_transfer_ids) AS selected_transfer(transfer_id)
    ON selected_transfer.transfer_id = transfer_row.id
  ORDER BY transfer_row.created_at_utc NULLS FIRST, transfer_row.id
  LIMIT 1;

  SELECT candidate_ranked.diagnostic,
         candidate_ranked.source_name,
         candidate_ranked.precedence_rank,
         NULLIF(UPPER(BTRIM(COALESCE(candidate_ranked.diagnostic->>'provider_submission_status', ''))), ''),
         NULLIF(BTRIM(COALESCE(candidate_ranked.diagnostic->>'review_reason_code', '')), '')
  INTO v_existing_diagnostic,
       v_existing_diagnostic_source,
       v_existing_rank,
       v_existing_status,
       v_existing_review_reason_code
  FROM (
    SELECT candidate_source.source_name,
           candidate_source.source_order,
           candidate_source.diagnostic,
           CASE
             WHEN (
                NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_transaction_id', candidate_source.diagnostic->>'provider_payment_id', candidate_source.diagnostic->>'rail_tx_id', '')), '') IS NOT NULL
                AND NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_transaction_id', candidate_source.diagnostic->>'provider_payment_id', candidate_source.diagnostic->>'rail_tx_id', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'request_id', '')), ''), '__NO_REQUEST_ID__')
                AND NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_transaction_id', candidate_source.diagnostic->>'provider_payment_id', candidate_source.diagnostic->>'rail_tx_id', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'idempotency_key', '')), ''), '__NO_IDEMPOTENCY_KEY__')
                AND NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_transaction_id', candidate_source.diagnostic->>'provider_payment_id', candidate_source.diagnostic->>'rail_tx_id', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'local_provider_request_id', '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
                AND UPPER(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_status', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
              )
              OR (
                NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_reference', '')), '') IS NOT NULL
                AND NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_reference', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'request_id', '')), ''), '__NO_REQUEST_ID__')
                AND NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_reference', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'idempotency_key', '')), ''), '__NO_IDEMPOTENCY_KEY__')
                AND NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_reference', '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'local_provider_request_id', '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
                AND UPPER(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_status', ''))) NOT IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL', 'NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
              ) THEN 500
             WHEN UPPER(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_status', ''))) = 'MANUAL_RESOLVED_NO_PAYMENT_MADE' THEN 450
             WHEN UPPER(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_status', ''))) IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED')
               OR lower(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_rejected', candidate_source.diagnostic->>'provider_rejected', candidate_source.diagnostic->>'provider_submission_failed', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
               OR CASE WHEN NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_http_status', '')), '') ~ '^[0-9]+$' THEN (candidate_source.diagnostic->>'provider_http_status')::integer >= 400 ELSE false END THEN 420
             WHEN UPPER(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_status', ''))) IN ('PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID')
               OR lower(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_response_present', candidate_source.diagnostic->>'provider_response_received', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
               OR NULLIF(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_error_code', '')), '') IS NOT NULL THEN 400
             WHEN UPPER(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_status', ''))) IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK')
               OR lower(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_request_sent', ''))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 300
             WHEN UPPER(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_status', ''))) = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 200
             WHEN UPPER(BTRIM(COALESCE(candidate_source.diagnostic->>'provider_submission_status', ''))) = 'NO_PROVIDER_SUBMISSION_ATTEMPTED' THEN 100
             ELSE 0
           END AS precedence_rank
    FROM (
      SELECT 'chunk_result'::text AS source_name,
             1 AS source_order,
             CASE WHEN jsonb_typeof(v_chunk_row.result_json->'provider_submit_diagnostic') = 'object' THEN v_chunk_row.result_json->'provider_submit_diagnostic' ELSE '{}'::jsonb END AS diagnostic
      UNION ALL
      SELECT 'chunk_error'::text AS source_name,
             2 AS source_order,
             CASE WHEN jsonb_typeof(v_chunk_row.error_json->'provider_submit_diagnostic') = 'object' THEN v_chunk_row.error_json->'provider_submit_diagnostic' ELSE '{}'::jsonb END AS diagnostic
      UNION ALL
      SELECT 'operation_progress'::text AS source_name,
             3 AS source_order,
             CASE WHEN jsonb_typeof(v_operation_row.progress_json->'provider_submit_diagnostic') = 'object' THEN v_operation_row.progress_json->'provider_submit_diagnostic' ELSE '{}'::jsonb END AS diagnostic
      UNION ALL
      SELECT 'operation_result'::text AS source_name,
             4 AS source_order,
             CASE WHEN jsonb_typeof(v_operation_row.result_json->'provider_submit_diagnostic') = 'object' THEN v_operation_row.result_json->'provider_submit_diagnostic' ELSE '{}'::jsonb END AS diagnostic
      UNION ALL
      SELECT 'operation_error'::text AS source_name,
             5 AS source_order,
             CASE WHEN jsonb_typeof(v_operation_row.error_json->'provider_submit_diagnostic') = 'object' THEN v_operation_row.error_json->'provider_submit_diagnostic' ELSE '{}'::jsonb END AS diagnostic
      UNION ALL
      SELECT 'diagnostic_helper'::text AS source_name,
             6 AS source_order,
             CASE WHEN jsonb_typeof(v_helper_result->'provider_submit_diagnostic') = 'object' THEN v_helper_result->'provider_submit_diagnostic' ELSE '{}'::jsonb END AS diagnostic
    ) AS candidate_source
    WHERE candidate_source.diagnostic <> '{}'::jsonb
  ) AS candidate_ranked
  ORDER BY candidate_ranked.precedence_rank DESC, candidate_ranked.source_order ASC
  LIMIT 1;

  IF v_existing_diagnostic IS NULL THEN
    v_existing_diagnostic := '{}'::jsonb;
    v_existing_diagnostic_source := NULL::text;
    v_existing_rank := 0;
    v_existing_status := NULL::text;
    v_existing_review_reason_code := NULL::text;
  END IF;

  IF v_provider_acceptance_evidence_count > 0 AND COALESCE(v_existing_rank, 0) < 500 THEN
    v_existing_diagnostic := jsonb_build_object(
      'diagnostic_version', 1,
      'generated_at_utc', v_now::text,
      'provider_submission_status', 'PROVIDER_SUBMISSION_ACCEPTED',
      'review_reason_code', 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT',
      'provider_submission_attempted', true,
      'provider_request_sent', true,
      'provider_response_received', true,
      'provider_response_present', true,
      'provider_submission_accepted', true,
      'provider_acceptance_evidence_present', true,
      'manual_resolution_required', false,
      'safe_retry_available', false,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'chunk_id', p_chunk_id::text,
      'chunk_ids', jsonb_build_array(p_chunk_id::text),
      'transfer_id', v_primary_transfer_id,
      'transfer_ids', COALESCE(v_transfer_ids_json, '[]'::jsonb),
      'rail_tx_id', v_primary_rail_tx_id,
      'rail_state', v_primary_rail_state,
      'provider_acceptance_evidence_count', v_provider_acceptance_evidence_count
    );
    v_existing_diagnostic_source := 'direct_provider_acceptance_evidence';
    v_existing_status := 'PROVIDER_SUBMISSION_ACCEPTED';
    v_existing_review_reason_code := 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT';
    v_existing_rank := 500;
  END IF;

  v_provider_call_stage := v_stage_upper;
  v_incoming_status := NULLIF(UPPER(BTRIM(COALESCE(v_input_diagnostic->>'provider_submission_status', v_input_diagnostic->>'provider_status', ''))), '');
  v_provider_transaction_id := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'provider_transaction_id', v_input_diagnostic->>'provider_payment_id', '')), '');
  v_provider_reference := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'provider_reference', '')), '');
  v_provider_state := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'provider_state', '')), '');
  v_rail_tx_id := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'rail_tx_id', '')), '');
  v_rail_state := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'rail_state', '')), '');
  v_request_id := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'request_id', v_primary_request_id, '')), '');
  v_idempotency_key := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'idempotency_key', '')), '');
  v_local_provider_request_id := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'local_provider_request_id', v_request_id, '')), '');
  v_provider_http_status := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'provider_http_status', '')), '');
  v_provider_error_code := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'provider_error_code', '')), '');
  v_provider_error_message_redacted := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'provider_error_message_redacted', '')), '');
  v_provider_response_redacted := v_input_diagnostic->'provider_response_redacted';
  v_provider_error_redacted := v_input_diagnostic->'provider_error_redacted';

  IF v_idempotency_key IS NULL THEN
    v_idempotency_key := NULLIF(BTRIM(COALESCE(v_request_id, '')), '');
  END IF;

  IF v_provider_reference IS NOT NULL
     AND v_provider_reference IN (
       COALESCE(v_primary_transfer_id, '__NO_TRANSFER_ID__'),
       COALESCE(v_request_id, '__NO_REQUEST_ID__'),
       COALESCE(v_idempotency_key, '__NO_IDEMPOTENCY_KEY__'),
       COALESCE(v_local_provider_request_id, '__NO_LOCAL_PROVIDER_REQUEST_ID__'),
       COALESCE(v_primary_payment_reference, '__NO_PAYMENT_REFERENCE__')
     ) THEN
    v_provider_reference := NULL::text;
  END IF;

  IF v_provider_transaction_id IS NOT NULL
     AND v_provider_transaction_id IN (
       COALESCE(v_primary_transfer_id, '__NO_TRANSFER_ID__'),
       COALESCE(v_request_id, '__NO_REQUEST_ID__'),
       COALESCE(v_idempotency_key, '__NO_IDEMPOTENCY_KEY__'),
       COALESCE(v_local_provider_request_id, '__NO_LOCAL_PROVIDER_REQUEST_ID__'),
       COALESCE(v_primary_payment_reference, '__NO_PAYMENT_REFERENCE__')
     ) THEN
    v_provider_transaction_id := NULL::text;
  END IF;

  IF v_rail_tx_id IS NOT NULL
     AND v_rail_tx_id IN (
       COALESCE(v_primary_transfer_id, '__NO_TRANSFER_ID__'),
       COALESCE(v_request_id, '__NO_REQUEST_ID__'),
       COALESCE(v_idempotency_key, '__NO_IDEMPOTENCY_KEY__'),
       COALESCE(v_local_provider_request_id, '__NO_LOCAL_PROVIDER_REQUEST_ID__'),
       COALESCE(v_primary_payment_reference, '__NO_PAYMENT_REFERENCE__')
     ) THEN
    v_rail_tx_id := NULL::text;
  END IF;

  v_provider_request_sent := lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_request_sent', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_request_sent_confirmed', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_stage_upper IN ('PROVIDER_PAYMENT_CREATE_REQUEST_SENT', 'PROVIDER_PAYMENT_CREATE_RESPONSE_RECEIVED', 'PROVIDER_PAYMENT_CREATE_ACCEPTED', 'PROVIDER_PAYMENT_CREATE_REJECTED', 'PROVIDER_PAYMENT_CREATE_UNKNOWN', 'PROVIDER_PAYMENT_CREATE_MALFORMED')
    OR v_stage_upper LIKE '%REQUEST_SENT%';

  v_provider_response_received := lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_response_received', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_stage_upper IN ('PROVIDER_PAYMENT_CREATE_RESPONSE_RECEIVED', 'PROVIDER_PAYMENT_CREATE_ACCEPTED', 'PROVIDER_PAYMENT_CREATE_REJECTED', 'PROVIDER_PAYMENT_CREATE_MALFORMED');

  v_provider_response_present := lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_response_present', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_provider_response_received IS TRUE
    OR NULLIF(BTRIM(COALESCE(v_provider_http_status, v_provider_error_code, '')), '') IS NOT NULL;

  v_provider_submission_accepted := (
    lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_submission_accepted', v_input_diagnostic->>'provider_accepted', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_incoming_status = 'PROVIDER_SUBMISSION_ACCEPTED'
    OR v_stage_upper = 'PROVIDER_PAYMENT_CREATE_ACCEPTED'
  )
  AND (
    (
      NULLIF(BTRIM(COALESCE(v_provider_transaction_id, v_rail_tx_id, '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(v_provider_transaction_id, v_rail_tx_id, '')), '') <> COALESCE(v_primary_transfer_id, '__NO_TRANSFER_ID__')
      AND NULLIF(BTRIM(COALESCE(v_provider_transaction_id, v_rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(v_request_id, '')), ''), '__NO_REQUEST_ID__')
      AND NULLIF(BTRIM(COALESCE(v_provider_transaction_id, v_rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(v_idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
      AND NULLIF(BTRIM(COALESCE(v_provider_transaction_id, v_rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(v_local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
      AND NULLIF(BTRIM(COALESCE(v_provider_transaction_id, v_rail_tx_id, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(v_primary_payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
    )
    OR (
      NULLIF(BTRIM(COALESCE(v_provider_reference, '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(v_provider_reference, '')), '') <> COALESCE(v_primary_transfer_id, '__NO_TRANSFER_ID__')
      AND NULLIF(BTRIM(COALESCE(v_provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(v_request_id, '')), ''), '__NO_REQUEST_ID__')
      AND NULLIF(BTRIM(COALESCE(v_provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(v_idempotency_key, '')), ''), '__NO_IDEMPOTENCY_KEY__')
      AND NULLIF(BTRIM(COALESCE(v_provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(v_local_provider_request_id, '')), ''), '__NO_LOCAL_PROVIDER_REQUEST_ID__')
      AND NULLIF(BTRIM(COALESCE(v_provider_reference, '')), '') <> COALESCE(NULLIF(BTRIM(COALESCE(v_primary_payment_reference, '')), ''), '__NO_PAYMENT_REFERENCE__')
    )
  );

  v_provider_submission_rejected := lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_submission_rejected', v_input_diagnostic->>'provider_rejected', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_incoming_status IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED')
    OR v_stage_upper = 'PROVIDER_PAYMENT_CREATE_REJECTED'
    OR CASE WHEN COALESCE(v_provider_http_status, '') ~ '^[0-9]+$' THEN v_provider_http_status::integer >= 400 ELSE false END;

  v_provider_request_impossible := lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_request_impossible', v_input_diagnostic->>'durable_provider_request_impossible', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_incoming_status = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL'
    OR v_stage_upper IN ('PROVIDER_PRECALL_VALIDATION_FAILED', 'PROVIDER_SUBMIT_BLOCKED_PRE_CALL', 'FUNDING_ACCOUNT_MISSING', 'FUNDING_CHECK_FAILED', 'COUNTERPARTY_MAPPING_FAILED', 'RAIL_ENV_MISMATCH', 'PROVIDER_PAYMENT_CREATE_BLOCKED_PRE_CALL');

  IF v_incoming_status = 'PROVIDER_SUBMISSION_ACCEPTED' AND v_provider_submission_accepted IS NOT TRUE THEN
    v_incoming_status := 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID';
    v_provider_response_received := true;
    v_provider_response_present := true;
    v_provider_request_sent := true;
  END IF;

  IF v_provider_submission_accepted IS TRUE THEN
    v_incoming_status := 'PROVIDER_SUBMISSION_ACCEPTED';
    v_provider_request_sent := true;
    v_provider_response_received := true;
    v_provider_response_present := true;
    v_provider_acceptance_evidence_present := true;
  ELSIF v_provider_submission_rejected IS TRUE THEN
    v_incoming_status := 'PROVIDER_SUBMISSION_REJECTED';
    v_provider_request_sent := true;
    v_provider_response_received := true;
    v_provider_response_present := true;
  ELSIF v_incoming_status = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' OR v_stage_upper = 'PROVIDER_PAYMENT_CREATE_MALFORMED' THEN
    v_incoming_status := 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE';
    v_provider_request_sent := true;
    v_provider_response_received := true;
    v_provider_response_present := true;
  ELSIF v_provider_request_sent IS TRUE AND v_provider_response_present IS NOT TRUE AND v_provider_response_received IS NOT TRUE THEN
    v_incoming_status := 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME';
    v_provider_response_received := false;
    v_provider_response_present := false;
  ELSIF v_provider_response_present IS TRUE AND v_provider_submission_accepted IS NOT TRUE AND v_provider_submission_rejected IS NOT TRUE THEN
    v_incoming_status := COALESCE(NULLIF(v_incoming_status, ''), 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID');
    IF v_incoming_status NOT IN ('PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID') THEN
      v_incoming_status := 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID';
    END IF;
    v_provider_request_sent := true;
    v_provider_response_received := true;
  ELSIF v_provider_request_impossible IS TRUE THEN
    v_incoming_status := 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL';
    v_provider_request_sent := false;
    v_provider_response_received := false;
    v_provider_response_present := false;
  ELSIF v_incoming_status IS NULL THEN
    v_incoming_status := 'NO_PROVIDER_SUBMISSION_ATTEMPTED';
  END IF;

  IF v_chunk_lock_expired IS TRUE
     AND v_provider_request_impossible IS NOT TRUE
     AND v_provider_acceptance_evidence_count = 0
     AND v_provider_response_present_count = 0
     AND v_incoming_status IN ('NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL') THEN
    v_incoming_status := 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK';
  END IF;

  v_provider_submission_attempted := lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_submission_attempted', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_provider_request_sent IS TRUE
    OR v_provider_response_present IS TRUE
    OR v_provider_submission_accepted IS TRUE
    OR v_provider_submission_rejected IS TRUE;

  v_provider_submission_unknown := lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_submission_unknown', v_input_diagnostic->>'provider_unknown', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_incoming_status IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID');

  v_provider_acceptance_evidence_present := v_provider_submission_accepted IS TRUE
    OR v_provider_acceptance_evidence_count > 0;

  v_provider_submission_failed := lower(BTRIM(COALESCE(v_input_diagnostic->>'provider_submission_failed', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_provider_submission_rejected IS TRUE;

  v_manual_resolution_required := lower(BTRIM(COALESCE(v_input_diagnostic->>'manual_resolution_required', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    OR v_incoming_status IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID');

  v_safe_retry_available := lower(BTRIM(COALESCE(v_input_diagnostic->>'safe_retry_available', ''))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND v_incoming_status NOT IN ('PROVIDER_SUBMISSION_ACCEPTED', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID');

  v_automatic_retry_blocked := v_incoming_status IN ('PROVIDER_SUBMISSION_ACCEPTED', 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK', 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID');
  v_retry_blocked_reason := CASE WHEN v_automatic_retry_blocked IS TRUE THEN v_incoming_status ELSE NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'retry_blocked_reason', '')), '') END;

  v_incoming_review_reason_code := NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'review_reason_code', '')), '');
  IF v_incoming_review_reason_code IS NULL THEN
    v_incoming_review_reason_code := CASE
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_REJECTED' THEN 'PROVIDER_REJECTED_PAYMENT'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'PROVIDER_RESPONSE_MALFORMED'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'PROVIDER_RESPONSE_PRESENT_NO_EXTERNAL_ID'
      WHEN v_incoming_status = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'PROVIDER_REQUEST_SENT_NO_RESPONSE'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'STALE_RUNNING_PROVIDER_SUBMIT_CHUNK'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'PROVIDER_SUBMIT_BLOCKED_PRE_CALL'
      ELSE 'NO_PROVIDER_SUBMIT_ATTEMPT'
    END;
  END IF;

  v_crash_safety_status_if_lock_expires := CASE
    WHEN v_provider_request_sent IS TRUE THEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME'
    WHEN v_provider_request_impossible IS TRUE THEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL'
    ELSE 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK'
  END;

  v_recommended_action := COALESCE(
    NULLIF(BTRIM(COALESCE(v_input_diagnostic->>'recommended_action', '')), ''),
    CASE
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence exists. Do not retry unless reconciled.'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_REJECTED' THEN 'Review the provider rejection/error and retry only after correction.'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider returned an unusable response. Check Revolut/bank records before retry.'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'A provider response was recorded, but no external transaction ID was captured. Check Revolut/bank records before retry.'
      WHEN v_incoming_status = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'Provider request may have been sent, but no usable response was recorded. Check Revolut/bank records before retry.'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Check Revolut/bank records before retry. If no payment was made, record manual no-payment confirmation and reset for retry.'
      WHEN v_incoming_status = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider was not called. Fix the local blocker and retry if cleanup allows.'
      ELSE 'Provider-submit stage recorded before the provider request boundary.'
    END
  );

  v_incoming_rank := CASE
    WHEN v_provider_submission_accepted IS TRUE THEN 500
    WHEN v_incoming_status = 'MANUAL_RESOLVED_NO_PAYMENT_MADE' THEN 450
    WHEN v_provider_submission_rejected IS TRUE OR v_provider_submission_failed IS TRUE OR v_incoming_status IN ('PROVIDER_SUBMISSION_REJECTED', 'PROVIDER_SUBMISSION_FAILED') THEN 420
    WHEN v_provider_response_present IS TRUE OR v_provider_response_received IS TRUE OR v_incoming_status IN ('PROVIDER_SUBMISSION_MALFORMED_RESPONSE', 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID') THEN 400
    WHEN v_provider_request_sent IS TRUE OR v_incoming_status IN ('UNKNOWN_PROVIDER_SUBMISSION_OUTCOME', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK') THEN 300
    WHEN v_incoming_status = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 200
    WHEN v_incoming_status = 'NO_PROVIDER_SUBMISSION_ATTEMPTED' THEN 100
    ELSE 0
  END;

  v_incoming_diagnostic := jsonb_strip_nulls(
    v_input_diagnostic
    || jsonb_build_object(
      'diagnostic_version', 1,
      'generated_at_utc', v_now::text,
      'stage_recorded_at_utc', v_now::text,
      'stage_recorded_by_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
      'provider_call_stage', v_provider_call_stage,
      'provider_stage_recorded', v_stage,
      'provider_submission_status', v_incoming_status,
      'review_reason_code', v_incoming_review_reason_code,
      'provider_submission_attempted', v_provider_submission_attempted,
      'provider_request_sent', v_provider_request_sent,
      'provider_response_received', v_provider_response_received,
      'provider_response_present', v_provider_response_present,
      'provider_submission_accepted', v_provider_submission_accepted,
      'provider_submission_rejected', v_provider_submission_rejected,
      'provider_submission_failed', v_provider_submission_failed,
      'provider_submission_unknown', v_provider_submission_unknown,
      'provider_acceptance_evidence_present', v_provider_acceptance_evidence_present,
      'manual_resolution_required', v_manual_resolution_required,
      'safe_retry_available', v_safe_retry_available,
      'automatic_retry_blocked', v_automatic_retry_blocked,
      'retry_blocked_reason', v_retry_blocked_reason,
      'recommended_action', v_recommended_action,
      'crash_safety_status_if_lock_expires', v_crash_safety_status_if_lock_expires,
      'provider_request_impossible', v_provider_request_impossible,
      'durable_provider_request_impossible', v_provider_request_impossible,
      'stage_precedence_rank', v_incoming_rank,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'chunk_id', p_chunk_id::text,
      'chunk_ids', jsonb_build_array(p_chunk_id::text),
      'transfer_id', v_primary_transfer_id,
      'transfer_ids', COALESCE(v_transfer_ids_json, '[]'::jsonb),
      'rail_provider', v_primary_rail_provider,
      'rail_env', v_primary_rail_env,
      'rail_tx_id', v_rail_tx_id,
      'rail_state', v_rail_state,
      'provider_transaction_id', v_provider_transaction_id,
      'provider_reference', v_provider_reference,
      'provider_state', v_provider_state,
      'request_id', v_request_id,
      'idempotency_key', v_idempotency_key,
      'local_provider_request_id', v_local_provider_request_id,
      'provider_http_status', v_provider_http_status,
      'provider_error_code', v_provider_error_code,
      'provider_error_message_redacted', v_provider_error_message_redacted,
      'provider_response_redacted', v_provider_response_redacted,
      'provider_error_redacted', v_provider_error_redacted,
      'chunk_status', v_chunk_row.status,
      'chunk_started_at_utc', CASE WHEN v_chunk_row.started_at_utc IS NULL THEN NULL ELSE v_chunk_row.started_at_utc::text END,
      'chunk_completed_at_utc', CASE WHEN v_chunk_row.completed_at_utc IS NULL THEN NULL ELSE v_chunk_row.completed_at_utc::text END,
      'chunk_lock_expires_at_utc', CASE WHEN v_chunk_row.lock_expires_at_utc IS NULL THEN NULL ELSE v_chunk_row.lock_expires_at_utc::text END,
      'chunk_lock_expired', v_chunk_lock_expired,
      'unfinalised_submit_chunk', v_unfinalised_submit_chunk,
      'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
      'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
      'provider_rejection_count', COALESCE(v_provider_rejection_count, 0),
      'provider_unknown_count', COALESCE(v_provider_unknown_count, 0),
      'provider_malformed_count', COALESCE(v_provider_malformed_count, 0),
      'local_correlation_only_fields', jsonb_build_array('request_id', 'idempotency_key', 'payment_reference', 'chunk_id', 'operation_id')
    )
  );

  IF COALESCE(v_existing_rank, 0) > v_incoming_rank THEN
    v_apply_incoming := false;
    v_preserved_stronger_provider_evidence := true;
    v_final_rank := v_existing_rank;
    v_final_diagnostic := jsonb_strip_nulls(
      v_existing_diagnostic
      || jsonb_build_object(
        'stage_recorded_at_utc', v_now::text,
        'stage_recorded_by_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
        'last_stage_recorded', v_stage,
        'last_stage_recorded_status', v_incoming_status,
        'ignored_weaker_stage_record', true,
        'ignored_weaker_stage', v_stage,
        'ignored_weaker_stage_status', v_incoming_status,
        'ignored_weaker_stage_rank', v_incoming_rank,
        'protected_by_existing_provider_evidence', true,
        'existing_provider_submit_diagnostic_source', v_existing_diagnostic_source,
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'chunk_id', p_chunk_id::text,
        'chunk_ids', jsonb_build_array(p_chunk_id::text),
        'transfer_id', v_primary_transfer_id,
        'transfer_ids', COALESCE(v_transfer_ids_json, '[]'::jsonb),
        'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
        'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
        'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
        'provider_rejection_count', COALESCE(v_provider_rejection_count, 0)
      )
    );
  ELSE
    v_apply_incoming := true;
    v_preserved_stronger_provider_evidence := false;
    v_final_rank := v_incoming_rank;
    v_final_diagnostic := jsonb_strip_nulls(COALESCE(v_existing_diagnostic, '{}'::jsonb) || v_incoming_diagnostic);
  END IF;

  v_final_status := NULLIF(UPPER(BTRIM(COALESCE(v_final_diagnostic->>'provider_submission_status', ''))), '');
  IF v_final_status IS NULL THEN
    v_final_status := 'NO_PROVIDER_SUBMISSION_ATTEMPTED';
  END IF;

  IF v_chunk_lock_expired IS TRUE
     AND v_provider_acceptance_evidence_count = 0
     AND v_provider_response_present_count = 0
     AND lower(BTRIM(COALESCE(v_final_diagnostic->>'provider_request_impossible', v_final_diagnostic->>'durable_provider_request_impossible', ''))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
     AND v_final_status IN ('NO_PROVIDER_SUBMISSION_ATTEMPTED', 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL') THEN
    v_final_status := 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK';
    v_final_rank := GREATEST(v_final_rank, 300);
    v_final_diagnostic := jsonb_strip_nulls(
      v_final_diagnostic || jsonb_build_object(
        'provider_submission_status', v_final_status,
        'review_reason_code', 'STALE_RUNNING_PROVIDER_SUBMIT_CHUNK',
        'provider_submission_unknown', true,
        'stale_submit_chunk', true,
        'manual_resolution_required', true,
        'safe_retry_available', false,
        'automatic_retry_blocked', true,
        'retry_blocked_reason', 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK',
        'recommended_action', 'Check Revolut/bank records before retry. If no payment was made, record manual no-payment confirmation and reset for retry.'
      )
    );
  END IF;

  v_final_review_reason_code := NULLIF(BTRIM(COALESCE(v_final_diagnostic->>'review_reason_code', '')), '');
  IF v_final_review_reason_code IS NULL THEN
    v_final_review_reason_code := CASE
      WHEN v_final_status = 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'PROVIDER_ACCEPTANCE_EVIDENCE_PRESENT'
      WHEN v_final_status = 'PROVIDER_SUBMISSION_REJECTED' THEN 'PROVIDER_REJECTED_PAYMENT'
      WHEN v_final_status = 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'PROVIDER_RESPONSE_MALFORMED'
      WHEN v_final_status = 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'PROVIDER_RESPONSE_PRESENT_NO_EXTERNAL_ID'
      WHEN v_final_status = 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'PROVIDER_REQUEST_SENT_NO_RESPONSE'
      WHEN v_final_status = 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'STALE_RUNNING_PROVIDER_SUBMIT_CHUNK'
      WHEN v_final_status = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'PROVIDER_SUBMIT_BLOCKED_PRE_CALL'
      ELSE 'NO_PROVIDER_SUBMIT_ATTEMPT'
    END;
  END IF;

  v_final_diagnostic := jsonb_strip_nulls(
    v_final_diagnostic
    || jsonb_build_object(
      'diagnostic_version', 1,
      'generated_at_utc', v_now::text,
      'provider_submission_status', v_final_status,
      'review_reason_code', v_final_review_reason_code,
      'stage_precedence_rank', v_final_rank,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'chunk_id', p_chunk_id::text,
      'chunk_ids', jsonb_build_array(p_chunk_id::text),
      'transfer_id', v_primary_transfer_id,
      'transfer_ids', COALESCE(v_transfer_ids_json, '[]'::jsonb),
      'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
      'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
      'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
      'provider_rejection_count', COALESCE(v_provider_rejection_count, 0),
      'preserved_stronger_provider_evidence', v_preserved_stronger_provider_evidence
    )
  );

  v_chunk_result_before := COALESCE(v_chunk_row.result_json, '{}'::jsonb);
  v_stage_history := CASE WHEN jsonb_typeof(v_chunk_result_before->'provider_submit_stage_history') = 'array' THEN v_chunk_result_before->'provider_submit_stage_history' ELSE '[]'::jsonb END;
  v_stage_history_entry := jsonb_strip_nulls(jsonb_build_object(
    'stage', v_stage,
    'recorded_at_utc', v_now::text,
    'actor_user_id', CASE WHEN v_actor_user_id IS NULL THEN NULL ELSE v_actor_user_id::text END,
    'incoming_provider_submission_status', v_incoming_status,
    'provider_submission_status', v_final_status,
    'review_reason_code', v_final_review_reason_code,
    'incoming_precedence_rank', v_incoming_rank,
    'existing_precedence_rank', v_existing_rank,
    'final_precedence_rank', v_final_rank,
    'incoming_applied', v_apply_incoming,
    'protected_by_existing_provider_evidence', v_preserved_stronger_provider_evidence,
    'provider_request_sent', lower(BTRIM(COALESCE(v_final_diagnostic->>'provider_request_sent', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'provider_response_present', lower(BTRIM(COALESCE(v_final_diagnostic->>'provider_response_present', v_final_diagnostic->>'provider_response_received', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'provider_acceptance_evidence_present', lower(BTRIM(COALESCE(v_final_diagnostic->>'provider_acceptance_evidence_present', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'crash_safety_status_if_lock_expires', COALESCE(v_final_diagnostic->>'crash_safety_status_if_lock_expires', v_crash_safety_status_if_lock_expires)
  ));

  v_before_json := jsonb_build_object(
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_status', v_operation_row.status,
    'operation_phase', v_operation_row.phase,
    'operation_progress_json', COALESCE(v_operation_row.progress_json, '{}'::jsonb),
    'chunk', to_jsonb(v_chunk_row)
  );

  v_chunk_result_after := jsonb_strip_nulls(
    v_chunk_result_before
    || jsonb_build_object(
      'provider_submit_diagnostic', v_final_diagnostic,
      'provider_submission_status', v_final_status,
      'review_reason_code', v_final_review_reason_code,
      'provider_call_stage', COALESCE(v_final_diagnostic->>'provider_call_stage', v_provider_call_stage),
      'provider_submit_stage_last_recorded', v_stage,
      'provider_submit_stage_last_recorded_at_utc', v_now::text,
      'provider_submit_stage_history', v_stage_history || jsonb_build_array(v_stage_history_entry),
      'provider_submit_stage_incoming_applied', v_apply_incoming,
      'provider_submit_stage_protected_by_existing_evidence', v_preserved_stronger_provider_evidence
    )
  );

  UPDATE public.banking_pay_operation_chunks AS chunk_update
  SET result_json = v_chunk_result_after,
      updated_at_utc = v_now
  WHERE chunk_update.id = p_chunk_id
    AND chunk_update.operation_id = p_operation_id
  RETURNING chunk_update.*
  INTO v_after_chunk_row;

  v_operation_progress_after := jsonb_strip_nulls(
    COALESCE(v_operation_row.progress_json, '{}'::jsonb)
    || jsonb_build_object(
      'provider_submit_diagnostic', v_final_diagnostic,
      'provider_submission_status', v_final_status,
      'review_reason_code', v_final_review_reason_code,
      'provider_call_stage', COALESCE(v_final_diagnostic->>'provider_call_stage', v_provider_call_stage),
      'provider_submit_stage_last_recorded', v_stage,
      'provider_submit_stage_last_recorded_at_utc', v_now::text,
      'provider_submit_stage_incoming_applied', v_apply_incoming,
      'provider_submit_stage_protected_by_existing_evidence', v_preserved_stronger_provider_evidence,
      'last_provider_submit_chunk_id', p_chunk_id::text
    )
  );

  UPDATE public.banking_pay_operations AS operation_update
  SET progress_json = v_operation_progress_after,
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  v_after_json := jsonb_build_object(
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'chunk_id', p_chunk_id::text,
    'stage', v_stage,
    'provider_submit_diagnostic', v_final_diagnostic,
    'provider_submission_status', v_final_status,
    'review_reason_code', v_final_review_reason_code,
    'incoming_applied', v_apply_incoming,
    'protected_by_existing_provider_evidence', v_preserved_stronger_provider_evidence,
    'existing_provider_submit_diagnostic_source', v_existing_diagnostic_source,
    'existing_precedence_rank', v_existing_rank,
    'incoming_precedence_rank', v_incoming_rank,
    'final_precedence_rank', v_final_rank,
    'transfer_ids', COALESCE(v_transfer_ids_json, '[]'::jsonb),
    'chunk', to_jsonb(v_after_chunk_row),
    'operation_progress_json', v_operation_progress_after
  );

  BEGIN
    PERFORM public._audit_insert(
      'banking_pay_operation_chunks',
      p_chunk_id::text,
      'PAYMENT_PROVIDER_SUBMIT_CHUNK_STAGE_RECORDED',
      v_before_json,
      v_after_json,
      'payment_provider_submit_chunk_stage_recorded',
      v_actor_user_id
    );
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END;

  RETURN jsonb_build_object(
    'ok', true,
    'recorded', true,
    'incoming_applied', v_apply_incoming,
    'protected_by_existing_provider_evidence', v_preserved_stronger_provider_evidence,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'chunk_id', p_chunk_id::text,
    'stage', v_stage,
    'transfer_ids', COALESCE(v_transfer_ids_json, '[]'::jsonb),
    'provider_submit_diagnostic', v_final_diagnostic,
    'provider_submission_status', v_final_status,
    'review_reason_code', v_final_review_reason_code,
    'manual_resolution_required', lower(BTRIM(COALESCE(v_final_diagnostic->>'manual_resolution_required', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'safe_retry_available', lower(BTRIM(COALESCE(v_final_diagnostic->>'safe_retry_available', ''))) IN ('true', 't', '1', 'yes', 'y', 'on'),
    'provider_acceptance_evidence_count', COALESCE(v_provider_acceptance_evidence_count, 0),
    'provider_response_present_count', COALESCE(v_provider_response_present_count, 0),
    'provider_request_sent_count', COALESCE(v_provider_request_sent_count, 0),
    'provider_rejection_count', COALESCE(v_provider_rejection_count, 0),
    'existing_precedence_rank', v_existing_rank,
    'incoming_precedence_rank', v_incoming_rank,
    'final_precedence_rank', v_final_rank,
    'recorded_at_utc', v_now::text
  );
END;
$function$;

