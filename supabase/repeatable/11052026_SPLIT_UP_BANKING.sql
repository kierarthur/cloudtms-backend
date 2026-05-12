-- ============================================================
-- NEW - Banking Pay operation RPCs 1-5
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
    v_pay_channel := upper(coalesce(nullif(btrim(p_pay_channel), ''), v_batch_kind));

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
      p_config_json => '{}'::jsonb
    ) AS operation_start_row;

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
      'root_operation_id', CASE WHEN p_root_operation_id IS NULL THEN NULL ELSE p_root_operation_id::text END,
      'queue_result', jsonb_build_object(
        'ok', true,
        'operation_mode', true,
        'operation_id', v_operation_start.operation_id::text,
        'operation_type', v_operation_start.operation_type,
        'status', v_operation_start.status,
        'phase', v_operation_start.phase,
        'pay_batch_id', p_pay_batch_id::text,
        'message_kind', v_operation_kind,
        'trigger_status', 'REMITTANCE_QUEUE_OPERATION_STARTED',
        'dispatch_required', false,
        'job_count', 0,
        'jobs', '[]'::jsonb
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
        'reason', COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'BULK_SESSION_CANDIDATE_RECOMPUTE'),
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
        'refresh_reason', COALESCE(NULLIF(BTRIM(COALESCE(p_reason, '')), ''), 'BULK_SESSION_CANDIDATE_RECOMPUTE'),
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
    'job_ids', COALESCE(v_job_ids_jsonb, '[]'::jsonb)
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
  v_offset integer := 0;
  v_limit integer := 100;
  v_items jsonb := '[]'::jsonb;
  v_known_count integer := 0;
  v_returned_count integer := 0;
  v_next_cursor jsonb := NULL::jsonb;
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
    RAISE EXCEPTION 'banking_pay_workbench_sessions row % not found', p_session_id;
  END IF;

  v_limit := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 500);
  v_offset := COALESCE(
    CASE
      WHEN COALESCE(v_cursor_json->>'offset', '') ~ '^[0-9]+$'
        THEN (v_cursor_json->>'offset')::integer
      ELSE 0
    END,
    0
  );

  IF v_section = 'candidates' THEN
    WITH flattened AS (
      SELECT
        state_row.candidate_id,
        1 AS item_order,
        jsonb_strip_nulls(
          COALESCE(state_row.effective_paye_candidate_json, '{}'::jsonb)
          || jsonb_build_object('candidate_id', state_row.candidate_id::text, 'preview_section', 'paye_candidates')
        ) AS item_json
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      WHERE state_row.session_id = p_session_id
        AND state_row.status = 'READY'
        AND jsonb_typeof(state_row.effective_paye_candidate_json) = 'object'

      UNION ALL

      SELECT
        state_row.candidate_id,
        2 AS item_order,
        jsonb_strip_nulls(
          COALESCE(state_row.effective_non_paye_payee_json, '{}'::jsonb)
          || jsonb_build_object('candidate_id', state_row.candidate_id::text, 'preview_section', 'non_paye_payees')
        ) AS item_json
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      WHERE state_row.session_id = p_session_id
        AND state_row.status = 'READY'
        AND jsonb_typeof(state_row.effective_non_paye_payee_json) = 'object'
    ),
    numbered AS (
      SELECT
        flattened.item_json,
        row_number() OVER (ORDER BY flattened.candidate_id, flattened.item_order) AS row_num,
        count(*) OVER () AS total_count
      FROM flattened
    )
    SELECT COALESCE(jsonb_agg(numbered.item_json ORDER BY numbered.row_num), '[]'::jsonb),
           COALESCE(max(numbered.total_count), 0)::integer,
           count(*)::integer
    INTO v_items, v_known_count, v_returned_count
    FROM numbered
    WHERE numbered.row_num > v_offset
      AND numbered.row_num <= v_offset + v_limit;
  ELSIF v_section = 'canonical_preview_lines' THEN
    WITH flattened AS (
      SELECT
        state_row.candidate_id,
        line_element.ordinality,
        line_element.value AS item_json
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(COALESCE(state_row.effective_canonical_preview_lines_json, '[]'::jsonb)) = 'array' THEN COALESCE(state_row.effective_canonical_preview_lines_json, '[]'::jsonb) ELSE '[]'::jsonb END
      ) WITH ORDINALITY AS line_element(value, ordinality)
      WHERE state_row.session_id = p_session_id
        AND state_row.status = 'READY'
        AND jsonb_typeof(line_element.value) = 'object'
    ),
    numbered AS (
      SELECT flattened.item_json,
             row_number() OVER (ORDER BY flattened.candidate_id, flattened.ordinality) AS row_num,
             count(*) OVER () AS total_count
      FROM flattened
    )
    SELECT COALESCE(jsonb_agg(numbered.item_json ORDER BY numbered.row_num), '[]'::jsonb),
           COALESCE(max(numbered.total_count), 0)::integer,
           count(*)::integer
    INTO v_items, v_known_count, v_returned_count
    FROM numbered
    WHERE numbered.row_num > v_offset
      AND numbered.row_num <= v_offset + v_limit;
  ELSIF v_section = 'itemisation' THEN
    WITH flattened AS (
      SELECT state_row.candidate_id, item_element.ordinality, item_element.value AS item_json
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(COALESCE(state_row.effective_candidate_fragment_json->'itemisation', '[]'::jsonb)) = 'array' THEN COALESCE(state_row.effective_candidate_fragment_json->'itemisation', '[]'::jsonb) ELSE '[]'::jsonb END
      ) WITH ORDINALITY AS item_element(value, ordinality)
      WHERE state_row.session_id = p_session_id
        AND state_row.status = 'READY'
        AND jsonb_typeof(item_element.value) = 'object'
    ),
    numbered AS (
      SELECT flattened.item_json,
             row_number() OVER (ORDER BY flattened.candidate_id, flattened.ordinality) AS row_num,
             count(*) OVER () AS total_count
      FROM flattened
    )
    SELECT COALESCE(jsonb_agg(numbered.item_json ORDER BY numbered.row_num), '[]'::jsonb),
           COALESCE(max(numbered.total_count), 0)::integer,
           count(*)::integer
    INTO v_items, v_known_count, v_returned_count
    FROM numbered
    WHERE numbered.row_num > v_offset
      AND numbered.row_num <= v_offset + v_limit;
  ELSIF v_section = 'blocked_items' THEN
    WITH flattened AS (
      SELECT state_row.candidate_id, item_element.ordinality, item_element.value AS item_json
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(COALESCE(state_row.effective_candidate_fragment_json->'blocked_items', '[]'::jsonb)) = 'array' THEN COALESCE(state_row.effective_candidate_fragment_json->'blocked_items', '[]'::jsonb) ELSE '[]'::jsonb END
      ) WITH ORDINALITY AS item_element(value, ordinality)
      WHERE state_row.session_id = p_session_id
        AND state_row.status = 'READY'
        AND jsonb_typeof(item_element.value) = 'object'
    ),
    numbered AS (
      SELECT flattened.item_json,
             row_number() OVER (ORDER BY flattened.candidate_id, flattened.ordinality) AS row_num,
             count(*) OVER () AS total_count
      FROM flattened
    )
    SELECT COALESCE(jsonb_agg(numbered.item_json ORDER BY numbered.row_num), '[]'::jsonb),
           COALESCE(max(numbered.total_count), 0)::integer,
           count(*)::integer
    INTO v_items, v_known_count, v_returned_count
    FROM numbered
    WHERE numbered.row_num > v_offset
      AND numbered.row_num <= v_offset + v_limit;
  ELSIF v_section = 'do_not_pay_items' THEN
    WITH flattened AS (
      SELECT state_row.candidate_id, item_element.ordinality, item_element.value AS item_json
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(COALESCE(state_row.effective_candidate_fragment_json->'do_not_pay_items', '[]'::jsonb)) = 'array' THEN COALESCE(state_row.effective_candidate_fragment_json->'do_not_pay_items', '[]'::jsonb) ELSE '[]'::jsonb END
      ) WITH ORDINALITY AS item_element(value, ordinality)
      WHERE state_row.session_id = p_session_id
        AND state_row.status = 'READY'
        AND jsonb_typeof(item_element.value) = 'object'
    ),
    numbered AS (
      SELECT flattened.item_json,
             row_number() OVER (ORDER BY flattened.candidate_id, flattened.ordinality) AS row_num,
             count(*) OVER () AS total_count
      FROM flattened
    )
    SELECT COALESCE(jsonb_agg(numbered.item_json ORDER BY numbered.row_num), '[]'::jsonb),
           COALESCE(max(numbered.total_count), 0)::integer,
           count(*)::integer
    INTO v_items, v_known_count, v_returned_count
    FROM numbered
    WHERE numbered.row_num > v_offset
      AND numbered.row_num <= v_offset + v_limit;
  ELSIF v_section = 'snoozed_items' THEN
    WITH flattened AS (
      SELECT state_row.candidate_id, item_element.ordinality, item_element.value AS item_json
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(COALESCE(state_row.effective_candidate_fragment_json->'snoozed_items', '[]'::jsonb)) = 'array' THEN COALESCE(state_row.effective_candidate_fragment_json->'snoozed_items', '[]'::jsonb) ELSE '[]'::jsonb END
      ) WITH ORDINALITY AS item_element(value, ordinality)
      WHERE state_row.session_id = p_session_id
        AND state_row.status = 'READY'
        AND jsonb_typeof(item_element.value) = 'object'
    ),
    numbered AS (
      SELECT flattened.item_json,
             row_number() OVER (ORDER BY flattened.candidate_id, flattened.ordinality) AS row_num,
             count(*) OVER () AS total_count
      FROM flattened
    )
    SELECT COALESCE(jsonb_agg(numbered.item_json ORDER BY numbered.row_num), '[]'::jsonb),
           COALESCE(max(numbered.total_count), 0)::integer,
           count(*)::integer
    INTO v_items, v_known_count, v_returned_count
    FROM numbered
    WHERE numbered.row_num > v_offset
      AND numbered.row_num <= v_offset + v_limit;
  ELSE
    WITH flattened AS (
      SELECT state_row.candidate_id, item_element.ordinality, item_element.value AS item_json
      FROM public.banking_pay_workbench_session_candidate_state AS state_row
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(COALESCE(state_row.effective_candidate_fragment_json->'baseline_component_rows', '[]'::jsonb)) = 'array' THEN COALESCE(state_row.effective_candidate_fragment_json->'baseline_component_rows', '[]'::jsonb) ELSE '[]'::jsonb END
      ) WITH ORDINALITY AS item_element(value, ordinality)
      WHERE state_row.session_id = p_session_id
        AND state_row.status = 'READY'
        AND jsonb_typeof(item_element.value) = 'object'
    ),
    numbered AS (
      SELECT flattened.item_json,
             row_number() OVER (ORDER BY flattened.candidate_id, flattened.ordinality) AS row_num,
             count(*) OVER () AS total_count
      FROM flattened
    )
    SELECT COALESCE(jsonb_agg(numbered.item_json ORDER BY numbered.row_num), '[]'::jsonb),
           COALESCE(max(numbered.total_count), 0)::integer,
           count(*)::integer
    INTO v_items, v_known_count, v_returned_count
    FROM numbered
    WHERE numbered.row_num > v_offset
      AND numbered.row_num <= v_offset + v_limit;
  END IF;

  IF v_offset + v_limit < COALESCE(v_known_count, 0) THEN
    v_next_cursor := jsonb_build_object('offset', v_offset + v_limit);
  ELSE
    v_next_cursor := NULL::jsonb;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'session_id', p_session_id::text,
    'section', v_section,
    'items', COALESCE(v_items, '[]'::jsonb),
    'next_cursor', v_next_cursor,
    'known_count', COALESCE(v_known_count, 0),
    'returned_count', COALESCE(v_returned_count, 0),
    'limit', v_limit,
    'offset', v_offset,
    'session_version', v_session_row.version,
    'session_signature', v_session_row.session_signature,
    'snapshot_run_id', CASE WHEN v_session_row.source_snapshot_run_id IS NULL THEN NULL ELSE v_session_row.source_snapshot_run_id::text END
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
  v_failed_transfer_count integer := 0;
  v_ambiguous_transfer_count integer := 0;
  v_blocked_transfer_count integer := 0;
  v_settled_candidate_count integer := 0;
  v_remittance_sent_count integer := 0;
  v_active_operation_id uuid := NULL::uuid;
  v_active_operation_type text := NULL;
  v_active_operation_status text := NULL;
  v_submission_evidence_json jsonb := '{}'::jsonb;
  v_freshness_result_json jsonb := '{}'::jsonb;
  v_freshness_is_stale boolean := false;
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

  v_submission_evidence_json := public.pay_batch_submission_evidence(p_pay_batch_id);
  v_transfer_count := coalesce((v_submission_evidence_json->>'transfer_count')::integer, 0);
  v_provider_submitted_transfer_count := coalesce((v_submission_evidence_json->>'provider_submitted_count')::integer, 0);
  v_local_only_transfer_count := coalesce((v_submission_evidence_json->>'local_only_count')::integer, coalesce((v_submission_evidence_json->>'local_idempotency_only_count')::integer, 0));
  v_pending_transfer_count := coalesce((v_submission_evidence_json->>'pending_count')::integer, 0);
  v_failed_transfer_count := coalesce((v_submission_evidence_json->>'failed_count')::integer, 0);
  v_ambiguous_transfer_count := coalesce((v_submission_evidence_json->>'ambiguous_count')::integer, 0);

  SELECT count(*)::integer,
         count(*) FILTER (WHERE upper(coalesce(transfer_row.status, '')) = 'BLOCKED')::integer
  INTO v_prepared_transfer_count, v_blocked_transfer_count
  FROM public.pay_bank_transfers AS transfer_row
  WHERE transfer_row.pay_batch_id = p_pay_batch_id;

  SELECT operation_row.id,
         operation_row.operation_type,
         operation_row.status
  INTO v_active_operation_id, v_active_operation_type, v_active_operation_status
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.pay_batch_id = p_pay_batch_id
    AND operation_row.status NOT IN ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED')
  ORDER BY operation_row.updated_at_utc DESC NULLS LAST, operation_row.created_at_utc DESC NULLS LAST, operation_row.id DESC
  LIMIT 1;

  RETURN jsonb_build_object(
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
    'pending_transfer_count', coalesce(v_pending_transfer_count, 0),
    'failed_transfer_count', coalesce(v_failed_transfer_count, 0),
    'ambiguous_transfer_count', coalesce(v_ambiguous_transfer_count, 0),
    'blocked_transfer_count', coalesce(v_blocked_transfer_count, 0),
    'submission_evidence', coalesce(v_submission_evidence_json, '{}'::jsonb),
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
                             FROM public.pay_bank_transfers AS prepared_transfer
                             WHERE prepared_transfer.id = scope_rows.pay_bank_transfer_id
                               AND prepared_transfer.pay_batch_id = scope_rows.pay_batch_id
                               AND prepared_transfer.pay_channel = scope_rows.pay_channel
                               AND prepared_transfer.transfer_group_key = scope_rows.transfer_group_key
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

  WITH provider_evidence_scope AS (
    SELECT
      transfer_scope.id AS transfer_scope_id,
      bank_transfer.id AS pay_bank_transfer_id
    FROM public.banking_pay_operation_transfer_scope AS transfer_scope
    JOIN pg_temp.tmp_transfer_scope_request AS requested_scope
      ON requested_scope.transfer_scope_id = transfer_scope.id
    JOIN public.pay_bank_transfers AS bank_transfer
      ON bank_transfer.pay_batch_id = transfer_scope.pay_batch_id
     AND bank_transfer.pay_channel = transfer_scope.pay_channel
     AND bank_transfer.transfer_group_key = transfer_scope.transfer_group_key
    WHERE transfer_scope.operation_id = p_operation_id
      AND transfer_scope.pay_batch_id = p_pay_batch_id
      AND transfer_scope.status = 'PENDING'
      AND (
        nullif(btrim(coalesce(bank_transfer.rail_tx_id, '')), '') IS NOT NULL
        OR EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS transfer_event
          WHERE transfer_event.pay_batch_id = bank_transfer.pay_batch_id
            AND transfer_event.pay_bank_transfer_id = bank_transfer.id
            AND upper(btrim(coalesce(transfer_event.event_source, ''))) IN ('PROVIDER_RESPONSE','PROVIDER_POLL','PROVIDER_WEBHOOK','WEBHOOK','POLL','RAIL_PROVIDER')
            AND (
              nullif(btrim(coalesce(transfer_event.provider_event_id, '')), '') IS NOT NULL
              OR (
                nullif(btrim(coalesce(transfer_event.provider_reference, '')), '') IS NOT NULL
                AND nullif(btrim(coalesce(transfer_event.provider_reference, '')), '') IS DISTINCT FROM nullif(btrim(coalesce(bank_transfer.request_id, '')), '')
                AND nullif(btrim(coalesce(transfer_event.provider_reference, '')), '') IS DISTINCT FROM nullif(btrim(coalesce(bank_transfer.payment_reference, '')), '')
                AND nullif(btrim(coalesce(transfer_event.provider_reference, '')), '') IS DISTINCT FROM nullif(btrim(coalesce(bank_transfer.transfer_group_key, '')), '')
              )
            )
        )
      )
  ), skipped_scope AS (
    UPDATE public.banking_pay_operation_transfer_scope AS transfer_scope_update
    SET status = 'SKIPPED',
        pay_bank_transfer_id = provider_evidence_scope.pay_bank_transfer_id,
        updated_at_utc = v_now
    FROM provider_evidence_scope
    WHERE transfer_scope_update.id = provider_evidence_scope.transfer_scope_id
      AND transfer_scope_update.status = 'PENDING'
    RETURNING transfer_scope_update.id
  )
  SELECT coalesce(count(*), 0)::integer
  INTO v_new_skipped_count
  FROM skipped_scope;

  v_skipped_count := COALESCE(v_existing_skipped_count, 0) + COALESCE(v_new_skipped_count, 0);

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
      regexp_replace(COALESCE(transfer_scope.sort_code, ''), '[^0-9]', '', 'g'),
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
        status = CASE WHEN upper(coalesce(public.pay_bank_transfers.status, '')) IN ('PENDING', 'BLOCKED', 'FAILED') THEN EXCLUDED.status ELSE public.pay_bank_transfers.status END,
        payment_reference = EXCLUDED.payment_reference,
        payee_name = EXCLUDED.payee_name,
        sort_code = EXCLUDED.sort_code,
        account_number = EXCLUDED.account_number,
        account_type = EXCLUDED.account_type,
        rail_provider = EXCLUDED.rail_provider,
        rail_env = EXCLUDED.rail_env,
        request_id = COALESCE(NULLIF(public.pay_bank_transfers.request_id, ''), EXCLUDED.request_id),
        bank_details_hash_snapshot = EXCLUDED.bank_details_hash_snapshot,
        payee_entity_kind = EXCLUDED.payee_entity_kind,
        payee_entity_id = EXCLUDED.payee_entity_id,
        grouping_mode_used = EXCLUDED.grouping_mode_used,
        week_ending_bucket = EXCLUDED.week_ending_bucket
    WHERE nullif(btrim(coalesce(public.pay_bank_transfers.rail_tx_id, '')), '') IS NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS protected_event
        WHERE protected_event.pay_batch_id = public.pay_bank_transfers.pay_batch_id
          AND protected_event.pay_bank_transfer_id = public.pay_bank_transfers.id
          AND upper(btrim(coalesce(protected_event.event_source, ''))) IN ('PROVIDER_RESPONSE','PROVIDER_POLL','PROVIDER_WEBHOOK','WEBHOOK','POLL','RAIL_PROVIDER')
          AND (
            nullif(btrim(coalesce(protected_event.provider_event_id, '')), '') IS NOT NULL
            OR (
              nullif(btrim(coalesce(protected_event.provider_reference, '')), '') IS NOT NULL
              AND nullif(btrim(coalesce(protected_event.provider_reference, '')), '') IS DISTINCT FROM nullif(btrim(coalesce(public.pay_bank_transfers.request_id, '')), '')
              AND nullif(btrim(coalesce(protected_event.provider_reference, '')), '') IS DISTINCT FROM nullif(btrim(coalesce(public.pay_bank_transfers.payment_reference, '')), '')
              AND nullif(btrim(coalesce(protected_event.provider_reference, '')), '') IS DISTINCT FROM nullif(btrim(coalesce(public.pay_bank_transfers.transfer_group_key, '')), '')
            )
          )
      )
      AND upper(coalesce(public.pay_bank_transfers.status, '')) IN ('PENDING', 'BLOCKED', 'FAILED')
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
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item_update.pay_batch_candidate_id
     AND batch_candidate.pay_batch_id = p_pay_batch_id
    WHERE batch_item_update.id IN (
        SELECT (item_id.value #>> '{}')::uuid
        FROM jsonb_array_elements(COALESCE(transfer_scope.pay_batch_item_ids_json, '[]'::jsonb)) AS item_id(value)
        WHERE (item_id.value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
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

  SELECT count(*)::integer
  INTO v_remaining_count
  FROM public.banking_pay_operation_transfer_scope AS remaining_scope
  WHERE remaining_scope.operation_id = p_operation_id
    AND remaining_scope.pay_batch_id = p_pay_batch_id
    AND remaining_scope.status = 'PENDING';

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'pay_batch_id', p_pay_batch_id::text,
    'prepared_count', COALESCE(v_prepared_count, 0),
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
    'freshness_validation_status', v_freshness_status,
    'freshness_result_hash_used', v_freshness_result_hash_used,
    'freshness_scope_hash_used', v_freshness_scope_hash_used
  );
END;
$function$;




DROP FUNCTION IF EXISTS public.pay_bank_transfers_claim_provider_submit_chunk(uuid, uuid, integer, text, integer);






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
        OR (
          operation_chunk.status = 'RUNNING'
          AND (
            operation_chunk.lock_expires_at_utc IS NULL
            OR operation_chunk.lock_expires_at_utc <= v_now
          )
        )
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
      JOIN public.pay_bank_transfers AS transfer_row
        ON transfer_row.id = claimed_transfer_ids.transfer_id
       AND transfer_row.pay_batch_id = p_pay_batch_id
      WHERE (
          upper(coalesce(transfer_row.status, '')) IN ('PENDING', 'READY', 'PREPARED', 'QUEUED', 'SUBMITTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'PENDING_SUBMISSION', 'PENDING_CONFIRMATION', 'PENDING_SETTLEMENT')
          OR (
            v_retry_mode = true
            AND upper(coalesce(transfer_row.status, '')) IN ('FAILED', 'BLOCKED')
          )
        )
        AND upper(coalesce(transfer_row.status, '')) NOT IN ('COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED')
        AND transfer_row.completed_at_utc IS NULL
        AND NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NULL
        AND NOT (
          lower(btrim(coalesce(transfer_row.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND (
            NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submission_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{submission_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{rail_submission_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{external_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{revolut_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_transfer_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_transaction_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{transaction_id}', '')), '') IS NOT NULL
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS evidence_event
          WHERE evidence_event.pay_bank_transfer_id = transfer_row.id
            AND upper(BTRIM(COALESCE(evidence_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL', 'RAIL_PROVIDER', 'PROVIDER')
            AND (
              NULLIF(BTRIM(COALESCE(evidence_event.provider_event_id, '')), '') IS NOT NULL
              OR (
                NULLIF(BTRIM(COALESCE(evidence_event.provider_reference, '')), '') IS NOT NULL
                AND NULLIF(BTRIM(COALESCE(evidence_event.provider_reference, '')), '') NOT IN (
                  COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__no_request_id__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__no_payment_reference__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(v_batch_row.bulk_reference, '')), ''), '__no_bulk_reference__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{request_id}', '')), ''), '__no_meta_request_id__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), ''), '__no_meta_idempotency_key__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_reference}', '')), ''), '__no_meta_payment_reference__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{bulk_reference}', '')), ''), '__no_meta_bulk_reference__')
                )
              )
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{provider_payment_id}', '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{external_payment_id}', '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{revolut_payment_id}', '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{provider_transaction_id}', '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{transaction_id}', '')), '') IS NOT NULL
            )
        )
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
        updated_at_utc = v_now
    WHERE operation_chunk_update.id = v_chunk_id;

    WITH eligible_remaining AS (
      SELECT remaining_transfer.id
      FROM public.pay_bank_transfers AS remaining_transfer
      WHERE remaining_transfer.pay_batch_id = p_pay_batch_id
        AND (
          upper(coalesce(remaining_transfer.status, '')) IN ('PENDING', 'READY', 'PREPARED', 'QUEUED', 'SUBMITTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'PENDING_SUBMISSION', 'PENDING_CONFIRMATION', 'PENDING_SETTLEMENT')
          OR (
            v_retry_mode = true
            AND upper(coalesce(remaining_transfer.status, '')) IN ('FAILED', 'BLOCKED')
          )
        )
        AND upper(coalesce(remaining_transfer.status, '')) NOT IN ('COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED')
        AND remaining_transfer.completed_at_utc IS NULL
        AND NULLIF(BTRIM(COALESCE(remaining_transfer.rail_tx_id, '')), '') IS NULL
        AND NOT (
          lower(btrim(coalesce(remaining_transfer.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND (
            NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{provider_submission_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{submission_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{rail_submission_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{provider_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{external_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{revolut_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{provider_transfer_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{provider_transaction_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{transaction_id}', '')), '') IS NOT NULL
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.pay_bank_transfer_events AS evidence_event
          WHERE evidence_event.pay_bank_transfer_id = remaining_transfer.id
            AND upper(BTRIM(COALESCE(evidence_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL', 'RAIL_PROVIDER', 'PROVIDER')
            AND (
              NULLIF(BTRIM(COALESCE(evidence_event.provider_event_id, '')), '') IS NOT NULL
              OR (
                NULLIF(BTRIM(COALESCE(evidence_event.provider_reference, '')), '') IS NOT NULL
                AND NULLIF(BTRIM(COALESCE(evidence_event.provider_reference, '')), '') NOT IN (
                  COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.request_id, '')), ''), '__no_request_id__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(v_batch_row.bulk_reference, '')), ''), '__no_bulk_reference__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{request_id}', '')), ''), '__no_meta_request_id__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{idempotency_key}', '')), ''), '__no_meta_idempotency_key__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{payment_reference}', '')), ''), '__no_meta_payment_reference__'),
                  COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{bulk_reference}', '')), ''), '__no_meta_bulk_reference__')
                )
              )
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{provider_payment_id}', '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{external_payment_id}', '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{revolut_payment_id}', '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{provider_transaction_id}', '')), '') IS NOT NULL
              OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{transaction_id}', '')), '') IS NOT NULL
            )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_chunks AS active_chunk
          CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(active_chunk.payload_json->'transfer_ids', '[]'::jsonb)) AS active_transfer_id(value)
          WHERE active_chunk.chunk_type = 'TRANSFER_SUBMIT'
            AND active_chunk.status = 'RUNNING'
            AND active_chunk.lock_expires_at_utc IS NOT NULL
            AND active_chunk.lock_expires_at_utc > v_now
            AND active_transfer_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND active_transfer_id.value::uuid = remaining_transfer.id
        )
    )
    SELECT COUNT(*)::integer
    INTO v_remaining_count
    FROM eligible_remaining;

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
      'remaining_count', COALESCE(v_remaining_count, 0),
      'has_more', COALESCE(v_remaining_count, 0) > 0,
      'idempotent_reuse', true
    );
  END IF;

  WITH candidate_transfers AS (
    SELECT transfer_row.*
    FROM public.pay_bank_transfers AS transfer_row
    WHERE transfer_row.pay_batch_id = p_pay_batch_id
      AND (
        upper(coalesce(transfer_row.status, '')) IN ('PENDING', 'READY', 'PREPARED', 'QUEUED', 'SUBMITTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'PENDING_SUBMISSION', 'PENDING_CONFIRMATION', 'PENDING_SETTLEMENT')
        OR (
          v_retry_mode = true
          AND upper(coalesce(transfer_row.status, '')) IN ('FAILED', 'BLOCKED')
        )
      )
      AND upper(coalesce(transfer_row.status, '')) NOT IN ('COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED')
      AND transfer_row.completed_at_utc IS NULL
      AND NULLIF(BTRIM(COALESCE(transfer_row.rail_tx_id, '')), '') IS NULL
      AND NOT (
        lower(btrim(coalesce(transfer_row.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_submission_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{submission_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{rail_submission_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_payment_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{external_payment_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{revolut_payment_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_transfer_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{provider_transaction_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{transaction_id}', '')), '') IS NOT NULL
        )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS evidence_event
        WHERE evidence_event.pay_bank_transfer_id = transfer_row.id
          AND upper(BTRIM(COALESCE(evidence_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL', 'RAIL_PROVIDER', 'PROVIDER')
          AND (
            NULLIF(BTRIM(COALESCE(evidence_event.provider_event_id, '')), '') IS NOT NULL
            OR (
              NULLIF(BTRIM(COALESCE(evidence_event.provider_reference, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(evidence_event.provider_reference, '')), '') NOT IN (
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch_row.bulk_reference, '')), ''), '__no_bulk_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{request_id}', '')), ''), '__no_meta_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{idempotency_key}', '')), ''), '__no_meta_idempotency_key__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{payment_reference}', '')), ''), '__no_meta_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(transfer_row.rail_meta_json #>> '{bulk_reference}', '')), ''), '__no_meta_bulk_reference__')
              )
            )
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{provider_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{external_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{revolut_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{provider_transaction_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{transaction_id}', '')), '') IS NOT NULL
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_chunks AS active_chunk
        CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(active_chunk.payload_json->'transfer_ids', '[]'::jsonb)) AS active_transfer_id(value)
        WHERE active_chunk.chunk_type = 'TRANSFER_SUBMIT'
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
      'remaining_count', 0,
      'has_more', false
    );
  END IF;

  SELECT (COUNT(*) + 1)::integer
  INTO v_sequence_no
  FROM public.banking_pay_operation_chunks AS operation_chunk
  WHERE operation_chunk.operation_id = p_operation_id
    AND operation_chunk.phase = 'SUBMIT_PROVIDER_TRANSFERS'
    AND operation_chunk.chunk_type = 'TRANSFER_SUBMIT';

  INSERT INTO public.banking_pay_operation_chunks (
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
    '{}'::jsonb,
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

  WITH eligible_remaining AS (
    SELECT remaining_transfer.id
    FROM public.pay_bank_transfers AS remaining_transfer
    WHERE remaining_transfer.pay_batch_id = p_pay_batch_id
      AND (
        upper(coalesce(remaining_transfer.status, '')) IN ('PENDING', 'READY', 'PREPARED', 'QUEUED', 'SUBMITTED', 'SENT', 'PROCESSING', 'IN_FLIGHT', 'PENDING_SUBMISSION', 'PENDING_CONFIRMATION', 'PENDING_SETTLEMENT')
        OR (
          v_retry_mode = true
          AND upper(coalesce(remaining_transfer.status, '')) IN ('FAILED', 'BLOCKED')
        )
      )
      AND upper(coalesce(remaining_transfer.status, '')) NOT IN ('COMPLETED', 'COMMITTED', 'SETTLED', 'PAID', 'EXECUTED', 'CANCELLED', 'CANCELED', 'RETURNED', 'REVERTED')
      AND remaining_transfer.completed_at_utc IS NULL
      AND NULLIF(BTRIM(COALESCE(remaining_transfer.rail_tx_id, '')), '') IS NULL
      AND NOT (
        lower(btrim(coalesce(remaining_transfer.rail_meta_json #>> '{last_update_provider_evidence}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{provider_submission_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{submission_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{rail_submission_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{provider_payment_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{payment_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{external_payment_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{revolut_payment_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{provider_transfer_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{provider_transaction_id}', '')), '') IS NOT NULL
          OR NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{transaction_id}', '')), '') IS NOT NULL
        )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS evidence_event
        WHERE evidence_event.pay_bank_transfer_id = remaining_transfer.id
          AND upper(BTRIM(COALESCE(evidence_event.event_source, ''))) IN ('PROVIDER_RESPONSE', 'PROVIDER_POLL', 'PROVIDER_WEBHOOK', 'WEBHOOK', 'POLL', 'RAIL_PROVIDER', 'PROVIDER')
          AND (
            NULLIF(BTRIM(COALESCE(evidence_event.provider_event_id, '')), '') IS NOT NULL
            OR (
              NULLIF(BTRIM(COALESCE(evidence_event.provider_reference, '')), '') IS NOT NULL
              AND NULLIF(BTRIM(COALESCE(evidence_event.provider_reference, '')), '') NOT IN (
                COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.request_id, '')), ''), '__no_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.payment_reference, '')), ''), '__no_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(v_batch_row.bulk_reference, '')), ''), '__no_bulk_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{request_id}', '')), ''), '__no_meta_request_id__'),
                COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{idempotency_key}', '')), ''), '__no_meta_idempotency_key__'),
                COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{payment_reference}', '')), ''), '__no_meta_payment_reference__'),
                COALESCE(NULLIF(BTRIM(COALESCE(remaining_transfer.rail_meta_json #>> '{bulk_reference}', '')), ''), '__no_meta_bulk_reference__')
              )
            )
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{provider_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{external_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{revolut_payment_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{provider_transaction_id}', '')), '') IS NOT NULL
            OR NULLIF(BTRIM(COALESCE(evidence_event.raw_payload #>> '{transaction_id}', '')), '') IS NOT NULL
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_chunks AS active_chunk
        CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(active_chunk.payload_json->'transfer_ids', '[]'::jsonb)) AS active_transfer_id(value)
        WHERE active_chunk.chunk_type = 'TRANSFER_SUBMIT'
          AND active_chunk.status = 'RUNNING'
          AND active_chunk.lock_expires_at_utc IS NOT NULL
          AND active_chunk.lock_expires_at_utc > v_now
          AND active_transfer_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND active_transfer_id.value::uuid = remaining_transfer.id
      )
  )
  SELECT COUNT(*)::integer
  INTO v_remaining_count
  FROM eligible_remaining;

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
    'remaining_count', COALESCE(v_remaining_count, 0),
    'has_more', COALESCE(v_remaining_count, 0) > 0
  );
END;
$function$;


















DROP FUNCTION IF EXISTS public.pay_operation_remittance_scope_seed(uuid, uuid, text, uuid);

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
        upper(COALESCE(batch_candidate.settlement_status, '')) IN ('SETTLED', 'PAID', 'CONFIRMED')
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
               'transfer_group_key', candidate_scope.transfer_group_key
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
               'transfer_group_key', umbrella_scope.transfer_group_key
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
    'scope_rows_created', COALESCE(v_created_count, 0),
    'scope_rows_reused', COALESCE(v_reused_count, 0),
    'recipient_count', COALESCE(v_recipient_count, 0),
    'stale_scope_skipped_count', COALESCE(v_stale_scope_skipped_count, 0)
  );
END;
$function$;
DROP FUNCTION IF EXISTS public.pay_operation_settlement_scope_seed(uuid, uuid, text, uuid);

CREATE OR REPLACE FUNCTION public.pay_operation_settlement_scope_seed(
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
  v_settlement_unit_count integer := 0;
  v_stale_scope_skipped_count integer := 0;
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

  IF v_scope NOT IN ('ALL', 'PAYE', 'UMBRELLA', 'LOANS') THEN
    RAISE EXCEPTION 'p_scope must be ALL, PAYE, UMBRELLA, or LOANS';
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

  IF v_operation_row.operation_type NOT IN ('PAYMENT_SETTLEMENT', 'PAYMENT_EXECUTE') THEN
    RAISE EXCEPTION 'operation % is not a settlement-capable operation', p_operation_id;
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

  WITH item_scope AS (
    SELECT
      batch_candidate.id AS pay_batch_candidate_id,
      batch_candidate.candidate_id,
      upper(BTRIM(COALESCE(batch_item.pay_channel, ''))) AS pay_channel,
      batch_item.pay_bank_transfer_id,
      transfer_row.transfer_group_key,
      CASE
        WHEN batch_item.pay_bank_transfer_id IS NOT NULL THEN 'transfer:' || batch_item.pay_bank_transfer_id::text
        ELSE 'batch_candidate:' || batch_candidate.id::text || ':channel:' || upper(BTRIM(COALESCE(batch_item.pay_channel, '')))
      END AS settlement_scope_key,
      COALESCE(jsonb_agg(to_jsonb(batch_item.id::text) ORDER BY batch_item.id::text), '[]'::jsonb) AS pay_batch_item_ids_json,
      COUNT(batch_item.id)::integer AS item_count,
      ROUND(COALESCE(SUM(COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0)), 0), 2) AS total_amount
    FROM public.pay_batch_candidates AS batch_candidate
    JOIN public.pay_batch_items AS batch_item
      ON batch_item.pay_batch_candidate_id = batch_candidate.id
    LEFT JOIN public.pay_bank_transfers AS transfer_row
      ON transfer_row.id = batch_item.pay_bank_transfer_id
    WHERE batch_candidate.pay_batch_id = p_pay_batch_id
      AND COALESCE(batch_item.is_voided, false) = false
      AND COALESCE(batch_item.item_type, '') <> 'DEBT_CREATED'
      AND (
        upper(COALESCE(batch_candidate.settlement_status, '')) NOT IN ('SETTLED', 'PAID', 'CONFIRMED')
        OR batch_candidate.settlement_status IS NULL
      )
      AND (
        v_scope = 'ALL'
        OR (v_scope = 'LOANS' AND batch_item.item_type = 'LOAN_PAYOUT')
        OR upper(BTRIM(COALESCE(batch_item.pay_channel, ''))) = v_scope
      )
    GROUP BY batch_candidate.id,
             batch_candidate.candidate_id,
             upper(BTRIM(COALESCE(batch_item.pay_channel, ''))),
             batch_item.pay_bank_transfer_id,
             transfer_row.transfer_group_key,
             CASE
               WHEN batch_item.pay_bank_transfer_id IS NOT NULL THEN 'transfer:' || batch_item.pay_bank_transfer_id::text
               ELSE 'batch_candidate:' || batch_candidate.id::text || ':channel:' || upper(BTRIM(COALESCE(batch_item.pay_channel, '')))
             END
    HAVING ROUND(COALESCE(SUM(COALESCE(batch_item.amount_inc_vat, batch_item.amount_ex_vat, 0)), 0), 2) <> 0
  ), scope_rows AS (
    SELECT
      item_scope.pay_batch_candidate_id,
      item_scope.candidate_id,
      item_scope.pay_channel,
      'settlement:batch:' || p_pay_batch_id::text || ':batch_candidate:' || item_scope.pay_batch_candidate_id::text || ':channel:' || item_scope.pay_channel || ':scope:' || item_scope.settlement_scope_key AS settlement_key,
      jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'pay_batch_candidate_id', item_scope.pay_batch_candidate_id::text,
        'candidate_id', item_scope.candidate_id::text,
        'pay_channel', item_scope.pay_channel,
        'payment_scope_json', jsonb_strip_nulls(jsonb_build_object(
          'scope_key', item_scope.settlement_scope_key,
          'pay_bank_transfer_id', CASE WHEN item_scope.pay_bank_transfer_id IS NULL THEN NULL ELSE item_scope.pay_bank_transfer_id::text END,
          'transfer_group_key', item_scope.transfer_group_key
        )),
        'pay_batch_item_ids', item_scope.pay_batch_item_ids_json,
        'item_count', item_scope.item_count,
        'total_amount', item_scope.total_amount,
        'scope', v_scope
      ) AS payload_json
    FROM item_scope
  ), stale_scope AS (
    UPDATE public.banking_pay_operation_settlement_scope AS scope_update
    SET status = 'SKIPPED',
        updated_at_utc = v_now
    WHERE scope_update.operation_id = p_operation_id
      AND scope_update.pay_batch_id = p_pay_batch_id
      AND scope_update.status IN ('PENDING', 'FAILED')
      AND NOT EXISTS (
        SELECT 1
        FROM scope_rows
        WHERE scope_rows.settlement_key = scope_update.settlement_key
      )
    RETURNING scope_update.id
  ), upserted_scope AS (
    INSERT INTO public.banking_pay_operation_settlement_scope (
      operation_id,
      pay_batch_id,
      pay_batch_candidate_id,
      candidate_id,
      pay_channel,
      settlement_key,
      payload_json,
      status,
      settlement_event_id,
      created_at_utc,
      updated_at_utc
    )
    SELECT
      p_operation_id,
      p_pay_batch_id,
      scope_rows.pay_batch_candidate_id,
      scope_rows.candidate_id,
      scope_rows.pay_channel,
      scope_rows.settlement_key,
      scope_rows.payload_json,
      'PENDING',
      NULL::uuid,
      v_now,
      v_now
    FROM scope_rows
    ON CONFLICT (operation_id, settlement_key)
    DO UPDATE
    SET pay_batch_candidate_id = CASE WHEN public.banking_pay_operation_settlement_scope.status = 'SETTLED' THEN public.banking_pay_operation_settlement_scope.pay_batch_candidate_id ELSE EXCLUDED.pay_batch_candidate_id END,
        candidate_id = CASE WHEN public.banking_pay_operation_settlement_scope.status = 'SETTLED' THEN public.banking_pay_operation_settlement_scope.candidate_id ELSE EXCLUDED.candidate_id END,
        pay_channel = CASE WHEN public.banking_pay_operation_settlement_scope.status = 'SETTLED' THEN public.banking_pay_operation_settlement_scope.pay_channel ELSE EXCLUDED.pay_channel END,
        payload_json = CASE WHEN public.banking_pay_operation_settlement_scope.status = 'SETTLED' THEN public.banking_pay_operation_settlement_scope.payload_json ELSE EXCLUDED.payload_json END,
        status = CASE WHEN public.banking_pay_operation_settlement_scope.status = 'SETTLED' THEN public.banking_pay_operation_settlement_scope.status ELSE EXCLUDED.status END,
        updated_at_utc = v_now
    RETURNING public.banking_pay_operation_settlement_scope.id,
              (xmax = 0) AS was_inserted
  )
  SELECT COUNT(*) FILTER (WHERE upserted_scope.was_inserted)::integer,
         COUNT(*) FILTER (WHERE upserted_scope.was_inserted IS NOT TRUE)::integer,
         COUNT(*)::integer,
         COALESCE((SELECT COUNT(*)::integer FROM stale_scope), 0)
  INTO v_created_count,
       v_reused_count,
       v_settlement_unit_count,
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
    'scope_rows_created', COALESCE(v_created_count, 0),
    'scope_rows_reused', COALESCE(v_reused_count, 0),
    'settlement_unit_count', COALESCE(v_settlement_unit_count, 0),
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
      freshness_validation_status = coalesce(pay_batch_update.freshness_validation_status, 'PENDING'),
      freshness_scope_hash = coalesce(pay_batch_update.freshness_scope_hash, v_scope_hash)
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
  select
    'ACTIVE_SNOOZE_CHANGED',
    snooze_row.timesheet_id,
    null::uuid,
    snooze_row.candidate_id,
    'SNOOZE',
    coalesce(snooze_row.source_ref, snooze_row.segment_stable_key, snooze_row.segment_id, snooze_row.id::text),
    jsonb_build_object('expected', 'no_active_snooze_affecting_batch_scope'),
    jsonb_build_object('snooze_id', snooze_row.id::text, 'snooze_kind', snooze_row.snooze_kind, 'snooze_until_date', snooze_row.snooze_until_date),
    50
  from public.pay_item_snoozes as snooze_row
  where snooze_row.cleared_at_utc is null
    and snooze_row.cancelled_at_utc is null
    and (
      snooze_row.timesheet_id = any(v_timesheet_ids)
      or snooze_row.candidate_id = any(v_candidate_ids)
    );

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
      or upper(coalesce(advance_row.status, '')) in ('CANCELLED', 'PAID_OFF')
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
  with finance_item_totals as (
    select
      batch_item.finance_case_id,
      batch_item.finance_component_id,
      round(sum(abs(coalesce(batch_item.amount_ex_vat, 0))), 2)::numeric as item_total_ex_vat
    from public.pay_batch_items as batch_item
    where batch_item.id = any(v_item_ids)
      and batch_item.item_type in ('LOAN_REPAYMENT', 'OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_ADJUSTMENT', 'MANUAL_CREDIT_ADJUSTMENT')
      and (batch_item.finance_case_id is not null or batch_item.finance_component_id is not null)
    group by batch_item.finance_case_id, batch_item.finance_component_id
  ),
  reservation_totals as (
    select
      reservation_row.finance_case_id,
      reservation_row.finance_component_id,
      round(sum(abs(coalesce(reservation_row.reserved_amount, reservation_row.frozen_rounded_target_amount, 0))), 2)::numeric as reservation_total_ex_vat
    from public.pay_advance_reservations as reservation_row
    where reservation_row.pay_batch_item_id = any(v_item_ids)
    group by reservation_row.finance_case_id, reservation_row.finance_component_id
  )
  select
    'DEDUCTION_RECOVERY_CHANGED',
    null::uuid,
    null::uuid,
    null::uuid,
    'DEDUCTION_RECOVERY',
    coalesce(finance_item_totals.finance_case_id::text, finance_item_totals.finance_component_id::text),
    jsonb_build_object('batch_item_total_ex_vat', finance_item_totals.item_total_ex_vat),
    jsonb_build_object('reservation_total_ex_vat', coalesce(reservation_totals.reservation_total_ex_vat, 0)),
    80
  from finance_item_totals
  left join reservation_totals
    on reservation_totals.finance_case_id is not distinct from finance_item_totals.finance_case_id
   and reservation_totals.finance_component_id is not distinct from finance_item_totals.finance_component_id
  where finance_item_totals.item_total_ex_vat <> coalesce(reservation_totals.reservation_total_ex_vat, 0);

  get diagnostics v_deduction_diff_count = row_count;

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

  select coalesce(jsonb_object_agg(reason_counts.reason, reason_counts.reason_count order by reason_counts.reason), '{}'::jsonb)
  into v_stale_reason_counts
  from (
    select diff_rows.reason, count(*)::integer as reason_count
    from pg_temp.tmp_validate_freshness_chunk_diffs as diff_rows
    group by diff_rows.reason
  ) as reason_counts;

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
      'deduction_recovery_changed', coalesce(v_deduction_diff_count, 0)
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
    select count(*)::integer
    into v_known_total_count
    from public.pay_batch_candidates as pay_batch_candidate_count
    where pay_batch_candidate_count.pay_batch_id = p_pay_batch_id;

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
      limit v_limit
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
    select count(*)::integer
    into v_known_total_count
    from public.pay_batch_items as pay_batch_item_count
    join public.pay_batch_candidates as pay_batch_candidate_for_item_count
      on pay_batch_candidate_for_item_count.id = pay_batch_item_count.pay_batch_candidate_id
    where pay_batch_candidate_for_item_count.pay_batch_id = p_pay_batch_id;

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
      limit v_limit
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
    select count(*)::integer
    into v_known_total_count
    from public.pay_batch_item_breakdowns as pay_batch_breakdown_count
    join public.pay_batch_items as pay_batch_item_for_breakdown_count
      on pay_batch_item_for_breakdown_count.id = pay_batch_breakdown_count.pay_batch_item_id
    join public.pay_batch_candidates as pay_batch_candidate_for_breakdown_count
      on pay_batch_candidate_for_breakdown_count.id = pay_batch_item_for_breakdown_count.pay_batch_candidate_id
    where pay_batch_candidate_for_breakdown_count.pay_batch_id = p_pay_batch_id;

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
      limit v_limit
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
    select count(*)::integer
    into v_known_total_count
    from public.pay_bank_transfers as transfer_count_row
    where transfer_count_row.pay_batch_id = p_pay_batch_id;

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
          nullif(btrim(coalesce(transfer_page.rail_tx_id, '')), '') is not null
          or exists (
            select 1
            from public.pay_bank_transfer_events as provider_event_page
            where provider_event_page.pay_batch_id = p_pay_batch_id
              and provider_event_page.pay_bank_transfer_id = transfer_page.id
              and upper(btrim(coalesce(provider_event_page.event_source, ''))) in ('PROVIDER_RESPONSE','PROVIDER_POLL','PROVIDER_WEBHOOK','WEBHOOK','POLL','RAIL_PROVIDER')
              and (
                nullif(btrim(coalesce(provider_event_page.provider_event_id, '')), '') is not null
                or (
                  nullif(btrim(coalesce(provider_event_page.provider_reference, '')), '') is not null
                  and nullif(btrim(coalesce(provider_event_page.provider_reference, '')), '') is distinct from nullif(btrim(coalesce(transfer_page.request_id, '')), '')
                  and nullif(btrim(coalesce(provider_event_page.provider_reference, '')), '') is distinct from nullif(btrim(coalesce(transfer_page.payment_reference, '')), '')
                  and nullif(btrim(coalesce(provider_event_page.provider_reference, '')), '') is distinct from nullif(btrim(coalesce(transfer_page.transfer_group_key, '')), '')
                )
              )
          )
        ) as has_provider_evidence
      from public.pay_bank_transfers as transfer_page
      where transfer_page.pay_batch_id = p_pay_batch_id
        and (v_cursor_id is null or transfer_page.id > v_cursor_id)
      order by transfer_page.id asc
      limit v_limit
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
    select count(*)::integer
    into v_known_total_count
    from (
      select coalesce(pay_batch_item_finance_count.finance_case_id, pay_batch_item_finance_count.finance_component_id) as group_id
      from public.pay_batch_items as pay_batch_item_finance_count
      join public.pay_batch_candidates as pay_batch_candidate_finance_count
        on pay_batch_candidate_finance_count.id = pay_batch_item_finance_count.pay_batch_candidate_id
      where pay_batch_candidate_finance_count.pay_batch_id = p_pay_batch_id
        and (pay_batch_item_finance_count.finance_case_id is not null or pay_batch_item_finance_count.finance_component_id is not null)
      group by coalesce(pay_batch_item_finance_count.finance_case_id, pay_batch_item_finance_count.finance_component_id)
    ) as finance_group_count_rows;

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
      limit v_limit
    ),
    page_rows as (
      select
        page_group_keys.group_id,
        max(pay_batch_item_finance_page.finance_case_id) filter (where pay_batch_item_finance_page.finance_case_id is not null) as finance_case_id,
        max(pay_batch_item_finance_page.finance_component_id) filter (where pay_batch_item_finance_page.finance_component_id is not null) as finance_component_id,
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
    select count(*)::integer
    into v_known_total_count
    from public.mail_outbox as mail_outbox_count
    where (
         mail_outbox_count.payment_scope_json->>'pay_batch_id' = p_pay_batch_id::text
      or (lower(coalesce(mail_outbox_count.context_kind, '')) in ('pay_batch', 'pay_batches') and mail_outbox_count.context_id = p_pay_batch_id)
      or mail_outbox_count.reference = p_pay_batch_id::text
    );

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
      limit v_limit
    )
    select
      coalesce(jsonb_agg(to_jsonb(page_rows) order by page_rows.id asc), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'communications' then
    select count(*)::integer
    into v_known_total_count
    from public.comms_outbox as comms_outbox_count
    where (
         (lower(coalesce(comms_outbox_count.context_kind, '')) in ('pay_batch', 'pay_batches') and comms_outbox_count.context_id = p_pay_batch_id)
      or comms_outbox_count.provider_payload_json->>'pay_batch_id' = p_pay_batch_id::text
      or comms_outbox_count.provider_response_json->>'pay_batch_id' = p_pay_batch_id::text
    );

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
      limit v_limit
    )
    select
      coalesce(jsonb_agg(to_jsonb(page_rows) order by page_rows.id asc), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'auth_history' then
    select count(*)::integer
    into v_known_total_count
    from public.pay_batch_auth_requests as auth_request_count
    where auth_request_count.pay_batch_id = p_pay_batch_id;

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
      limit v_limit
    )
    select
      coalesce(jsonb_agg(to_jsonb(page_rows) order by page_rows.id asc), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;

  elsif v_section = 'events' then
    select count(*)::integer
    into v_known_total_count
    from public.pay_bank_transfer_events as transfer_event_count
    where transfer_event_count.pay_batch_id = p_pay_batch_id;

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
      limit v_limit
    )
    select
      coalesce(jsonb_agg(to_jsonb(page_rows) order by page_rows.id asc), '[]'::jsonb),
      count(*)::integer,
      (array_agg(page_rows.id order by page_rows.id desc))[1]
    into v_items, v_returned_count, v_last_id
    from page_rows;
  end if;

  if v_last_id is not null and v_returned_count = v_limit then
    v_next_cursor := jsonb_build_object('last_id', v_last_id::text);
  else
    v_next_cursor := null;
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




