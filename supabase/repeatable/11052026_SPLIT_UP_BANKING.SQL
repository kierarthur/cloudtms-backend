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

    if v_chunk_type in ('TRANSFER_SUBMIT', 'PAYEE_READINESS') then
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
        'PREVIEW_PAGE'
    ) then
        raise exception 'Unsupported banking pay operation chunk_type: %', v_chunk_type;
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
        'PREVIEW_PAGE'
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
            raise exception 'banking_pay_operation_seed_chunks received an empty unit list but chunks already exist for operation %, phase %, chunk_type %', p_operation_id, v_phase, v_chunk_type;
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
        raise exception 'banking_pay_operation_seed_chunks found existing chunks with different payloads for operation %, phase %, chunk_type %', p_operation_id, v_phase, v_chunk_type;
    end if;

    select count(*)::integer
    into v_extra_chunk_count
    from public.banking_pay_operation_chunks as extra_chunk
    where extra_chunk.operation_id = p_operation_id
      and extra_chunk.phase = v_phase
      and extra_chunk.chunk_type = v_chunk_type
      and extra_chunk.sequence_no > v_chunk_count;

    if v_extra_chunk_count > 0 then
        raise exception 'banking_pay_operation_seed_chunks found extra existing chunks for operation %, phase %, chunk_type %', p_operation_id, v_phase, v_chunk_type;
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
    select
        component_row.id,
        component_row.finance_case_id,
        component_row.candidate_id,
        component_row.linked_timesheet_id,
        component_row.component_key_type,
        component_row.component_key_value,
        component_row.classification,
        component_row.source_pay_method,
        component_row.source_basis_json,
        component_row.saved_target_pay_method,
        component_row.saved_resolution_mode,
        component_row.saved_resolution_payload_json,
        component_row.saved_resolution_result_json,
        round(coalesce(component_row.source_amount, 0), 2)::numeric(12,2),
        round(coalesce(component_row.remaining_source_amount, 0), 2)::numeric(12,2),
        component_row.allocation_priority_group,
        component_row.allocation_priority_order,
        component_row.created_at_utc
    from public.pay_finance_case_components as component_row
    where component_row.finance_case_id in (
        select distinct selected_row.finance_case_id
        from pg_temp.tmp_pay_build_selected_preview_rows as selected_row
        where selected_row.finance_case_id is not null
    )
      and component_row.closed_at_utc is null
      and coalesce(component_row.remaining_source_amount, 0) > 0;

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
create or replace function public.pay_workbench_prepare_draft_scope_seed(
    p_operation_id uuid,
    p_workbench_session_id uuid,
    p_actor_user_id uuid,
    p_selected_preview_row_ids jsonb default null,
    p_pay_channel_scope text default null,
    p_same_week_paye_override_json jsonb default '{}'::jsonb
)
returns table (
    candidate_scope_count integer,
    selected_row_count integer,
    timesheet_count integer,
    finance_case_count integer,
    pay_channel_count integer
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
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
        );

    if exists (
        select 1
        from pg_temp.tmp_pay_workbench_scope_seed_scopes as proposed_scope
        join public.banking_pay_operation_candidate_scope as existing_scope
          on existing_scope.operation_id = proposed_scope.operation_id
         and existing_scope.candidate_id = proposed_scope.candidate_id
         and existing_scope.pay_channel = proposed_scope.pay_channel
        where existing_scope.scope_hash is distinct from encode(digest(jsonb_build_object(
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
        )::text::bytea, 'sha256'), 'hex')
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
        encode(digest(jsonb_build_object(
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
        )::text::bytea, 'sha256'), 'hex'),
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


CREATE OR REPLACE FUNCTION public.pay_remittance_maybe_queue_for_trigger(
  p_pay_batch_id uuid,
  p_trigger text,
  p_scope text DEFAULT 'ALL'::text,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_only_confirmed boolean DEFAULT false
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
  v_has_remittance_confirmed_overload boolean := false;
  v_has_finance_confirmed_overload boolean := false;
  v_has_remittance_legacy_function boolean := false;
  v_has_finance_legacy_function boolean := false;
  v_dispatch_required boolean := false;
  v_job_count integer := 0;
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

  v_has_remittance_confirmed_overload := to_regprocedure('public.pay_remittance_queue_commit_stage(uuid,text,uuid,boolean)') IS NOT NULL;
  v_has_finance_confirmed_overload := to_regprocedure('public.pay_finance_payout_notice_queue_commit_stage(uuid,uuid,boolean)') IS NOT NULL;
  v_has_remittance_legacy_function := to_regprocedure('public.pay_remittance_queue_commit_stage(uuid,text,uuid)') IS NOT NULL;
  v_has_finance_legacy_function := to_regprocedure('public.pay_finance_payout_notice_queue_commit_stage(uuid,uuid)') IS NOT NULL;

  IF v_is_loans_batch THEN
    IF v_has_finance_confirmed_overload THEN
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
    IF v_has_remittance_confirmed_overload THEN
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







