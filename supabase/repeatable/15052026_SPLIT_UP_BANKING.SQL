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













