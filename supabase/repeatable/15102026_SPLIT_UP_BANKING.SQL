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
