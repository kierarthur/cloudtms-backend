-- ============================================================
-- CloudTMS Banking Pay scalable operations foundation
-- DB-1 to DB-9
-- Corrected after schema recheck.
-- Safe to rerun.
--
-- Verified against uploaded schema:
--   public.pay_finance_cases DOES NOT EXIST, so no FK is used.
--   public.pay_finance_case_components DOES EXIST.
--   All existing referenced tables/columns below were checked.
-- ============================================================

begin;

create extension if not exists pgcrypto;

-- ============================================================
-- DB-1. banking_pay_operations
-- ============================================================

create table if not exists public.banking_pay_operations (
    id uuid primary key default gen_random_uuid(),

    operation_type text not null,
    status text not null default 'QUEUED',
    phase text not null default 'INITIALISE',

    actor_user_id uuid null,

    workbench_session_id uuid null references public.banking_pay_workbench_sessions(id) on delete set null,
    pay_batch_id uuid null references public.pay_batches(id) on delete set null,
    root_operation_id uuid null references public.banking_pay_operations(id) on delete set null,

    idempotency_key text not null,

    input_json jsonb not null default '{}'::jsonb,
    config_json jsonb not null default '{}'::jsonb,
    progress_json jsonb not null default '{}'::jsonb,
    result_json jsonb null,
    error_json jsonb null,

    total_units integer not null default 0,
    completed_units integer not null default 0,
    failed_units integer not null default 0,
    current_chunk_index integer not null default 0,
    chunk_count integer not null default 0,

    locked_by text null,
    lock_expires_at_utc timestamptz null,

    created_at_utc timestamptz not null default now(),
    started_at_utc timestamptz null,
    updated_at_utc timestamptz not null default now(),
    completed_at_utc timestamptz null,
    failed_at_utc timestamptz null,

    constraint banking_pay_operations_operation_type_chk
        check (operation_type in (
            'DRAFT_CREATE',
            'PAYMENT_EXECUTE',
            'PAYMENT_RETRY_BLOCKED_FUNDS',
            'PAYMENT_SETTLEMENT',
            'REMITTANCE_QUEUE',
            'PREVIEW_REFRESH'
        )),

    constraint banking_pay_operations_status_chk
        check (status in (
            'QUEUED',
            'RUNNING',
            'WAITING',
            'COMPLETE',
            'FAILED',
            'CANCELLED',
            'REVIEW_REQUIRED'
        )),

    constraint banking_pay_operations_units_nonneg_chk
        check (
            total_units >= 0
            and completed_units >= 0
            and failed_units >= 0
            and current_chunk_index >= 0
            and chunk_count >= 0
        ),

    constraint banking_pay_operations_json_shape_chk
        check (
            jsonb_typeof(input_json) = 'object'
            and jsonb_typeof(config_json) = 'object'
            and jsonb_typeof(progress_json) = 'object'
            and (result_json is null or jsonb_typeof(result_json) = 'object')
            and (error_json is null or jsonb_typeof(error_json) = 'object')
        )
);

comment on table public.banking_pay_operations is
'One logical long-running Banking Pay operation, used for scalable draft creation, execution, retry, settlement, remittance and preview refresh.';

comment on column public.banking_pay_operations.config_json is
'Snapshotted operation config. Backend must use this stored config for in-flight operations, not live config.';

create unique index if not exists ux_banking_pay_operations_active_idempotency
on public.banking_pay_operations (idempotency_key)
where status not in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED');

create index if not exists idx_banking_pay_operations_idempotency_key
on public.banking_pay_operations (idempotency_key);

create index if not exists idx_banking_pay_operations_type_status
on public.banking_pay_operations (operation_type, status);

create index if not exists idx_banking_pay_operations_actor_status
on public.banking_pay_operations (actor_user_id, status);

create index if not exists idx_banking_pay_operations_workbench_session
on public.banking_pay_operations (workbench_session_id);

create index if not exists idx_banking_pay_operations_pay_batch
on public.banking_pay_operations (pay_batch_id);

create index if not exists idx_banking_pay_operations_root_operation
on public.banking_pay_operations (root_operation_id);

create index if not exists idx_banking_pay_operations_status_lock_expiry
on public.banking_pay_operations (status, lock_expires_at_utc);


-- ============================================================
-- DB-2. banking_pay_operation_chunks
-- ============================================================

create table if not exists public.banking_pay_operation_chunks (
    id uuid primary key default gen_random_uuid(),

    operation_id uuid not null references public.banking_pay_operations(id) on delete cascade,

    phase text not null,
    chunk_type text not null,
    sequence_no integer not null,

    status text not null default 'PENDING',

    payload_json jsonb not null default '{}'::jsonb,
    result_json jsonb null,
    error_json jsonb null,

    unit_count integer not null default 0,
    completed_count integer not null default 0,
    failed_count integer not null default 0,

    locked_by text null,
    lock_expires_at_utc timestamptz null,

    created_at_utc timestamptz not null default now(),
    started_at_utc timestamptz null,
    completed_at_utc timestamptz null,
    updated_at_utc timestamptz not null default now(),

    constraint banking_pay_operation_chunks_chunk_type_chk
        check (chunk_type in (
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
        )),

    constraint banking_pay_operation_chunks_status_chk
        check (status in (
            'PENDING',
            'RUNNING',
            'COMPLETE',
            'FAILED',
            'SKIPPED'
        )),

    constraint banking_pay_operation_chunks_sequence_chk
        check (sequence_no > 0),

    constraint banking_pay_operation_chunks_counts_nonneg_chk
        check (
            unit_count >= 0
            and completed_count >= 0
            and failed_count >= 0
        ),

    constraint banking_pay_operation_chunks_json_shape_chk
        check (
            jsonb_typeof(payload_json) = 'object'
            and (result_json is null or jsonb_typeof(result_json) = 'object')
            and (error_json is null or jsonb_typeof(error_json) = 'object')
        )
);

comment on table public.banking_pay_operation_chunks is
'Bounded chunks belonging to Banking Pay long-running operations. Draft chunks must use candidate scope ids, not arbitrary timesheet ids.';

create unique index if not exists ux_banking_pay_operation_chunks_sequence
on public.banking_pay_operation_chunks (operation_id, phase, chunk_type, sequence_no);

create index if not exists idx_banking_pay_operation_chunks_operation_status
on public.banking_pay_operation_chunks (operation_id, status);

create index if not exists idx_banking_pay_operation_chunks_operation_phase_status
on public.banking_pay_operation_chunks (operation_id, phase, status);

create index if not exists idx_banking_pay_operation_chunks_operation_sequence
on public.banking_pay_operation_chunks (operation_id, sequence_no);

create index if not exists idx_banking_pay_operation_chunks_status_lock_expiry
on public.banking_pay_operation_chunks (status, lock_expires_at_utc);


-- ============================================================
-- DB-3. banking_pay_operation_config
-- ============================================================

create table if not exists public.banking_pay_operation_config (
    id uuid primary key default gen_random_uuid(),

    operation_type text not null,
    phase text not null default 'ALL',
    chunk_type text not null,

    default_chunk_size integer not null,
    min_chunk_size integer not null,
    max_chunk_size integer not null,
    max_advance_ms integer not null default 15000,
    lock_seconds integer not null default 60,

    enabled boolean not null default true,

    updated_at_utc timestamptz not null default now(),
    updated_by uuid null,

    constraint banking_pay_operation_config_operation_type_chk
        check (operation_type in (
            'ALL',
            'DRAFT_CREATE',
            'PAYMENT_EXECUTE',
            'PAYMENT_RETRY_BLOCKED_FUNDS',
            'PAYMENT_SETTLEMENT',
            'REMITTANCE_QUEUE',
            'PREVIEW_REFRESH'
        )),

    constraint banking_pay_operation_config_chunk_type_chk
        check (chunk_type in (
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
        )),

    constraint banking_pay_operation_config_sizes_chk
        check (
            min_chunk_size > 0
            and default_chunk_size > 0
            and max_chunk_size >= min_chunk_size
            and default_chunk_size between min_chunk_size and max_chunk_size
            and max_advance_ms between 1000 and 120000
            and lock_seconds between 5 and 3600
        )
);

comment on table public.banking_pay_operation_config is
'Configurable chunk sizes and operation advance limits. Resolved values are snapshotted onto banking_pay_operations.config_json at operation start.';

create unique index if not exists ux_banking_pay_operation_config_scope
on public.banking_pay_operation_config (operation_type, phase, chunk_type);

insert into public.banking_pay_operation_config (
    operation_type,
    phase,
    chunk_type,
    default_chunk_size,
    min_chunk_size,
    max_chunk_size,
    max_advance_ms,
    lock_seconds
)
values
    ('DRAFT_CREATE', 'SEED_DRAFT_CHUNKS', 'CANDIDATE_SCOPE', 100, 1, 500, 15000, 60),
    ('DRAFT_CREATE', 'SEED_ALLOCATION_ROWS', 'CANDIDATE_SCOPE', 50, 1, 250, 15000, 60),
    ('DRAFT_CREATE', 'DRAIN_TSFIN', 'TSFIN', 100, 1, 500, 15000, 60),
    ('DRAFT_CREATE', 'ENSURE_PAYEE_READINESS', 'PAYEE_READINESS', 50, 1, 250, 15000, 60),
    ('PREVIEW_REFRESH', 'PAGE', 'PREVIEW_PAGE', 100, 1, 500, 15000, 60),
    ('PAYMENT_EXECUTE', 'PREPARE_TRANSFER_SCOPE', 'TRANSFER_GROUP', 100, 1, 500, 15000, 60),
    ('PAYMENT_EXECUTE', 'PREPARE_TRANSFER_CHUNKS', 'TRANSFER_GROUP', 100, 1, 500, 15000, 60),
    ('PAYMENT_EXECUTE', 'SUBMIT_PROVIDER_TRANSFERS', 'TRANSFER_SUBMIT', 50, 1, 250, 15000, 60),
    ('PAYMENT_EXECUTE', 'APPLY_RAIL_UPDATES', 'RAIL_UPDATE', 100, 1, 500, 15000, 60),
    ('PAYMENT_SETTLEMENT', 'APPLY_SETTLEMENT_CHUNKS', 'SETTLEMENT', 100, 1, 500, 15000, 60),
    ('REMITTANCE_QUEUE', 'QUEUE_REMITTANCE_CHUNKS', 'REMITTANCE', 100, 1, 500, 15000, 60),
    ('REMITTANCE_QUEUE', 'QUEUE_PAYOUT_NOTICE_CHUNKS', 'PAYOUT_NOTICE', 100, 1, 500, 15000, 60)
on conflict (operation_type, phase, chunk_type) do nothing;


-- ============================================================
-- DB-4. banking_pay_operation_candidate_scope
-- ============================================================

create table if not exists public.banking_pay_operation_candidate_scope (
    id uuid primary key default gen_random_uuid(),

    operation_id uuid not null references public.banking_pay_operations(id) on delete cascade,
    workbench_session_id uuid not null references public.banking_pay_workbench_sessions(id) on delete restrict,
    source_snapshot_run_id uuid null references public.banking_pay_snapshot_runs(id) on delete set null,
    source_session_version bigint null,
    candidate_state_id uuid null references public.banking_pay_workbench_session_candidate_state(id) on delete set null,

    candidate_id uuid not null references public.candidates(id) on delete restrict,
    pay_channel text not null,

    pay_batch_id uuid null references public.pay_batches(id) on delete set null,

    selected_preview_row_ids_json jsonb not null default '[]'::jsonb,
    selected_timesheet_ids_json jsonb not null default '[]'::jsonb,
    selected_finance_case_ids_json jsonb not null default '[]'::jsonb,

    effective_candidate_fragment_json jsonb not null default '{}'::jsonb,
    effective_summary_fragment_json jsonb not null default '{}'::jsonb,
    effective_paye_candidate_json jsonb not null default '{}'::jsonb,
    effective_non_paye_payee_json jsonb not null default '{}'::jsonb,
    effective_payees_json jsonb not null default '[]'::jsonb,
    effective_case_resolution_states_json jsonb not null default '{}'::jsonb,
    effective_canonical_preview_lines_json jsonb not null default '[]'::jsonb,
    selected_canonical_preview_lines_json jsonb not null default '[]'::jsonb,

    baseline_component_rows_json jsonb not null default '[]'::jsonb,
    hidden_recovery_template_lines_json jsonb not null default '[]'::jsonb,

    candidate_totals_json jsonb not null default '{}'::jsonb,
    allocation_basis_json jsonb not null default '{}'::jsonb,

    scope_hash text not null,
    chunk_sequence integer null,

    status text not null default 'PENDING',

    created_at_utc timestamptz not null default now(),
    updated_at_utc timestamptz not null default now(),

    constraint banking_pay_operation_candidate_scope_status_chk
        check (status in (
            'PENDING',
            'SCOPED',
            'ALLOCATED',
            'DRAFTED',
            'FAILED'
        )),

    constraint banking_pay_operation_candidate_scope_json_shape_chk
        check (
            jsonb_typeof(selected_preview_row_ids_json) = 'array'
            and jsonb_typeof(selected_timesheet_ids_json) = 'array'
            and jsonb_typeof(selected_finance_case_ids_json) = 'array'
            and jsonb_typeof(effective_candidate_fragment_json) = 'object'
            and jsonb_typeof(effective_summary_fragment_json) = 'object'
            and jsonb_typeof(effective_paye_candidate_json) = 'object'
            and jsonb_typeof(effective_non_paye_payee_json) = 'object'
            and jsonb_typeof(effective_payees_json) = 'array'
            and jsonb_typeof(effective_case_resolution_states_json) = 'object'
            and jsonb_typeof(effective_canonical_preview_lines_json) = 'array'
            and jsonb_typeof(selected_canonical_preview_lines_json) = 'array'
            and jsonb_typeof(baseline_component_rows_json) = 'array'
            and jsonb_typeof(hidden_recovery_template_lines_json) = 'array'
            and jsonb_typeof(candidate_totals_json) = 'object'
            and jsonb_typeof(allocation_basis_json) = 'object'
        )
);

comment on table public.banking_pay_operation_candidate_scope is
'Full selected candidate/pay-channel scope snapshot for scalable draft creation. This preserves candidate-wide deduction/recovery correctness under chunking.';

create unique index if not exists ux_banking_pay_operation_candidate_scope_identity
on public.banking_pay_operation_candidate_scope (operation_id, candidate_id, pay_channel);

create index if not exists idx_banking_pay_operation_candidate_scope_operation_status
on public.banking_pay_operation_candidate_scope (operation_id, status);

create index if not exists idx_banking_pay_operation_candidate_scope_operation_candidate
on public.banking_pay_operation_candidate_scope (operation_id, candidate_id);

create index if not exists idx_banking_pay_operation_candidate_scope_batch_candidate
on public.banking_pay_operation_candidate_scope (pay_batch_id, candidate_id);

create index if not exists idx_banking_pay_operation_candidate_scope_workbench_candidate
on public.banking_pay_operation_candidate_scope (workbench_session_id, candidate_id);

create index if not exists idx_banking_pay_operation_candidate_scope_operation_channel_status
on public.banking_pay_operation_candidate_scope (operation_id, pay_channel, status);


-- ============================================================
-- DB-5. banking_pay_operation_candidate_allocation_rows
-- ============================================================

create table if not exists public.banking_pay_operation_candidate_allocation_rows (
    id uuid primary key default gen_random_uuid(),

    operation_id uuid not null references public.banking_pay_operations(id) on delete cascade,
    candidate_scope_id uuid not null references public.banking_pay_operation_candidate_scope(id) on delete cascade,

    pay_batch_id uuid null references public.pay_batches(id) on delete set null,
    candidate_id uuid not null references public.candidates(id) on delete restrict,
    pay_channel text not null,

    -- No FK here because public.pay_finance_cases does not exist in the checked schema.
    finance_case_id uuid null,

    finance_component_id uuid null references public.pay_finance_case_components(id) on delete set null,

    allocation_type text not null,
    source_ref text null,
    operation_source_key text not null,

    allocated_amount numeric not null default 0,
    allocation_basis_json jsonb not null default '{}'::jsonb,
    sort_order integer not null default 0,

    status text not null default 'PENDING',

    pay_batch_item_id uuid null references public.pay_batch_items(id) on delete set null,

    created_at_utc timestamptz not null default now(),
    updated_at_utc timestamptz not null default now(),

    constraint banking_pay_operation_candidate_allocation_status_chk
        check (status in (
            'PENDING',
            'ITEM_CREATED',
            'SKIPPED',
            'FAILED'
        )),

    constraint banking_pay_operation_candidate_allocation_json_shape_chk
        check (jsonb_typeof(allocation_basis_json) = 'object')
);

comment on table public.banking_pay_operation_candidate_allocation_rows is
'Expected finance/deduction/recovery rows extracted from the full candidate operation scope. This is an idempotency/integrity layer, not a replacement deduction engine. finance_case_id is intentionally not FK constrained because public.pay_finance_cases is not present in the current schema.';

create unique index if not exists ux_banking_pay_operation_candidate_allocation_source
on public.banking_pay_operation_candidate_allocation_rows (operation_id, operation_source_key);

create index if not exists idx_banking_pay_operation_candidate_allocation_operation_candidate
on public.banking_pay_operation_candidate_allocation_rows (operation_id, candidate_id);

create index if not exists idx_banking_pay_operation_candidate_allocation_scope
on public.banking_pay_operation_candidate_allocation_rows (candidate_scope_id);

create index if not exists idx_banking_pay_operation_candidate_allocation_batch_candidate
on public.banking_pay_operation_candidate_allocation_rows (pay_batch_id, candidate_id);

create index if not exists idx_banking_pay_operation_candidate_allocation_finance_case
on public.banking_pay_operation_candidate_allocation_rows (finance_case_id)
where finance_case_id is not null;

create index if not exists idx_banking_pay_operation_candidate_allocation_component
on public.banking_pay_operation_candidate_allocation_rows (finance_component_id)
where finance_component_id is not null;


-- ============================================================
-- DB-6. banking_pay_operation_transfer_scope
-- ============================================================

create table if not exists public.banking_pay_operation_transfer_scope (
    id uuid primary key default gen_random_uuid(),

    operation_id uuid not null references public.banking_pay_operations(id) on delete cascade,
    pay_batch_id uuid not null references public.pay_batches(id) on delete cascade,

    pay_channel text not null,
    transfer_group_key text not null,

    candidate_id uuid null references public.candidates(id) on delete set null,
    umbrella_id uuid null references public.umbrellas(id) on delete set null,

    payee_entity_kind text null,
    payee_entity_id uuid null,

    pay_batch_item_ids_json jsonb not null default '[]'::jsonb,
    candidate_ids_json jsonb not null default '[]'::jsonb,

    currency text not null default 'GBP',
    amount numeric not null default 0,
    payment_reference text null,

    payee_name text null,
    sort_code text null,
    account_number text null,
    account_type text null,

    bank_details_hash_snapshot text null,
    grouping_mode_used text null,
    week_ending_bucket date null,

    request_id text null,

    status text not null default 'PENDING',

    pay_bank_transfer_id uuid null references public.pay_bank_transfers(id) on delete set null,

    created_at_utc timestamptz not null default now(),
    updated_at_utc timestamptz not null default now(),

    constraint banking_pay_operation_transfer_scope_status_chk
        check (status in (
            'PENDING',
            'PREPARED',
            'SUBMITTED',
            'FAILED',
            'SKIPPED'
        )),

    constraint banking_pay_operation_transfer_scope_amount_chk
        check (amount >= 0),

    constraint banking_pay_operation_transfer_scope_json_shape_chk
        check (
            jsonb_typeof(pay_batch_item_ids_json) = 'array'
            and jsonb_typeof(candidate_ids_json) = 'array'
        )
);

comment on table public.banking_pay_operation_transfer_scope is
'Full intended transfer grouping for an execution operation before chunked transfer preparation/submission. Reuses pay_bank_transfers.transfer_group_key semantics.';

create unique index if not exists ux_banking_pay_operation_transfer_scope_operation_group
on public.banking_pay_operation_transfer_scope (operation_id, pay_channel, transfer_group_key);

create unique index if not exists ux_banking_pay_operation_transfer_scope_batch_group
on public.banking_pay_operation_transfer_scope (pay_batch_id, pay_channel, transfer_group_key);

create index if not exists idx_banking_pay_operation_transfer_scope_operation_status
on public.banking_pay_operation_transfer_scope (operation_id, status);

create index if not exists idx_banking_pay_operation_transfer_scope_batch_status
on public.banking_pay_operation_transfer_scope (pay_batch_id, status);


-- ============================================================
-- DB-7. banking_pay_operation_remittance_scope
-- ============================================================

create table if not exists public.banking_pay_operation_remittance_scope (
    id uuid primary key default gen_random_uuid(),

    operation_id uuid not null references public.banking_pay_operations(id) on delete cascade,
    pay_batch_id uuid not null references public.pay_batches(id) on delete cascade,

    pay_batch_candidate_id uuid null references public.pay_batch_candidates(id) on delete set null,
    candidate_id uuid null references public.candidates(id) on delete set null,

    recipient_kind text not null,
    recipient_id uuid null,

    remittance_type text not null,
    deterministic_outbox_key text not null,

    payload_json jsonb not null default '{}'::jsonb,

    status text not null default 'PENDING',

    outbox_id uuid null,

    created_at_utc timestamptz not null default now(),
    updated_at_utc timestamptz not null default now(),

    constraint banking_pay_operation_remittance_scope_status_chk
        check (status in (
            'PENDING',
            'QUEUED',
            'SKIPPED',
            'FAILED'
        )),

    constraint banking_pay_operation_remittance_scope_payload_shape_chk
        check (jsonb_typeof(payload_json) = 'object')
);

comment on table public.banking_pay_operation_remittance_scope is
'Remittance/outbox units to queue in bounded chunks. deterministic_outbox_key makes retry safe.';

create unique index if not exists ux_banking_pay_operation_remittance_scope_outbox_key
on public.banking_pay_operation_remittance_scope (operation_id, deterministic_outbox_key);

create index if not exists idx_banking_pay_operation_remittance_scope_operation_status
on public.banking_pay_operation_remittance_scope (operation_id, status);

create index if not exists idx_banking_pay_operation_remittance_scope_batch_status
on public.banking_pay_operation_remittance_scope (pay_batch_id, status);


-- ============================================================
-- DB-8. banking_pay_operation_settlement_scope
-- ============================================================

create table if not exists public.banking_pay_operation_settlement_scope (
    id uuid primary key default gen_random_uuid(),

    operation_id uuid not null references public.banking_pay_operations(id) on delete cascade,
    pay_batch_id uuid not null references public.pay_batches(id) on delete cascade,

    pay_batch_candidate_id uuid null references public.pay_batch_candidates(id) on delete set null,
    candidate_id uuid null references public.candidates(id) on delete set null,

    pay_channel text not null,
    settlement_key text not null,

    payload_json jsonb not null default '{}'::jsonb,

    status text not null default 'PENDING',

    settlement_event_id uuid null,

    created_at_utc timestamptz not null default now(),
    updated_at_utc timestamptz not null default now(),

    constraint banking_pay_operation_settlement_scope_status_chk
        check (status in (
            'PENDING',
            'SETTLED',
            'SKIPPED',
            'FAILED'
        )),

    constraint banking_pay_operation_settlement_scope_payload_shape_chk
        check (jsonb_typeof(payload_json) = 'object')
);

comment on table public.banking_pay_operation_settlement_scope is
'Settlement units for chunked manual, CSV, external and rail settlement. settlement_key makes retry safe.';

create unique index if not exists ux_banking_pay_operation_settlement_scope_key
on public.banking_pay_operation_settlement_scope (operation_id, settlement_key);

create index if not exists idx_banking_pay_operation_settlement_scope_operation_status
on public.banking_pay_operation_settlement_scope (operation_id, status);

create index if not exists idx_banking_pay_operation_settlement_scope_batch_status
on public.banking_pay_operation_settlement_scope (pay_batch_id, status);


-- ============================================================
-- DB-9. Existing table/index changes
-- ============================================================

alter table public.pay_batch_items
    add column if not exists operation_source_key text null;

create index if not exists idx_pay_batch_items_operation_source_key
on public.pay_batch_items (operation_source_key)
where operation_source_key is not null;

create unique index if not exists ux_pay_batch_items_candidate_operation_source_key
on public.pay_batch_items (pay_batch_candidate_id, operation_source_key)
where operation_source_key is not null;


alter table public.pay_batch_item_breakdowns
    add column if not exists operation_source_key text null;

create unique index if not exists ux_pay_batch_item_breakdowns_item_operation_source_key
on public.pay_batch_item_breakdowns (pay_batch_item_id, operation_source_key)
where operation_source_key is not null;


alter table public.mail_outbox
    add column if not exists deterministic_outbox_key text null;

create unique index if not exists ux_mail_outbox_deterministic_outbox_key
on public.mail_outbox (deterministic_outbox_key)
where deterministic_outbox_key is not null;


alter table public.comms_outbox
    add column if not exists deterministic_outbox_key text null;

create unique index if not exists ux_comms_outbox_deterministic_outbox_key
on public.comms_outbox (deterministic_outbox_key)
where deterministic_outbox_key is not null;


create index if not exists idx_pay_bank_transfers_batch_status
on public.pay_bank_transfers (pay_batch_id, status);

create index if not exists idx_pay_bank_transfers_batch_rail_state
on public.pay_bank_transfers (pay_batch_id, rail_state);

create index if not exists idx_pay_bank_transfers_batch_channel_group_key
on public.pay_bank_transfers (pay_batch_id, pay_channel, transfer_group_key);


create index if not exists idx_banking_pay_workbench_candidate_state_session_status
on public.banking_pay_workbench_session_candidate_state (session_id, status);

create index if not exists idx_banking_pay_workbench_candidate_state_session_candidate
on public.banking_pay_workbench_session_candidate_state (session_id, candidate_id);


do $$
begin
    if exists (
        select 1
        from public.pay_batch_candidates pbc
        where pbc.pay_batch_id is not null
          and pbc.candidate_id is not null
        group by pbc.pay_batch_id, pbc.candidate_id
        having count(*) > 1
    ) then
        raise notice 'Skipping unique index ux_pay_batch_candidates_batch_candidate because duplicate pay_batch_id/candidate_id rows already exist. Creating non-unique fallback index instead.';

        create index if not exists idx_pay_batch_candidates_batch_candidate
        on public.pay_batch_candidates (pay_batch_id, candidate_id);
    else
        create unique index if not exists ux_pay_batch_candidates_batch_candidate
        on public.pay_batch_candidates (pay_batch_id, candidate_id);
    end if;
end $$;


-- ============================================================
-- updated_at trigger helper for new operation tables
-- ============================================================

create or replace function public._banking_pay_operation_touch_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at_utc := now();
    return new;
end;
$$;

drop trigger if exists trg_banking_pay_operations_touch_updated_at on public.banking_pay_operations;
create trigger trg_banking_pay_operations_touch_updated_at
before update on public.banking_pay_operations
for each row
execute function public._banking_pay_operation_touch_updated_at();

drop trigger if exists trg_banking_pay_operation_chunks_touch_updated_at on public.banking_pay_operation_chunks;
create trigger trg_banking_pay_operation_chunks_touch_updated_at
before update on public.banking_pay_operation_chunks
for each row
execute function public._banking_pay_operation_touch_updated_at();

drop trigger if exists trg_banking_pay_operation_config_touch_updated_at on public.banking_pay_operation_config;
create trigger trg_banking_pay_operation_config_touch_updated_at
before update on public.banking_pay_operation_config
for each row
execute function public._banking_pay_operation_touch_updated_at();

drop trigger if exists trg_banking_pay_operation_candidate_scope_touch_updated_at on public.banking_pay_operation_candidate_scope;
create trigger trg_banking_pay_operation_candidate_scope_touch_updated_at
before update on public.banking_pay_operation_candidate_scope
for each row
execute function public._banking_pay_operation_touch_updated_at();

drop trigger if exists trg_banking_pay_operation_candidate_allocation_touch_updated_at on public.banking_pay_operation_candidate_allocation_rows;
create trigger trg_banking_pay_operation_candidate_allocation_touch_updated_at
before update on public.banking_pay_operation_candidate_allocation_rows
for each row
execute function public._banking_pay_operation_touch_updated_at();

drop trigger if exists trg_banking_pay_operation_transfer_scope_touch_updated_at on public.banking_pay_operation_transfer_scope;
create trigger trg_banking_pay_operation_transfer_scope_touch_updated_at
before update on public.banking_pay_operation_transfer_scope
for each row
execute function public._banking_pay_operation_touch_updated_at();

drop trigger if exists trg_banking_pay_operation_remittance_scope_touch_updated_at on public.banking_pay_operation_remittance_scope;
create trigger trg_banking_pay_operation_remittance_scope_touch_updated_at
before update on public.banking_pay_operation_remittance_scope
for each row
execute function public._banking_pay_operation_touch_updated_at();

drop trigger if exists trg_banking_pay_operation_settlement_scope_touch_updated_at on public.banking_pay_operation_settlement_scope;
create trigger trg_banking_pay_operation_settlement_scope_touch_updated_at
before update on public.banking_pay_operation_settlement_scope
for each row
execute function public._banking_pay_operation_touch_updated_at();

commit;
