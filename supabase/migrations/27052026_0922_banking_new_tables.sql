begin;

create extension if not exists pgcrypto;

-- ============================================================
-- 1. Provider webhook configuration
-- ============================================================

create table if not exists public.bank_provider_webhook_configs (
    id uuid primary key default gen_random_uuid(),
    provider_key text not null,
    rail_env text not null default 'PROD',
    webhook_public_id text not null,
    provider_webhook_id text null,
    webhook_url text not null,
    event_types jsonb not null default '[]'::jsonb,
    signing_secret_ref text null,
    signing_secret_encrypted text null,
    old_signing_secret_encrypted text null,
    old_secret_expires_at_utc timestamptz null,
    status text not null default 'ACTIVE',
    created_at_utc timestamptz not null default now(),
    updated_at_utc timestamptz not null default now(),
    last_verified_at_utc timestamptz null,
    last_event_at_utc timestamptz null,
    last_error_at_utc timestamptz null,
    last_error text null,
    last_failed_event_sync_at_utc timestamptz null,
    meta_json jsonb not null default '{}'::jsonb
);

alter table public.bank_provider_webhook_configs
    add column if not exists id uuid default gen_random_uuid();

alter table public.bank_provider_webhook_configs
    add column if not exists provider_key text;

alter table public.bank_provider_webhook_configs
    add column if not exists rail_env text default 'PROD';

alter table public.bank_provider_webhook_configs
    add column if not exists webhook_public_id text;

alter table public.bank_provider_webhook_configs
    add column if not exists provider_webhook_id text;

alter table public.bank_provider_webhook_configs
    add column if not exists webhook_url text;

alter table public.bank_provider_webhook_configs
    add column if not exists event_types jsonb default '[]'::jsonb;

alter table public.bank_provider_webhook_configs
    add column if not exists signing_secret_ref text;

alter table public.bank_provider_webhook_configs
    add column if not exists signing_secret_encrypted text;

alter table public.bank_provider_webhook_configs
    add column if not exists old_signing_secret_encrypted text;

alter table public.bank_provider_webhook_configs
    add column if not exists old_secret_expires_at_utc timestamptz;

alter table public.bank_provider_webhook_configs
    add column if not exists status text default 'ACTIVE';

alter table public.bank_provider_webhook_configs
    add column if not exists created_at_utc timestamptz default now();

alter table public.bank_provider_webhook_configs
    add column if not exists updated_at_utc timestamptz default now();

alter table public.bank_provider_webhook_configs
    add column if not exists last_verified_at_utc timestamptz;

alter table public.bank_provider_webhook_configs
    add column if not exists last_event_at_utc timestamptz;

alter table public.bank_provider_webhook_configs
    add column if not exists last_error_at_utc timestamptz;

alter table public.bank_provider_webhook_configs
    add column if not exists last_error text;

alter table public.bank_provider_webhook_configs
    add column if not exists last_failed_event_sync_at_utc timestamptz;

alter table public.bank_provider_webhook_configs
    add column if not exists meta_json jsonb default '{}'::jsonb;

update public.bank_provider_webhook_configs
set
    rail_env = coalesce(rail_env, 'PROD'),
    event_types = coalesce(event_types, '[]'::jsonb),
    status = coalesce(status, 'ACTIVE'),
    created_at_utc = coalesce(created_at_utc, now()),
    updated_at_utc = coalesce(updated_at_utc, now()),
    meta_json = coalesce(meta_json, '{}'::jsonb);

alter table public.bank_provider_webhook_configs
    alter column id set default gen_random_uuid(),
    alter column provider_key set not null,
    alter column rail_env set default 'PROD',
    alter column rail_env set not null,
    alter column webhook_public_id set not null,
    alter column webhook_url set not null,
    alter column event_types set default '[]'::jsonb,
    alter column event_types set not null,
    alter column status set default 'ACTIVE',
    alter column status set not null,
    alter column created_at_utc set default now(),
    alter column created_at_utc set not null,
    alter column updated_at_utc set default now(),
    alter column updated_at_utc set not null,
    alter column meta_json set default '{}'::jsonb,
    alter column meta_json set not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_configs'::regclass
          and conname = 'bank_provider_webhook_configs_status_chk'
    ) then
        alter table public.bank_provider_webhook_configs
            add constraint bank_provider_webhook_configs_status_chk
            check (status in ('ACTIVE','DISABLED','ROTATING_SECRET','ERROR','DELETED'));
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_configs'::regclass
          and conname = 'bank_provider_webhook_configs_event_types_array_chk'
    ) then
        alter table public.bank_provider_webhook_configs
            add constraint bank_provider_webhook_configs_event_types_array_chk
            check (jsonb_typeof(event_types) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_configs'::regclass
          and conname = 'bank_provider_webhook_configs_meta_object_chk'
    ) then
        alter table public.bank_provider_webhook_configs
            add constraint bank_provider_webhook_configs_meta_object_chk
            check (jsonb_typeof(meta_json) = 'object');
    end if;
end $$;

create unique index if not exists bank_provider_webhook_configs_public_uidx
    on public.bank_provider_webhook_configs (provider_key, rail_env, webhook_public_id);

create unique index if not exists bank_provider_webhook_configs_provider_webhook_uidx
    on public.bank_provider_webhook_configs (provider_key, rail_env, provider_webhook_id)
    where provider_webhook_id is not null;

create index if not exists bank_provider_webhook_configs_status_idx
    on public.bank_provider_webhook_configs (provider_key, rail_env, status);

create index if not exists bank_provider_webhook_configs_last_event_idx
    on public.bank_provider_webhook_configs (last_event_at_utc desc)
    where last_event_at_utc is not null;


-- ============================================================
-- 2. Provider webhook receipt ledger
-- ============================================================

create table if not exists public.bank_provider_webhook_receipts (
    id uuid primary key default gen_random_uuid(),
    provider_key text not null,
    rail_env text not null default 'PROD',
    webhook_config_id uuid null references public.bank_provider_webhook_configs(id),
    provider_webhook_id text null,
    provider_event_type text null,
    provider_event_id text null,
    provider_event_key text not null,
    provider_transaction_id text null,
    provider_request_id text null,
    signature_valid boolean not null default false,
    signature_version text null,
    signature_header text null,
    request_timestamp text null,
    request_received_at_utc timestamptz not null default now(),
    event_time_utc timestamptz null,
    raw_payload_json jsonb null,
    raw_payload_hash text not null,
    raw_headers_redacted jsonb not null default '{}'::jsonb,
    normalised_events_json jsonb not null default '[]'::jsonb,
    ingest_results_json jsonb not null default '[]'::jsonb,
    status text not null default 'RECEIVED',
    error_code text null,
    error_message text null,
    attempt_count integer not null default 1,
    processed_at_utc timestamptz null,
    created_at_utc timestamptz not null default now(),
    updated_at_utc timestamptz not null default now()
);

alter table public.bank_provider_webhook_receipts
    add column if not exists id uuid default gen_random_uuid();

alter table public.bank_provider_webhook_receipts
    add column if not exists provider_key text;

alter table public.bank_provider_webhook_receipts
    add column if not exists rail_env text default 'PROD';

alter table public.bank_provider_webhook_receipts
    add column if not exists webhook_config_id uuid;

alter table public.bank_provider_webhook_receipts
    add column if not exists provider_webhook_id text;

alter table public.bank_provider_webhook_receipts
    add column if not exists provider_event_type text;

alter table public.bank_provider_webhook_receipts
    add column if not exists provider_event_id text;

alter table public.bank_provider_webhook_receipts
    add column if not exists provider_event_key text;

alter table public.bank_provider_webhook_receipts
    add column if not exists provider_transaction_id text;

alter table public.bank_provider_webhook_receipts
    add column if not exists provider_request_id text;

alter table public.bank_provider_webhook_receipts
    add column if not exists signature_valid boolean default false;

alter table public.bank_provider_webhook_receipts
    add column if not exists signature_version text;

alter table public.bank_provider_webhook_receipts
    add column if not exists signature_header text;

alter table public.bank_provider_webhook_receipts
    add column if not exists request_timestamp text;

alter table public.bank_provider_webhook_receipts
    add column if not exists request_received_at_utc timestamptz default now();

alter table public.bank_provider_webhook_receipts
    add column if not exists event_time_utc timestamptz;

alter table public.bank_provider_webhook_receipts
    add column if not exists raw_payload_json jsonb;

alter table public.bank_provider_webhook_receipts
    add column if not exists raw_payload_hash text;

alter table public.bank_provider_webhook_receipts
    add column if not exists raw_headers_redacted jsonb default '{}'::jsonb;

alter table public.bank_provider_webhook_receipts
    add column if not exists normalised_events_json jsonb default '[]'::jsonb;

alter table public.bank_provider_webhook_receipts
    add column if not exists ingest_results_json jsonb default '[]'::jsonb;

alter table public.bank_provider_webhook_receipts
    add column if not exists status text default 'RECEIVED';

alter table public.bank_provider_webhook_receipts
    add column if not exists error_code text;

alter table public.bank_provider_webhook_receipts
    add column if not exists error_message text;

alter table public.bank_provider_webhook_receipts
    add column if not exists attempt_count integer default 1;

alter table public.bank_provider_webhook_receipts
    add column if not exists processed_at_utc timestamptz;

alter table public.bank_provider_webhook_receipts
    add column if not exists created_at_utc timestamptz default now();

alter table public.bank_provider_webhook_receipts
    add column if not exists updated_at_utc timestamptz default now();

update public.bank_provider_webhook_receipts
set
    rail_env = coalesce(rail_env, 'PROD'),
    signature_valid = coalesce(signature_valid, false),
    request_received_at_utc = coalesce(request_received_at_utc, now()),
    raw_headers_redacted = coalesce(raw_headers_redacted, '{}'::jsonb),
    normalised_events_json = coalesce(normalised_events_json, '[]'::jsonb),
    ingest_results_json = coalesce(ingest_results_json, '[]'::jsonb),
    status = coalesce(status, 'RECEIVED'),
    attempt_count = coalesce(attempt_count, 1),
    created_at_utc = coalesce(created_at_utc, now()),
    updated_at_utc = coalesce(updated_at_utc, now());

alter table public.bank_provider_webhook_receipts
    alter column id set default gen_random_uuid(),
    alter column provider_key set not null,
    alter column rail_env set default 'PROD',
    alter column rail_env set not null,
    alter column provider_event_key set not null,
    alter column signature_valid set default false,
    alter column signature_valid set not null,
    alter column request_received_at_utc set default now(),
    alter column request_received_at_utc set not null,
    alter column raw_payload_hash set not null,
    alter column raw_headers_redacted set default '{}'::jsonb,
    alter column raw_headers_redacted set not null,
    alter column normalised_events_json set default '[]'::jsonb,
    alter column normalised_events_json set not null,
    alter column ingest_results_json set default '[]'::jsonb,
    alter column ingest_results_json set not null,
    alter column status set default 'RECEIVED',
    alter column status set not null,
    alter column attempt_count set default 1,
    alter column attempt_count set not null,
    alter column created_at_utc set default now(),
    alter column created_at_utc set not null,
    alter column updated_at_utc set default now(),
    alter column updated_at_utc set not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_receipts'::regclass
          and conname = 'bank_provider_webhook_receipts_config_fkey'
    ) then
        alter table public.bank_provider_webhook_receipts
            add constraint bank_provider_webhook_receipts_config_fkey
            foreign key (webhook_config_id)
            references public.bank_provider_webhook_configs(id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_receipts'::regclass
          and conname = 'bank_provider_webhook_receipts_status_chk'
    ) then
        alter table public.bank_provider_webhook_receipts
            add constraint bank_provider_webhook_receipts_status_chk
            check (
                status in (
                    'RECEIVED',
                    'DUPLICATE',
                    'SIGNATURE_INVALID',
                    'VERIFIED',
                    'NORMALISED',
                    'INGESTED',
                    'UNMATCHED_REVIEW_REQUIRED',
                    'FAILED_RETRYABLE',
                    'FAILED_FINAL',
                    'IGNORED_EVENT_TYPE'
                )
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_receipts'::regclass
          and conname = 'bank_provider_webhook_receipts_raw_headers_object_chk'
    ) then
        alter table public.bank_provider_webhook_receipts
            add constraint bank_provider_webhook_receipts_raw_headers_object_chk
            check (jsonb_typeof(raw_headers_redacted) = 'object');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_receipts'::regclass
          and conname = 'bank_provider_webhook_receipts_normalised_events_array_chk'
    ) then
        alter table public.bank_provider_webhook_receipts
            add constraint bank_provider_webhook_receipts_normalised_events_array_chk
            check (jsonb_typeof(normalised_events_json) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_receipts'::regclass
          and conname = 'bank_provider_webhook_receipts_ingest_results_array_chk'
    ) then
        alter table public.bank_provider_webhook_receipts
            add constraint bank_provider_webhook_receipts_ingest_results_array_chk
            check (jsonb_typeof(ingest_results_json) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.bank_provider_webhook_receipts'::regclass
          and conname = 'bank_provider_webhook_receipts_attempt_count_chk'
    ) then
        alter table public.bank_provider_webhook_receipts
            add constraint bank_provider_webhook_receipts_attempt_count_chk
            check (attempt_count >= 1);
    end if;
end $$;

create unique index if not exists bank_provider_webhook_receipts_event_key_uidx
    on public.bank_provider_webhook_receipts (provider_key, rail_env, provider_event_key);

create index if not exists bank_provider_webhook_receipts_status_idx
    on public.bank_provider_webhook_receipts (provider_key, rail_env, status);

create index if not exists bank_provider_webhook_receipts_transaction_idx
    on public.bank_provider_webhook_receipts (provider_transaction_id)
    where provider_transaction_id is not null;

create index if not exists bank_provider_webhook_receipts_request_idx
    on public.bank_provider_webhook_receipts (provider_request_id)
    where provider_request_id is not null;

create index if not exists bank_provider_webhook_receipts_config_received_idx
    on public.bank_provider_webhook_receipts (webhook_config_id, request_received_at_utc desc)
    where webhook_config_id is not null;

create index if not exists bank_provider_webhook_receipts_unmatched_idx
    on public.bank_provider_webhook_receipts (provider_key, rail_env, request_received_at_utc desc)
    where status = 'UNMATCHED_REVIEW_REQUIRED';


-- ============================================================
-- 3. Extend canonical bank transfer event ledger
-- ============================================================

alter table public.pay_bank_transfer_events
    add column if not exists provider_webhook_receipt_id uuid;

alter table public.pay_bank_transfer_events
    add column if not exists provider_event_type text;

alter table public.pay_bank_transfer_events
    add column if not exists provider_transaction_id text;

alter table public.pay_bank_transfer_events
    add column if not exists provider_request_id text;

alter table public.pay_bank_transfer_events
    add column if not exists provider_event_key text;

alter table public.pay_bank_transfer_events
    add column if not exists provider_signature_valid boolean;

alter table public.pay_bank_transfer_events
    add column if not exists provider_event_transport text;

alter table public.pay_bank_transfer_events
    add column if not exists adapter_key text;

alter table public.pay_bank_transfer_events
    add column if not exists adapter_version text;

alter table public.pay_bank_transfer_events
    add column if not exists rail_env text;

alter table public.pay_bank_transfer_events
    add column if not exists provider_failure_reason_code text;

alter table public.pay_bank_transfer_events
    add column if not exists provider_failure_reason_group text;

alter table public.pay_bank_transfer_events
    add column if not exists mapping_hints_json jsonb default '{}'::jsonb;

update public.pay_bank_transfer_events
set
    mapping_hints_json = coalesce(mapping_hints_json, '{}'::jsonb);

alter table public.pay_bank_transfer_events
    alter column mapping_hints_json set default '{}'::jsonb,
    alter column mapping_hints_json set not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.pay_bank_transfer_events'::regclass
          and conname = 'pay_bank_transfer_events_webhook_receipt_fkey'
    ) then
        alter table public.pay_bank_transfer_events
            add constraint pay_bank_transfer_events_webhook_receipt_fkey
            foreign key (provider_webhook_receipt_id)
            references public.bank_provider_webhook_receipts(id);
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.pay_bank_transfer_events'::regclass
          and conname = 'pay_bank_transfer_events_provider_transport_chk'
    ) then
        alter table public.pay_bank_transfer_events
            add constraint pay_bank_transfer_events_provider_transport_chk
            check (
                provider_event_transport is null
                or provider_event_transport in (
                    'PROVIDER_RESPONSE',
                    'PROVIDER_POLL',
                    'PROVIDER_WEBHOOK',
                    'FAILED_WEBHOOK_REPLAY',
                    'MANUAL_CONFIRM',
                    'LOCAL_STATE'
                )
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.pay_bank_transfer_events'::regclass
          and conname = 'pay_bank_transfer_events_failure_reason_group_chk'
    ) then
        alter table public.pay_bank_transfer_events
            add constraint pay_bank_transfer_events_failure_reason_group_chk
            check (
                provider_failure_reason_group is null
                or provider_failure_reason_group in (
                    'INSUFFICIENT_FUNDS',
                    'UNKNOWN_RECIPIENT',
                    'INVALID_ACCOUNT',
                    'ACCOUNT_CLOSED',
                    'BANK_REJECTED',
                    'PROVIDER_OUTAGE',
                    'PROVIDER_UNKNOWN',
                    'COMPLIANCE_REVIEW',
                    'DUPLICATE_RISK',
                    'PAID_RECOVERY_REQUIRED',
                    'MANUAL_ADJUSTMENT_BLOCKER',
                    'WEBHOOK_UNMATCHED',
                    'PROVIDER_FAILED_UNSPECIFIED'
                )
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.pay_bank_transfer_events'::regclass
          and conname = 'pay_bank_transfer_events_mapping_hints_object_chk'
    ) then
        alter table public.pay_bank_transfer_events
            add constraint pay_bank_transfer_events_mapping_hints_object_chk
            check (jsonb_typeof(mapping_hints_json) = 'object');
    end if;
end $$;

create unique index if not exists pay_bank_transfer_events_provider_transport_event_uidx
    on public.pay_bank_transfer_events (
        provider_key,
        rail_env,
        provider_event_transport,
        provider_event_key
    )
    where provider_event_key is not null;

create index if not exists pay_bank_transfer_events_webhook_receipt_idx
    on public.pay_bank_transfer_events (provider_webhook_receipt_id)
    where provider_webhook_receipt_id is not null;

create index if not exists pay_bank_transfer_events_provider_transaction_idx
    on public.pay_bank_transfer_events (provider_key, provider_transaction_id)
    where provider_transaction_id is not null;

create index if not exists pay_bank_transfer_events_provider_request_idx
    on public.pay_bank_transfer_events (provider_key, provider_request_id)
    where provider_request_id is not null;

create index if not exists pay_bank_transfer_events_provider_transport_state_idx
    on public.pay_bank_transfer_events (
        pay_batch_id,
        provider_event_transport,
        normalised_state,
        received_at_utc desc
    )
    where provider_event_transport is not null;

create index if not exists pay_bank_transfer_events_failure_reason_idx
    on public.pay_bank_transfer_events (
        provider_failure_reason_group,
        received_at_utc desc
    )
    where provider_failure_reason_group is not null;


-- ============================================================
-- 4. Lightweight live-update signal for open Banking Pay modals
-- ============================================================

create table if not exists public.banking_pay_batch_change_signals (
    pay_batch_id uuid primary key references public.pay_batches(id) on delete cascade,
    version bigint not null default 0,
    payment_status_version bigint not null default 0,
    correction_progress_version bigint not null default 0,
    alert_version bigint not null default 0,
    overview_version bigint not null default 0,
    last_changed_at_utc timestamptz not null default now(),
    last_payment_status_changed_at_utc timestamptz null,
    last_correction_progress_changed_at_utc timestamptz null,
    last_alert_changed_at_utc timestamptz null,
    last_change_reason text null,
    last_change_source text null,
    last_change_scope_json jsonb not null default '{}'::jsonb,
    last_changed_transfer_ids jsonb not null default '[]'::jsonb,
    last_changed_candidate_ids jsonb not null default '[]'::jsonb,
    last_changed_pay_batch_item_ids jsonb not null default '[]'::jsonb,
    last_status_hash text null,
    last_alert_hash text null,
    updated_at_utc timestamptz not null default now()
);

alter table public.banking_pay_batch_change_signals
    add column if not exists pay_batch_id uuid;

alter table public.banking_pay_batch_change_signals
    add column if not exists version bigint default 0;

alter table public.banking_pay_batch_change_signals
    add column if not exists payment_status_version bigint default 0;

alter table public.banking_pay_batch_change_signals
    add column if not exists correction_progress_version bigint default 0;

alter table public.banking_pay_batch_change_signals
    add column if not exists alert_version bigint default 0;

alter table public.banking_pay_batch_change_signals
    add column if not exists overview_version bigint default 0;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_changed_at_utc timestamptz default now();

alter table public.banking_pay_batch_change_signals
    add column if not exists last_payment_status_changed_at_utc timestamptz;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_correction_progress_changed_at_utc timestamptz;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_alert_changed_at_utc timestamptz;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_change_reason text;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_change_source text;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_change_scope_json jsonb default '{}'::jsonb;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_changed_transfer_ids jsonb default '[]'::jsonb;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_changed_candidate_ids jsonb default '[]'::jsonb;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_changed_pay_batch_item_ids jsonb default '[]'::jsonb;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_status_hash text;

alter table public.banking_pay_batch_change_signals
    add column if not exists last_alert_hash text;

alter table public.banking_pay_batch_change_signals
    add column if not exists updated_at_utc timestamptz default now();

update public.banking_pay_batch_change_signals
set
    version = coalesce(version, 0),
    payment_status_version = coalesce(payment_status_version, 0),
    correction_progress_version = coalesce(correction_progress_version, 0),
    alert_version = coalesce(alert_version, 0),
    overview_version = coalesce(overview_version, 0),
    last_changed_at_utc = coalesce(last_changed_at_utc, now()),
    last_change_scope_json = coalesce(last_change_scope_json, '{}'::jsonb),
    last_changed_transfer_ids = coalesce(last_changed_transfer_ids, '[]'::jsonb),
    last_changed_candidate_ids = coalesce(last_changed_candidate_ids, '[]'::jsonb),
    last_changed_pay_batch_item_ids = coalesce(last_changed_pay_batch_item_ids, '[]'::jsonb),
    updated_at_utc = coalesce(updated_at_utc, now());

alter table public.banking_pay_batch_change_signals
    alter column pay_batch_id set not null,
    alter column version set default 0,
    alter column version set not null,
    alter column payment_status_version set default 0,
    alter column payment_status_version set not null,
    alter column correction_progress_version set default 0,
    alter column correction_progress_version set not null,
    alter column alert_version set default 0,
    alter column alert_version set not null,
    alter column overview_version set default 0,
    alter column overview_version set not null,
    alter column last_changed_at_utc set default now(),
    alter column last_changed_at_utc set not null,
    alter column last_change_scope_json set default '{}'::jsonb,
    alter column last_change_scope_json set not null,
    alter column last_changed_transfer_ids set default '[]'::jsonb,
    alter column last_changed_transfer_ids set not null,
    alter column last_changed_candidate_ids set default '[]'::jsonb,
    alter column last_changed_candidate_ids set not null,
    alter column last_changed_pay_batch_item_ids set default '[]'::jsonb,
    alter column last_changed_pay_batch_item_ids set not null,
    alter column updated_at_utc set default now(),
    alter column updated_at_utc set not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_pay_batch_change_signals'::regclass
          and conname = 'banking_pay_batch_change_signals_batch_fkey'
    ) then
        alter table public.banking_pay_batch_change_signals
            add constraint banking_pay_batch_change_signals_batch_fkey
            foreign key (pay_batch_id)
            references public.pay_batches(id)
            on delete cascade;
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_pay_batch_change_signals'::regclass
          and conname = 'banking_pay_batch_change_signals_versions_chk'
    ) then
        alter table public.banking_pay_batch_change_signals
            add constraint banking_pay_batch_change_signals_versions_chk
            check (
                version >= 0
                and payment_status_version >= 0
                and correction_progress_version >= 0
                and alert_version >= 0
                and overview_version >= 0
            );
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_pay_batch_change_signals'::regclass
          and conname = 'banking_pay_batch_change_signals_scope_object_chk'
    ) then
        alter table public.banking_pay_batch_change_signals
            add constraint banking_pay_batch_change_signals_scope_object_chk
            check (jsonb_typeof(last_change_scope_json) = 'object');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_pay_batch_change_signals'::regclass
          and conname = 'banking_pay_batch_change_signals_transfer_ids_array_chk'
    ) then
        alter table public.banking_pay_batch_change_signals
            add constraint banking_pay_batch_change_signals_transfer_ids_array_chk
            check (jsonb_typeof(last_changed_transfer_ids) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_pay_batch_change_signals'::regclass
          and conname = 'banking_pay_batch_change_signals_candidate_ids_array_chk'
    ) then
        alter table public.banking_pay_batch_change_signals
            add constraint banking_pay_batch_change_signals_candidate_ids_array_chk
            check (jsonb_typeof(last_changed_candidate_ids) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_pay_batch_change_signals'::regclass
          and conname = 'banking_pay_batch_change_signals_item_ids_array_chk'
    ) then
        alter table public.banking_pay_batch_change_signals
            add constraint banking_pay_batch_change_signals_item_ids_array_chk
            check (jsonb_typeof(last_changed_pay_batch_item_ids) = 'array');
    end if;
end $$;

create index if not exists banking_pay_batch_change_signals_updated_idx
    on public.banking_pay_batch_change_signals (updated_at_utc);

create index if not exists banking_pay_batch_change_signals_changed_idx
    on public.banking_pay_batch_change_signals (last_changed_at_utc);


-- ============================================================
-- 5. Per-user Banking Alert preferences
-- ============================================================

create table if not exists public.banking_alert_user_preferences (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null references auth.users(id),
    enabled boolean not null default true,
    alert_kind_allowlist jsonb null,
    alert_kind_blocklist jsonb not null default '[]'::jsonb,
    failure_reason_allowlist jsonb null,
    failure_reason_blocklist jsonb not null default '[]'::jsonb,
    include_action_required boolean not null default true,
    include_progress_alerts boolean not null default true,
    include_informational_alerts boolean not null default false,
    include_success_alerts boolean not null default false,
    severity_min text not null default 'ACTION_REQUIRED',
    muted_provider_keys jsonb not null default '[]'::jsonb,
    muted_pay_batch_ids jsonb not null default '[]'::jsonb,
    snoozed_until_utc timestamptz null,
    created_at_utc timestamptz not null default now(),
    updated_at_utc timestamptz not null default now()
);

alter table public.banking_alert_user_preferences
    add column if not exists id uuid default gen_random_uuid();

alter table public.banking_alert_user_preferences
    add column if not exists user_id uuid;

alter table public.banking_alert_user_preferences
    add column if not exists enabled boolean default true;

alter table public.banking_alert_user_preferences
    add column if not exists alert_kind_allowlist jsonb;

alter table public.banking_alert_user_preferences
    add column if not exists alert_kind_blocklist jsonb default '[]'::jsonb;

alter table public.banking_alert_user_preferences
    add column if not exists failure_reason_allowlist jsonb;

alter table public.banking_alert_user_preferences
    add column if not exists failure_reason_blocklist jsonb default '[]'::jsonb;

alter table public.banking_alert_user_preferences
    add column if not exists include_action_required boolean default true;

alter table public.banking_alert_user_preferences
    add column if not exists include_progress_alerts boolean default true;

alter table public.banking_alert_user_preferences
    add column if not exists include_informational_alerts boolean default false;

alter table public.banking_alert_user_preferences
    add column if not exists include_success_alerts boolean default false;

alter table public.banking_alert_user_preferences
    add column if not exists severity_min text default 'ACTION_REQUIRED';

alter table public.banking_alert_user_preferences
    add column if not exists muted_provider_keys jsonb default '[]'::jsonb;

alter table public.banking_alert_user_preferences
    add column if not exists muted_pay_batch_ids jsonb default '[]'::jsonb;

alter table public.banking_alert_user_preferences
    add column if not exists snoozed_until_utc timestamptz;

alter table public.banking_alert_user_preferences
    add column if not exists created_at_utc timestamptz default now();

alter table public.banking_alert_user_preferences
    add column if not exists updated_at_utc timestamptz default now();

update public.banking_alert_user_preferences
set
    enabled = coalesce(enabled, true),
    alert_kind_blocklist = coalesce(alert_kind_blocklist, '[]'::jsonb),
    failure_reason_blocklist = coalesce(failure_reason_blocklist, '[]'::jsonb),
    include_action_required = coalesce(include_action_required, true),
    include_progress_alerts = coalesce(include_progress_alerts, true),
    include_informational_alerts = coalesce(include_informational_alerts, false),
    include_success_alerts = coalesce(include_success_alerts, false),
    severity_min = coalesce(severity_min, 'ACTION_REQUIRED'),
    muted_provider_keys = coalesce(muted_provider_keys, '[]'::jsonb),
    muted_pay_batch_ids = coalesce(muted_pay_batch_ids, '[]'::jsonb),
    created_at_utc = coalesce(created_at_utc, now()),
    updated_at_utc = coalesce(updated_at_utc, now());

alter table public.banking_alert_user_preferences
    alter column id set default gen_random_uuid(),
    alter column user_id set not null,
    alter column enabled set default true,
    alter column enabled set not null,
    alter column alert_kind_blocklist set default '[]'::jsonb,
    alter column alert_kind_blocklist set not null,
    alter column failure_reason_blocklist set default '[]'::jsonb,
    alter column failure_reason_blocklist set not null,
    alter column include_action_required set default true,
    alter column include_action_required set not null,
    alter column include_progress_alerts set default true,
    alter column include_progress_alerts set not null,
    alter column include_informational_alerts set default false,
    alter column include_informational_alerts set not null,
    alter column include_success_alerts set default false,
    alter column include_success_alerts set not null,
    alter column severity_min set default 'ACTION_REQUIRED',
    alter column severity_min set not null,
    alter column muted_provider_keys set default '[]'::jsonb,
    alter column muted_provider_keys set not null,
    alter column muted_pay_batch_ids set default '[]'::jsonb,
    alter column muted_pay_batch_ids set not null,
    alter column created_at_utc set default now(),
    alter column created_at_utc set not null,
    alter column updated_at_utc set default now(),
    alter column updated_at_utc set not null;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_alert_user_preferences'::regclass
          and conname = 'banking_alert_user_preferences_user_fkey'
    ) then
        alter table public.banking_alert_user_preferences
            add constraint banking_alert_user_preferences_user_fkey
            foreign key (user_id)
            references auth.users(id)
            on delete cascade;
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_alert_user_preferences'::regclass
          and conname = 'banking_alert_user_preferences_severity_chk'
    ) then
        alter table public.banking_alert_user_preferences
            add constraint banking_alert_user_preferences_severity_chk
            check (severity_min in ('INFO','PROGRESS','ACTION_REQUIRED','CRITICAL'));
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_alert_user_preferences'::regclass
          and conname = 'banking_alert_user_preferences_alert_allowlist_array_chk'
    ) then
        alter table public.banking_alert_user_preferences
            add constraint banking_alert_user_preferences_alert_allowlist_array_chk
            check (alert_kind_allowlist is null or jsonb_typeof(alert_kind_allowlist) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_alert_user_preferences'::regclass
          and conname = 'banking_alert_user_preferences_alert_blocklist_array_chk'
    ) then
        alter table public.banking_alert_user_preferences
            add constraint banking_alert_user_preferences_alert_blocklist_array_chk
            check (jsonb_typeof(alert_kind_blocklist) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_alert_user_preferences'::regclass
          and conname = 'banking_alert_user_preferences_reason_allowlist_array_chk'
    ) then
        alter table public.banking_alert_user_preferences
            add constraint banking_alert_user_preferences_reason_allowlist_array_chk
            check (failure_reason_allowlist is null or jsonb_typeof(failure_reason_allowlist) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_alert_user_preferences'::regclass
          and conname = 'banking_alert_user_preferences_reason_blocklist_array_chk'
    ) then
        alter table public.banking_alert_user_preferences
            add constraint banking_alert_user_preferences_reason_blocklist_array_chk
            check (jsonb_typeof(failure_reason_blocklist) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_alert_user_preferences'::regclass
          and conname = 'banking_alert_user_preferences_muted_providers_array_chk'
    ) then
        alter table public.banking_alert_user_preferences
            add constraint banking_alert_user_preferences_muted_providers_array_chk
            check (jsonb_typeof(muted_provider_keys) = 'array');
    end if;

    if not exists (
        select 1
        from pg_constraint
        where conrelid = 'public.banking_alert_user_preferences'::regclass
          and conname = 'banking_alert_user_preferences_muted_batches_array_chk'
    ) then
        alter table public.banking_alert_user_preferences
            add constraint banking_alert_user_preferences_muted_batches_array_chk
            check (jsonb_typeof(muted_pay_batch_ids) = 'array');
    end if;
end $$;

create unique index if not exists banking_alert_user_preferences_user_uidx
    on public.banking_alert_user_preferences (user_id);

create index if not exists banking_alert_user_preferences_enabled_idx
    on public.banking_alert_user_preferences (enabled);

create index if not exists banking_alert_user_preferences_snoozed_idx
    on public.banking_alert_user_preferences (snoozed_until_utc)
    where snoozed_until_utc is not null;

commit;
