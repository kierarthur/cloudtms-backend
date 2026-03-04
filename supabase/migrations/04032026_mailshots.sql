-- ============================================================================
-- CloudTMS — Mailshots / Document Templates / Unified Outbox (DB migration)
-- Safe to re-run (idempotent)
-- ============================================================================

-- 0) Required extension for gen_random_uuid()
create extension if not exists pgcrypto;

-- ============================================================================
-- A1) client_settings: add opt-in/out (NOT NULL, default true, backfill true)
-- ============================================================================

alter table public.client_settings
  add column if not exists opt_in_email boolean;
alter table public.client_settings
  alter column opt_in_email set default true;
update public.client_settings set opt_in_email = true where opt_in_email is null;
alter table public.client_settings
  alter column opt_in_email set not null;

alter table public.client_settings
  add column if not exists opt_in_sms boolean;
alter table public.client_settings
  alter column opt_in_sms set default true;
update public.client_settings set opt_in_sms = true where opt_in_sms is null;
alter table public.client_settings
  alter column opt_in_sms set not null;

alter table public.client_settings
  add column if not exists opt_in_whatsapp boolean;
alter table public.client_settings
  alter column opt_in_whatsapp set default true;
update public.client_settings set opt_in_whatsapp = true where opt_in_whatsapp is null;
alter table public.client_settings
  alter column opt_in_whatsapp set not null;

-- ============================================================================
-- A8) settings_defaults: add comms_adaptors_json (NOT NULL {} and object CHECK)
-- ============================================================================

alter table public.settings_defaults
  add column if not exists comms_adaptors_json jsonb;

alter table public.settings_defaults
  alter column comms_adaptors_json set default '{}'::jsonb;

update public.settings_defaults
  set comms_adaptors_json = '{}'::jsonb
  where comms_adaptors_json is null;

alter table public.settings_defaults
  alter column comms_adaptors_json set not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'settings_defaults'
      and c.conname = 'settings_defaults_comms_adaptors_json_is_object_chk'
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_comms_adaptors_json_is_object_chk
      check (jsonb_typeof(comms_adaptors_json) = 'object');
  end if;
end $$;

-- ============================================================================
-- A4) document_templates (template storage)
-- ============================================================================

create table if not exists public.document_templates (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null,
  output_type text not null,
  filename text not null,
  description text null,
  email_type text null,
  selected_field_keys text[] not null default '{}'::text[],
  template_content_json jsonb not null default '{}'::jsonb,
  created_by uuid null,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create index if not exists idx_document_templates_entity_output
  on public.document_templates(entity_type, output_type);

create index if not exists idx_document_templates_entity_output_filename
  on public.document_templates(entity_type, output_type, filename);

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='document_templates'
      and c.conname='document_templates_created_by_fkey'
  ) then
    alter table public.document_templates
      add constraint document_templates_created_by_fkey
      foreign key (created_by) references public.tms_users(id);
  end if;
end $$;

-- ============================================================================
-- A6) mailshot_runs (group a send operation)
-- ============================================================================

create table if not exists public.mailshot_runs (
  id uuid primary key default gen_random_uuid(),
  context_kind text not null,
  output_type text not null,
  document_template_id uuid null,
  created_by uuid null,
  created_at_utc timestamptz not null default now(),
  selection_json jsonb not null,
  result_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_mailshot_runs_created_at
  on public.mailshot_runs(created_at_utc desc);

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='mailshot_runs'
      and c.conname='mailshot_runs_document_template_id_fkey'
  ) then
    alter table public.mailshot_runs
      add constraint mailshot_runs_document_template_id_fkey
      foreign key (document_template_id) references public.document_templates(id);
  end if;

  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='mailshot_runs'
      and c.conname='mailshot_runs_created_by_fkey'
  ) then
    alter table public.mailshot_runs
      add constraint mailshot_runs_created_by_fkey
      foreign key (created_by) references public.tms_users(id);
  end if;
end $$;

-- ============================================================================
-- A5) mailshot_fields + mailshot_field_overrides (Fields Manager)
-- ============================================================================

create table if not exists public.mailshot_fields (
  id uuid primary key default gen_random_uuid(),
  field_key text not null unique,
  label_default text not null,
  enabled_global boolean not null default true,
  allowed_entity_types text[] not null default '{}'::text[],
  resolver_spec_json jsonb not null default '{}'::jsonb,
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create index if not exists idx_mailshot_fields_enabled_global
  on public.mailshot_fields(enabled_global);

create table if not exists public.mailshot_field_overrides (
  id uuid primary key default gen_random_uuid(),
  field_id uuid not null,
  entity_type text not null,
  label_override text null,
  enabled_local boolean not null default true
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='mailshot_field_overrides'
      and c.conname='mailshot_field_overrides_field_id_fkey'
  ) then
    alter table public.mailshot_field_overrides
      add constraint mailshot_field_overrides_field_id_fkey
      foreign key (field_id) references public.mailshot_fields(id)
      on delete cascade;
  end if;

  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='mailshot_field_overrides'
      and c.conname='mailshot_field_overrides_field_id_entity_type_key'
  ) then
    alter table public.mailshot_field_overrides
      add constraint mailshot_field_overrides_field_id_entity_type_key
      unique (field_id, entity_type);
  end if;
end $$;

create index if not exists idx_mailshot_field_overrides_entity_type
  on public.mailshot_field_overrides(entity_type);

-- ============================================================================
-- A2) mail_outbox: add comms-log metadata + email completeness fields
-- ============================================================================

alter table public.mail_outbox
  add column if not exists recipient_kind text;

alter table public.mail_outbox
  add column if not exists recipient_id uuid;

alter table public.mail_outbox
  add column if not exists context_kind text;

alter table public.mail_outbox
  add column if not exists context_id uuid;

alter table public.mail_outbox
  add column if not exists mailshot_run_id uuid;

alter table public.mail_outbox
  add column if not exists document_template_id uuid;

alter table public.mail_outbox
  add column if not exists provider_status text;

alter table public.mail_outbox
  add column if not exists delivered_at timestamptz;

alter table public.mail_outbox
  add column if not exists read_at timestamptz;

alter table public.mail_outbox
  add column if not exists bcc text;

alter table public.mail_outbox
  add column if not exists reply_to text;

alter table public.mail_outbox
  add column if not exists importance text;

alter table public.mail_outbox
  add column if not exists email_type text;

-- optional FKs (safe because columns are nullable)
do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='mail_outbox'
      and c.conname='mail_outbox_mailshot_run_id_fkey'
  ) then
    alter table public.mail_outbox
      add constraint mail_outbox_mailshot_run_id_fkey
      foreign key (mailshot_run_id) references public.mailshot_runs(id);
  end if;

  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='mail_outbox'
      and c.conname='mail_outbox_document_template_id_fkey'
  ) then
    alter table public.mail_outbox
      add constraint mail_outbox_document_template_id_fkey
      foreign key (document_template_id) references public.document_templates(id);
  end if;
end $$;

-- indexes for comms history / filtering
create index if not exists idx_mail_outbox_recipient
  on public.mail_outbox(recipient_kind, recipient_id, created_at_utc desc);

create index if not exists idx_mail_outbox_context
  on public.mail_outbox(context_kind, context_id, created_at_utc desc);

-- Extend mail_outbox_type_check to allow MAILSHOT_EMAIL (safe to re-run)
do $$
declare
  v_def text;
  v_inner text;
  v_new text;
begin
  select pg_get_constraintdef(c.oid, true)
    into v_def
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
  where n.nspname='public'
    and t.relname='mail_outbox'
    and c.conname='mail_outbox_type_check'
  limit 1;

  if v_def is null then
    -- constraint not found: do nothing (schema differs)
    return;
  end if;

  if v_def ilike '%MAILSHOT_EMAIL%' then
    -- already supports it
    return;
  end if;

  -- v_def looks like: CHECK (<expr>)
  v_inner := regexp_replace(v_def, '^CHECK\s*', '');

  v_new := 'CHECK (' || v_inner || ' OR (type = ''MAILSHOT_EMAIL''::text))';

  execute 'alter table public.mail_outbox drop constraint mail_outbox_type_check';
  execute 'alter table public.mail_outbox add constraint mail_outbox_type_check ' || v_new;
end $$;

-- ============================================================================
-- A3) comms_outbox (non-email queue + log)
-- ============================================================================

create table if not exists public.comms_outbox (
  id uuid primary key default gen_random_uuid(),
  channel text not null,
  status text not null,
  to_address text not null,
  message_text text not null,
  provider_key text not null,
  provider_message_id text null,
  provider_payload_json jsonb not null default '{}'::jsonb,
  provider_response_json jsonb not null default '{}'::jsonb,
  last_error text null,
  created_at_utc timestamptz not null default now(),
  sent_at timestamptz null,
  delivered_at timestamptz null,
  read_at timestamptz null,
  failed_at timestamptz null,
  created_by uuid null,
  recipient_kind text null,
  recipient_id uuid null,
  context_kind text null,
  context_id uuid null,
  mailshot_run_id uuid null,
  document_template_id uuid null
);

-- If the table existed but was partial, ensure columns exist (safe)
alter table public.comms_outbox add column if not exists provider_payload_json jsonb;
alter table public.comms_outbox add column if not exists provider_response_json jsonb;
alter table public.comms_outbox add column if not exists mailshot_run_id uuid;
alter table public.comms_outbox add column if not exists document_template_id uuid;

-- Defaults and NOT NULL enforcement for json columns (safe)
alter table public.comms_outbox
  alter column provider_payload_json set default '{}'::jsonb;
update public.comms_outbox
  set provider_payload_json = '{}'::jsonb
  where provider_payload_json is null;
alter table public.comms_outbox
  alter column provider_payload_json set not null;

alter table public.comms_outbox
  alter column provider_response_json set default '{}'::jsonb;
update public.comms_outbox
  set provider_response_json = '{}'::jsonb
  where provider_response_json is null;
alter table public.comms_outbox
  alter column provider_response_json set not null;

-- FKs (safe)
do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='comms_outbox'
      and c.conname='comms_outbox_created_by_fkey'
  ) then
    alter table public.comms_outbox
      add constraint comms_outbox_created_by_fkey
      foreign key (created_by) references public.tms_users(id);
  end if;

  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='comms_outbox'
      and c.conname='comms_outbox_mailshot_run_id_fkey'
  ) then
    alter table public.comms_outbox
      add constraint comms_outbox_mailshot_run_id_fkey
      foreign key (mailshot_run_id) references public.mailshot_runs(id);
  end if;

  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='comms_outbox'
      and c.conname='comms_outbox_document_template_id_fkey'
  ) then
    alter table public.comms_outbox
      add constraint comms_outbox_document_template_id_fkey
      foreign key (document_template_id) references public.document_templates(id);
  end if;
end $$;

-- Basic check constraints for channel + status (optional but recommended)
do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='comms_outbox'
      and c.conname='comms_outbox_channel_chk'
  ) then
    alter table public.comms_outbox
      add constraint comms_outbox_channel_chk
      check (channel in ('WHATSAPP','SMS','VOICE'));
  end if;

  if not exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname='public'
      and t.relname='comms_outbox'
      and c.conname='comms_outbox_status_chk'
  ) then
    alter table public.comms_outbox
      add constraint comms_outbox_status_chk
      check (status in ('QUEUED','SENT','DELIVERED','READ','FAILED'));
  end if;
end $$;

create index if not exists idx_comms_outbox_status_created
  on public.comms_outbox(status, created_at_utc);

create index if not exists idx_comms_outbox_recipient
  on public.comms_outbox(recipient_kind, recipient_id, created_at_utc desc);

create index if not exists idx_comms_outbox_context
  on public.comms_outbox(context_kind, context_id, created_at_utc desc);

create index if not exists idx_comms_outbox_provider_lookup
  on public.comms_outbox(provider_key, provider_message_id);

-- ============================================================================
-- A7) v_outbox_unified (Unified Outbox + Comms tabs)
-- ============================================================================

create or replace view public.v_outbox_unified as
select
  'EMAIL'::text as channel,
  o.id as outbox_id,
  o.type as outbox_type,
  o.status::text as status,
  o.provider_status as delivery_status,
  o.created_at_utc,
  o.sent_at,
  o.delivered_at,
  o.read_at,
  o.failed_at,
  o."to" as to_address,
  o.cc,
  o.bcc,
  o.reply_to,
  o.importance,
  o.email_type,
  o.subject,
  o.body_text,
  o.body_html,
  o.attachments,
  o.reference,
  o.provider_message_id,
  o.last_error,
  o.created_by,
  o.recipient_kind,
  o.recipient_id,
  o.context_kind,
  o.context_id,
  o.mailshot_run_id,
  o.document_template_id
from public.mail_outbox o

union all

select
  c.channel::text as channel,
  c.id as outbox_id,
  null::text as outbox_type,
  c.status::text as status,
  case
    when c.read_at is not null then 'READ'
    when c.delivered_at is not null then 'DELIVERED'
    when c.sent_at is not null then 'SENT'
    when c.failed_at is not null then 'FAILED'
    else null
  end as delivery_status,
  c.created_at_utc,
  c.sent_at,
  c.delivered_at,
  c.read_at,
  c.failed_at,
  c.to_address,
  null::text as cc,
  null::text as bcc,
  null::text as reply_to,
  null::text as importance,
  null::text as email_type,
  null::text as subject,
  c.message_text as body_text,
  null::text as body_html,
  null::jsonb as attachments,
  null::text as reference,
  c.provider_message_id,
  c.last_error,
  c.created_by,
  c.recipient_kind,
  c.recipient_id,
  c.context_kind,
  c.context_id,
  c.mailshot_run_id,
  c.document_template_id
from public.comms_outbox c;
