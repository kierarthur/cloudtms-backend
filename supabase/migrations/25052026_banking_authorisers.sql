/*
  CloudTMS Banking — Payment authorisers + golden key + auth tables (DB only)
  SAFE TO RERUN (idempotent)

  Changes:
  - public.tms_users: add payment_authoriser, payment_golden_key + CHECK golden=>authoriser
  - public.settings_defaults: add payment_authoriser_quantity, remittance_test_recipient_email + CHECK range
  - public.pay_batches: extend CHECK pay_batches_status_chk_v2 to allow AWAITING_AUTHORISATION + AUTHORISED_FOR_PAYMENT
  - create tables:
      public.pay_batch_auth_requests
      public.pay_batch_auth_actions
      public.pay_batch_auth_tokens
*/

-- ─────────────────────────────────────────────────────────────
-- 1) tms_users flags
-- ─────────────────────────────────────────────────────────────

alter table public.tms_users
  add column if not exists payment_authoriser boolean not null default false;

alter table public.tms_users
  add column if not exists payment_golden_key boolean not null default false;

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    where c.conname = 'tms_users_payment_golden_implies_authoriser_chk'
      and c.conrelid = 'public.tms_users'::regclass
  ) then
    alter table public.tms_users
      add constraint tms_users_payment_golden_implies_authoriser_chk
      check (payment_golden_key = false or payment_authoriser = true);
  end if;
end$$;

-- ─────────────────────────────────────────────────────────────
-- 2) settings_defaults fields
-- ─────────────────────────────────────────────────────────────

alter table public.settings_defaults
  add column if not exists payment_authoriser_quantity integer not null default 1;

alter table public.settings_defaults
  add column if not exists remittance_test_recipient_email text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    where c.conname = 'settings_defaults_payment_authoriser_quantity_range_chk'
      and c.conrelid = 'public.settings_defaults'::regclass
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_payment_authoriser_quantity_range_chk
      check (payment_authoriser_quantity >= 1 and payment_authoriser_quantity <= 10);
  end if;
end$$;

-- ─────────────────────────────────────────────────────────────
-- 3) New auth tables
-- ─────────────────────────────────────────────────────────────

create table if not exists public.pay_batch_auth_requests (
  id uuid primary key default gen_random_uuid(),
  pay_batch_id uuid not null references public.pay_batches(id) on delete cascade,
  requested_by_user_id uuid not null references public.tms_users(id) on delete restrict,

  required_quantity integer not null,

  -- staged schedule fields
  schedule_kind text not null,                 -- IMMEDIATE / SCHEDULED (validated at RPC layer)
  scheduled_at_utc timestamptz not null,       -- for IMMEDIATE, RPC can set now()
  funding_account_ref text not null,
  funds_warning_hours_json jsonb null,

  state text not null default 'AWAITING',      -- AWAITING / AUTHORISED / REJECTED / CANCELLED

  golden_key_used boolean not null default false,
  golden_key_user_id uuid null references public.tms_users(id) on delete restrict,

  created_at_utc timestamptz not null default now(),
  finalised_at_utc timestamptz null,
  finalised_by_user_id uuid null references public.tms_users(id) on delete restrict
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    where c.conname = 'pay_batch_auth_requests_state_chk'
      and c.conrelid = 'public.pay_batch_auth_requests'::regclass
  ) then
    alter table public.pay_batch_auth_requests
      add constraint pay_batch_auth_requests_state_chk
      check (state in ('AWAITING','AUTHORISED','REJECTED','CANCELLED'));
  end if;
end$$;

-- One active (AWAITING) request per batch
create unique index if not exists ux_pay_batch_auth_requests_one_active
  on public.pay_batch_auth_requests(pay_batch_id)
  where state = 'AWAITING';

create index if not exists idx_pay_batch_auth_requests_pay_batch_id
  on public.pay_batch_auth_requests(pay_batch_id);

create index if not exists idx_pay_batch_auth_requests_requested_by
  on public.pay_batch_auth_requests(requested_by_user_id);

create table if not exists public.pay_batch_auth_actions (
  id uuid primary key default gen_random_uuid(),
  auth_request_id uuid not null references public.pay_batch_auth_requests(id) on delete cascade,
  pay_batch_id uuid not null references public.pay_batches(id) on delete cascade,
  actor_user_id uuid not null references public.tms_users(id) on delete restrict,

  action text not null,                        -- AUTHORISE / USE_GOLDEN_KEY / REJECT
  action_at_utc timestamptz not null default now(),
  note text null,

  constraint ux_pay_batch_auth_actions_one_per_user unique (auth_request_id, actor_user_id)
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    where c.conname = 'pay_batch_auth_actions_action_chk'
      and c.conrelid = 'public.pay_batch_auth_actions'::regclass
  ) then
    alter table public.pay_batch_auth_actions
      add constraint pay_batch_auth_actions_action_chk
      check (action in ('AUTHORISE','USE_GOLDEN_KEY','REJECT'));
  end if;
end$$;

create index if not exists idx_pay_batch_auth_actions_auth_request_id
  on public.pay_batch_auth_actions(auth_request_id);

create index if not exists idx_pay_batch_auth_actions_pay_batch_id
  on public.pay_batch_auth_actions(pay_batch_id);

create index if not exists idx_pay_batch_auth_actions_actor_user_id
  on public.pay_batch_auth_actions(actor_user_id);

create table if not exists public.pay_batch_auth_tokens (
  token text primary key,
  auth_request_id uuid not null references public.pay_batch_auth_requests(id) on delete cascade,
  target_user_id uuid not null references public.tms_users(id) on delete restrict,

  expires_at_utc timestamptz not null,
  used_at_utc timestamptz null,
  created_at_utc timestamptz not null default now(),

  constraint ux_pay_batch_auth_tokens_one_per_target unique (auth_request_id, target_user_id)
);

create index if not exists idx_pay_batch_auth_tokens_auth_request_id
  on public.pay_batch_auth_tokens(auth_request_id);

create index if not exists idx_pay_batch_auth_tokens_target_user_id
  on public.pay_batch_auth_tokens(target_user_id);

-- ─────────────────────────────────────────────────────────────
-- 4) Extend pay_batches status CHECK to allow new statuses
--    We preserve the existing CHECK definition and OR in the new values.
-- ─────────────────────────────────────────────────────────────

do $$
declare
  v_oid oid;
  v_def text;
  v_expr text;
  v_new text;
begin
  select c.oid, pg_get_constraintdef(c.oid, true)
    into v_oid, v_def
  from pg_constraint c
  where c.conname = 'pay_batches_status_chk_v2'
    and c.conrelid = 'public.pay_batches'::regclass
    and c.contype = 'c'
  limit 1;

  if v_oid is null then
    raise exception 'pay_batches_status_chk_v2 not found on public.pay_batches';
  end if;

  -- Only patch if the new statuses are not already allowed.
  if position('AWAITING_AUTHORISATION' in v_def) = 0
     or position('AUTHORISED_FOR_PAYMENT' in v_def) = 0
  then
    -- Strip leading "CHECK (" and the final ")" (one level) to keep the original expression intact.
    v_expr := regexp_replace(v_def, '^CHECK\s*\(', '');
    v_expr := regexp_replace(v_expr, '\)\s*$', '');

    v_new := format(
      '(%s) OR (status::text = ANY (ARRAY[%L,%L]::text[]))',
      v_expr,
      'AWAITING_AUTHORISATION',
      'AUTHORISED_FOR_PAYMENT'
    );

    execute 'alter table public.pay_batches drop constraint pay_batches_status_chk_v2';
    execute 'alter table public.pay_batches add constraint pay_batches_status_chk_v2 check (' || v_new || ')';
  end if;
end$$;
