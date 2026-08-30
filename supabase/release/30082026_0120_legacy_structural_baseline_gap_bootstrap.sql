-- One-time LEGACY_UPGRADE bridge for four structural-baseline tables and one
-- settings column that have no historical repository migration. Safe to rerun. The protected
-- release applies this only after validating the legacy migration ledger and
-- before replaying the remaining migrations. No application row is inserted,
-- updated or deleted here.

create table if not exists public.import_apply_operations (
  id uuid not null default gen_random_uuid(),
  import_id uuid not null,
  source_system public.hr_source_enum not null,
  import_revision text not null,
  request_hash text not null,
  actor_user_id uuid not null,
  state text not null default 'PREPARED'::text,
  response_json jsonb not null default '{}'::jsonb,
  committed_at_utc timestamp with time zone,
  financialised_at_utc timestamp with time zone,
  finalised_at_utc timestamp with time zone,
  created_at_utc timestamp with time zone not null default now(),
  updated_at_utc timestamp with time zone not null default now(),
  constraint import_apply_operations_pkey primary key (id),
  constraint uq_import_apply_operations_request unique (import_id, import_revision, request_hash),
  constraint import_apply_operations_import_revision_chk check (btrim(import_revision) <> ''::text),
  constraint import_apply_operations_request_hash_chk check (btrim(request_hash) <> ''::text),
  constraint import_apply_operations_response_json_chk check (jsonb_typeof(response_json) = 'object'::text),
  constraint import_apply_operations_state_chk check (
    state = any (array[
      'PREPARED'::text,
      'SOURCE_COMMITTED_TSFIN_PENDING'::text,
      'FINANCIALISED_PENDING_FINALISATION'::text,
      'COMPLETE'::text,
      'BLOCKED'::text,
      'FAILED_BEFORE_COMMIT'::text
    ])
  ),
  constraint import_apply_operations_actor_fk foreign key (actor_user_id)
    references public.tms_users(id) on delete restrict,
  constraint import_apply_operations_import_fk foreign key (import_id)
    references public.hr_imports(id) on delete restrict
);

create index if not exists idx_import_apply_operations_import_created
  on public.import_apply_operations using btree (import_id, created_at_utc desc, id);
create index if not exists idx_import_apply_operations_pending
  on public.import_apply_operations using btree (state, updated_at_utc, id)
  where state = any (array[
    'PREPARED'::text,
    'SOURCE_COMMITTED_TSFIN_PENDING'::text,
    'FINANCIALISED_PENDING_FINALISATION'::text
  ]);
alter table public.import_apply_operations enable row level security;
revoke all privileges on table public.import_apply_operations from public, anon, authenticated, service_role;
grant all privileges on table public.import_apply_operations to current_user, service_role;

create table if not exists public.pay_manual_adjustment_carry_forwards (
  id uuid not null default gen_random_uuid(),
  source_pay_batch_id uuid not null,
  source_pay_batch_item_id uuid not null,
  source_pay_bank_transfer_id uuid,
  source_pay_batch_candidate_id uuid,
  source_correction_request_id uuid,
  source_correction_work_item_id uuid,
  candidate_id uuid not null,
  umbrella_id uuid,
  client_id uuid,
  timesheet_id uuid,
  pay_channel text not null,
  adjustment_direction text not null,
  amount_ex_vat numeric(12,2),
  amount_vat numeric(12,2),
  amount_inc_vat numeric(12,2) not null,
  amount_basis text,
  paye_treatment text,
  tax_treatment_json jsonb not null default '{}'::jsonb,
  description text not null,
  reason text,
  source_ref text,
  source_operation_source_key text,
  source_snapshot_json jsonb not null default '{}'::jsonb,
  status text not null default 'PENDING_CARRY_FORWARD'::text,
  target_pay_batch_id uuid,
  target_pay_batch_item_id uuid,
  target_operation_source_key text,
  created_by_user_id uuid,
  created_at_utc timestamp with time zone not null default now(),
  updated_at_utc timestamp with time zone not null default now(),
  reserved_at_utc timestamp with time zone,
  consumed_at_utc timestamp with time zone,
  released_at_utc timestamp with time zone,
  cancelled_at_utc timestamp with time zone,
  status_reason text,
  constraint pay_manual_adjustment_carry_forwards_pkey primary key (id),
  constraint pay_manual_adjustment_carry_forwards_amount_nonzero_chk check (amount_inc_vat <> 0::numeric),
  constraint pay_manual_adjustment_carry_forwards_direction_chk check (
    adjustment_direction = any (array['CREDIT'::text, 'DEBIT'::text])
  ),
  constraint pay_manual_adjustment_carry_forwards_signed_direction_chk check (
    (amount_inc_vat > 0::numeric and adjustment_direction = 'CREDIT'::text)
    or (amount_inc_vat < 0::numeric and adjustment_direction = 'DEBIT'::text)
  ),
  constraint pay_manual_adjustment_carry_forwards_status_chk check (
    status = any (array[
      'PENDING_CARRY_FORWARD'::text,
      'RESERVED_IN_DRAFT'::text,
      'CONSUMED_IN_BATCH'::text,
      'CANCELLED'::text,
      'SUPERSEDED'::text,
      'NEEDS_REVIEW'::text
    ])
  ),
  constraint pay_manual_adjustment_carry_f_source_pay_batch_candidate_i_fkey
    foreign key (source_pay_batch_candidate_id) references public.pay_batch_candidates(id),
  constraint pay_manual_adjustment_carry_fo_source_pay_bank_transfer_id_fkey
    foreign key (source_pay_bank_transfer_id) references public.pay_bank_transfers(id),
  constraint pay_manual_adjustment_carry_forwa_source_pay_batch_item_id_fkey
    foreign key (source_pay_batch_item_id) references public.pay_batch_items(id),
  constraint pay_manual_adjustment_carry_forwa_target_pay_batch_item_id_fkey
    foreign key (target_pay_batch_item_id) references public.pay_batch_items(id),
  constraint pay_manual_adjustment_carry_forwards_candidate_fkey
    foreign key (candidate_id) references public.candidates(id),
  constraint pay_manual_adjustment_carry_forwards_candidate_id_fkey
    foreign key (candidate_id) references public.candidates(id),
  constraint pay_manual_adjustment_carry_forwards_client_fkey
    foreign key (client_id) references public.clients(id),
  constraint pay_manual_adjustment_carry_forwards_client_id_fkey
    foreign key (client_id) references public.clients(id),
  constraint pay_manual_adjustment_carry_forwards_created_by_fkey
    foreign key (created_by_user_id) references auth.users(id),
  constraint pay_manual_adjustment_carry_forwards_created_by_user_id_fkey
    foreign key (created_by_user_id) references auth.users(id),
  constraint pay_manual_adjustment_carry_forwards_source_batch_fkey
    foreign key (source_pay_batch_id) references public.pay_batches(id),
  constraint pay_manual_adjustment_carry_forwards_source_candidate_fkey
    foreign key (source_pay_batch_candidate_id) references public.pay_batch_candidates(id),
  constraint pay_manual_adjustment_carry_forwards_source_item_fkey
    foreign key (source_pay_batch_item_id) references public.pay_batch_items(id),
  constraint pay_manual_adjustment_carry_forwards_source_pay_batch_id_fkey
    foreign key (source_pay_batch_id) references public.pay_batches(id),
  constraint pay_manual_adjustment_carry_forwards_source_transfer_fkey
    foreign key (source_pay_bank_transfer_id) references public.pay_bank_transfers(id),
  constraint pay_manual_adjustment_carry_forwards_target_batch_fkey
    foreign key (target_pay_batch_id) references public.pay_batches(id),
  constraint pay_manual_adjustment_carry_forwards_target_item_fkey
    foreign key (target_pay_batch_item_id) references public.pay_batch_items(id),
  constraint pay_manual_adjustment_carry_forwards_target_pay_batch_id_fkey
    foreign key (target_pay_batch_id) references public.pay_batches(id),
  constraint pay_manual_adjustment_carry_forwards_timesheet_fkey
    foreign key (timesheet_id) references public.timesheets(timesheet_id),
  constraint pay_manual_adjustment_carry_forwards_timesheet_id_fkey
    foreign key (timesheet_id) references public.timesheets(timesheet_id),
  constraint pay_manual_adjustment_carry_forwards_umbrella_fkey
    foreign key (umbrella_id) references public.umbrellas(id),
  constraint pay_manual_adjustment_carry_forwards_umbrella_id_fkey
    foreign key (umbrella_id) references public.umbrellas(id)
);

create index if not exists idx_pay_manual_adjustment_carry_forwards_candidate_status_chann
  on public.pay_manual_adjustment_carry_forwards using btree (candidate_id, status, pay_channel);
create index if not exists idx_pay_manual_adjustment_carry_forwards_source_batch_status
  on public.pay_manual_adjustment_carry_forwards using btree (source_pay_batch_id, status);
create index if not exists idx_pay_manual_adjustment_carry_forwards_source_request
  on public.pay_manual_adjustment_carry_forwards using btree (source_correction_request_id)
  where source_correction_request_id is not null;
create index if not exists idx_pay_manual_adjustment_carry_forwards_source_transfer
  on public.pay_manual_adjustment_carry_forwards using btree (source_pay_bank_transfer_id)
  where source_pay_bank_transfer_id is not null;
create index if not exists idx_pay_manual_adjustment_carry_forwards_status
  on public.pay_manual_adjustment_carry_forwards using btree (status);
create index if not exists idx_pay_manual_adjustment_carry_forwards_target_batch_status
  on public.pay_manual_adjustment_carry_forwards using btree (target_pay_batch_id, status);
create index if not exists idx_pay_manual_adjustment_carry_forwards_target_operation_key
  on public.pay_manual_adjustment_carry_forwards using btree (target_operation_source_key)
  where target_operation_source_key is not null;
create unique index if not exists ux_pay_manual_adjustment_carry_forwards_source_item
  on public.pay_manual_adjustment_carry_forwards using btree (source_pay_batch_item_id);
create unique index if not exists ux_pay_manual_adjustment_carry_forwards_target_item
  on public.pay_manual_adjustment_carry_forwards using btree (target_pay_batch_item_id)
  where target_pay_batch_item_id is not null;
alter table public.pay_manual_adjustment_carry_forwards enable row level security;
revoke all privileges on table public.pay_manual_adjustment_carry_forwards from public, anon, authenticated, service_role;
grant all privileges on table public.pay_manual_adjustment_carry_forwards to current_user, service_role;

create table if not exists public.schema_repeatables (
  filename text not null,
  content_sha256 text not null,
  applied_at timestamp with time zone not null default now(),
  constraint schema_repeatables_pkey primary key (filename)
);
alter table public.schema_repeatables enable row level security;
revoke all privileges on table public.schema_repeatables from public, anon, authenticated, service_role;
grant all privileges on table public.schema_repeatables to current_user, service_role;

-- Historical LIVE records the repository smoke migration as installed, but its
-- disposable smoke relation is absent. Recreate only its schema (never its two
-- historical TEST rows) so the current data-free contract and provider-owner ACL
-- authority can be reproduced without changing business data.
do $legacy_smoke_relation_bootstrap$
begin
  if to_regclass('public.migration_smoke_once_only') is null
     and to_regclass('public.migration_smoke_once_only_id_seq') is null then
    create sequence public.migration_smoke_once_only_id_seq
      as bigint increment by 1 minvalue 1 maxvalue 9223372036854775807 start with 1 cache 1 no cycle;
    create table public.migration_smoke_once_only (
      id bigint not null default nextval('public.migration_smoke_once_only_id_seq'::regclass),
      created_at timestamp with time zone not null default now(),
      constraint migration_smoke_once_only_pkey primary key (id)
    );
    alter sequence public.migration_smoke_once_only_id_seq
      owned by public.migration_smoke_once_only.id;
  elsif to_regclass('public.migration_smoke_once_only') is null
        or to_regclass('public.migration_smoke_once_only_id_seq') is null then
    raise exception 'LEGACY_UPGRADE migration smoke relation is partially present';
  end if;
end
$legacy_smoke_relation_bootstrap$;

alter table public.settings_defaults
  add column if not exists temp_log boolean not null default false;

do $legacy_structural_gap_verify$
declare
  v_missing text[];
begin
  select array_agg(required_relation order by required_relation)
  into v_missing
  from unnest(array[
    'public.import_apply_operations',
    'public.migration_smoke_once_only',
    'public.migration_smoke_once_only_id_seq',
    'public.pay_manual_adjustment_carry_forwards',
    'public.schema_repeatables'
  ]) as expected(required_relation)
  where to_regclass(required_relation) is null;

  if coalesce(cardinality(v_missing), 0) > 0 then
    raise exception 'LEGACY_UPGRADE structural gap bootstrap is incomplete: %', v_missing;
  end if;

  if not exists (
    select 1
    from pg_catalog.pg_attribute a
    join pg_catalog.pg_class c on c.oid = a.attrelid
    join pg_catalog.pg_namespace n on n.oid = c.relnamespace
    left join pg_catalog.pg_attrdef d
      on d.adrelid = a.attrelid
     and d.adnum = a.attnum
    where n.nspname = 'public'
      and c.relname = 'settings_defaults'
      and a.attname = 'temp_log'
      and not a.attisdropped
      and a.atttypid = 'boolean'::regtype
      and a.attnotnull
      and pg_catalog.pg_get_expr(d.adbin, d.adrelid) = 'false'
  ) then
    raise exception 'LEGACY_UPGRADE settings_defaults.temp_log definition mismatch';
  end if;

  if not exists (
    select 1
    from pg_catalog.pg_attribute a
    join pg_catalog.pg_class c on c.oid=a.attrelid
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    left join pg_catalog.pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum
    where n.nspname='public'
      and c.relname='migration_smoke_once_only'
      and a.attname='id'
      and not a.attisdropped
      and a.atttypid='bigint'::regtype
      and a.attnotnull
      and pg_catalog.pg_get_expr(d.adbin,d.adrelid)
        = 'nextval(''migration_smoke_once_only_id_seq''::regclass)'
  ) or not exists (
    select 1
    from pg_catalog.pg_constraint con
    where con.conrelid='public.migration_smoke_once_only'::regclass
      and con.contype='p'
      and con.conname='migration_smoke_once_only_pkey'
  ) then
    raise exception 'LEGACY_UPGRADE migration smoke relation definition mismatch';
  end if;
end
$legacy_structural_gap_verify$;
