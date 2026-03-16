begin;

-- =========================================================
-- 1) Canonical finance-case enum
-- =========================================================
do $$
begin
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public'
      and t.typname = 'pay_finance_case_type_enum'
  ) then
    create type public.pay_finance_case_type_enum as enum (
      'PAYMENT_ADVANCE',
      'OVERPAYMENT',
      'MANUAL_DEBT_ADJUSTMENT',
      'MANUAL_CREDIT_ADJUSTMENT'
    );
  end if;
end
$$;

-- =========================================================
-- 2) Expand pay_advances into canonical finance-case master
-- =========================================================
alter table public.pay_advances
  add column if not exists case_type public.pay_finance_case_type_enum,
  add column if not exists adjustment_comment text,
  add column if not exists source_original_paid_amount numeric,
  add column if not exists source_corrected_paid_amount numeric,
  add column if not exists minimum_earnings_threshold numeric,
  add column if not exists take_home_floor_override numeric,
  add column if not exists written_off_at_utc timestamptz,
  add column if not exists written_off_by_user_id uuid,
  add column if not exists write_off_reason text,
  add column if not exists cleared_at_utc timestamptz,
  add column if not exists cleared_by_user_id uuid;

create index if not exists idx_pay_advances_case_type_status_candidate
  on public.pay_advances(case_type, status, candidate_id);

create index if not exists idx_pay_advances_case_type_client
  on public.pay_advances(case_type, client_id);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'pay_advances_written_off_by_user_id_fkey'
      and conrelid = 'public.pay_advances'::regclass
  ) then
    alter table public.pay_advances
      add constraint pay_advances_written_off_by_user_id_fkey
      foreign key (written_off_by_user_id)
      references public.tms_users(id);
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'pay_advances_cleared_by_user_id_fkey'
      and conrelid = 'public.pay_advances'::regclass
  ) then
    alter table public.pay_advances
      add constraint pay_advances_cleared_by_user_id_fkey
      foreign key (cleared_by_user_id)
      references public.tms_users(id);
  end if;
end
$$;

-- deterministic backfill
update public.pay_advances pa
set case_type = case
  when pa.advance_kind = 'OVERPAYMENT'::public.pay_advance_kind_enum
       or pa.reason in (
         'OVERPAYMENT'::public.pay_advance_reason_enum,
         'OVERPAY_NHSP'::public.pay_advance_reason_enum,
         'OVERPAY_HR'::public.pay_advance_reason_enum
       )
    then 'OVERPAYMENT'::public.pay_finance_case_type_enum
  when pa.advance_kind = 'LOAN'::public.pay_advance_kind_enum
       or pa.reason in (
         'LOAN'::public.pay_advance_reason_enum,
         'MISSING_SHIFT'::public.pay_advance_reason_enum,
         'MANUAL_ADVANCE'::public.pay_advance_reason_enum
       )
    then 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
  else 'PAYMENT_ADVANCE'::public.pay_finance_case_type_enum
end
where pa.case_type is null;

alter table public.pay_advances
  alter column case_type set not null;

-- =========================================================
-- 3) Finance-case reservation layer
-- =========================================================
create table if not exists public.pay_advance_reservations (
  id uuid primary key default gen_random_uuid(),
  finance_case_id uuid not null,
  pay_batch_id uuid not null,
  pay_batch_candidate_id uuid null,
  pay_batch_item_id uuid null,
  reserved_amount numeric not null,
  repayment_week_start date null,
  status text not null,
  created_at_utc timestamptz not null default now(),
  committed_at_utc timestamptz null,
  settled_at_utc timestamptz null,
  released_at_utc timestamptz null,
  released_reason text null,
  created_by_user_id uuid null,
  updated_by_user_id uuid null
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_advance_reservations_finance_case_id_fkey'
      and conrelid = 'public.pay_advance_reservations'::regclass
  ) then
    alter table public.pay_advance_reservations
      add constraint pay_advance_reservations_finance_case_id_fkey
      foreign key (finance_case_id)
      references public.pay_advances(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_advance_reservations_pay_batch_id_fkey'
      and conrelid = 'public.pay_advance_reservations'::regclass
  ) then
    alter table public.pay_advance_reservations
      add constraint pay_advance_reservations_pay_batch_id_fkey
      foreign key (pay_batch_id)
      references public.pay_batches(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_advance_reservations_pay_batch_candidate_id_fkey'
      and conrelid = 'public.pay_advance_reservations'::regclass
  ) then
    alter table public.pay_advance_reservations
      add constraint pay_advance_reservations_pay_batch_candidate_id_fkey
      foreign key (pay_batch_candidate_id)
      references public.pay_batch_candidates(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_advance_reservations_pay_batch_item_id_fkey'
      and conrelid = 'public.pay_advance_reservations'::regclass
  ) then
    alter table public.pay_advance_reservations
      add constraint pay_advance_reservations_pay_batch_item_id_fkey
      foreign key (pay_batch_item_id)
      references public.pay_batch_items(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_advance_reservations_created_by_user_id_fkey'
      and conrelid = 'public.pay_advance_reservations'::regclass
  ) then
    alter table public.pay_advance_reservations
      add constraint pay_advance_reservations_created_by_user_id_fkey
      foreign key (created_by_user_id)
      references public.tms_users(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_advance_reservations_updated_by_user_id_fkey'
      and conrelid = 'public.pay_advance_reservations'::regclass
  ) then
    alter table public.pay_advance_reservations
      add constraint pay_advance_reservations_updated_by_user_id_fkey
      foreign key (updated_by_user_id)
      references public.tms_users(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_advance_reservations_status_chk'
      and conrelid = 'public.pay_advance_reservations'::regclass
  ) then
    alter table public.pay_advance_reservations
      add constraint pay_advance_reservations_status_chk
      check (status in ('RESERVED','COMMITTED','SETTLED','RELEASED'));
  end if;
end
$$;

create index if not exists idx_pay_advance_reservations_case_status
  on public.pay_advance_reservations(finance_case_id, status);

create index if not exists idx_pay_advance_reservations_batch_status
  on public.pay_advance_reservations(pay_batch_id, status);

create unique index if not exists uq_pay_advance_reservations_pay_batch_item_id
  on public.pay_advance_reservations(pay_batch_item_id)
  where pay_batch_item_id is not null;

-- =========================================================
-- 4) Rich finance event chain
-- =========================================================
create table if not exists public.pay_finance_case_events (
  id uuid primary key default gen_random_uuid(),
  finance_case_id uuid not null,
  event_type text not null,
  event_at_utc timestamptz not null default now(),
  actor_user_id uuid null,
  pay_batch_id uuid null,
  reservation_id uuid null,
  before_json jsonb null,
  after_json jsonb null,
  reason text null,
  note text null
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_finance_case_events_finance_case_id_fkey'
      and conrelid = 'public.pay_finance_case_events'::regclass
  ) then
    alter table public.pay_finance_case_events
      add constraint pay_finance_case_events_finance_case_id_fkey
      foreign key (finance_case_id)
      references public.pay_advances(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_finance_case_events_actor_user_id_fkey'
      and conrelid = 'public.pay_finance_case_events'::regclass
  ) then
    alter table public.pay_finance_case_events
      add constraint pay_finance_case_events_actor_user_id_fkey
      foreign key (actor_user_id)
      references public.tms_users(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_finance_case_events_pay_batch_id_fkey'
      and conrelid = 'public.pay_finance_case_events'::regclass
  ) then
    alter table public.pay_finance_case_events
      add constraint pay_finance_case_events_pay_batch_id_fkey
      foreign key (pay_batch_id)
      references public.pay_batches(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_finance_case_events_reservation_id_fkey'
      and conrelid = 'public.pay_finance_case_events'::regclass
  ) then
    alter table public.pay_finance_case_events
      add constraint pay_finance_case_events_reservation_id_fkey
      foreign key (reservation_id)
      references public.pay_advance_reservations(id);
  end if;
end
$$;

create index if not exists idx_pay_finance_case_events_case_at
  on public.pay_finance_case_events(finance_case_id, event_at_utc desc);

create index if not exists idx_pay_finance_case_events_batch_at
  on public.pay_finance_case_events(pay_batch_id, event_at_utc desc);

-- =========================================================
-- 5) Finance linkage + PAYE treatment on pay_batch_items
-- =========================================================
alter table public.pay_batch_items
  add column if not exists finance_case_id uuid,
  add column if not exists reservation_id uuid,
  add column if not exists paye_treatment text;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_batch_items_finance_case_id_fkey'
      and conrelid = 'public.pay_batch_items'::regclass
  ) then
    alter table public.pay_batch_items
      add constraint pay_batch_items_finance_case_id_fkey
      foreign key (finance_case_id)
      references public.pay_advances(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_batch_items_reservation_id_fkey'
      and conrelid = 'public.pay_batch_items'::regclass
  ) then
    alter table public.pay_batch_items
      add constraint pay_batch_items_reservation_id_fkey
      foreign key (reservation_id)
      references public.pay_advance_reservations(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_batch_items_paye_treatment_chk'
      and conrelid = 'public.pay_batch_items'::regclass
  ) then
    alter table public.pay_batch_items
      add constraint pay_batch_items_paye_treatment_chk
      check (paye_treatment is null or paye_treatment in ('GROSS_ADD','GROSS_DEDUCT','NET_DEDUCT','NONE'));
  end if;
end
$$;

create index if not exists idx_pay_batch_items_finance_case_id
  on public.pay_batch_items(finance_case_id);

create index if not exists idx_pay_batch_items_reservation_id
  on public.pay_batch_items(reservation_id);

create index if not exists idx_pay_batch_items_finance_source_ref
  on public.pay_batch_items(source_ref)
  where source_ref is not null;

-- backfill finance_case_id from existing source_ref='advance:<uuid>'
update public.pay_batch_items pbi
set finance_case_id = pa.id
from public.pay_advances pa
where pbi.finance_case_id is null
  and pbi.source_ref = ('advance:' || pa.id::text);

-- backfill paye_treatment for existing finance rows only
update public.pay_batch_items
set paye_treatment = case
  when item_type = 'OVERPAYMENT_RECOVERY' then 'GROSS_DEDUCT'
  when item_type = 'LOAN_REPAYMENT' then 'NET_DEDUCT'
  when item_type = 'LOAN_PAYOUT' then 'NONE'
  else paye_treatment
end
where paye_treatment is null
  and item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','LOAN_PAYOUT');

-- =========================================================
-- 6) Remittance-send state on pay_batch_candidates
-- =========================================================
alter table public.pay_batch_candidates
  add column if not exists remittance_sent_at_utc timestamptz,
  add column if not exists remittance_sent_by_user_id uuid,
  add column if not exists remittance_trigger_status text,
  add column if not exists last_remittance_error text;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_batch_candidates_remittance_sent_by_user_id_fkey'
      and conrelid = 'public.pay_batch_candidates'::regclass
  ) then
    alter table public.pay_batch_candidates
      add constraint pay_batch_candidates_remittance_sent_by_user_id_fkey
      foreign key (remittance_sent_by_user_id)
      references public.tms_users(id);
  end if;
end
$$;

create index if not exists idx_pay_batch_candidates_remittance_sent_at_utc
  on public.pay_batch_candidates(remittance_sent_at_utc);

-- =========================================================
-- 7) Authoritative payment-date + same-week PAYE override audit on pay_batches
-- =========================================================
alter table public.pay_batches
  add column if not exists authoritative_payment_date date,
  add column if not exists authoritative_payment_date_source text,
  add column if not exists same_week_paye_override_used boolean not null default false,
  add column if not exists same_week_paye_override_reason text,
  add column if not exists same_week_paye_override_verified_at_utc timestamptz,
  add column if not exists same_week_paye_override_verified_by_user_id uuid;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_batches_same_week_paye_override_verified_by_user_id_fkey'
      and conrelid = 'public.pay_batches'::regclass
  ) then
    alter table public.pay_batches
      add constraint pay_batches_same_week_paye_override_verified_by_user_id_fkey
      foreign key (same_week_paye_override_verified_by_user_id)
      references public.tms_users(id);
  end if;
end
$$;

create index if not exists idx_pay_batches_authoritative_payment_date
  on public.pay_batches(authoritative_payment_date);

create index if not exists idx_pay_batches_same_week_paye_override_used
  on public.pay_batches(same_week_paye_override_used);

-- backfill authoritative payment date/source from available current fields
update public.pay_batches pb
set authoritative_payment_date = coalesce(
      pb.authoritative_payment_date,
      pb.monzo_confirmed_at_utc::date,
      pb.scheduled_at_utc::date,
      pb.pay_date
    ),
    authoritative_payment_date_source = coalesce(
      pb.authoritative_payment_date_source,
      case
        when pb.monzo_confirmed_at_utc is not null then 'MONZO_CONFIRMED_AT_UTC'
        when pb.scheduled_at_utc is not null then 'SCHEDULED_AT_UTC'
        else 'PAY_DATE'
      end
    )
where pb.authoritative_payment_date is null
   or pb.authoritative_payment_date_source is null;

-- =========================================================
-- 8) Expand pay_item_snoozes; keep table
-- =========================================================
alter table public.pay_item_snoozes
  add column if not exists updated_at_utc timestamptz,
  add column if not exists updated_by_user_id uuid;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'pay_item_snoozes_updated_by_user_id_fkey'
      and conrelid = 'public.pay_item_snoozes'::regclass
  ) then
    alter table public.pay_item_snoozes
      add constraint pay_item_snoozes_updated_by_user_id_fkey
      foreign key (updated_by_user_id)
      references public.tms_users(id);
  end if;
end
$$;

update public.pay_item_snoozes
set updated_at_utc = coalesce(updated_at_utc, created_at_utc),
    updated_by_user_id = coalesce(updated_by_user_id, created_by_user_id)
where updated_at_utc is null
   or updated_by_user_id is null;

-- clear duplicate ACTIVE snoozes by timesheet identity, keep newest
with ranked as (
  select
    s.id,
    row_number() over (
      partition by s.timesheet_id, coalesce(s.segment_id, '')
      order by s.created_at_utc desc, s.id desc
    ) as rn
  from public.pay_item_snoozes s
  where s.cleared_at_utc is null
    and s.source_ref is null
    and s.timesheet_id is not null
)
update public.pay_item_snoozes s
set cleared_at_utc = coalesce(s.cleared_at_utc, now())
where s.id in (
  select r.id
  from ranked r
  where r.rn > 1
);

-- clear duplicate ACTIVE snoozes by source_ref identity, keep newest
with ranked as (
  select
    s.id,
    row_number() over (
      partition by s.source_ref
      order by s.created_at_utc desc, s.id desc
    ) as rn
  from public.pay_item_snoozes s
  where s.cleared_at_utc is null
    and s.source_ref is not null
)
update public.pay_item_snoozes s
set cleared_at_utc = coalesce(s.cleared_at_utc, now())
where s.id in (
  select r.id
  from ranked r
  where r.rn > 1
);

create unique index if not exists uq_pay_item_snoozes_active_timesheet_identity
  on public.pay_item_snoozes (timesheet_id, coalesce(segment_id, ''))
  where cleared_at_utc is null
    and source_ref is null
    and timesheet_id is not null;

create unique index if not exists uq_pay_item_snoozes_active_source_identity
  on public.pay_item_snoozes (source_ref)
  where cleared_at_utc is null
    and source_ref is not null;

-- =========================================================
-- 9) Timesheet payment overrides
-- =========================================================
create table if not exists public.timesheet_payment_overrides (
  id uuid primary key default gen_random_uuid(),
  timesheet_id uuid not null,
  candidate_id uuid not null,
  override_type text not null default 'ADVANCE_THIS_PAYMENT',
  reason text not null,
  created_at_utc timestamptz not null default now(),
  created_by_user_id uuid null,
  consumed_by_pay_batch_id uuid null,
  consumed_at_utc timestamptz null,
  cleared_at_utc timestamptz null,
  cleared_by_user_id uuid null,
  clear_reason text null
);

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'timesheet_payment_overrides_timesheet_id_fkey'
      and conrelid = 'public.timesheet_payment_overrides'::regclass
  ) then
    alter table public.timesheet_payment_overrides
      add constraint timesheet_payment_overrides_timesheet_id_fkey
      foreign key (timesheet_id)
      references public.timesheets(timesheet_id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'timesheet_payment_overrides_candidate_id_fkey'
      and conrelid = 'public.timesheet_payment_overrides'::regclass
  ) then
    alter table public.timesheet_payment_overrides
      add constraint timesheet_payment_overrides_candidate_id_fkey
      foreign key (candidate_id)
      references public.candidates(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'timesheet_payment_overrides_created_by_user_id_fkey'
      and conrelid = 'public.timesheet_payment_overrides'::regclass
  ) then
    alter table public.timesheet_payment_overrides
      add constraint timesheet_payment_overrides_created_by_user_id_fkey
      foreign key (created_by_user_id)
      references public.tms_users(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'timesheet_payment_overrides_consumed_by_pay_batch_id_fkey'
      and conrelid = 'public.timesheet_payment_overrides'::regclass
  ) then
    alter table public.timesheet_payment_overrides
      add constraint timesheet_payment_overrides_consumed_by_pay_batch_id_fkey
      foreign key (consumed_by_pay_batch_id)
      references public.pay_batches(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'timesheet_payment_overrides_cleared_by_user_id_fkey'
      and conrelid = 'public.timesheet_payment_overrides'::regclass
  ) then
    alter table public.timesheet_payment_overrides
      add constraint timesheet_payment_overrides_cleared_by_user_id_fkey
      foreign key (cleared_by_user_id)
      references public.tms_users(id);
  end if;

  if not exists (
    select 1 from pg_constraint
    where conname = 'timesheet_payment_overrides_override_type_chk'
      and conrelid = 'public.timesheet_payment_overrides'::regclass
  ) then
    alter table public.timesheet_payment_overrides
      add constraint timesheet_payment_overrides_override_type_chk
      check (override_type in ('ADVANCE_THIS_PAYMENT'));
  end if;
end
$$;

create unique index if not exists uq_timesheet_payment_overrides_active_one
  on public.timesheet_payment_overrides(timesheet_id)
  where cleared_at_utc is null
    and consumed_at_utc is null
    and consumed_by_pay_batch_id is null;

create index if not exists idx_timesheet_payment_overrides_candidate_active
  on public.timesheet_payment_overrides(candidate_id, created_at_utc desc);

-- =========================================================
-- 10) Read-optimised finance register view
-- =========================================================
create or replace view public.v_finance_cases_register as
with reservation_rollup as (
  select
    r.finance_case_id,
    round(coalesce(sum(case when r.status in ('RESERVED','COMMITTED') then r.reserved_amount else 0 end), 0), 2) as active_reserved_amount,
    round(coalesce(sum(case when r.status = 'RESERVED' then r.reserved_amount else 0 end), 0), 2) as reserved_amount,
    round(coalesce(sum(case when r.status = 'COMMITTED' then r.reserved_amount else 0 end), 0), 2) as committed_amount,
    round(coalesce(sum(case when r.status = 'SETTLED' then r.reserved_amount else 0 end), 0), 2) as settled_amount,
    round(coalesce(sum(case when r.status = 'RELEASED' then r.reserved_amount else 0 end), 0), 2) as released_amount,
    count(*) filter (where r.status in ('RESERVED','COMMITTED'))::int as active_reservation_count,
    max(r.created_at_utc) as latest_reservation_created_at_utc,
    max(r.committed_at_utc) as latest_committed_at_utc,
    max(r.settled_at_utc) as latest_settled_at_utc,
    max(r.released_at_utc) as latest_released_at_utc
  from public.pay_advance_reservations r
  group by r.finance_case_id
),
latest_remittance as (
  select distinct on (x.finance_case_id)
    x.finance_case_id,
    x.pay_batch_id,
    x.pay_date,
    x.remittance_sent_at_utc,
    x.remittance_trigger_status,
    x.last_remittance_error
  from (
    select
      coalesce(pbi.finance_case_id, pa.id) as finance_case_id,
      pbc.pay_batch_id,
      pb.pay_date,
      pbc.remittance_sent_at_utc,
      pbc.remittance_trigger_status,
      pbc.last_remittance_error
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    join public.pay_batches pb
      on pb.id = pbc.pay_batch_id
    left join public.pay_advances pa
      on pbi.finance_case_id is null
     and pbi.source_ref = ('advance:' || pa.id::text)
    where pbi.finance_case_id is not null
       or (pbi.source_ref is not null and pbi.source_ref like 'advance:%')
  ) x
  where x.finance_case_id is not null
  order by x.finance_case_id, x.remittance_sent_at_utc desc nulls last, x.pay_date desc nulls last, x.pay_batch_id desc
),
latest_recovery_batch as (
  select distinct on (x.finance_case_id)
    x.finance_case_id,
    x.pay_batch_id as latest_recovery_pay_batch_id,
    x.pay_date as latest_recovery_pay_date
  from (
    select
      coalesce(pbi.finance_case_id, pa.id) as finance_case_id,
      pbc.pay_batch_id,
      pb.pay_date
    from public.pay_batch_items pbi
    join public.pay_batch_candidates pbc
      on pbc.id = pbi.pay_batch_candidate_id
    join public.pay_batches pb
      on pb.id = pbc.pay_batch_id
    left join public.pay_advances pa
      on pbi.finance_case_id is null
     and pbi.source_ref = ('advance:' || pa.id::text)
    where pbi.item_type in ('OVERPAYMENT_RECOVERY','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY')
      and (
        pbi.finance_case_id is not null
        or (pbi.source_ref is not null and pbi.source_ref like 'advance:%')
      )
  ) x
  where x.finance_case_id is not null
  order by x.finance_case_id, x.pay_date desc nulls last, x.pay_batch_id desc
),
active_snooze as (
  select distinct on (pa.id)
    pa.id as finance_case_id,
    s.id as snooze_id,
    s.snooze_kind,
    s.snooze_until_date,
    s.note,
    s.created_at_utc,
    s.updated_at_utc,
    s.created_by_user_id,
    s.updated_by_user_id
  from public.pay_advances pa
  join public.pay_item_snoozes s
    on s.source_ref = ('advance:' || pa.id::text)
   and s.cleared_at_utc is null
  order by pa.id, s.updated_at_utc desc nulls last, s.created_at_utc desc, s.id desc
)
select
  pa.id as finance_case_id,
  pa.case_type,
  pa.advance_kind,
  pa.reason,
  pa.candidate_id,
  c.tms_ref as candidate_tms_ref,
  c.display_name as candidate_display_name,
  c.first_name as candidate_first_name,
  c.last_name as candidate_last_name,
  c.pay_method,
  pa.client_id,
  cli.name as client_name,
  pa.linked_timesheet_id,
  pa.linked_shift_date,
  pa.created_at,
  pa.created_by,
  pa.updated_at,
  pa.status,
  pa.payout_status,
  pa.payout_pay_batch_id,
  pa.payout_transfer_id,
  pa.original_amount,
  pa.outstanding_amount,
  pa.weekly_due,
  pa.weeks_total,
  pa.start_week_start,
  pa.next_due_week_start,
  pa.schedule_json,
  pa.adjustment_comment,
  pa.source_original_paid_amount,
  pa.source_corrected_paid_amount,
  pa.minimum_earnings_threshold,
  pa.take_home_floor_override,
  pa.baseline_signature,
  pa.best_guess_hours,
  pa.notes,
  pa.written_off_at_utc,
  pa.written_off_by_user_id,
  pa.write_off_reason,
  pa.cleared_at_utc,
  pa.cleared_by_user_id,
  rr.active_reserved_amount,
  rr.reserved_amount,
  rr.committed_amount,
  rr.settled_amount,
  rr.released_amount,
  rr.active_reservation_count,
  rr.latest_reservation_created_at_utc,
  rr.latest_committed_at_utc,
  rr.latest_settled_at_utc,
  rr.latest_released_at_utc,
  lrb.latest_recovery_pay_batch_id,
  lrb.latest_recovery_pay_date,
  lr.remittance_sent_at_utc as latest_remittance_sent_at_utc,
  lr.remittance_trigger_status as latest_remittance_trigger_status,
  lr.last_remittance_error,
  s.snooze_id as active_snooze_id,
  s.snooze_kind as active_snooze_kind,
  s.snooze_until_date as active_snooze_until_date,
  s.note as active_snooze_note,
  s.created_at_utc as active_snooze_created_at_utc,
  s.updated_at_utc as active_snooze_updated_at_utc
from public.pay_advances pa
join public.candidates c
  on c.id = pa.candidate_id
left join public.clients cli
  on cli.id = pa.client_id
left join reservation_rollup rr
  on rr.finance_case_id = pa.id
left join latest_remittance lr
  on lr.finance_case_id = pa.id
left join latest_recovery_batch lrb
  on lrb.finance_case_id = pa.id
left join active_snooze s
  on s.finance_case_id = pa.id;

-- =========================================================
-- 11) Backfill / cleanup notes from target plan
-- =========================================================

-- Keep pay_date aligned to the authoritative value where present.
update public.pay_batches
set pay_date = authoritative_payment_date
where authoritative_payment_date is not null
  and pay_date is distinct from authoritative_payment_date;

commit;
