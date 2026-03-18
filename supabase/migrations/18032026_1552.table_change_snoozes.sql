-- 20260318_pay_item_snoozes_schema_prereq.sql
-- Step 0 only:
-- - widen pay_item_snoozes to support whole-timesheet + segment + finance-case snoozes
-- - add stable identity columns for later function upgrades
-- - make the migration safe to rerun
--
-- Notes:
-- - test mode / no legacy-data preservation requirement
-- - this migration does NOT change payment policy logic
-- - it fixes the current table/constraint mismatch and lays the schema groundwork
--   for the later function rewrites

begin;

-- -------------------------------------------------------------------
-- 1) Add stable identity columns needed for the snooze model
-- -------------------------------------------------------------------
alter table public.pay_item_snoozes
  add column if not exists booking_id text null;

alter table public.pay_item_snoozes
  add column if not exists segment_stable_key text null;

-- -------------------------------------------------------------------
-- 2) Normalise/derive snooze identity fields on insert/update
--    - booking_id is derived from timesheet_id when available
--    - segment_stable_key uses segment_id as a transitional fallback
--    - snooze_kind aliases are normalised to canonical stored values
-- -------------------------------------------------------------------
create or replace function public.pay_item_snoozes_sync_identity_fields()
returns trigger
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_booking_id text;
begin
  new.source_ref := nullif(btrim(coalesce(new.source_ref, '')), '');
  new.segment_id := nullif(btrim(coalesce(new.segment_id, '')), '');
  new.booking_id := nullif(btrim(coalesce(new.booking_id, '')), '');
  new.segment_stable_key := nullif(btrim(coalesce(new.segment_stable_key, '')), '');
  new.note := nullif(btrim(coalesce(new.note, '')), '');

  new.snooze_kind := upper(btrim(coalesce(new.snooze_kind, 'DO_NOT_PAY')));

  if new.snooze_kind = 'BLOCKED' then
    new.snooze_kind := 'BLOCKED_TIMESHEET';
  elsif new.snooze_kind = 'LOAN_REPAYMENT' then
    new.snooze_kind := 'PAYMENT_ADVANCE_REPAYMENT';
  end if;

  if new.timesheet_id is not null and new.booking_id is null then
    select nullif(btrim(coalesce(t.booking_id, '')), '')
    into v_booking_id
    from public.timesheets t
    where t.timesheet_id = new.timesheet_id
    limit 1;

    if v_booking_id is not null then
      new.booking_id := v_booking_id;
    end if;
  end if;

  if new.segment_stable_key is null and new.segment_id is not null then
    new.segment_stable_key := new.segment_id;
  end if;

  return new;
end;
$function$;

drop trigger if exists trg_pay_item_snoozes_sync_identity_fields_biu on public.pay_item_snoozes;

create trigger trg_pay_item_snoozes_sync_identity_fields_biu
before insert or update of timesheet_id, segment_id, source_ref, snooze_kind, booking_id, segment_stable_key, note
on public.pay_item_snoozes
for each row
execute function public.pay_item_snoozes_sync_identity_fields();

-- -------------------------------------------------------------------
-- 3) Backfill the new identity columns for any existing rows
--    (safe in test mode; rerunnable)
-- -------------------------------------------------------------------
update public.pay_item_snoozes s
set booking_id = nullif(btrim(coalesce(t.booking_id, '')), '')
from public.timesheets t
where s.timesheet_id = t.timesheet_id
  and s.booking_id is null;

update public.pay_item_snoozes s
set segment_stable_key = nullif(btrim(coalesce(s.segment_id, '')), '')
where s.segment_id is not null
  and s.segment_stable_key is null;

update public.pay_item_snoozes s
set snooze_kind = 'BLOCKED_TIMESHEET'
where s.snooze_kind = 'BLOCKED';

update public.pay_item_snoozes s
set snooze_kind = 'PAYMENT_ADVANCE_REPAYMENT'
where s.snooze_kind = 'LOAN_REPAYMENT';

-- -------------------------------------------------------------------
-- 4) Replace outdated table checks
-- -------------------------------------------------------------------
do $$
begin
  if exists (
    select 1
    from pg_constraint c
    join pg_class t
      on t.oid = c.conrelid
    join pg_namespace n
      on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'pay_item_snoozes'
      and c.conname = 'pay_item_snoozes_identity_check'
  ) then
    alter table public.pay_item_snoozes
      drop constraint pay_item_snoozes_identity_check;
  end if;
end $$;

do $$
begin
  if exists (
    select 1
    from pg_constraint c
    join pg_class t
      on t.oid = c.conrelid
    join pg_namespace n
      on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'pay_item_snoozes'
      and c.conname = 'pay_item_snoozes_kind_check'
  ) then
    alter table public.pay_item_snoozes
      drop constraint pay_item_snoozes_kind_check;
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t
      on t.oid = c.conrelid
    join pg_namespace n
      on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'pay_item_snoozes'
      and c.conname = 'pay_item_snoozes_identity_check'
  ) then
    alter table public.pay_item_snoozes
      add constraint pay_item_snoozes_identity_check
      check (
        source_ref is not null
        or booking_id is not null
        or timesheet_id is not null
      );
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class t
      on t.oid = c.conrelid
    join pg_namespace n
      on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'pay_item_snoozes'
      and c.conname = 'pay_item_snoozes_kind_check'
  ) then
    alter table public.pay_item_snoozes
      add constraint pay_item_snoozes_kind_check
      check (
        snooze_kind = any (
          array[
            'DO_NOT_PAY'::text,
            'BLOCKED_TIMESHEET'::text,
            'TIMESHEET_PAYMENT'::text,
            'OVERPAYMENT_RECOVERY'::text,
            'PAYMENT_ADVANCE_REPAYMENT'::text,
            'MANUAL_DEBT_RECOVERY'::text
          ]
        )
      );
  end if;
end $$;

-- -------------------------------------------------------------------
-- 5) Add new stable-identity indexes
--    Keep old raw timesheet indexes in place for transitional compatibility.
--    The later function rewrites will move reads/writes fully onto the
--    booking/segment_stable_key identity path.
-- -------------------------------------------------------------------
create index if not exists idx_pay_item_snoozes_lookup_booking
on public.pay_item_snoozes (
  candidate_id,
  booking_id,
  segment_stable_key,
  snooze_kind
)
where cleared_at_utc is null
  and source_ref is null
  and booking_id is not null;

create unique index if not exists uq_pay_item_snoozes_active_booking_identity
on public.pay_item_snoozes (
  candidate_id,
  booking_id,
  coalesce(segment_stable_key, ''::text),
  snooze_kind
)
where cleared_at_utc is null
  and source_ref is null
  and booking_id is not null;

commit;
