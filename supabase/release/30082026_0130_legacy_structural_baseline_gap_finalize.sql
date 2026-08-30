-- LEGACY_UPGRADE structural finalizer. The referenced correction tables are
-- created by later historical migrations, so these exact baseline foreign keys
-- are attached only after every pending migration has committed. Safe to rerun;
-- it does not insert, update or delete application rows.

do $legacy_structural_gap_finalize$
begin
  if to_regclass('public.pay_manual_adjustment_carry_forwards') is null
     or to_regclass('public.pay_payment_correction_requests') is null
     or to_regclass('public.pay_payment_correction_work_items') is null then
    raise exception 'LEGACY_UPGRADE structural finalizer prerequisites are incomplete';
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.pay_manual_adjustment_carry_forwards'::regclass
      and conname = 'pay_manual_adjustment_carry_f_source_correction_request_id_fkey'
  ) then
    alter table public.pay_manual_adjustment_carry_forwards
      add constraint pay_manual_adjustment_carry_f_source_correction_request_id_fkey
      foreign key (source_correction_request_id)
      references public.pay_payment_correction_requests(id);
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.pay_manual_adjustment_carry_forwards'::regclass
      and conname = 'pay_manual_adjustment_carry_f_source_correction_work_item__fkey'
  ) then
    alter table public.pay_manual_adjustment_carry_forwards
      add constraint pay_manual_adjustment_carry_f_source_correction_work_item__fkey
      foreign key (source_correction_work_item_id)
      references public.pay_payment_correction_work_items(id);
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.pay_manual_adjustment_carry_forwards'::regclass
      and conname = 'pay_manual_adjustment_carry_forwards_source_request_fkey'
  ) then
    alter table public.pay_manual_adjustment_carry_forwards
      add constraint pay_manual_adjustment_carry_forwards_source_request_fkey
      foreign key (source_correction_request_id)
      references public.pay_payment_correction_requests(id);
  end if;

  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conrelid = 'public.pay_manual_adjustment_carry_forwards'::regclass
      and conname = 'pay_manual_adjustment_carry_forwards_source_work_item_fkey'
  ) then
    alter table public.pay_manual_adjustment_carry_forwards
      add constraint pay_manual_adjustment_carry_forwards_source_work_item_fkey
      foreign key (source_correction_work_item_id)
      references public.pay_payment_correction_work_items(id);
  end if;
end
$legacy_structural_gap_finalize$;
