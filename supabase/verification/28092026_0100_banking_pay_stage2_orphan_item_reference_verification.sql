-- PostgreSQL 17 rollback verification: banking_pay_stage2_orphan_item_reference
--
-- Worker CA amendment ORPHAN-V5 (28 September 2026). Hosted TEST holds 165 pay items whose only Timesheet
-- reference is the frozen basis timesheet_id of a Timesheet row that has since been deleted (parent batches
-- CANCELLED 155, DRAFT 2, SETTLED 7, WAITING_BANK_CONFIRM 1). Before Stage 2 the finance readers mapped such an
-- item with the CASE "item timesheet_id, else UUID-shaped frozen timesheet_id, else component linked Timesheet":
-- the orphan maps to its recorded UUID, so it matches no live Timesheet family and never falls back to the
-- component. Stage 2 stores that mapping in pay_batch_items.bpay_direct_* through the item trigger.
--
-- This file proves, with real first-use writes and reads, that:
--   1. the item trigger stores (recorded UUID, key NULL, revision NULL) for an orphan instead of raising, and
--      still stores the live key/revision for every genuine write;
--   2. deleting a Timesheet whose only pay-item reference is the frozen UUID succeeds exactly as today, and leaves
--      that orphan shape; a Timesheet still referenced directly is refused exactly as today (retention FK);
--   3. orphan items in CANCELLED, DRAFT, SETTLED and WAITING_BANK_CONFIRM batches accept every column-set that the
--      installed cancel / execute / settle writers update, without any change to the reference shape;
--   4. the Stage 2 settled reader and scope helpers give the pre-Stage-2 CASE result for orphans: an orphan is
--      found only under its own recorded UUID and is never a component fallback;
--   5. every stored item reference equals the retired CASE (whole table) and the consumer set is exactly the
--      audited one.
--
-- Everything this file writes is rolled back. It calls no Draft, execution, cancellation, settlement, provider,
-- payment, remittance or mail routine; it writes no stored money. Policy X: reference identity only.

\set ON_ERROR_STOP on
\pset pager off

begin;

set local jit = off;
select pg_catalog.set_config('request.jwt.claim.role','service_role',true);

create or replace function pg_temp.ca5_assert(p_ok boolean, p_message text)
returns void language plpgsql as $assert$
begin
  if p_ok is distinct from true then
    raise exception 'BPAY_STAGE2_ORPHAN_VERIFICATION_FAILED: %', p_message;
  end if;
end
$assert$;

-- ---------------------------------------------------------------------------------------------------------------
-- 0. Catalogue: amended trigger, constraints and the audited consumer set.
-- ---------------------------------------------------------------------------------------------------------------
do $catalogue$
declare
  v_def text;
  v_consumers text[];
begin
  v_def := pg_catalog.pg_get_functiondef('private.bpay_item_work_reference_v1()'::regprocedure);
  perform pg_temp.ca5_assert(pg_catalog.strpos(v_def, 'INTO STRICT') = 0,
    'item trigger must not use INTO STRICT (an orphan must not raise)');
  perform pg_temp.ca5_assert(pg_catalog.strpos(v_def, 'INTO NEW.bpay_direct_work_key_v1,NEW.bpay_direct_work_revision_v1') > 0,
    'item trigger must look the live key/revision up without STRICT');
  perform pg_temp.ca5_assert(pg_catalog.strpos(v_def, 'NEW.bpay_direct_source_timesheet_id_v1:=v_ref;') > 0,
    'item trigger must store the recorded reference');

  perform pg_temp.ca5_assert(exists (
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid = 'public.pay_batch_items'::regclass and c.conname = 'bpay_item_work_reference_presence_v1'
      and c.convalidated
      and pg_catalog.pg_get_constraintdef(c.oid) =
        'CHECK ((((bpay_direct_work_revision_v1 IS NULL) OR (bpay_direct_source_timesheet_id_v1 IS NOT NULL)) AND ((bpay_direct_work_key_v1 IS NULL) OR (bpay_direct_work_revision_v1 IS NOT NULL))))'),
    'bpay_item_work_reference_presence_v1 must admit exactly the orphan shape and be validated');
  perform pg_temp.ca5_assert(exists (
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid = 'public.pay_batch_items'::regclass and c.conname = 'bpay_item_work_reference_v1'
      and c.convalidated and c.confmatchtype = 's' and c.confdeltype = 'n' and c.confupdtype = 'a'
      and pg_catalog.pg_get_constraintdef(c.oid) =
        'FOREIGN KEY (bpay_direct_source_timesheet_id_v1, bpay_direct_work_revision_v1) REFERENCES timesheets(timesheet_id, bpay_work_revision_v1) ON DELETE SET NULL (bpay_direct_work_revision_v1)'),
    'bpay_item_work_reference_v1 must be MATCH SIMPLE, ON DELETE SET NULL (revision only) and validated');
  perform pg_temp.ca5_assert(exists (
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid = 'public.pay_batch_items'::regclass and c.conname = 'pay_batch_items_timesheet_id_fkey'
      and c.confdeltype = 'n'),
    'the pre-existing pay_batch_items.timesheet_id FK must still be ON DELETE SET NULL');
  perform pg_temp.ca5_assert(exists (
    select 1 from pg_catalog.pg_trigger t
    where t.tgrelid = 'public.pay_batch_items'::regclass and t.tgname = 'bpay_item_work_reference_v1'
      and not t.tgisinternal and t.tgenabled = 'O'
      and t.tgfoid = 'private.bpay_item_work_reference_v1()'::regprocedure),
    'item reference trigger must be installed and enabled');

  -- Every installed consumer of the direct reference columns (audited in h8/orphan_v5/ORPHAN_FIX_REPORT.md).
  select pg_catalog.array_agg(x.identity order by x.identity) into v_consumers
  from (
    select 'fn:' || p.oid::regprocedure::text as identity
    from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    where n.nspname not in ('pg_catalog', 'information_schema') and n.nspname !~ '^pg_temp'
      and p.prokind in ('f', 'p') and pg_catalog.pg_get_functiondef(p.oid) ~ 'bpay_direct_'
    union all
    select 'view:' || c.oid::regclass::text from pg_catalog.pg_class c
    where c.relkind in ('v', 'm') and pg_catalog.pg_get_viewdef(c.oid) ~ 'bpay_direct_'
    union all
    select 'policy:' || pol.polname from pg_catalog.pg_policy pol
    where coalesce(pg_catalog.pg_get_expr(pol.polqual, pol.polrelid), '')
          || coalesce(pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid), '') ~ 'bpay_direct_'
  ) x;
  perform pg_temp.ca5_assert(v_consumers = array[
      'fn:_pay_active_settled_components(uuid[])',
      'fn:private.bpay_entitlement_financial_scope_v1(uuid[])',
      'fn:private.bpay_item_work_reference_v1()',
      'fn:private.bpay_settled_financial_scope_v1(uuid[])'],
    'direct-reference consumer set changed; re-audit orphan behaviour: ' || coalesce(v_consumers::text, '{}'));
end
$catalogue$;

-- ---------------------------------------------------------------------------------------------------------------
-- 1. Fixture: one candidate, four Timesheets, one batch per in-flight / terminal status, one finance component.
-- ---------------------------------------------------------------------------------------------------------------
insert into public.tms_users(id,email,role,password_hash,display_name,is_active)
values ('ca5e0000-0000-4000-8000-000000000001','ca5v-owner@example.invalid','admin','not-a-real-password',
  'Orphan reference verifier',true);
insert into public.clients(id,name,vat_chargeable)
values ('ca5e1000-0000-4000-8000-000000000001','Orphan Reference Trust',true);
insert into public.client_settings(id,client_id,effective_from,hr_validation_required,autoprocess_hr,
  self_bill_no_invoices_sent,no_timesheet_required,requires_hr)
values ('ca5e1100-0000-4000-8000-000000000001','ca5e1000-0000-4000-8000-000000000001','2026-01-01',
  false,false,true,true,false);
insert into public.candidates(id,tms_ref,first_name,last_name,display_name,email)
values ('ca5e2000-0000-4000-8000-000000000001','CA5V-001','Orla','Orphan','Orla Orphan','orla.ca5v@example.invalid');
insert into public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json)
values ('ca5e3000-0000-4000-8000-000000000001','ca5e2000-0000-4000-8000-000000000001',
  'ca5e1000-0000-4000-8000-000000000001','2026-01-01','2026-12-31','PAYE','{}');

-- 1 = live, 2 = to be deleted (frozen-only references), 3 = component-linked, 4 = directly referenced
insert into public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  worked_start_iso,worked_end_iso,break_minutes,worked_minutes,week_ending_date,r2_nurse_key,img_sha256_nurse,
  contract_id,sheet_scope,line_type,version,is_current)
select ('ca5e4000-0000-4000-8000-00000000000' || n)::uuid,
  case n when 1 then 'ca5v-live' when 2 then 'ca5v-deleted' when 3 then 'ca5v-linked' else 'ca5v-direct' end,
  'orla-orphan','orphan-reference-trust','ward-a','nurse','2026-03-16 08:00:00+00','2026-03-16 16:00:00+00',
  30,450,'2026-03-22','verify/ca5v-' || n || '.png',repeat(n::text,64),
  'ca5e3000-0000-4000-8000-000000000001','WEEKLY','HOURS',1,true
from pg_catalog.generate_series(1,4) n;

insert into public.pay_batches(id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
  rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,created_by_user_id)
values
  ('ca5e5000-0000-4000-8000-000000000001','2026-03-27','CANCELLED','MONZO_CSV','CSV','CSV','SANDBOX','PAYE','ca5e0000-0000-4000-8000-000000000001'),
  ('ca5e5000-0000-4000-8000-000000000002','2026-03-27','DRAFT','MONZO_CSV','CSV','CSV','SANDBOX','PAYE','ca5e0000-0000-4000-8000-000000000001'),
  ('ca5e5000-0000-4000-8000-000000000003','2026-03-27','SETTLED','MONZO_CSV','CSV','CSV','SANDBOX','PAYE','ca5e0000-0000-4000-8000-000000000001'),
  ('ca5e5000-0000-4000-8000-000000000004','2026-03-27','WAITING_BANK_CONFIRM','MONZO_CSV','CSV','CSV','SANDBOX','PAYE','ca5e0000-0000-4000-8000-000000000001');
insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id,candidate_tms_ref,candidate_display_name,
  paye_state,settlement_status,settled_at_utc)
select ('ca5e6000-0000-4000-8000-00000000000' || n)::uuid, ('ca5e5000-0000-4000-8000-00000000000' || n)::uuid,
  'ca5e2000-0000-4000-8000-000000000001','CA5V-001','Orla Orphan','READY',
  case when n = 3 then 'SETTLED' else 'PENDING' end, case when n = 3 then pg_catalog.now() end
from pg_catalog.generate_series(1,4) n;

insert into public.pay_advances(id,candidate_id,reason,original_amount,outstanding_amount,case_type)
values ('ca5e7000-0000-4000-8000-000000000001','ca5e2000-0000-4000-8000-000000000001','UNDERPAYMENT',5,5,'UNDERPAYMENT');
insert into public.pay_finance_case_components(id,finance_case_id,candidate_id,source_family_key,component_key_type,
  component_key_value,classification,source_pay_method,linked_timesheet_id,source_amount,remaining_source_amount)
values ('ca5e7100-0000-4000-8000-000000000001','ca5e7000-0000-4000-8000-000000000001',
  'ca5e2000-0000-4000-8000-000000000001','ca5v-family','TS_TOTAL','CA5V-TOTAL','TAXABLE_CHANNEL_SENSITIVE','PAYE',
  'ca5e4000-0000-4000-8000-000000000003',5,5);

-- Items 0001..0004: frozen-only reference to Timesheet 2, one per batch, a settled-reader shape (UNDERPAYMENT_PAYMENT,
-- TS_TOTAL, 5.00) and a component linked to Timesheet 3, so the component fallback would find them if the direct
-- reference were NULL. Item 0011: the same, but with no Timesheet reference at all (a genuine component fallback).
insert into public.pay_batch_items(id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,
  amount_ex_vat,amount_vat,amount_inc_vat,finance_component_id,frozen_component_key_type,frozen_component_key_value,
  frozen_source_amount,frozen_source_basis_json)
select ('ca5e8000-0000-4000-8000-00000000000' || n)::uuid, ('ca5e6000-0000-4000-8000-00000000000' || n)::uuid,
  'UNDERPAYMENT_PAYMENT',null::uuid,'PAYE',false,5,0,5,'ca5e7100-0000-4000-8000-000000000001'::uuid,'TS_TOTAL','CA5V-TOTAL',5,
  '{"timesheet_id":"ca5e4000-0000-4000-8000-000000000002"}'::jsonb
from pg_catalog.generate_series(1,4) n
union all
select 'ca5e8000-0000-4000-8000-000000000011'::uuid,'ca5e6000-0000-4000-8000-000000000003'::uuid,
  'UNDERPAYMENT_PAYMENT',null::uuid,'PAYE',false,5,0,5,'ca5e7100-0000-4000-8000-000000000001'::uuid,'TS_TOTAL','CA5V-TOTAL',5,
  '{}'::jsonb;

-- ---------------------------------------------------------------------------------------------------------------
-- 2. Genuine new writes still store the live key and revision.
-- ---------------------------------------------------------------------------------------------------------------
insert into public.pay_batch_items(id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,
  amount_ex_vat,amount_inc_vat,frozen_source_basis_json)
values
  ('ca5e8000-0000-4000-8000-000000000021','ca5e6000-0000-4000-8000-000000000002','SEGMENT_DELTA',
   'ca5e4000-0000-4000-8000-000000000001','PAYE',false,10,10,'{"timesheet_id":"ca5e4000-0000-4000-8000-000000000001"}'),
  ('ca5e8000-0000-4000-8000-000000000022','ca5e6000-0000-4000-8000-000000000002','SEGMENT_DELTA',
   null,'PAYE',false,10,10,'{"timesheet_id":" ca5e4000-0000-4000-8000-000000000001 "}'),
  ('ca5e8000-0000-4000-8000-000000000023','ca5e6000-0000-4000-8000-000000000002','SEGMENT_DELTA',
   null,'PAYE',false,10,10,'{}'),
  ('ca5e8000-0000-4000-8000-000000000024','ca5e6000-0000-4000-8000-000000000002','SEGMENT_DELTA',
   null,'PAYE',false,10,10,'{"timesheet_id":"not-a-uuid"}');

do $genuine$
begin
  perform pg_temp.ca5_assert((select pg_catalog.count(*) = 2 from public.pay_batch_items i
      where i.id in ('ca5e8000-0000-4000-8000-000000000021','ca5e8000-0000-4000-8000-000000000022')
        and i.bpay_direct_source_timesheet_id_v1 = 'ca5e4000-0000-4000-8000-000000000001'
        and i.bpay_direct_work_key_v1 = 'ca5v-live' and i.bpay_direct_work_revision_v1 = 0),
    'a genuine direct or frozen reference must store the live key and revision');
  perform pg_temp.ca5_assert((select pg_catalog.count(*) = 2 from public.pay_batch_items i
      where i.id in ('ca5e8000-0000-4000-8000-000000000023','ca5e8000-0000-4000-8000-000000000024')
        and i.bpay_direct_source_timesheet_id_v1 is null and i.bpay_direct_work_key_v1 is null
        and i.bpay_direct_work_revision_v1 is null),
    'an item without a UUID-shaped reference must store no reference');
  perform pg_temp.ca5_assert((select pg_catalog.count(*) = 4 from public.pay_batch_items i
      where i.id::text like 'ca5e8000-0000-4000-8000-00000000000_'
        and i.bpay_direct_source_timesheet_id_v1 = 'ca5e4000-0000-4000-8000-000000000002'
        and i.bpay_direct_work_key_v1 = 'ca5v-deleted' and i.bpay_direct_work_revision_v1 = 0),
    'the four frozen-only items must reference the still-live Timesheet 2 before it is deleted');
end
$genuine$;

-- ---------------------------------------------------------------------------------------------------------------
-- 3. Timesheet-delete path. Today a Timesheet whose only pay-item reference is the frozen UUID can be deleted; the
--    items keep their frozen JSON. The candidate must allow the same delete and leave the orphan shape.
-- ---------------------------------------------------------------------------------------------------------------
create temp table ca5_items_before on commit drop as
select i.id, pg_catalog.to_jsonb(i) - array['bpay_direct_source_timesheet_id_v1','bpay_direct_work_key_v1',
  'bpay_direct_work_revision_v1'] as row_json
from public.pay_batch_items i where i.id::text like 'ca5e8000-%';

delete from public.timesheets where timesheet_id = 'ca5e4000-0000-4000-8000-000000000002';

do $deleted$
begin
  perform pg_temp.ca5_assert(not exists (select 1 from public.timesheets
      where timesheet_id = 'ca5e4000-0000-4000-8000-000000000002'),
    'the frozen-only Timesheet delete must succeed as today');
  perform pg_temp.ca5_assert((select pg_catalog.count(*) = 4 from public.pay_batch_items i
      where i.id::text like 'ca5e8000-0000-4000-8000-00000000000_'
        and i.timesheet_id is null
        and i.bpay_direct_source_timesheet_id_v1 = 'ca5e4000-0000-4000-8000-000000000002'
        and i.bpay_direct_work_key_v1 is null and i.bpay_direct_work_revision_v1 is null),
    'after the delete the four items must hold (recorded UUID, NULL, NULL) in CANCELLED, DRAFT, SETTLED and WAITING_BANK_CONFIRM');
  perform pg_temp.ca5_assert(not exists (
      select 1 from public.pay_batch_items i join pg_temp.ca5_items_before b on b.id = i.id
      where (pg_catalog.to_jsonb(i) - array['bpay_direct_source_timesheet_id_v1','bpay_direct_work_key_v1',
        'bpay_direct_work_revision_v1']) is distinct from b.row_json),
    'the Timesheet delete must change no pay-item column other than the Stage 2 reference columns');
  perform pg_temp.ca5_assert((select pg_catalog.count(*) = 2 from public.pay_batch_items i
      where i.id in ('ca5e8000-0000-4000-8000-000000000021','ca5e8000-0000-4000-8000-000000000022')
        and i.bpay_direct_work_key_v1 = 'ca5v-live' and i.bpay_direct_work_revision_v1 = 0),
    'unrelated live references must be untouched by the delete');
end
$deleted$;

-- A Timesheet still referenced directly (item timesheet_id) is refused today by the retention FK; it must be refused
-- the same way (not by the Stage 2 trigger or FK).
insert into public.pay_batch_items(id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,
  amount_ex_vat,amount_inc_vat,frozen_source_basis_json)
values ('ca5e8000-0000-4000-8000-000000000041','ca5e6000-0000-4000-8000-000000000002','SEGMENT_DELTA',
  'ca5e4000-0000-4000-8000-000000000004','PAYE',false,10,10,'{"timesheet_id":"ca5e4000-0000-4000-8000-000000000004"}');
do $direct_delete$
declare v_state text; v_constraint text;
begin
  begin
    delete from public.timesheets where timesheet_id = 'ca5e4000-0000-4000-8000-000000000004';
    v_state := 'NO_ERROR';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate, v_constraint = constraint_name;
  end;
  perform pg_temp.ca5_assert(v_state = '23503' and v_constraint = 'timesheet_financial_retention_timesheet_fk',
    pg_catalog.format('a directly referenced Timesheet delete must be refused by the retention FK as today (got %s / %s)',
      v_state, coalesce(v_constraint, '')));
end
$direct_delete$;

-- A new write whose frozen reference names a Timesheet that does not exist is accepted today; it must be accepted
-- with the orphan shape (the unamended trigger raised P0002 here).
insert into public.pay_batch_items(id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,is_voided,
  amount_ex_vat,amount_inc_vat,frozen_source_basis_json)
values ('ca5e8000-0000-4000-8000-000000000031','ca5e6000-0000-4000-8000-000000000001','SEGMENT_DELTA',
  null,'PAYE',true,0,0,'{"timesheet_id":"ca5effff-0000-4000-8000-000000000031"}');
do $direct_orphan$
begin
  perform pg_temp.ca5_assert(exists (select 1 from public.pay_batch_items i
      where i.id = 'ca5e8000-0000-4000-8000-000000000031'
        and i.bpay_direct_source_timesheet_id_v1 = 'ca5effff-0000-4000-8000-000000000031'
        and i.bpay_direct_work_key_v1 is null and i.bpay_direct_work_revision_v1 is null),
    'an item written with a frozen reference to a missing Timesheet must store the orphan shape');
end
$direct_orphan$;

-- ---------------------------------------------------------------------------------------------------------------
-- 4. In-flight orphans: every column-set written by the installed cancel / execute / settle writers of
--    pay_batch_items (void, transfer link/unlink, bank reference, reservation, payout snapshot, operation key,
--    component snapshot, manual amount rewrite), plus a frozen-basis rewrite that re-fires the trigger and a Draft
--    item removal. Every write must succeed and keep the orphan shape.
-- ---------------------------------------------------------------------------------------------------------------
do $inflight$
declare v_id uuid;
begin
  foreach v_id in array array['ca5e8000-0000-4000-8000-000000000001','ca5e8000-0000-4000-8000-000000000002',
                              'ca5e8000-0000-4000-8000-000000000003','ca5e8000-0000-4000-8000-000000000004']::uuid[] loop
    update public.pay_batch_items set pay_bank_transfer_id = null, bank_reference = 'CA5V-REF', updated_at = pg_catalog.now()
      where id = v_id;
    update public.pay_batch_items set reservation_id = null, updated_at = pg_catalog.now() where id = v_id;
    update public.pay_batch_items set payout_instruction_snapshot_json = '{"ca5v":"payout"}'::jsonb where id = v_id;
    update public.pay_batch_items set operation_source_key = 'ca5v-op-' || v_id::text where id = v_id;
    update public.pay_batch_items
       set frozen_component_snapshot_json = coalesce(frozen_component_snapshot_json, '{}'::jsonb) || '{"ca5v":1}'::jsonb
     where id = v_id;
    update public.pay_batch_items set amount_ex_vat = amount_ex_vat, amount_vat = amount_vat,
      amount_inc_vat = amount_inc_vat, description = description where id = v_id;
    update public.pay_batch_items set frozen_source_basis_json = frozen_source_basis_json || '{"ca5v_touch":1}'::jsonb
      where id = v_id;
    perform pg_temp.ca5_assert(exists (select 1 from public.pay_batch_items i where i.id = v_id
        and i.bpay_direct_source_timesheet_id_v1 = 'ca5e4000-0000-4000-8000-000000000002'
        and i.bpay_direct_work_key_v1 is null and i.bpay_direct_work_revision_v1 is null
        and i.amount_ex_vat = 5 and i.amount_inc_vat = 5 and i.frozen_source_amount = 5),
      'orphan item ' || v_id::text || ' must keep its reference shape and amounts through every writer column-set');
  end loop;
  -- Pre-bank cancel / Draft overlay removal void the item.
  update public.pay_batch_items set is_voided = true, updated_at = pg_catalog.now()
   where id in ('ca5e8000-0000-4000-8000-000000000002','ca5e8000-0000-4000-8000-000000000004');
  perform pg_temp.ca5_assert((select pg_catalog.count(*) = 2 from public.pay_batch_items i
      where i.id in ('ca5e8000-0000-4000-8000-000000000002','ca5e8000-0000-4000-8000-000000000004')
        and i.is_voided and i.bpay_direct_source_timesheet_id_v1 = 'ca5e4000-0000-4000-8000-000000000002'
        and i.bpay_direct_work_revision_v1 is null),
    'voiding in-flight orphan items (DRAFT, WAITING_BANK_CONFIRM) must succeed');
  update public.pay_batch_items set is_voided = false
   where id in ('ca5e8000-0000-4000-8000-000000000002','ca5e8000-0000-4000-8000-000000000004');
end
$inflight$;

-- ---------------------------------------------------------------------------------------------------------------
-- 5. Readers: identical to the retired CASE.
-- ---------------------------------------------------------------------------------------------------------------
do $readers$
declare v_rows jsonb;
begin
  -- The orphan is found under its own recorded UUID (the defensive unresolved scope row), only from the settled batch.
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r) order by r.key_type, r.key_value), '[]'::jsonb) into v_rows
  from public._pay_active_settled_components(array['ca5e4000-0000-4000-8000-000000000002']::uuid[]) r;
  perform pg_temp.ca5_assert(v_rows = '[{"timesheet_id":"ca5e4000-0000-4000-8000-000000000002","key_type":"TS_TOTAL","key_value":"CA5V-TOTAL","amount_ex_vat":5.00,"amount_inc_vat":5.00}]'::jsonb,
    'settled reader for the deleted Timesheet must equal the retired CASE result: ' || v_rows::text);

  -- Under the component-linked Timesheet only the genuine fallback item (0011) counts; the orphan (direct non-NULL)
  -- is never a fallback. The retired CASE gave 5.00 as well (10.00 would mean the orphan fell back).
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r) order by r.key_type, r.key_value), '[]'::jsonb) into v_rows
  from public._pay_active_settled_components(array['ca5e4000-0000-4000-8000-000000000003']::uuid[]) r;
  perform pg_temp.ca5_assert(v_rows = '[{"timesheet_id":"ca5e4000-0000-4000-8000-000000000003","key_type":"TS_TOTAL","key_value":"CA5V-TOTAL","amount_ex_vat":5.00,"amount_inc_vat":5.00}]'::jsonb,
    'settled reader for the component-linked Timesheet must exclude the orphan: ' || v_rows::text);

  -- Scope helpers: a deleted Timesheet resolves to the same defensive row as public._pay_timesheet_rotation_scope,
  -- and an orphan contributes no physical Timesheet to any live family.
  select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(s)) into v_rows
  from private.bpay_settled_financial_scope_v1(array['ca5e4000-0000-4000-8000-000000000002']::uuid[]) s;
  perform pg_temp.ca5_assert(v_rows = pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'requested_timesheet_id','ca5e4000-0000-4000-8000-000000000002','booking_id',null,
      'canonical_timesheet_id','ca5e4000-0000-4000-8000-000000000002','family_timesheet_id','ca5e4000-0000-4000-8000-000000000002',
      'family_is_current',null,'family_version',null,'requested_is_canonical',false)),
    'settled scope for a deleted Timesheet must be the defensive unresolved row: ' || coalesce(v_rows::text, 'null'));
  select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(s)) into v_rows
  from private.bpay_entitlement_financial_scope_v1(array['ca5e4000-0000-4000-8000-000000000002']::uuid[]) s;
  perform pg_temp.ca5_assert(v_rows = pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'requested_timesheet_id','ca5e4000-0000-4000-8000-000000000002','booking_id',null,
      'canonical_timesheet_id','ca5e4000-0000-4000-8000-000000000002','family_timesheet_id','ca5e4000-0000-4000-8000-000000000002',
      'family_is_current',null,'family_version',null,'requested_is_canonical',false)),
    'entitlement scope for a deleted Timesheet must be the defensive unresolved row: ' || coalesce(v_rows::text, 'null'));
  perform pg_temp.ca5_assert(not exists (
      select 1 from private.bpay_settled_financial_scope_v1(array['ca5e4000-0000-4000-8000-000000000001',
        'ca5e4000-0000-4000-8000-000000000003']::uuid[]) s
      where s.family_timesheet_id = 'ca5e4000-0000-4000-8000-000000000002')
    and not exists (
      select 1 from private.bpay_entitlement_financial_scope_v1(array['ca5e4000-0000-4000-8000-000000000001',
        'ca5e4000-0000-4000-8000-000000000003']::uuid[]) s
      where s.family_timesheet_id = 'ca5e4000-0000-4000-8000-000000000002'),
    'an orphan must add no Timesheet to a live family scope');
end
$readers$;

-- ---------------------------------------------------------------------------------------------------------------
-- 6. Whole table: every stored item reference equals the retired CASE's direct reference, and key/revision equal the
--    live Timesheet's (NULL when the Timesheet row is gone). This is the backfill's COMPLETE drift rule.
-- ---------------------------------------------------------------------------------------------------------------
do $whole$
declare v_drift bigint; v_orphans bigint;
begin
  select pg_catalog.count(*) filter (where (i.bpay_direct_source_timesheet_id_v1, i.bpay_direct_work_key_v1,
           i.bpay_direct_work_revision_v1) is distinct from (r.ref, t.bpay_work_key_v1, t.bpay_work_revision_v1)),
         pg_catalog.count(*) filter (where r.ref is not null and t.timesheet_id is null)
    into v_drift, v_orphans
  from public.pay_batch_items i
  cross join lateral (select case
      when i.timesheet_id is not null then i.timesheet_id
      when nullif(btrim(coalesce(i.frozen_source_basis_json->>'timesheet_id', '')), '')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (btrim(i.frozen_source_basis_json->>'timesheet_id'))::uuid
      else null::uuid end as ref) r
  left join public.timesheets t on t.timesheet_id = r.ref;
  perform pg_temp.ca5_assert(v_drift = 0,
    pg_catalog.format('%s pay items differ from the retired CASE / live Timesheet derivation', v_drift));
  perform pg_temp.ca5_assert(v_orphans >= 5, pg_catalog.format('expected at least the 5 fixture orphans, saw %s', v_orphans));
  raise notice 'BPAY_STAGE2_ORPHAN_VERIFICATION orphan_items_including_fixture=%', v_orphans;
end
$whole$;

-- Draft item removal of an orphan is an ordinary delete.
delete from public.pay_batch_items where id = 'ca5e8000-0000-4000-8000-000000000002';
do $removed$
begin
  perform pg_temp.ca5_assert(not exists (select 1 from public.pay_batch_items
      where id = 'ca5e8000-0000-4000-8000-000000000002'), 'a DRAFT orphan item must be removable');
end
$removed$;

select 'BPAY_STAGE2_ORPHAN_ITEM_REFERENCE_VERIFIED' as result;

rollback;
