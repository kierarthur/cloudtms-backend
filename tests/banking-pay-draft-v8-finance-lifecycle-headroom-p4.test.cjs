const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const json = relativePath => JSON.parse(read(relativePath));
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const quote = value => `'${String(value).replaceAll("'", "''")}'`;

const fixturePath = 'tests/fixtures/banking-pay-draft-v8-finance-lifecycle-headroom-p4-v1.json';
const bindingPath = 'tests/fixtures/banking-pay-create-draft-v8-class-execution-bindings-v1.json';
const policyPath = 'codex_outputs/banking-pay-create-draft-policy-v1/BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json';
const runtimePath = 'tests/04092026_1220_banking_pay_draft_v8_finance_lifecycle_headroom_p4_runtime.sql';
const resultPath = 'codex_outputs/h2-draft-parity/P4_FINANCE_LIFECYCLE_HEADROOM_RESULTS_V1.json';
const bridgePath = 'tests/03092026_1600_banking_pay_draft_v8_finance_category_runtime.sql';
const canonicalPath = 'tests/02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
const selectionFixturePath = 'tests/fixtures/28082026_1429_banking_pay_selection_setup.sql';
const certificateOwnerPath = 'supabase/repeatable/02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql';
const parityOwnerPath = 'supabase/repeatable/02092026_2315_banking_pay_draft_constituent_parity_v8.sql';
const insertItemsOwnerPath = 'supabase/repeatable/02092026_1040_banking_pay_draft_insert_items_finance_handoff_v1.sql';
const carryForwardTransportOwnerPath = 'supabase/repeatable/04092026_1340_banking_pay_draft_carry_forward_policy_transport_v8.sql';
const cancelledRepaymentOwnerPath = 'supabase/repeatable/04092026_1056_banking_pay_cancelled_advance_repayment_preview_authority_v1.sql';
const carryForwardSelectionOwnerPath = 'supabase/repeatable/04092026_1330_banking_pay_manual_carry_forward_selection_authority_v1.sql';
const carryForwardPreviewContractOwnerPath = 'supabase/repeatable/04092026_1350_banking_pay_manual_carry_forward_preview_contract_v8.sql';
const carryForwardAllocationOwnerPath = 'supabase/repeatable/04092026_1360_banking_pay_manual_carry_forward_allocation_seed_v8.sql';
const mixedRecoveryOwnerPath = 'supabase/repeatable/04092026_1300_banking_pay_draft_mixed_recovery_partial_link_v8.sql';

const expectedClasses = [
  'advance_part_repaid_residual',
  'advance_paid_off_absent',
  'advance_cancelled_absent',
  'advance_voided_reappears_once',
  'overpayment_zero_headroom',
  'overpayment_exact_headroom',
  'overpayment_partial_headroom',
  'manual_debt_zero_headroom',
  'manual_debt_partial_headroom',
  'mixed_recoveries_deterministic_order',
  'carry_forward_credit',
  'carry_forward_debit'
];

function replaceOnce(text, needle, replacement, label) {
  const parts = text.split(needle);
  if (parts.length !== 2) throw new Error(`${label} expected once; observed ${parts.length - 1}`);
  return `${parts[0]}${replacement}${parts[1]}`;
}

function replaceBlock(text, start, end, replacement, label) {
  const startIndex = text.indexOf(start);
  const endIndex = text.indexOf(end, startIndex + start.length);
  if (startIndex < 0 || endIndex < 0 || text.indexOf(start, startIndex + 1) >= 0) {
    throw new Error(`${label} splice boundary changed`);
  }
  return `${text.slice(0, startIndex)}${replacement}${text.slice(endIndex + end.length)}`;
}

function stripExactOuterTransaction(sql, label) {
  const begin = /(^|\r?\n)begin;\r?\n/i.exec(sql);
  const commits = [...sql.matchAll(/^commit;\s*$/gim)];
  if (!begin || commits.length !== 1 || commits[0].index + commits[0][0].length < sql.trimEnd().length - 2) {
    throw new Error(`${label} exact outer transaction changed`);
  }
  return `${sql.slice(0, begin.index + begin[1].length)}${sql.slice(begin.index + begin[0].length, commits[0].index)}`;
}

function mapLogicalPostgresOwnerForDisposableProvider(sql, databaseUser) {
  if (databaseUser === 'postgres') return sql;
  // The managed release engine maps the repository's logical `postgres`
  // owner to the connected provider owner. Mirror only that ownership token
  // in this rollback-only disposable replay; do not grant extra privileges.
  return String(sql).replace(
    /\bowner\s+to\s+(?:"postgres"|postgres)(?=\s*;)/gi,
    'OWNER TO CURRENT_USER',
  );
}

const frozenSettingsAuthoritySql = `
insert into public.client_settings(
  id,client_id,effective_from,autoprocess_hr
) values (
  '43000000-0000-4000-8000-000000000021',
  '43000000-0000-4000-8000-000000000020','2026-01-01',false
);
do $h2_p4_freeze_settings_authority$
declare v_row record;
begin
  update public.timesheets_financials
  set processed_at_utc=coalesce(processed_at_utc,statement_timestamp())
  where client_id='43000000-0000-4000-8000-000000000020'
    and is_current=true;
  for v_row in
    select timesheet.timesheet_id
    from public.timesheets as timesheet
    join public.timesheets_financials as financial
      on financial.timesheet_id=timesheet.timesheet_id
     and financial.is_current=true
    where financial.client_id='43000000-0000-4000-8000-000000000020'
    order by timesheet.timesheet_id
  loop
    perform private._timesheet_settings_authority_frozen_v1(v_row.timesheet_id);
  end loop;
  if exists(
    select 1
    from public.timesheets as timesheet
    join public.timesheets_financials as financial
      on financial.timesheet_id=timesheet.timesheet_id
     and financial.is_current=true
    where financial.client_id='43000000-0000-4000-8000-000000000020'
      and timesheet.settings_authority_json='{}'::jsonb
  ) then
    raise exception 'H2_P4_SETTINGS_AUTHORITY_NOT_FROZEN';
  end if;
end;
$h2_p4_freeze_settings_authority$;
`;

function variantOrdinal(testCase, channel) {
  if (testCase.scenario.kind.startsWith('ADVANCE_REPAYMENT')) return channel === 'PAYE' ? 9 : 10;
  if (testCase.scenario.kind === 'MANUAL_DEBT') return channel === 'PAYE' ? 5 : 6;
  return channel === 'PAYE' ? 1 : 2;
}

function baseIdsSql() {
  return `
create temporary table pg_temp.h2_p4_source_ids on commit drop as
select variant.ordinal,variant.pay_channel,
 ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid candidate_id,
 ('43000000-0000-4000-8000-'||lpad((50000+variant.ordinal)::text,12,'0'))::uuid finance_case_id,
 ('43000000-0000-4000-8000-'||lpad((80000+variant.ordinal)::text,12,'0'))::uuid finance_component_id,
 ('43000000-0000-4000-8000-'||lpad((20000+variant.ordinal)::text,12,'0'))::uuid timesheet_id
from pg_temp.h2_f013_canonical_variants variant;
`;
}

function historicalReleasedReservationSql() {
  return `
insert into public.pay_batches(id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
  rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,execution_commit_state)
select '47000000-0000-4000-8000-000000000401','2026-08-21','DRAFT',
  'MONZO_CSV','CSV','CSV','SANDBOX',pay_channel,'NOT_SUBMITTED'
from pg_temp.h2_p4_source_ids;
insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id,candidate_tms_ref,
  candidate_display_name,paye_state,settlement_status)
select '47000000-0000-4000-8000-000000000402','47000000-0000-4000-8000-000000000401',
  candidate_id,'H2-P4-RELEASED','H2 P4 released','READY','UNPAID'
from pg_temp.h2_p4_source_ids;
insert into public.pay_batch_items(id,pay_batch_candidate_id,item_type,source_ref,description,
  amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,finance_case_id,
  finance_component_id,is_voided,paye_treatment,operation_source_key,
  frozen_component_snapshot_json,frozen_component_key_type,frozen_component_key_value,
  frozen_component_classification,frozen_source_basis_json,frozen_source_pay_method,
  frozen_target_pay_method,frozen_source_amount,frozen_target_amount_ex_vat,
  frozen_target_amount_vat,frozen_target_amount_inc_vat)
select '47000000-0000-4000-8000-000000000403','47000000-0000-4000-8000-000000000402',
  'LOAN_REPAYMENT','advance:'||finance_case_id::text,'Released repayment',-40,0,-40,
  pay_channel,finance_case_id,finance_component_id,false,
  case when pay_channel='PAYE' then 'NET_DEDUCT' else 'NONE' end,'h2-p4-released-repayment',
  jsonb_build_object(
    'finance_case_id',finance_case_id::text,'finance_component_id',finance_component_id::text,
    'component_key_type','CASE_TOTAL','component_key_value','TOTAL',
    'classification','NET_PAY_FIXED_RECOVERY','source_amount_ex_vat','40.00',
    'source_reservation_amount_ex_vat','40.00','target_amount_ex_vat','-40.00',
    'source_basis_json',jsonb_build_object(
      'finance_case_id',finance_case_id::text,'finance_component_id',finance_component_id::text,
      'component_key_type','CASE_TOTAL','component_key_value','TOTAL',
      'source_reservation_amount_ex_vat','40.00')),
  'CASE_TOTAL','TOTAL','NET_PAY_FIXED_RECOVERY',
  jsonb_build_object(
    'finance_case_id',finance_case_id::text,'finance_component_id',finance_component_id::text,
    'component_key_type','CASE_TOTAL','component_key_value','TOTAL',
    'source_reservation_amount_ex_vat','40.00'),
  pay_channel,pay_channel,40,-40,0,-40
from pg_temp.h2_p4_source_ids;
insert into public.pay_advance_reservations(id,finance_case_id,pay_batch_id,pay_batch_candidate_id,
  pay_batch_item_id,reserved_amount,repayment_week_start,status,released_at_utc,released_reason,
  finance_component_id,reserved_source_amount,frozen_rounded_target_amount,
  frozen_component_snapshot_json,frozen_component_key_type,frozen_component_key_value,
  frozen_component_classification,frozen_source_basis_json,frozen_source_pay_method,
  frozen_target_pay_method)
select '47000000-0000-4000-8000-000000000404',finance_case_id,
  '47000000-0000-4000-8000-000000000401','47000000-0000-4000-8000-000000000402',
  '47000000-0000-4000-8000-000000000403',40,'2026-08-24','RESERVED',
  null,null,finance_component_id,40,40,
  jsonb_build_object(
    'finance_case_id',finance_case_id::text,'finance_component_id',finance_component_id::text,
    'component_key_type','CASE_TOTAL','component_key_value','TOTAL',
    'classification','NET_PAY_FIXED_RECOVERY','source_amount_ex_vat','40.00',
    'source_reservation_amount_ex_vat','40.00','target_amount_ex_vat','-40.00'),
  'CASE_TOTAL','TOTAL','NET_PAY_FIXED_RECOVERY',
  jsonb_build_object(
    'finance_case_id',finance_case_id::text,'finance_component_id',finance_component_id::text,
    'component_key_type','CASE_TOTAL','component_key_value','TOTAL',
    'source_reservation_amount_ex_vat','40.00'),pay_channel,pay_channel
from pg_temp.h2_p4_source_ids;

-- Execute the actual current cancellation owners rather than synthesising the
-- post-cancel state.  This is the policy gate for the reappearing-advance case.
do $h2_p4_real_cancel$
declare
  v_actor_id uuid := '10000000-0000-4000-8000-000000000001';
  v_batch_id uuid := '47000000-0000-4000-8000-000000000401';
  v_batch_candidate_id uuid := '47000000-0000-4000-8000-000000000402';
  v_item_id uuid := '47000000-0000-4000-8000-000000000403';
  v_worker_id text := 'h2-p4-real-cancel';
  v_status jsonb;
  v_started jsonb;
  v_prepared jsonb;
  v_expanded jsonb;
  v_processed jsonb;
  v_finalised jsonb;
  v_request_id uuid;
  v_operation_id uuid;
begin
  update public.settings_defaults
  set banking_pay_candidate_cancellation_enabled=true,
      banking_pay_draft_overlay_fast_cancel_v1_enabled=false
  where id=1;

  v_status:=public.pay_batch_payment_status_page_v1(
    v_batch_id,v_actor_id,'{}'::jsonb,'STATUS','ASC',25,null);
  if coalesce((v_status->>'ok')::boolean,false) is not true
     or coalesce(v_status->>'explicit_snapshot_token','') !~ '^[0-9a-f]{64}$' then
    raise exception 'H2_P4_REAL_CANCEL_STATUS_READ_FAILED:%',v_status;
  end if;

  v_started:=public.pay_payment_correction_request_start(
    v_batch_id,
    jsonb_build_object(
      'command','PREPARE','mode','EXPLICIT','requested_action','PRE_BANK_CANCEL',
      'filter_json','{}'::jsonb,'sort_key','STATUS','sort_direction','ASC',
      'snapshot_token',v_status->>'explicit_snapshot_token',
      'explicit_candidate_tokens',jsonb_build_array(v_batch_candidate_id::text)),
    'H2 deterministic rollback cancellation proof',v_actor_id,null,false,null);
  if coalesce((v_started->>'ok')::boolean,false) is not true then
    raise exception 'H2_P4_REAL_CANCEL_START_FAILED:%',v_started;
  end if;
  v_request_id:=(v_started->>'correction_request_id')::uuid;
  v_operation_id:=(v_started->>'operation_id')::uuid;

  update public.banking_pay_operations
  set status='RUNNING',runner_state='RUNNING',phase='PREPARE_SELECTION',
      lease_owner=v_worker_id,lease_expires_at_utc=clock_timestamp()+interval '5 minutes',
      locked_by=v_worker_id,lock_expires_at_utc=clock_timestamp()+interval '5 minutes'
  where id=v_operation_id;
  v_prepared:=public.pay_payment_correction_selection_prepare_chunk_v1(
    v_request_id,v_operation_id,null,50,v_worker_id,v_actor_id);
  if coalesce((v_prepared->>'ok')::boolean,false) is not true
     or coalesce((v_prepared->>'complete')::boolean,false) is not true then
    raise exception 'H2_P4_REAL_CANCEL_PREPARE_FAILED:%',v_prepared;
  end if;

  update public.pay_payment_correction_requests
  set status='AUTHORISED',authorised_at_utc=clock_timestamp(),updated_at_utc=clock_timestamp()
  where id=v_request_id;
  update public.banking_pay_operations
  set status='RUNNING',runner_state='RUNNABLE',phase='EXPAND_WORK',
      lease_owner=null,lease_expires_at_utc=null,locked_by=null,lock_expires_at_utc=null
  where id=v_operation_id;
  v_expanded:=public.pay_payment_correction_expand_work(v_request_id,v_actor_id);
  if coalesce((v_expanded->>'ok')::boolean,false) is not true then
    raise exception 'H2_P4_REAL_CANCEL_EXPAND_FAILED:%',v_expanded;
  end if;
  update public.banking_pay_operations
  set status='RUNNING',runner_state='RUNNING',
      lease_owner=v_worker_id,lease_expires_at_utc=clock_timestamp()+interval '5 minutes',
      locked_by=v_worker_id,lock_expires_at_utc=clock_timestamp()+interval '5 minutes'
  where id=v_operation_id;
  v_processed:=public.pay_payment_correction_process_chunk(
    v_request_id,50,v_worker_id,v_actor_id);
  if coalesce((v_processed->>'ok')::boolean,false) is not true
     or v_processed->>'phase'<>'FINALISE' then
    raise exception 'H2_P4_REAL_CANCEL_PROCESS_FAILED:%',v_processed;
  end if;

  v_finalised:=public.pay_payment_correction_process_chunk(
    v_request_id,50,v_worker_id,v_actor_id);
  if coalesce((v_finalised->>'ok')::boolean,false) is not true
     or coalesce((v_finalised->>'financial_complete')::boolean,false) is not true
     or v_finalised->>'phase'<>'REFRESH_WORKBENCH'
     or not exists (select 1 from public.pay_batches where id=v_batch_id and status='CANCELLED')
     or not exists (select 1 from public.pay_batch_items where id=v_item_id and is_voided)
     or not exists (select 1 from public.pay_advance_reservations
                    where id='47000000-0000-4000-8000-000000000404'
                      and status='RELEASED')
     or not exists (select 1 from public.pay_advances
                    where id in (select finance_case_id from pg_temp.h2_p4_source_ids)
                      and payout_status='PAID'
                      and outstanding_amount=40)
    or not exists (select 1 from public.pay_finance_case_components
                   where id in (select finance_component_id from pg_temp.h2_p4_source_ids)
                      and remaining_source_amount=40) then
    raise exception 'H2_P4_REAL_CANCEL_FINALISE_OR_POLICY_MISMATCH:%',v_finalised;
  end if;
end;
$h2_p4_real_cancel$;
`;
}

function carryForwardSql(testCase, channel) {
  const ex = testCase.scenario.stored_ex_vat;
  const vat = testCase.scenario.stored_vat[channel];
  const inc = testCase.scenario.stored_inc_vat[channel];
  const paye = testCase.expected.paye_treatment[channel];
  return `
delete from public.pay_finance_case_components where finance_case_id in
  (select finance_case_id from pg_temp.h2_p4_source_ids);
delete from public.pay_advances where id in (select finance_case_id from pg_temp.h2_p4_source_ids);
insert into public.pay_batches(id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot,
  rail_provider_snapshot,rail_env_snapshot,batch_kind_fixed,execution_commit_state,execution_commit_ref)
select '47000000-0000-4000-8000-000000000501','2026-08-21','CANCELLED',
  'MONZO_CSV','CSV','CSV','SANDBOX',pay_channel,'NOT_SUBMITTED','H2-P4-SOURCE-BATCH'
from pg_temp.h2_p4_source_ids;
insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id,candidate_tms_ref,
  candidate_display_name,paye_state,settlement_status)
select '47000000-0000-4000-8000-000000000502','47000000-0000-4000-8000-000000000501',
  candidate_id,'H2-P4-CF','H2 P4 carry source','READY','UNPAID'
from pg_temp.h2_p4_source_ids;
insert into public.pay_batch_items(id,pay_batch_candidate_id,item_type,timesheet_id,source_ref,
  description,amount_ex_vat,amount_vat,amount_inc_vat,pay_channel,is_voided,paye_treatment,
  operation_source_key)
select '47000000-0000-4000-8000-000000000503','47000000-0000-4000-8000-000000000502',
  'ADJUSTMENT_DELTA',timesheet_id,'timesheet:'||timesheet_id::text,'Cancelled source adjustment',
  ${ex},${vat},${inc},pay_channel,true,${quote(paye)},'h2-p4-source-adjustment'
from pg_temp.h2_p4_source_ids;
insert into public.pay_manual_adjustment_carry_forwards(
  id,source_pay_batch_id,source_pay_batch_item_id,source_pay_batch_candidate_id,candidate_id,
  umbrella_id,timesheet_id,pay_channel,adjustment_direction,amount_ex_vat,amount_vat,
  amount_inc_vat,amount_basis,paye_treatment,tax_treatment_json,description,reason,source_ref,
  source_operation_source_key,source_snapshot_json,status)
select '47000000-0000-4000-8000-000000000510','47000000-0000-4000-8000-000000000501',
  '47000000-0000-4000-8000-000000000503','47000000-0000-4000-8000-000000000502',
  ids.candidate_id,case when ids.pay_channel='UMBRELLA' then candidate.umbrella_id else null end,
  ids.timesheet_id,ids.pay_channel,${quote(testCase.scenario.direction)},${ex},${vat},${inc},
  'FROZEN_SOURCE_ITEM',${quote(paye)},jsonb_build_object('taxability','NON_TAXABLE'),
  'H2 P4 stored carry-forward','CERTIFIED_PRE_BANK_CANCELLATION',
  'batch-item:47000000-0000-4000-8000-000000000503','h2-p4-source-adjustment',
  jsonb_build_object('source_pay_batch_item_id','47000000-0000-4000-8000-000000000503',
                     'policy_x_authority_scope','FROZEN_SOURCE_ARTIFACT'),'PENDING_CARRY_FORWARD'
from pg_temp.h2_p4_source_ids ids join public.candidates candidate on candidate.id=ids.candidate_id;
`;
}

function mixedRecoveriesSql() {
  return `
update public.pay_advances set original_amount=60,outstanding_amount=60,weekly_due=60,
 schedule_json=jsonb_build_array(jsonb_build_object('week_start','2026-08-24','amount',60,'status','DUE')),
 created_at='2026-01-01 00:00:00+00'
where id in (select finance_case_id from pg_temp.h2_p4_source_ids);
update public.pay_finance_case_components set source_amount=60,remaining_source_amount=60,
 allocation_priority_group=0,allocation_priority_order=1
where finance_case_id in (select finance_case_id from pg_temp.h2_p4_source_ids);
insert into public.pay_advances(id,candidate_id,reason,original_amount,outstanding_amount,status,
 schedule_json,weekly_due,weeks_total,start_week_start,advance_kind,payout_status,case_type,
 taxability,routing_kind,oneoff_bank_details_required,adjustment_comment,created_by,created_at)
select '47000000-0000-4000-8000-000000000560',candidate_id,'MANUAL_ADVANCE',60,60,'ACTIVE',
 jsonb_build_array(jsonb_build_object('week_start','2026-08-24','amount',60,'status','DUE')),
 60,1,'2026-08-24','LEGACY_ADVANCE',null,'MANUAL_DEBT_ADJUSTMENT','NON_TAXABLE',
 (case when pay_channel='UMBRELLA' then 'UMBRELLA_COMPANY' else 'NORMAL_PAY_ROUTE' end)::public.pay_finance_routing_kind_enum,
 false,'H2 P4 mixed manual debt','10000000-0000-4000-8000-000000000001',
 '2026-01-02 00:00:00+00'
from pg_temp.h2_p4_source_ids;
insert into public.pay_advances(id,candidate_id,reason,original_amount,outstanding_amount,status,
 schedule_json,weekly_due,weeks_total,start_week_start,advance_kind,payout_status,case_type,
 taxability,routing_kind,oneoff_bank_details_required,adjustment_comment,created_by,created_at)
select '47000000-0000-4000-8000-000000000570',candidate_id,'LOAN',60,60,'ACTIVE',
 jsonb_build_array(jsonb_build_object('week_start','2026-08-24','amount',60,'status','DUE')),
 60,1,'2026-08-24','LOAN','PAID','PAYMENT_ADVANCE','NON_TAXABLE',
 'NORMAL_PAY_ROUTE',false,'H2 P4 mixed repayment','10000000-0000-4000-8000-000000000001',
 '2026-01-03 00:00:00+00'
from pg_temp.h2_p4_source_ids;
insert into public.pay_finance_case_components(
 id,finance_case_id,candidate_id,source_family_key,component_key_type,component_key_value,
 classification,source_pay_method,source_basis_json,source_amount,remaining_source_amount,
 is_resolution_stale,allocation_priority_group,allocation_priority_order)
select '47000000-0000-4000-8000-000000000860','47000000-0000-4000-8000-000000000560',
 candidate_id,'case:47000000-0000-4000-8000-000000000560','CASE_TOTAL','TOTAL',
 'NET_PAY_FIXED_RECOVERY',pay_channel,jsonb_build_object('case_type','MANUAL_DEBT_ADJUSTMENT',
 'visible_alias','MANUAL_DEBT_RECOVERY','paye_treatment',case when pay_channel='PAYE' then 'NET_DEDUCT' else 'NONE' end),
 60,60,false,0,2 from pg_temp.h2_p4_source_ids;
insert into public.pay_finance_case_components(
 id,finance_case_id,candidate_id,source_family_key,component_key_type,component_key_value,
 classification,source_pay_method,source_basis_json,source_amount,remaining_source_amount,
 is_resolution_stale,allocation_priority_group,allocation_priority_order)
select '47000000-0000-4000-8000-000000000870','47000000-0000-4000-8000-000000000570',
 candidate_id,'case:47000000-0000-4000-8000-000000000570','CASE_TOTAL','TOTAL',
 'NET_PAY_FIXED_RECOVERY',pay_channel,jsonb_build_object('case_type','PAYMENT_ADVANCE',
 'visible_alias','PAYMENT_ADVANCE_REPAYMENT','paye_treatment',case when pay_channel='PAYE' then 'NET_DEDUCT' else 'NONE' end),
 60,60,false,0,3 from pg_temp.h2_p4_source_ids;
`;
}

function scenarioBeforeBuildSql(testCase, channel) {
  const headroom = testCase.scenario.headroom_ex_vat ?? '100.00';
  const due = testCase.scenario.nominal_due_ex_vat ?? '10.00';
  let extra = '';
  if (testCase.scenario.kind === 'CARRY_FORWARD') extra = carryForwardSql(testCase, channel);
  else if (testCase.scenario.kind === 'MIXED_RECOVERIES') extra = mixedRecoveriesSql();
  else {
    extra = `
update public.pay_advances set original_amount=${due},outstanding_amount=${testCase.scenario.outstanding_ex_vat},
 weekly_due=case when ${due}::numeric>0 then ${due} else null end,
 schedule_json=case when ${due}::numeric>0 then jsonb_build_array(jsonb_build_object(
   'week_start','2026-08-24','amount',${due},'status','DUE')) else '[]'::jsonb end,
 payout_status=${testCase.scenario.payout_status ? `${quote(testCase.scenario.payout_status)}::public.pay_advance_payout_status_enum` : 'payout_status'}
where id in (select finance_case_id from pg_temp.h2_p4_source_ids);
update public.pay_finance_case_components set source_amount=${due},remaining_source_amount=${testCase.scenario.outstanding_ex_vat}
where finance_case_id in (select finance_case_id from pg_temp.h2_p4_source_ids);
${testCase.scenario.kind === 'ADVANCE_REPAYMENT_RELEASED' ? historicalReleasedReservationSql() : ''}
`;
  }
  return `${baseIdsSql()}
update public.timesheets_financials financial
set pay_day=round(${headroom}::numeric/10,6),total_pay_ex_vat=${headroom},
    margin_ex_vat=round(total_charge_ex_vat-${headroom}::numeric,2)
where financial.candidate_id in (select candidate_id from pg_temp.h2_p4_source_ids);
${extra}`;
}

function expectedRows(testCase, channel) {
  const paye = testCase.expected.paye_treatment[channel];
  const selected = testCase.expected.preview_disposition === 'SELECTED';
  const financeIds = testCase.scenario.kind === 'MIXED_RECOVERIES'
    ? ['BASE', '47000000-0000-4000-8000-000000000560', '47000000-0000-4000-8000-000000000570']
    : testCase.scenario.kind === 'CARRY_FORWARD' ? [] : ['BASE'];
  if (testCase.expected.preview_disposition === 'ABSENT') {
    return { financeIds, previewRows: [], itemRows: [], selectedPreviewCount: 0,
      certificateEntryCount: 0, newReservation:'0.00', activeReservation:'0.00' };
  }
  if (testCase.scenario.kind === 'MIXED_RECOVERIES') {
    const aliases = testCase.scenario.source_order;
    const previewRows = aliases.map((alias,index) => ({
      visible_alias:alias,
      amount_ex_vat:testCase.expected.canonical_amounts_by_alias[alias],amount_vat:null,amount_inc_vat:null,
      paye_treatment:channel==='PAYE'?'NET_DEDUCT':'NONE',pay_channel:channel,
      // Successful Draft creation deliberately makes every materialised source row
      // unavailable in the live Workbench.  The immutable certificate entry below,
      // rather than this post-Draft projection, proves that the row was selected.
      selected:false,status:'READY',selection_state:'NOT_SELECTABLE',
      presentation_reason:index<2?'READY_TO_PAY':'NO_PAY_HEADROOM',
      finance_case_id:null,carry_forward_id:null
    }));
    const itemRows = aliases.slice(0,2).map((alias,index)=>({item_type:alias,
      finance_case_id:null,source_ref:null,amount_ex_vat:testCase.expected.canonical_amounts_by_alias[alias],
      amount_vat:'0.00',amount_inc_vat:testCase.expected.canonical_amounts_by_alias[alias],
      pay_channel:channel,paye_treatment:channel==='PAYE'?'NET_DEDUCT':'NONE',is_voided:false,has_reservation:true}))
      .sort((left,right)=>left.item_type.localeCompare(right.item_type));
    return {financeIds,previewRows,itemRows,selectedPreviewCount:0,certificateEntryCount:2,
      newReservation:'100.00',activeReservation:'100.00'};
  }
  const amount = testCase.expected.canonical_ex_vat;
  const visible = testCase.expected.visible_alias;
  const carry = testCase.scenario.kind === 'CARRY_FORWARD';
  const vat = carry ? testCase.scenario.stored_vat[channel] : null;
  const inc = carry ? testCase.scenario.stored_inc_vat[channel] : null;
  const previewRows = [{visible_alias:visible,amount_ex_vat:amount,amount_vat:vat,amount_inc_vat:inc,
    paye_treatment:paye,pay_channel:channel,selected:false,status:'READY',
    selection_state:'NOT_SELECTABLE',presentation_reason:selected?'READY_TO_PAY':'NO_PAY_HEADROOM',
    finance_case_id:null,carry_forward_id:carry?'47000000-0000-4000-8000-000000000510':null}];
  const itemRows = selected ? [{item_type:testCase.expected.frozen_item_type,
    finance_case_id:null,source_ref:carry?'carry_forward:47000000-0000-4000-8000-000000000510':null,
    amount_ex_vat:amount,amount_vat:vat ?? '0.00',amount_inc_vat:inc ?? amount,
    pay_channel:channel,paye_treatment:paye,is_voided:false,has_reservation:!carry}] : [];
  const reservation = selected && !carry ? String(Math.abs(Number(amount))).padEnd(1) : '0';
  return {financeIds,previewRows,itemRows,selectedPreviewCount:0,
    certificateEntryCount:selected?1:0,newReservation:Number(reservation).toFixed(2),
    activeReservation:Number(reservation).toFixed(2)};
}

function caseContractSql(testCase, channel) {
  const expected = expectedRows(testCase, channel);
  const previewRowsSql = JSON.stringify(expected.previewRows)
    .replaceAll('"finance_case_id":null', '"finance_case_id":"__DYNAMIC__"');
  const itemRowsSql = JSON.stringify(expected.itemRows)
    .replaceAll('"finance_case_id":null', '"finance_case_id":"__DYNAMIC__"')
    .replaceAll('"source_ref":null', '"source_ref":"__DYNAMIC__"');
  const financeIds = expected.financeIds.map(value => value === 'BASE'
    ? '(select finance_case_id from pg_temp.h2_p4_source_ids)'
    : `${quote(value)}::uuid`);
  const dynamicExpected = `jsonb_build_object(
    'preview_count',${expected.previewRows.length},'selected_preview_count',${expected.selectedPreviewCount},
    'certificate_entry_count',${expected.certificateEntryCount},'item_count',${expected.itemRows.length},
    'new_reservation_amount_ex_vat',${quote(expected.newReservation)},
    'active_reservation_amount_ex_vat',${quote(expected.activeReservation)},
    'preview_rows',(${quote(previewRowsSql)}::jsonb),
    'item_rows',(${quote(itemRowsSql)}::jsonb)
  )`;
  return `
create temporary table pg_temp.h2_p4_case_contract(
 class_id text primary key,pay_channel text not null,target_candidate_id uuid not null,
 target_finance_case_ids uuid[] not null,target_carry_forward_id uuid,expected_json jsonb not null
) on commit drop;
insert into pg_temp.h2_p4_case_contract
select ${quote(testCase.class_id)},${quote(channel)},ids.candidate_id,
 array[${financeIds.join(',')}]::uuid[],
 ${testCase.scenario.kind==='CARRY_FORWARD'?"'47000000-0000-4000-8000-000000000510'::uuid":'null::uuid'},
 ${dynamicExpected}
from pg_temp.h2_p4_source_ids ids;
-- Bind dynamic identities after they have been produced. Values remain source-owned.
update pg_temp.h2_p4_case_contract contract
set expected_json=jsonb_set(jsonb_set(contract.expected_json,'{preview_rows}',COALESCE((
 select jsonb_agg(row_value.value
   || case when row_value.value->>'finance_case_id'='__DYNAMIC__'
           then jsonb_build_object('finance_case_id',preview.row_json->>'finance_case_id') else '{}'::jsonb end
   order by row_value.ordinality)
 from jsonb_array_elements(contract.expected_json->'preview_rows') with ordinality row_value(value,ordinality)
 left join lateral (
   select p.row_json from public.banking_pay_workbench_preview_rows p
   where p.session_id='43000000-0000-4000-8000-000000000005'
     and p.row_json->>'line_type'=row_value.value->>'visible_alias'
   order by p.row_ordinal,p.id limit 1
 ) preview on true
),'[]'::jsonb),false),'{item_rows}',COALESCE((
 select jsonb_agg(row_value.value
   || case when row_value.value->>'finance_case_id'='__DYNAMIC__'
           then jsonb_build_object('finance_case_id',preview.row_json->>'finance_case_id') else '{}'::jsonb end
   || case when row_value.value->>'source_ref'='__DYNAMIC__'
           then jsonb_build_object('source_ref','advance:'||(preview.row_json->>'finance_case_id')) else '{}'::jsonb end
   order by row_value.ordinality)
 from jsonb_array_elements(contract.expected_json->'item_rows') with ordinality row_value(value,ordinality)
 left join lateral (
   select p.row_json from public.banking_pay_workbench_preview_rows p
   where p.session_id='43000000-0000-4000-8000-000000000005'
     and p.row_json->>'line_type'=case row_value.value->>'item_type'
       when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE_REPAYMENT'
       else row_value.value->>'item_type' end
   order by p.row_ordinal,p.id limit 1
 ) preview on true
),'[]'::jsonb),false);
`;
}

function buildRuntimeSql(testCase, channel, database, databaseUser = 'postgres') {
  const bridge = read(bridgePath);
  const canonical = read(canonicalPath);
  const selectionFixture = read(selectionFixturePath);
  const certificateOwner = read(certificateOwnerPath);
  const parityOwner = read(parityOwnerPath);
  const insertItemsOwner = read(insertItemsOwnerPath);
  const carryForwardTransportOwner = read(carryForwardTransportOwnerPath);
  const cancelledRepaymentOwner = read(cancelledRepaymentOwnerPath);
  const carryForwardSelectionOwner = read(carryForwardSelectionOwnerPath);
  const carryForwardPreviewContractOwner = read(carryForwardPreviewContractOwnerPath);
  const carryForwardAllocationOwner = read(carryForwardAllocationOwnerPath);
  const mixedRecoveryOwner = read(mixedRecoveryOwnerPath);
  const bridgeMarker = '\\ir 02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
  let bridgePrefix = bridge.split(bridgeMarker)[0];
  if (bridge.split(bridgeMarker).length !== 2) throw new Error('V8 bridge splice changed');
  bridgePrefix = `${bridgePrefix}\n${stripExactOuterTransaction(certificateOwner,'certificate owner')}\n${parityOwner}\n${insertItemsOwner}\n${carryForwardTransportOwner}\n${cancelledRepaymentOwner}\n${carryForwardSelectionOwner}\n${carryForwardPreviewContractOwner}\n${carryForwardAllocationOwner}\n${mixedRecoveryOwner}`;

  const canonicalStop = 'insert into public.banking_pay_operations(';
  const stopIndex = canonical.indexOf(canonicalStop);
  if (stopIndex < 0) throw new Error('canonical operation splice changed');
  let prefix = canonical.slice(0,stopIndex);
  let selection = replaceOnce(selectionFixture,"current_database()<>'banking_modal_v2_test'",
    `current_database()<>${quote(database)}`,'selection database guard');
  prefix = replaceOnce(prefix,"current_database()<>'banking_modal_v2_test'",
    `current_database()<>${quote(database)}`,'canonical database guard');
  prefix = replaceOnce(prefix,'\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',selection,
    'selection include');
  prefix = replaceOnce(prefix,'create temporary table pg_temp.h2_f013_canonical_lines(',
    `${scenarioBeforeBuildSql(testCase,channel)}\n${frozenSettingsAuthoritySql}\ncreate temporary table pg_temp.h2_f013_canonical_lines(`,
    'P4 source scenario injection');
  prefix = replaceBlock(prefix,'    if build_result#>>\'{source_build_canonical_diagnostics,source_rows_seen}\'<>\'1\'',
    '    end if;','', 'canonical fixed build count guard');
  prefix = replaceBlock(prefix,'do $canonical_shape$','$canonical_shape$;','',
    'canonical fixed shape guard');
  prefix = replaceOnce(prefix,
    `'OPEN',1,'[]',true,true,1,1,1,0,0,1,1,0,0,3,2,'READY',1,`,
    `'OPEN',1,'[]',true,true,1,1,1,0,0,1,1,0,0,\n  (select count(*) from pg_temp.h2_f013_canonical_lines),\n  (select count(*) from pg_temp.h2_f013_canonical_lines where coalesce((line_json->>'selection_allowed')::boolean,false)),\n  'READY',1,`, 'dynamic session counts');
  prefix = replaceOnce(prefix,
    `       or publication_result->>'preview_row_count'<>'3'\n       or publication_result->>'selected_row_count'<>'2' then`,
    `       or (publication_result->>'preview_row_count')::integer<>(select count(*) from pg_temp.h2_f013_canonical_lines)\n       or (publication_result->>'selected_row_count')::integer<>(select count(*) from pg_temp.h2_f013_canonical_lines where coalesce((line_json->>'selection_allowed')::boolean,false)) then`,
    'dynamic publication counts');
  prefix = replaceOnce(prefix,
    `set server_selected_preview_row_ids=(\n  select jsonb_agg(preview_row.id::text order by preview_row.row_ordinal,preview_row.id)\n  from public.banking_pay_workbench_preview_rows as preview_row\n  where preview_row.session_id=session_update.id\n    and preview_row.selected=true\n)`,
    `set server_selected_preview_row_ids=coalesce((\n  select jsonb_agg(preview_row.id::text order by preview_row.row_ordinal,preview_row.id)\n  from public.banking_pay_workbench_preview_rows as preview_row\n  where preview_row.session_id=session_update.id\n    and preview_row.selected=true\n),'[]'::jsonb)`,
    'empty selected-set fixture authority');

  const assertionSource = read(runtimePath);
  const assertion = assertionSource.slice(
    assertionSource.indexOf('-- H2_P4_ASSERTION_BEGIN')+'-- H2_P4_ASSERTION_BEGIN'.length,
    assertionSource.indexOf('-- H2_P4_ASSERTION_END'));
  const flow = `
insert into public.candidates(id,tms_ref,display_name,pay_method,active,account_holder,sort_code,account_number,bank_details_hash)
values ('47000000-0000-4000-8000-000000000999','H2-P4-UNRELATED','H2 P4 unrelated Candidate',
 'PAYE',true,'H2 P4 unrelated','010203','99887766','h2-p4-unrelated-bank');
create temporary table pg_temp.h2_p4_unrelated_before(digest_sha256 text not null) on commit drop;
insert into pg_temp.h2_p4_unrelated_before
select private.pay_workbench_settled_certificate_sha256_text_v8(
 private.pay_workbench_settled_certificate_stable_stringify_v8(to_jsonb(candidate_row)))
from public.candidates candidate_row where id='47000000-0000-4000-8000-000000000999';
${caseContractSql(testCase,channel)}
\\if ${expectedRows(testCase,channel).certificateEntryCount > 0 ? 'true' : 'false'}
insert into public.banking_pay_operations(
 id,operation_type,status,phase,actor_user_id,workbench_session_id,idempotency_key,input_json,config_json,progress_json
) values (
 '43000000-0000-4000-8000-000000000100','DRAFT_CREATE','RUNNING','VALIDATE_SESSION',
 '10000000-0000-4000-8000-000000000001','43000000-0000-4000-8000-000000000005',
 'h2-p4-${testCase.class_id}-${channel.toLowerCase()}',
 '{"pay_date":"2026-08-28","week_start":"2026-08-24","pay_channel_scope":"ALL"}','{}','{}');
update public.banking_pay_operations operation_row
set input_json=operation_row.input_json||jsonb_build_object(
 'expected_workbench_progress_counter_version',session_row.progress_counter_version::text,
 'expected_workbench_selected_preview_row_ids',(
   select jsonb_agg(id::text order by row_ordinal,id) from public.banking_pay_workbench_preview_rows
   where session_id=session_row.id and selected and selection_state='SELECTED' and status='READY'),
 'selection_review_contract_version',1,
 'selection_reviewed_by_user_id','10000000-0000-4000-8000-000000000001')
from public.banking_pay_workbench_sessions session_row
where operation_row.id='43000000-0000-4000-8000-000000000100'
  and session_row.id=operation_row.workbench_session_id;
do $h2_p4_prepare_scope$
declare v_ids jsonb; v_prepare jsonb; v_scope jsonb; v_scope_ids jsonb; v_seed jsonb;
        v_result jsonb; v_replay jsonb; v_receipt text; v_batch_id uuid;
        v_saved_phase text; v_items_before integer; v_items_after integer; i integer;
        v_carry_item_id uuid; v_original_ex_vat numeric; v_original_inc_vat numeric;
        v_carry_allocation_id uuid; v_original_allocation_basis jsonb;
        v_expected_rejection_observed boolean := false;
begin
 select jsonb_agg(id::text order by row_ordinal,id) into v_ids
 from public.banking_pay_workbench_preview_rows
 where session_id='43000000-0000-4000-8000-000000000005' and selected;
 v_prepare:=public.pay_workbench_prepare_draft('43000000-0000-4000-8000-000000000005',
   '10000000-0000-4000-8000-000000000001',v_ids,'ALL',null,false,false,null,null,
   '43000000-0000-4000-8000-000000000100',true,false);
 update public.banking_pay_operations set phase='SEED_CANDIDATE_SCOPE'
 where id='43000000-0000-4000-8000-000000000100';
 select to_jsonb(scope_seed) into v_scope from public.pay_workbench_prepare_draft_scope_seed(
   '43000000-0000-4000-8000-000000000100','43000000-0000-4000-8000-000000000005',
   '10000000-0000-4000-8000-000000000001',v_ids,'ALL','{}') scope_seed;
 update public.banking_pay_operations set phase='SEED_ALLOCATION_ROWS'
 where id='43000000-0000-4000-8000-000000000100';
 select jsonb_agg(id::text order by chunk_sequence,id) into v_scope_ids
 from public.banking_pay_operation_candidate_scope
 where operation_id='43000000-0000-4000-8000-000000000100';
  select to_jsonb(seed) into v_seed from public.pay_workbench_prepare_draft_allocation_rows_seed(
    '43000000-0000-4000-8000-000000000100',v_scope_ids) seed;
 select receipt_digest_sha256 into strict v_receipt
 from private.banking_pay_draft_frozen_stage_receipts_v8
 where operation_id='43000000-0000-4000-8000-000000000100'
   and stage_kind='CERTIFICATE_PARTITION_REFS' and stage_status='TERMINAL' and has_more=false
 order by page_sequence desc limit 1;
 for i in 1..100 loop
   v_result:=public.banking_pay_draft_advance_bounded_v8(
     '43000000-0000-4000-8000-000000000100','H2_V8_P4_FINANCE_LIFECYCLE',v_receipt);
   exit when v_result->>'work_kind'='READY_FOR_TERMINAL_FINISH';
 end loop;
 if v_result->>'work_kind'<>'READY_FOR_TERMINAL_FINISH' then
   raise exception 'H2_P4_DRAFT_DID_NOT_TERMINATE:%',v_result;
 end if;

 -- Simulate a lost successful INSERT_ITEMS response by replaying that exact
 -- bounded stage after all downstream work has linked the rows. It must reuse
 -- the authoritative items and must never duplicate or repair economics.
 select phase into strict v_saved_phase from public.banking_pay_operations
 where id='43000000-0000-4000-8000-000000000100';
 select id into strict v_batch_id from public.pay_batches
 where source_workbench_session_id='43000000-0000-4000-8000-000000000005'
 order by created_at_utc desc,id limit 1;
 select count(*) into v_items_before from public.pay_batch_items item
 join public.pay_batch_candidates candidate on candidate.id=item.pay_batch_candidate_id
 where candidate.pay_batch_id=v_batch_id and coalesce(item.is_voided,false)=false;
 update public.banking_pay_operations set phase='INSERT_ITEMS'
 where id='43000000-0000-4000-8000-000000000100';
 if upper(coalesce(current_setting('cloudtms.h2_p4_replay_mode',true),'NORMAL'))='MALFORMED_IDENTITY' then
   if ${testCase.scenario.kind==='CARRY_FORWARD'?'true':'false'} is not true then
     raise exception 'H2_P4_MALFORMED_IDENTITY_MODE_REQUIRES_CARRY_FORWARD';
   end if;
   select id,allocation_basis_json
   into strict v_carry_allocation_id,v_original_allocation_basis
   from public.banking_pay_operation_candidate_allocation_rows
   where operation_id='43000000-0000-4000-8000-000000000100'
     and source_ref='carry_forward:47000000-0000-4000-8000-000000000510';
   update public.banking_pay_operation_candidate_allocation_rows
   set allocation_basis_json=jsonb_set(
     allocation_basis_json,'{line,manual_adjustment_carry_forward_id}',
     to_jsonb('47000000-0000-4000-8000-000000000599'::text),false)
   where id=v_carry_allocation_id;
   begin
     v_replay:=public.pay_batch_insert_items_from_preview(
       v_batch_id,'10000000-0000-4000-8000-000000000001',
       '43000000-0000-4000-8000-000000000100',v_scope_ids);
   exception when others then
     if sqlstate='P0001' and sqlerrm='DRAFT_CARRY_FORWARD_HANDOFF_INVALID' then
       v_expected_rejection_observed:=true;
     else
       raise;
     end if;
   end;
   if not v_expected_rejection_observed
      or not exists (
        select 1 from public.banking_pay_operation_candidate_allocation_rows
        where id=v_carry_allocation_id
          and allocation_basis_json#>>'{line,manual_adjustment_carry_forward_id}'='47000000-0000-4000-8000-000000000599'
      ) then
     raise exception 'H2_P4_MALFORMED_IDENTITY_WAS_ACCEPTED_OR_REPAIRED';
   end if;
   update public.banking_pay_operation_candidate_allocation_rows
   set allocation_basis_json=v_original_allocation_basis
   where id=v_carry_allocation_id;
   v_replay:=jsonb_build_object('ok',true,'inserted_item_rows',0,'failed_item_rows',0);
 elseif upper(coalesce(current_setting('cloudtms.h2_p4_replay_mode',true),'NORMAL'))='WRONG_LINK' then
   if ${testCase.scenario.kind==='CARRY_FORWARD'?'true':'false'} is not true then
     raise exception 'H2_P4_WRONG_LINK_MODE_REQUIRES_CARRY_FORWARD';
   end if;
   select item.id,item.amount_ex_vat,item.amount_inc_vat
   into strict v_carry_item_id,v_original_ex_vat,v_original_inc_vat
   from public.pay_batch_items item
   join public.pay_batch_candidates candidate on candidate.id=item.pay_batch_candidate_id
   where candidate.pay_batch_id=v_batch_id
     and item.source_ref='carry_forward:47000000-0000-4000-8000-000000000510'
     and coalesce(item.is_voided,false)=false;
   update public.pay_batch_items
   set amount_ex_vat=v_original_ex_vat+0.01,
       amount_inc_vat=v_original_inc_vat+0.01
   where id=v_carry_item_id;
   begin
     v_replay:=public.pay_batch_insert_items_from_preview(
       v_batch_id,'10000000-0000-4000-8000-000000000001',
       '43000000-0000-4000-8000-000000000100',v_scope_ids);
   exception when others then
     if sqlstate='P0001' and sqlerrm='DRAFT_CARRY_FORWARD_PREEXISTING_ITEM_LINK_MISMATCH' then
       v_expected_rejection_observed:=true;
     else
       raise;
     end if;
   end;
   if not v_expected_rejection_observed
      or not exists (
        select 1 from public.pay_batch_items
        where id=v_carry_item_id
          and amount_ex_vat=v_original_ex_vat+0.01
          and amount_inc_vat=v_original_inc_vat+0.01
      ) then
     raise exception 'H2_P4_WRONG_LINK_WAS_ACCEPTED_OR_REPAIRED';
   end if;
   update public.pay_batch_items
   set amount_ex_vat=v_original_ex_vat,amount_inc_vat=v_original_inc_vat
   where id=v_carry_item_id;
   v_replay:=jsonb_build_object('ok',true,'inserted_item_rows',0,'failed_item_rows',0);
 else
   v_replay:=public.pay_batch_insert_items_from_preview(
     v_batch_id,'10000000-0000-4000-8000-000000000001',
     '43000000-0000-4000-8000-000000000100',v_scope_ids);
 end if;
 select count(*) into v_items_after from public.pay_batch_items item
 join public.pay_batch_candidates candidate on candidate.id=item.pay_batch_candidate_id
 where candidate.pay_batch_id=v_batch_id and coalesce(item.is_voided,false)=false;
 if coalesce((v_replay->>'ok')::boolean,false) is not true
    or coalesce((v_replay->>'inserted_item_rows')::integer,0)<>0
    or coalesce((v_replay->>'failed_item_rows')::integer,0)<>0
    or v_items_after<>v_items_before then
   raise exception 'H2_P4_RESPONSE_LOSS_REPLAY_MISMATCH:%',jsonb_build_object(
     'result',v_replay,'items_before',v_items_before,'items_after',v_items_after);
 end if;
 update public.banking_pay_operations set phase=v_saved_phase
 where id='43000000-0000-4000-8000-000000000100';
end;
$h2_p4_prepare_scope$;
\\endif
${assertion}
ROLLBACK;
`;
  return mapLogicalPostgresOwnerForDisposableProvider(
    `${bridgePrefix}\n${prefix}\n${flow}`,
    databaseUser,
  );
}

function runRuntime() {
  const args = new Map();
  for (let index=3;index<process.argv.length;index+=2) args.set(process.argv[index],process.argv[index+1]);
  const container=args.get('--container');
  const database=args.get('--database');
  const databaseUser=args.get('--user')??'postgres';
  const onlyCase=args.get('--case');
  const onlyChannel=args.get('--channel');
  const replayMode=(args.get('--replay-mode')??'NORMAL').toUpperCase();
  if (!/^[A-Za-z0-9_.-]+$/.test(container??'')
      || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(database??'')
      || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(databaseUser)) {
    throw new Error('safe --container, --database and --user values required');
  }
  const fixture=json(fixturePath);
  const cases=onlyCase?fixture.classes.filter(row=>row.class_id===onlyCase):fixture.classes;
  const channels=onlyChannel?[onlyChannel]:fixture.common_assertions.channels;
  if (onlyCase&&cases.length!==1) throw new Error(`unknown P4 case ${onlyCase}`);
  if (channels.some(value=>!fixture.common_assertions.channels.includes(value))) throw new Error('unknown P4 channel');
  if (!['NORMAL','WRONG_LINK','MALFORMED_IDENTITY'].includes(replayMode)) throw new Error('unknown P4 replay mode');
  if (replayMode!=='NORMAL' && cases.some(value=>value.scenario.kind!=='CARRY_FORWARD')) {
    throw new Error(`${replayMode} replay mode is carry-forward only`);
  }
  const version=spawnSync('docker',['exec',container,'psql','-X','-U',databaseUser,'-d',database,'-Atc','select version()'],{encoding:'utf8'});
  if (version.status!==0) throw new Error(version.stderr);
  const results=[];
  for (const testCase of cases) for (const channel of channels) {
    const sql=buildRuntimeSql(testCase,channel,database,databaseUser);
    if ((sql.match(/^\s*ROLLBACK\s*;\s*$/gim)??[]).length!==1 || (sql.match(/^\s*COMMIT\s*;\s*$/gim)??[]).length) {
      throw new Error(`${testCase.class_id}/${channel} transaction envelope invalid`);
    }
    const started=performance.now();
    const execution=spawnSync('docker',['exec','-i','-e',
      `PGOPTIONS=-c cloudtms.h2_f013_variant_ordinal=${variantOrdinal(testCase,channel)} -c cloudtms.h2_p4_replay_mode=${replayMode} -c jit=off`,
      container,'psql','-X','-v','ON_ERROR_STOP=1','-U',databaseUser,'-d',database],
      {input:sql,encoding:'utf8',maxBuffer:96*1024*1024});
    const output=`${execution.stdout??''}\n${execution.stderr??''}`;
    if (execution.status!==0 || !output.includes('H2_P4_FINANCE_LIFECYCLE_RUNTIME_PASS=')) {
      throw new Error(`${testCase.class_id}/${channel} failed ${execution.status}:\n${output.split(/\r?\n/).slice(-160).join('\n')}`);
    }
    results.push({class_id:testCase.class_id,pay_channel:channel,replay_mode:replayMode,status:'PASS',
      transaction_outcome:'ROLLBACK',elapsed_ms:Math.round((performance.now()-started)*1000)/1000});
  }
  process.stdout.write(`${JSON.stringify({engine:version.stdout.trim(),container,database,database_user:databaseUser,
    case_count:results.length,pass_count:results.length,fail_count:0,results},null,2)}\n`);
}

if (process.argv[2]==='--run-runtime') { runRuntime(); return; }

test('P4 fixture binds the exact twelve open finance lifecycle/headroom classes',()=>{
  const fixture=json(fixturePath); const binding=json(bindingPath); const policy=json(policyPath);
  const bound=binding.class_execution_bindings.filter(row=>expectedClasses.includes(row.class_id));
  const policyRows=policy.finite_equivalence_classes.filter(row=>expectedClasses.includes(row.class_id));
  assert.equal(fixture.contract,'BANKING_PAY_DRAFT_V8_FINANCE_LIFECYCLE_HEADROOM_P4_V1');
  assert.deepEqual(fixture.classes.map(row=>row.class_id),expectedClasses);
  assert.deepEqual(bound.map(row=>row.class_id),expectedClasses);
  assert.deepEqual(policyRows.map(row=>row.class_id),expectedClasses);
  assert.equal(new Set(expectedClasses).size,12);
  assert.equal(fixture.scope.policy_delta_allowed,false);
  assert.equal(fixture.scope.production_runtime_edits,false);
  assert.equal(fixture.scope.provider_payment_settlement_remittance_actions,false);
  assert.deepEqual(fixture.common_assertions.channels,['PAYE','UMBRELLA']);
  assert.equal(fixture.common_assertions.totals_are_secondary,true);
  for (const policyRow of policyRows) {
    const row=fixture.classes.find(candidate=>candidate.class_id===policyRow.class_id);
    assert.equal(row.policy_condition,policyRow.condition);
    assert.equal(policyRow.proof_rule,fixture.frozen_policy_contract.proof_rule);
    assert.equal(policyRow.channel,'PAYE+UMBRELLA');
  }
});

test('P4 source-owner identities and frozen policy are byte-bound',()=>{
  const fixture=json(fixturePath);
  assert.equal(sha256(read(policyPath)),fixture.frozen_policy_contract.sha256);
  for (const owner of fixture.owners) assert.equal(sha256(read(owner.path)),owner.sha256,owner.path);
});

test('P4 owners retain distinct lifecycle, headroom, carry-forward and finance policies',()=>{
  const collect=read('supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql');
  const canonical=read('supabase/repeatable/17082026_2052_pay_finance_resolution_cancel_authority.sql');
  const overlay=read('supabase/repeatable/09082026_0712_banking_pay_semantic_ready_helpers.sql');
  const finance=read('supabase/repeatable/21072026_1235_49_pay_batch_apply_finance_adjustments.sql');
  const lifecycle=read('supabase/migrations/19072026_0120_fix_finance_case_resolution_register.sql');
  const cancellation=read('supabase/repeatable/04082026_1158_pay_pre_bank_cancel_apply_work_item.sql');
  assert.match(collect,/upper\(coalesce\(vfcr\.status::text,''\)\) = 'ACTIVE'/);
  assert.match(collect,/coalesce\(vfcr\.outstanding_amount,0\) > 0/);
  assert.match(collect,/pay_manual_adjustment_carry_forwards as carry_forward_rows/);
  assert.match(collect,/PENDING_CARRY_FORWARD/);
  assert.match(canonical,/semantic_recovery_sort_at_utc nulls last/);
  assert.match(canonical,/greatest\(ranked\.ordinary_positive_headroom-ranked\.prior_semantic_recovery_amount,0\)/);
  assert.match(canonical,/'MANUAL_ADJUSTMENT_CARRY_FORWARD'/);
  assert.match(canonical,/'PAYMENT_ADVANCE_REPAYMENT'/);
  assert.match(canonical,/vfcr\.case_type = 'PAYMENT_ADVANCE' and upper\(coalesce\(vfcr\.payout_status::text,''\)\) = 'PAID'/);
  assert.match(canonical,/when fcrr\.case_type = 'PAYMENT_ADVANCE' and upper\(coalesce\(fcrr\.lifecycle_status_display,''\)\) <> 'PAID' then 'LOAN_PAYOUT'/);
  assert.match(overlay,/selected_positive_headroom_ex_vat/);
  assert.match(overlay,/ORDER BY recovery_base\.row_ordinal, recovery_base\.id/);
  assert.match(finance,/STAGE_16C_APPLY_PAYMENT_ADVANCE_REPAYMENTS/);
  assert.match(finance,/spr\.line_type = 'PAYMENT_ADVANCE_REPAYMENT'/);
  assert.match(lifecycle,/latest_finance_batch_cancelled_at_utc IS NOT NULL[\s\S]{0,300}THEN 'Cancelled'/);
  assert.match(lifecycle,/upper\(COALESCE\(\(br\.payout_status\)::text, ''::text\)\) = 'PAID'::text/);
  assert.match(lifecycle,/THEN 'Paid'::text/);
  assert.match(cancellation,/released the reservation without changing the live component outstanding balance/i);
});

test('P4 carry-forward corrections preserve row-backed V8 ownership and fail closed on identity drift',()=>{
  const allocationGenerator=read('scripts/generate-banking-pay-manual-carry-forward-allocation-seed-v8.mjs');
  const allocationOwner=read(carryForwardAllocationOwnerPath);
  const transportOwner=read(carryForwardTransportOwnerPath);
  assert.match(allocationGenerator,/02092026_2312_banking_pay_draft_row_backed_orchestration_v8\.sql/);
  assert.doesNotMatch(allocationGenerator,/08082026_0717_banking_pay_draft_allocation_source_owned_v1\.sql/);
  assert.match(allocationOwner,/private\.pay_workbench_draft_scope_line_rows_v8/);
  assert.match(allocationOwner,/MANUAL_ADJUSTMENT_CARRY_FORWARD/);
  assert.match(transportOwner,/DRAFT_CARRY_FORWARD_HANDOFF_INVALID/);
  assert.match(transportOwner,/DRAFT_CARRY_FORWARD_PREEXISTING_ITEM_LINK_MISMATCH/);
  assert.match(transportOwner,/MANUAL_CREDIT_PAYOUT/);
  assert.match(transportOwner,/pay_manual_adjustment_carry_forwards/);
});

test('P4 fixture mutations cannot merge categories or relax policy boundaries',()=>{
  const fixture=json(fixturePath);
  const validate=value=>{
    assert.deepEqual(value.classes.map(row=>row.class_id),expectedClasses);
    assert.equal(new Set(value.classes.map(row=>row.class_id)).size,12);
    assert.deepEqual(value.common_assertions.channels,['PAYE','UMBRELLA']);
    assert.equal(value.scope.policy_delta_allowed,false);
    assert.equal(value.scope.production_runtime_edits,false);
    assert.equal(value.scope.provider_payment_settlement_remittance_actions,false);
    assert.equal(value.frozen_policy_contract.sha256,sha256(read(policyPath)));
    assert.equal(value.classes.find(row=>row.class_id==='advance_part_repaid_residual').expected.frozen_item_type,'LOAN_REPAYMENT');
    assert.equal(value.classes.find(row=>row.class_id==='carry_forward_credit').expected.frozen_item_type,'MANUAL_CREDIT_PAYOUT');
  };
  validate(fixture);
  const mutations=[
    value=>value.classes.pop(),value=>value.classes.push(structuredClone(value.classes[0])),
    value=>{value.common_assertions.channels=['PAYE'];},value=>{value.scope.policy_delta_allowed=true;},
    value=>{value.scope.production_runtime_edits=true;},value=>{value.scope.provider_payment_settlement_remittance_actions=true;},
    value=>{value.frozen_policy_contract.sha256='0'.repeat(64);},
    value=>{value.classes.find(row=>row.class_id==='advance_part_repaid_residual').expected.frozen_item_type='PAYMENT_ADVANCE_REPAYMENT';},
    value=>{value.classes.find(row=>row.class_id==='carry_forward_credit').expected.frozen_item_type='ADJUSTMENT_DELTA';}
  ];
  for (const mutate of mutations) { const changed=structuredClone(fixture); mutate(changed); assert.throws(()=>validate(changed)); }
});

test('P4 runtime assertion is rollback-only and never invokes external-action owners',()=>{
  const sql=read(runtimePath);
  assert.match(sql,/^BEGIN;$/m); assert.match(sql,/^SET LOCAL jit = off;$/m);
  assert.match(sql,/^SET LOCAL statement_timeout = '15s';$/m);
  assert.match(sql,/^SET LOCAL lock_timeout = '1500ms';$/m);
  assert.match(sql,/^SET LOCAL idle_in_transaction_session_timeout = '30s';$/m);
  assert.equal((sql.match(/^ROLLBACK;$/gm)??[]).length,1);
  assert.doesNotMatch(sql,/\b(?:pay_settle|provider_submit|remittance_generate|bank_transfer_submit)\s*\(/i);
});

test('P4 result ledger records dual-engine closure without policy inference',()=>{
  const result=json(resultPath);
  assert.equal(result.status,'LOCAL_DUAL_ENGINE_P4_PASS_POLICY_PRESERVED');
  assert.equal(result.counts.classes_total,12);
  assert.equal(result.counts.normal_channel_cases_per_engine,24);
  assert.equal(result.counts.normal_channel_cases_pass_total,48);
  assert.equal(result.counts.wrong_link_fail_closed_cases_pass,8);
  assert.equal(result.counts.malformed_identity_fail_closed_cases_pass,8);
  assert.equal(result.counts.runtime_cases_pass_total,64);
  assert.equal(result.counts.runtime_cases_fail_total,0);
  assert.equal(result.counts.policy_or_economic_changes,0);
  assert.equal(result.counts.provider_payment_settlement_remittance_actions,0);
  assert.equal(result.local_runtime_owner_corrections_made,true);
  assert.equal(result.installed_in_miget,false);
  assert.equal(result.engine_evidence.pg17.normal_cases,'24/24 PASS');
  assert.equal(result.engine_evidence.pg18.normal_cases,'24/24 PASS');
  assert.equal(result.closed_divergences.length,3);
  assert.deepEqual(new Set(result.closed_divergences.map(row=>row.policy_delta)),new Set([false]));
  assert.equal(result.replay_and_fail_closed_evidence.zero_partial_state,true);
  assert.ok(result.remaining_gates.includes('TRUE_V1_V8_TYPED_PARITY_OR_AUTHORITATIVE_HISTORICAL_ORACLE'));
});
