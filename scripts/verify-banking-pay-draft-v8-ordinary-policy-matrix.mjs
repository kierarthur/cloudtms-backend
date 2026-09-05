import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index];
  const value = process.argv[index + 1];
  if (!key?.startsWith('--') || !value) throw new Error(`Invalid argument at position ${index}`);
  args.set(key.slice(2), value);
}
const container = args.get('container');
const database = args.get('database');
const expectedEngine = args.get('engine');
const onlyCase = args.get('case');
if (!container || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(container)) {
  throw new Error('A safe --container value is required');
}
if (!database || !/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(database)) {
  throw new Error('A safe --database value is required');
}
if (expectedEngine && !/^PostgreSQL (17\.11|18\.6)\b/.test(expectedEngine)) {
  throw new Error('--engine must start with PostgreSQL 17.11 or PostgreSQL 18.6');
}

const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const bridgePath = 'tests/03092026_1600_banking_pay_draft_v8_finance_category_runtime.sql';
const canonicalPath = 'tests/02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
const fixturePath = 'tests/fixtures/28082026_1429_banking_pay_selection_setup.sql';
const casesPath = 'tests/fixtures/banking-pay-draft-v8-ordinary-policy-cases-v1.json';
const bridge = read(bridgePath);
const canonical = read(canonicalPath);
const selectionFixture = read(fixturePath);
const caseContractText = read(casesPath);
const caseContract = JSON.parse(caseContractText);

function replaceOnce(text, needle, replacement, label) {
  const parts = text.split(needle);
  if (parts.length !== 2) throw new Error(`${label} expected exactly once; observed ${parts.length - 1}`);
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
const quoteLiteral = value => `'${String(value).replaceAll("'", "''")}'`;

const frozenSettingsAuthoritySql = `
insert into public.client_settings(
  id,client_id,effective_from,autoprocess_hr
) values (
  '43000000-0000-4000-8000-000000000021',
  '43000000-0000-4000-8000-000000000020','2026-01-01',false
);
do $h2_p1_freeze_settings_authority$
declare
  v_row record;
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
    raise exception 'H2_P1_SETTINGS_AUTHORITY_NOT_FROZEN';
  end if;
end;
$h2_p1_freeze_settings_authority$;
`;

const bridgeMarker = '\\ir 02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
const canonicalMarker = '\\if true';
if (bridge.split(bridgeMarker).length !== 2 || canonical.split(canonicalMarker).length < 2) {
  throw new Error('The immutable finance fixture splice boundary changed');
}

let fixture = replaceOnce(
  selectionFixture,
  "current_database()<>'banking_modal_v2_test'",
  `current_database()<>${quoteLiteral(database)}`,
  'selection fixture database guard'
);
let prefix = canonical.split(canonicalMarker)[0];
prefix = replaceOnce(
  prefix,
  "current_database()<>'banking_modal_v2_test'",
  `current_database()<>${quoteLiteral(database)}`,
  'canonical fixture database guard'
);
prefix = replaceOnce(prefix, '\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql', fixture, 'selection fixture include');

const legacyScopeAssertion = /     or not exists \(\s*select 1\s*from public\.banking_pay_operation_candidate_scope as scope_row\s*cross join lateral jsonb_array_elements\(\s*scope_row\.selected_canonical_preview_lines_json\s*\) as selected\(line_json\)\s*where scope_row\.operation_id='43000000-0000-4000-8000-000000000100'\s*and selected\.line_json->>'finance_case_id'=selected_finance_row->>'finance_case_id'\s*and selected\.line_json->>'line_type'=selected_finance_row->>'item_type'\s*and selected\.line_json->>'row_key'=selected_finance_row->>'row_key'\s*\) then/s;
const normalizedScopeAssertion = `     or not exists (
       select 1
       from private.banking_pay_draft_frozen_constituent_payloads_v8 as payload
       where payload.operation_id='43000000-0000-4000-8000-000000000100'
         and payload.finance_case_id=(selected_finance_row->>'finance_case_id')::uuid
         and payload.payload_json->>'line_type'=selected_finance_row->>'item_type'
         and payload.payload_json->>'row_key'=selected_finance_row->>'row_key'
     ) then`;
const scopeMatches = prefix.match(new RegExp(legacyScopeAssertion.source, 'gs')) ?? [];
if (scopeMatches.length !== 1) throw new Error(`Expected one legacy scope assertion, found ${scopeMatches.length}`);
prefix = prefix.replace(legacyScopeAssertion, normalizedScopeAssertion);

const buildGuardStart = `    if build_result#>>'{source_build_canonical_diagnostics,source_rows_seen}'<>'1'`;
const buildGuardEnd = `    end if;`;
prefix = replaceBlock(prefix, buildGuardStart, buildGuardEnd, `    if build_result#>>'{source_build_canonical_diagnostics,source_rows_seen}'<>
         (select expected_timesheet_count::text from pg_temp.h2_p1_case)
       or build_result->>'ready_preview_line_count'<>
         ((select expected_ordinary_constituents from pg_temp.h2_p1_case)+1)::text
       or build_result->>'blocked_preview_line_count'<>'0' then
      raise exception 'H2_P1_CANONICAL_BUILD_NOT_READY:%:%',
        variant.ordinal,jsonb_build_object(
          'build',build_result-'hidden_recovery_template_lines',
          'fixture_rows',(select jsonb_agg(jsonb_build_object(
            'timesheet_id',financial.timesheet_id,'is_current',financial.is_current,
            'processing_status',financial.processing_status,'pay_method',financial.pay_method,
            'candidate_id',financial.candidate_id,'timesheet_current',timesheet.is_current,
            'week_ending_date',timesheet.week_ending_date,'sheet_scope',timesheet.sheet_scope
          ) order by financial.timesheet_id)
          from public.timesheets_financials financial
          join public.timesheets timesheet on timesheet.timesheet_id=financial.timesheet_id
          where financial.candidate_id=variant.candidate_id)
        );
    end if;`, 'canonical build guard');

const shapeStart = `do $canonical_shape$`;
const shapeEnd = `$canonical_shape$;`;
prefix = replaceBlock(prefix, shapeStart, shapeEnd, `do $h2_p1_canonical_shape$
declare
  v_actual_count integer;
  v_expected_count integer;
  v_context_count integer;
  v_finance_count integer;
  v_component_mismatch integer;
  v_is_forced boolean;
begin
  select count(*) filter(where coalesce((line_json->>'selection_allowed')::boolean,false)),
         count(*) filter(where line_json->>'line_type'='TIMESHEET_PAYMENT'
           and nullif(coalesce(line_json#>>'{economic_key,key_type}',line_json->>'component_key_type'),'') is null),
         count(*) filter(where nullif(line_json->>'finance_case_id','') is not null)
  into v_actual_count,v_context_count,v_finance_count
  from pg_temp.h2_f013_canonical_lines;
  select expected_ordinary_constituents+1,forced_advance
  into v_expected_count,v_is_forced from pg_temp.h2_p1_case;
  with actual as (
    select coalesce(line_json#>>'{economic_key,key_type}',line_json->>'component_key_type') as key_type,
           coalesce(line_json#>>'{economic_key,key_value}',line_json->>'component_key_value') as key_value,
           round((line_json->>'amount_ex_vat')::numeric,2) as amount_ex_vat
    from pg_temp.h2_f013_canonical_lines
    where line_json->>'line_type'='TIMESHEET_PAYMENT'
      and coalesce((line_json->>'selection_allowed')::boolean,false)
    union all
    select line_json->>'line_type',
           coalesce(line_json#>>'{economic_key,key_value}',line_json->>'component_key_value'),
           round((line_json->>'amount_ex_vat')::numeric,2)
    from pg_temp.h2_f013_canonical_lines
    where line_json->>'line_type'<>'TIMESHEET_PAYMENT'
      and nullif(line_json->>'finance_case_id','') is null
      and coalesce((line_json->>'selection_allowed')::boolean,false)
  ), differences as (
    (select * from actual except all select * from pg_temp.h2_p1_expected_components)
    union all
    (select * from pg_temp.h2_p1_expected_components except all select * from actual)
  )
  select count(*) into v_component_mismatch from differences;
  if v_actual_count<>v_expected_count
     or v_context_count<>(select expected_timesheet_count from pg_temp.h2_p1_case)
     or v_finance_count<>1
     or v_component_mismatch<>0
     or (v_is_forced and not exists(
       select 1 from pg_temp.h2_f013_canonical_lines
       where line_json->>'line_type'='TIMESHEET_PAYMENT'
         and nullif(coalesce(line_json#>>'{economic_key,key_type}',line_json->>'component_key_type'),'') is null
         and coalesce((line_json->>'is_advanced')::boolean,false)
         and nullif(line_json->>'advanced_override_id','') is not null
     ))
     or exists(select 1 from pg_temp.h2_f013_canonical_lines
               where line_json->>'line_type' in ('LOAN_REPAYMENT','MANUAL_CREDIT_PAYOUT')) then
    raise exception 'H2_P1_CANONICAL_PRODUCER_SHAPE_CHANGED:%',(
      select jsonb_agg(jsonb_build_object(
        'line_type',line_json->>'line_type','key_type',coalesce(line_json#>>'{economic_key,key_type}',line_json->>'component_key_type'),
        'key_value',coalesce(line_json#>>'{economic_key,key_value}',line_json->>'component_key_value'),
        'amount_ex_vat',line_json->>'amount_ex_vat','row_key',line_json->>'row_key') order by line_json::text)
      from pg_temp.h2_f013_canonical_lines
    );
  end if;
end;
$h2_p1_canonical_shape$;`, 'canonical shape guard');

prefix = replaceOnce(
  prefix,
  `'OPEN',1,'[]',true,true,1,1,1,0,0,1,1,0,0,3,2,'READY',1,`,
  `'OPEN',1,'[]',true,true,1,1,1,0,0,1,1,0,0,
  (select count(*) from pg_temp.h2_f013_canonical_lines),
  (select count(*) from pg_temp.h2_f013_canonical_lines where coalesce((line_json->>'selection_allowed')::boolean,false)),
  'READY',1,`,
  'session preview and selected counts'
);
prefix = replaceOnce(
  prefix,
  `       or publication_result->>'preview_row_count'<>'3'
       or publication_result->>'selected_row_count'<>'2' then`,
  `       or (publication_result->>'preview_row_count')::integer<>
            (select count(*) from pg_temp.h2_f013_canonical_lines)
       or (publication_result->>'selected_row_count')::integer<>
            (select count(*) from pg_temp.h2_f013_canonical_lines
             where coalesce((line_json->>'selection_allowed')::boolean,false)) then`,
  'publication counts'
);
prefix = replaceOnce(
  prefix,
  `     or (scope_result->>'timesheet_count')::integer<>1
     or (scope_result->>'finance_case_count')::integer<>1`,
  `     or (scope_result->>'timesheet_count')::integer<>(select expected_timesheet_count from pg_temp.h2_p1_case)
     or (scope_result->>'finance_case_count')::integer<>1`,
  'scope timesheet count'
);
prefix = replaceOnce(
  prefix,
  `  if jsonb_array_length(coalesce(allocation_rows,'[]'::jsonb))<>2 then`,
  `  if jsonb_array_length(coalesce(allocation_rows,'[]'::jsonb))<>
       (select expected_ordinary_constituents+1 from pg_temp.h2_p1_case) then`,
  'allocation evidence count'
);

function scenarioSql(testCase) {
  const common = `
create temporary table pg_temp.h2_p1_case(
  case_id text primary key, expected_timesheet_count integer not null,
  expected_ordinary_constituents integer not null, forced_advance boolean not null
) on commit drop;
insert into pg_temp.h2_p1_case values (
  ${quoteLiteral(testCase.case_id)},${testCase.expected_timesheet_count},${testCase.expected_ordinary_constituents},
  ${testCase.profile === 'COMPONENT_MIX' ? 'true' : 'false'}
);
create temporary table pg_temp.h2_p1_expected_components(
  key_type text not null,key_value text not null,amount_ex_vat numeric(12,2) not null
) on commit drop;
`;
  if (testCase.profile === 'SIMPLE') {
    return `${common}insert into pg_temp.h2_p1_expected_components values ('TS_DAY','2026-08-24',100.00);\n${frozenSettingsAuthoritySql}`;
  }
  if (testCase.profile === 'MULTI_TIMESHEET') {
    return `${common}
select set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
  week_ending_date,sheet_scope,authorised_at_server,is_current,version,is_adjustment
)
select ('43000000-0000-4000-8000-'||lpad((22000+ordinal)::text,12,'0'))::uuid,
  'h2-p1-multi-'||ordinal,'h2-canonical-multi-'||ordinal,'h2 canonical hospital',
  'h2 canonical ward','h2 canonical role','2026-08-30','DAILY',now(),true,1,false
from pg_temp.h2_f013_canonical_variants;
select set_config('cloudtms.lifecycle_mutation_context','',true);
insert into public.timesheets_financials(
  id,timesheet_id,timesheet_version,basis,is_current,is_stale,worked_start_iso,worked_end_iso,
  actual_schedule_json,candidate_id,client_id,role,pay_method,occupant_key_norm,
  candidate_assignment,processing_status,hours_day,pay_day,charge_day,total_hours,
  total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue
)
select ('43000000-0000-4000-8000-'||lpad((32000+ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((22000+ordinal)::text,12,'0'))::uuid,1,'SELF_REPORTED',true,false,
  '2026-08-25 08:00:00+01','2026-08-25 16:00:00+01',
  '{"date":"2026-08-25","start":"08:00","end":"16:00","break_minutes":0}'::jsonb,
  ('43000000-0000-4000-8000-'||lpad((10000+ordinal)::text,12,'0'))::uuid,
  '43000000-0000-4000-8000-000000000020','h2 canonical role',pay_channel,
  'h2-canonical-multi-'||ordinal,'ASSIGNED','READY_FOR_INVOICE',8,10,15,8,80,120,40,false,false,false
from pg_temp.h2_f013_canonical_variants;
insert into pg_temp.h2_p1_expected_components values
  ('TS_DAY','2026-08-24',100.00),('TS_DAY','2026-08-25',80.00);
${frozenSettingsAuthoritySql}
`;
  }
  if (testCase.profile === 'FALLBACK_EXPENSES') {
    return `${common}
update public.timesheets_financials as financial
set expenses_pay_ex_vat=10.00,expenses_charge_ex_vat=12.00,
    expenses_description='H2 P1 fallback expense'
from pg_temp.h2_f013_canonical_variants as variant
where financial.id=('43000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid;
insert into pg_temp.h2_p1_expected_components values
  ('TS_DAY','2026-08-24',100.00),('EXPENSE_CODE','EXPENSES',10.00);
${frozenSettingsAuthoritySql}
`;
  }
  if (testCase.profile === 'COMPONENT_MIX') {
    return `${common}
update public.umbrellas set vat_chargeable=${testCase.vat_chargeable ? 'true' : 'false'}
where id='41000000-0000-4000-8000-000000000010';
update public.timesheets_financials as financial
set hours_day=2,hours_night=2,hours_sat=2,hours_sun=2,hours_bh=2,
    pay_day=10,pay_night=12,pay_sat=14,pay_sun=16,pay_bh=18,
    total_hours=10,total_pay_ex_vat=140,total_charge_ex_vat=210,margin_ex_vat=70,
    travel_pay_ex_vat=3,travel_charge_ex_vat=4,
    accommodation_pay_ex_vat=4,accommodation_charge_ex_vat=5,
    other_pay_ex_vat=5,other_charge_ex_vat=6,
    mileage_units=12,mileage_pay_rate=.5,mileage_charge_rate=.75,
    mileage_pay_ex_vat=6,mileage_charge_ex_vat=9,
    additional_units_json='{"BONUS":{"unit_count":2,"pay_rate":5,"charge_rate":8,"pay_ex_vat":10,"charge_ex_vat":16,"bucket_name":"Bonus","unit_name":"units"}}'::jsonb,
    additional_pay_ex_vat=10,additional_charge_ex_vat=16,additional_margin_ex_vat=6
from pg_temp.h2_f013_canonical_variants as variant
where financial.id=('43000000-0000-4000-8000-'||lpad((30000+variant.ordinal)::text,12,'0'))::uuid;
insert into public.ts_pay_adjustments(
  id,timesheet_id,candidate_id,client_id,week_ending_date,delta_pay_ex_vat,
  reason,as_advance,meta_json
)
select ('43000000-0000-4000-8000-'||lpad((60000+ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((20000+ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((10000+ordinal)::text,12,'0'))::uuid,
  '43000000-0000-4000-8000-000000000020','2026-08-30',7.00,
  'H2 P1 adjustment',false,'{"h2_fixture":true}'::jsonb
from pg_temp.h2_f013_canonical_variants;
insert into public.timesheet_payment_overrides(
  id,timesheet_id,candidate_id,override_type,reason,created_by_user_id
)
select ('43000000-0000-4000-8000-'||lpad((70000+ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((20000+ordinal)::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((10000+ordinal)::text,12,'0'))::uuid,
  'ADVANCE_THIS_PAYMENT','H2 P1 forced advance','10000000-0000-4000-8000-000000000001'
from pg_temp.h2_f013_canonical_variants;
insert into pg_temp.h2_p1_expected_components values
  ('TS_DAY','2026-08-24',20.00),('TS_DAY','2026-08-24',24.00),
  ('TS_DAY','2026-08-24',28.00),('TS_DAY','2026-08-24',32.00),
  ('TS_DAY','2026-08-24',36.00),('ADDITIONAL_CODE','BONUS',10.00),
  ('ADJUSTMENT_CODE',(select ('43000000-0000-4000-8000-'||lpad((60000+ordinal)::text,12,'0')) from pg_temp.h2_f013_canonical_variants),7.00),
  ('EXPENSE_CODE','TRAVEL',3.00),('EXPENSE_CODE','ACCOMMODATION',4.00),
  ('EXPENSE_CODE','OTHER',5.00),('EXPENSE_CODE','MILEAGE',6.00);
${frozenSettingsAuthoritySql}
`;
  }
  throw new Error(`Unknown P1 profile ${testCase.profile}`);
}

const suffix = testCase => String.raw`
DO $h2_p1_full_chain$
DECLARE
  v_result jsonb;
  v_partition_receipt text;
  v_phase text;
  v_iteration integer;
  v_unlinked integer;
  v_selected_ordinary integer;
  v_allocation_ordinary integer;
  v_item_count integer;
  v_snapshot_count integer;
  v_breakdown_count integer;
  v_amount_ex numeric;
  v_amount_vat numeric;
  v_amount_inc numeric;
  v_expected_vat numeric;
BEGIN
  SELECT receipt_digest_sha256 INTO STRICT v_partition_receipt
  FROM private.banking_pay_draft_frozen_stage_receipts_v8
  WHERE operation_id='43000000-0000-4000-8000-000000000100'
    AND stage_kind='CERTIFICATE_PARTITION_REFS' AND stage_status='TERMINAL' AND has_more=false
  ORDER BY page_sequence DESC LIMIT 1;
  FOR v_iteration IN 1..100 LOOP
    v_result:=public.banking_pay_draft_advance_bounded_v8(
      '43000000-0000-4000-8000-000000000100','H2_V8_P1_ORDINARY_POLICY',v_partition_receipt
    );
    EXIT WHEN v_result->>'work_kind'='READY_FOR_TERMINAL_FINISH';
  END LOOP;
  SELECT phase INTO STRICT v_phase FROM public.banking_pay_operations
  WHERE id='43000000-0000-4000-8000-000000000100';
  SELECT count(*)::integer INTO v_unlinked
  FROM public.banking_pay_operation_candidate_allocation_rows
  WHERE operation_id='43000000-0000-4000-8000-000000000100' AND pay_batch_item_id IS NULL;
  SELECT count(*)::integer INTO v_selected_ordinary
  FROM pg_temp.h2_f013_canonical_lines
  WHERE nullif(line_json->>'finance_case_id','') is null
    AND coalesce((line_json->>'selection_allowed')::boolean,false);
  SELECT count(*)::integer INTO v_allocation_ordinary
  FROM public.banking_pay_operation_candidate_allocation_rows
  WHERE operation_id='43000000-0000-4000-8000-000000000100' AND finance_case_id IS NULL;
  SELECT count(distinct item.id)::integer,
         coalesce(sum(distinct item.amount_ex_vat),0),coalesce(sum(distinct item.amount_vat),0),
         coalesce(sum(distinct item.amount_inc_vat),0)
  INTO v_item_count,v_amount_ex,v_amount_vat,v_amount_inc
  FROM public.pay_batch_items item
  JOIN public.pay_batch_candidates candidate on candidate.id=item.pay_batch_candidate_id
  JOIN public.banking_pay_operation_candidate_allocation_rows allocation
    on allocation.pay_batch_item_id=item.id
   and allocation.operation_id='43000000-0000-4000-8000-000000000100'
   and allocation.finance_case_id is null
  WHERE coalesce(item.is_voided,false)=false;
  SELECT count(*)::integer INTO v_snapshot_count
  FROM public.pay_batch_timesheet_snapshots snapshot
  JOIN public.pay_batches batch on batch.id=snapshot.pay_batch_id
  WHERE batch.source_workbench_session_id='43000000-0000-4000-8000-000000000005';
  SELECT count(*)::integer INTO v_breakdown_count
  FROM public.pay_batch_item_breakdowns breakdown
  JOIN public.pay_batch_items item on item.id=breakdown.pay_batch_item_id
  JOIN public.pay_batch_candidates candidate on candidate.id=item.pay_batch_candidate_id
  JOIN public.pay_batches batch on batch.id=candidate.pay_batch_id
  WHERE batch.source_workbench_session_id='43000000-0000-4000-8000-000000000005'
    AND item.finance_case_id IS NULL;

  if v_result->>'work_kind'<>'READY_FOR_TERMINAL_FINISH' or v_phase<>'POST_CREATE_REFRESH'
     or v_unlinked<>0 or v_selected_ordinary<>${testCase.expected_ordinary_constituents}
     or v_allocation_ordinary<>${testCase.expected_ordinary_constituents}
     or v_snapshot_count<>${testCase.expected_timesheet_count}
     or exists(
       select 1
       from pg_temp.h2_f013_canonical_lines line
       where nullif(line.line_json->>'finance_case_id','') is null
         and coalesce((line.line_json->>'selection_allowed')::boolean,false)
         and not exists(
           select 1
           from public.banking_pay_operation_candidate_allocation_rows allocation
           join public.pay_batch_items item on item.id=allocation.pay_batch_item_id
           where allocation.operation_id='43000000-0000-4000-8000-000000000100'
             and allocation.finance_case_id is null
             and allocation.allocation_basis_json#>>'{line,row_key}'=line.line_json->>'row_key'
             and round(allocation.allocated_amount,2)=round((line.line_json->>'amount_ex_vat')::numeric,2)
             and allocation.status='ITEM_CREATED'
             and item.timesheet_id=(line.line_json->>'timesheet_id')::uuid
             and item.pay_channel=line.line_json->>'pay_channel'
             and item.finance_case_id is null and item.finance_component_id is null
             and item.pay_bank_transfer_id is null and coalesce(item.is_voided,false)=false
         )
     )
     or exists(
       select 1
       from public.pay_batch_items item
       join public.pay_batch_candidates candidate on candidate.id=item.pay_batch_candidate_id
       join public.pay_batches batch on batch.id=candidate.pay_batch_id
       where batch.source_workbench_session_id='43000000-0000-4000-8000-000000000005'
         and item.finance_case_id is null and coalesce(item.is_voided,false)=false
       group by item.id,item.amount_ex_vat
       having round(item.amount_ex_vat,2)<>round((
         select coalesce(sum(allocation.allocated_amount),0)
         from public.banking_pay_operation_candidate_allocation_rows allocation
         where allocation.operation_id='43000000-0000-4000-8000-000000000100'
           and allocation.pay_batch_item_id=item.id and allocation.finance_case_id is null
       ),2)
     ) then
    raise exception 'H2_P1_TYPED_DRAFT_OUTPUT_MISMATCH:%',jsonb_build_object(
      'case_id',${quoteLiteral(testCase.case_id)},'result',v_result,'phase',v_phase,'unlinked',v_unlinked,
      'selected_ordinary',v_selected_ordinary,'allocation_ordinary',v_allocation_ordinary,
      'item_count',v_item_count,'snapshot_count',v_snapshot_count,'breakdown_count',v_breakdown_count,
      'amount_ex_vat',v_amount_ex,'amount_vat',v_amount_vat,'amount_inc_vat',v_amount_inc
    );
  end if;
  v_expected_vat:=case when ${quoteLiteral(testCase.pay_channel)}='UMBRELLA' and ${testCase.vat_chargeable ? 'true' : 'false'}
                       then round(v_amount_ex*.20,2) else 0 end;
  if round(v_amount_vat,2)<>v_expected_vat or round(v_amount_inc,2)<>round(v_amount_ex+v_amount_vat,2) then
    raise exception 'H2_P1_CHANNEL_VAT_POLICY_MISMATCH:%',jsonb_build_object(
      'case_id',${quoteLiteral(testCase.case_id)},'amount_ex_vat',v_amount_ex,
      'amount_vat',v_amount_vat,'expected_vat',v_expected_vat,'amount_inc_vat',v_amount_inc
    );
  end if;
  raise notice 'H2_V8_ORDINARY_POLICY_PASS=${testCase.case_id}|%|%|%|%|%|%|%',
    v_phase,v_selected_ordinary,v_allocation_ordinary,v_item_count,v_snapshot_count,v_breakdown_count,
    jsonb_build_object('amount_ex_vat',v_amount_ex,'amount_vat',v_amount_vat,'amount_inc_vat',v_amount_inc);
END;
$h2_p1_full_chain$;
ROLLBACK;
`;

const injectionMarker = `set constraints all immediate;`;
const version = spawnSync('docker', ['exec', container, 'psql', '-X', '-U', 'postgres', '-d', database, '-Atc', 'select version()'], {
  encoding: 'utf8', maxBuffer: 1024 * 1024
});
if (version.status !== 0) throw new Error(`Unable to read disposable PostgreSQL identity: ${version.stderr.trim()}`);
const engine = version.stdout.trim();
if (!/^PostgreSQL (17\.11|18\.6)\b/.test(engine)) throw new Error(`Unsupported disposable PostgreSQL engine: ${engine}`);
if (expectedEngine && !engine.startsWith(expectedEngine)) {
  throw new Error(`Disposable engine mismatch: expected ${expectedEngine}; observed ${engine}`);
}

const selectedCases = onlyCase ? caseContract.cases.filter(testCase => testCase.case_id === onlyCase) : caseContract.cases;
if (onlyCase && selectedCases.length !== 1) throw new Error(`Unknown --case ${onlyCase}`);
const results = [];
for (const testCase of selectedCases) {
  let casePrefix = replaceOnce(prefix, injectionMarker, `${scenarioSql(testCase)}\n${injectionMarker}`, 'scenario injection');
  const sql = `${bridge.split(bridgeMarker)[0]}\n${casePrefix}\n${suffix(testCase)}`;
  const rollbackCount = (sql.match(/^\s*ROLLBACK\s*;\s*$/gim) ?? []).length;
  if (!sql.endsWith('\n') || rollbackCount !== 1) {
    const rollbackContexts = [...sql.matchAll(/^\s*ROLLBACK\s*;\s*$/gim)].map(match =>
      sql.slice(Math.max(0, match.index - 80), Math.min(sql.length, match.index + 100))
    );
    throw new Error(`${testCase.case_id} must end in exactly one rollback; observed ${rollbackCount}: ${JSON.stringify(rollbackContexts)}`);
  }
  if (/\b(?:provider|settlement|remittance)[a-z_]*\s*\(/i.test(suffix(testCase))) {
    throw new Error(`${testCase.case_id} crossed a forbidden external-action function boundary`);
  }
  const started = performance.now();
  const execution = spawnSync('docker', [
    'exec', '-i', '-w', '/repo/tests', '-e', `PGOPTIONS=-c cloudtms.h2_f013_variant_ordinal=${testCase.finance_variant_ordinal} -c jit=off`,
    container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database
  ], { input: sql, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  const output = `${execution.stdout ?? ''}\n${execution.stderr ?? ''}`;
  const marker = `H2_V8_ORDINARY_POLICY_PASS=${testCase.case_id}|POST_CREATE_REFRESH|${testCase.expected_ordinary_constituents}|${testCase.expected_ordinary_constituents}|`;
  if (execution.status !== 0 || !output.includes(marker)) {
    throw new Error(`${testCase.case_id} failed with exit ${execution.status}:\n${output.split(/\r?\n/).slice(-100).join('\n')}`);
  }
  results.push({
    case_id: testCase.case_id,
    class_ids: testCase.class_ids,
    status: 'PASS',
    evidence_tier: 'FULL_TYPED_CURRENT_V8_POLICY_OWNER',
    v1_v8_typed_parity_status: 'OPEN',
    transaction_outcome: 'ROLLBACK',
    elapsed_ms: Math.round((performance.now() - started) * 1000) / 1000
  });
}

process.stdout.write(`${JSON.stringify({
  contract: 'BANKING_PAY_DRAFT_V8_ORDINARY_POLICY_MATRIX_RESULT_V1',
  engine,database,
  harness_isolation: {
    kind: 'EXACT_LOCAL_DATABASE_GUARD_SUBSTITUTION_ONLY',
    committed_fixture_unchanged: true,
    pre_insert_collision_guard_preserved: true
  },
  unchanged_budgets: { statement_timeout_ms: 15000, lock_timeout_ms: 1500, idle_in_transaction_session_timeout_ms: 30000 },
  source_sha256: {
    [bridgePath]: sha256(bridge),[canonicalPath]: sha256(canonical),[fixturePath]: sha256(selectionFixture),[casesPath]: sha256(caseContractText)
  },
  case_count: results.length,pass_count: results.length,fail_count: 0,
  class_pass_count: results.flatMap(result => result.class_ids).length,
  external_payment_actions: 0,transaction_outcome: 'ROLLBACK',results
}, null, 2)}\n`);
