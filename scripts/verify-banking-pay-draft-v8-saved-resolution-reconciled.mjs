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
const mode = args.get('mode') ?? 'boundary';
if (!container || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(container)) {
  throw new Error('A safe --container value is required');
}
if (!database || !/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(database)) {
  throw new Error('A safe --database value is required');
}
if (expectedEngine && !/^PostgreSQL (17\.11|18\.6)\b/.test(expectedEngine)) {
  throw new Error('--engine must start with PostgreSQL 17.11 or PostgreSQL 18.6');
}
if (!['boundary', 'full'].includes(mode)) throw new Error('--mode must be boundary or full');

const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const canonicalPath = 'tests/02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
const fixturePath = 'tests/fixtures/28082026_1429_banking_pay_selection_setup.sql';
const casesPath = 'tests/fixtures/banking-pay-draft-v8-saved-resolution-cases-v1.json';
const writerPath = 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql';
const consumerPath = 'supabase/repeatable/17082026_2052_pay_finance_resolution_cancel_authority.sql';
const replacementPath = 'supabase/repeatable/04092026_0910_banking_pay_finance_saved_resolution_evidence_v1.sql';
const provisionalScopePath = 'supabase/repeatable/02092026_1030_banking_pay_draft_scope_finance_constituent_handoff_v1.sql';
const canonical = read(canonicalPath);
const selectionFixture = read(fixturePath);
const caseContractText = read(casesPath);
const caseContract = JSON.parse(caseContractText);
const writerSource = read(writerPath);
const consumerSource = read(consumerPath);
const replacementSource = read(replacementPath);
const provisionalScopeSource = read(provisionalScopePath);

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
const sqlBoolean = value => value ? 'true' : 'false';
const frozenSettingsAuthoritySql = String.raw`
insert into public.client_settings(
  id,client_id,effective_from,autoprocess_hr
) values (
  '43000000-0000-4000-8000-000000000021',
  '43000000-0000-4000-8000-000000000020','2026-01-01',false
);
do $h2_saved_resolution_freeze_settings_authority$
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
    raise exception 'H2_SAVED_RESOLUTION_SETTINGS_AUTHORITY_NOT_FROZEN';
  end if;
end;
$h2_saved_resolution_freeze_settings_authority$;
`;

const canonicalShapeMarker = 'do $canonical_shape$';
if (canonical.split(canonicalShapeMarker).length !== 2) {
  throw new Error('The canonical producer stop boundary changed');
}
let prefix = canonical.split(canonicalShapeMarker)[0];
let fixture = replaceOnce(
  selectionFixture,
  "current_database()<>'banking_modal_v2_test'",
  `current_database()<>${quoteLiteral(database)}`,
  'selection fixture database guard'
);
prefix = replaceOnce(
  prefix,
  "current_database()<>'banking_modal_v2_test'",
  `current_database()<>${quoteLiteral(database)}`,
  'canonical fixture database guard'
);
prefix = replaceOnce(prefix, '\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql', `${fixture}\n${replacementSource}`, 'selection fixture include and local replacement');

const buildGuardStart = `    if build_result#>>'{source_build_canonical_diagnostics,source_rows_seen}'<>'1'`;
const buildGuardEnd = `    end if;`;
prefix = replaceBlock(prefix, buildGuardStart, buildGuardEnd, `    if build_result#>>'{source_build_canonical_diagnostics,source_rows_seen}'<>'1'
       or build_result->>'ready_preview_line_count'<>'2'
       or build_result->>'blocked_preview_line_count'<>'0' then
      raise exception 'H2_SAVED_RESOLUTION_CONSUMER_BOUNDARY_CHANGED:%:%',
        variant.ordinal,build_result-'hidden_recovery_template_lines';
    end if;`, 'canonical build guard');

function scenarioSql(testCase) {
  const mismatchTarget = testCase.candidate_target_pay_method === 'PAYE' ? 'UMBRELLA' : 'PAYE';
  const resolutionInput = {
    finance_component_id: `43000000-0000-4000-8000-${String(80000 + testCase.finance_variant_ordinal).padStart(12, '0')}`,
    resolution_mode: testCase.resolution_mode,
    target_pay_method: testCase.candidate_target_pay_method,
    target_units: testCase.target_units,
    relevant_erni_pct: testCase.relevant_erni_pct,
    vat_rate_pct: testCase.vat_rate_pct,
    umbrella_vat_chargeable: testCase.umbrella_vat_chargeable
  };
  if (testCase.replacement_rate !== null) resolutionInput.replacement_rate = testCase.replacement_rate;
  const mismatchInput = { ...resolutionInput, target_pay_method: mismatchTarget };
  return `
create temporary table pg_temp.h2_saved_resolution_case(
  case_id text primary key,class_id text not null,candidate_id uuid not null,
  finance_case_id uuid not null,finance_component_id uuid not null,
  candidate_target_pay_method text not null,component_source_pay_method text not null,
  expected_target_ex numeric(12,2) not null,expected_target_vat numeric(12,2) not null,
  expected_target_inc numeric(12,2) not null
) on commit drop;
insert into pg_temp.h2_saved_resolution_case values (
  ${quoteLiteral(testCase.case_id)},${quoteLiteral(testCase.class_id)},
  ('43000000-0000-4000-8000-'||lpad((10000+${testCase.finance_variant_ordinal})::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((50000+${testCase.finance_variant_ordinal})::text,12,'0'))::uuid,
  ('43000000-0000-4000-8000-'||lpad((80000+${testCase.finance_variant_ordinal})::text,12,'0'))::uuid,
  ${quoteLiteral(testCase.candidate_target_pay_method)},${quoteLiteral(testCase.component_source_pay_method)},
  ${testCase.expected_target_amount_ex_vat},${testCase.expected_target_amount_vat},${testCase.expected_target_amount_inc_vat}
);

update pg_temp.h2_f013_e2e_variants
set expected_amount_ex_vat=-${testCase.expected_reconciled_recovery_amount_ex_vat},
    expected_amount_vat=-${testCase.expected_reconciled_recovery_amount_vat},
    expected_amount_inc_vat=-${testCase.expected_reconciled_recovery_amount_inc_vat}
where ordinal=${testCase.finance_variant_ordinal};

select set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
insert into public.timesheets(
  timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,
  job_title_norm,week_ending_date,sheet_scope,
  authorised_at_server,is_current,version,is_adjustment
) values (
  ('43000000-0000-4000-8000-'||lpad((90000+${testCase.finance_variant_ordinal})::text,12,'0'))::uuid,
  'h2-saved-origin-${testCase.finance_variant_ordinal}','h2-saved-origin-${testCase.finance_variant_ordinal}',
  'h2 saved origin hospital','h2 saved origin ward','h2 saved origin role',
  '2026-08-23','DAILY',now(),true,1,false
);
select set_config('cloudtms.lifecycle_mutation_context','',true);

update public.settings_finance_windows
set erni_pct=${testCase.relevant_erni_pct},vat_rate_pct=${testCase.vat_rate_pct}
where id='41000000-0000-4000-8000-000000000020';

update public.pay_finance_case_components as component
set linked_timesheet_id=('43000000-0000-4000-8000-'||lpad((90000+${testCase.finance_variant_ordinal})::text,12,'0'))::uuid,
    client_id='43000000-0000-4000-8000-000000000020',
    source_family_key='timesheet:43000000-0000-4000-8000-'||lpad((90000+${testCase.finance_variant_ordinal})::text,12,'0'),
    component_key_type='TS_DAY',component_key_value='2026-08-24',
    classification='TAXABLE_CHANNEL_SENSITIVE',
    source_pay_method=${quoteLiteral(testCase.component_source_pay_method)},
    source_basis_json=jsonb_build_object(
      'timesheet_id','43000000-0000-4000-8000-'||lpad((90000+${testCase.finance_variant_ordinal})::text,12,'0'),
      'work_date','2026-08-24','bucket_code','DAY',
      'source_units',${testCase.source_units},'source_rate',${testCase.source_rate},
      'source_charge_rate',${testCase.source_charge_rate},
      'source_charge_ex_vat',round(${testCase.source_units}*${testCase.source_charge_rate},2),
      'umbrella_vat_chargeable',${sqlBoolean(testCase.umbrella_vat_chargeable)}
    ),
    source_amount=${testCase.source_amount_ex_vat},remaining_source_amount=${testCase.source_amount_ex_vat},
    saved_target_pay_method=null,saved_resolution_mode=null,
    saved_resolution_payload_json=null,saved_resolution_result_json=null,
    resolution_fingerprint=null,is_resolution_stale=false,stale_reason=null,resolved_at_utc=null
from pg_temp.h2_saved_resolution_case as fixture
where component.id=fixture.finance_component_id;

update public.pay_advances as finance_case
set original_amount=${testCase.source_amount_ex_vat},
    outstanding_amount=${testCase.source_amount_ex_vat},
    schedule_json=jsonb_build_array(jsonb_build_object(
      'week_start','2026-08-24','amount',${testCase.source_amount_ex_vat},'status','DUE'
    )),
    weekly_due=${testCase.source_amount_ex_vat},
    weeks_total=1,
    start_week_start='2026-08-24'::date
from pg_temp.h2_saved_resolution_case as fixture
where finance_case.id=fixture.finance_case_id;

do $h2_saved_target_mismatch$
declare
  v_before jsonb;
  v_after jsonb;
  v_message text;
begin
  select to_jsonb(component) into strict v_before
  from public.pay_finance_case_components component
  join pg_temp.h2_saved_resolution_case fixture on fixture.finance_component_id=component.id;
  begin
    perform public.pay_finance_component_resolutions_apply(
      (select candidate_id from pg_temp.h2_saved_resolution_case),
      ${quoteLiteral(JSON.stringify([mismatchInput]))}::jsonb,
      '10000000-0000-4000-8000-000000000001',
      (select finance_case_id from pg_temp.h2_saved_resolution_case),
      'H2_SAVED_RESOLUTION_TARGET_MISMATCH_NEGATIVE'
    );
    raise exception 'H2_TARGET_MISMATCH_UNEXPECTEDLY_ACCEPTED';
  exception when others then
    get stacked diagnostics v_message=message_text;
    if position('TARGET_PAY_METHOD_MISMATCH' in v_message)=0 then raise; end if;
  end;
  select to_jsonb(component) into strict v_after
  from public.pay_finance_case_components component
  join pg_temp.h2_saved_resolution_case fixture on fixture.finance_component_id=component.id;
  if v_after is distinct from v_before then
    raise exception 'H2_TARGET_MISMATCH_MUTATED_COMPONENT';
  end if;
end;
$h2_saved_target_mismatch$;

create temporary table pg_temp.h2_saved_resolution_writer_result(result_json jsonb not null) on commit drop;
insert into pg_temp.h2_saved_resolution_writer_result
select public.pay_finance_component_resolutions_apply(
  (select candidate_id from pg_temp.h2_saved_resolution_case),
  ${quoteLiteral(JSON.stringify([resolutionInput]))}::jsonb,
  '10000000-0000-4000-8000-000000000001',
  (select finance_case_id from pg_temp.h2_saved_resolution_case),
  'H2_SAVED_RESOLUTION_WRITER_POSITIVE'
);

do $h2_saved_writer_contract$
declare
  v_component public.pay_finance_case_components%rowtype;
  v_result jsonb;
  v_writer_component jsonb;
begin
  select component.* into strict v_component
  from public.pay_finance_case_components component
  join pg_temp.h2_saved_resolution_case fixture on fixture.finance_component_id=component.id;
  select result_json into strict v_result from pg_temp.h2_saved_resolution_writer_result;
  v_writer_component:=v_result#>'{components,0}';
  if v_result->>'ok'<>'true' or v_result->>'applied_count'<>'1'
     or v_component.saved_target_pay_method<>${quoteLiteral(testCase.candidate_target_pay_method)}
     or v_component.saved_resolution_mode::text<>${quoteLiteral(testCase.resolution_mode)}
     or v_component.source_pay_method<>${quoteLiteral(testCase.component_source_pay_method)}
     or round(v_component.source_amount,2)<>${testCase.source_amount_ex_vat}
     or round((v_component.saved_resolution_payload_json->>'relevant_erni_pct')::numeric,6)<>${testCase.relevant_erni_pct}
     or round((v_component.saved_resolution_payload_json->>'source_units')::numeric,6)<>${testCase.source_units}
     or round((v_component.saved_resolution_payload_json->>'source_rate')::numeric,6)<>${testCase.source_rate}
     or round((v_component.saved_resolution_payload_json->>'source_charge_rate')::numeric,6)<>${testCase.source_charge_rate}
     or round((v_component.saved_resolution_result_json->>'target_amount_ex_vat')::numeric,2)<>${testCase.expected_target_amount_ex_vat}
     or round((v_component.saved_resolution_result_json->>'target_amount_vat')::numeric,2)<>${testCase.expected_target_amount_vat}
     or round((v_component.saved_resolution_result_json->>'target_amount_inc_vat')::numeric,2)<>${testCase.expected_target_amount_inc_vat}
     or v_component.resolution_fingerprint !~ '^[0-9a-f]{32}$'
     or v_writer_component->>'resolution_fingerprint' is distinct from v_component.resolution_fingerprint
     or v_writer_component->>'saved_target_pay_method' is distinct from v_component.saved_target_pay_method
     or v_writer_component->>'saved_resolution_mode' is distinct from v_component.saved_resolution_mode::text
     or coalesce(v_component.is_resolution_stale,true) then
    raise exception 'H2_WRITER_PRODUCED_RESOLUTION_SHAPE_CHANGED:%',jsonb_build_object(
      'case_id',${quoteLiteral(testCase.case_id)},'writer_result',v_result,
      'component',to_jsonb(v_component)
    );
  end if;
end;
$h2_saved_writer_contract$;
`;
}

function suffixSql(testCase) {
  return `
do $h2_saved_consumer_contract$
declare
  v_component public.pay_finance_case_components%rowtype;
  v_review record;
  v_selected_finance_count integer;
  v_draft_write_count integer;
  v_diagnostics jsonb;
begin
  select component.* into strict v_component
  from public.pay_finance_case_components component
  join pg_temp.h2_saved_resolution_case fixture on fixture.finance_component_id=component.id;
  select review.* into strict v_review
  from pg_temp.finance_case_component_review_rows_effective review
  join pg_temp.h2_saved_resolution_case fixture on fixture.finance_component_id=review.finance_component_id;
  select count(*)::integer into v_selected_finance_count
  from pg_temp.h2_f013_canonical_lines line
  join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=(line.line_json->>'finance_case_id')::uuid
  where coalesce((line.line_json->>'selection_allowed')::boolean,false);
  select jsonb_build_object(
    'finance_case',coalesce((select jsonb_agg(jsonb_build_object(
      'original_amount',row_value.original_amount,'outstanding_amount',row_value.outstanding_amount,
      'active_reserved_amount',row_value.active_reserved_amount,'minimum_earnings_threshold',row_value.minimum_earnings_threshold,
      'take_home_floor_override',row_value.take_home_floor_override,'weekly_due',row_value.weekly_due
    )) from pg_temp.finance_case_baseline_scope row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=row_value.finance_case_id),'[]'::jsonb),
    'candidate_headroom',coalesce((select jsonb_agg(to_jsonb(row_value)) from pg_temp.candidate_authoritative_recovery_headroom row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.candidate_id=row_value.candidate_id),'[]'::jsonb),
    'overpayment_rows',coalesce((select jsonb_agg(to_jsonb(row_value)) from pg_temp.overpayment_recovery_rows row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=row_value.finance_case_id),'[]'::jsonb),
    'overpayment_allocations',coalesce((select jsonb_agg(to_jsonb(row_value)) from pg_temp.overpayment_recovery_allocations row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=row_value.finance_case_id),'[]'::jsonb),
    'due_source',coalesce((select jsonb_agg(to_jsonb(row_value)) from pg_temp.finance_case_due_source_amounts row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=row_value.finance_case_id),'[]'::jsonb),
    'protected_allocations',coalesce((select jsonb_agg(to_jsonb(row_value)) from pg_temp.finance_case_protected_allocations row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=row_value.finance_case_id),'[]'::jsonb),
    'source_allocations',coalesce((select jsonb_agg(to_jsonb(row_value)) from pg_temp.finance_case_component_due_source_allocations row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=row_value.finance_case_id),'[]'::jsonb),
    'preview_allocations',coalesce((select jsonb_agg(to_jsonb(row_value)) from pg_temp.finance_case_component_due_preview_allocations row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=row_value.finance_case_id),'[]'::jsonb),
    'resolution_rollup',coalesce((select jsonb_agg(jsonb_build_object(
      'finance_case_id',row_value.finance_case_id,'due_amount_ex_vat',row_value.due_amount_ex_vat,
      'unresolved_taxable_count',row_value.unresolved_taxable_count,'stale_count',row_value.stale_count,
      'has_current_saved_resolution',row_value.has_current_saved_resolution,
      'case_resolution_satisfied_now',row_value.case_resolution_satisfied_now
    )) from pg_temp.finance_case_resolution_rollup row_value
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=row_value.finance_case_id),'[]'::jsonb),
    'canonical_finance_lines',coalesce((select jsonb_agg(jsonb_build_object(
      'line_type',line.line_json->>'line_type','amount_ex_vat',line.line_json->>'amount_ex_vat',
      'allocated_source_due_amount_ex_vat',line.line_json#>>'{case_components,0,allocated_source_due_amount_ex_vat}',
      'preview_due_amount_ex_vat',line.line_json#>>'{case_components,0,preview_due_amount_ex_vat}',
      'draftable',line.line_json->>'draftable','selection_allowed',line.line_json->>'selection_allowed',
      'presentation_reason',line.line_json->>'presentation_reason'
    )) from pg_temp.h2_f013_canonical_lines line
      join pg_temp.h2_saved_resolution_case fixture on fixture.finance_case_id=(line.line_json->>'finance_case_id')::uuid),'[]'::jsonb)
  ) into v_diagnostics;
  select
    (select count(*) from public.pay_batches where source_workbench_session_id='43000000-0000-4000-8000-000000000005')+
    (select count(*) from public.banking_pay_operations where workbench_session_id='43000000-0000-4000-8000-000000000005')+
    (select count(*) from public.banking_pay_operation_candidate_allocation_rows where operation_id='43000000-0000-4000-8000-000000000100')+
    (select count(*) from public.pay_advance_reservations where pay_batch_id in (
      select id from public.pay_batches where source_workbench_session_id='43000000-0000-4000-8000-000000000005'
    )) into v_draft_write_count;
  if v_review.suggestion_provenance<>'REUSABLE_SAVED_RESOLUTION'
     or not coalesce(v_review.is_reusable_saved_resolution,false)
     or coalesce(v_review.is_stale_saved_resolution,true)
     or coalesce(v_review.requires_resolution,true)
     or v_review.current_component_fingerprint is distinct from v_component.resolution_fingerprint
     or v_selected_finance_count<>1
     or v_draft_write_count<>0 then
    raise exception 'H2_SAVED_RESOLUTION_FIRST_DIVERGENCE_CHANGED:%',jsonb_build_object(
      'case_id',${quoteLiteral(testCase.case_id)},'suggestion_provenance',v_review.suggestion_provenance,
      'stored_fingerprint',v_component.resolution_fingerprint,
      'consumer_fingerprint',v_review.current_component_fingerprint,
      'is_reusable',v_review.is_reusable_saved_resolution,'is_stale',v_review.is_stale_saved_resolution,
      'requires_resolution',v_review.requires_resolution,'selected_finance_count',v_selected_finance_count,
       'draft_write_count',v_draft_write_count,'diagnostics',v_diagnostics
    );
  end if;
  raise notice 'H2_V8_SAVED_RESOLUTION_PASS=${testCase.case_id}|REUSABLE_SAVED_RESOLUTION|1|%',jsonb_build_object(
    'writer_fingerprint',v_component.resolution_fingerprint,
    'consumer_fingerprint',v_review.current_component_fingerprint,
    'target_pay_method',v_component.saved_target_pay_method,
    'target_amount_ex_vat',v_component.saved_resolution_result_json->>'target_amount_ex_vat',
    'target_amount_vat',v_component.saved_resolution_result_json->>'target_amount_vat',
    'target_amount_inc_vat',v_component.saved_resolution_result_json->>'target_amount_inc_vat'
  );
end;
$h2_saved_consumer_contract$;
rollback;
`;
}

const sourceContracts = [
  ['replacement persisted payload fingerprint', replacementSource, 'p_target_basis_json         => v_resolution_payload_json'],
  ['replacement payload family', replacementSource, "'resolution_family', 'TAXABLE_CHANNEL_RESTRUCTURE'"],
  ['writer target-basis fingerprint', writerSource, 'p_target_basis_json         => v_target_basis_json'],
  ['consumer payload-as-target-basis fingerprint', consumerSource, "coalesce(pfc.saved_resolution_payload_json, pfc.saved_resolution_result_json, '{}'::jsonb)"],
  ['consumer stale provenance', consumerSource, "then 'STALE_SAVED_RESOLUTION'"],
  ['strict owner payload resolution-family requirement', consumerSource, "component_row.saved_resolution_payload_json->>'resolution_family'"],
  ['strict owner result resolution-family requirement', consumerSource, "component_row.saved_resolution_result_json->>'resolution_family'"],
  ['writer entry point', writerSource, 'CREATE OR REPLACE FUNCTION public.pay_finance_component_resolutions_apply(']
];
for (const [label, source, marker] of sourceContracts) {
  if (!source.includes(marker)) throw new Error(`${label} source marker changed`);
}

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
  let sql;
  if (mode === 'boundary') {
    const casePrefix = replaceOnce(
      prefix,
      'set constraints all immediate;',
      `${scenarioSql(testCase)}\n${frozenSettingsAuthoritySql}\nset constraints all immediate;`,
      'scenario and settings-authority injection'
    );
    sql = `${casePrefix}${suffixSql(testCase)}`;
    const rollbackCount = (sql.match(/^\s*rollback\s*;\s*$/gim) ?? []).length;
    if (!sql.endsWith('\n') || rollbackCount !== 1) {
      throw new Error(`${testCase.case_id} must end in exactly one rollback; observed ${rollbackCount}`);
    }
  } else {
    // Rewrite the canonical file's own guard before inlining the separately
    // rewritten selection fixture.  Reversing this order makes the canonical
    // local database name appear twice and defeats the exact-one splice guard.
    sql = replaceOnce(
      canonical,
      "current_database()<>'banking_modal_v2_test'",
      `current_database()<>${quoteLiteral(database)}`,
      'full-chain canonical fixture database guard'
    );
    sql = replaceOnce(
      sql,
      '\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',
      `${fixture}\n${provisionalScopeSource}\n${replacementSource}`,
      'full-chain selection fixture include and local replacements'
    );
    sql = replaceOnce(
      sql,
      'set constraints all immediate;',
      `${scenarioSql(testCase)}\n${frozenSettingsAuthoritySql}\nset constraints all immediate;`,
      'full-chain scenario and settings-authority injection'
    );
    sql = replaceOnce(
      sql,
      'or round(interface_row.reserved_amount,2)<>10.00',
      `or round(interface_row.reserved_amount,2)<>${testCase.source_amount_ex_vat}`,
      'full-chain source reservation expectation'
    );
  }
  if (/\b(?:provider|settlement|remittance)[a-z_]*\s*\(/i.test(suffixSql(testCase))) {
    throw new Error(`${testCase.case_id} crossed a forbidden external-action function boundary`);
  }
  const started = performance.now();
  const execution = spawnSync('docker', [
    'exec', '-i', '-w', '/repo/tests', '-e', `PGOPTIONS=-c cloudtms.h2_f013_variant_ordinal=${testCase.finance_variant_ordinal} -c jit=off`,
    container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database
  ], { input: sql, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  const output = `${execution.stdout ?? ''}\n${execution.stderr ?? ''}`;
  const marker = mode === 'boundary'
    ? `H2_V8_SAVED_RESOLUTION_PASS=${testCase.case_id}|REUSABLE_SAVED_RESOLUTION|1|`
    : 'H2_F013_COMPLETE_GREEN_POLICY_PARITY_PASS=';
  if (execution.status !== 0 || !output.includes(marker)) {
    const boundedTail = output.split(/\r?\n/).slice(-120).map(line => line.slice(0, 4000)).join('\n');
    throw new Error(`${testCase.case_id} failed with exit ${execution.status}:\n${boundedTail}`);
  }
  const noticeLine = output.split(/\r?\n/).find(line => line.includes(marker)) ?? '';
  results.push({
    case_id: testCase.case_id,
    class_id: testCase.class_id,
    status: mode === 'boundary' ? 'LOCAL_RECONCILED_OWNER_PASS' : 'LOCAL_FULL_DRAFT_OUTPUT_PASS',
    current_v8_draft_output_status: mode === 'boundary' ? 'OPEN_NOT_REACHED' : 'PASS_ROLLBACK_ONLY',
    reconciled_boundary: mode === 'boundary'
      ? 'SAVED_RESOLUTION_WRITER_TO_CANONICAL_PREVIEW_CONSUMER'
      : 'SAVED_RESOLUTION_WRITER_TO_CURRENT_V8_FINALIZER',
    observed_consumer_provenance: 'REUSABLE_SAVED_RESOLUTION',
    typed_zero_draft_write_count: 0,
    writer_consumer_notice: noticeLine.slice(noticeLine.indexOf(marker)),
    transaction_outcome: 'ROLLBACK',
    elapsed_ms: Math.round((performance.now() - started) * 1000) / 1000
  });
}

process.stdout.write(`${JSON.stringify({
  contract: mode === 'boundary'
    ? 'BANKING_PAY_DRAFT_V8_SAVED_RESOLUTION_RECONCILED_RESULT_V1'
    : 'BANKING_PAY_DRAFT_V8_SAVED_RESOLUTION_FULL_CHAIN_RESULT_V1',
  engine,database,
  harness_isolation: {
    kind: 'EXACT_LOCAL_DATABASE_GUARD_SUBSTITUTION_ONLY',
    committed_fixture_unchanged: true,
    pre_insert_collision_guard_preserved: true
  },
  unchanged_budgets: { statement_timeout_ms: 15000, lock_timeout_ms: 1500, idle_in_transaction_session_timeout_ms: 30000 },
  source_sha256: {
    [canonicalPath]: sha256(canonical),[fixturePath]: sha256(selectionFixture),[casesPath]: sha256(caseContractText),
    [writerPath]: sha256(writerSource),[consumerPath]: sha256(consumerSource),[replacementPath]: sha256(replacementSource),
    [provisionalScopePath]: sha256(provisionalScopeSource)
  },
  case_count: results.length,local_reconciled_pass_count: results.length,unexpected_pass_count: 0,fail_count: 0,
  current_v8_full_typed_draft_output_pass: mode === 'full' ? results.length : 0,full_typed_v1_v8_parity_pass: 0,
  external_payment_actions: 0,transaction_outcome: 'ROLLBACK',results
}, null, 2)}\n`);
