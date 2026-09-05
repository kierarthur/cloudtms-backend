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

const fixturePath = 'tests/fixtures/banking-pay-draft-v8-prior-exclusion-parity-p3-v1.json';
const bindingPath = 'tests/fixtures/banking-pay-create-draft-v8-class-execution-bindings-v1.json';
const policyPath = 'codex_outputs/banking-pay-create-draft-policy-v1/BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json';
const runtimePath = 'tests/04092026_1210_banking_pay_draft_v8_prior_exclusion_parity_p3_runtime.sql';
const resultPath = 'codex_outputs/h2-draft-parity/P3_PRIOR_EXCLUSION_PARITY_RESULTS_V1.json';

const expectedClasses = [
  'part_paid_residual',
  'fully_settled_absent',
  'active_reservation_residual',
  'superseded_absent',
  'cancelled_untouched_reappears_once',
  'timesheet_snoozed_excluded',
  'segment_snooze_isolation',
  'finance_case_snoozed_excluded',
  'snooze_expiry_reappears_once',
  'action_required_excluded',
  'blocked_excluded',
  'active_draft_excluded'
];

const bridgePath = 'tests/03092026_1600_banking_pay_draft_v8_finance_category_runtime.sql';
const canonicalPath = 'tests/02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
const selectionFixturePath = 'tests/fixtures/28082026_1429_banking_pay_selection_setup.sql';
const certificateOwnerPath = 'supabase/repeatable/02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql';
const parityOwnerPath = 'supabase/repeatable/02092026_2315_banking_pay_draft_constituent_parity_v8.sql';

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

const quoteLiteral = value => `'${String(value).replaceAll("'", "''")}'`;

const frozenSettingsAuthoritySql = `
insert into public.client_settings(
  id,client_id,effective_from,autoprocess_hr
) values (
  '43000000-0000-4000-8000-000000000021',
  '43000000-0000-4000-8000-000000000020','2026-01-01',false
);
do $h2_p3_freeze_settings_authority$
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
    raise exception 'H2_P3_SETTINGS_AUTHORITY_NOT_FROZEN';
  end if;
end;
$h2_p3_freeze_settings_authority$;
`;

function stripExactOuterTransaction(sql, label) {
  const begin = /(^|\r?\n)begin;\r?\n/i.exec(sql);
  const commits = [...sql.matchAll(/^commit;\s*$/gim)];
  if (!begin || commits.length !== 1 || commits[0].index + commits[0][0].length < sql.trimEnd().length - 2) {
    throw new Error(`${label} exact outer transaction changed`);
  }
  return `${sql.slice(0, begin.index + begin[1].length)}${sql.slice(begin.index + begin[0].length, commits[0].index)}`;
}

function classificationFragment(testCase) {
  if (testCase.class_id === 'fully_settled_absent') {
    return `, 'eligible',false,'is_eligible',false,'eligibility_state','INELIGIBLE','target_section','blocked_for_pay','section','blocked_for_pay','presentation_section','BLOCKED_FOR_PAY','readiness_state','BLOCKED'`;
  } else if (['timesheet_snoozed_excluded', 'segment_snooze_isolation', 'finance_case_snoozed_excluded'].includes(testCase.class_id)) {
    return `, 'snooze_state',jsonb_build_object('state','INDEFINITE_SNOOZED'),'target_section','blocked_for_pay','section','blocked_for_pay','presentation_section','BLOCKED_FOR_PAY','readiness_state','BLOCKED'`;
  } else if (testCase.class_id === 'action_required_excluded') {
    return `, 'target_section','cases_resolutions','section','cases_resolutions','presentation_section','CASES_RESOLUTIONS','readiness_state','RESOLUTION_REQUIRED'`;
  } else if (testCase.class_id === 'blocked_excluded') {
    return `, 'target_section','blocked_for_pay','section','blocked_for_pay','presentation_section','BLOCKED_FOR_PAY','readiness_state','BLOCKED'`;
  } else if (testCase.class_id === 'active_draft_excluded') {
    return `, 'blocked_reason_codes',jsonb_build_array('ACTIVE_DRAFT_RESERVATION'),'post_draft_unavailable',true,'post_draft_overlay_active',true,'target_section','blocked_for_pay','section','blocked_for_pay','presentation_section','BLOCKED_FOR_PAY','readiness_state','BLOCKED'`;
  } else if (testCase.class_id === 'snooze_expiry_reappears_once') {
    return `, 'snooze_state',jsonb_build_object('state','CLEARED')`;
  }
  return `, 'snooze_state',jsonb_build_object('state','NONE')`;
}

function scenarioSql(testCase, payChannel) {
  const targetFinance = testCase.class_id === 'finance_case_snoozed_excluded';
  const targetSelected = testCase.expected.target_selected === 1;
  const amount = testCase.expected.canonical_ex_vat ?? '100.00';
  const priorPaid = testCase.class_id === 'part_paid_residual' ? '40.00' : '0.00';
  const sourceReservation = testCase.class_id === 'active_reservation_residual' ? '25.00' : '0.00';
  const classification = targetSelected ? classificationFragment(testCase) : '';

  return `
create temporary table pg_temp.h2_p3_case_contract(
  class_id text primary key,pay_channel text not null,target_selected boolean not null,
  target_candidate_id uuid not null,expected_amount_ex_vat text not null,
  expected_prior_paid_ex_vat text not null,expected_source_reservation_ex_vat text not null,
  expected_supersession_treatment text,expected_universe text,
  expected_stale_selection_error text
) on commit drop;
insert into pg_temp.h2_p3_case_contract
select
  ${quoteLiteral(testCase.class_id)},${quoteLiteral(payChannel)},${targetSelected},
  ('43000000-0000-4000-8000-'||lpad((10000+variant.ordinal)::text,12,'0'))::uuid,
  ${quoteLiteral(amount)},${quoteLiteral(priorPaid)},${quoteLiteral(sourceReservation)},
  ${testCase.expected.supersession_treatment ? quoteLiteral(testCase.expected.supersession_treatment) : 'null'},
  ${testCase.expected.target_universe ? quoteLiteral(testCase.expected.target_universe) : 'null'},
  ${testCase.expected.stale_selection_error ? quoteLiteral(testCase.expected.stale_selection_error) : 'null'}
from pg_temp.h2_f013_canonical_variants variant;

update pg_temp.h2_f013_canonical_lines line
set line_json = line.line_json || jsonb_build_object(
  'h2_p3_target',true,
  'amount_ex_vat',${quoteLiteral(amount)},
  'draftable',true,'is_ready_for_draft',true,
  'selection_allowed',true${classification}
)
where ${targetFinance ? "nullif(line.line_json->>'finance_case_id','') is not null" : "line.line_json->>'line_type'='TIMESHEET_PAYMENT' and nullif(coalesce(line.line_json#>>'{economic_key,key_type}',line.line_json->>'component_key_type'),'') is not null"};

do $h2_p3_scenario_shape$
begin
  if (select count(*) from pg_temp.h2_f013_canonical_lines where line_json->'h2_p3_target'='true'::jsonb)<>1
     or (select count(*) from pg_temp.h2_f013_canonical_lines
         where coalesce((line_json->>'selection_allowed')::boolean,false)) < 1 then
    raise exception 'H2_P3_SCENARIO_SHAPE_INVALID';
  end if;
end;
$h2_p3_scenario_shape$;
`;
}

function evidenceAfterBuildSql(testCase) {
  const targetFinance = testCase.class_id === 'finance_case_snoozed_excluded';
  if (targetFinance) return '';
  const amount = testCase.expected.canonical_ex_vat ?? '100.00';
  const truth = testCase.class_id === 'part_paid_residual' ? '100.00'
    : testCase.class_id === 'active_reservation_residual' ? '100.00' : amount;
  const priorPaid = testCase.class_id === 'part_paid_residual' ? '40.00' : '0.00';
  const sourceReservation = testCase.class_id === 'active_reservation_residual' ? '25.00' : '0.00';
  return `
insert into private.banking_pay_workbench_economic_build_facts(
  build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
  dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
  truth_ex_vat,truth_inc_vat,baseline_ex_vat,baseline_inc_vat,financial_digest
)
select build.id,'ENTITLEMENT_COMPONENT','H2-P3:'||${quoteLiteral(testCase.class_id)}||':ENTITLEMENT',
  source.candidate_id,source.timesheet_id,array[source.timesheet_id],
  'H2-P3-UNIT','H2_P3_FIXTURE',source.timesheet_id,
  source.economic_key_json->>'key_type',source.economic_key_json->>'key_value',
  ${truth},${truth},${priorPaid},${priorPaid},md5('H2-P3:'||${quoteLiteral(testCase.class_id)}||':ENTITLEMENT')
from public.banking_pay_workbench_candidate_source_lines source
join private.banking_pay_workbench_economic_builds build
  on build.session_id=source.session_id and build.candidate_id=source.candidate_id
where source.session_id='43000000-0000-4000-8000-000000000005'
  and source.status='CURRENT' and source.source_row_json->'h2_p3_target'='true'::jsonb;
${testCase.class_id === 'active_reservation_residual' ? `
insert into private.banking_pay_workbench_economic_build_facts(
  build_id,fact_family,natural_key,candidate_id,timesheet_id,subject_timesheet_ids,
  dependency_unit_key,source_relation,source_id,economic_key_type,economic_key_value,
  reserved_source_amount,reservation_id,financial_digest
)
select build.id,'RESERVATION_COMPONENT','H2-P3:ACTIVE-RESERVATION',source.candidate_id,
  source.timesheet_id,array[source.timesheet_id],'GLOBAL','H2_P3_FIXTURE',
  '46000000-0000-4000-8000-000000000025'::uuid,
  source.economic_key_json->>'key_type',source.economic_key_json->>'key_value',
  ${sourceReservation},'46000000-0000-4000-8000-000000000025'::uuid,md5('H2-P3:ACTIVE-RESERVATION')
from public.banking_pay_workbench_candidate_source_lines source
join private.banking_pay_workbench_economic_builds build
  on build.session_id=source.session_id and build.candidate_id=source.candidate_id
where source.session_id='43000000-0000-4000-8000-000000000005'
  and source.status='CURRENT' and source.source_row_json->'h2_p3_target'='true'::jsonb;
` : ''}
${testCase.class_id === 'superseded_absent' ? `
insert into public.banking_pay_workbench_candidate_source_lines(
 id,session_id,candidate_id,session_version,source_change_seq,source_build_run_id,
 source_publication_id,source_ordinal,line_key,parent_line_key,split_suffix,timesheet_id,
 section,source_row_json,economic_key_json,contract_json,pay_channel_scope,refresh_scope_kind,status
)
select '46000000-0000-4000-8000-000000000055',source.session_id,source.candidate_id,
 source.session_version,source.source_change_seq,'46000000-0000-4000-8000-000000000056',
 null,999,source.line_key,source.parent_line_key,source.split_suffix,source.timesheet_id,
 source.section,source.source_row_json||jsonb_build_object('h2_p3_superseded',true),
 source.economic_key_json,source.contract_json,source.pay_channel_scope,source.refresh_scope_kind,'SUPERSEDED'
from public.banking_pay_workbench_candidate_source_lines source
where source.session_id='43000000-0000-4000-8000-000000000005'
  and source.status='CURRENT' and source.source_row_json->'h2_p3_target'='true'::jsonb;
` : ''}
`;
}

function postPublicationExclusionSql(testCase) {
  if (testCase.expected.target_selected === 1) return '';
  const classification = classificationFragment(testCase);
  const targetSection = testCase.class_id === 'action_required_excluded' ? 'cases_resolutions' : 'blocked_for_pay';
  return `
update public.banking_pay_workbench_candidate_source_lines source
set source_row_json=source.source_row_json||jsonb_build_object(
      'draftable',false,'is_ready_for_draft',false,'selection_allowed',false${classification}),
    section=${quoteLiteral(targetSection)},updated_at_utc=clock_timestamp()
where source.session_id='43000000-0000-4000-8000-000000000005'
  and source.status='CURRENT' and source.source_row_json->'h2_p3_target'='true'::jsonb;
update public.banking_pay_workbench_preview_rows preview
set selected=false,selection_state='NOT_SELECTED',status=case when ${quoteLiteral(targetSection)}='cases_resolutions' then 'ACTION_REQUIRED' else 'BLOCKED' end,
    section=${quoteLiteral(targetSection)},
    row_json=preview.row_json||jsonb_build_object(
      'draftable',false,'is_ready_for_draft',false,'selection_allowed',false${classification}),
    updated_at_utc=clock_timestamp()
where preview.session_id='43000000-0000-4000-8000-000000000005'
  and preview.row_json->'h2_p3_target'='true'::jsonb;
update public.banking_pay_workbench_sessions session_row
set server_selected_preview_row_ids=(select jsonb_agg(id::text order by row_ordinal,id)
      from public.banking_pay_workbench_preview_rows where session_id=session_row.id and selected),
    selected_row_count=(select count(*) from public.banking_pay_workbench_preview_rows where session_id=session_row.id and selected)
where session_row.id='43000000-0000-4000-8000-000000000005';
`;
}

function buildRuntimeSql(testCase, payChannel, database) {
  const bridge = read(bridgePath);
  const canonical = read(canonicalPath);
  const selectionFixture = read(selectionFixturePath);
  const certificateOwner = read(certificateOwnerPath);
  const parityOwner = read(parityOwnerPath);
  const bridgeMarker = '\\ir 02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
  let bridgePrefix = bridge.split(bridgeMarker)[0];
  if (bridge.split(bridgeMarker).length !== 2) throw new Error('V8 bridge splice changed');
  bridgePrefix = `${bridgePrefix}\n${stripExactOuterTransaction(certificateOwner, 'certificate owner')}\n${parityOwner}`;

  const canonicalStop = 'insert into public.banking_pay_operations(';
  const canonicalStopIndex = canonical.indexOf(canonicalStop);
  if (canonicalStopIndex < 0) throw new Error('canonical operation splice changed');
  let prefix = canonical.slice(0, canonicalStopIndex);
  let localFixture = replaceOnce(selectionFixture,
    "current_database()<>'banking_modal_v2_test'", `current_database()<>${quoteLiteral(database)}`,
    'selection fixture database guard');
  prefix = replaceOnce(prefix, "current_database()<>'banking_modal_v2_test'",
    `current_database()<>${quoteLiteral(database)}`, 'canonical database guard');
  prefix = replaceOnce(prefix, '\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql', localFixture,
    'selection fixture include');
  prefix = replaceOnce(prefix, 'set constraints all immediate;',
    `${frozenSettingsAuthoritySql}\nset constraints all immediate;`,
    'frozen settings authority lifecycle injection');

  const shapeStart = 'do $canonical_shape$';
  const shapeEnd = '$canonical_shape$;';
  prefix = replaceBlock(prefix, shapeStart, shapeEnd, '', 'canonical fixed shape guard');
  prefix = replaceOnce(prefix, '$canonical_build$;', `$canonical_build$;\n${scenarioSql(testCase, payChannel)}`,
    'P3 scenario injection');
  prefix = replaceOnce(prefix,
    `'OPEN',1,'[]',true,true,1,1,1,0,0,1,1,0,0,3,2,'READY',1,`,
    `'OPEN',1,'[]',true,true,1,1,1,0,0,1,1,0,0,\n  (select count(*) from pg_temp.h2_f013_canonical_lines),\n  (select count(*) from pg_temp.h2_f013_canonical_lines where coalesce((line_json->>'selection_allowed')::boolean,false)),\n  'READY',1,`, 'session dynamic counts');
  prefix = replaceOnce(prefix, 'do $certified_publication$', `${evidenceAfterBuildSql(testCase)}\ndo $certified_publication$`,
    'P3 evidence before publication');
  prefix = replaceOnce(prefix,
    `       or publication_result->>'preview_row_count'<>'3'\n       or publication_result->>'selected_row_count'<>'2' then`,
    `       or (publication_result->>'preview_row_count')::integer<>(select count(*) from pg_temp.h2_f013_canonical_lines)\n       or (publication_result->>'selected_row_count')::integer<>(select count(*) from pg_temp.h2_f013_canonical_lines where coalesce((line_json->>'selection_allowed')::boolean,false)) then`,
    'publication dynamic counts');
  prefix = replaceOnce(prefix, '$certified_publication$;\n\nupdate public.banking_pay_workbench_sessions',
    `$certified_publication$;\n${postPublicationExclusionSql(testCase)}\nupdate public.banking_pay_workbench_sessions`,
    'post-publication exclusion transition');

  const runtime = read(runtimePath);
  const assertion = runtime.slice(runtime.indexOf('-- H2_P3_ASSERTION_BEGIN') + '-- H2_P3_ASSERTION_BEGIN'.length,
    runtime.indexOf('-- H2_P3_ASSERTION_END'));
  const targetSelected = testCase.expected.target_selected === 1;
  const flow = `
insert into public.candidates(id,tms_ref,display_name,pay_method,active,account_holder,sort_code,account_number,bank_details_hash)
values ('45000000-0000-4000-8000-000000000999','H2-P3-UNRELATED','H2 P3 unrelated Candidate','PAYE',true,'H2 P3 unrelated','010203','99887766','h2-p3-unrelated-bank');
create temporary table pg_temp.h2_p3_unrelated_before(digest_sha256 text not null) on commit drop;
insert into pg_temp.h2_p3_unrelated_before
select private.pay_workbench_settled_certificate_sha256_text_v8(
 private.pay_workbench_settled_certificate_stable_stringify_v8(to_jsonb(candidate_row)))
from public.candidates candidate_row where id='45000000-0000-4000-8000-000000000999';

insert into public.banking_pay_operations(
 id,operation_type,status,phase,actor_user_id,workbench_session_id,idempotency_key,input_json,config_json,progress_json
) values (
 '43000000-0000-4000-8000-000000000100','DRAFT_CREATE','RUNNING','VALIDATE_SESSION',
 '10000000-0000-4000-8000-000000000001','43000000-0000-4000-8000-000000000005',
 'h2-p3-${testCase.class_id}-${payChannel.toLowerCase()}',
 '{"pay_date":"2026-08-28","week_start":"2026-08-24","pay_channel_scope":"ALL"}','{}','{}'
);
create temporary table pg_temp.h2_p3_stale_rejection(
 sqlstate text,error_message text,draft_row_delta integer,financial_row_delta integer
) on commit drop;
${targetSelected ? `insert into pg_temp.h2_p3_stale_rejection values (null,null,0,0);` : `
do $h2_p3_stale_reject$
declare v_ids jsonb; v_state text; v_message text; v_before integer; v_after integer;
begin
  select jsonb_agg(id::text order by row_ordinal,id) into v_ids
  from public.banking_pay_workbench_preview_rows
  where session_id='43000000-0000-4000-8000-000000000005'
    and (selected or row_json->'h2_p3_target'='true'::jsonb);
  select count(*) into v_before from public.pay_batches where source_workbench_session_id='43000000-0000-4000-8000-000000000005';
  begin
    perform public.pay_workbench_prepare_draft(
      '43000000-0000-4000-8000-000000000005','10000000-0000-4000-8000-000000000001',
      v_ids,'ALL',null,false,false,null,null,'43000000-0000-4000-8000-000000000100',true,false);
    raise exception 'H2_P3_STALE_SELECTION_UNEXPECTEDLY_ACCEPTED';
  exception when others then get stacked diagnostics v_state=returned_sqlstate,v_message=message_text; end;
  select count(*) into v_after from public.pay_batches where source_workbench_session_id='43000000-0000-4000-8000-000000000005';
  insert into pg_temp.h2_p3_stale_rejection values(v_state,v_message,v_after-v_before,0);
end;
$h2_p3_stale_reject$;`}

update public.banking_pay_operations operation_row
set input_json=operation_row.input_json||jsonb_build_object(
 'expected_workbench_progress_counter_version',session_row.progress_counter_version::text,
 'expected_workbench_selected_preview_row_ids',(
   select jsonb_agg(id::text order by row_ordinal,id) from public.banking_pay_workbench_preview_rows
   where session_id=session_row.id and selected and selection_state='SELECTED' and status='READY'),
 'selection_review_contract_version',1,
 'selection_reviewed_by_user_id','10000000-0000-4000-8000-000000000001')
from public.banking_pay_workbench_sessions session_row
where operation_row.id='43000000-0000-4000-8000-000000000100' and session_row.id=operation_row.workbench_session_id;

do $h2_p3_prepare_scope$
declare v_ids jsonb; v_prepare jsonb; v_scope jsonb; v_scope_ids jsonb; v_seed jsonb; v_result jsonb; v_receipt text; i integer;
begin
 select jsonb_agg(id::text order by row_ordinal,id) into v_ids from public.banking_pay_workbench_preview_rows
 where session_id='43000000-0000-4000-8000-000000000005' and selected;
 v_prepare:=public.pay_workbench_prepare_draft('43000000-0000-4000-8000-000000000005',
   '10000000-0000-4000-8000-000000000001',v_ids,'ALL',null,false,false,null,null,
   '43000000-0000-4000-8000-000000000100',true,false);
 update public.banking_pay_operations set phase='SEED_CANDIDATE_SCOPE' where id='43000000-0000-4000-8000-000000000100';
 select to_jsonb(scope_seed) into v_scope from public.pay_workbench_prepare_draft_scope_seed(
   '43000000-0000-4000-8000-000000000100','43000000-0000-4000-8000-000000000005',
   '10000000-0000-4000-8000-000000000001',v_ids,'ALL','{}') scope_seed;
 update public.banking_pay_operations set phase='SEED_ALLOCATION_ROWS' where id='43000000-0000-4000-8000-000000000100';
 select jsonb_agg(id::text order by chunk_sequence,id) into v_scope_ids
 from public.banking_pay_operation_candidate_scope where operation_id='43000000-0000-4000-8000-000000000100';
 select to_jsonb(seed) into v_seed from public.pay_workbench_prepare_draft_allocation_rows_seed(
   '43000000-0000-4000-8000-000000000100',v_scope_ids) seed;
 select receipt_digest_sha256 into strict v_receipt from private.banking_pay_draft_frozen_stage_receipts_v8
 where operation_id='43000000-0000-4000-8000-000000000100' and stage_kind='CERTIFICATE_PARTITION_REFS'
   and stage_status='TERMINAL' and has_more=false order by page_sequence desc limit 1;
 for i in 1..100 loop
   v_result:=public.banking_pay_draft_advance_bounded_v8('43000000-0000-4000-8000-000000000100','H2_V8_P3_PRIOR_EXCLUSION',v_receipt);
   exit when v_result->>'work_kind'='READY_FOR_TERMINAL_FINISH';
 end loop;
 if v_result->>'work_kind'<>'READY_FOR_TERMINAL_FINISH' then raise exception 'H2_P3_DRAFT_DID_NOT_TERMINATE:%',v_result; end if;
end;
$h2_p3_prepare_scope$;
${assertion}
ROLLBACK;
`;
  return `${bridgePrefix}\n${prefix}\n${flow}`;
}

function runRuntime() {
  const args = new Map();
  for (let index = 3; index < process.argv.length; index += 2) args.set(process.argv[index], process.argv[index + 1]);
  const container = args.get('--container');
  const database = args.get('--database');
  const onlyCase = args.get('--case');
  if (!/^[A-Za-z0-9_.-]+$/.test(container ?? '') || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(database ?? '')) {
    throw new Error('safe --container and --database required');
  }
  const fixture = json(fixturePath);
  const cases = onlyCase ? fixture.classes.filter(row => row.class_id === onlyCase) : fixture.classes;
  if (onlyCase && cases.length !== 1) throw new Error(`unknown P3 case ${onlyCase}`);
  const version = spawnSync('docker', ['exec', container, 'psql', '-X', '-U', 'postgres', '-d', database, '-Atc', 'select version()'], {encoding:'utf8'});
  if (version.status !== 0) throw new Error(version.stderr);
  const results = [];
  for (const testCase of cases) {
    for (const payChannel of fixture.common_assertions.channels) {
      const sql = buildRuntimeSql(testCase, payChannel, database);
      if ((sql.match(/^\s*ROLLBACK\s*;\s*$/gim) ?? []).length !== 1 || (sql.match(/^\s*COMMIT\s*;\s*$/gim) ?? []).length) {
        throw new Error(`${testCase.class_id}/${payChannel} transaction envelope invalid`);
      }
      const started = performance.now();
      const variantOrdinal = payChannel === 'PAYE' ? 1 : 2;
      const execution = spawnSync('docker', ['exec','-i','-e',
        `PGOPTIONS=-c cloudtms.h2_f013_variant_ordinal=${variantOrdinal} -c jit=off`,
        container,'psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d',database], {
        input: sql,encoding:'utf8',maxBuffer:64*1024*1024
      });
      const output = `${execution.stdout ?? ''}\n${execution.stderr ?? ''}`;
      const marker = `H2_P3_PRIOR_EXCLUSION_RUNTIME_PASS=`;
      if (execution.status !== 0 || !output.includes(marker)) {
        throw new Error(`${testCase.class_id}/${payChannel} failed ${execution.status}:\n${output.split(/\r?\n/).slice(-260).join('\n')}`);
      }
      results.push({class_id:testCase.class_id,pay_channel:payChannel,status:'PASS',transaction_outcome:'ROLLBACK',elapsed_ms:Math.round((performance.now()-started)*1000)/1000});
    }
  }
  process.stdout.write(`${JSON.stringify({engine:version.stdout.trim(),container,database,case_count:results.length,pass_count:results.length,fail_count:0,results},null,2)}\n`);
}

if (process.argv[2] === '--run-runtime') {
  runRuntime();
  return;
}

test('P3 fixture binds the complete frozen 12-class group without policy substitution', () => {
  const fixture = json(fixturePath);
  const bindings = json(bindingPath);
  const policy = json(policyPath);
  const groupBindings = bindings.class_execution_bindings.filter(row => row.group_id === fixture.group_id);
  const policyClasses = policy.finite_equivalence_classes.filter(row => expectedClasses.includes(row.class_id));

  assert.equal(fixture.contract, 'BANKING_PAY_DRAFT_V8_PRIOR_EXCLUSION_PARITY_P3_V1');
  assert.equal(fixture.group_id, 'P3_PRIOR_PAYMENT_AND_EXCLUSIONS');
  assert.equal(fixture.scope.proof_kind, 'CURRENT_V8_POLICY_OWNER_DRAFT_OUTPUT_OR_TYPED_REJECTION');
  assert.equal(fixture.scope.v1_v8_typed_parity_status, 'OPEN_NO_ENABLED_HISTORICAL_V1_HARNESS');
  assert.equal(fixture.scope.policy_delta_allowed, false);
  assert.equal(fixture.scope.production_runtime_edits, false);
  assert.equal(fixture.scope.provider_payment_settlement_remittance_actions, false);
  assert.equal(fixture.common_assertions.totals_are_secondary, true);
  assert.deepEqual(fixture.common_assertions.channels, ['PAYE', 'UMBRELLA']);
  assert.deepEqual(fixture.classes.map(row => row.class_id), expectedClasses);
  assert.deepEqual(groupBindings.map(row => row.class_id), expectedClasses);
  assert.deepEqual(policyClasses.map(row => row.class_id), expectedClasses);
  assert.equal(new Set(fixture.classes.map(row => row.class_id)).size, expectedClasses.length);

  for (const row of policyClasses) {
    const bound = fixture.classes.find(candidate => candidate.class_id === row.class_id);
    assert.ok(bound, `missing ${row.class_id}`);
    assert.equal(row.channel, 'PAYE+UMBRELLA');
    assert.equal(row.proof_rule, fixture.frozen_policy_contract.proof_rule);
    assert.equal(bound.policy_condition, row.condition);
  }

  const positive = fixture.classes.filter(row => row.expected.target_selected === 1);
  const excluded = fixture.classes.filter(row => row.expected.target_selected === 0);
  assert.deepEqual(positive.map(row => row.class_id), [
    'part_paid_residual',
    'active_reservation_residual',
    'superseded_absent',
    'cancelled_untouched_reappears_once',
    'snooze_expiry_reappears_once'
  ]);
  assert.deepEqual(excluded.map(row => row.class_id), [
    'fully_settled_absent',
    'timesheet_snoozed_excluded',
    'segment_snooze_isolation',
    'finance_case_snoozed_excluded',
    'action_required_excluded',
    'blocked_excluded',
    'active_draft_excluded'
  ]);
  assert.equal(fixture.classes.length * fixture.common_assertions.channels.length, 24);
});

test('P3 owner identities and the frozen policy artifact are byte-bound', () => {
  const fixture = json(fixturePath);
  assert.equal(sha256(read(policyPath)), fixture.frozen_policy_contract.sha256);
  for (const owner of fixture.owners) {
    assert.equal(sha256(read(owner.path)), owner.sha256, owner.path);
  }
});

test('P3 source owners retain residual, exclusion, currentness and typed rejection gates', () => {
  const residual = read('supabase/repeatable/21072026_1235_09_pay_correction_chain_residual_v1.sql');
  const certificate = read('supabase/repeatable/02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql');
  const selection = read('supabase/repeatable/20072026_0117_banking_pay_preview_selection_revision.sql');
  const cancellation = read('supabase/repeatable/09082026_0825_pay_workbench_patch_preview_after_batch_mutation.sql');
  const integrity = read('supabase/repeatable/03092026_1620_banking_pay_draft_integrity_row_backed_v8.sql');

  assert.match(residual, /raw_outstanding_ex_vat/);
  assert.match(residual, /truth_ex_vat/);
  assert.match(residual, /baseline_ex_vat/);
  assert.match(residual, /reserved_ex_vat/);
  assert.match(residual, /reservation_overrun/);

  assert.match(certificate, /WORKBENCH_CERTIFICATE_SERVER_SELECTION_MISMATCH/);
  assert.match(certificate, /WORKBENCH_CERTIFICATE_DRAFT_GATE_REJECTED/);
  assert.match(certificate, /WORKBENCH_CERTIFICATE_SOURCE_ROW_NOT_DRAFT_ELIGIBLE/);
  assert.match(certificate, /WORKBENCH_CERTIFICATE_SOURCE_AUTHORITY_CHANGED/);
  assert.match(certificate, /'ACTIVE_DRAFT'/);
  assert.match(certificate, /'INELIGIBLE'/);
  assert.match(certificate, /'SNOOZED'/);
  assert.match(certificate, /'ACTION_REQUIRED'/);
  assert.match(certificate, /'BLOCKED'/);
  assert.match(certificate, /ACTIVE_SETTLED_COMPONENT_BASELINE_APPLIED/);
  assert.match(certificate, /CURRENT_CERTIFIED_SOURCE_REPLACES_PROVED_LINEAGE/);
  assert.match(certificate, /RESERVATION_COMPONENT/);

  assert.match(selection, /selection_allowed/);
  assert.match(certificate, /server_selected_preview_row_ids/);
  assert.match(cancellation, /post_draft_unavailable/);
  assert.match(cancellation, /post_draft_overlay_active/);
  assert.match(integrity, /PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED/);
});

test('P3 comparator keeps pre-Draft reservation evidence separate from the new residual item amount', () => {
  const parity = read(parityOwnerPath);
  const validate = source => {
    assert.match(source, /v_recomputed_source_reservation_digest IS DISTINCT FROM v_entry\.expected_reservation_source_digest_sha256/);
    assert.match(source, /v_source_reservation_id_count = 0/);
    assert.match(source, /v_entry\.expected_reservation_amount_ex_vat IS NULL/);
    assert.match(source, /v_allocation_total, 2\) IS DISTINCT FROM pg_catalog\.round\(v_entry\.expected_item_amount_ex_vat::numeric, 2\)/);
    assert.match(source, /v_bad_reservation_count > 0/);
    assert.doesNotMatch(source,
      /pg_catalog\.round\(v_source_amount_total, 2\) IS DISTINCT FROM pg_catalog\.round\(v_entry\.expected_reservation_amount_ex_vat::numeric, 2\)/);
  };
  validate(parity);
  const crossDomainMutation = parity.replace(
    "  ELSIF v_entry.expected_allocated_recovery_amount_ex_vat IS NOT NULL",
    "  ELSIF v_entry.expected_reservation_applicability = 'APPLICABLE'\n" +
    "        AND v_entry.expected_reservation_amount_ex_vat IS NOT NULL\n" +
    "        AND pg_catalog.round(v_source_amount_total, 2) IS DISTINCT FROM pg_catalog.round(v_entry.expected_reservation_amount_ex_vat::numeric, 2) THEN\n" +
    "    v_mismatch := 'DRAFT_PARITY_SOURCE_RESERVATION_AMOUNT_MISMATCH';\n" +
    "  ELSIF v_entry.expected_allocated_recovery_amount_ex_vat IS NOT NULL"
  );
  assert.throws(() => validate(crossDomainMutation));
});

test('P3 result ledger preserves all first-divergence evidence and records dual-engine current-V8 closure', () => {
  const fixture = json(fixturePath);
  const result = json(resultPath);
  assert.equal(result.status, 'CURRENT_V8_P3_LOCAL_DUAL_ENGINE_PASS_HISTORICAL_V1_PARITY_OPEN');
  assert.equal(result.evidence_scope, fixture.scope.proof_kind);
  assert.equal(result.v1_v8_typed_parity_status, fixture.scope.v1_v8_typed_parity_status);
  assert.equal(result.counts.classes_total, 12);
  assert.equal(result.counts.channel_cases_per_engine, 24);
  assert.equal(result.counts.channel_case_executions_total, 48);
  assert.equal(result.counts.classes_current_v8_runtime_pass, 12);
  assert.equal(result.counts.classes_runtime_fail, 0);
  assert.equal(result.counts.classes_runtime_pending, 0);
  assert.equal(result.counts.v1_v8_parity_pass, 0);
  assert.equal(result.counts.policy_or_economic_changes, 0);
  assert.equal(result.counts.provider_payment_settlement_remittance_actions, 0);
  assert.deepEqual(result.class_results.map(row => row.class_id), expectedClasses);
  assert.equal(result.open_findings.length, 0);
  assert.deepEqual(result.findings.map(row => row.finding_id), [
    'P3-F001-CERTIFICATE-SNOOZE-STATE-SHAPE',
    'P3-F002-CERTIFICATE-INELIGIBLE-NULL-BOOLEAN',
    'P3-F003-PARITY-SOURCE-RESERVATION-DOMAIN-CONFLATION'
  ]);
  for (const finding of result.findings) {
    assert.equal(finding.classification, 'FIXED_LOCAL_PASS');
  }
  assert.equal(result.findings[2].pre_change_evidence.expected_pre_draft_active_source_reservation_ex_vat, '25.00');
  assert.equal(result.findings[2].pre_change_evidence.canonical_residual_and_new_draft_item_ex_vat, '75.00');
  assert.equal(result.audit_events[0].superseded_expectation,
    '55000/WORKBENCH_CERTIFICATE_DRAFT_GATE_REJECTED');
  assert.equal(result.audit_events[0].current_first_boundary,
    'P0001/WORKBENCH_SELECTION_CHANGED_REVIEW_REQUIRED');
  for (const engine of ['pg17', 'pg18']) {
    assert.equal(result.engines[engine].status, 'PASS');
    assert.equal(result.engines[engine].channel_cases_pass, 24);
    assert.equal(result.engines[engine].fail, 0);
    assert.equal(result.engines[engine].rollback, '24/24');
    assert.deepEqual(result.engines[engine].post_rollback_fixture_counts,
      {operations: 0, batches: 0, certificates: 0, unrelated_candidate: 0});
  }
  assert.ok(result.blocking_gates.includes('TRUE_V1_V8_TYPED_PARITY_OR_AUTHORITATIVE_HISTORICAL_ORACLE'));
});

test('P3 fixture mutations cannot silently omit a class or relax the policy boundary', () => {
  const fixture = json(fixturePath);
  const mutations = [
    value => value.classes.pop(),
    value => value.classes.push(structuredClone(value.classes[0])),
    value => { value.scope.policy_delta_allowed = true; },
    value => { value.scope.provider_payment_settlement_remittance_actions = true; },
    value => { value.common_assertions.totals_are_secondary = false; },
    value => { value.common_assertions.channels = ['PAYE']; },
    value => { value.frozen_policy_contract.sha256 = '0'.repeat(64); },
    value => { value.scope.v1_v8_typed_parity_status = 'PASS'; }
  ];
  const validate = value => {
    assert.deepEqual(value.classes.map(row => row.class_id), expectedClasses);
    assert.equal(new Set(value.classes.map(row => row.class_id)).size, 12);
    assert.equal(value.scope.policy_delta_allowed, false);
    assert.equal(value.scope.provider_payment_settlement_remittance_actions, false);
    assert.equal(value.common_assertions.totals_are_secondary, true);
    assert.deepEqual(value.common_assertions.channels, ['PAYE', 'UMBRELLA']);
    assert.equal(value.frozen_policy_contract.sha256, sha256(read(policyPath)));
    assert.equal(value.scope.v1_v8_typed_parity_status, 'OPEN_NO_ENABLED_HISTORICAL_V1_HARNESS');
  };
  validate(fixture);
  for (const mutate of mutations) {
    const changed = structuredClone(fixture);
    mutate(changed);
    assert.throws(() => validate(changed));
  }
});

test('P3 runtime fixture is fail-closed, rollback-only and free of external-action owners', () => {
  const sql = read(runtimePath);
  assert.match(sql, /^\\set ON_ERROR_STOP on/m);
  assert.match(sql, /^BEGIN;$/m);
  assert.match(sql, /^SET LOCAL jit = off;$/m);
  assert.match(sql, /^SET LOCAL statement_timeout = '15s';$/m);
  assert.match(sql, /^SET LOCAL lock_timeout = '1500ms';$/m);
  assert.match(sql, /^SET LOCAL idle_in_transaction_session_timeout = '30s';$/m);
  assert.equal((sql.match(/^ROLLBACK;$/gm) || []).length, 1);
  assert.match(sql, /H2_P3_PRIOR_EXCLUSION_RUNTIME_PASS/);
  assert.doesNotMatch(sql, /\b(?:pay_settle|provider_submit|remittance_generate|bank_transfer_submit)\s*\(/i);
});
