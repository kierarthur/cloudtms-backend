import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=[];
function read(file){const b=fs.readFileSync(path.join(root,file));inputs.push({path:file,sha256:sha(b),bytes:b.length});return b.toString('utf8');}
const contract=JSON.parse(read('supabase/release/current-contract.json'));
const targets=[
 ['supabase/repeatable/17092026_0200_weekly_source_rotation_authority_v1.sql',['private.weekly_source_candidate_serial_gate_v1','private.weekly_source_lock_family_rows_v1']],
 ['supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql',['public._pay_workbench_candidate_serial_try_gate','public._pay_workbench_candidate_serial_active_state','public._pay_workbench_candidate_serial_audit','public._pay_workbench_candidate_serial_is_candidate_job','public._pay_workbench_candidate_serial_key','public._pay_workbench_candidate_serial_candidate_id','public._pay_timesheet_rotation_scope']],
 ['supabase/repeatable/08012026_invoice_rpcs_issue_hold_paid_creditnote.sql',['public._audit_insert']],
 ['supabase/repeatable/10102026_1347_import_expense_qr_approval_v1.sql',['public.candidate_workflow_transition_atomic_v1','public.candidate_submission_finalize_atomic_v1']],
 ['supabase/repeatable/04092026_1420_candidate_finalisation_single_flight_v1.sql',['public.candidate_submission_finalize_single_flight_v1']],
 ['supabase/repeatable/27082026_2004_candidate_weekly_withdrawal_null_target_v1.sql',['private._candidate_weekly_withdrawal_reset_v1']],
 ['supabase/repeatable/06092026_1636_candidate_advanced_expense_component_policy_v1.sql',['private._candidate_expense_components_sync_v1','private._candidate_expense_owner_context_lock_v1','private._candidate_expense_components_sync_financial_v1']],
 ['supabase/repeatable/08092026_0918_candidate_expense_update_root_snapshot_validation_v1.sql',['public.candidate_expense_update_submit_atomic_v1']],
 ['supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql',['public.weekly_source_group_rule_assign_atomic_v1','public.weekly_source_group_rule_feature_set_atomic_v1']],
 ['supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10d_retained_report_and_date_boundaries.sqlinc',['public.weekly_source_client_settings_save_atomic_v1']],
 ['supabase/repeatable/21072026_1235_37_tsfin_write_current_snapshot_single_bounded.sql',['public.tsfin_write_current_snapshot_single_bounded']],
 ['supabase/repeatable/21072026_1235_38_tsfin_write_snapshots_and_complete.sql',['public.tsfin_write_snapshots_and_complete']],
 ['supabase/repeatable/13102026_1143_healthroster_scoped_credit/13102026_1143_generation.sqlinc',['private._invoice_generation_advance_core_v8']],
 ['supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/05_complete_admission.sqlinc',['public.weekly_source_invoice_admit_atomic_v1']],
 ['supabase/repeatable/13102026_1705_weekly_source_invoice_adjustments/01_invoice_apply_edits.sqlinc',['public.invoice_apply_edits']],
 ['supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql',['private._timesheet_route_version_core_v1','public.timesheet_route_version_rotate']],
 ['supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_context.sqlinc',['private.timesheet_correction_signed_witness_v1']],
 ['supabase/repeatable/21072026_1235_36_tsfin_load_context_batch.sql',['public.tsfin_load_context_batch']],
 ['supabase/repeatable/06012026_tsfin_load_weekly_context_batch.sql',['public.tsfin_load_weekly_context_batch']],
 ['supabase/repeatable/06012026_tsfin_load_nhsp_shifts_batch.sql',['public.tsfin_load_nhsp_shifts_batch']],
 ['supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql',['public.hr_weekly_apply_transactional']],
 ['supabase/repeatable/21072026_1820_08_hr_daily_apply_transactional.sql',['public.hr_daily_apply_transactional']],
 ['supabase/repeatable/19122025_add_ready_to_pay_to_timesheets_summary_views.sql',['public.timesheets_invalidate_prevalidation_on_change']],
 ['supabase/repeatable/21072026_1235_00_import_correction_policy_helpers.sql',['public._ctms_correction_policy_envelope_read_v1','public._ctms_assert_tsfin_snapshot_policy_v1']],
 ['supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql',['private._timesheet_settings_authority_frozen_v1','private._contract_settings_effective_core_v1','private._contract_settings_after_contract_v1','private._contract_settings_after_client_v1']]
];
const routines=[],bodies=[];
for(const [file,names] of targets){const text=read(file);
 const definitions=[...text.matchAll(/create\s+(?:or\s+replace\s+)?function\s+([\w.]+)\s*\([\s\S]*?\bas\s+(\$\w*\$)([\s\S]*?)\2\s*;/gi)];
 for(const name of names){const matches=definitions.filter(m=>m[1].toLowerCase()===name);assert.equal(matches.length,1,file+' '+name);
  const m=matches[0],raw=m[0],body=m[3],masked=body.replace(/--[^\n]*/g,'').replace(/'(?:''|[^'])*'/g,"''");
  const statements=masked.split(';').map(s=>s.trim()).filter(s=>/\bfor\s+(?:update|share)|\bpg_(?:try_)?advisory|\binsert\s+into|\bupdate\s+(?:public|private)\.|\bdelete\s+from/i.test(s));
  const calls=[...new Set([...masked.matchAll(/\b((?:public|private)\.\w+)\s*\(/g)].map(x=>x[1]))].sort();
  routines.push({file,name,sourceDefinitionSha256:sha(raw),line:text.slice(0,m.index).split('\n').length,
   lockMutationStatements:statements,callees:calls,status:'EXACT_SOURCE_BODY_NOT_INSTALLED_RUNTIME_PROOF'});
  bodies.push('-- INPUT ONLY '+file+' / '+name+'\n'+raw);
 }
}
const index=read('broker/src/index.js');
const workerOwners=['handleContractsUpdate','handleContractsReplace','handleContractsCloneAndExtend','handleSettingsFinanceWindows'];
const js=[];
for(const name of workerOwners){const start=index.search(new RegExp('(?:async\\s+)?function '+name+'\\s*\\('));assert.ok(start>=0,name);
 const next=index.slice(start+10).search(/\n(?:async\s+)?function\s+/);const body=next<0?index.slice(start):index.slice(start,start+10+next);
 js.push({name,sourceBodySha256:sha(body),line:index.slice(0,start).split('\n').length,
  mutationPaths:[...new Set([...body.matchAll(/\/rest\/v1\/(contracts|contract_weeks|settings_finance_windows)/g)].map(m=>m[1]))],
  status:'ACTUAL_HTTP_ROW_MUTATION_ENTRY_NOT_ATOMIC_ACROSS_FOLLOWUPS'});
}
const tables=['timesheets','timesheets_financials','nhsp_shifts','candidate_expense_components','weekly_source_row_timesheet_lineages',
 'contracts','clients','candidates','umbrellas','client_settings','weekly_source_group_clients','weekly_source_client_policies',
 'candidate_submission_workflows','candidate_approval_requests','candidate_submission_components','contract_weeks','timesheet_evidence','timesheet_validations','invoice_lines'];
const fk=contract.relations.filter(r=>r.schema==='public'&&tables.includes(r.name)).flatMap(r=>r.constraints.filter(c=>c.type==='f').map(c=>({relation:r.schema+'.'+r.name,...c})));
const triggers=contract.triggers.filter(t=>t.schema==='public'&&tables.includes(t.table));
const auditTriggers=contract.triggers.filter(t=>t.schema==='public'&&t.table==='audit_events');
read('supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql');
fs.writeFileSync(path.join(here,'NATIVE_WRITER_INPUT_BODIES.sqlinc'),bodies.join('\n\n')+'\n');
fs.writeFileSync(path.join(here,'WRITER_LOCK_CENSUS.json'),JSON.stringify({contract:'HEALTHROSTER_A0_WRITER_LOCK_SOURCE_CENSUS_V1',
 status:'SOURCE_AUDIT_WITH_EXPLICIT_NATIVE_HOLDS',inputs,routines,workerEntries:js,foreignKeys:fk,triggers,auditTriggers,
 declaredRuleAssignmentFk:'13102026_1133 migration declares source_group_id REFERENCES weekly_source_groups(id); actual installed constraint/RI checks remain required.',
 uncovered:[
  {owner:'public.timesheet_validations INSERT/repoint',source:'current-contract.json + actual HR Weekly/Daily writers',reason:'No Timesheet FK exists on timesheet_validations. Existing rows receive NOWAIT locks and the exact normal Worker read is fingerprinted/compared, but absent-row insertion is not fenced by the Timesheet parent. Every actual active validation creator must prove the same native family admission, or receive a narrowly integrated native writer guard. No invented FK or table lock is asserted.'},
  {owner:'public.invoice_lines INSERT/repoint',source:'current-contract.json + native invoice owner boundary',reason:'No Timesheet FK exists on invoice_lines.timesheet_id; only invoice_id is FK-backed. Existing lines receive NOWAIT locks, but a concurrent first line may not be excluded by the Timesheet parent alone. The normal native invoice creator/issued-parent contract must separately qualify its exact shared family/Candidate admission before this A0 route can be exposed. No invoice calculator or global invoice table mutex is introduced.'},
  {owner:'public.tsfin_load_context_batch',source:'21072026_1235_36',reason:'Candidate alias/key matching may involve foreign Candidate rows. Selected Candidate row lock alone does not fence foreign alias mutations. Native weekly Contract/FIN identity preservation through final authorisation requires joined first-use/alias-race proof; no global Candidate table mutex invented.'},
  {owner:'timesheets_financials trigger closure',source:'current-contract.json trigger list',reason:'Native bpay_clock_changed_v1, bpay_next_ordinary_authorisation_trigger_v1, pay_workbench_mark_candidate_dirty, summary/invoice-revision/retention/document and expense sync leaves remain call-only. This package does not certify complete Banking late-hook lock/operation/GUC cleanup graph.'},
  {owner:'public.timesheets booking family creation',source:'native route/Candidate/import writers',reason:'Native I-1 advisory fence covers cooperating canonical booking writers. A new raw booking sibling with NULL/foreign Contract is not protected by the selected Contract FK alone; no raw service DML/unsupported producer may be admitted as a qualified A0 family creator without native gate proof.'}
 ],
 limitation:'Recorded exact complete source bodies and executable lock/mutation statements/callees; source metadata and static census do not replace canonical PostgreSQL/API proof. SQL includes are INPUT ONLY and must never be independently installed. No DB/network/payment/provider/email calls executed.'},null,2)+'\n');
console.log(JSON.stringify({inputs:inputs.length,routines:routines.length,workerEntries:js.length,foreignKeys:fk.length,triggers:triggers.length}));
