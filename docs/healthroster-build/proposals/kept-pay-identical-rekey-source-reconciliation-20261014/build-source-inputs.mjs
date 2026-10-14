import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {functionEvents,splitSqlStatements} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';

const out=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(out,'../../../..');
const current='61ce56ff5a0df2e4e4a85fdbb9efc25267d3c966';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=file=>execFileSync('git',['--no-replace-objects','show',`${current}:${file}`],{cwd:root,maxBuffer:32*1024*1024});
const index=JSON.parse(fs.readFileSync(path.join(out,'../current-final-classification-union-20261014/CURRENT_FINAL_DECLARATION_OWNERS.json'),'utf8')).functions;
const names=[
 'public.weekly_source_upload_context_v1','public.weekly_source_projection_rows_apply_atomic_v1',
 'private.weekly_source_rebill_movement_insert_v1','private.weekly_source_finalise_engine_v1',
 'public.weekly_source_finalise_atomic_v1','public.weekly_source_later_change_decide_atomic_v1',
 'private.weekly_source_office_authority_v1','private.weekly_source_ordinary_projection_current_segments_v1',
 'private.weekly_source_invoice_adjustments_apply_v1','private.weekly_source_invoice_adjustment_line_valid_v1','private.weekly_source_self_bill_report_projection_rows_v1',
 'private.weekly_source_self_bill_report_workspace_v1','public.weekly_source_combined_finalise_workspace_v1',
 'public.weekly_source_combined_review_workspace_v1','public.weekly_source_office_workspace_v1',
 'private.weekly_source_missing_scope_facts_v1','private.weekly_source_accepted_publication_action_record_v1',
 'private.weekly_source_invoice_adjustment_authority_v1','private.weekly_source_office_payment_progress_v1',
 'private.weekly_source_office_next_paid_evidence_page_v1','private.weekly_source_office_lifecycle_phase_v1',
 'private.weekly_source_pay_actor_allowed_v1','private.weekly_source_correct_final_prepared_context_v1',
 'public.weekly_source_import_prepare_atomic_v1','public.weekly_source_correct_final_prepare_atomic_v1',
 'private.weekly_source_protected_match_candidates_v1','public.weekly_source_office_recheck_begin_v1',
 'private.weekly_source_settlement_allocation_v1','private.weekly_source_office_invoice_movements_v1',
 'public.bpay_next_source_paid_evidence_page_v1','private.weekly_source_ordinary_projection_active_movements_v1',
 'private.weekly_source_entitlement_components_v1','private.weekly_source_lock_and_resolve_families_v1',
 'private.weekly_source_is_import_authoritative_v1','private._weekly_source_immutable_record_guard_v1',
 'private._weekly_source_immutable_fact_guard_v1','public.weekly_source_office_authorise_scope_v1'
];
const cache=new Map(),pins=[];
function read(file){if(!cache.has(file)){const bytes=git(file);cache.set(file,bytes);pins.push({commit:current,file,bytes:bytes.length,sha256:sha(bytes)});}return cache.get(file);}
fs.mkdirSync(path.join(out,'inputs'),{recursive:true});
const owners=[];
for(const row of index.filter(x=>names.includes(x.name))){
 const events=functionEvents(read(row.file).toString('utf8')).events;
 const event=events.find(x=>x.kind==='CREATE'&&x.id===row.id);
 assert.ok(event,`Absent actual owner ${row.id}`);assert.equal(sha(event.definition),row.definition_sha256);
 const leaf=row.id.replace(/[^a-zA-Z0-9_.-]/g,'_')+'.sqlinc';
 fs.writeFileSync(path.join(out,'inputs',leaf),event.definition+'\n');
 owners.push({...row,actual_current_commit:current,source_body_sha256:sha(event.body),complete_definition_file:'inputs/'+leaf});
}
for(const name of names)assert.ok(owners.some(x=>x.name===name),`No controlling source owner ${name}`);
const schemaFiles=[
 'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',
 'supabase/migrations/04102026_2155_weekly_source_accepted_publication_actions_v1.sql',
 'supabase/migrations/04102026_1257_weekly_source_local_protected_decision_receipts_v1.sql',
 'supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql',
 'supabase/migrations/30092026_2052_weekly_source_import_preparation.sql',
 'supabase/migrations/30092026_2129_weekly_source_office_resolution.sql'
];
const tableNames=['weekly_source_uploads','weekly_source_upload_rows','weekly_work_events','weekly_source_row_resolutions','weekly_source_projection_publications','weekly_source_projection_memberships','weekly_source_entitlement_decision_bundles','weekly_source_office_row_choices','weekly_source_office_rechecks','weekly_source_import_preparations','weekly_source_accepted_publication_actions_v1','weekly_source_local_protected_decision_receipts','weekly_source_self_bill_report_line_bindings','weekly_source_entitlement_heads','weekly_source_root_authorisations','weekly_source_state_transitions','weekly_source_final_snapshot_lines','weekly_source_entitlement_head_components'];
const tables=[];
for(const file of schemaFiles){
 const text=read(file).toString('utf8');
 for(const statement of splitSqlStatements(text).statements){
  const sql=typeof statement==='string'?statement:statement.sql??statement.text;
  if(!sql)continue;
  const match=sql.match(/create\s+table\s+(?:if\s+not\s+exists\s+)?((?:public|private)\.([a-z_0-9]+))/i);
  if(match&&tableNames.includes(match[2])){const leaf=match[1]+'.table.sqlinc';fs.writeFileSync(path.join(out,'inputs',leaf),sql+'\n');tables.push({name:match[1],file,statement_sha256:sha(sql),complete_statement_file:'inputs/'+leaf});}
 }
}
const runtime=['broker/src/weekly-source/routes.js','broker/src/weekly-source/upload-publication-owner.mjs','broker/src/weekly-source/rule-set-validation.js'];
for(const file of runtime){const bytes=read(file),leaf=file.split('/').at(-1);fs.writeFileSync(path.join(out,'inputs',leaf),bytes);}
const missing=['private.weekly_source_identical_rekey_prior_v1','private.weekly_source_kept_as_paid_facts_v1','public.weekly_source_keep_as_paid_decide_atomic_v1'];
for(const name of missing)assert.ok(!index.some(x=>x.name===name));
const primary=path.resolve(root,'../../cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_PLAN_V2_CONSOLIDATION_13102026/HEALTHROSTER_SELFBILL_IMPLEMENTATION_PLAN_V2/03_IMPLEMENTATION_PLAN.md');
const primaryBytes=fs.readFileSync(primary);
fs.writeFileSync(path.join(out,'SOURCE_INPUTS_AND_OWNER_PINS.json'),JSON.stringify({schema_version:'KEPT_PAY_REKEY_CURRENT_SOURCE_DESIGN_V1',source_only:true,current_root_selected_source_commit:current,primary_plan:{absolute_path:primary,bytes:primaryBytes.length,sha256:sha(primaryBytes),sections:['6.1','6.2','26.11']},source_inputs:pins.sort((a,b)=>a.file.localeCompare(b.file)),complete_existing_owners:owners,selected_existing_tables:tables,missing_planned_functions:missing,finite_index_context:'Selected exact61ce bodies individually match current b5 declaration index. No entire61ce recensus or physical catalogue claim.',native_and_implementation_qualification:false},null,2)+'\n');
console.log(JSON.stringify({current,owners:owners.length,tables:tables.length,inputs:pins.length,primary_sha256:sha(primaryBytes)}));
