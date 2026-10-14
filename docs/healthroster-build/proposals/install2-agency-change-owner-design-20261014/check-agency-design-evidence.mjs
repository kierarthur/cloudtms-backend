import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
const read=rel=>fs.readFileSync(path.join(dir,rel));
const text=rel=>read(rel).toString('utf8');
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const inputs=JSON.parse(text('SOURCE_INPUTS.json'));
const design=JSON.parse(text('STORE_FIELDS_AND_TRANSITIONS.json'));
const checks=[];
function check(name,fn){fn();checks.push({name,status:'PASS',scope:'EFFECT_FREE_SOURCE_DESIGN_EVIDENCE'});}
const fn=name=>{const row=inputs.native_owners.find(row=>row.name===name);assert(row,name);return text(row.evidence_path);};
check('exact_actual_current_source_74a9_no_native_seal',()=>{
  assert.equal(inputs.actual_current_git_commit,'74a9c10f7370cd3641ee95d31f6702ecadd900a7');
  assert.equal(inputs.native_calls,false);assert.equal(inputs.database_calls,false);
  assert.equal(inputs.compiled_contract_sha256,null);assert.equal(inputs.qualification_receipt,null);
});
check('all_complete_native_store_and_primary_excerpts_exact',()=>{
  for(const row of [...inputs.native_owners,...inputs.closest_and_native_store_definitions,...inputs.controlling_plan_sections]){
    assert.equal(sha(read(row.evidence_path)),row.evidence_sha256,row.evidence_path);
  }
  assert.equal(inputs.native_owners.length,20);assert.equal(inputs.closest_and_native_store_definitions.length,8);
});
check('planned_callable_absence_is_separate_from_semantic_disposition',()=>{
  assert.equal(inputs.planned_names.length,7);assert(inputs.planned_names.every(row=>row.declaration_found===false));
  assert.match(text('NATIVE_SEMANTIC_MAP_AND_REVIEW_BOUNDARIES.md'),/substantial mechanics present/);
});
check('actual_current_manual_upsert_preserves_query_and_family_guards',()=>{
  const row=inputs.native_owners.find(row=>row.name==='public.contract_week_manual_upsert_atomic');
  assert.equal(row.source_path,'supabase/repeatable/10102026_0942_weekly_source_manual_upsert_busy_passthrough_v1.sql');
  assert.match(fn(row.name),/private\.weekly_source_pay_query_admit_v2\(/);
  assert.match(fn(row.name),/private\.weekly_source_managed_root_guard_v1\(/);
  assert.match(fn(row.name),/p_expected_row_signature/);
});
check('current_generic_authorise_is_actual_next_successor_call_only',()=>{
  const row=inputs.native_owners.find(row=>row.name==='public.timesheet_authorise_generic_atomic');
  assert.equal(row.source_path,'supabase/repeatable/11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql');
  assert.match(fn(row.name),/private\.bpay_next_module_control/);
  assert.match(fn(row.name),/query_pre_root/);
});
check('actual_unchanged_editor_keeps_service_boundary_and_ordinary_admin',()=>{
  const source=fn('public.invoice_apply_edits');
  assert.match(source,/v_service boolean:=coalesce\(auth\.role\(\),''\)='service_role'/);
  assert.match(source,/if\(not found or v_actor_role<>'admin'\) and not v_service then/i);
  assert.match(source,/INVOICE_ADMINISTRATOR_PERMISSION_REQUIRED/);
  assert.match(source,/IMPORT_CORRECTION_INVOICE_FINANCIAL_EDIT_FORBIDDEN/);
  assert.match(source,/remove_invoice_line_ids/);assert.match(source,/add_timesheet_ids/);
});
check('current_send_agency_is_issued_origin_not_preset_t0_amendment',()=>{
  const source=fn('public.timesheet_correction_send_v1');
  assert.match(source,/AGENCY_WHOLE_CORRECTION_REQUIRES_ISSUED_PARENT/);
  assert.match(source,/v_req\.proposal_json:=p_request->'proposal'/);
  assert.match(source,/replacement_timesheet_id:=gen_random_uuid\(\)/);
  assert.match(source,/private\.timesheet_correction_a0_send_family_fence_v1/);
});
check('existing_correction_store_requires_distinct_physical_p0_and_terminal_unit',()=>{
  const row=inputs.closest_and_native_store_definitions.find(row=>row.name==='private.timesheet_correction_requests');
  const source=text(row.evidence_path);
  assert.match(source,/replacement_timesheet_id<>reversed_timesheet_id and replacement_timesheet_id<>root_timesheet_id/);
  assert.match(source,/reversal_timesheet_id is not null/);
  assert.match(source,/state in\('WAITING','APPLIED','CANCELLED'\)/);
});
check('ordinary_recompute_uses_actual_outbox_identity_without_new_run_task',()=>{
  const row=inputs.closest_and_native_store_definitions.find(row=>row.name==='public.ts_financials_outbox');
  assert.match(text(row.evidence_path),/id uuid not null/);
  assert.match(fn('public.enqueue_ts_financials'),/ON CONFLICT \(timesheet_id, reason\)/);
  assert.equal(design.fields.some(row=>/run_id|task_id|fake|prepared/.test(row.name)),false);
  assert.equal(design.fields.find(row=>row.name==='outbox_id').references,undefined);
});
check('w14_weekly_only_and_signed_daily_native_scope_remain_distinct',()=>{
  assert.match(fn('public.weekly_source_office_authorise_scope_v1'),/sheet_scope is distinct from 'WEEKLY'/);
  assert.match(fn('private.weekly_source_signed_check_only_v1'),/sheet_scope::text in \('WEEKLY','DAILY'\)/);
  assert.match(fn('private.weekly_source_signed_check_only_v1'),/VALIDATION_ONLY/);
});
check('native_pay_predicate_requires_real_active_actor',()=>{
  const source=fn('private.weekly_source_pay_actor_allowed_v1');
  assert.match(source,/actor\.is_active is true/);assert.match(source,/payment_authoriser/);assert.match(source,/payment_golden_key/);
});
check('same_draft_native_owner_already_checks_full_placement_and_unrelated_lines',()=>{
  const source=fn('private.timesheet_correction_draft_round_trip_v1');
  assert.match(source,/placement_complete/);assert.match(source,/v_unrelated/);
  assert.match(source,/AGENCY_ROUND_TRIP_UNRELATED_PLACEMENT_CHANGED/);
  assert.match(source,/public\.invoice_apply_edits/);
});
check('single_planned_private_store_closed_fields_and_owner_only_wrapper',()=>{
  assert.equal(design.table,'private.weekly_source_agency_timesheet_change_requests');
  assert.equal(design.fields.length,40);assert.equal(new Set(design.fields.map(row=>row.name)).size,40);
  assert.equal(design.direct_table_roles.service_role,false);assert.equal(design.direct_table_roles.authenticated,false);
  assert.equal(design.rls.forced,true);assert.deepEqual(design.rls.policy_roles,['CURRENT_USER']);
  assert.equal(design.entries[1].service_execute,false);assert.equal(design.entries[1].owner_only,true);
  assert.equal(design.entries[0].phase_selector,false);
});
check('immutable_capture_conflict_keeps_true_phase_hold_and_no_unsafe_cleanup',()=>{
  assert.match(design.replay.source_or_row_conflict,/no silent rebase/);assert.equal(design.replay.cleanup,'none');
  const fields=['initial_timesheet_id','initial_timesheet_version','source_cas','source_publication_id','original_draft_invoice_id','actor_user_id'];
  for(const name of fields)assert.equal(design.fields.find(row=>row.name===name)?.immutable,true,name);
  assert.match(text('STORAGE_PHASE_AND_OWNER_PROPOSAL.md'),/DA-6 explicitly allows a truthful recoverable hold/);
});
check('no_executable_owner_mounted_or_fabricated_native_design_verdict',()=>{
  assert.equal(design.status,'DESIGN_FOR_INDEPENDENT_REVIEW_NOT_IMPLEMENTED');
  assert(design.native_holds.includes('hidden repeatable-read writer schedules'));
  assert.match(text('NATIVE_SEMANTIC_MAP_AND_REVIEW_BOUNDARIES.md'),/No agency SQL\/route\/Office owner is installed/);
});
const result={schema:'AGENCY_SINGLE_STORE_SOURCE_DESIGN_CHECKS_V1',actual_source_pin:inputs.actual_current_git_commit,
  checks,pass_count:checks.length,native_calls:false,database_calls:false,implementation_complete:false,critic_review_pending:true};
fs.writeFileSync(path.join(dir,'SOURCE_CHECK_RESULT.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({pass_count:checks.length,scope:'DESIGN_SOURCE_EVIDENCE_ONLY',implementation_complete:false}));
