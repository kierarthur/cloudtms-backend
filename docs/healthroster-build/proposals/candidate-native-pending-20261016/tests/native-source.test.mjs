import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
const read=n=>fs.readFileSync(new URL('../'+n,import.meta.url),'utf8').replace(/\r\n/g,'\n');
const source=n=>fs.readFileSync(new URL('../../../../../../healthroster-install2-14102026/'+n,import.meta.url),'utf8').replace(/\r\n/g,'\n');
const f=n=>read('forward/'+n);
const section=(s,a,z)=>{const x=s.indexOf(a),y=s.indexOf(z,x);assert(x>=0&&y>x,a);return s.slice(x,y);};
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');

test('actual7d schema bytes retained; nullable correction retains native PK/FK/UNIQUE and enforces deferred genuine pending',()=>{
 const original=read('../../checklist-v6-contract-correction-20261016/forward/sql/physical-schema.sqlinc'.replace('../../','../'));
 assert.equal(sha(original),'5b2509ffe94bdb19e52f7bcda77ffd22686bd4b18e63b2ab1f8783a63b6b5115');
 const s=read('one-time/01_nullable_pending_schema.sqlinc');
 assert.match(s,/alter column current_timesheet_id drop not null/);
 assert.doesNotMatch(s,/drop constraint|disable trigger|create table|update private\.timesheet_contract_current_placements/i);
 assert.match(s,/deferrable initially deferred/);assert.match(s,/ALLOCATION_NOT_PROCESSED/);
 assert.match(s,/native_pending_v1\(to_jsonb\(actual\),false\) is null/);
});
test('real native withdrawal retains paid/invoice guard, actual revoke and CW-null owner',()=>{
 const old=source('supabase/repeatable/27082026_2004_candidate_weekly_withdrawal_null_target_v1.sql'),s=f('07_native_withdrawal.sqlinc');
 assert(s.includes(section(old,'    select financial_row.* into v_fin','    -- Plan 6.2')));
 assert(s.includes(section(old,'    update public.timesheets set','  perform private._candidate_audit_v1(')));
 assert(s.indexOf('timesheet_contract_move_family_lock_v1')<s.indexOf('    for update;'));
 assert(s.indexOf('timesheet_contract_native_pending_publish_v1')>s.indexOf('    timesheet_id=null,'));
 assert.match(s,/original_timesheet_id/);assert.match(s,/CANDIDATE_NATIVE_CORRECTION_PAIR_REQUIRED/);
});
test('manual source retains complete native financial/evidence path, physical callback after real allocation and before TSFIN',()=>{
 const old=source('supabase/repeatable/10102026_0942_weekly_source_manual_upsert_busy_passthrough_v1.sql'),s=f('09_native_manual_allocation.sqlinc');
 assert(s.includes(section(old,'  IF v_week.timesheet_id IS NOT NULL THEN','  IF v_tsfin_snapshot_json IS NULL THEN')));
 assert(s.includes(section(old,'  v_tsfin_result := public.tsfin_write_current_snapshot_single_bounded(','  PERFORM public._audit_insert(')));
 assert(s.indexOf('timesheet_contract_native_pending_enter_week_v1')<s.indexOf('    FOR UPDATE;'));
 assert(s.indexOf('timesheet_contract_native_allocation_publish_v1')>s.indexOf('UPDATE public.contract_weeks'));
 assert(s.indexOf('timesheet_contract_native_allocation_publish_v1')<s.indexOf('public.tsfin_write_current_snapshot_single_bounded('));
 assert.match(s,/native_allocation_audit_id',v_native_allocation_audit_id/);
 assert.match(s,/CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED/);
});
test('allocation publisher is truthful physical event, actual native booking and flat CAS, no early approval/processed or Move mutation',()=>{
 const s=f('03_allocation_publish.sqlinc');
 assert.match(s,/p_created_now is distinct from true/);assert.match(s,/cw.timesheet_id is distinct from t.timesheet_id/);
 assert.match(s,/'CONTRACT_WEEK_NATIVE_TIMESHEET_ALLOCATED'/);assert.doesNotMatch(s,/CREATED_PROCESSED|MANAGER_APPROVED|APPROVED'|update private\.timesheet_contract_move_(operations|members)/i);
 assert.match(s,/'booking_id',t.booking_id/);assert.doesNotMatch(s,/m\.current_booking_id|CANDIDATE-WEEKLY-|bk_/);
 assert.match(s,/v\.placement_revision=p\.placement_revision/);
 const proof=f('02_allocation_binding.sqlinc');
 assert.match(proof,/processed\.after_json->>'native_allocation_audit_id'=a\.id::text/);
 assert.match(proof,/processed\.actor_user_id=a\.actor_user_id/);
 assert.match(proof,/pg_backend_pid\(\)/);assert.match(proof,/pg_current_xact_id_if_assigned\(\)/);
});
test('H103 remains exactly eight closed keys; current NULL only certified native pending; ordinary alias fallback stays refused',()=>{
 const s=f('04_current_placement.sqlinc'),native=section(s,'  if p.current_timesheet_id is null then','  select v.* into t');
 const keys=[...native.matchAll(/'([a-z_]+)'\s*,/g)].map(x=>x[1]).filter(x=>!['current_timesheet_id','placement_revision'].includes(x));
 assert(native.includes('timesheet_contract_native_pending_v1(to_jsonb(p),false) is null'));
 for(const key of ['version','requested_timesheet_id','family_root_id','current_timesheet_id','current_contract_id','placement_revision','placement_receipt_id','legacy_origin'])assert(native.includes("'"+key+"'"),key);
 assert.match(s,/A previously unknown alias cannot fall through/);
 assert.match(s,/native_allocation_binding_v1\(to_jsonb\(p\),false\)/);
 assert.match(s,/native_allocation_alias_v1\(to_jsonb\(a\)\)/);
 assert(!keys.includes('workflow_id'));
});
test('new native bookings require actual allocation alias/audit, accepted original booking remains immutable',()=>{
 const s=f('03b_native_booking.sqlinc');assert.match(s,/allocated\.booking_id=p_booking_id/);assert.match(s,/native_allocation_alias_v1/);
 for(const n of ['03c_rotation_bindings.sqlinc','03d_electronic_rotation_publish.sqlinc','03e_qr_rotation_publish.sqlinc']){
  const s=f(n);assert.match(s,/m\.current_booking_id/);assert.match(s,/native_booking_bound_v1/);assert.doesNotMatch(s,/update private\.timesheet_contract_move_members/i);
 }
});
test('exact pending accessor fails closed and uses exact family/member/revision native audit index predicates',()=>{
 const s=f('03a_pending_read_enter.sqlinc');assert.match(s,/if not found or p\.current_timesheet_id is not null then return null/);
 assert.match(s,/jsonb_array_length\(matches\)<>1/);assert.match(s,/CURRENT_PLACEMENT_NATIVE_PENDING_UNBOUND/);
 for(const k of ['family_root_id','member_key','placement_revision'])assert(s.includes('placement,'+k+'}'));
 assert.match(s,/array\[source_id,original_id\]/);assert.match(s,/authorisation_record_without_authorised_timesheet/);
});
test('Candidate pending history retains exact old row and removes actions; current pending CW has no fabricated Move17',()=>{
 const s=f('11_candidate_detail.sqlinc');assert.match(s,/v_pending_history or v_contract_move#>>'\{dto,is_history\}'='true'/);
 assert.match(s,/'available_actions','\[\]'::jsonb,'manager_approval',null/);
 assert.match(s,/'candidate_mutation_locked',true/);assert.match(s,/v_tab_bucket:='HISTORY'/);
 assert.match(s,/p_timesheet_id:=null;p_contract_week_id:=\(v_native_pending->>'contract_week_id'\)::uuid/);
 assert.match(s,/p_workflow_id is not null and v_contract_move is null and v_native_pending is null/);
 assert.match(f('08_candidate_context.sqlinc'),/if placement->'current_timesheet_id'='null'::jsonb then return null/);
 assert.match(f('13_page_v2.sqlinc'),/if v_pending is not null or v_move/);
});
test('pending workflow association is direct member/CW/source identity; no same-week claim adoption',()=>{
 const s=f('03f_pending_workflow.sqlinc');assert.match(s,/w\.id=any\(m\.workflow_ids\)/);
 assert.match(s,/w\.target_timesheet_id is null and w\.anchor_timesheet_id is null/);
 assert.match(s,/a\.family_root_id=m\.family_root_id and a\.member_key=m\.member_key/);
 assert.match(s,/native_allocation_alias_v1/);assert.doesNotMatch(s,/max\(|order by.*version|limit 1/i);
});
test('full workflow uses exact external locking kernel before mutable WF locks while original mutation replay remains before pending refusal',()=>{
 const s=f('10_native_workflow_admission.sqlinc');assert.match(s,/Invoicef017235/);
 assert(s.indexOf('timesheet_contract_expense_family_enter_v1')<s.indexOf('  if v_is_service_action then\n    if v_action='));
 assert.match(s,/v_family_workflow\.candidate_id,v_family_workflow\.account_id/);
 assert.match(s,/a\.review_manifest_sha256=w\.review_manifest_sha256/);
 assert.match(s,/a\.expires_at_utc>p_now_utc/);
 assert(s.indexOf("return v_mutation_receipt->'response'")<s.indexOf('-- Original native immutable receipt above remains first.'));
 assert(s.indexOf('return jsonb_build_object(\n          \'ok\',true,\'idempotent_replay\',true')<s.indexOf('v_creation_pending:=private.timesheet_contract_native_pending_enter_week_v1'));
 assert.match(s,/when v_resubmit_pending is not null then/);
 assert.match(s,/CANDIDATE_NATIVE_PENDING_CURRENT_WEEK_REQUIRED/);
});
test('full workflow native component/signature/final document transitions preserved byte-for-byte from a10f',()=>{
 const old=read('../candidate-move-qr-paper-20261016/forward/05_native_paper_source.sqlinc'),s=f('10_native_workflow_admission.sqlinc');
 assert(s.includes(section(old,"  elsif v_action='COMPONENT_PREPARE' then","  if v_action='WORKER_SUBMIT' then")));
 assert(s.includes(section(old,"  if v_action='WORKER_SUBMIT' then","  elsif v_action='REGISTER_REVIEW_COMPONENT' then")));
 assert(s.includes(section(old,"  elsif v_action='REGISTER_FINAL_SIGNED_DOCUMENT' then","  elsif v_action='PAPER_PREPARE' then")));
});
test('original evidence access is separately sealed and does not lend old signatures to changed live facts',()=>{
 const s=f('14_original_evidence_source.sqlinc');
 assert.match(s,/m\.immutable_evidence_hash is distinct from private\.timesheet_contract_move_row_hash_v1\(evidence\)/);
 assert.match(s,/p_timesheet_id is distinct from original_t\.timesheet_id/);
 assert.match(s,/CURRENT_NATIVE_EVIDENCE_REQUIRED/);assert.match(s,/CANDIDATE_NATIVE_PENDING_CURRENT_WEEK_REQUIRED/);
 assert.match(s,/current_t\.candidate_workflow_id,current_t\.candidate_workflow_generation/);
 assert.doesNotMatch(s,/update public\.timesheets|r2_nurse_key\s*:=|r2_auth_key\s*:=/i);
});
test('workflow trigger cache invalidation handles genuine pending without strict current-Timesheet lookup or NULL identity key',()=>{
 const s=f('15_summary_pending.sqlinc');
 const pending=section(s,'    if v_current_timesheet_id is null then','    else\n');
 assert.match(pending,/timesheet_contract_native_pending_for_v1/);
 assert.match(pending,/'CONTRACT_WEEK',v_current_week_id,null,v_current_week_id/);
 assert.doesNotMatch(pending,/'TIMESHEET'|into strict v_current_week_id/);
});
test('frozen delivery covers every byte and private helper ACL; only four existing public RPCs retain service execution',()=>{
 const manifest=JSON.parse(read('SOURCE_MANIFEST.json')),recipe=JSON.parse(read('MOUNT_RECIPE.json'));
 for(const entry of manifest.files)assert.equal(sha(fs.readFileSync(new URL('../'+entry.path,import.meta.url))),entry.sha256,entry.path);
 for(const entry of recipe.payloads)assert.equal(sha(fs.readFileSync(new URL('../'+entry.path,import.meta.url))),entry.sha256,entry.path);
 assert.equal(recipe.functions.length,27);assert(recipe.functions.every(x=>x.securityDefiner));
 const acl=f('16_acl.sqlinc');
 for(const item of recipe.functions){
  assert(acl.includes(`revoke all on function ${item.signature} from public,anon,authenticated,service_role;`));
  assert.equal(acl.includes(`grant execute on function ${item.signature} to service_role;`),item.exposure==='service_role');
 }
 assert.equal(recipe.functions.filter(x=>x.exposure==='service_role').length,4);
 assert.equal(recipe.verification.native,'NOT_RUN');assert.equal(recipe.verification.databaseCalls,0);
});
