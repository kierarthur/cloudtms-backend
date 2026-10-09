// Inert source-only integration. ROOT is the sole native executor.
// Import reads no files and opens no connection. No CLI/main/import dispatch.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const SHA=x=>createHash('sha256').update(x).digest('hex').toUpperCase();
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SIGNATURES=Object.freeze([
 'private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)',
 'private.bpay_next_source_comparison_case_capacity_proof_v2(private.bpay_next_source_comparison_receipt,uuid,uuid,text)'
]);
const FILES=Object.freeze({
 candidate:{file:'08102026_0510_banking_pay_next_overpayment_week_applicability_v1.sql',sha256:'9E2488857DC7AD0823B172E2C4EFBC9EEF6687C0300392ED812E7A99E88F027E'},
 inverse:{file:'08102026_0510_banking_pay_next_overpayment_week_applicability_v1.inverse.sql',sha256:'9B173688EEB2A3826409C5F5059505CBE659E45159079E0F01A516BB781D24E5'}
});
const BEFORE=Object.freeze({
 bpay_next_prepare_case_page_v1:'fb6946e158aafa2327f075fde9b70615',
 bpay_next_source_comparison_case_capacity_proof_v2:'f5e8ccd8c08fb88ebe4a4204f746d713'
});
const AFTER=Object.freeze({
 bpay_next_prepare_case_page_v1:'f533c6f67f4d5d9edad0b465d6742093',
 bpay_next_source_comparison_case_capacity_proof_v2:'f3f3c0bb9bc2b460aeb877dbadfe12f2'
});
export function sourceOnly(){
 const files={};
 for(const [kind,pin]of Object.entries(FILES)){
  const value=fs.readFileSync(fileURLToPath(new URL('./'+pin.file,import.meta.url)));
  assert.equal(SHA(value),pin.sha256,'CASE_WEEK_EXACT_SQL_SOURCE');
  files[kind]={...pin};
 }
 return {native:false,counted:false,awards:[],files,signatures:SIGNATURES};
}
export function candidateSql(){sourceOnly();return fs.readFileSync(new URL('./'+FILES.candidate.file,import.meta.url),'utf8');}
export function inverseSql(){sourceOnly();return fs.readFileSync(new URL('./'+FILES.inverse.file,import.meta.url),'utf8');}
export const catalogueExpression=`(select coalesce(jsonb_agg(jsonb_build_object(
 'name',p.proname,'signature',p.oid::regprocedure::text,'body_md5',md5(p.prosrc),
 'owner',p.proowner::regrole::text,'security_definer',p.prosecdef,
 'language',l.lanname,'config',p.proconfig,'acl',p.proacl::text) order by p.proname),'[]'::jsonb)
 from pg_catalog.pg_proc p join pg_catalog.pg_language l on l.oid=p.prolang
 where p.oid in (to_regprocedure('private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)'),
 to_regprocedure('private.bpay_next_source_comparison_case_capacity_proof_v2(private.bpay_next_source_comparison_receipt,uuid,uuid,text)')))`;

// Predicate-only truth table: VALUES are not seeded financial rows/facts.
// This is a negative branch control, not genuine protected-debt journey proof.
export const selectionPredicateControlExpression=`(with scenarios(label,expected_count,actual_count,
 fc_kind,cc_kind,instruction_kind,payroll_stage,all_rows_exact,expected) as (values
 ('OVERPAYMENT_ONLY',1,1,'OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT',true,true),
 ('EMPTY',0,0,'OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT',true,false),
 ('COUNT_MISMATCH',2,1,'OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT',true,false),
 ('MISSING_COMPONENT',1,1,'OVERPAYMENT',null,'RECOVERY','GROSS_DEDUCT',true,false),
 ('MISSING_CASE',1,1,null,'OVERPAYMENT','RECOVERY','GROSS_DEDUCT',true,false),
 ('LOAN',1,1,'LOAN','LOAN','RECOVERY','GROSS_DEDUCT',true,false),
 ('ADVANCE',1,1,'ADVANCE','ADVANCE','RECOVERY','GROSS_DEDUCT',true,false),
 ('MANUAL_DEBT',1,1,'MANUAL_DEBT','MANUAL_DEBT','RECOVERY','GROSS_DEDUCT',true,false),
 ('PAYOUT',1,1,'OVERPAYMENT','OVERPAYMENT','PAYOUT','GROSS_DEDUCT',true,false),
 ('NET_DEDUCT',1,1,'OVERPAYMENT','OVERPAYMENT','RECOVERY','NET_DEDUCT',true,false),
 ('MIXED_PROTECTED',2,2,'OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT',false,false)),
 actual as (select label,expected,(actual_count=expected_count and actual_count>0
   and (fc_kind,cc_kind,instruction_kind,payroll_stage)
     is not distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT')
   and all_rows_exact) as eligible from scenarios)
 select jsonb_build_object('scope','PREDICATE_ONLY_NOT_FINANCIAL_FACTS',
 'scenario_count',count(*),'all_match',bool_and(eligible=expected)) from actual)`;
export const protectedKernelControlExpression=`(select jsonb_build_object(
 'scope','UNCHANGED_EXISTING_ROW_OWNER_ONLY',
 'protected_count',count(*) filter(where x.protected_recovery),
 'protected_floor_preserved',bool_and(x.taken_amount=5 and x.effective_floor=50
   and x.floor_cap=5 and x.affordability_reason='TAKE_HOME_FLOOR' and x.next_worker_take_home=50))
 from (values('LOAN'::text),('ADVANCE'::text),('MANUAL_DEBT'::text)) k(kind)
 cross join lateral private.bpay_next_case_allocation_row_v1(k.kind,100,100,100,55,null,null,50) x)`;

function assertCatalogue(rows,bodies){
 assert.equal(Array.isArray(rows),true,'CASE_WEEK_CATALOGUE_ARRAY');
 assert.equal(rows.length,2,'CASE_WEEK_EXACT_TWO_EXISTING_OWNERS');
 for(const row of rows){
  assert.equal(row.body_md5,bodies[row.name],'CASE_WEEK_EFFECTIVE_BODY_PIN');
  assert.equal(row.security_definer,false,'CASE_WEEK_SECURITY_INVOKER');
  assert.equal(row.language,'plpgsql','CASE_WEEK_LANGUAGE_UNCHANGED');
  assert.deepEqual(row.config,['search_path=pg_catalog, private, public'],'CASE_WEEK_FIXED_SEARCH_PATH');
 }
}
export async function applyToOuter({c,rootAuthority}={}){
 assert.equal(rootAuthority,'/root','CASE_WEEK_ROOT_ONLY_NATIVE');
 assert.equal(typeof c?.query,'function');assert.equal(typeof c?.sql,'function');
 const source=sourceOnly(),before=await c.query(catalogueExpression);
 assertCatalogue(before,BEFORE);
 await c.sql(candidateSql());
 const after=await c.query(catalogueExpression);assertCatalogue(after,AFTER);
 const posture=rows=>rows.map(({body_md5,...row})=>row);
 assert.deepEqual(posture(after),posture(before),'CASE_WEEK_NO_OWNER_ACL_OR_SECURITY_DRIFT');
 const predicate=await c.query(selectionPredicateControlExpression);
 assert.equal(predicate?.scenario_count,11,'CASE_WEEK_FINITE_CLASS_CONTROLS');
 assert.equal(predicate?.all_match,true,'CASE_WEEK_NON_OVERPAYMENT_DENIED');
 const protectedKernel=await c.query(protectedKernelControlExpression);
 assert.equal(protectedKernel?.protected_count,3,'CASE_WEEK_PROTECTED_KINDS_COMPLETE');
 assert.equal(protectedKernel?.protected_floor_preserved,true,'CASE_WEEK_PROTECTED_FLOOR_UNCHANGED');
 return {kind:'CASE_WEEK_FUNCTION_ONLY_OUTER_TRANSACTION_INSTALL',source,before,after,
  controls:{predicate,protectedKernel},native:true,counted:false,awards:[],
  genuineProtectedPreparationControlsExecuted:false};
}

export function readinessExpression({runId,candidate}={}){
 assert.match(runId??'',UUID,'CASE_WEEK_EXACT_ACTUAL_RUN_UUID');
 assert.match(candidate??'',UUID,'CASE_WEEK_EXACT_ACTUAL_CANDIDATE_UUID');
 return `(with current_worker as (
 select w.*,r.pay_date,r.created_at_utc as run_created_at,
   r.pay_date-(extract(isodow from r.pay_date)::integer-1) as week_start
 from private.bpay_next_run_worker w join private.bpay_next_pay_run r on r.id=w.run_id
 where w.run_id='${runId}'::uuid and w.candidate_id='${candidate}'::uuid),
 prior as (select p.id,p.target_pay_channel,
   exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=p.id) as contribution_present
 from current_worker w join private.bpay_next_run_worker p on p.candidate_id=w.candidate_id
 join private.bpay_next_pay_run r on r.id=p.run_id
 where p.status in ('READY','DRAFT','ISSUED','COMPLETE')
 and r.pay_date>=w.week_start and r.pay_date<w.week_start+7
 and (r.pay_date,r.created_at_utc,p.id)<(w.pay_date,w.run_created_at,w.id)),
 states as (select s.* from private.bpay_next_case_allocation_state s join current_worker w on w.id=s.run_worker_id
 where s.run_id=w.run_id and s.candidate_id=w.candidate_id and s.preparation_revision=w.preparation_revision
 and s.selection_revision=w.case_selection_revision and s.pass_kind='DRAFT' and s.projection_no=0),
 instructions as (select i.* from private.bpay_next_run_case_instruction i join current_worker w on w.id=i.run_worker_id
 where i.run_id=w.run_id and i.candidate_id=w.candidate_id and i.preparation_revision=w.preparation_revision
 and i.selection_revision=w.case_selection_revision),
 selections as (select cs.* from current_worker w cross join lateral (
 select s.* from private.bpay_next_case_selection s
 where s.run_id=w.run_id and s.candidate_id=w.candidate_id
 order by s.selection_revision desc limit 1) cs),
 week as (select ww.* from private.bpay_next_worker_week ww join current_worker w
 on ww.candidate_id=w.candidate_id and ww.pay_week_start=w.week_start)
 select jsonb_build_object(
 'worker_count',(select count(*) from current_worker),
 'worker_status',(select status from current_worker),
 'review_issue_code',(select review_issue_code from current_worker),
 'target_pay_channel',(select target_pay_channel from current_worker),
 'prior_worker_count',(select count(*) from prior),
 'prior_missing_contribution_count',(select count(*) from prior where not contribution_present),
 'prior_umbrella_missing_contribution_count',(select count(*) from prior where target_pay_channel='UMBRELLA' and not contribution_present),
 'week_metadata_count',(select count(*) from week),
 'week_unresolved_contribution_count',(select unresolved_contribution_count from week),
 'selection_count',(select count(*) from selections),
 'selection_sealed',(select status='SEALED' from selections),
 'selected_count',(select selected_count from selections),
 'allocation_state_count',(select count(*) from states),
 'weekly_binding_state',(select weekly_binding_state from states),
 'prepare_stage',(select prepare_stage from states),
 'allocation_status',(select status from states),
 'expected_instruction_count',(select expected_instruction_count from states),
 'captured_instruction_count',(select captured_instruction_count from states),
 'processed_instruction_count',(select processed_instruction_count from states),
 'instruction_count',(select count(*) from instructions),
 'protected_instruction_count',(select count(*) from instructions where case_kind in ('LOAN','ADVANCE','MANUAL_DEBT')),
 'non_overpayment_gross_recovery_count',(select count(*) from instructions where
  (case_kind,instruction_kind,payroll_stage,hold_purpose)
   is distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT','GROSS_RECOVERY')),
 'instruction_selection_mismatch_count',(select count(*) from instructions i where not exists(
  select 1 from private.bpay_next_case_selection_item si where si.run_id=i.run_id and si.candidate_id=i.candidate_id
   and si.selection_revision=i.selection_revision and si.case_id=i.case_id
   and si.case_component_id=i.case_component_id and si.is_selected))))`;
}
export async function probeReadiness({c,actor,runId,candidate,specKey,allReady}={}){
 assert.equal(typeof c?.query,'function');
 assert.match(actor??'',UUID,'CASE_WEEK_ACTUAL_ACTOR_UUID');
 assert.match(specKey??'',/^(?:PAYE|UMBRELLA)_(?:PAYE|UMBRELLA)_(?:-30|0|30)$/,'CASE_WEEK_ORIGINAL_VECTOR');
 assert.equal(typeof allReady,'boolean','CASE_WEEK_ACTUAL_REVIEW_BOOLEAN');
 const catalogue=await c.query(catalogueExpression);assertCatalogue(catalogue,AFTER);
 const context=await c.query(readinessExpression({runId,candidate}));
 return {kind:'CASE_WEEK_EXACT_REVIEW_CONTEXT',specKey,allReady,catalogue,context};
}
