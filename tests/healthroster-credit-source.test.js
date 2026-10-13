import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
const root=path.resolve(import.meta.dirname,'..');
const base='supabase/repeatable/13102026_1111_healthroster_credit_runtime';
const scoped='supabase/repeatable/13102026_1143_healthroster_scoped_credit';
const scopedRead=name=>fs.readFileSync(path.join(root,scoped,`13102026_1143_${name}.sqlinc`),'utf8').replaceAll('\r\n','\n');
const read=name=>['start','resolver','generation','legacy'].includes(name)?scopedRead(name):
  fs.readFileSync(path.join(root,base,`13102026_1111_${name}.sqlinc`),'utf8').replaceAll('\r\n','\n');
const closure=fs.readFileSync(path.join(root,'supabase/repeatable/13102026_1111_healthroster_credit_runtime_authority_v1.sql'),'utf8');
const requestRead=name=>fs.readFileSync(path.join(root,'supabase/repeatable/13102026_1127_healthroster_correction_requests',`13102026_1127_${name}.sqlinc`),'utf8').replaceAll('\r\n','\n');

test('credit command identity and source classification cannot manufacture a Timesheet chunk',()=>{
  const start=read('start');
  assert.equal((start.match(/when [vc]\.command_type='GENERATE_CREDIT_NOTE' then 'INVOICE'/g)||[]).length,2);
  assert.match(start,/then \(c\.command_json->>'base_invoice_id'\)::uuid else g\.client_id/);
  const resolver=read('resolver');
  const shape=resolver.slice(resolver.indexOf('shape_errors as ('),resolver.indexOf('credit_results as ('));
  assert.match(shape,/where s\.command_type<>'GENERATE_CREDIT_NOTE' and \(/);
});

test('new helpers cannot escape the explicit final ACL or acquire browser/default access',()=>{
  const inventory=JSON.parse(fs.readFileSync(path.join(root,'docs/healthroster-build/SOURCE_ROUTINE_ACL_INVENTORY.json'),'utf8'));
  const folders={'13102026_1111':base,'13102026_1127':'supabase/repeatable/13102026_1127_healthroster_correction_requests','13102026_1143':scoped};
  const unique=new Map();
  for(const row of inventory.closures) {
    const acl=fs.readFileSync(path.join(root,folders[row.closure],`${row.closure}_acl.sqlinc`),'utf8');
    const statements=acl.split(';').map(s=>s.trim());
    const revokes=statements.find(s=>s.startsWith('revoke all on function'));
    const ownerGrant=statements.find(s=>s.startsWith('grant execute on function')&&s.endsWith('to postgres'));
    const serviceGrant=statements.find(s=>s.startsWith('grant execute on function')&&s.endsWith('to service_role'))||'';
    assert.match(revokes,/from public,anon,authenticated,service_role,authenticator,supabase_admin$/);
    const sources=fs.readdirSync(path.join(root,folders[row.closure])).filter(f=>f.endsWith('.sqlinc')&&!f.endsWith('_acl.sqlinc'))
      .map(f=>fs.readFileSync(path.join(root,folders[row.closure],f),'utf8')).join('\n');
    const names=[...sources.matchAll(/create or replace function\s+([\w.]+)\s*\(/gi)].map(m=>m[1]);
    assert.equal(names.length,row.routines.length,'every source definition must have a reviewed permission row');
    for(const routine of row.routines) {
      assert.ok(names.includes(routine.signature.split('(')[0]));
      assert.ok(acl.includes('alter function '+routine.signature+' owner to postgres;'));
      assert.ok(revokes.includes(routine.signature));
      assert.ok(ownerGrant.includes(routine.signature));
      assert.equal(serviceGrant.includes(routine.signature),routine.service);
      if(!routine.native&&routine.signature.startsWith('private.'))assert.equal(routine.service,false);
      unique.set(routine.signature,routine);
    }
  }
  assert.equal(unique.size,62);
  assert.equal([...unique.values()].filter(r=>r.service).length,24);
});

test('Candidate CREATE and workflow reads retain the exact physical request through repeated corrections',()=>{
  const source=requestRead('request');
  const workflow=source.slice(source.indexOf('function public.timesheet_correction_workflow_projection_v1'),source.indexOf('-- Physical family allocation'));
  const create=source.slice(source.indexOf('function public.timesheet_correction_create_projection_v1'));
  assert.match(workflow,/w\.id=p_workflow_id and w\.candidate_id=p_candidate_id/);
  assert.match(workflow,/private\.timesheet_correction_projection_by_request_v1\(v_request\.id,p_candidate_id\)/);
  assert.match(create,/request\.candidate_id=p_candidate_id/);
  for(const key of ['p_contract_week_id','p_target_timesheet_id','p_anchor_timesheet_id'])assert.match(create,new RegExp(key+' is not null and '+key+' is distinct from'));
  assert.match(create,/raise exception 'CANDIDATE_CONTEXT_STALE'/);
  assert.match(create,/if v_request\.state<>'CANCELLED' then/);
  assert.match(create,/private\.timesheet_correction_projection_by_request_v1\(v_request\.id,p_candidate_id\)/);
  assert.match(source,/CORRECTION_SEND\|'\|\|v_key/);
});

test('atomic approval uses actual claimed native R0 output and raises on every returned partial failure',()=>{
  const source=requestRead('approval'),guard=requestRead('finance_guard');
  assert.match(source,/public\.enqueue_ts_financials\(v_r,'MANUAL'/);
  assert.match(source,/public\.tsfin_dequeue_specific\(array\[v_r\],1\)/);
  assert.match(source,/jsonb_array_length\(v_claim\)<>1/);
  assert.match(source,/public\.tsfin_write_snapshots_and_complete\(jsonb_build_array/);
  assert.match(source,/v_batch_result\.ok_count is distinct from 1 or v_batch_result\.fail_count is distinct from 0/);
  assert.match(source,/v_batch_result\.errors is distinct from '\[\]'::jsonb/);
  assert.match(source,/perform private\.timesheet_correction_reversal_assert_v1\(v_original,v_after_fin\)/);
  assert.match(source,/'mileage_units','mileage_pay_ex_vat'/);
  assert.match(source,/timesheet_correction_reversal_component_assert_v1\(p_original->'additional_units_json'/);
  assert.match(source,/v_result->>'all_success'/);
  assert.match(source,/raise exception 'CORRECTION_COMPLETE_UNIT_AUTHORISATION_FAILED'/);
  assert.match(guard,/after insert or update or delete on public\.timesheets\s+deferrable initially deferred/);
  assert.match(guard,/'_timesheet_authority_hash'/);
  assert.doesNotMatch(source,/update public\.pay_|insert into public\.pay_|mail_outbox|http_post/i);
});

test('nullable Daily scope cannot bypass the native batch or admit an unproved/fake scope',()=>{
  const advance=scopedRead('advance'),native=scopedRead('validator'),qualification=scopedRead('nullable_daily');
  assert.match(advance,/_invoice_correction_validate_batch/);
  assert.match(advance,/'target_revision'/);
  assert.match(native,/CREDIT_SCOPE_AUTHORITY_UNPROVED/);
  assert.match(native,/contract_count=0 and .*member_count=1/s);
  assert.match(native,/timesheet_correction_nullable_daily_contract_v1/);
  assert.match(qualification,/operation\.operation_type='GENERATE_CREDIT_NOTE' and operation\.entity_type='INVOICE'/);
  assert.match(qualification,/request\.target_revision=p_scope->>'target_revision'/);
  assert.match(qualification,/t\.sheet_scope='DAILY' and t\.contract_id is null/);
  assert.match(qualification,/timesheet_correction_signed_witness_v1\(p_timesheet_id,false\)/);
});

test('generic authorisation and the canonical financial writers remain pinned source',()=>{
  for(const filename of ['21072026_1235_32_timesheet_authorise_bulk_atomic.sql','21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql',
    '21072026_1235_37_tsfin_write_current_snapshot_single_bounded.sql','21072026_1235_38_tsfin_write_snapshots_and_complete.sql']) {
    const relative='supabase/repeatable/'+filename;
    const original=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show','HEAD:'+relative],{cwd:root,encoding:'utf8'}).replaceAll('\r\n','\n');
    assert.equal(fs.readFileSync(path.join(root,relative),'utf8').replaceAll('\r\n','\n'),original,filename);
  }
});

test('exact cancellation retains immutable audit assets outside the native cleanup receipt',()=>{
  const source=requestRead('cancel');
  assert.match(source,/c\.workflow_generation=v_p\.candidate_workflow_generation and c\.state='IMMUTABLE'/);
  assert.match(source,/c\.storage_key,c\.review_storage_key,c\.final_signed_storage_key,c\.paper_return_page_key/);
  assert.match(source,/where not \(key\.value=any\(v_retained_assets\)\)/);
  assert.match(source,/is distinct from v_component_audit/);
  assert.ok(source.indexOf("jsonb_set(v_result,'{r2_cleanup_keys}'")<source.indexOf("'state','CANCELLED'"));
  assert.match(source,/tf\.unlocked_by_credit_note_id is not null or tf\.processed_at_utc is not null/);
  assert.doesNotMatch(source,/delete from public\.timesheet_r2_cleanup_queue/i);
});

test('scoped money and locks come only from the approved exact X lines, leaving I1 and all other finance untouched',()=>{
  const context=scopedRead('context'),advance=scopedRead('advance');
  assert.match(context,/l\.invoice_id=p_invoice_id and l\.timesheet_id=v_request\.reversed_timesheet_id/);
  assert.match(context,/'subtotal_ex_vat',-v_net,'vat_amount',-v_vat,'total_inc_vat',-v_gross/);
  assert.doesNotMatch(context,/i\.updated_at|v_invoice\.updated_at/);
  assert.match(advance,/where l\.invoice_id=v_original\.id and l\.timesheet_id=v_x/);
  assert.match(advance,/where tf\.timesheet_id=v_r and tf\.is_current and tf\.locked_by_invoice_id is null/);
  assert.match(advance,/'invoice_locked_invoice_id',v_id,'invoice_locked_at_utc',v_now/);
  assert.doesNotMatch(advance,/update public\.invoices|ts_financials_outbox|unlocked_by_credit_note_id/);
  assert.match(advance,/if not found then raise exception 'CREDIT_SCOPE_ALREADY_CREDITED'/);
});

test('frozen original input and references have no current-document or whole-bundle fallback',()=>{
  const context=scopedRead('context'),receipt=scopedRead('original_input'),planner=scopedRead('planner'),snapshot=scopedRead('snapshot');
  assert.match(context,/CREDIT_SCOPE_SUPPORT_UNPROVED/);
  assert.match(context,/input\.entity_type='TIMESHEET' and input\.entity_id=v_request\.reversed_timesheet_id/);
  assert.match(receipt,/input\.result_json is not distinct from p_frozen_source->'original_document_input_result'/);
  assert.match(receipt,/original\.status='READY' and original\.verified_at_utc is not null/);
  assert.match(planner,/original_document_input_id' and receipt\.result_json is not null then 'COMPLETE'/);
  assert.match(planner,/original_document_input_id' then 'BLOCKED'/);
  assert.match(planner,/and not exists\(select 1 from invoice_ts frozen/);
  assert.match(snapshot,/'lines',coalesce\(scoped_lines\.lines,l\.lines/);
  assert.match(snapshot,/frozen_timesheet_source/);
  assert.match(snapshot,/'unit_price',\(-\(model\.value->>'unit_price'\)::numeric\)::text/);
});

test('request identity uses text business bookings and an immutable initial proposal with no browser table access',()=>{
  const schema=fs.readFileSync(path.join(root,'supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql'),'utf8');
  const invariant=fs.readFileSync(path.join(root,'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_invariants.sqlinc'),'utf8');
  assert.match(schema,/original_booking_id text not null/);
  assert.match(schema,/replacement_booking_id text not null unique/);
  assert.match(schema,/proposal_revision=1/);
  assert.match(schema,/revoke all on private\.timesheet_correction_requests from public,anon,authenticated,service_role/);
  assert.doesNotMatch(invariant,/\['[^\]]*proposal_json/);
  assert.match(invariant,/CORRECTION_REQUEST_ORIGINAL_PROPOSAL_IMMUTABLE/);
  assert.match(invariant,/deferrable initially deferred/);
});

test('credit retry has stable artifact identity and validates creator and persisted negative lines',()=>{
  const source=read('generation'), resume=read('resume');
  const plan=source.slice(source.indexOf('credit_plans as materialized ('),source.indexOf('all_plans as materialized'));
  assert.match(plan,/plan,planned_invoice_id/);
  assert.match(plan,/resume_invoice_id/);
  assert.match(source,/'credit_operation_id',c\.operation_id/);
  assert.match(resume,/cn\.id=p_planned_credit_id and cn\.original_invoice_id=p_source_invoice_id/);
  assert.match(resume,/credit_operation_id'\)=p_operation_id::text/);
  for(const field of ['total_charge_ex_vat','total_pay_ex_vat','vat_amount','total_inc_vat']) {
    assert.ok(resume.includes(`cloned.${field} is distinct from -original.${field}`));
  }
  assert.match(source,/where c\.resume_invoice_id is null\n    on conflict\(id\)/);
});

test('all original writers lock I1 before downstream checks and ordinary Unissue has a relationship fence',()=>{
  const generation=read('generation'), legacy=read('legacy'), guard=read('unissue');
  assert.ok(generation.indexOf('order by i.id for update;')<generation.indexOf('credit_revisions as materialized'));
  assert.ok(legacy.indexOf('where id = p_invoice_id for update;')<legacy.indexOf("raise exception 'CREDIT_ALREADY_EXISTS'"));
  assert.ok(guard.indexOf("cn.header_snapshot_json?'credit_scope'")<guard.indexOf('public._ctms_import_correction_classify_v1'));
  assert.ok(guard.indexOf('where id=p_invoice_id for update')<guard.indexOf("cn.header_snapshot_json?'credit_scope'"));
});

test('whole-credit unlock mutates each financial row once and issue child uses the existing direct invoice contract',()=>{
  const source=read('generation');
  const credit=source.slice(source.indexOf('-- Credit-note COMMIT'),source.indexOf('-- Document work is queued'));
  assert.equal((credit.match(/update public\.timesheets_financials tf/g)||[]).length,1);
  assert.match(source,/'ISSUE_INVOICES','INVOICE',q\.planned_invoice_id/);
  assert.doesNotMatch(source,/'ISSUE_INVOICES','INVOICE_BATCH',null/);
  assert.match(source,/q\.parent_operation_id\|\|'\|'\|\|coalesce\(q\.command_token/);
  assert.match(source,/d\.purpose='FINAL_ISSUE'/);
  assert.match(source,/d\.status='READY' and d\.verified_at_utc is not null/);
});

test('one credit exception cannot retire another credit or the ordinary group',()=>{
  const source=read('dispatcher');
  assert.match(source,/select x\.ordinality,jsonb_build_array\(x\.value\)/);
  assert.match(source,/for v_group in select value from jsonb_array_elements\(v_groups\) loop\s+begin/);
  assert.match(source,/from jsonb_array_elements\(v_group\) x\(value\)/);
  assert.match(source,/c\.lease_token=s\.lease_token and c\.fence_token=s\.fence_token/);
});

test('support membership is exact and immutable after issue; each changed closure finishes in service/browser ACL state',()=>{
  const generation=read('generation'), support=read('support'), acl=read('acl');
  assert.match(generation,/jsonb_object_agg\(m\.timesheet_id::text,m\.member_rows/);
  assert.match(support,/ISSUED_INVOICE_SUPPORT_MEMBER_IMMUTABLE/);
  assert.match(support,/not pg_input_is_valid\(m\.key,'uuid'\) or jsonb_typeof\(m\.value\)<>'array'/);
  assert.ok(closure.indexOf('13102026_1111_acl.sqlinc')>closure.indexOf('13102026_1111_generation.sqlinc'));
  assert.match(acl,/from public,anon,authenticated,service_role,authenticator,supabase_admin/);
  assert.match(closure,/notify pgrst, 'reload schema';/);
  for(const name of ['start','resolver','generation','resume','unissue','legacy','dispatcher','support']) {
    const sql=read(name);
    assert.doesNotMatch(sql,/pg_catalog\.(coalesce|nullif|least|greatest)\s*\(/i);
    assert.doesNotMatch(sql,/\b(?:min|max)\(\s*(?:\w+\.)?(?:timesheet_id|parent_timesheet_id|booking_id)\s*\)/i);
  }
});
