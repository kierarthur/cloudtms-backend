import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),lane=path.resolve(dir,'../../../..');
const root=path.resolve(lane,'../healthroster-v2-backend-13102026');
const read=p=>fs.readFileSync(path.join(dir,p),'utf8'),hash=x=>crypto.createHash('sha256').update(x).digest('hex');
const pins=JSON.parse(read('CONTROLLING_INPUTS.json')).inputs,workerPins=JSON.parse(read('WORKER_INPUTS.json')).inputs;
function functionBody(sql,name){const marker='create or replace function '+name+'(',start=sql.indexOf(marker);assert.ok(start>=0,name);return sql.slice(start,sql.indexOf('$function$;',start)+11);}
// Lexical SQL check only. PostgreSQL semantic compilation remains root-owned.
// Keep function bodies visible; remove quoted literals/comments without using
// a regex that can hide an unterminated single-quoted guard or unmatched paren.
function code(sql){let out='',stack=[],i=0;while(i<sql.length){
  if(sql.startsWith('--',i)){while(i<sql.length&&sql[i]!=='\n')i++;out+='\n';continue;}
  if(sql.startsWith('/*',i)){let depth=1;i+=2;while(i<sql.length&&depth){if(sql.startsWith('/*',i)){depth++;i+=2;}else if(sql.startsWith('*/',i)){depth--;i+=2;}else i++;}assert.equal(depth,0);continue;}
  if(sql[i]==="'"){i++;let closed=false;while(i<sql.length){if(sql[i]==="'"){if(sql[i+1]==="'"){i+=2;continue;}i++;closed=true;break;}i++;}assert.equal(closed,true,'unterminated SQL literal');out+=' STR ';continue;}
  if(sql.startsWith('$function$',i)){i+=10;continue;}
  if(sql[i]==='(')stack.push(i);if(sql[i]===')')assert.notEqual(stack.pop(),undefined,'unmatched close parenthesis');
  out+=sql[i++];
}assert.equal(stack.length,0,'unclosed parenthesis');return out;}
function reverse(body,hunks){for(const h of [...hunks].reverse()){const before=h.oldText??h.before,after=h.newText??h.after;assert.equal(body.split(after).length,2,'reverse hunk cardinality');body=body.replace(after,()=>before);}return body;}
test('all saved controlling root inputs still match the actual read bytes',()=>{
  for(const p of [...pins,...workerPins]){const b=fs.readFileSync(path.join(root,p.path));assert.equal(b.length,p.bytes,p.path);assert.equal(hash(b),p.sha256,p.path);}
});
test('complete FIN overload reverses exactly to the original ordinary/NHSP body',()=>{
  const original=fs.readFileSync(path.join(root,pins[0].path),'utf8');
  assert.equal(reverse(read('04_complete_fin_overload.sqlinc'),JSON.parse(read('FIN_HUNKS.json'))),original);
  assert.equal(JSON.parse(read('FIN_HUNKS.json')).length,3);
});
test('FIN error/admission/actor/delegate order is retained and variant is internal without default',()=>{
  const f=read('04_complete_fin_overload.sqlinc');
  const sequence=['WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED','WEEKLY_SOURCE_FINALISE_REQUEST_UNKNOWN_FIELD',
    'WEEKLY_SOURCE_FINALISE_SCOPE_INVALID','WEEKLY_SOURCE_ISSUE_HASH_REQUIRED','perform private.weekly_source_pay_query_admit_v2();',
    "if v_scope_kind='SELF_BILL_REPORT_SCOPE' then",'perform private.weekly_source_office_authority_v1(v_actor,',
    'return private.weekly_source_self_bill_report_finalise_v1(p_request,p_report_vat_rounding_variant);'];
  let previous=-1;for(const token of sequence){const n=f.indexOf(token,previous+1);assert.ok(n>previous,token);previous=n;}
  assert.match(f,/p_report_vat_rounding_variant text\s*\)/);assert.doesNotMatch(f,/p_report_vat_rounding_variant text\s+default/i);
  assert.doesNotMatch(f,/p_request->>'(?:p_report_vat_rounding_variant|vat_rounding_variant|invoice_only)'/);
});
test('unchanged full Report finalizer preserves zero PAY/frozen rules/F8/G1/raw-vs-target controls',()=>{
  const f=read('05_complete_report_finalise.sqlinc');assert.equal(f,fs.readFileSync(path.join(root,pins[5].path),'utf8'));
  assert.ok(f.indexOf('from public.weekly_source_groups')<f.indexOf('from public.weekly_source_cycles where id='));
  for(const token of ['private.weekly_source_self_bill_report_rules_assert_v1','private.weekly_source_final_upload_assert_v1',
    'WEEKLY_SOURCE_REPORT_LITERAL_GROSS_MISMATCH','WEEKLY_SOURCE_REPORT_INVOICE_TARGET_TOTALS_MISMATCH',
    "private.weekly_source_invoice_only_vector_v1('PAY',0)","if v_facts ? 'row_resolution_id'"])assert.ok(f.includes(token),token);
  assert.doesNotMatch(code(f),/\b(?:tsfin_write|pay_open_atomic|timesheet_authorise_generic)/);
});
test('native receipt hash is complete and retains exact controlling fields/domain',()=>{
  const owner=functionBody(fs.readFileSync(path.join(root,pins[10].path),'utf8'),'private.weekly_source_ordinary_projection_receipt_insert_v1');
  const native=owner.slice(owner.indexOf("'WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_RECEIPT_V1'"),owner.indexOf('  insert into public.weekly_source_ordinary_pay_projection_receipts'));
  const next=read('01_projection_receipt.sqlinc');
  const keys=['final_revision_id','source_cycle_id','client_id','root_timesheet_id','published_timesheet_financial_id','source_profile_kind','source_mode',
    'outcome','source_units','source_unit_manifest_hash','source_expense_authorities','source_expense_manifest_hash','final_manifest_hash',
    'final_policy_fingerprint','service_snapshot_hash','server_calculation_fingerprint','root_before_hash','root_after_hash','request_hash','actor_user_id'];
  for(const key of keys){assert.ok(native.includes("'"+key+"'"),key);assert.ok(next.includes("'"+key+"'"),key);}
  assert.ok(next.includes('source_unit_count=jsonb_array_length'));assert.ok(next.includes('source_expense_manifest_hash=private.weekly_source_sha256_jsonb_v1'));
});
test('pay authority has concrete exact accepted binding/snapshot/run/task/receipt guards',()=>{
  const a=read('02_pay_authority.sqlinc');
  for(const token of ['rs.current_projection_publication_id','b.bound_row_resolution_id','b.export_projection_publication_id',
    'ep.id is distinct from ec.current_projection_publication_id','s.row_resolution_id=rr.id',
    'WEEKLY_SOURCE_FINALISATION_PAY_TASK_MANIFEST_V1','run.run_hash is distinct from expected_run_hash',
    'task.state is distinct from receipt.outcome','task.projection_idempotency_key is distinct from receipt.idempotency_key',
    'receipt.actor_user_id is distinct from run.requested_by_user_id','NATIVE_PAY_ORCHESTRATION_UNPROVED'])assert.ok(a.includes(token),token);
  assert.doesNotMatch(a,/fin\.status|financial\.status|generation\.lifecycle_state='PUBLISHED'\s+then\s+ready:=true/);
});
test('PROPOSED cannot qualify without same-source real publication/approved inventory',()=>{
  const a=read('02_pay_authority.sqlinc');
  for(const token of ['PROPOSED_SOURCE_NOT_PUBLISHED',"bundle.state='COMMITTED'",'private.weekly_source_entitlement_publication_receipts',
    "native_origin->>'final_revision_id'=fr.id::text",'bundle.source_revision_digest=head.source_generation_digest',
    "private.weekly_source_effective_inventory_v1(lineage.timesheet_id)"])assert.ok(a.includes(token),token);
});
test('protected current proof reuses native receipts and refuses old sibling-only/missing evidence',()=>{
  const a=read('02_pay_authority.sqlinc');
  for(const token of ['private.weekly_source_protected_wait_witness_v1','private.weekly_source_protected_wait_receipt_v1',
    't.financial_generation_id=generation.id',"current_receipt->>'generation_id'=generation.id::text",
    "current_receipt->>'lane'='SOURCE_LOCAL'","current_receipt->>'lane'='RETAINED_C1'","current_receipt->>'lane'='DIRECT_NEXT'",
    'PROTECTED_EFFECTIVE_PUBLICATION_UNPROVED','t.booking_id=root.booking_id','private.bpay_next_source_origin_state_v1'])assert.ok(a.includes(token),token);
});
test('unknown active/withheld money is never zero; approved amount remains a family-scoped native value',()=>{
  const a=read('02_pay_authority.sqlinc');assert.ok(a.includes('active_amount text:=null'));assert.ok(a.includes("active_amount:=basis->>'approved_pay_ex_vat'"));
  assert.ok(a.includes("'active_amount_scope','ROOT_FAMILY'"));assert.ok(a.includes("'withheld_pay_ex_vat',null,'withheld_amount_known',false"));
  assert.doesNotMatch(a,/coalesce\([^\n]*(?:active_amount|withheld)[^\n]*,\s*0\)/i);assert.doesNotMatch(a,/'paid'|holds_pay/);
});
test('prepared writer identity never exposes mutable same-ID economics as a retained prepared amount',()=>{
  const a=read('02_pay_authority.sqlinc');assert.ok(a.includes("'prepared_amount_known',false"));
  assert.ok(a.includes('NATIVE_PREPARED_AMOUNT_RETENTION_UNPROVED'));
  assert.doesNotMatch(a,/prepared_amount\s*:=\s*fin\.|fin\.total_pay_ex_vat/);
  assert.doesNotMatch(a,/fin\.is_current\s+is\s+(?:true|false)|fin\.is_current\s*=\s*(?:true|false)/i);
});
test('compact invoice witness retains exactly its original nine keys and strict readiness',()=>{
  const w=read('03_pay_witness.sqlinc');assert.ok(w.includes("a->'invoice_ready' is distinct from 'true'::jsonb"));
  const keys=[...w.matchAll(/'([a-z_]+)',a->/g)].map(m=>m[1]);
  assert.deepEqual(keys,['bound_pay_snapshot_line_id','bound_pay_final_revision_id','bound_pay_projection_receipt_id','published_timesheet_financial_id',
    'invoice_timesheet_id','candidate_id','client_id','contract_id','work_event_id']);
});
test('Report four-facts owner is a complete unchanged native owner with exact cycle-vs-report semantics',()=>{
  const b=fs.readFileSync(path.join(root,pins[6].path),'utf8'),marker='create or replace function private.weekly_source_report_shift_facts_v1';
  assert.equal(read('06_complete_report_shift_facts.sqlinc'),marker+b.split(marker)[1]);
});
test('all proposed read leaves are lock/write/claim/drain-free and owner-private',()=>{
  for(const p of ['01_projection_receipt.sqlinc','02_pay_authority.sqlinc','03_pay_witness.sqlinc','06_complete_report_shift_facts.sqlinc']) {
    const c=code(read(p));assert.match(c,/\bstable\b/i);assert.doesNotMatch(c,/\b(?:insert\s+into|update\s+public\.|delete\s+from|for\s+(?:update|share|key\s+share)|pg_advisory|set_config|claim_ready|drain|apply_jobs_chunk)\b/i,p);
  }
  const acl=read('07_private_acl.sqlinc');assert.ok(acl.includes('private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)'));
  assert.doesNotMatch(acl,/grant execute on function private\./i);
});
test('complete Worker successors reverse exactly, retaining native F8/NHSP/M1/calculator/price hooks',()=>{
  const all=JSON.parse(read('WORKER_HUNKS.json'));
  for(const file of ['worker/upload-staging-adapter.mjs','worker/upload-publication-owner.mjs']){
    const native=fs.readFileSync(path.join(root,'broker/src/weekly-source/'+path.basename(file)),'utf8');
    assert.equal(reverse(read(file),all.filter(h=>h.file===file)),native,file);
  }
});
test('every copied native relative import module remains byte-identical to its controlling source',()=>{
  for(const pin of workerPins.filter(p=>p.path.startsWith('broker/src/weekly-source/'))){
    if(['upload-staging-adapter.mjs','upload-publication-owner.mjs'].includes(path.basename(pin.path)))continue;
    assert.deepEqual(fs.readFileSync(path.join(dir,'worker',pin.path.slice('broker/src/weekly-source/'.length))),
      fs.readFileSync(path.join(root,pin.path)),pin.path);
  }
});
test('native Report upload context successor reverses exactly and supplies actual immutable phase witnesses',()=>{
  const all=JSON.parse(read('WORKER_HUNKS.json')),p='08_complete_upload_context.sqlinc';
  assert.equal(reverse(read(p),all.filter(h=>h.file===p)),fs.readFileSync(path.join(root,'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_upload_context.sqlinc'),'utf8'));
  for(const k of ['environment','agency_id','source_family','authority_scope_kind','authority_scope_version','profile_id','self_bill_report_id',
    'effective_rule_set','row_manifest_hash','projection_publication_id','projection_state','comparison_manifest_hash','issue_set_hash'])assert.ok(read(p).includes("'"+k+"'"),k);
});
test('forward regex repair restores exactly two native money guards without weakening M1/Report body',()=>{
  for(const p of ['09_complete_report_projection_rows_forwardfix.sqlinc','10_complete_m1_projection_rows_forwardfix.sqlinc']){
    const all=JSON.parse(read('WORKER_HUNKS.json')).filter(h=>h.file===p);assert.equal(all.length,2);
    assert.equal((read(p).match(/!~'\^\[-\]\?\[0-9\]\+\$'/g)??[]).length,2);
    const original=p.startsWith('09')?fs.readFileSync(path.join(root,'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_rows.sqlinc'),'utf8')
      :functionBody(fs.readFileSync(path.join(root,'supabase/repeatable/13102026_1561_weekly_source_magnit_contract_price_v1.sql'),'utf8'),'public.weekly_source_projection_rows_apply_atomic_v1');
    assert.equal(reverse(read(p),all),original);
  }
});
test('callback replacements preserve SQL dollar/apostrophe literals and all SQL has closed lexical shape',()=>{
  const native="!~'^[-]?[0-9]+$'";assert.equal('OWNER'.replace('OWNER',()=>native),native);assert.notEqual('OWNER'.replace('OWNER',native),native);
  for(const p of fs.readdirSync(dir).filter(p=>p.endsWith('.sqlinc')))code(read(p));
});
