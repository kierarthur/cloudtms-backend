import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { here, signatures, inputs, sha, once, CAPTURE_HUNK, AMOUNT_HUNK, reverseVerifierForward } from './build-proposal.mjs';
const read=p=>fs.readFileSync(path.join(here,p),'utf8');
const receipt=JSON.parse(read('SOURCE_RECEIPT.json')),info=JSON.parse(read('CODEC_FIELDS.json'));
const root=path.resolve(here,'../../../../../healthroster-v2-backend-13102026');
// Use the current canonical source scanner, not a SQL execution tool.
const scannerPath=path.join(root,inputs.scanner);
const { scanManagedSql }=await import(pathToFileURL(scannerPath));
const allSQL=fs.readdirSync(here).filter(p=>p.endsWith('.sqlinc')).map(p=>[p,read(p)]);
const primary=allSQL.filter(([p])=>/^0[2-5]_/.test(p));
const apply=read('06_complete_ordinary_apply.sqlinc'),capture=read('03_capture_context.sqlinc'),guard=read('04_receipt_insert_guard.sqlinc'),amount=read('05_retained_amount.sqlinc');
function dropAddedRegistry(text){let r=text;for(const s of signatures)r=once(r,`    ('${s}'),\n`,'');return r;}
test('all complete proposal SQL roots have official statement packaging',()=>{
 for(const [p,s] of allSQL)assert.ok(scanManagedSql(s).statements.length>0,p);
 for(const p of fs.readdirSync(path.join(here,'inputs')))assert.ok(scanManagedSql(read('inputs/'+p)).statements.length>0,p);
});
test('exact native ordinary owner reverses completely',()=>assert.equal(once(apply,CAPTURE_HUNK,''),read('inputs/ordinary_apply.sqlinc')));
test('complete Report read reverses every additive change',()=>{
 let r=once(read('07_complete_report_pay_authority.sqlinc'),AMOUNT_HUNK,'');
 r=once(r,"'prepared_pay_ex_vat',prepared_amount,'prepared_amount_known',prepared_amount is not null,","'prepared_pay_ex_vat',prepared_amount,'prepared_amount_known',false,");
 r=once(r,"case when position='PREPARED_FOR_AUTHORISATION' and prepared_amount is null\n      then 'NATIVE_PREPARED_AMOUNT_RETENTION_UNPROVED'","case when position='PREPARED_FOR_AUTHORISATION'\n      then 'NATIVE_PREPARED_AMOUNT_RETENTION_UNPROVED'");
 r=once(r,"    'prepared_amount_scope','ROOT_FAMILY','prepared_amount_authority',case when prepared_amount is not null\n      then 'IMMUTABLE_NATIVE_WRITER_OUTPUT' else null end,\n",'');
 assert.equal(r,read('inputs/report_authority.sqlinc'));
});
test('receipt insertion and bounded writer are retained call-only inputs',()=>{
 assert.equal(sha(read('inputs/receipt_insert.sqlinc').trimEnd()),receipt.retainedOwners.receiptInsertSha256);
 assert.equal(sha(read('inputs/bounded_writer.sqlinc').trimEnd()),receipt.retainedOwners.boundedWriterSha256);
 assert.equal(allSQL.filter(([,s])=>/create or replace function private\.weekly_source_ordinary_projection_receipt_insert_v1/.test(s)).length,0);
 assert.equal(allSQL.filter(([,s])=>/create or replace function public\.tsfin_write_current_snapshot_single_bounded/.test(s)).length,0);
});
test('90-field codec is the exact complete native INSERT output field set plus metadata',()=>{
 const writer=read('inputs/bounded_writer.sqlinc');
 const fields=writer.match(/INSERT INTO public\.timesheets_financials \(\n([\s\S]*?)\n  \)\n  VALUES/)[1].split(',').map(s=>s.trim());
 assert.deepEqual(info.fields,[...new Set(['id','created_at','updated_at',...fields])].sort());
 assert.equal(info.fields.length,90);assert.equal(info.numeric.length,39);assert.equal(info.json.length,9);
});
test('all decimal quantities, amounts and historical rates use native exact strings',()=>{
 const codec=read('02_codec.sqlinc');
 for(const key of info.numeric)assert.ok(codec.includes(`'${key}',p_fin.${key}::text`),key);
 for(const key of info.json)assert.ok(codec.includes(`'${key}',p_fin.${key}::text`),key);
 // This boundary intentionally never decodes these values through Number.
 assert.equal('9007199254740993.01','9007199254740993.01');
 assert.notEqual(String(Number('9007199254740993.01')),'9007199254740993.01');
 assert.doesNotMatch(codec,/::float|double precision|round\(|coalesce\(p_fin\./i);
});
test('codec covers additional, expense, mileage, VAT, rates, policy and raw detail',()=>{
 for(const key of ['additional_pay_ex_vat','additional_units_json','expenses_pay_ex_vat','travel_pay_ex_vat',
  'mileage_units','mileage_pay_rate','pay_vat_amount_snapshot','pay_total_inc_vat_snapshot','pay_day',
  'policy_snapshot_json','rate_source_refs_json','invoice_breakdown_json','actual_schedule_json'])assert.ok(info.fields.includes(key),key);
});
test('codec top-level membership is closed and does not fake absent numeric zero',()=>{
 for(const s of [read('02_codec.sqlinc'),amount]){
  assert.match(s,/array_agg\(k order by k\).*jsonb_object_keys/s);
  assert.match(s,/PREPARED_OUTPUT_CODEC_INVALID/);
  assert.doesNotMatch(s,/coalesce\([^\n]*total_pay_ex_vat[^\n]*,\s*0\)/i);
  assert.match(s,/'total_pay_ex_vat'\)='null'/);
 }
});
test('codec is STABLE because native timestamp JSON serialization is session-sensitive',()=>assert.match(read('02_codec.sqlinc'),/financial_codec_v1\([\s\S]*?language plpgsql stable security definer/));
test('Report-only supported predicate uses actual frozen version and complete report purpose',()=>{
 const s=read('02_codec.sqlinc').split('create or replace function private.weekly_source_report_prepared_financial_codec_v1')[0];
 assert.match(s,/weekly_source_frozen_rule_values_v1\(p_final_revision_id\)/);
 for(const key of ['SEPARATE_FINAL_REPORT','ACACIUM_SELF_BILL_REPORT_V1','FOLLOW_FINAL_REPORT_LINES','SOURCE_AMOUNT',
  'REPORT_LINE_NET','LINE_VAT_FROM_REPORT','ON_ACCEPTED_REPORT_OR_PROTECTED'])assert.ok(s.includes(key),key);
 assert.doesNotMatch(s,/current_setting|set_config|\.active|is_current|'COMPLETE'/);
});
test('public request closure and all native admission precede writer/capture',()=>{
 assert.match(apply,/'schema_version','service_snapshot'/);
 assert.equal(apply.match(/v_keys text\[\]:=array\[[\s\S]*?\];/)[0],read('inputs/ordinary_apply.sqlinc').match(/v_keys text\[\]:=array\[[\s\S]*?\];/)[0]);
 assert.ok(apply.indexOf('v_request_hash:=')<apply.indexOf('v_write_result:=public.tsfin_write_current_snapshot_single_bounded'));
 assert.ok(apply.indexOf('v_canonical_tsfin:=private.weekly_source_ordinary_projection_snapshot_assert_v1')<apply.indexOf(CAPTURE_HUNK));
 assert.equal(apply.match(/weekly_source_report_prepared_output_capture_v1/g).length,1);
});
test('capture follows actual successful native writer and precedes expenses',()=>{
 assert.ok(apply.indexOf('WEEKLY_SOURCE_ORDINARY_TSFIN_WRITE_FAILED')<apply.indexOf(CAPTURE_HUNK));
 assert.ok(apply.indexOf(CAPTURE_HUNK)<apply.indexOf('perform private.weekly_source_ordinary_projection_expenses_materialise_v1'));
 assert.ok(apply.indexOf('v_write_result:=')<apply.indexOf(CAPTURE_HUNK));
 assert.match(CAPTURE_HUNK,/v_server_calculation_fingerprint,v_write_result/);
 assert.doesNotMatch(CAPTURE_HUNK,/v_canonical_tsfin|v_service_snapshot[^_]|total_pay/);
});
test('capture compares the complete eight-key returned native result',()=>{
 assert.match(capture,/p_write_result is distinct from v_expected_result/);
 for(const key of ['ok','timesheet_id','timesheet_financials_id','processing_status','timesheet_version','updated_at','computed_at_utc','version_marker'])assert.ok(capture.includes(`'${key}'`),key);
 assert.match(capture,/WRITER_RESULT_CHANGED/);
 assert.match(capture,/v_output:=private\.weekly_source_report_prepared_financial_codec_v1\(v_fin\)/);
 assert.match(capture,/select \* into strict v_fin from public\.timesheets_financials/);
});
test('context is freshly owner-created with exact backend/XID/request ownership',()=>{
 assert.match(capture,/if pg_catalog\.to_regclass\('pg_temp\.[^']+'\) is not null then\n    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CAPTURE_REUSED'/);
 assert.match(capture,/on commit drop/);assert.match(capture,/revoke all on table pg_temp\./);
 for(const value of ['pg_backend_pid()','txid_current()','relowner=v_owner','pg_my_temp_schema()','aclexplode','attacl','conkey=array[1]'])assert.ok(capture.includes(value),value);
 assert.doesNotMatch(capture,/current_setting|set_config|report_total|source_gross|pricingEvidence/);
});
test('insert guard forbids caller output and consumes exactly one matching context',()=>{
 assert.match(guard,/new\.prepared_writer_output_json is not null or new\.prepared_writer_output_hash is not null/);
 for(const key of ['backend_pid','transaction_id','owner_oid','request_hash','actor_user_id','source_cycle_id','final_revision_id',
  'published_timesheet_financial_id','service_snapshot_hash','server_calculation_fingerprint','rule_set_version_id'])assert.ok(guard.includes(key),key);
 assert.match(guard,/v_count<>1/);assert.match(guard,/delete from pg_temp\./);assert.match(guard,/drop table pg_temp\./);
 assert.ok(guard.indexOf('new.prepared_writer_output_hash:=')<guard.indexOf('delete from pg_temp.'));
});
test('any intervening native row/component rewrite refuses before receipt',()=>{
 assert.match(guard,/financial_codec_v1\(v_fin\) is distinct from v_context\.financial_output_json/);
 assert.match(guard,/to_jsonb\(v_fin\)[\s\S]*?is distinct from v_context\.financial_row_hash/);
 assert.match(guard,/CHANGED_BEFORE_RECEIPT' using errcode='40001'/);
 assert.ok(guard.indexOf('CHANGED_BEFORE_RECEIPT')<guard.indexOf('new.prepared_writer_output_json:='));
 assert.doesNotMatch(guard,/for update|for share|advisory/i);
});
test('same-id later rewrite cannot supply a historical prepared amount',()=>{
 assert.doesNotMatch(amount,/from public\.timesheets_financials|\.is_current|\.processing_status|financial_codec_v1\(/);
 assert.match(amount,/return v_fin->>'total_pay_ex_vat'/);
 assert.match(amount,/prepared_writer_output_json is null and p_receipt\.prepared_writer_output_hash is null then return null/);
 assert.doesNotMatch(amount,/coalesce\([^\n]*,\s*0/);
});
test('reader binds saved amount to V1 receipt, original writer marker, exact original scope',()=>{
 assert.match(amount,/weekly_source_report_projection_receipt_assert_v1\(p_receipt\)/);
 assert.match(amount,/prepared_writer_output_hash is distinct from private\.weekly_source_report_prepared_output_hash_v1/);
 for(const key of ['candidate_id','client_id','timesheet_version','request_hash','root_timesheet_id','rule_set_version_id'])assert.ok(amount.includes(key),key);
 assert.match(amount,/v_writer is distinct from v_expected_writer/);
 assert.match(AMOUNT_HUNK,/fs\.candidate_id::text/);assert.match(AMOUNT_HUNK,/task\.root_timesheet_version::text/);
});
test('output attachment uses independent domain bound to unchanged V1 receipt',()=>{
 assert.match(read('02_codec.sqlinc'),/'WEEKLY_SOURCE_REPORT_PREPARED_WRITER_OUTPUT_V1'[\s\S]*?'native_receipt_hash'/);
 assert.doesNotMatch(read('inputs/receipt_insert.sqlinc'),/prepared_writer_output/);
 assert.match(read('01_schema.sqlinc'),/is not distinct from 'WEEKLY_SOURCE_REPORT_PREPARED_WRITER_OUTPUT_V1'/);
});
test('ordinary, NULL, NHSP, no-op, proposed and replay body bytes are retained',()=>{
 assert.equal(once(apply,CAPTURE_HUNK,''),read('inputs/ordinary_apply.sqlinc'));
 const before=apply.split(CAPTURE_HUNK)[0];
 assert.ok(before.includes("'TARGET_MANAGED_SUPPRESSED'"));assert.ok(before.includes("'NO_OP_FIRST_NEGATIVE'"));
 assert.equal(before.match(/'idempotent_replay',true/g).length,2);
 const supports=read('02_codec.sqlinc').split('financial_codec_v1')[0];assert.match(supports,/source_family='ROSTER'/);
 assert.match(supports,/authority_scope_kind is distinct from 'CYCLE'/);
});
test('historical immutability continues to cover all sidecar columns',()=>{
 assert.match(read('09_complete_final_acl_union.sqlinc'),/\('weekly_source_ordinary_pay_projection_receipts', 'IMMUTABLE_APPEND_ONLY'\)/);
 assert.match(read('09_complete_final_acl_union.sqlinc'),/v_old_facts:=pg_catalog\.to_jsonb\(old\)/);
 assert.doesNotMatch(read('09_complete_final_acl_union.sqlinc'),/\('weekly_source_ordinary_pay_projection_receipts','prepared_/);
 assert.doesNotMatch(read('01_schema.sqlinc'),/update public|add column[^\n]*(?:default|not null)|create table/i);
});
test('all seven new signatures are owner-only and no service route is added',()=>{
 const defs=primary.flatMap(([,s])=>scanManagedSql(s).statements.map(x=>scanManagedSql(s).source.slice(x.start,x.end))).filter(s=>s.startsWith('create or replace function'));
 assert.equal(defs.length,7);assert.equal(signatures.length,7);
 for(const s of signatures){assert.match(read('11_final_private_acl.sqlinc'),new RegExp(s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));}
 assert.equal(receipt.census.tables,105);assert.equal(receipt.census.service,142);assert.equal(receipt.census.privateAfter,239);
});
test('both complete ACL owners reverse to all later native 232 registrations',()=>{
 for(const [p,key] of [['08_complete_registry_union.sqlinc','registry'],['09_complete_final_acl_union.sqlinc','acl']]){
  const original=fs.readFileSync(path.join(root,inputs[key]),'utf8').replaceAll('\r\n','\n');
  assert.equal(sha(dropAddedRegistry(read(p))),sha(original));
 }
});
test('independent ACL verifier preserves the old expected set and diagnostics',()=>{
 let r=read('10_complete_independent_acl_verifier.sqlinc');
 for(const s of signatures)r=once(r,`    '${s}',\n`,'');
 r=r.replace(/\b239\b/g,()=> '232');
 assert.equal(sha(r),sha(fs.readFileSync(path.join(root,inputs.aclVerifier),'utf8').replaceAll('\r\n','\n')));
});
test('dedicated verifier is data-free and invokes no business or transport owner',()=>{
 const s=read('13_data_free_verifier.sqlinc');
 assert.doesNotMatch(s,/insert into|update public|delete from|writer_output_capture|projection_apply_atomic|outbox|payment|provider|settlement|email/i);
 assert.match(s,/supported_v1\(null\)/);assert.match(s,/pay_amount_v1\(null\)/);
 assert.match(s,/rollback;/);assert.doesNotMatch(s,/\\set weekly_source_verification_outer_transaction/);
});
test('closure orders codec, owner context, guard, read and complete successors before final ACL',()=>{
 const rootSQL=read('12_proposed_repeatable_root.sqlinc');
 const include=[...rootSQL.matchAll(/\\ir (.+)/g)].map(m=>m[1]);
 assert.equal(include.length,9);assert.ok(include.every(p=>p.endsWith('.sqlinc')));
 assert.ok(include[0].endsWith('02_codec.sqlinc'));assert.ok(include[2].endsWith('04_receipt_insert_guard.sqlinc'));
 assert.ok(include.at(-1).endsWith('11_final_private_acl.sqlinc'));
 assert.ok(rootSQL.indexOf("notify pgrst")>rootSQL.indexOf(include.at(-1)));
});
test('active, withheld and NEXT authorities stay byte-preserved and unknown is not zero',()=>{
 const r=read('07_complete_report_pay_authority.sqlinc');
 assert.match(r,/'withheld_pay_ex_vat',null,'withheld_amount_known',false/);
 assert.match(r,/'active_amount_scope','ROOT_FAMILY'/);
 assert.equal(r.match(/private\.bpay_next_source_origin_state_v1\(/g).length,1);
 assert.equal(r.match(/private\.weekly_source_effective_inventory_v1\(/g).length,read('inputs/report_authority.sqlinc').match(/private\.weekly_source_effective_inventory_v1\(/g).length);
 assert.match(r,/'prepared_amount_known',prepared_amount is not null/);
 assert.match(r,/'prepared_amount_authority',case when prepared_amount is not null/);
});
test('native input source pins are exact and Banking source paths are absent',()=>{
 for(const pin of receipt.sourceInputs){assert.doesNotMatch(pin.path,/banking_pay|\/banking-pay\/|newtablesbanking|\/bpay-next\//i);
  assert.equal(sha(fs.readFileSync(path.join(root,pin.path))),pin.rawSha256,pin.path);}
});
test('root alias/closed-error pin-forward has a full reverse proof',()=>{
 assert.equal(sha(reverseVerifierForward(fs.readFileSync(path.join(root,inputs.automaticVerifier),'utf8').replaceAll('\r\n','\n'))),receipt.rootPinForward.originalVerifierCanonicalSha256);
 assert.equal(receipt.rootPinForward.originalVerifierCanonicalSha256,receipt.rootPinForward.reverseVerifierCanonicalSha256);
});
test('all declaration-only checks are explicitly not native acceptance',()=>{
 assert.equal(receipt.nativeCompilation,false);assert.equal(receipt.nativeFinancialProof,false);
 assert.equal(receipt.databaseCalls,false);assert.equal(receipt.rootWrites,false);assert.equal(receipt.bankingSourceReads,false);
 assert.ok(receipt.remainingGates.length>=4);
});
