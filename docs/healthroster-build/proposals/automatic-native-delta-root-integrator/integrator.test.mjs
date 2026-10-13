import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { here, hash, prepare, validatePrepared, applyPrepared, TARGETS, SUPPORT, REPEATABLE, VERIFIER, RELEASE,
 ACL_VERIFIER, OBSERVED_HEAD, REBASE, REBASE_ANCHORS, projectionRebase, appendVerifier, reverseLocal, bankingSource, inside, definition } from './integrator.mjs';
const lane=path.resolve(here,'../../../..');
const native=path.resolve(lane,'../healthroster-v2-backend-13102026');
const plan=await prepare(native);
const receipt=JSON.parse(fs.readFileSync(path.join(here,'RECEIPT_PROPOSAL.json')));
const generated=path.join(here,'generated');
const official=plan.official;
const canonical=x=>String(x).replaceAll('\r\n','\n');
function parsedFunctions(text) {
 const p=official.scanManagedSql(canonical(text));return p.statements.map(s=>p.source.slice(s.start,s.end))
  .filter(s=>s.startsWith('create or replace function '));
}
test('exact current root dry-run retains fixed native inputs and all runtime holds',()=>{
 assert.equal(plan.metadata.observedHead,OBSERVED_HEAD);assert.equal(plan.status,'GUARDED_AUTOMATIC_SOURCE_PLAN_ONLY');
 assert.deepEqual(plan.counts,{tables:105,private:232,service:142});
 assert.deepEqual(plan.runtime,{compiled:false,qualified:false,exposed:false,capabilities:'ABSENT_AT_SOURCE_BASE_AND_FAIL_CLOSED',
  prerequisiteV2:'OPERATIONAL_ACTIVATION_REJECTED_NOT_IMPORTED'});
 assert.ok(plan.inputFacts.length>40);assert.ok(plan.inputFacts.every(f=>!bankingSource(f.path)));
 assert.equal(receipt.rootWritesPerformed,false);assert.equal(receipt.bankingSourceRead,false);
});
test('all16 exact generated outputs match the full guarded receipt',()=>{
 assert.equal(plan.outputFacts.length,16);assert.equal(plan.files.size,16);
 assert.deepEqual(new Set(plan.outputFacts.map(f=>f.target)),new Set(TARGETS));
 for(const f of receipt.outputs){assert.equal(hash(fs.readFileSync(path.join(here,f.artifact))),f.sha256,f.target);
  assert.equal(hash(plan.files.get(f.target)),f.sha256,f.target);}
});
test('prewrite validator has no writes and refuses forbidden targets or altered source bytes',()=>{
 assert.equal(validatePrepared(plan).status,'SOURCE_PREWRITE_GUARDS_MATCH_NO_WRITES');
 const bad={...plan,outputFacts:plan.outputFacts.map((f,i)=>i===0?{...f,target:'broker/src/index.js'}:f)};
 assert.throws(()=>validatePrepared(bad),/CLOSED_SOURCE_WRITE_SET_CHANGED/);
 const files=new Map(plan.files);files.set(REPEATABLE,Buffer.from('select true;'));
 assert.throws(()=>validatePrepared({...plan,files}),/CLOSED_SOURCE_WRITE_SET_CHANGED/);
 assert.throws(()=>applyPrepared(plan),/EXPLICIT_SOURCE_APPLY_FLAG_REQUIRED/);
});
test('root input CAS rejects a changed hash before a permanent write can begin',()=>{
 const inputFacts=plan.inputFacts.map((f,i)=>i===0?{...f,sha256:'0'.repeat(64)}:f);
 assert.throws(()=>validatePrepared({...plan,inputFacts}),/ROOT_INPUT_HASH_CHANGED/);
 const outputFacts=plan.outputFacts.map((f,i)=>i===0?{...f,beforeSha256:'0'.repeat(64)}:f);
 assert.throws(()=>validatePrepared({...plan,outputFacts}),/TARGET_CHANGED_AFTER_PLAN/);
});
test('fresh root head is mandatory and unsupported root native drift stays refused',async()=>{
 await assert.rejects(()=>prepare(native,{expectedHead:'0'.repeat(40)}),/EXACT_ROOT_HEAD_OR_BRANCH_REQUIRED/);
 const source=fs.readFileSync(path.join(here,'integrator.mjs'),'utf8');
 assert.ok(source.includes("nativeChanges.some(p=>!Object.hasOwn(REBASE,p))"));
 assert.ok(source.includes("if(untracked)fail('UNREVIEWED_NATIVE_SOURCE_PRESENT')"));
});
test('path guards reject absolute, parent traversal and Banking source paths',()=>{
 for(const p of ['../supabase/x','C:/tmp/x','/tmp/x'])assert.throws(()=>inside(native,p),/INTEGRATOR_PATH_ESCAPE/);
 assert.equal(bankingSource('broker/src/banking-pay/native.mjs'),true);
 assert.equal(bankingSource('supabase/repeatable/04102026_banking_pay_native.sql'),true);
 assert.ok(plan.metadata.unreadBankingInputPaths.length>0);
 assert.ok(!TARGETS.some(bankingSource));
});
test('both whole repaired projection pages reverse exactly through only seven suffix removals',()=>{
 assert.equal(plan.projectionRebases.length,2);
 for(const [p,e] of Object.entries(REBASE)){
  const bytes=fs.readFileSync(path.join(native,p)),proof=projectionRebase(p,bytes);
  assert.equal(proof.reverseSha256,e.before);assert.equal(proof.suffixInsertions,7);
  assert.equal(proof.nativeRegexPredicates,9);assert.equal(proof.unrelatedBytesRetained,true);
  assert.throws(()=>projectionRebase(p,Buffer.concat([bytes,Buffer.from('\n')])),/PROJECTION_REBASE_INPUT_CHANGED/);
  assert.equal(proof.canonicalFileSha256,hash(canonical(bytes)));
 }
 assert.equal(REBASE_ANCHORS.length,7);
});
test('release registrations append only the dedicated verifier in both arrays with exact byte reversal',()=>{
 const before=fs.readFileSync(path.join(native,RELEASE),'utf8'),after=plan.files.get(RELEASE).toString('utf8');
 const a=JSON.parse(before),b=JSON.parse(after);
 assert.deepEqual(b,{...a,verificationFiles:[...a.verificationFiles,VERIFIER],newVerificationFiles:[...a.newVerificationFiles,VERIFIER]});
 let restored=after;
 for(const key of ['newVerificationFiles','verificationFiles']){
  const start=restored.indexOf(JSON.stringify(key)),next=restored.indexOf(']',start);
  const part=restored.slice(start,next),addition=',\n    '+JSON.stringify(VERIFIER);
  assert.equal(part.split(addition).length,2);restored=restored.slice(0,start)+part.replace(addition,'')+restored.slice(next);
 }
 assert.equal(restored,before);
 for(const eol of ['\n','\r\n']){
  const src=`{${eol}  "verificationFiles": [${eol}    "one"${eol}  ],${eol}  "newVerificationFiles": [${eol}    "two"${eol}  ]${eol}}${eol}`;
  const x=appendVerifier(appendVerifier(src,'verificationFiles','new'),'newVerificationFiles','new');
  assert.deepEqual(JSON.parse(x),{verificationFiles:['one','new'],newVerificationFiles:['two','new']});
  assert.throws(()=>appendVerifier(x,'verificationFiles','new'),/RELEASE_ARRAY_OR_REPLAY_CHANGED/);
 }
});
test('one official closure has all12 ordered sqlinc leaves and no repeated Stage20/27 owner',()=>{
 const closure=official.readManagedClosure(REPEATABLE+'inc',generated);
 assert.equal(closure.ordered.length,13);assert.doesNotThrow(()=>official.exactAtomicBody(closure.expanded));
 const leaves=closure.ordered.slice(1).map(f=>path.basename(f.path));
 assert.equal(leaves[7],'08_complete_automatic_reconcile_delta_forward.sqlinc');
 assert.equal(leaves[8],'09_complete_local_successor.sqlinc');
 assert.equal(leaves.at(-1),'15_final_automatic_private_acl.sqlinc');
 assert.ok(closure.ordered.slice(1).every(f=>f.path.endsWith('.sqlinc')));
 const funcs=parsedFunctions(closure.expanded);
 for(const name of ['public.weekly_exceptional_pay_stage_c1_request_v1','public.weekly_source_automatic_reconcile_atomic_v1',
  'private.weekly_source_automatic_reconcile_certificate_v1'])assert.ok(!funcs.some(f=>f.startsWith('create or replace function '+name+'(')));
});
test('full ordinary Local and receipt reverse proofs preserve complete native bodies',()=>{
 assert.equal(plan.proofs.ordinaryReceiptReverse.length,3);assert.equal(plan.proofs.ACLCompleteReverse.length,2);
 const p=canonical(plan.files.get(SUPPORT+'09_complete_local_successor.sqlinc'));
 const name='public.weekly_exceptional_pay_complete_local_v1';
 const original=definition(fs.readFileSync(path.join(native,'supabase/repeatable/13102026_1113_weekly_source_protected_pay_authority_v1.sql')),name,official.scanManagedSql);
 const next=definition(p,name,official.scanManagedSql);
 assert.equal(reverseLocal(next,original),hash(original));
 assert.throws(()=>reverseLocal(next.replace('v_automatic_components jsonb','v_changed_components jsonb'),original),/UNIQUE_REVERSE_ANCHOR_CHANGED/);
});
test('independent ACL union is232 private,105 tables and142 service without a dropped original signature',()=>{
 const sql=canonical(plan.files.get(SUPPORT+'12_complete_private_registry_union.sqlinc'));
 const contract=definition(sql,'private._weekly_source_acl_private_helper_contract_v1',official.scanManagedSql);
 const sigs=[...contract.matchAll(/'((?:private|public)\.[^']+\([^']*\))'/g)].map(m=>m[1]);
 assert.equal(sigs.length,232);assert.equal(new Set(sigs).size,232);
 assert.ok(plan.privateSignatures.every(s=>sigs.includes(s)));
 const independent=canonical(plan.files.get(ACL_VERIFIER));
 assert.ok(plan.privateSignatures.every(s=>independent.includes("    '"+s+"',")));
 const privateACL=canonical(plan.files.get(SUPPORT+'15_final_automatic_private_acl.sqlinc'));
 assert.ok(plan.privateSignatures.every(s=>privateACL.includes(`revoke all on function ${s} from public,anon,authenticated,service_role;`)));
});
test('dedicated NEW/UPGRADE verifier includes exactly11 private hashes and four retained bodies',()=>{
 const sql=canonical(plan.files.get(VERIFIER));
 const declarations=sql.slice(sql.indexOf('for r in select'),sql.indexOf('-- Full ordinary'));
 assert.equal([...declarations.matchAll(/\('[^']+','[0-9a-f]{64}'\)/g)].length,11);
 const retained=sql.slice(sql.indexOf('-- Full ordinary'),sql.indexOf('$automatic_early_refusals$'));
 assert.equal([...retained.matchAll(/\('[^']+','[0-9a-f]{64}'\)/g)].length,4);
 assert.ok(sql.includes("p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']"));
 assert.ok(sql.includes("p.prorettype<>'jsonb'::regtype::oid"));
 assert.ok(sql.includes("p.prosrc,'UTF8'"));
 assert.ok(!retained.includes('_weekly_source_acl_'));
});
test('dedicated negative calls stop at null/missing selectors or proven absent dependency',()=>{
 const sql=canonical(plan.files.get(VERIFIER));
 assert.ok(sql.includes('where id=missing_id) then'));
 assert.ok(sql.indexOf('where id=missing_id) then')<sql.indexOf('staged_publication_context_v1(missing_id,missing_id)'));
 assert.ok(sql.includes("exception when no_data_found then refused:=true"));
 assert.ok(sql.includes("if pg_catalog.to_regprocedure('private.weekly_source_automatic_reconcile_producer_ready_v1(uuid)') is null then"));
 assert.ok(sql.includes("WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE"));
 assert.ok(!/\b(?:insert\s+into|update\s+public|delete\s+from|for\s+update|set_config|call\s+public)\b/i.test(sql));
 assert.ok(!sql.includes('weekly_source_automatic_reconcile_atomic_v1('));
 assert.ok(!sql.includes('weekly_source_finalise_engine_v1('));
 const parsed=official.scanManagedSql(sql);
 assert.deepEqual(parsed.statements.map(s=>s.tokens[0]),['BEGIN','DO','DO','ROLLBACK']);
});
test('no current Report/FIN/CN/A0/Bank owner is in the closed integration write set',()=>{
 for(const p of TARGETS)assert.ok(!/report|invoice_window|magnit|correction|tsfin|banking|1700/.test(p));
 assert.ok(plan.stageFacts.every(f=>!TARGETS.includes(f.path)));
 const src=fs.readFileSync(path.join(here,'integrator.mjs'),'utf8');
 assert.ok(!/tools\.|exec_command|https?:\/\/|SUPABASE_SERVICE|process\.env|databaseUrl\(|psql\(/.test(src));
});
