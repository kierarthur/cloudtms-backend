import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {dispatchWeeklySourceRequest,WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT} from '../../../broker/src/weekly-source/routes.js';
const actualRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../..');
const dir=path.join(actualRoot,"docs/healthroster-build/install2-composition/reviewed-no-shifts-undo-14102026/native");
const pin=JSON.parse(fs.readFileSync(path.join(dir,'CURRENT_UNDO_FULL_REVERSE.json'),'utf8'));
const actor='81000000-0000-4000-8000-000000000001';
const ids={source_cycle_id:'82000000-0000-4000-8000-000000000001',source_group_id:'82000000-0000-4000-8000-000000000002',
  client_id:'82000000-0000-4000-8000-000000000003',completion_id:'82000000-0000-4000-8000-000000000004'};
const payload={...ids,expected_cycle_version:'0'};
const receipt=(override={})=>({ok:true,status:'NO_SHIFTS_UNDONE',idempotent:false,...ids,cycle_state:'OPEN',
  cycle_version:'1',undone_by_user_id:actor,undone_at_utc:'2026-10-14T12:00:00+00:00',...override});
const request=body=>new Request('https://effect-free.invalid/api/weekly-source/v1/commands',{
  method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
globalThis.fetch=()=>{throw new Error('UNIT_ONLY_NO_NETWORK_ALLOWED');};
async function run(body={action:'UNDO_NO_ONE_WORKED',payload},reply=receipt(),user={id:actor,role:'admin',is_active:true}){
  const calls=[];
  const response=await dispatchWeeklySourceRequest(request(body),{}, {},{
    requireUser:async(env,req,roles)=>{calls.push(['auth',roles]);return user;},
    rpc:async(...args)=>{calls.push(['rpc',...args]);if(reply instanceof Error)throw reply;return reply;},
  });
  return {response,body:await response.json(),calls};
}
test('Undo is one literal owner command and is absent from pay-only mutation allowance',()=>{
  assert.equal(WEEKLY_SOURCE_OFFICE_ROUTE_CONTRACT.directRpcCommands.UNDO_NO_ONE_WORKED,'weekly_source_no_shifts_undo_atomic_v1');
});
test('real dispatcher injects authenticated admin actor and canonical nonnegative CAS',async()=>{
  const result=await run();assert.equal(result.response.status,200);assert.equal(result.body.status,'NO_SHIFTS_UNDONE');
  assert.deepEqual(result.calls.filter(row=>row[0]==='rpc'),[['rpc','weekly_source_no_shifts_undo_atomic_v1',
    {p_request:{...payload,actor_user_id:actor}},{timeoutMs:45000}]]);
});
test('numeric zero input canonicalises to exact decimal CAS string',async()=>{
  const result=await run({action:'UNDO_NO_ONE_WORKED',payload:{...payload,expected_cycle_version:0}});
  assert.equal(result.response.status,200);assert.equal(result.calls.at(-1)[2].p_request.expected_cycle_version,'0');
});
for(const user of [{id:actor,role:'pay_authoriser',is_active:true,payment_authoriser:true},
  {id:actor,role:'staff',is_active:true,payment_golden_key:true},
  {id:actor,role:'admin',is_active:false}, {id:actor,role:'staff',is_active:true}]){
  test(`current ${user.role} active=${user.is_active} cannot mutate No-one history without admin`,async()=>{
    const result=await run(undefined,receipt(),user);assert([401,403].includes(result.response.status));
    assert.equal(result.calls.some(row=>row[0]==='rpc'),false);
  });
}
for(const patch of [{actor_user_id:actor},{invoice_id:ids.client_id},{prepared:true},{foo:'bar'}]){
  test(`closed Undo rejects caller field ${Object.keys(patch)[0]} before RPC`,async()=>{
    const result=await run({action:'UNDO_NO_ONE_WORKED',payload:{...payload,...patch}});
    assert.equal(result.response.status,400);assert.equal(result.calls.some(row=>row[0]==='rpc'),false);
  });
}
for(const version of [-1,0.1,9007199254740992,'00','1.0','-1','9223372036854775807','9999999999999999999',null]){
  test(`invalid or unrepresentable expected CAS ${String(version)} refuses before RPC`,async()=>{
    const result=await run({action:'UNDO_NO_ONE_WORKED',payload:{...payload,expected_cycle_version:version}});
    assert.equal(result.response.status,400);assert.equal(result.calls.some(row=>row[0]==='rpc'),false);
  });
}
test('missing completion is refused by exact closed transport',async()=>{
  const p={...payload};delete p.completion_id;const result=await run({action:'UNDO_NO_ONE_WORKED',payload:p});
  assert.equal(result.response.status,400);assert.equal(result.calls.some(row=>row[0]==='rpc'),false);
});
test('invalid completion is refused before any native call',async()=>{
  const result=await run({action:'UNDO_NO_ONE_WORKED',payload:{...payload,completion_id:'bad'}});
  assert.equal(result.response.status,400);assert.equal(result.calls.some(row=>row[0]==='rpc'),false);
});
for(const bad of [{},null,[],receipt({ok:false}),receipt({client_id:actor}),receipt({undone_by_user_id:ids.client_id}),
  receipt({cycle_version:'0'}),receipt({cycle_version:1}),receipt({cycle_state:'FINALISING'}),receipt({undone_at_utc:''}),
  {...receipt(),ready:true}]){
  test(`unproved/mismatching Undo receipt ${JSON.stringify(bad)} stays dependency refusal`,async()=>{
    const result=await run(undefined,bad);assert.equal(result.response.status,503);
    assert.equal(result.body.error_code||result.body.error,'WEEKLY_SOURCE_NO_SHIFTS_UNDO_RECEIPT_UNAVAILABLE');
  });
}
test('same actual prior Undo receipt with idempotent true is retained without pretending a fresh CAS bump',async()=>{
  const result=await run(undefined,receipt({idempotent:true,cycle_version:'1'}));
  assert.equal(result.response.status,200);assert.equal(result.body.idempotent,true);
});
test('native stale/role/isolation failure never becomes successful Undo',async()=>{
  const result=await run(undefined,Object.assign(new Error('WEEKLY_SOURCE_NO_SHIFTS_UNDO_CYCLE_STALE'),{code:'40001'}));
  assert.notEqual(result.response.status,200);assert.notEqual(result.body.status,'NO_SHIFTS_UNDONE');
});
test('complete current runtime forward and two unique reversals recover actual74a9',()=>{
  const file=pin.changed_owner.path;let after=fs.readFileSync(path.join(dir,'forward',file),'utf8');
  const before=fs.readFileSync(path.join(dir,'before',file),'utf8');
  for(const hunk of [...pin.changed_owner.hunks].reverse()){
    assert.equal(after.split(hunk.after).length,2);after=after.replace(hunk.after,()=>hunk.before);
  }
  assert.equal(after,before);assert.equal(crypto.createHash('sha256').update(before).digest('hex'),pin.changed_owner.before_sha256);
});
test('concrete native Undo writes existing completion and audit only with actual cycle +1',()=>{
  const source=fs.readFileSync(path.join(actualRoot,"supabase/repeatable/14102026_0953_healthroster_no_shifts_undo/01_no_shifts_undo_atomic.sqlinc"),'utf8');
  assert.match(source,/completion_kind is distinct from 'NO_SHIFTS_TO_IMPORT'/);
  assert.match(source,/state='SUPERSEDED',superseded_at_utc=v_now/);
  assert.match(source,/version=version\+1/);assert.match(source,/FINALISING/);
  assert.match(source,/undone_by_user_id/);assert.match(source,/insert into public.audit_events/);
  assert.match(source,/v_cycle.version::text/);assert.match(source,/transaction_isolation/);
  assert.doesNotMatch(source,/delete from|create table|set_config|set role|weekly_source_.*ledger/i);
});
test('canonical-contained actual parser sees four native owner statements and one reload without SQL execution',async()=>{
  const root=actualRoot;
  assert(root.endsWith('healthroster-install2-14102026'));
  const {scanManagedSql,exactAtomicBody}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
  const source=fs.readFileSync(path.join(actualRoot,"supabase/repeatable/14102026_0953_healthroster_no_shifts_undo/01_no_shifts_undo_atomic.sqlinc"),'utf8');
  const scan=scanManagedSql(source);assert.equal(scan.statements.length,4);
  assert.equal(scan.statements.some(row=>row.unterminated),false);assert.equal(exactAtomicBody(source).trim(),source.trim());
  const top=fs.readFileSync(path.join(actualRoot,"supabase/repeatable/14102026_0953_healthroster_no_shifts_undo_v1.sql"),'utf8');
  assert.match(top,/\\ir 14102026_0953_healthroster_no_shifts_undo\/01_no_shifts_undo_atomic.sqlinc/);
  assert.equal(scanManagedSql(top).statements.filter(row=>row.tokens[0]==='NOTIFY').length,1);
});
