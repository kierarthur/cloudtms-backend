import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import test from'node:test';
import{createHash}from'node:crypto';import{fileURLToPath}from'node:url';
import{functionEvents}from'../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const proof=JSON.parse(read('COMPLETE_NATIVE_REVERSES.json'));
test('six full current native owners reverse through fourteen unique hunks',()=>{
 assert.equal(proof.proofs.length,6);assert.equal(proof.proofs.reduce((n,p)=>n+p.hunks.length,0),14);
 for(const p of proof.proofs){let a=read('forward/'+p.output),b=read('before/'+p.output);assert.equal(sha(a),p.after_sha256);assert.equal(sha(b),p.before_sha256);
  for(const h of[...p.hunks].reverse()){assert.equal(a.split(h.after).length,2,h.name);a=a.replace(h.after,()=>h.before);}assert.equal(a,b);}
});
test('actual current-contract T0 has Contract/booking/physical ID and no Client or Candidate column',()=>{
 const schema=JSON.parse(read('input/CURRENT_TIMESHEETS_CONTRACT_SCHEMA.json'));const cols=schema.relation.columns.map(c=>c.name);
 for(const col of['timesheet_id','booking_id','contract_id','is_current','version'])assert.ok(cols.includes(col));
 assert.ok(!cols.includes('client_id'));assert.ok(!cols.includes('candidate_id'));assert.equal(schema.full_source_sha256,proof.current_contract_sha256);
 const ddl=JSON.parse(read('input/RAW_TIMESHEETS_DDL.json'));assert.equal(ddl.physical.relation,'public.timesheets');assert.ok(ddl.physical.ddl.length>0);
});
test('real physical aliases are fixed while genuine report/DTO Client remains byte-exact',()=>{
 for(const p of proof.proofs){const s=read('forward/'+p.output);if(p.output!=='04_complete_native_parent_scope.sqlinc')assert.ok(!/\b(?:sheet|s)\.client_id\b/.test(s));
  assert.ok(!/\broot\.client_id\b/.test(s));}
 const parent=read('forward/04_complete_native_parent_scope.sqlinc');assert.ok(parent.includes("'client_id',s.client_id,'report_scope_id',s.id"));
 assert.ok(parent.includes('from public.weekly_source_report_scopes s where s.id=selector'));
 assert.ok(parent.includes("'client_id',r.client_id,'actor_user_id',r.actor_user_id"));
});
// These fixtures evaluate the exact new data grain/predicate only. They are
// in-memory source counterexamples, not PostgreSQL/financial/native fixtures.
function identity(physical,contract){
 const source=read('forward/02_complete_original_census_assert.sqlinc');
 assert.ok(source.includes("'candidate_id',contract.candidate_id,'client_id',contract.client_id) into actual"));
 assert.ok(source.includes('contract.candidate_id is not null and contract.client_id is not null'));
 if(!physical||!contract||physical.contract_id!==contract.id||!contract.candidate_id||!contract.client_id)return null;
 return{timesheet_id:physical.timesheet_id,booking_id:physical.booking_id,contract_id:contract.id,candidate_id:contract.candidate_id,client_id:contract.client_id};
}
const physical={timesheet_id:'T0',booking_id:'booking',contract_id:'K1'},contract={id:'K1',candidate_id:'C1',client_id:'Client1'};
test('real physical row without Client fields derives the exact five-key native Contract identity',()=>{
 const value=identity(physical,contract);assert.deepEqual(value,{timesheet_id:'T0',booking_id:'booking',contract_id:'K1',candidate_id:'C1',client_id:'Client1'});
 assert.equal(Object.keys(value).length,5);assert.ok(!Object.hasOwn(physical,'client_id'));assert.ok(!Object.hasOwn(physical,'candidate_id'));
});
test('NULL/missing physical Contract or unbound Candidate/Client remains unavailable, never upload fallback',()=>{
 for(const[root,k]of[[{...physical,contract_id:null},contract],[physical,null],[physical,{...contract,client_id:null}],
  [physical,{...contract,candidate_id:null}],[null,contract]])assert.equal(identity(root,k),null);
 for(const p of proof.proofs){const after=read('forward/'+p.output),before=read('before/'+p.output);
  assert.equal(after.split("file_metadata_json->>'client_id'").length,before.split("file_metadata_json->>'client_id'").length);}
});
test('captured physical booking/Contract/Candidate/Client drift refuses original identity equality',()=>{
 const captured=identity(physical,contract);
 for(const[current,k]of[[{...physical,booking_id:'different'},contract],[physical,{...contract,client_id:'Foreign'}],
  [physical,{...contract,candidate_id:'Foreign'}],[{...physical,contract_id:'K2'},{...contract,id:'K2'}]])assert.notDeepEqual(identity(current,k),captured);
 for(const f of['02_complete_original_census_assert.sqlinc','03_complete_off_scope_assert.sqlinc','06_complete_bulk_original_root_assert.sqlinc']){
  assert.match(read('forward/'+f),/actual is distinct from (?:original|item)/);
 }
});
test('original union cannot silently drop a missing or unbound pay/evidence root at OPEN',()=>{
 const s=read('forward/01_complete_original_census.sqlinc'),start=s.indexOf('  if exists(\n    select 1 from (');assert.ok(start>=0);
 const guard=s.slice(start,s.indexOf('  with ids as (',start));for(const expected of["item->>'evidence_timesheet_id'","facts->'root_identities'",
  'left join public.timesheets physical','left join public.contracts native_contract','physical.timesheet_id is null','native_contract.id is null',
  'native_contract.candidate_id is null','native_contract.client_id is null','native_contract.client_id=any(clients)',
  'WEEKLY_SOURCE_QUALIFICATION_ORIGINAL_ROOT_SCOPE_INVALID'])assert.ok(guard.includes(expected),expected);
});
test('automatic/close and bulk checks still use captured-once entire original census and Source CAS',()=>{
 for(const f of['02_complete_original_census_assert.sqlinc','03_complete_off_scope_assert.sqlinc','06_complete_bulk_original_root_assert.sqlinc']){
  const a=read('forward/'+f),b=read('before/'+f);assert.equal(a.split('original_root_identities').length,b.split('original_root_identities').length);
  assert.ok(a.includes('original_census'));assert.ok(a.includes('weekly_source_bulk_source_cas_v1'));
 }
});
test('native parent keeps genuine membership, sibling/actor/whole fixture and ordinary discovery boundaries',()=>{
 const a=read('forward/04_complete_native_parent_scope.sqlinc'),b=read('before/04_complete_native_parent_scope.sqlinc');
 for(const text of['root public.timesheets%rowtype','contract public.contracts%rowtype','select k.* into strict contract from public.contracts k where k.id=root.contract_id',
  "original_root->>'timesheet_id'=root.timesheet_id::text",'observed_cycle_ids','if qualified then raise; end if;',
  "p_auth#>'{fixture_manifest,client_ids}'","p_auth#>'{fixture_manifest,candidate_ids}'","p_auth#>'{fixture_manifest,contract_ids}'"]){assert.ok(a.includes(text),text);assert.equal(a.split(text).length,b.split(text).length);}
 assert.ok(a.includes('if qualified and (contract.client_id is null or contract.candidate_id is null) then'));
});
test('native step exact root tuple refers to explicit local variables, preserving every finite native branch',()=>{
 const a=read('forward/05_complete_native_qualification_step.sqlinc'),b=read('before/05_complete_native_qualification_step.sqlinc');
 assert.ok(a.includes('<<qualification_step_identity>>'));
 for(const field of['contract_id','candidate_id','client_id'])assert.ok(a.includes('qualification_step_identity.'+field));
 const from=a.indexOf('  -- No arbitrary routine text'),old=b.indexOf('  -- No arbitrary routine text');assert.equal(a.slice(from),b.slice(old));
 assert.ok(a.includes('WEEKLY_SOURCE_QUALIFICATION_NATIVE_ROOT_SCOPE_INVALID'));
});
test('Source39 bulk facts successor is not duplicated or overwritten in this packet',()=>{
 assert.ok(!proof.proofs.some(p=>p.name==='private.weekly_source_bulk_source_facts_v1'));
 assert.equal(proof.Report_Source20_bulk_facts_reader_is_separate,true);assert.equal(proof.Source39_keys_count_actor_physical_Contract_Candidate_guards_preserved,true);
});
test('all copied raw native input lengths/hashes match exact controlling pins',()=>{
 for(const p of proof.raw_input_pins){const b=fs.readFileSync(path.join(dir,p.saved));assert.equal(b.length,p.bytes);assert.equal(sha(b),p.sha256);}
});
test('existing private ACL identities exactly match all six actual native declarations including uuid array',()=>{
 const acl=read('forward/07_existing_owner_only_acl.sqlinc');
 for(const p of proof.proofs){const parsed=functionEvents(read('forward/'+p.output));assert.equal(parsed.events.length,1);
  const id=parsed.events[0].id;assert.ok(acl.includes('alter function '+id+' owner to postgres;'),id);assert.ok(acl.includes(id),id);}
 assert.ok(acl.includes('from public,anon,authenticated,service_role;'));assert.ok(!/grant\s/i.test(acl.replace(/^--.*$/gm,'')));
});
