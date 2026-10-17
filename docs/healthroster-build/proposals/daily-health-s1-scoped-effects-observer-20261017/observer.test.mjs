import test from 'node:test';
import assert from 'node:assert/strict';
import {build,validateFrame,CLIENT,NAME,SCOPES,SEQUENCES,RELATIONS,scopedSql} from './observer.mjs';
const context={identity:{physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:456,key:[20261013,1],turnId:'hr_turn_'+'a'.repeat(32)},actorId:'f717d043-0000-4000-8000-000000000099',phase:'BEFORE'};
const clone=x=>structuredClone(x),now=Date.parse('2026-10-17T13:00:00Z');
function frame(mode='EFFECTS'){
 const {receipt}=build(mode,context),v={version:'S1_SCOPED_EFFECT_OBSERVER_V1',mode,phase:'BEFORE',turnBindingSha256:receipt.turnBindingSha256,database:'cloudtms_test_clone',databaseOid:123,backendPid:789,holderPid:456,holderGranted:true,readOnly:true,noRoleSwitch:true,observedAt:new Date(now).toISOString(),testIdentity:true,legacyOne:true,applyingCount:'0',actorAllowed:true,observation:mode==='EFFECTS'?{scope:Object.fromEntries(SCOPES.map(k=>[k,{count:'0',sha256:'b'.repeat(64)}])),sequenceState:Object.fromEntries(SEQUENCES.map(k=>[k,{lastValue:'1',isCalled:false}])),counterSeq:'0'}:{relations:RELATIONS.map(identity=>({identity})),sequences:SEQUENCES.map(identity=>({identity})),triggerRoutines:[]}};
 return{receipt,v};
}
test('fixed readonly inputs keep original 30s/2s, one-row bound and no executor',()=>{
 for(const mode of ['METADATA','EFFECTS'])for(const phase of ['BEFORE','AFTER','RECONCILE']){
  const{input,receipt}=build(mode,{...context,phase});assert.equal(input.database,'agency_test');assert.equal(input.statement_timeout_ms,30000);assert.equal(input.lock_timeout_ms,2000);assert.equal(input.max_rows_per_statement,1);assert.equal(input.continue_on_error,false);assert(input.sql.startsWith('SET LOCAL transaction_read_only=on;\n'));assert.equal(input.verification_sql,input.sql.slice(input.sql.indexOf('\n')+1));assert.equal(receipt.native,'NOT_RUN');assert.equal(receipt.restoration,'NOT_A_RESTORATION_PROGRAMME');assert(Buffer.byteLength(input.sql)<32768);assert(!input.sql.includes(context.actorId));assert(!input.sql.includes(context.identity.turnId));
 }
});
test('fixed seven scopes retain fixture namespace and genuine FK equality',()=>{
 assert.deepEqual(Object.keys(scopedSql),SCOPES);for(const s of ['client','settings','audit','contracts','plannedWeeks'])assert(scopedSql[s].includes(CLIENT));assert(scopedSql.client.includes(NAME.toLowerCase()));assert(scopedSql.plannedWeeks.includes('w.timesheet_id is null'));assert(scopedSql.counterTransaction.includes('c.scope_change_tx_token=t.tx_token'));assert(scopedSql.counterTransaction.includes("c.entity_key='clients'"));assert(!scopedSql.counterTransaction.includes(CLIENT));
 const sql=build('EFFECTS',context).input.sql;assert.equal((sql.match(/pg_catalog\.count\(\*\)::text/g)||[]).length,8);assert.equal((sql.match(/pg_catalog\.string_agg\(s\.row_hash,'\|' order by s\.row_hash\)/g)||[]).length,7);assert.equal((sql.match(/pg_catalog\.to_jsonb\(r\)::text/g)||[]).length,7);assert(!/jsonb_(?:agg|object_agg)/i.test(sql));assert(!/\bnextval\s*\(/i.test(sql));
});
test('metadata contains definition hashes and real effective privileges, no body output',()=>{
 const sql=build('METADATA',context).input.sql;assert(sql.includes('pg_catalog.pg_get_functiondef(p.oid)'));assert(sql.includes("'definitionSha256',pg_catalog.encode(pg_catalog.sha256"));assert(!sql.includes("'definition',"));assert(sql.includes('not t.tgisinternal'));assert(sql.includes('k.condeferred'));assert(sql.includes('pg_catalog.has_table_privilege'));assert(sql.includes('pg_catalog.has_sequence_privilege'));assert(sql.includes('pg_catalog.has_function_privilege'));assert(!/\bselect\s+[a-z]+\.\*\s+from\s+public\./i.test(sql));
});
test('arbitrary modes/phases/identity/UUID/extra identity input refuse before SQL',()=>{
 for(const mode of ['QUERY','effects','',null,{}])assert.throws(()=>build(mode,context),/S1_OBSERVER_/);
 const patches=[{phase:'POST'},{actorId:"x');select 1--"},{actorId:null},{identity:{...context.identity,physicalDatabase:'uofvkfi5'}},{identity:{...context.identity,key:[20261013,2]}},{identity:{...context.identity,turnId:'hr_turn_x'}},{identity:{...context.identity,backendPid:0}},{identity:{...context.identity,databaseOid:1.1}},{identity:{...context.identity,sql:'select 1'}}];for(const p of patches)assert.throws(()=>build('EFFECTS',{...context,...p}),/S1_OBSERVER_/);
 // Canonical UUID syntax does not restrict native version/variant bits.
 assert.doesNotThrow(()=>build('EFFECTS',{...context,actorId:'00000000-0000-f000-0000-000000000000'}));
});
test('synthetic shape vectors, never a native observation',()=>{
 const {receipt,v}=frame();assert.equal(validateFrame(v,receipt,{now}),v);const max=clone(v);max.observation.scope.audit.count='9223372036854775807';max.observation.sequenceState[SEQUENCES[0]].lastValue='9223372036854775807';assert.doesNotThrow(()=>validateFrame(max,receipt,{now}));
 for(const change of [x=>x.extra=true,x=>x.holderGranted=false,x=>x.readOnly=false,x=>x.noRoleSwitch=false,x=>x.actorAllowed=false,x=>x.applyingCount='1',x=>x.database='uofvkfi5',x=>x.backendPid=x.holderPid,x=>x.turnBindingSha256='0'.repeat(64),x=>x.observedAt='2020-01-01T00:00:00Z',x=>x.observation.scope.audit.count=0,x=>x.observation.scope.audit.count='9223372036854775808',x=>x.observation.scope.audit.count='01',x=>x.observation.scope.audit.sha256='not-a-hash',x=>delete x.observation.scope.settings,x=>x.observation.scope.extra={count:'0',sha256:'b'.repeat(64)},x=>x.observation.sequenceState[SEQUENCES[0]].lastValue=-1,x=>x.observation.sequenceState[SEQUENCES[0]].isCalled='false',x=>x.observation.counterSeq='-1',x=>x.observation.rawRows=[]]){const bad=clone(v);change(bad);assert.throws(()=>validateFrame(bad,receipt,{now}),/S1_OBSERVER_/);}
});
test('metadata shape alone remains evidence only, duplicate/missing relation refuses',()=>{
 const{receipt,v}=frame('METADATA');assert.doesNotThrow(()=>validateFrame(v,receipt,{now}));for(const change of [x=>x.observation.relations.pop(),x=>x.observation.relations[0]=x.observation.relations[1],x=>x.observation.sequences.pop(),x=>x.observation.extra=true]){const bad=clone(v);change(bad);assert.throws(()=>validateFrame(bad,receipt,{now}),/S1_OBSERVER_/);}
});
