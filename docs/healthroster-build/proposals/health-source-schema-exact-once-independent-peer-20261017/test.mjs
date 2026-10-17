import test from 'node:test';
import assert from 'node:assert/strict';
import {load,model,item,context,read} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/health-source-schema-exact-once-source-20261017/test.mjs';
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
test('independent: frozen source authority cannot acquire arbitrary tables or consumers',async()=>{
 const {helper}=await load(),m=helper.HEALTH_SOURCE_COVERAGE;
 assert(Object.isFrozen(m));assert(Object.isFrozen(m.pins));assert(Object.isFrozen(m.consumers));
 assert.throws(()=>m.consumers.push('unknown.sql'),e=>e.name==='TypeError');
 assert.throws(()=>m.pins[model.constructor].sha256='0'.repeat(64),e=>e.name==='TypeError');
 assert.equal(helper.healthSourceCoverageFor({path:model.constructor},'migration',{root}),null);
});
test('independent: actual helper rejects multi-file and duplicate-ordered closures',async()=>{
 const {api,helper}=await load(),u=api.compileManagedUnit(item(model.consumers[0]),'migration',{root});
 for(const closure of [
  {...u.closure,files:new Map([...u.closure.files,['unknown.sql','0'.repeat(64)]])},
  {...u.closure,ordered:[...u.closure.ordered,...u.closure.ordered]},
 ])assert.throws(()=>helper.healthSourceCoverageFor(closure,'migration',{root}),/COVERAGE_SCOPE/);
});
test('independent: closure source, hash and expanded-byte substitutions all refuse',async()=>{
 const {api,helper}=await load(),u=api.compileManagedUnit(item(model.consumers[0]),'migration',{root});
 for(const closure of [
  {...u.closure,closureHash:'0'.repeat(64)},
  {...u.closure,expanded:u.closure.expanded+'\n'},
  {...u.closure,ordered:[{...u.closure.ordered[0],source:u.closure.ordered[0].source+'\n'}]},
  {...u.closure,files:new Map([[u.item.path,'0'.repeat(64)]])},
 ])assert.throws(()=>helper.healthSourceCoverageFor(closure,'migration',{root}),/COVERAGE_CLOSURE/);
});
test('independent: deleting companion cannot resume duplicate original DDL',async()=>{
 const {api}=await load();
 for(const file of model.consumers){const u=api.compileManagedUnit(item(file),'migration',{root});
  for(const value of [null,undefined,false])assert.throws(()=>api.unitCheckpointSql({...u,healthSourceCoverage:value},context),/COVERAGE_UNIT/);
 }
});
test('independent: same invocation source mapping drift refuses at checkpoint',async()=>{
 const {api}=await load();let altered=false;
 const mapSource=s=>altered?s+'\nselect 1;':s;
 const u=api.compileManagedUnit(item(model.consumers[0]),'migration',{root,mapSource});
 altered=true;assert.throws(()=>api.unitCheckpointSql(u,context),/COVERAGE_CLOSURE/);
});
test('independent: missing release context and forged body/authority refuse',async()=>{
 const {api,helper}=await load(),u=api.compileManagedUnit(item(model.consumers[0]),'migration',{root});
 for(const c of [null,{}, {releaseId:''}])assert.throws(()=>helper.healthSourceCoverageSql(u,c),/COVERAGE_CONTEXT/);
 for(const change of [{body:u.body+'\n'}, {authoritySha256:'0'.repeat(64)}, {path:model.consumers[1]}])
  assert.throws(()=>helper.healthSourceCoverageSql({...u,healthSourceCoverage:{...u.healthSourceCoverage,...change}},context),/COVERAGE_UNIT/);
});
test('independent: exact constructor ledger precedes locking, coverage, receipt and normal ledger',async()=>{
 const {api}=await load();
 for(const file of model.consumers){
  const s=api.unitCheckpointSql(api.compileManagedUnit(item(file),'migration',{root}),context);
  const positions=['HEALTH_COVERAGE_CONSTRUCTOR_LEDGER_REQUIRED','lock table private.','HEALTH_COVERAGE_OWNER_REQUIRED','do $health_coverage_receipt$','insert into private.cloudtms_migration_ledger'].map(x=>s.indexOf(x));
  assert(positions.every(x=>x>=0));assert.deepEqual([...positions].sort((a,b)=>a-b),positions);
  assert.equal(s.match(/begin;/g).length,1);assert.equal(s.match(/commit;/g).length,1);
  assert.equal(s.match(/HEALTH_COVERAGE_RELEASE_REQUIRED/g).length,1);
  assert(!/exception\s+when|create\s+table\s+if\s+not\s+exists/i.test(s));
 }
});
test('independent: original compiler whole-image inverse preserves all unselected code',()=>{
 const f=JSON.parse(read('FORWARD.json')).targets[0];let s=read('payload/scripts/cloudtms-managed-release.mjs');
 for(const h of [...f.edits].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}
 assert.equal(s,read('parents/scripts/cloudtms-managed-release.mjs'));
});
