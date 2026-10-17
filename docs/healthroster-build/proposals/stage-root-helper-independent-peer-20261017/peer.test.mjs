import fs from 'node:fs';
import test from 'node:test';
import assert from 'node:assert/strict';
const source=fs.readFileSync(new URL('./reference/stage-bookkeeper.mjs',import.meta.url),'utf8');
const first=source.indexOf("git(root,'commit','-m','Record genuine '");
const end=source.indexOf('\n}\nfunction small',first);
assert.ok(first>0&&end>first);
// Exact publication tail only, never the runner, filesystem or native readers.
const run=Function('phase','stage','files','git','root','report','source','must',source.slice(first,end));
const commit='a'.repeat(40),reportHead='b'.repeat(40);
function call(phase,fault){
 const calls=[];
 const git=(repository,...args)=>{
  calls.push([repository,...args]);
  if(args[0]==='commit'){assert.deepEqual(args.slice(-3),['--only','--','owned.json']);return Buffer.alloc(0);}
  if(args[0]==='rev-parse')return Buffer.from((repository==='report'?(fault==='reportHead'?'c'.repeat(40):reportHead):commit)+'\n');
  if(args[0]==='push'){if(fault==='push')throw Error('PUSH');return Buffer.alloc(0);}
  assert.equal(args[0],'ls-remote');return Buffer.from((fault==='remote'?'d'.repeat(40):commit)+'\trefs/heads/codex/healthroster-install2-14102026\n');
 };
 const must=(v,c)=>{if(!v)throw Error(c);};
 return {result:run(phase,'S1',['owned.json'],git,'root','report',reportHead,must),calls};
}
for(const phase of ['BEFORE','SETUP','AFTER','PUBLISH'])test(`exact ${phase} publication order and final Report binding`,()=>{
 const {result,calls}=call(phase);
 assert.equal(result.status,`ROOT_GENUINE_S1_${phase}_COMMITTED`);assert.equal(result.commit,commit);
 assert.deepEqual(calls.map(x=>x[1]),phase==='BEFORE'?['commit','rev-parse','rev-parse']:['commit','rev-parse','push','ls-remote','rev-parse']);
 assert.equal(calls.at(-1)[0],'report');
});
for(const phase of ['BEFORE','SETUP','AFTER','PUBLISH'])test(`final Report HEAD drift refuses ${phase} return`,()=>{
 assert.throws(()=>call(phase,'reportHead'),/BOOKKEEPER_REPORT_HEAD_MOVED/);
});
test('SETUP push failure cannot return committed status',()=>assert.throws(()=>call('SETUP','push'),/PUSH/));
test('SETUP wrong remote cannot return committed status',()=>assert.throws(()=>call('SETUP','remote'),/BOOKKEEPER_REMOTE_REQUIRED/));
