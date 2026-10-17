import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),repo=path.resolve(here,'../../../..');
const root=path.resolve(repo,'../healthroster-install2-14102026');
const {scanManagedSql,canonical}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
const {sqlDateKey}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')).href);
const profilePath='docs/healthroster-build/proposals/source-read-delegation-acl-forward-20261017/ACL_SOURCE_TRANSFER.json';
const profile=JSON.parse(fs.readFileSync(path.join(here,'ACL_SOURCE_TRANSFER.json')));
const original=JSON.parse(fs.readFileSync(path.join(root,profile.historicalClassificationPath)));
const compact=JSON.parse(fs.readFileSync(path.join(here,'reference/ROOT_R1_ACL_COMPACT_DIFF.json')));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const current=[...original.aclEvents,...profile.addedEvents],checks=[],check=(name,fn)=>{fn();checks.push(name);};
const source=fs.readFileSync(path.join(here,'payload/scripts/healthroster-source-classifier-delegation-review.mjs'),'utf8');
const context=vm.createContext({fs,path,createHash:crypto.createHash,repoRoot:root,sqlDateKey,scanManagedSql,canonical,Buffer,
 routineDefinitions:()=>assert.fail('This ACL-only peer never inventories Banking bodies.')});
vm.runInContext(source.replace(/^import[^\n]+\n/gm,'').replace(/^export\s+/gm,'')+
 '\nthis.inverse=sourceClassifierAclPredecessor;',context,{timeout:3000});
const read=file=>fs.readFileSync(file===profilePath?path.join(here,'ACL_SOURCE_TRANSFER.json'):path.join(root,file));
const apply=(events=current,historical=original.aclEvents,mutations=new Map())=>context.inverse(events,historical,
 file=>mutations.has(file)?mutations.get(file):read(file));
check('root-mechanical77-minus75-is-exact-two-owner-only-events',()=>{
 assert.equal(sha(fs.readFileSync(path.join(here,'reference/ROOT_R1_ACL_COMPACT_DIFF.json'))),profile.rootReceipt.sha256);
 assert.equal(compact.definitionsExactlyUnchanged,true);assert.deepEqual(compact.additions,profile.addedEvents);
 assert.deepEqual(compact.removed,[]);assert.equal(compact.historicalAclEvents,75);assert.equal(compact.currentAclEvents,77);
});
check('all-complete-current-sources-equal-reviewed3f01-Git-payloads',()=>{
 for(const pin of profile.completeCurrentSourcePins){
  const bytes=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',
   '3f01b8f1782175c3f2be3b167b296a478e3a042a:docs/healthroster-build/proposals/source-complete-classification-current-successor-20261017/payload/'+pin.path],{cwd:repo,maxBuffer:2000000});
  assert.equal(sha(canonical(bytes)),pin.sha256);assert.equal(canonical(read(pin.path)),canonical(bytes));
 }
});
check('historical75-remains-byte-structure-identical',()=>assert.equal(apply(original.aclEvents),original.aclEvents));
check('current77-inverses-only-two-identical-original-source-statements',()=>assert.equal(apply(),original.aclEvents));
for(const [name,mutate]of[
 ['missing-old-event',v=>v.splice(4,1)],['changed-old-revoke',v=>{v[2]={...v[2],statement:v[2].statement+' '};}],
 ['missing-added-owner',v=>v.splice(75,1)],['missing-added-revoke',v=>v.splice(76,1)],
 ['extra-duplicate-owner',v=>v.push({...v[75]})],['extra-broad-grant',v=>v.push({root:profile.addedEvents[0].root,
 physical:profile.addedEvents[0].physical,statement:'grant execute on all functions in schema private to authenticated;',sha256:'0'.repeat(64)})],
 ['changed-role',v=>{v[76]={...v[76],statement:v[76].statement.replace('service_role','authenticated')};}],
 ['changed-source-path',v=>{v[75]={...v[75],physical:'supabase/repeatable/other.sql'};}],
 ['changed-root',v=>{v[75]={...v[75],root:'supabase/repeatable/other.sql'};}],
 ['changed-order',v=>{[v[75],v[76]]=[v[76],v[75]];}],
 ['moved-new-pair',v=>{v.unshift(...v.splice(75));}]
])check('current-event-drift-refuses:'+name,()=>{const value=structuredClone(current);mutate(value);assert.throws(()=>apply(value),/DELEGATION_/);});
for(const file of[profilePath,profile.historicalClassificationPath,profile.unchangedOriginalSource.path,
 ...profile.completeCurrentSourcePins.map(p=>p.path)]){
 check('complete-source-or-review-drift-refuses:'+file,()=>assert.throws(()=>apply(current,original.aclEvents,
  new Map([[file,Buffer.concat([read(file),Buffer.from('\n-- drift\n')])]])),/DELEGATION_/));
 check('missing-complete-source-refuses:'+file,()=>assert.throws(()=>apply(current,original.aclEvents,
  new Map([[file,Buffer.from('')]])),/DELEGATION_|JSON/));
}
check('historical-argument-cannot-be-refreshed',()=>assert.throws(()=>apply(current,current),/ACL_HISTORICAL_EVENTS_CHANGED/));
check('complete-ACL-extra-unrelated-statement-cannot-hide-behind-two-events',()=>{
 const file=profile.addedEvents[0].physical;assert.throws(()=>apply(current,original.aclEvents,
  new Map([[file,read(file)+"\ngrant execute on function private.unrelated(uuid) to service_role;\n"]])),/ACL_COMPLETE_SOURCE_CHANGED/);
});
check('complete-original-helper-parent-is-reconstructed-by-two-hunks',()=>{
 const edits=JSON.parse(fs.readFileSync(path.join(here,'HELPER_PARENT_EDITS.json')));
 let after=source;assert.equal(sha(after),edits.outputSha256);
 for(const h of [...edits.hunks].reverse()){assert.equal(after.split(h.after).length,2);after=after.replace(h.after,()=>h.before);}
 assert.equal(sha(after),edits.inputSha256);
 assert.equal(after,fs.readFileSync(path.join(here,'reference/healthroster-source-classifier-delegation-review.mjs'),'utf8'));
 assert.equal(edits.hunks.length,2);
});
fs.writeFileSync(path.join(here,'TEST_RESULTS.json'),JSON.stringify({passed:checks.length,failed:0,checks,
 completeSourcePins:profile.completeCurrentSourcePins,originalDefinitionEventsUnchanged:17,originalRowsUnchanged:13,
 historicalAclEvents:75,currentAclEvents:77,exactInverseEvents:2,
 actualFullSnapshot:'ROOT_OWNED_R1_COMPACT_DIFF_ONLY_NO_BANK_BODY_REPLAY',native:'NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({passed:checks.length,inverse:'77_TO_ORIGINAL75_ONLY',native:'NOT_RUN'}));
