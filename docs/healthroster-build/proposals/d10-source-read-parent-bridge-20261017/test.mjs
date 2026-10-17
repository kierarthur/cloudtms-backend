import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';
import {createHash} from 'node:crypto';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {createReviewedD10SourceReadClassifier,createReviewedD10ReaderClassifier} from './payload/scripts/healthroster-d10-reader-type-review.mjs';
import {createReviewedSourceReadClassifier} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/scripts/healthroster-source-read-type-review.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const base='docs/healthroster-build/proposals/d10-reader-current-source-20261017/';
const read=p=>fs.readFileSync(path.join(here,p));
const json=p=>JSON.parse(read(p));
const hash=b=>createHash('sha256').update(b).digest('hex');
const target='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql';
const forward=json('original-proof/FULL_PARENT_FORWARD.json').rows.find(r=>r.target===target);
const originalGate=read('parents/scripts/bpay-next-source-gates.mjs').toString();
const newGate=read('payload/scripts/bpay-next-source-gates.mjs').toString();

// Import complete actual gate initializers and callbacks, not a replacement
// allowance oracle. Only import locators and two test-only exports differ.
async function actualGate(source,variant){
 const instrumented=source.replace(/from '(\.\/[^']+)'/g,(_,p)=>{
  const absolute=p==='./healthroster-d10-reader-type-review.mjs'
   ?path.join(here,variant==='old'?'parents':'payload','scripts/healthroster-d10-reader-type-review.mjs')
   :path.join(root,'scripts',p.slice(2));
  return `from ${JSON.stringify(pathToFileURL(absolute).href)}`;
 })+'\nexport {reviewedD10ReaderHazards,reviewedSourceReadHazards};\n';
 return import('data:text/javascript;base64,'+Buffer.from(instrumented).toString('base64'));
}
const old=await actualGate(originalGate,'old');
const current=await actualGate(newGate,'new');
const census=current.specialConstructViolations;
const points=census(forward.after,target);
const frozen=json('FROZEN_BASELINE.json'),frozenBytes=read(frozen.source);
assert.equal(hash(frozenBytes),frozen.sha256);assert.equal(frozenBytes.length,frozen.bytes);assert.equal(frozen.commit,current.FROZEN_BASE);
const originalBaseline=new Map();
for(const point of census(frozenBytes.toString(),target))originalBaseline.set(point.key,(originalBaseline.get(point.key)??0)+1);
const options={hazardCensus:census,classifyPrior:(...args)=>{
 // The real complete original gate callback is exercised in integration below.
 // Unit cases deliberately refuse any attempt to manufacture a prior allowance.
 throw Error('UNIT_PRIOR_MUST_NOT_BE_REACHED');
},readFile:p=>p.startsWith(base)?read('original-proof/'+p.slice(base.length)):fs.readFileSync(path.join(root,p))};
function bridge(extra={}){return createReviewedD10SourceReadClassifier(root,{sourceReadFactory:createReviewedSourceReadClassifier,...options,...extra});}
function realBoth(source=forward.after,violations=census(source,target)){
 return current.reviewedD10ReaderHazards(source,target,
  current.reviewedSourceReadHazards(source,target,new Map(originalBaseline),violations));
}

test('original eager actual factory fails real D10 whole-file image; complete real chain successor passes',()=>{
 assert.throws(()=>old.reviewedSourceReadHazards(forward.after,target,new Map(originalBaseline),points),/HEALTH_SOURCE_READ_TYPE_CURRENT_SOURCE_CHANGED/);
 const result=realBoth();
 assert.equal(points.length,1);assert.equal(result.get(points[0].key),1);
 current.assertSpecialConstructs(forward.after,target,result);
});
test('original complete parent still passes the actual retained Source READ factory',()=>{
 const actual=census(forward.before,target);
 const before=old.reviewedSourceReadHazards(forward.before,target,new Map(originalBaseline),actual);
 const wrapped=current.reviewedSourceReadHazards(forward.before,target,new Map(originalBaseline),actual);
 assert.deepEqual([...wrapped],[...before]);assert.equal(wrapped.get(actual[0].key),1);
});
test('all real original D10 points still pass the outer current factory and complete tuple proof',()=>{
 const raw=json('original-proof/RAW_F23_CENSUS.json');let count=0;
 const provenance=json('original-proof/SOURCE_PROVENANCE.json');
 const oldRoot=path.join(here,'../d10-reader-current-source-20261017');
 const selection=JSON.parse(fs.readFileSync(path.join(oldRoot,'PAYLOAD_SELECTION.json')));
 for(const row of raw){
  const selected=selection.targets.find(x=>x.target===row.target);
  const source=fs.readFileSync(path.join(oldRoot,selected.source),'utf8');
  const map=row.target===target?realBoth(source):current.reviewedD10ReaderHazards(source,row.target,new Map());
  current.assertSpecialConstructs(source,row.target,map);count+=row.hazards.length;
 }
 assert.equal(count,13);assert.equal(provenance.owners.length,7);
});
const newNames=[
 'private.weekly_source_check_only_candidate_reminder_context_v1(uuid,jsonb,text)',
 'private.weekly_source_check_only_candidate_reminder_subject_v1(jsonb,text,uuid)',
 'private.weekly_source_check_only_issue_filter_keys_v1(text,jsonb)',
 'private.weekly_source_check_only_query_groups_v1(uuid,uuid,jsonb)'];
for(const signature of newNames)for(const kind of ['missing','duplicate','altered'])test(`${kind} literal refuses BOTH-factory composition: ${signature}`,()=>{
 const line="    '"+signature+"',\n";
 const source=forward.after.replace(line,()=>kind==='missing'?'':kind==='duplicate'?line+line:line.replace('_v1','_foreign_v1'));
 assert.throws(()=>realBoth(source),/HEALTH_D10_FINITE_TYPE_SOURCE_READ_CURRENT_SOURCE_CHANGED/);
});
test('same hazard census never permits arbitrary whole-body or comment drift',()=>{
 for(const source of [forward.after+'\n-- foreign\n',forward.after.replace('v_bad<>0','v_bad<>1'),forward.after.replace('rollback;','commit;')]){
  assert.notEqual(source,forward.after);assert.throws(()=>realBoth(source),/SOURCE_READ_CURRENT_SOURCE_CHANGED/);
 }
});
test('missing, duplicated, foreign caller census refuses before old allowance can transfer',()=>{
 for(const supplied of [[],[...points,...points],[{...points[0],key:'foreign'}]])
  assert.throws(()=>realBoth(forward.after,supplied),/SOURCE_READ_CENSUS_CHANGED/);
});
test('every actual complete bridge proof refuses raw-byte substitution',()=>{
 for(const name of ['FULL_PARENT_FORWARD.json','SOURCE_PROVENANCE.json','SOURCE_OWNER_REVERSES.json','RAW_F23_CENSUS.json']){
  const review=bridge({readFile:p=>p===base+name?Buffer.concat([read('original-proof/'+name),Buffer.from(' ')]):options.readFile(p)});
  assert.throws(()=>review(forward.after,target,new Map(),points),/PROOF_INPUT_CHANGED/);
 }
});
test('wrong-path dispatch preserves original factory and cannot acquire this one-path transfer',()=>{
 for(const file of [target+'.foreign','supabase/verification/17102026_0722_source_current_read_security.sql']){
  const baseline=new Map([['sentinel',3]]);
  if(file.endsWith('.foreign'))assert.equal(bridge()(forward.after,file,baseline,points),baseline);
  else assert.throws(()=>bridge()(forward.after,file,baseline,points),/HEALTH_SOURCE_READ_TYPE_CURRENT_SOURCE_CHANGED/);
 }
});
test('baseline is not mutated and unrelated prior allowances survive both real factories',()=>{
 const baseline=new Map([...originalBaseline,['sentinel',3]]),original=[...baseline],result=current.reviewedD10ReaderHazards(forward.after,target,
  current.reviewedSourceReadHazards(forward.after,target,baseline,points));
 assert.deepEqual([...baseline],original);assert.equal(result.get('sentinel'),3);assert.equal(result.get(points[0].key),1);
});
test('retained READ review and its real prior artifact remain required by actual factory',()=>{
 const reviewPath='docs/healthroster-build/proposals/source-current-read-peer-f23-20261017/CLASSIFICATION.json';
 const r=createReviewedD10SourceReadClassifier(root,{...options,sourceReadFactory:createReviewedSourceReadClassifier,
  readFile:p=>p===reviewPath?Buffer.from('{}'):options.readFile(p)});
 assert.throws(()=>r(forward.after,target,new Map(),points),/HEALTH_SOURCE_READ_TYPE_REVIEW_CHANGED/);
});
test('bridge requires actual factories; tuple proof is still necessary for unrelated D10 native tuples',()=>{
 assert.throws(()=>createReviewedD10SourceReadClassifier(root,{}),/SOURCE_READ_FACTORY_REQUIRED/);
 assert.throws(()=>createReviewedD10SourceReadClassifier(root,{...options,sourceReadFactory:()=>null}),/SOURCE_READ_CLASSIFIER_REQUIRED/);
 const raw=json('original-proof/RAW_F23_CENSUS.json'),row=raw.find(x=>x.hazards.some(h=>h.hazard.startsWith('ROWTYPE_SELECT:')));
 const oldRoot=path.join(here,'../d10-reader-current-source-20261017'),selection=JSON.parse(fs.readFileSync(path.join(oldRoot,'PAYLOAD_SELECTION.json')));
 const source=fs.readFileSync(path.join(oldRoot,selection.targets.find(x=>x.target===row.target).source),'utf8');
 const d10=createReviewedD10ReaderClassifier(root,{...options,tupleProof:()=>false});
 assert.throws(()=>d10(source,row.target,new Map()),/SEMANTIC_TYPE_NOT_PROVED/);
});
