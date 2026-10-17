import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import {fileURLToPath} from 'node:url';
import {createReviewedD10ReaderClassifier} from './payload/scripts/healthroster-d10-reader-type-review.mjs';
import {provesSingleTableTuple} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/scripts/healthroster-rowtype-type-review.mjs';
import {specialConstructViolations,assertSpecialConstructs} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/scripts/bpay-next-source-gates.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),base='docs/healthroster-build/proposals/d10-reader-current-source-20261017/';
const json=p=>JSON.parse(fs.readFileSync(path.join(here,p))),selection=json('PAYLOAD_SELECTION.json'),census=json('RAW_F23_CENSUS.json');
const source=file=>fs.readFileSync(path.join(here,selection.targets.find(x=>x.target===file).source),'utf8');
function classifier(override=null,tupleProof=provesSingleTableTuple){return createReviewedD10ReaderClassifier('NO_RUNTIME_ROOT',{tupleProof,hazardCensus:specialConstructViolations,readFile:file=>{assert(file.startsWith(base));const relative=file.slice(base.length),raw=fs.readFileSync(path.join(here,relative));return override?override(relative,raw):raw;}});}
test('actual thirteen points classify using exact native schema and retained construction',()=>{
 const review=classifier();let total=0;
 for(const row of census){const sql=source(row.target),m=review(sql,row.target,new Map());assertSpecialConstructs(sql,row.target,m);total+=row.hazards.length;}
 assert.equal(total,13);
});
test('every classified file refuses a changed complete owner',()=>{
 const review=classifier();for(const row of census.filter(x=>x.hazards.length))assert.throws(()=>review(source(row.target)+'\n-- foreign successor\n',row.target,new Map()),/CURRENT_SOURCE_CHANGED/);
});
test('same SQL outside reviewed scope gains no allowance',()=>{
 const row=census.find(x=>x.hazards.length),sql=source(row.target),m=classifier()(sql,'supabase/repeatable/20990101_unknown.sql',new Map());assert.equal(m.size,0);assert.throws(()=>assertSpecialConstructs(sql,row.target,m));
});
test('each whole proof input is pinned and a substituted artifact refuses',()=>{
 for(const name of ['GRAMMAR_TYPE_RESULTS.json','RAW_F23_CENSUS.json','parents/supabase/release/current-contract.json','SOURCE_PROVENANCE.json','SOURCE_OWNER_REVERSES.json','FULL_PARENT_FORWARD.json']){
  const review=classifier((p,b)=>p===name?Buffer.concat([b,Buffer.from(' ')]):b),row=census.find(x=>x.hazards.length);assert.throws(()=>review(source(row.target),row.target,new Map()),/PROOF_INPUT_CHANGED/);
 }
});
test('tuple proof is necessary and a duplicate actual census cannot inherit admission',()=>{
 const row=census.find(x=>x.hazards.some(h=>h.hazard.startsWith('ROWTYPE_SELECT:')));assert.throws(()=>classifier(null,()=>false)(source(row.target),row.target,new Map()),/SEMANTIC_TYPE_NOT_PROVED/);
 const review=createReviewedD10ReaderClassifier('NO_RUNTIME_ROOT',{tupleProof:provesSingleTableTuple,hazardCensus:(s,f)=>[...specialConstructViolations(s,f),...specialConstructViolations(s,f)],readFile:p=>fs.readFileSync(path.join(here,p.slice(base.length)))});assert.throws(()=>review(source(row.target),row.target,new Map()),/ACTUAL_CENSUS_CHANGED/);
});
test('baseline allowances are preserved and inputs are never mutated',()=>{
 const row=census.find(x=>x.hazards.length),old=new Map([['unrelated-prior-key',4]]),result=classifier()(source(row.target),row.target,old);assert.equal(old.size,1);assert.equal(old.get('unrelated-prior-key'),4);assert.equal(result.get('unrelated-prior-key'),4);
});
