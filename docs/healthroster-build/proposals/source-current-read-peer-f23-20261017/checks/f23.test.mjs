import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {originalReviews} from './prior-review.mjs';
import {createReviewedSourceReadClassifier,reviewPath} from '../payload/scripts/healthroster-source-read-type-review.mjs';

const here=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const report=path.resolve(here,'../../../..');
const root=path.resolve(report,'../healthroster-install2-14102026');
const sha=b=>createHash('sha256').update(b).digest('hex');
const proof=JSON.parse(fs.readFileSync(path.join(here,'CLASSIFICATION.json')));
const original=await originalReviews(root),gate=original.gate;
// The actual gate's FROZEN_BASE census includes verification SQL as well as
// repeatables. The earlier Same-specific test fixture omitted this verifier.
const mandatory='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql';
const frozenMandatory=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',gate.FROZEN_BASE+':'+mandatory],{cwd:report});
for(const point of gate.specialConstructViolations(frozenMandatory.toString(),mandatory))
  original.baseline.set(point.key,(original.baseline.get(point.key)??0)+1);
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
const {createReviewedSameSeparateClassifier}=await import(pathToFileURL(path.join(root,'scripts/healthroster-same-separate-type-review.mjs')).href);
const same=createReviewedSameSeparateClassifier(root,{tupleProof:original.tupleProof,classifyPrior:original.classifyPrior,
  hazardCensus:gate.specialConstructViolations,statementScan:scanManagedSql});
const fullPrior=(source,file,initial)=>same(source,file,original.classifyPrior(source,file,initial),
  gate.specialConstructViolations(source,file),gate.executableParts(source,file));
const normalRead=file=>fs.readFileSync(path.join(file.startsWith('docs/healthroster-build/proposals/source-current-read-peer-f23-20261017/')?report:root,file));
const make=(mutations=new Map(),prior=fullPrior)=>createReviewedSourceReadClassifier(root,{
  readFile:file=>mutations.has(file)?mutations.get(file):normalRead(file),
  classifyPrior:prior,hazardCensus:gate.specialConstructViolations});
const checks=[],check=(name,fn)=>{fn();checks.push(name);};
const classifier=make(),stages=[];
for(const [name,variants]of [['READ',proof.variants.slice(0,11)],['CLASSIFICATION',proof.variants.slice(11,16)],['ADMISSION',proof.variants.slice(16)]]){
  let occurrences=0;
  for(const entry of variants){
    const source=normalRead(entry.source).toString(),points=gate.specialConstructViolations(source,entry.file);
    check('actual-current-census-and-original-proof:'+entry.file+':'+entry.sha256,()=>{
      assert.equal(sha(source),entry.sha256);assert.equal(points.length,entry.points.length);
      gate.assertSqlWriteBoundaries(source,entry.file);gate.assertNextExactNumericTypes(source,entry.file);
      const before=new Map(original.baseline),qualified=classifier(source,entry.file,before,points);
      assert.deepEqual(before,original.baseline);gate.assertSpecialConstructs(source,entry.file,qualified);
      for(const point of entry.points)assert.equal(qualified.get(point.key),point.count);
    });
    occurrences+=points.length;
  }
  stages.push({name,files:variants.length,rawOccurrences:occurrences,uniqueKeys:occurrences});
}
const target=proof.variants.find(v=>v.file.includes('0741_')&&v.points.length===18);
const targetSource=normalRead(target.source).toString();
check('original-unregistered-classifier-refuses-relocated-outer-statements',()=>{
  assert.throws(()=>gate.assertSpecialConstructs(targetSource,target.file,fullPrior(targetSource,target.file,original.baseline)),/SQL_.*UNCLASSIFIED/);
});
check('prior-qualification-cannot-be-replaced-by-same-hash',()=>{
  assert.throws(()=>make(new Map(),()=>new Map())(targetSource,target.file,new Map()),/PRIOR_STATEMENT_UNPROVED/);
});
check('caller-cannot-omit-raw-hazards',()=>assert.throws(()=>classifier(targetSource,target.file,original.baseline,[]),/CALLER_CENSUS_CHANGED/));
for(const [name,changed]of [
  ['added-dynamic-SQL',targetSource+"\ndo $x$ begin execute 'select 1'; end $x$;\n"],
  ['changed-role',targetSource.replace('to service_role','to authenticated')],
  ['changed-aggregate',targetSource.replace('pg_catalog.min(allowed.column_ordinal)','pg_catalog.max(allowed.column_ordinal)')],
  ['duplicated-statement',targetSource+targetSource]
])check('current-whole-source-refuses:'+name,()=>{assert.notEqual(changed,targetSource);assert.throws(()=>classifier(changed,target.file,original.baseline),/CURRENT_SOURCE_CHANGED/);});
for(const pin of [reviewPath,...proof.dependencies.map(p=>p.path),...proof.actualParents.map(p=>p.source??p.path),
  ...proof.reverses.flatMap(p=>[p.output,p.parentSource??p.parent]),...proof.variants.map(p=>p.source)]){
  check('full-owner-or-evidence-mutation-refuses:'+pin,()=>{
    const mutations=new Map([[pin,Buffer.concat([normalRead(pin),Buffer.from('\n-- mutation\n')])]]);
    assert.throws(()=>make(mutations)(targetSource,target.file,original.baseline),/HEALTH_SOURCE_READ_TYPE_/);
  });
}
check('unknown-file-within-read-namespace-gets-no-whole-prefix-allowance',()=>assert.throws(()=>classifier('select 1;',
  'supabase/repeatable/17102026_0722_weekly_source_current_read_health/other.sqlinc',original.baseline),/CURRENT_SOURCE_CHANGED/));
check('unrelated-source-keeps-original-map-and-does-not-load-evidence',()=>{
  const baseline=new Map([['retained',2]]),unrelated=createReviewedSourceReadClassifier(root,{readFile:()=>assert.fail('unrelated source must not load evidence')});
  assert.equal(unrelated('select 1;','supabase/repeatable/unrelated.sql',baseline),baseline);
});
check('canonical-CRLF-keeps-exact-statement-proof',()=>gate.assertSpecialConstructs(targetSource.replaceAll('\n','\r\n'),target.file,
  classifier(targetSource.replaceAll('\n','\r\n'),target.file,original.baseline)));
check('all-three-gate-hunks-reverse-complete-current-parent',()=>{
  const edits=JSON.parse(fs.readFileSync(path.join(here,'GATE_PARENT_EDITS.json'))),parent=fs.readFileSync(path.join(here,'reference/bpay-next-source-gates.mjs'),'utf8');
  let source=fs.readFileSync(path.join(here,'payload/scripts/bpay-next-source-gates.mjs'),'utf8');
  assert.equal(sha(source),edits.outputSha256);assert.equal(sha(parent),edits.inputSha256);
  for(const h of [...edits.hunks].reverse()){assert.equal(source.split(h.after).length,2);source=source.replace(h.after,()=>h.before);}
  assert.equal(source,parent);assert.equal(edits.hunks.length,3);
});
fs.writeFileSync(path.join(here,'F23_TEST_RESULTS.json'),JSON.stringify({passed:checks.length,failed:0,checks,stages,
  originalProofReplayed:true,priorSameAndKeptLaterEntryUnchanged:true,native:'NOT_RUN',dbConnections:0},null,2)+'\n');
console.log(JSON.stringify({passed:checks.length,stages,native:'NOT_RUN'}));
