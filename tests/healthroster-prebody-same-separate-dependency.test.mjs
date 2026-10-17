import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
import {canonical,digest} from '../scripts/cloudtms-managed-release-sql.mjs';
import {prebodyDependencySource} from '../scripts/healthroster-prebody-dynamic-review.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const proof=JSON.parse(fs.readFileSync(path.join(root,'docs/healthroster-build/v6-owner-delivery-20261016/SAME_SEPARATE_PREBODY_DEPENDENCY_FORWARD_20261017.json')));
const file=proof.targetAdapter,current=canonical(fs.readFileSync(path.join(root,file)));
const input={file,sha256:proof.adapterBeforeSha256};
let prior=current;
for(const[before,after]of proof.adapterLiteralEdits){assert.equal(prior.split(after).length,2);prior=prior.replace(after,()=>before);}
test('exact reviewed adapter reconstructs complete historical bytes without changing runtime',()=>{
 assert.equal(digest(current),proof.adapterAfterSha256);
 assert.equal(digest(prior),input.sha256);
 assert.equal(prebodyDependencySource(input,current),prior);
 assert.equal(canonical(fs.readFileSync(path.join(root,file))),current);
});
test('genuine historical adapter remains admitted without translation',()=>assert.equal(prebodyDependencySource(input,prior),prior));
test('any additional or missing adapter change refuses',()=>{
 for(const source of [current+'\n// drift',current.replace('weekly_source_later_change_decide_atomic_v1','another_operation'),current.replace('qualified_fixture','different_fixture')])
  assert.throws(()=>prebodyDependencySource(input,source),/BOUND_SOURCE_CHANGED/);
});
test('another file or expected hash gains no historical translation',()=>{
 assert.equal(prebodyDependencySource({...input,file:file+'.copy'},current),current);
 assert.equal(prebodyDependencySource({...input,sha256:'0'.repeat(64)},current),current);
});
test('both build helpers retain complete literal reversals',()=>{
 for(const name of ['SAME_SEPARATE_PREBODY_DEPENDENCY_FORWARD_20261017.json','SAME_SEPARATE_F23_PREBODY_BINDING_FORWARD_20261017.json']){
  const row=JSON.parse(fs.readFileSync(path.join(root,'docs/healthroster-build/v6-owner-delivery-20261016',name)));
  const source=fs.readFileSync(path.join(root,row.file),'utf8');
  assert.equal(digest(source),row.afterSha256);
  assert.equal(source.split(row.hunk.after).length,2);
  assert.equal(digest(source.replace(row.hunk.after,()=>row.hunk.before)),row.beforeSha256);
 }
});
