import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {parseGitTree,assertTreeEquivalence,sourceInventory} from './source-equivalence.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)), own=path.resolve(out,'../../../..');
const root=path.resolve(own,'../healthroster-v2-backend-13102026');
const commit='cea15289e0283e3383e71f0acb8b18ce184647c2';
const git=(args,input)=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,input,maxBuffer:256*1024*1024});
const raw=git(['ls-tree','-r','-z',commit]), rows=parseGitTree(raw);
const ids=[...new Set(rows.map(row=>row.blob))],batch=git(['cat-file','--batch'],ids.join('\n')+'\n');
const blobs=new Map();let offset=0;
for(const id of ids){
 const end=batch.indexOf(10,offset),header=batch.subarray(offset,end).toString('ascii').match(/^([0-9a-f]{40}) blob ([0-9]+)$/);
 assert.equal(header?.[1],id);const size=Number(header[2]),start=end+1;blobs.set(id,batch.subarray(start,start+size));offset=start+size+1;
 assert.equal(batch[offset-1],10);
}
assert.equal(offset,batch.length);
const read=id=>{assert.equal(blobs.has(id),true);return blobs.get(id);};
const clone=()=>rows.map(row=>({...row}));
const serialize=values=>Buffer.from(values.map(row=>`${row.mode} ${row.type} ${row.blob}\t${row.path}\0`).join(''));
const prefixJson=/\|\|\s*[A-Za-z_][A-Za-z_0-9.]*\s*->>?/;

test('actual current Git tree is entirely selected with mode/type/bytes intact',()=>{
 assert.equal(rows.length,raw.toString('utf8').split('\0').filter(Boolean).length);
 assert.equal(sourceInventory.selection,'ALL_TRACKED_GIT_OBJECTS');
 assert.equal(assertTreeEquivalence(rows,rows,read).files.length,rows.length);
 for(const file of ['osv-scanner.toml','.gitattributes','AGENTS.md','infra/miget/cloudtms-miget-operations/src/index.js']){
  if(file.startsWith('infra/'))assert.equal(rows.some(row=>row.path.startsWith('infra/')),true);
  else assert.equal(rows.some(row=>row.path===file),true);
 }
});
for(const file of ['osv-scanner.toml','.gitattributes']){
 test(`actual ${file} blob mutation to a different real Git object is refused`,()=>{
  const mutated=clone(),target=mutated.find(row=>row.path===file),other=rows.find(row=>row.path==='.node-version');
  assert.notEqual(target.blob,other.blob);assert.notDeepEqual(read(target.blob),read(other.blob));target.blob=other.blob;
  assert.throws(()=>assertTreeEquivalence(rows,mutated,read),/QUALIFICATION_NATIVE_SOURCE_CHANGED/);
 });
 test(`actual ${file} executable mode and unsupported object type mutations are refused`,()=>{
  const mode=clone(),item=mode.find(row=>row.path===file);item.mode=item.mode==='100644'?'100755':'100644';
  assert.throws(()=>assertTreeEquivalence(rows,mode,read),/QUALIFICATION_SOURCE_MODE_TYPE_OR_PATH_CHANGED/);
  for(const changes of [{type:'commit',mode:'160000'},{type:'blob',mode:'120000'}]){
   const kind=clone();Object.assign(kind.find(row=>row.path===file),changes);
   assert.throws(()=>parseGitTree(serialize(kind)),/QUALIFICATION_EXECUTION_OBJECT_UNSUPPORTED/);
  }
 });
}
test('AGENTS and actual documentary/native-result inputs cannot change between R1 and R2',()=>{
 for(const file of ['AGENTS.md','docs/healthroster-build/checkpoint13102026/AUTOMATIC_SHORT_COMPILE_TRIAL_3.json']){
  const changed=clone(),target=changed.find(row=>row.path===file);assert.ok(target);target.blob=rows.find(row=>row.path==='.node-version').blob;
  assert.throws(()=>assertTreeEquivalence(rows,changed,read),/QUALIFICATION_NATIVE_SOURCE_CHANGED/);
 }
 const extended=parseGitTree(Buffer.concat([raw,Buffer.from(`100644 blob ${rows[0].blob}\tnew-root-toolchain-config.toml\0`)]));
 assert.equal(extended.length,rows.length+1);
 assert.throws(()=>assertTreeEquivalence(rows,extended,read),/QUALIFICATION_SOURCE_SET_CHANGED/);
});
test('actual first-use setup lock extracts JSON text before concatenating; guard catches former class',()=>{
 const corrected=fs.readFileSync(path.join(out,'TEST_JOURNEY_SCOPE.sqlinc'),'utf8');
 const former=fs.readFileSync(path.resolve(out,'../protected-source-qualification-operational-gate/TEST_JOURNEY_SCOPE.sqlinc'),'utf8');
 assert.match(corrected,/hashtextextended\('weekly-source-test-journey:'\|\|\(auth->>'scope_id'\),0\)/);
 assert.equal(prefixJson.test(corrected),false);assert.equal(prefixJson.test(former),true);
 for(const illegal of ["'prefix:' || auth ->> 'key'", "value ||\n item->'payload'", "'a:'||v.other->>'key'"]){assert.equal(prefixJson.test(illegal),true);}
 for(const legal of ["'prefix:' || (auth->>'key')", "value || (item->'payload')"]){assert.equal(prefixJson.test(legal),false);}
});
test('complete SQL DTO guard and native sidecar retain required actual source-gate inputs',()=>{
 const cert=fs.readFileSync(path.join(out,'OPERATIONAL_CERTIFICATE.sqlinc'),'utf8');
 assert.match(cert,/unnest\(array\['\.node-version','\.gitattributes','osv-scanner\.toml'/);
 const a=fs.readFileSync(path.join(out,'source-equivalence.mjs'),'utf8'), b=fs.readFileSync(path.join(out,'weekly-source-protected-execution-tree.mjs'),'utf8');
 assert.equal(a.replace("from '../protected-source-capability/release-qualification.mjs'","from './weekly-source-protected-release-qualification.mjs'"),b);
});
