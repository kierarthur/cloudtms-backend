// Owner-authorised source integration only. No database/deploy/runtime call.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const folder=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(folder,'../../..');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,maxBuffer:64_000_000});
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const object=(commit,file)=>git(['show',commit+':'+file]);
const sameCheckoutText=(saved,committed)=>saved.equals(committed)
 || saved.toString('utf8').replaceAll('\r\n','\n')===committed.toString('utf8').replaceAll('\r\n','\n');
const current='37ddadc0a50721834c9e8a6c06becfc983f0dc32';
const report='ae49914f8b5acbf00c7ab11c2bd1db849ffb5bd6';
const money='d4b62cb15159d0afb02cfebf8c1e034c68c26550';
const prefix='docs/healthroster-build/proposals/install2-shared-runtime-forward-20261014/';
assert.equal(git(['rev-parse','--abbrev-ref','HEAD']).toString().trim(),'codex/healthroster-install2-14102026');
assert.equal(git(['rev-parse','HEAD']).toString().trim(),'03221cb237f59658ecde5217ff84e45d3ab48be8');
const expected=new Map([
 ['broker/src/candidate-app-backend.js','345741edbd5569cc2d27dee2a73a51d510e6498e0d2e761c9c158d050c3e1587'],
 ['broker/src/index.js','ca621191964eaf1c707b38c3cabaf69ade0a717dc4d0b3b94c183788fe293785'],
 ['broker/src/weekly-source/routes.js','284b3487d14ab215e3764bcdfe49c44e39ada7856e5d087f7436df05f5b47a53'],
 ['candidate-broker/src/candidate-broker.js','0d1f0435872793ed0cb1021b7c9122151891c0c5bfd915c7d73de47f95df07ea']
]);
const pinBytes=object(report,prefix+'RUNTIME_DEPENDENCY_PINS.json'),pins=JSON.parse(pinBytes);
assert.equal(pins.files.length,136);
const reverseBytes=object(report,prefix+'FULL_RUNTIME_REVERSE.json'),reverse=JSON.parse(reverseBytes);
const prepared=[];
for(const row of pins.files){const target=path.join(root,row.file);assert.ok(target.startsWith(root+path.sep));
 const forward=expected.has(row.file),addition=row.source_commit===money;
 if(forward||addition){
  assert.equal(git(['status','--porcelain','--',row.file]).toString().trim(),'','SOURCE_TARGET_DIRTY '+row.file);
  const sourceFile=forward?prefix+'forward/'+row.file:row.source_file;
  const commit=forward?report:money,bytes=object(commit,sourceFile);
  assert.equal(sha(bytes),expected.get(row.file)??row.sha256,row.file);
  const prior=fs.existsSync(target)?fs.readFileSync(target):null;
  if(forward){assert.ok(prior);assert.ok(sameCheckoutText(prior,object(current,row.file)),row.file+' SAVED_CURRENT_SOURCE_CHANGED');
   const proof=reverse.files.find(r=>r.file===row.file);assert.ok(proof);
   const lines=bytes.toString('utf8').match(/[^\n]*\n|[^\n]+$/g)??[];
   for(const h of [...proof.hunks].reverse())lines.splice(h.new_line_start,h.new_line_count,...(h.before.match(/[^\n]*\n|[^\n]+$/g)??[]));
   assert.equal(lines.join(''),object(current,row.file).toString('utf8'),'WHOLE_SOURCE_REVERSE '+row.file);
  }else assert.equal(prior,null,'NEW_SOURCE_ALREADY_EXISTS '+row.file);
  prepared.push({file:row.file,sourceCommit:commit,sourceFile,bytes,beforeRawSha256:prior?sha(prior):null,afterSha256:sha(bytes)});
 }else{
  assert.ok(fs.existsSync(target),'CLOSURE_DEPENDENCY_ABSENT '+row.file);
  const committed=object('HEAD',row.file),saved=fs.readFileSync(target);
  assert.equal(sha(committed),row.sha256,'COMMITTED_CLOSURE_DEPENDENCY_CHANGED '+row.file);
  assert.ok(sameCheckoutText(saved,committed),'SAVED_CLOSURE_DEPENDENCY_CHANGED '+row.file);
  assert.equal(git(['status','--porcelain','--',row.file]).toString().trim(),'','CLOSURE_DEPENDENCY_DIRTY '+row.file);
 }
}
assert.equal(prepared.length,6);
for(const row of prepared){const target=path.join(root,row.file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,row.bytes);}
const syntax=[];
for(const file of pins.syntax_files){const check=spawnSync(process.execPath,['--check',path.join(root,file)],{windowsHide:true,encoding:'utf8'});assert.equal(check.status,0,'ACTUAL_MOUNTED_SYNTAX '+file);syntax.push(file);}
fs.writeFileSync(path.join(folder,'RUNTIME_IMPORT_CLOSURE_14102026.json'),pinBytes);
fs.writeFileSync(path.join(folder,'RUNTIME_WHOLE_SOURCE_REVERSE_14102026.json'),reverseBytes);
fs.writeFileSync(path.join(folder,'RUNTIME_THREE_WAY_INPUTS_14102026.json'),object(report,prefix+'THREE_WAY_INPUTS.json'));
fs.writeFileSync(path.join(folder,'RUNTIME_SOURCE_BACKUP_STATUS_14102026.md'),object(report,prefix+'SOURCE_BACKUP_STATUS.md'));
const receipt={sourceOnly:true,status:'MOUNTED_SOURCE_SYNTAX_PASS_NATIVE_UNPROVED',beforeRootCommit:'03221cb237f59658ecde5217ff84e45d3ab48be8',reportSource:report,moneySource:money,closurePaths:136,syntaxFiles:syntax.length,fullReverseHunks:reverse.files.reduce((n,row)=>n+row.hunks.length,0),mounted:prepared.map(({bytes,...row})=>row),sourcePinsSha256:sha(pinBytes),noCollectorMapperSealMounted:true,noDatabaseReleaseOrRuntimeCall:true};
fs.writeFileSync(path.join(folder,'RUNTIME_COMPOSITION_14102026.json'),JSON.stringify(receipt,null,2)+'\n');
console.log(JSON.stringify(receipt));
