// Retain all frozen predecessor assertions; redirect only whole DTO/transport imports.
// No SQL, VM-native business state, database or network is executed.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';import {createHash} from 'node:crypto';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),pins=[];
const sha=b=>createHash('sha256').update(b).digest('hex');
for(const file of ['tests/healthroster-checklist-source.test.mjs','tests/healthroster-checklist-read-transport.test.mjs','tests/healthroster-checklist-source-query-money.test.mjs','tests/healthroster-checklist-source-invoice-block.test.mjs','tests/healthroster-checklist-invoice-decision-transport.test.mjs','tests/healthroster-checklist-ordinary-next-hold.test.mjs','tests/healthroster-checklist-invoice-decision-action.test.mjs']){
 const commit='e7e0b572bc94ef65ad05899d755cd43d75a82b97',raw=execFileSync('git',['-c','maintenance.auto=false','cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:4e6}),url=pathToFileURL(path.join(repo,file));assert.deepEqual(raw,fs.readFileSync(path.join(repo,file)));
 let source=raw.toString();const expected={'healthroster-checklist-source-query-money.test.mjs':3,'healthroster-checklist-source-invoice-block.test.mjs':2,'healthroster-checklist-invoice-decision-transport.test.mjs':1,'healthroster-checklist-ordinary-next-hold.test.mjs':1,'healthroster-checklist-invoice-decision-action.test.mjs':2};
 assert.equal((source.match(/import\.meta\.url/g)||[]).length,expected[path.basename(file)]||0);source=source.replaceAll('import.meta.url',JSON.stringify(url.href));
 source=source.replace(/from\s+(['"])(\.[^'"]+)\1/g,(all,q,r)=>{let target=new URL(r,url);if(r.endsWith('/healthroster-checklist-source.mjs'))target=pathToFileURL(path.join(here,'forward/healthroster-checklist-source.mjs'));if(r.endsWith('/checklist-read.mjs'))target=pathToFileURL(path.join(here,'forward/checklist-read.mjs'));return 'from '+q+target.href+q;});
 pins.push({commit,file,raw_sha256:sha(raw),adapted_sha256:sha(source),loader_changes_only:true,all_original_assertions_unchanged:true});await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
}
fs.writeFileSync(path.join(here,'WHOLE_PREDECESSOR_PINS.json'),JSON.stringify({source_only:true,native:false,pins},null,2)+'\n');
