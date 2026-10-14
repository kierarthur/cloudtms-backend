// Execute unchanged predecessor test bodies with the WHOLE successor DTO and
// transport imports. No simplified guards or selective assertion omission.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {pathToFileURL,fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),commit='aea33728bb76c4d2c57a9e82abffa570e743a0f0';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),pins=[];
for(const file of ['tests/healthroster-checklist-source.test.mjs','tests/healthroster-checklist-read-transport.test.mjs','tests/healthroster-checklist-source-query-money.test.mjs','tests/healthroster-checklist-source-invoice-block.test.mjs']){
 const bytes=execFileSync('git',['cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:4e6}),url=pathToFileURL(path.join(repo,file));
 let source=bytes.toString().replaceAll('import.meta.url',JSON.stringify(url.href));
 source=source.replace(/from\s+(['"])(\.[^'"]+)\1/g,(all,quote,relative)=>{
  let target=new URL(relative,url);if(relative.endsWith('/healthroster-checklist-source.mjs'))target=pathToFileURL(path.join(here,'forward/healthroster-checklist-source.mjs'));
  if(relative.endsWith('/checklist-read.mjs'))target=pathToFileURL(path.join(here,'forward/checklist-read.mjs'));return 'from '+quote+target.href+quote;});
 pins.push({commit,file,original_sha256:sha(bytes),adapted_sha256:sha(source),changes:'Only relative imports/import.meta resolved to actual pinned predecessor location and whole successor DTO/transport. All test bodies/assertions unchanged.'});
 await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
}
fs.writeFileSync(path.join(here,'WHOLE_TRANSPORT_REGRESSION_PINS.json'),JSON.stringify({source_only:true,native:false,pins},null,2)+'\n');
