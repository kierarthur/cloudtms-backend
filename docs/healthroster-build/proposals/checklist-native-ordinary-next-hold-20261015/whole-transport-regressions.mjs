// Preserve unchanged whole predecessor assertion bodies against the complete
// ordinary successor DTO/transport. This is repeated compatibility evidence.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {pathToFileURL,fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),commit='9b44412d7fdda7f8e099036e7c8a7297a8eeb795',pins=[];
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
for(const file of ['tests/healthroster-checklist-source.test.mjs','tests/healthroster-checklist-read-transport.test.mjs','tests/healthroster-checklist-source-query-money.test.mjs',
 'tests/healthroster-checklist-source-invoice-block.test.mjs','tests/healthroster-checklist-invoice-decision-transport.test.mjs']){
 const b=execFileSync('git',['cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:4e6}),url=pathToFileURL(path.join(repo,file));
 let source=b.toString().replaceAll('import.meta.url',JSON.stringify(url.href));source=source.replace(/from\s+(['"])(\.[^'"]+)\1/g,(all,q,r)=>{
  let target=new URL(r,url);if(r.endsWith('/healthroster-checklist-source.mjs'))target=pathToFileURL(path.join(here,'forward/healthroster-checklist-source.mjs'));
  if(r.endsWith('/checklist-read.mjs'))target=pathToFileURL(path.join(here,'forward/checklist-read.mjs'));return 'from '+q+target.href+q;});
 pins.push({commit,file,original_sha256:sha(b),adapted_sha256:sha(source),changes:'Only imports/import.meta redirect to pinned predecessor sources and complete successor DTO/transport. Test bodies/assertions unchanged.'});
 await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));}
fs.writeFileSync(path.join(here,'WHOLE_TRANSPORT_REGRESSION_PINS.json'),JSON.stringify({source_only:true,native:false,pins},null,2)+'\n');
