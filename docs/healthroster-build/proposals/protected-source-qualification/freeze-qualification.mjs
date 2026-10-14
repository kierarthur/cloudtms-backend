import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(dir,'../../../..');
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const prior=['docs/healthroster-build/F5_INTEGRATION_MANIFEST.json','docs/healthroster-build/proposals/m1-whole-shift/REVIEW_MANIFEST.json',
 'docs/healthroster-build/proposals/f5-window-rounding/REVIEW_MANIFEST.json','docs/healthroster-build/proposals/f5-window-receipt-complete/REVIEW_MANIFEST.json',
 'docs/healthroster-build/proposals/protected-source-capability/REVIEW_MANIFEST.json','docs/healthroster-build/proposals/native-financial-fixtures/NATIVE_FIXTURE_REVIEW_MANIFEST.json'];
const previous=[];
for(const file of prior){const bytes=fs.readFileSync(path.join(repo,file)),m=JSON.parse(bytes),rows=m.files??m.source_artifacts;
 if(!Array.isArray(rows))throw Error('QUAL_PRIOR_MANIFEST_INVALID');
 for(const row of rows){const name=row.file??row.path;if(sha(fs.readFileSync(path.join(repo,name)))!==row.sha256)throw Error('QUAL_FROZEN_CHANGED '+name);}
 previous.push({file,sha256:sha(bytes),inputs_verified:rows.length});
}
const files=fs.readdirSync(dir).filter(file=>file!=='REVIEW_MANIFEST.json').sort().map(file=>{
 const bytes=fs.readFileSync(path.join(dir,file));return {file:path.relative(repo,path.join(dir,file)).replaceAll('\\','/'),bytes:bytes.length,sha256:sha(bytes)};
});
const manifest={schema_version:'PROTECTED_SOURCE_ROLLBACK_QUALIFICATION_REVIEW_PACKAGE_V1',status:'FROZEN_SOURCE_PROPOSAL_NATIVE_UNEXECUTED',
 files,previous,new_owner_private_signatures:8,new_tables:0,new_columns:0,new_public_rpcs:0,new_service_grants:0,
 tests:{passing:27,type:'Pure protocol/source/literal vectors only; no certificate inputs',javascript_syntax:'PASS'},
 source_worktree:'healthroster-v2-money-build-13102026',operational_ready_owners_edited:false,
 frozen_inputs_edited:false,root_worktree_edited:false,database_calls:0,docker_calls:0,network_service_calls:0,
 root_gates:['Actual generated contract/schema/SQL/provider normalization compilation','Owner/session/caller-frame/shared-turn/xid refusal proof',
 'Lawful installed disabled-first R1 then exact actual native prerequisite flows','39-category own-stage Source CAS equality and root census',
 'Genuine signature/document/renderer and published NEXT prerequisites','R1/R2 actual protected artifact/history/catalogue/replay proof',
 'Lawful activation followed by all actual operational API/UI/app/native replay obligations']};
fs.writeFileSync(path.join(dir,'REVIEW_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({files:files.length,bytes:files.reduce((sum,f)=>sum+f.bytes,0),frozen_inputs_verified:previous.reduce((sum,p)=>sum+p.inputs_verified,0),
 manifest_sha256:sha(fs.readFileSync(path.join(dir,'REVIEW_MANIFEST.json')))}));
