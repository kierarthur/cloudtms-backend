import fs from 'node:fs';
import crypto from 'node:crypto';
const base='docs/healthroster-build/proposals/protected-source-capability';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const record=file=>{const b=fs.readFileSync(file);return {file,bytes:b.length,sha256:sha(b)};};
const money=JSON.parse(fs.readFileSync('docs/healthroster-build/F5_INTEGRATION_MANIFEST.json','utf8'));
for(const item of money.files){const actual=record(item.file);if(actual.bytes!==item.bytes||actual.sha256!==item.sha256)throw Error('FROZEN_MONEY_CHANGED '+item.file);}
const prior=[
 ['docs/healthroster-build/proposals/f5-window-receipt-complete/REVIEW_MANIFEST.json','b57c4601fd792b3489e5fab6178ad325819b829e4fde36d517ed3af9132045c7'],
 ['docs/healthroster-build/proposals/f5-window-receipt-complete/generated/SOURCE_MANIFEST.json','273ccce5151c3433ccb97d888bf5c289c0aa997338af7a84f763bfc59cbe7430']
].map(([file,expected])=>{const actual=record(file);if(actual.sha256!==expected)throw Error('FROZEN_WINDOW_CHANGED '+file);return actual;});
const window=JSON.parse(fs.readFileSync(prior[0].file,'utf8'));
const inspect=value=>{if(Array.isArray(value))return value.forEach(inspect);if(!value||typeof value!=='object')return;
 if(typeof value.file==='string'&&typeof value.sha256==='string'&&value.file.startsWith('docs/healthroster-build/proposals/')){
  const actual=record(value.file);if(actual.sha256!==value.sha256||(value.bytes!==undefined&&actual.bytes!==value.bytes))throw Error('FROZEN_ARTIFACT_CHANGED '+value.file);
 }
 for(const child of Object.values(value))if(typeof child==='object')inspect(child);
};inspect(window);
const walk=dir=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(x=>x.isDirectory()?walk(dir+'/'+x.name):[dir+'/'+x.name]);
const artifacts=walk(base).filter(x=>!x.endsWith('/REVIEW_MANIFEST.json')).sort().map(record);
const source=JSON.parse(fs.readFileSync(base+'/generated/SOURCE_MANIFEST.json','utf8'));
const functions=source.artifacts.filter(x=>x.file.endsWith('.sqlinc')).flatMap(x=>[...fs.readFileSync(x.file,'utf8').matchAll(/create or replace function\s+([\w.]+)\s*\(/gi)].map(m=>m[1]));
if(functions.length!==18)throw Error('COMPLETE_OWNER_CENSUS_CHANGED');
const result={schema_version:'PROTECTED_SOURCE_CAPABILITY_REVIEW_PACKAGE_V1',status:'FROZEN_SOURCE_PROPOSAL_ROOT_REVIEW',
 source_artifacts:artifacts,source_manifest:record(base+'/generated/SOURCE_MANIFEST.json'),complete_sql_definitions:18,new_private_signatures:10,
 replaced_private_signatures:2,replaced_service_rpcs:6,new_service_rpcs:0,new_tables:0,new_columns:0,
 local_tests:{command:'node --test '+base+'/proposal.test.mjs',passing:14,qualification_fixtures:'SYNTHETIC_UNIT_ONLY_NOT_ACTIVATION_EVIDENCE'},
 native_financial_acceptance:false,db_git_network_mutations:0,frozen_money28_unchanged:true,frozen_window_proposal_unchanged:true,prior_manifest_proofs:prior,
 remaining_gates:['root schema/complete-owner compile and exact ACL union','actual native Source CAS C01/C02/C03 and identity fixtures','actual activation/evidence bootstrap sequence',
  'non-self-bill whole native Candidate/Bank financial journey','Root1745 owned-stage/shared four-fact proof','credit-owned automatic full component proof','whole native G1/I-1 caller graph and concurrency',
  'source/report/protected pay witness closure','actual Client/API/document/FINAL_ISSUE/early-window proof']};
fs.writeFileSync(base+'/REVIEW_MANIFEST.json',JSON.stringify(result,null,2)+'\n');
process.stdout.write(JSON.stringify(record(base+'/REVIEW_MANIFEST.json'))+'\n');
