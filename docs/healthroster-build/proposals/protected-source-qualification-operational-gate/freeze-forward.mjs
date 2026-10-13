import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(out,'../../../..');
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const frozen=[];
for(const [folder,name]of [['f5-window-rounding','REVIEW_MANIFEST.json'],['m1-whole-shift','REVIEW_MANIFEST.json'],
 ['f5-window-receipt-complete','REVIEW_MANIFEST.json'],['protected-source-capability','REVIEW_MANIFEST.json'],
 ['native-financial-fixtures','NATIVE_FIXTURE_REVIEW_MANIFEST.json'],['protected-source-qualification','REVIEW_MANIFEST.json']]){
 const file=path.join(out,'..',folder,name),manifest=JSON.parse(fs.readFileSync(file,'utf8'));
 const rows=manifest.files??manifest.source_artifacts??manifest.artifacts;
 if(!Array.isArray(rows))throw Error('FORWARD_FROZEN_MANIFEST_SHAPE '+folder);
 let count=0;
 for(const row of rows){
  const relative=row.file??row.path;if(typeof relative!=='string'||!row.sha256)throw Error('FORWARD_FROZEN_ROW_SHAPE '+folder);
  const file=path.resolve(repo,relative);if(!file.startsWith(repo+path.sep))throw Error('FORWARD_FROZEN_PATH_OUTSIDE_OWN_WORKTREE');
  if(sha(fs.readFileSync(file))!==row.sha256)throw Error('FORWARD_FROZEN_CHANGED '+relative);count++;
 }
 frozen.push({path:path.relative(repo,file).replaceAll('\\','/'),sha256:sha(fs.readFileSync(file)),verified_files:count});
}
const files=fs.readdirSync(out).filter(file=>file!=='REVIEW_MANIFEST.json').sort().map(file=>{
 const full=path.join(out,file);if(!fs.statSync(full).isFile())throw Error('FORWARD_NON_FILE');const bytes=fs.readFileSync(full);
 if(/(?:\.env|\.dev\.vars|auth\.json|raw.*log|\.secret)/i.test(file))throw Error('FORWARD_PRIVATE_PATH');
 return {file:path.relative(repo,full).replaceAll('\\','/'),bytes:bytes.length,sha256:sha(bytes)};
});
const closure=JSON.parse(fs.readFileSync(path.join(out,'COMPLETE_CLOSURE_CENSUS.json'),'utf8'));
const manifest={schema_version:'PROTECTED_SOURCE_STRICT_OPERATIONAL_GATE_REVIEW_PACKAGE_V1',
 status:'FROZEN_SOURCE_PROPOSAL_NATIVE_REMOTE_ACTIVATION_UNEXECUTED',files,frozen_predecessors:frozen,
 source_checks:{new_protocol_source_handler_tests_passed:18,javascript_syntax_passed:true,
  reverse_hunks_complete:true,actual_git_object_negative_vectors:true,normalized_constructor_bypass_negatives:true},
 literal_private_signature_union:closure.proposed_complete_private_signature_union,
 root_private_signatures_preserved:closure.base_private_count,new_distinct_private_signatures:closure.new_distinct_private_count,
 planned_private_count:closure.planned_private_count,tables_preserved:105,service_signatures_preserved:142,
 new_public_rpcs:0,new_service_grants:0,new_tables:0,new_columns:0,operational_prerequisite_v2:false,
 actual_sql_compile:false,actual_native_business_acceptance:false,actual_deployed_http_ui_phone:false,actual_activation:false,
 holds:['Real Banking install/current compatible NEXT and actual installed verified strict source',
  'Root native compile/caller/ACL/provider proof and all-39 original CAS equality',
  'Actual genuine app account/signatures/documents/native financial/HTTP/UI/phone acceptance',
  'Original full SECURITY activation assertion through actual intended native owner',
  'Lawful automatic F1 rule/date and full component/native/caller journey',
  'Separate Report A/B operational dispatch qualification'],
 root_integration:'Preserve all current 232 private/142 service owners and 2205/2206 registrations; compose literal 258 source union and remeasure actual catalogue. Do not reset current root registry.'};
fs.writeFileSync(path.join(out,'REVIEW_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({files:files.length+1,bytes:files.reduce((n,row)=>n+row.bytes,0),
 manifest_sha256:sha(fs.readFileSync(path.join(out,'REVIEW_MANIFEST.json'))),frozen_verified:frozen.reduce((n,row)=>n+row.verified_files,0),
 planned_private_count:closure.planned_private_count,source_only:true}));
