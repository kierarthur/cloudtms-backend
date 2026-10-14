import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(out,'../../../..');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:repo,maxBuffer:5_000_000});
const predecessorCommit='6ea0656eab7c54e6da6554fa55db9acb35760997';
const manifests=[['f5-window-rounding','REVIEW_MANIFEST.json'],['m1-whole-shift','REVIEW_MANIFEST.json'],
 ['f5-window-receipt-complete','REVIEW_MANIFEST.json'],['protected-source-capability','REVIEW_MANIFEST.json'],
 ['native-financial-fixtures','NATIVE_FIXTURE_REVIEW_MANIFEST.json'],['protected-source-qualification','REVIEW_MANIFEST.json'],
 ['protected-source-qualification-operational-gate','REVIEW_MANIFEST.json'],
 ['protected-source-qualification-operational-gate-pin-forward-2206','REVIEW_MANIFEST.json'],
 ['protected-source-qualification-operational-gate-pin-forward-2206-trial2','REVIEW_MANIFEST.json']];
const frozen=manifests.map(([folder,name])=>{
 const relative=`docs/healthroster-build/proposals/${folder}/${name}`,bytes=fs.readFileSync(path.join(repo,relative));
 assert.deepEqual(bytes,git(['show',`${predecessorCommit}:${relative}`]));
 const m=JSON.parse(bytes),rows=m.files??m.source_artifacts??m.artifacts;
 for(const row of rows){const file=row.file??row.path,b=fs.readFileSync(path.join(repo,file));assert.equal(sha(b),row.sha256,file);}
 return {file:relative,sha256:sha(bytes),verified_declared_inputs:rows.length};
});
const criticSource=path.resolve(repo,'../../cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_BUILD_13102026/STRICT_QUALIFICATION_SOURCE_CRITIC.md');
const critic=fs.readFileSync(criticSource);assert.equal(sha(critic),'5fe44bacfbbafc6ac5c793fd1932bc5021b745d6ddada405b2b8f864c748d9d5');
fs.writeFileSync(path.join(out,'STRICT_QUALIFICATION_SOURCE_CRITIC.md'),critic);
const result=execFileSync(process.execPath,['--test','--test-reporter=tap',path.join(out,'forward.test.mjs')],{cwd:repo,maxBuffer:1_000_000});
const text=result.toString('utf8');assert.match(text,/# pass 8\r?\n/);assert.match(text,/# fail 0\r?\n/);
fs.writeFileSync(path.join(out,'FORWARD_TEST_RESULTS.tap'),result);
for(const file of fs.readdirSync(out).filter(file=>/\.(mjs|js)$/.test(file)))execFileSync(process.execPath,['--check',path.join(out,file)],{cwd:repo,maxBuffer:1_000_000});
const files=fs.readdirSync(out).filter(file=>file!=='REVIEW_MANIFEST.json').sort().map(file=>{
 const b=fs.readFileSync(path.join(out,file));return {file:path.relative(repo,path.join(out,file)).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};
});
const record={schema_version:'STRICT_QUALIFICATION_FIRST_USE_INVENTORY_CORRECTION_REVIEW_V1',status:'FROZEN_SOURCE_CORRECTION_NATIVE_OPEN_CLOSE_REVOKE_AND_OPERATIONAL_GATES_HELD',
 files,frozen_predecessors:frozen,critic_sha256:sha(critic),source_tests_passed:8,source_tests_failed:0,javascript_syntax_passed:true,full_owner_reverse_proofs:4,
 root_source_commit:'cea15289e0283e3383e71f0acb8b18ce184647c2',runtime_verifier_source_identity:'d67f94e19000a612ee4f1099c14429d8a8b4bee1',
 source_inventory:'ALL_TRACKED_GIT_OBJECTS',new_signatures:0,new_tables:0,new_grants:0,root_private_preserved:232,planned_complete_private_union:258,tables_preserved:105,service_signatures_preserved:142,
 actual_sql_execution:false,actual_native_financial_acceptance:false,actual_remote_http_ui_phone:false,actual_activation:false,lane_database_calls:0,
 next_separate_interfaces:['Literal native SECURITY active_admin_activation collector/owner proof; no renamed lease OPEN','Lawful OFF automatic independent-session assignment/effective-rule qualification after all-consumer census'],
 root_integration:'Replace only four exact Q45 predecessor artifacts during root composition. Preserve frozen45/pin12/prior156 and current root runtime. No new operational certificate/readiness evidence is supplied.'};
fs.writeFileSync(path.join(out,'REVIEW_MANIFEST.json'),JSON.stringify(record,null,2)+'\n');
console.log(JSON.stringify({files:files.length+1,manifest_sha256:sha(fs.readFileSync(path.join(out,'REVIEW_MANIFEST.json'))),source_tests_passed:8,
 predecessor_manifests:frozen.length,verified_declared_inputs:frozen.reduce((n,p)=>n+p.verified_declared_inputs,0),source_only:true}));
