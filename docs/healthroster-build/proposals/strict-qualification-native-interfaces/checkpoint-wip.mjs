import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),root=path.resolve(own,'../healthroster-v2-backend-13102026');
const sha=b=>createHash('sha256').update(b).digest('hex');
const files=fs.readdirSync(out).filter(file=>file!=='WIP_CHECKPOINT.json').sort();
for(const name of files.filter(file=>file.endsWith('.mjs')))execFileSync(process.execPath,['--check',path.join(out,name)],{stdio:'pipe'});
const frozen=[];for(const folder of ['protected-source-qualification-operational-gate','protected-source-qualification-operational-gate-pin-forward-2206',
 'protected-source-qualification-operational-gate-pin-forward-2206-trial2','strict-qualification-first-use-inventory-forward','strict-qualification-git-path-parser-forward']){
 const manifest=JSON.parse(fs.readFileSync(path.resolve(out,'..',folder,'REVIEW_MANIFEST.json')));
 for(const row of manifest.files){const b=fs.readFileSync(path.resolve(own,row.file));assert.equal(sha(b),row.sha256,row.file);}
 frozen.push({folder,files:manifest.files.length,sha256:sha(fs.readFileSync(path.resolve(out,'..',folder,'REVIEW_MANIFEST.json')))});
}
const tests=execFileSync(process.execPath,['--test',path.join(out,'native-interface.test.mjs')],{encoding:'utf8'});
assert.match(tests,/pass 7/);assert.match(tests,/fail 0/);
const result={schema_version:'STRICT_QUALIFICATION_NATIVE_INTERFACES_WIP_CHECKPOINT_V1',status:'REVIEWED_UNMOUNTED_WIP_NOT_FROZEN_NOT_INTEGRATION_READY',
 source_tests_pass:7,source_tests_fail:0,javascript_syntax_pass:files.filter(file=>file.endsWith('.mjs')).length,
 preserved_frozen_predecessors:frozen,
 runtime_source_pin:'ab26a5ff22200be037d215b1100c969d6c0bae73',latest_root_metadata_not_an_equivalence_claim:'14fc8a524c5568de38112dd784b198495361fb22',
 files:files.map(file=>{const b=fs.readFileSync(path.join(out,file));return {file:path.relative(own,path.join(out,file)).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};}),
 required_remaining_source:['Explicit historical fixture/future activation certificate binding','OFF closure and complete original-census probe bridge',
  'Protected recorder Git/raw-artifact remeasurement controller','Complete literal ACL/effect closure and current-root metadata forward',
  'Remaining native journey recipes/negative vectors and independent source review'],
 actual_database_call:false,actual_runtime_qualification:false,actual_financial_acceptance:false,actual_http_ui_phone_acceptance:false,
 actual_next_acceptance:false,operational_assignment_changed:false,banking_action:false};
fs.writeFileSync(path.join(out,'WIP_CHECKPOINT.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,files:result.files.length,source_tests_pass:result.source_tests_pass,syntax_pass:result.javascript_syntax_pass,
 frozen_predecessor_files_preserved:frozen.reduce((n,row)=>n+row.files,0),sha256:sha(fs.readFileSync(path.join(out,'WIP_CHECKPOINT.json')))}));
