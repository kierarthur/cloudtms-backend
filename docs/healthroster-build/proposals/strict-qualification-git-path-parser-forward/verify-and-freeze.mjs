import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(out,'../../../..');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:repo,maxBuffer:5_000_000});
const predecessor='1c7d4d53ccc17cb269715e2e6ff902942d73e452';
const list=[['f5-window-rounding','REVIEW_MANIFEST.json'],['m1-whole-shift','REVIEW_MANIFEST.json'],['f5-window-receipt-complete','REVIEW_MANIFEST.json'],
 ['protected-source-capability','REVIEW_MANIFEST.json'],['native-financial-fixtures','NATIVE_FIXTURE_REVIEW_MANIFEST.json'],['protected-source-qualification','REVIEW_MANIFEST.json'],
 ['protected-source-qualification-operational-gate','REVIEW_MANIFEST.json'],['protected-source-qualification-operational-gate-pin-forward-2206','REVIEW_MANIFEST.json'],
 ['protected-source-qualification-operational-gate-pin-forward-2206-trial2','REVIEW_MANIFEST.json'],['strict-qualification-first-use-inventory-forward','REVIEW_MANIFEST.json']];
const frozen=list.map(([folder,name])=>{const file=`docs/healthroster-build/proposals/${folder}/${name}`,b=fs.readFileSync(path.join(repo,file));assert.deepEqual(b,git(['show',`${predecessor}:${file}`]));
 const m=JSON.parse(b),rows=m.files??m.source_artifacts??m.artifacts;for(const row of rows){const item=row.file??row.path;assert.equal(sha(fs.readFileSync(path.join(repo,item))),row.sha256,item);}
 return {file,sha256:sha(b),verified_declared_inputs:rows.length};});
const critic=fs.readFileSync(path.resolve(repo,'../../cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_BUILD_13102026/STRICT_QUALIFICATION_FORWARD_SOURCE_CRITIC.md'));
assert.equal(sha(critic),'49682eab129c98fb8f91679b8d8d68602ef287a65fc1b1b118f7c632a6a7fb80');fs.writeFileSync(path.join(out,'STRICT_QUALIFICATION_FORWARD_SOURCE_CRITIC.md'),critic);
const tap=execFileSync(process.execPath,['--test','--test-reporter=tap',path.join(out,'path-parser.test.mjs')],{cwd:repo,maxBuffer:1_000_000});assert.match(tap.toString('utf8'),/# pass 5\r?\n/);assert.match(tap.toString('utf8'),/# fail 0\r?\n/);fs.writeFileSync(path.join(out,'PATH_PARSER_TEST_RESULTS.tap'),tap);
for(const file of fs.readdirSync(out).filter(file=>/\.(mjs|js)$/.test(file)))execFileSync(process.execPath,['--check',path.join(out,file)],{cwd:repo,maxBuffer:1_000_000});
const files=fs.readdirSync(out).filter(file=>file!=='REVIEW_MANIFEST.json').sort().map(file=>{const b=fs.readFileSync(path.join(out,file));return{file:path.relative(repo,path.join(out,file)).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};});
const manifest={schema_version:'STRICT_QUALIFICATION_FATAL_GIT_PATH_REVIEW_V1',status:'FROZEN_SOURCE_CORRECTION_NATIVE_AND_OPERATIONAL_GATES_HELD',files,frozen_predecessors:frozen,
 source_tests_passed:5,source_tests_failed:0,javascript_syntax_passed:true,full_reverse_proofs:2,all_raw_records_admitted_or_refused:true,
 actual_complete_same_commit_source_vector:{commit:'cea15289e0283e3383e71f0acb8b18ce184647c2',files:3694,json_bytes:2031987,domain_sha256:'8a5d6071b8dae6e9eea50d0bffc6a7c5459139440b4738ab0da8ef20a6096279',
  before_after_proposal_native_and_dto_mapper_equal:true,proof_kind:'READONLY_COMMITTED_GIT_SOURCE_PROTOCOL_ONLY',actual_r1_r2_execution_evidence:false},
 root_private_preserved:232,planned_complete_private_union:258,tables_preserved:105,service_signatures_preserved:142,new_signatures:0,new_tables:0,new_grants:0,
 actual_sql_execution:false,actual_native_financial_acceptance:false,actual_remote_http_ui_phone:false,actual_activation:false,lane_database_calls:0,
 envelope_hold:'Complete vector SQL/fences exceed Arthur2MB. No trimming/fabrication/account borrowing. Root owner-approved full fallback/release after Banking first-install record is required for whole-vector native proof.',
 integration:'Replace only two F12 execution-tree artifacts; retain F12 setup/certificate corrections, all frozen predecessors and complete root closure. Protected writer must remeasure actual complete Git source, and every permanent change precedes actual R1.'};
fs.writeFileSync(path.join(out,'REVIEW_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');console.log(JSON.stringify({source_only:true,files:files.length+1,manifest_sha256:sha(fs.readFileSync(path.join(out,'REVIEW_MANIFEST.json'))),
 predecessor_manifests:frozen.length,verified_declared_inputs:frozen.reduce((n,row)=>n+row.verified_declared_inputs,0),source_tests_passed:5}));
