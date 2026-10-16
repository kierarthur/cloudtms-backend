// Bounded source/grammar verification only. No connection, native adapter or deployment.
import {spawnSync,execFileSync} from 'node:child_process';
import {readFileSync,writeFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex');
const run=(program,args)=>{const r=spawnSync(program,args,{cwd:root,encoding:'utf8',maxBuffer:3e6});if(r.error||r.status!==0)throw Error(`${program} ${args.join(' ')} failed: ${r.error||r.stderr||r.stdout}`);return r.stdout;};
const summary=(output)=>{const m=output.match(/^# tests (\d+)\r?\n# suites \d+\r?\n# pass (\d+)\r?\n# fail (\d+)/m);if(!m||m[3]!=='0')throw Error('Missing clean TAP summary');return {tests:+m[1],passed:+m[2],failed:+m[3]};};
const relative=path.relative(root,base).replaceAll('\\','/');
const native=summary(run(process.execPath,['--test','--test-reporter=tap',relative+'/tests/source.test.mjs']));
const contact='docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/tests';
const contactFiles=readdirSync(path.join(root,contact)).filter(f=>f.endsWith('.test.mjs')).sort().map(f=>contact+'/'+f);
const preserved=summary(run(process.execPath,['--test','--test-reporter=tap',...contactFiles]));
run('python',[relative+'/tests/grammar.py']);
const grammar=JSON.parse(readFileSync(path.join(base,'tests/GRAMMAR_RESULT.json'),'utf8'));
for(const f of ['build.mjs','check.mjs','tests/source.test.mjs'])run(process.execPath,['--check',relative+'/'+f]);
const frozen='e4661d28219b85c66b854d99c6522b55aae33139';
const frozenFiles=['forward/50_manager_selection_capture.sqlinc','forward/51_native_initial_manager_owner.sqlinc'];
const frozenContact=frozenFiles.map(file=>{
 const p='docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/'+file;
 const saved=readFileSync(path.join(root,p)),git=execFileSync('git',['show',`${frozen}:${p}`],{cwd:root,maxBuffer:8e6});
 if(!saved.equals(git))throw Error('Frozen contact changed '+p);return {commit:frozen,path:p,sha256:sha(saved)};
});
const reviews={
 'reviews/SOURCE_ROUTE_REVIEW.json':'4866959074c07153812932280804465e736e96211bf1ba10e4cad33cd4186649',
 'reviews/SOURCE_ROUTE_REVIEW.md':'ffe9626d6f283c46359a5e63e576f001b16682972a1450377dff650ed444a401',
 'reviews/DYNAMIC_SQL_REVIEW.json':'ec6085b5746ef5542c38c1cb4624b04e86040ba66aef3a82e45f82b0d2de631a',
 'reviews/DYNAMIC_SQL_REVIEW.md':'3cd43dfd2e3fd95d84c233f1849af3f23aedf51f329f6075a566408a4ace2822'};
for(const [file,expected]of Object.entries(reviews))if(sha(readFileSync(path.join(base,file)))!==expected)throw Error('Preserved source review changed '+file);
const checks={version:'SOURCE_PENDING_CURRENT_SOURCE_CHECKS_V1',qualification:'SOURCE_CHECKS_PASS_NATIVE_NOT_RUN',
 source_receipt_sha256:sha(readFileSync(path.join(base,'SOURCE_RECEIPT.json'))),source_tests:native,preserved_contact_source_and_mock_tests:preserved,
 syntax_files:3,grammar_result_sha256:sha(readFileSync(path.join(base,'tests/GRAMMAR_RESULT.json'))),grammar_counts:grammar.counts,
 exact_frozen_contact:frozenContact,preserved_reviews:reviews,
 repository_db_check:{status:'BASELINE_BLOCKED',error:'Unrecorded migration: supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql',repair_attempted:false},
 peer_source_review:'Invoice lane reviewed full classifier/helper/ACL, exact original-A identity and nine-key scope; no native/runtime approval claimed',
 native_qualification:'NOT_RUN',database_connections:0,deployment:false,bank_body_or_chat_changes:false,secrets_printed:false,policy_x:'UNCHANGED'};
writeFileSync(path.join(base,'CHECKS.json'),JSON.stringify(checks,null,2)+'\n');
function files(dir,prefix=''){return readdirSync(dir,{withFileTypes:true}).flatMap(entry=>{
 const p=prefix+entry.name;return entry.isDirectory()?files(path.join(dir,entry.name),p+'/'):[p];});}
const manifest={version:'SOURCE_PENDING_CURRENT_FULL_SOURCE_MANIFEST_V1',qualification:'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN',
 files:files(base).filter(f=>f!=='SOURCE_MANIFEST.json').sort().map(file=>({path:file,bytes:readFileSync(path.join(base,file)).length,sha256:sha(readFileSync(path.join(base,file)))}))};
writeFileSync(path.join(base,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({source_tests:native,preserved_contact_tests:preserved,grammar_counts:grammar.counts,
 source_receipt_sha256:checks.source_receipt_sha256,source_manifest_sha256:sha(readFileSync(path.join(base,'SOURCE_MANIFEST.json'))),native:'NOT_RUN'}));
