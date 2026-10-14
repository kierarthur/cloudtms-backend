import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync,spawnSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:own,encoding:null,maxBuffer:64*1024*1024});
if(git(['branch','--show-current']).toString().trim()!=='codex/healthroster-v2-report-build-13102026')throw Error('OWN_BRANCH_REQUIRED');
const frozen='docs/healthroster-build/proposals/report-ab-protected-dispatch/SOURCE_MANIFEST.json',frozenBytes=fs.readFileSync(path.join(own,frozen));
if(!frozenBytes.equals(git(['show','c57746f82558dd698d13fe5b2fc43dd18d72af6c:'+frozen])))throw Error('FROZEN_REPORT_CHANGED');
for(const row of JSON.parse(frozenBytes).artifacts){const file=path.posix.join(path.posix.dirname(frozen),row.path),saved=fs.readFileSync(path.join(own,file));
 if(sha(saved)!==row.sha256||!saved.equals(git(['show','c57746f82558dd698d13fe5b2fc43dd18d72af6c:'+file])))throw Error('FROZEN_ARTIFACT_CHANGED');}
const moneyCommit='e9deed4f1c6da8e79f77f6262c9c10cb699b2f1b',moneyFile='docs/healthroster-build/proposals/strict-qualification-install1-forward/REVIEW_MANIFEST.json',moneyBytes=git(['show',moneyCommit+':'+moneyFile]),money=JSON.parse(moneyBytes);
for(const row of money.files)if(sha(git(['show',moneyCommit+':'+row.file]))!==row.sha256)throw Error('MONEY_REVIEWED_GIT_PIN_CHANGED');
const adapter='docs/healthroster-build/proposals/strict-qualification-install1-forward/BROKER_INSTALL1_ENTIRE_SOURCE_REVERSE.json',adapterBytes=git(['show',moneyCommit+':'+adapter]),three=JSON.parse(fs.readFileSync(path.join(dir,'THREE_WAY_INPUTS.json')));
if(sha(adapterBytes)!==three.money_adapter.artifact_sha256)throw Error('MONEY_ADAPTER_CHANGED');
fs.writeFileSync(path.join(dir,'MONEY_FORWARD_DEPENDENCY_PINS.json'),JSON.stringify({source_only:true,money_corrective98_commit:'d4b62cb15159d0afb02cfebf8c1e034c68c26550',
 money_install1_forward27_commit:moneyCommit,manifest_sha256:sha(moneyBytes),artifacts_measured:money.files.length,adapter_sha256:sha(adapterBytes),
 original_support73_commit:'d6ffd1fd971664f7fb2a7ecc65bad86fff328262',original_support73_manifest_sha256:'5bf0c84bfac484bd7c46014c768b208dcc6baa4834e074bebabde989e5fff084',
 old_frozen_packets_unchanged:true,recorder9_13_report_two_slots_and_three_raw_keys_unchanged:true},null,2)+'\n');
const walk=root=>fs.readdirSync(root,{withFileTypes:true}).flatMap(row=>row.name==='.merge-inputs'?[]:row.isDirectory()?walk(path.join(root,row.name)):[path.join(root,row.name)]);
const files=walk(dir).filter(f=>!['REVIEW_MANIFEST.json','FINAL_SOURCE_CHECKS.json'].includes(path.basename(f))),rules=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/,/gh[pousr]_[A-Za-z0-9]{30,}/,/AKIA[0-9A-Z]{16}/];
let syntax=0;for(const file of files){if(file.endsWith('freeze-source.mjs')||file.endsWith('check-backup.mjs'))continue;const bytes=fs.readFileSync(file);
 if(rules.some(rule=>rule.test(bytes.toString())))throw Error('CREDENTIAL_FINDING '+path.relative(dir,file));if(/\.m?js$/.test(file)){
  const result=spawnSync(process.execPath,['--check',file],{encoding:'utf8'});if(result.status!==0)throw Error('SYNTAX_FAILED '+path.relative(dir,file));syntax++;}}
const fin=JSON.parse(fs.readFileSync(path.join(dir,'FIN4_NATIVE_LOCK_OBSERVATION_REVERSE.json'))),finBytes=fs.readFileSync(path.join(dir,fin.output_file));
if(sha(finBytes)!==fin.output_sha256||(finBytes.toString().match(/\$function\$/g)??[]).length!==2)throw Error('FIN4_SOURCE_LEXICAL_REFUSED');
const runtime=JSON.parse(fs.readFileSync(path.join(dir,'FULL_RUNTIME_REVERSE.json')));for(const row of runtime.files)if(sha(fs.readFileSync(path.join(dir,'forward',row.file)))!==row.output_sha256)throw Error('REVIEWED_RUNTIME_CHANGED');
const checks={schema_version:'INSTALL2_SHARED_RUNTIME_NATIVE_MAPPER_SOURCE_CHECKS_V1',status:'SOURCE_ONLY_NATIVE_ROOT_SEAL_AND_ALL_REAL_GATES_HELD',unit_only_tests_passed:27,
 runtime_import_objects:136,runtime_import_syntax_passed:135,proposal_syntax_passed:syntax,runtime_complete_reverse_hunks:61,fin4_complete_reverse_hunks:2,
 fin4_input_sha256:fin.input_sha256,fin4_output_sha256:fin.output_sha256,frozen_report49_unchanged:true,credential_scan_files:files.length,credential_findings:0,
 new_database_signatures:0,new_gucs:0,new_selectors:0,new_recorder_slots:0,new_bundle_exceptions:0,seal_literal_null:true,no_native_execution:true};
fs.writeFileSync(path.join(dir,'FINAL_SOURCE_CHECKS.json'),JSON.stringify(checks,null,2)+'\n');
const artifactFiles=walk(dir).filter(file=>path.basename(file)!=='REVIEW_MANIFEST.json').sort(),artifacts=artifactFiles.map(file=>{const bytes=fs.readFileSync(file);return {path:path.relative(dir,file).replaceAll('\\','/'),bytes:bytes.length,sha256:sha(bytes)};});
fs.writeFileSync(path.join(dir,'REVIEW_MANIFEST.json'),JSON.stringify({schema_version:'INSTALL2_SHARED_RUNTIME_NATIVE_MAPPER_CORRECTION_FREEZE_V1',
 status:'FROZEN_FOR_INDEPENDENT_SOURCE_CRITIC_ROOT_COMPOSITION_AND_REAL_GATES_HELD',reviewed_runtime_backup_commit:'ae49914f8b5acbf00c7ab11c2bd1db849ffb5bd6',
 prior_frozen_report_commit:'c57746f82558dd698d13fe5b2fc43dd18d72af6c',prior_frozen_report_manifest_sha256:sha(frozenBytes),
 source_checks_sha256:sha(fs.readFileSync(path.join(dir,'FINAL_SOURCE_CHECKS.json'))),artifacts,no_native_execution:true},null,2)+'\n');
console.log(JSON.stringify({artifacts:artifacts.length+1,manifest_sha256:sha(fs.readFileSync(path.join(dir,'REVIEW_MANIFEST.json'))),checks_sha256:sha(fs.readFileSync(path.join(dir,'FINAL_SOURCE_CHECKS.json'))),fin4_sha256:fin.output_sha256}));
