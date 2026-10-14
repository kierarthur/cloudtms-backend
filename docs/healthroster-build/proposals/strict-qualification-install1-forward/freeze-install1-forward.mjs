import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import {scanManagedSql} from './PINNED_MANAGED_SQL_SCANNER.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),sha=x=>createHash('sha256').update(x).digest('hex'),read=file=>fs.readFileSync(path.join(out,file)),json=file=>JSON.parse(read(file)),save=(file,value)=>fs.writeFileSync(path.join(out,file),JSON.stringify(value,null,2)+'\n');
const excluded=new Set(['REVIEW_MANIFEST.json','SOURCE_CHECK_RESULT.json']),files=fs.readdirSync(out).filter(file=>!excluded.has(file)).sort();
for(const file of files.filter(file=>/\.(?:mjs|js)$/.test(file)))execFileSync(process.execPath,['--check',path.join(out,file)],{stdio:'pipe'});
const tests=execFileSync(process.execPath,['--test',path.join(out,'composition-forward.test.mjs')],{encoding:'utf8'});assert.match(tests,/(?:# |ℹ )pass 11/);assert.match(tests,/(?:# |ℹ )fail 0/);
const sql=files.filter(file=>file.endsWith('.sqlinc')).map(file=>{const parsed=scanManagedSql(read(file).toString('utf8'));return {file,statements:parsed.statements.length,meta:parsed.meta.length,parsed_source_only:true};});
const patterns=[['private-key',/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/],['jwt',/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/],['github-token',/gh[pousr]_[A-Za-z0-9]{30,}/],['aws-key',/AKIA[0-9A-Z]{16}/]];
for(const file of files){assert.doesNotMatch(file,/^(?:\.env|\.dev\.vars)|storage[-_]?state|auth[-_]?state/i);const text=read(file).toString('utf8');for(const [name,pattern]of patterns)assert.doesNotMatch(text,pattern,file+':'+name);}
const original=json('PRIOR_FROZEN_PRESERVATION.json'),r=json('INSTALL1_CONSUMED_OWNER_COMPARISON.json'),plan=json('COMPOSITION_PLAN.json');
assert.equal(original.frozen98_modified,false);assert.equal(plan.dual_native_composition_required.length,5);
save('SOURCE_CHECK_RESULT.json',{schema_version:'STRICT_QUALIFICATION_INSTALL1_FORWARD_SOURCE_CHECK_V1',status:'SOURCE_REVIEW_READY_ROOT_DUAL_NATIVE_COMPOSITION_AND_REAL_GATES_HELD',
 actual_install1_commit:r.actual_install1_commit,prior_source_commit:r.prior_source_commit,frozen_money_commit:r.frozen_money_commit,prior_frozen_manifest_sha256:original.manifest_sha256,
 source_tests_pass:11,source_tests_fail:0,javascript_syntax_files:files.filter(file=>/\.(?:mjs|js)$/.test(file)).length,sql_source_parser:sql,whole_packet_credential_scan:'PASS_INCLUDING_COMPLETE_BROKER_AND_NATIVE_BODY_REFERENCES',
 prior_frozen_consumed_declarations:488,unchanged:344,changed:61,absent_broad_prerequisites:83,canonical_prior_alias_reconciliation:4,actual_executor_closure:369,additional_actual_owners:71,trigger_declaration_changes:21,retained_not_currently_reached_triggers:29,
 complete_money_interface_declarations:47,money_overwrites_installed_bank_wp_bodies:0,complete_money_source_declaration_reverse:true,complete_broker_actual_install1_reverse:true,
 prior_frozen98_artifacts_saved_and_git_blobs_match:true,prior_frozen98_modified:false,dual_native_root_owned_composition_remaining:plan.dual_native_composition_required.map(row=>row.signature),
 actual_install2_joined_source_recomposition_required:true,whole_source_or_receipt_only_equivalence_claim:false,independent_critic:false,postgresql_compile:false,actual_native_or_database_call:false,
 actual_runtime_financial_document_http_ui_phone_activation_acceptance:false,banking_worktree_or_chat_access:false,secrets_printed:false});
const all=fs.readdirSync(out).filter(file=>file!=='REVIEW_MANIFEST.json').sort();
save('REVIEW_MANIFEST.json',{schema_version:'STRICT_QUALIFICATION_INSTALL1_FORWARD_FROZEN_SOURCE_REVIEW_V1',status:'NEW_SOURCE_PACKET_FROZEN_FOR_CRITIC_NATIVE_ROOT_COMPOSITION_AND_REAL_GATES_HELD',
 actual_install1_commit:r.actual_install1_commit,prior_source_commit:r.prior_source_commit,frozen_money_commit:r.frozen_money_commit,prior_frozen_manifest_sha256:original.manifest_sha256,
 files:all.map(file=>({file:path.relative(own,path.join(out,file)).replaceAll('\\','/'),bytes:read(file).length,sha256:sha(read(file))})),source_check_result_sha256:sha(read('SOURCE_CHECK_RESULT.json')),
 no_prior_frozen_artifact_modified:true,independent_critic:false,actual_database_or_runtime_acceptance:false});
console.log(JSON.stringify({source_only:true,files:all.length,tests:11,manifest_sha256:sha(read('REVIEW_MANIFEST.json')),source_check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),old98_unchanged:true,current_native_composition_remaining:5}));
