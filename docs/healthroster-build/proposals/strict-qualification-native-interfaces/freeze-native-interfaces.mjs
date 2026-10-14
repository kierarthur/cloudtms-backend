import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),sha=x=>createHash('sha256').update(x).digest('hex');
const read=name=>fs.readFileSync(path.join(out,name)),json=name=>JSON.parse(read(name)),save=(name,value)=>fs.writeFileSync(path.join(out,name),JSON.stringify(value,null,2)+'\n');
const frozen=[];
for(const folder of ['protected-source-qualification-operational-gate','protected-source-qualification-operational-gate-pin-forward-2206',
 'protected-source-qualification-operational-gate-pin-forward-2206-trial2','strict-qualification-first-use-inventory-forward','strict-qualification-git-path-parser-forward']){
 const file=path.resolve(out,'..',folder,'REVIEW_MANIFEST.json'),manifest=JSON.parse(fs.readFileSync(file));
 for(const row of manifest.files)assert.equal(sha(fs.readFileSync(path.resolve(own,row.file))),row.sha256,row.file);
 frozen.push({folder,files:manifest.files.length,sha256:sha(fs.readFileSync(file))});}
const files=fs.readdirSync(out).filter(file=>!['REVIEW_MANIFEST.json','SOURCE_CHECK_RESULT.json','WIP_CHECKPOINT.json','SOURCE_PIN_REVIEW.json','EFFECT_REVIEW.json'].includes(file)).sort();
for(const file of files.filter(file=>/\.(?:mjs|js)$/.test(file)))execFileSync(process.execPath,['--check',path.join(out,file)],{stdio:'pipe'});
const tests=execFileSync(process.execPath,['--test',path.join(out,'native-interface.test.mjs')],{encoding:'utf8'});
const passes=Number(tests.match(/(?:# |ℹ )pass (\d+)/)?.[1]);assert.equal(passes,21);assert.match(tests,/(?:# |ℹ )fail 0/);
const financialVectors=execFileSync(process.execPath,['--test',path.resolve(out,'../native-financial-fixtures/native-money-vectors.test.mjs'),
 path.resolve(out,'../native-financial-fixtures/capability-lineage-forward.test.mjs')],{encoding:'utf8'});
assert.match(financialVectors,/(?:# |ℹ )pass 16/);assert.match(financialVectors,/(?:# |ℹ )fail 0/);
const sqlFiles=files.filter(file=>file.endsWith('.sqlinc')),parsedSql=sqlFiles.map(file=>{
 const source=read(file).toString('utf8');assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i,file);
 const parsed=scanManagedSql(source);return {file,statements:parsed.statements.length,meta:parsed.meta.length,source_parsed_only:true};});
const union=json('INTERFACE_LITERAL_SIGNATURE_UNION.json');
for(const row of union.inputs)assert.equal(sha(read(row.file)),row.sha256,row.file);
assert.equal(union.preserved_all_275,true);assert.equal(union.distinct_private_signatures,301);assert.equal(union.additions.length,26);
const runtimes=json('RUNTIME_SIDECAR_REVERSE.json'),runtimeDestinations=new Set(runtimes.proofs.map(row=>row.destination));
const external=new Set(runtimes.external_required_scripts.map(file=>'scripts/'+file));
for(const row of runtimes.proofs){assert.equal(sha(read(row.input)),row.input_sha256);assert.equal(sha(read(row.output)),row.output_sha256);
 assert.equal(row.reverse_hunks_match_entire_module,true);
 for(const specifier of row.imports)if(!specifier.startsWith('node:')){
  const resolved=path.posix.normalize(path.posix.join(path.posix.dirname(row.destination),specifier));
  assert.ok(runtimeDestinations.has(resolved)||external.has(resolved),'EXACT_RUNTIME_IMPORT '+resolved);}}
const credentialRules=[['private-key',/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/],
 ['jwt',/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/],['github-token',/gh[pousr]_[A-Za-z0-9]{30,}/],['aws-key',/AKIA[0-9A-Z]{16}/]];
for(const file of files){assert.doesNotMatch(file,/^(?:\.env|\.dev\.vars)|auth[-_]?state|storage[-_]?state/i);
 const source=read(file).toString('utf8');for(const [name,pattern]of credentialRules)assert.doesNotMatch(source,pattern,file+':'+name);}
const current=json('NATIVE_CONSUMER_AND_EXECUTOR_CENSUS.json'),old=JSON.parse(execFileSync('git',['show','3a4f1a7:docs/healthroster-build/proposals/strict-qualification-native-interfaces/NATIVE_CONSUMER_AND_EXECUTOR_CENSUS.json'],{cwd:own,maxBuffer:8_000_000}));
const rows=c=>new Map([...c.source_executor_chain,...c.direct_effective_rule_consumers].map(row=>[row.signature,row])),before=rows(old),after=rows(current);
const changed=[...after.values()].filter(row=>before.has(row.signature)&&before.get(row.signature).definition_sha256!==row.definition_sha256);
assert.equal(changed.length,0);assert.ok([...before.keys()].every(key=>after.has(key)));
const added=[...after.values()].filter(row=>!before.has(row.signature));
assert.deepEqual(added.map(row=>row.signature),['public.weekly_exceptional_pay_action_publication_status_v1(jsonb)']);
save('SOURCE_PIN_REVIEW.json',{source_only:true,previous_consumed_commit:old.root_source_commit,current_consumed_commit:current.root_source_commit,
 entire_root_source_equivalence_claim:false,changed_consumed_definitions:changed,additional_actual_native_entry_owner:added,compared_complete_definitions:after.size,
 complete_consumed_definitions:[...after.values()].map(row=>({signature:row.signature,file:row.file,sha256:row.definition_sha256})),
 recompose_against_actual_install1_and_final_install2_required:true});
const effects=read('SOURCE_EXECUTOR_EFFECT_INPUTS.sqlinc').toString('utf8');
assert.doesNotMatch(effects,/\b(?:net\.http_[a-z_]+|http_post|http_get|dblink|send_email|mail_outbox_drain)\s*\(/i);
const dynamicSourceUnits=scanManagedSql(effects).statements.map(unit=>effects.slice(unit.start,unit.end)).filter(body=>/\bexecute\s+(?:format|[a-z_][a-z_0-9]*)/i.test(body));
save('EFFECT_REVIEW.json',{source_only:true,consumed_source_commit:current.root_source_commit,complete_effect_input_sha256:sha(effects),
 conservative_routines:current.source_executor_chain.length,literal_trigger_declarations:current.source_write_trigger_chain.length,
 physical_timesheet_write_leaves:current.physical_timesheet_write_leaves,unresolved_source_callees:current.unresolved_callees,
 unresolved_classification:{actual_original_dependency_functions:['private.weekly_source_automatic_reconcile_producer_ready_v1','private.weekly_source_bulk_producer_ready_v1','private.weekly_source_bulk_source_cas_v1'],
  conservative_source_relation_reference:'private.weekly_source_entitlement_publication_receipts'},
 no_literal_network_provider_submission_email_send_or_drain_invocation_found:true,
 dynamic_source_units:dynamicSourceUnits.map(body=>({signature:body.match(/function\s+([^\s(]+)\s*\(([^]*?)\)\s*returns/i)?.[1],sha256:sha(body)})),
 review_boundary:'Complete conservative source declarations and literal trigger graph only. Native metadata/queue writes, Banking hooks and document invalidation remain. Read-only historical provider/settlement facts are not payment execution. Dynamic SQL and actual branch/trigger behavior still need root review and real first use.',
 native_effect_acceptance:false,external_call_run:false});
const db=spawnSync(process.execPath,['scripts/cloudtms-db-release.mjs','check'],{cwd:own,encoding:'utf8'});
assert.notEqual(db.status,0);assert.match(db.stderr+db.stdout,/Unrecorded migration: supabase\/migrations\/13102026_1430_weekly_source_invoice_money_origins.sql/);
save('SOURCE_CHECK_RESULT.json',{schema_version:'STRICT_QUALIFICATION_NATIVE_INTERFACES_SOURCE_CHECK_V1',
 status:'SOURCE_PACKET_CHECKS_PASS_INTEGRATED_RELEASE_CHECK_HELD',source_tests_pass:passes,source_tests_fail:0,
 preserved_money_and_lineage_source_vectors_pass:16,preserved_money_and_lineage_source_vectors_fail:0,total_source_vectors_pass:37,
 javascript_syntax_files:files.filter(file=>/\.(?:mjs|js)$/.test(file)).length,sql_source_parser:parsedSql,
 frozen_predecessors:frozen,packet_credential_scan:'PASS_INCLUDING_DOCS_PROPOSALS_AND_SQLINC',
 runtime_import_closure:'PASS_ALL_LOCAL_OR_EXPLICIT3_ORIGINAL_EXTERNAL_MODULES',
 integrated_db_check:{status:'FAIL_PREEXISTING_UNRECORDED_MIGRATION',file:'supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql',release_lock_or_contract_changed:false},
 source_pin:current.root_source_commit,original_consumed_definition_drift:changed.length,private_union:301,owner_only_additions:26,
 independent_critic:false,postgresql_compile:false,actual_database_call:false,actual_runtime_qualification:false,actual_financial_acceptance:false,
 actual_http_ui_phone_acceptance:false,actual_next_acceptance:false,operational_assignment_changed:false,banking_action:false,secrets_printed:false});
save('WIP_CHECKPOINT.json',{schema_version:'STRICT_QUALIFICATION_NATIVE_INTERFACES_CHECKPOINT_V2',
 status:'COMPLETE_UNMOUNTED_SOURCE_REVIEW_READY_REAL_GATES_HELD',supersedes_prior_unfinished_checkpoint:true,
 source_check_result_sha256:sha(read('SOURCE_CHECK_RESULT.json')),source_pin:current.root_source_commit,
 source_test_pass:passes,private_union:301,owner_only_additions:26,source_remaining_in_this_interface_packet:[],
 remaining_real_gates:'Independent critic/root composition, integrated source/contract check, PostgreSQL/canonical/ACL/API first use, full protected rehearsal, genuine TEST historical fixture/source/signature/document, LEGACY/NEXT and HTTP/UI/phone/future activation',
 actual_database_or_runtime_acceptance:false});
const all=fs.readdirSync(out).filter(file=>file!=='REVIEW_MANIFEST.json').sort();
save('REVIEW_MANIFEST.json',{schema_version:'STRICT_QUALIFICATION_NATIVE_INTERFACES_FROZEN_SOURCE_REVIEW_V1',
 status:'SOURCE_FROZEN_FOR_CRITIC_UNMOUNTED_ALL_REAL_GATES_HELD',source_pin:current.root_source_commit,predecessors:frozen,
 files:all.map(file=>({file:path.relative(own,path.join(out,file)).replaceAll('\\','/'),bytes:read(file).length,sha256:sha(read(file))})),
 source_checks_sha256:sha(read('SOURCE_CHECK_RESULT.json')),runtime_acceptance:false,independent_critic:false});
console.log(JSON.stringify({status:'SOURCE_FROZEN_FOR_CRITIC',files:all.length,source_tests:passes,
 manifest_sha256:sha(read('REVIEW_MANIFEST.json')),source_checks_sha256:sha(read('SOURCE_CHECK_RESULT.json')),private_union:301,db_check:'HELD_PREEXISTING_MIGRATION_LOCK'}));
