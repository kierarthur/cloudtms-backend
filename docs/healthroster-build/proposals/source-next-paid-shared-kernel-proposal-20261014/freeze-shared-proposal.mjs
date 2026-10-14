import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),current='5db7abee7925fffbeaf7c4e885cce809980fa079';
const sha=b=>createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(path.join(dir,f));
const walk=base=>fs.readdirSync(base,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(base,e.name)):[path.relative(dir,path.join(base,e.name)).replaceAll('\\','/')]).sort();
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')),'Never overwrite frozen proposal');
const proof=JSON.parse(read('COMPLETE_SHARED_EXTRACTION_AND_REVERSES.json'));
assert.equal(proof.shared_banking_owner_change_approved,false);assert.equal(proof.input.commit,current);
const index=JSON.parse(fs.readFileSync(path.join(dir,'../current-final-classification-union-20261014/CURRENT_FINAL_DECLARATION_OWNERS.json'),'utf8'));
const nativeNames=['public._pay_timesheet_rotation_scope','private.weekly_source_query_require_service_v1','private.weekly_source_office_authority_v1',
 'private.weekly_source_office_next_owner_v1','private.weekly_source_entitlement_component_id_v1','private.bpay_next_source_origin_state_v1'];
const nativePins=[],cache=new Map();for(const name of nativeNames){const definitions=index.functions.filter(x=>x.name===name);assert.ok(definitions.length,name);
 for(const pin of definitions){if(!cache.has(pin.file))cache.set(pin.file,execFileSync('git',['--no-replace-objects','show',current+':'+pin.file],{cwd:root,maxBuffer:32*1024*1024}));
 const bytes=cache.get(pin.file),e=functionEvents(bytes.toString('utf8')).events.filter(x=>x.kind==='CREATE'&&x.id===pin.id).at(-1);assert.ok(e,pin.id);assert.equal(sha(e.definition),pin.definition_sha256);
 nativePins.push({signature:pin.id,commit:current,file:pin.file,input_bytes:bytes.length,input_sha256:sha(bytes),definition_sha256:sha(e.definition),body_sha256:sha(e.body)});}}
fs.writeFileSync(path.join(dir,'CURRENT_NATIVE_CALL_ONLY_DEPENDENCY_PINS.json'),JSON.stringify({proposal_only:true,source_only:true,current,complete_current_native_reader:proof.input,selected_native_owner_pins:nativePins,source_context_dependency:proof.source_context,full_transitive_physical_catalogue_or_final_seal_claim:false,native_proof:false},null,2)+'\n');
const tests=execFileSync(process.execPath,['--test',path.join(dir,'shared-proposal-source.test.mjs')],{cwd:root,maxBuffer:2*1024*1024,encoding:'utf8',stdio:['ignore','pipe','pipe']});
assert.match(tests,/tests 7/);assert.match(tests,/pass 7/);assert.match(tests,/fail 0/);
const syntax=['build-shared-proposal.mjs','shared-proposal-source.test.mjs','freeze-shared-proposal.mjs'];for(const f of syntax)execFileSync(process.execPath,['--check',path.join(dir,f)],{cwd:root,maxBuffer:1024*1024,stdio:'pipe'});
const files=walk(dir),rules=[['PRIVATE_KEY',/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/],['JWT_LITERAL',/\beyJ[A-Za-z0-9_-]{25,}\.[A-Za-z0-9_-]{25,}\.[A-Za-z0-9_-]{20,}\b/],['GITHUB_TOKEN',/\b(?:gh[pousr]_[A-Za-z0-9]{25,}|github_pat_[A-Za-z0-9_]{25,})\b/]];
const findings=[];for(const file of files)for(const [rule,re]of rules)if(re.test(read(file).toString('utf8')))findings.push({file,rule});assert.equal(findings.length,0,'File/rule only credential results');
const checks={schema_version:'SHARED_OWNER_NATIVE_PAID_KERNEL_PROPOSAL_SOURCE_CHECK_V1',proposal_only:true,source_only:true,current,
 tests:{count:7,pass:7,fail:0,kind:'Inspected lexical/full reverse source tests only, not functional SQL or native financial/security proof',output_sha256:sha(tests)},inert_syntax_files:syntax,
 complete_unique_reversals:3,financial_suffix_and_three_branches_byte_identical:true,public_prefix_both_admin_checks_byte_identical:true,public_request_response_fields_cursor_acl_unchanged_at_source:true,
 proposed_new_private_signatures:[proof.kernel.signature,proof.source_purpose.signature],new_table_or_service_public_signature_count:0,
 shared_banking_owner_change_proposed:true,shared_banking_owner_change_approved:false,canonical_source_modified:false,installed_or_mounted:false,
 credential_scan:{files:files.length,rules:rules.map(x=>x[0]),findings:[]},sql_compiled:false,sql_executed:false,native_proof:false,
 native_permission_positive_negative_error_order_current_scope_snapshot_component_all_pages_money_cursor_role_catalogue:'HOLD',
 default_missing_exclusion_accept_overlap_prepared_and_agency_paid_amendment_authority:'UNCHANGED_SOURCE_ENGINEERING_GAPS',final_literal_classification_and_full_verifier_body_source_contract_tool_catalogue_installed_seal:'REQUIRED_AFTER_OWNER_APPROVAL'};
fs.writeFileSync(path.join(dir,'SOURCE_CHECK_RESULT.json'),JSON.stringify(checks,null,2)+'\n');
const artifacts=walk(dir).map(file=>{const b=read(file);return {file,bytes:b.length,sha256:sha(b)};});
fs.writeFileSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json'),JSON.stringify({schema_version:'SHARED_OWNER_PAID_KERNEL_REVIEW_PROPOSAL_SOURCE_FREEZE_V1',proposal_only:true,source_only:true,current,shared_banking_owner_change_proposed:true,shared_banking_owner_change_approved:false,canonical_source_modified:false,mounted:false,native_proof:false,artifact_count:artifacts.length,artifacts},null,2)+'\n');
console.log(JSON.stringify({proposal_only:true,shared_owner_change_not_approved:true,canonical_modified:false,artifacts:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),checks_sha256:sha(read('SOURCE_CHECK_RESULT.json')),full_reversals:3,native_proof:false}));
