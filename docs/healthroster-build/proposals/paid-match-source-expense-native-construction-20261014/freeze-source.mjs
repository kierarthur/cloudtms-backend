import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync,spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),rel=path.relative(root,dir).replaceAll('\\','/');
const sha=b=>createHash('sha256').update(b).digest('hex'),read=f=>fs.readFileSync(path.join(dir,f)),write=(f,o)=>fs.writeFileSync(path.join(dir,f),JSON.stringify(o,null,2)+'\n');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const writer='docs/healthroster-build/proposals/paid-shift-match-writer-none-construction-20261014',own='b0e5db406b904589bb6aaa9e7531a75ff9674743';
const manifestRaw=fs.readFileSync(path.join(root,writer,'FROZEN_ARTIFACT_MANIFEST.json')),manifest=JSON.parse(manifestRaw);
assert.equal(sha(manifestRaw),'41b09a6b804db5604003c632dd4ca0e506ba44db4b537dc68fc7e3b4c0fd2453');
for(const a of manifest.artifacts){const file=writer+'/'+a.file,b=fs.readFileSync(path.join(root,file));assert.equal(b.length,a.bytes);assert.equal(sha(b),a.sha256);assert.deepEqual(b,execFileSync('git',['show',own+':'+file],{cwd:root,maxBuffer:2e6}));}
const native=JSON.parse(read('NATIVE_VALUES_REVERSE.json')),successor=JSON.parse(read('EXPENSE_WRITER_REVERSES.json'));
const source=execFileSync('git',['show',native.commit+':'+native.file],{cwd:root,maxBuffer:3e6});assert.equal(sha(source),native.input_sha256);
const schemaFile='supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql';
const bindingSchema='supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql';
const inputs=[{commit:native.commit,file:native.file,bytes:source.length,sha256:sha(source)},...['supabase/migrations/30092026_2129_weekly_source_office_resolution.sql',schemaFile,bindingSchema,native.exact_case_census.path].map(file=>{
 const b=execFileSync('git',['show',native.commit+':'+file],{cwd:root,maxBuffer:8e6});return{commit:native.commit,file,bytes:b.length,sha256:sha(b)};
})];
write('CURRENT_SOURCE_INPUT_PINS.json',{source_only:true,inputs,writer58_manifest_sha256:sha(manifestRaw),writer58_saved_committed_preserved:true,
 corrected_case_census:native.exact_case_census,effective_body_closure:false,native_catalogue_claim:false});
write('SELECTED_SOURCE_RECIPE.json',{source_only:true,independent_review:'PENDING',writer_input_commit:own,current_SQL_input_commit:native.commit,
 preceding_writer58_manifest_sha256:sha(manifestRaw),one_time_source_order:[writer+'/one-time/00_paid_shift_choice_fields.sqlinc','one-time/08_complete_snapshot_origin_constraints.sqlinc'],
 excluded_alternative_origin_leaf:writer+'/one-time/01_invoice_snapshot_origins.sqlinc',previous_origin_leaf_already_installed_requires_separate_upgrade_disposition:true,
 repeatable_selected_order:['forward/01_native_expense_invoice_values.sqlinc','forward/02_complete_native_expense_materialiser.sqlinc','forward/05_complete_hours_companion.sqlinc','forward/04_expense_invoice_origin_assert.sqlinc','forward/06_complete_origin_dispatch.sqlinc','forward/03_expense_invoice_companion.sqlinc','forward/07_complete_fin4_expense.sqlinc','forward/09_complete_invoice_admission.sqlinc'],
 unchanged_writer58_leaves_retained_except_selected_complete_successors:successor.records.map(r=>({excluded:r.input,selected:r.output,sha256:r.after_sha256})),
 new_private_signatures:['private.weekly_source_fixed_expense_invoice_values_v1(uuid)','private.weekly_source_paid_match_expense_invoice_insert_v1(uuid,uuid)','private.weekly_source_paid_match_expense_invoice_origin_assert_v1(uuid)'],
 new_private_acl:'logical postgres; PUBLIC/anon/authenticated/service_role revoked; owner only',new_public_signatures:[],new_business_tables:0,new_columns:0,
 existing_public_invoice_admit_ACL:'logical postgres; PUBLIC/anon/authenticated revoked; service_role only',
 native_Source_expense_calculation_copied_once:true,duplicate_money_classifier:false,payable_expense_generation_created_for_NONE:false,
 source_expense_invoice_companion_construction:true,complete_prepared_pay_root_census:false,Office_OFF_NHSP_CorrectFinal_Legacy_Report_complete:false,
 canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
function run(args){const r=spawnSync(process.execPath,args,{cwd:root,encoding:'utf8',maxBuffer:2e6});assert.equal(r.status,0,r.stdout+r.stderr);return r.stdout;}
const out=run(['--test',rel+'/expense-source.test.mjs']);assert.match(out,/tests 12/);assert.match(out,/pass 12/);assert.match(out,/fail 0/);
const files=[];function walk(prefix=''){for(const e of fs.readdirSync(path.join(dir,prefix),{withFileTypes:true})){const f=prefix?prefix+'/'+e.name:e.name;if(e.isDirectory())walk(f);else files.push(f);}}walk();
const scripts=files.filter(f=>f.endsWith('.mjs'));for(const f of scripts)run(['--check',rel+'/'+f]);
const rx=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/\bgh[pousr]_[A-Za-z0-9]{20,}\b/,/\bAKIA[0-9A-Z]{16}\b/,/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b/],findings=[];
const whitespace=[];for(const f of files){const s=read(f).toString();if(rx.some(r=>r.test(s)))findings.push(f);s.split(/\r?\n/).forEach((line,i)=>{if(/[ \t]+$/.test(line))whitespace.push({file:f,line:i+1});});if(/(?:\r?\n){2,}$/.test(s))whitespace.push({file:f,kind:'EXACT_SOURCE_EOF_BLANK_LINES'});}
assert.equal(findings.length,0);
write('SOURCE_CHECK_RESULT.json',{source_only:true,independent_review:'PENDING',effect_free_cases:12,passed:12,failed:0,syntax_passed:scripts.length,
 native_Source_full_owner_reversals:1,whole_writer58_successor_reversals:5,total_hunks:native.hunks.length+successor.records.reduce((n,r)=>n+r.hunks.length,0),
 extracted_native_block_bytes:native.extracted_native_block_bytes,extracted_native_block_sha256:native.extracted_native_block_sha256,
 writer58_artifacts_preserved:manifest.artifacts.length,source_input_blobs:inputs.length,private_additions:3,public_additions:0,
 new_stores_or_columns:0,new_payable_expense_generations_for_NONE:0,credential_pattern_findings:0,
 whitespace_disposition:whitespace.length?'EXCEPTIONS PRESERVED; NO BLANKET PASS':'PASS',whitespace_exceptions:whitespace,
 SQL_compiled:false,native_proof:false,canonical_mounted:false,real_database_or_API_calls:0,scratch_created:false,cleanup_retried:false,
 complete_prepared_pay_root_census:false,full_primary_complete:false,remaining_gates:'SOURCE_BOUNDARY_AND_REMAINING_CODE.md'});
files.push('SOURCE_CHECK_RESULT.json');const artifacts=[...new Set(files)].sort().map(file=>{const b=read(file);return{file,bytes:b.length,sha256:sha(b)};});
write('FROZEN_ARTIFACT_MANIFEST.json',{schema_version:'PAID_SAME_NONE_SOURCE_FIXED_EXPENSE_NATIVE_CONSTRUCTION_FREEZE_V1',source_only:true,
 independent_review:'PENDING',canonical_mounted:false,SQL_compiled:false,native_proof:false,full_primary_complete:false,artifact_count:artifacts.length,artifacts});
console.log(JSON.stringify({artifacts:artifacts.length,manifest_sha256:sha(read('FROZEN_ARTIFACT_MANIFEST.json')),check_sha256:sha(read('SOURCE_CHECK_RESULT.json')),
 recipe_sha256:sha(read('SELECTED_SOURCE_RECIPE.json')),cases:12,syntax_passed:scripts.length,new_private:3,source_only:true,native_proof:false}));
