import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),office=path.resolve(process.argv[2]||''),backend=path.resolve(process.argv[3]||'');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=(base,rel)=>fs.readFileSync(path.join(base,rel));
const recipe=JSON.parse(read(dir,'CURRENT_OFFICE_READ_FULL_REVERSE.json'));
const git=(root,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:40*1024*1024});
if(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')))throw new Error('FROZEN_OFFICE_READ_DO_NOT_OVERWRITE');
const checks=[];function check(name,fn){fn();checks.push({name,status:'PASS',scope:'EFFECT_FREE_SOURCE_ONLY'});}
check('all14_predecessor_artifacts_and_manifest_unchanged',()=>{
 const prior=path.resolve(dir,'../install2-office-correction-read-consumer-20261014');
 const bytes=read(prior,'REVIEW_MANIFEST.json');assert.equal(sha(bytes),recipe.predecessor_manifest_sha256);
 const old=JSON.parse(bytes);assert.equal(old.artifact_count,14);
 for(const row of old.entries)assert.equal(sha(read(prior,row.path)),row.sha256,row.path);
});
check('four_unique_session_cache_successor_hunks_restore_exact_frozen14',()=>{
 assert.equal(recipe.successor_changed_owners.flatMap(row=>row.hunks).length,4);
 for(const row of recipe.successor_changed_owners){let after=read(path.join(dir,'forward'),row.path).toString();
  assert.equal(sha(after),row.after_sha256);for(const h of [...row.hunks].reverse()){
   assert.equal(after.split(h.after).length,2);after=after.replace(h.after,()=>h.before);}
  assert.equal(sha(after),row.before_sha256);assert.equal(after,read(path.join(dir,'predecessor'),row.path).toString());}
});
check('actual_roles_contents_and_current_session_identity_bound_without_client_authority_minting',()=>{
 const hunk=recipe.changed_current_owners.find(row=>row.path==='js/main.js').hunks[0].after;
 assert.match(hunk,/SESSION === sessionAtRead/);assert.match(hunk,/SESSION\?\.user === currentUser/);
 assert.match(hunk,/Array\.isArray\(currentUser\?\.roles\)/);assert.match(hunk,/rolesAtRead/);
 assert.match(hunk,/const view = 'NATIVE'/);assert.doesNotMatch(hunk,/roles.*includes\(['"]admin/);
 const me=read(dir,'CURRENT_ME_SOURCE.js.txt').toString();assert.match(me,/roles:/);
});
check('both_whole_current_office_owners_saved_before_and_git_f2de_exact',()=>{
  for(const row of recipe.changed_current_owners){const before=read(path.join(dir,'before'),row.path);assert.equal(sha(before),row.before_sha256);
    assert(before.equals(read(office,row.path)),row.path);assert(before.equals(git(office,'show',`${recipe.actual_source_commit}:${row.path}`)),row.path);}
});
check('two_unique_full_reverse_hunks_recover_all_current_office_bytes',()=>{
  for(const row of recipe.changed_current_owners){const after=read(path.join(dir,'forward'),row.path);assert.equal(sha(after),row.after_sha256);
    let reverse=after.toString('utf8');for(const h of [...row.hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,()=>h.before);}
    assert.equal(reverse,read(path.join(dir,'before'),row.path).toString('utf8'));}
});
check('new_module_absent_current_and_exact_generated_source',()=>{
  for(const row of recipe.new_modules){assert.equal(git(office,'ls-tree',recipe.actual_source_commit,'--',row.path).toString(),'');
    assert.equal(sha(read(path.join(dir,'forward'),row.path)),row.after_sha256);}
});
const backendModule='broker/src/timesheet-correction-office-context.mjs',backendIndex='broker/src/index.js';
const backendBytes=read(backend,backendModule),indexBytes=read(backend,backendIndex);
check('exact_current_backend_context_and_actual_me_shape_source_bound',()=>{
  assert.equal(sha(backendBytes),recipe.backend_context_module_sha256);
  assert(backendBytes.equals(git(backend,'show',`${recipe.actual_backend_mount_commit}:${backendModule}`)));
  const index=indexBytes.toString('utf8'),start=index.indexOf('async function handleMe(env, req) {');
  const end=index.indexOf('async function handleInvoiceSaveEdits',start);assert(start>=0&&end>start);
  const me=index.slice(start,end).trimEnd();assert.match(me,/select=id,email,display_name,role,email_signature_html/);
  assert.doesNotMatch(me,/is_active:|payment_authoriser:|payment_golden_key:/);
  fs.writeFileSync(path.join(dir,'CURRENT_ME_SOURCE.js.txt'),me+'\n');
  fs.writeFileSync(path.join(dir,'CURRENT_BACKEND_READ_SOURCE_PINS.json'),JSON.stringify({actual_source_pin:recipe.actual_backend_mount_commit,
    context:{path:backendModule,raw_sha256:sha(backendBytes)},current_me:{path:backendIndex,full_source_raw_sha256:sha(indexBytes),
      whole_definition_sha256:sha(me),evidence_file:'CURRENT_ME_SOURCE.js.txt'},no_native_claim:true},null,2)+'\n');
});
const syntax=['forward/js/main.js','before/js/main.js','forward/js/timesheet-correction-office-context.js',
  'prepare-session-successor.mjs','check-and-freeze-session-successor.mjs','office-correction-read.test.mjs'];
check('six_source_syntax_checks_without_running_main_classic_browser_script_semantics',()=>{
  for(const file of syntax){if(file.endsWith('.js'))new vm.Script(read(dir,file).toString('utf8'),{filename:file});
    else execFileSync(process.execPath,['--check',path.join(dir,file)],{stdio:'pipe'});}
});
const output=execFileSync(process.execPath,['--test',path.join(dir,'office-correction-read.test.mjs')],{maxBuffer:2*1024*1024}).toString();
check('32_real_module_actual_me_shape_and_exact_main_hunk_effect_free_vectors',()=>{assert.match(output,/tests 32/);assert.match(output,/pass 32/);assert.match(output,/fail 0/);});
const whitespace=[];
check('all_full_copy_trailing_whitespace_is_existing_parent_content_no_introduced',()=>{
  for(const row of recipe.changed_current_owners){
    const bad=bytes=>bytes.toString('utf8').split(/\r?\n/).filter(line=>/[\t ]+$/.test(line));
    const before=bad(read(path.join(dir,'before'),row.path)),after=bad(read(path.join(dir,'forward'),row.path));
    const pool=new Map();for(const line of before)pool.set(line,(pool.get(line)||0)+1);
    for(const line of after){assert((pool.get(line)||0)>0,'Introduced whitespace');pool.set(line,pool.get(line)-1);}
    whitespace.push({path:row.path,before_trailing_lines:before.length,after_trailing_lines:after.length,introduced:0,
      raw_whitespace_pass:false,parent_provenance_sha256:sha(JSON.stringify(before))});
  }
});
const result={schema:'OFFICE_CORRECTION_READ_SESSION_SOURCE_CHECKS_V1',checks,source_check_count:checks.length,
  unit_vector_count:32,syntax_count:syntax.length,whitespace_provenance:whitespace,native_calls:false,http_calls:false,
  browser_calls:false,source_read_seam_complete:true,full_correction_workflow_complete:false,native_seal:null};
fs.writeFileSync(path.join(dir,'SOURCE_CHECK_RESULT.json'),JSON.stringify(result,null,2)+'\n');
const entries=[];function visit(base){for(const e of fs.readdirSync(base,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){const file=path.join(base,e.name);
  if(e.isDirectory())visit(file);else if(e.isFile()){const bytes=fs.readFileSync(file),rel=path.relative(dir,file).replaceAll('\\','/');
    if(/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:sb_secret_|ghp_|github_pat_)[A-Za-z0-9_]{15,}|eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/.test(bytes.toString('utf8')))throw new Error(`CREDENTIAL_PATTERN:${rel}`);
    entries.push({path:rel,bytes:bytes.length,sha256:sha(bytes)});
  }
}}
visit(dir);
const manifest={schema:'OFFICE_CORRECTION_READ_SESSION_SOURCE_MANIFEST_V1',actual_office_input:recipe.actual_source_commit,
  backend_mount:recipe.actual_backend_mount_commit,predecessor_manifest_sha256:recipe.predecessor_manifest_sha256,artifact_count:entries.length+1,entries,source_checks_sha256:sha(read(dir,'SOURCE_CHECK_RESULT.json')),
  source_read_seam_complete:true,full_correction_workflow_complete:false,native_claims:false,native_seal:null,critic_review_pending:true};
fs.writeFileSync(path.join(dir,'REVIEW_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({artifacts:manifest.artifact_count,manifest_sha256:sha(read(dir,'REVIEW_MANIFEST.json')),
  checks_sha256:manifest.source_checks_sha256,source_checks:checks.length,unit_vectors:32,syntax:syntax.length,whitespace},null,2));
