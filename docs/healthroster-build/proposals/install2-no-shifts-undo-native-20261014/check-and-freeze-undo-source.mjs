import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
const sourceRoot=path.resolve(process.argv[2]||'');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const proof=JSON.parse(fs.readFileSync(path.join(dir,'CURRENT_UNDO_FULL_REVERSE.json'),'utf8'));
const checks=[];function check(name,fn){fn();checks.push({name,status:'PASS',scope:'EFFECT_FREE_SOURCE'});}
const read=(base,rel)=>fs.readFileSync(path.join(base,rel));
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:sourceRoot,maxBuffer:24*1024*1024});
if(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')))throw new Error('FROZEN_UNDO_PACKET_DO_NOT_OVERWRITE');
check('eight_saved_current_native_compiler_package_inputs_equal_actual74a9_git',()=>{
  assert.equal(proof.native_source_pins.length,8);
  for(const row of proof.native_source_pins){const bytes=read(sourceRoot,row.path);assert.equal(sha(bytes),row.raw_sha256,row.path);
    assert(bytes.equals(git('show',`${proof.actual_input_commit}:${row.path}`)),row.path);}
});
check('two_complete_unique_runtime_hunks_reverse_current_owner_exactly',()=>{
  const row=proof.changed_owner;const before=read(path.join(dir,'before'),row.path);const after=read(path.join(dir,'forward'),row.path);
  assert.equal(sha(before),row.before_sha256);assert.equal(sha(after),row.after_sha256);assert(before.equals(git('show',`${proof.actual_input_commit}:${row.path}`)));
  let reversed=after.toString('utf8');for(const h of [...row.hunks].reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);}
  assert.equal(reversed,before.toString('utf8'));
});
check('all32_runtime_import_bytes_and_relative_edges_match_actual_current_or_exact_forward',()=>{
  assert.equal(proof.runtime_import_closure.length,32);
  for(const row of proof.runtime_import_closure){assert.equal(sha(read(path.join(dir,'test-mount'),row.path)),row.sha256,row.path);
    for(const edge of row.edges)assert(proof.runtime_import_closure.some(p=>p.path===edge.target),edge.target);
    if(row.path!==proof.changed_owner.path)assert.equal(sha(git('show',`${proof.actual_input_commit}:${row.path}`)),row.sha256,row.path);}
});
const syntaxFiles=[];function enumerate(base){for(const e of fs.readdirSync(base,{withFileTypes:true})){const file=path.join(base,e.name);if(e.isDirectory())enumerate(file);else if(/\.(?:js|mjs)$/.test(e.name))syntaxFiles.push(file);}}
enumerate(dir);
check('all_packet_javascript_syntax_source_only',()=>{for(const file of syntaxFiles)execFileSync(process.execPath,['--check',file],{stdio:'pipe'});});
const native=read(path.join(dir,'forward'),proof.new_native_owners[0].path).toString('utf8');
check('one_new_public_native_owner_only_exact_grants_and_no_store_or_guc',()=>{
  assert.match(native,/alter function public\.weekly_source_no_shifts_undo_atomic_v1\(jsonb\) owner to postgres/);
  assert.match(native,/revoke all on function public\.weekly_source_no_shifts_undo_atomic_v1\(jsonb\) from public,anon,authenticated,service_role/);
  assert.match(native,/grant execute on function public\.weekly_source_no_shifts_undo_atomic_v1\(jsonb\) to service_role/);
  assert.doesNotMatch(native,/create table|delete from|set_config|set role/i);
});
check('new_native_destinations_absent_and_full_generated_hashes_exact',()=>{
  for(const row of proof.new_native_owners){assert.equal(sha(read(path.join(dir,'forward'),row.path)),row.after_sha256,row.path);
    const listing=git('ls-tree',proof.actual_input_commit,'--',row.path).toString();assert.equal(listing,'',row.path);}
});
const parser=await import(pathToFileURL(path.join(sourceRoot,'scripts/cloudtms-managed-release-sql.mjs')).href);
check('actual_canonical_packaging_parser_four_opaque_native_statements',()=>{
  assert.equal(parser.scanManagedSql(native).statements.length,4);assert.equal(parser.exactAtomicBody(native).trim(),native.trim());
});
const testArgs=['--import',pathToFileURL(path.join(dir,'unit-source-loader.mjs')).href,'--test',path.join(dir,'undo-current-transport.test.mjs')];
const testOutput=execFileSync(process.execPath,testArgs,{cwd:dir,maxBuffer:2*1024*1024}).toString();
check('38_real_dispatcher_unit_and_source_vectors_no_network_or_native',()=>{
  assert.match(testOutput,/tests 38/);assert.match(testOutput,/pass 38/);assert.match(testOutput,/fail 0/);
});
const result={schema:'NO_SHIFTS_UNDO_SOURCE_CHECKS_V1',source_commit:proof.actual_input_commit,
  checks,source_check_count:checks.length,unit_vector_count:38,syntax_count:syntaxFiles.length,runtime_import_objects:32,
  native_packaging_statements:4,native_calls:false,database_calls:false,postgres_compilation:false,native_seal:null};
fs.writeFileSync(path.join(dir,'SOURCE_CHECK_RESULT.json'),JSON.stringify(result,null,2)+'\n');
const entries=[];function visit(base){for(const e of fs.readdirSync(base,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){
  const file=path.join(base,e.name);if(e.isDirectory())visit(file);else if(e.isFile()){
    const bytes=fs.readFileSync(file);const rel=path.relative(dir,file).replaceAll('\\','/');
    if(/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:sb_secret_|ghp_|github_pat_)[A-Za-z0-9_]{15,}|eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/.test(bytes.toString('utf8')))throw new Error(`CREDENTIAL_PATTERN:${rel}`);
    entries.push({path:rel,bytes:bytes.length,sha256:sha(bytes)});
  }
}}
visit(dir);
const manifest={schema:'NO_SHIFTS_UNDO_NATIVE_SOURCE_REVIEW_MANIFEST_V1',source_commit:proof.actual_input_commit,
  source_checks_sha256:sha(read(dir,'SOURCE_CHECK_RESULT.json')),artifact_count:entries.length+1,entries,native_seal:null,
  runtime_changed_owners:1,new_public_native_signatures:['public.weekly_source_no_shifts_undo_atomic_v1(jsonb)'],
  new_private_signatures:[],new_tables:[],native_claims:false,critic_review_pending:true};
fs.writeFileSync(path.join(dir,'REVIEW_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({manifest_sha256:sha(read(dir,'REVIEW_MANIFEST.json')),checks_sha256:manifest.source_checks_sha256,
  artifacts:manifest.artifact_count,source_checks:checks.length,unit_vectors:38,syntax:syntaxFiles.length,runtime_forward_sha256:proof.changed_owner.after_sha256},null,2));
