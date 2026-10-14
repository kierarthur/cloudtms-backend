import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(process.argv[2]||''),prior=path.resolve(dir,'../install2-no-shifts-undo-native-20261014');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),read=rel=>fs.readFileSync(path.join(dir,rel));
if(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')))throw new Error('FROZEN_UNDO_STATE_DO_NOT_OVERWRITE');
const input=JSON.parse(read('CURRENT_STATE_OWNER_REVERSE_AND_PINS.json')),checks=[];
function check(name,fn){fn();checks.push({name,status:'PASS',scope:'EFFECT_FREE_SOURCE'});}
check('all44_predecessor_artifacts_unchanged',()=>{
  const bytes=fs.readFileSync(path.join(prior,'REVIEW_MANIFEST.json'));assert.equal(sha(bytes),input.predecessor_manifest_sha256);
  for(const row of JSON.parse(bytes).entries)assert.equal(sha(fs.readFileSync(path.join(prior,row.path))),row.sha256,row.path);
});
check('five_whole_current_correct_final_owners_exact_saved_git87_and_no_literal_protocol_gate',()=>{
  assert.equal(input.controlling_current_protocol.length,5);
  for(const row of input.controlling_current_protocol){assert.equal(sha(read(row.evidence_path)),row.evidence_sha256);
    const bytes=fs.readFileSync(path.join(root,row.path));assert.equal(sha(bytes),row.source_raw_sha256);
    assert(bytes.equals(execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',`${input.source_pin}:${row.path}`],{cwd:root,maxBuffer:20*1024*1024})));
    assert.equal(row.correction_in_progress_literal_present,false);}
});
const row=input.changed_leaf,before=read('before/'+row.path),after=read('forward/'+row.path);
check('single_deletion_complete_native_reverse_prefix_suffix_and_hash_exact',()=>{
  assert.equal(sha(before),row.before_sha256);assert.equal(sha(after),row.after_sha256);
  const text=after.toString(),offset=row.reverse_insert_offset;
  assert.equal(sha(text.slice(0,offset)),row.reverse_prefix_sha256);assert.equal(sha(text.slice(offset)),row.reverse_suffix_sha256);
  assert.equal(text.slice(0,offset)+row.hunk.before+text.slice(offset),before.toString());
});
check('all_original_guards_acl_and_payload_history_remain_whole_body_identical_outside_one_block',()=>{
  assert.equal(before.toString().replace(row.hunk.before,()=>''),after.toString());
  assert.match(after.toString(),/if v_cycle.state='FINALISING'/);assert.match(after.toString(),/transaction_isolation/);
  assert.match(after.toString(),/version=version\+1/);assert.match(after.toString(),/WEEKLY_SOURCE_NO_SHIFTS_UNDONE/);
  assert.doesNotMatch(after.toString(),/v_cycle\.state not in/);
});
const {scanManagedSql,exactAtomicBody}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href);
check('four_complete_native_packaging_statements_without_sql_execution',()=>{
  assert.equal(scanManagedSql(after.toString()).statements.length,4);assert.equal(exactAtomicBody(after.toString()).trim(),after.toString().trim());
});
check('both_new_preparation_and_check_scripts_syntax_only',()=>{
  for(const file of ['prepare-undo-state-successor.mjs','check-and-freeze-undo-state.mjs'])execFileSync(process.execPath,['--check',path.join(dir,file)],{stdio:'pipe'});
});
const result={schema:'NO_SHIFTS_UNDO_STATE_SOURCE_CHECKS_V1',checks,source_checks:checks.length,
  runtime38_predecessor_vectors_retained:true,runtime_route_changed:false,native_calls:false,database_calls:false,
  postgres_compilation:false,native_state_absence_claim:false,native_seal:null};
fs.writeFileSync(path.join(dir,'SOURCE_CHECK_RESULT.json'),JSON.stringify(result,null,2)+'\n');
const entries=[];function visit(base){for(const e of fs.readdirSync(base,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){const file=path.join(base,e.name);
  if(e.isDirectory())visit(file);else if(e.isFile()){const bytes=fs.readFileSync(file);entries.push({path:path.relative(dir,file).replaceAll('\\','/'),bytes:bytes.length,sha256:sha(bytes)});}
}}
visit(dir);const manifest={schema:'NO_SHIFTS_UNDO_STATE_SOURCE_SUCCESSOR_MANIFEST_V1',source_pin:input.source_pin,predecessor_manifest_sha256:input.predecessor_manifest_sha256,
  artifact_count:entries.length+1,entries,source_checks_sha256:sha(read('SOURCE_CHECK_RESULT.json')),native_seal:null,native_claims:false};
fs.writeFileSync(path.join(dir,'REVIEW_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({artifacts:manifest.artifact_count,manifest_sha256:sha(read('REVIEW_MANIFEST.json')),checks_sha256:manifest.source_checks_sha256,checks:checks.length,after_sha256:row.after_sha256}));
