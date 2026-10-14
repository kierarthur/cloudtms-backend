import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import assert from 'node:assert/strict';
const here = path.dirname(fileURLToPath(import.meta.url));
const own = path.resolve(here, '../../../..'), sha = b => crypto.createHash('sha256').update(b).digest('hex');
const git = args => execFileSync('git', ['-c','gc.auto=0','-c','maintenance.auto=false',...args], { cwd:own,maxBuffer:30000000 });
const read = file => fs.readFileSync(path.join(here,file));
const reverse = JSON.parse(read('CURRENT_INDEX_FULL_REVERSE.json'));
const before = git(['show',`${reverse.input_commit}:${reverse.owner}`]), after = read(`forward/${reverse.owner}`);
assert.equal(sha(before), reverse.before_sha256); assert.equal(sha(after), reverse.after_sha256);
let restored = after.toString('utf8'), reapplied = before.toString('utf8');
for (const h of reverse.hunks) { assert.equal(reapplied.split(h.original).length,2); reapplied = reapplied.replace(h.original,h.replacement); }
for (const h of [...reverse.hunks].reverse()) { assert.equal(restored.split(h.replacement).length,2); restored = restored.replace(h.replacement,h.original); }
assert.ok(Buffer.from(restored).equals(before)); assert.ok(Buffer.from(reapplied).equals(after));
for (const module of reverse.unchanged_modules) {
  assert.equal(sha(git(['show',`${reverse.input_commit}:${module.path}`])),module.sha256);
  assert.equal(sha(read(`reference/${module.path}`)),module.sha256);
}
const nativePaths = [
  'docs/healthroster-build/install2-composition/A0_CURRENT_SOURCE_COMPOSITION_14102026.json',
  'docs/healthroster-build/install2-composition/A0_CURRENT_SOURCE_CHECKS_14102026.json',
  'supabase/repeatable/14102026_0519_healthroster_a0_current_native_composition_v1.sql',
  ...['01_canonical_input_assert','02_complete_clock_scope','03_native_input_witness','04_complete_native_lock_fence',
    '05_native_creator_intents_and_triggers','06_send_family_fence','07_complete_current_approval','08_complete_current_request']
    .map(name=>`supabase/repeatable/14102026_0519_healthroster_a0_current_native_composition/${name}.sqlinc`),
  'supabase/repeatable/13102026_1649_healthroster_correction_request_read_authority_v1.sql',
  ...['context','whole','transition','cancel','draft','acl'].map(name=>`supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_${name}.sqlinc`),
  'supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql',
  'supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql',
  ...['09_01_operational_rule_projection','10_02_effective_rule_owner','18_FINAL_OPERATIONAL_CERTIFICATE','30_09_complete_controlled_setup_transport']
    .map(name=>`supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/${name}.sqlinc`),
  'supabase/repeatable/21072026_1235_32_timesheet_authorise_bulk_atomic.sql',
  'supabase/repeatable/21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql',
  'supabase/repeatable/21072026_1235_36_tsfin_load_context_batch.sql'
];
const native = nativePaths.map(file => { const bytes=git(['show',`${reverse.input_commit}:${file}`]); return {path:file,sha256:sha(bytes),bytes:bytes.length,origin:'EXACT_CURRENT_GIT_SOURCE_ONLY'}; });
const requestOwner = git(['show',`${reverse.input_commit}:${nativePaths[10]}`]).toString();
assert.ok(requestOwner.includes("v_req.correction_id:='awc:'||v_root||':'||v_req.id"));
const approval = git(['show',`${reverse.input_commit}:supabase/repeatable/14102026_0519_healthroster_a0_current_native_composition/07_complete_current_approval.sqlinc`]).toString();
assert.ok(approval.includes("'lifecycle_group_id','correction:'||v_request.correction_id"));
const moduleSource = read('forward/broker/src/timesheet-correction-office-routes.mjs').toString();
assert.ok(moduleSource.includes('`awc:${projection.root_timesheet_id}:${projection.request_id}`'));
assert.ok(moduleSource.includes('`correction:${transition.correction_id}`'));
assert.ok(!moduleSource.includes('fetch('));
const tree = new Set(git(['ls-tree','-r','--name-only',reverse.input_commit]).toString().trim().split('\n'));
const seen=new Map(), pending=['broker/src/index.js'];
while(pending.length) {
  const file=pending.pop(); if(seen.has(file))continue;
  const future=path.join(here,'forward',file), bytes=fs.existsSync(future)?fs.readFileSync(future):git(['show',`${reverse.input_commit}:${file}`]);
  const source=bytes.toString(), edges=[];
  for(const match of source.matchAll(/\b(?:import|export)\s+(?:(?:[\s\S]*?)\s+from\s+)?['"]([^'"\r\n]+)['"]/g)) {
    if(!match[1].startsWith('.'))continue;
    let target=path.posix.normalize(path.posix.join(path.posix.dirname(file),match[1]));
    if(!tree.has(target)&&!fs.existsSync(path.join(here,'forward',target))) {
      const found=['.js','.mjs','/index.js'].map(suffix=>target+suffix).find(candidate=>tree.has(candidate));
      assert.ok(found,`Unresolved literal import ${file}:${match[1]}`);target=found;
    }
    edges.push(target);pending.push(target);
  }
  seen.set(file,{path:file,sha256:sha(bytes),bytes:bytes.length,origin:fs.existsSync(future)?'REVIEWED_FORWARD':'EXACT_CURRENT_GIT',literal_imports:edges});
}
const priorFolders=['report-ab-protected-dispatch','install2-shared-runtime-forward-20261014','install2-report-canonical-integration-20261014',
  'install2-report-canonical-seal-successor-20261014','install2-report-native-btrim-successor-20261014','office-current-main-source-review-20261014'];
const prior=[];
for(const folder of priorFolders) {
  const prefix=`docs/healthroster-build/proposals/${folder}`;
  const files=git(['ls-tree','-r','--name-only','3ab25cfbcdc4cfafd82b0f1ffb0116e5886c4990','--',prefix]).toString().trim().split('\n').filter(Boolean);
  for(const file of files) { const committed=git(['show',`3ab25cfbcdc4cfafd82b0f1ffb0116e5886c4990:${file}`]);
    assert.ok(fs.readFileSync(path.join(own,file)).equals(committed),`Prior freeze changed ${file}`);prior.push({path:file,sha256:sha(committed)}); }
}
const test = execFileSync(process.execPath,['--test',path.join(here,'office-route-wiring.test.mjs')],{encoding:'utf8'});
const syntaxFiles=['prepare-a0-route-wiring.mjs','verify-a0-route-wiring.mjs','office-route-wiring.test.mjs','unit-fixtures.mjs',
  'forward/broker/src/index.js','forward/broker/src/timesheet-correction-office-routes.mjs',...reverse.unchanged_modules.map(row=>'reference/'+row.path)];
for(const file of syntaxFiles) execFileSync(process.execPath,['--check',path.join(here,file)],{encoding:'utf8'});
const results={schema_version:'A0_OFFICE_RUNTIME_SOURCE_CHECKS_V1',source_only:true,input_commit:reverse.input_commit,
  full_current_reverse:true,unique_hunks:reverse.hunks.length,current_native_source_files:native.length,
  runtime_literal_import_closure_files:seen.size,syntax_files:syntaxFiles.length,unit_tests:20,unit_pass:20,
  unit_output_sha256:sha(test),prior_publication_files_unchanged:prior.length,native_calls:0,qualification_receipts_created:0,
  native_compiled_contract:null,actual_api_ui_phone_acceptance:false};
if(process.argv.includes('--save')) {
  fs.writeFileSync(path.join(here,'NATIVE_OWNER_SOURCE_PINS.json'),JSON.stringify({input_commit:reverse.input_commit,status:'SOURCE_ONLY_NATIVE_CONTRACT_AND_CALLS_HELD',files:native},null,2)+'\n');
  fs.writeFileSync(path.join(here,'RUNTIME_IMPORT_CLOSURE.json'),JSON.stringify({input_commit:reverse.input_commit,
    classification:'LITERAL_STATIC_IMPORT_SOURCE_GRAPH_NOT_RUNTIME_EVIDENCE',files:[...seen.values()].sort((a,b)=>a.path.localeCompare(b.path))},null,2)+'\n');
  fs.writeFileSync(path.join(here,'PRESERVED_PRIOR_SOURCE.json'),JSON.stringify({publication:'3ab25cfbcdc4cfafd82b0f1ffb0116e5886c4990',files:prior},null,2)+'\n');
  fs.writeFileSync(path.join(here,'SOURCE_CHECK_RESULT.json'),JSON.stringify(results,null,2)+'\n');
}
console.log(JSON.stringify(results));
