import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),root=path.resolve(repo,'../healthroster-install2-14102026');
const commit='a7bb9f54214a0c2ee539f9efb2f7444084953799',pins=[],sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const get=p=>{const b=execFileSync('git',['cat-file','blob',commit+':'+p],{cwd:repo,maxBuffer:3e6});assert.ok(b.equals(fs.readFileSync(path.join(root,p))),p);pins.push({commit,path:p,bytes:b.length,sha256:sha(b),saved_git_equal:true});return b;};
const prefix='docs/healthroster-build/proposals/a0-source-grammar-case-compilation-fix-20261015/',native='docs/healthroster-build/root-source-arbiter-native-kernel-20261015/';
const manifest=JSON.parse(get(prefix+'SOURCE_MANIFEST.json'));for(const a of manifest.artifacts){const b=get(prefix+a.file);assert.equal(b.length,a.bytes);assert.equal(sha(b),a.sha256);}
const before=get(prefix+'before/00_source_token_grammar.sqlinc'),after=get(prefix+'forward/00_source_token_grammar.sqlinc'),reverse=JSON.parse(get(prefix+'COMPLETE_CURRENT_REVERSE.json'));
assert.equal(reverse.hunks.length,1);const h=reverse.hunks[0];assert.equal(before.toString().split(h.before).length,2);assert.equal(after.toString().split(h.after).length,2);
assert.equal(after.toString().replace(h.after,h.before),before.toString());assert.equal(after.length,before.length+2);assert.equal(h.after,h.before.replace('<>case','<>(case').replace('end then','end) then'));
const original=execFileSync('git',['cat-file','blob',reverse.input_commit+':'+reverse.input_path],{cwd:repo,maxBuffer:3e6});assert.ok(original.equals(before));assert.equal(scanManagedSql(after.toString()).statements.length,3);
const trial=get(native+'TRIAL.sql'),retry=get(native+'RETRY_01.sql');assert.equal(retry.length,trial.length+2);assert.equal(retry.toString().replace(h.after,h.before),trial.toString());
assert.equal(scanManagedSql(trial.toString()).statements.length,28);assert.equal(scanManagedSql(retry.toString()).statements.length,28);
const initial=JSON.parse(get(native+'INITIAL_NATIVE_RESULT.json')),success=JSON.parse(get(native+'RETRY_01_NATIVE_RESULT.json'));get(native+'ABSENCE.sql');get(native+'SHARED_TURN_JOURNAL.txt');
assert.equal(initial.result.accepted,false);assert.equal(initial.result.fatalError.code,'42601');assert.equal(initial.result.statements.at(-1).statement,11);assert.equal(initial.result.sqlSha256,sha(trial));
assert.equal(success.result.accepted,true);assert.equal(success.result.sqlSha256,sha(retry));assert.equal(success.result.statements.length,28);
for(const receipt of [initial,success]){assert.equal(receipt.result.database,'cloudtms_test_clone');assert.equal(receipt.result.rolledBack,true);assert.equal(receipt.result.rollbackVerification.connectedFresh,true);
 assert.equal(receipt.result.rollbackVerification.database,'cloudtms_test_clone');assert.equal(receipt.result.rollbackVerification.statements.at(-1).rows[0].table_and_five_functions_absent,true);
 assert.equal(receipt.shared_turn.explicit_unlock_confirmed,true);assert.equal(receipt.shared_turn.connection_closed,true);assert.deepEqual(receipt.shared_turn.key,[20261013,1]);}
assert.equal(success.result.statements.at(-1).rows[0].actual_scalar_coverage_kernel_assertions_passed,true);assert.equal(success.result.statements.at(-1).rows[0].retained_native_intents,'5');
for(const fragment of ['HR_A0_KERNEL_RETAINED_DEDUP_FAILED','HR_A0_KERNEL_RETAINED_RANGES_MERGED','HR_A0_KERNEL_RANGE_DOMAIN_COLLISION','HR_A0_KERNEL_ORDINARY_CHANGED_RETAINED','HR_A0_KERNEL_ORDINARY_INTENT_LEAK','HR_A0_KERNEL_SCALAR_DISPATCH_FAILED','HR_A0_KERNEL_PARTIAL_INVALID_ADMISSION','HR_A0_KERNEL_KIND_WHITELIST_FAILED','HR_A0_KERNEL_TIMEOUT_NOT_RESTORED','HR_A0_KERNEL_PRIVATE_ACL_FAILED'])assert.ok(retry.includes(Buffer.from(fragment)),fragment);
for(const p of pins)assert.equal(sha(fs.readFileSync(path.join(root,p.path))),p.sha256,'after '+p.path);
fs.writeFileSync(path.join(here,'SOURCE_PINS.json'),JSON.stringify({status:'FINITE_SOURCE_FIX_AND_RECORDED_NATIVE_SCOPE_ACCEPT',reviewer_SQL_executed:false,reviewed_commit:commit,checks:8,pins},null,2)+'\n');
console.log(JSON.stringify({checks:8,source_fix_accept:true,full_owner_reverse_exact:true,trial_only_two_parentheses:true,initial_failure_preserved:true,recorded_native_28_statement_kernel_pass:true,recorded_fresh_absence:true,reviewer_SQL_executed:false,full_rehearsal:false,pins:pins.length}));
