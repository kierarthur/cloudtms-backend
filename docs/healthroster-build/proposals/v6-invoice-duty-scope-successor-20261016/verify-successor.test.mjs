import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const sha=value=>createHash('sha256').update(value).digest('hex');
const m=JSON.parse(fs.readFileSync(path.join(here,'DUTY_SUCCESSOR_MANIFEST.json')));
const successor=fs.readFileSync(path.join(here,m.successor.path),'utf8');
const result=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show',`${m.original.commit}:${m.original.path}`],{cwd:repo,maxBuffer:100_000});
assert.equal(result.status,0);const original=result.stdout.toString('utf8').replaceAll('\r\n','\n');
test('complete Invoice duty changes exactly one scope invocation and reverses to immutable c86',()=>{
 assert.equal(sha(result.stdout),m.original.rawSha256);assert.equal(sha(original),m.original.canonicalLfSha256);
 assert.equal(successor.split(m.exactOnlyDelta.after).length,2);assert(!successor.includes(m.exactOnlyDelta.before));
 assert.equal(successor.replace(m.exactOnlyDelta.after,m.exactOnlyDelta.before),original);
 assert.equal(sha(successor),m.successor.canonicalLfSha256);assert.equal(sha(successor.trim()),m.successor.completeStatementSha256);
 assert(!successor.includes('\r'));assert.equal(Buffer.byteLength(successor),m.successor.bytes);
 assert.equal(m.original.headerSha256,m.successor.headerSha256);
});
test('immutable original-A scope retains exact native nine-key ABI and closed source dependency',()=>{
 assert.deepEqual(m.sourceHelper.scopeKeys,['root_timesheet_id','family_booking_id','target_family_id','candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']);
 assert.equal(m.sourceHelper.exactNativeReturnSha256,'340323d39f855d0149327128d04855e090507fcbc5aace9789e0a32d8a6ae6ef');
 assert.match(m.sourceHelper.commit,/^[0-9a-f]{40}$/);assert.match(m.sourceHelper.aclPath,/forward\/99_acl\.sqlinc$/);
 assert.equal(m.native.postgres17,'NOT_RUN');assert.equal(m.native.rollback,'NOT_RUN');assert.equal(m.noDatabaseOrDeployment,true);
});
test('all successor package artifact hashes are exact',()=>{
 const q=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json')));
 for(const [file,hash] of Object.entries(q.files))assert.equal(sha(fs.readFileSync(path.join(here,file))),hash,file);
});
