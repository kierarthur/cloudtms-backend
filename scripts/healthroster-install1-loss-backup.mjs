// One-time source/evidence preservation only. No database/API/credential reads.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
const sha=b=>createHash('sha256').update(b).digest('hex');
const root=process.cwd(),base='docs/healthroster-build/install1-rehearsal';
assert.equal(execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),'37643920a1f71170fe54804a9eee324a56408769');
const writes=[];
for(const name of ['snapshot-diagnostic','background-explain','live-fields-first-use','loss-reconcile','restart-observation']){
 const file='healthroster-install1-'+name+'.mjs',bytes=fs.readFileSync(path.join(root,'.codex-tmp',file));writes.push({file:'scripts/'+file,bytes,kind:'historical read-only diagnostic source; do not replay without current route/turn gates'});
}
const folder='C:/tmp/healthroster-install1-native-20261014T085448Z-cewJAg';
const raw=fs.readFileSync(path.join(folder,'000267_RECEIPT.json')),old=JSON.parse(raw);
assert.equal(old.status,'HELD_NOT_COMPLETE');assert.equal(old.failure,'TURN_CONNECTION_CLOSED');assert.equal(old.candidatePin,'37643920a1f71170fe54804a9eee324a56408769');assert.equal(sha(raw),'249069a914c77c02c128593cc1b2147d22d076ff65bc051bd4068f95e5fbfeef');
assert.equal(old.publication,null);assert.equal(old.checkpoints.at(-1).execution,null);
assert.ok(!Object.hasOwn(old.before,'backgroundRows'));assert.ok(!Object.hasOwn(old.before,'backgroundSnapshotXip'));
writes.push({file:base+'/FULL_ATTEMPT3_ORIGINAL_HELD_RECEIPT_14102026.json',bytes:raw,kind:'exact original interrupted receipt; intermediate SQL-pass label is erroneous and is not qualification'});
writes.push({file:base+'/FULL_ATTEMPT3_ORIGINAL_SHARED_TURN_14102026.md',bytes:fs.readFileSync(path.join(folder,'SHARED_TURN.md')),kind:'exact own lost turn record; connection closed, explicit unlock not confirmed'});
const rules=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/,/gh[pousr]_[A-Za-z0-9]{30,}/,/AKIA[0-9A-Z]{16}/];
for(const w of writes){assert.ok(!fs.existsSync(w.file),w.file+' must be new');assert.ok(rules.every(re=>!re.test(w.bytes.toString())),w.file+' credential material');}
for(const w of writes)fs.writeFileSync(w.file,w.bytes);
const result={status:'INTERRUPTED_THIRD_ATTEMPT_PRESERVED_INSTALLATION_HELD',reviewedCandidate:old.candidatePin,sourceOnlyBackup:true,
 files:writes.map(({file,bytes,kind})=>({file,bytes:bytes.length,sha256:sha(bytes),purpose:kind})),
 evidenceLost:['Complete native SQL result and error collection','Rehearsal transaction and fixture-capture frames','Qualified live-background restoration proof'],
 misleadingIntermediateStatus:'Original checkpoint SQL_PASS_FINISHED_RESTORATION_UNVERIFIED has execution:null and EXECUTION_ADAPTER_FAILED. It is preserved as original history, not SQL-pass evidence. Source fix records interrupted/SQL-error status truthfully before fresh restoration.',
 confirmed:'PostgreSQL postmaster restarted at 09:07:33.240096Z; original own turn and lock are absent. Fresh ledger/catalogue/relation fingerprints match, marker absent, LEGACY:1. Only two observed business-table fingerprints differ; complete live-writer attribution remains unproved because original RAM-only row maps/capture were lost.',
 cause:'Not established. Memory maximum 3810172928 bytes and restart counter increase are observations; OOM counter remained zero.',
 replayWarning:'These historical read-only diagnostic scripts contain exact old candidate/marker/paths. The EXPLAIN script sourceOnlyExplain field is a historical label: it made a native read-only planning query, not an effect-free source test. Do not run these automatically.',
 currentAttemptWindow:'At most three full fix/retry rounds consumed; no fourth rehearsal or managed release dispatched.',
 leftovers:[{path:'C:/tmp/healthroster-install1-native-20261014T084044Z-PEoqab',retained:true,deletionAttempted:false,deletionRefused:false},{path:folder,retained:true,deletionAttempted:false,deletionRefused:false},{path:root+'/.codex-tmp',purpose:'five preserved historical diagnostic script originals; other preexisting task-private contents remain separate',retained:true,deletionAttempted:false,deletionRefused:false}],
 nativeExecution:false,installation:false,otherSessionsCancelled:false,backgroundJobsPaused:false,credentialFindings:0};
fs.writeFileSync(base+'/FULL_ATTEMPT3_BACKUP_CHECKPOINT_14102026.json',JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,files:writes.length,originalReceiptSha256:sha(raw)}));
