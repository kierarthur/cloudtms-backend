import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2],old=path.resolve(here,'../cn-a0-lock-fence');assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const target='supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_request.sqlinc';
const before=fs.readFileSync(path.join(root,target));assert.equal(sha(before),'5fb1e628a66dd1a295e0b5533da5e02ff57f4e484d9ba52632bd64cc08f37675','Current request producer must be reread/reviewed');
const anchor='  -- Lock every existing invoice first, using the same authority as all credit writers.';
const replacement='  -- New missing-A0 purpose acquires canonical native Candidate/I-1 before\n  -- the existing invoice/chain/workflow/TS order. All other arms return unchanged.\n  perform private.timesheet_correction_a0_send_family_fence_v1(v_id);\n'+anchor;
assert.equal(before.toString().split(anchor).length,2);
const after=before.toString().replace(anchor,replacement);assert.equal(after.replace(replacement,anchor),before.toString());
fs.writeFileSync(path.join(here,'13102026_2153_request.complete-proposed.sqlinc'),after);
const fenceInput=fs.readFileSync(path.join(old,'13102026_2034_a0_native_lock_fence.sqlinc'));
assert.equal(sha(fenceInput),'455768aaa41bf9d949a1c4f52830938a86c1ba231f45a95c72f772fc89b58f8f');
const fa="  v_scope:=private.timesheet_correction_a0_clock_scope_v1(r.id);";
const fr="  if current_setting('transaction_isolation')<>'read committed' then\n    raise exception 'CORRECTION_A0_TRANSACTION_SNAPSHOT_UNPROVED' using errcode='55000'; end if;\n"+fa;
assert.equal(fenceInput.toString().split(fa).length,2);const fenceAfter=fenceInput.toString().replace(fa,fr);assert.equal(fenceAfter.replace(fr,fa),fenceInput.toString());
fs.writeFileSync(path.join(here,'13102026_2153_a0_native_lock_fence.complete-proposed.sqlinc'),fenceAfter);
const diff=spawnSync('git',['diff','--no-index','--',path.join(root,target),path.join(here,'13102026_2153_request.complete-proposed.sqlinc')],{encoding:'utf8',windowsHide:true});assert.equal(diff.status,1);
const lines=diff.stdout.split('\n');fs.writeFileSync(path.join(here,'REQUEST_ROOT.diff'),'--- a/'+target+'\n+++ b/'+target+'\n'+lines.slice(lines.findIndex(l=>l.startsWith('@@'))).join('\n'));
fs.writeFileSync(path.join(here,'SUCCESSOR_GUARD.json'),JSON.stringify({contract:'HEALTHROSTER_A0_CREATOR_SUCCESSORS_SOURCE_GUARD_V1',status:'UNMOUNTED_REVIEW_PROPOSAL_NATIVE_HELD',
 request:{target,beforeSha256:sha(before),afterSha256:sha(Buffer.from(after)),anchor,replacement,reverseByteExact:true},
 lockFence:{input:'../cn-a0-lock-fence/13102026_2034_a0_native_lock_fence.sqlinc',beforeSha256:sha(fenceInput),afterSha256:sha(Buffer.from(fenceAfter)),anchor:fa,replacement:fr,reverseByteExact:true},
 nativeHolds:'No ordinary RR changes. New missing-A0 SEND/atomic/visible-active writer arms explicitly refuse non-RC; old snapshot unable to see parent/request still needs exact native admission/visibility qualification. All previous holds retained.'
},null,2)+'\n');console.log(JSON.stringify({successors:2,reverseByteExact:true,requestSha256:sha(Buffer.from(after)),lockFenceSha256:sha(Buffer.from(fenceAfter))}));
