import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),dir=path.join(here,'issue-support-construction');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false);
const input='docs/healthroster-build/proposals/agency-validation-marker-override-source-successor-20261014/construction/72_complete_worker_index_with_normal_typed_validation.js';
const before=fs.readFileSync(path.join(repo,input),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');assert.equal(sha(before),'8c3c73ca57bafb1ccd3a00c4b14bb63342e77784caf16dc2dcdcc5aca660e2e1');
const output='10_complete_worker_index_with_native_source_crosscheck.js',reverse='CROSSCHECK_WORKER_FULL_REVERSE.json';
if(fs.existsSync(path.join(dir,output))){assert.ok(process.argv.includes('--refresh-unfrozen'));assert.equal(sha(fs.readFileSync(path.join(dir,output))),JSON.parse(fs.readFileSync(path.join(dir,reverse))).forward_sha256);}
const marker='  // If not HR-autoprocess weekly OR no-timesheet-required, clear HR fields';assert.equal(before.split(marker).length,2);
const after=fs.readFileSync(path.join(dir,'09_agency_crosscheck_worker.fragment.mjs'),'utf8').replace(/\r?\n/g,before.includes('\r\n')?'\r\n':'\n')+'\n'+marker;
const forward=before.replace(marker,()=>after);assert.equal(forward.split(after).length,2);assert.equal(forward.replace(after,()=>marker),before);
fs.writeFileSync(path.join(dir,output),forward);fs.writeFileSync(path.join(dir,reverse),JSON.stringify({source_only:true,native:false,input,input_sha256:sha(before),output,forward_sha256:sha(forward),hunks:[{before:marker,after}],complete_reverse:true,shared_current_bank_changes:false,independent_review_pending:true,mount_ready:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,forward_sha256:sha(forward),hunks:1,complete_reverse:true}));
