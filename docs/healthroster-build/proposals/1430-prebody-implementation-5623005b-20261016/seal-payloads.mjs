// Data-free payload inventory for Root's independent exact-file integration.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const dir=path.dirname(fileURLToPath(import.meta.url)),base=path.join(dir,'forward');
const hash=x=>createHash('sha256').update(x).digest('hex');
const proposal=JSON.parse(fs.readFileSync(path.join(dir,'PROPOSAL.json'))),tests=JSON.parse(fs.readFileSync(path.join(dir,'TEST_RESULTS.json')));
assert.equal(tests.status,'PASS_SOURCE_ONLY');assert.deepEqual([tests.tests,tests.passed,tests.failed],[14,14,0]);
const files=[];
function walk(at){for(const entry of fs.readdirSync(at,{withFileTypes:true})){const target=path.join(at,entry.name);if(entry.isDirectory())walk(target);else{const bytes=fs.readFileSync(target);files.push({target:path.relative(base,target).replaceAll('\\','/'),sha256:hash(bytes),bytes:bytes.length});}}}
walk(base);files.sort((a,b)=>a.target.localeCompare(b.target));assert.equal(files.length,17);
for(const row of proposal.pins)assert.equal(files.find(x=>x.target===row.file)?.sha256,row.afterSha256);
for(const row of proposal.preservation)assert.equal(files.find(x=>x.target===row.target)?.sha256,row.sha256);
const evidence=files.find(x=>x.target.endsWith('F23_PHYSICAL_PREDECESSOR_PEER_REVIEW_20261016/REVIEW.json'));
assert.equal(evidence.sha256,'10573f03cf4d6667627efe08d9e614a8a71d18d77dab9b70f259bf006a688799');
const result={version:'HEALTH_1430_PREBODY_FORWARD_PAYLOADS_V1',status:'SOURCE_ONLY_ROOT_REVIEW_AND_NATIVE_REQUIRED',rootCommit:proposal.rootCommit,
 proposalSha256:hash(fs.readFileSync(path.join(dir,'PROPOSAL.json'))),testResultsSha256:hash(fs.readFileSync(path.join(dir,'TEST_RESULTS.json'))),
 preservationCount:10,completeForwardFiles:files.length,files,native:'NOT_RUN',rootEdited:false};
fs.writeFileSync(path.join(dir,'PAYLOADS.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({completeForwardFiles:files.length,preservationCount:10,sourceTestsPassed:14,native:'NOT_RUN'}));
