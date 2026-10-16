// Execute source-only tests through the complete proposed compiler/harness.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../../../healthroster-install2-14102026');
const hash=x=>createHash('sha256').update(x).digest('hex'),proposal=JSON.parse(fs.readFileSync(path.join(dir,'PROPOSAL.json')));
for(const pin of proposal.pins)assert.equal(hash(fs.readFileSync(path.join(root,pin.file),'utf8').replaceAll('\r\n','\n')),pin.beforeSha256,'Root changed: '+pin.file);
const result=spawnSync(process.execPath,['--no-warnings','--experimental-loader',pathToFileURL(path.join(dir,'proposal-loader.mjs')).href,'--test','--test-reporter=spec',path.join(dir,'forward/tests/healthroster-money-predecessor-boundary.test.mjs')],
 {cwd:dir,encoding:'utf8',windowsHide:true,maxBuffer:1024*1024,env:{...process.env,HEALTHROSTER_PROPOSAL_SOURCE_ROOT:root}});
process.stdout.write(result.stdout);process.stderr.write(result.stderr);
const proof={version:'HEALTH_1430_PREBODY_SOURCE_TESTS_V1',status:result.status===0?'PASS_SOURCE_ONLY':'FAIL_SOURCE_ONLY',rootCommit:proposal.rootCommit,
 tests:Number(result.stdout.match(/tests (\d+)/)?.[1]??0),passed:Number(result.stdout.match(/pass (\d+)/)?.[1]??0),failed:Number(result.stdout.match(/fail (\d+)/)?.[1]??0),
 native:'NOT_RUN',databaseCalls:0,rootEdits:0,dockerResources:0,releaseOrDeployment:0,temporarySourceFixtures:'Created only inside exact task-prefixed OS temp directories and removed by tests'};
for(const pin of proposal.pins)assert.equal(hash(fs.readFileSync(path.join(root,pin.file),'utf8').replaceAll('\r\n','\n')),pin.beforeSha256,'Root moved during test: '+pin.file);
fs.writeFileSync(path.join(dir,'TEST_RESULTS.json'),JSON.stringify(proof,null,2)+'\n');
assert.equal(result.status,0,'Source-only proposal tests failed');
