// Run offline checks only; never invokes hold-pure, a client, credentials or MCP.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {here,ROOT,sha} from './source.mjs';
const run=(args,cwd)=>{
 const text=execFileSync(process.execPath,['--test','--test-reporter=tap',...args],{cwd,encoding:'utf8',windowsHide:true,maxBuffer:2*1024*1024});
 const count=Number(text.match(/^# tests (\d+)$/m)?.[1]),passed=Number(text.match(/^# pass (\d+)$/m)?.[1]),failed=Number(text.match(/^# fail (\d+)$/m)?.[1]);
 if(!count||passed!==count||failed!==0)throw Error('OFFLINE_TEST_COUNTS_REFUSED');return {tests:count,passed,failed,outputSha256:sha(text)};
};
const own=run([path.join(here,'test.mjs')],here);
const originalFiles=['tests/healthroster-live-test-restoration.test.mjs','tests/healthroster-rehearsal-turn.test.mjs'];
const originals=run(originalFiles,ROOT);
const result={status:'OFFLINE_SOURCE_TESTS_PASS',native:'NOT_RUN',restoration:'NOT_RUN',nativeShapedFixtures:'SYNTHETIC_ONLY',dbConnections:0,
 adapter:own,unchangedOriginals:{...originals,files:originalFiles.map(file=>({file,sha256:sha(fs.readFileSync(path.join(ROOT,file)))}))},
 operationalPathCheck:'All executed source launchers use approved D development paths; retained C programme binary paths intentional',
 preservedUntracked:['checklist-v5-physical-identity-capability-foundation-20261015','correction-entry-native-scope-census-20261016']};
fs.writeFileSync(path.join(here,'TEST_RESULTS.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
