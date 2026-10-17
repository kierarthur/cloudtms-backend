import assert from 'node:assert/strict';
import {waitProgress} from './reference/progress.mjs';
const sourceCommit='a'.repeat(40),ownedDirectory='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/.codex-tmp/daily-genuine-stages-20261017-'+'0'.repeat(32);
const record={status:'BEFORE_READY',stage:'S0',ownedDirectory,operatorSourceCommit:sourceCommit,remainingWorkMs:50000,beforeInput:'S0_BEFORE_INPUT.json',beforeResult:'S0_BEFORE_NATIVE.json',setupEntry:'S0_SETUP_ENTRY.md',afterInput:'S0_AFTER_INPUT.json',afterResult:'S0_AFTER_NATIVE.json',effectFile:'docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json',afterEffectFile:'docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_S0_AFTER_EFFECT_QUALIFICATION_20261017.json',businessRequests:0};
let time=0,reads=0;
await assert.rejects(waitProgress('CONTROLLED', {expected:'BEFORE_READY',sourceCommit,ownedDirectory,createdAfterMs:10000}, {
 now:()=>time,pause:async()=>{time+=50;},read:()=>{reads++;return {bytes:time>=6000?Buffer.from(JSON.stringify(record)+'\n'):Buffer.alloc(0),identity:'OWNED_CONTROLLED',born:10000};}
}),{message:'PROGRESS_READY_TIMEOUT'});
assert.equal(time,5000);assert.equal(reads,100);
console.log(JSON.stringify({status:'REPRODUCED_SOURCE_P2',defaultReaderTimeoutMs:5000,controlledActualReadyAtMs:6000,reads,originalStagedRuntimeChanged:false,native:'NOT_RUN'}));
