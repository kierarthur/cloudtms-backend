// One short native ownership precheck, no fingerprint, DDL, fixture or rehearsal.
import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';
import {withSharedRehearsalTurn} from './healthroster-rehearsal-turn.mjs';
const workspace='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub';
const bankRecord=path.join(workspace,'.codex-worktrees/bpay-next-build-backend-20261009/docs/bpay-next/TEST_INSTALLS.md');
const saved=fs.readFileSync(bankRecord,'utf8');
if(!saved.includes('First disabled-first install — COMPLETE')||!saved.includes('pg_advisory_unlock returned true')
 ||!saved.includes('2026-10-13T23:45:36.217Z')||!saved.includes('07e66fb2b2742fbe634a41582f2ad3e004eed3b9'))throw Error('BANK_FIRST_COMPLETION_RELEASE_RECORD_REQUIRED');
const remote=execFileSync('git',['ls-remote','origin','refs/heads/test'],{encoding:'utf8',windowsHide:true}).trim().split(/\s/)[0];
if(remote!=='07e66fb2b2742fbe634a41582f2ad3e004eed3b9')throw Error('NATIVE_TURN_PRECHECK_BASE_CHANGED');
const progressPath=path.join(workspace,'.codex-worktrees/healthroster-v2-backend-13102026/docs/healthroster-build/PROGRESS.md');
const beganAt=new Date().toISOString();
try{
 const result=await withSharedRehearsalTurn({task:'Install1 native SEND7 PID/OID/tag precheck only',progressPath,
  async work(turn){await turn.assertHeld('native-precheck-only');return {status:'NATIVE_OWN_TURN_ADDRESS_PROVED_NO_REHEARSAL',identity:turn.identity,
   physicalDatabase:turn.identity.physicalDatabase,keyNamespace:'two-int32/objsubid=2',fingerprintsRead:false,trialRun:false,
   ddlOrFixtureApplied:false,fullRehearsalProved:false};}});
 const receipt={beganAt,endedAt:new Date().toISOString(),remote,result};
 const file=path.join(workspace,'cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_BUILD_13102026/INSTALL1_NATIVE_TURN_PRECHECK_14102026.json');
 if(fs.existsSync(file))throw Error('NATIVE_TURN_PRECHECK_RECEIPT_ALREADY_EXISTS');
 fs.writeFileSync(file,JSON.stringify(receipt,null,2)+'\n');
 console.log(JSON.stringify(receipt));
}catch(error){console.error(error.code??error.message);process.exitCode=1;}
