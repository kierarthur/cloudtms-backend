// Fixed S0-only choreography. Root supplies actual generated native results and commits.
import {check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
export const EFFECT='docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json';
export const AFTER_EFFECT='docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_S0_AFTER_EFFECT_QUALIFICATION_20261017.json';
export async function s0Sequence({turn,controls,signal,within,phase,announce}){
 const active=()=>{within();check(!signal.aborted,'S0_STAGE_TURN_OR_BUDGET_LOST');controls.assertActive();};
 const run=async name=>{active();await turn.assertHeld('s0-stage-before-'+name);active();const result=await phase(name);active();check(result.status==='PASS'&&result.stage==='S0'&&result.phase===name&&result.browserClosed===true&&result.childClosed===true,'S0_STAGE_PHASE_REFUSED');return result;};
 const wait=async(command,status)=>{active();const ready=controls.wait(command);announce(status);await ready;active();await turn.assertHeld('s0-stage-root-'+command.toLowerCase());active();};
 await run('bind');await wait('BEFORE_SAVED','BEFORE_READY');
 await run('entry');await wait('SETUP_COMMITTED','SETUP_ENTRY_READY');
 await run('submit');await wait('AFTER_SAVED','AFTER_READY');
 await run('qualify');await wait('RELEASE','S0_QUALIFIED_PENDING_RELEASE');
 return {status:'S0_NATIVE_QUALIFIED_PENDING_ORIGINAL_RELEASE',stage:'S0',businessRequests:0,browserAndChildrenClosed:true,publicComposer:'NOT_PROVIDED',financialSuccess:'NOT_CLAIMED'};
}
