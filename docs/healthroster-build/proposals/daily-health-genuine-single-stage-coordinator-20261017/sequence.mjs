// Same finite sequence as S0, with prepare in the existing qualified session and exactly one stage.
import {check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';import {stagePlan} from './stage.mjs';
export async function oneStageSequence({stage,turn,controls,signal,within,phase,announce,held}){
 stagePlan(stage);const active=()=>{within();check(!signal.aborted,'GENUINE_STAGE_TURN_OR_BUDGET_LOST');controls.assertActive();};
 const run=async name=>{active();await turn.assertHeld('genuine-'+stage+'-before-'+name);active();const result=await phase(name);active();
  if(result.status==='HOLD'){const code=/^FIXTURE_[A-Z0-9_]+$/.test(result.code??'')?result.code:'GENUINE_STAGE_PHASE_HELD';held?.({stage,phase:name,code});throw Error(code);}
  check(result.status==='PASS'&&result.stage===stage&&result.phase===name&&result.browserClosed===true&&result.childClosed===true,'GENUINE_STAGE_PHASE_REFUSED');return result;};
 const wait=async(command,status)=>{active();const ready=controls.wait(command);announce(status);await ready;active();await turn.assertHeld('genuine-'+stage+'-root-'+command.toLowerCase());active();};
 await run('prepare');await wait('BEFORE_SAVED','BEFORE_READY');
 await run('entry');await wait('SETUP_COMMITTED','SETUP_ENTRY_READY');
 await run('submit');await wait('AFTER_SAVED','AFTER_READY');
 await run('qualify');await wait('RELEASE','STAGE_QUALIFIED_PENDING_RELEASE');
 return {status:'GENUINE_STAGE_NATIVE_QUALIFIED_PENDING_ORIGINAL_RELEASE',stage,oneBusinessRequestQualified:true,browserAndChildrenClosed:true,publicComposer:'NOT_PROVIDED',financialSuccess:'NOT_CLAIMED'};
}
