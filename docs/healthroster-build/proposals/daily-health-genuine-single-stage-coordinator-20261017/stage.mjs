// One explicitly selected genuine stage. No path discovery, automatic next-stage selection or transport.
import path from 'node:path';import {ROOT,check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {runStaged} from '../daily-health-s0-stage-coordinator-20261017/phase-parent.mjs';
export const stages=Object.freeze(['S1','S1a','S2','S3','S4']);
const evidence='docs/healthroster-build/v6-owner-delivery-20261016/';
export function stagePlan(stage){check(stages.includes(stage),'GENUINE_STAGE_EXPLICIT_SELECTION_REQUIRED');return Object.freeze({stage,effect:evidence+'DAILY_GENUINE_'+stage+'_EFFECT_ADMISSION_20261017.json',afterEffect:evidence+'DAILY_GENUINE_'+stage+'_AFTER_EFFECT_QUALIFICATION_20261017.json'});}
export function selection(flags){
 check(flags.length===5&&flags[0]==='--authorised-root-one-genuine-stage'&&flags[1]==='--stage'&&flags[3]==='--owned','GENUINE_STAGE_FIXED_ARGUMENTS_REQUIRED');
 const plan=stagePlan(flags[2]);check(typeof flags[4]==='string'&&path.isAbsolute(flags[4]),'GENUINE_STAGE_ACTUAL_QUALIFIED_SESSION_REQUIRED');const out=path.resolve(flags[4]).replaceAll('\\','/');
 check(path.posix.dirname(out)===ROOT+'/.codex-tmp'&&/^daily-genuine-stages-20261017-[a-f0-9]{32}$/.test(path.posix.basename(out)),'GENUINE_STAGE_ACTUAL_QUALIFIED_SESSION_REQUIRED');
 return Object.freeze({...plan,out}); // The unchanged launcher/child separately prove physical path, ACL and prefix.
}
export function phaseFlags(phase,stage,out){
 const plan=stagePlan(stage);check(['prepare','entry','submit','qualify'].includes(phase),'GENUINE_STAGE_FIXED_PHASE_REQUIRED');
 return ['--authorised-root-staged-fixture','--outside-other-native-interval','--phase',phase,'--stage',stage,'--owned',out,...phase==='entry'?['--effect-file',plan.effect]:phase==='qualify'?['--effect-file',plan.afterEffect]:[]];
}
export async function runPhase({phase,stage,out,controlEnvironment,signal,assertHeld,runOwnedTurnChild,treeFactory,currentNative}){
 return runStaged(phaseFlags(phase,stage,out),{controlEnvironment,signal,assertHeld,runOwnedTurnChild,treeFactory,currentNative});
}
