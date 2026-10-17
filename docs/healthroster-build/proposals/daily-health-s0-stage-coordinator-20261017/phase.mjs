// Only these four genuine S0 phases are reachable from this coordinator.
import {check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {runStaged} from './phase-parent.mjs';import {EFFECT,AFTER_EFFECT} from './sequence.mjs';
export function phaseFlags(phase,out){
 check(['bind','entry','submit','qualify'].includes(phase),'S0_STAGE_FIXED_PHASE_REQUIRED');
 return ['--authorised-root-staged-fixture','--outside-other-native-interval','--phase',phase,'--stage','S0',...phase==='bind'?[]:['--owned',out],...phase==='entry'?['--effect-file',EFFECT]:phase==='qualify'?['--effect-file',AFTER_EFFECT]:[]];
}
export async function runPhase({phase,out,controlEnvironment,signal,assertHeld,runOwnedTurnChild,treeFactory}){
 return runStaged(phaseFlags(phase,out),{controlEnvironment,signal,assertHeld,runOwnedTurnChild,treeFactory});
}
