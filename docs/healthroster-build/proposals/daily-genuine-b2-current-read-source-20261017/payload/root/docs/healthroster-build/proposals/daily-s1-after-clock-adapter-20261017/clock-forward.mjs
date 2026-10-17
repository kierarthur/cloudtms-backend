// Pure finite guard around the unchanged original S1 data composer.
import {prepareS1After as originalPrepare,validateCommittedS1After as originalValidate} from '../daily-s1-actual-effect-recorder-20261017/recorder.mjs';
const must=(v,c)=>{if(!v)throw Error('S1_AFTER_CLOCK_'+c)};
export function assertActualAuxiliaryAfterClock(responseBytes,background){
 must(responseBytes instanceof Uint8Array&&responseBytes.byteLength>0&&responseBytes.byteLength<=2e6,'BOUNDED_ACTUAL_RESPONSE_REQUIRED');
 const response=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(responseBytes)),done=Date.parse(response.finishedAt);
 must(Number.isFinite(done)&&background?.stage==='S1'&&Array.isArray(background.observations)&&background.observations.length===4,'ACTUAL_RESPONSE_AND_FOUR_OBSERVATIONS_REQUIRED');
 const after=background.observations.slice(2);must(after.every(v=>v.phase==='AFTER'),'ACTUAL_AFTER_PHASE_REQUIRED');
 const times=after.map(v=>Date.parse(v.observedAt));must(times.every(t=>Number.isFinite(t)&&t>=done)&&times[1]>=times[0],'AUXILIARY_AFTER_NOT_BEFORE_ACTUAL_RESPONSE');
}
export function prepareS1After(a){const p=originalPrepare(a);assertActualAuxiliaryAfterClock(a.responseBytes,p.background);return p;}
export function validateCommittedS1After(a){prepareS1After(a);return originalValidate(a);}
