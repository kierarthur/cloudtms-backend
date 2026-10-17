// Pure committed-byte validation only. The original root AFTER/PUBLISH owners remain required.
import {bindAfterCurrent} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/docs/healthroster-build/proposals/daily-b2-current-effect-20261017/current-effect.mjs';
import {createHash} from 'node:crypto';
import {prepareOriginalStageAfter} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/docs/healthroster-build/proposals/daily-original-stages-after-response-clock-forward-20261017/recorder.mjs';
import {afterProof} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-genuine-stage-root-bookkeeper-20261017/core.mjs';
import {readEnvelope} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/native-read.mjs';
import {assertBefore,assertAfter} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/stages.mjs';
const sha=b=>createHash('sha256').update(b).digest('hex'),parse=b=>JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(b));
export function validateCommittedOriginalStageAfter(a){
 const p=prepareOriginalStageAfter(a);for(const r of p.records)if(!Buffer.from(a.readPublic(r.file)).equals(r.bytes))throw Error('STAGE_RECORD_EXACT_COMMITTED_RETURNED_RECORD_REQUIRED');
 const plan=parse(a.readPublic(a.planFile)),effect=parse(p.records[2].bytes);
 const current=bindAfterCurrent({plan,...a});
 return afterProof({...current.afterArgs,stage:a.stage,source:a.source,plan,prior:parse(a.readPublic(plan.previousEvidence)),beforeBuilt:a.beforeBuilt,beforeBytes:a.beforeBytes,beforeResult:parse(a.beforeBytes),built:a.afterBuilt,bytes:a.afterBytes,result:parse(a.afterBytes),response:parse(a.responseBytes),afterEffect:effect,readPublic:a.readPublic,now:a.now},{readEnvelope,assertBefore,assertAfter,sha,...current.afterOwners});
}
