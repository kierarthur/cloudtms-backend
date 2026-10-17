// Pure adapter addition. Keeps original source/auxiliary-effect review separate from B2 currentness.
import {sha,entryText} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/contract.mjs';
import {assertSetupEntry} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/stages.mjs';
import {currentFiles,validateCurrentAfter,assertCurrentPrefix} from 'file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/current-read.mjs';
export function bindAfterCurrent(a){
 const f=currentFiles(a.stage),effectBytes=a.readPublic('docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_'+a.stage+'_EFFECT_ADMISSION_20261017.json');
 if(a.plan.currentReadEvidence!==f.evidence||a.plan.currentConsumptionEvidence!==f.consumptionEvidence)throw Error('FIXTURE_CURRENT_EFFECT_PATH_REQUIRED');
 const entry={sourceCommit:a.source,beforeSqlSha256:a.beforeBuilt.receipt.sqlSha256,verificationSqlSha256:a.beforeBuilt.receipt.verificationSqlSha256,beforeEnvelopeSha256:sha(a.beforeBytes),effectAdmissionSha256:sha(effectBytes)};
 const entrySha256=assertSetupEntry(a.readPublic('docs/healthroster-build/TEST_SETUP_MANIFEST.md').toString(),entry,a.stage,a.beforeBytes,a.beforeBuilt.receipt,effectBytes);
 if(entrySha256!==sha(entryText(a.stage,entry)))throw Error('FIXTURE_CURRENT_EFFECT_ENTRY_REQUIRED');
 const currentEvidenceBytes=a.readPublic(f.evidence),afterArgs={currentEvidenceBytes,consumptionBytes:a.consumptionBytes,beforeBuilt:a.beforeBuilt,beforeInputBytes:a.beforeInputBytes,beforeBytes:a.beforeBytes,entry,entrySha256,response:JSON.parse(a.responseBytes),now:a.now};
 const currentBefore=validateCurrentAfter(afterArgs);
 return {afterArgs,afterOwners:{validateCurrentAfter,assertCurrentPrefix},currentBefore,afterFields:{currentReadEvidence:f.evidence,currentConsumptionEvidence:f.consumptionEvidence,currentEvidenceSha256:sha(currentEvidenceBytes),currentConsumptionSha256:sha(a.consumptionBytes)},consumptionRecord:{file:f.consumptionEvidence,bytes:Buffer.from(a.consumptionBytes)}};
}
