// Read-only, public-source phase measurements. No sourcePreflight/private state/owned child/browser/IPC/native.
import fs from 'node:fs';import {performance} from 'node:perf_hooks';
import {here,assertReviewedImage,committedRootFile,git} from '../daily-health-genuine-staged-operator-20261017/source.mjs';
import {ROOT} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
const report='D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026',relative='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/',expected='4d5873f820b764681d5fc36ec689cde6e5882ee8',measure=(label,fn)=>{const start=performance.now(),v=fn();return {label,elapsedMs:Math.round((performance.now()-start)*100)/100,value:v};};
if(git(report,'rev-parse','HEAD').toString().trim()!==expected)throw Error('MEASUREMENT_REPORT_HEAD_CHANGED');
const manifest=JSON.parse(fs.readFileSync(here+'/MANIFEST.json')),effectFile='docs/healthroster-build/v6-owner-delivery-20261016/DAILY_GENUINE_S0_EFFECT_ADMISSION_20261017.json',effect=JSON.parse(committedRootFile(effectFile));
const evidence=[effectFile,...['nativeOwnerEvidence','schemaEffectEvidence','canonicalBeforeEvidence','reviewRecord'].map(k=>effect[k])],rows=[];
for(const file of evidence){const r=measure('committedRootFile',()=>committedRootFile(file).length);rows.push({label:r.label,file,elapsedMs:r.elapsedMs,bytes:r.value});}
const reviewed=measure('assertReviewedImage_complete133',()=>assertReviewedImage(report,expected,[relative+'MANIFEST.json',...manifest.files.map(r=>relative+r.file)]));
if(git(report,'rev-parse','HEAD').toString().trim()!==expected)throw Error('MEASUREMENT_REPORT_HEAD_CHANGED');
console.log(JSON.stringify({publicEvidence:rows,publicEvidenceTotalMs:Math.round(rows.reduce((n,r)=>n+r.elapsedMs,0)*100)/100,reviewedImage:{elapsedMs:reviewed.elapsedMs,files:manifest.files.length+1},stateAccess:'NONE',sourcePreflight:'NOT_INVOKED',childBrowserNative:'NOT_RUN'}));
