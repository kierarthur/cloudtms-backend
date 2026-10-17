import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {sha,check} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {blobImages,git} from '../daily-health-genuine-staged-preflight-batch-forward-20261017/git-image-batch.mjs';
import {source as originalHolderSource,report} from '../daily-health-s0-catalogue-holder-20261017/source.mjs';
export const here=path.dirname(fileURLToPath(import.meta.url));
export function source(expectedReportHead=null){
 const {holderSourceCommit:head}=originalHolderSource();check(!expectedReportHead||head===expectedReportHead,'S0_STAGE_LIVE_REPORT_HEAD_CHANGED');
 const relative=path.relative(report,here).replaceAll('\\','/');check(relative==='docs/healthroster-build/proposals/daily-health-s0-stage-coordinator-20261017','S0_STAGE_REPORT_LOCATION_REQUIRED');
 const manifest=JSON.parse(fs.readFileSync(here+'/MANIFEST.json')),rows=manifest.files,pins=JSON.parse(fs.readFileSync(here+'/SOURCE_PINS.json'));
 const requests=[{ref:head,file:relative+'/MANIFEST.json'},{ref:null,file:relative+'/MANIFEST.json'},...rows.flatMap(r=>[head,null].map(ref=>({ref,file:relative+'/'+r.file}))),...pins.flatMap(r=>[head,r.commit,null].map(ref=>({ref,file:r.file})))];
 const images=blobImages(report,requests),manifestBytes=fs.readFileSync(here+'/MANIFEST.json');check([head,null].every(ref=>manifestBytes.equals(images.get(ref,relative+'/MANIFEST.json'))),'S0_STAGE_MANIFEST_COMMIT_REQUIRED');
 for(const r of rows){const file=relative+'/'+r.file,b=fs.readFileSync(report+'/'+file);check(b.length===r.bytes&&sha(b)===r.sha256&&[head,null].every(ref=>b.equals(images.get(ref,file))),'S0_STAGE_COMMITTED_SOURCE_REQUIRED');}
 for(const r of pins){const b=fs.readFileSync(report+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256&&[head,r.commit,null].every(ref=>b.equals(images.get(ref,r.file))),'S0_STAGE_ORIGINAL_PARENT_CHANGED');}
 check(git(report,'rev-parse','HEAD').toString().trim()===head,'S0_STAGE_REPORT_HEAD_CHANGED_DURING_CHECK');return head;
}
