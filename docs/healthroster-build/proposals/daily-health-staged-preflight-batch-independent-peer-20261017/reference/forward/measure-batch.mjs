// Real Git/public-source/path-metadata admission only. Does not run any operator phase.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import * as contract from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
import {preflight,reviewedImage} from './git-image-batch.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),base=path.resolve(here,'../daily-health-genuine-staged-operator-20261017'),calls=[];
for(const invocation of ['parent-sourcePreflight','child-sourcePreflight','submit-reviewed-execution-sourcePreflight']){const began=performance.now(),heads=preflight({...contract,here:base});calls.push({invocation,elapsedMs:Math.round(performance.now()-began),...heads});}
const imageStart=performance.now(),manifest=JSON.parse(fs.readFileSync(base+'/MANIFEST.json')),relative='docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017';
const image=reviewedImage(path.resolve(base,'../../../..'),'163646cb752fc64d4221ac70b685ecd952afe7b0',[relative+'/MANIFEST.json',...manifest.files.map(r=>relative+'/'+r.file)]);
const result={version:'DAILY_GENUINE_BATCH_PREFLIGHT_TIMING_V1',calls,historicalImageMs:Math.round(performance.now()-imageStart),image,
 entryPreflightMs:calls[0].elapsedMs+calls[1].elapsedMs,submitPreflightMs:calls.reduce((n,r)=>n+r.elapsedMs,0)+Math.round(performance.now()-imageStart),
 qualification:'Real original public source images and actual Git refs. New helper receives the exact original source location, and preserves all path/image guards. No simulated Git, native or browser.',
 privateContentsRead:false,browser:false,ipc:false,network:false,native:false};
fs.writeFileSync(here+(process.argv.includes('--final')?'/BATCH_TIMING_FINAL.json':'/BATCH_TIMING.json'),JSON.stringify(result,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify(result));
