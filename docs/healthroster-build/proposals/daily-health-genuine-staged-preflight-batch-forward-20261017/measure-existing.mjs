// Approved read-only original source/path metadata timing. No private contents/browser/IPC/network/native.
import fs from 'node:fs';import {fileURLToPath} from 'node:url';import path from 'node:path';
import {sourcePreflight} from '../daily-health-genuine-staged-operator-20261017/source.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),start=performance.now(),calls=[];
for(const invocation of ['entry-parent-sourcePreflight','entry-child-sourcePreflight']){const began=performance.now(),heads=sourcePreflight();calls.push({invocation,elapsedMs:Math.round(performance.now()-began),...heads});}
const result={version:'DAILY_GENUINE_ORIGINAL_PREFLIGHT_TIMING_V1',calls,combinedMs:Math.round(performance.now()-start),beforeFreshnessMs:30000,
 measuredScope:'Two unchanged safe preflight invocations required before entry reads BEFORE. Excludes process startup/ACL/current actor/native tool/commit/browser time.',privateContentsRead:false,browser:false,ipc:false,network:false,native:false};
fs.writeFileSync(here+'/ORIGINAL_TIMING.json',JSON.stringify(result,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify(result));
