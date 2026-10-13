import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const root=process.argv[2],here=path.dirname(fileURLToPath(import.meta.url));
const input=fs.readFileSync(path.join(root,'broker/src/index.js')),source=input.toString('utf8').replace(/\r\n/g,'\n');
const sha=s=>crypto.createHash('sha256').update(s).digest('hex');
assert.equal(sha(input),'a1cb181fa8588813637598873606438e6dd0a10ab57d286a210a253c63fd6a86','controlling complete native worker source changed');
const worker=source.slice(source.indexOf('async function runTsfinWorkerOnce('),source.indexOf('async function handleTimesheetAuthoriseGeneric('));
function span(start,end){const a=worker.indexOf(start),b=worker.indexOf(end,a);assert.ok(a>=0&&b>a,start);return worker.slice(a,b);}
const roles=span('  const norm = (s) => String(s || "").trim();','  const anyMissingRatesLocal =');
fs.writeFileSync(path.join(here,'native-daily-role-helpers.mjs'),`// Exact local helper extraction from the ordinary TSFIN worker. No finance or transport.\n${roles}\nexport { normRole as normaliseRole, normBand as normaliseBand, parseCandidateRoleCodes };\n`);
const boolish=span('  const boolish = (v) => {','  const norm =');
const dailyPath='broker/src/daily-canonical-authority.js';
const dailySource=fs.readFileSync(path.join(root,dailyPath));
assert.equal(sha(dailySource),'64cd2109da90f9ee277c2829a734683e7e06649ebebe14b1e2d296ac1c04d00b','controlling DAILY canonical source changed');
fs.writeFileSync(path.join(here,'daily-canonical-authority.native-input.js'),dailySource);
const route=span('        const basis = String(curFin?.basis || \'\').toUpperCase();','        weeklyWork.push({');
const dispatch=span('      const tsid = ts?.timesheet_id ? String(ts.timesheet_id) : null;','      if (!buildRes || buildRes.ok !== true || !buildRes.snapshot) {');
// This exact body owns only dispatch to the unchanged existing calculators.
// No lease, writer, success/fail, self-heal or debug-audit portion is extracted.
assert.ok(!/tsfin_(work_success|work_fail|dequeue|write|repair)|_imp_debug_audit/.test(dispatch));
const module=`// Bounded ordinary-worker read-only route extraction; calculators stay with their native owner.\n${boolish}\nexport function deriveNativeWeeklyCorrectionRoute(ts,curFin,effFlags) {\n${route}\n  return {routeType,basis,isNhspRoute,isHrRoute,isNhspBasis,isHrBasis};\n}\nexport async function buildNativeWeeklyCorrectionSnapshot(input,owners) {\n  const {env,ts,curFin,effFlags,cw,contract,policySql,routeType,basis,isNhspRoute,isHrRoute,isNhspBasis,isHrBasis}=input;\n  const {buildWeeklyScheduleSegmentsSnapshot,buildNhspWeeklySnapshotCached,rebuildFromExistingSegmentsEvidence,enrichTsfinWithHrCrosscheck}=owners;\n  const shiftsByTsId=new Map([[String(ts.timesheet_id),input.shifts]]);\n  const weeklyValidationByTsId=new Map([[String(ts.timesheet_id),input.validation]]);\n  const outbox_id=null;\n  const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;\n  const asNumberLocal = (v) => (v == null ? 0 : Number(v) || 0);\n${dispatch}\n  if (!buildRes || buildRes.ok !== true || !buildRes.snapshot) throw new Error(buildRes?.reason || 'CORRECTION_NATIVE_WEEKLY_BUILD_FAILED');\n  const snapshot=buildRes.snapshot;\n  if (snapshot.occupant_key_norm == null && ts.occupant_key_norm != null) snapshot.occupant_key_norm=ts.occupant_key_norm;\n  await enrichTsfinWithHrCrosscheck(env,ts,snapshot,effFlags || null,input.shifts);\n  return snapshot;\n}\n`;
fs.writeFileSync(path.join(here,'native-weekly-correction-dispatch.mjs'),module);
const facts={status:'READ_ONLY_NATIVE_EXTRACTION_NOT_MOUNTED',input:{file:'broker/src/index.js',sha256:sha(input)},extractions:[
 {output:'native-daily-role-helpers.mjs',sourceBlockSha256:sha(roles),outputSha256:sha(fs.readFileSync(path.join(here,'native-daily-role-helpers.mjs')))},
 {output:'native-weekly-correction-dispatch.mjs',sourceRouteSha256:sha(route),sourceDispatchSha256:sha(dispatch),outputSha256:sha(module)}
],dailyNativeInput:{file:dailyPath,sha256:sha(dailySource),copy:'daily-canonical-authority.native-input.js'},retainedOwners:['buildWeeklyScheduleSegmentsSnapshot','buildNhspWeeklySnapshotCached','rebuildFromExistingSegmentsEvidence','enrichTsfinWithHrCrosscheck'],excludedWorkerArms:['dequeue','write','repair/self-heal','outbox success/fail','debug audit','no-op queue completion']};
fs.writeFileSync(path.join(here,'NATIVE_EXTRACTION_SOURCE_GUARD.json'),JSON.stringify(facts,null,2)+'\n');
console.log(JSON.stringify({inputSha256:sha(input),daily:sha(roles),dispatch:sha(dispatch)}));
