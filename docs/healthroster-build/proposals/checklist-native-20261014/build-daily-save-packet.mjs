// Complete pinned current Office Daily route, with one post-save follow-on.
// The integrator owns the actual index.js merge; no runtime source is patched.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const repo=fileURLToPath(new URL('../../../../',import.meta.url));
const directory=path.dirname(fileURLToPath(import.meta.url));
const hash=s=>crypto.createHash('sha256').update(s).digest('hex');
export const indexSavedHash='63d1f8364d8c7fb9544a5b0f8a39810a4481033c5417584c80d9f3a2de21d10d';
export const dailySaveImport="import { prepareSubmittedDailyCycleAfterSave } from './weekly-source/submission-cycle-prepare.mjs';\n";
export const dailySaveFollowOn=`  // The canonical Daily save/recalculation and existing audits have succeeded.
  // Prepare only native Checklist metadata using the real current identity.
  const checklistCycleReadiness = await prepareSubmittedDailyCycleAfterSave({
    rpc: (name, args) => sbRpc(env, name, args, { timeoutMs: 45000 }),
    actorUserId: user.id,
    timesheetId: currentTimesheetId
  });
  if (checklistCycleReadiness.ok !== true) {
    return withCORS(env, req, new Response(JSON.stringify(checklistCycleReadiness), {
      status: 502, headers: { 'Content-Type': 'application/json' }
    }));
  }

`;
export function currentDailySaveOwner(){
  const saved=fs.readFileSync(path.join(repo,'broker/src/index.js'));
  if(hash(saved)!==indexSavedHash)throw Error('Current Daily route predecessor changed; re-read and re-pin before composition.');
  const source=saved.toString('utf8').replaceAll('\r\n','\n');
  const marker='async function handleTimesheetDailyManualUpsert(env, req, timesheetId) {';
  const successor='async function handleContractWeekDeleteTimesheet(env, req, weekId) {';
  if(source.split(marker).length!==2||source.split(successor).length!==2)throw Error('Daily route boundaries are not unique.');
  const start=source.indexOf(marker),end=source.indexOf(successor,start);
  if(end<=start)throw Error('Complete Daily route boundary missing.');
  return source.slice(start,end).trimEnd()+'\n';
}
export function forwardDailySaveOwner(){
  const source=currentDailySaveOwner(),seam='  if (bulkPatchResponseRequested) {\n';
  if(source.split(seam).length!==2)throw Error('Post-save success response seam is not unique.');
  return source.replace(seam,dailySaveFollowOn+seam);
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  for(const direction of ['forward','reverse']){
    fs.writeFileSync(path.join(directory,`office-daily-save.${direction}.jsinc`),
      `// SOURCE ONLY: complete ${direction} current Daily save route.\n`+(direction==='forward'?forwardDailySaveOwner():currentDailySaveOwner()));
  }
  fs.writeFileSync(path.join(directory,'office-daily-save.import.jsinc'),dailySaveImport);
  console.log('Complete pinned Office Daily forward/reverse and import proposals generated. Runtime source unchanged.');
}
