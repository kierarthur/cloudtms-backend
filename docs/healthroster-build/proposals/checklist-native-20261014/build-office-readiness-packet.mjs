// Exact finite Office caller patch; reads the named current Office source only.
// It does not edit the Office lane or its global API/finance/action owners.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const directory=path.dirname(fileURLToPath(import.meta.url));
export const officePath='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-office-13102026/js/main.js';
export const officeSavedHash='d86f2ef841f53243997c7f5bb73b913a4a04a2c563378ab47e58d1a5ee82c847';
export const officeHead='2d15d121ac888a2e2a076dc7a2c4be85f9958556';
export function officeReadinessEdits(){
  const helper=fs.readFileSync(path.join(directory,'office-daily-save-readiness.jsinc'),'utf8');
  const standardEntry='  const st     = mc.timesheetState || {};\n\n  const lifecycleBusyAtSaveStart';
  const standardCatch='         // ✅ Evidence-required errors are already shown to the user; do not show generic failure\n';
  const processEntry="  const editability = classifyBulkProcessEditability(st.active_ctx || {});\n  const dirtyNow = (typeof isBulkProcessEditableDirty === 'function') ? isBulkProcessEditableDirty(st) : !!st.dirty;\n";
  const processCatch='      st.__bulk_process_save_in_flight = false;\n    try {\n      const preservedEvidenceRowsOnError';
  const authoriseEntry="    return { ok: false, error: 'No active Bulk Authorise row is available.' };\n  }\n  if (st.__bulk_authorise_freshness_checking";
  const authoriseCatch="  } catch (err) {\n    st.saving = false;\n    st.error_text = String(err?.message || err || 'Failed to save Bulk Authorise changes.');";
  return [
    {name:'single local saved/readiness boundary',before:'async function apiPostJson(path, body, options = {}) {',after:helper+'\nasync function apiPostJson(path, body, options = {}) {'},
    {name:'standard same-editor save guard',before:standardEntry,after:"  const st     = mc.timesheetState || {};\n\n  const blockedSavedDaily = blockedOfficeDailySavedReadiness(mc, rowNow);\n  if (blockedSavedDaily) {\n    alert(blockedSavedDaily.message);\n    GE();\n    return blockedSavedDaily;\n  }\n\n  const lifecycleBusyAtSaveStart"},
    {name:'standard confirmed-save catch',before:standardCatch,after:"      const savedDailyReadiness = officeDailySavedReadiness(err);\n      if (savedDailyReadiness) {\n        retainOfficeDailySavedReadiness(window.modalCtx, savedDailyReadiness);\n        alert(savedDailyReadiness.message);\n        GE();\n        return savedDailyReadiness;\n      }\n\n"+standardCatch},
    {name:'Bulk Process same-row save guard',before:processEntry,after:"  const blockedSavedDaily = blockedOfficeDailySavedReadiness(st, st.active_row);\n  if (blockedSavedDaily) {\n    st.error_text = blockedSavedDaily.message;\n    await rerenderBulkProcessWorkbench(st, '[TS][BULK-PROCESS][SAVE][READINESS]');\n    GE();\n    return blockedSavedDaily;\n  }\n\n"+processEntry},
    {name:'Bulk Process confirmed-save catch',before:processCatch,after:"      st.__bulk_process_save_in_flight = false;\n    const savedDailyReadiness = officeDailySavedReadiness(err);\n    if (savedDailyReadiness) {\n      retainOfficeDailySavedReadiness(st, savedDailyReadiness);\n      st.error_text = savedDailyReadiness.message;\n      await rerenderBulkProcessWorkbench(st, '[TS][BULK-PROCESS][SAVE][READINESS]');\n      st.__suppress_dirty_marking = false;\n      GE();\n      return savedDailyReadiness;\n    }\n    try {\n      const preservedEvidenceRowsOnError"},
    {name:'Bulk Authorise same-row save guard',before:authoriseEntry,after:"    return { ok: false, error: 'No active Bulk Authorise row is available.' };\n  }\n  const blockedSavedDaily = blockedOfficeDailySavedReadiness(st, st.active_row);\n  if (blockedSavedDaily) {\n    st.error_text = blockedSavedDaily.message;\n    await rerenderBulkAuthoriseWorkbench(st, '[TS][BULK-AUTH][SAVE][READINESS]');\n    GE();\n    return blockedSavedDaily;\n  }\n  if (st.__bulk_authorise_freshness_checking"},
    {name:'Bulk Authorise confirmed-save catch',before:authoriseCatch,after:"  } catch (err) {\n    st.saving = false;\n    const savedDailyReadiness = officeDailySavedReadiness(err);\n    if (savedDailyReadiness) {\n      retainOfficeDailySavedReadiness(st, savedDailyReadiness);\n      st.error_text = savedDailyReadiness.message;\n      await rerenderBulkAuthoriseWorkbench(st, '[TS][BULK-AUTH][SAVE][READINESS]');\n      GE();\n      return savedDailyReadiness;\n    }\n    st.error_text = String(err?.message || err || 'Failed to save Bulk Authorise changes.');"}
  ];
}
export function applyOfficeReadinessEdits(source,direction='forward'){
  const edits=direction==='forward'?officeReadinessEdits():officeReadinessEdits().reverse().map(edit=>({...edit,before:edit.after,after:edit.before}));
  let result=source;
  for(const edit of edits){
    if(result.split(edit.before).length!==2)throw Error(`Office seam changed or is not unique: ${edit.name}`);
    result=result.replace(edit.before,edit.after);
  }
  return result;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const saved=fs.readFileSync(officePath);
  if(crypto.createHash('sha256').update(saved).digest('hex')!==officeSavedHash)throw Error('Office source changed; re-read/re-pin before composition.');
  const source=saved.toString('utf8').replaceAll('\r\n','\n'),forward=applyOfficeReadinessEdits(source);
  if(applyOfficeReadinessEdits(forward,'reverse')!==source)throw Error('Office reverse preservation failed.');
  fs.writeFileSync(path.join(directory,'office-daily-readiness.forward.json'),JSON.stringify({source_path:officePath,source_head:officeHead,saved_sha256:officeSavedHash,edits:officeReadinessEdits()},null,2)+'\n');
  fs.writeFileSync(path.join(directory,'office-daily-readiness.reverse.json'),JSON.stringify({source_path:officePath,source_head:officeHead,saved_sha256:officeSavedHash,edits:officeReadinessEdits().reverse().map(edit=>({...edit,before:edit.after,after:edit.before}))},null,2)+'\n');
  console.log('Seven exact Office readiness hunks with reverse generated; no Office source changed.');
}
