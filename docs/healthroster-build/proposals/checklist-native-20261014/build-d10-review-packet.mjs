// Exact current Office navigation seam; no Office lane mutation.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const directory=path.dirname(fileURLToPath(import.meta.url));
const office='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-office-13102026/';
export const d10OfficePins=[
  {path:office+'js/weekly-source/workspace-actions.js',saved_sha256:'04d2614210a6d32a0c8ed42b21fab9dc384b3958e053fbec28e704f33c81f267'},
  {path:office+'js/import-review-v1.js',saved_sha256:'767051ef2f8f9a5beaf53067bfb80566e4db4992e194be4c6b9fc66320f17034'}
];
export function d10ReviewEdits(){
  const helper=fs.readFileSync(path.join(directory,'office-d10-review-action.jsinc'),'utf8');
  return [
    {path:d10OfficePins[0].path,before:'  function handleAction(detailValue) {',after:helper+'\n  function handleAction(detailValue) {'},
    {path:d10OfficePins[0].path,before:"    const label = asText(detail.label);\n    if (label === 'Retry recheck'",after:"    const label = asText(detail.label);\n    if (detail.kind === 'CHECK_ONLY_D10_REVIEW') {\n      const actorId = typeof SESSION !== 'undefined' ? SESSION?.user?.id : null;\n      const action = normaliseCheckOnlyD10ReviewAction(detail.payload, actorId);\n      if (!action || label !== 'View details' || detail.enabled === false || typeof root.CloudTmsImportReviewV1?.openReview !== 'function') {\n        return openDetail({ detail: { problem: 'This HealthRoster query could not be verified. Return to Queries and recheck.' } }, 'View details');\n      }\n      const workspace = workspaceApi();\n      return root.CloudTmsImportReviewV1.openReview(action.action_bindings[0].import_id, {\n        checkOnlyD10Action: action, skipAutoRecheck: true,\n        returnTo: () => (typeof SESSION !== 'undefined' ? SESSION?.user?.id : null) === actorId\n          && workspaceApi() === workspace ? workspace?.refresh?.('queries') : undefined,\n        view: action.action_bindings.every(binding => ['EMAIL_ISSUE','EMAIL_REMINDER'].includes(binding.action_kind)) ? 'EMAIL' : 'PENDING'\n      });\n    }\n    if (label === 'Retry recheck'"},
    {path:d10OfficePins[1].path,before:'  async function openReview(importId, options = {}) {',after:helper+'\n  async function openReview(importId, options = {}) {'},
    {path:d10OfficePins[1].path,before:"    if (epoch !== state.reviewEpoch || !returnCallerCurrent()) return;\n    let autoRecheckWarning = '';",after:"    if (epoch !== state.reviewEpoch || !returnCallerCurrent()) return;\n    if (options.checkOnlyD10Action) {\n      const actorId = typeof SESSION !== 'undefined' ? SESSION?.user?.id : null;\n      await assertCheckOnlyD10ReviewFresh(options.checkOnlyD10Action, actorId, importId, header,\n        cursor => request(`/api/import-reviews/${encodeURIComponent(importId)}?action_cursor=${encodeURIComponent(cursor)}&action_limit=200&event_limit=1`),\n        () => epoch === state.reviewEpoch && returnCallerCurrent()\n          && (typeof SESSION !== 'undefined' ? SESSION?.user?.id : null) === actorId);\n      if (epoch !== state.reviewEpoch || !returnCallerCurrent()) return;\n    }\n    let autoRecheckWarning = '';"}
  ];
}
export function applyD10ReviewEdits(source,file,direction='forward'){
  const edits=d10ReviewEdits().filter(edit=>edit.path===file);
  const ordered=direction==='forward'?edits:edits.reverse().map(edit=>({...edit,before:edit.after,after:edit.before}));
  for(const edit of ordered){if(source.split(edit.before).length!==2)throw Error('Current Office D10 seam changed or ambiguous.');source=source.replace(edit.before,edit.after);}
  return source;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  for(const pin of d10OfficePins){
    const saved=fs.readFileSync(pin.path);if(crypto.createHash('sha256').update(saved).digest('hex')!==pin.saved_sha256)throw Error('Current Office D10 source changed; re-read/re-pin.');
    const source=saved.toString('utf8').replaceAll('\r\n','\n');
    if(applyD10ReviewEdits(applyD10ReviewEdits(source,pin.path),pin.path,'reverse')!==source)throw Error('Office D10 reverse failed.');
  }
  fs.writeFileSync(path.join(directory,'office-d10-review.forward.json'),JSON.stringify({pins:d10OfficePins,edits:d10ReviewEdits()},null,2)+'\n');
  fs.writeFileSync(path.join(directory,'office-d10-review.reverse.json'),JSON.stringify({pins:d10OfficePins,edits:d10ReviewEdits().reverse().map(edit=>({...edit,before:edit.after,after:edit.before}))},null,2)+'\n');
  console.log('Four exact Office D10 navigation hunks and reverse generated; Office source unchanged.');
}
