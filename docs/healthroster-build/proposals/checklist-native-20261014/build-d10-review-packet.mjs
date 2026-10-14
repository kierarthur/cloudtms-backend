// Exact current Office navigation seam; no Office lane mutation.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const directory=path.dirname(fileURLToPath(import.meta.url));
const office='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-office-13102026/';
export const d10OfficePins=[
  {path:office+'js/weekly-source/workspace-actions.js',saved_sha256:'04d2614210a6d32a0c8ed42b21fab9dc384b3958e053fbec28e704f33c81f267'},
  {path:office+'js/import-review-v1.js',saved_sha256:'767051ef2f8f9a5beaf53067bfb80566e4db4992e194be4c6b9fc66320f17034'}
];
function once(source,before,after){if(source.split(before).length!==2)throw Error('Native D10 continuation seam changed.');return source.replace(before,after);}
export function currentOpenReviewOwner(){
  const saved=fs.readFileSync(d10OfficePins[1].path);
  if(crypto.createHash('sha256').update(saved).digest('hex')!==d10OfficePins[1].saved_sha256)throw Error('Native openReview predecessor changed.');
  const source=saved.toString('utf8').replaceAll('\r\n','\n'),start=source.indexOf('  async function openReview(importId, options = {}) {'),end=source.indexOf('  function reviewUiState(review) {',start);
  if(start<0||end<=start)throw Error('Complete native openReview boundary absent.');return source.slice(start,end).trimEnd()+'\n';
}
export function forwardOpenReviewOwner(){
  let source=currentOpenReviewOwner();
  source=once(source,'    const previousReview = state.review;\n',`    const previousReview = state.review;
    const boundAction = options.checkOnlyD10Action || null;
    const boundSession = typeof SESSION !== 'undefined' ? SESSION : null;
    const boundUser = boundSession?.user;
    const boundActorId = boundUser?.id;
    const boundRoles = JSON.stringify(Array.isArray(boundUser?.roles) ? boundUser.roles : null);
    const boundFlags = JSON.stringify([boundUser?.role, boundUser?.is_active]);
    const boundSessionCurrent = () => !boundAction || (!!boundSession && !!boundUser
      && (typeof SESSION !== 'undefined' ? SESSION : null) === boundSession
      && SESSION?.user === boundUser && SESSION?.user?.id === boundActorId
      && JSON.stringify(Array.isArray(SESSION?.user?.roles) ? SESSION.user.roles : null) === boundRoles
      && JSON.stringify([SESSION?.user?.role, SESSION?.user?.is_active]) === boundFlags);
`);
  source=once(source,'    if (!returnCallerCurrent()) return;','    if (!returnCallerCurrent() || !boundSessionCurrent()) return;');
  source=source.replaceAll('if (epoch !== state.reviewEpoch || !returnCallerCurrent()) return;',
    'if (epoch !== state.reviewEpoch || !returnCallerCurrent() || !boundSessionCurrent()) return;');
  source=once(source,"    let autoRecheckWarning = '';",`    if (boundAction) {
      await assertCheckOnlyD10ReviewFresh(boundAction, boundActorId, importId, header,
        cursor => request(\`/api/import-reviews/\${encodeURIComponent(importId)}?action_cursor=\${encodeURIComponent(cursor)}&action_limit=200&event_limit=1\`),
        () => epoch === state.reviewEpoch && returnCallerCurrent() && boundSessionCurrent());
      if (epoch !== state.reviewEpoch || !returnCallerCurrent() || !boundSessionCurrent()) return;
    }
    let autoRecheckWarning = '';`);
  source=once(source,'    if (options.skipAutoRecheck !== true && canRefresh) {','    if (!boundAction && options.skipAutoRecheck !== true && canRefresh) {');
  source=once(source,'    state.review = {','    const openingReview = {');
  source=once(source,'      returnOwner,\n','      returnOwner,\n      ...(boundAction ? { checkOnlyD10Action: boundAction, checkOnlyD10SessionCurrent: boundSessionCurrent } : {}),\n');
  const tail=`    if (isSingleClientHealthRoster(state.review) && state.review.sortBy === 'CLIENT') state.review.sortBy = 'CANDIDATE';
    state.review.scope = await fetchScope(importId, 1, 25).catch(() => null);
    state.review.pageData = await fetchActionPage(state.review);
    if (epoch !== state.reviewEpoch || !returnCallerCurrent() || !boundSessionCurrent()) return;
    showScreen('Import review', renderReview, 'import-review-v1');
    installReviewReturn(state.review);
    scheduleDocumentPreparationPoll(state.review);`;
  source=once(source,tail,`    if (boundAction) {
      // Keep old-actor header/scope/page private until every native await and
      // the original epoch/modal ownership plus real session context agree.
      if (isSingleClientHealthRoster(openingReview) && openingReview.sortBy === 'CLIENT') openingReview.sortBy = 'CANDIDATE';
      const scope = await fetchScope(importId, 1, 25).catch(() => null);
      if (epoch !== state.reviewEpoch || !returnCallerCurrent() || !boundSessionCurrent()) return;
      openingReview.scope = scope;
      const pageData = await fetchActionPage(openingReview);
      if (epoch !== state.reviewEpoch || !returnCallerCurrent() || !boundSessionCurrent()) return;
      openingReview.pageData = pageData;
      state.review = openingReview;
    } else {
      // Preserve the complete ordinary unbound continuation.
      state.review = openingReview;
      if (isSingleClientHealthRoster(state.review) && state.review.sortBy === 'CLIENT') state.review.sortBy = 'CANDIDATE';
      state.review.scope = await fetchScope(importId, 1, 25).catch(() => null);
      state.review.pageData = await fetchActionPage(state.review);
    }
    if (epoch !== state.reviewEpoch || !returnCallerCurrent() || !boundSessionCurrent()) return;
    showScreen('Import review', boundAction ? () => epoch === state.reviewEpoch && returnCallerCurrent() && boundSessionCurrent() ? renderReview() : '' : renderReview, 'import-review-v1');
    if (boundAction && (epoch !== state.reviewEpoch || !boundSessionCurrent())) return;
    installReviewReturn(state.review);
    if (boundAction && (epoch !== state.reviewEpoch || !returnCallerCurrent() || !boundSessionCurrent())) return;
    scheduleDocumentPreparationPoll(state.review);`);
  return source;
}
export function d10ReviewEdits(){
  const helper=fs.readFileSync(path.join(directory,'office-d10-review-action.jsinc'),'utf8');
  return [
    {path:d10OfficePins[0].path,before:'  function handleAction(detailValue) {',after:helper+'\n  function handleAction(detailValue) {'},
    {path:d10OfficePins[0].path,before:"    const label = asText(detail.label);\n    if (label === 'Retry recheck'",after:"    const label = asText(detail.label);\n    if (detail.kind === 'CHECK_ONLY_D10_REVIEW') {\n      const actorId = typeof SESSION !== 'undefined' ? SESSION?.user?.id : null;\n      const action = normaliseCheckOnlyD10ReviewAction(detail.payload, actorId);\n      if (!action || label !== 'View details' || detail.enabled === false || typeof root.CloudTmsImportReviewV1?.openReview !== 'function') {\n        return openDetail({ detail: { problem: 'This HealthRoster query could not be verified. Return to Queries and recheck.' } }, 'View details');\n      }\n      const workspace = workspaceApi();\n      return root.CloudTmsImportReviewV1.openReview(action.action_bindings[0].import_id, {\n        checkOnlyD10Action: action, skipAutoRecheck: true,\n        returnTo: () => (typeof SESSION !== 'undefined' ? SESSION?.user?.id : null) === actorId\n          && workspaceApi() === workspace ? workspace?.refresh?.('queries') : undefined,\n        view: action.action_bindings.every(binding => ['EMAIL_ISSUE','EMAIL_REMINDER'].includes(binding.action_kind)) ? 'EMAIL' : 'PENDING'\n      });\n    }\n    if (label === 'Retry recheck'"},
    {path:d10OfficePins[1].path,before:'  async function openReview(importId, options = {}) {',after:helper+'\n  async function openReview(importId, options = {}) {'},
    {path:d10OfficePins[1].path,before:currentOpenReviewOwner().trimEnd(),after:forwardOpenReviewOwner().trimEnd()},
    {path:d10OfficePins[1].path,before:'    cancelDocumentPreparationPoll();\n    if (!review || state.review !== review || !pageNeedsDocumentPreparationPoll(review)) return;',after:'    cancelDocumentPreparationPoll();\n    if (review?.checkOnlyD10SessionCurrent && !review.checkOnlyD10SessionCurrent()) return;\n    if (!review || state.review !== review || !pageNeedsDocumentPreparationPoll(review)) return;'},
    {path:d10OfficePins[1].path,before:"      const current = state.review;\n      if (!current || current !== review || current.importId !== importId || current.epoch !== epoch) return;",after:"      const current = state.review;\n      if (review?.checkOnlyD10SessionCurrent && !review.checkOnlyD10SessionCurrent()) return;\n      if (!current || current !== review || current.importId !== importId || current.epoch !== epoch) return;"},
    {path:d10OfficePins[1].path,before:'        await openReview(importId, { preserveLocal: true, documentPoll: true });',after:'        await openReview(importId, { preserveLocal: true, documentPoll: true,\n          ...(current.checkOnlyD10Action ? { checkOnlyD10Action: current.checkOnlyD10Action, skipAutoRecheck: true } : {}) });\n        if (current.checkOnlyD10SessionCurrent && !current.checkOnlyD10SessionCurrent()) return;'},
    {path:d10OfficePins[1].path,before:'      } catch (error) {\n        if (state.review === current) {',after:'      } catch (error) {\n        if (current.checkOnlyD10SessionCurrent && !current.checkOnlyD10SessionCurrent()) return;\n        if (state.review === current) {'},
    {path:d10OfficePins[1].path,before:'            await owner.callback();',after:'            if (review.checkOnlyD10SessionCurrent && !review.checkOnlyD10SessionCurrent()) return;\n            await owner.callback();'},
    {path:d10OfficePins[1].path,before:"          }).catch(() => notify('The source list could not be refreshed. Recheck before continuing.'));",after:"          }).catch(() => {\n            if (review.checkOnlyD10SessionCurrent && !review.checkOnlyD10SessionCurrent()) return;\n            notify('The source list could not be refreshed. Recheck before continuing.');\n          });"}
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
  fs.writeFileSync(path.join(directory,'office-open-review.forward.jsinc'),forwardOpenReviewOwner());
  fs.writeFileSync(path.join(directory,'office-open-review.reverse.jsinc'),currentOpenReviewOwner());
  console.log('Ten exact Office D10 navigation hunks, complete openReview forward/reverse generated; Office source unchanged.');
}
