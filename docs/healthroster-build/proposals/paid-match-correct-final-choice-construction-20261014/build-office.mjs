import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const source='docs/healthroster-build/proposals/paid-match-office-confirm-consumer-construction-20261014/forward/frontend/js/weekly-source/workspace-actions.js';
const raw=fs.readFileSync(path.join(root,source));assert.deepEqual(raw,execFileSync('git',['show','65c82dcc6d8f2748fef7c4209d96a673f7ec8756:'+source],{cwd:root,maxBuffer:2e6}));
const before=raw.toString(),eol=before.includes('\r\n')?'\r\n':'\n',sha=b=>createHash('sha256').update(b).digest('hex');
const hunks=[];
let after=before;
function edit(name,a,b){a=a.replaceAll('\n',eol);b=b.replaceAll('\n',eol);assert.equal(after.split(a).length,2,name);after=after.replace(a,()=>b);hunks.push({name,before:a,after:b});}
edit('retain actual native monetary preview fields',`        result: plainMessage(row.result, 'Changed')`,`        result: plainMessage(row.result, 'Changed'),
        current_final_amount: plainMessage(row.current_final_amount, '—'),
        replacement_amount: plainMessage(row.replacement_amount, '—'),
        amount_changed: row.amount_changed === true`);
edit('actual native questions retained in correction model',`      changes,
      blockers,
      confirmation_text:`,`      changes,
      blockers,
      paid_match_questions: asArray(raw.paid_match_questions),
      confirmation_text:`);
edit('show native current and replacement amount labels without calculation',`<td>\${escapeHtml(row.current_final)}</td><td>\${escapeHtml(row.replacement)}</td>`,`<td>\${escapeHtml(row.current_final)}<br>\${escapeHtml(row.current_final_amount)}</td><td>\${escapeHtml(row.replacement)}<br>\${escapeHtml(row.replacement_amount)}</td>`);
edit('existing blocked tab exposes genuine paid questions',`      return \`<div class="ws-child-scroll"><table class="grid mini ws-child-table"><thead><tr><th>Candidate</th><th>Day/date</th><th>Problem</th><th>Action</th></tr></thead><tbody>\${rows || '<tr><td colspan="4" class="ws-empty">Nothing is blocking this correction.</td></tr>'}</tbody></table></div>\`;`,`      const paid = asArray(model.paid_match_questions).map((item,index) => \`<p>\${escapeHtml(item.candidate)} · \${escapeHtml(item.shift)} <button type="button" class="btn btn-outline" data-wsa-correction-paid-match="\${index}">Compare paid shift</button></p>\`).join('');
      return \`<div class="ws-child-scroll"><table class="grid mini ws-child-table"><thead><tr><th>Candidate</th><th>Day/date</th><th>Problem</th><th>Action</th></tr></thead><tbody>\${rows || '<tr><td colspan="4" class="ws-empty">Nothing is blocking this correction.</td></tr>'}</tbody></table>\${paid}</div>\`;`);
edit('correction paid caller retained by exact session and role state',`    const kind = 'weekly-source-correct-final-v1';
    const initiatingActor = typeof SESSION !== 'undefined' ? SESSION?.user?.id : null;
    let initiatingFrame = null;
    const callerCurrent = () => protectedCaller(kind, initiatingActor, initiatingFrame)();`,`    const kind = 'weekly-source-correct-final-v1';
    const initiatingSession = typeof SESSION !== 'undefined' ? SESSION : null;
    const initiatingUser = initiatingSession?.user;
    const initiatingActor = initiatingUser?.id;
    const roles = () => JSON.stringify({role:initiatingUser?.role,roles:initiatingUser?.roles,active:initiatingUser?.active,
      payment_authoriser:initiatingUser?.payment_authoriser,payment_golden_key:initiatingUser?.payment_golden_key});
    const initialRoles = roles();
    const initiatingAuthority = workspaceApi();
    let initiatingFrame = null;
    const callerCurrent = () => (typeof SESSION !== 'undefined' ? SESSION : null) === initiatingSession
      && initiatingSession?.user === initiatingUser && roles() === initialRoles && workspaceApi() === initiatingAuthority
      && protectedCaller(kind, initiatingActor, initiatingFrame)();`);
edit('actual reviewed correction child uses same existing paid dialog',`      host.querySelector('[data-wsa-review-correction]')?.addEventListener('click', review);`,`      host.querySelector('[data-wsa-review-correction]')?.addEventListener('click', review);
      host.querySelectorAll('[data-wsa-correction-paid-match]').forEach(button => button.addEventListener('click', () => {
        if (!callerCurrent() || state.busy || state.pendingReview || state.pendingApply) return;
        const index = Number(button.dataset.wsaCorrectionPaidMatch), item = state.preview?.paid_match_questions?.[index];
        if (!Number.isSafeInteger(index) || index<0 || !item) return;
        const context = state.preview.review_context, q = item.paid_match_question;
        const payload = { ...item, upload_row_id:q.upload_row_id,contract_id:q.contract_id,
          recheck_payload:{request_id:root.crypto.randomUUID(),upload_id:q.upload_id,upload_row_id:q.upload_row_id,
            projection_publication_id:q.projection_publication_id,
            expected_authority_scope_version:q.expected_authority_scope_version,expected_row_manifest_hash:q.expected_row_manifest_hash,
            correction_session_id:context.correction_session_id,expected_correction_session_version:context.expected_session_version,
            expected_comparison_manifest_hash:context.expected_comparison_manifest_hash,expected_issue_set_hash:context.expected_issue_set_hash}};
        openPaidMatch(payload, result => {
          if (!callerCurrent()) return;
          const reviewed=result?.correction_review;
          if (reviewed?.correction_session_id!==context.correction_session_id
            || reviewed?.review_context?.replacement_upload_id!==q.upload_id
            || reviewed?.review_context?.replacement_projection_publication_id!==result.publication_id) return;
          state.preview=normaliseCorrectFinalPreview(reviewed);state.phase='reviewed';
          state.active_tab=state.preview.blockers.length?'blocked':'changes';
          state.pendingReview=null;state.pendingApply=null;state.confirmed=false;state.reason='';state.error='';
          rerender(kind);
        });
      }));`);
edit('closed correction payload extends the six existing request keys only',`    const requestKeys = ['request_id','upload_id','upload_row_id','projection_publication_id',
      'expected_authority_scope_version','expected_row_manifest_hash'];`,`    const ordinaryRequestKeys = ['request_id','upload_id','upload_row_id','projection_publication_id',
      'expected_authority_scope_version','expected_row_manifest_hash'];
    const correction = Object.hasOwn(asObject(payload.recheck_payload),'correction_session_id');
    const requestKeys = correction ? [...ordinaryRequestKeys,'correction_session_id','expected_correction_session_version',
      'expected_comparison_manifest_hash','expected_issue_set_hash'] : ordinaryRequestKeys;`);
edit('correction request binds actual review context fields and all base question identities',`      || !requestKeys.slice(1).every(key => request[key] === question[key])
      || payload.upload_row_id !== question.upload_row_id || payload.contract_id !== question.contract_id) invalid();`,`      || !ordinaryRequestKeys.slice(1).every(key => request[key] === question[key])
      || (correction && (!uuid(request.correction_session_id) || !Number.isSafeInteger(request.expected_correction_session_version)
        || request.expected_correction_session_version<1 || !hash(request.expected_comparison_manifest_hash) || !hash(request.expected_issue_set_hash)))
      || payload.upload_row_id !== question.upload_row_id || payload.contract_id !== question.contract_id) invalid();`);
edit('same dialog constructor accepts a finite correction completion consumer',`  function openPaidMatch(payloadValue) {`,`  function openPaidMatch(payloadValue, onCorrectionConfirmed = null) {`);
edit('paid dialog keeps actual session/user and role state before and after awaits',`    const kind = 'weekly-source-paid-match-v1';
    const initiatingActor = typeof SESSION !== 'undefined' ? SESSION?.user?.id : null;
    let initiatingFrame = null;
    const authority = workspaceApi();
    const callerCurrent = () => workspaceApi() === authority && protectedCaller(kind, initiatingActor, initiatingFrame)();`,`    const kind = 'weekly-source-paid-match-v1';
    const initiatingSession = typeof SESSION !== 'undefined' ? SESSION : null;
    const initiatingUser = initiatingSession?.user, initiatingActor = initiatingUser?.id;
    const roles = () => JSON.stringify({role:initiatingUser?.role,roles:initiatingUser?.roles,active:initiatingUser?.active,
      payment_authoriser:initiatingUser?.payment_authoriser,payment_golden_key:initiatingUser?.payment_golden_key});
    const initialRoles = roles();
    let initiatingFrame = null;
    const authority = workspaceApi();
    const callerCurrent = () => (typeof SESSION !== 'undefined' ? SESSION : null) === initiatingSession
      && initiatingSession?.user === initiatingUser && roles() === initialRoles
      && workspaceApi() === authority && protectedCaller(kind, initiatingActor, initiatingFrame)();`);
edit('genuine CORRECTION_READY receipt is not relabelled CURRENT',`          if (result?.ok !== true || result?.status !== 'CURRENT'
            || result?.upload_id !== state.pending.upload_id`,`          const correction = Object.hasOwn(state.pending,'correction_session_id');
          if (result?.ok !== true || result?.status !== (correction ? 'CORRECTION_READY' : 'CURRENT')
            || result?.upload_id !== state.pending.upload_id`);
edit('actual native review restores original parent child without an extra rebuild',`            || result.publication_id === state.pending.projection_publication_id) throw new Error('The saved comparison is not yet confirmed. Retry exact request.');
          await finishAction(undefined, callerCurrent);`,`            || result.publication_id === state.pending.projection_publication_id) throw new Error('The saved comparison is not yet confirmed. Retry exact request.');
          if (correction) {
            const reviewed=result.correction_review;
            if (typeof onCorrectionConfirmed!=='function' || reviewed?.ok!==true
              || !['BLOCKED','READY_FOR_CONFIRMATION'].includes(reviewed.status)
              || reviewed.correction_session_id!==state.pending.correction_session_id
              || reviewed.review_context?.replacement_projection_publication_id!==result.publication_id
              || reviewed.review_context?.replacement_upload_id!==state.pending.upload_id)
              throw new Error('The saved correction review is not yet confirmed. Retry exact request.');
            markCompletedChildClean();closeChild();onCorrectionConfirmed(result);
          } else await finishAction(undefined, callerCurrent);`);
let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);
const file='frontend/js/weekly-source/workspace-actions.js';for(const side of['before','forward'])fs.mkdirSync(path.dirname(path.join(dir,side,file)),{recursive:true});
fs.writeFileSync(path.join(dir,'before',file),raw);fs.writeFileSync(path.join(dir,'forward',file),after);
fs.writeFileSync(path.join(dir,'COMPLETE_OFFICE_REVERSE.json'),JSON.stringify({source_only:true,source,containing_commit:'65c82dcc6d8f2748fef7c4209d96a673f7ec8756',
 file,before_bytes:raw.length,before_sha256:sha(raw),after_bytes:Buffer.byteLength(after),after_sha256:sha(after),hunks,whole_reverse_verified:true,
 current_root_Report32_Money28_NativeR254_SESSION_union_must_be_preserved:true,native_proof:false},null,2)+'\n');
console.log(JSON.stringify({whole_Office_reverse:true,unique_hunks:hunks.length,source_only:true}));
