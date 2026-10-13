import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),input=process.argv[2];
assert.ok(input,'explicit read-only invoice-batch-modal input required');
const bytes=fs.readFileSync(input),native=bytes.toString('utf8').replace(/\r\n/g,'\n');let next=native;const hunks=[];
function change(anchor,replacement){assert.equal(next.split(anchor).length,2,'changed AI Office anchor '+anchor.slice(0,60));hunks.push({anchor,replacement});next=next.replace(anchor,replacement);}
change('      weekly_source_rows: [],','      weekly_source_rows: [],\n      healthroster_credit_notes: null,\n      credit_note_results: [],');
change('      state.candidate_page = payload;','      state.candidate_page = payload;\n      if(state.mode===\'GENERATE\') {\n        state.healthroster_credit_notes=payload.healthroster_credit_notes||null;\n        window.CloudTmsAutomaticCreditNotes?.rows(state.healthroster_credit_notes);\n      }');
change("    if (!ordinary && !source) return '<div class=\"invbatch-empty\">No matching items.</div>';\n    return `${source}${ordinary}`;", "    const credits=state.mode==='GENERATE'?window.CloudTmsAutomaticCreditNotes?.render(state.healthroster_credit_notes,escapeHtml)||'':'';\n    if (!ordinary && !source && !credits) return '<div class=\"invbatch-empty\">No matching items.</div>';\n    return `${credits}${source}${ordinary}`;");
change("${(selected.exact && selected.count > 0 && !state.submitting) ? '' : 'disabled'}", "${(selected.exact && (selected.count > 0 || (state.mode==='GENERATE' && window.CloudTmsAutomaticCreditNotes?.rows(state.healthroster_credit_notes).length>0)) && !state.submitting) ? '' : 'disabled'}");
change('      <h3>Generate selected items?</h3>','      <h3>Generate selected items?</h3>\n      ${state.healthroster_credit_notes?`<p>${window.CloudTmsAutomaticCreditNotes.rows(state.healthroster_credit_notes).length.toLocaleString(\'en-GB\')} credit notes will also be prepared automatically. Any blocked credit remains visible in the invoice Outbox.</p>`:\'\'}');
change('    if (state.source_submission_complete && !state.root_operation_id) body = renderWeeklySourceSubmissionResults(state, { close: true });',`    if (state.credit_note_results?.length && !state.root_operation_id && !state.weekly_source_submission_results?.length)
      body=window.CloudTmsAutomaticCreditNotes.renderResults(state.credit_note_results,escapeHtml)+'<button type="button" class="btn btn-outline" data-batch-action="close">Close</button>';
    else if (state.source_submission_complete && !state.root_operation_id) body = renderWeeklySourceSubmissionResults(state, { close: true });`);
change('    root.innerHTML = body;','    root.innerHTML = body+(state.root_operation_id?window.CloudTmsAutomaticCreditNotes?.renderResults(state.credit_note_results,escapeHtml)||\'\':\'\');');
change('    state.weekly_source_rows = [];','    state.weekly_source_rows = [];\n    state.healthroster_credit_notes=null;\n    state.credit_note_results=[];');
change('      state.weekly_source_submission_results = asArray(payload.weekly_source_per_row_results);\n      state.source_submission_complete = state.weekly_source_submission_results.length > 0 && !state.root_operation_id;',`      state.credit_note_results=asArray(payload.credit_note_results);
      window.CloudTmsAutomaticCreditNotes?.register(state.credit_note_results,window.registerInvoiceOperationsFromResponse,{modal_identity:state.id});
      state.weekly_source_submission_results = asArray(payload.weekly_source_per_row_results);
      state.source_submission_complete = state.weekly_source_submission_results.length > 0 && !state.root_operation_id;`);
const proposed=path.join(here,'invoice-batch-modal.complete-proposed.js');fs.writeFileSync(proposed,next);
const diff=spawnSync('git',['diff','--no-index','--',input,proposed],{encoding:'utf8',windowsHide:true,maxBuffer:8*1024*1024});assert.ok([0,1].includes(diff.status),diff.stderr);
const lines=diff.stdout.split('\n');fs.writeFileSync(path.join(here,'AI_OFFICE_ROOT.diff'),'--- a/js/invoice-batch-modal.js\n+++ b/js/invoice-batch-modal.js\n'+lines.slice(lines.findIndex(line=>line.startsWith('@@'))).join('\n'));
const sha=v=>crypto.createHash('sha256').update(v).digest('hex');
fs.writeFileSync(path.join(here,'AI_OFFICE_SOURCE_GUARD.json'),JSON.stringify({input:{path:input,sha256:sha(bytes)},output:{path:'invoice-batch-modal.complete-proposed.js',sha256:sha(Buffer.from(next))},
  mount:'UNMOUNTED',requiredScriptBeforeBatch:'js/weekly-source/agency-invoice-credit-ui.js',hunks},null,2)+'\n');
console.log(JSON.stringify({status:'SOURCE_GENERATED',hunks:hunks.length}));
