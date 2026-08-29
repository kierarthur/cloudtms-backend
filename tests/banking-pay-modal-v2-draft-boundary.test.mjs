import assert from 'node:assert/strict';
import test from 'node:test';
import path from 'node:path';
import {createRequire} from 'node:module';
import {validateBankingPayModalEnvelope} from '../broker/src/banking-pay-modal-v2.js';
const require=createRequire(import.meta.url);
const frontend=process.env.BANKING_MODAL_FRONTEND_ROOT;
const enabled=Boolean(frontend);
const fixture=enabled?require(path.join(frontend,'tests/fixtures/banking-pay-v2-table-page.cjs')):null;
const table=enabled?require(path.join(frontend,'js/banking-pay-modal-v2-table.js')):null;
function page(){const value=fixture.page();Object.assign(value.global.draft,{
  session_ready:true,read_only:false,work_queued:false,display_ready:true,draft_safe:true,draft_block_reason_code:null,
  session_selected_row_count:4,session_selected_eligible_ready_row_count:4});return value;}
function worker(value){return validateBankingPayModalEnvelope(value,'summary',{p_session_id:value.session_id,
  p_options_json:{expected_session_version:value.session_version,expected_progress_counter_version:value.progress_counter_version,
    scope_hash:value.scope_hash,pay_channel_scope:'ALL'},p_sort_key:'CANDIDATE',p_sort_direction:'ASC',p_limit:100,p_cursor:null});}
const negatives={
  missingDisplay:d=>delete d.display_ready,missingScopeSafety:d=>delete d.draft_safe,
  missingReason:d=>delete d.draft_block_reason_code,missingSessionCount:d=>delete d.session_selected_row_count,
  missingEligibleCount:d=>delete d.session_selected_eligible_ready_row_count,missingCounterReady:d=>delete d.session_ready,
  missingReadOnly:d=>delete d.read_only,missingWorkQueued:d=>delete d.work_queued,
  stringSafety:d=>d.draft_safe='true',stringDisplay:d=>d.display_ready='true',objectReason:d=>d.draft_block_reason_code={},
  stringCount:d=>d.session_selected_row_count='4',negativeCount:d=>d.session_selected_eligible_ready_row_count=-1,
  fractionalCount:d=>d.session_selected_row_count=1.5,unsafeCount:d=>d.session_selected_row_count=Number.MAX_SAFE_INTEGER+1,
  earlyEnable:d=>Object.assign(d,{draft_safe:false,draft_block_reason_code:'SCOPE_RECONCILIATION_REQUIRED',blocker_codes:['SCOPE_RECONCILIATION_REQUIRED']}),
  safetyWithoutDisplay:d=>d.display_ready=false,safeWithBlockReason:d=>d.draft_block_reason_code='SCOPE_RECONCILIATION_REQUIRED',
  missingScopeBlocker:d=>Object.assign(d,{can_create_draft:false,draft_safe:false,draft_block_reason_code:'SCOPE_RECONCILIATION_REQUIRED'}),
  enableWithCounterBlocker:d=>d.blocker_codes=['WORKBENCH_REFRESH_PENDING'],enableWhileReadOnly:d=>d.read_only=true,
  enableWhileQueued:d=>d.work_queued=true,enableWithoutCounterReady:d=>d.session_ready=false,
  enableWithoutSessionSelection:d=>d.session_selected_row_count=0,
  enableWithoutEligibleSelection:d=>d.session_selected_eligible_ready_row_count=0,
  oversizedGate:d=>Object.assign(d,{can_create_draft:false,blocker_codes:['x'.repeat(2048)]})
};
for(const [label,mutate]of Object.entries(negatives))for(const boundary of ['worker','browser'])
 test(`${boundary} rejects inconsistent or incomplete Draft safety: ${label}`,{skip:!enabled},()=>{
  const value=page();mutate(value.global.draft);
  assert.throws(()=>boundary==='worker'?worker(value):table.validateSummary(value),/BANKING_PAY_V2_INVALID_RESPONSE/);
 });
for(const boundary of ['worker','browser'])test(`${boundary} preserves current and background-blocked Draft states`,{skip:!enabled},()=>{
  const current=page();assert.doesNotThrow(()=>boundary==='worker'?worker(current):table.validateSummary(current));
  const blocked=page();Object.assign(blocked.global.draft,{can_create_draft:false,draft_safe:false,
    draft_block_reason_code:'UPSTREAM_SCOPE_EXPANSION_IN_PROGRESS',blocker_codes:['UPSTREAM_SCOPE_EXPANSION_IN_PROGRESS']});
  assert.doesNotThrow(()=>boundary==='worker'?worker(blocked):table.validateSummary(blocked));
  assert.equal(blocked.global.draft.session_selected_row_count,4);
});
