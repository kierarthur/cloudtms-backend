import assert from 'node:assert/strict';
import test from 'node:test';
import { BANKING_PAY_MODAL_CONTRACT, dispatchBankingPayModalV2Request } from '../broker/src/banking-pay-modal-v2.js';

const session = '00000000-0000-4000-8000-000000000001';
const candidate = '00000000-0000-4000-8000-000000000002';
const actor = '00000000-0000-4000-8000-000000000003';
const intent = '00000000-0000-4000-8000-000000000004';
const base = `/api/banking/pay/workbench/v2/session/${session}`;
const query = { expected_session_version: 2, expected_progress_counter_version: 3, scope_hash: 'a'.repeat(64), pay_channel_scope: 'ALL' };
const row = () => ({ candidate_id: candidate, candidate_name: 'Test candidate', candidate_reference: 'FIXTURE', candidate_sort_name: 'test candidate', candidate_sort_reference: 'fixture', child_revision: 'revision-token', facts_digest:'b'.repeat(64), selectable_ready_count: 2, selected_ready_count: 0, selection_state: 'NONE', selected_display_amount: '0.00', selected_deduction_exists: false, selected_timesheet_count: 0, selected_timesheet_ids: [], selected_timesheet_scope_token: null });
const global = () => ({ candidate_count: 1, selected_candidate_count: 0, selected_ready_count: 0, selectable_ready_count: 2, selection_state: 'NONE', selected_ready_display_amount: '0.00', action_required_count: 0, updating_count: 0, blocked_count: 0,
  draft: { can_create_draft: false, blocker_codes: ['NO_SELECTED_ROWS'],session_ready:true,read_only:false,work_queued:false,
    display_ready:true,draft_safe:true,draft_block_reason_code:null,session_selected_row_count:0,session_selected_eligible_ready_row_count:0 } });
const envelope = extra => ({ ok: true, contract: BANKING_PAY_MODAL_CONTRACT, contract_version: 1, session_id: session, session_version: 2, progress_counter_version: 3, scope_hash: query.scope_hash, ...extra });
const summary = () => envelope({ view_digest:'c'.repeat(64), global: global(), rows: [row()], sort_key: 'CANDIDATE', sort_direction: 'ASC', next_cursor: null, has_more: false, total_count: 1,
  page_number:1,has_previous:false,previous_cursor:null,page_anchor:'current_page_anchor',next_page_anchor:null,previous_page_anchor:null });
const selection = () => envelope({ progress_counter_version: 4, request_id: intent, state_changed: true, global: global(), candidate_absent: false, candidate: row(),
 view_digest:'d'.repeat(64),retention:{before_view_digest:'c'.repeat(64),other_candidates_unchanged:true,membership_unchanged:true,
 candidate_sort_unchanged:true,amount_sort_unchanged:false,deduction_sort_unchanged:true},
 movements: [],movements_complete:true,movement_count:0,movement_digest:'b'.repeat(64),
 invalidations:{scope:'ALL_PREVIOUS_DETAILS',ready:true,actions:true,updating:true,blocked:true} });
const ready = () => envelope({ candidate_id: candidate, candidate: row(),
  rows: ['ready-1','ready-2'].map(identity=>({identity,candidate_id:candidate,effective_section:'canonical_preview_lines',selected:false,
    selection_group_kind:null,selection_group_key:null,selection_group_member_count:0,selection_group_selected_count:0,selection_group_state:null,
    selection_group_display_amount:null,selection_group_selected_display_amount:null})),
  next_cursor: null, has_more: false, total_count: 2, page_number:1, has_previous:false, previous_cursor:null, page_anchor:'current_anchor' });
async function invoke({ path = `${base}/candidates`, params = query, body, rpc = async () => summary(), user = { id: actor }, method = body ? 'POST' : 'GET' } = {}) {
  let calls = 0;
  let called;
  const url = `https://test.invalid${path}${body ? '' : `?${new URLSearchParams(params)}`}`;
  const req = new Request(url, { method, ...(body ? { body: JSON.stringify(body), headers: { 'content-type': 'application/json' } } : {}) });
  const res = await dispatchBankingPayModalV2Request({ req, user, rpc: async (...args) => { calls++; called = args; return rpc(...args); } });
  return { status: res?.status, json: res ? await res.json() : null, calls, called };
}
for(const [name,change] of Object.entries({
  falseEmpty:payload=>{payload.rows=[];},
  firstAndFinalShort:payload=>{payload.rows.pop();},
  rowsExceedTotal:payload=>{payload.candidate={...payload.candidate,selectable_ready_count:1};payload.total_count=1;},
  incompleteContinuation:payload=>{payload.has_more=true;payload.next_cursor='next';payload.total_count=101;}
}))test(`Ready response rejects ${name} before publishing`,async()=>{
  const payload=ready();change(payload);
  const result=await invoke({path:`${base}/candidate/${candidate}/ready`,rpc:async()=>payload});
  assert.equal(result.status,502);assert.equal(result.json.code,'BANKING_PAY_V2_INVALID_RESPONSE');
});

test('unrelated requests do not dispatch or touch a database', async () => {
  const result = await invoke({ path: '/api/banking/other' });
  assert.equal(result.json, null);
  assert.equal(result.calls, 0);
});
test('capability remains false until acceptance; no second contract RPC', async () => {
  const result = await invoke({ path: '/api/banking/pay/workbench/v2/capability', params: {} });
  assert.deepEqual(result.json, { banking_pay_workbench_v2: { available: false, contract_version: 1 } });
  assert.equal(result.calls, 0);
});
test('summary uses one exact RPC and preserves decimal strings unchanged', async () => {
  const result = await invoke();
  assert.equal(result.status, 200);
  assert.equal(result.calls, 1);
  assert.equal(result.called[0], 'pay_workbench_session_get_candidate_summary_page_v1');
  assert.equal(result.called[1].p_limit, 100);
  assert.equal(result.called[1].p_actor_user_id, actor);
  assert.equal(result.json.rows[0].selected_display_amount, '0.00');
});

for(const [name,change] of Object.entries({
  truncated:p=>p.rows.pop(),
  falseLast:p=>{p.has_more=false;p.next_cursor=null;p.next_page_anchor=null;},
  inventedMore:p=>{p.total_count=100;p.global.candidate_count=100;}
}))test('Worker rejects incomplete100-row summary '+name,async()=>{
  const p=summary();
  p.rows=Array.from({length:100},(_,i)=>({...row(),candidate_id:'00000000-0000-4000-8000-'+String(i+100).padStart(12,'0')}));
  Object.assign(p,{total_count:101,has_more:true,next_cursor:'next',next_page_anchor:'next_anchor'});
  Object.assign(p.global,{candidate_count:101,selectable_ready_count:202});change(p);
  const result=await invoke({rpc:async()=>p});assert.equal(result.status,502);
});
test('first summary obtains its scope from the server without a second context request',async()=>{
  const params={...query};delete params.scope_hash;
  const result=await invoke({params});
  assert.equal(result.status,200);assert.equal(result.calls,1);
  assert.equal(result.called[1].p_options_json.scope_hash,null);
  assert.equal(result.json.scope_hash,query.scope_hash);
});
test('a missing scope cannot renew an existing page or enter candidate details',async()=>{
  const params={...query};delete params.scope_hash;
  for(const request of [{params:{...params,cursor:'old_page'}},{path:`${base}/candidate/${candidate}/ready`,params}]){
    const result=await invoke(request);assert.equal(result.status,400);assert.equal(result.calls,0);
  }
});
test('initial server scope is still mandatory and correctly typed in the response',async()=>{
  const params={...query};delete params.scope_hash;
  for(const scope of [undefined,null,'',12,'invalid','A'.repeat(64)]){
    const result=await invoke({params,rpc:async()=>({...summary(),scope_hash:scope})});
    assert.equal(result.status,502);assert.equal(result.calls,1);
  }
});
for (const [name, params] of Object.entries({
  search: { ...query, search: 'new search is prohibited' },
  extraFinancialInput: { ...query, amount: 10 },
  oldSort: { ...query, sort_key: 'TIMESHEETS' },
  badDirection: { ...query, sort_direction: 'descending' },
  excessivePage: { ...query, limit: 101 }, zeroPage: { ...query, limit: 0 },
  fractionalPage: { ...query, limit: 1.5 }, badCursor: { ...query, cursor: '../elsewhere' },
  missingVersion: { ...query, expected_session_version: undefined },
  unsafeVersion: { ...query, expected_session_version: '99999999999999999999' },
  invalidChannel: { ...query, pay_channel_scope: 'LIVE' }, badScope: { ...query, scope_hash: 'anything' },
  shortScope: { ...query, scope_hash: 'a'.repeat(32) }
})) test(`rejects ${name} without submitting an RPC`, async () => {
  const result = await invoke({ params });
  assert.equal(result.status, 400);
  assert.equal(result.calls, 0);
  assert.equal(result.json.outcome, 'NOT_SUBMITTED');
});
test('authentication is required before any RPC', async () => {
  const result = await invoke({ user: null });
  assert.equal(result.status, 401); assert.equal(result.calls, 0);
});
for (const [name, modify] of Object.entries({
  wrongSession: x => { x.session_id = candidate; },
  missingViewEvidence: x=>{delete x.view_digest;},
  invalidViewEvidence: x=>{x.view_digest='not-a-digest';},
  missingFactsEvidence: x=>{delete x.rows[0].facts_digest;},
  invalidFactsEvidence: x=>{x.rows[0].facts_digest=null;},
  staleRevision: x => { x.progress_counter_version = 2; },
  wrongScope: x => { x.scope_hash = 'b'.repeat(64); },
  duplicateCandidate: x => { x.rows.push(row()); },
  floatAmount: x => { x.rows[0].selected_display_amount = 12.34; },
  undefinedAmount: x => { delete x.rows[0].selected_display_amount; },
  negativeZero: x => { x.rows[0].selected_display_amount = '-0.00'; },
  missingSelectedCandidateCount: x => { delete x.global.selected_candidate_count; },
  wrongTotalCount: x => { x.total_count = 2; },
  wrongGlobalSelection: x => { x.global.selection_state = 'ALL'; },
  draftWithoutSelectedPayments: x => { x.global.draft.can_create_draft = true; },
  rowExceedsGlobalScope: x => { x.rows[0].selectable_ready_count = 3; },
  emptySelectedDeduction: x => { x.rows[0].selected_deduction_exists = true; },
  ineligiblePlaceholder: x => { x.rows[0].selectable_ready_count = 0; },
  wrongHalfTick: x => { x.rows[0].selection_state = 'SOME'; },
  untickedTimesheet: x => { x.rows[0].selected_timesheet_ids = [candidate]; },
  duplicateTimesheets: x => { x.rows[0].selected_ready_count = 2; x.rows[0].selection_state = 'ALL'; x.rows[0].selected_timesheet_count = 2; x.rows[0].selected_timesheet_ids = [candidate, candidate]; },
  missingCursor: x => { x.has_more = true; },
  missingAnchor: x => { delete x.page_anchor; },
  invalidAnchor: x => { x.page_anchor='../invalid'; },
  missingNextAnchor: x => { delete x.next_page_anchor; },
  unexpectedNextAnchor: x => { x.next_page_anchor='next'; },
  missingPreviousAnchor: x => { delete x.previous_page_anchor; },
  unexpectedPreviousAnchor: x => { x.previous_page_anchor='previous'; },
  missingPreviousFlag: x => { delete x.has_previous; },
  incorrectPreviousFlag: x => { x.has_previous=true; },
  unexpectedPreviousCursor: x => { x.previous_cursor='previous_page'; },
  missingPreviousCursor: x => { delete x.previous_cursor; },
  missingPageNumber: x => { delete x.page_number; },
  emptyPageNumberForRows: x => { x.page_number=0; },
  pageOutsideResult: x => { x.page_number=2;x.has_previous=true; },
  wrongSort: x => { x.sort_key = 'TIMESHEETS'; }
})) test(`fails closed for ${name} without a second RPC`, async () => {
  const result = await invoke({ rpc: async () => { const data = summary(); modify(data); return data; } });
  assert.equal(result.status, 502); assert.equal(result.calls, 1);
  assert.equal(result.json.code, 'BANKING_PAY_V2_INVALID_RESPONSE');
});
test('selected Timesheets over 25 use a token, never a partial ID page', async () => {
  const result = await invoke({ rpc: async () => {
    const data = summary();
    Object.assign(data.rows[0], { selectable_ready_count: 26, selected_ready_count: 26, selection_state: 'ALL', selected_timesheet_count: 26, selected_timesheet_scope_token: 'opaque_selected_scope' });
    Object.assign(data.global, { selectable_ready_count:26, selected_ready_count:26, selected_candidate_count:1, selection_state:'ALL',
      draft:{...data.global.draft,can_create_draft:true,blocker_codes:[],session_selected_row_count:26,session_selected_eligible_ready_row_count:26} });
    return data;
  } });
  assert.equal(result.status, 200);
});
test('candidate selection submits one explicit scope-fenced intent', async () => {
  const result = await invoke({ path: `${base}/candidate/${candidate}/selection`, body: { ...query, action: 'CLEAR_ALL_READY', request_id: intent, expected_view_digest:'c'.repeat(64) }, rpc: async () => selection() });
  assert.equal(result.status, 200); assert.equal(result.calls, 1);
  assert.equal(result.called[0], 'pay_workbench_session_set_candidate_ready_selection_v1');
  assert.equal(result.called[1].p_request_id, intent);
  assert.equal(result.called[1].p_action, 'CLEAR_ALL_READY');
});
test('lost mutation response requires read-back, never a blind retry', async () => {
  const result = await invoke({ path: `${base}/candidate/${candidate}/selection`, body: { ...query, action: 'SELECT_ALL_READY', request_id: intent, expected_view_digest:'c'.repeat(64) }, rpc: async () => { throw new Error('sensitive upstream content must not escape'); } });
  assert.equal(result.status, 503); assert.equal(result.calls, 1);
  assert.equal(result.json.outcome, 'UNCERTAIN'); assert.equal(result.json.read_back_required, true);
  assert.equal(result.json.retry_mutation, false);
  assert.doesNotMatch(JSON.stringify(result.json), /sensitive/);
});
test('invalid accepted mutation envelope is also uncertain', async () => {
  const result = await invoke({ path: `${base}/candidate/${candidate}/selection`, body: { ...query, action: 'SELECT_ALL_READY', request_id: intent, expected_view_digest:'c'.repeat(64) }, rpc: async () => ({}) });
  assert.equal(result.json.outcome, 'UNCERTAIN'); assert.equal(result.json.read_back_required, true);
});
test('typed stale rejection remains a rejection', async () => {
  const result = await invoke({ path: `${base}/candidate/${candidate}/selection`, body: { ...query, action: 'SELECT_ALL_READY', request_id: intent, expected_view_digest:'c'.repeat(64) }, rpc: async () => ({ ok: false, code: 'BANKING_PAY_V2_STALE_REVISION' }) });
  assert.equal(result.status, 409); assert.equal(result.json.outcome, 'REJECTED');
});
test('missing dependency is not a false payment validation', async () => {
  const result = await invoke({ rpc: async () => { throw { code: 'PGRST202' }; } });
  assert.equal(result.status, 503);
  assert.equal(result.json.code, 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE');
});
test('Ready details cannot leak another candidate or section', async () => {
  for (const change of [{ candidate_id: actor }, { effective_section: 'blocked_for_pay' }]) {
    const result = await invoke({ path: `${base}/candidate/${candidate}/ready`, rpc: async () => { const data=ready(); Object.assign(data.rows[0],change); return data; } });
    assert.equal(result.status, 502);
  }
});
for(const [name,change] of Object.entries({missingKind:r=>delete r.selection_group_kind,missingKey:r=>delete r.selection_group_key,
 partialNull:r=>r.selection_group_member_count=1,badKind:r=>Object.assign(r,{selection_group_kind:'ROW',selection_group_key:'group',selection_group_member_count:1,selection_group_selected_count:0,selection_group_state:'NONE'}),
 inconsistentState:r=>Object.assign(r,{selection_group_kind:'TIMESHEET',selection_group_key:'group',selection_group_member_count:2,selection_group_selected_count:1,selection_group_state:'ALL'}),
 controlKey:r=>Object.assign(r,{selection_group_kind:'OVERPAYMENT',selection_group_key:'bad\nkey',selection_group_member_count:1,selection_group_selected_count:1,selection_group_state:'ALL'})}))
 test('Ready details reject incomplete complete-group authority: '+name,async()=>{
  const result=await invoke({path:`${base}/candidate/${candidate}/ready`,rpc:async()=>{const data=ready();change(data.rows[0]);return data;}});
  assert.equal(result.status,502);assert.equal(result.calls,1);
 });
test('Ready detail carries complete candidate authority in the same single RPC', async () => {
  const result=await invoke({path:`${base}/candidate/${candidate}/ready`,params:{...query,limit:1},
    rpc:async()=>({...ready(),rows:ready().rows.slice(0,1),has_more:true,next_cursor:'next_ready_page'})});
  assert.equal(result.status,200);assert.equal(result.calls,1);
  assert.equal(result.json.candidate.selectable_ready_count,2);
  assert.equal(result.json.rows.length,1);
  assert.equal(result.json.candidate.selected_display_amount,'0.00');
});
test('a genuine last Ready continuation page may contain fewer rows than its complete candidate count',async()=>{
  const result=await invoke({path:`${base}/candidate/${candidate}/ready`,params:{...query,cursor:'current_ready_page',limit:1},
    rpc:async()=>({...ready(),rows:ready().rows.slice(1),page_number:2,has_previous:true})});
  assert.equal(result.status,200);assert.equal(result.calls,1);
  assert.equal(result.json.rows.length,1);assert.equal(result.json.total_count,2);
  assert.equal(result.json.candidate.selectable_ready_count,2);
});
for(const [name,change] of Object.entries({
  missing:data=>{delete data.candidate;},
  wrongOwner:data=>{data.candidate.candidate_id=actor;},
  floatAmount:data=>{data.candidate.selected_display_amount=0;},
  partialCount:data=>{data.candidate.selectable_ready_count=3;},
  absentWithRows:data=>{data.candidate=null;},
  absentWithCount:data=>{data.candidate=null;data.rows=[];},
  stringSelection:data=>{data.rows[0].selected='false';}
}))test(`Ready detail rejects ${name} candidate authority without a follow-up RPC`,async()=>{
  const result=await invoke({path:`${base}/candidate/${candidate}/ready`,rpc:async()=>{const data=ready();change(data);return data;}});
  assert.equal(result.status,502);assert.equal(result.calls,1);
});
test('Ready detail can report a departed candidate without leaking context-only rows',async()=>{
  const result=await invoke({path:`${base}/candidate/${candidate}/ready`,rpc:async()=>({...ready(),candidate:null,rows:[],total_count:0,page_number:0,page_anchor:null})});
  assert.equal(result.status,200);assert.equal(result.calls,1);assert.equal(result.json.candidate,null);
});
test('PostgREST SQLSTATE errors retain the whitelisted business rejection only', async () => {
  const result = await invoke({ rpc: async () => { throw { json: { code: 'P0001', message: 'BANKING_PAY_V2_STALE_REVISION', details: 'private SQL content' } }; } });
  assert.equal(result.status, 409); assert.equal(result.json.code, 'BANKING_PAY_V2_STALE_REVISION');
  assert.doesNotMatch(JSON.stringify(result.json), /private SQL/);
});
test('large JSON body is bounded and never submitted', async () => {
  const result = await invoke({ path: `${base}/candidate/${candidate}/selection`, body: { ...query, action: 'SELECT_ALL_READY', request_id: intent, expected_view_digest:'c'.repeat(64), oversized: 'x'.repeat(9000) } });
  assert.equal(result.status, 413); assert.equal(result.calls, 0);
});

// BP-102/BP-121: list-local navigation is not a main-table/Draft scope filter.
for (const [kind, segment, keys] of [
  ['actions', 'action-required', ['TITLE', 'CANDIDATES', 'PAYMENTS']],
  ['blocked', 'blocked', ['CANDIDATE', 'REASON', 'AMOUNT']]
]) {
  const list = (search = '', sort_key = keys[0], sort_direction = 'ASC') => envelope({
    rows: [], total_count: 0, scope_count: search ? 9 : 0, next_cursor: null, has_more: false,
    page_number: 0, has_previous: false, previous_cursor: null,
    search, sort_key, sort_direction, ...(kind === 'actions' ? { view:'ACTION_REQUIRED',updating_count: 2,
      updating: [1,2].map(n=>({identity:'updating_'+n,issue_state:'UPDATING',title:'Refreshing…',affected_candidate_count:1,
        affected_payment_count:null,affected_payment_count_complete:false})),updating_has_more:false,updating_next_cursor:null } : {})
  });
  for (const key of keys) for (const direction of ['ASC', 'DESC']) {
    test(`${kind} retains bounded list-only search and ${key} ${direction}`, async () => {
      const result = await invoke({ path: `${base}/${segment}`,
        params: { ...query, search: '  Test %_ candidate  ', sort_key: key, sort_direction: direction },
        rpc: async () => list('Test %_ candidate', key, direction) });
      assert.equal(result.status, 200); assert.equal(result.calls, 1);
      assert.equal(result.called[1].p_search, 'Test %_ candidate');
      assert.equal(result.called[1].p_sort_key, key); assert.equal(result.called[1].p_sort_direction, direction);
      assert.deepEqual(result.called[1].p_options_json, query, 'List search must never change current Draft filters');
      assert.equal(result.json.scope_count, 9); assert.equal(result.json.total_count, 0);
    });
  }
  test(`${kind} has explicit empty-search/default-sort contracts`, async () => {
    const result = await invoke({ path: `${base}/${segment}`, rpc: async () => list() });
    assert.equal(result.status, 200); assert.equal(result.called[1].p_search, '');
    assert.equal(result.called[1].p_sort_key, keys[0]);
  });
  for (const [label, change] of Object.entries({
    oversized: { search: 'x'.repeat(201) }, control: { search: 'candidate\u0000' },
    newline: { search: 'candidate\nother' }, arbitrarySort: { sort_key: 'selected;DELETE' },
    wrongDirection: { sort_direction: 'desc' }
  })) test(`${kind} rejects ${label} without an RPC`, async () => {
    const result = await invoke({ path: `${base}/${segment}`, params: { ...query, ...change } });
    assert.equal(result.status, 400); assert.equal(result.calls, 0);
  });
  for (const [label, change] of Object.entries({
    search: { search: 'a different list' }, sort: { sort_key: 'UNKNOWN' },
    direction: { sort_direction: 'DESC' }, scopeCount: { scope_count: -1 },
    missingScopeCount: { scope_count: undefined }, matchedCount: { total_count: 10 }
  })) test(`${kind} rejects mismatched ${label} response without partial adoption`, async () => {
    const result = await invoke({ path: `${base}/${segment}`, rpc: async () => ({ ...list(), ...change }) });
    assert.equal(result.status, 502); assert.equal(result.calls, 1);
  });
}

test('list-only query inputs cannot enter candidate details, Timesheets or mutations', async () => {
  for (const suffix of [`candidate/${candidate}/ready`, 'action-required/task_key', 'blocked/blocker_key']) {
    const result = await invoke({ path: `${base}/${suffix}`, params: { ...query, search: 'no widening' } });
    assert.equal(result.status, 400); assert.equal(result.calls, 0);
  }
  const result = await invoke({ path: `${base}/candidate/${candidate}/selection`,
    body: { ...query, action: 'CLEAR_ALL_READY', request_id: intent, expected_view_digest:'c'.repeat(64), search: 'no selection filter' } });
  assert.equal(result.status, 400); assert.equal(result.calls, 0);
});
test('Updating uses one explicit view on the Action read without changing Draft options',async()=>{
 const task={identity:'updating_one',issue_state:'UPDATING',title:'Refreshing…',affected_candidate_count:1,
  affected_payment_count:null,affected_payment_count_complete:false};
 const result=await invoke({path:`${base}/action-required`,params:{...query,view:'UPDATING'},rpc:async()=>envelope({
  view:'UPDATING',search:'',sort_key:'TITLE',sort_direction:'ASC',rows:[task],total_count:1,scope_count:1,
  page_number:1,has_previous:false,previous_cursor:null,has_more:false,next_cursor:null,
  updating_count:1,updating:[task],updating_has_more:false,updating_next_cursor:null})});
 assert.equal(result.status,200);assert.equal(result.calls,1);
 assert.equal(result.called[0],'pay_workbench_session_get_action_required_page_v1');
 assert.equal(result.called[1].p_view,'UPDATING');assert.deepEqual(result.called[1].p_options_json,query);
});
for(const change of [{view:'arbitrary'},{view:'UPDATING',search:'candidate'},
 {view:'UPDATING',sort_key:'PAYMENTS'},{view:'UPDATING',sort_direction:'DESC'}])
test(`invalid Updating request cannot reach RPC: ${JSON.stringify(change)}`,async()=>{
 const result=await invoke({path:`${base}/action-required`,params:{...query,...change}});
 assert.equal(result.status,400);assert.equal(result.calls,0);
});
