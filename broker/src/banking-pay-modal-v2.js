/** Banking Pay Modal Structure Bible v2. Transport only: no payment arithmetic.
 * This module is called inside the existing admin-authenticated Banking router.
 * Capability stays off until SQL, action parity and browser acceptance are proved.
 */
export const BANKING_PAY_MODAL_CONTRACT = 'BANKING_PAY_MODAL_STRUCTURE_V2';
export const BANKING_PAY_MODAL_AVAILABLE = false;
const PREFIX = '/api/banking/pay/workbench/v2';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TOKEN = /^[A-Za-z0-9_-]{1,4096}$/;
const DECIMAL = /^-?(?:0|[1-9]\d{0,15})\.\d{2}$/;
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const count = value => Number.isSafeInteger(value) && value >= 0;
const text = value => typeof value === 'string';
const LIST_SORTS = Object.freeze({
  actions: Object.freeze(['TITLE', 'CANDIDATES', 'PAYMENTS']),
  blocked: Object.freeze(['CANDIDATE', 'REASON', 'AMOUNT'])
});

// Exact route inventory; no broad search endpoint and no mutation fallback.
export const BANKING_PAY_MODAL_ROUTES = Object.freeze([
  ['GET', '/api/banking/pay/workbench/v2/capability', null, 'capability'],
  ['GET', '/api/banking/pay/workbench/v2/session/:id/candidates', 'pay_workbench_session_get_candidate_summary_page_v1', 'summary'],
  ['POST', '/api/banking/pay/workbench/v2/session/:id/selection', 'pay_workbench_session_set_filtered_ready_selection_v1', 'globalSelection'],
  ['POST', '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/selection', 'pay_workbench_session_set_candidate_ready_selection_v1', 'selection'],
  ['POST', '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/ready-selection', 'pay_workbench_session_set_ready_rows_v1', 'rowSelection'],
  ['POST', '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/group-selection', 'pay_workbench_session_set_ready_group_v1', 'groupSelection'],
  ['GET', '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/ready', 'pay_workbench_session_get_candidate_ready_page_v1', 'ready'],
  ['GET', '/api/banking/pay/workbench/v2/session/:id/action-required', 'pay_workbench_session_get_action_required_page_v1', 'actions'],
  ['GET', '/api/banking/pay/workbench/v2/session/:id/action-required/:taskKey', 'pay_workbench_session_get_action_required_detail_v1', 'actionDetail'],
  ['GET', '/api/banking/pay/workbench/v2/session/:id/blocked', 'pay_workbench_session_get_blocked_page_v1', 'blocked'],
  ['GET', '/api/banking/pay/workbench/v2/session/:id/blocked/:blockerKey', 'pay_workbench_session_get_blocked_detail_v1', 'blockedDetail'],
  ['GET', '/api/banking/pay/workbench/v2/session/:id/candidate/:candidateId/selected-ready-timesheets', 'pay_workbench_session_get_selected_ready_timesheets_v1', 'timesheets']
].map(Object.freeze));

export class BankingPayModalContractError extends Error {
  constructor(code, status = 400, outcome = 'NOT_SUBMITTED') {
    super(code);
    this.code = code;
    this.status = status;
    this.outcome = outcome;
  }
}
function requireValue(condition, code, status = 400) {
  if (!condition) throw new BankingPayModalContractError(code, status);
}
function exactKeys(value, allowed) {
  requireValue(isObject(value) && Object.keys(value).every(key => allowed.includes(key)), 'BANKING_PAY_V2_INVALID_INPUT');
}
function integer(value, fallback) {
  if (value === undefined && fallback !== undefined) return fallback;
  requireValue((text(value) && /^(0|[1-9]\d*)$/.test(value)) || count(value), 'BANKING_PAY_V2_INVALID_INPUT');
  const parsed = Number(value);
  requireValue(count(parsed), 'BANKING_PAY_V2_INVALID_INPUT');
  return parsed;
}
function routeFor(pathname) {
  for (const [method, pattern, rpc, kind] of BANKING_PAY_MODAL_ROUTES) {
    const names = [];
    const expression = pattern.replace(/:[A-Za-z]+/g, name => {
      names.push(name.slice(1));
      return '([^/]+)';
    });
    const match = pathname.match(new RegExp(`^${expression}$`));
    if (match) return { method, rpc, kind, params: Object.fromEntries(names.map((name, index) => [name, match[index + 1]])) };
  }
  return null;
}

export function parseBankingPayModalInput(route, input, actorId) {
  requireValue(UUID.test(actorId), 'BANKING_PAY_V2_UNAUTHORISED', 401);
  requireValue(UUID.test(route.params.id), 'BANKING_PAY_V2_INVALID_SESSION');
  const commonKeys = ['expected_session_version', 'expected_progress_counter_version', 'scope_hash', 'pay_channel_scope'];
  const pageKeys = ['cursor', 'limit'];
  const kindKeys = {
    summary: [...pageKeys, 'sort_key', 'sort_direction'],
    ready: pageKeys,
    actions: [...pageKeys, 'search', 'sort_key', 'sort_direction', 'view'],
    blocked: [...pageKeys, 'search', 'sort_key', 'sort_direction'],
    actionDetail: pageKeys, blockedDetail: pageKeys,
    timesheets: ['scope_token'], selection: ['action', 'request_id', 'expected_view_digest', 'open_ready'],
    globalSelection: ['action', 'request_id', 'expected_view_digest'],
    rowSelection: ['preview_row_ids', 'selected', 'request_id', 'expected_view_digest', 'open_ready'],
    groupSelection: ['group_kind', 'group_key', 'selected', 'request_id', 'expected_view_digest', 'open_ready']
  };
  exactKeys(input, [...commonKeys, ...kindKeys[route.kind]]);
  const options = {
    expected_session_version: integer(input.expected_session_version),
    expected_progress_counter_version: integer(input.expected_progress_counter_version),
    scope_hash: input.scope_hash ?? null,
    pay_channel_scope: input.pay_channel_scope
  };
  const initialSummary = route.kind === 'summary' && input.scope_hash === undefined && input.cursor === undefined;
  requireValue(initialSummary || (text(options.scope_hash) && /^[a-f0-9]{64}$/.test(options.scope_hash)), 'BANKING_PAY_V2_INVALID_SCOPE');
  requireValue(['ALL', 'PAYE', 'UMBRELLA'].includes(options.pay_channel_scope), 'BANKING_PAY_V2_INVALID_SCOPE');
  const args = { p_session_id: route.params.id, p_options_json: options, p_actor_user_id: actorId };
  if (route.params.candidateId !== undefined) {
    requireValue(UUID.test(route.params.candidateId), 'BANKING_PAY_V2_INVALID_CANDIDATE');
    args.p_candidate_id = route.params.candidateId;
  }
  if (kindKeys[route.kind].includes('limit')) {
    args.p_limit = integer(input.limit, 100);
    requireValue(args.p_limit >= 1 && args.p_limit <= 100, 'BANKING_PAY_V2_INVALID_LIMIT');
    args.p_cursor = input.cursor ?? null;
    requireValue(args.p_cursor === null || (text(args.p_cursor) && TOKEN.test(args.p_cursor)), 'BANKING_PAY_V2_INVALID_CURSOR');
  }
  if (route.kind === 'summary') {
    args.p_sort_key = input.sort_key ?? 'CANDIDATE';
    args.p_sort_direction = input.sort_direction ?? 'ASC';
    requireValue(['CANDIDATE', 'DEDUCTIONS', 'READY_TO_PAY'].includes(args.p_sort_key), 'BANKING_PAY_V2_INVALID_SORT');
    requireValue(['ASC', 'DESC'].includes(args.p_sort_direction), 'BANKING_PAY_V2_INVALID_SORT');
  }
  if (Object.hasOwn(LIST_SORTS, route.kind)) {
    // BP-102/BP-121 list navigation only. Never put this in p_options_json,
    // selection arguments or the authoritative Draft/candidate filter scope.
    const search = input.search ?? '';
    requireValue(text(search) && search.length <= 200 && !/[\u0000-\u001f\u007f]/u.test(search), 'BANKING_PAY_V2_INVALID_INPUT');
    args.p_search = search.trim();
    args.p_sort_key = input.sort_key ?? LIST_SORTS[route.kind][0];
    args.p_sort_direction = input.sort_direction ?? 'ASC';
    requireValue(LIST_SORTS[route.kind].includes(args.p_sort_key), 'BANKING_PAY_V2_INVALID_SORT');
    requireValue(['ASC', 'DESC'].includes(args.p_sort_direction), 'BANKING_PAY_V2_INVALID_SORT');
  }
  if (route.kind === 'actions') {
    args.p_view = input.view ?? 'ACTION_REQUIRED';
    requireValue(['ACTION_REQUIRED', 'UPDATING'].includes(args.p_view), 'BANKING_PAY_V2_INVALID_INPUT');
    requireValue(args.p_view !== 'UPDATING' || (args.p_search === '' && args.p_sort_key === 'TITLE'
      && args.p_sort_direction === 'ASC'), 'BANKING_PAY_V2_INVALID_INPUT');
  }
  if (['selection', 'globalSelection', 'rowSelection', 'groupSelection'].includes(route.kind)) {
    if (route.kind === 'rowSelection') {
      requireValue(Array.isArray(input.preview_row_ids) && input.preview_row_ids.length >= 1 && input.preview_row_ids.length <= 100
        && input.preview_row_ids.every(id => text(id) && UUID.test(id))
        && new Set(input.preview_row_ids.map(id => id.toLowerCase())).size === input.preview_row_ids.length
        && typeof input.selected === 'boolean', 'BANKING_PAY_V2_INVALID_INPUT');
      args.p_preview_row_ids = input.preview_row_ids;
      args.p_selected = input.selected;
    } else if (route.kind === 'groupSelection') {
      requireValue(['TIMESHEET','OVERPAYMENT'].includes(input.group_kind)
        && text(input.group_key) && input.group_key.length >= 1 && input.group_key.length <= 512
        && !/[\u0000-\u001f\u007f]/u.test(input.group_key)
        && typeof input.selected === 'boolean', 'BANKING_PAY_V2_INVALID_INPUT');
      args.p_group_kind = input.group_kind;
      args.p_group_key = input.group_key;
      args.p_selected = input.selected;
    } else {
      requireValue(['SELECT_ALL_READY', 'CLEAR_ALL_READY'].includes(input.action), 'BANKING_PAY_V2_INVALID_ACTION');
      args.p_action = input.action;
    }
    requireValue(text(input.request_id) && UUID.test(input.request_id), 'BANKING_PAY_V2_INVALID_REQUEST_ID');
    requireValue(text(input.expected_view_digest) && /^[a-f0-9]{64}$/.test(input.expected_view_digest), 'BANKING_PAY_V2_INVALID_INPUT');
    args.p_request_id = input.request_id;
    args.p_expected_view_digest = input.expected_view_digest;
    if (input.open_ready !== undefined && input.open_ready !== null) {
      exactKeys(input.open_ready, ['cursor', 'limit']);
      const { cursor, limit } = input.open_ready;
      requireValue((cursor === null || (text(cursor) && TOKEN.test(cursor)))
        && Number.isInteger(limit) && limit >= 1 && limit <= 100, 'BANKING_PAY_V2_INVALID_INPUT');
      // Navigation metadata only. The RPC must read complete current rows after
      // its existing selection owner settles; clients never send payment facts.
      args.p_open_ready_json = { cursor, limit };
    }
  }
  for (const [param, argument] of [['taskKey', 'p_task_key'], ['blockerKey', 'p_blocker_key']]) {
    if (route.params[param] !== undefined) {
      requireValue(TOKEN.test(route.params[param]), 'BANKING_PAY_V2_INVALID_ITEM');
      args[argument] = route.params[param];
    }
  }
  if (route.kind === 'timesheets') {
    requireValue(text(input.scope_token) && TOKEN.test(input.scope_token), 'BANKING_PAY_V2_INVALID_SCOPE_TOKEN');
    args.p_scope_token = input.scope_token;
  }
  return args;
}

function validAmount(value) { return text(value) && DECIMAL.test(value) && value !== '-0.00'; }
function validReadyGroup(row) {
  const keys=['selection_group_kind','selection_group_key','selection_group_member_count','selection_group_selected_count','selection_group_state',
    'selection_group_display_amount','selection_group_selected_display_amount'];
  if (!keys.every(key=>Object.hasOwn(row,key)) || !count(row.selection_group_member_count)
      || !count(row.selection_group_selected_count) || row.selection_group_selected_count>row.selection_group_member_count) return false;
  if (row.selection_group_kind===null) return row.selection_group_key===null&&row.selection_group_member_count===0
    &&row.selection_group_selected_count===0&&row.selection_group_state===null
    &&row.selection_group_display_amount===null&&row.selection_group_selected_display_amount===null;
  return ['TIMESHEET','OVERPAYMENT'].includes(row.selection_group_kind)&&text(row.selection_group_key)
    &&row.selection_group_key.length<=512&&!/[\u0000-\u001f\u007f]/u.test(row.selection_group_key)
    &&row.selection_group_member_count>=1&&validAmount(row.selection_group_display_amount)&&validAmount(row.selection_group_selected_display_amount)
    &&['NONE','SOME','ALL'].includes(row.selection_group_state)
    &&row.selection_group_state===(row.selection_group_selected_count===0?'NONE'
      :row.selection_group_selected_count===row.selection_group_member_count?'ALL':'SOME');
}
function validIds(ids) {
  return Array.isArray(ids) && ids.every(id => text(id) && UUID.test(id)) && new Set(ids).size === ids.length;
}
function validCandidate(row) {
  if (!isObject(row) || !UUID.test(row.candidate_id) || !text(row.candidate_name) || !text(row.candidate_reference)
      || !text(row.candidate_sort_name) || !text(row.candidate_sort_reference) || !text(row.child_revision)
      || !text(row.facts_digest) || !/^[a-f0-9]{64}$/.test(row.facts_digest)
      || !count(row.selectable_ready_count) || row.selectable_ready_count < 1 || !count(row.selected_ready_count)
      || row.selected_ready_count > row.selectable_ready_count || !validAmount(row.selected_display_amount)
      || typeof row.selected_deduction_exists !== 'boolean' || !count(row.selected_timesheet_count)) return false;
  const expected = row.selected_ready_count === 0 ? 'NONE' : row.selected_ready_count === row.selectable_ready_count ? 'ALL' : 'SOME';
  if (row.selection_state !== expected) return false;
  if (expected === 'NONE' && (row.selected_display_amount !== '0.00' || row.selected_deduction_exists || row.selected_timesheet_count !== 0)) return false;
  if (row.selected_timesheet_count <= 25) {
    return validIds(row.selected_timesheet_ids) && row.selected_timesheet_ids.length === row.selected_timesheet_count && row.selected_timesheet_scope_token === null;
  }
  return Array.isArray(row.selected_timesheet_ids) && row.selected_timesheet_ids.length === 0
    && text(row.selected_timesheet_scope_token) && TOKEN.test(row.selected_timesheet_scope_token);
}
function validDraftGate(value) {
  if (!isObject(value)
    || !['can_create_draft','session_ready','read_only','work_queued','display_ready','draft_safe'].every(key => typeof value[key] === 'boolean')
    || !(value.draft_block_reason_code === null || text(value.draft_block_reason_code) && value.draft_block_reason_code.length > 0)
    || !count(value.session_selected_row_count) || !count(value.session_selected_eligible_ready_row_count)
    || !Array.isArray(value.blocker_codes) || !value.blocker_codes.every(code => text(code) && code.length > 0)
    || new TextEncoder().encode(JSON.stringify(value)).byteLength > 2048) return false;
  if (value.draft_safe && (!value.display_ready || value.draft_block_reason_code !== null)) return false;
  if (!value.draft_safe && value.draft_block_reason_code !== null && !value.blocker_codes.includes(value.draft_block_reason_code)) return false;
  return !value.can_create_draft || value.session_ready && value.display_ready && value.draft_safe
    && !value.read_only && !value.work_queued && value.blocker_codes.length === 0
    && value.session_selected_row_count > 0 && value.session_selected_eligible_ready_row_count > 0;
}
function validateGlobal(value) {
  if (!(isObject(value) && validAmount(value.selected_ready_display_amount)
    && ['NONE', 'SOME', 'ALL'].includes(value.selection_state)
    && ['candidate_count', 'selected_candidate_count', 'selected_ready_count', 'selectable_ready_count', 'action_required_count', 'updating_count', 'blocked_count'].every(key => count(value[key]))
    && validDraftGate(value.draft))) return false;
  const expected = value.selected_ready_count === 0 ? 'NONE'
    : value.selected_ready_count === value.selectable_ready_count ? 'ALL' : 'SOME';
  return value.selection_state === expected
    && value.selected_ready_count <= value.selectable_ready_count
    && value.selected_candidate_count <= value.candidate_count
    && value.selected_candidate_count <= value.selected_ready_count
    && value.candidate_count <= value.selectable_ready_count
    && (value.selected_ready_count === 0
      ? value.selected_candidate_count === 0 && value.selected_ready_display_amount === '0.00' && !value.draft.can_create_draft
      : value.selected_candidate_count > 0);
}

function sameCandidate(left, right) {
  if (left === null || right === null) return left === right;
  const keys = Object.keys(left);
  return keys.length === Object.keys(right).length
    && keys.every(key => Object.hasOwn(right, key) && JSON.stringify(left[key]) === JSON.stringify(right[key]));
}

export function validateBankingPayMovementEnvelope(value, candidateId = null) {
  const fail = () => { throw new BankingPayModalContractError('BANKING_PAY_V2_INVALID_RESPONSE', 502); };
  if (!isObject(value) || typeof value.state_changed !== 'boolean' || !Array.isArray(value.movements)
      || typeof value.movements_complete !== 'boolean' || !count(value.movement_count)
      || !text(value.movement_digest) || !/^[a-f0-9]{64}$/.test(value.movement_digest)) fail();
  const invalidations = value.invalidations;
  if (!isObject(invalidations) || invalidations.scope !== 'ALL_PREVIOUS_DETAILS'
      || !['ready', 'actions', 'updating', 'blocked'].every(key => invalidations[key] === true)
      || Object.keys(invalidations).length !== 5) fail();
  if (value.movements_complete ? value.movement_count !== value.movements.length
    : value.movements.length !== 0 || value.movement_count === 0) fail();
  if (!value.state_changed && value.movement_count !== 0) fail();
  if (new TextEncoder().encode(JSON.stringify(value.movements)).byteLength > 8192) fail();
  const ids = new Set();
  for (const row of value.movements) {
    if (!isObject(row) || !text(row.identity) || !UUID.test(row.identity)
        || !text(row.candidate_id) || !UUID.test(row.candidate_id)
        || (candidateId !== null && row.candidate_id !== candidateId)
        || !text(row.row_key) || !row.row_key || !text(row.from) || !row.from || !text(row.to) || !row.to
        || row.from === row.to || typeof row.selected !== 'boolean' || ids.has(row.identity.toLowerCase())) fail();
    ids.add(row.identity.toLowerCase());
  }
  return value;
}

function validIssueCounts(value) {
  return count(value.affected_candidate_count) && value.affected_candidate_count > 0
    && typeof value.affected_payment_count_complete === 'boolean'
    && (value.affected_payment_count_complete ? count(value.affected_payment_count) : value.affected_payment_count === null);
}
function validTask(row, state) {
  return isObject(row) && text(row.identity) && TOKEN.test(row.identity) && row.issue_state === state
    && text(row.title) && row.title.trim().length > 0 && validIssueCounts(row)
    && row.indefinite_snooze !== true && row.updating !== true;
}
function validateTasks(payload, args, fail) {
  if (!['ACTION_REQUIRED', 'UPDATING'].includes(args.p_view) || payload.view !== args.p_view
      || !payload.rows.every(row => validTask(row, args.p_view))
      || !count(payload.updating_count) || !Array.isArray(payload.updating)
      || payload.updating.length !== Math.min(100, payload.updating_count)
      || payload.updating_has_more !== (payload.updating_count > 100)
      || (payload.updating_has_more ? !(text(payload.updating_next_cursor) && TOKEN.test(payload.updating_next_cursor))
        : payload.updating_next_cursor !== null)
      || !payload.updating.every(row => validTask(row, 'UPDATING'))
      || new Set(payload.updating.map(row => row.identity)).size !== payload.updating.length) fail();
  const inline = new Set(payload.updating.map(row => row.identity));
  if (args.p_view === 'ACTION_REQUIRED' && payload.rows.some(row => inline.has(row.identity))) fail();
  if (args.p_view === 'UPDATING' && (payload.scope_count !== payload.updating_count
      || args.p_search !== '' || args.p_sort_key !== 'TITLE' || args.p_sort_direction !== 'ASC'
      || (args.p_cursor === null && args.p_limit === 100
        && JSON.stringify(payload.rows) !== JSON.stringify(payload.updating)))) fail();
}
function validateBlocked(payload, fail) {
  if (payload.rows.some(row => !TOKEN.test(row.identity) || !UUID.test(row.candidate_id)
      || !text(row.candidate_name) || !row.candidate_name.trim() || !text(row.candidate_reference)
      || !text(row.reason) || !row.reason.trim()
      || (row.affected_display_amount !== null && !(text(row.affected_display_amount) && DECIMAL.test(row.affected_display_amount)
        && row.affected_display_amount !== '-0.00'))
      || (row.source_kind !== undefined && (row.source_kind === 'PREVIEW_ROW' ? !UUID.test(row.preview_row_id)
        : !['STORED_PAYEE', 'SOURCE_PROGRESS'].includes(row.source_kind) || row.preview_row_id !== null)))) fail();
}
function validateIssueDetail(payload, kind, args, fail) {
  const key = kind === 'actionDetail' ? 'task_key' : 'blocker_key';
  if (payload[key] !== args[`p_${key}`] || !validIssueCounts(payload)
      || !count(payload.total_count) || payload.total_count < 1
      || !count(payload.page_number) || payload.page_number < 1
      || payload.page_number > Math.ceil(payload.total_count / args.p_limit)
      || (args.p_cursor === null && payload.page_number !== 1)
      || payload.rows.length !== Math.min(args.p_limit, payload.total_count - (payload.page_number - 1) * args.p_limit)
      || payload.has_more !== (payload.page_number * args.p_limit < payload.total_count)
      || payload.has_previous !== (payload.page_number > 1)
      || (payload.page_number > 2 ? !(text(payload.previous_cursor) && TOKEN.test(payload.previous_cursor)) : payload.previous_cursor !== null)
      || payload.rows.some(row => !UUID.test(row.candidate_id) || typeof row.context_only !== 'boolean'
        || !isObject(row.payload) || row.indefinite_snooze === true
        || (row.payload.candidate_id != null && row.payload.candidate_id !== row.candidate_id)
        || row.payload.presentation_role === 'HIDDEN_INDEFINITE_SNOOZE'
        || (row.source_kind === 'PREVIEW_ROW' ? !UUID.test(row.preview_row_id) || row.payload.preview_row_id !== row.preview_row_id
          : !['STORED_PAYEE', 'SOURCE_PROGRESS'].includes(row.source_kind) || row.preview_row_id !== null))) fail();
}

function validateSelectionPage(payload, args, fail) {
  // DEV-0007: only an already-open child may use the existing Ready-page
  // allowance. An unsolicited child cannot bypass the compact base limit.
  const requested = args.p_open_ready_json;
  if (requested) {
    if (!isObject(payload.ready_page)) fail();
    validateBankingPayModalEnvelope(payload.ready_page, 'ready', {
      p_session_id: args.p_session_id, p_candidate_id: args.p_candidate_id,
      p_cursor: requested.cursor, p_limit: requested.limit,
      p_options_json: { ...args.p_options_json,
        expected_progress_counter_version: payload.progress_counter_version }
    });
    if (!sameCandidate(payload.candidate, payload.ready_page.candidate)) fail();
  } else if (payload.ready_page !== undefined && payload.ready_page !== null) fail();
  const encoder = new TextEncoder();
  const size = value => encoder.encode(JSON.stringify(value)).byteLength;
  const base = Object.hasOwn(payload, 'ready_page') ? { ...payload, ready_page: null } : payload;
  if (size(base) > 32 * 1024
      || (requested && (size(payload.ready_page) > 512 * 1024 || size(payload) > 544 * 1024))) fail();
}

export function validateBankingPayModalEnvelope(payload, kind, args) {
  const fail = () => { throw new BankingPayModalContractError('BANKING_PAY_V2_INVALID_RESPONSE', 502); };
  if (!isObject(payload) || payload.ok !== true || payload.contract !== BANKING_PAY_MODAL_CONTRACT || payload.contract_version !== 1
      || payload.session_id !== args.p_session_id || !count(payload.session_version) || !count(payload.progress_counter_version)
      || !text(payload.scope_hash) || !/^[a-f0-9]{64}$/.test(payload.scope_hash)
      || (!(kind === 'summary' && args.p_options_json.scope_hash === null) && payload.scope_hash !== args.p_options_json.scope_hash)) fail();
  const expected = args.p_options_json;
  const mutating = ['selection', 'globalSelection', 'rowSelection', 'groupSelection'].includes(kind);
  if (payload.session_version !== expected.expected_session_version
      || (!mutating && payload.progress_counter_version !== expected.expected_progress_counter_version)
      || (mutating && payload.progress_counter_version < expected.expected_progress_counter_version)) fail();
  if (kind === 'summary') {
    if (!text(payload.view_digest) || !/^[a-f0-9]{64}$/.test(payload.view_digest)
        || !validateGlobal(payload.global) || payload.sort_key !== args.p_sort_key || payload.sort_direction !== args.p_sort_direction
        || !Array.isArray(payload.rows) || !payload.rows.every(validCandidate)
        || payload.total_count !== payload.global.candidate_count
        || payload.rows.some(row => row.selected_ready_count > payload.global.selected_ready_count
          || row.selectable_ready_count > payload.global.selectable_ready_count
          || (payload.global.selection_state === 'NONE' && row.selection_state !== 'NONE')
          || (payload.global.selection_state === 'ALL' && row.selection_state !== 'ALL'))
        || new Set(payload.rows.map(row => row.candidate_id)).size !== payload.rows.length) fail();
    // Paging metadata is server-owned too. An anchor may renew a read after a
    // selection revision; it never supplies financial or mutation authority.
    if (!count(payload.page_number) || typeof payload.has_previous !== 'boolean'
        || payload.has_previous !== (payload.page_number > 1)
        || (payload.has_more ? !(text(payload.next_page_anchor) && TOKEN.test(payload.next_page_anchor)) : payload.next_page_anchor !== null)
        || (payload.has_previous ? !(text(payload.previous_page_anchor) && TOKEN.test(payload.previous_page_anchor)) : payload.previous_page_anchor !== null)
        || (payload.page_number > 2
          ? !(text(payload.previous_cursor) && TOKEN.test(payload.previous_cursor))
          : payload.previous_cursor !== null)
        || (payload.rows.length === 0
          ? payload.total_count !== 0 || payload.page_number !== 0 || payload.page_anchor !== null
          : payload.page_number < 1 || payload.page_number > Math.ceil(payload.total_count / args.p_limit)
            || !(text(payload.page_anchor) && TOKEN.test(payload.page_anchor)))) fail();
    if (payload.total_count === 0 ? payload.rows.length !== 0 || payload.has_more
      : payload.rows.length !== Math.min(args.p_limit, payload.total_count - (payload.page_number - 1) * args.p_limit)
        || payload.has_more !== (payload.page_number * args.p_limit < payload.total_count)) fail();
  } else if (kind === 'selection' || kind === 'rowSelection' || kind === 'groupSelection') {
    const proof = payload.retention;
    const proofFlags = ['other_candidates_unchanged', 'membership_unchanged', 'candidate_sort_unchanged',
      'amount_sort_unchanged', 'deduction_sort_unchanged'];
    if (!text(args.p_expected_view_digest) || !/^[a-f0-9]{64}$/.test(args.p_expected_view_digest)
        || !text(payload.view_digest) || !/^[a-f0-9]{64}$/.test(payload.view_digest)
        || !isObject(proof) || Object.keys(proof).length !== 6
        || proof.before_view_digest !== args.p_expected_view_digest
        || !proofFlags.every(key => typeof proof[key] === 'boolean')) fail();
    if (payload.request_id !== args.p_request_id || typeof payload.state_changed !== 'boolean'
        || (kind === 'rowSelection' || kind === 'groupSelection'
          ? payload.state_changed !== true || payload.selection_scope !== (kind === 'groupSelection' ? 'COMPLETE_READY_GROUP' : 'EXACT_READY_ROWS')
            || payload.progress_counter_version <= expected.expected_progress_counter_version
          : payload.progress_counter_version !== expected.expected_progress_counter_version + (payload.state_changed ? 1 : 0))
        || !validateGlobal(payload.global) || typeof payload.candidate_absent !== 'boolean'
        || (payload.candidate_absent ? payload.candidate !== null : !validCandidate(payload.candidate))
        || (!payload.candidate_absent && payload.candidate.candidate_id !== args.p_candidate_id)
        || !Array.isArray(payload.movements) || !isObject(payload.invalidations)) fail();
    if (kind === 'groupSelection' && (payload.selection_scope !== 'COMPLETE_READY_GROUP'
      || payload.group_kind !== args.p_group_kind || payload.group_key !== args.p_group_key
      || !count(payload.group_member_count) || payload.group_member_count < 1
      || payload.owner_call_count !== 1)) fail();
    validateBankingPayMovementEnvelope(payload, kind === 'selection' ? args.p_candidate_id : null);
    validateSelectionPage(payload, args, fail);
  } else if (kind === 'globalSelection') {
    if (payload.request_id !== args.p_request_id || typeof payload.state_changed !== 'boolean'
        || payload.progress_counter_version !== expected.expected_progress_counter_version + (payload.state_changed ? 1 : 0)
        || !validateGlobal(payload.global) || payload.selection_scope !== 'FILTERED_READY'
        || payload.requires_summary_refresh !== true || !text(args.p_expected_view_digest)
        || !/^[a-f0-9]{64}$/.test(args.p_expected_view_digest) || payload.before_view_digest !== args.p_expected_view_digest
        || !text(payload.view_digest) || !/^[a-f0-9]{64}$/.test(payload.view_digest)
        || ['candidate', 'candidate_id', 'candidate_absent', 'ready_page', 'retention'].some(key => Object.hasOwn(payload, key))) fail();
    validateBankingPayMovementEnvelope(payload);
    if (new TextEncoder().encode(JSON.stringify(payload)).byteLength > 32 * 1024) fail();
  } else if (kind === 'timesheets') {
    if (payload.candidate_id !== args.p_candidate_id || !validIds(payload.timesheet_ids)
        || payload.timesheet_count !== payload.timesheet_ids.length) fail();
  } else {
    if (!Array.isArray(payload.rows) || payload.rows.some(row => !isObject(row) || !text(row.identity) || !row.identity)
        || new Set(payload.rows.map(row => row.identity)).size !== payload.rows.length) fail();
    if (kind === 'ready') {
      if (payload.candidate_id !== args.p_candidate_id || !Object.hasOwn(payload, 'candidate')
          || payload.rows.some(row => row.candidate_id !== args.p_candidate_id
            || row.effective_section !== 'canonical_preview_lines' || typeof row.selected !== 'boolean' || !validReadyGroup(row))) fail();
      if (!count(payload.total_count) || !count(payload.page_number) || typeof payload.has_previous !== 'boolean'
          || payload.has_previous !== (payload.page_number > 1)
          || (payload.page_number > 2 ? !(text(payload.previous_cursor) && TOKEN.test(payload.previous_cursor)) : payload.previous_cursor !== null)
          || (payload.total_count === 0
            ? payload.rows.length !== 0 || payload.page_number !== 0 || payload.page_anchor !== null
            : payload.page_number < 1 || payload.page_number > Math.ceil(payload.total_count / args.p_limit)
              || (args.p_cursor === null && payload.page_number !== 1)
              || payload.rows.length !== Math.min(args.p_limit, payload.total_count - (payload.page_number - 1) * args.p_limit)
              || payload.has_more !== (payload.page_number * args.p_limit < payload.total_count)
              || !(text(payload.page_anchor) && TOKEN.test(payload.page_anchor)))) fail();
      if (payload.candidate === null) {
        if (payload.total_count !== 0 || payload.rows.length !== 0 || payload.has_more !== false || payload.next_cursor !== null) fail();
      } else if (!validCandidate(payload.candidate) || payload.candidate.candidate_id !== args.p_candidate_id
          || payload.candidate.selectable_ready_count > payload.total_count) fail();
    }
    if (kind === 'actions') validateTasks(payload, args, fail);
    if (kind === 'blocked') validateBlocked(payload, fail);
    if (kind === 'actionDetail' || kind === 'blockedDetail') validateIssueDetail(payload, kind, args, fail);
    if (Object.hasOwn(LIST_SORTS, kind) && (payload.search !== args.p_search
        || payload.sort_key !== args.p_sort_key || payload.sort_direction !== args.p_sort_direction
        || !count(payload.scope_count) || payload.total_count > payload.scope_count
        || payload.rows.some(row => row.indefinite_snooze === true || row.updating === true))) fail();
    if (Object.hasOwn(LIST_SORTS, kind) && (
        !count(payload.page_number) || typeof payload.has_previous !== 'boolean'
        || payload.has_previous !== (payload.page_number > 1)
        || (payload.page_number > 2 ? !(text(payload.previous_cursor) && TOKEN.test(payload.previous_cursor)) : payload.previous_cursor !== null)
        || (payload.total_count === 0
          ? payload.page_number !== 0 || payload.rows.length !== 0 || payload.has_more !== false || args.p_cursor !== null
          : payload.page_number < 1 || payload.page_number > Math.ceil(payload.total_count / args.p_limit)
            || (args.p_cursor === null && payload.page_number !== 1)
            || payload.rows.length !== Math.min(args.p_limit, payload.total_count - (payload.page_number - 1) * args.p_limit)
            || payload.has_more !== (payload.page_number * args.p_limit < payload.total_count))
        || (payload.search === '' && payload.total_count !== payload.scope_count))) fail();
  }
  if (args.p_limit !== undefined) {
    if (payload.rows.length > args.p_limit || !count(payload.total_count) || typeof payload.has_more !== 'boolean'
        || (payload.has_more ? !(text(payload.next_cursor) && TOKEN.test(payload.next_cursor)) : payload.next_cursor !== null)) fail();
  }
  if (['actions', 'blocked', 'actionDetail', 'blockedDetail'].includes(kind)
      && new TextEncoder().encode(JSON.stringify(payload)).byteLength > 256 * 1024) fail();
  if (kind === 'ready' && new TextEncoder().encode(JSON.stringify(payload)).byteLength > 512 * 1024) fail();
  return payload;
}

const REJECTIONS = Object.freeze({
  WORKBENCH_SESSION_NOT_FOUND: 404, OBSOLETE_SESSION: 409,
  WORKBENCH_STALE_SELECTION: 409, WORKBENCH_SESSION_VERSION_MISMATCH: 409,
  WORKBENCH_PROGRESS_COUNTER_VERSION_MISMATCH: 409,
  BANKING_PAY_V2_STALE_REVISION: 409, BANKING_PAY_V2_SCOPE_MISMATCH: 409,
  BANKING_PAY_V2_STALE_VIEW: 409, BANKING_PAY_V2_SELECTION_TOO_LARGE: 413, BANKING_PAY_V2_READY_TOO_LARGE: 413,
  BANKING_PAY_V2_ROW_NOT_SELECTABLE: 409,
  BANKING_PAY_V2_GROUP_NOT_SELECTABLE: 409,
  BANKING_PAY_V2_STALE_CURSOR: 409, BANKING_PAY_V2_INVALID_CURSOR: 400,
  BANKING_PAY_V2_CANDIDATE_NOT_CURRENT: 409, BANKING_PAY_V2_ITEM_NOT_CURRENT: 409,
  BANKING_PAY_V2_UNAUTHORISED: 403, BANKING_PAY_V2_NOT_READY: 409,
  BANKING_PAY_V2_INVALID_INPUT: 400, BANKING_PAY_V2_INVALID_AMOUNT: 409,
  BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE: 503
});
function safeFailure(error, submitted) {
  // Never serialize raw upstream errors: they can contain SQL, data or credentials.
  if (error instanceof BankingPayModalContractError) {
    return { ok: false, code: error.code, status: error.status, outcome: submitted ? 'UNCERTAIN' : error.outcome };
  }
  const upstream = isObject(error?.json) ? error.json : error;
  let detail = upstream?.details;
  if (text(detail) && detail.length <= 4096) {
    try { detail = JSON.parse(detail); } catch { detail = null; }
  }
  const rejectionCode = [detail?.code, upstream?.code, upstream?.message, error?.code, error?.message]
    .find(value => text(value) && Object.hasOwn(REJECTIONS, value));
  if (rejectionCode) return { ok: false, code: rejectionCode, status: REJECTIONS[rejectionCode], outcome: 'REJECTED' };
  const code = upstream?.code;
  if (!submitted && ['PGRST202', 'PGRST204', '42883'].includes(code)) {
    return { ok: false, code: 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE', status: 503, outcome: 'NOT_SUBMITTED' };
  }
  return { ok: false, code: submitted ? 'BANKING_PAY_V2_MUTATION_UNCERTAIN' : 'BANKING_PAY_V2_UNAVAILABLE', status: 503, outcome: submitted ? 'UNCERTAIN' : 'NOT_SUBMITTED' };
}
function response(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' } });
}

async function boundedJsonBody(req) {
  requireValue(req.body !== null, 'BANKING_PAY_V2_INVALID_INPUT');
  const reader = req.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let bytes = 0;
  let raw = '';
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > 8192) {
        await reader.cancel();
        throw new BankingPayModalContractError('BANKING_PAY_V2_INPUT_TOO_LARGE', 413);
      }
      raw += decoder.decode(chunk.value, { stream: true });
    }
    raw += decoder.decode();
    return JSON.parse(raw);
  } catch (error) {
    if (error instanceof BankingPayModalContractError) throw error;
    throw new BankingPayModalContractError('BANKING_PAY_V2_INVALID_INPUT');
  } finally { reader.releaseLock(); }
}

export async function dispatchBankingPayModalV2Request({ req, user, contract, rpc }) {
  const url = new URL(req.url);
  if (url.pathname !== PREFIX && !url.pathname.startsWith(`${PREFIX}/`)) return null;
  let submitted = false;
  try {
    requireValue(user && UUID.test(user.id), 'BANKING_PAY_V2_UNAUTHORISED', 401);
    const route = routeFor(url.pathname);
    requireValue(route !== null, 'BANKING_PAY_V2_ROUTE_NOT_FOUND', 404);
    requireValue(req.method === route.method, 'BANKING_PAY_V2_METHOD_NOT_ALLOWED', 405);
    if (route.kind === 'capability') {
      requireValue(!url.search, 'BANKING_PAY_V2_INVALID_INPUT');
      return response({ banking_pay_workbench_v2: {
        available: BANKING_PAY_MODAL_AVAILABLE && contract?.banking_pay_workbench_v2?.available === true
          && contract?.banking_pay_workbench_v2?.contract_version === 1,
        contract_version: 1
      } });
    }
    // Routes are testable before activation; every RPC independently enforces
    // its full session/revision/actor/scope contract. No cached financial state.
    let input;
    if (req.method === 'POST') {
      requireValue(!url.search, 'BANKING_PAY_V2_INVALID_INPUT');
      requireValue((req.headers.get('content-type') || '').split(';')[0].trim() === 'application/json', 'BANKING_PAY_V2_INVALID_INPUT');
      input = await boundedJsonBody(req);
    } else {
      requireValue(new Set(url.searchParams.keys()).size === [...url.searchParams.keys()].length, 'BANKING_PAY_V2_INVALID_INPUT');
      input = Object.fromEntries(url.searchParams);
    }
    const args = parseBankingPayModalInput(route, input, user.id);
    submitted = ['selection', 'globalSelection', 'rowSelection', 'groupSelection'].includes(route.kind);
    let payload = await rpc(route.rpc, args, {
      routeClass: submitted ? 'CONTROL' : 'PREVIEW_PROGRESS',
      purpose: 'BANKING_PAY_MODAL_STRUCTURE_V2', timeoutMs: submitted ? 8000 : 3000, bankingPay: true
    });
    if (Array.isArray(payload) && payload.length === 1) payload = payload[0];
    if (isObject(payload) && Object.hasOwn(payload, route.rpc)) payload = payload[route.rpc];
    if (isObject(payload) && payload.ok === false) throw { code: payload.code || payload.error_code };
    return response(validateBankingPayModalEnvelope(payload, route.kind, args));
  } catch (error) {
    const failure = safeFailure(error, submitted);
    return response({ ...failure, retry_mutation: false, read_back_required: failure.outcome === 'UNCERTAIN' }, failure.status);
  }
}
