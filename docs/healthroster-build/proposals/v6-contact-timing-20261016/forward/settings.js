(function (global) {
  'use strict';

  const API_ROOT = '/api/weekly-source/v1/settings';
  const CLIENT_STATE = '__weeklySourceClientSettings';
  const CONTRACT_STATE = '__weeklySourceContractSettings';
  const GLOBAL_STATE = '__weeklySourceGlobalSettings';
  const CLIENT_BINDINGS = new WeakMap();
  const CONTRACT_SETTING_KEYS = Object.freeze([
    'effective_from',
    'weekly_rate_classification_method_override',
    'duration_break_tie_rule_override',
    'source_fixed_expenses_enabled_override',
    'source_expense_vat_enabled_override',
    'candidate_queries_enabled_override',
    'manager_queries_enabled_override',
    'manager_query_recipient_override',
    'completed_pack_copy_enabled_override',
    'completed_pack_recipient_override'
  ]);

  const clone = (value) => {
    try { return JSON.parse(JSON.stringify(value == null ? null : value)); } catch { return value; }
  };
  const text = (value) => String(value == null ? '' : value).trim();
  const upper = (value) => text(value).toUpperCase();
  const bool = (value, fallback = false) => {
    if (value === true || value === 'true' || value === '1' || value === 1 || value === 'on') return true;
    if (value === false || value === 'false' || value === '0' || value === 0 || value === '') return false;
    return fallback;
  };
  const esc = (value) => String(value == null ? '' : value)
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
  const endpoint = (path) => {
    const relative = `${API_ROOT}${path || ''}`;
    return typeof global.API === 'function' ? global.API(relative) : relative;
  };
  const authFetch = (...args) => {
    if (typeof global.authFetch !== 'function') throw new Error('Weekly source settings are unavailable.');
    return global.authFetch(...args);
  };
  const requestJson = async (path, options) => {
    const response = await authFetch(endpoint(path), {
      cache: 'no-store',
      ...(options || {}),
      headers: {
        ...(options?.body ? { 'content-type': 'application/json' } : {}),
        ...(options?.headers || {})
      }
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload?.message || 'Weekly source settings could not be saved.');
    return payload;
  };
  const signalDirty = (source) => {
    try { global.dispatchEvent(new CustomEvent('modal-dirty', { detail: { source } })); } catch {}
    try {
      const frame = global.__getModalFrame?.();
      if (frame) { frame.isDirty = true; frame._updateButtons?.(); }
    } catch {}
  };

  function soleActiveNhspGroup(sourceGroups) {
    const matches = (sourceGroups || []).filter((group) =>
      upper(group?.source_family) === 'NHSP' && group?.active !== false
    );
    return matches.length === 1 ? matches[0] : null;
  }

  function legacyCapability(ctx) {
    const settings = ctx?.clientSettingsState || ctx?.clientSettingsBaseline || {};
    const mode = upper(settings.weekly_mode);
    const behaviour = upper(settings.hr_weekly_behaviour || 'VERIFY');
    if (mode === 'NHSP') return {
      eligible: true, source_family: 'NHSP', authority_mode: 'SOURCE_AUTHORITY',
      document_mode: 'CHECK_ONLY', self_bill_enabled: true
    };
    if (mode === 'HEALTHROSTER') return {
      eligible: true, source_family: 'ROSTER',
      authority_mode: behaviour === 'CREATE' ? 'SOURCE_AUTHORITY' : 'TIMESHEET_AUTHORITY',
      document_mode: behaviour === 'CREATE' ? 'CHECK_ONLY' : 'INVOICE_EVIDENCE_REQUIRED',
      self_bill_enabled: behaviour === 'CREATE'
    };
    return { eligible: false, source_family: null, authority_mode: null, document_mode: null, self_bill_enabled: false };
  }

  function defaultPolicy(capability) {
    return {
      source_group_id: '',
      weekly_rate_classification_method: 'SPLIT_RATE_WINDOWS',
      duration_break_tie_rule: 'EARLIEST_LONGEST_PORTION',
      candidate_queries_enabled: capability.authority_mode === 'SOURCE_AUTHORITY',
      manager_queries_enabled: capability.authority_mode === 'SOURCE_AUTHORITY',
      manager_query_recipient: '',
      completed_pack_copy_enabled: false,
      completed_pack_recipient: '',
      self_bill_correction_presentation: capability.source_family === 'NHSP' ? null : 'FULL_REVERSAL_REPLACEMENT',
      source_fixed_expenses_enabled: false,
      source_expense_vat_enabled: false
    };
  }

  function normaliseClientPayload(payload, ctx) {
    const fallback = legacyCapability(ctx);
    const capabilities = {
      ...fallback,
      ...(payload?.capabilities || {})
    };
    capabilities.eligible = payload?.eligible == null ? bool(capabilities.eligible) : bool(payload.eligible);
    capabilities.source_family = upper(capabilities.source_family || fallback.source_family) || null;
    capabilities.authority_mode = upper(capabilities.authority_mode || fallback.authority_mode) || null;
    capabilities.document_mode = upper(capabilities.document_mode || fallback.document_mode) || null;
    capabilities.self_bill_enabled = bool(capabilities.self_bill_enabled, fallback.self_bill_enabled);
    capabilities.show_query_settings = payload?.capabilities?.show_query_settings == null
      ? capabilities.authority_mode === 'SOURCE_AUTHORITY'
      : bool(payload.capabilities.show_query_settings);
    capabilities.show_completed_pack = payload?.capabilities?.show_completed_pack == null
      ? capabilities.document_mode !== 'IMPORT_ONLY'
      : bool(payload.capabilities.show_completed_pack);
    capabilities.show_rate_settings = payload?.capabilities?.show_rate_settings == null
      ? capabilities.authority_mode === 'SOURCE_AUTHORITY'
      : bool(payload.capabilities.show_rate_settings);
    capabilities.show_source_expenses = bool(payload?.capabilities?.show_source_expenses);
    capabilities.show_self_bill_correction = payload?.capabilities?.show_self_bill_correction == null
      ? capabilities.self_bill_enabled && capabilities.source_family !== 'NHSP'
      : bool(payload.capabilities.show_self_bill_correction);
    const sourceGroups = Array.isArray(payload?.source_groups) ? clone(payload.source_groups) : [];
    const settings = { ...defaultPolicy(capabilities), ...(payload?.settings || {}) };
    if (capabilities.source_family === 'NHSP') {
      const nhspGroup = soleActiveNhspGroup(sourceGroups);
      settings.source_group_id = nhspGroup?.id || null;
    }
    return {
      loaded: true,
      eligible: capabilities.eligible,
      configured: bool(payload?.configured),
      client_id: payload?.client_id || ctx?.data?.id || null,
      settings_version: payload?.settings_version ?? null,
      capabilities,
      source_groups: sourceGroups,
      baseline: clone(settings),
      draft: clone(settings),
      dirty: !ctx?.data?.id && capabilities.eligible
    };
  }

  async function loadClient(clientId, ctx, force = false) {
    if (!force && ctx?.[CLIENT_STATE]?.loaded) return ctx[CLIENT_STATE];
    let payload = {};
    if (clientId) {
      payload = await requestJson(`/clients/${encodeURIComponent(clientId)}`);
    } else {
      const groups = await requestJson('/source-groups');
      payload = { source_groups: groups?.source_groups || [] };
    }
    const state = normaliseClientPayload(payload, ctx);
    if (ctx) ctx[CLIENT_STATE] = state;
    return state;
  }

  const option = (value, label, selected) => `<option value="${esc(value)}" ${String(value) === String(selected) ? 'selected' : ''}>${esc(label)}</option>`;
  const row = (label, control) => `<label class="weekly-source-setting-row"><span>${esc(label)}</span><span>${control}</span></label>`;
  const check = (name, label, checked) => `<label class="weekly-source-setting-check"><input type="checkbox" name="${esc(name)}" ${checked ? 'checked' : ''}/><span>${esc(label)}</span></label>`;
  const card = (key, title, body) => `<section class="record-settings-card weekly-source-settings-card" data-weekly-source-card="${esc(key)}"><h3>${esc(title)}</h3><div class="weekly-source-settings-fields">${body}</div></section>`;

  function clientCards(state) {
    if (!state?.eligible) return {};
    const d = state.draft || {};
    const c = state.capabilities || {};
    const matchingGroups = (state.source_groups || []).filter((group) => upper(group.source_family) === upper(c.source_family));
    const groupOptions = [option('', 'Choose a source group', !d.source_group_id)]
      .concat(matchingGroups.map((group) => option(group.id, group.display_name || group.code, d.source_group_id))).join('');
    let queries = c.source_family === 'NHSP'
      ? ''
      : row('Source group', `<select name="weekly_source_group_id">${groupOptions}</select>`);
    if (c.show_query_settings) {
      queries += check('weekly_source_candidate_queries_enabled', 'Ask candidates about differences', bool(d.candidate_queries_enabled));
      queries += check('weekly_source_manager_queries_enabled', 'Email managers about differences', bool(d.manager_queries_enabled));
      if (bool(d.manager_queries_enabled)) {
        queries += row('Manager email', `<input type="email" name="weekly_source_manager_query_recipient" value="${esc(d.manager_query_recipient || '')}"/>`);
      }
    }
    if (c.show_completed_pack) {
      queries += check('weekly_source_completed_pack_copy_enabled', 'Email the completed Timesheet', bool(d.completed_pack_copy_enabled));
      if (bool(d.completed_pack_copy_enabled)) {
        queries += row('Timesheet email', `<input type="email" name="weekly_source_completed_pack_recipient" value="${esc(d.completed_pack_recipient || '')}"/>`);
      }
    }

    let rate = '';
    if (c.show_rate_settings) {
      rate += row('Rate calculation', `<select name="weekly_source_rate_method">
        ${option('SPLIT_RATE_WINDOWS', 'Split across rate windows', d.weekly_rate_classification_method)}
        ${option('WHOLE_SHIFT_START_DAY', 'One rate for the whole shift', d.weekly_rate_classification_method)}
      </select>`);
      if (upper(d.weekly_rate_classification_method) === 'SPLIT_RATE_WINDOWS') {
        rate += row('Equal break portions', `<select name="weekly_source_break_tie_rule">
          ${option('EARLIEST_LONGEST_PORTION', 'Use the first longest portion', d.duration_break_tie_rule)}
          ${option('LATEST_LONGEST_PORTION', 'Use the last longest portion', d.duration_break_tie_rule)}
        </select>`);
      } else {
        rate += row('Classify the shift by', '<input type="text" value="Day shift starts" readonly/>');
      }
    }

    let correction = '';
    if (c.show_self_bill_correction) {
      correction = row('Corrections', `<select name="weekly_source_correction_presentation">
        ${option('NET_DIFFERENCE_PRESENTATION', 'Show the difference only', d.self_bill_correction_presentation)}
        ${option('FULL_REVERSAL_REPLACEMENT', 'Reverse the original and replace it', d.self_bill_correction_presentation)}
      </select>`);
    }

    let expenses = '';
    if (c.show_source_expenses) {
      expenses += check('weekly_source_fixed_expenses_enabled', 'Use expenses supplied in the weekly source', bool(d.source_fixed_expenses_enabled));
      if (bool(d.source_fixed_expenses_enabled)) {
        expenses += check('weekly_source_expense_vat_enabled', 'Add VAT to source expenses', bool(d.source_expense_vat_enabled));
      }
    }
    return {
      queries: card('queries', 'Weekly source queries', queries),
      rate: rate ? card('rate', 'Weekly rate calculation', rate) : '',
      correction: correction ? card('correction', 'Weekly self-bill corrections', correction) : '',
      expenses: expenses ? card('expenses', 'Expenses from weekly source', expenses) : ''
    };
  }

  function readClientDraft(root, state) {
    if (!root || !state) return;
    const get = (name) => root.querySelector(`[name="${name}"]`);
    const draft = state.draft || (state.draft = {});
    if (upper(state?.capabilities?.source_family) === 'NHSP') {
      draft.source_group_id = soleActiveNhspGroup(state.source_groups)?.id || null;
    } else {
      draft.source_group_id = text(get('weekly_source_group_id')?.value) || null;
    }
    if (get('weekly_source_candidate_queries_enabled')) draft.candidate_queries_enabled = !!get('weekly_source_candidate_queries_enabled').checked;
    if (get('weekly_source_manager_queries_enabled')) draft.manager_queries_enabled = !!get('weekly_source_manager_queries_enabled').checked;
    draft.manager_query_recipient = draft.manager_queries_enabled ? (text(get('weekly_source_manager_query_recipient')?.value) || null) : null;
    if (get('weekly_source_completed_pack_copy_enabled')) draft.completed_pack_copy_enabled = !!get('weekly_source_completed_pack_copy_enabled').checked;
    draft.completed_pack_recipient = draft.completed_pack_copy_enabled ? (text(get('weekly_source_completed_pack_recipient')?.value) || null) : null;
    if (get('weekly_source_rate_method')) draft.weekly_rate_classification_method = upper(get('weekly_source_rate_method').value);
    draft.duration_break_tie_rule = draft.weekly_rate_classification_method === 'SPLIT_RATE_WINDOWS'
      ? (upper(get('weekly_source_break_tie_rule')?.value) || 'EARLIEST_LONGEST_PORTION') : null;
    if (get('weekly_source_correction_presentation')) draft.self_bill_correction_presentation = upper(get('weekly_source_correction_presentation').value);
    if (get('weekly_source_fixed_expenses_enabled')) draft.source_fixed_expenses_enabled = !!get('weekly_source_fixed_expenses_enabled').checked;
    draft.source_expense_vat_enabled = draft.source_fixed_expenses_enabled && !!get('weekly_source_expense_vat_enabled')?.checked;
    state.dirty = JSON.stringify(state.baseline || {}) !== JSON.stringify(draft);
  }

  // The established Client settings form owns a capture-phase change handler
  // that can repaint its panels before target-level listeners run. Capture the
  // Weekly Source draft at the document boundary first, so that repaint keeps
  // the user's newly selected source group and message settings.
  function captureClientDraftBeforeLegacyRepaint(event) {
    const control = event?.target;
    if (!control?.closest?.('[data-weekly-source-card]')) return;
    const root = control.closest('#clientSettingsForm');
    const binding = root && CLIENT_BINDINGS.get(root);
    const state = binding?.ctx?.[CLIENT_STATE] || binding?.state;
    if (!root || !state) return;
    readClientDraft(root, state);
    signalDirty('weekly-source-client-settings');
  }

  try {
    global.document?.addEventListener?.('input', captureClientDraftBeforeLegacyRepaint, true);
    global.document?.addEventListener?.('change', captureClientDraftBeforeLegacyRepaint, true);
  } catch {}

  function paintClient(root, ctx) {
    const state = ctx?.[CLIENT_STATE];
    root?.querySelectorAll('[data-weekly-source-card]').forEach((node) => node.remove());
    if (!root || !state?.loaded || !state.eligible) return;
    const cards = clientCards(state);
    const timesheets = root.querySelector('[data-record-panel="timesheets"]');
    const shifts = root.querySelector('[data-record-panel="shifts"]');
    const invoicing = root.querySelector('[data-record-panel="invoicing"]');
    if (timesheets && cards.queries) timesheets.firstElementChild?.insertAdjacentHTML('afterend', cards.queries);
    if (shifts && cards.rate) shifts.insertAdjacentHTML('beforeend', cards.rate);
    if (invoicing && cards.correction) {
      const consolidation = Array.from(invoicing.querySelectorAll('.record-settings-card')).find((node) => node.querySelector('h3')?.textContent === 'Invoice consolidation');
      (consolidation || invoicing.firstElementChild)?.insertAdjacentHTML('afterend', cards.correction);
    }
    if (invoicing && cards.expenses) invoicing.insertAdjacentHTML('beforeend', cards.expenses);
    root.querySelectorAll('[data-weekly-source-card] input, [data-weekly-source-card] select').forEach((control) => {
      control.addEventListener('change', () => {
        readClientDraft(root, state);
        paintClient(root, ctx);
        signalDirty('weekly-source-client-settings');
      });
      control.addEventListener('input', () => {
        readClientDraft(root, state);
        signalDirty('weekly-source-client-settings');
      });
    });
    const frame = global.__getModalFrame?.();
    const editable = !frame || ['edit', 'create'].includes(frame.mode);
    if (!editable) root.querySelectorAll('[data-weekly-source-card] input, [data-weekly-source-card] select').forEach((control) => { control.disabled = true; });
  }

  function mountClient(root, ctx) {
    if (!root || !ctx) return;
    CLIENT_BINDINGS.set(root, { ctx, state: ctx[CLIENT_STATE] || null });
    const capability = legacyCapability(ctx);
    if (ctx[CLIENT_STATE]?.loaded) {
      const previous = ctx[CLIENT_STATE].capabilities || {};
      if (previous.source_family !== capability.source_family || previous.authority_mode !== capability.authority_mode) {
        const retainedGroups = ctx[CLIENT_STATE].source_groups || [];
        const requiresConfiguration = capability.eligible;
        ctx[CLIENT_STATE] = normaliseClientPayload({ source_groups: retainedGroups }, ctx);
        ctx[CLIENT_STATE].dirty = requiresConfiguration;
      }
      paintClient(root, ctx);
      return;
    }
    loadClient(ctx?.data?.id || null, ctx).then(() => paintClient(root, ctx)).catch(() => {});
  }

  async function saveClient(clientId, ctx) {
    let state = ctx?.[CLIENT_STATE];
    if (!state?.eligible || !state.dirty) return null;
    if (!state.settings_version || String(state.client_id || '') !== String(clientId || '')) {
      const stagedDraft = clone(state.draft || {});
      const freshPayload = await requestJson(`/clients/${encodeURIComponent(clientId)}`);
      const refreshed = normaliseClientPayload(freshPayload, ctx);
      refreshed.draft = {
        ...refreshed.draft,
        ...stagedDraft,
        effective_from: stagedDraft.effective_from || refreshed.draft.effective_from
      };
      refreshed.dirty = true;
      ctx[CLIENT_STATE] = refreshed;
      state = refreshed;
    }
    const payload = await requestJson(`/clients/${encodeURIComponent(clientId)}`, {
      method: 'PUT',
      body: JSON.stringify({ expected_settings_version: state.settings_version, settings: state.draft })
    });
    ctx[CLIENT_STATE] = normaliseClientPayload(payload, ctx);
    return payload;
  }

  function normaliseContractPayload(payload, clientPayload) {
    const capabilities = { ...(clientPayload?.capabilities || {}), ...(payload?.capabilities || {}) };
    const rawSettings = payload?.settings && typeof payload.settings === 'object' && !Array.isArray(payload.settings)
      ? payload.settings : {};
    const defaults = {
      effective_from: payload?.effective_date || null,
      weekly_rate_classification_method_override: null,
      duration_break_tie_rule_override: null,
      source_fixed_expenses_enabled_override: null,
      source_expense_vat_enabled_override: null,
      candidate_queries_enabled_override: null,
      manager_queries_enabled_override: null,
      manager_query_recipient_override: null,
      completed_pack_copy_enabled_override: null,
      completed_pack_recipient_override: null
    };
    const settings = { ...defaults };
    CONTRACT_SETTING_KEYS.forEach((key) => {
      if (Object.prototype.hasOwnProperty.call(rawSettings, key)) settings[key] = rawSettings[key];
    });
    return {
      loaded: true, eligible: payload?.eligible == null ? bool(clientPayload?.eligible) : bool(payload.eligible),
      contract_id: payload?.contract_id || null,
      settings_version: payload?.settings_version ?? null,
      capabilities, effective: clone(rawSettings.effective || clientPayload?.draft || {}),
      baseline: clone(settings), draft: clone(settings), dirty: false
    };
  }

  const inheritOption = (label, value, selected) => option('', `Use Client setting - ${label}`, selected == null) + option(value, label, selected);
  function triBooleanSelect(name, label, value, inherited) {
    return row(label, `<select name="${name}">
      ${option('', `Use Client setting - ${inherited ? 'On' : 'Off'}`, value == null)}
      ${option('true', 'On', value === true)}${option('false', 'Off', value === false)}
    </select>`);
  }

  function contractHtml(state) {
    if (!state?.eligible) return '';
    const d = state.draft || {}, e = state.effective || {}, c = state.capabilities || {};
    let rate = '';
    if (c.show_rate_settings !== false) {
      rate += row('Rate calculation', `<select name="weekly_source_contract_rate_method">
        ${option('', `Use Client setting - ${upper(e.weekly_rate_classification_method) === 'WHOLE_SHIFT_START_DAY' ? 'One rate whole shift' : 'Split rate windows'}`, d.weekly_rate_classification_method_override == null)}
        ${option('SPLIT_RATE_WINDOWS', 'Split across rate windows', d.weekly_rate_classification_method_override)}
        ${option('WHOLE_SHIFT_START_DAY', 'One rate for the whole shift', d.weekly_rate_classification_method_override)}
      </select>`);
      rate += row('Equal break portions', `<select name="weekly_source_contract_break_tie">
        ${option('', `Use Client setting - ${upper(e.duration_break_tie_rule) === 'LATEST_LONGEST_PORTION' ? 'Last longest portion' : 'First longest portion'}`, d.duration_break_tie_rule_override == null)}
        ${option('EARLIEST_LONGEST_PORTION', 'Use the first longest portion', d.duration_break_tie_rule_override)}
        ${option('LATEST_LONGEST_PORTION', 'Use the last longest portion', d.duration_break_tie_rule_override)}
      </select>`);
    }
    let expenses = '';
    if (c.show_source_expenses) {
      expenses += triBooleanSelect('weekly_source_contract_expenses', 'Use expenses supplied in the weekly source', d.source_fixed_expenses_enabled_override, bool(e.source_fixed_expenses_enabled));
      expenses += triBooleanSelect('weekly_source_contract_expense_vat', 'Add VAT to source expenses', d.source_expense_vat_enabled_override, bool(e.source_expense_vat_enabled));
    }
    let comms = '';
    if (c.show_query_settings) {
      comms += triBooleanSelect('weekly_source_contract_candidate_queries', 'Ask candidates about differences', d.candidate_queries_enabled_override, bool(e.candidate_queries_enabled));
      comms += triBooleanSelect('weekly_source_contract_manager_queries', 'Email managers about differences', d.manager_queries_enabled_override, bool(e.manager_queries_enabled));
      comms += row('Manager email', `<input type="email" name="weekly_source_contract_manager_email" value="${esc(d.manager_query_recipient_override || '')}" placeholder="Use Client setting"/>`);
    }
    if (c.show_completed_pack) {
      comms += triBooleanSelect('weekly_source_contract_completed_pack', 'Email the completed Timesheet', d.completed_pack_copy_enabled_override, bool(e.completed_pack_copy_enabled));
      comms += row('Timesheet email', `<input type="email" name="weekly_source_contract_pack_email" value="${esc(d.completed_pack_recipient_override || '')}" placeholder="Use Client setting"/>`);
    }
    return `<div data-weekly-source-contract-settings="1">
      ${expenses ? `<section class="ctms-policy-card"><div class="ctms-policy-card__heading"><div><h3>Expenses from weekly source</h3></div></div><div class="weekly-source-settings-fields">${expenses}</div></section>` : ''}
      ${rate ? `<section class="ctms-policy-card"><div class="ctms-policy-card__heading"><div><h3>Weekly rate calculation</h3></div></div><div class="weekly-source-settings-fields">${rate}</div></section>` : ''}
      ${comms ? `<section class="ctms-policy-card"><div class="ctms-policy-card__heading"><div><h3>Weekly source messages</h3></div></div><div class="weekly-source-settings-fields">${comms}</div></section>` : ''}
    </div>`;
  }

  const nullableBool = (value) => value === '' || value == null ? null : value === true || value === 'true';
  function collectContract(root, state) {
    if (!root || !state) return;
    const get = (name) => root.querySelector(`[name="${name}"]`);
    const d = state.draft || (state.draft = {});
    d.weekly_rate_classification_method_override = upper(get('weekly_source_contract_rate_method')?.value) || null;
    d.duration_break_tie_rule_override = upper(get('weekly_source_contract_break_tie')?.value) || null;
    d.source_fixed_expenses_enabled_override = nullableBool(get('weekly_source_contract_expenses')?.value);
    d.source_expense_vat_enabled_override = nullableBool(get('weekly_source_contract_expense_vat')?.value);
    d.candidate_queries_enabled_override = nullableBool(get('weekly_source_contract_candidate_queries')?.value);
    d.manager_queries_enabled_override = nullableBool(get('weekly_source_contract_manager_queries')?.value);
    d.manager_query_recipient_override = text(get('weekly_source_contract_manager_email')?.value) || null;
    d.completed_pack_copy_enabled_override = nullableBool(get('weekly_source_contract_completed_pack')?.value);
    d.completed_pack_recipient_override = text(get('weekly_source_contract_pack_email')?.value) || null;
    state.dirty = JSON.stringify(state.baseline || {}) !== JSON.stringify(d);
  }

  async function mountContract(root, parentCtx, contractId, clientId, viewOnly, childSession) {
    if (!root || !parentCtx) return;
    let state = parentCtx[CONTRACT_STATE];
    if (!state?.loaded) {
      let clientState = parentCtx[CLIENT_STATE];
      if (!clientState?.loaded && clientId) clientState = await loadClient(clientId, parentCtx);
      const payload = contractId ? await requestJson(`/contracts/${encodeURIComponent(contractId)}`) : {};
      state = parentCtx[CONTRACT_STATE] = normaliseContractPayload(payload, clientState);
    }
    if (state.child_session !== childSession || !state.child_draft) {
      state.child_session = childSession;
      state.child_draft = clone(state.draft || {});
    }
    const childState = { ...state, draft: state.child_draft, baseline: clone(state.draft || {}) };
    root.querySelector('[data-weekly-source-contract-settings]')?.remove();
    const html = contractHtml(childState);
    if (!html) return;
    const specific = root.querySelector('#contractSpecificSettingsHeading')?.closest('.ctms-contract-settings-card');
    const queryBlock = specific?.querySelector('.ctms-contract-query-email');
    if (queryBlock) queryBlock.insertAdjacentHTML('beforebegin', html);
    else specific?.insertAdjacentHTML('beforeend', html);
    const host = root.querySelector('[data-weekly-source-contract-settings]');
    host?.querySelectorAll('input,select').forEach((control) => {
      control.disabled = !!viewOnly;
      const onChange = () => {
        collectContract(root, childState);
        state.child_draft = clone(childState.draft);
        signalDirty('weekly-source-contract-settings');
      };
      control.addEventListener('input', onChange);
      control.addEventListener('change', onChange);
    });
  }

  function applyContractDraft(root, parentCtx, childSession) {
    const state = parentCtx?.[CONTRACT_STATE];
    if (!state?.loaded || state.child_session !== childSession) return;
    const childState = { ...state, draft: clone(state.child_draft || state.draft || {}), baseline: clone(state.draft || {}) };
    collectContract(root, childState);
    state.draft = clone(childState.draft);
    state.child_draft = clone(childState.draft);
    state.dirty = JSON.stringify(state.baseline || {}) !== JSON.stringify(state.draft || {});
  }
  async function saveContract(contractId, parentCtx) {
    let state = parentCtx?.[CONTRACT_STATE];
    if (!state?.eligible || !state.dirty) return null;
    if (!state.settings_version || String(state.contract_id || '') !== String(contractId || '')) {
      const stagedDraft = clone(state.draft || {});
      const freshPayload = await requestJson(`/contracts/${encodeURIComponent(contractId)}`);
      const refreshed = normaliseContractPayload(freshPayload, parentCtx[CLIENT_STATE]);
      refreshed.draft = {
        ...refreshed.draft,
        ...stagedDraft,
        effective_from: stagedDraft.effective_from || refreshed.draft.effective_from
      };
      refreshed.dirty = true;
      parentCtx[CONTRACT_STATE] = refreshed;
      state = refreshed;
    }
    const payload = await requestJson(`/contracts/${encodeURIComponent(contractId)}`, {
      method: 'PUT', body: JSON.stringify({ expected_settings_version: state.settings_version, settings: state.draft })
    });
    parentCtx[CONTRACT_STATE] = normaliseContractPayload(payload, parentCtx[CLIENT_STATE]);
    return payload;
  }

  function assertGlobalTimingState(payload) {
    const rev = (value) => typeof value === 'string' && /^[1-9][0-9]*$/.test(value)
      && (value.length < 19 || (value.length === 19 && value <= '9223372036854775807'));
    const keys = ['candidate_reminder_minutes','candidate_response_deadline_minutes','manager_digest_minutes',
      'manager_manual_resend_cooldown_minutes','candidate_manual_reminder_cooldown_minutes','manager_secure_link_days',
      'automatic_reconcile_actor_user_id','manager_first_delay_value','manager_first_delay_unit','manager_followups',
      'manager_recent_completed_weeks','manager_max_work_age_days'];
    const exact = (value,allowed) => value && typeof value === 'object' && !Array.isArray(value)
      && Object.keys(value).length === allowed.length && allowed.every(key => Object.hasOwn(value,key));
    if (!exact(payload,['ok','settings_version','policy_revision','settings','cutoff','actions']) || payload.ok !== true || !rev(payload.settings_version) || payload.policy_revision !== payload.settings_version
      || !payload.settings || Object.keys(payload.settings).length !== keys.length || keys.some(key => !Object.hasOwn(payload.settings,key))
      || !exact(payload.cutoff,['kind','editable','explanation']) || payload.cutoff?.kind !== 'CLIENT_CUTOFF_PRECEDENCE' || payload.cutoff?.editable !== false
      || typeof payload.cutoff?.explanation !== 'string' || !Array.isArray(payload.actions)
      || payload.actions.length !== 1 || !exact(payload.actions[0],['action','enabled','reason']) || payload.actions[0]?.action !== 'SAVE_GLOBAL_CONTACT_TIMING'
      || payload.actions[0]?.enabled !== true || payload.actions[0]?.reason !== null) {
      throw new Error('Contact settings cannot be confirmed. Refresh settings.');
    }
    if (payload.settings.automatic_reconcile_actor_user_id !== null
      && (typeof payload.settings.automatic_reconcile_actor_user_id !== 'string'
        || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(payload.settings.automatic_reconcile_actor_user_id))) {
      throw new Error('Contact settings cannot be confirmed. Refresh settings.');
    }
    validateGlobalTimingDraft(payload.settings);
    return payload;
  }
  function validateGlobalTimingDraft(d) {
    const integer = (v,min,max) => Number.isSafeInteger(v) && v >= min && v <= max;
    const interval = (v,u,zero) => ['HOURS','CALENDAR_DAYS','CLIENT_WORKING_DAYS'].includes(u)
      && integer(v,zero ? 0 : 1,u === 'HOURS' ? 8784 : 366);
    if (!integer(d.candidate_reminder_minutes,1,719)
      || !integer(d.candidate_response_deadline_minutes,d.candidate_reminder_minutes+1,10080)
      || !integer(d.manager_digest_minutes,1,10080) || !integer(d.manager_manual_resend_cooldown_minutes,5,1440)
      || !integer(d.candidate_manual_reminder_cooldown_minutes,60,10080) || !integer(d.manager_secure_link_days,1,30)
      || !interval(d.manager_first_delay_value,d.manager_first_delay_unit,true)
      || !integer(d.manager_recent_completed_weeks,1,52) || !integer(d.manager_max_work_age_days,1,366)
      || !Array.isArray(d.manager_followups) || d.manager_followups.length > 10
      || d.manager_followups.some((step,index) => !step || Object.keys(step).length !== 4
        || !['sequence','interval_value','interval_unit','mode'].every(key => Object.hasOwn(step,key))
        || step.sequence !== index+1 || !interval(step.interval_value,step.interval_unit,false)
        || !['MANUAL','AUTOMATIC'].includes(step.mode))) {
      throw new Error('Check the contact timing values and ordered follow-ups.');
    }
    return d;
  }
  function timingUnitOptions(selected) {
    return [['HOURS','Hours'],['CALENDAR_DAYS','Calendar days'],['CLIENT_WORKING_DAYS','Client working days']]
      .map(([value,label]) => option(value,label,selected)).join('');
  }

  function canonicalGlobalSettings(value) {
    const settings = { ...(value || {}) };
    // These old Office-only aliases are not part of the SQL save contract.
    // Fresh server canonical fields are authoritative, including cached drafts.
    delete settings.manager_partial_digest_minutes;
    delete settings.manager_manual_send_cooldown_minutes;
    return settings;
  }

  async function loadGlobal(ctx, force = false) {
    if (!force && ctx?.[GLOBAL_STATE]?.loaded) return ctx[GLOBAL_STATE];
    const [globalPayload, groupPayload] = await Promise.all([
      requestJson('/global'), requestJson('/source-groups')
    ]);
    assertGlobalTimingState(globalPayload);
    const settings = canonicalGlobalSettings({
      candidate_reminder_minutes: 360,
      candidate_response_deadline_minutes: 720,
      manager_digest_minutes: 360,
      manager_manual_resend_cooldown_minutes: 5,
      candidate_manual_reminder_cooldown_minutes: 60,
      manager_secure_link_days: 7,
      ...(globalPayload?.settings || {})
    });
    const sourceGroups = (groupPayload?.source_groups || []).map(normaliseSourceGroupDraft);
    const state = {
      loaded: true, settings_version: globalPayload?.settings_version ?? settings.version ?? null,
      baseline: clone(settings), draft: clone(settings),
      source_groups: clone(sourceGroups), source_groups_baseline: clone(sourceGroups),
      policy_revision: globalPayload.policy_revision, cutoff: clone(globalPayload.cutoff), actions: clone(globalPayload.actions),
      pending_global_save: null, dirty: false, groups_dirty: false
    };
    if (ctx) ctx[GLOBAL_STATE] = state;
    return state;
  }

  function globalHtml(state) {
    const d = state.draft || {};
    const timing = [
      row('Candidate reminder', `<input type="number" min="1" name="weekly_source_candidate_reminder_hours" value="${esc(Number(d.candidate_reminder_minutes || 360) / 60)}"/><small>hours</small>`),
      row('Candidate response time', `<input type="number" min="1" name="weekly_source_candidate_deadline_hours" value="${esc(Number(d.candidate_response_deadline_minutes || 720) / 60)}"/><small>hours</small>`),
      row('Manager email wait', `<input type="number" min="0.016666666666666666" step="any" name="weekly_source_manager_digest_hours" value="${esc(Number(d.manager_digest_minutes ?? 360) / 60)}"/><small>hours</small>`),
      row('Manager resend wait', `<input type="number" min="5" name="weekly_source_manager_resend_minutes" value="${esc(d.manager_manual_resend_cooldown_minutes ?? 5)}"/><small>minutes</small>`),
      row('Candidate reminder wait', `<input type="number" min="60" name="weekly_source_candidate_resend_minutes" value="${esc(d.candidate_manual_reminder_cooldown_minutes || 60)}"/><small>minutes</small>`),
      row('Manager link valid for', `<input type="number" min="1" max="30" name="weekly_source_manager_link_days" value="${esc(d.manager_secure_link_days || 7)}"/><small>days</small>`)
    ].join('');
    const followups = (d.manager_followups || []).map((step,index) => `<div data-manager-followup-index="${index}">
      <label>Follow-up ${index+1}<input type="number" min="1" max="8784" name="manager_followup_value" value="${esc(step.interval_value)}"/></label>
      <select name="manager_followup_unit">${timingUnitOptions(step.interval_unit)}</select>
      <select name="manager_followup_mode">${option('MANUAL','Manual',step.mode)}${option('AUTOMATIC','Automatic',step.mode)}</select>
      <button type="button" class="btn" data-remove-manager-followup="${index}">Remove follow-up ${index+1}</button>
    </div>`).join('');
    const managerTiming = `<h3>Manager contact timing</h3>
      <label>First enquiry delay<input type="number" min="0" max="8784" name="weekly_source_manager_first_delay_value" value="${esc(d.manager_first_delay_value)}"/></label>
      <select name="weekly_source_manager_first_delay_unit">${timingUnitOptions(d.manager_first_delay_unit)}</select>
      <div data-manager-followups>${followups}</div>
      <button type="button" class="btn" data-add-manager-followup ${d.manager_followups?.length >= 10 ? 'disabled' : ''}>Add follow-up</button>
      <p>Each follow-up runs from the preceding accepted send. Manual steps wait for an Office action.</p>
      <label>Recent completed weeks<input type="number" min="1" max="52" name="weekly_source_manager_recent_completed_weeks" value="${esc(d.manager_recent_completed_weeks)}"/></label>
      <label>Maximum work age (calendar days)<input type="number" min="1" max="366" name="weekly_source_manager_max_work_age_days" value="${esc(d.manager_max_work_age_days)}"/></label>
      <p>${esc(state.cutoff?.explanation || '')}</p>`;
    const nhspGroupIndex = (state.source_groups || []).findIndex((group) => upper(group.source_family) === 'NHSP');
    const groups = (state.source_groups || []).map((group, index) => `<div class="weekly-source-group-row" data-source-group-index="${index}">
      <input type="hidden" name="source_group_id" value="${esc(group.id || '')}"/>
      <label><span>Name</span><input name="source_group_display_name" value="${esc(group.display_name || '')}"/></label>
      <label><span>Source</span><select name="source_group_family">${
        (nhspGroupIndex < 0 || nhspGroupIndex === index ? option('NHSP', 'NHSP', group.source_family) : '')
      }${option('ROSTER', 'Roster', group.source_family)}</select></label>
      <label><span>Cut-off day</span><select name="source_group_cutoff_weekday">${['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'].map((label, value) => option(value, label, Number(group.cutoff_weekday))).join('')}</select></label>
      <label><span>Cut-off time</span><input type="time" name="source_group_cutoff_time" value="${esc(text(group.cutoff_local_time).slice(0,5) || '15:00')}"/></label>
      <label class="source-group-nhsp-heading" ${upper(group.source_family) === 'NHSP' ? '' : 'hidden'}><span>Report heading</span><input name="source_group_heading" value="${esc(group.nhsp_report_heading_name || '')}"/></label>
      ${check('source_group_active', 'Active', group.active !== false)}
    </div>`).join('');
    return `<section class="card weekly-source-global-settings" data-weekly-source-global-settings="1">
      <h3>Weekly source</h3><div class="weekly-source-settings-fields">${timing}</div>
      ${managerTiming}
      <h3>Source groups</h3><div data-weekly-source-groups>${groups}</div>
      <button type="button" class="btn" data-add-weekly-source-group>Add source group</button>
    </section>`;
  }

  function normaliseSourceGroupDraft(group) {
    const expectedVersion = Number(group?.expected_version ?? group?.version ?? 0);
    return {
      id: text(group?.id) || null,
      expected_version: Number.isInteger(expectedVersion) && expectedVersion > 0 ? expectedVersion : null,
      code: text(group?.code) || null,
      display_name: text(group?.display_name),
      source_family: upper(group?.source_family),
      cutoff_weekday: Number(group?.cutoff_weekday),
      cutoff_local_time: text(group?.cutoff_local_time).slice(0, 5),
      nhsp_report_heading_name: upper(group?.source_family) === 'NHSP'
        ? (text(group?.nhsp_report_heading_name) || null) : null,
      active: group?.active !== false
    };
  }

  function readGlobal(root, state) {
    const value = (name) => root.querySelector(`[name="${name}"]`)?.value;
    const positive = (name, multiplier = 1) => Math.round(Number(value(name)) * multiplier);
    state.draft = canonicalGlobalSettings(state.draft);
    state.draft.candidate_reminder_minutes = positive('weekly_source_candidate_reminder_hours', 60);
    state.draft.candidate_response_deadline_minutes = positive('weekly_source_candidate_deadline_hours', 60);
    state.draft.manager_digest_minutes = positive('weekly_source_manager_digest_hours', 60);
    state.draft.manager_manual_resend_cooldown_minutes = positive('weekly_source_manager_resend_minutes');
    state.draft.candidate_manual_reminder_cooldown_minutes = positive('weekly_source_candidate_resend_minutes');
    state.draft.manager_secure_link_days = positive('weekly_source_manager_link_days');
    state.draft.manager_first_delay_value = value('weekly_source_manager_first_delay_value') === '' ? NaN : Number(value('weekly_source_manager_first_delay_value'));
    state.draft.manager_first_delay_unit = value('weekly_source_manager_first_delay_unit');
    state.draft.manager_recent_completed_weeks = Number(value('weekly_source_manager_recent_completed_weeks'));
    state.draft.manager_max_work_age_days = Number(value('weekly_source_manager_max_work_age_days'));
    state.draft.manager_followups = Array.from(root.querySelectorAll('[data-manager-followup-index]')).map((step,index) => ({
      sequence:index+1,interval_value:Number(step.querySelector('[name="manager_followup_value"]')?.value),
      interval_unit:step.querySelector('[name="manager_followup_unit"]')?.value,mode:step.querySelector('[name="manager_followup_mode"]')?.value
    }));
    state.dirty = JSON.stringify(state.baseline || {}) !== JSON.stringify(state.draft);
    const priorGroups = state.source_groups || [];
    const nextGroups = Array.from(root.querySelectorAll('[data-source-group-index]')).map((groupRoot, index) => normaliseSourceGroupDraft({
      id: groupRoot.querySelector('[name="source_group_id"]')?.value,
      expected_version: groupRoot.dataset.expectedVersion,
      code: priorGroups[index]?.code,
      display_name: groupRoot.querySelector('[name="source_group_display_name"]')?.value,
      source_family: groupRoot.querySelector('[name="source_group_family"]')?.value,
      cutoff_weekday: groupRoot.querySelector('[name="source_group_cutoff_weekday"]')?.value,
      cutoff_local_time: groupRoot.querySelector('[name="source_group_cutoff_time"]')?.value,
      nhsp_report_heading_name: groupRoot.querySelector('[name="source_group_heading"]')?.value,
      active: !!groupRoot.querySelector('[name="source_group_active"]')?.checked
    }));
    state.source_groups = nextGroups;
    state.groups_dirty = JSON.stringify(state.source_groups_baseline || []) !== JSON.stringify(nextGroups);
  }

  function mountGlobal(root, ctx) {
    if (!root || !ctx) return;
    const mount = (state) => {
      root.querySelector('[data-weekly-source-global-settings]')?.remove();
      root.insertAdjacentHTML('beforeend', globalHtml(state));
      const host = root.querySelector('[data-weekly-source-global-settings]');
      (state.source_groups || []).forEach((group, index) => {
        const groupRoot = host?.querySelector(`[data-source-group-index="${index}"]`);
        if (groupRoot) groupRoot.dataset.expectedVersion = String(group.expected_version || group.version || 0);
      });
      host?.querySelector('[data-add-manager-followup]')?.addEventListener('click', () => {
        readGlobal(host,state);
        if (state.draft.manager_followups.length >= 10) return;
        state.draft.manager_followups.push({sequence:state.draft.manager_followups.length+1,interval_value:2,interval_unit:'CLIENT_WORKING_DAYS',mode:'MANUAL'});
        state.dirty = true; mount(state); signalDirty('weekly-source-global-settings');
      });
      host?.querySelectorAll('[data-remove-manager-followup]').forEach(button => button.addEventListener('click', () => {
        readGlobal(host,state);
        state.draft.manager_followups.splice(Number(button.dataset.removeManagerFollowup),1);
        state.draft.manager_followups.forEach((step,index) => {step.sequence=index+1;});
        state.dirty = JSON.stringify(state.baseline || {}) !== JSON.stringify(state.draft);
        mount(state); signalDirty('weekly-source-global-settings');
      }));
      host?.querySelector('[data-add-weekly-source-group]')?.addEventListener('click', () => {
        readGlobal(host, state);
        state.source_groups.push(normaliseSourceGroupDraft({ display_name: '', source_family: 'ROSTER', cutoff_weekday: 3, cutoff_local_time: '15:00', active: true }));
        state.groups_dirty = true;
        mount(state); signalDirty('weekly-source-global-settings');
      });
      host?.querySelectorAll('input,select').forEach((control) => {
        const onChange = () => {
          readGlobal(host, state);
          host.querySelectorAll('.weekly-source-group-row').forEach((groupRoot) => {
            const family = upper(groupRoot.querySelector('[name="source_group_family"]')?.value);
            const heading = groupRoot.querySelector('.source-group-nhsp-heading');
            if (heading) heading.hidden = family !== 'NHSP';
          });
          signalDirty('weekly-source-global-settings');
        };
        control.addEventListener('input', onChange); control.addEventListener('change', onChange);
      });
      const frame = global.__getModalFrame?.();
      const editable = !frame || ['edit', 'create'].includes(frame.mode);
      if (!editable) {
        host?.querySelectorAll('input,select,button').forEach((control) => { control.disabled = true; });
      }
      global.CloudTMSWeeklySourceRuleSettings?.mount(root, ctx, state.source_groups || []);
    };
    if (ctx[GLOBAL_STATE]?.loaded) mount(ctx[GLOBAL_STATE]);
    else loadGlobal(ctx).then(mount).catch(() => {});
  }

  async function saveGlobal(ctx) {
    const state = ctx?.[GLOBAL_STATE];
    if (!state?.loaded) return null;
    const mounted = global.document?.querySelector?.('[data-weekly-source-global-settings]');
    if (mounted) readGlobal(mounted, state);
    if ((state.source_groups || []).filter((group) => upper(group.source_family) === 'NHSP').length > 1) {
      throw new Error('Only one NHSP source group can be used. All NHSP Clients share its cut-off time.');
    }
    let globalResult = null;
    if (state.dirty) {
      state.draft = validateGlobalTimingDraft(canonicalGlobalSettings(state.draft));
      const content = JSON.stringify({expected_settings_version:state.settings_version,settings:state.draft});
      if (state.pending_global_save && state.pending_global_save.content !== content) {
        throw new Error('A previous save is being checked. Retry that save or refresh settings before changing it.');
      }
      if (!state.pending_global_save) {
        if (typeof global.crypto?.randomUUID !== 'function') throw new Error('A save identity could not be created. Refresh settings.');
        state.pending_global_save = {content,body:JSON.stringify({request_id:global.crypto.randomUUID(),...JSON.parse(content)})};
      }
      // Keep this exact request on transport/response uncertainty, including a
      // successful HTTP response which fails the strict current-state decoder.
      globalResult = assertGlobalTimingState(await requestJson('/global', {method:'PUT',body:state.pending_global_save.body}));
      state.settings_version = globalResult.settings_version;
      state.policy_revision = globalResult.policy_revision;
      state.baseline = clone(globalResult.settings); state.draft = clone(globalResult.settings);
      state.cutoff = clone(globalResult.cutoff); state.actions = clone(globalResult.actions);
      state.pending_global_save = null; state.dirty = false;
    }
    if (state.groups_dirty) {
      for (const sourceGroup of state.source_groups) {
        await requestJson('/source-groups', { method: 'PUT', body: JSON.stringify({ source_group: sourceGroup }) });
      }
    }
    await loadGlobal(ctx, true);
    return globalResult;
  }

  const api = Object.freeze({
    legacyCapability, defaultPolicy, normaliseClientPayload, normaliseContractPayload, normaliseSourceGroupDraft,
    soleActiveNhspGroup, clientCards, globalHtml,
    loadClient, mountClient, saveClient,
    mountContract, applyContractDraft, saveContract,
    loadGlobal, mountGlobal, saveGlobal,
    stateKeys: Object.freeze({ CLIENT_STATE, CONTRACT_STATE, GLOBAL_STATE })
  });
  global.CloudTMSWeeklySourceSettings = api;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof window === 'undefined' ? globalThis : window);
