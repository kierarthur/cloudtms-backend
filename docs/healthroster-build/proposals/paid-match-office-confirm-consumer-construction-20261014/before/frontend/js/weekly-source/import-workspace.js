(function initialiseWeeklySourceImportWorkspace(root, factory) {
  'use strict';
  const api = factory(root);
  if (typeof module === 'object' && module && module.exports) module.exports = api;
  if (typeof window === 'object' && root === window && !root.CloudTMSWeeklySourceImportWorkspaceV1) {
    Object.defineProperty(root, 'CloudTMSWeeklySourceImportWorkspaceV1', {
      configurable: false, enumerable: true, writable: false, value: api
    });
  }
})(typeof globalThis !== 'undefined' ? globalThis : this, function buildWeeklySourceImportWorkspace(root) {
  'use strict';

  const CONTRACT = 'WEEKLY_SOURCE_IMPORT_WORKSPACE_V1';
  const ENDPOINTS = Object.freeze({
    workspace: '/api/weekly-source/v1/workspace',
    preview: '/api/weekly-source/v1/uploads/preview',
    accept: '/api/weekly-source/v1/uploads/accept',
    command: '/api/weekly-source/v1/commands',
    checklist: '/api/weekly-source/v1/checklist'
  });
  const TABS = Object.freeze(['imports', 'queries', 'finalise', 'history', 'checklist']);
  const CYCLE_STATES = new Set(['Before cutoff', 'Ready for finalisation', 'Finalised', 'Correction in progress']);
  const FINALISE_LABELS = Object.freeze({
    NHSP_FINAL_BACKING_V1: 'Finalise report',
    NHSP_BACKING_REPORT_ACTUAL_V1: 'Finalise report',
    HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1: 'Finalise source',
    HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1: 'Finalise source',
    HEALTHROSTER_TIMESHEET_ACTUAL_V1: 'Finalise source',
    HEALTHROSTER_TIMESHEET_ACTUAL_LAYOUT_A_V1: 'Finalise source',
    HEALTHROSTER_TIMESHEET_ACTUAL_LAYOUT_B_V1: 'Finalise source',
    ROSTER_WEEKLY_SUMMARY_ACTUAL_V1: 'Finalise week'
  });
  const ACTIONS = Object.freeze({
    imports: new Set(['Review', 'Review pricing', 'View', 'View final source', 'Correct final source', 'View Timesheet', 'Email manager', 'Continue daily check', 'View daily check']),
    queries: new Set(['Open', 'View details', 'Remind candidate', 'Remind missing timesheet', 'Retry recheck', 'Send reminder', 'Send reminder for selected', 'Send again', 'Correct this Timesheet', 'Cancel correction', 'Authorise both', 'Open Timesheet']),
    shifts: new Set(['Accept system hours', 'Accept current source hours', 'View details', 'Protect pay', 'Change protected shift', 'Review protected pay', 'Review and reconcile']),
    finalise: new Set(['Confirm shift match', 'Link candidate', 'Link client', 'Choose contract', 'Create contract', 'Create contract for this band', 'Review overlapping shift', 'Open Banking Pay', 'Upload corrected source', 'Open charge details', 'Review source details', 'View query', 'Protect pay', 'View details', 'Send to Pay Queries', 'Send back to Queries']),
    tracker: new Set(['No shifts to import', 'View'])
  });
  const TONES = new Set(['neutral', 'info', 'warning', 'positive', 'danger']);
  // These labels can be received during the ordered server rollout. Enable
  // them only when their exact request contracts and handlers are installed.
  const PENDING_ACTIONS = new Set(['Continue daily check', 'View daily check', 'Send reminder',
    'Send reminder for selected', 'Send again', 'Correct this Timesheet', 'Cancel correction',
    'Authorise both', 'Open Timesheet']);
  // This is deliberately a presentation contract, not a charge calculator.
  // The Weekly Source service has already classified every line and sealed the
  // acceptance binding.  Office can select only the server-provided warning
  // keys, and returns that exact binding with the selected keys.  In
  // particular, source or Contract money never enters this browser contract.
  const RATE_WARNING_CONTRACT = 'NHSP_RATE_WARNING_WORKSPACE_V1';
  const RATE_WARNING_PHASES = new Set(['PREFINAL', 'FINAL_AWAITING_ACCEPTANCE', 'READY']);
  const RATE_WARNING_ACTION = 'ACCEPT_NHSP_SOURCE_CHARGES';
  const NHSP_UPLOAD_PROFILES = Object.freeze([
    Object.freeze({ id: 'NHSP_PREFINAL_RELEASED_V1', label: 'Previously released shifts' }),
    Object.freeze({ id: 'NHSP_FINAL_BACKING_V1', label: 'Final backing report' })
  ]);
  const MUTATING_ACTIONS = new Set([
    'Review', 'Review pricing', 'Correct final source', 'Remind candidate', 'Accept system hours',
    'Link candidate', 'Link client', 'Choose contract', 'Create contract', 'Create contract for this band',
    'Review overlapping shift', 'Upload corrected source', 'Protect pay', 'No shifts to import', 'Confirm shift match',
    'Accept current source hours', 'Send to Pay Queries', 'Send back to Queries'
  ]);
  const asText = (value) => String(value == null ? '' : value).trim();
  const asArray = (value) => Array.isArray(value) ? value : [];
  const asObject = (value) => value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  const integer = (value) => Number.isInteger(Number(value)) ? Math.max(0, Number(value)) : 0;
  const escapeHtml = (value) => String(value == null ? '' : value)
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
  const tone = (value, fallback = 'neutral') => TONES.has(asText(value).toLowerCase()) ? asText(value).toLowerCase() : fallback;
  const safeKey = (value) => asText(value).replace(/[^a-z0-9_-]+/gi, '-').replace(/^-+|-+$/g, '') || 'row';

  const NEXT_PAY_NOTICE = 'Pay is published through Banking Pay NEXT; there is nothing to recover here';

  const SOURCE_ADMISSION_BUSY_MESSAGE = 'Another Source update is in progress. Try again in a moment.';
  function isSourceAdmissionBusy(error) {
    const body = error?.json || error?.response || error;
    return error?.status === 409 && body && !Array.isArray(body)
      && (body.error_code || body.code) === 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY'
      && body.retryable === true && body.failed_rpc_outcome === 'ROLLED_BACK'
      && body.automatic_retry === false;
  }

  function friendlyWorkspaceError(value, owner = session.bankingOwnerClient?.peek()) {
    if (isSourceAdmissionBusy(value)) return SOURCE_ADMISSION_BUSY_MESSAGE;
    const error = value instanceof Error ? value : null;
    const raw = asText(error?.message || value);
    const code = asText(error?.code || raw.match(/(?:WEEKLY_SOURCE|BPAY_NEXT)_[A-Z0-9_]+/)?.[0]).toUpperCase();
    if (code === 'BPAY_NEXT_LEGACY_WRITER_RETIRED' && owner?.active_owner === 'NEXT') return NEXT_PAY_NOTICE;
    if (code === 'WEEKLY_SOURCE_CYCLE_NOT_FOUND') return 'This source is still being prepared. Recheck in a moment.';
    if (code === 'WEEKLY_SOURCE_CUTOFF_NOT_REACHED') return 'This source can be finalised after the cutoff shown above.';
    if (code === 'WEEKLY_SOURCE_GROUP_NOT_ACTIVE') return 'This source is no longer available. Choose another source.';
    if (code === 'WEEKLY_SOURCE_CLIENT_NOT_IN_GROUP') return 'This client is not available for the selected source and week.';
    if (code === 'WEEKLY_SOURCE_WORKSPACE_CURSOR_STALE') return 'This information has changed. Recheck before continuing.';
    if (code === 'WEEKLY_SOURCE_CHECKLIST_QUERY_TARGET_STALE') return 'This query has changed. Return to Checklist and recheck before continuing.';
    if (code === 'WEEKLY_SOURCE_RECHECK_INCOMPLETE') return 'Your selection was saved, but the source recheck did not finish. Retry the saved recheck in Office Checks.';
    return 'Weekly source information is temporarily unavailable. Recheck or try again shortly.';
  }

  function normaliseStatus(value) {
    const raw = typeof value === 'string' ? { text: value } : asObject(value);
    return { text: asText(raw.text) || '—', tone: tone(raw.tone), subtext: asText(raw.subtext), subtone: tone(raw.subtone) };
  }

  function normalisePage(value) {
    const raw = asObject(value);
    return {
      rows: asArray(raw.rows), total_count: integer(raw.total_count), next_cursor: asText(raw.next_cursor),
      has_more: raw.has_more === true, record_version: asText(raw.record_version), stale: raw.stale === true
    };
  }

  function normaliseControls(value) {
    return asArray(value).slice(0, 5).map((entry, index) => {
      const raw = asObject(entry);
      const options = asArray(raw.options).map((option) => ({ value: asText(option?.value), label: asText(option?.label),
        scope_client_id: asText(option?.scope_client_id), finalisation_week_ending: asText(option?.finalisation_week_ending)
      })).filter((option) => option.value && option.label);
      return { key: asText(raw.key) || `context_${index + 1}`, label: asText(raw.label) || 'Context', value: asText(raw.value), options };
    });
  }

  function normaliseFinalisationTracker(value) {
    const raw = asObject(value);
    return {
      title: asText(raw.title) || 'Finalisation progress',
      cycle_label: asText(raw.cycle_label),
      cycle_id: asText(raw.cycle_id),
      cycle_options: asArray(raw.cycle_options).map((option) => ({ value: asText(option?.value), label: asText(option?.label) })).filter((option) => option.value && option.label),
      complete: raw.complete === true,
      rows: asArray(raw.rows).map((entry, index) => {
        const row = asObject(entry);
        return {
          row_key: asText(row.row_key) || `tracker-${index + 1}`,
          source: asText(row.source),
          client: asText(row.client || row.trust),
          status: normaliseStatus(row.status),
          detail: asText(row.detail),
          actions: normaliseActions(row.actions, ACTIONS.tracker)
        };
      })
    };
  }

  function normaliseApprovedHoursFollowUp(value) {
    const raw = asObject(value);
    const action = asObject(raw.action);
    const command = asText(action.command).toUpperCase();
    return {
      state: asText(raw.state).toUpperCase(),
      title: asText(raw.title),
      body: asText(raw.body),
      action: action.label && ['FINALISE_WEEK', 'RECOVER_FINALISED_PAY'].includes(command)
        ? { label: asText(action.label), command, payload: asObject(action.payload) }
        : null
    };
  }

  function normaliseBulkOutreachAction(value, command) {
    const raw = asObject(value);
    const request = asObject(raw.request);
    const selection = asObject(request.selection);
    const proof = asText(raw.selection_proof);
    const list = (entry) => asArray(entry).map(asText).filter(Boolean);
    const valid = asText(raw.action).toUpperCase() === command
      && asText(request.action).toUpperCase() === command
      && [request.actor_user_id, request.source_cycle_id, request.projection_publication_id, request.expected_workspace_version].every((entry) => asText(entry))
      && /^[0-9a-f]{64}$/.test(proof)
      && asText(selection.selection_proof) === proof
      && asText(selection.mode).toUpperCase() === 'ALL_FILTERED'
      && list(selection.group_keys).length === 0
      && list(selection.excluded_group_keys).length === 0
      && list(selection.incident_ids).length === 0
      && ['asc', 'desc'].includes(asText(selection.sort_direction).toLowerCase())
      && !!asText(selection.sort_key)
      && Object.keys(asObject(selection.filters)).every((key) => ['status', 'candidate', 'issue'].includes(key));
    if (!valid) return null;
    return {
      action: command,
      enabled: raw.enabled === true,
      eligible_group_count: integer(raw.eligible_group_count),
      selection_proof: proof,
      request: {
        ...request,
        selection: {
          ...selection,
          mode: 'ALL_FILTERED',
          group_keys: [],
          excluded_group_keys: [],
          incident_ids: [],
          filters: { ...asObject(selection.filters) },
          sort_key: asText(selection.sort_key),
          sort_direction: asText(selection.sort_direction).toLowerCase(),
          selection_proof: proof
        }
      }
    };
  }

  function normaliseBulkActions(value) {
    const raw = asObject(value);
    if (asText(raw.contract) !== 'WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1' || raw.selection_complete !== true) {
      return { contract: '', filtered_group_count: 0, selection_complete: false, ask_candidates: null, send_manager_now: null };
    }
    return {
      contract: 'WEEKLY_SOURCE_BULK_FILTER_SELECTION_V1',
      filtered_group_count: integer(raw.filtered_group_count),
      selection_complete: true,
      ask_candidates: normaliseBulkOutreachAction(raw.ask_candidates, 'ASK_CANDIDATES'),
      send_manager_now: normaliseBulkOutreachAction(raw.send_manager_now, 'SEND_MANAGER_NOW')
    };
  }

  function normaliseRateWarningRow(value) {
    const raw = asObject(value);
    const key = asText(raw.warning_key || raw.row_key || raw.source_line_key);
    const detailRows = asArray(raw.detail_rows).slice(0, 100).map((entry) => {
      const detail = asObject(entry);
      return {
        candidate: asText(detail.candidate), day_date: asText(detail.day_date),
        source_charge: asText(detail.source_charge), warning: asText(detail.warning)
      };
    }).filter((detail) => detail.candidate || detail.day_date || detail.warning);
    return {
      // A client-generated fallback key would turn an unbound warning into an
      // acceptance candidate, so a missing key is intentionally not rendered
      // as selectable (and disables the whole acceptance request below).
      warning_key: key,
      source_row_id: asText(raw.source_row_id),
      candidate: asText(raw.candidate), day_date: asText(raw.day_date),
      source_charge: asText(raw.source_charge), warning: asText(raw.warning),
      commission: asText(raw.commission), total_cost: asText(raw.total_cost),
      calculated_charge: asText(raw.calculated_charge), difference: asText(raw.difference),
      contract: asText(raw.contract),
      warning_tone: tone(raw.warning_tone || raw.tone, 'warning'),
      action_label: asText(raw.action_label) || (detailRows.length ? 'View affected shifts' : 'Review rate warning'),
      accept_eligible: raw.accept_eligible === true,
      detail_rows: detailRows
    };
  }

  function normaliseRateWarningAcceptance(value) {
    const raw = asObject(value);
    const selection = asObject(raw.selection);
    const action = asText(raw.action).toUpperCase();
    const selectionKey = asText(selection.key);
    const proofKey = asText(selection.proof_key);
    const proof = asText(selection.proof).toLowerCase();
    const valid = raw.enabled === true
      && action === RATE_WARNING_ACTION
      && /^[a-z][a-z0-9_]{0,63}$/.test(selectionKey)
      && /^[a-z][a-z0-9_]{0,63}$/.test(proofKey)
      && /^[0-9a-f]{64}$/.test(proof)
      && Object.keys(asObject(raw.payload)).every((key) => /^[a-z][a-z0-9_]{0,63}$/.test(key));
    return valid ? {
      action, payload: { ...asObject(raw.payload) },
      selection: { key: selectionKey, proof_key: proofKey, proof }
    } : null;
  }

  function normaliseRateWarnings(value, profileId) {
    const raw = asObject(value);
    const phase = asText(raw.phase).toUpperCase();
    // No generic Weekly route receives the NHSP rate-card presentation.  A
    // malformed or absent projection is simply unavailable rather than being
    // inferred from a numeric field in a row.
    if (!/^NHSP_/.test(profileId) || asText(raw.contract) !== RATE_WARNING_CONTRACT || !RATE_WARNING_PHASES.has(phase)) {
      return { available: false, phase: '', rows: [], total_count: 0, accepted_count: 0, ready_count: 0, hard_blocker_count: 0, notice: null, acceptance: null };
    }
    const rawRows = asArray(raw.rows);
    const rows = rawRows.map(normaliseRateWarningRow).filter((row) => !!row.warning_key);
    const boundRows = rows.length === rawRows.length && new Set(rows.map((row) => row.warning_key)).size === rows.length;
    const notice = asObject(raw.notice);
    return {
      available: true, phase, rows,
      total_count: integer(raw.total_count || rows.length),
      accepted_count: integer(raw.accepted_count), ready_count: integer(raw.ready_count),
      hard_blocker_count: integer(raw.hard_blocker_count),
      notice: (asText(notice.title) || asText(notice.body)) ? { title: asText(notice.title), body: asText(notice.body), tone: tone(notice.tone, 'warning') } : null,
      acceptance: boundRows ? normaliseRateWarningAcceptance(raw.acceptance) : null
    };
  }

  function normaliseWorkspace(payload) {
    const source = asObject(payload?.workspace || payload);
    const profile = asObject(source.profile);
    const profileId = asText(profile.id || source.profile_id || source.source_profile_kind);
    const context = asObject(source.context);
    const counts = asObject(source.counts);
    const cycleState = asText(context.cycle_state || source.cycle_state);
    const finalise = asObject(source.finalise);
    const queries = asObject(source.queries);
    const imports = asObject(source.imports);
    const journey = asObject(imports.journey);
    const permissions = asObject(source.permissions);
    const payView = permissions.contract === 'WEEKLY_SOURCE_PAYMENT_VIEW_V1' || permissions.view === 'PAY';
    const payChecklist = permissions.checklist?.available === true
      && permissions.checklist?.read_only === true && permissions.checklist?.pay_only === true
      && permissions.checklist?.action_links === false && asArray(permissions.allowed_tabs).includes('checklist');
    return {
      contract: asText(source.contract) || CONTRACT,
      permissions: payView ? {
        contract: 'WEEKLY_SOURCE_PAYMENT_VIEW_V1', view: 'PAY', allowed_tabs: payChecklist ? ['queries', 'checklist'] : ['queries'], landing_tab: 'queries',
        allowed_sections: asArray(permissions.allowed_sections).filter(section => ['questions', 'checks', 'protected'].includes(section)),
        can_upload: false, can_finalise: false, can_manage_settings: false,
        checklist: { available: payChecklist, read_only: true, pay_only: true, action_links: false }
      } : null,
      combined_source_workspace: source.combined_source_workspace === true,
      workspace_version: asText(source.workspace_version || source.record_version),
      recheck_payload: asObject(source.recheck_payload),
      profile: {
        id: profileId, label: asText(profile.label || source.source_label) || 'Weekly source',
        finalise_label: asText(profile.finalise_label) || FINALISE_LABELS[profileId] || 'Finalise week'
      },
      context: {
        subtitle: asText(context.subtitle || source.subtitle),
        cycle_state: CYCLE_STATES.has(cycleState) ? cycleState : (cycleState || 'To check'),
        cycle_tone: tone(context.cycle_tone, cycleState === 'Finalised' ? 'positive' : 'warning'),
        controls: normaliseControls(context.controls)
      },
      selected: {
        source_group_id: asText(source.selected?.source_group_id || context.source_group_id),
        source_cycle_id: asText(source.selected?.source_cycle_id || context.source_cycle_id),
        client_id: asText(source.selected?.client_id || context.client_id),
        report_scope_id: asText(source.selected?.report_scope_id || context.report_scope_id),
        projection_publication_id: asText(source.selected?.projection_publication_id || context.projection_publication_id)
      },
      counts: { queries: integer(counts.queries), blockers: integer(counts.blockers), paid_unresolved: integer(counts.paid_unresolved) },
      imports: {
        ...normalisePage(imports),
        journey: {
          authority_mode: asText(journey.authority_mode).toUpperCase(),
          title: asText(journey.title), body: asText(journey.body),
          waiting_count: integer(journey.waiting_count),
          waiting_rows: asArray(journey.waiting_rows),
          attention_count: integer(journey.attention_count),
          attention_rows: asArray(journey.attention_rows),
          ready_count: integer(journey.ready_count),
          ready_rows: asArray(journey.ready_rows)
        }
      },
      queries: { ...normalisePage(queries), protected_pay_enabled: queries.protected_pay_enabled === true, protected_shifts: normalisePage(queries.protected_shifts), bulk_actions: normaliseBulkActions(payView ? {} : queries.bulk_actions), office_checks: normalisePage(queries.office_checks) },
      finalise: {
        prepared: finalise.prepared !== false,
        prepare_action: asObject(finalise.prepare_action),
        exclusion_confirmation: asText(finalise.exclusion_confirmation),
        excluded_rows: asArray(finalise.excluded_rows),
        ...normalisePage(finalise), ready: normalisePage(finalise.ready), blocked: normalisePage(finalise.blocked), complete: normalisePage(finalise.complete),
        active_list: ((journey.authority_mode === 'TIMESHEET_AUTHORITY' ? ['ready', 'blocked', 'complete'] : ['ready', 'blocked'])
          .includes(asText(finalise.active_list).toLowerCase()) && finalise[asText(finalise.active_list).toLowerCase()]?.total_count)
          ? asText(finalise.active_list).toLowerCase() : finalise.ready?.total_count ? 'ready'
            : finalise.blocked?.total_count ? 'blocked'
              : journey.authority_mode === 'TIMESHEET_AUTHORITY' && finalise.complete?.total_count ? 'complete' : 'blocked',
        confirmation_text: asText(finalise.confirmation_text), confirmation_required: finalise.confirmation_required !== false,
        finalise_enabled: finalise.finalise_enabled === true, finalise_payload: asObject(finalise.finalise_payload),
        source_summary: asText(finalise.source_summary),
        rate_warnings: normaliseRateWarnings(finalise.rate_warnings, profileId),
        tracker: normaliseFinalisationTracker(finalise.tracker || source.finalisation_tracker),
        approved_hours_follow_up: normaliseApprovedHoursFollowUp(finalise.approved_hours_follow_up)
      },
      history: {
        ...normalisePage(source.history), cycle_filter: asText(source.history?.cycle_filter) || 'CURRENT_PAY_CYCLE',
        cycle_options: asArray(source.history?.cycle_options).map((entry) => ({ value: asText(entry?.value), label: asText(entry?.label) })).filter((entry) => entry.value && entry.label)
      },
      notices: asArray(source.notices).map((entry) => ({ tone: tone(entry?.tone, 'warning'), title: asText(entry?.title), body: asText(entry?.body) })).filter((entry) => entry.title || entry.body)
    };
  }

  function emptyWorkspace() {
    return normaliseWorkspace({ profile: {}, context: {}, imports: {}, queries: {}, finalise: { ready: {}, blocked: {} }, history: {} });
  }

  function tabDescriptors(workspace) {
    const ws = workspace || emptyWorkspace();
    return [
      { key: 'imports', label: 'Imports' },
      { key: 'queries', label: `Queries${ws.counts.queries ? ` (${ws.counts.queries})` : ''}` },
      { key: 'finalise', label: `${ws.profile.finalise_label}${ws.finalise.prepared && ws.counts.blockers ? ` (${ws.counts.blockers} blocker${ws.counts.blockers === 1 ? '' : 's'})` : ''}` },
      { key: 'history', label: 'History' },
      { key: 'checklist', label: 'Checklist' }
    ].filter(tab => !ws.permissions || ws.permissions.allowed_tabs.includes(tab.key));
  }

  function availableTab(workspace, tab) {
    const descriptors = tabDescriptors(workspace);
    return descriptors.some((descriptor) => descriptor.key === tab) ? tab : descriptors[0]?.key || 'imports';
  }

  function normaliseActions(value, allowlist) {
    return asArray(Array.isArray(value) ? value : value ? [value] : []).map((entry) => {
      const raw = typeof entry === 'string' ? { label: entry } : asObject(entry);
      const label = asText(raw.label);
      return allowlist.has(label) ? {
        label,
        enabled: !PENDING_ACTIONS.has(label) && raw.enabled !== false,
        payload: asObject(raw.payload),
        context: asObject(raw.context),
        reason: asText(raw.reason) || (PENDING_ACTIONS.has(label) ? 'This action is not available yet.' : ''),
        kind: asText(raw.kind).toUpperCase(),
        command: PENDING_ACTIONS.has(label) ? '' : asText(raw.command).toUpperCase()
      } : null;
    }).filter(Boolean);
  }

  function renderActions(actions, stale = false, scopeKey = '', protectedReviewReady = false) {
    return actions.map((action, index) => {
      const mutation = !!action.command || action.kind === 'MUTATION' || MUTATING_ACTIONS.has(action.label);
      const enabled = action.enabled && !(stale && mutation);
      const reason = stale && mutation ? 'This information has changed. Recheck before continuing.' : action.reason;
      const reasonId = reason ? safeKey(`${scopeKey || 'action'}-${action.label}-${index + 1}-reason`) : '';
      const ready = protectedReviewReady && !stale && enabled && ['Review protected pay','Review and reconcile'].includes(action.label);
      const button = `<button type="button" class="btn ${ready ? 'ws-protected-review-ready' : 'btn-outline'} ws-row-action" data-ws-action="${escapeHtml(action.label)}" data-ws-kind="${escapeHtml(action.kind)}" data-ws-command="${escapeHtml(action.command)}" data-ws-payload="${escapeHtml(JSON.stringify(action.payload))}" data-ws-context="${escapeHtml(JSON.stringify(action.context))}"${enabled ? '' : ' disabled'}${reason ? ` aria-describedby="${escapeHtml(reasonId)}"` : ''}>${escapeHtml(action.label)}</button>`;
      return reason ? `<span class="ws-action-hint" title="${escapeHtml(reason)}" tabindex="0">${button}<span id="${escapeHtml(reasonId)}" class="sr-only">${escapeHtml(reason)}</span></span>` : button;
    }).join('');
  }

  function renderStatus(value) {
    const status = normaliseStatus(value);
    return `<span class="ws-status ws-status--${escapeHtml(status.tone)}">${escapeHtml(status.text)}</span>${status.subtext ? `<span class="ws-status-sub ws-status-sub--${escapeHtml(status.subtone)}">${escapeHtml(status.subtext)}</span>` : ''}`;
  }

  function withFallbackDetail(actions, detail) {
    return actions.map((action) => {
      if (!['Open', 'View', 'Review', 'Review pricing', 'View details', 'Open charge details', 'Review source details', 'View query'].includes(action.label)) return action;
      if (Object.keys(asObject(action.payload?.detail || action.payload?.details)).length) return action;
      return { ...action, payload: { ...action.payload, detail } };
    });
  }

  function renderSortHeader(label, key, sortState = {}) {
    const active = asText(sortState.key) === key;
    const direction = active && asText(sortState.direction).toLowerCase() === 'desc' ? 'descending' : active ? 'ascending' : 'none';
    const arrow = active ? (direction === 'descending' ? ' ↓' : ' ↑') : '';
    return `<th aria-sort="${direction}"><button type="button" data-ws-sort="${escapeHtml(key)}">${escapeHtml(label)}<span aria-hidden="true">${arrow}</span></button></th>`;
  }

  function renderMobileSort(fields, sortState = {}) {
    const direction = asText(sortState.direction).toLowerCase() === 'desc' ? 'desc' : 'asc';
    return `<div class="ws-mobile-sort" aria-label="Sort results"><label>Sort by<select data-ws-mobile-sort>${fields.map(([label, key]) => `<option value="${escapeHtml(key)}"${sortState.key === key ? ' selected' : ''}>${escapeHtml(label)}</option>`).join('')}</select></label><label>Order<select data-ws-mobile-direction><option value="asc"${direction === 'asc' ? ' selected' : ''}>Ascending</option><option value="desc"${direction === 'desc' ? ' selected' : ''}>Descending</option></select></label></div>`;
  }

  function displayedCycleState(workspace) {
    const current = workspace.context.cycle_state;
    if (current !== 'Before cutoff') return current;
    const cutoff = asText(workspace.context.controls.find(control => control.key === 'cutoff')?.value);
    const match = /^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2})$/.exec(cutoff);
    if (!match) return current;
    const cutoffKey = `${match[3]}${match[2]}${match[1]}${match[4]}${match[5]}`;
    const now = Object.fromEntries(new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hourCycle: 'h23'
    }).formatToParts(new Date()).map(part => [part.type, part.value]));
    const nowKey = `${now.year}${now.month}${now.day}${now.hour}${now.minute}`;
    return nowKey >= cutoffKey ? 'Cutoff passed · not finalised' : current;
  }

  function renderContext(workspace, uploadOnly = false) {
    const fields = workspace.context.controls.filter(control=>!uploadOnly||['source_group','client'].includes(control.key)).map((control) => {
      const optionalClient = control.key === 'client';
      const placeholder = optionalClient
        ? `<option value=""${!control.value ? ' selected' : ''}>${workspace.profile.id.startsWith('NHSP_') ? 'All trusts' : 'All clients'}</option>`
        : !control.value && control.options.length
        ? `<option value="" selected disabled>${escapeHtml(control.key === 'client' ? `Choose a ${workspace.profile.id.startsWith('NHSP_') ? 'trust' : 'client'}` : `Choose ${control.label.toLowerCase()}`)}</option>`
        : '';
      const value = control.options.length
        ? `<select data-ws-context="${escapeHtml(control.key)}" aria-label="${escapeHtml(control.label)}" title="${escapeHtml(control.options.find((option) => option.value === control.value)?.label || control.label)}">${placeholder}${control.options.filter((option) => !optionalClient || option.value !== '').map((option) => `<option value="${escapeHtml(option.value)}"${option.value === control.value ? ' selected' : ''}>${escapeHtml(option.label)}</option>`).join('')}</select>`
        : `<span class="ws-context-value">${escapeHtml(control.value || '—')}</span>`;
      return `<div class="ws-context-item"><span class="ws-context-label">${escapeHtml(control.label)}</span>${value}</div>`;
    }).join('');
    return `<div class="ws-context-bar" aria-label="Weekly source context">${fields}${uploadOnly?'':`<span class="ws-status ws-status--${escapeHtml(workspace.context.cycle_tone)}">${escapeHtml(displayedCycleState(workspace))}</span>`}</div>`;
  }

  function renderImports(workspace, state) {
    const journey = workspace.imports.journey || {};
    const signedChecks = asArray(journey.attention_rows);
    const signedReady = asArray(journey.ready_rows);
    const signedWaiting = asArray(journey.waiting_rows);
    const signedGroups = { checks: signedChecks, ready: signedReady, waiting: signedWaiting };
    const signedTab = signedGroups[state.signedTab]?.length ? state.signedTab
      : signedChecks.length ? 'checks' : signedReady.length ? 'ready'
      : signedWaiting.length ? 'waiting' : 'checks';
    const signedRows = signedGroups[signedTab].map((rowValue) => {
      const row = asObject(rowValue);
      const actions = normaliseActions(row.actions, ACTIONS.imports);
      return `<tr><td data-label="Candidate">${escapeHtml(asText(row.candidate) || '—')}</td><td data-label="Day/date">${escapeHtml(asText(row.day_date) || '—')}</td><td data-label="Check">${escapeHtml(asText(row.attention) || '—')}</td><td data-label="Reference">${escapeHtml(asText(row.reference) || '—')}</td><td data-label="Status">${renderStatus(row.status)}</td><td data-label="Action" class="ws-actions">${renderActions(actions, false, asText(row.row_key))}</td></tr>`;
    }).join('');
    const signedJourneyPanel = journey.authority_mode === 'TIMESHEET_AUTHORITY'
      ? `<div class="ws-inner-tabs" role="tablist" aria-label="Signed Timesheet checks"><button type="button" role="tab" data-ws-signed-tab="checks" aria-selected="${signedTab === 'checks'}"${!signedChecks.length ? ' disabled' : ''}>Checks (${signedChecks.length})</button><button type="button" role="tab" data-ws-signed-tab="ready" aria-selected="${signedTab === 'ready'}"${!signedReady.length ? ' disabled' : ''}>Ready (${signedReady.length})</button><button type="button" role="tab" data-ws-signed-tab="waiting" aria-selected="${signedTab === 'waiting'}"${!signedWaiting.length ? ' disabled' : ''}>Waiting (${signedWaiting.length})</button></div><div class="ws-inner-scroll"><table class="grid mini ws-inner-grid"><thead><tr><th>Candidate</th><th>Day/date</th><th>Check</th><th>Reference</th><th>Status</th><th>Action</th></tr></thead><tbody>${signedRows || `<tr><td colspan="6" class="ws-empty">${signedTab === 'ready' ? 'No Timesheets are ready for Office authorisation.' : signedTab === 'waiting' ? 'No Timesheets are waiting for completion.' : 'No signed Timesheet checks need attention.'}</td></tr>`}</tbody></table></div>`
      : '';
    const journeyStatus = journey.attention_count
      ? `<span class="ws-status ws-status--warning">${journey.attention_count} need attention</span>`
      : journey.waiting_count ? `<span class="ws-status ws-status--neutral">${journey.waiting_count} waiting</span>`
      : '<span class="ws-status ws-status--positive">Up to date</span>';
    const journeyPanel = journey.title ? `<section class="ws-import-journey" data-ws-import-journey="${escapeHtml(journey.authority_mode)}"><div class="ws-import-journey__heading"><div><span>Journey</span><strong>${escapeHtml(journey.title)}</strong></div>${journeyStatus}</div>${journey.body ? `<p>${escapeHtml(journey.body)}</p>` : ''}${signedJourneyPanel}</section>` : '';
    const isNhsp = workspace.profile.id.startsWith('NHSP_');
    const uploadProfileId = isNhsp
      ? (NHSP_UPLOAD_PROFILES.some((profile) => profile.id === state.uploadProfileId)
        ? state.uploadProfileId
        : (workspace.context.cycle_state === 'Before cutoff'
          ? 'NHSP_PREFINAL_RELEASED_V1'
          : 'NHSP_FINAL_BACKING_V1'))
      : workspace.profile.id;
    const uploadProfileControl = isNhsp
      ? `<label>File type<select data-ws-upload-profile aria-label="File type">${NHSP_UPLOAD_PROFILES.map((profile) => `<option value="${profile.id}"${profile.id === uploadProfileId ? ' selected' : ''}>${profile.label}</option>`).join('')}</select></label>`
      : '';
    if(state.toolbarOnly) return `<details open><summary>Upload the next file</summary>${renderContext(workspace,true)}<div class="ws-toolbar">${uploadProfileControl}<button type="button" class="btn primary" data-ws-upload>Upload source file</button><input type="file" data-ws-upload-input hidden accept=".xlsx,.xls,.csv,.htm,.html"${uploadProfileId==='NHSP_FINAL_BACKING_V1'?' multiple':''}></div></details>`;
    const rows = workspace.imports.rows.map((rowValue) => {
      const row = asObject(rowValue);
      const detail = {
        heading: asText(row.file) || 'Source file',
        file: asText(row.file), uploaded: asText(row.uploaded), rows: asText(row.rows),
        coverage: asText(row.coverage), report_number: asText(row.report), cutoff: asText(row.cutoff),
        status: normaliseStatus(row.status).text, final_source: asText(row.final_source)
      };
      const actions = withFallbackDetail(
        normaliseActions(row.actions, ACTIONS.imports), detail
      );
      const scope = isNhsp
        ? `<td data-label="Report">${escapeHtml(asText(row.report) || 'Not confirmed')}</td><td data-label="Cutoff">${escapeHtml(asText(row.cutoff) || 'Not confirmed')}</td>`
        : `<td data-label="Coverage">${escapeHtml(asText(row.coverage) || '—')}</td>`;
      const filename = escapeHtml(asText(row.file) || '—');
      return `<tr><td data-label="File"><span class="ws-import-filename" title="${filename}">${filename}</span></td><td data-label="Uploaded">${escapeHtml(asText(row.uploaded) || '—')}</td><td data-label="Rows">${escapeHtml(asText(row.rows) || '0')}</td>${scope}<td data-label="Status">${renderStatus(row.status)}</td><td data-label="Final source">${escapeHtml(asText(row.final_source) || '—')}</td><td data-label="Actions" class="ws-actions">${renderActions(actions, workspace.imports.stale, asText(row.row_key || row.file))}</td></tr>`;
    }).join('');
    const sort = state.sort?.imports || {};
    const sortable = isNhsp
      ? [['File','file'],['Uploaded','uploaded'],['Rows','rows'],['Report','report'],['Cutoff','cutoff'],['Status','status'],['Final source','final_source']]
      : [['File','file'],['Uploaded','uploaded'],['Rows','rows'],['Coverage','coverage'],['Status','status'],['Final source','final_source']];
    const stale = workspace.imports.stale ? '<div class="ws-notice ws-notice--warning" role="status"><span>This information has changed. Recheck before continuing.</span></div>' : '';
    return `${stale}${journeyPanel}<div class="ws-toolbar">${uploadProfileControl}<button type="button" class="btn primary" data-ws-upload${workspace.imports.stale ? ' disabled title="Recheck before uploading another source file."' : ''}>Upload source file</button><button type="button" class="btn btn-outline" data-ws-recheck>Recheck</button><button type="button" class="btn btn-outline" data-ws-daily>Daily rota check</button><input type="file" data-ws-upload-input hidden accept=".xlsx,.xls,.csv,.htm,.html"${uploadProfileId === 'NHSP_FINAL_BACKING_V1' ? ' multiple' : ''}><input type="file" data-ws-daily-input hidden accept=".xlsx,.xls,.csv"></div>${renderMobileSort(sortable, sort)}<div class="ws-scroll" data-ws-scroll><table class="grid mini ws-grid ws-import-grid"><thead><tr>${sortable.map(([label,key]) => renderSortHeader(label,key,sort)).join('')}<th>Actions</th></tr></thead><tbody>${rows || `<tr><td colspan="${sortable.length + 1}" class="ws-empty">Nothing matches the current filters.</td></tr>`}</tbody></table><div data-ws-sentinel></div></div><div class="ws-sticky-footer"><span>${workspace.imports.total_count} source file${workspace.imports.total_count === 1 ? '' : 's'}</span></div>`;
  }

  function renderTick(value, yesLabel, noLabel) {
    return value === true ? `<span class="ws-tick" aria-label="${escapeHtml(yesLabel)}">✓</span>` : `<span class="ws-blank" aria-label="${escapeHtml(noLabel)}"></span>`;
  }

  function exactAcceptSystemHoursPayload(payloadValue) {
    const envelope = asObject(payloadValue);
    const nested = asObject(envelope.payload);
    const payload = Object.keys(nested).length ? nested : envelope;
    const selection = asObject(payload.selection);
    const list = (value) => asArray(value).map(asText).filter(Boolean);
    const groupKeys = list(selection.group_keys);
    const excludedGroupKeys = list(selection.excluded_group_keys);
    const incidentIds = list(selection.incident_ids);
    const filters = asObject(selection.filters);
    const filterKeys = Object.keys(filters);
    const proofs = asArray(selection.group_selection_proofs).map((value) => {
      const proof = asObject(value);
      return { group_key: asText(proof.group_key), selection_proof: asText(proof.selection_proof).toLowerCase() };
    });
    const unique = (values) => new Set(values).size === values.length;
    const groupKeyValid = (value) => /^qg_[0-9a-f]{64}$/.test(value);
    const hashValid = (value) => /^[0-9a-f]{64}$/.test(value);
    if (asText(payload.action).toUpperCase() !== 'ACCEPT_SYSTEM_HOURS'
      || asText(selection.mode).toUpperCase() !== 'EXPLICIT'
      || ![payload.actor_user_id, payload.source_cycle_id, payload.projection_publication_id].every((value) => asText(value))
      || !hashValid(asText(payload.expected_workspace_version).toLowerCase())
      || !groupKeys.length || !incidentIds.length || excludedGroupKeys.length
      || !unique(groupKeys) || !unique(incidentIds)
      || groupKeys.some((value) => !groupKeyValid(value))
      || filterKeys.some((key) => !['status', 'candidate', 'issue'].includes(key) || typeof filters[key] !== 'string')
      || !['asc', 'desc'].includes(asText(selection.sort_direction).toLowerCase())
      || !asText(selection.sort_key)
      || selection.selection_proof != null
      || proofs.length !== groupKeys.length || !unique(proofs.map((proof) => proof.group_key))
      || proofs.some((proof) => !groupKeys.includes(proof.group_key) || !hashValid(proof.selection_proof))) return null;
    return {
      actor_user_id: asText(payload.actor_user_id),
      source_cycle_id: asText(payload.source_cycle_id),
      projection_publication_id: asText(payload.projection_publication_id),
      expected_workspace_version: asText(payload.expected_workspace_version).toLowerCase(),
      action: 'ACCEPT_SYSTEM_HOURS',
      selection: {
        mode: 'EXPLICIT', group_keys: [...groupKeys].sort(), excluded_group_keys: [],
        incident_ids: [...incidentIds].sort(), filters: { ...filters },
        sort_key: asText(selection.sort_key), sort_direction: asText(selection.sort_direction).toLowerCase(),
        selection_proof: null,
        group_selection_proofs: proofs.sort((left, right) => left.group_key.localeCompare(right.group_key))
      }
    };
  }

  function exactAcceptSystemHoursAction(group) {
    return exactAcceptSystemHoursPayload(group?.accept_system_hours_action);
  }

  function acceptSingleSystemShiftPayload(group, incidentId) {
    return acceptSystemShiftSelectionPayload(group, [incidentId]);
  }

  function acceptSystemShiftSelectionPayload(group, incidentIds) {
    if (group?.accept_system_hours_action?.enabled !== true) return null;
    const payload = exactAcceptSystemHoursAction(group);
    if (!payload || payload.selection.group_keys.length !== 1
      || payload.selection.group_keys[0] !== group.group_key
      || !Array.isArray(incidentIds) || !incidentIds.length
      || new Set(incidentIds).size !== incidentIds.length
      || incidentIds.some(id => !payload.selection.incident_ids.includes(id))) return null;
    return { ...payload, selection: { ...payload.selection, incident_ids: [...incidentIds].sort() } };
  }

  function combinedAcceptSystemHoursPayload() {
    const selectedGroups = [];
    for (const [groupKey, selected] of session.shiftSelections.entries()) {
      if (!selected.size) continue;
      const group = session.workspace?.queries?.rows?.find((entry) => asText(entry.group_key) === groupKey);
      const request = exactAcceptSystemHoursAction(group);
      if (!request || request.selection.group_keys.length !== 1 || request.selection.group_keys[0] !== groupKey) return null;
      const eligible = new Set(request.selection.incident_ids);
      const incidentIds = [...selected].map(asText).filter(Boolean).sort();
      if (!incidentIds.length || new Set(incidentIds).size !== incidentIds.length
        || incidentIds.some((incidentId) => !eligible.has(incidentId))) return null;
      selectedGroups.push({ request, groupKey, incidentIds });
    }
    if (!selectedGroups.length) return null;
    const first = selectedGroups[0].request;
    const filters = JSON.stringify(first.selection.filters);
    if (selectedGroups.some(({ request }) => request.actor_user_id !== first.actor_user_id
      || request.source_cycle_id !== first.source_cycle_id
      || request.projection_publication_id !== first.projection_publication_id
      || request.expected_workspace_version !== first.expected_workspace_version
      || request.selection.sort_key !== first.selection.sort_key
      || request.selection.sort_direction !== first.selection.sort_direction
      || JSON.stringify(request.selection.filters) !== filters)) return null;
    const incidentIds = selectedGroups.flatMap((entry) => entry.incidentIds);
    if (new Set(incidentIds).size !== incidentIds.length) return null;
    return {
      actor_user_id: first.actor_user_id,
      source_cycle_id: first.source_cycle_id,
      projection_publication_id: first.projection_publication_id,
      expected_workspace_version: first.expected_workspace_version,
      action: 'ACCEPT_SYSTEM_HOURS',
      selection: {
        mode: 'EXPLICIT',
        group_keys: selectedGroups.map((entry) => entry.groupKey).sort(),
        excluded_group_keys: [], incident_ids: [...incidentIds].sort(),
        filters: { ...first.selection.filters }, sort_key: first.selection.sort_key,
        sort_direction: first.selection.sort_direction, selection_proof: null,
        group_selection_proofs: selectedGroups.flatMap((entry) => entry.request.selection.group_selection_proofs)
          .sort((left, right) => left.group_key.localeCompare(right.group_key))
      }
    };
  }

  function bulkOutreachAction(queries, command) {
    if (queries?.bulk_actions?.selection_complete !== true) return null;
    if (command === 'ASK_CANDIDATES') return queries.bulk_actions.ask_candidates;
    if (command === 'SEND_MANAGER_NOW') return queries.bulk_actions.send_manager_now;
    return null;
  }

  function buildOutreachRequest(queries, selectionState, command) {
    const action = bulkOutreachAction(queries, command);
    const selection = selectionState || {};
    if (!action?.enabled || !action.request || !['ALL_FILTERED', 'EXPLICIT'].includes(selection.mode)) return null;
    const request = { ...action.request, selection: { ...action.request.selection } };
    if (selection.mode === 'ALL_FILTERED') {
      request.selection.mode = 'ALL_FILTERED';
      request.selection.group_keys = [];
      request.selection.excluded_group_keys = [...new Set(selection.exclusions || [])].map(asText).filter(Boolean).sort();
      request.selection.incident_ids = [];
    } else {
      const groupKeys = [...new Set(selection.ids || [])].map(asText).filter((key) => /^qg_[0-9a-f]{64}$/.test(key)).sort();
      if (!groupKeys.length || groupKeys.length !== new Set(selection.ids || []).size) return null;
      request.selection.mode = 'EXPLICIT';
      request.selection.group_keys = groupKeys;
      request.selection.excluded_group_keys = [];
      request.selection.incident_ids = [];
    }
    return request;
  }

  function renderShiftRows(group, stale = false) {
    const groupKey = asText(group.group_key);
    const rows = asArray(group.children).map((rowValue) => {
      const row = asObject(rowValue);
      const detail = {
        day_date: asText(row.day_date), candidate_hours: asText(row.candidate_hours),
        system_hours: asText(row.system_hours), issue: asText(row.issue), status: normaliseStatus(row.status).text
      };
      const actions = withFallbackDetail(normaliseActions(row.actions, ACTIONS.shifts), detail);
      return `<tr class="ws-shift-row"><td data-label="Select"><input type="checkbox" data-ws-shift-select="${escapeHtml(groupKey)}" value="${escapeHtml(asText(row.incident_id))}"${row.accept_eligible === true && !stale ? '' : ' disabled'} aria-label="Select ${escapeHtml(asText(row.day_date) || 'shift')}"></td><td data-label="Day/date"><strong>${escapeHtml(asText(row.day_date) || '—')}</strong></td><td data-label="Job role">${escapeHtml(asText(row.job_role) || '—')}</td><td data-label="Candidate says they worked">${escapeHtml(asText(row.candidate_hours) || 'Timesheet not submitted')}</td><td data-label="System hours">${escapeHtml(asText(row.system_hours) || '—')}</td><td data-label="Issue">${escapeHtml(asText(row.issue) || '—')}</td><td data-label="Status">${renderStatus(row.status)}</td><td data-label="Actions" class="ws-actions">${renderActions(actions, stale, `${groupKey}-${asText(row.incident_id)}`)}</td></tr>`;
    }).join('');
    const exactGroupPayload = exactAcceptSystemHoursAction(group);
    const eligible = !!exactGroupPayload;
    return `<tr class="ws-expanded"><td colspan="10"><div class="ws-inner-scroll"><table class="grid mini ws-inner-grid ws-query-shift-grid"><thead><tr><th><input type="checkbox" data-ws-shift-header="${escapeHtml(groupKey)}" aria-label="Select all shifts in this group"${eligible && !stale ? '' : ' disabled'}></th>${['Day/date','Job role','Candidate says they worked','System hours','Issue','Status','Actions'].map((label) => `<th>${label}</th>`).join('')}</tr></thead><tbody>${rows}</tbody></table></div></td></tr>`;
  }

  function renderQueries(workspace, state) {
    const payView = workspace.permissions?.view === 'PAY';
    const stale = workspace.queries.stale;
    const hasOutreachAuthority = bulkOutreachAction(workspace.queries, 'ASK_CANDIDATES')?.enabled === true
      || bulkOutreachAction(workspace.queries, 'SEND_MANAGER_NOW')?.enabled === true;
    const rows = workspace.queries.rows.map((rowValue) => {
      const row = asObject(rowValue);
      const key = asText(row.group_key);
      const selectable = hasOutreachAuthority && !stale;
      const detail = {
        candidate: asText(row.candidate), client: asText(row.client), status: normaliseStatus(row.status).text,
        age: asText(row.age), shifts: asArray(row.children).map((shift) => ({
          day_date: asText(shift?.day_date), candidate_hours: asText(shift?.candidate_hours),
          system_hours: asText(shift?.system_hours), issue: asText(shift?.issue), status: normaliseStatus(shift?.status).text
        }))
      };
      const actions = withFallbackDetail(normaliseActions(row.actions, ACTIONS.queries), detail);
      return `<tr data-ws-group-key="${escapeHtml(key)}"><td data-label="Select"><input type="checkbox" data-ws-group-select value="${escapeHtml(key)}"${selectable ? '' : ' disabled'} aria-label="Select ${escapeHtml(asText(row.candidate) || 'query group')}"></td><td data-label="Shifts"><button type="button" class="ws-expand" data-ws-expand="${escapeHtml(key)}" aria-expanded="${row.expanded === true}" aria-label="${row.expanded === true ? 'Collapse' : 'Expand'} ${escapeHtml(asText(row.candidate) || 'query group')}">⌄</button></td><td data-label="Candidate">${escapeHtml(asText(row.candidate) || '—')}</td><td data-label="Client">${escapeHtml(asText(row.client) || '—')}</td><td data-label="Issues">${escapeHtml(asText(row.issues) || '0')}</td><td data-label="Candidate asked">${renderTick(row.candidate_asked, 'Candidate asked', 'Candidate not yet asked')}</td><td data-label="Manager informed">${renderTick(row.manager_informed, 'Manager informed', 'Manager not yet informed')}</td><td data-label="Status">${renderStatus(row.status)}</td><td data-label="Age">${escapeHtml(asText(row.age) || '—')}</td><td data-label="Actions" class="ws-actions">${renderActions(actions, stale, key)}</td></tr>${row.expanded === true ? renderShiftRows(row, stale) : ''}`;
    }).join('');
    const paid = workspace.counts.paid_unresolved ? '<div class="ws-notice ws-notice--danger"><strong>Paid shifts still need attention</strong><span>Paid unresolved shifts remain at the top until resolved.</span></div>' : '';
    const sort = state.sort?.queries || {};
    const sortable = [['Candidate','candidate'],['Client','client'],['Issues','issues'],['Candidate asked','candidate_asked'],['Manager informed','manager_informed'],['Status','status'],['Age','age']];
    const filters = state.query?.filters || {};
    const staleNotice = stale ? '<div class="ws-notice ws-notice--warning" role="status"><span>This information has changed. Recheck before continuing.</span></div>' : '';
    const filterControls = `
      <label>Status<select data-ws-filter="status"><option value="UNRESOLVED"${filters.status !== 'ALL' ? ' selected' : ''}>All unresolved</option><option value="ALL"${filters.status === 'ALL' ? ' selected' : ''}>All</option></select></label>
      <label>Candidate<input type="search" data-ws-filter="candidate" placeholder="All" value="${escapeHtml(filters.candidate || '')}"></label>
      <label>Issue<select data-ws-filter="issue"><option value="ALL"${!filters.issue || filters.issue === 'ALL' ? ' selected' : ''}>All</option><option value="HOURS_DIFFER"${filters.issue === 'HOURS_DIFFER' ? ' selected' : ''}>Hours differ</option><option value="MISSING_SHIFT"${filters.issue === 'MISSING_SHIFT' ? ' selected' : ''}>Missing shift</option><option value="TIMESHEET_MISSING"${filters.issue === 'TIMESHEET_MISSING' ? ' selected' : ''}>Timesheet missing</option></select></label>
      ${renderMobileSort(sortable, sort)}
      ${payView ? '<button type="button" class="btn btn-outline" data-ws-refresh>Refresh list</button>' : '<button type="button" class="btn btn-outline" data-ws-recheck>Recheck</button>'}`;
    const actionControls = `
      <button type="button" class="btn primary" data-ws-bulk-action="ASK_CANDIDATES" disabled>Ask selected candidates</button>
      <button type="button" class="btn btn-outline" data-ws-bulk-action="SEND_MANAGER_NOW" disabled>Send selected to manager now</button>
      <button type="button" class="btn btn-outline" data-ws-accept-selected hidden${stale ? ' disabled' : ''}>Accept system hours for selected shifts</button>`;
    const checks = workspace.queries.office_checks;
    const officeChecks = checks.rows.length ? `<section class="ws-office-checks"><h3>Office checks (${checks.total_count})</h3><div class="ws-inner-scroll"><table class="grid mini ws-inner-grid"><thead><tr><th>Candidate in source</th><th>Source reference</th><th>Client / Trust</th><th>Booking reference</th><th>Day/date</th><th>System hours</th><th>Problem</th><th>Action</th></tr></thead><tbody>${checks.rows.map((row) => {
      const detail = { candidate: row.candidate, client: row.client, day_date: row.day_date, system_hours: row.system_hours,
        source_reference: row.source_reference, booking_reference: row.booking_reference, problem: row.problem };
      const actions = withFallbackDetail(normaliseActions(row.actions, ACTIONS.finalise), detail);
      return `<tr class="${row.pay_blocking===true?'ws-query-hold':'ws-query-nonblocking'}">${['candidate','source_reference','client','booking_reference','day_date','system_hours','problem'].map(key => `<td>${escapeHtml(row[key] || '—')}</td>`).join('')}<td class="ws-actions">${renderActions(actions, checks.stale, row.row_key)}</td></tr>`;
    }).join('')}</tbody></table></div></section>` : '';
    const addProtected = workspace.queries.protected_pay_enabled
      ? `<div class="ws-child-actions"><button type="button" class="btn btn-outline ws-row-action" data-ws-action="Add protected shift" data-ws-payload="${escapeHtml(JSON.stringify({ source_group_id: workspace.selected.source_group_id, client_id: workspace.selected.client_id }))}">Add protected shift</button></div>` : '';
    const protectedRows = workspace.queries.protected_shifts;
    const protectedSection = protectedRows.rows.length ? `<section><h3>Protected shifts (${protectedRows.total_count})</h3><div class="ws-inner-scroll"><table class="grid mini ws-inner-grid ws-protected-grid"><thead><tr><th>Candidate</th><th>Client</th><th>Day/date</th><th>Protected hours</th><th>Status</th><th>Actions</th></tr></thead><tbody>${protectedRows.rows.map(row => `<tr>${['candidate','client','day_date','protected_hours'].map(key => `<td>${escapeHtml(row[key] || '—')}</td>`).join('')}<td>${renderStatus(row.status)}</td><td class="ws-actions">${renderActions(normaliseActions(row.actions, ACTIONS.shifts).filter(action => !['Review protected pay','Review and reconcile'].includes(action.label) || row.requires_attention === true), protectedRows.stale, row.row_key, row.requires_attention === true)}</td></tr>`).join('')}</tbody></table></div></section>` : '';
    return `${staleNotice}${paid}${addProtected}${protectedSection}${officeChecks}<h3>Hours questions</h3>
      <div class="ws-query-controls">
        <details class="ws-query-filter-menu" open>
          <summary class="btn btn-outline">Filters and sorting</summary>
          <div class="ws-query-filter-fields">${filterControls}</div>
        </details>
        ${payView ? '' : `<details class="ws-query-actions-menu" open>
          <summary class="btn primary" data-ws-actions-summary aria-disabled="true">Actions for selected</summary>
          <div class="ws-query-action-choices">${actionControls}</div>
        </details>`}
      </div>
      <div class="ws-selection-summary" data-ws-selection-summary>0 query groups selected</div>
      <div class="ws-scroll ws-query-scroll" data-ws-scroll><table class="grid mini ws-grid ws-query-grid"><thead><tr><th><input type="checkbox" data-ws-group-header aria-label="Select all query groups"${stale || !hasOutreachAuthority ? ' disabled' : ''}></th><th></th>${sortable.map(([label,key]) => renderSortHeader(label,key,sort)).join('')}<th>Actions</th></tr></thead><tbody>${rows || '<tr><td colspan="10" class="ws-empty">Nothing matches the current filters.</td></tr>'}</tbody></table><div data-ws-sentinel></div></div>
      <div class="ws-sticky-footer"><span>${workspace.counts.queries} unresolved</span><span data-ws-shift-summary>0 shifts selected</span></div>`;
  }

  function finaliseColumns(workspace, blocked) {
    if (/^HEALTHROSTER_WEEKLY_/.test(workspace.profile.id)) return [
      ['Candidate','candidate'], ['Day/date','day_date'], ['System hours','system_hours'], ['Status','status'], ['Action','actions']
    ];
    if (blocked) return [
      ['Candidate', 'candidate'], ['Day/date', 'day_date'], ['Problem', 'problem'], ['Action', 'actions']
    ];
    if (workspace.profile.id === 'NHSP_FINAL_BACKING_V1' || workspace.profile.id === 'NHSP_BACKING_REPORT_ACTUAL_V1') return [
      ['Candidate','candidate'], ['Day/date','day_date'], ['Actual hours','actual_hours'], ['Movement','movement'],
      ['Commission','commission'], ['Total cost','total_cost'], ['Invoice charge','invoice_charge'], ['Status','status'], ['Action','actions']
    ];
    return [
      ['Candidate','candidate'], ['Day/date','day_date'], ['Job role','job_role'], ['Client','client'],
      ['System hours','system_hours'], ['Contract','contract'], ['Outcome','outcome'], ['Action','actions']
    ];
  }

  function renderFinaliseTable(workspace, page, blocked, state) {
    const rateWarnings = workspace.finalise.rate_warnings;
    const chargeDecisionRows = blocked && page === workspace.finalise.blocked
      && workspace.profile.id === 'NHSP_FINAL_BACKING_V1'
      && rateWarnings.phase === 'FINAL_AWAITING_ACCEPTANCE';
    const warningsBySourceRow = new Map(rateWarnings.rows.filter((warning) => warning.source_row_id)
      .map((warning) => [warning.source_row_id, warning]));
    const selectable = chargeDecisionRows ? page.rows.filter((row) => {
      const warning = warningsBySourceRow.get(asText(row.row_key));
      return warning?.accept_eligible && row.problem === 'Accept the final NHSP source charge';
    }).map((row) => warningsBySourceRow.get(asText(row.row_key)).warning_key) : [];
    const selected = state.rateWarningSelection || new Set();
    const columns = page === workspace.finalise.complete
      ? [['Candidate','candidate'], ...(!workspace.selected.client_id ? [['Client','client']] : []), ['Day/date','day_date'], ['System hours','system_hours'], ['Finalised','finalised_at'], ['Status','status']]
      : [...(selectable.length ? [['Select','charge_select']] : []), ...finaliseColumns(workspace, blocked)];
    const sort = state.sort?.finalise || {};
    const rows = page.rows.map((rowValue) => {
      const row = asObject(rowValue);
      const warning = warningsBySourceRow.get(asText(row.row_key));
      const canReviewCharge = !!warning && chargeDecisionRows && row.problem === 'Accept the final NHSP source charge';
      const cells = columns.map(([label, key]) => {
        if (key === 'charge_select') return `<td data-label="Select"><input type="checkbox" data-ws-rate-warning-select value="${escapeHtml(warning?.warning_key || '')}" aria-label="Select charge for ${escapeHtml(row.candidate || 'shift')} on ${escapeHtml(row.day_date || '')}"${canReviewCharge && warning.accept_eligible ? '' : ' disabled'}${selected.has(warning?.warning_key) ? ' checked' : ''}></td>`;
        if (key === 'actions') {
          if (canReviewCharge) return `<td data-label="${escapeHtml(label)}" class="ws-actions"><button type="button" class="btn btn-outline ws-charge-review" data-ws-rate-expand="${escapeHtml(warning.warning_key)}" aria-expanded="${state.expandedRateWarning === warning.warning_key}">Review charge</button></td>`;
          const detail = { candidate: asText(row.candidate), day_date: asText(row.day_date), problem: asText(row.problem), guidance: asText(row.guidance || row.next_step) };
          const actions = withFallbackDetail(normaliseActions(row.actions, ACTIONS.finalise), detail);
          return `<td data-label="${escapeHtml(label)}" class="ws-actions">${renderActions(actions, page.stale, asText(row.row_key || `${row.candidate}-${row.day_date}`))}</td>`;
        }
        if (key === 'status') return `<td data-label="${escapeHtml(label)}">${renderStatus(row.status)}</td>`;
        if (key === 'problem') return `<td data-label="${escapeHtml(label)}">${integer(row.issue_count) > 1 ? `<span class="ws-issue-count">${integer(row.issue_count)} issues</span>` : ''}${escapeHtml(asText(row.problem) || '—')}</td>`;
        return `<td data-label="${escapeHtml(label)}">${escapeHtml(asText(row[key]) || '—')}</td>`;
      }).join('');
      const expanded = canReviewCharge && state.expandedRateWarning === warning.warning_key;
      const detail = expanded ? `<tr class="ws-charge-detail-row"><td colspan="${columns.length}"><div class="ws-charge-detail"><div><strong>${escapeHtml(warning.warning)}</strong><span>${escapeHtml(warning.contract || 'Selected contract')}</span></div><dl><div><dt>NHSP Commission</dt><dd>${escapeHtml(warning.commission || '—')}</dd></div><div><dt>NHSP Total Cost</dt><dd>${escapeHtml(warning.total_cost || '—')}</dd></div><div><dt>NHSP charge</dt><dd>${escapeHtml(warning.source_charge || '—')}</dd></div><div><dt>Calculated charge</dt><dd>${escapeHtml(warning.calculated_charge || '—')}</dd></div><div><dt>Difference</dt><dd>${escapeHtml(warning.difference || '—')}</dd></div></dl><p>Accepting keeps the NHSP invoice charge. It does not change candidate pay.</p></div></td></tr>` : '';
      return `<tr data-ws-finalise-row="${escapeHtml(asText(row.row_key))}">${cells}</tr>${detail}`;
    }).join('');
    const sourceAuthorityClass = workspace.imports.journey.authority_mode === 'SOURCE_AUTHORITY' ? ' ws-source-finalise-grid' : '';
    return `<table class="grid mini ws-grid${sourceAuthorityClass}"><thead><tr>${columns.map(([label, key]) => key === 'charge_select' ? `<th><input type="checkbox" data-ws-rate-warning-header aria-label="Select all eligible charges"${selectable.every((key) => selected.has(key)) ? ' checked' : ''}></th>` : key === 'actions' ? `<th>${label}</th>` : renderSortHeader(label, key, sort)).join('')}</tr></thead><tbody>${rows || `<tr><td colspan="${columns.length}" class="ws-empty">Nothing matches the current filters.</td></tr>`}</tbody></table>`;
  }

  function renderFinalisationTracker(workspace) {
    const tracker = workspace.finalise.tracker;
    const isNhsp = workspace.profile.id.startsWith('NHSP_');
    if (!tracker.rows.length && !(isNhsp && tracker.cycle_options.length)) return '';
    const rows = tracker.rows.map((row) => `<tr><td data-label="Source">${escapeHtml(row.source || '—')}</td><td data-label="Trust or client">${escapeHtml(row.client || '—')}</td><td data-label="Status">${renderStatus(row.status)}${row.detail ? `<span class="ws-status-sub">${escapeHtml(row.detail)}</span>` : ''}</td><td data-label="Action" class="ws-actions">${renderActions(row.actions, false, row.row_key)}</td></tr>`).join('');
    const complete = tracker.complete
      ? '<span class="ws-status ws-status--positive">Complete</span>'
      : '<span class="ws-status ws-status--warning">Still to complete</span>';
    const week = isNhsp && tracker.cycle_options.length
      ? `<label class="ws-tracker-week">Week<select data-ws-tracker-cycle aria-label="Finalisation week">${tracker.cycle_options.map((option) => `<option value="${escapeHtml(option.value)}"${option.value === tracker.cycle_id ? ' selected' : ''}>${escapeHtml(option.label)}</option>`).join('')}</select></label>`
      : '';
    return `<section class="ws-tracker" aria-labelledby="ws-tracker-title"><div class="ws-tracker-heading"><div><h3 id="ws-tracker-title">${escapeHtml(tracker.title)}</h3>${!week && tracker.cycle_label ? `<span>${escapeHtml(tracker.cycle_label)}</span>` : ''}</div><div class="ws-tracker-controls">${week}${complete}</div></div><div class="ws-inner-scroll"><table class="grid mini ws-inner-grid"><thead><tr><th>Source</th><th>Trust or client</th><th>Status</th><th>Action</th></tr></thead><tbody>${rows || '<tr><td colspan="4" class="ws-empty">No trusts for this week.</td></tr>'}</tbody></table></div></section>`;
  }

  function renderApprovedHoursFollowUp(finalise, viewState) {
    const followUp = finalise.approved_hours_follow_up || {};
    const retry = asObject(viewState.finaliseRetry);
    if (retry.task_id) {
      return '<div class="ws-notice ws-notice--warning" role="status"><strong>Approved hours update was not confirmed</strong><span>The source is already finalised. Retry only the approved-hours update.</span><button type="button" class="btn btn-outline" data-ws-finalise-retry>Retry approved hours update</button></div>';
    }
    if (!followUp.title && !followUp.body) return '';
    const noticeTone = followUp.state === 'ACTION_REQUIRED' ? 'danger' : 'warning';
    const action = followUp.action
      ? `<button type="button" class="btn btn-outline" data-ws-approved-follow-up data-ws-command="${escapeHtml(followUp.action.command)}" data-ws-payload="${escapeHtml(JSON.stringify(followUp.action.payload))}">${escapeHtml(followUp.action.label)}</button>`
      : '';
    return `<div class="ws-notice ws-notice--${noticeTone}" role="status"><strong>${escapeHtml(followUp.title)}</strong>${followUp.body ? `<span>${escapeHtml(followUp.body)}</span>` : ''}${action}</div>`;
  }

  function buildRateWarningAcceptancePayload(rateWarnings, selectedKeys) {
    const warnings = rateWarnings || {};
    const acceptance = warnings.acceptance;
    if (warnings.phase !== 'FINAL_AWAITING_ACCEPTANCE' || !acceptance) return null;
    const eligible = new Set(warnings.rows.filter((row) => row.accept_eligible).map((row) => row.warning_key));
    const selected = [...new Set(asArray(selectedKeys).map(asText).filter((key) => eligible.has(key)))];
    if (!selected.length) return null;
    return {
      action: acceptance.action,
      payload: {
        ...acceptance.payload,
        [acceptance.selection.key]: selected,
        [acceptance.selection.proof_key]: acceptance.selection.proof
      }
    };
  }

  function renderRateWarningRows(rateWarnings, viewState) {
    const selected = viewState.rateWarningSelection || new Set();
    const accepting = rateWarnings.phase === 'FINAL_AWAITING_ACCEPTANCE';
    return rateWarnings.rows.map((row) => {
      const rowSelected = selected.has(row.warning_key);
      const selectCell = accepting
        ? `<td data-label="Select"><input type="checkbox" data-ws-rate-warning-select value="${escapeHtml(row.warning_key)}"${row.accept_eligible ? '' : ' disabled'} aria-label="Select ${escapeHtml(row.candidate || 'rate warning')}"${rowSelected ? ' checked' : ''}></td>`
        : '';
      const action = row.detail_rows.length
        ? `<button type="button" class="btn btn-outline" data-ws-rate-expand="${escapeHtml(row.warning_key)}" aria-expanded="${viewState.expandedRateWarning === row.warning_key}">${escapeHtml(row.action_label)}</button>`
        : `<span class="ws-muted-action">${escapeHtml(row.action_label)}</span>`;
      const base = `<tr data-ws-rate-warning-row="${escapeHtml(row.warning_key)}">${selectCell}<td data-label="Candidate">${escapeHtml(row.candidate || '—')}</td><td data-label="Day/date">${escapeHtml(row.day_date || '—')}</td><td data-label="Source charge">${escapeHtml(row.source_charge || '—')}</td><td data-label="Warning">${renderStatus({ text: row.warning || 'Rate warning', tone: row.warning_tone })}</td><td data-label="Action" class="ws-actions">${action}</td></tr>`;
      if (viewState.expandedRateWarning !== row.warning_key || !row.detail_rows.length) return base;
      const detailRows = row.detail_rows.map((detail) => `<tr><td>${escapeHtml(detail.candidate || '—')}</td><td>${escapeHtml(detail.day_date || '—')}</td><td>${escapeHtml(detail.source_charge || '—')}</td><td>${escapeHtml(detail.warning || '—')}</td></tr>`).join('');
      return `${base}<tr class="ws-rate-warning-drilldown"><td colspan="${accepting ? 6 : 5}"><div class="ws-inner-scroll"><table class="grid mini ws-inner-grid"><thead><tr><th>Candidate</th><th>Day/date</th><th>Source charge</th><th>Warning</th></tr></thead><tbody>${detailRows}</tbody></table></div></td></tr>`;
    }).join('');
  }

  function renderRateWarnings(rateWarnings, viewState) {
    if (!rateWarnings.available) return '';
    const accepting = rateWarnings.phase === 'FINAL_AWAITING_ACCEPTANCE';
    const selected = viewState.rateWarningSelection || new Set();
    const eligible = rateWarnings.rows.filter((row) => row.accept_eligible);
    const allSelected = eligible.length > 0 && eligible.every((row) => selected.has(row.warning_key));
    const someSelected = eligible.some((row) => selected.has(row.warning_key));
    const notice = rateWarnings.notice
      ? `<div class="ws-notice ws-notice--${escapeHtml(rateWarnings.notice.tone)}" role="status"><strong>${escapeHtml(rateWarnings.notice.title)}</strong>${rateWarnings.notice.body ? `<span>${escapeHtml(rateWarnings.notice.body)}</span>` : ''}</div>`
      : '';
    if (rateWarnings.phase === 'READY') {
      return `<section class="ws-rate-warnings" data-ws-rate-warning-phase="ready"><div class="ws-rate-warning-heading"><h3>Rate warnings accepted</h3><span class="ws-status ws-status--positive">Ready for finalisation</span></div><p>${rateWarnings.accepted_count} rate warning${rateWarnings.accepted_count === 1 ? '' : 's'} accepted. The final source charge remains unchanged.</p>${notice}</section>`;
    }
    const heading = rateWarnings.phase === 'PREFINAL' ? 'Rate warnings found' : 'Rate warnings to accept';
    const intro = rateWarnings.phase === 'PREFINAL'
      ? 'Check these warnings with NHSP before cutoff. Hours checking remains available.'
      : 'Select the warnings checked, then confirm the acceptance.';
    const headerSelect = accepting
      ? `<th><input type="checkbox" data-ws-rate-warning-header aria-label="${allSelected ? 'Clear visible rate warnings' : 'Select visible rate warnings'}"${eligible.length ? '' : ' disabled'}${allSelected ? ' checked' : ''}${someSelected && !allSelected ? ' data-indeterminate="1" aria-checked="mixed"' : ` aria-checked="${allSelected ? 'true' : 'false'}"`}></th>`
      : '';
    const confirmation = accepting
      ? `<label class="ws-confirm"><input type="checkbox" data-ws-rate-warning-confirm${selected.size ? '' : ' disabled'}><span>I have checked the warnings shown.</span></label><div class="ws-rate-warning-actions"><span data-ws-rate-warning-summary>${selected.size} warning row${selected.size === 1 ? '' : 's'} selected</span><button type="button" class="btn primary" data-ws-rate-warning-accept disabled>Accept selected source charges</button></div>`
      : '';
    return `<section class="ws-rate-warnings" data-ws-rate-warning-phase="${escapeHtml(rateWarnings.phase.toLowerCase())}"><div class="ws-rate-warning-heading"><div><h3>${heading}</h3><p>${intro}</p></div><span class="ws-status ws-status--warning">${rateWarnings.total_count} warning${rateWarnings.total_count === 1 ? '' : 's'}</span></div>${notice}<div class="ws-inner-scroll"><table class="grid mini ws-inner-grid ws-rate-warning-grid${accepting ? ' ws-rate-warning-grid--selectable' : ''}"><thead><tr>${headerSelect}<th>Candidate</th><th>Day/date</th><th>Source charge</th><th>Warning</th><th>Action</th></tr></thead><tbody>${renderRateWarningRows(rateWarnings, viewState) || `<tr><td colspan="${accepting ? 6 : 5}" class="ws-empty">No rate warnings are currently shown.</td></tr>`}</tbody></table></div>${confirmation}</section>`;
  }

  function renderChargeAcceptanceControls(finalise, viewState) {
    if (finalise.active_list !== 'blocked' || finalise.rate_warnings.phase !== 'FINAL_AWAITING_ACCEPTANCE') return '';
    const eligibleRows = new Set(finalise.blocked.rows.filter((row) => row.problem === 'Accept the final NHSP source charge')
      .map((row) => asText(row.row_key)));
    const eligible = finalise.rate_warnings.rows.filter((row) => row.accept_eligible && eligibleRows.has(row.source_row_id));
    if (!eligible.length) return '';
    const selected = [...(viewState.rateWarningSelection || new Set())].filter((key) => eligible.some((row) => row.warning_key === key));
    return `<div class="ws-charge-accept-controls"><span data-ws-rate-warning-summary>${selected.length} charge${selected.length === 1 ? '' : 's'} selected</span><label><input type="checkbox" data-ws-rate-warning-confirm${selected.length ? '' : ' disabled'}> I have checked the selected NHSP charges.</label><button type="button" class="btn primary" data-ws-rate-warning-accept disabled>Accept selected source charges</button></div>`;
  }

  function renderFinalise(workspace, viewState) {
    const finalise = workspace.finalise;
    const signedTimesheetJourney = workspace.imports.journey.authority_mode === 'TIMESHEET_AUTHORITY';
    if (!finalise.prepared && !(signedTimesheetJourney && finalise.complete.total_count)) {
      const prepare = finalise.prepare_action;
      return `${signedTimesheetJourney ? renderFinalisationTracker(workspace) : ''}<div class="ws-notice"><strong>No finalisation report has been prepared</strong><span>Checking files remain available in Imports and Queries.</span>${prepare.command === 'PREPARE_FINALISATION' ? '<button type="button" class="btn primary" data-ws-prepare>Prepare current file for finalisation</button>' : ''}</div>`;
    }
    const active = finalise.active_list;
    const ready = finalise.ready.total_count;
    const blocked = finalise.blocked.total_count;
    const page = finalise[active];
    const tabs = `<div class="ws-inner-tabs" role="tablist">${(signedTimesheetJourney ? ['ready', 'blocked', 'complete'] : ['ready', 'blocked']).map((key) => `<button type="button" role="tab" data-ws-finalise-list="${key}" aria-selected="${active === key}"${finalise[key].total_count ? '' : ' class="ws-tab-empty" disabled'}>${key[0].toUpperCase() + key.slice(1)} (${finalise[key].total_count})</button>`).join('')}</div>`;
    if (signedTimesheetJourney && active === 'complete') {
      return `${renderFinalisationTracker(workspace)}${renderApprovedHoursFollowUp(finalise, viewState)}${tabs}<div class="ws-scroll" data-ws-scroll>${renderFinaliseTable(workspace, page, false, viewState)}<div data-ws-sentinel></div></div><div class="ws-sticky-footer"><span>${page.total_count} complete</span><button type="button" class="btn btn-outline" data-ws-recheck>Recheck</button></div>`;
    }
    const sortable = finaliseColumns(workspace, active === 'blocked').filter(([,key]) => key !== 'actions');
    const stale = page.stale || finalise.stale;
    const finaliseReady = finalise.finalise_enabled
      && finalise.rate_warnings.phase !== 'FINAL_AWAITING_ACCEPTANCE'
      && !stale
      && Object.keys(finalise.finalise_payload).length > 0
      && blocked === 0;
    const confirmationChecked = !finalise.confirmation_required;
    const intentionalConfirmationLock = finaliseReady ? '' : ' disabled data-ctms-intentional-lock="1"';
    const intentionalActionLock = finaliseReady && confirmationChecked ? '' : ' disabled data-ctms-intentional-lock="1"';
    const staleNotice = stale ? '<div class="ws-notice ws-notice--warning" role="status"><span>This information has changed. Recheck before continuing.</span></div>' : '';
    const excluded = finalise.exclusion_confirmation ? `<section><h3>Not included as finalised shifts</h3>${renderFinaliseTable(workspace, { rows: finalise.excluded_rows, stale }, true, viewState)}<label class="ws-confirm"><input type="checkbox" data-ws-exclusion-confirm><span>${escapeHtml(finalise.exclusion_confirmation)}</span></label></section>` : '';
    return `${signedTimesheetJourney ? renderFinalisationTracker(workspace) : ''}${renderApprovedHoursFollowUp(finalise, viewState)}${staleNotice}<div class="ws-source-summary">${escapeHtml(finalise.source_summary || 'Current report')}</div>${excluded}${tabs}${renderMobileSort(sortable, viewState.sort?.finalise || {})}<div class="ws-scroll" data-ws-scroll>${renderFinaliseTable(workspace, page, active === 'blocked', viewState)}<div data-ws-sentinel></div></div>${renderChargeAcceptanceControls(finalise, viewState)}<label class="ws-confirm"><input type="checkbox" data-ws-finalise-confirm${confirmationChecked ? ' checked' : ''}${intentionalConfirmationLock}><span>${escapeHtml(finalise.confirmation_text || 'I confirm this source is complete for the period shown.')}</span></label><div class="ws-sticky-footer"><span>${ready} ready · ${finalise.rate_warnings.accepted_count ? `${finalise.rate_warnings.accepted_count} source charges accepted · ` : ''}${blocked} blocked</span><div><button type="button" class="btn btn-outline" data-ws-recheck>Recheck</button><button type="button" class="btn primary" data-ws-finalise${intentionalActionLock}>${escapeHtml(workspace.profile.finalise_label)}</button></div></div>`;
  }

  function renderHistory(workspace, state) {
    const history = workspace.history;
    const options = history.cycle_options.length ? history.cycle_options : [{ value: 'CURRENT_PAY_CYCLE', label: 'Current pay cycle' }];
    const rows = history.rows.map((rowValue) => { const row = asObject(rowValue); const detail = escapeHtml(asText(row.detail) || '—'); return `<tr><td data-label="When">${escapeHtml(asText(row.when) || '—')}</td><td data-label="Source">${escapeHtml(asText(row.source) || '—')}</td><td data-label="Event">${escapeHtml(asText(row.event) || '—')}</td><td data-label="By">${escapeHtml(asText(row.by) || '—')}</td><td data-label="Details"><span class="ws-history-detail" title="${detail}">${detail}</span></td></tr>`; }).join('');
    const sort = state.sort?.history || {};
    const sortable = [['When','when'],['Source','source'],['Event','event'],['By','by'],['Details','detail']];
    return `<div class="ws-toolbar"><label>Period<select data-ws-history-cycle>${options.map((option) => `<option value="${escapeHtml(option.value)}"${option.value === history.cycle_filter ? ' selected' : ''}>${escapeHtml(option.label)}</option>`).join('')}</select></label><button type="button" class="btn btn-outline" data-ws-recheck>Recheck</button></div>${renderMobileSort(sortable, sort)}<div class="ws-scroll" data-ws-scroll><table class="grid mini ws-grid ws-history-grid"><thead><tr>${sortable.map(([label,key]) => renderSortHeader(label,key,sort)).join('')}</tr></thead><tbody>${rows || '<tr><td colspan="5" class="ws-empty">Nothing matches the current filters.</td></tr>'}</tbody></table><div data-ws-sentinel></div></div><div class="ws-sticky-footer"><span>${history.total_count} event${history.total_count === 1 ? '' : 's'}</span></div>`;
  }

  function renderWorkspace(workspace, tabKey, state = {}) {
    const tab = availableTab(workspace, tabKey);
    const nextPayNotice = state.bankingOwnerClient?.peek()?.active_owner === 'NEXT'
      ? `<div class="ws-notice ws-notice--info" role="status"><span>${escapeHtml(NEXT_PAY_NOTICE)}</span></div>` : '';
    const notices = workspace.notices.map((notice) => `<div class="ws-notice ws-notice--${escapeHtml(notice.tone)}"><strong>${escapeHtml(notice.title)}</strong>${notice.body ? `<span>${escapeHtml(notice.body)}</span>` : ''}</div>`).join('');
    const visibleError = state.error === NEXT_PAY_NOTICE && !nextPayNotice
      ? 'Weekly source information is temporarily unavailable. Recheck or try again shortly.' : state.error;
    const error = asText(visibleError) ? `<div class="ws-notice ws-notice--danger" role="alert"><strong>Weekly source information could not be loaded</strong><span>${escapeHtml(visibleError)}</span></div>` : '';
    const returnLink = state.checklistReturnLink && tab !== 'checklist' ? '<button type="button" class="btn btn-outline" data-ws-checklist-return>Back to Checklist</button>' : '';
    const combined = tab === 'finalise' && workspace.combined_source_workspace && root.CloudTMSCombinedFinaliseV1;
    const review = ['imports','queries','history'].includes(tab) && workspace.combined_source_workspace && !state.reviewSingleScope && root.CloudTMSCombinedReviewV1;
    if(review) return `<div class="ws-workspace" data-ws-contract="${CONTRACT}" data-ws-tab="${tab}">${returnLink}${nextPayNotice}${error}${tab==='imports'?renderImports(workspace,{...state,toolbarOnly:true}):''}${state.loading?'<p role="status">Loading source work…</p>':root.CloudTMSCombinedReviewV1.render(state.reviews?.[tab]?.model,state.reviews?.[tab])}</div>`;
    if (tab === 'checklist') return `<div class="ws-workspace" data-ws-contract="${CONTRACT}" data-ws-tab="checklist">${renderContext(workspace)}${state.checklist?.render() || '<p role="status">Checklist information is unavailable. Recheck shortly.</p>'}</div>`;
    const body = state.loading === true ? '<div class="ws-loading" role="status">Loading weekly source information...</div>' : combined ? root.CloudTMSCombinedFinaliseV1.render(state.combined?.model,state.combined) : tab === 'imports' ? renderImports(workspace, state) : tab === 'queries' ? renderQueries(workspace, state) : tab === 'finalise' ? renderFinalise(workspace, state) : renderHistory(workspace, state);
    return `<div class="ws-workspace" data-ws-contract="${CONTRACT}" data-ws-tab="${tab}">${returnLink}${tab==='finalise'&&state.combined?.singleScope?'<button class="btn btn-outline" data-wsc-back>Back to all reports</button>':''}${combined?'':renderContext(workspace)}${nextPayNotice}${notices}${error}${body}</div>`;
  }

  const session = {
    pendingSourceOutreach: null, pendingSourceFinalise: null, pendingSourceRate: null, openCurrent: null,
    checklist: null, checklistActor: null, checklistFilters: '', checklistOnReturn: false,
    bankingOwnerClient: null, openGeneration: 0, reviews: {}, reviewSingleScope: false, sourceFilters: {},
    combined: { model: null, filters: {}, list: 'ready', sort: '', direction: 'asc', seek: '', selected: new Set(), exclusions: new Set(), batch: null, error: '' },
    workspace: null, activeTab: 'imports', loading: false, error: '', requestSequence: 0,
    query: { filters: {} },
    sort: {
      imports: { key: 'uploaded', direction: 'desc' },
      queries: { key: 'candidate', direction: 'asc' },
      finalise: { key: 'candidate', direction: 'asc' },
      history: { key: 'when', direction: 'desc' }
    },
    scrollByTab: Object.create(null), finaliseRetry: null, loadedTab: '',
    rateWarningSelection: new Set(), expandedRateWarning: '', uploadProfileId: '', signedTab: 'checks',
    groupSelection: { mode: 'NONE', ids: new Set(), exclusions: new Set() }, shiftSelections: new Map(), observer: null
  };

  const currentFrame = () => { const stack = Array.isArray(root.__modalStack) ? root.__modalStack : []; const frame = stack[stack.length - 1]; return frame?.kind === 'weekly-source-imports-v1' ? frame : null; };
  function clearChecklistNavigation() {
    session.checklistReturnLink=false;session.checklistOnReturn=false;
    session.checklistQueryTarget=null;session.checklistQueryScrollPending=false;session.reviewSingleScope=false;
  }
  // These named UI actions retain literal inputs; there is no automatic reissue.
  function sourceRequestContext() {
    const generation = session.openGeneration, frame = currentFrame(), current = session.openCurrent;
    const selected = JSON.stringify(session.workspace?.selected), filters = JSON.stringify(session.sourceFilters);
    return () => !!frame && generation === session.openGeneration && frame === currentFrame()
      && (!current || current()) && selected === JSON.stringify(session.workspace?.selected)
      && filters === JSON.stringify(session.sourceFilters);
  }
  const sourceRequestCopy = value => JSON.parse(JSON.stringify(value));
  function lockSourceRequest(host, pending, actionSelector, actionAttribute) {
    if (!pending) return;
    host.querySelectorAll('input, select, textarea, button').forEach(control => {
      const retry = control.matches(actionSelector) && (!actionAttribute || control.dataset[actionAttribute] === pending.command);
      control.disabled = !retry || pending.inFlight === true;
      if (retry) control.textContent = pending.busy ? 'Try again' : 'Retry exact request';
    });
  }
  const endpoint = (path) => typeof root.API === 'function' ? root.API(path) : path;

  async function requestJson(path, options) {
    const fetcher = typeof root.authFetch === 'function' ? root.authFetch : root.fetch;
    if (typeof fetcher !== 'function') throw new Error('Weekly source access is unavailable.');
    const response = await fetcher(endpoint(path), options);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(asText(payload.message) || 'Weekly source request failed.');
      error.status = response.status;
      error.code = asText(payload.error_code);
      error.details = asObject(payload.details);
      if (response.status === 409 && payload.error_code === 'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY'
          && payload.retryable === true && payload.failed_rpc_outcome === 'ROLLED_BACK'
          && payload.automatic_retry === false) {
        error.retryable = true; error.failed_rpc_outcome = 'ROLLED_BACK'; error.automatic_retry = false;
        error.message = SOURCE_ADMISSION_BUSY_MESSAGE;
      }
      throw error;
    }
    return payload;
  }

  function queryFor(tab, extra = {}) {
    const selected = session.workspace?.selected || {};
    const sort = session.sort[tab] || {};
    const checklistTarget = tab === 'queries' && session.checklistReturnLink && session.reviewSingleScope
      ? session.checklistQueryTarget : null;
    return Object.fromEntries(Object.entries({
      tab,
      source_group_id: selected.source_group_id,
      source_cycle_id: selected.source_cycle_id,
      client_id: selected.client_id,
      report_scope_id: checklistTarget ? null : selected.report_scope_id,
      sort_key: sort.key,
      sort_direction: sort.direction,
      ...(tab === 'queries' ? {
        ...session.query.filters
      } : {}),
      ...(tab === 'history' ? { cycle_filter: session.workspace?.history?.cycle_filter } : {}),
      ...extra,
      ...(checklistTarget ? { row_key: checklistTarget } : {})
    }).filter(([, value]) => value !== null && value !== undefined && value !== ''));
  }

  function pageFor(tab) { return tab === 'finalise' ? session.workspace?.finalise?.[session.workspace.finalise.active_list] : session.workspace?.[tab]; }
  function repaint() {
    const frame = currentFrame();
    if (!frame?.setTab) return Promise.resolve();
    frame.tabs = tabDescriptors(session.workspace);
    session.activeTab = availableTab(session.workspace, session.activeTab);
    return Promise.resolve(frame.setTab(session.activeTab)).catch(() => {});
  }

  function cycleForClientChange(workspace, clientId) {
    const currentId = asText(workspace?.selected?.source_cycle_id);
    if (!workspace?.combined_source_workspace) return currentId;
    const options = asArray(workspace.context?.controls).find(control => control.key === 'cycle')?.options || [];
    const current = options.find(option => option.value === currentId);
    if (!current?.scope_client_id || current.scope_client_id === clientId) return currentId;
    const sameWeek = options.filter(option => option.finalisation_week_ending
      && option.finalisation_week_ending === current.finalisation_week_ending);
    const next = sameWeek.find(option => clientId && option.scope_client_id === clientId)
      || sameWeek.find(option => !option.scope_client_id);
    if (!next) throw new Error('The selected client has no available source period for this week. Refresh the Imports screen before continuing.');
    return next.value;
  }

  function defaultUploadProfileId(workspace) {
    if (!workspace?.profile?.id?.startsWith('NHSP_')) return asText(workspace?.profile?.id);
    return workspace.context.cycle_state === 'Before cutoff'
      ? 'NHSP_PREFINAL_RELEASED_V1'
      : 'NHSP_FINAL_BACKING_V1';
  }

  function syncUploadProfile(workspace) {
    const validNhspSelection = NHSP_UPLOAD_PROFILES.some((profile) => profile.id === session.uploadProfileId);
    if (!workspace.profile.id.startsWith('NHSP_') || !validNhspSelection) {
      session.uploadProfileId = defaultUploadProfileId(workspace);
    }
  }

  function checklistFilters() {
    const selected=session.workspace?.selected||{};
    return Object.fromEntries(['source_group_id','client_id','source_cycle_id','week_ending']
      .map(key=>[key,session.sourceFilters[key]||selected[key]])
      .filter(([,value])=>typeof value==='string'&&value));
  }

  async function loadChecklist(append=false) {
    session.loading=false;session.loadedTab='checklist';
    const api=root.CloudTMSWeeklySourceChecklistV1;
    if(!api||!session.checklistActor||!session.openCurrent?.())return repaint();
    if(!session.checklist){
      const generation=session.openGeneration;
      session.checklist=api.create({actorId:session.checklistActor,
        permissionView:session.workspace.permissions?'PAY_READONLY':'ADMIN',
        current:()=>generation===session.openGeneration&&session.openCurrent?.()===true
          &&session.activeTab==='checklist'&&!!currentFrame(),
        read:request=>requestJson(`${ENDPOINTS.checklist}?${new URLSearchParams(request)}`),
        changed:()=>repaint(),onAction:openChecklistAction});
    }
    const filters=checklistFilters(),identity=JSON.stringify(filters);
    if(identity!==session.checklistFilters){session.checklistFilters=identity;return session.checklist.reset(filters);}
    if(append||!session.checklist.state.loaded)return session.checklist.load(append);
    return repaint();
  }

  async function returnToChecklist() {
    if(!session.openCurrent?.()||!currentFrame())return;
    session.checklistReturnLink=false;session.activeTab='checklist';session.loadedTab='checklist';
    session.checklistQueryTarget=null;session.checklistQueryScrollPending=false;session.reviewSingleScope=false;
    await repaint();await session.checklist?.refresh();
  }

  async function openChecklistAction(action,row) {
    if(session.checklistActionInFlight||!session.openCurrent?.()||!currentFrame()
      ||session.workspace.permissions||action.native_allowed!==true)return;
    const frame=currentFrame(),generation=session.openGeneration;
    const current=()=>frame===currentFrame()&&generation===session.openGeneration&&session.openCurrent?.();
    session.checklist?.capture();session.checklistOnReturn=true;
    try {
      if(['NO_ONE_WORKED','UNDO_NO_ONE_WORKED'].includes(action.kind)){
        const message=action.kind==='NO_ONE_WORKED'?'Confirm that no one worked for this client and week?':'Undo the saved “No one worked” decision for this client and week?';
        if(typeof root.confirm!=='function'||!root.confirm(message))return;
        if(!current())return;session.checklistActionInFlight=true;
        const names=action.kind==='UNDO_NO_ONE_WORKED'
          ?['source_cycle_id','source_group_id','client_id','completion_id','expected_cycle_version']
          :['source_cycle_id','source_group_id','client_id','expected_cycle_version','attestation_text'];
        const payload=Object.fromEntries(names.filter(key=>Object.hasOwn(action.payload,key)).map(key=>[key,action.payload[key]]));
        await issueCommand(action.kind==='NO_ONE_WORKED'?'NO_SHIFTS_TO_IMPORT':'UNDO_NO_ONE_WORKED',payload);
        if(current()){session.checklistOnReturn=false;await session.checklist.refresh();}
        return;
      }
      const target=action.target;
      if(action.kind==='TIMESHEET'&&typeof root.openTimesheet==='function')return root.openTimesheet({timesheet_id:target.timesheet_id});
      if(action.kind==='DAILY_REVIEW'&&typeof root.CloudTmsImportReviewV1?.openReview==='function')
        return root.CloudTmsImportReviewV1.openReview(target.import_id,{returnTo:returnToChecklist});
      if(action.kind==='UPLOAD_DETAIL'&&root.CloudTMSWeeklySourceWorkspaceActionsV1)
        return root.CloudTMSWeeklySourceWorkspaceActionsV1.handleAction({label:'View',payload:target});
      if(['QUERIES','FINALISE','UPLOAD'].includes(action.kind)){
        session.checklistReturnLink=true;session.checklistOnReturn=false;
        session.workspace.selected={...session.workspace.selected,...row.scope,...target};
        if(action.kind==='QUERIES'){
          session.workspace.selected.report_scope_id='';session.workspace.selected.projection_publication_id='';
          session.reviewSingleScope=true;session.query.filters={};session.checklistQueryTarget=target.row_key;
          session.checklistQueryScrollPending=true;
        } else {
          session.checklistQueryTarget=null;session.checklistQueryScrollPending=false;
        }
        const tab=action.kind==='QUERIES'?'queries':action.kind==='FINALISE'?'finalise':'imports';
        session.activeTab=tab;session.loadedTab='';await repaint();
        return;
      }
      throw new Error('Checklist action is unavailable.');
    }catch(error){if(current()){session.checklist.state.error='This action could not be confirmed. Recheck before trying again.';await repaint();}}
    finally{session.checklistActionInFlight=false;}
  }

  async function loadWorkspace(tab = session.activeTab, append = false) {
    if (tab === 'checklist') return loadChecklist(append);
    if(append&&['imports','queries','history'].includes(tab)&&session.workspace?.combined_source_workspace&&!session.reviewSingleScope)return loadCombinedReview(tab,true);
    if (tab === 'finalise' && session.workspace?.combined_source_workspace) return loadCombinedFinalise(append);
    const sequence = ++session.requestSequence;
    const openGeneration = session.openGeneration;
    const openCurrent = session.openCurrent;
    const requestCurrent = () => sequence === session.requestSequence
      && openGeneration === session.openGeneration && (!openCurrent || openCurrent());
    session.loading = !append; session.error = ''; if (session.workspace && !append) await repaint();
    if (!requestCurrent()) return;
    const current = pageFor(tab);
    const params = new URLSearchParams(queryFor(tab, append ? { cursor: current?.next_cursor || '' } : {}));
    try {
      const next = normaliseWorkspace(await requestJson(`${ENDPOINTS.workspace}?${params}`));
      if(tab==='finalise'&&session.combined.singleScope) next.combined_source_workspace=false;
      if (!requestCurrent()) return;
      if (append && current) {
        const nextPage = tab === 'finalise' ? next.finalise[next.finalise.active_list] : next[tab];
        const keys = new Set(current.rows.map((row) => asText(row.row_key || row.group_key || row.id)));
        nextPage.rows = [...current.rows, ...nextPage.rows.filter((row) => { const key = asText(row.row_key || row.group_key || row.id); if (!key || keys.has(key)) return false; keys.add(key); return true; })];
      }
      if (tab === 'queries' && session.checklistReturnLink && session.reviewSingleScope && session.checklistQueryTarget) {
        const target = next.queries.rows.find(row => asText(row.group_key || row.row_key) === session.checklistQueryTarget);
        if (!target || next.queries.rows.length !== 1) {
          const error = new Error('WEEKLY_SOURCE_CHECKLIST_QUERY_TARGET_STALE');
          error.code='WEEKLY_SOURCE_CHECKLIST_QUERY_TARGET_STALE';throw error;
        }
        target.expanded = true;session.checklistQueryScrollPending=true;
      }
      session.workspace = next; syncUploadProfile(next); session.activeTab = availableTab(next, tab); session.loadedTab = session.activeTab; session.loading = false;
      session.reloadOnReturn = !currentFrame();
      if(['imports','queries','history'].includes(tab)&&next.combined_source_workspace&&!session.reviewSingleScope) return loadCombinedReview(tab);
    } catch (error) {
      if (!requestCurrent()) return;
      if (error?.code === 'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED' && !append) {
        try {
          const discovered = await requestJson(ENDPOINTS.workspace);
          if (!requestCurrent()) return;
          session.workspace = normaliseWorkspace(discovered);
          session.activeTab = availableTab(session.workspace, tab);
          session.loadedTab = session.activeTab; session.error = '';
        } catch (discoveryError) {
          if (!requestCurrent()) return;
          session.workspace = emptyWorkspace(); session.error = friendlyWorkspaceError(discoveryError);
        }
      } else { session.loadedTab = tab; session.error = friendlyWorkspaceError(error); }
      session.loading = false;
    }
    await repaint();
    const focus = session.contextFocus;
    if (requestCurrent() && focus?.tab === session.activeTab) {
      const replacement = [...(root.document?.querySelectorAll('[data-ws-context]') || [])]
        .find((element) => element.dataset.wsContext === focus.key && element.value === focus.value);
      if (replacement) { replacement.focus(); session.contextFocus = null; }
    }
  }

  async function loadCombinedReview(tab,append=false) {
    const state=session.reviews[tab] ||= {filters:{},section:tab==='imports'?'current':'questions',sort:tab==='history'?'finalised_at':tab==='imports'?'uploaded':'',direction:tab==='queries'?'asc':'desc',seek:''};
    state.filters=tab==='history'?{...session.sourceFilters,...(state.historyDates||{})}:session.sourceFilters;
    const filterIdentity=JSON.stringify(state.filters);
    if(state.filterIdentity!==filterIdentity){
      state.selected?.clear();state.contactReview=null;state.reportDetail=null;state.seek='';state.filterIdentity=filterIdentity;
      append=false;
    }
    if(append&&state.loadingMore)return;
    state.loadingMore=append;
    const sequence=++session.requestSequence; session.loading=!append; session.error=''; await repaint();
    try {
      const model=await issueCommand('COMBINED_REVIEW_WORKSPACE',{...state.filters,tab,section:state.section,
        sort_key:state.sort||(state.filters.client_id?'candidate':'client'),sort_direction:state.direction,
        ...(tab==='queries'?{attention_first:!state.sort&&!state.seek,
          ...(state.attentionKind?{attention_kind:state.attentionKind}:{})}:{}),
        seek:state.seek,limit:50,...(append?{cursor:state.model?.next_cursor||''}:{})});
      if(sequence!==session.requestSequence)return;
      if(model?.contract!==(tab==='history'?'WEEKLY_SOURCE_REPORT_HISTORY_V1':'WEEKLY_SOURCE_COMBINED_REVIEW_V1'))throw new Error('The source work could not be verified.');
      if(!Object.values(state.filters).some(Boolean))state.options=model.scope_options;
      if(tab==='history'&&!append&&!Object.prototype.hasOwnProperty.call(state.filters,'week_ending')){
        const recent=asArray(model.scope_options).map(option=>option.week_ending).filter(Boolean).sort().at(-1);
        if(recent){state.options=model.scope_options;state.historyDates={...(state.historyDates||{}),week_ending:recent};return loadCombinedReview(tab);}
      }
      if(append&&state.model)model.rows=[...state.model.rows,...model.rows];
      state.model=model; session.loadedTab=tab;
      if(tab==='queries')session.workspace.counts.queries=Number(model.counts.questions||0)+Number(model.counts.checks||0);
    } catch(error){if(sequence===session.requestSequence)session.error=friendlyWorkspaceError(error);}
    finally {if(sequence===session.requestSequence){state.loadingMore=false;session.loading=false;await repaint();restoreCombinedFilterFocus(state,'data-wsr-filter');}}
  }

  function restoreCombinedFilterFocus(state,attribute){
    if(!state.filterFocus)return;
    const input=[...(root.document?.querySelectorAll(`[${attribute}]`)||[])].find(element=>element.getAttribute(attribute)===state.filterFocus);
    input?.focus();state.filterFocus=null;
  }

  function bindCombinedReview(host,tab) {
    const requestCurrent = sourceRequestContext();
    const state=session.reviews[tab],model=state?.model; if(!model)return;
    state.selected ||= new Set();
    const reload=()=>{if(state.busy)return;state.seek='';state.selected.clear();state.contactReview=null;session.scrollByTab[tab]=0;loadCombinedReview(tab);};
    const loadReport=async(key,append=false)=>{
      if(state.reportBusy)return;
      state.reportBusy=true;
      try {
        const detail=await issueCommand('COMBINED_REVIEW_WORKSPACE',{tab:'history',report_key:key,limit:50,
          ...(append?{cursor:state.reportDetail?.next_cursor}:{} )});
        if(detail?.contract!=='WEEKLY_SOURCE_COMPLETED_REPORT_V1')throw new Error('The completed report could not be verified.');
        if(append&&state.reportDetail){detail.shifts=[...state.reportDetail.shifts,...detail.shifts];detail.movements=[...state.reportDetail.movements,...detail.movements];}
        state.reportDetail=detail;
      }catch(error){session.error=friendlyWorkspaceError(error);}
      finally{state.reportBusy=false;await repaint();}
    };
    host.querySelectorAll('[data-wsr-report]').forEach(button=>button.addEventListener('click',()=>loadReport(button.dataset.wsrReport)));
    host.querySelectorAll('[data-wsr-report-action]').forEach(button=>button.addEventListener('click',()=>{
      const action=normaliseActions([state.reportDetail?.actions?.[Number(button.dataset.wsrReportAction)]],ACTIONS.imports)[0];
      if(action&&action.enabled!==false)root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action',{detail:action}));
    }));
    host.querySelectorAll('[data-wsr-manual-review]').forEach(button=>button.addEventListener('click',()=>{
      const shift=(state.reportDetail?.shifts||[]).find(item=>item.source_row_id===button.dataset.wsrManualReview);
      if(shift)root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action',{detail:{
        label:'Send to Pay Queries',payload:{source_row_id:shift.source_row_id,
          candidate:shift.candidate,client:state.reportDetail.report.client,day_date:shift.day_date}
      }}));
    }));
    host.querySelectorAll('[data-wsr-follow-up]').forEach(button=>button.addEventListener('click',async()=>{
      const row=model.rows.find(item=>item.combined_key===button.dataset.wsrFollowUp);
      if(!row?.follow_up_scope)return;
      session.combined.singleScope=true;session.workspace.combined_source_workspace=false;
      session.workspace.selected={...row.follow_up_scope};
      await currentFrame()?.setTab?.('finalise');
      await loadWorkspace('finalise');
    }));
    host.querySelector('[data-wsr-close-report]')?.addEventListener('click',()=>{state.reportDetail=null;repaint();});
    host.querySelector('[data-wsr-report-more]')?.addEventListener('click',()=>loadReport(state.reportDetail.report.report_key,true));
    const contactView=root.CloudTMSCombinedReviewV1;
    const contactEnabled=()=>tab==='queries'&&model.section==='questions'&&!model.attention_kind&&!!contactView;
    const freshContacts=async(keys,current=requestCurrent)=>{
      const wanted=new Set(keys),rows=[],seen=new Set();let cursor='',fresh;
      for(let page=0;page<100;page++){
        if(!current())throw new Error('Refresh the source questions before continuing.');
        const next=await issueCommand('COMBINED_REVIEW_WORKSPACE',{...state.filters,tab:'queries',section:'questions',
          sort_key:state.sort||(state.filters.client_id?'candidate':'client'),sort_direction:state.direction,
          attention_first:false,limit:100,...(cursor?{cursor}:{})});
        if(!current())throw new Error('Refresh the source questions before continuing.');
        if(next?.contract!=='WEEKLY_SOURCE_COMBINED_REVIEW_V1')throw new Error('Contact eligibility could not be verified.');
        fresh=next;
        for(const row of asArray(next.rows))if(wanted.has(row.combined_key)&&!seen.has(row.combined_key)){rows.push(row);seen.add(row.combined_key);}
        if(seen.size===wanted.size||!next.has_more)break;
        if(!next.next_cursor||next.next_cursor===cursor)throw new Error('Refresh the source questions before continuing.');
        cursor=next.next_cursor;
      }
      return {...fresh,rows};
    };
    host.querySelectorAll('[data-wsr-audience]').forEach(button=>button.addEventListener('click',()=>{
      if(state.busy||!contactEnabled())return;
      state.audience=button.dataset.wsrAudience;state.selected.clear();state.contactReview=null;repaint();
    }));
    host.querySelectorAll('[data-wsr-select]').forEach(input=>input.addEventListener('change',()=>{
      if(state.busy||state.contactReview||!contactEnabled())return;
      const row=model.rows.find(item=>item.combined_key===input.dataset.wsrSelect);
      if(input.checked&&contactView.contactPolicy(row,model,state.audience).eligible)state.selected.add(input.dataset.wsrSelect);
      else state.selected.delete(input.dataset.wsrSelect);
      repaint();
    }));
    host.querySelector('[data-wsr-select-eligible]')?.addEventListener('change',event=>{
      if(state.busy||state.contactReview||!contactEnabled())return;
      for(const row of model.rows)if(contactView.contactPolicy(row,model,state.audience).eligible){
        event.target.checked?state.selected.add(row.combined_key):state.selected.delete(row.combined_key);
      }
      repaint();
    });
    host.querySelector('[data-wsr-contact-review]')?.addEventListener('click',async()=>{
      if(state.busy||!contactEnabled()||!state.selected.size)return;
      state.busy=true;session.error='';await repaint();
      try{
        const fresh=await freshContacts([...state.selected]);
        if(!requestCurrent())return;
        const plan=contactView.contactPlan(fresh,state.selected,state.audience);
        for(const key of state.selected)if(!fresh.rows.some(row=>row.combined_key===key))plan.excluded.push({key,
          candidate:model.rows.find(row=>row.combined_key===key)?.candidate||'Selected record',reason:'This question is no longer in the current filtered results.'});
        state.contactReview=plan;
      }catch(error){if(requestCurrent())session.error=friendlyWorkspaceError(error);}
      finally{state.busy=false;if(requestCurrent())await repaint();}
    });
    host.querySelector('[data-wsr-contact-back]')?.addEventListener('click',()=>{if(!state.busy){state.contactReview=null;repaint();}});
    host.querySelector('[data-wsr-contact-dismiss]')?.addEventListener('click',()=>{if(!state.busy){state.contactResults=[];repaint();}});
    host.querySelector('[data-wsr-contact-confirm]')?.addEventListener('click',async()=>{
      const retained=state.pendingSourceOutreach,review=retained?.review??state.contactReview;
      if(state.busy||retained?.inFlight||!review||!contactEnabled()||!review.recipients.length||retained&&!retained.current())return;
      const pending=retained||{command:review.audience==='manager'?'SEND_MANAGER_NOW':'ASK_CANDIDATES',
        review:sourceRequestCopy(review),nextIndex:0,request:null,current:requestCurrent,busy:false,inFlight:false};
      pending.busy=false;pending.inFlight=true;state.busy=true;
      if(!retained)state.contactResults=review.excluded.map(item=>({...item,state:'skipped'}));
      await repaint();
      try{
        // Keep main's per-recipient eligibility/review/outcomes. A proven rolled-back
        // busy retry reuses its original payload; never regenerate it or resend automatically.
        for(;pending.nextIndex<pending.review.recipients.length;pending.nextIndex++){
          if(!pending.current())return;
          const recipient=pending.review.recipients[pending.nextIndex];let outcome;
          try{
            if(!pending.request){
              const fresh=await freshContacts(recipient.rows.map(row=>row.combined_key),pending.current);
              if(!pending.current())return;
              const rows=recipient.rows.map(row=>fresh.rows.find(item=>item.combined_key===row.combined_key));
              if(rows.some((row,index)=>!row||!contactView.contactPolicy(row,fresh,pending.review.audience).eligible
                ||contactView.contactSignature(row)!==contactView.contactSignature(recipient.rows[index]))) {
                outcome=recipient.rows.map(row=>({key:row.combined_key,candidate:row.candidate,state:'skipped',
                  reason:'The question, recipient or eligibility changed after review. Refresh and review it again.'}));
              }else{
                const owner=asArray(fresh.owners).find(item=>item.key===recipient.scope_key);
                const payload=buildOutreachRequest({bulk_actions:owner?.bulk_actions},{mode:'EXPLICIT',ids:rows.map(row=>row.group_key)},pending.command);
                if(!payload)throw new Error('Contact permission changed.');
                pending.request={payload:sourceRequestCopy(payload),rows:sourceRequestCopy(rows)};
              }
            }
            if(pending.request){
              if(!pending.current())return;
              const result=await issueCommand(pending.command,pending.request.payload);
              if(!pending.current())return;
              outcome=contactView.contactOutcome(result,pending.request.rows);
            }
          }catch(error){
            if(!pending.current())return;
            if(pending.request&&(isSourceAdmissionBusy(error)||retained)){
              pending.busy=isSourceAdmissionBusy(error);state.pendingSourceOutreach=pending;
              state.contactReview=pending.review;session.error=friendlyWorkspaceError(error);return;
            }
            // Main's initial unknown outcome remains a status-check/fresh-review result.
            outcome=recipient.rows.map(row=>({key:row.combined_key,candidate:row.candidate,state:'failed',
              reason:'Contact not confirmed. '+friendlyWorkspaceError(error)+' Refresh and check request status before retrying.'}));
          }
          pending.request=null;
          for(const item of outcome){state.contactResults.push({...item,recipient:recipient.recipient,channel:recipient.channel});
            if(item.state==='queued')state.selected.delete(item.key);}
          await repaint();
        }
        if(!pending.current())return;
        state.pendingSourceOutreach=null;state.contactReview=null;session.error='';
        await loadCombinedReview(tab);
      }finally{pending.inFlight=false;state.busy=false;if(pending.current())await repaint();}
    });
    host.querySelector('[data-wsr-refresh]')?.addEventListener('click',reload);
    host.querySelectorAll('[data-wsr-filter]').forEach(input=>input.addEventListener('change',()=>{
      if(state.busy)return;
      state.filterFocus=input.dataset.wsrFilter;
      if(tab==='history'&&['date_from','date_to','week_ending'].includes(input.dataset.wsrFilter)){
        state.historyDates={...(state.historyDates||{}),[input.dataset.wsrFilter]:input.value};
      } else session.sourceFilters[input.dataset.wsrFilter]=input.value;
      state.sort=tab==='history'?'finalised_at':tab==='imports'?'uploaded':'';reload();
    }));
    host.querySelectorAll('[data-wsr-section]').forEach(button=>button.addEventListener('click',()=>{
      if(state.busy)return;
      state.section=button.dataset.wsrSection;
      state.selected?.clear();
      state.attentionKind=button.dataset.wsrAttention||'';
      if(button.hasAttribute('data-wsr-attention')){state.sort='';state.seek='';state.direction='asc';}
      reload();
    }));
    host.querySelectorAll('[data-wsr-sort]').forEach(button=>button.addEventListener('click',()=>{if(state.busy)return;const key=button.dataset.wsrSort;state.direction=(state.sort||model.sort_key)===key&&state.direction==='asc'?'desc':'asc';state.sort=key;reload();}));
    host.querySelectorAll('[data-wsr-action]').forEach(button=>button.addEventListener('click',()=>{
      const row=model.rows.find(item=>item.combined_key===button.dataset.wsrRow);
      const actions=button.hasAttribute('data-wsr-child')?row?.children?.[Number(button.dataset.wsrChild)]?.actions:row?.actions;
      const action=normaliseActions([actions?.[Number(button.dataset.wsrAction)]],new Set([...ACTIONS.imports,...ACTIONS.queries,...ACTIONS.finalise,...ACTIONS.shifts]))[0];
      if(action?.label==='Open'&&model.section==='questions')action.payload={...action.payload,accept_group:row};
      if(action&&action.enabled!==false)root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action',{detail:action}));
    }));
    host.querySelectorAll('[data-wsr-accept-shift]').forEach(button=>button.addEventListener('click',()=>{
      const row=model.rows.find(item=>item.combined_key===button.dataset.wsrRow);
      const child=row?.children?.[Number(button.dataset.wsrAcceptShift)];
      const payload=acceptSingleSystemShiftPayload(row,child?.incident_id);
      if(!payload)return;
      root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action',{detail:{
        label:'Accept system hours',command:'ACCEPT_SYSTEM_HOURS',payload,
        context:{candidate:row.candidate,client:row.client,status:child.day_date+' · 1 shift selected',
          candidate_hours:child.candidate_hours,system_hours:child.system_hours}
      }}));
    }));
    host.querySelector('[data-wsr-add-protected]')?.addEventListener('click',()=>root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action',{detail:{label:'Add protected shift',payload:{source_group_id:state.filters.source_group_id||undefined,client_id:state.filters.client_id||undefined}}})));
    if (tab === 'queries' && state.targetWorkEventId) {
      const group = model.rows.find(row => asArray(row.children).some(child =>
        child.protected_pay_seed?.work_event_id === state.targetWorkEventId));
      const element = [...host.querySelectorAll('[data-wsr-group]')].find(item => item.dataset.wsrGroup === group?.combined_key);
      if (element) {
        element.nextElementSibling?.querySelector('details')?.setAttribute('open', '');
        element.scrollIntoView?.({ block: 'center' });
        state.targetWorkEventId = '';
      }
    }
    const table=host.querySelector('[data-wsr-table]');
    if(table){table.scrollTop=session.scrollByTab[tab]||0;table.addEventListener('scroll',()=>{session.scrollByTab[tab]=table.scrollTop;},{passive:true});}
    table?.addEventListener('keydown',event=>{
      if(state.busy||state.contactReview||event.target!==table||event.ctrlKey||event.altKey||event.metaKey||event.key.length!==1||!/\S/.test(event.key))return;
      event.preventDefault();const now=Date.now();state.seek=now-(state.typedAt||0)>900?event.key:state.seek+event.key;state.typedAt=now;
      clearTimeout(state.seekTimer);state.seekTimer=setTimeout(()=>{if(!state.busy&&!state.contactReview)loadCombinedReview(tab).then(()=>root.document?.querySelector('[data-wsr-table]')?.focus());},250);
    });
    lockSourceRequest(host, state.pendingSourceOutreach??(state.busy?{inFlight:true}:null), '[data-wsr-contact-confirm]');
    const more=host.querySelector('[data-wsr-more]');more?.addEventListener('click',()=>{if(!state.busy&&!state.contactReview)loadCombinedReview(tab,true);});
    session.observer?.disconnect?.();
    if(more&&typeof IntersectionObserver==='function'){session.observer=new IntersectionObserver(entries=>{if(!state.busy&&!state.contactReview&&entries.some(item=>item.isIntersecting))loadCombinedReview(tab,true);},{root:table,rootMargin:'160px'});session.observer.observe(more);}
  }

  async function loadCombinedFinalise(append = false) {
    if (append && session.combined.loadingMore) return;
    session.combined.loadingMore = append;
    const state = session.combined, sequence = ++session.requestSequence;
    state.filters=session.sourceFilters;
    const filterIdentity=JSON.stringify(state.filters);
    if(state.filterIdentity!==filterIdentity){
      state.selected.clear();state.exclusions.clear();state.seek='';state.filterIdentity=filterIdentity;
      append=false;
    }
    session.loading = !append; session.error = ''; await repaint();
    try {
      const model = await issueCommand('COMBINED_FINALISE_WORKSPACE', { ...state.filters,
        list: state.list, sort_key: state.sort || (state.filters.client_id ? 'period' : 'client'),
        sort_direction: state.direction, seek: state.seek, limit: 50,
        ...(append ? { cursor: state.model?.next_cursor || '' } : {}) });
      if (sequence !== session.requestSequence) return;
      if (model?.contract !== 'WEEKLY_SOURCE_COMBINED_FINALISE_V1') throw new Error('Finalisation information could not be verified.');
      if(!append&&!Number(model.counts?.[state.list]||0)){
        const available=['ready','blocked'].find(key=>Number(model.counts?.[key]||0)>0);
        if(available&&available!==state.list){state.list=available;return loadCombinedFinalise();}
      }
      if (!Object.values(state.filters).some(Boolean)) state.options = model.scope_options;
      if (append && state.model) model.rows = [...state.model.rows,...model.rows];
      state.model = model; session.loadedTab = 'finalise';
    } catch (error) { if (sequence !== session.requestSequence) return; session.error = friendlyWorkspaceError(error); }
    finally { if (sequence === session.requestSequence) { state.loadingMore = false; session.loading = false; await repaint(); restoreCombinedFilterFocus(state,'data-wsc-filter'); } }
  }

  function bindCombinedFinalise(host) {
    const requestCurrent = sourceRequestContext();
    const state = session.combined, model = state.model;
    if (!model) return;
    const reload = () => { state.seek = ''; state.selected.clear(); state.exclusions.clear(); session.scrollByTab.finalise=0; loadCombinedFinalise(); };
    host.querySelector('[data-wsc-refresh]')?.addEventListener('click',reload);
    host.querySelectorAll('[data-wsc-progress]').forEach(button=>button.addEventListener('click',()=>{
      const details=host.querySelector('[data-wsc-obligations]');
      if(!details)return;
      details.open=true;
      details.scrollIntoView({block:'nearest'});
      details.querySelector('summary')?.focus();
    }));
    host.querySelectorAll('[data-wsc-open-report]').forEach(button=>button.addEventListener('click',async()=>{
      const report=[...model.scopes,...(model.obligations||[])].find(item=>item.key===button.dataset.wscOpenReport);
      if(!report||state.batch?.running)return;
      state.singleScope=true; session.workspace.combined_source_workspace=false;
      session.workspace.selected={...report.scope}; await loadWorkspace('finalise');
    }));
    host.querySelectorAll('[data-wsc-zero]').forEach(button=>button.addEventListener('click',()=>{
      const obligation=(model.obligations||[]).find(item=>item.key===button.dataset.wscZero);
      const action=normaliseActions(obligation?.progress?.actions,ACTIONS.tracker).find(item=>item.label==='No shifts to import');
      if(action&&action.enabled!==false)root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action',{detail:action}));
    }));
    host.querySelectorAll('[data-wsc-filter]').forEach(input=>input.addEventListener('change',()=>{ state.filterFocus=input.dataset.wscFilter; state.filters[input.dataset.wscFilter]=input.value; state.sort=''; reload(); }));
    host.querySelectorAll('[data-wsc-list]').forEach(button=>button.addEventListener('click',()=>{ state.list=button.dataset.wscList; reload(); }));
    host.querySelectorAll('[data-wsc-sort]').forEach(button=>button.addEventListener('click',()=>{ const key=button.dataset.wscSort; state.direction=(state.sort||model.sort_key)===key&&state.direction==='asc'?'desc':'asc'; state.sort=key; reload(); }));
    host.querySelectorAll('[data-wsc-select]').forEach(input=>input.addEventListener('change',()=>{ input.checked?state.selected.add(input.dataset.wscSelect):state.selected.delete(input.dataset.wscSelect); repaint(); }));
    host.querySelectorAll('[data-wsc-exclude]').forEach(input=>input.addEventListener('change',()=>{ input.checked?state.exclusions.add(input.dataset.wscExclude):state.exclusions.delete(input.dataset.wscExclude); }));
    host.querySelector('[data-wsc-select-all]')?.addEventListener('change',event=>{
      const eligible=model.scopes.filter(item=>item.finalise_enabled===true&&item.blocked_count===0);
      eligible.forEach(item=>event.target.checked?state.selected.add(item.key):state.selected.delete(item.key));
      repaint();
    });
    host.querySelector('[data-wsc-review]')?.addEventListener('click',()=>{
      try { if (!requestCurrent()) return; state.batch=root.CloudTMSWeeklySourceFinaliseBatchV1.create(model.scopes,[...state.selected],[...state.exclusions]); state.batch.current=requestCurrent; state.error=''; }
      catch(error){ state.error=error.message; } repaint();
    });
    host.querySelector('[data-wsc-dismiss]')?.addEventListener('click',()=>{
      if(state.batch&&!state.batch.running&&(state.batch.items.every(item=>item.state==='READY')
        ||state.batch.items.every(item=>['COMPLETE','SOURCE_COMPLETE_PAY_PENDING'].includes(item.state)))){
        state.batch=null; state.selected.clear(); state.exclusions.clear(); state.error=''; repaint();
      }
    });
    host.querySelector('[data-wsc-run]')?.addEventListener('click',async()=>{
      const batch=state.batch;
      if(batch?.running||!batch?.current?.())return;
      try {
        await root.CloudTMSWeeklySourceFinaliseBatchV1.run(batch,issueCommand,()=>batch.current()?repaint():undefined,batch.current);
        if(batch.current())await loadCombinedFinalise();
      } catch(error){ if(batch.current()){state.error=asText(error.message);await repaint();} }
    });
    host.querySelectorAll('[data-wsc-action]').forEach(button=>button.addEventListener('click',()=>{
      const row=model.rows.find(item=>item.scope_key===button.dataset.wscRow&&asText(item.row_key)===button.dataset.wscKey);
      const action=normaliseActions([row?.actions?.[Number(button.dataset.wscAction)]],ACTIONS.finalise)[0];
      if(action?.enabled!==false&&action)root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action',{detail:action}));
    }));
    let fetching=false;
    const more=async()=>{ if(fetching||!state.model?.has_more)return; fetching=true; try{await loadCombinedFinalise(true);}finally{fetching=false;} };
    const moreButton=host.querySelector('[data-wsc-more]'); moreButton?.addEventListener('click',more);
    session.observer?.disconnect?.();
    if(moreButton&&typeof IntersectionObserver==='function'){
      session.observer=new IntersectionObserver(entries=>{if(entries.some(item=>item.isIntersecting))more();},{root:host.querySelector('[data-wsc-table]'),rootMargin:'160px'});
      session.observer.observe(moreButton);
    }
    const table=host.querySelector('[data-wsc-table]');
    if(table) {
      table.scrollTop=session.scrollByTab.finalise||0;
      table.addEventListener('scroll',()=>{session.scrollByTab.finalise=table.scrollTop;},{passive:true});
    }
    table?.addEventListener('keydown',event=>{
      if(event.target!==table||event.ctrlKey||event.altKey||event.metaKey||event.key.length!==1||!/\S/.test(event.key))return;
      event.preventDefault(); const now=Date.now(); state.seek=now-(state.typedAt||0)>900?event.key:state.seek+event.key; state.typedAt=now;
      clearTimeout(state.seekTimer); state.seekTimer=setTimeout(()=>loadCombinedFinalise().then(()=>root.document?.querySelector('[data-wsc-table]')?.focus()),250);
    });
  }

  function selectionSpec() {
    return {
      mode: session.groupSelection.mode,
      ids: new Set(session.groupSelection.ids),
      exclusions: new Set(session.groupSelection.exclusions)
    };
  }

  async function issueCommand(action, payload) {
    const command = asText(action).toUpperCase();
    const commandPayload = { ...asObject(payload) };
    return requestJson(ENDPOINTS.command, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ action: command, payload: commandPayload })
    });
  }

  function updateSelection(host) {
    host.querySelectorAll('[data-ws-group-select]').forEach((input) => { input.checked = session.groupSelection.mode === 'ALL_FILTERED' ? !session.groupSelection.exclusions.has(input.value) : session.groupSelection.ids.has(input.value); });
    const selectedVisible = [...host.querySelectorAll('[data-ws-group-select]:checked')].filter((input) => !input.disabled).length;
    const total = session.groupSelection.mode === 'ALL_FILTERED' ? Math.max(selectedVisible, session.workspace.queries.total_count - session.groupSelection.exclusions.size) : session.groupSelection.ids.size;
    const header = host.querySelector('[data-ws-group-header]');
    if (header) { header.checked = session.groupSelection.mode === 'ALL_FILTERED' && session.groupSelection.exclusions.size === 0; header.indeterminate = !header.checked && total > 0; header.setAttribute('aria-label', header.checked ? 'Clear all query groups' : 'Select all query groups'); }
    const summary = host.querySelector('[data-ws-selection-summary]'); if (summary) summary.textContent = `${total} query group${total === 1 ? '' : 's'} selected`;
    const shiftCount = [...session.shiftSelections.values()].reduce((sum, value) => sum + value.size, 0);
    const shiftSummary = host.querySelector('[data-ws-shift-summary]'); if (shiftSummary) shiftSummary.textContent = `${shiftCount} shift${shiftCount === 1 ? '' : 's'} selected`;
    const actionsSummary = host.querySelector('[data-ws-actions-summary]');
    if (actionsSummary) {
      const selectedCount = total + shiftCount;
      actionsSummary.textContent = selectedCount > 0 ? `Actions for ${selectedCount} selected` : 'Actions for selected';
      actionsSummary.setAttribute('aria-disabled', selectedCount > 0 ? 'false' : 'true');
    }
    const accept = host.querySelector('[data-ws-accept-selected]'); if (accept) accept.hidden = shiftCount === 0;
    host.querySelectorAll('[data-ws-bulk-action]').forEach((button) => {
      button.disabled = session.workspace.queries.stale || total === 0
        || !buildOutreachRequest(session.workspace.queries, selectionSpec(), button.dataset.wsBulkAction);
    });
    if (accept) accept.disabled = session.workspace.queries.stale || shiftCount === 0 || !combinedAcceptSystemHoursPayload();
    host.querySelectorAll('[data-ws-shift-header]').forEach((shiftHeader) => {
      const group = session.workspace.queries.rows.find((entry) => asText(entry.group_key) === shiftHeader.dataset.wsShiftHeader);
      const exactIds = exactAcceptSystemHoursAction(group)?.selection?.incident_ids || [];
      const selected = session.shiftSelections.get(shiftHeader.dataset.wsShiftHeader) || new Set();
      shiftHeader.checked = exactIds.length > 0 && exactIds.every((id) => selected.has(id));
      shiftHeader.indeterminate = !shiftHeader.checked && exactIds.some((id) => selected.has(id));
      shiftHeader.disabled = session.workspace.queries.stale || exactIds.length === 0;
      shiftHeader.setAttribute('aria-label', shiftHeader.checked ? 'Clear all shifts in this group' : 'Select all shifts in this group');
    });
    if (header) header.disabled = session.workspace.queries.stale || session.workspace.queries.total_count === 0
      || (bulkOutreachAction(session.workspace.queries, 'ASK_CANDIDATES')?.enabled !== true && bulkOutreachAction(session.workspace.queries, 'SEND_MANAGER_NOW')?.enabled !== true);
  }

  function bindQueries(host) {
    const requestCurrent = sourceRequestContext();
    if (root.matchMedia?.('(max-width: 1500px)').matches) {
      host.querySelectorAll('.ws-query-filter-menu, .ws-query-actions-menu').forEach((menu) => menu.removeAttribute('open'));
    }
    host.querySelector('[data-ws-actions-summary]')?.addEventListener('click', (event) => {
      if (event.currentTarget.getAttribute('aria-disabled') === 'true') event.preventDefault();
    });
    host.querySelector('[data-ws-group-header]')?.addEventListener('change', (event) => { session.groupSelection = event.target.checked ? { mode: 'ALL_FILTERED', ids: new Set(), exclusions: new Set() } : { mode: 'NONE', ids: new Set(), exclusions: new Set() }; updateSelection(host); });
    host.querySelectorAll('[data-ws-group-select]').forEach((input) => input.addEventListener('change', () => { if (session.groupSelection.mode === 'ALL_FILTERED') { input.checked ? session.groupSelection.exclusions.delete(input.value) : session.groupSelection.exclusions.add(input.value); } else { session.groupSelection.mode = 'EXPLICIT'; input.checked ? session.groupSelection.ids.add(input.value) : session.groupSelection.ids.delete(input.value); if (!session.groupSelection.ids.size) session.groupSelection.mode = 'NONE'; } updateSelection(host); }));
    host.querySelectorAll('[data-ws-expand]').forEach((button) => button.addEventListener('click', () => { const row = session.workspace.queries.rows.find((entry) => asText(entry.group_key) === button.dataset.wsExpand); if (row) row.expanded = row.expanded !== true; repaint(); }));
    host.querySelectorAll('[data-ws-shift-header]').forEach((input) => input.addEventListener('change', () => { const group = session.workspace.queries.rows.find((entry) => asText(entry.group_key) === input.dataset.wsShiftHeader); const ids = exactAcceptSystemHoursAction(group)?.selection?.incident_ids || []; if (!ids.length) { input.checked = false; return; } session.shiftSelections.set(input.dataset.wsShiftHeader, new Set(input.checked ? ids : [])); host.querySelectorAll(`[data-ws-shift-select="${CSS.escape(input.dataset.wsShiftHeader)}"]`).forEach((rowInput) => { rowInput.checked = input.checked && ids.includes(rowInput.value); }); updateSelection(host); }));
    host.querySelectorAll('[data-ws-shift-select]').forEach((input) => input.addEventListener('change', () => { const key = input.dataset.wsShiftSelect; const set = session.shiftSelections.get(key) || new Set(); input.checked ? set.add(input.value) : set.delete(input.value); session.shiftSelections.set(key, set); const header = host.querySelector(`[data-ws-shift-header="${CSS.escape(key)}"]`); const eligible = [...host.querySelectorAll(`[data-ws-shift-select="${CSS.escape(key)}"]`)].filter((entry) => !entry.disabled); if (header) { header.checked = eligible.length > 0 && eligible.every((entry) => entry.checked); header.indeterminate = !header.checked && eligible.some((entry) => entry.checked); header.setAttribute('aria-label', header.checked ? 'Clear all shifts in this group' : 'Select all shifts in this group'); } updateSelection(host); }));
    host.querySelectorAll('[data-ws-filter]').forEach((control) => control.addEventListener('change', () => {
      session.query.filters[control.dataset.wsFilter] = control.value;
      session.groupSelection = { mode: 'NONE', ids: new Set(), exclusions: new Set() };
      loadWorkspace('queries');
    }));
    host.querySelectorAll('[data-ws-bulk-action]').forEach((button) => button.addEventListener('click', async () => {
      const retained = session.pendingSourceOutreach;
      if (retained?.inFlight) return;
      if (retained && (retained.command !== button.dataset.wsBulkAction || !retained.current())) return;
      const built = retained?.payload || buildOutreachRequest(session.workspace.queries, selectionSpec(), button.dataset.wsBulkAction);
      if (!built) return;
      const request = retained || { command: button.dataset.wsBulkAction, current: requestCurrent, payload: sourceRequestCopy(built) };
      if (!request.current()) return;
      request.busy = false; request.inFlight = true; session.pendingSourceOutreach = request; button.disabled = true;
      try {
        await issueCommand(request.command, request.payload);
        if (!request.current()) return;
        session.pendingSourceOutreach = null; session.error = '';
        session.groupSelection = { mode: 'NONE', ids: new Set(), exclusions: new Set() }; await loadWorkspace('queries');
      } catch (error) {
        if (!request.current()) return;
        request.busy = isSourceAdmissionBusy(error);
        if (request.busy || retained) session.pendingSourceOutreach = request;
        request.inFlight = false; session.error = friendlyWorkspaceError(error); repaint();
      }
      finally { request.inFlight = false; }
    }));
    host.querySelector('[data-ws-accept-selected]')?.addEventListener('click', async (event) => {
      const payload = combinedAcceptSystemHoursPayload();
      if (!payload) return;
      event.currentTarget.disabled = true;
      const count = payload.selection.incident_ids.length;
      root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action', {
        detail: {
          label: 'Accept system hours', command: 'ACCEPT_SYSTEM_HOURS', payload,
          context: { status: `${count} shift${count === 1 ? '' : 's'} selected` }
        }
      }));
    });
    updateSelection(host);
    lockSourceRequest(host, session.pendingSourceOutreach, '[data-ws-bulk-action]', 'wsBulkAction');
  }

  function selectedUploadProfileId() {
    const workspace = session.workspace || emptyWorkspace();
    if (!workspace.profile.id.startsWith('NHSP_')) return workspace.profile.id;
    if (NHSP_UPLOAD_PROFILES.some((profile) => profile.id === session.uploadProfileId)) return session.uploadProfileId;
    return workspace.context.cycle_state === 'Before cutoff'
      ? 'NHSP_PREFINAL_RELEASED_V1'
      : 'NHSP_FINAL_BACKING_V1';
  }

  async function uploadSource(file, options = {}) {
    if (!file || typeof root.uploadImportFileToR2 !== 'function') throw new Error('File upload is unavailable.');
    const profileId = selectedUploadProfileId();
    const nhsp = profileId === 'NHSP_PREFINAL_RELEASED_V1' || profileId === 'NHSP_FINAL_BACKING_V1';
    const stored = await root.uploadImportFileToR2(file);
    // NHSP final Trust is parsed from each file and bound by the service.
    // A previously selected Trust must not reject another Trust's report.
    const preview = await requestJson(ENDPOINTS.preview, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ file_key: stored.fileKey, original_filename: stored.filename, source_group_id: session.workspace.selected.source_group_id, source_cycle_id: session.workspace.selected.source_cycle_id, client_id: nhsp ? null : session.workspace.selected.client_id, profile_id: profileId, parser_options: { profileId } }) });
    const serverContext = asObject(preview?.accept_context);
    const controlValue = (key) => asText(session.workspace.context.controls.find((control) => control.key === key)?.value);
    const acceptContext = {
      file_key: stored.fileKey,
      original_filename: stored.filename,
      source_group_id: asText(serverContext.source_group_id || session.workspace.selected.source_group_id),
      source_cycle_id: asText(serverContext.source_cycle_id || session.workspace.selected.source_cycle_id),
      client_id: asText(Object.hasOwn(serverContext, 'client_id') ? serverContext.client_id : session.workspace.selected.client_id),
      report_scope_id: asText(Object.hasOwn(serverContext, 'report_scope_id') ? serverContext.report_scope_id : session.workspace.selected.report_scope_id),
      authority_scope_version: serverContext.authority_scope_version,
      cutoff: controlValue('cutoff'),
      profile_id: asText(preview?.preview?.profileId || session.workspace.profile.id),
      parser_options: { profileId: asText(preview?.preview?.profileId || session.workspace.profile.id) }
    };
    // The server may resolve an NHSP Trust into the exact Client and report
    // scope only after it has parsed the uploaded workbook.  Preserve that
    // server-owned selection for the refresh which follows acceptance;
    // otherwise the Imports row is visible at cycle level while Queries and
    // Finalise incorrectly reopen with no Trust selected.
    if (options.keepSelection !== true && session.workspace?.selected) {
      session.workspace.selected.source_group_id = acceptContext.source_group_id;
      session.workspace.selected.source_cycle_id = acceptContext.source_cycle_id;
      session.workspace.selected.client_id = acceptContext.client_id;
      session.workspace.selected.report_scope_id = acceptContext.report_scope_id;
      session.workspace.selected.projection_publication_id = '';
    }
    if (options.dispatch !== false) root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-preview', { detail: { ...preview, accept_context: acceptContext } }));
    return { ...preview, accept_context: acceptContext };
  }

  async function uploadSources(files) {
    const selected = [...files];
    if (selected.length < 2 || selectedUploadProfileId() !== 'NHSP_FINAL_BACKING_V1') {
      throw new Error('Select several files only for NHSP final backing reports.');
    }
    const results = [];
    for (const file of selected) {
      try {
        results.push({ filename: file.name, detail: await uploadSource(file, { dispatch: false, keepSelection: true }) });
      } catch (error) {
        results.push({ filename: file.name, error: asText(error?.message) || 'This file could not be reviewed.' });
      }
    }
    root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-batch-preview', { detail: results }));
    return results;
  }

  function selectAcceptedScope(context) {
    if (!session.workspace?.selected) return;
    for (const key of ['source_group_id', 'source_cycle_id', 'client_id', 'report_scope_id']) {
      session.workspace.selected[key] = asText(context?.[key]);
    }
    session.workspace.selected.projection_publication_id = '';
  }

  async function acceptUpload(payload) {
    return requestJson(ENDPOINTS.accept, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(asObject(payload))
    });
  }

  function bindCommon(host) {
    const requestCurrent = sourceRequestContext();
    host.querySelector('[data-ws-checklist-return]')?.addEventListener('click', returnToChecklist);
    host.querySelector('[data-ws-tracker-cycle]')?.addEventListener('change', (event) => {
      if (!session.workspace?.selected) return;
      session.workspace.selected.source_cycle_id = asText(event.target.value);
      session.workspace.selected.client_id = '';
      session.workspace.selected.report_scope_id = '';
      session.workspace.selected.projection_publication_id = '';
      session.uploadProfileId = '';
      session.groupSelection = { mode: 'NONE', ids: new Set(), exclusions: new Set() };
      session.shiftSelections.clear();
      loadWorkspace(session.activeTab);
    });
    host.querySelector('[data-ws-upload-profile]')?.addEventListener('change', (event) => {
      session.uploadProfileId = asText(event.target.value);
      const input = host.querySelector('[data-ws-upload-input]');
      if (input) input.multiple = session.uploadProfileId === 'NHSP_FINAL_BACKING_V1';
    });
    host.querySelectorAll('[data-ws-context]').forEach((control) => control.addEventListener('change', async () => {
      const contextKey = control.dataset.wsContext;
      const chosenValue = control.value;
      session.contextFocus = { key: contextKey, value: chosenValue, tab: session.activeTab };
      session.pendingRecheck = null; session.pendingRecheckCurrent = null; // The server retains resumable work for its exact scope.
      const keyMap = { source_group: 'source_group_id', cycle: 'source_cycle_id', client: 'client_id' };
      const key = keyMap[control.dataset.wsContext] || control.dataset.wsContext;
      if (key === 'client_id') {
        try { session.workspace.selected.source_cycle_id = cycleForClientChange(session.workspace, chosenValue); }
        catch (error) { session.error = error.message; await repaint(); return; }
      }
      if (Object.prototype.hasOwnProperty.call(session.workspace.selected, key)) session.workspace.selected[key] = control.value;
      if (key === 'source_group_id') {
        session.uploadProfileId = '';
        session.workspace.selected.source_cycle_id = '';
        session.workspace.selected.client_id = '';
        session.workspace.selected.report_scope_id = '';
        session.workspace.selected.projection_publication_id = '';
      } else if (key === 'source_cycle_id') {
        session.uploadProfileId = '';
        session.workspace.selected.client_id = '';
        session.workspace.selected.report_scope_id = '';
        session.workspace.selected.projection_publication_id = '';
      } else if (key === 'client_id') {
        session.workspace.selected.report_scope_id = '';
        session.workspace.selected.projection_publication_id = '';
      }
      session.groupSelection = { mode: 'NONE', ids: new Set(), exclusions: new Set() };
      session.shiftSelections.clear();
      await loadWorkspace(session.activeTab);
    }));
    host.querySelectorAll('[data-ws-recheck]').forEach((button) => button.addEventListener('click', async () => {
      if (session.recheckBusy || !requestCurrent()) return;
      if (session.pendingRecheck && !session.pendingRecheckCurrent?.()) return;
      const payload = session.pendingRecheck || sourceRequestCopy(session.workspace.recheck_payload);
      if (!payload?.request_id) return loadWorkspace(session.activeTab);
      session.recheckBusy = true; session.pendingRecheck = payload; session.pendingRecheckCurrent ||= requestCurrent; session.pendingRecheckBusy = false; button.disabled = true;
      try {
        await issueCommand('RECHECK_SOURCE', payload);
        if (!requestCurrent()) return;
        session.pendingRecheck = null; session.pendingRecheckCurrent = null;
        await loadWorkspace(session.activeTab);
      } catch (error) {
        if (!requestCurrent()) return;
        session.pendingRecheckBusy = isSourceAdmissionBusy(error);
        session.recheckBusy = false; session.error = friendlyWorkspaceError(error); repaint();
      }
      finally { session.recheckBusy = false; }
    }));
    host.querySelectorAll('.ws-row-action').forEach((button) => button.addEventListener('click', () => {
      if (button.disabled) return;
      let payload = {}; let context = {};
      try { payload = JSON.parse(button.dataset.wsPayload || '{}'); } catch {}
      try { context = JSON.parse(button.dataset.wsContext || '{}'); } catch {}
      root.dispatchEvent?.(new CustomEvent('cloudtms:weekly-source-action', { detail: { label: button.dataset.wsAction, kind: button.dataset.wsKind, command: button.dataset.wsCommand, payload, context } }));
    }));
    host.querySelector('[data-ws-refresh]')?.addEventListener('click', () => loadWorkspace(session.activeTab));
    host.querySelectorAll('[data-ws-sort]').forEach((button) => button.addEventListener('click', () => {
      const sort = session.sort[session.activeTab];
      const key = button.dataset.wsSort;
      if (!sort || !key) return;
      if (sort.key === key) sort.direction = sort.direction === 'asc' ? 'desc' : 'asc';
      else { sort.key = key; sort.direction = 'asc'; }
      session.scrollByTab[session.activeTab] = 0;
      loadWorkspace(session.activeTab);
    }));
    const mobileSort = host.querySelector('[data-ws-mobile-sort]');
    const mobileDirection = host.querySelector('[data-ws-mobile-direction]');
    const applyMobileSort = () => {
      const sort = session.sort[session.activeTab];
      if (!sort || !mobileSort?.value || !mobileDirection?.value) return;
      sort.key = mobileSort.value;
      sort.direction = mobileDirection.value === 'desc' ? 'desc' : 'asc';
      session.scrollByTab[session.activeTab] = 0;
      loadWorkspace(session.activeTab);
    };
    mobileSort?.addEventListener('change', applyMobileSort);
    mobileDirection?.addEventListener('change', applyMobileSort);
    const uploadInput = host.querySelector('[data-ws-upload-input]'); host.querySelector('[data-ws-upload]')?.addEventListener('click', () => {
      if (!uploadInput) return;
      // The file chooser must reflect the currently visible selection even if
      // a native select's change event is deferred until it loses focus.
      session.uploadProfileId = asText(host.querySelector('[data-ws-upload-profile]')?.value) || selectedUploadProfileId();
      uploadInput.multiple = session.uploadProfileId === 'NHSP_FINAL_BACKING_V1';
      uploadInput.click();
    }); uploadInput?.addEventListener('change', async () => { const files = [...(uploadInput.files || [])]; uploadInput.value = ''; if (!files.length) return; try { if (files.length > 1) await uploadSources(files); else await uploadSource(files[0]); await loadWorkspace('imports'); } catch (error) { session.error = friendlyWorkspaceError(error); repaint(); } });
    const dailyInput = host.querySelector('[data-ws-daily-input]'); host.querySelector('[data-ws-daily]')?.addEventListener('click', () => dailyInput?.click()); dailyInput?.addEventListener('change', async () => { const file = dailyInput.files?.[0]; dailyInput.value = ''; if (file && typeof root.handleHrRotaFileDrop === 'function') await root.handleHrRotaFileDrop(file); });
    host.querySelectorAll('[data-ws-signed-tab]').forEach((button) => button.addEventListener('click', () => {
      if (button.disabled) return;
      session.signedTab = ['checks', 'ready', 'waiting'].includes(button.dataset.wsSignedTab)
        ? button.dataset.wsSignedTab : 'checks';
      repaint();
    }));
    if (session.pendingRecheck) lockSourceRequest(host,
      { busy: session.pendingRecheckBusy, inFlight: session.recheckBusy }, '[data-ws-recheck]');
  }

  function bindFinalise(host) {
    const requestCurrent = sourceRequestContext();
    host.querySelector('[data-ws-prepare]')?.addEventListener('click', async (event) => {
      event.currentTarget.disabled = true;
      try { await issueCommand('PREPARE_FINALISATION', session.workspace.finalise.prepare_action.payload); await loadWorkspace('finalise'); }
      catch (error) { session.error = friendlyWorkspaceError(error); repaint(); }
    });
    host.querySelectorAll('[data-ws-finalise-list]').forEach((button) => button.addEventListener('click', () => { if (!button.disabled && (session.workspace.imports.journey.authority_mode === 'TIMESHEET_AUTHORITY' ? ['ready', 'blocked', 'complete'] : ['ready', 'blocked']).includes(button.dataset.wsFinaliseList)) { session.workspace.finalise.active_list = button.dataset.wsFinaliseList; repaint(); } }));
    const confirmation = host.querySelector('[data-ws-finalise-confirm]'); const action = host.querySelector('[data-ws-finalise]');
    const canFinalise = () => session.workspace.finalise.finalise_enabled
      && session.workspace.finalise.rate_warnings.phase !== 'FINAL_AWAITING_ACCEPTANCE'
      && !session.workspace.finalise.stale
      && !session.workspace.finalise[session.workspace.finalise.active_list]?.stale
      && Object.keys(session.workspace.finalise.finalise_payload).length > 0
      && session.workspace.finalise.blocked.total_count === 0
      && (!session.workspace.finalise.confirmation_required || confirmation?.checked)
      && (!session.workspace.finalise.exclusion_confirmation || host.querySelector('[data-ws-exclusion-confirm]')?.checked);
    const sync = () => {
      if (!action) return;
      const allowed = session.pendingSourceFinalise ? session.pendingSourceFinalise.current() : canFinalise();
      action.disabled = !allowed || session.pendingSourceFinalise?.inFlight === true;
      if (allowed) delete action.dataset.ctmsIntentionalLock;
      else action.dataset.ctmsIntentionalLock = '1';
    };
    confirmation?.addEventListener('change', sync); sync();
    host.querySelector('[data-ws-exclusion-confirm]')?.addEventListener('change', sync);
    action?.addEventListener('click', async () => {
      const retained = session.pendingSourceFinalise;
      if (retained?.inFlight) return;
      if (retained ? !retained.current() : !canFinalise()) { sync(); return; }
      const request = retained || { command: 'FINALISE_WEEK', current: requestCurrent, payload: sourceRequestCopy({
        ...session.workspace.finalise.finalise_payload,
        ...(session.workspace.finalise.exclusion_confirmation ? { exclude_unfinalised_acknowledged: true } : {}) }) };
      if (!request.current()) return;
      request.busy = false; request.inFlight = true; session.pendingSourceFinalise = request; action.disabled = true; action.dataset.ctmsIntentionalLock = '1';
      try {
        session.finaliseRetry = null; await issueCommand(request.command, request.payload);
        if (!request.current()) return;
        session.pendingSourceFinalise = null; session.error = ''; await loadWorkspace('finalise');
      } catch (error) {
        if (!request.current()) return;
        request.busy = isSourceAdmissionBusy(error);
        if (request.busy || retained) session.pendingSourceFinalise = request;
        request.inFlight = false; session.error = friendlyWorkspaceError(error); repaint();
      }
      finally { request.inFlight = false; }
    });
    host.querySelector('[data-ws-approved-follow-up]')?.addEventListener('click', async (event) => {
      const button = event.currentTarget;
      let payload = {};
      try { payload = JSON.parse(button.dataset.wsPayload || '{}'); } catch {}
      button.disabled = true;
      try {
        const result = await issueCommand(button.dataset.wsCommand, payload);
        const recovery = asObject(result?.recovery);
        const projection = asObject(result?.ordinary_pay_projection);
        if (asText(button.dataset.wsCommand) === 'RECOVER_FINALISED_PAY'
            && result?.status === 'FINALISED_PAY_RECOVERY_REQUIRED'
            && recovery.task_id && recovery.run_id && recovery.expected_task_version
            && projection.final_revision_id) {
          session.finaliseRetry = {
            final_revision_id: projection.final_revision_id,
            run_id: recovery.run_id,
            task_id: recovery.task_id,
            expected_task_version: recovery.expected_task_version,
            confirm_retry: true
          };
          repaint();
          return;
        }
        session.finaliseRetry = null;
        await loadWorkspace('finalise');
      } catch (error) { session.error = friendlyWorkspaceError(error); repaint(); }
    });
    host.querySelector('[data-ws-finalise-retry]')?.addEventListener('click', async (event) => {
      const payload = asObject(session.finaliseRetry);
      if (!payload.task_id) return;
      event.currentTarget.disabled = true;
      try {
        await issueCommand('RECOVER_FINALISED_PAY', payload);
        session.finaliseRetry = null;
        await loadWorkspace('finalise');
      } catch (error) { session.error = friendlyWorkspaceError(error); repaint(); }
    });
    const warnings = session.workspace.finalise.rate_warnings;
    const updateRateWarningSelection = (resetConfirmation = false) => {
      const eligible = [...host.querySelectorAll('[data-ws-rate-warning-select]:not(:disabled)')].map((input) => input.value);
      const selected = new Set(eligible.filter((key) => session.rateWarningSelection.has(key)));
      const allSelected = eligible.length > 0 && eligible.every((key) => selected.has(key));
      const someSelected = eligible.some((key) => selected.has(key));
      const header = host.querySelector('[data-ws-rate-warning-header]');
      if (header) {
        header.checked = allSelected;
        header.indeterminate = someSelected && !allSelected;
        header.setAttribute('aria-checked', header.indeterminate ? 'mixed' : (allSelected ? 'true' : 'false'));
        header.setAttribute('aria-label', allSelected ? 'Clear eligible charges' : 'Select all eligible charges');
      }
      const summary = host.querySelector('[data-ws-rate-warning-summary]');
      if (summary) summary.textContent = `${selected.size} charge${selected.size === 1 ? '' : 's'} selected`;
      const accept = host.querySelector('[data-ws-rate-warning-accept]');
      const confirmation = host.querySelector('[data-ws-rate-warning-confirm]');
      if (resetConfirmation && confirmation) confirmation.checked = false;
      if (confirmation) confirmation.disabled = selected.size === 0;
      const checked = confirmation?.checked === true;
      if (accept) accept.disabled = !checked || !buildRateWarningAcceptancePayload(warnings, [...selected]);
    };
    host.querySelector('[data-ws-rate-warning-header]')?.addEventListener('change', (event) => {
      const eligible = [...host.querySelectorAll('[data-ws-rate-warning-select]:not(:disabled)')].map((input) => input.value);
      session.rateWarningSelection = new Set(event.target.checked ? eligible : []);
      host.querySelectorAll('[data-ws-rate-warning-select]').forEach((input) => { input.checked = event.target.checked && !input.disabled; });
      updateRateWarningSelection(true);
    });
    host.querySelectorAll('[data-ws-rate-warning-select]').forEach((input) => input.addEventListener('change', () => {
      input.checked ? session.rateWarningSelection.add(input.value) : session.rateWarningSelection.delete(input.value);
      updateRateWarningSelection(true);
    }));
    host.querySelector('[data-ws-rate-warning-confirm]')?.addEventListener('change', () => updateRateWarningSelection());
    host.querySelector('[data-ws-rate-warning-accept]')?.addEventListener('click', async (event) => {
      const retained = session.pendingSourceRate;
      if (retained?.inFlight) return;
      if (retained && !retained.current()) return;
      const visibleSelected = [...host.querySelectorAll('[data-ws-rate-warning-select]:not(:disabled):checked')].map(input => input.value);
      const built = retained || buildRateWarningAcceptancePayload(warnings, visibleSelected);
      if (!built) return;
      const request = retained || { ...sourceRequestCopy(built), current: requestCurrent };
      if (!request.current()) return;
      request.busy = false; request.inFlight = true; session.pendingSourceRate = request; event.currentTarget.disabled = true;
      try {
        await issueCommand(request.action, request.payload);
        if (!request.current()) return;
        session.pendingSourceRate = null; session.error = ''; session.rateWarningSelection.clear();
        await loadWorkspace('finalise');
      } catch (error) {
        if (!request.current()) return;
        request.busy = isSourceAdmissionBusy(error);
        if (request.busy || retained) session.pendingSourceRate = request;
        request.inFlight = false; session.error = friendlyWorkspaceError(error); repaint();
      }
      finally { request.inFlight = false; }
    });
    host.querySelectorAll('[data-ws-rate-expand]').forEach((button) => button.addEventListener('click', () => {
      session.expandedRateWarning = session.expandedRateWarning === button.dataset.wsRateExpand ? '' : button.dataset.wsRateExpand;
      repaint();
    }));
    updateRateWarningSelection();
    lockSourceRequest(host, session.pendingSourceFinalise, '[data-ws-finalise]');
    lockSourceRequest(host, session.pendingSourceRate, '[data-ws-rate-warning-accept]');
  }

  function bindInfiniteScroll(host) {
    session.observer?.disconnect?.(); const sentinel = host.querySelector('[data-ws-sentinel]'); const page = pageFor(session.activeTab);
    if (!sentinel || !page?.has_more || !page.next_cursor || typeof IntersectionObserver !== 'function') return;
    session.observer = new IntersectionObserver((entries) => { if (entries.some((entry) => entry.isIntersecting)) loadWorkspace(session.activeTab, true); }, { root: host.querySelector('[data-ws-scroll]'), rootMargin: '160px' }); session.observer.observe(sentinel);
  }

  function wire(tab) {
    const host = document.querySelector(`.ws-workspace[data-ws-tab="${CSS.escape(tab)}"]`); if (!host || host.dataset.wsWired === '1') return; host.dataset.wsWired = '1';
    // The shared shell highlights clicks, but programmatic deep-links and
    // return-to-parent renders also need the selected tab to match the body.
    const descriptors = tabDescriptors(session.workspace);
    document.querySelectorAll('#modalTabs > button').forEach((button) => {
      const descriptor = descriptors.find((entry) => entry.key === button.dataset.tabKey);
      if (!descriptor) return;
      button.classList.toggle('active', descriptor.key === tab);
      button.setAttribute('aria-selected', String(descriptor.key === tab));
    });
    if(tab==='checklist'){ bindCommon(host); session.checklist?.bind(host); return; }
    session.checklist?.suspend();
    if(tab==='finalise'&&session.workspace?.combined_source_workspace){ bindCombinedFinalise(host); return; }
    if(['imports','queries','history'].includes(tab)&&session.workspace?.combined_source_workspace&&!session.reviewSingleScope){bindCommon(host);bindCombinedReview(host,tab);return;}
    if(tab==='queries'&&session.reviewSingleScope){
      const back=document.createElement('button');back.className='btn btn-outline';back.textContent='Back to all queries';host.prepend(back);
      back.addEventListener('click',()=>{session.reviewSingleScope=false;session.checklistQueryTarget=null;session.checklistQueryScrollPending=false;loadCombinedReview('queries');});
    }
    host.querySelector('[data-wsc-back]')?.addEventListener('click',()=>{
      session.combined.singleScope=false; session.workspace.combined_source_workspace=true; loadCombinedFinalise();
    });
    bindCommon(host); if (tab === 'queries') bindQueries(host); if (tab === 'finalise') bindFinalise(host);
    const scroll = host.querySelector('[data-ws-scroll]');
    if (scroll) {
      scroll.scrollTop = Number(session.scrollByTab[tab] || 0);
      scroll.addEventListener('scroll', () => { session.scrollByTab[tab] = scroll.scrollTop; }, { passive: true });
    }
    if(tab==='queries'&&tab===session.activeTab&&session.checklistReturnLink&&session.reviewSingleScope
      &&session.checklistQueryScrollPending&&session.openCurrent?.()){
      const target=[...host.querySelectorAll('[data-ws-group-key]')].find(row=>row.dataset.wsGroupKey===session.checklistQueryTarget);
      if(target){target.scrollIntoView?.({block:'nearest'});session.checklistQueryScrollPending=false;}
    }
    host.querySelector('[data-ws-history-cycle]')?.addEventListener('change', (event) => { session.workspace.history.cycle_filter = event.target.value; session.scrollByTab.history = 0; loadWorkspace('history'); }); bindInfiniteScroll(host);
  }

  function renderTab(tab) {
    if(tab!=='checklist') session.checklist?.suspend();
    session.activeTab = availableTab(session.workspace || emptyWorkspace(), tab);
    if (session.activeTab !== session.loadedTab && !session.loading) {
      session.loading = true;
      setTimeout(() => loadWorkspace(session.activeTab), 0);
    }
    const markup = renderWorkspace(session.workspace || emptyWorkspace(), session.activeTab, session);
    setTimeout(() => wire(session.activeTab), 0);
    return typeof root.html === 'function' ? root.html(markup) : markup;
  }

  async function open(initialTab = 'imports', options = {}) {
    const tab = TABS.includes(initialTab) ? initialTab : 'imports';
    const generation = ++session.openGeneration;
    const actorId = options.actorId;
    const current = () => generation === session.openGeneration
      && (!options.currentActorId || options.currentActorId() === actorId)
      && (!options.current || options.current());
    if (!current()) return;
    session.checklist?.dispose(); session.checklist = null; session.checklistActor = actorId; session.checklistFilters = '';
    clearChecklistNavigation();
    session.openCurrent = current;
    session.pendingRecheck = null; session.pendingRecheckCurrent = null; session.pendingRecheckBusy = false;
    session.pendingSourceOutreach = null; session.pendingSourceFinalise = null; session.pendingSourceRate = null;
    for (const state of Object.values(session.reviews)) state.pendingSourceOutreach = null;
    session.activeTab = tab; session.loading = true; session.error = '';
    session.bankingOwnerClient = options.bankingOwnerClient || null;
    // F15 qualifies this actor/open. Display lookup failure never blocks factual Source.
    const ownerRead = session.bankingOwnerClient?.begin('weekly-source-workspace').catch(() => null);

    try {
      // Discover the server permission view before sending a requested tab.
      const loaded = await requestJson(ENDPOINTS.workspace);
      if (!current()) return;
      session.workspace = normaliseWorkspace(loaded);
      session.activeTab = availableTab(session.workspace, tab);
      session.uploadProfileId = '';
      syncUploadProfile(session.workspace);
      session.loadedTab = session.workspace.permissions?.landing_tab || 'imports';
      if(['finalise','imports','queries','history'].includes(tab)&&session.workspace.combined_source_workspace) session.loadedTab='';
    } catch (error) {
      if (!current()) return;
      session.workspace = emptyWorkspace(); session.loadedTab = tab; session.error = friendlyWorkspaceError(error);
    }
    await ownerRead;
    if (!current()) return;
    session.loading = false; root.modalCtx = { entity: 'weekly-source-imports', data: {}, weeklySourceState: session };
    if (typeof root.showModal !== 'function') throw new Error('The Imports screen is unavailable.');
    root.showModal('Weekly source imports', tabDescriptors(session.workspace), renderTab, null, false, () => {
      if (session.activeTab === 'checklist' && session.checklistOnReturn && currentFrame()) { session.checklistOnReturn = false; repaint().then(() => session.checklist?.refresh()); }
      else if (session.reloadOnReturn && currentFrame()) { session.reloadOnReturn = false; repaint(); }
      else wire(session.activeTab);
    }, { kind: 'weekly-source-imports-v1', noParentGate: true, stayOpenOnSave: false, showSave: false, showApply: false, runOnRender: true, onDismiss: () => { session.checklist?.dispose(); session.checklist = null; clearChecklistNavigation(); } });
    if (tab !== 'imports') await currentFrame()?.setTab?.(availableTab(session.workspace, tab));
  }

  root.addEventListener?.('cloudtms:bpay-next-owner-changed', () => {
    if (!session.bankingOwnerClient) return;
    if (currentFrame()) repaint();
    else session.reloadOnReturn = true;
  });

  async function navigateToExistingShift(target) {
    if (!currentFrame() || !target?.client_id || !target?.source_group_id) return false;
    const kind = target.pay_query_open ? 'queries'
      : target.final_report_key ? 'history'
      : target.finalise_source_cycle_id ? 'finalise' : '';
    if (!kind) return false;
    session.error = '';
    if (kind === 'finalise') {
      session.combined.singleScope = true;
      session.workspace.combined_source_workspace = false;
      session.workspace.selected = { source_group_id: target.source_group_id,
        source_cycle_id: target.finalise_source_cycle_id, client_id: target.client_id,
        report_scope_id: target.finalise_report_scope_id || undefined };
      await currentFrame()?.setTab?.('finalise');
      await loadWorkspace('finalise');
      return true;
    }
    session.reviewSingleScope = false;
    session.workspace.combined_source_workspace = true;
    const period = kind === 'queries' ? target.query_week_ending : target.final_report_week_ending;
    session.sourceFilters = { source_group_id: target.source_group_id,
      client_id: target.client_id, ...(period ? { week_ending: period } : {}) };
    const state = session.reviews[kind] ||= { filters: {}, section: 'questions', sort: '', direction: 'asc', seek: '' };
    if (kind === 'queries') {
      state.section = 'questions'; state.sort = 'candidate'; state.direction = 'asc';
      state.filterIdentity = JSON.stringify(session.sourceFilters);
      state.seek = asText(target.candidate_sort || target.candidate).toLocaleLowerCase('en-GB');
      state.targetWorkEventId = asText(target.work_event_id);
    } else {
      state.historyDates = {};
      state.reportDetail = null;
    }
    await currentFrame()?.setTab?.(kind);
    await loadCombinedReview(kind);
    if (kind === 'history') {
      const detail = await issueCommand('COMBINED_REVIEW_WORKSPACE', {
        ...session.sourceFilters, tab: 'history', report_key: target.final_report_key, limit: 50
      });
      if (detail?.contract !== 'WEEKLY_SOURCE_COMPLETED_REPORT_V1') throw new Error('The completed report could not be opened.');
      state.reportDetail = detail;
      await repaint();
    }
    return true;
  }

  return Object.freeze({
    CONTRACT, ENDPOINTS, normaliseWorkspace, tabDescriptors, renderWorkspace, selectionSpec,
    disposeChecklist: () => { session.checklist?.dispose(); session.checklist = null; clearChecklistNavigation(); },
    normaliseBulkActions, normaliseRateWarnings, buildRateWarningAcceptancePayload, buildOutreachRequest,
    isSourceAdmissionBusy, SOURCE_ADMISSION_BUSY_MESSAGE, open, requestJson, issueCommand, acceptUpload, uploadSource, uploadSources, selectAcceptedScope,
    acceptSystemShiftSelectionPayload,
    refresh: (tab = session.activeTab) => tab === 'checklist' ? session.checklist?.refresh() : loadWorkspace(tab), navigateToExistingShift,
    clearShiftSelections: () => { session.shiftSelections.clear(); },
    _test: Object.freeze({ exactAcceptSystemHoursPayload, combinedAcceptSystemHoursPayload, acceptSingleSystemShiftPayload, friendlyWorkspaceError, cycleForClientChange, queryFor }),
    _session: session
  });
});
