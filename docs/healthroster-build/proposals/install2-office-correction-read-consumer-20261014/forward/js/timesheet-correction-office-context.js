(function (root) {
  'use strict';
  const CONTRACT = 'TIMESHEET_CORRECTION_OFFICE_CONTEXT_V1';
  const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const STATES = new Set(['WAITING', 'APPLIED', 'CANCELLED']);
  const mounts = new WeakMap();
  const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
  const same = (a, b) => typeof a === 'string' && typeof b === 'string' && a.toLowerCase() === b.toLowerCase();
  function closed(value, keys) {
    if (!object(value) || Object.keys(value).length !== keys.length || keys.some(key => !Object.hasOwn(value, key))) {
      throw new Error('Correction details are unavailable. Refresh this Timesheet.');
    }
  }
  function descriptor(value, admin, create) {
    if (value === null) return null;
    const keys = ['request_id', 'correction_id', 'origin', 'state', 'state_revision', 'root_timesheet_id',
      'reversed_timesheet_id', 'replacement_timesheet_id'];
    closed(value, admin && !create ? keys.concat('return_invoice_id') : keys);
    if (!['request_id', 'root_timesheet_id', 'reversed_timesheet_id', 'replacement_timesheet_id'].every(key => UUID.test(value[key] || ''))
        || !STATES.has(value.state) || !Number.isSafeInteger(value.state_revision) || value.state_revision < 1
        || !['SIGNED_TIMESHEET_CORRECTION', 'AGENCY_INVOICE_CORRECTION'].includes(value.origin)
        || value.correction_id !== `awc:${value.root_timesheet_id}:${value.request_id}`
        || (admin && !create && value.return_invoice_id !== null && !UUID.test(value.return_invoice_id || ''))) {
      throw new Error('Correction details are unavailable. Refresh this Timesheet.');
    }
    // Public display retains only the genuine request identity/state and P0.
    // No invoice descriptor, source/finance context or action allowance is made.
    return { requestId: value.request_id, state: value.state, revision: value.state_revision,
      replacementTimesheetId: value.replacement_timesheet_id };
  }
  function decode(value, timesheetId, version, view) {
    closed(value, ['ok', 'contract_version', 'view', 'target', 'create_descriptor', 'request']);
    closed(value.target, ['timesheet_id', 'timesheet_version']);
    if (value.ok !== true || value.contract_version !== CONTRACT || !['ADMIN', 'PAY'].includes(value.view)
        || (view !== 'NATIVE' && value.view !== view)
        || !same(value.target.timesheet_id, timesheetId) || value.target.timesheet_version !== version
        || !Number.isSafeInteger(version) || version < 1 || !UUID.test(timesheetId || '')) {
      throw new Error('Correction details changed. Refresh this Timesheet.');
    }
    if (value.view === 'PAY' && value.create_descriptor !== null) throw new Error('Correction details are unavailable. Refresh this Timesheet.');
    const create = descriptor(value.create_descriptor, value.view === 'ADMIN', true);
    const current = descriptor(value.request, value.view === 'ADMIN', false);
    if (create && (!current || create.requestId !== current.requestId || create.revision !== current.revision
        || create.state !== current.state || create.replacementTimesheetId !== current.replacementTimesheetId)) {
      throw new Error('Correction details changed. Refresh this Timesheet.');
    }
    return current;
  }
  function mount(options) {
    const host = options?.host, document = options?.document || root.document;
    if (!host || !document || typeof options.callerCurrent !== 'function' || typeof options.readContext !== 'function'
        || typeof options.openReplacement !== 'function' || !UUID.test(options.timesheetId || '')
        || !['ADMIN', 'PAY', 'NATIVE'].includes(options.view) || !Number.isSafeInteger(options.version) || options.version < 1) return null;
    const old = mounts.get(host);
    if (old) { old.active = false; old.panel.remove(); }
    const panel = document.createElement('section');
    panel.className = 'ws-correction-context';
    panel.setAttribute('data-correction-office-context', '');
    panel.setAttribute('aria-live', 'polite');
    const state = { panel, active: true };
    mounts.set(host, state);
    const current = () => state.active && mounts.get(host) === state && host.isConnected && options.callerCurrent();
    const drawText = (message, tone) => {
      if (!current()) return;
      panel.replaceChildren();
      const text = document.createElement('p'); text.textContent = message; text.className = `ws-correction-context--${tone}`;
      panel.appendChild(text);
    };
    host.appendChild(panel);
    drawText('Loading correction details…', 'muted');
    const ready = Promise.resolve().then(() => {
      if (!current()) return null;
      return options.readContext();
    }).then(value => {
      if (!current()) return null;
      const request = decode(value, options.timesheetId, options.version, options.view);
      if (!request) { panel.remove(); return null; }
      const labels = { WAITING: 'Correction waiting for the replacement Timesheet.',
        APPLIED: 'Correction applied.', CANCELLED: 'Correction cancelled.' };
      drawText(labels[request.state], request.state === 'WAITING' ? 'warning' : 'muted');
      if (request.state !== 'CANCELLED' && !same(request.replacementTimesheetId, options.timesheetId)) {
        const button = document.createElement('button');button.type = 'button';button.className = 'btn btn-outline';
        button.textContent = 'Open replacement Timesheet';
        button.addEventListener('click', () => {
          if (!current()) return;
          try {
            Promise.resolve(options.openReplacement(request.replacementTimesheetId)).catch(() => {
              if (current()) drawText('Replacement details are unavailable. Refresh this Timesheet.', 'warning');
            });
          } catch {
            if (current()) drawText('Replacement details are unavailable. Refresh this Timesheet.', 'warning');
          }
        });
        panel.appendChild(button);
      }
      return request;
    }).catch(() => {
      if (current()) drawText('Correction details are unavailable. Refresh this Timesheet.', 'warning');
      return null;
    });
    return { ready, dispose() { state.active = false; panel.remove(); if (mounts.get(host) === state) mounts.delete(host); } };
  }
  const api = Object.freeze({ CONTRACT, decode, mount });
  root.CloudTmsCorrectionOfficeContext = api;
  if (typeof module === 'object' && module.exports) module.exports = api;
})(typeof globalThis === 'object' ? globalThis : this);
