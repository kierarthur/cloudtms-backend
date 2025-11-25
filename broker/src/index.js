// ===== Base URL + helpers =====
const BROKER_BASE_URL = window.BROKER_BASE_URL;
const API = (path)=> `${BROKER_BASE_URL}${path}`;

let SESSION = null;  // {accessToken, user, exp}
let refreshTimer = 0;

// ==== DEBUG SWITCHES (global) ====
// Put this at the very top of main.js so all functions can read them.
window.__LOG_RATES  = true;   // logs for rate staging + rates table + rates tab
window.__LOG_PAYTAB = true;   // logs for payment tab + umbrella prefill
window.__LOG_CONTRACTS = true;

window.__LOG_MODAL  = true;   // logs from modal framework (showModal)
const __LOG_API = true;   // turns on authFetch + rates/hospitals/client POST/PATCH logging

// Default friendly labels (fallbacks if user hasn't set custom labels for a section)
const DEFAULT_COLUMN_LABELS = {
  candidates: {
    first_name: 'First Name',
    last_name: 'Last Name',
    email: 'Email',
    phone: 'Phone',
    postcode: 'Postcode',
    role: 'Role',
    tms_ref: 'TMS Ref',
    job_titles_display: 'Job Titles',
    rev: 'Revision'
  },
  clients: {
    name: 'Client Name',
    primary_invoice_email: 'Invoice Email',
    invoice_address: 'Invoice Address',
    postcode: 'Postcode',
    ap_phone: 'A/P Phone',
    cli_ref: 'Client Ref',
    rev: 'Revision'
  },
  contracts: {
    candidate_display: 'Candidate',
    client_name: 'Client',
    role: 'Role',
    band: 'Band',
    pay_method_snapshot: 'Pay Method',
    default_submission_mode: 'Submission Mode',
    start_date: 'Start',
    end_date: 'End',
    bucket_labels_preview: 'Buckets'
  },
  audit: {
    created_at_utc: 'Created (UTC)',
    last_error: 'Last Error'
  }
};


const GRID_COLUMN_META_DEFAULTS = {
  // Candidate list (drawn from candidates + any summary fields you render)
  candidates: {
    id:                     { selectable: false },
    tms_ref:                { selectable: true },
    first_name:             { selectable: true },
    last_name:              { selectable: true },
    display_name:           { selectable: true },
    email:                  { selectable: true },
    phone:                  { selectable: true },
    mobile:                 { selectable: true },
    address_line1:          { selectable: false },
    address_line2:          { selectable: false },
    address_line3:          { selectable: false },
    town_city:              { selectable: true },
    county:                 { selectable: false },
    postcode:               { selectable: true },
    country:                { selectable: false },
    ni_number:              { selectable: true },
    date_of_birth:          { selectable: true },
    gender:                 { selectable: true },
    nationality:            { selectable: true },
    right_to_work_status:   { selectable: false },
    right_to_work_expiry:   { selectable: false },
    pay_method:             { selectable: true },
    job_titles_display: { selectable: true },
    umbrella_id:            { selectable: false },
    bank_name:              { selectable: false },
    bank_account_name:      { selectable: true },
    bank_account_number:    { selectable: true },
    bank_sort_code:         { selectable: true },
    umbrella_name:          { selectable: true },
    umbrella_reference:     { selectable: false },
    notes:                  { selectable: true },
    status:                 { selectable: true },
    archived:               { selectable: true },
    active:                 { selectable: true },
    key_norm:               { selectable: false },
    created_at:             { selectable: true },
    updated_at:             { selectable: true },
    rev:                    { selectable: false },
    // derived / summary fields you show:
    roles_display:          { selectable: true },
    primary_role:           { selectable: true },
    bands_display:          { selectable: true },
    last_booking_date:      { selectable: true },
    next_booking_date:      { selectable: true }
  },

  // Clients (public.clients)
  clients: {
    id:                       { selectable: false },
    cli_ref:                  { selectable: true },
    name:                     { selectable: true },
    invoice_address:          { selectable: true },
    primary_invoice_email:    { selectable: true },
    ap_phone:                 { selectable: true },
    vat_chargeable:           { selectable: true },
    payment_terms_days:       { selectable: true },
    created_at:               { selectable: true },
    updated_at:               { selectable: true },
    mileage_charge_rate:      { selectable: true },
    ts_queries_email:         { selectable: true },
    rev:                      { selectable: false },
    // any extra client_settings snapshot fields you show in the grid:
    default_submission_mode:  { selectable: true },
    week_ending_weekday:      { selectable: true },
    pay_reference_required:   { selectable: true },
    invoice_reference_required:{ selectable: true },
    auto_invoice_default:     { selectable: true }
  },

  // Contracts (public.contracts + summary fields)
  contracts: {
    id:                           { selectable: false },
    tms_ref:                      { selectable: true },
    candidate_id:                 { selectable: false },
    client_id:                    { selectable: false },
    candidate_display:            { selectable: true }, // your summary label
    client_name:                  { selectable: true }, // joined from clients.name
    role:                         { selectable: true },
    band:                         { selectable: true },
    display_site:                 { selectable: true },
    ward_hint:                    { selectable: true },
    start_date:                   { selectable: true },
    end_date:                     { selectable: true },
    pay_method_snapshot:          { selectable: true },
    default_submission_mode:      { selectable: true },
    week_ending_weekday_snapshot: { selectable: true },
    auto_invoice:                 { selectable: true },
    require_reference_to_pay:     { selectable: true },
    require_reference_to_invoice: { selectable: true },
    status:                       { selectable: true }, // high-level status you derive (e.g. Active/Unassigned/Completed)
    status_detail:                { selectable: true }, // e.g. next action / next week
    // bucket labels
    bucket_labels_json:           { selectable: false },
    bucket_day:                   { selectable: false },
    bucket_night:                 { selectable: false },
    bucket_sat:                   { selectable: false },
    bucket_sun:                   { selectable: false },
    bucket_bh:                    { selectable: false },
    // convenience label columns you render:
    bucket_label_day:             { selectable: true },
    bucket_label_night:           { selectable: true },
    bucket_label_sat:             { selectable: true },
    bucket_label_sun:             { selectable: true },
    bucket_label_bh:              { selectable: true },
    // rates_json is complex; but you might expose individual buckets:
    paye_day:                     { selectable: true },
    paye_night:                   { selectable: true },
    paye_sat:                     { selectable: true },
    paye_sun:                     { selectable: true },
    paye_bh:                      { selectable: true },
    umb_day:                      { selectable: true },
    umb_night:                    { selectable: true },
    umb_sat:                      { selectable: true },
    umb_sun:                      { selectable: true },
    umb_bh:                       { selectable: true },
    charge_day:                   { selectable: true },
    charge_night:                 { selectable: true },
    charge_sat:                   { selectable: true },
    charge_sun:                   { selectable: true },
    charge_bh:                    { selectable: true },
    // mileage
    mileage_pay_rate:             { selectable: true },
    mileage_charge_rate:          { selectable: true },
    // schedule / hours if ever surfaced:
    std_schedule_json:            { selectable: false },
    std_hours_json:               { selectable: false },
    gh_mon:                       { selectable: false },
    gh_tue:                       { selectable: false },
    gh_wed:                       { selectable: false },
    gh_thu:                       { selectable: false },
    gh_fri:                       { selectable: false },
    gh_sat:                     { selectable: false },
    gh_sun:                       { selectable: false },
    // meta
    created_at:                   { selectable: true },
    updated_at:                   { selectable: true },
    rev:                         { selectable: false }
  },

  // Timesheets (likely from a view such as timesheets_hr_view or a join)
  timesheets: {
    id:                      { selectable: true },
    timesheet_id:            { selectable: true },   // if using a view
    booking_id:              { selectable: true },
    candidate_id:            { selectable: true },
    candidate_display:       { selectable: true },
    client_id:               { selectable: true },
    client_name:             { selectable: true },
    hospital:                { selectable: true },
    ward:                    { selectable: true },
    unit:                    { selectable: true },
    role_code:               { selectable: true },
    band:                    { selectable: true },
    start_utc:               { selectable: true },
    end_utc:                 { selectable: true },
    work_date:               { selectable: true },
    week_ending_date:        { selectable: true },
    submission_mode:         { selectable: true },
    authorised:              { selectable: true },
    authorised_at_utc:       { selectable: true },
    status:                  { selectable: true },   // high-level status string
    processing_status:       { selectable: true },   // ts_fin_processing_status_enum
    fin_basis:               { selectable: true },   // timesheet_fin_basis_enum
    pay_method:              { selectable: true },
    hours_day:               { selectable: true },
    hours_night:             { selectable: true },
    hours_sat:               { selectable: true },
    hours_sun:               { selectable: true },
    hours_bh:                { selectable: true },
    total_hours:             { selectable: true },
    pay_rate_day:            { selectable: true },
    pay_rate_night:          { selectable: true },
    pay_rate_sat:            { selectable: true },
    pay_rate_sun:            { selectable: true },
    pay_rate_bh:             { selectable: true },
    charge_rate_day:         { selectable: true },
    charge_rate_night:       { selectable: true },
    charge_rate_sat:         { selectable: true },
    charge_rate_sun:         { selectable: true },
    charge_rate_bh:          { selectable: true },
    pay_total:               { selectable: true },
    charge_total:            { selectable: true },
    margin_total:            { selectable: true },
    pay_on_hold:             { selectable: true },
    remittance_last_sent_at: { selectable: true },
    created_at:              { selectable: true },
    updated_at:              { selectable: true }
  },

  // Invoices (public.invoices)
  invoices: {
    id:                     { selectable: true },
    type:                   { selectable: true },
    invoice_no:             { selectable: true },
    client_id:              { selectable: true },
    client_name:            { selectable: true }, // joined from clients
    issued_at_utc:          { selectable: true },
    due_at_utc:             { selectable: true },
    paid_at_utc:            { selectable: true },
    status:                 { selectable: true },
    status_date_utc:        { selectable: true },
    currency:               { selectable: true },
    subtotal_ex_vat:        { selectable: true },
    vat_rate_pct:           { selectable: true },
    vat_amount:             { selectable: true },
    total_inc_vat:          { selectable: true },
    credit_note_total:      { selectable: true },
    balance_outstanding:    { selectable: true },
    on_hold:                { selectable: true },
    on_hold_reason:         { selectable: true },
    original_invoice_id:    { selectable: true },
    created_at:             { selectable: true },
    updated_at:             { selectable: true }
  },

  // Umbrellas (public.umbrellas)
  umbrellas: {
    id:             { selectable: false },
    name:           { selectable: true },
    email:          { selectable: true },
    phone:          { selectable: true },
    address_line1:  { selectable: false },
    address_line2:  { selectable: false },
    address_line3:  { selectable: false },
    town_city:      { selectable: false },
    county:         { selectable: false },
    postcode:       { selectable: false },
    country:        { selectable: false },
    bank_name:      { selectable: true },
    bank_account:   { selectable: true },
    bank_sort_code: { selectable: true },
    vat_number:     { selectable: true },
    company_number: { selectable: true },
    active:         { selectable: true },
    created_at:     { selectable: true },
    updated_at:     { selectable: true },
    rev:            { selectable: false }
  },

  // Audit / mail_outbox (public.mail_outbox)
  audit: {
    id:           { selectable: true },
    type:         { selectable: true },
    to:           { selectable: true },
    cc:           { selectable: true },
    subject:      { selectable: true },
    body_html:    { selectable: true },
    body_text:    { selectable: true },
    attachments:  { selectable: true },
    status:       { selectable: true },
    last_error:   { selectable: true },
    provider:     { selectable: true },
    provider_id:  { selectable: true },
    created_at:   { selectable: true },
    updated_at:   { selectable: true },
    sent_at:      { selectable: true },
    failed_at:    { selectable: true },
    attempts:     { selectable: true }
  }
};


// Quick DOM helper
const byId = (id)=>document.getElementById(id);

// ===== Session handling =====
function saveSession(sess){
  SESSION = sess;
  const persist = document.getElementById('rememberMe')?.checked ?? true;
  const store = persist ? localStorage : sessionStorage;
  store.setItem('cloudtms.session', JSON.stringify(sess));
  if (persist) sessionStorage.removeItem('cloudtms.session');
  scheduleRefresh();
  renderUserChip();
}
// FRONTEND — loadUserGridPrefs
async function loadUserGridPrefs(section) {
  // If already loaded and shaped correctly, just reuse
  if (window.__gridPrefs && typeof window.__gridPrefs === 'object') {
    if (!window.__gridPrefs.grid || typeof window.__gridPrefs.grid !== 'object') {
      window.__gridPrefs = { grid: {} };
    }
    if (!window.__gridPrefs.grid[section] || typeof window.__gridPrefs.grid[section] !== 'object') {
      window.__gridPrefs.grid[section] = {};
    }
    return window.__gridPrefs;
  }

  // GET once per app session from backend (which applies defaults if needed)
  let prefs;
  try {
    const res = await authFetch(API('/api/users/me/grid-prefs'));
    prefs = (await res.json()) || {};
  } catch (e) {
    console.error('[GRID] failed to load user grid prefs', e);
    prefs = {};
  }

  // Normalise to at least { grid: {} }
  if (!prefs || typeof prefs !== 'object') {
    prefs = { grid: {} };
  }
  if (!prefs.grid || typeof prefs.grid !== 'object') {
    prefs.grid = {};
  }
  // ensure object for this section
  if (!prefs.grid[section] || typeof prefs.grid[section] !== 'object') {
    prefs.grid[section] = {};
  }

  // One-time import from legacy localStorage, if present and no columns yet
  try {
    const legacyKey = 'cloudtms.cols.' + section;
    const legacy = localStorage.getItem(legacyKey);
    if (legacy && !prefs.grid[section].columns) {
      const cols = JSON.parse(legacy);
      if (Array.isArray(cols)) {
        const columns = {};
        cols.forEach((k, i) => {
          columns[k] = { visible: true, order: i };
        });
        prefs.grid[section].columns = columns;
        // Persist immediately so backend has them
        window.__gridPrefs = prefs;
        await saveUserGridPrefsDebounced(section, { columns }, true);
      }
      try { localStorage.removeItem(legacyKey); } catch (_) {}
    }
  } catch (e) {
    console.warn('[GRID] legacy grid prefs import failed', e);
  }

  window.__gridPrefs = prefs;
  return prefs;
}

const __saveTimers = new Map();
async function saveUserGridPrefsDebounced(section, partial, immediate = false) {
  // Ensure global shape
  if (!window.__gridPrefs || typeof window.__gridPrefs !== 'object') {
    window.__gridPrefs = { grid: {} };
  }
  if (!window.__gridPrefs.grid || typeof window.__gridPrefs.grid !== 'object') {
    window.__gridPrefs.grid = {};
  }

  const grid = window.__gridPrefs.grid;
  const existing = (grid[section] && typeof grid[section] === 'object') ? grid[section] : {};
  grid[section] = { ...existing, ...(partial || {}) };

  const key = `grid:${section}`;

  const fire = async () => {
   const body = {
  section,
  prefs: grid[section] || {}
};

    try {
      const res = await authFetch(API('/api/users/me/grid-prefs'), {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      const saved = await res.json().catch(() => null);
      // If server returned a full prefs object with .grid, adopt it as source of truth
      if (saved && typeof saved === 'object' && saved.grid && typeof saved.grid === 'object') {
        window.__gridPrefs = saved;
      }
    } catch (e) {
      console.error('Failed to save grid prefs', e);
    }
  };

  if (immediate) {
    return fire();
  }

  window.__saveTimers = window.__saveTimers || new Map();
  if (window.__saveTimers.has(key)) {
    clearTimeout(window.__saveTimers.get(key));
  }
  window.__saveTimers.set(key, setTimeout(fire, 300));
}

function getVisibleColumnsForSection(section, rows) {
  const defaults = (typeof defaultColumnKeysForSection === 'function')
    ? defaultColumnKeysForSection(section)
    : (typeof defaultColumnsFor === 'function'
        ? defaultColumnsFor(section)
        : []);

  const root = (window.__gridPrefs && window.__gridPrefs.grid) || {};
  const prefsRoot = (root[section] && typeof root[section] === 'object') ? root[section] : {};
  const colPrefs  = prefsRoot.columns      || {};
  const userMeta  = prefsRoot.columns_meta || {};

  const globalMeta =
    (typeof GRID_COLUMN_META_DEFAULTS === 'object' &&
     GRID_COLUMN_META_DEFAULTS[section]) || {};

  // Build catalog of known columns: defaults + first row's keys
  const known = new Set(defaults);
  if (Array.isArray(rows) && rows.length > 0 && rows[0] && typeof rows[0] === 'object') {
    Object.keys(rows[0]).forEach((k) => known.add(k));
  }

  const entries = Array.from(known).map((k) => {
    const p = colPrefs[k] || {};
    const meta = {
      ...(globalMeta[k] || {}),
      ...(userMeta[k]   || {})
    };

    const selectable = (meta.selectable !== false); // default true
    const visible    = (p.visible !== false);       // default true

    const order = (typeof p.order === 'number')
      ? p.order
      : (defaults.indexOf(k) >= 0 ? defaults.indexOf(k) : 9999);

    const width = (typeof p.width === 'number') ? p.width : null;

    return { key: k, selectable, visible, order, width };
  });

  // Only include columns that are globally/user-selectable AND marked visible
  const filtered = entries.filter((e) => e.selectable && e.visible);
  filtered.sort((a, b) => a.order - b.order);

  return filtered.map((e) => e.key);
}

function applyUserGridPrefs(section, tables, cols) {
  const root = (window.__gridPrefs && window.__gridPrefs.grid) || {};
  const prefsRoot = root[section] || {};
  const colPrefs = prefsRoot.columns || {};
  const MIN_W = 80, MAX_W = 600;

  const headTable = (tables && tables.head) ? tables.head : tables;
  const bodyTable = (tables && tables.body) ? tables.body : tables;

  const widthOf = (k) => {
    let w = colPrefs[k]?.width;
    if (typeof w !== 'number' || !(w > 0)) return null; // auto → we’ll measure below
    if (w < MIN_W) w = MIN_W;
    if (w > MAX_W) w = MAX_W;
    return w;
  };

  const setColWidthPx = (colKey, pxOrNull) => {
    if (headTable) {
      const th = headTable.querySelector(
        `thead th[data-col-key="${CSS.escape(colKey)}"]`
      );
      if (th) {
        th.style.width = (pxOrNull == null ? '' : `${pxOrNull}px`);
      }
    }

    if (bodyTable) {
      const tds = bodyTable.querySelectorAll(
        `tbody td[data-col-key="${CSS.escape(colKey)}"]`
      );
      tds.forEach(td => {
        td.style.width = (pxOrNull == null ? '' : `${pxOrNull}px`);
      });
    }
  };

  // 🔑 NEW: ensure we have a mutable columns prefs object so we can persist
  const ensureColsPrefs = () => {
    window.__gridPrefs = window.__gridPrefs || { grid: {} };
    window.__gridPrefs.grid = window.__gridPrefs.grid || {};
    const g = window.__gridPrefs.grid;
    g[section] = g[section] || {};
    g[section].columns = g[section].columns || {};
    return g[section].columns;
  };

  (cols || []).forEach((k) => {
    let w = widthOf(k);

    // If no saved width, measure the header cell’s current width
    if (w == null && headTable) {
      const th = headTable.querySelector(
        `thead th[data-col-key="${CSS.escape(k)}"]`
      );
      if (th) {
        const rect = th.getBoundingClientRect();
        w = Math.round(rect.width);
        if (w < MIN_W) w = MIN_W;
        if (w > MAX_W) w = MAX_W;

        // Persist this as the column’s width so future renders stay aligned
        const colsPrefs = ensureColsPrefs();
        colsPrefs[k] = { ...(colsPrefs[k] || {}), width: w };
        // Fire-and-forget; no need to await
        saveUserGridPrefsDebounced(section, { columns: colsPrefs });
      }
    }

    // Now apply the width (either from prefs or measured)
    if (w != null) {
      setColWidthPx(k, w);
    }
  });
}

function wireGridColumnResizing(section, tables) {
  const MIN_W = 80, MAX_W = 600;

  const headTable = (tables && tables.head) ? tables.head : tables;
  const bodyTable = (tables && tables.body) ? tables.body : tables;

  const ensureColsPrefs = () => {
    if (!window.__gridPrefs || typeof window.__gridPrefs !== 'object') {
      window.__gridPrefs = { grid: {} };
    }
    if (!window.__gridPrefs.grid || typeof window.__gridPrefs.grid !== 'object') {
      window.__gridPrefs.grid = {};
    }
    const g = window.__gridPrefs.grid;
    g[section] = g[section] || {};
    g[section].columns = g[section].columns || {};
    return g[section].columns;
  };

  let drag = null;

  const onMove = (ev) => {
    if (!drag) return;
    const dx = (ev.clientX || 0) - drag.startX;
    let w = Math.max(MIN_W, Math.min(MAX_W, drag.startW + dx));
    drag.th.style.width = `${w}px`;
    drag.cells.forEach(td => { td.style.width = `${w}px`; });
  };

  const onUp = () => {
    if (!drag) return;
    const th = drag.th;
    const key = th.dataset.colKey;
    const rect = th.getBoundingClientRect();
    const w = Math.max(MIN_W, Math.min(MAX_W, Math.round(rect.width)));
    const colsPrefs = ensureColsPrefs();
    colsPrefs[key] = { ...(colsPrefs[key] || {}), width: w };
    saveUserGridPrefsDebounced(section, { columns: colsPrefs });
    drag = null;
    document.removeEventListener('mousemove', onMove);
    document.removeEventListener('mouseup', onUp, true);
  };

  if (!headTable) return;

  headTable.querySelectorAll('thead th').forEach((th) => {
    const handle = th.querySelector('.col-resizer');
    if (!handle) return;

    handle.addEventListener('mousedown', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      const key = th.dataset.colKey;
      const cells = bodyTable
        ? Array.from(bodyTable.querySelectorAll(`tbody td[data-col-key="${CSS.escape(key)}"]`))
        : [];
      drag = {
        th,
        startX: ev.clientX || 0,
        startW: Math.round(th.getBoundingClientRect().width || MIN_W),
        cells
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp, true);
    });

    // Double-click handle resets width for this column to auto
    handle.addEventListener('dblclick', (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      const key = th.dataset.colKey;
      th.style.width = '';
      if (bodyTable) {
        bodyTable
          .querySelectorAll(`tbody td[data-col-key="${CSS.escape(key)}"]`)
          .forEach(td => { td.style.width = ''; });
      }
      const colsPrefs = ensureColsPrefs();
      if (colsPrefs[key]) delete colsPrefs[key].width;
      saveUserGridPrefsDebounced(section, { columns: colsPrefs });
    });
  });
}

// FRONTEND — wireGridColumnReorder
function wireGridColumnReorder(section, tables) {
  const headTable = (tables && tables.head) ? tables.head : tables;

  const ensureSectionPrefs = () => {
    if (!window.__gridPrefs || typeof window.__gridPrefs !== 'object') {
      window.__gridPrefs = { grid: {} };
    }
    if (!window.__gridPrefs.grid || typeof window.__gridPrefs.grid !== 'object') {
      window.__gridPrefs.grid = {};
    }
    const g = window.__gridPrefs.grid;
    g[section] = g[section] || {};
    g[section].columns = g[section].columns || {};
    return g[section];
  };

  if (!headTable) return;

  let dragKey = null;

  headTable.querySelectorAll('thead th[data-col-key]').forEach((th) => {
    if (!th.dataset.colKey) return;

    th.addEventListener('dragstart', (ev) => {
      dragKey = th.dataset.colKey;
      if (ev.dataTransfer) {
        ev.dataTransfer.setData('text/plain', dragKey);
        ev.dataTransfer.effectAllowed = 'move';
      }
    });

    th.addEventListener('dragover', (ev) => {
      ev.preventDefault();
      if (ev.dataTransfer) {
        ev.dataTransfer.dropEffect = 'move';
      }
    });

    th.addEventListener('drop', async (ev) => {
      ev.preventDefault();
      const targetKey = th.dataset.colKey;
      if (!dragKey || dragKey === targetKey) return;

      const headers = Array.from(headTable.querySelectorAll('thead th[data-col-key]'));
      const keys = headers.map((h) => h.dataset.colKey);

      const from = keys.indexOf(dragKey);
      const to   = keys.indexOf(targetKey);
      if (from < 0 || to < 0) return;

      // Move dragKey to the index of targetKey
      keys.splice(to, 0, keys.splice(from, 1)[0]);

      const secPrefs = ensureSectionPrefs();
      const colPrefs = { ...(secPrefs.columns || {}) };

      keys.forEach((k, i) => {
        colPrefs[k] = { ...(colPrefs[k] || {}), order: i };
      });

      await saveUserGridPrefsDebounced(section, { columns: colPrefs }, true);

      const data = await loadSection();
      renderSummary(data);
    });
  });
}

async function restoreGridPrefsToDefault(section) {
  try {
    const res = await authFetch(API('/api/users/me/grid-prefs'), {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ section, reset: true })
    });
    const prefs = await res.json().catch(() => null);
    if (prefs && typeof prefs === 'object' && prefs.grid) {
      window.__gridPrefs = prefs;
    } else {
      window.__gridPrefs = prefs || { grid: {} };
    }

    await loadUserGridPrefs(section);
    const data = await loadSection();
    renderSummary(data);
  } catch (e) {
    console.error('[GRID] restoreGridPrefsToDefault failed', e);
  }
}
// FRONTEND — attachHeaderContextMenu
function attachHeaderContextMenu(section, tables) {
  const headTable = (tables && tables.head) ? tables.head : tables;
  const bodyTable = (tables && tables.body) ? tables.body : tables;

  let menu = document.createElement('div');
  menu.style.cssText =
    'position:fixed;z-index:10000;background:#0b1528;border:1px solid var(--line);' +
    'padding:6px;border-radius:8px;display:none;min-width:220px;';
  document.body.appendChild(menu);

  const hide = () => { menu.style.display = 'none'; };
  document.addEventListener('click', hide);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') hide(); });

  const mkItem = (label, cb) => {
    const it = document.createElement('div');
    it.textContent = label;
    it.style.cssText = 'padding:6px 10px;cursor:pointer;';
    it.addEventListener('click', () => { hide(); cb && cb(); });
    it.addEventListener('mouseover', () => { it.style.background = '#101c36'; });
    it.addEventListener('mouseout',  () => { it.style.background = 'transparent'; });
    return it;
  };

  const ensureSectionPrefs = () => {
    if (!window.__gridPrefs || typeof window.__gridPrefs !== 'object') {
      window.__gridPrefs = { grid: {} };
    }
    if (!window.__gridPrefs.grid || typeof window.__gridPrefs.grid !== 'object') {
      window.__gridPrefs.grid = {};
    }
    const g = window.__gridPrefs.grid;
    g[section] = g[section] || {};
    g[section].columns = g[section].columns || {};
    return g[section];
  };

  const resetAllWidths = () => {
    const sec = ensureSectionPrefs();
    const cols = { ...(sec.columns || {}) };
    Object.keys(cols).forEach((k) => {
      if ('width' in cols[k]) delete cols[k].width;
    });

    if (headTable) {
      headTable.querySelectorAll('thead th[data-col-key]').forEach((th) => {
        th.style.width = '';
      });
    }
    if (bodyTable) {
      bodyTable.querySelectorAll('tbody td[data-col-key]').forEach((td) => {
        td.style.width = '';
      });
    }

    saveUserGridPrefsDebounced(section, { columns: cols }, true);
  };

  const autoWidthThisColumn = (colKey) => {
    if (!headTable) return;
    const th = headTable.querySelector(`thead th[data-col-key="${CSS.escape(colKey)}"]`);
    if (!th) return;

    const cells = bodyTable
      ? bodyTable.querySelectorAll(`tbody td[data-col-key="${CSS.escape(colKey)}"]`)
      : [];

    const measure = (el) => Math.ceil(el.scrollWidth) + 16;
    let maxW = measure(th);
    cells.forEach((td) => { maxW = Math.max(maxW, measure(td)); });

    const w = Math.max(80, Math.min(600, maxW));
    th.style.width = `${w}px`;
    cells.forEach((td) => { td.style.width = `${w}px`; });

    const sec = ensureSectionPrefs();
    const cols = { ...(sec.columns || {}) };
    cols[colKey] = { ...(cols[colKey] || {}), width: w };
    saveUserGridPrefsDebounced(section, { columns: cols }, true);
  };

  if (!headTable) return;

  headTable.addEventListener('contextmenu', (ev) => {
    const th = ev.target && ev.target.closest('th');
    if (!th || !th.dataset || !th.dataset.colKey) return;
    ev.preventDefault();

    const colKey = th.dataset.colKey;
    menu.innerHTML = '';

    // Restore full layout for this section from backend defaults
    menu.appendChild(
      mkItem('Restore layout to default', () => {
        restoreGridPrefsToDefault(section);
      })
    );

    // Reset only widths for this section
    menu.appendChild(
      mkItem('Reset View (Auto widths)', () => resetAllWidths())
    );

    const hr = document.createElement('hr');
    hr.style.border = '1px solid var(' + '--line' + ')';
    menu.appendChild(hr);

    menu.appendChild(
      mkItem('Auto-size this column', () => autoWidthThisColumn(colKey))
    );

    menu.appendChild(
      mkItem('Reset this column width', () => {
        th.style.width = '';
        if (bodyTable) {
          bodyTable
            .querySelectorAll(`tbody td[data-col-key="${CSS.escape(colKey)}"]`)
            .forEach(td => { td.style.width = ''; });
        }

        const sec = ensureSectionPrefs();
        const cols = { ...(sec.columns || {}) };
        if (cols[colKey]) delete cols[colKey].width;
        saveUserGridPrefsDebounced(section, { columns: cols }, true);
      })
    );

    menu.appendChild(
      mkItem('Hide column', async () => {
        const sec = ensureSectionPrefs();
        const cols = { ...(sec.columns || {}) };
        cols[colKey] = { ...(cols[colKey] || {}), visible: false };
        await saveUserGridPrefsDebounced(section, { columns: cols }, true);
        const data = await loadSection();
        renderSummary(data);
      })
    );

    menu.appendChild(
      mkItem('Columns…', () => openColumnsDialog(section))
    );

    // Position the context menu
    menu.style.left = `${ev.clientX}px`;
    menu.style.top  = `${ev.clientY}px`;
    menu.style.display = 'block';
  });
}

function openColumnsDialog(section) {
  const rootPrefs =
    (window.__gridPrefs &&
     window.__gridPrefs.grid &&
     window.__gridPrefs.grid[section]) || {};

  const colPrefs  = rootPrefs.columns      || {};
  const userMeta  = rootPrefs.columns_meta || {};
  const globalMeta = (typeof GRID_COLUMN_META_DEFAULTS === 'object' && GRID_COLUMN_META_DEFAULTS[section]) || {};

  const mergeMetaFor = (key) => ({
    ...(globalMeta[key] || {}),
    ...(userMeta[key]   || {})
  });

  const useFriendly = (rootPrefs.use_friendly_labels !== false);
  const labels      = rootPrefs.labels || {};

  // Build master key list: visible columns, defaults, current row keys
  const known = new Set(
    getVisibleColumnsForSection(section, currentRows).concat(defaultColumnsFor(section))
  );
  if (Array.isArray(currentRows) && currentRows[0]) {
    Object.keys(currentRows[0]).forEach(k => known.add(k));
  }

  // Filter out columns that are globally/user marked selectable:false
  const list = Array.from(known).filter(k => {
    const meta = mergeMetaFor(k);
    return meta.selectable !== false;
  });

  const overlay = document.createElement('div');
  overlay.style.cssText =
    'position:fixed;inset:0;background:rgba(0,0,0,0.5);z-index:10000;' +
    'display:flex;align-items:center;justify-content:center;';
  const modal = document.createElement('div');
  modal.style.cssText =
    'background:#0b152a;border:1px solid var(--line);border-radius:10px;' +
    'min-width:480px;max-width:80vw;max-height:80vh;overflow:auto;padding:14px;';
  overlay.appendChild(modal);

  const title = document.createElement('div');
  title.textContent = `Columns — ${section}`;
  title.style.cssText = 'font-weight:600;margin-bottom:10px;';
  modal.appendChild(title);

  // Friendly labels toggle
  const lblWrap = document.createElement('label');
  const lblCb = document.createElement('input');
  lblCb.type = 'checkbox';
  lblCb.checked = useFriendly;
  lblWrap.appendChild(lblCb);
  lblWrap.appendChild(document.createTextNode(' Use friendly header labels'));
  lblWrap.style.cssText = 'display:flex;align-items:center;gap:8px;margin-bottom:8px;font-size:12px;';
  modal.appendChild(lblWrap);

  lblCb.addEventListener('change', () => {
    saveUserGridPrefsDebounced(section, { use_friendly_labels: !!lblCb.checked });
  });

  // Table: Visible + Column key + Display name + Order
  const t = document.createElement('table');
  t.style.cssText = 'width:100%;border-collapse:collapse;font-size:12px;';
  t.innerHTML = `
    <thead>
      <tr>
        <th style="text-align:left;padding:6px;border-bottom:1px solid var(--line)">Visible</th>
        <th style="text-align:left;padding:6px;border-bottom:1px solid var(--line)">Column key</th>
        <th style="text-align:left;padding:6px;border-bottom:1px solid var(--line)">Display name</th>
        <th style="text-align:left;padding:6px;border-bottom:1px solid var(--line)">Order</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;
  modal.appendChild(t);
  const tb = t.querySelector('tbody');

  const orderOf = (k) => {
    const cp = colPrefs[k];
    if (cp && typeof cp.order === 'number') return cp.order;
    const idx = defaultColumnsFor(section).indexOf(k);
    return (idx >= 0 ? idx : 9999);
  };

  const rowsModel = list
    .map(k => ({
      key: k,
      visible: (colPrefs[k]?.visible !== false),
      label: labels[k] || k,
      order: orderOf(k)
    }))
    .sort((a, b) => a.order - b.order);

  const persist = () => {
    const columns   = {};
    const labelsOut = {};
    const metaOut   = { ...(rootPrefs.columns_meta || {}) }; // carry existing meta

    rowsModel.forEach((r, idx) => {
      columns[r.key] = { visible: !!r.visible, order: r.order };
      labelsOut[r.key] = String(r.label || r.key);
    });

    saveUserGridPrefsDebounced(section, {
      columns,
      labels: labelsOut,
      columns_meta: metaOut,
      use_friendly_labels: !!lblCb.checked
    });
  };

  const reindex = () => {
    rowsModel.forEach((r, i) => { r.order = i; });
    persist();
    refresh();
  };

  const refresh = () => {
    tb.innerHTML = '';
    rowsModel.sort((a, b) => a.order - b.order).forEach(r => {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td style="padding:6px"><input type="checkbox" ${r.visible ? 'checked' : ''}></td>
        <td style="padding:6px;font-family:monospace">${r.key}</td>
        <td style="padding:6px"><input type="text" style="width:100%;background:#000;border:1px solid var(--line);color:var(--text);border-radius:6px;padding:4px 8px;font-size:12px;"></td>
        <td style="padding:6px;white-space:nowrap">
          <button class="btn mini" data-move="up">▲</button>
          <button class="btn mini" data-move="down">▼</button>
        </td>
      `;

      const elVisible = tr.querySelector('td:nth-child(1) input');
      const elLabel   = tr.querySelector('td:nth-child(3) input');
      const btnUp     = tr.querySelector('button[data-move="up"]');
      const btnDown   = tr.querySelector('button[data-move="down"]');

      elLabel.value = r.label;

      elVisible.addEventListener('change', () => {
        r.visible = !!elVisible.checked;
        persist();
      });

      elLabel.addEventListener('change', () => {
        r.label = elLabel.value;
        persist();
      });

      btnUp.addEventListener('click', () => {
        const i = rowsModel.indexOf(r);
        if (i > 0) {
          [rowsModel[i - 1], rowsModel[i]] = [rowsModel[i], rowsModel[i - 1]];
          reindex();
        }
      });

      btnDown.addEventListener('click', () => {
        const i = rowsModel.indexOf(r);
        if (i >= 0 && i < rowsModel.length - 1) {
          [rowsModel[i + 1], rowsModel[i]] = [rowsModel[i], rowsModel[i - 1]];
          reindex();
        }
      });

      tb.appendChild(tr);
    });
  };

  refresh();

  // Footer: Reset widths + Close
  const footer = document.createElement('div');
  footer.style.cssText = 'display:flex;justify-content:space-between;gap:8px;margin-top:10px;';
  const left  = document.createElement('div');
  const right = document.createElement('div');
  footer.appendChild(left);
  footer.appendChild(right);

  const btnResetWidths = document.createElement('button');
  btnResetWidths.textContent = 'Reset all widths';
  btnResetWidths.className = 'btn mini';
  btnResetWidths.addEventListener('click', () => {
    const prefsRoot =
      (window.__gridPrefs &&
       window.__gridPrefs.grid &&
       window.__gridPrefs.grid[section]) || {};

    const cols = { ...(prefsRoot.columns || {}) };
    Object.keys(cols).forEach(k => { if ('width' in cols[k]) delete cols[k].width; });
    saveUserGridPrefsDebounced(section, { columns: cols });
  });
  left.appendChild(btnResetWidths);

  const btnClose = document.createElement('button');
  btnClose.textContent = 'Close';
  btnClose.className = 'btn mini';
  btnClose.addEventListener('click', async () => {
    document.body.removeChild(overlay);
    const data = await loadSection();
    renderSummary(data);
  });
  right.appendChild(btnClose);

  modal.appendChild(footer);
  document.body.appendChild(overlay);
}


function loadSession(){
  try {
    const raw = localStorage.getItem('cloudtms.session') || sessionStorage.getItem('cloudtms.session');
    const sess = raw ? JSON.parse(raw) : null;
    if (!sess || !sess.accessToken) return false;
    saveSession(sess);             // mirrors globals & schedules refresh
    return true;
  } catch { return false; }
}


// Get label honoring section prefs toggle and overrides
function getFriendlyHeaderLabel(section, key) {
  const prefs = (window.__gridPrefs && window.__gridPrefs.grid && window.__gridPrefs.grid[section]) || {};
  const useFriendly = prefs.use_friendly_labels !== false; // default ON
  if (!useFriendly) return key;
  const overrides = prefs.labels || {};
  const def = (DEFAULT_COLUMN_LABELS[section] || {});
  return overrides[key] || def[key] || key;
}


// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: loadSection()
// - After loading the visible page, triggers background priming of membership
//   (ALL matching ids for current filters) regardless of page size.
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: loadSection()
// - Ensures grid prefs are loaded
// - Defaults Contracts status quick filter to "active"
// - After loading the visible page, triggers background priming of membership
//   (ALL matching ids for current filters) regardless of page size.
// ─────────────────────────────────────────────────────────────────────────────
async function loadSection() {
  window.__listState = window.__listState || {};
  const st = (window.__listState[currentSection] ||= {
    page: 1,
    pageSize: 50,
    total: null,
    hasMore: false,
    filters: null,
    sort: { key: null, dir: 'asc' }
  });

  // Ensure sort object exists (for older sessions where it wasn't seeded)
  if (!st.sort || typeof st.sort !== 'object') {
    st.sort = { key: null, dir: 'asc' };
  }

  // Default Contracts quick-filter to "active" if nothing specified
  if (currentSection === 'contracts') {
    if (!st.filters || typeof st.filters !== 'object') st.filters = {};
    if (!('status' in st.filters) || !st.filters.status) {
      st.filters.status = 'active';
    }
  }

  // Ensure user grid prefs are loaded once per session (per section).
  // These prefs come from tms_users.grid_prefs_json if present, else
  // fall back to DEFAULT_GRID_PREFS on the backend.
  await loadUserGridPrefs(currentSection);

  // Decide whether to use the search endpoints:
  // - ALWAYS for 'candidates' and 'contracts' (so we hit candidates_summary /
  //   contracts search and get all derived fields, including job_titles_display)
  // - For other sections, only if there are filters or an active sort key.
  const hasFilters = !!st.filters && Object.keys(st.filters).length > 0;
  const hasSort    = !!(st.sort && st.sort.key);

  const useSearch =
    (currentSection === 'candidates' || currentSection === 'contracts')
      ? true
      : (hasFilters || hasSort);

  const fetchOne = async (section, page, pageSize) => {
    window.__listState[section].page = page;
    window.__listState[section].pageSize = pageSize;

    if (useSearch) {
      // For candidates + contracts this always runs, so candidates come from
      // /api/search/candidates (candidates_summary) and include job_titles_display.
      return await search(section, window.__listState[section].filters || {});
    } else {
      // Legacy/simple list endpoints for non-search sections
      switch (section) {
        case 'candidates': return await listCandidates();   // used only when some other caller asks loadSection with useSearch=false for candidates (not the normal grid path now)
        case 'clients':    return await listClients();
        case 'umbrellas':  return await listUmbrellas();
        case 'settings':   return await getSettings();
        case 'audit':      return await listOutbox();
        default:           return [];
      }
    }
  };

  // PageSize = ALL → fetch all pages sequentially (respecting filters + sort)
  if (st.pageSize === 'ALL') {
    const acc = [];
    let p = 1;
    const chunk = 200;
    let gotMore = true;
    while (gotMore) {
      const rows = await fetchOne(currentSection, p, chunk);
      acc.push(...(rows || []));
      gotMore = Array.isArray(rows) && rows.length === chunk;
      p += 1;
      if (!gotMore) break;
    }
    window.__listState[currentSection].page = 1;
    window.__listState[currentSection].hasMore = false;
    window.__listState[currentSection].total = acc.length;

    try {
      primeSummaryMembership(currentSection, getSummaryFingerprint(currentSection));
    } catch {}
    return acc;
  }

  // Normal paged case
  const page = Number(st.page || 1);
  const ps   = Number(st.pageSize || 50);
  const rows = await fetchOne(currentSection, page, ps);
  const hasMore = Array.isArray(rows) && rows.length === ps;
  window.__listState[currentSection].hasMore = hasMore;

  try {
    primeSummaryMembership(currentSection, getSummaryFingerprint(currentSection));
  } catch {}
  return rows;
}

function clearSession(){
  localStorage.removeItem('cloudtms.session');
  sessionStorage.removeItem('cloudtms.session');
  SESSION = null; renderUserChip();
}
function scheduleRefresh(){
  clearTimeout(refreshTimer);
  if (!SESSION?.exp) return;
  const ms = Math.max(15_000, (SESSION.exp*1000) - Date.now() - 60_000);
  refreshTimer = setTimeout(refreshToken, ms);
}

// Unwrap list/envelope responses into arrays
async function toList(res) {
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  const j = await res.json();
  if (Array.isArray(j)) return j;
  if (Array.isArray(j.items)) return j.items;
  if (Array.isArray(j.rows))  return j.rows;
  if (j.data && Array.isArray(j.data)) return j.data;
  return [];
}

// --- helpers for normalising/time validation ---
function _toHHMM(val) {
  if (val == null) return '';
  const s = String(val).trim();
  if (!s) return '';
  // accept HH:MM, HH:MM:SS, H:MM, HHMM
  const m = s.match(/^(\d{1,2}):?(\d{2})(?::(\d{2}))?$/);
  if (!m) return '';
  let hh = Number(m[1]), mm = Number(m[2]);
  if (!Number.isFinite(hh) || !Number.isFinite(mm) || hh < 0 || hh > 23 || mm < 0 || mm > 59) return '';
  return String(hh).padStart(2,'0') + ':' + String(mm).padStart(2,'0');
}

function _toHHMMSS(val) {
  const hm = _toHHMM(val);
  return hm ? hm + ':00' : null; // server likes HH:MM:SS (supabase time)
}

// Build a clean object to send to the API

function normalizeClientSettingsForSave(raw) {
  const src = raw || {};
  const out = {};
  // Only normalise keys that were actually provided (form/baseline),
  // and only flag invalid when a provided key fails HH:MM
  let invalid = false;

  // Timezone + day/night times
  ['day_start','day_end','night_start','night_end','timezone_id'].forEach(k => {
    if (!(k in src)) return;            // not provided → ignore
    const v = src[k];
    if (k === 'timezone_id') { if (v) out[k] = String(v); return; }
    if (v == null || v === '') return;  // empty → ignore (do not mark invalid)
    const hhmmss = _toHHMMSS(v);        // returns null if fails
    if (hhmmss === null) { invalid = true; return; }
    out[k] = hhmmss;
  });

  // NEW: gates + default submission mode
  if ('pay_reference_required' in src) {
    out.pay_reference_required = !!(src.pay_reference_required === true || src.pay_reference_required === 'true' || src.pay_reference_required === 'on' || src.pay_reference_required === 1 || src.pay_reference_required === '1');
  }
  if ('invoice_reference_required' in src) {
    out.invoice_reference_required = !!(src.invoice_reference_required === true || src.invoice_reference_required === 'true' || src.invoice_reference_required === 'on' || src.invoice_reference_required === 1 || src.invoice_reference_required === '1');
  }
  if ('default_submission_mode' in src) {
    const mode = String(src.default_submission_mode || '').toUpperCase();
    if (mode === 'ELECTRONIC' || mode === 'MANUAL') out.default_submission_mode = mode;
    else out.default_submission_mode = 'ELECTRONIC';
  }

  return { cleaned: out, invalid };
}

// ===== Auth fetch with refresh retry =====
async function authFetch(input, init={}){
  const APILOG = (typeof window !== 'undefined' && !!window.__LOG_API) || (typeof __LOG_API !== 'undefined' && !!__LOG_API);
  const headers = new Headers(init.headers || {});
  if (SESSION?.accessToken) headers.set('Authorization', `Bearer ${SESSION.accessToken}`);
  if (APILOG) {
    const safeHeaders = {};
    headers.forEach((v,k)=>{ safeHeaders[k] = (k.toLowerCase()==='authorization') ? '***' : v; });
    const bodyPreview = typeof init.body === 'string' ? (init.body.length > 500 ? init.body.slice(0,500)+'…' : init.body) : init.body;
    console.log('[authFetch] →', { url: typeof input==='string'?input:input?.url, method: (init.method||'GET'), headers: safeHeaders, body: bodyPreview });
  }
  let res = await fetch(input, { ...init, headers, credentials: init.credentials || 'omit' });
  if (APILOG) {
    try { const txt = await res.clone().text(); console.log('[authFetch] ←', res.status, res.ok, txt.slice(0,500)); } catch {}
  }
  if (res.status === 401) {
    const ok = await refreshToken();
    if (!ok) throw new Error('Unauthorised');
    headers.set('Authorization', `Bearer ${SESSION.accessToken}`);
    if (APILOG) console.log('[authFetch] retrying after 401');
    res = await fetch(input, { ...init, headers, credentials: init.credentials || 'omit' });
    if (APILOG) {
      try { const txt2 = await res.clone().text(); console.log('[authFetch] ← (retry)', res.status, res.ok, txt2.slice(0,500)); } catch {}
    }
  }
  return res;
}

// ===== Auth API calls =====
async function apiLogin(email, password){
  const res = await fetch(API('/auth/login'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    credentials: 'include', // cookie for refresh
    body: JSON.stringify({ email, password })
  });

  let data = {};
  try { data = await res.json(); } catch {}

  if (!res.ok) {
    const msg = data?.error || data?.message || 'Invalid credentials';
    throw new Error(msg);
  }

  const token =
    data.access_token ||
    data.token ||
    data.accessToken;

  if (!token) {
    throw new Error('No access token returned');
  }

  const rawTtl = data.expires_in ?? data.token_ttl_sec ?? data.ttl ?? 3600; // seconds
  const ttl    = Math.max(60, Number(rawTtl) || 3600); // floor at 60s
  const skew   = 30; // renew slightly early

  saveSession({
    accessToken: token,
    user: data.user || data.profile || null,
    exp: Math.floor(Date.now() / 1000) + (ttl - skew)
  });

  if (typeof scheduleRefresh === 'function') {
    scheduleRefresh();
  }

  return data;
}

// single, de-duplicated definition
async function refreshToken(){
  try{
    const res = await fetch(API('/auth/refresh'), {
      method:'POST',
      credentials:'include',
      headers:{'content-type':'application/json'},
      body: JSON.stringify({})
    });
    if (!res.ok) { clearSession(); return false; }

    const data  = await res.json();
    const token = data.access_token || data.token || data.accessToken;
    const ttl   = data.expires_in || data.token_ttl_sec || data.ttl || 3600;

    // Preserve existing user; hydrate if missing id
    let user = SESSION?.user || data.user || null;
    if (!user || !user.id) {
      try {
        const meRes = await fetch(API('/api/me'), { headers: { 'Authorization': `Bearer ${token}` } });
        if (meRes.ok) {
          const meJson = await meRes.json().catch(()=> ({}));
          user = (meJson && (meJson.user || meJson)) || user;
        }
      } catch {}
      // Extra guard: fall back to persisted user if present
      if (!user || !user.id) {
        try {
          const persisted = JSON.parse(localStorage.getItem('cloudtms.session')
                           || sessionStorage.getItem('cloudtms.session') || 'null');
          if (persisted?.user?.id) user = persisted.user;
        } catch {}
      }
    }

    saveSession({
      accessToken: token,
      user,
      exp: Math.floor(Date.now()/1000) + ttl
    });
    return true;
  }catch{
    clearSession();
    return false;
  }
}



async function apiForgot(email){
  const r = await fetch(API('/auth/forgot'), { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ email })});
  if(!r.ok) throw new Error('Failed to request reset');
  return true;
}
async function apiReset(token, newPassword){
  const r = await fetch(API('/auth/reset'), { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ token, new_password: newPassword })});
  if(!r.ok) throw new Error('Failed to reset password');
  return true;
}

// ===== Auth overlays wiring (login / forgot / reset) =====
function openLogin(){ byId('loginOverlay').style.display='grid'; byId('forgotOverlay').style.display='none'; byId('resetOverlay').style.display='none'; }
function openForgot(){ byId('loginOverlay').style.display='none'; byId('forgotOverlay').style.display='grid'; byId('resetOverlay').style.display='none'; }
function openReset(){ byId('loginOverlay').style.display='none'; byId('forgotOverlay').style.display='none'; byId('resetOverlay').style.display='grid'; }

// show/hide password helpers
function toggleVis(inputId, toggleId){
  const inp = byId(inputId), t = byId(toggleId);
  t.onclick = ()=>{ inp.type = inp.type==='password' ? 'text' : 'password'; };
}

function renderUserChip(){
  const chip = byId('userChip');
  if (SESSION?.user) {
    chip.textContent = (SESSION.user.display_name || SESSION.user.email || 'User');
  } else chip.textContent = 'Signed out';
}
function initAuthUI(){
  // Buttons and links
  byId('btnLogout').onclick = ()=>{ clearSession(); openLogin(); };

  toggleVis('loginPassword','toggleLoginPw');
  toggleVis('resetPw1','toggleResetPw1'); toggleVis('resetPw2','toggleResetPw2');

  byId('linkForgot').onclick = openForgot;
  byId('linkBackToLogin').onclick = openLogin;
  byId('linkResetToLogin').onclick = openLogin;

  byId('loginForm').onsubmit = async (e)=>{
    e.preventDefault();
    const email = byId('loginEmail').value.trim();
    const pw = byId('loginPassword').value;
    const err = byId('loginError'); err.style.display='none';
    try{
      await apiLogin(email, pw);
      if (typeof scheduleRefresh === 'function') scheduleRefresh();
      byId('loginOverlay').style.display='none';
      bootstrapApp();
    }catch(ex){
      err.textContent = ex.message || 'Sign in failed'; err.style.display='block';
    }
  };

  byId('forgotForm').onsubmit = async (e)=>{
    e.preventDefault();
    const email = byId('forgotEmail').value.trim();
    byId('forgotError').style.display='none';
    byId('forgotMsg').style.display='none';
    try{
      await apiForgot(email);
      byId('forgotMsg').textContent = 'If that email exists, a reset link has been sent.'; byId('forgotMsg').style.display='block';
    }catch(ex){
      byId('forgotError').textContent = ex.message || 'Could not send reset email'; byId('forgotError').style.display='block';
    }
  };

  byId('resetForm').onsubmit = async (e)=>{
    e.preventDefault();
    const p1 = byId('resetPw1').value, p2 = byId('resetPw2').value;
    const err = byId('resetError'), ok = byId('resetMsg'); err.style.display='none'; ok.style.display='none';
    if (p1.length < 8) { err.textContent='Use at least 8 characters'; err.style.display='block'; return; }
    if (p1 !== p2) { err.textContent='Passwords do not match'; err.style.display='block'; return; }
    const url = new URL(location.href); const token = url.searchParams.get('k') || url.searchParams.get('token');
    if (!token){ err.textContent='Reset token missing. Use the email link again.'; err.style.display='block'; return; }
    try{
      await apiReset(token, p1);
      ok.textContent='Password updated. You can sign in now.'; ok.style.display='block';
    }catch(ex){
      err.textContent = ex.message || 'Reset failed'; err.style.display='block';
    }
  };

  // Open reset overlay automatically if URL carries a token (only if not already signed in)
  const url = new URL(location.href);
  const hasResetToken = url.searchParams.get('k') || url.searchParams.get('token');
  if (hasResetToken && !(typeof getSession === 'function' && getSession()?.accessToken)) openReset();
}
// ===== App state + rendering =====
const sections = [
  {key:'candidates', label:'Candidates', icon:'👤'},
  {key:'clients', label:'Clients', icon:'🏥'},
  {key:'contracts', label:'Contracts', icon:'📄'},
  {key:'timesheets', label:'Timesheets', icon:'🗒️'},
  {key:'healthroster', label:'Healthroster', icon:'📅'},
  {key:'invoices', label:'Invoices', icon:'📄'},
  {key:'umbrellas', label:'Umbrellas', icon:'☂️'},
  {key:'settings', label:'Settings', icon:'⚙️'},
  {key:'audit', label:'Audit', icon:'🛡️'}
];

let currentSection = 'candidates';
let currentRows = [];
let currentSelection = null;

// =========================== renderTopNav (kept with reset) ===========================
// ──────────────────────────────────────────────────────────────────────────────
// renderTopNav (amended) — adds Contracts quick-search branch { q: text }
// ──────────────────────────────────────────────────────────────────────────────
function renderTopNav(){
  const nav = byId('nav'); nav.innerHTML = '';

  // ensure per-section list + selection exist
  window.__listState = window.__listState || {};
  window.__selection = window.__selection || {};

  // helper: common section switch (mirrors original behaviour)
  const switchToSection = (sectionKey) => {
    if (!confirmDiscardChangesIfDirty()) return;

    if ((window.__modalStack?.length || 0) > 0 || modalCtx?.entity) {
      discardAllModalsAndState();
    }

    if (!window.__listState[sectionKey]) {
      window.__listState[sectionKey] = { page: 1, pageSize: 50, total: null, hasMore: false, filters: null };
    }

    // IDs-only selection seed for the new section
    window.__selection[sectionKey] = window.__selection[sectionKey] || { fingerprint:'', ids:new Set() };

    currentSection   = sectionKey;
    currentRows      = [];
    currentSelection = null;

    renderAll();
  };

  // Helper: close any open settings dropdown
  const closeSettingsMenu = () => {
    const m = document.getElementById('__settingsMenu');
    if (m) m.remove();
    const btn = document.querySelector('button.__settingsBtn');
    if (btn) btn.classList.remove('active');
    document.removeEventListener('click', onAnyDocClick, true);
    document.removeEventListener('keydown', onEsc, true);
  };
  const onAnyDocClick = (e) => {
    const menu = document.getElementById('__settingsMenu');
    const anchor = document.querySelector('button.__settingsBtn');
    if (!menu) return;
    if (menu.contains(e.target) || anchor?.contains(e.target)) return;
    closeSettingsMenu();
  };
  const onEsc = (e) => { if (e.key === 'Escape') closeSettingsMenu(); };

  // Build buttons
  sections.forEach(s => {
    const b = document.createElement('button');
    b.innerHTML = `<span class="ico">${s.icon}</span> ${s.label}`;
    if (s.key === currentSection) b.classList.add('active');

    if (s.key !== 'settings') {
      // normal buttons keep the original click behaviour
      b.onclick = () => switchToSection(s.key);
    } else {
      // SETTINGS: small dropdown instead of direct navigation
      b.classList.add('__settingsBtn');
      b.onclick = (ev) => {
        ev.preventDefault();
        // toggle
        const existing = document.getElementById('__settingsMenu');
        if (existing) { closeSettingsMenu(); return; }

        // Build a light menu styled with your palette
        const m = document.createElement('div');
        m.id = '__settingsMenu';
        m.style.position      = 'absolute';
        m.style.zIndex        = '1000';
        m.style.background    = 'var(--panel, #0b1221)';
        m.style.border        = '1px solid var(--line, #334155)';
        m.style.borderRadius  = '10px';
        m.style.boxShadow     = 'var(--shadow, 0 6px 20px rgba(0,0,0,.25))';
        m.style.padding       = '6px';
        m.style.minWidth      = '180px';
        m.style.userSelect    = 'none';

        m.innerHTML = `
          <button type="button" class="menu-item" data-k="global"
                  style="display:flex;gap:8px;align-items:center;width:100%;
                         background:#0b1427;border:1px solid var(--line);color:#fff;
                         padding:8px 10px;border-radius:8px;cursor:pointer;margin:4px 0;">
            🌐 Global settings
          </button>
          <button type="button" class="menu-item" data-k="rates"
                  style="display:flex;gap:8px;align-items:center;width:100%;
                         background:#0b1427;border:1px solid var(--line);color:#fff;
                         padding:8px 10px;border-radius:8px;cursor:pointer;margin:4px 0;">
            💱 Preset Rates
          </button>
          <button type="button" class="menu-item" data-k="job-titles"
                  style="display:flex;gap:8px;align-items:center;width:100%;
                         background:#0b1427;border:1px solid var(--line);color:#fff;
                         padding:8px 10px;border-radius:8px;cursor:pointer;margin:4px 0;">
            🏷 Job Titles
          </button>
        `;

        // Position under the button
        document.body.appendChild(m);
        const r = b.getBoundingClientRect();
        m.style.left = `${Math.round(window.scrollX + r.left)}px`;
        m.style.top  = `${Math.round(window.scrollY + r.bottom + 6)}px`;

        // Wire actions
        m.addEventListener('click', (e) => {
          const it = e.target.closest('.menu-item');
          if (!it) return;
          const k = it.getAttribute('data-k');
          closeSettingsMenu();

          if (k === 'global') {
            // keep current behaviour
            switchToSection('settings');
          } else if (k === 'rates') {
            // Preset Rates manager (parent modal)
            if (!confirmDiscardChangesIfDirty()) return;
            openPresetRatesManager();
          } else if (k === 'job-titles') {
            // New Job Titles manager (side-panel modal)
            if (!confirmDiscardChangesIfDirty()) return;
            openJobTitleSettingsModal();
          }
        });

        // Close on outside click / Esc
        setTimeout(() => {
          document.addEventListener('click', onAnyDocClick, true);
          document.addEventListener('keydown', onEsc, true);
        }, 0);

        // Visual cue
        b.classList.add('active');
      };
    }

    nav.appendChild(b);
  });

  // Quick search: Enter runs a search and resets to page 1
  try {
    const q = byId('quickSearch');
    if (q && !q.__wired) {
      q.addEventListener('keydown', async (e) => {
        if (e.key !== 'Enter') return;
        if (!confirmDiscardChangesIfDirty()) return;

        window.__listState = window.__listState || {};
        window.__selection = window.__selection || {};

        const st  = (window.__listState[currentSection]  ||= { page: 1, pageSize: 50, total: null, hasMore: false, filters: null });
        const sel = (window.__selection[currentSection] ||= { fingerprint:'', ids:new Set() });

        st.page = 1;
        const text = (q.value || '').trim();

        if (!text) {
          st.filters = null;
          sel.fingerprint = JSON.stringify({ section: currentSection, filters: {} });
          sel.ids.clear();
          const data = await loadSection();
          return renderSummary(data);
        }

        // Minimal quick-search filters by section
        let filters = null;
        if (currentSection === 'candidates') {
          if (text.includes('@'))       filters = { email: text };
          else if (text.replace(/\D/g,'').length >= 7) filters = { phone: text };
          else if (text.includes(' ')) {
            const [fn, ln] = text.split(' ').filter(Boolean);
            filters = { first_name: fn || text, last_name: ln || '' };
          } else filters = { first_name: text };
        } else if (currentSection === 'clients' || currentSection === 'umbrellas') {
          filters = { name: text };
        } else if (currentSection === 'contracts') {
          // free-text passthrough for contracts
          filters = { q: text };
        } else {
          const data = await loadSection();
          return renderSummary(data);
        }

        st.filters = filters;
        sel.fingerprint = JSON.stringify({ section: currentSection, filters });
        sel.ids.clear();

        const rows = await search(currentSection, filters);
        renderSummary(rows);
      });
      q.__wired = true;
    }
  } catch {}
}

// NEW: advanced, section-aware search modal
// === UPDATED: Advanced Search — add Roles (any) multi-select, use UK date pickers ===
// -----------------------------
// Search presets FE cache (optional but handy)
// -----------------------------
const __PRESETS_CACHE__ = new Map(); // key = `${section}:${kind}|shared=${0/1}|q=...|p=..|ps=..`

function cacheKey(section, kind = 'search', opts = {}) {
  const shared = opts.include_shared ? 1 : 0;
  const q      = opts.q ? String(opts.q) : '';
  const page   = Number.isFinite(opts.page) ? opts.page : 1;
  const ps     = Number.isFinite(opts.page_size) ? opts.page_size : 100;
  return `${section || ''}:${kind || 'search'}|shared=${shared}|q=${q}|p=${page}|ps=${ps}`;
}

function invalidatePresetCache(section, kind = 'search', opts) {
  if (opts) {
    __PRESETS_CACHE__.delete(cacheKey(section, kind, opts));
    return;
  }
  // No opts provided: clear all variants for this (section,kind)
  const prefix = `${section || ''}:${kind || 'search'}|`;
  for (const key of __PRESETS_CACHE__.keys()) {
    if (key.startsWith(prefix)) __PRESETS_CACHE__.delete(key);
  }
}



function getPresetCache(section, kind = 'search', opts) {
  return __PRESETS_CACHE__.get(cacheKey(section, kind, opts)) || null;
}

function setPresetCache(section, kind, rows, opts) {
  __PRESETS_CACHE__.set(
    cacheKey(section, kind, opts),
    Array.isArray(rows) ? rows : []
  );
}
// Quick helper for ownership checks; adapt if you keep user in a different global
function currentUserId(){
  try {
    return (window.SESSION && window.SESSION.user && window.SESSION.user.id)
        || (window.__auth && window.__auth.user && window.__auth.user.id)
        || window.__USER_ID
        || null;
  } catch (_) {
    return null;
  }
}






// -----------------------------
// Preset API wrappers
// -----------------------------
// ─────────────────────────────────────────────────────────────────────────────
// Child modal — Rate preset (create / view / edit)
// ─────────────────────────────────────────────────────────────────────────────

function computeRatePresetMargins(state){
  const buckets = ['day','night','sat','sun','bh'];
  const out = { bucket:{}, anyNegative:false };
  if (!state || typeof state !== 'object') return out;

  let erniDec = 0;
  try { if (typeof ensureErniMultiplier === 'function') ensureErniMultiplier(); } catch {}

  if (typeof window !== 'undefined' && Number.isFinite(window.__ERNI_MULT__)) {
    erniDec = Math.max(0, Number(window.__ERNI_MULT__) - 1);
  } else if (typeof getCurrentErniMultiplier === 'function') {
    const m = getCurrentErniMultiplier();
    if (Number.isFinite(m)) erniDec = Math.max(0, m - 1);
  } else if (typeof getCurrentErniPct === 'function') {
    const p = getCurrentErniPct();
    if (Number.isFinite(p)) erniDec = Math.max(0, p / 100);
  } else if (Number.isFinite(window?.__erniMultiplier)) {
    erniDec = Math.max(0, Number(window.__erniMultiplier) - 1);
  } else if (Number.isFinite(window?.__erniPct)) {
    erniDec = Math.max(0, Number(window.__erniPct) / 100);
  }

  const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : NaN; };

  buckets.forEach(b => {
    const paye   = num(state[`paye_${b}`]);
    const umb    = num(state[`umb_${b}`]);
    const charge = num(state[`charge_${b}`]);

    const hasP = !!state.enable_paye && Number.isFinite(paye);
    const hasU = !!state.enable_umbrella && Number.isFinite(umb);
    const hasC = Number.isFinite(charge);

    const marginP = (hasP && hasC) ? (charge - (paye + (paye * erniDec))) : null;
    const marginU = (hasU && hasC) ? (charge - umb) : null;

    const negP = (marginP != null) && (marginP < 0);
    const negU = (marginU != null) && (marginU < 0);
    if (negP || negU) out.anyNegative = true;

    out.bucket[b] = {
      paye:        hasP ? paye   : null,
      umb:         hasU ? umb    : null,
      charge:      hasC ? charge : null,
      marginPaye:  marginP,
      marginUmb:   marginU,
      negPaye:     negP,
      negUmb:      negU
    };
  });

  return out;
}

function openRatePresetPicker(applyCb, opts = {}) {
  const LOG = (typeof window.__LOG_RATES === 'boolean') ? window.__LOG_RATES : true;
  const L   = (...a)=> { if (LOG) console.log('[PRESETS]', ...a); };

  const {
    client_id = null,
    start_date = null,
    defaultScope = (client_id ? 'CLIENT' : 'GLOBAL')
  } = opts;

  let pickerRows = [];
  let pickerSelectedIndex = -1;
  let pickerSelectedId = null;
  let applyInFlight = false; // debounce guard

  const content = () => `
    <div class="tabc" id="ratePresetPicker">
      <div class="row">
        <label>Scope</label>
        <div class="controls">
          <label><input type="radio" name="rp_scope" value="ALL" ${defaultScope==='ALL'?'checked':''}/> All</label>
          <label><input type="radio" name="rp_scope" value="GLOBAL" ${defaultScope==='GLOBAL'?'checked':''}/> Global</label>
          <label ${client_id?'':'title="Pick a client to enable Client presets"'} >
            <input type="radio" name="rp_scope" value="CLIENT" ${defaultScope==='CLIENT'?'checked':''} ${client_id?'':'disabled'}/> Client
          </label>
          <input type="text" id="rp_search" class="input" placeholder="Search…" style="margin-left:auto;min-width:200px"/>
        </div>
      </div>
      <div class="hint" style="margin:6px 0 10px">
        Double-click a row to apply. Single-click selects; click <em>Apply</em> to use the selected preset.
      </div>
      <div style="border:1px solid var(--line);border-radius:10px;overflow:hidden">
        <table class="grid" id="rp_table">
          <thead>
            <tr>
              <th style="width:36px"></th>
              <th>Name / Role / Band</th>
              <th>Scope</th>
              <th>Dates</th>
              <th>Charge (D/N/Sa/Su/BH)</th>
              <th>PAYE</th>
              <th>Umbrella</th>
              <th>Mileage</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>`;

  // onApply: just apply the preset and tell saveForFrame we're OK.
  const onApply = async () => {
    if (applyInFlight) return false;
    if (!pickerRows || pickerSelectedIndex < 0) return false;
    const chosen = pickerRows[pickerSelectedIndex];
    if (!chosen) return false;

    applyInFlight = true;
    L('onApply: applying preset row', { chosen });

    try {
      if (typeof applyCb === 'function') {
        await applyCb(chosen);
      }
      // Let saveForFrame() close this child modal normally.
      return { ok: true };
    } catch (e) {
      console.error('[PRESETS] onApply failed', e);
      return false;
    } finally {
      applyInFlight = false;
    }
  };

  showModal(
    'Rate Presets',
    [{ key: 'p', title: 'Presets' }],
    () => content(),
    onApply,
    false,
    async () => {
      const root   = document.getElementById('ratePresetPicker');
      const tbody  = root?.querySelector('#rp_table tbody');
      const search = root?.querySelector('#rp_search');
      const radios = Array.from(root?.querySelectorAll('input[name="rp_scope"]') || []);
      if (!tbody) return;

      // Scope any header/button lookups to THIS picker modal
      const pickerModalEl = root?.closest('.modal');

      const scopeVal = () =>
        (radios.find(r => r.checked)?.value || (client_id ? 'CLIENT' : 'GLOBAL'));

      const pill = (v) => (v == null || v === '' ? '-' : String(v));

      const rateRow = (p) => [
        `D:${pill(p.charge_day)} N:${pill(p.charge_night)} Sa:${pill(p.charge_sat)} Su:${pill(p.charge_sun)} BH:${pill(p.charge_bh)}`,
        `D:${pill(p.umb_day)   } N:${pill(p.umb_night)   } Sa:${pill(p.umb_sat)   } Su:${pill(p.umb_sun)   } BH:${pill(p.umb_bh)   }`,
        `D:${pill(p.paye_day)  } N:${pill(p.paye_night)  } Sa:${pill(p.paye_sat)  } Su:${pill(p.paye_sun)  } BH:${pill(p.paye_bh)  }`
      ];

      const updateApplyState = () => {
        const canApply = !!(pickerRows.length && pickerSelectedIndex >= 0);

        // Only touch THIS picker’s Save/Apply button
        const btn = pickerModalEl?.querySelector('#btnSave');
        if (btn) {
          btn.disabled = !canApply;
          btn.title = canApply ? '' : 'Select a preset to apply';
        }

        // Only update the picker frame (not the parent contracts frame)
        const fr = window.__getModalFrame?.();
        if (fr?.kind === 'rate-presets-picker' && typeof fr._updateButtons === 'function') {
          fr.__canSave = canApply;
          fr._updateButtons();
        }
      };

      const paint = () => {
        const activeIndex = pickerSelectedIndex;
        const body = pickerRows.map((r, i) => {
          const scope = String(r.scope || (r.client_id ? 'CLIENT' : 'GLOBAL')).toUpperCase();
          const name =
            r.name ||
            [r.role, r.band ? `Band ${r.band}` : ''].filter(Boolean).join(' / ') ||
            'Preset';

          const [chg, umb, paye] = rateRow(r);
          const isActive = (i === activeIndex);
          const cls = isActive ? ' class="active selected"' : '';

          const mileagePay = pill(r.mileage_pay_rate);
          const mileageCharge = pill(r.mileage_charge_rate);
          const mileageTxt =
            (mileagePay === '-' && mileageCharge === '-') ? '-' : `Pay ${mileagePay} / Charge ${mileageCharge}`;

          return `
            <tr data-i="${i}"${cls}>
              <td></td>
              <td>${name}</td>
              <td>${scope}</td>
              <td>${r.from_date || '-'} → ${r.to_date || '-'}</td>
              <td>${chg}</td>
              <td>${paye}</td>
              <td>${umb}</td>
              <td>${mileageTxt}</td>
            </tr>`;
        }).join('');

        tbody.innerHTML =
          body || '<tr><td colspan="8" class="mini" style="text-align:center">No presets found</td></tr>';

        updateApplyState();
      };

      const fetchRows = async () => {
        const scope = scopeVal();
        const qRaw = (search?.value || '').trim();
        const q = qRaw.toLowerCase();
        const cid = client_id ? String(client_id) : null;

        try {
          let rows = [];

          if (scope === 'GLOBAL') {
            // All GLOBAL presets (search handled client-side)
            rows = await listRatePresets({ scope: 'GLOBAL' });
          } else if (scope === 'CLIENT') {
            // All CLIENT presets for this client id
            rows = cid ? await listRatePresets({ scope: 'CLIENT', client_id: cid }) : [];
          } else { // ALL
            const globals = await listRatePresets({ scope: 'GLOBAL' });
            const clientRows = cid ? await listRatePresets({ scope: 'CLIENT', client_id: cid }) : [];
            rows = [...globals, ...clientRows];
          }

          rows = Array.isArray(rows) ? rows : [];

          // Front-end search: match on name / role / band / display_site
          if (q) {
            rows = rows.filter(r => {
              const name = String(r.name || '').toLowerCase();
              const role = String(r.role || '').toLowerCase();
              const band = String(r.band || '').toLowerCase();
              const site = String(r.display_site || '').toLowerCase();
              return (
                name.includes(q) ||
                role.includes(q) ||
                band.includes(q) ||
                site.includes(q)
              );
            });
          }

          // Alphabetical by Name, then Role/Band
          rows.sort((a, b) => {
            const aName = (a.name || a.role || '').toString().toLowerCase();
            const bName = (b.name || b.role || '').toString().toLowerCase();
            if (aName < bName) return -1;
            if (aName > bName) return 1;
            return 0;
          });

          // If you have sortPresetsForView, keep using it; otherwise rows as-is
          pickerRows = (typeof sortPresetsForView === 'function')
            ? sortPresetsForView(scope, rows)
            : rows;

          L('fetchRows: got presets', { scope, q: qRaw, count: pickerRows.length });
        } catch (e) {
          console.error('[PRESETS] fetchRows error', e);
          pickerRows = [];
        }

        // reset selection
        pickerSelectedIndex = -1;
        pickerSelectedId = null;
        paint();
      };

      // Single-click: just select the row; don't repaint tbody
      tbody.addEventListener('click', (e) => {
        const tr = e.target.closest('tr[data-i]');
        if (!tr) return;
        const idx = +tr.getAttribute('data-i');
        if (!Number.isFinite(idx)) return;

        pickerSelectedIndex = idx;
        pickerSelectedId = pickerRows[idx]?.id || null;
        L('row click → select', { idx, id: pickerSelectedId });

        // Toggle selection classes without rebuilding the tbody
        tbody.querySelectorAll('tr.active, tr.selected').forEach(n => n.classList.remove('active','selected'));
        tr.classList.add('active','selected');

        updateApplyState();
      });

      // Double-click: select + programmatically press the picker’s Apply button
      tbody.addEventListener('dblclick', (e) => {
        let idx = pickerSelectedIndex;
        const tr = e.target.closest('tr[data-i]');
        if (tr && Number.isFinite(+tr.getAttribute('data-i'))) {
          idx = +tr.getAttribute('data-i');
        }
        if (!Number.isFinite(idx) || idx < 0) return;

        pickerSelectedIndex = idx;
        pickerSelectedId = pickerRows[idx]?.id || null;
        L('row dblclick → apply', { idx, id: pickerSelectedId });

        const btn = pickerModalEl?.querySelector('#btnSave');
        if (btn && !btn.disabled) {
          btn.click();   // triggers saveForFrame → onApply → framework closes child properly
        }

        e.preventDefault();
        e.stopPropagation();
      });

      search?.addEventListener('input', fetchRows);
      radios.forEach(r => r.addEventListener('change', fetchRows));

      await fetchRows();

      try {
        const fr = window.__getModalFrame?.();
        if (fr && fr.kind === 'rate-presets-picker' && typeof setFrameMode === 'function') {
          setFrameMode(fr, 'view');
          fr._updateButtons && fr._updateButtons();
        }
      } catch {}
    },
    { kind: 'rate-presets-picker', noParentGate: false }
  );

  setTimeout(() => {
    const fr = window.__getModalFrame?.();
    if (!fr || fr.kind !== 'rate-presets-picker') return;

    if (typeof fr.onReturn === 'function' && !fr.__init__) {
      fr.__init__ = true;
      fr.onReturn(fr);
    }
    fr._onSave = onApply;
  }, 0);
}


// ─────────────────────────────────────────────────────────────────────────────
// Parent modal — Preset Rates manager
// ─────────────────────────────────────────────────────────────────────────────


function openPresetRatesManager(){
  // Shared state for the manager; child modals can request refresh via this handle
  window.__ratesPresets__ = window.__ratesPresets__ || {};
  const S = window.__ratesPresets__;

  S.scope     = S.scope || 'ALL';     // 'ALL' | 'GLOBAL' | 'CLIENT'
  S.client_id = S.client_id || null;  // when scope === 'CLIENT'
  S.client_label = S.client_label || '';
  S.q         = S.q || '';

  const renderTable = (rows) => {
    const hasRows = Array.isArray(rows) && rows.length;
    if (!hasRows) {
      if (S.scope === 'CLIENT') {
        if (!S.client_id) {
          return `<div class="hint">Pick a client to see client-specific presets.</div>`;
        }
        return `<div class="hint">No presets exist for this client yet.</div>`;
      }
      return `<div class="hint">No presets match the current filter.</div>`;
    }

    const fmtWhen = (iso) => {
      if (!iso) return '';
      try { return (new Date(iso)).toLocaleString(); } catch { return iso; }
    };
    return `
      <table class="grid" id="ratesPresetsTable">
        <thead>
          <tr>
            <th>Name</th>
            <th>Scope</th>
            <th>Client</th>
            <th>Role</th>
            <th>Band</th>
            <th>Last edited</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          ${rows.map(r => `
            <tr data-id="${r.id}">
              <td>${r.name || ''}</td>
              <td>${(String(r.scope || (r.client_id ? 'CLIENT' : 'GLOBAL')).toUpperCase())}</td>
              <td>${(r.client && r.client.name) ? r.client.name : (r.client_name || '')}</td>
              <td>${r.role || ''}</td>
              <td>${r.band ?? ''}</td>
              <td class="mini">${fmtWhen(r.updated_at)}</td>
              <td class="mini">
                <button
                  type="button"
                  class="icon bin"
                  data-del="${r.id}"
                  title="Delete"
                >🗑</button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  };

  const buildBody = (rows=[]) => {
    const scopeAll    = S.scope === 'ALL'    ? 'checked' : '';
    const scopeGlobal = S.scope === 'GLOBAL' ? 'checked' : '';
    const scopeClient = S.scope === 'CLIENT' ? 'checked' : '';

    const clientBadgeInner = S.client_label
      ? `<span class="pill">${S.client_label}</span>`
      : `<span class="mini">No client selected</span>`;

    return `
      <div class="tabc">
        <div class="row">
          <label>Scope</label>
          <div class="controls">
            <label><input type="radio" name="rp_scope" value="ALL" ${scopeAll}/> All</label>
            <label><input type="radio" name="rp_scope" value="GLOBAL" ${scopeGlobal}/> Global</label>
            <label><input type="radio" name="rp_scope" value="CLIENT" ${scopeClient}/> Client</label>
          </div>
        </div>

        <div class="row" id="rp_client_row" style="display:${S.scope==='CLIENT' ? 'block':'none'}">
          <label>Client</label>
          <div class="controls">
            <div class="split">
              <button type="button" class="btn mini" id="rp_pick_client">Pick…</button>
              <button type="button" class="btn mini" id="rp_clear_client">Clear</button>
              <div id="rp_cli_badge">${clientBadgeInner}</div>
            </div>
          </div>
        </div>

        <div class="row">
          <label>Search</label>
          <div class="controls">
            <input class="input" type="text" id="rp_q" value="${S.q || ''}" placeholder="Filter by name, role, band…"/>
          </div>
        </div>

        <div id="rp_table_wrap">${renderTable(rows)}</div>
      </div>
    `;
  };

  const fetchRows = async () => {
    const params = {
      scope: S.scope,
      client_id: (S.scope === 'CLIENT' ? S.client_id : null),
      q: S.q || undefined
    };

    // Avoid noisy calls when client scope is chosen but no client selected yet.
    if (params.scope === 'CLIENT' && !params.client_id) {
      return [];
    }

    const raw = await listRatePresets(params);
    let rows = Array.isArray(raw) ? raw : [];

    // Hard filter: in CLIENT scope we only ever show client-specific rows for this client.
    if (S.scope === 'CLIENT' && S.client_id) {
      const cid = String(S.client_id);
      rows = rows.filter(r => {
        const sc = String(r.scope || (r.client_id ? 'CLIENT' : 'GLOBAL')).toUpperCase();
        return sc === 'CLIENT' && String(r.client_id || '') === cid;
      });
    }

    return sortPresetsForView(S.scope, rows);
  };

  // Keep selected row id in the manager context
  S.selectedId = null;

  showModal(
    'Preset Rates',
    [{ key: 'main', title: 'Presets' }],
    () => '<div class="tabc"><div class="hint">Loading…</div></div>',
    async () => true, // no save in parent
    false,
    async () => {
      const rows = await fetchRows();
      const mb = document.getElementById('modalBody');
      if (mb) mb.innerHTML = buildBody(rows);

      // Make sure header Delete stays hidden for this manager
      const delBtn = document.getElementById('btnDelete');
      if (delBtn) {
        delBtn.style.display = 'none';
        delBtn.onclick = null;
      }

      // Inject a scoped "New" button (idempotent) into the real actions bar
      if (!document.getElementById('btnRpNew')) {
        const bar = document.getElementById('btnSave')?.parentElement;
        if (bar) {
          const nb = document.createElement('button');
          nb.id = 'btnRpNew';
          nb.type = 'button';
          nb.className = 'btn';
          nb.textContent = 'New';
          nb.style.marginLeft = 'auto';
          nb.onclick = () => openRatePresetModal({ mode: 'edit' });
          bar.insertBefore(nb, document.getElementById('btnSave'));
        }
      }

      // Clean-up hook so "New" disappears when this frame is not top-most
      try {
        const fr = window.__getModalFrame?.();
        const prevDetach = fr && fr._detachGlobal;
        if (fr) {
          fr._detachGlobal = () => {
            try { document.getElementById('btnRpNew')?.remove(); } catch {}
            if (typeof prevDetach === 'function') { try { prevDetach(); } catch {} }
          };
        }
      } catch {}

      // Expose refresh to child modal
      S.refresh = async () => {
        const newRows = await fetchRows();
        const host = document.getElementById('rp_table_wrap');
        if (!host) return; // parent closed or not mounted
        host.innerHTML = renderTable(newRows);
        S.selectedId = null;
        wireTable();
      };

      function wireTable(){
        const tbl = document.getElementById('ratesPresetsTable');
        if (!tbl) return;

        tbl.addEventListener('click', (e) => {
          const delEl = e.target.closest('button[data-del]');
          if (delEl) {
            const id = delEl.getAttribute('data-del');
            if (!id) return;
            if (!confirm('Delete this preset?')) return;
            (async () => {
              try {
                await deleteRatePreset(id);
                if (S.selectedId === id) S.selectedId = null;
                await S.refresh();
              } catch (err) {
                alert(err?.message || 'Delete failed');
              }
            })();
            e.stopPropagation();
            return;
          }

          const tr = e.target.closest('tr[data-id]');
          if (!tr) return;
          S.selectedId = tr.getAttribute('data-id');
          tbl.querySelectorAll('tr').forEach(n => n.classList.remove('active'));
          tr.classList.add('active');
        });

        tbl.addEventListener('dblclick', (e) => {
          const tr = e.target.closest('tr[data-id]'); if (!tr) return;
          const id = tr.getAttribute('data-id');
          openRatePresetModal({ id, mode:'view' });
        });
      }

      function wireFilters(){
        const radios = Array.from(document.querySelectorAll('input[name="rp_scope"]'));
        radios.forEach(r => r.addEventListener('change', async () => {
          S.scope = r.value;
          const row = document.getElementById('rp_client_row');
          if (row) row.style.display = (S.scope === 'CLIENT' ? 'block':'none');
          await S.refresh();
        }));

        const pick = document.getElementById('rp_pick_client');
        if (pick) pick.onclick = () => {
          openClientPicker(({ id, label }) => {
            S.client_id = id;
            S.client_label = label;
            const badge = document.getElementById('rp_cli_badge');
            if (badge) badge.innerHTML = `<span class="pill">${label}</span>`;
            S.refresh();
          });
        };
        const clr = document.getElementById('rp_clear_client');
        if (clr) clr.onclick = () => {
          S.client_id = null;
          S.client_label = '';
          const badge = document.getElementById('rp_cli_badge');
          if (badge) badge.innerHTML = '<span class="mini">No client selected</span>';
          S.refresh();
        };

        const inpQ = document.getElementById('rp_q');
        if (inpQ && !inpQ.__wired) {
          inpQ.__wired = true;
          let t = 0;
          inpQ.addEventListener('input', () => {
            if (t) clearTimeout(t);
            t = setTimeout(() => { S.q = inpQ.value.trim(); S.refresh(); }, 180);
          });
        }
      }

      wireTable();
      wireFilters();
    },
    { kind:'rates-presets' }
  );

  // Kick the manager’s onReturn so it replaces the “Loading…” stub
  setTimeout(() => {
    const fr = window.__getModalFrame?.();
    if (fr && fr.kind === 'rates-presets' && typeof fr.onReturn === 'function' && !fr.__init__) {
      fr.__init__ = true;
      fr.onReturn();
    }
  }, 0);
}


async function openRatePresetModal({ id, mode } = {}) {
  const isCreate = !id;
  let initialMode = mode || (isCreate ? 'edit' : 'view');
  initialMode = String(initialMode || '').toLowerCase();
  if (initialMode === 'create') initialMode = 'edit';
  if (initialMode !== 'edit' && initialMode !== 'view') {
    initialMode = isCreate ? 'edit' : 'view';
  }

  const st = {
    id: id || null,
    scope: 'GLOBAL',
    client_id: null,
    client_label: '',
    name: '',
    role: '',
    band: '',
    display_site: '',
    bucket_day: 'Day',
    bucket_night: 'Night',
    bucket_sat: 'Sat',
    bucket_sun: 'Sun',
    bucket_bh: 'BH',
    enable_paye: false,
    enable_umbrella: false,
    payMode: 'PAYE', // 'PAYE' | 'UMB' | 'BOTH'
    paye_day: '',
    paye_night: '',
    paye_sat: '',
    paye_sun: '',
    paye_bh: '',
    umb_day: '',
    umb_night: '',
    umb_sat: '',
    umb_sun: '',
    umb_bh: '',
    charge_day: '',
    charge_night: '',
    charge_sat: '',
    charge_sun: '',
    charge_bh: '',
    mileage_pay_rate: '',
    mileage_charge_rate: '',
    use_schedule: false,
    mon_start: '',
    mon_end: '',
    mon_break: '',
    tue_start: '',
    tue_end: '',
    tue_break: '',
    wed_start: '',
    wed_end: '',
    wed_break: '',
    thu_start: '',
    thu_end: '',
    thu_break: '',
    fri_start: '',
    fri_end: '',
    fri_break: '',
    sat_start: '',
    sat_end: '',
    sat_break: '',
    sun_start: '',
    sun_end: '',
    sun_break: ''
  };

  if (id) {
    try {
      const row = await loadRatePreset(id);
      st.id = row.id || id;
      st.scope = (String(row.scope || (row.client_id ? 'CLIENT' : 'GLOBAL')).toUpperCase() === 'CLIENT') ? 'CLIENT' : 'GLOBAL';
      st.client_id = row.client_id || null;
      st.client_label = (row.client && row.client.name) ? row.client.name : (row.client_name || '');
      st.name = row.name || '';
      st.role = row.role || '';
      st.band = (row.band == null ? '' : String(row.band));
      st.display_site = row.display_site || '';

      const L = normaliseBucketLabelsInput(row.bucket_labels_json || null) || labelsDefault();
      st.bucket_day = L.day;
      st.bucket_night = L.night;
      st.bucket_sat = L.sat;
      st.bucket_sun = L.sun;
      st.bucket_bh = L.bh;

      st.enable_paye = !!row.enable_paye;
      st.enable_umbrella = !!row.enable_umbrella;

      if (st.enable_paye && st.enable_umbrella) {
        st.payMode = 'BOTH';
      } else if (st.enable_paye) {
        st.payMode = 'PAYE';
      } else if (st.enable_umbrella) {
        st.payMode = 'UMB';
      } else {
        st.payMode = 'PAYE';
      }

      const put = (k, v) => { if (v === 0 || (v != null && v !== '')) st[k] = String(v); };
      const R = row || {};

      put('paye_day', R.paye_day);
      put('paye_night', R.paye_night);
      put('paye_sat', R.paye_sat);
      put('paye_sun', R.paye_sun);
      put('paye_bh', R.paye_bh);

      put('umb_day', R.umb_day);
      put('umb_night', R.umb_night);
      put('umb_sat', R.umb_sat);
      put('umb_sun', R.umb_sun);
      put('umb_bh', R.umb_bh);

      put('charge_day', R.charge_day);
      put('charge_night', R.charge_night);
      put('charge_sat', R.charge_sat);
      put('charge_sun', R.charge_sun);
      put('charge_bh', R.charge_bh);

      put('mileage_pay_rate', R.mileage_pay_rate);
      put('mileage_charge_rate', R.mileage_charge_rate);

      if (row.std_schedule_json && typeof row.std_schedule_json === 'object') {
        st.use_schedule = true;
        const S = row.std_schedule_json || {};
        const days = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
        days.forEach(d => {
          if (S[d]) {
            st[`${d}_start`] = S[d].start || '';
            st[`${d}_end`] = S[d].end || '';
            st[`${d}_break`] = (S[d].break_minutes == null ? '' : String(S[d].break_minutes));
          }
        });
      }
    } catch (e) {
      alert(e?.message || 'Failed to load preset');
      return;
    }
  }

  const buckets = ['day', 'night', 'sat', 'sun', 'bh'];

  function getFieldValue(root, name) {
    if (!root) return '';
    const el = root.querySelector(`[name="${name}"]`);
    return (el && typeof el.value === 'string') ? el.value.trim() : '';
  }

  function parseNumeric(raw) {
    if (raw == null) return null;
    const s = String(raw).trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  function parseNumericFromRoot(root, name) {
    return parseNumeric(getFieldValue(root, name));
  }

  function computePresetEligibility(root, st) {
    const result = { margin: null, eligible: false, rateState: null, scope: '', payMode: '', reasons: [] };
    if (!root) return result;

    const rtEl = root.querySelector('#rp_rate_type');
    let scopeVal = (rtEl && rtEl.value) ? String(rtEl.value).toUpperCase() : '';
    if (scopeVal !== 'GLOBAL' && scopeVal !== 'CLIENT') scopeVal = '';
    result.scope = scopeVal;
    if (scopeVal) st.scope = scopeVal;

    const pmEl = root.querySelector('#rp_pay_mode');
    let payModeVal = (pmEl && pmEl.value) ? String(pmEl.value).toUpperCase() : (st.payMode || '');
    if (!['PAYE', 'UMB', 'BOTH'].includes(payModeVal)) payModeVal = '';
    result.payMode = payModeVal;

    const rateState = {
      enable_paye: ['PAYE', 'BOTH'].includes(payModeVal),
      enable_umbrella: ['UMB', 'BOTH'].includes(payModeVal)
    };

    buckets.forEach(b => {
      ['paye', 'umb', 'charge'].forEach(prefix => {
        rateState[`${prefix}_${b}`] = parseNumericFromRoot(root, `${prefix}_${b}`);
      });
    });

    result.rateState = rateState;
    const margin = computeRatePresetMargins(rateState);
    result.margin = margin;

    let eligible = true;

    const nameVal = getFieldValue(root, 'name');
    if (!nameVal) { eligible = false; result.reasons.push('name'); }

    const roleVal = getFieldValue(root, 'role');
    if (!roleVal) { eligible = false; result.reasons.push('role'); }

    if (!scopeVal) { eligible = false; result.reasons.push('scope'); }
    if (scopeVal === 'CLIENT') {
      const cid = st.client_id || '';
      if (!cid) { eligible = false; result.reasons.push('client'); }
    }

    const labelNames = ['bucket_day', 'bucket_night', 'bucket_sat', 'bucket_sun', 'bucket_bh'];
    const anyLabel = labelNames.some(n => !!getFieldValue(root, n));
    if (!anyLabel) { eligible = false; result.reasons.push('labels'); }

    if (!payModeVal) { eligible = false; result.reasons.push('payMode'); }

    const hasCharge = buckets.some(b => Number.isFinite(rateState[`charge_${b}`]));
    if (!hasCharge) { eligible = false; result.reasons.push('rates_charge'); }

    if (payModeVal === 'PAYE') {
      const okRow = buckets.some(
        b => Number.isFinite(rateState[`paye_${b}`]) && Number.isFinite(rateState[`charge_${b}`])
      );
      if (!okRow) { eligible = false; result.reasons.push('rates_paye'); }
    } else if (payModeVal === 'UMB') {
      const okRow = buckets.some(
        b => Number.isFinite(rateState[`umb_${b}`]) && Number.isFinite(rateState[`charge_${b}`])
      );
      if (!okRow) { eligible = false; result.reasons.push('rates_umb'); }
    } else if (payModeVal === 'BOTH') {
      const okRow = buckets.some(
        b =>
          Number.isFinite(rateState[`paye_${b}`]) &&
          Number.isFinite(rateState[`umb_${b}`]) &&
          Number.isFinite(rateState[`charge_${b}`])
      );
      if (!okRow) { eligible = false; result.reasons.push('rates_both'); }
    }

    const useScheduleEl = root.querySelector('#rp_use_schedule');
    const useSchedule = !!useScheduleEl && !!useScheduleEl.checked;
    if (useSchedule) {
      const days = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
      const S = {};
      const hhmm = (s) => {
        if (!s) return '';
        const m = String(s).match(/^(\d{1,2}):?(\d{2})$/);
        if (!m) return '';
        const h = +m[1], mi = +m[2];
        if (h < 0 || h > 23 || mi < 0 || mi > 59) return '';
        return String(h).padStart(2, '0') + ':' + String(mi).padStart(2, '0');
      };
      days.forEach(d => {
        const s = hhmm(getFieldValue(root, `${d}_start`));
        const e = hhmm(getFieldValue(root, `${d}_end`));
        const br = getFieldValue(root, `${d}_break`);
        if (s && e) {
          S[d] = { start: s, end: e, break_minutes: Math.max(0, Number(br || 0)) };
        }
      });
      if (Object.keys(S).length === 0) {
        eligible = false;
        result.reasons.push('schedule');
      }
    }

    result.eligible = eligible;
    return result;
  }

  const renderGrid = () => {
    const tempState = {
      enable_paye: ['PAYE', 'BOTH'].includes(st.payMode),
      enable_umbrella: ['UMB', 'BOTH'].includes(st.payMode)
    };
    buckets.forEach(b => {
      tempState[`paye_${b}`] = parseNumeric(st[`paye_${b}`]);
      tempState[`umb_${b}`] = parseNumeric(st[`umb_${b}`]);
      tempState[`charge_${b}`] = parseNumeric(st[`charge_${b}`]);
    });
    const margin = computeRatePresetMargins(tempState);

    const row = (lab, key) => {
      const MP = margin.bucket[key]?.marginPaye;
      const MU = margin.bucket[key]?.marginUmb;
      const negP = margin.bucket[key]?.negPaye;
      const negU = margin.bucket[key]?.negUmb;
      const mTxt = [
        (tempState.enable_paye ? `PAYE: ${MP == null ? '—' : MP.toFixed(2)}${negP ? ' ⚠' : ''}` : ''),
        (tempState.enable_umbrella ? `Umb: ${MU == null ? '—' : MU.toFixed(2)}${negU ? ' ⚠' : ''}` : '')
      ].filter(Boolean).join(' • ');

      return `
        <div class="grid-5 rp-rate-row" data-bucket="${key}">
          <div class="split"><span class="lbl">${lab}</span></div>
          <div class="rp-col-paye">
            <input class="input" name="paye_${key}" placeholder="PAYE" value="${st[`paye_${key}`] || ''}"/>
          </div>
          <div class="rp-col-umb">
            <input class="input" name="umb_${key}" placeholder="Umbrella" value="${st[`umb_${key}`] || ''}"/>
          </div>
          <div class="rp-col-charge">
            <input class="input" name="charge_${key}" placeholder="Charge" value="${st[`charge_${key}`] || ''}"/>
          </div>
          <div class="mini" data-role="margin">${mTxt || ''}</div>
        </div>`;
    };

    return `
      <div class="group">
        <div class="row"><label>Rates</label>
          <div class="controls small">
            <div class="grid-5" id="rp_rates_header">
              <div></div>
              <div class="mini rp-col-paye">PAYE</div>
              <div class="mini rp-col-umb">Umbrella</div>
              <div class="mini rp-col-charge">Charge</div>
              <div class="mini">Margin</div>
            </div>
            ${row(st.bucket_day, 'day')}
            ${row(st.bucket_night, 'night')}
            ${row(st.bucket_sat, 'sat')}
            ${row(st.bucket_sun, 'sun')}
            ${row(st.bucket_bh, 'bh')}
            <div class="mini" id="rp_margin_warn" style="margin-top:6px;${margin.anyNegative ? '' : 'display:none'}">Margin can't be negative.</div>
          </div>
        </div>
      </div>
    `;
  };

  const renderSchedule = () => {
    const timeInput = (name, val) => `<input class="input rp-time" name="${name}" value="${val || ''}" placeholder="HH:MM" />`;
    const breakInput = (name, val) => `<input class="input rp-break" type="number" min="0" step="1" name="${name}" value="${val || ''}" placeholder="0" />`;
    const row = (key, label) => `
      <div class="rp-day" data-day="${key}" style="margin-bottom:10px">
        <div class="grid-3">
          <div class="split"><span class="mini">${label} start</span>${timeInput(`${key}_start`, st[`${key}_start`])}</div>
          <div class="split"><span class="mini">${label} end</span>${timeInput(`${key}_end`, st[`${key}_end`])}</div>
          <div class="split"><span class="mini">Break (min)</span>${breakInput(`${key}_break`, st[`${key}_break`])}</div>
        </div>
        <div class="split" style="margin-top:6px">
          <button type="button" class="btn mini rp_copy" data-day="${key}">Copy</button>
          <button type="button" class="btn mini rp_paste" data-day="${key}">Paste</button>
        </div>
      </div>`;
    return `
      <div class="group">
        <label><input type="checkbox" id="rp_use_schedule" ${st.use_schedule ? 'checked' : ''}/> Default shift times</label>
        <div id="rp_sched_block" style="display:${st.use_schedule ? 'block' : 'none'}; margin-top:8px">
          ${row('mon', 'Mon')}${row('tue', 'Tue')}${row('wed', 'Wed')}
          ${row('thu', 'Thu')}${row('fri', 'Fri')}${row('sat', 'Sat')}
          ${row('sun', 'Sun')}
        </div>
      </div>`;
  };

  const renderLabels = () => `
    <div class="group">
      <div class="row"><label>Bucket labels</label>
        <div class="controls small">
          <div class="grid-5" id="rp_labels_grid">
            <div><span class="mini">Standard</span><input class="input" name="bucket_day"   value="${st.bucket_day}"/></div>
            <div><span class="mini">OT1</span>     <input class="input" name="bucket_night" value="${st.bucket_night}"/></div>
            <div><span class="mini">OT2</span>     <input class="input" name="bucket_sat"   value="${st.bucket_sat}"/></div>
            <div><span class="mini">OT3</span>     <input class="input" name="bucket_sun"   value="${st.bucket_sun}"/></div>
            <div><span class="mini">OT4</span>     <input class="input" name="bucket_bh"    value="${st.bucket_bh}"/></div>
          </div>
          <div style="margin-top:8px">
            <span class="mini">Pay mode</span>
            <select class="input" name="rp_pay_mode" id="rp_pay_mode">
              <option value="PAYE" ${st.payMode === 'PAYE' ? 'selected' : ''}>PAYE</option>
              <option value="UMB" ${st.payMode === 'UMB' ? 'selected' : ''}>Umbrella</option>
              <option value="BOTH" ${st.payMode === 'BOTH' ? 'selected' : ''}>PAYE &amp; Umbrella</option>
            </select>
          </div>
        </div>
      </div>
    </div>`;

  const renderTop = () => `
    <div class="group">
      <div class="row">
        <label>Name</label>
        <div class="controls"><input class="input" name="name" value="${st.name}"/></div>
      </div>

      <div class="row">
        <label>Rate type</label>
        <div class="controls">
          <select id="rp_rate_type" class="input">
            <option value="">Please select</option>
            <option value="GLOBAL" ${st.scope === 'GLOBAL' ? 'selected' : ''}>Global</option>
            <option value="CLIENT" ${st.scope === 'CLIENT' ? 'selected' : ''}>Client specific</option>
          </select>
        </div>
      </div>

      <div class="row" id="rp_client_row" style="margin-top:6px; display:${st.scope === 'CLIENT' ? 'block' : 'none'}">
        <label>Client rate</label>
        <div class="controls">
          <div class="split">
            <button type="button" class="btn mini" id="rp_pick_cli_btn">Pick…</button>
            <button type="button" class="btn mini" id="rp_clear_cli_btn">Clear</button>
            <span class="mini" id="rp_cli_lbl">${st.client_label ? `Chosen: ${st.client_label}` : 'No client chosen'}</span>
          </div>
        </div>
      </div>

      <div class="grid-3">
        <div class="row"><label>Role</label><div class="controls"><input class="input" name="role" value="${st.role}"/></div></div>
        <div class="row"><label>Band</label><div class="controls"><input class="input" name="band" value="${st.band}"/></div></div>
        <div class="row"><label>Display site</label><div class="controls"><input class="input" name="display_site" value="${st.display_site}"/></div></div>
      </div>
    </div>`;

  const renderMileage = () => `
    <div class="group">
      <div class="row"><label>Mileage</label>
        <div class="controls">
          <div class="grid-3">
            <div class="split"><span class="mini">Pay</span>   <input class="input" name="mileage_pay_rate"    value="${st.mileage_pay_rate || ''}" placeholder="0.00"/></div>
            <div class="split"><span class="mini">Charge</span><input class="input" name="mileage_charge_rate" value="${st.mileage_charge_rate || ''}" placeholder="0.00"/></div>
          </div>
        </div>
      </div>
    </div>`;

  const renderer = () => `
    <div class="tabc">
      <div class="form" id="rp_form">
        <div>
          ${renderTop()}
          ${renderLabels()}
          ${renderGrid()}
          ${renderMileage()}
        </div>
        <div>
          ${renderSchedule()}
        </div>
      </div>
    </div>
  `;

  const onSave = async () => {
    const root = document.getElementById('rp_form');
    if (!root) return false;

    const v = (n) => getFieldValue(root, n);

    const rtEl = root.querySelector('#rp_rate_type');
    let scopeVal = (rtEl && rtEl.value) ? String(rtEl.value).toUpperCase() : '';
    const scopeIsClient = (scopeVal === 'CLIENT');
    const scopeIsGlobal = (scopeVal === 'GLOBAL');

    if (!scopeIsClient && !scopeIsGlobal) {
      showModalHint('Select a rate type (Global or Client specific).', 'warn');
      return false;
    }

    const name = v('name');
    if (!name) {
      showModalHint('Name is required.', 'warn');
      return false;
    }

    const clientId = scopeIsClient ? (st.client_id || '') : '';
    if (scopeIsClient && !clientId) {
      showModalHint('Pick a client for a Client scope preset.', 'warn');
      return false;
    }

    const roleVal = v('role');
    if (!roleVal) {
      showModalHint('Role is required.', 'warn');
      return false;
    }

    const pmEl = root.querySelector('#rp_pay_mode');
    let payModeVal = (pmEl && pmEl.value) ? String(pmEl.value).toUpperCase() : (st.payMode || '');
    if (!['PAYE', 'UMB', 'BOTH'].includes(payModeVal)) {
      showModalHint('Choose PAYE, Umbrella or PAYE & Umbrella.', 'warn');
      return false;
    }

    const eligibility = computePresetEligibility(root, st);
    const margin = eligibility.margin || { anyNegative: false };
    if (margin.anyNegative) {
      showModalHint('Margin can’t be negative.', 'warn');
      return false;
    }
    if (!eligibility.eligible) {
      showModalHint('Fill in all required fields (name, role, labels, rates and schedule if used).', 'warn');
      return false;
    }

    const enable_paye = ['PAYE', 'BOTH'].includes(payModeVal);
    const enable_umbrella = ['UMB', 'BOTH'].includes(payModeVal);

    const payload = {
      id: st.id || undefined,
      name,
      scope: scopeIsClient ? 'CLIENT' : 'GLOBAL',
      client_id: scopeIsClient ? st.client_id : null,
      role: roleVal || null,
      band: (v('band') === '' ? null : v('band')),
      display_site: v('display_site') || null,
      enable_paye,
      enable_umbrella
    };

    const labels = {
      day: v('bucket_day'),
      night: v('bucket_night'),
      sat: v('bucket_sat'),
      sun: v('bucket_sun'),
      bh: v('bucket_bh')
    };
    const Lnorm = normaliseBucketLabelsInput(labels);
    if (Lnorm) payload.bucket_labels_json = Lnorm;

    const bucketsList = ['day','night','sat','sun','bh'];
    const push5 = (prefix, enabled) => {
      if (!enabled) return;
      bucketsList.forEach(b => {
        const key = `${prefix}_${b}`;
        const n = parseNumericFromRoot(root, key);
        if (n != null) payload[key] = n;
      });
    };
    push5('paye', enable_paye);
    push5('umb', enable_umbrella);
    push5('charge', true);

    const mileage_pay = parseNumericFromRoot(root, 'mileage_pay_rate');
    const mileage_charge = parseNumericFromRoot(root, 'mileage_charge_rate');
    if (mileage_pay != null && mileage_pay < 0) {
      showModalHint('Mileage pay must be ≥ 0', 'warn');
      return false;
    }
    if (mileage_charge != null && mileage_charge < 0) {
      showModalHint('Mileage charge must be ≥ 0', 'warn');
      return false;
    }
    if (mileage_pay != null) payload.mileage_pay_rate = mileage_pay;
    if (mileage_charge != null) payload.mileage_charge_rate = mileage_charge;

    const use_schedule = !!document.getElementById('rp_use_schedule')?.checked;
    if (use_schedule) {
      const days = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
      const S = {};
      const hhmm = (s) => {
        if (!s) return '';
        const m = String(s).match(/^(\d{1,2}):?(\d{2})$/);
        if (!m) return '';
        const h = +m[1], mi = +m[2];
        if (h < 0 || h > 23 || mi < 0 || mi > 59) return '';
        return String(h).padStart(2, '0') + ':' + String(mi).padStart(2, '0');
      };
      days.forEach(d => {
        const s = hhmm(v(`${d}_start`));
        const e = hhmm(v(`${d}_end`));
        const br = v(`${d}_break`);
        if (s && e) {
          S[d] = { start: s, end: e, break_minutes: Math.max(0, Number(br || 0)) };
        }
      });
      if (Object.keys(S).length) payload.std_schedule_json = S;
    }

    try {
      await saveRatePreset(payload);
      try { window.__ratesPresets__?.refresh && window.__ratesPresets__.refresh(); } catch {}
      return true;
    } catch (e) {
      showModalHint(e?.message || 'Save failed', 'fail');
      return false;
    }
  };

  showModal(
    isCreate ? 'Create preset' : 'Rate preset',
    [{ key: 'main', title: 'Main' }],
    () => renderer(),
    onSave,
    !!st.id,
    () => {
      const root = document.getElementById('rp_form');

      const saveBtn = document.getElementById('btnSave');
      if (saveBtn && initialMode === 'view') {
        saveBtn.disabled = true;
        saveBtn.title = 'Click Edit to make changes';
      }

      const rateTypeSel = root?.querySelector('#rp_rate_type');
      const cliRow = root?.querySelector('#rp_client_row');
      const cliLbl = root?.querySelector('#rp_cli_lbl');
      const btnPick = root?.querySelector('#rp_pick_cli_btn');
      const btnClr = root?.querySelector('#rp_clear_cli_btn');

      const ensureMileagePrefill = async () => {
        const isCreateNow = !st.id;
        if (isCreateNow && st.scope === 'CLIENT' && st.client_id) {
          try {
            const cli = await getClient(st.client_id);
            const ch = Number(cli?.mileage_charge_rate);
            if (Number.isFinite(ch)) {
              const pay = Math.max(0, ch - 0.10);
              const payEl = root.querySelector('[name="mileage_pay_rate"]');
              const chEl = root.querySelector('[name="mileage_charge_rate"]');
              if (chEl && !chEl.value) { chEl.value = String(ch); st.mileage_charge_rate = String(ch); }
              if (payEl && !payEl.value) { payEl.value = pay.toFixed(2); st.mileage_pay_rate = payEl.value; }
            }
          } catch {}
        }
      };

      const toggleColDisplay = (selector, on) => {
        root?.querySelectorAll(selector).forEach(el => {
          el.style.display = on ? '' : 'none';
        });
      };

      function refreshPayModeColumns() {
        const payMode = st.payMode || 'PAYE';
        st.enable_paye = (payMode === 'PAYE' || payMode === 'BOTH');
        st.enable_umbrella = (payMode === 'UMB' || payMode === 'BOTH');

        buckets.forEach(b => {
          const paye = root?.querySelector(`[name="paye_${b}"]`);
          const umb = root?.querySelector(`[name="umb_${b}"]`);
          if (paye) paye.disabled = !st.enable_paye;
          if (umb) umb.disabled = !st.enable_umbrella;
        });

        toggleColDisplay('#rp_form .rp-col-paye', st.enable_paye);
        toggleColDisplay('#rp_form .rp-col-umb', st.enable_umbrella);
      }

      function updatePresetSaveState() {
        const { margin, rateState, eligible } = computePresetEligibility(root, st);

        const warn = root?.querySelector('#rp_margin_warn');
        if (warn) warn.style.display = (margin && margin.anyNegative) ? '' : 'none';

        buckets.forEach(b => {
          const cell = root?.querySelector(`.rp-rate-row[data-bucket="${b}"] [data-role="margin"]`);
          if (!cell) return;
          const bucketMargin = margin?.bucket?.[b] || {};
          const mp = bucketMargin.marginPaye;
          const mu = bucketMargin.marginUmb;
          const negP = bucketMargin.negPaye;
          const negU = bucketMargin.negUmb;
          const parts = [];
          if (rateState.enable_paye) {
            parts.push(`PAYE: ${mp == null || Number.isNaN(mp) ? '—' : mp.toFixed(2)}${negP ? ' ⚠' : ''}`);
          }
          if (rateState.enable_umbrella) {
            parts.push(`Umb: ${mu == null || Number.isNaN(mu) ? '—' : mu.toFixed(2)}${negU ? ' ⚠' : ''}`);
          }
          cell.textContent = parts.filter(Boolean).join(' • ');
        });

        const fr = window.__getModalFrame?.();
        const btn = document.getElementById('btnSave');
        const modeNow = fr?.mode || initialMode || 'view';
        const canSave = !!(modeNow === 'edit' && margin && !margin.anyNegative && eligible);
        if (btn) {
          btn.disabled = !canSave;
          if (modeNow !== 'edit') {
            btn.title = 'Click Edit to make changes';
          } else {
            btn.title = canSave ? '' : 'Fill required fields and fix any negative margins';
          }
        }
        if (fr && typeof fr._updateButtons === 'function') {
          fr.__canSave = canSave;
          fr._updateButtons();
        }
      }

      function setPayMode(newMode, opts) {
        const optsNorm = opts || {};
        const clearPrev = !!optsNorm.clearPrev;
        const prevMode = st.payMode || 'PAYE';
        if (!newMode) return;
        const modeNorm = String(newMode).toUpperCase();
        if (!['PAYE', 'UMB', 'BOTH'].includes(modeNorm)) return;

        if (clearPrev && prevMode !== modeNorm) {
          if (prevMode === 'PAYE' && modeNorm === 'UMB') {
            buckets.forEach(b => {
              st[`paye_${b}`] = '';
              const inp = root?.querySelector(`[name="paye_${b}"]`);
              if (inp) inp.value = '';
            });
          } else if (prevMode === 'UMB' && modeNorm === 'PAYE') {
            buckets.forEach(b => {
              st[`umb_${b}`] = '';
              const inp = root?.querySelector(`[name="umb_${b}"]`);
              if (inp) inp.value = '';
            });
          }
        }

        st.payMode = modeNorm;
        refreshPayModeColumns();
        updatePresetSaveState();
      }

      function syncPayModeFromDom(opts) {
        const pmEl = root?.querySelector('#rp_pay_mode');
        const valRaw = pmEl?.value || st.payMode || 'PAYE';
        const val = String(valRaw).toUpperCase();
        const mode = ['PAYE', 'UMB', 'BOTH'].includes(val) ? val : 'PAYE';
        setPayMode(mode, opts);
      }

      function applyScopeFromControl(isInit) {
        if (!rateTypeSel) return;
        const prevScope = st.scope || 'GLOBAL';
        let val = String(rateTypeSel.value || '').toUpperCase();
        if (val !== 'GLOBAL' && val !== 'CLIENT') val = 'GLOBAL';
        const nextScope = val;

        if (nextScope === 'CLIENT') {
          st.scope = 'CLIENT';
          if (cliRow) cliRow.style.display = 'block';
          ensureMileagePrefill();
        } else {
          st.scope = 'GLOBAL';
          if (cliRow) cliRow.style.display = 'none';
          if (!isInit && prevScope === 'CLIENT') {
            st.client_id = null;
            st.client_label = '';
            if (cliLbl) cliLbl.textContent = 'No client chosen';
          }
        }
        updatePresetSaveState();
      }

      if (rateTypeSel) {
        rateTypeSel.addEventListener('change', () => applyScopeFromControl(false));
        applyScopeFromControl(true);
      }

      if (btnPick) {
        btnPick.onclick = () => {
          openClientPicker(({ id, label }) => {
            st.client_id = id;
            st.client_label = label || '';
            if (cliLbl) cliLbl.textContent = label ? `Chosen: ${label}` : 'No client chosen';
            ensureMileagePrefill();
            updatePresetSaveState();
          }, { allowBackdropModal: true });
        };
      }

      if (btnClr) {
        btnClr.onclick = () => {
          st.client_id = null;
          st.client_label = '';
          if (cliLbl) cliLbl.textContent = 'No client chosen';
          updatePresetSaveState();
        };
      }

      const pmSelect = root?.querySelector('#rp_pay_mode');
      if (pmSelect) {
        pmSelect.addEventListener('change', () => syncPayModeFromDom({ clearPrev: true }));
        syncPayModeFromDom({ clearPrev: false });
      } else {
        st.payMode = st.payMode || 'PAYE';
        refreshPayModeColumns();
      }

      const lblMap = {
        bucket_day: 'day',
        bucket_night: 'night',
        bucket_sat: 'sat',
        bucket_sun: 'sun',
        bucket_bh: 'bh'
      };
      Object.keys(lblMap).forEach(n => {
        const el = root?.querySelector(`#rp_labels_grid [name="${n}"]`);
        if (!el) return;
        el.addEventListener('input', () => {
          const k = lblMap[n];
          st[`bucket_${k}`] = el.value || '';
          const cell = root?.querySelector(`.rp-rate-row[data-bucket="${k}"] .lbl`);
          if (cell) cell.textContent = st[`bucket_${k}`] || cell.textContent;
          updatePresetSaveState();
        });
      });

      const useSch = document.getElementById('rp_use_schedule');
      const schBlk = document.getElementById('rp_sched_block');
      if (useSch && schBlk) {
        const togg = () => {
          st.use_schedule = !!useSch.checked;
          schBlk.style.display = st.use_schedule ? 'block' : 'none';
          updatePresetSaveState();
        };
        useSch.addEventListener('change', togg);
        togg();
      }

      const normalizeRateInput = (el) => {
        if (!el) return;
        let v = (el.value || '').trim();
        if (!v) return;
        v = v.replace(/\s+/g, '');
        if (v.startsWith('.')) v = '0' + v;
        const n = Number(v);
        if (!Number.isFinite(n)) return;
        el.value = n.toFixed(2);
      };

      buckets.forEach(b => {
        ['paye', 'umb', 'charge'].forEach(prefix => {
          const inp = root?.querySelector(`[name="${prefix}_${b}"]`);
          if (inp) {
            inp.addEventListener('blur', () => {
              normalizeRateInput(inp);
              st[`${prefix}_${b}`] = inp.value || '';
              updatePresetSaveState();
            });
            inp.addEventListener('input', () => {
              st[`${prefix}_${b}`] = inp.value || '';
            });
          }
        });
      });

      const mileagePayEl = root?.querySelector('[name="mileage_pay_rate"]');
      const mileageChargeEl = root?.querySelector('[name="mileage_charge_rate"]');
      const normalizeMileageInput = (el) => {
        if (!el) return;
        let v = (el.value || '').trim();
        if (!v) return;
        if (v.startsWith('.')) v = '0' + v;
        let numVal;
        if (v.includes('.')) {
          numVal = Number(v);
        } else {
          numVal = Number(v) / 100;
        }
        if (!Number.isFinite(numVal)) return;
        el.value = numVal.toFixed(2);
      };
      if (mileagePayEl) {
        mileagePayEl.addEventListener('blur', () => {
          normalizeMileageInput(mileagePayEl);
          st.mileage_pay_rate = mileagePayEl.value || '';
          const isCreateNow = !st.id;
          if (isCreateNow && mileageChargeEl && !(mileageChargeEl.value || '').trim()) {
            mileageChargeEl.value = mileagePayEl.value;
            st.mileage_charge_rate = mileageChargeEl.value || '';
          }
          updatePresetSaveState();
        });
        mileagePayEl.addEventListener('input', () => {
          st.mileage_pay_rate = mileagePayEl.value || '';
        });
      }
      if (mileageChargeEl) {
        mileageChargeEl.addEventListener('blur', () => {
          normalizeMileageInput(mileageChargeEl);
          st.mileage_charge_rate = mileageChargeEl.value || '';
          updatePresetSaveState();
        });
        mileageChargeEl.addEventListener('input', () => {
          st.mileage_charge_rate = mileageChargeEl.value || '';
        });
      }

      let schedClipboard = null;
      const schedRoot = document.getElementById('rp_sched_block');

      const normaliseTimeInput = (t) => {
        if (!t || !/^(mon|tue|wed|thu|fri|sat|sun)_(start|end)$/.test(t.name)) return;
        const raw = (t.value || '').trim();
        const norm = (function (x) {
          if (!x) return '';
          const y = x.replace(/\s+/g, '');
          let h, m;
          if (/^\d{3,4}$/.test(y)) {
            const s = y.padStart(4, '0'); h = +s.slice(0, 2); m = +s.slice(2, 4);
          } else if (/^\d{1,2}:\d{1,2}$/.test(y)) {
            const parts = y.split(':'); h = +parts[0]; m = +parts[1];
          } else return '';
          if (h < 0 || h > 23 || m < 0 || m > 59) return '';
          return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0');
        })(raw);

        if (!norm && raw) {
          t.value = '';
          t.setAttribute('data-invalid', '1');
          t.setAttribute('title', 'Enter a valid time HH:MM (00:00–23:59)');
          try {
            t.dispatchEvent(new Event('input', { bubbles: true }));
            t.dispatchEvent(new Event('change', { bubbles: true }));
          } catch {}
          st[t.name] = '';
          return;
        }

        if (norm) {
          t.value = norm;
          t.removeAttribute('data-invalid');
          t.removeAttribute('title');
          st[t.name] = norm;
          try {
            t.dispatchEvent(new Event('input', { bubbles: true }));
            t.dispatchEvent(new Event('change', { bubbles: true }));
          } catch {}
        }
      };

      if (schedRoot) {
        schedRoot.addEventListener('blur', (e) => {
          normaliseTimeInput(e.target);
        }, true);

        schedRoot.addEventListener('keydown', (e) => {
          if (e.key === 'Tab') normaliseTimeInput(e.target);
        }, true);

        schedRoot.addEventListener('click', (e) => {
          const copyBtn = e.target.closest('button.rp_copy');
          const pasteBtn = e.target.closest('button.rp_paste');
          if (copyBtn) {
            const day = copyBtn.dataset.day;
            const sEl = root.querySelector(`[name="${day}_start"]`);
            const eEl = root.querySelector(`[name="${day}_end"]`);
            const bEl = root.querySelector(`[name="${day}_break"]`);
            schedClipboard = {
              start: sEl?.value || '',
              end: eEl?.value || '',
              br: bEl?.value || ''
            };
            return;
          }
          if (pasteBtn && schedClipboard) {
            const day = pasteBtn.dataset.day;
            const sEl = root.querySelector(`[name="${day}_start"]`);
            const eEl = root.querySelector(`[name="${day}_end"]`);
            const bEl = root.querySelector(`[name="${day}_break"]`);
            if (sEl) { sEl.value = schedClipboard.start; st[`${day}_start`] = sEl.value || ''; }
            if (eEl) { eEl.value = schedClipboard.end;   st[`${day}_end`]   = eEl.value || ''; }
            if (bEl) { bEl.value = schedClipboard.br;    st[`${day}_break`] = bEl.value || ''; }
            updatePresetSaveState();
          }
        });
      }

      ['input', 'change'].forEach(evt => {
        root?.addEventListener(evt, (e) => {
          const t = e.target;
          if (!t?.name) return;
          if (t.name === 'rp_pay_mode' || t.name === 'rp_rate_type') return;
          if (/^(name|role|band|display_site)$/.test(t.name)) {
            st[t.name] = t.value || '';
          } else if (/^bucket_(day|night|sat|sun|bh)$/.test(t.name)) {
            st[t.name] = t.value || '';
          } else if (/^mileage_(pay|charge)_rate$/.test(t.name)) {
            st[t.name] = t.value || '';
          } else if (/^(mon|tue|wed|thu|fri|sat|sun)_(start|end|break)$/.test(t.name)) {
            st[t.name] = t.value || '';
          } else if (/^(paye|umb|charge)_(day|night|sat|sun|bh)$/.test(t.name)) {
            st[t.name] = t.value || '';
          }
          if (
            /^(paye|umb|charge)_(day|night|sat|sun|bh)$/.test(t.name) ||
            /^(name|role|band|display_site|bucket_(day|night|sat|sun|bh)|mileage_(pay|charge)_rate)$/.test(t.name) ||
            /^(mon|tue|wed|thu|fri|sat|sun)_(start|end|break)$/.test(t.name)
          ) {
            updatePresetSaveState();
          }
        }, true);
      });

      updatePresetSaveState();
    },
    {
      kind: 'rate-preset',
      noParentGate: true,
      forceEdit: initialMode === 'edit'
    }
  );

  setTimeout(() => {
    const fr = window.__getModalFrame?.();
    if (fr && fr.kind === 'rate-preset' && typeof fr.onReturn === 'function' && !fr.__init__) {
      fr.__init__ = true;
      fr.onReturn();
    }
  }, 0);
}
// ─────────────────────────────────────────────────────────────────────────────
// Rates Presets — API wrappers
// ─────────────────────────────────────────────────────────────────────────────

async function listRatePresets({ scope, client_id, q } = {}) {
  const qs = new URLSearchParams();
  if (scope && scope !== 'ALL') qs.set('scope', String(scope).toUpperCase()); // 'GLOBAL' | 'CLIENT'
  if (client_id) qs.set('client_id', String(client_id));
  if (q) qs.set('q', String(q));
  const url = API(`/api/rates/presets${qs.toString() ? `?${qs.toString()}` : ''}`);
  const r = await authFetch(url);
  const j = await r.json().catch(()=>({ rows: [] }));
  // Return array of full rows (endpoint returns extended shape)
  const rows =
    (Array.isArray(j) ? j :
     Array.isArray(j.rows) ? j.rows :
     Array.isArray(j.data) ? j.data : []);
  return rows;
}

async function deleteRatePreset(id) {
  const r = await authFetch(API(`/api/rates/presets/${encodeURIComponent(String(id))}`), { method:'DELETE' });
  if (!r.ok) throw new Error(await r.text().catch(()=> 'Delete failed'));
  return true;
}

async function loadRatePreset(id) {
  const r = await authFetch(API(`/api/rates/presets/${encodeURIComponent(String(id))}`));
  if (!r.ok) throw new Error(await r.text().catch(()=> 'Failed to load'));
  const j = await r.json().catch(()=> ({}));
  return (j && (j.preset || j.row || j)) || {};
}

async function saveRatePreset(payload /* { id?, ... } */) {
  const hasId = !!payload?.id;
  const url   = hasId
    ? API(`/api/rates/presets/${encodeURIComponent(String(payload.id))}`)
    : API(`/api/rates/presets`);
  const method = hasId ? 'PATCH' : 'POST';

  const body = { ...payload };
  // Do not send id inside body on PATCH
  if (hasId) delete body.id;

  const r = await authFetch(url, {
    method,
    headers: { 'content-type':'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(await r.text().catch(()=> 'Save failed'));
  return r.json().catch(()=> ({}));
}

// ─────────────────────────────────────────────────────────────────────────────
// Stable sort helper per spec
// ─────────────────────────────────────────────────────────────────────────────
function sortPresetsForView(scopeFilter /* 'ALL'|'GLOBAL'|'CLIENT' */, rows = []) {
  const arr = (rows || []).map((r, i) => ({ r, i })); // keep stable index
  const name = (x) => (x?.name || '').toString().toLowerCase();
  const cli  = (x) => (x?.client?.name || x?.client_name || '').toString().toLowerCase();
  const isGlobal = (x) => !x?.client_id && (String(x?.scope || '').toUpperCase() !== 'CLIENT');

  const cmpStr = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

  arr.sort((A,B) => {
    const a = A.r, b = B.r;
    if (scopeFilter === 'GLOBAL') {
      const c = cmpStr(name(a), name(b));
      return c !== 0 ? c : (A.i - B.i);
    }
    if (scopeFilter === 'CLIENT') {
      const c1 = cmpStr(cli(a), cli(b));
      if (c1 !== 0) return c1;
      const c2 = cmpStr(name(a), name(b));
      return c2 !== 0 ? c2 : (A.i - B.i);
    }
    // ALL → globals first, then client name, then rate name
    const gA = isGlobal(a) ? 0 : 1;
    const gB = isGlobal(b) ? 0 : 1;
    if (gA !== gB) return gA - gB;
    const c1 = cmpStr(cli(a), cli(b));
    if (c1 !== 0) return c1;
    const c2 = cmpStr(name(a), name(b));
    return c2 !== 0 ? c2 : (A.i - B.i);
  });

  return arr.map(x => x.r);
}




async function listReportPresets({ section, kind = 'search', include_shared = true, q, page = 1, page_size = 100 } = {}) {
  const opts = { include_shared, q, page, page_size };
  const cached = getPresetCache(section, kind, opts);
  if (cached) return cached;

  const qs = new URLSearchParams();
  if (section) qs.set('section', section);
  if (kind) qs.set('kind', kind);
  if (include_shared) qs.set('include_shared', 'true');
  if (q) qs.set('q', q);
  qs.set('page', page);
  qs.set('page_size', page_size);

  const res = await authFetch(API(`/api/report-presets?${qs.toString()}`));
  const data = await res.json().catch(() => ({ rows: [] }));
  // Keep user_id in cache so ownership checks work downstream
  const rows = data && Array.isArray(data.rows) ? data.rows : [];
  setPresetCache(section, kind, rows, opts);
  return rows;
}

async function createReportPreset({ section, kind='search', name, filters, selection, is_shared=false, is_default=false }) {
  const res = await authFetch(
    API(`/api/report-presets`),
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ section, kind, name, filters, selection, is_shared, is_default })
    }
  );

  if (res.status === 409) {
    let conflicting = null;
    try {
      const presets = await listReportPresets({ section, kind, include_shared: false, q: name, page: 1, page_size: 100 });
      const lower = String(name || '').toLowerCase();
      conflicting = (presets || []).find(p => String(p.name || '').toLowerCase() === lower) || null;
    } catch {}

    try {
      const form = document.getElementById('saveSearchForm');
      if (form) {
        const overwriteRadio = form.querySelector('input[name="mode"][value="overwrite"]') || form.querySelector('input[name="mode"][value="append"]');
        const overwriteWrap  = form.querySelector('#overwriteWrap') || form.querySelector('#appendWrap');
        const selectEl       = form.querySelector('#overwritePresetId') || form.querySelector('#appendPresetId');
        if (overwriteRadio) overwriteRadio.checked = true;
        if (overwriteWrap)  overwriteWrap.style.display = 'block';
        if (selectEl && conflicting) {
          const hasOption = Array.from(selectEl.options).some(o => o.value === String(conflicting.id));
          if (!hasOption) {
            const opt = document.createElement('option');
            opt.value = String(conflicting.id);
            opt.textContent = conflicting.name || '(unnamed)';
            selectEl.appendChild(opt);
          }
          selectEl.value = String(conflicting.id);
        }
      }
    } catch {}

    const err = new Error('Preset name already exists. Switched to Overwrite—pick the preset and save again.');
    err.code = 'PRESET_NAME_CONFLICT';
    if (conflicting) err.preset = conflicting;
    err.section = section;
    err.kind = kind;
    throw err;
  }

  if (!res.ok) throw new Error(await res.text());

  invalidatePresetCache(section, kind);
  const data = await res.json().catch(()=>({}));
  return data.row || null;
}


async function updateReportPreset({ id, name, filters, selection, is_shared, is_default, section, kind }) {
  const patch = {};
  if (typeof name === 'string') patch.name = name;
  if (filters && typeof filters === 'object') patch.filters = filters;
  if (selection && typeof selection === 'object') patch.selection = selection;
  if (typeof is_shared === 'boolean') patch.is_shared = is_shared;
  if (typeof is_default === 'boolean') patch.is_default = is_default;
  if (typeof section === 'string') patch.section = section;
  if (typeof kind === 'string') patch.kind = kind;

  const res = await authFetch(
    API(`/api/report-presets/${encodeURIComponent(id)}`),
    {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(patch)
    }
  );
  if (!res.ok) throw new Error(await res.text());
  __PRESETS_CACHE__.clear();
  const data = await res.json().catch(()=>({}));
  return data.row || null;
}

async function deleteReportPreset(id) {
  const res = await authFetch(API(`/api/report-presets/${encodeURIComponent(id)}`), { method: 'DELETE' });
  if (!res.ok) throw new Error(await res.text());
  __PRESETS_CACHE__.clear();
  const data = await res.json().catch(()=> ({}));
  return data.deleted_id || id;
}

// -----------------------------
// Search helpers
// -----------------------------
function extractFiltersFromForm(formSel='#searchForm'){
  const raw = collectForm(formSel, false);

  // Convert select[multiple] to array and booleans from "true"/"false"
  Object.keys(raw).forEach(k=>{
    const el = document.querySelector(`${formSel} [name="${k}"]`);
    if (!el) return;
    if (el.tagName==='SELECT' && el.multiple){
      raw[k] = Array.from(el.selectedOptions).map(o=>o.value);
    }
    if (el.tagName==='SELECT' && (el.value === 'true' || el.value === 'false')){
      raw[k] = (el.value === 'true');
    }
    if (el.type === 'number' && raw[k] === '') raw[k] = null;
    if (raw[k] === '') delete raw[k];
  });

  // Convert UK dates → ISO (backend expects ISO)
  const dateFields = [
    'created_from','created_to',
    'updated_from','updated_to',
    'worked_from','worked_to',
    'week_ending_from','week_ending_to',
    'issued_from','issued_to',
    'due_from','due_to',
    'dob',
    'start_date_from','start_date_to',
    'end_date_from','end_date_to',
    'active_on'
  ];

  dateFields.forEach(f => {
    if (raw[f]) {
      const iso = parseUkDateToIso(raw[f]);
      if (iso) raw[f] = iso;
    }
  });

  return raw;
}

function populateSearchFormFromFilters(filters={}, formSel='#searchForm'){
  const form = document.querySelector(formSel);
  if (!form) return;

  const dateFields = [
    'created_from','created_to',
    'updated_from','updated_to',
    'worked_from','worked_to',
    'week_ending_from','week_ending_to',
    'issued_from','issued_to',
    'due_from','due_to',
    'dob',
    'start_date_from','start_date_to',
    'end_date_from','end_date_to',
    'active_on'
  ];

  for (const [k,v] of Object.entries(filters || {})) {
    const el = form.querySelector(`[name="${k}"]`);
    if (!el) continue;

    if (Array.isArray(v) && el.tagName === 'SELECT' && el.multiple) {
      const values = new Set(v.map(String));
      Array.from(el.options).forEach(opt => { opt.selected = values.has(opt.value); });
      continue;
    }

    if (typeof v === 'boolean' && el.tagName === 'SELECT') {
      el.value = v ? 'true' : 'false';
      continue;
    }

    const isDateField = dateFields.includes(k);
    if (isDateField && typeof v === 'string') {
      const uk = (typeof formatIsoToUk === 'function') ? formatIsoToUk(v) : v;
      el.value = uk || '';
      continue;
    }

    // Default
    el.value = (v == null ? '' : String(v));
  }
}



// Build querystring per section


// -----------------------------
// UPDATED: search()
// -----------------------------
// ======================================
// FRONTEND — search (UPDATED: no extra logic beyond existing; kept for completeness)
// ======================================

// ──────────────────────────────────────────────────────────────────────────────
// FIX 1: Search route mismatch (contracts now calls /api/contracts with filters)
// ──────────────────────────────────────────────────────────────────────────────
async function search(section, filters = {}) {
  window.__listState = window.__listState || {};
  const st = (window.__listState[section] ||= {
    page: 1,
    pageSize: 50,
    total: null,
    hasMore: false,
    filters: null,
    sort: { key: null, dir: 'asc' }
  });

  if (!st.sort || typeof st.sort !== 'object') {
    st.sort = { key: null, dir: 'asc' };
  }

  // Reset selection when applying a new dataset (fingerprint change)
  window.__selection = window.__selection || {};
  const sel = (window.__selection[section] ||= { fingerprint:'', ids:new Set() });
  sel.fingerprint = JSON.stringify({
    section,
    filters: filters || {},
    sort: st.sort
  });
  sel.ids.clear();

  // Default mappings for existing sections
  const map = {
    candidates:'/api/search/candidates',
    clients:'/api/search/clients',
    umbrellas:'/api/search/umbrellas',
    timesheets:'/api/search/timesheets',
    invoices:'/api/search/invoices'
  };

  // Contracts use /api/contracts (admin list)
  let p = (section === 'contracts') ? '/api/contracts' : map[section];
  if (!p) return [];

  const qs = buildSearchQS(section, filters);
  const url = qs ? `${p}?${qs}` : p;

  const r = await authFetch(API(url));
  const rows = toList(r);

  // update state
  st.filters = { ...(filters || {}) };
  const ps = (st.pageSize === 'ALL') ? null : Number(st.pageSize || 50);
  st.hasMore = (ps != null) ? (Array.isArray(rows) && rows.length === ps) : false;

  return rows;
}


function defaultColumnsFor(section){
  // No longer read localStorage; server grid prefs are the source of truth.
  switch(section){
    case 'candidates':
      return ['last_name','first_name','phone','role','postcode','email'];
    case 'clients':
      return ['name','primary_invoice_email','invoice_address','postcode','ap_phone'];
    case 'umbrellas':
      return ['name','vat_chargeable','bank_name','sort_code','account_number','enabled'];
    case 'audit':
      return ['type','to','subject','status','created_at_utc','last_error'];
    case 'contracts':
      // Sensible defaults for the new section
      return ['candidate_display','client_name','role','band','pay_method_snapshot','default_submission_mode','start_date','end_date','bucket_labels_preview'];
    default:
      return ['id'];
  }
}






// ──────────────────────────────────────────────────────────────────────────────
// Data / API wrappers — Contracts + Weeks + Utilities
// ─────────────────────────────────────────────────────────────────────────────-

const _enc = (v) => encodeURIComponent(String(v ?? ''));
const _json = (o) => JSON.stringify(o ?? {});

async function listContracts(filters = {}) {
  // Mirrors /api/contracts (admin list) with common FE filters.
  const qs = new URLSearchParams();
  if (filters.candidate_id) qs.set('candidate_id', filters.candidate_id);
  if (filters.client_id)    qs.set('client_id',    filters.client_id);
  if (filters.role)         qs.set('role',         filters.role);
  if (filters.band != null) qs.set('band',         filters.band);
  if (filters.pay_method_snapshot) qs.set('pay_method_snapshot', String(filters.pay_method_snapshot).toUpperCase());
  if (filters.active_on)    qs.set('active_on',    filters.active_on);  // YYYY-MM-DD
  if (typeof filters.auto_invoice === 'boolean') qs.set('auto_invoice', String(filters.auto_invoice));
  if (filters.status) qs.set('status', String(filters.status)); // NEW

  const url = qs.toString() ? `/api/contracts?${qs}` : `/api/contracts`;
  const r = await authFetch(API(url));
  return toList(r);
}


// ✅ CHANGED: add cache-busting and explicit no-cache header

async function getContract(contract_id) {
  const url = API(`/api/contracts/${_enc(contract_id)}?ts=${Date.now()}`);
  const r = await authFetch(url);
  if (!r?.ok) return null;
  const j = await r.json();
  return j && j.contract ? j.contract : j;
}

async function upsertContract(payload, id /* optional */) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : true;
  const patch = { ...payload };

  // Ensure required window fields are present for PUT without re-clobber.
  if (id && patch.__userChangedDates !== true) {
    const snap = (window.modalCtx && window.modalCtx.data) || {};
    if (!patch.start_date && snap.start_date) patch.start_date = snap.start_date;
    if (!patch.end_date   && snap.end_date)   patch.end_date   = snap.end_date;
  }
  if ('__userChangedDates' in patch) delete patch.__userChangedDates;

  if ('bucket_labels_json' in patch) {
    const norm = normaliseBucketLabelsInput(patch.bucket_labels_json);
    patch.bucket_labels_json = (norm === false) ? null : norm;
  }

  const BUCKETS = [
    'paye_day','paye_night','paye_sat','paye_sun','paye_bh',
    'umb_day','umb_night','umb_sat','umb_sun','umb_bh',
    'charge_day','charge_night','charge_sat','charge_sun','charge_bh'
  ];

  const method = id ? 'PUT' : 'POST';
  const url = id ? `/api/contracts/${_enc(id)}` : `/api/contracts`;

  try {
    const currentTab = (window.modalCtx && window.modalCtx.currentTabKey) || null;
    const baseRates = (window.modalCtx && window.modalCtx.data && window.modalCtx.data.rates_json) || {};
    const incoming  = (patch.rates_json && typeof patch.rates_json === 'object') ? patch.rates_json : {};

    if (id) {
      const merged = { ...baseRates };
      for (const k of BUCKETS) {
        const n = Number(incoming[k]);
        if (Number.isFinite(n)) {
          merged[k] = n;
        } else if (merged[k] !== undefined) {
          merged[k] = Number(merged[k]);
        }
      }
      patch.rates_json = merged;
    }

    // Prune PAYE vs Umbrella buckets according to pay_method_snapshot
    try {
      const pm = String(patch.pay_method_snapshot || '').toUpperCase();
      if (patch.rates_json && typeof patch.rates_json === 'object') {
        const keepPrefixes =
          pm === 'PAYE'     ? ['paye_','charge_'] :
          pm === 'UMBRELLA' ? ['umb_','charge_'] :
                              ['charge_'];
        for (const key of Object.keys(patch.rates_json)) {
          if (!keepPrefixes.some(pre => key.startsWith(pre))) {
            delete patch.rates_json[key];
          }
        }
      }
    } catch (e) {
      if (LOGC) console.warn('[CONTRACTS][UPSERT] rate pruning failed (non-fatal)', e);
    }

    if (LOGC) {
      console.groupCollapsed('[CONTRACTS][UPSERT] sending');
      console.log('method', method, 'url', API(url));
      console.log('currentTab', currentTab);
      console.log('payload (final)', patch);
      if (id) console.log('baseRates (from modalCtx.data.rates_json)', baseRates);
      console.groupEnd();
    }
  } catch (e) {
    if (LOGC) console.warn('[CONTRACTS][UPSERT] logging/pre-seed failed', e);
  }

  const res = await authFetch(API(url), {
    method,
    headers: { 'content-type': 'application/json' },
    body: _json(patch)
  });

  let data = null;
  try { data = await res.json(); } catch (_) {}

  if (!res || !res.ok) {
    const msg =
      (data && (data.error || data.message || data.detail)) ||
      (res && res.statusText) ||
      `Contract ${id ? 'update' : 'create'} failed`;
    if (LOGC) console.error('[CONTRACTS][UPSERT] error', { status: res?.status, msg, data });
    throw new Error(msg);
  }

  if (LOGC) {
    console.log('[CONTRACTS][UPSERT] success', { method, id, status: res.status });
    if (data) console.log('[CONTRACTS][UPSERT] response body', data);
  }

  // Merge saved contract into list cache so reopen uses fresh values
  try {
    const savedContract = (data && (data.contract || data)) || null;
    const savedId = savedContract && savedContract.id;
    if (savedId && Array.isArray(window.currentRows)) {
      const idx = window.currentRows.findIndex(r => String(r.id) === String(savedId));
      if (idx >= 0) {
        window.currentRows[idx] = { ...window.currentRows[idx], ...savedContract };
      }
    }
  } catch (e) {
    if (LOGC) console.warn('[CONTRACTS][UPSERT] list cache merge failed', e);
  }

  return data;
}


// ✅ CHANGED: after successful upsert, also merge into currentRows and stamp recency


async function deleteContract(contract_id) {
  const r = await authFetch(API(`/api/contracts/${_enc(contract_id)}`), { method: 'DELETE' });
  if (!r?.ok) throw new Error('Delete contract failed');
  return r.json();
}
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: checkContractOverlap (adds logging)
// ─────────────────────────────────────────────────────────────────────────────
async function checkContractOverlap(payload /* {candidate_id,start_date,end_date,ignore_contract_id?} */) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;
  if (LOGC) console.log('[CONTRACTS] POST /api/contracts/check-overlap', payload);
  const r = await authFetch(API(`/api/contracts/check-overlap`), {
    method: 'POST', headers: { 'content-type':'application/json' }, body: _json(payload)
  });
  if (!r?.ok) {
    if (LOGC) console.error('[CONTRACTS] overlap check failed', r);
    throw new Error('Overlap check failed');
  }
  const json = await r.json();
  if (LOGC) console.log('[CONTRACTS] overlap check OK', json);
  return json;
}

async function generateContractWeeks(contract_id) {
  const r = await authFetch(API(`/api/contracts/${_enc(contract_id)}/generate-weeks`), { method: 'POST' });
  if (!r?.ok) throw new Error('Generate weeks failed');
  return r.json();
}


async function listContractWeeks(contract_id, filters = {}) {
  const qs = new URLSearchParams();
  if (contract_id) qs.set('contract_id', contract_id);
  if (filters.status) qs.set('status', filters.status);
  if (filters.submission_mode_snapshot) qs.set('submission_mode_snapshot', filters.submission_mode_snapshot);
  if (filters.week_ending_from) qs.set('week_ending_from', filters.week_ending_from);
  if (filters.week_ending_to)   qs.set('week_ending_to',   filters.week_ending_to);
  const url = qs.toString() ? `/api/contract-weeks?${qs}` : `/api/contract-weeks`;
  const r = await authFetch(API(url));
  return toList(r);
}

async function contractWeekSwitchMode(week_id, newMode /* optional; server toggles if omitted */) {
  // Backend toggles if no body; allow hint mode to be explicit
  const init = newMode
    ? { method:'POST', headers:{'content-type':'application/json'}, body:_json({ submission_mode_snapshot: String(newMode).toUpperCase() }) }
    : { method:'POST' };
  const r = await authFetch(API(`/api/contract-weeks/${_enc(week_id)}/switch-mode`), init);
  if (!r?.ok) throw new Error('Switch mode failed');
  return r.json();
}

async function contractWeekPresignPdf(week_id) {
  const r = await authFetch(API(`/api/contract-weeks/${_enc(week_id)}/presign-manual-pdf`), { method:'POST' });
  if (!r?.ok) throw new Error('Presign failed');
  return r.json(); // { key, upload_url, token, expires_in }
}

async function contractWeekReplacePdf(week_id, r2_key) {
  const r = await authFetch(API(`/api/contract-weeks/${_enc(week_id)}/replace-manual-pdf`), {
    method:'POST', headers:{'content-type':'application/json'}, body:_json({ r2_key })
  });
  if (!r?.ok) throw new Error('Replace manual PDF failed');
  return r.json();
}

async function contractWeekManualUpsert(week_id, payload /* { hours or day_entries_json, reference_number? } */) {
  // Ensure numeric 5-bucket totals if passed in
  if (payload?.hours) {
    const h = payload.hours;
    payload.hours = {
      day: Number(h?.day || 0), night: Number(h?.night || 0),
      sat: Number(h?.sat || 0), sun: Number(h?.sun || 0), bh: Number(h?.bh || 0)
    };
  }
  const r = await authFetch(API(`/api/contract-weeks/${_enc(week_id)}/manual-upsert`), {
    method:'POST', headers:{'content-type':'application/json'}, body:_json(payload)
  });
  if (!r?.ok) throw new Error('Manual upsert failed');
  return r.json(); // { timesheet_id, processing_status, hours, had_day_entries }
}

async function contractWeekAuthorise(week_id) {
  const r = await authFetch(API(`/api/contract-weeks/${_enc(week_id)}/manual-authorise`), { method:'POST' });
  if (!r?.ok) throw new Error('Authorise failed');
  return r.json();
}

async function contractWeekDeleteTimesheet(week_id) {
  const r = await authFetch(API(`/api/contract-weeks/${_enc(week_id)}/timesheet`), { method:'DELETE' });
  if (!r?.ok) throw new Error('Delete timesheet failed');
  return r.json();
}

async function contractWeekCreateExpenseSheet(week_id) {
  const r = await authFetch(API(`/api/contract-weeks/${_enc(week_id)}/create-expense-sheet`), { method:'POST' });
  if (!r?.ok) throw new Error('Create expense sheet failed');
  return r.json();
}

// Minimal picker feed
async function listContractsBasic() {
  const rows = await listContracts({});
  return (rows || []).map(c => ({
    id: c.id,
    label: [
      (c.candidate_display || c.candidate_id || '').toString(),
      '—',
      (c.client_name || c.client_id || '').toString(),
      (c.role ? `(${c.role}${c.band ? ` ${c.band}` : ''})` : ''),
      ' ',
      `[${c.start_date || ''} → ${c.end_date || ''}]`
    ].join(' ').replace(/\s+/g, ' ').trim()
  }));
}


// ──────────────────────────────────────────────────────────────────────────────
// Bucket label helpers (display-only; math stays canonical)
// ─────────────────────────────────────────────────────────────────────────────-

function labelsDefault() {
  return { day:'Day', night:'Night', sat:'Sat', sun:'Sun', bh:'BH' };
}

/**
 * @returns {object|false} normalized labels object or false if invalid (to clear)
 */
function normaliseBucketLabelsInput(raw) {
  if (!raw || typeof raw !== 'object') return false;
  const keys = ['day','night','sat','sun','bh'];
  const out = {};
  for (const k of keys) {
    const v = raw[k];
    if (typeof v !== 'string' || !v.trim()) return false;
    out[k] = v.trim();
  }
  return out;
}

function getBucketLabelsForContract(contract) {
  const userSet = normaliseBucketLabelsInput(contract?.bucket_labels_json || null);
  return userSet || labelsDefault();
}

function summariseBucketLabels(labels) {
  const L = normaliseBucketLabelsInput(labels);
  return L ? [L.day, L.night, L.sat, L.sun, L.bh].join('/') : '';
}

function applyBucketLabelsToHoursGrid(gridEl, labels) {
  const L = normaliseBucketLabelsInput(labels) || labelsDefault();
  if (!gridEl) return;
  const map = {
    day:   gridEl.querySelector('[data-bucket="day"] .lbl'),
    night: gridEl.querySelector('[data-bucket="night"] .lbl'),
    sat:   gridEl.querySelector('[data-bucket="sat"] .lbl'),
    sun:   gridEl.querySelector('[data-bucket="sun"] .lbl'),
    bh:    gridEl.querySelector('[data-bucket="bh"] .lbl'),
  };
  Object.entries(map).forEach(([k, el]) => { if (el) el.textContent = L[k]; });
}

function renderBucketLabelsEditor(ctx /* modalCtx */) {
  // Prefer staged labels from formState.main; fallback to contract's stored labels; finally to defaults
  const fsMain = (window.modalCtx && window.modalCtx.formState && window.modalCtx.formState.main) || {};
  const stored = getBucketLabelsForContract(ctx.data || {});
  const L = {
    day:   fsMain.bucket_day   ?? stored.day   ?? 'Day',
    night: fsMain.bucket_night ?? stored.night ?? 'Night',
    sat:   fsMain.bucket_sat   ?? stored.sat   ?? 'Sat',
    sun:   fsMain.bucket_sun   ?? stored.sun   ?? 'Sun',
    bh:    fsMain.bucket_bh    ?? stored.bh    ?? 'BH',
  };
  return `
    <div class="group">
      <div class="row"><label>Bucket labels (optional)</label>
        <div class="controls small">
          <div class="grid-5" id="bucketLabelsGrid">
            <div data-k="day"><span>Standard</span><input class="input" type="text" name="bucket_day"   value="${(L.day||'Day')}" /></div>
            <div data-k="night"><span>OT1</span>     <input class="input" type="text" name="bucket_night" value="${(L.night||'Night')}" /></div>
            <div data-k="sat"><span>OT2</span>       <input class="input" type="text" name="bucket_sat"   value="${(L.sat||'Sat')}" /></div>
            <div data-k="sun"><span>OT3</span>       <input class="input" type="text" name="bucket_sun"   value="${(L.sun||'Sun')}" /></div>
            <div data-k="bh"><span>OT4</span>        <input class="input" type="text" name="bucket_bh"    value="${(L.bh||'BH')}" /></div>
          </div>
        </div>
      </div>
    </div>`;
}


function _collectBucketLabelsFromForm(rootSel = '#contractForm') {
  const root = document.querySelector(rootSel);
  if (!root) return null;
  const day   = root.querySelector('input[name="bucket_day"]')?.value?.trim();
  const night = root.querySelector('input[name="bucket_night"]')?.value?.trim();
  const sat   = root.querySelector('input[name="bucket_sat"]')?.value?.trim();
  const sun   = root.querySelector('input[name="bucket_sun"]')?.value?.trim();
  const bh    = root.querySelector('input[name="bucket_bh"]')?.value?.trim();
  const raw = { day, night, sat, sun, bh };
  // If all empty → treat as null; if partially filled → require full 5, else clear to null
  const filled = Object.values(raw).filter(Boolean).length;
  if (filled === 0) return null;
  const norm = normaliseBucketLabelsInput(raw);
  return norm || null;
}


// ──────────────────────────────────────────────────────────────────────────────
// Overlap guard flow
// ─────────────────────────────────────────────────────────────────────────────-

// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: preSaveContractWithOverlapCheck (adds logging)
// ─────────────────────────────────────────────────────────────────────────────
async function preSaveContractWithOverlapCheck(formData /* object */) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;
  const payload = {
    candidate_id: formData.candidate_id,
    start_date:   formData.start_date,
    end_date:     formData.end_date,
    ignore_contract_id: formData.id || null
  };
  if (LOGC) console.log('[CONTRACTS] overlap check → request', payload);
  const res = await checkContractOverlap(payload);
  if (LOGC) console.log('[CONTRACTS] overlap check → response', res);
  if (!res?.has_overlap) return true;
  const ok = await showContractOverlapWarningDialog(res.overlaps || []);
  if (LOGC) console.log('[CONTRACTS] overlap dialog → userChoice', ok);
  return !!ok;
}

function showContractOverlapWarningDialog(overlaps = []) {
  // Returns a Promise<boolean> that resolves when user chooses.
  return new Promise((resolve) => {
    const list = overlaps.map(o => `
      <li>
        <div><b>${(o.client_name || o.client_id || '')}</b> — ${o.role || ''}${o.band ? ` (Band ${o.band})` : ''}</div>
        <div class="mini">Existing: ${o.existing_start_date} → ${o.existing_end_date}</div>
        <div class="mini">Overlap: <b>${o.overlap_start_date} → ${o.overlap_end_date}</b> (${o.overlap_days} day(s))</div>
      </li>`).join('');

    const content = `
      <div class="warn">
        <p>The proposed dates overlap the following contract(s) for this candidate:</p>
        <ul class="overlap-list">${list || '<li>(none)</li>'}</ul>
        <p>Do you want to proceed anyway?</p>
      </div>`;

    showModal(
      'Overlap detected',
      [{ key: 'ov', title: 'Warning'}],
      () => content,
      async () => { resolve(true); return true; },
      false,
      () => {}, // onReturn
      {
        kind:'overlap-warning',
        extraButtons: [
          { label:'Cancel', role:'secondary', onClick: () => { resolve(false); discardTopModal && discardTopModal(); } }
        ]
      }
    );
  });
}
function confirmProceedWithOverlap(){ /* kept for API parity; handled in modal above */ return Promise.resolve(true); }
function cancelOverlapSave(){ /* kept for API parity; handled in modal above */ return Promise.resolve(false); }


// ──────────────────────────────────────────────────────────────────────────────
// UI — Contracts section (table + modal tabs)
// ─────────────────────────────────────────────────────────────────────────────-

function renderContractsTable(rows) {
  // Custom lightweight renderer that highlights <Unassigned> in red.
  // If your environment prefers the generic grid, you can still call that instead.
  // This function simply returns a DOM node that your caller can insert.

  const make = (tag, attrs = {}, children = []) => {
    const el = document.createElement(tag);
    Object.entries(attrs).forEach(([k, v]) => {
      if (k === 'style' && typeof v === 'object') {
        Object.assign(el.style, v);
      } else if (k === 'className') {
        el.className = v;
      } else {
        el.setAttribute(k, v);
      }
    });
    (Array.isArray(children) ? children : [children]).forEach(ch => {
      if (ch == null) return;
      if (typeof ch === 'string') el.appendChild(document.createTextNode(ch));
      else el.appendChild(ch);
    });
    return el;
  };

  const wrap = make('div');
  const style = make('style', {}, `
    .contracts-table { width:100%; border-collapse:collapse; font-size:13px; }
    .contracts-table th, .contracts-table td { border:1px solid var(--line,#e5e5e5); padding:8px; text-align:left; }
    .contracts-table th { background: var(--panel,#fafafa); }
    .contracts-table .unassigned { color: var(--danger,#c0392b); font-weight: 600; }
    .contracts-table .sm { color: var(--muted,#666); font-size: 12px; }
  `);
  wrap.appendChild(style);

  const table = make('table', { className: 'contracts-table' });
  const thead = make('thead');
  const trh = make('tr');
  ['ID','Candidate','Client','Role','Start','End'].forEach(h => trh.appendChild(make('th', {}, h)));
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = make('tbody');

  (Array.isArray(rows) ? rows : []).forEach(r => {
    const tr = make('tr');

    // ID
    tr.appendChild(make('td', {}, String(r.id ?? '')));

    // Candidate (show <Unassigned> in red if missing)
    const candLabel = (r.candidate_display || r.candidate_name || '').trim();
    if (candLabel) {
      tr.appendChild(make('td', {}, candLabel));
    } else {
      const td = make('td');
      const span = make('span', { className: 'unassigned' }, '<Unassigned>');
      td.appendChild(span);
      tr.appendChild(td);
    }

    // Client
    tr.appendChild(make('td', {}, (r.client_name || '').trim()));

    // Role
    tr.appendChild(make('td', {}, (r.role || '').trim()));

    // Start / End (show raw ISO or formatted upstream)
    tr.appendChild(make('td', {}, (r.start_date || '')));
    tr.appendChild(make('td', {}, (r.end_date || '')));

    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  wrap.appendChild(table);

  return wrap;
}

// ──────────────────────────────────────────────────────────────────────────────
// FIX 6: Add guarded Delete entry point inside openContract (plus std_hours_json save)
// (Delete only if unused; handled by backend; no change to global openDelete())
// ──────────────────────────────────────────────────────────────────────────────
// ──────────────────────────────────────────────────────────────────────────────
// openContract (amended) — surface PAY_METHOD_MISMATCH warnings after save
// ──────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: openContract (Rates tab enabled on create; picker wiring always-on;
// typing in Candidate/Client field will open the picker; rich logging)
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: openContract — adds initial onReturn() kick so Pick buttons are wired
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: openContract(row)
// (unchanged logic except it opens the updated pickers; initial onReturn retained)
// ─────────────────────────────────────────────────────────────────────────────

// ✅ CHANGED: honour fresh row; give the modal an openToken for stable formState binding

function openContract(row) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : true;
  const isCreate = !row || !row.id;
  if (LOGC) console.log('[CONTRACTS] openContract ENTRY', { isCreate, rowPreview: !!row });

   window.modalCtx = {
    entity: 'contracts',
    mode: isCreate ? 'create' : 'view',
    data: { ...(row || {}) },
    _saveInFlight: false
  };

  const preToken = window.__preOpenToken || null;
  if (LOGC) console.log('[CONTRACTS] preOpenToken snapshot', preToken);

  if (isCreate) {
    if (preToken) {
      window.modalCtx.openToken = preToken;
      try { delete window.__preOpenToken; } catch {}
      if (LOGC) console.log('[CONTRACTS] using preOpenToken for create', preToken);
    } else if (!window.modalCtx.openToken) {
      window.modalCtx.openToken = `contract:new:${Date.now()}:${Math.random().toString(36).slice(2)}`;
      if (LOGC) console.log('[CONTRACTS] openToken issued for create', window.modalCtx.openToken);
    }
  }

  // If this create comes from Clone&Extend staging, pull intent (end-old etc.)
  try {
    const intents = (window.__cloneIntents || {});
    const token   = window.modalCtx.openToken;
    const ci      = intents[token];

    if (LOGC) console.groupCollapsed('[CLONE][attach-intent]');
    if (LOGC) console.log('openToken', token);
    if (LOGC) console.log('staging.keys', Object.keys(intents || {}));
    if (LOGC) console.log('staging.has(openToken)?', Object.prototype.hasOwnProperty.call(intents, token));

    if (ci) {
      // Normalise & echo intent
      const endIso = ci.end_existing_on || null;
      window.modalCtx.__cloneIntent = {
        source_contract_id: ci.source_contract_id || null,
        end_existing: !!ci.end_existing,
        end_existing_on: endIso
      };
      if (LOGC) console.log('ATTACHED', window.modalCtx.__cloneIntent);
      // one-shot: keep it only on this modal
      try { delete intents[token]; if (LOGC) console.log('intent cleared from staging bucket'); } catch {}
    } else {
      if (LOGC) console.log('NO_INTENT_FOR_TOKEN (possible token mismatch) – will not truncate tail unless a later step re-attaches.');
    }
    if (LOGC) console.groupEnd?.();
  } catch (e) {
    if (LOGC) console.warn('[CLONE][attach-intent] EXCEPTION', e);
  }

  try {
    const base = window.modalCtx.data || {};

    const fs = (window.modalCtx.formState ||= { __forId: (base.id ?? window.modalCtx.openToken ?? null), main:{}, pay:{} });
    fs.__forId = preToken || fs.__forId || (base.id ?? window.modalCtx.openToken ?? null);
    if (LOGC) console.log('[CONTRACTS] formState forId bound', { preToken, forId: fs.__forId, openToken: window.modalCtx.openToken });

    const m = (fs.main ||= {});
    if (m.__seeded !== true) {
      if (base.candidate_id != null) m.candidate_id = base.candidate_id;
      if (base.client_id != null)    m.client_id    = base.client_id;
      if (base.role != null)         m.role         = base.role;
      if (base.band != null)         m.band         = base.band;
      if (base.display_site != null) m.display_site = base.display_site;
      if (base.start_date)           m.start_date   = base.start_date;
      if (base.end_date)             m.end_date     = base.end_date;
      if (base.pay_method_snapshot)  m.pay_method_snapshot = base.pay_method_snapshot;
      if (base.default_submission_mode) m.default_submission_mode = base.default_submission_mode;
      if (base.week_ending_weekday_snapshot != null) m.week_ending_weekday_snapshot = String(base.week_ending_weekday_snapshot);
      if (base.bucket_labels_json)   m.__bucket_labels = base.bucket_labels_json;
      if (base.std_schedule_json)    m.__template      = base.std_schedule_json;
      if (base.std_hours_json)       m.__hours         = base.std_hours_json;
      // Seed mileage if present on row
      if (base.mileage_charge_rate != null) m.mileage_charge_rate = base.mileage_charge_rate;
      if (base.mileage_pay_rate != null)    m.mileage_pay_rate    = base.mileage_pay_rate;
      m.__seeded = true;
      if (LOGC) console.log('[CONTRACTS] seed formState (main/pay) from base row', {
        forId: (window.modalCtx.formState && window.modalCtx.formState.__forId),
        mainKeys: Object.keys(window.modalCtx.formState?.main || {}),
        payKeys: Object.keys(window.modalCtx.formState?.pay || {})
      });

    }
    const p = (fs.pay ||= {});
    if (!Object.keys(p).length && base.rates_json && typeof base.rates_json === 'object') {
      const buckets = ['paye_day','paye_night','paye_sat','paye_sun','paye_bh','umb_day','umb_night','umb_sat','umb_sun','umb_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh'];
      for (const k of buckets) {
        const v = base.rates_json[k];
        if (v === 0 || (typeof v === 'number' && Number.isFinite(v))) p[k] = String(v);
      }
    }
  } catch {}

  const extraButtons = [];
  if (!isCreate && window.modalCtx.data?.id) {
    extraButtons.push({
      label: 'Delete contract',
      role: 'danger',
      onClick: async () => {
        const id = window.modalCtx.data?.id;
        if (!id) return;
        if (!confirm('Delete this contract? (Only allowed if no timesheets exist)')) return;
        try {
          if (LOGC) console.log('[CONTRACTS] deleteContract', { id });
          await deleteContract(id);
          try { discardAllModalsAndState(); } catch {}
          await renderAll();
        } catch (e) {
          alert(e?.message || 'Delete failed');
        }
      }
    });
  }

   const tabDefs = [
    { key: 'main',     title: 'Main' },
    { key: 'rates',    title: 'Rates' },
    { key: 'calendar', title: 'Calendar' }
  ];
  if (LOGC) console.log('[CONTRACTS] tabs', tabDefs.map(t => t.key));

 const hasId = !!window.modalCtx.data?.id;
  const isSuccessorCreate = isCreate && ( !!window.modalCtx?.__cloneIntent || !!preToken );
  if (LOGC) console.log('[CONTRACTS] showModal opts preview', {
    hasId, isCreate, isSuccessorCreate,
    stayOpenOnSave: !!isSuccessorCreate, noParentGate: !!isSuccessorCreate,
    openToken: window.modalCtx.openToken, hasCloneIntent: !!window.modalCtx.__cloneIntent
  });

  showModal(

    isCreate ? 'Create Contract' : 'Edit Contract',
    tabDefs,
    (key, row) => {
      const ctx = { data: row };
      if (key === 'main')     return renderContractMainTab(ctx);
      if (key === 'rates')    return renderContractRatesTab(ctx);
      if (key === 'calendar') return renderContractCalendarTab(ctx);
      return `<div class="tabc">Unknown tab.</div>`;
    },
    async () => {
      if (window.modalCtx?._saveInFlight) return false;
      window.modalCtx._saveInFlight = true;
      try {
        if (LOGC) console.groupCollapsed('[CONTRACTS] onSave pipeline');

        snapshotContractForm();
        console.warn('[BREACH A] after snapshotContractForm');

        // Keep a stable copy in case something re-renders and drops modalCtx.__cloneIntent
        const __preCloneIntent = window.modalCtx?.__cloneIntent
          ? { ...window.modalCtx.__cloneIntent }
          : null;
        if (LOGC) console.log('[CLONE][pre-save snapshot]', __preCloneIntent || '(none)');

        const base = window.modalCtx?.data || {};
        const fs   = (window.modalCtx?.formState || { main:{}, pay:{} });
        const fdForm = document.querySelector('#contractForm');
        const fd = fdForm ? new FormData(fdForm) : null;

        const fromFS = (k, fallback='') => {
          const v = (fs.main||{})[k]; return (v===undefined ? fallback : v);
        };
        const fromFD = (k, fallback='') => {
          if (!fd) return fallback;
          const raw = fd.get(k); return (raw==null ? fallback : String(raw).trim());
        };
        const choose = (key, fallback='') => {
          const fsVal = fromFS(key, null);
          if (fsVal !== null && fsVal !== undefined && fsVal !== '') return fsVal;
          const fdVal = fromFD(key, null);
          if (fdVal !== null && fdVal !== undefined && fdVal !== '') return fdVal;
          return (base[key] ?? fallback);
        };

        const ukToIso = (ddmmyyyy, fb=null) => {
          try {
            return (typeof parseUkDateToIso === 'function')
              ? (parseUkDateToIso(ddmmyyyy) || fb)
              : ((ddmmyyyy && /^\d{2}\/\d{2}\/\d{4}$/.test(ddmmyyyy)) ? ddmmyyyy : (ddmmyyyy || fb));
          } catch { return ddmmyyyy || fb; }
        };

              const domLabels = (typeof _collectBucketLabelsFromForm === 'function')
          ? _collectBucketLabelsFromForm('#contractForm')
          : null;

        let bucket_labels_json = null;

        // 1) Prefer DOM-collected labels (used as-is, including empty strings)
        if (domLabels && typeof domLabels === 'object' && Object.keys(domLabels).length) {
          bucket_labels_json = { ...domLabels };
        }

        // 2) Else prefer staged labels from preset/application (__bucket_labels)
        if (!bucket_labels_json) {
          const stagedMap = (fs.main && typeof fs.main.__bucket_labels === 'object')
            ? fs.main.__bucket_labels
            : null;
          if (stagedMap && Object.keys(stagedMap).length) {
            bucket_labels_json = { ...stagedMap };
          }
        }

        // 3) Else fall back to individual bucket_* / bucket_label_* fields
        if (!bucket_labels_json) {
          const staged = {
            day   : String(fs.main?.bucket_day            ?? fs.main?.bucket_label_day   ?? '').trim(),
            night : String(fs.main?.bucket_night          ?? fs.main?.bucket_label_night ?? '').trim(),
            sat   : String(fs.main?.bucket_sat            ?? fs.main?.bucket_label_sat   ?? '').trim(),
            sun   : String(fs.main?.bucket_sun            ?? fs.main?.bucket_label_sun   ?? '').trim(),
            bh    : String(fs.main?.bucket_bh             ?? fs.main?.bucket_label_bh    ?? '').trim()
          };
          const hasAnyFromStaged = Object.values(staged).some(v => v !== '');
          bucket_labels_json = hasAnyFromStaged ? staged : (base.bucket_labels_json ?? null);
        }


        const numOrNull = (s) => {
          const raw = fromFS(s, fromFD(s, ''));
          if (raw === '' || raw === null || raw === undefined) return null;
          const n = Number(raw); return Number.isFinite(n) ? n : null;
        };
        const gh = { mon: numOrNull('gh_mon'), tue: numOrNull('gh_tue'), wed: numOrNull('gh_wed'),
                     thu: numOrNull('gh_thu'), fri: numOrNull('gh_fri'), sat: numOrNull('gh_sat'), sun: numOrNull('gh_sun') };
        const ghFilled = Object.values(gh).some(v => v != null && v !== 0);
        let std_hours_json = ghFilled ? gh : (base.std_hours_json ?? null);
        if (!std_hours_json && fs.main && fs.main.__hours) std_hours_json = fs.main.__hours;

 const days = ['mon','tue','wed','thu','fri','sat','sun'];
const get = (n) => fromFS(n, fromFD(n, ''));
const hhmmOk = (v) => /^\d{2}:\d{2}$/.test(String(v||'').trim());
const normHHMM = (v) => {
  const t = String(v || '').trim();
  if (!t) return '';
  const m = t.match(/^(\d{1,2})(?::?(\d{2}))$/);
  if (!m) return '';
  const h = +m[1], mi = +m[2];
  if (Number.isNaN(h) || Number.isNaN(mi) || h < 0 || h > 23 || mi < 0 || mi > 59) return '';
  return String(h).padStart(2,'0') + ':' + String(mi);
};

const schedule = {};
let hasAnySchedule = false;

for (const d2 of days) {
  const s  = normHHMM(get(`${d2}_start`));
  const e  = normHHMM(get(`${d2}_end`));
  const br = get(`${d2}_break`);

  if (hhmmOk(s) && hhmmOk(e)) {
    hasAnySchedule = true;
    schedule[d2] = {
      start: s,
      end:   e,
      break_minutes: Math.max(0, Number(br || 0))
    };
    // 🔹 NOTE: if a day has no valid times, we simply do NOT add it
    // to `schedule` – that clears the day when the whole JSON is replaced.
  }
}

let std_schedule_json = null;

// If there is at least ONE valid day, this becomes the full schedule
// and overwrites what was there (missing days = cleared).
if (hasAnySchedule) {
  std_schedule_json = schedule;
} else if (fs.main && fs.main.__template) {
  // No valid times anywhere → fall back to existing template
  std_schedule_json = fs.main.__template;
} else if (base.std_schedule_json) {
  std_schedule_json = base.std_schedule_json;
}


        const prevStartIso = base.start_date || null;
        const prevEndIso   = base.end_date   || null;

        const startIso = ukToIso(choose('start_date', ''), base.start_date ?? null);
        const endIso   = ukToIso(choose('end_date', ''),   base.end_date   ?? null);

        const payMethodSnap = String(
          (fs.main?.pay_method_snapshot) ||
          fromFD('pay_method_snapshot', fromFD('default_pay_method_snapshot', base.pay_method_snapshot || 'PAYE')) ||
          base.pay_method_snapshot || 'PAYE'
        ).toUpperCase();

        const default_submission_mode = String(
          choose('default_submission_mode', base.default_submission_mode || 'ELECTRONIC')
        ).toUpperCase();

        const week_ending_weekday_snapshot = String(
          choose('week_ending_weekday_snapshot', (base.week_ending_weekday_snapshot ?? '0'))
        );

    const candidate_id = choose('candidate_id', base.candidate_id ?? null) || null;

// NEW: polite confirmation when saving with no candidate
if (!candidate_id) {
  const okProceed = window.confirm('No candidate is selected. Save this contract as “<Unassigned>”?');
  if (!okProceed) {
    window.modalCtx._saveInFlight = false;
    if (LOGC) console.groupEnd?.();
    return false;
  }
}

const client_id    = choose('client_id', base.client_id ?? null) || null;
const role         = choose('role', base.role ?? null);
const band         = choose('band', base.band ?? null);
const display_site = choose('display_site', base.display_site ?? '');


        const boolFromFS = (name, baseVal=false) => {
          if (fs && fs.main && Object.prototype.hasOwnProperty.call(fs.main, name)) {
            const v = fs.main[name];
            return v === 'on' || v === true || v === 'true' || v === 1 || v === '1';
          }
          return !!base[name];
        };
        const auto_invoice                 = boolFromFS('auto_invoice',                 !!base.auto_invoice);
        const require_reference_to_pay     = boolFromFS('require_reference_to_pay',     !!base.require_reference_to_pay);
        const require_reference_to_invoice = boolFromFS('require_reference_to_invoice', !!base.require_reference_to_invoice);

        const BUCKETS = ['paye_day','paye_night','paye_sat','paye_sun','paye_bh','umb_day','umb_night','umb_sat','umb_sun','umb_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh'];
        const baseRates = { ...(base.rates_json || {}) };
        const mergedRates = { ...baseRates };
        for (const k of BUCKETS) {
          const staged = (fs.pay || {})[k];
          if (staged !== undefined && staged !== '') {
            const n = Number(staged);
            mergedRates[k] = Number.isFinite(n) ? n : 0;
          } else {
            const domVal = fd ? fd.get(k) : null;
            if (domVal !== null && domVal !== undefined && String(domVal).trim() !== '') {
              const n = Number(domVal);
              mergedRates[k] = Number.isFinite(n) ? n : 0;
            }
          }
        }

        // NEW: mileage values — prefer Rates tab DOM, then FS staging, else null
        const mcrDom = document.querySelector('#contractRatesTab input[name="mileage_charge_rate"]');
        const mprDom = document.querySelector('#contractRatesTab input[name="mileage_pay_rate"]');
        const mileage_charge_rate = (mcrDom && mcrDom.value !== '') ? (Number(mcrDom.value) || null) : numOrNull('mileage_charge_rate');
        const mileage_pay_rate    = (mprDom && mprDom.value !== '') ? (Number(mprDom.value) || null) : numOrNull('mileage_pay_rate');

        const data = {
          id: window.modalCtx.data?.id || null,
          candidate_id,
          client_id,
          role,
          band,
          display_site,
          start_date:   startIso,
          end_date:     endIso,
          pay_method_snapshot: payMethodSnap,
          default_submission_mode,
          week_ending_weekday_snapshot,
          auto_invoice,
          require_reference_to_pay,
          require_reference_to_invoice,
          rates_json: mergedRates,
          std_hours_json,
          std_schedule_json,
          bucket_labels_json,
          // NEW: mileage in payload
          mileage_charge_rate: mileage_charge_rate,
          mileage_pay_rate: mileage_pay_rate
        };

        if (LOGC) {
          const preview = { ...data, rates_json: '(object)', std_hours_json: std_hours_json ? '(object)' : null, std_schedule_json: std_schedule_json ? '(object)' : null };
          console.log('[CONTRACTS] onSave payload (preview)', preview);
        }

        let overlapProceed = true;
        try {
          if (typeof checkContractOverlap === 'function' && data.candidate_id && data.start_date && data.end_date) {
            const ov = await checkContractOverlap({
              candidate_id: data.candidate_id,
              start_date: data.start_date,
              end_date: data.end_date,
              ignore_contract_id: data.id || null
            });
            if (ov && ov.has_overlap) {
              const lines = (ov.overlaps || []).slice(0, 3).map(o => {
                const nm = o.client_name || o.client || 'Client';
                const a  = o.overlap_from || '';
                const b  = o.overlap_to   || '';
                return `${nm} ${a}→${b}`;
              });
              const extra = (ov.overlaps || []).length > 3 ? ` …and ${ov.overlaps.length - 3} more` : '';
              const msg = `This contract overlaps existing contract(s):\n• ${lines.join('\n• ')}${extra}\n\nProceed anyway?`;
              overlapProceed = !!window.confirm(msg);
            }
          }
        } catch (e) {
          if (LOGC) console.warn('[CONTRACTS] overlap check failed (non-blocking)', e);
        }
        if (!overlapProceed) {
          window.modalCtx._saveInFlight = false;
          if (LOGC) console.log('[CONTRACTS] Save cancelled by user on overlap dialog');
          console.groupEnd?.();
          return false;
        }

        try {} catch {}

        if (!isCreate && data.id && typeof callCheckTimesheetBoundary === 'function' && data.start_date && data.end_date) {
          try {
            const boundary = await callCheckTimesheetBoundary(data.id, data.start_date, data.end_date);
            window.__tsBoundaryResult = boundary || null;
            if (!boundary || boundary.ok === false) {
              let msg = 'Date range excludes existing timesheets.';
              try {
                const v = boundary?.violations || [];
                if (v.length) {
                  const sample = v.slice(0,3).map(x => {
                    const nm = x.client_name || 'Client';
                    const dt = x.date || '';
                    const st = x.status || '';
                    return `${nm} ${dt}${st?` (${st})`:''}`;
                  }).join(' • ');
                  msg = `Dates exclude existing timesheets: ${sample}${v.length>3? '…':''}`;
                } else if (boundary?.min_ts_date || boundary?.max_ts_date) {
                  const a = boundary.min_ts_date || '';
                  const b = boundary.max_ts_date || '';
                  msg = `Dates exclude timesheets in range ${a} → ${b}.`;
                }
              } catch {}
              if (typeof showModalHint === 'function') showModalHint(msg, 'warn'); else alert(msg);
              window.modalCtx._saveInFlight = false;
              console.groupEnd?.();
              return false;
            }
          } catch (e) {
            if (LOGC) console.warn('[CONTRACTS] timesheet boundary check failed (non-blocking fallback)', e);
          }
        }

        let hasManualStage = false;
        try {
          const stageKey = data.id || window.modalCtx.openToken || null;
          if (stageKey && typeof getContractCalendarStageState === 'function') {
            const st = getContractCalendarStageState(stageKey);
            hasManualStage = !!(st && (st.add?.size || st.remove?.size || Object.keys(st.additional || {}).length));
          }
        } catch {}

        if (!isCreate && hasManualStage) data.skip_generate_weeks = true;

        // --- detect calendar stage shape BEFORE any persistence ---
        let stageShape = { hasAny:false, hasRemoveAll:false, hasAdds:false, hasAdditionals:false };
        try {
          const stageKey = data.id || window.modalCtx.openToken || null;
          if (stageKey && typeof getContractCalendarStageState === 'function') {
            const st = getContractCalendarStageState(stageKey);
            stageShape.hasAny         = !!st && (!!st.removeAll || st.add.size || st.remove.size || Object.keys(st.additional||{}).length);
            stageShape.hasRemoveAll   = !!st?.removeAll;
            stageShape.hasAdds        = !!(st && st.add && st.add.size);
            stageShape.hasAdditionals = !!(st && st.additional && Object.keys(st.additional).length);
          }
        } catch {}

        // === CREATE vs EDIT ordering ===
        // CREATE: upsert first to obtain id → then commit stage (if any) → normalize window → (maybe) generate defaults
        // EDIT: if any stage present, always commit calendar FIRST → normalize window → then upsert metadata
  if (!isCreate && data.id && stageShape.hasAny) {
  if (LOGC) console.log('[CONTRACTS] calendar (any stage) → commitContractCalendarStageIfPending');
  const preCalRes = await commitContractCalendarStageIfPending(data.id);
  if (!preCalRes.ok) {
    const msg = `Calendar save failed: ${preCalRes.message || 'unknown error'}. Contract details were not saved.`;
    if (LOGC) console.warn('[CONTRACTS] calendar commit failed (pre-upsert)', preCalRes);
    if (typeof showModalHint === 'function') showModalHint(msg, 'warn'); else alert(msg);
    window.modalCtx._saveInFlight = false;
    console.groupEnd?.();
    return false;
  }

  // Do NOT normalize on the FE — backend now owns window shrink/extend.
  // Instead, pull the fresh contract (authoritative window) and bind it.
  try {
    const fresh = await getContract(data.id);
    if (fresh && fresh.id) {
      window.modalCtx.data = fresh;

      // Update formState (so subsequent PUTs never push stale dates)
      const fs = (window.modalCtx.formState ||= { __forId: (data.id||null), main:{}, pay:{} });
      fs.main ||= {};
      fs.main.start_date = fresh.start_date || null;
      fs.main.end_date   = fresh.end_date   || null;

      // Update visible inputs if we're on the Main tab
      try {
        const form = document.querySelector('#contractForm');
        if (form) {
          const sd = form.querySelector('input[name="start_date"]');
          const ed = form.querySelector('input[name="end_date"]');
          const toUk = (iso) => {
            try { return (typeof formatIsoToUk === 'function') ? (formatIsoToUk(iso) || iso) : iso; } catch { return iso; }
          };
          if (sd && fresh.start_date) sd.value = toUk(fresh.start_date);
          if (ed && fresh.end_date)   ed.value = toUk(fresh.end_date);
        }
      } catch {}
      // Ensure the payload carries authoritative dates unless user explicitly changed them
      const userEditedStart = !!(prevStartIso && startIso && startIso !== prevStartIso);
      const userEditedEnd   = !!(prevEndIso   && endIso   && endIso   !== prevEndIso);
      data.__userChangedDates = (userEditedStart || userEditedEnd);
      if (!data.__userChangedDates) {
        data.start_date = fresh.start_date;
        data.end_date   = fresh.end_date;
      }
    }
  } catch (e) {
    if (LOGC) console.warn('[CONTRACTS] fresh refetch failed (proceeding with current modal data)', e);
  }

  // Avoid auto-generation when calendar stage was present
  data.skip_generate_weeks = true;
}


if (LOGC) console.log('[CONTRACTS] upsert → upsertContract');
const saved = await upsertContract(data, data.id || undefined);

const persistedId = saved?.id || saved?.contract?.id || null;
if (LOGC) console.log('[CONTRACTS] upsertContract result', {
  isCreate, persistedId, rawHasSaved: !!saved
});

window.modalCtx.data = saved?.contract || saved || window.modalCtx.data;
if (LOGC) console.log('[CONTRACTS] modalCtx.data snapshot', {
  id: window.modalCtx.data?.id || null,
  start_date: window.modalCtx.data?.start_date || null,
  end_date:   window.modalCtx.data?.end_date   || null
});

// 🔎 breadcrumb to prove we reached the post-save gate
console.warn('[AFTER UPSERT] reached post-save pre-gate', {
  modalId: window.modalCtx?.data?.id,
  isCreate,
  openToken: window.modalCtx?.openToken,
  persistedId
});

        try {
          const warnings = saved?.warnings || saved?.contract?.warnings || [];
          const warnStr  = Array.isArray(warnings) ? warnings.join(', ') : (saved?.warning || '');
          if (warnStr) { if (LOGC) console.warn('[CONTRACTS] warnings', warnStr); showModalHint?.(`Warning: ${warnStr}`, 'warn'); }
        } catch {}

        const contractId = saved?.id || saved?.contract?.id;
        if (contractId) {
          window.__pendingFocus = { section: 'contracts', id: contractId };

          // If this was a Clone&Extend staging and user opted to end the old contract, apply now.
          try {
            const t0 = Date.now();
            const savedContractId = contractId || (saved?.contract?.id) || (saved?.id) || null;

            // Prefer the live intent; if lost due to UI state flips, fall back to the pre-save snapshot
            const ciLive     = window.modalCtx?.__cloneIntent || null;
            const ciSnapshot = __preCloneIntent || null;
            const ci         = (ciLive ?? ciSnapshot) || null;

            const hasCi      = !!ci;
            const wantsEnd   = !!ci?.end_existing;
            const hasSource  = !!ci?.source_contract_id;
            const hasEndDate = !!ci?.end_existing_on;

            // Resolve callable from either module/global or window
            const fnLive = (typeof endContractSafely === 'function') ? endContractSafely : undefined;
            const fnWin  = (typeof window !== 'undefined' && typeof window.endContractSafely === 'function') ? window.endContractSafely : undefined;
            const trimFn = fnLive || fnWin;
            const hasFn  = !!trimFn;

            if (LOGC) {
              console.groupCollapsed('[CLONE][post-save gate]');
              console.log({
                isCreate,
                savedContractId,
                modalCtxId: window.modalCtx?.data?.id || null,
                savedStart: window.modalCtx?.data?.start_date || null,
                savedEnd:   window.modalCtx?.data?.end_date   || null,
                openToken:  window.modalCtx?.openToken || null,
                hasCi, wantsEnd, hasSource, hasEndDate,
                ciLive: !!ciLive,
                ciSnapshot: !!ciSnapshot,
                ci: {
                  source_contract_id: ci?.source_contract_id ?? null,
                  end_existing:       ci?.end_existing ?? null,
                  end_existing_on:    ci?.end_existing_on ?? null
                },
                hasFnLive:  !!fnLive,
                hasFnWin:   !!fnWin,
                hasFnResolved: hasFn
              });
              console.groupEnd();
            }

            if (hasCi && wantsEnd && hasSource && hasEndDate && hasFn) {
              if (LOGC) console.log('WILL_CALL endContractSafely', { source: ci.source_contract_id, desired_end: ci.end_existing_on });

              let res = null, ok=false, clamped=false, safe_end=null, message=null, t1=0;
              try {
                console.groupCollapsed('[TRIM_CALL] →', { source: ci.source_contract_id, desired_end: ci.end_existing_on });
                res = await trimFn(ci.source_contract_id, ci.end_existing_on);
                t1 = Date.now();
                ok       = !!(res && (res.ok ?? (res === true)));
                clamped  = !!res?.clamped;
                safe_end = res?.safe_end || null;
                message  = res?.message  || null;
                console.log('result', { ok, clamped, safe_end, message, raw: res, elapsed_ms: (t1 - t0) });
                console.groupEnd();
              } catch (err) {
                console.warn('[TRIM_CALL] ✖ threw', err);
              }

              if (ok) {
                if (clamped && typeof showTailClampWarning === 'function') {
                  try { showTailClampWarning(safe_end, ci.end_existing_on); } catch {}
                }
                if (typeof refreshOldContractAfterTruncate === 'function') {
                  try { await refreshOldContractAfterTruncate(ci.source_contract_id); } catch (e) { if (LOGC) console.warn('refreshOldContractAfterTruncate failed', e); }
                }
                if (LOGC) console.log('CLEAR_INTENT (after endContractSafely)');
                clearCloneIntent();
              } else {
                if (LOGC) console.warn('TRIM_CALL did not report ok', { res });
                if (LOGC) console.log('CLEAR_INTENT (after not-ok result)');
                clearCloneIntent();
              }
            } else {
              const reasons = [];
              if (!hasCi)               reasons.push('NO_INTENT');
              if (hasCi && !wantsEnd)   reasons.push('BOX_UNTICKED_end_existing=false');
              if (hasCi && !hasSource)  reasons.push('MISSING_source_contract_id');
              if (hasCi && !hasEndDate) reasons.push('MISSING_end_existing_on');
              if (!hasFn)               reasons.push(`NO_endContractSafely (live=${!!fnLive}, window=${!!fnWin})`);
              console.warn('[CLONE][post-save SKIP] not calling endContractSafely', { reasons });
              console.log('CLEAR_INTENT (skip path)');
              clearCloneIntent();
            }

          } catch (e) {
            if (LOGC) console.warn('[CLONE][post-save decision] EXCEPTION', e);
          }

        }

        try { if (typeof computeContractMargins === 'function') computeContractMargins(); } catch {}

        try {
          const fr = window.__getModalFrame?.();
          const currentTab = fr?.currentTabKey || (document.querySelector('#modalTabs button.active')?.textContent?.toLowerCase() || 'main');
          if (LOGC) console.log('[CONTRACTS] post-save repaint (in-place)', { currentTab, contractId: (window.modalCtx?.data?.id) });

          if (currentTab === 'calendar' && window.modalCtx?.data?.id) {
            const contractId2 = window.modalCtx.data.id;
            const win = (window.__calState?.[contractId2]?.win) || null;
            const candId = window.modalCtx?.data?.candidate_id || null;
            const scrollBox = document.getElementById('__calScroll');
            const prevScroll = scrollBox ? scrollBox.scrollTop : 0;
            if (typeof fetchAndRenderCandidateCalendarForContract === 'function' && candId) {
              await fetchAndRenderCandidateCalendarForContract(contractId2, candId, {
                from: win?.from, to: win?.to, view: window.__calState?.[contractId2]?.view,
                weekEnding: window.modalCtx?.data?.week_ending_weekday_snapshot ?? 0
              });
            } else {
              await fetchAndRenderContractCalendar(contractId2, win ? { from: win.from, to: win.to, view: window.__calState?.[contractId2]?.view } : undefined);
            }
            const newScrollBox = document.getElementById('__calScroll');
            if (newScrollBox) newScrollBox.scrollTop = prevScroll;
          } else if (currentTab === 'rates') {
            try { computeContractMargins(); } catch {}
          }
          try { window.__toast?.('Saved'); } catch {}
        } catch (e) {
          if (LOGC) console.warn('[CONTRACTS] post-save repaint failed', e);
        }

        if (LOGC) console.groupEnd?.();
        // Return the saved row so saveForFrame() can set hasId=true and flip to View
        const savedRow = (window.modalCtx.data || null);
        return { ok: true, saved: savedRow };

       } catch (e) {
        if (LOGC) { console.error('[CONTRACTS] Save failed', e); console.groupEnd?.(); }
        alert(`Save failed: ${e?.message || e}`);
        return false;
      } finally {
        window.modalCtx._saveInFlight = false;
      }
    },

    hasId, // 5th

    // 6th: onReturn — single, deduped
    () => {
      const wire = () => {
        snapshotContractForm();

              const form   = document.querySelector('#contractForm');
        const tabsEl = document.getElementById('modalTabs');
        const active = tabsEl?.querySelector('button.active')?.textContent?.toLowerCase() || 'main';


        if (form) {
          if (!form.__wiredStage) {
            form.__wiredStage = true;
            const stage = (e) => {
              const t = e.target;
              if (!t || !t.name) return;
              let v = t.type === 'checkbox' ? (t.checked ? 'on' : '') : t.value;

              const isTimeField = /^(mon|tue|wed|thu|fri|sat|sun)_(start|end)$/.test(t.name);
              if (isTimeField) {
                if (e.type === 'input') {
                  v = v.replace(/[^\d:]/g,'');
                  t.value = v;
                  try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
                  return;
                }
              }

              setContractFormValue(t.name, v);
              if (t.name === 'pay_method_snapshot' || /^(paye_|umb_|charge_)/.test(t.name)) computeContractMargins();
              try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
            };
            form.addEventListener('input', stage, true);
            form.addEventListener('change', stage, true);

            const normaliseTimeInput = (t) => {
              if (!t || !/^(mon|tue|wed|thu|fri|sat|sun)_(start|end)$/.test(t.name)) return;
              const raw = (t.value || '').trim();
              const norm = (function (x) {
                if (!x) return '';
                const y = x.replace(/\s+/g, '');
                let h, m;
                if (/^\d{3,4}$/.test(y)) {
                  const s = y.padStart(4, '0'); h = +s.slice(0, 2); m = +s.slice(2, 4);
                } else if (/^\d{1,2}:\d{1,2}$/.test(y)) {
                  const parts = y.split(':'); h = +parts[0]; m = +parts[1];
                } else return '';
                if (h < 0 || h > 23 || m < 0 || m > 59) return '';
                return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0');
              })(raw);

              if (!norm && raw) {
                t.value = '';
                t.setAttribute('data-invalid', '1');
                t.setAttribute('title', 'Enter a valid time HH:MM (00:00–23:59)');
                setContractFormValue(t.name, '');
                try { t.dispatchEvent(new Event('input', { bubbles: true })); t.dispatchEvent(new Event('change', { bubbles: true })); } catch {}
                try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
                return;
              }

              if (norm) {
                t.value = norm;
                t.removeAttribute('data-invalid');
                t.removeAttribute('title');
                setContractFormValue(t.name, norm);
                try { t.dispatchEvent(new Event('input', { bubbles: true })); t.dispatchEvent(new Event('change', { bubbles: true })); } catch {}
                try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
              }
            };

            const onBlurNorm = (e) => { normaliseTimeInput(e.target); };
            form.addEventListener('blur', onBlurNorm, true);

            const onKeydownNorm = (e) => {
              if (e.key === 'Tab') normaliseTimeInput(e.target);
            };
            form.addEventListener('keydown', onKeydownNorm, true);
          }

          if (active === 'main') {
            try {
              const sd = form.querySelector('input[name="start_date"]');
              const ed = form.querySelector('input[name="end_date"]');
              const toUk = (iso) => {
                try { return (typeof formatIsoToUk === 'function') ? (formatIsoToUk(iso) || '') : (iso || ''); }
                catch { return iso || ''; }
              };
              if (sd && /^\d{4}-\d{2}-\d{2}$/.test(sd.value||'')) sd.value = toUk(sd.value);
              if (ed && /^\d{4}-\d{2}-\d{2}$/.test(ed.value||'')) ed.value = toUk(ed.value);
              if (sd) { sd.setAttribute('placeholder','DD/MM/YYYY'); if (typeof attachUkDatePicker === 'function') attachUkDatePicker(sd); }
              if (ed) { ed.setAttribute('placeholder','DD/MM/YYYY'); if (typeof attachUkDatePicker === 'function') attachUkDatePicker(ed, { minDate: sd?.value || null }); }
              if (sd && ed) {
                sd.addEventListener('change', () => {
                  const sv = sd.value || '';
                  if (typeof attachUkDatePicker === 'function') attachUkDatePicker(ed, { minDate: sv || null });
                  if (sv && ed.value) {
                    try {
                      const si = parseUkDateToIso?.(sv) || sv;
                      const ei = parseUkDateToIso?.(ed.value) || ed.value;
                      if (si && ei && si > ei) { ed.value=''; showModalHint?.('Pick an end date after start','warn'); setContractFormValue('end_date',''); }
                    } catch {}
                  }
                });
              }
              if (LOGC) console.log('[CONTRACTS] datepickers wired for start_date/end_date', { hasStart: !!sd, hasEnd: !!ed });
            } catch (e) {
              if (LOGC) console.warn('[CONTRACTS] datepicker wiring failed', e);
            }

            const btnPC = document.getElementById('btnPickCandidate');
            const btnCC = document.getElementById('btnClearCandidate');
            const btnPL = document.getElementById('btnPickClient');
            const btnCL = document.getElementById('btnClearClient');
            const candInput = document.getElementById('candidate_name_display');
            const cliInput  = document.getElementById('client_name_display');

            const ensurePrimed = async (entity) => {
              try {
                await ensurePickerDatasetPrimed(entity);
                const fp = getSummaryFingerprint(entity);
                const mem = getSummaryMembership(entity, fp);
                if (!mem?.ids?.length || mem?.stale) {
                  await primeSummaryMembership(entity, fp);
                }
              } catch (e) { if (LOGC) console.warn('[CONTRACTS] typeahead priming failed', entity, e); }
            };

            const buildItemLabel = (entity, r) => {
              if (entity === 'candidates') {
                const first = (r.first_name||'').trim();
                const last  = (r.last_name||'').trim();
                const role  = ((r.roles_display||'').split(/[•;,]/)[0]||'').trim();
                return `${last}${last?', ':''}${first}${role?` ${role}`:''}`.trim();
              } else {
                const name  = (r.name||'').trim();
                return name;
              }
            };

            const wireTypeahead = async (entity, inputEl, hiddenName, labelElId) => {
              if (!inputEl) return;
              await ensurePrimed(entity);

              const menuId = entity === 'candidates' ? 'candTypeaheadMenu' : 'clientTypeaheadMenu';
              let menu = document.getElementById(menuId);
              if (!menu) {
                menu = document.createElement('div');
                menu.id = menuId;
                menu.className = 'typeahead-menu';
                menu.style.position = 'absolute';
                menu.style.zIndex = '1000';
                menu.style.background = 'var(--panel, #fff)';
                menu.style.border = '1px solid var(--line, #ddd)';
                menu.style.boxShadow = '0 4px 12px rgba(0,0,0,0.08)';
                menu.style.maxHeight = '240px';
                menu.style.overflowY = 'auto';
                menu.style.display = 'none';
                document.body.appendChild(menu);
              }

              const positionMenu = () => {
                const r = inputEl.getBoundingClientRect();
                menu.style.minWidth = `${Math.max(260, r.width)}px`;
                menu.style.left = `${window.scrollX + r.left}px`;
                menu.style.top  = `${window.scrollY + r.bottom + 4}px`;
              };

              const closeMenu = () => { menu.style.display = 'none'; menu.innerHTML = ''; };
              const openMenu  = () => { positionMenu(); menu.style.display = ''; };

              const getDataset = () => {
                const fp  = getSummaryFingerprint(entity);
                const mem = getSummaryMembership(entity, fp);
                const ds  = (window.__pickerData ||= {})[entity] || { since:null, itemsById:{} };
                const items = ds.itemsById || {};
                let ids = Array.isArray(mem?.ids) ? mem.ids : [];
                if (!ids.length) ids = Object.keys(items);
                return { ids, items };
              };

              const applyList = (rows) => {
                menu.innerHTML = rows.slice(0, 10).map(r => {
                  const label = buildItemLabel(entity, r);
                  return `<div class="ta-item" data-id="${r.id||''}" data-label="${(label||'').replace(/"/g,'&quot;')}" style="padding:8px 10px;cursor:pointer;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${label}</div>`;
                }).join('');
                const first = menu.querySelector('.ta-item');
                if (first) first.style.background = 'var(--hover, #f5f5f5)';
              };

              const selectRow = (id, label) => {
                setContractFormValue(hiddenName, id);
                inputEl.value = label || '';
                const labEl = document.getElementById(labelElId);
                if (labEl) labEl.textContent = label ? `Chosen: ${label}` : '';

                try {
                  const fs = (window.modalCtx.formState ||= { __forId: (window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null), main:{}, pay:{} });
                  fs.main ||= {};
                  fs.main[hiddenName] = id;
                  if (hiddenName === 'candidate_id') fs.main['candidate_display'] = label;
                  if (hiddenName === 'client_id')    fs.main['client_name']       = label;
                } catch {}

                try {
                  window.modalCtx.data = window.modalCtx.data || {};
                  if (hiddenName === 'candidate_id') { window.modalCtx.data.candidate_id = id; window.modalCtx.data.candidate_display = label; }
                  if (hiddenName === 'client_id')    { window.modalCtx.data.client_id    = id; window.modalCtx.data.client_name = label; }
                } catch {}

                if (hiddenName === 'candidate_id') {
                  (async () => {
                    try {
                      const cand = await getCandidate(id);
                      const derived = (String(cand?.pay_method || '').toUpperCase() === 'UMBRELLA' && cand?.umbrella_id) ? 'UMBRELLA' : 'PAYE';
                      const fsm = (window.modalCtx.formState ||= { main:{}, pay:{} }).main ||= {};
                      fsm.pay_method_snapshot = derived;
                      fsm.__pay_locked = true;
                      const sel = document.querySelector('select[name="pay_method_snapshot"], select[name="default_pay_method_snapshot"]');
                      if (sel) { sel.value = derived; sel.disabled = true; }
                      computeContractMargins();
                    } catch (e) { if (LOGC) console.warn('[CONTRACTS] derive pay method failed', e); }
                  })();
                }

                closeMenu();
                try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
              };

              let debTimer = 0;
              const handleInput = () => {
                const q = (inputEl.value||'').trim();
                if (q.length < 3) { closeMenu(); return; }
                if (debTimer) clearTimeout(debTimer);
                debTimer = setTimeout(() => {
                  const { ids, items } = getDataset();
                  const rows = pickersLocalFilterAndSort(entity, ids, q, entity==='candidates'?'last_name':'name', 'asc')
                    .map(v => (typeof v === 'object' ? v : items[String(v)]))
                    .filter(Boolean);
                  if (!rows.length) { closeMenu(); return; }
                  applyList(rows);
                  openMenu();
                }, 120);
              };

              const handleKeyDown = (e) => {
                if (menu.style.display === 'none') return;
                const items = Array.from(menu.querySelectorAll('.ta-item'));
                if (!items.length) return;
                const idx = items.findIndex(n => n.style.background && n.style.background.includes('hover'));
                const setActive = (i) => {
                  items.forEach(n => n.style.background='');
                  items[i].style.background = 'var(--hover, #f5f5f5)';
                  items[i].scrollIntoView({ block:'nearest' });
                };
                if (e.key === 'ArrowDown') { e.preventDefault(); setActive(Math.min((idx<0?0:idx+1), items.length-1)); }
                if (e.key === 'ArrowUp')   { e.preventDefault(); setActive(Math.max((idx<0?0:idx-1), 0)); }
                if (e.key === 'Enter')     { e.preventDefault(); const n = items[Math.max(idx,0)]; if (n) selectRow(n.dataset.id, n.dataset.label); }
                if (e.key === 'Escape')    { e.preventDefault(); closeMenu(); }
              };

              menu.addEventListener('click', (ev) => {
                const n = ev.target && ev.target.closest('.ta-item'); if (!n) return;
                selectRow(n.dataset.id, n.dataset.label);
              });

              let blurTimer = 0;
              inputEl.addEventListener('blur', () => { blurTimer = setTimeout(closeMenu, 150); });
              menu.addEventListener('mousedown', () => { if (blurTimer) clearTimeout(blurTimer); });

              inputEl.addEventListener('input', handleInput);
              inputEl.addEventListener('keydown', handleKeyDown);
            };

            wireTypeahead('candidates', candInput, 'candidate_id', 'candidatePickLabel');
            wireTypeahead('clients',    cliInput,  'client_id',    'clientPickLabel');

            if (btnPC && !btnPC.__wired) {
              btnPC.__wired = true;
              btnPC.addEventListener('click', async () => {
                if (LOGC) console.log('[CONTRACTS] Pick Candidate clicked');
                openCandidatePicker(async ({ id, label }) => {
                  if (LOGC) console.log('[CONTRACTS] Pick Candidate → selected', { id, label });
                  setContractFormValue('candidate_id', id);
                  const lab = document.getElementById('candidatePickLabel'); if (lab) lab.textContent = `Chosen: ${label}`;
                  try {
                    const fs2 = (window.modalCtx.formState ||= { __forId: (window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null), main:{}, pay:{} });
                    fs2.main ||= {}; fs2.main.candidate_id = id; fs2.main.candidate_display = label;
                    window.modalCtx.data = window.modalCtx.data || {};
                    window.modalCtx.data.candidate_id = id; window.modalCtx.data.candidate_display = label;
                  } catch {}
                  try {
                    const cand = await getCandidate(id);
                    const derived = (String(cand?.pay_method || '').toUpperCase() === 'UMBRELLA' && cand?.umbrella_id) ? 'UMBRELLA' : 'PAYE';
                    const fsm = (window.modalCtx.formState ||= { main:{}, pay:{} }).main ||= {};
                    fsm.pay_method_snapshot = derived;
                    fsm.__pay_locked = true;
                    const sel = document.querySelector('select[name="pay_method_snapshot"], select[name="default_pay_method_snapshot"]');
                    if (sel) { sel.value = derived; sel.disabled = true; }
                    computeContractMargins();
                  } catch (e) { if (LOGC) console.warn('[CONTRACTS] prefillPayMethodFromCandidate failed', e); }
                });
              });
              if (LOGC) console.log('[CONTRACTS] wired btnPickCandidate');
            }
      if (btnCC && !btnCC.__wired) {
  btnCC.__wired = true;
  btnCC.addEventListener('click', () => {
    if (LOGC) console.log('[CONTRACTS] Clear Candidate clicked');
    setContractFormValue('candidate_id', '');

    // 🔹 Clear the visible candidate text input as well
    const candInput = document.getElementById('candidate_name_display');
    if (candInput) candInput.value = '';

    const lab = document.getElementById('candidatePickLabel');
    if (lab) lab.textContent = '';

    try {
      const fs2 = (window.modalCtx.formState ||= {
        __forId: (window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null),
        main:{}, pay:{}
      });
      fs2.main ||= {};
      delete fs2.main.candidate_id;
      delete fs2.main.candidate_display;
      fs2.main.__pay_locked = false;
      fs2.main.pay_method_snapshot = 'PAYE';

      const sel = document.querySelector('select[name="pay_method_snapshot"], select[name="default_pay_method_snapshot"]');
      if (sel) { sel.disabled = false; sel.value = 'PAYE'; }

      window.modalCtx.data = window.modalCtx.data || {};
      delete window.modalCtx.data.candidate_id;
      delete window.modalCtx.data.candidate_display;
    } catch {}
    try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
  });
  if (LOGC) console.log('[CONTRACTS] wired btnClearCandidate');
}


            if (btnPL && !btnPL.__wired) {
              btnPL.__wired = true;
              btnPL.addEventListener('click', async () => {
                if (LOGC) console.log('[CONTRACTS] Pick Client clicked');
                openClientPicker(async ({ id, label }) => {
                  if (LOGC) console.log('[CONTRACTS] Pick Client → selected', { id, label });
                  setContractFormValue('client_id', id);
                  const lab = document.getElementById('clientPickLabel'); if (lab) lab.textContent = `Chosen: ${label}`;
                  try {
                    const fs2 = (window.modalCtx.formState ||= { __forId: (window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null), main:{}, pay:{} });
                    fs2.main ||= {}; fs2.main.client_id = id; fs2.main.client_name = label;
                    window.modalCtx.data = window.modalCtx.data || {};
                    window.modalCtx.data.client_id = id; window.modalCtx.data.client_name = label;
                  } catch {}
                  try {
                    const client = await getClient(id);
                    const h = checkClientInvoiceEmailPresence(client);
                    if (h) showModalHint(h, 'warn');
                    const we = (client?.week_ending_weekday ?? (client?.client_settings && client.client_settings.week_ending_weekday)) ?? 0;
                    const fs2 = (window.modalCtx.formState ||= { main:{}, pay:{} });
                    fs2.main ||= {}; fs2.main.week_ending_weekday_snapshot = String(we);
                    const weekNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
                    const lbl = document.getElementById('weLabel'); if (lbl) lbl.textContent = weekNames[Number(we)] || 'Sunday';
                    const hidden = form?.querySelector('input[name="week_ending_weekday_snapshot"]'); if (hidden) hidden.value = String(we);

                    // NEW: 2.5 defaults (only on brand new contract and if not manually set yet)
try {
  const isNewContract = !window.modalCtx?.data?.id;
  const cs = client?.client_settings || {};
  if (isNewContract) {
    const main = (window.modalCtx.formState ||= {main:{},pay:{}}).main ||= {};

    if (!Object.prototype.hasOwnProperty.call(main, 'require_reference_to_pay')) {
      const v = !!cs.pay_reference_required;
      setContractFormValue('require_reference_to_pay', v ? 'on' : '');
      main.require_reference_to_pay = v;
    }
    if (!Object.prototype.hasOwnProperty.call(main, 'require_reference_to_invoice')) {
      const v = !!cs.invoice_reference_required;
      setContractFormValue('require_reference_to_invoice', v ? 'on' : '');
      main.require_reference_to_invoice = v;
    }
    if (!Object.prototype.hasOwnProperty.call(main, 'default_submission_mode')) {
      const mode = String(cs.default_submission_mode || 'ELECTRONIC').toUpperCase();
      const sel = document.querySelector('select[name="default_submission_mode"]');
      if (sel) sel.value = mode;
      main.default_submission_mode = mode;
    }

    // NEW: auto_invoice default from client settings
    if (!Object.prototype.hasOwnProperty.call(main, 'auto_invoice')) {
      const v = !!cs.auto_invoice_default;              // tweak property name if needed
      setContractFormValue('auto_invoice', v ? 'on' : '');
      main.auto_invoice = v;
      const cb = form?.querySelector('input[name="auto_invoice"]');
      if (cb) cb.checked = v;
    }

    // NEW: mileage default from client when empty
    const mcrEl = document.querySelector('#contractRatesTab input[name="mileage_charge_rate"]');
    const mprEl = document.querySelector('#contractRatesTab input[name="mileage_pay_rate"]');
    const isBlank = (el) => !el || String(el.value||'').trim()==='';
    if ((isBlank(mcrEl) && !main.mileage_charge_rate) || (isBlank(mprEl) && !main.mileage_pay_rate)) {
      const charge = (client?.mileage_charge_rate != null) ? Number(client.mileage_charge_rate) : null;
      if (charge != null && Number.isFinite(charge)) {
        const pay = Math.max(0, charge - 0.10);
        if (mcrEl) mcrEl.value = charge;
        if (mprEl) mprEl.value = pay;
        main.mileage_charge_rate = charge;
        main.mileage_pay_rate    = pay;
        try {
          if (mcrEl) { mcrEl.dispatchEvent(new Event('input',{bubbles:true})); mcrEl.dispatchEvent(new Event('change',{bubbles:true})); }
          if (mprEl) { mprEl.dispatchEvent(new Event('input',{bubbles:true})); mprEl.dispatchEvent(new Event('change',{bubbles:true})); }
        } catch {}
      }
    }
  }
} catch (e) { if (LOGC) console.warn('[CONTRACTS] client defaults (gates/submission/mileage) failed', e); }

                    try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
                  } catch (e) { if (LOGC) console.warn('[CONTRACTS] client hint/week-ending check failed', e); }
                });
              });
              if (LOGC) console.log('[CONTRACTS] wired btnPickClient');
            }
        if (btnCL && !btnCL.__wired) {
  btnCL.__wired = true;
  btnCL.addEventListener('click', () => {
    if (LOGC) console.log('[CONTRACTS] Clear Client clicked');
    setContractFormValue('client_id', '');

    // 🔹 Clear the visible client text input as well
    const cliInput = document.getElementById('client_name_display');
    if (cliInput) cliInput.value = '';

    const lab = document.getElementById('clientPickLabel');
    if (lab) lab.textContent = '';

    try {
      const fs2 = (window.modalCtx.formState ||= {
        __forId: (window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null),
        main:{}, pay:{}
      });
      fs2.main ||= {};
      delete fs2.main.client_id;
      delete fs2.main.client_name;
      delete fs2.main.week_ending_weekday_snapshot;

      window.modalCtx.data = window.modalCtx.data || {};
      delete window.modalCtx.data.client_id;
      delete window.modalCtx.data.client_name;

      const lbl = document.getElementById('weLabel');
      if (lbl) lbl.textContent = 'Sunday';

      const hidden = form?.querySelector('input[name="week_ending_weekday_snapshot"]');
      if (hidden) hidden.value = '';
    } catch {}
    try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
  });
  if (LOGC) console.log('[CONTRACTS] wired btnClearClient');
}


            const openOnType = (inputEl, openerName) => {
              if (!inputEl || inputEl.__wiredTyping) return;
              inputEl.__wiredTyping = true;
              if (LOGC) console.log('[CONTRACTS] typing handler installed for', openerName);
            };
            openOnType(candInput, 'candidate');
            openOnType(cliInput, 'client');
          }
        }

           const ratesTab = document.querySelector('#contractRatesTab');
        if (ratesTab) {
          // One-time wiring for the tab container (stageRates, margins)
          if (!ratesTab.__wiredStage) {
            ratesTab.__wiredStage = true;

            // Stage numeric rate fields + margins
            const stageRates = (e) => {
              const t = e.target;
              if (!t || !t.name) return;
              if (/^(paye_|umb_|charge_)/.test(t.name) || /^mileage_(charge|pay)_rate$/.test(t.name)) {
                setContractFormValue(t.name, t.value);
                if (/^(paye_|umb_|charge_)/.test(t.name)) computeContractMargins();
                try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
              }
            };
            ratesTab.addEventListener('input', stageRates, true);
            ratesTab.addEventListener('change', stageRates, true);
            computeContractMargins();
          }

          // Choose preset wiring: may run on every wire(), guarded per-button
 // Choose preset wiring: may run on every wire(), guarded per-button
const chooseBtn = document.getElementById('btnChoosePreset');
if (chooseBtn && !chooseBtn.__wired) {
  chooseBtn.__wired = true;
  chooseBtn.addEventListener('click', () => {
    console.log('[CONTRACTS] Choose preset clicked');

    const payMethod = (function () {
      try {
        const v =
          (window.modalCtx?.formState?.main?.pay_method_snapshot) ||
          document.querySelector('select[name="pay_method_snapshot"], select[name="default_pay_method_snapshot"]')?.value ||
          window.modalCtx?.data?.pay_method_snapshot ||
          'PAYE';
        return String(v).toUpperCase();
      } catch { return 'PAYE'; }
    })();

    const formEl = document.querySelector('#contractForm');
    const fsMain = window.modalCtx?.formState?.main || {};

    // Prefer staged state and modalCtx.data for client_id; DOM is last resort
    let clientId =
      (fsMain.client_id && String(fsMain.client_id).trim()) ||
      (window.modalCtx?.data?.client_id && String(window.modalCtx.data.client_id).trim()) ||
      (formEl?.querySelector('[name="client_id"]')?.value?.trim()) ||
      null;

    clientId = clientId || null; // normalise empty string to null

    const start =
      (formEl?.querySelector('[name="start_date"]')?.value?.trim()) ||
      (fsMain.start_date && String(fsMain.start_date)) ||
      (window.modalCtx?.data?.start_date && String(window.modalCtx.data.start_date)) ||
      null;

    openRatePresetPicker(
      (preset) => {
        // NEW: pre-apply snapshot for Reset
        try {
          if (typeof snapshotContractForm === 'function') snapshotContractForm();
          const src = window.modalCtx && window.modalCtx.formState ? window.modalCtx.formState : null;
          if (src) {
            window.modalCtx.__presetBefore =
              (typeof structuredClone === 'function')
                ? structuredClone(src)
                : JSON.parse(JSON.stringify(src));
          }
        } catch (e) {
          console.warn('[CONTRACTS] pre-apply snapshot failed (non-fatal)', e);
        }

        applyRatePresetToContractForm(preset, payMethod);
        try {
          const chip = document.getElementById('presetChip');
          if (chip) {
            chip.style.display = '';
            const title =
              preset.name ||
              [preset.role, preset.band ? `Band ${preset.band}` : '']
                .filter(Boolean)
                .join(' / ') ||
              'Preset';
            chip.textContent = `Preset: ${title}`;
          }
        } catch {}
        try { computeContractMargins(); } catch {}
        // Mark the contracts frame dirty so Save becomes available
        try {
          const fr = window.__getModalFrame?.();
          if (fr && (fr.kind === 'contracts' || fr.entity === 'contracts')) {
            fr.isDirty = true;
            if (typeof fr._updateButtons === 'function') fr._updateButtons();
          }
        } catch {}
        try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
      },
      {
        client_id:    clientId,
        start_date:   start,
        defaultScope: clientId ? 'CLIENT' : 'GLOBAL'
      }
    );
  });
}



          // Full reset wiring: same pattern (per-button guard, not tied to __wiredStage)
              // Full reset wiring: same pattern (per-button guard, not tied to __wiredStage)
            const resetBtn = document.getElementById('btnResetPreset');
          if (resetBtn && !resetBtn.__wired) {
            resetBtn.__wired = true;
            resetBtn.addEventListener('click', () => {
              const snap = window.modalCtx && window.modalCtx.__presetBefore ? window.modalCtx.__presetBefore : null;
              if (snap && typeof snap === 'object') {
                // Restore from snapshot
                try {
                  const form = document.querySelector('#contractForm');
                  const writeInput = (name, value) => {
                    const el = document.querySelector(`#contractRatesTab input[name="${name}"]`) ||
                               (form && form.querySelector(`[name="${name}"]`)) ||
                               document.querySelector(`[name="${name}"]`);
                    if (el) el.value = (value == null ? '' : String(value));
                    setContractFormValue(name, (value == null ? '' : String(value)));
                  };

                  // Rates
                  const rateKeys = ['paye_day','paye_night','paye_sat','paye_sun','paye_bh','umb_day','umb_night','umb_sat','umb_sun','umb_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh'];
                  for (const k of rateKeys) writeInput(k, (snap.pay || {})[k] ?? '');

                  // Mileage – try both main & pay snapshots
                  const mp = (snap.main || {})['mileage_pay_rate'];
                  const mc = (snap.main || {})['mileage_charge_rate'];
                  const mp2= (snap.pay  || {})['mileage_pay_rate'];
                  const mc2= (snap.pay  || {})['mileage_charge_rate'];
                  writeInput('mileage_pay_rate',    mp  ?? mp2  ?? '');
                  writeInput('mileage_charge_rate', mc  ?? mc2  ?? '');

                  // Bucket labels (prefer consolidated labels map if present)
                  const L = (snap.main && snap.main.__bucket_labels) ? snap.main.__bucket_labels : {
                    day:   (snap.main || {})['bucket_day']   || '',
                    night: (snap.main || {})['bucket_night'] || '',
                    sat:   (snap.main || {})['bucket_sat']   || '',
                    sun:   (snap.main || {})['bucket_sun']   || '',
                    bh:    (snap.main || {})['bucket_bh']    || ''
                  };
                  [['day','bucket_label_day'],
                   ['night','bucket_label_night'],
                   ['sat','bucket_label_sat'],
                   ['sun','bucket_label_sun'],
                   ['bh','bucket_label_bh']].forEach(([k, field]) => {
                    writeInput(field, L[k] || '');
                    // mirror to any "bucket_" fields if present
                    if (form) {
                      const el2 = form.querySelector(`[name="bucket_${k}"]`);
                      if (el2) el2.value = (L[k] || '');
                    }
                    const tr = document.querySelector(`#marginsTable tr[data-b="${k}"] > td:first-child`);
                    if (tr) tr.textContent = (L[k] || '');
                    ['cardPAYE','cardUMB','cardCHG'].forEach(cid=>{
                      const card = document.getElementById(cid);
                      const inp = card?.querySelector(`input[name$="_${k}"]`);
                      if (card && inp) { const row = inp.closest('.row'); if (row) { const lab=row.querySelector('label'); if (lab) lab.textContent=(L[k] || ''); } }
                    });
                  });

                  // Schedule
                  const tpl = (snap.main || {}).__template || null;
                  const days = ['mon','tue','wed','thu','fri','sat','sun'];
                  if (tpl) {
                    days.forEach(d => {
                      const S = tpl[d] || {};
                      writeInput(`${d}_start`, S.start || '');
                      writeInput(`${d}_end`,   S.end   || '');
                      writeInput(`${d}_break`, (S.break_minutes == null ? '' : String(S.break_minutes)));
                    });
                    const fs = (window.modalCtx.formState ||= { main:{}, pay:{} });
                    fs.main.__template = tpl;
                  } else {
                    // fallback: if snapshot had raw fields
                    days.forEach(d => {
                      writeInput(`${d}_start`, (snap.main || {})[`${d}_start`] || '');
                      writeInput(`${d}_end`,   (snap.main || {})[`${d}_end`]   || '');
                      writeInput(`${d}_break`, (snap.main || {})[`${d}_break`] || '');
                    });
                    const fs = (window.modalCtx.formState ||= { main:{}, pay:{} });
                    fs.main.__template = null;
                  }

                  // Role / band / display_site
                  writeInput('role',         (snap.main || {}).role || '');
                  writeInput('band',         (snap.main || {}).band || '');
                  writeInput('display_site', (snap.main || {}).display_site || '');

                  // Hide chip
                  try { const chip=document.getElementById('presetChip'); if (chip) { chip.style.display='none'; chip.textContent=''; } } catch {}
                } catch (err) {
                  console.warn('[CONTRACTS] preset reset restore failed, falling back to clear', err);
                }

                try { computeContractMargins(); } catch {}
                try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
                return;
              }

              // Fallback: your previous clear-to-blank behavior
              const clear = (sel) => { ratesTab.querySelectorAll(sel).forEach(el => { el.value=''; setContractFormValue(el.name, ''); }); };
              clear('input[name^="paye_"]');
              clear('input[name^="umb_"]');
              clear('input[name^="charge_"]');

              // Clear mileage
              ['mileage_charge_rate','mileage_pay_rate'].forEach(n=>{
                const el = ratesTab.querySelector(`input[name="${n}"]`);
                if (el) el.value = '';
                setContractFormValue(n, '');
              });

              // Reset bucket labels to defaults & update headings
              try {
                const defaults = { day:'Day', night:'Night', sat:'Sat', sun:'Sun', bh:'BH' };
                const form = document.querySelector('#contractForm');
                Object.entries(defaults).forEach(([k,v])=>{
                  setContractFormValue(`bucket_label_${k}`, v);
                  if (form) {
                    const el1 = form.querySelector(`[name="bucket_label_${k}"]`);
                    const el2 = form.querySelector(`[name="bucket_${k}"]`);
                    if (el1) el1.value = v;
                    if (el2) el2.value = v;
                  }
                  const tr = document.querySelector(`#marginsTable tr[data-b="${k}"] > td:first-child`);
                  if (tr) tr.textContent = v;
                  ['cardPAYE','cardUMB','cardCHG'].forEach(cid=>{
                    const card = document.getElementById(cid);
                    const inp = card?.querySelector(`input[name$="_${k}"]`);
                    if (card && inp) { const row = inp.closest('.row'); if (row) { const lab=row.querySelector('label'); if (lab) lab.textContent=v; } }
                  });
                });
              } catch {}

              // Clear schedule grid and stage std_schedule_json=null
              try {
                const days = ['mon','tue','wed','thu','fri','sat','sun'];
                days.forEach(d=>{
                  const s = document.querySelector(`input[name="${d}_start"]`);
                  const e = document.querySelector(`input[name="${d}_end"]`);
                  const b = document.querySelector(`input[name="${d}_break"]`);
                  if (s) s.value = '';
                  if (e) e.value = '';
                  if (b) b.value = '';
                });
                const fs = (window.modalCtx.formState ||= { main:{}, pay:{} });
                fs.main.__template = null;
              } catch {}

              // Clear role/band/display_site
              try {
                const fs = (window.modalCtx.formState ||= { main:{}, pay:{} }).main ||= {};
                fs.role = ''; fs.band = ''; fs.display_site = '';
                const form = document.querySelector('#contractForm');
                if (form) {
                  const r=form.querySelector('[name="role"]'); if (r) r.value='';
                  const b=form.querySelector('[name="band"]'); if (b) b.value='';
                  const s=form.querySelector('[name="display_site"]'); if (s) s.value='';
                }
              } catch {}

              // Clear chip & recompute margins
              try { const chip=document.getElementById('presetChip'); if (chip) { chip.style.display='none'; chip.textContent=''; } } catch {}
              try { computeContractMargins(); } catch {}
              try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
            });
          }


        }

       };

  setTimeout(() => {
        const fr = window.__getModalFrame?.();
        const prevDirty = fr?.isDirty;
        if (fr) fr._suppressDirty = true;

        wire();

        if (fr) {
          fr._suppressDirty = false;
          fr.isDirty = prevDirty;
          fr._updateButtons && fr._updateButtons();
        }
      }, 0);
      if (!window.__contractsWireBound) {
        window.__contractsWireBound = true;

        const rewire = () => {
          const fr = window.__getModalFrame?.();
          const prevDirty = fr?.isDirty;
          if (fr) fr._suppressDirty = true;

          setTimeout(() => {
            wire();
            if (fr) {
              fr._suppressDirty = false;
              fr.isDirty = prevDirty;
              fr._updateButtons && fr._updateButtons();
            }
          }, 0);
        };

        window.addEventListener('contracts-main-rendered', rewire);
        window.addEventListener('contracts-rates-rendered', rewire);
      }

 // Re-wire when the user clicks between Main / Rates / Calendar
      const tabsEl = document.getElementById('modalTabs');
      if (tabsEl && !tabsEl.__wired_contract_stage) {
        tabsEl.__wired_contract_stage = true;
        tabsEl.addEventListener('click', () => {
          const fr = window.__getModalFrame?.();
          const prevDirty = fr?.isDirty;
          if (fr) fr._suppressDirty = true;

          snapshotContractForm();
          setTimeout(() => {
            wire();
            if (fr) {
              fr._suppressDirty = false;
              fr.isDirty = prevDirty;
              fr._updateButtons && fr._updateButtons();
            }
          }, 0);
        });
      }

    },


    // 7th: options (now with noParentGate)
    {
      kind: 'contracts',
      extraButtons,
      noParentGate: !!isSuccessorCreate,
      stayOpenOnSave: !!isSuccessorCreate,
      _trace: (LOGC && {
        tag: 'contracts-open',
        isCreate,
        isSuccessorCreate,
        openToken: window.modalCtx.openToken
      })
    }
  );
   setTimeout(() => {
    try {
      const fr = window.__getModalFrame?.();
      if (fr && (fr.entity === 'contracts' || fr.kind === 'contracts') && typeof fr.onReturn === 'function' && !fr.__contractsInit) {
        fr.__contractsInit = true;
        fr.onReturn();
        if (LOGC) console.log('[CONTRACTS] initial onReturn() executed');
      } else if (LOGC) {
        console.log('[CONTRACTS] onReturn not executed', {
          hasFrame: !!fr, entity: fr?.entity, kind: fr?.kind, hasOnReturn: typeof fr?.onReturn === 'function', init: !!fr?.__contractsInit
        });
      }

      // 🔹 After wiring, surface any pay-method mismatch warnings returned by backend
      try {
        const baseData = (window.modalCtx && window.modalCtx.data)
          ? window.modalCtx.data
          : (row || {});
        const warnings = Array.isArray(baseData.warnings) ? baseData.warnings : [];
        if (warnings.length && typeof showModalHint === 'function') {
          showModalHint(warnings.join(' '), 'warn');
        }
      } catch (warnErr) {
        if (LOGC) console.warn('[CONTRACTS] show pay-method warnings failed', warnErr);
      }

    } catch (e) {
      if (LOGC) console.warn('[CONTRACTS] initial onReturn failed', e);
    } finally {
      if (LOGC) console.log('[CONTRACTS] openContract EXIT');
    }
  }, 0);
}



// ─────────────────────────────────────────────────────────────────────────────
// NEW: getSummaryFingerprint(section)
// Deterministic fingerprint of current filters (and section)
// ─────────────────────────────────────────────────────────────────────────────
function getSummaryFingerprint(section){
  window.__listState = window.__listState || {};
  const st = (window.__listState[section] ||= { page:1, pageSize:50, total:null, hasMore:false, filters:null });
  const filters = st.filters || {};
  const norm = (o)=> {
    const k = Object.keys(o||{}).sort();
    const out = {};
    for (const key of k) {
      const v = o[key];
      if (Array.isArray(v)) out[key] = v.slice().map(x=>String(x)).sort();
      else out[key] = v;
    }
    return out;
  };
  return JSON.stringify({ section, filters: norm(filters) });
}



// ─────────────────────────────────────────────────────────────────────────────
/* NEW: primeSummaryMembership(section, fingerprint)
   - Calls backend id-list endpoint to fetch **all ids** for the current filters
   - Stores into window.__summaryCache[section][fingerprint] = { ids, total, updatedAt }
   - Non-blocking; safe to call repeatedly (dedup by fingerprint)
*/
// ─────────────────────────────────────────────────────────────────────────────
async function primeSummaryMembership(section, fingerprint){
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;
  window.__summaryCache = window.__summaryCache || { candidates:{}, clients:{} };
  const secKey = (section==='candidates'||section==='clients') ? section : null;
  if (!secKey) return;

  const cache = window.__summaryCache[secKey] ||= {};
  const existing = cache[fingerprint];
  if (existing && existing._inflight) return;
  if (existing && Array.isArray(existing.ids) && existing.ids.length) return;
  if (existing && existing.updatedAt && (Date.now() - existing.updatedAt > 60_000)) existing.stale = true;

  cache[fingerprint] = cache[fingerprint] || {};
  cache[fingerprint]._inflight = true;

  try {
    window.__listState = window.__listState || {};
    const st   = window.__listState[secKey] || {};
    const qs   = buildSummaryFilterQSForIdList(secKey, st.filters || {});
    const url  = API(`/api/pickers/${secKey}/id-list${qs ? ('?'+qs) : ''}`);
    const resp = await authFetch(url);
    const json = await resp.json().catch(()=>null);

    const ids = Array.isArray(json?.ids) ? json.ids.map(String) : [];
    cache[fingerprint] = {
      ids,
      total: Number(json?.total || ids.length || 0),
      updatedAt: Date.now(),
      stale: false,
    };
    if (LOGC) console.log('[SUMMARY][primeMembership]', { section: secKey, total: ids.length });
  } catch (e) {
    cache[fingerprint] = { ids: [], total: 0, updatedAt: Date.now(), stale: true };
    if (LOGC) console.warn('[SUMMARY][primeMembership] failed', e);
  } finally {
    if (cache[fingerprint]) delete cache[fingerprint]._inflight;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// NEW: getSummaryMembership(section, fingerprint)
// Returns { ids, total, updatedAt, stale } or a stub if missing
// ─────────────────────────────────────────────────────────────────────────────
function getSummaryMembership(section, fingerprint){
  window.__summaryCache = window.__summaryCache || { candidates:{}, clients:{} };
  const secKey = (section==='candidates'||section==='clients') ? section : null;
  if (!secKey) return { ids: [], total: 0, updatedAt: 0, stale: true };
  const ent = window.__summaryCache[secKey] || {};
  const res = ent[fingerprint] || { ids: [], total: 0, updatedAt: 0, stale: true };
  return { ids: Array.isArray(res.ids) ? res.ids : [], total: Number(res.total||0), updatedAt: Number(res.updatedAt||0), stale: !!res.stale };
}

// ─────────────────────────────────────────────────────────────────────────────
// NEW: buildSummaryFilterQSForIdList(section, filters)
// Converts current summary filters to QS for id-list endpoints.
// Reuse your search QS rules; keep only filter params (no paging).
// ─────────────────────────────────────────────────────────────────────────────
function buildSummaryFilterQSForIdList(section, filters){
  const sp = new URLSearchParams();
  const f  = filters || {};

  if (Array.isArray(f.ids) && f.ids.length) sp.set('ids', f.ids.join(','));
  if (f.role)      sp.set('role', f.role);
  if (f.band)      sp.set('band', f.band);
  if (f.client_id) sp.set('client_id', f.client_id);
  if (f.q)         sp.set('q', f.q);

  if (f.active != null && section === 'candidates') sp.set('active', String(!!f.active));

  return sp.toString();
}



// ─────────────────────────────────────────────────────────────────────────────
// NEW: ensurePickerDatasetPrimed(entity)  entity in {'candidates','clients'}
// - Ensures dataset snapshot is loaded, then applies pending deltas.
// - Safe to call before opening a picker.
// ─────────────────────────────────────────────────────────────────────────────

async function ensurePickerDatasetPrimed(entity){
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;
  window.__pickerData = window.__pickerData || { candidates:{ since:null, itemsById:{} }, clients:{ since:null, itemsById:{} } };
  const ds = window.__pickerData[entity] ||= { since:null, itemsById:{} };

  if (!ds._initStarted) {
    ds._initStarted = true;
    try {
      const url  = API(`/api/pickers/${entity}/snapshot`);
      const resp = await authFetch(url);
      const json = await resp.json();
      ds.itemsById = ds.itemsById || {};
      const arr = Array.isArray(json?.items) ? json.items : [];
      for (const it of arr) ds.itemsById[String(it.id)] = it;
      ds.since = json?.since ?? ds.since ?? null;
      if (LOGC) console.log('[PICKER][dataset snapshot]', { entity, count: arr.length, since: ds.since });
    } catch (e) {
      if (LOGC) console.warn('[PICKER][dataset snapshot] failed', e);
    }
  }

  try {
    if (ds.since != null) {
      const url  = API(`/api/pickers/${entity}/delta?since=${encodeURIComponent(ds.since)}`);
      const resp = await authFetch(url);
      if (resp && resp.ok) {
        const json = await resp.json();
        applyDatasetDelta(entity, json);
        if (LOGC) console.log('[PICKER][dataset delta]', { entity, added: json?.added?.length||0, updated: json?.updated?.length||0, removed: json?.removed?.length||0, since: json?.since });
      }
    }
  } catch (e) {
    if (LOGC) console.warn('[PICKER][dataset delta] failed', e);
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// NEW: applyDatasetDelta(entity, delta)  // { added:[], updated:[], removed:[], since }
// ─────────────────────────────────────────────────────────────────────────────
function applyDatasetDelta(entity, delta){
  window.__pickerData = window.__pickerData || { candidates:{ since:null, itemsById:{} }, clients:{ since:null, itemsById:{} } };
  const ds = window.__pickerData[entity] ||= { since:null, itemsById:{} };
  ds.itemsById = ds.itemsById || {};

  // merge in additions
  for (const it of (delta?.added || [])) {
    ds.itemsById[String(it.id)] = it;
  }

  // shallow-merge updates into existing (preserve any fields not present in payload)
  for (const it of (delta?.updated || [])) {
    const k = String(it.id);
    ds.itemsById[k] = { ...(ds.itemsById[k] || {}), ...it };
  }

  // remove deleted ids
  for (const id of (delta?.removed || [])) {
    delete ds.itemsById[String(id)];
  }

  // advance since watermark
  if (delta?.since != null) ds.since = delta.since;
}
// ─────────────────────────────────────────────────────────────────────────────
// NEW: pickersLocalFilterAndSort(entity, ids, query, sortKey, sortDir)
// Uses dataset cache rows restricted to {ids}, filters locally and sorts.
// ─────────────────────────────────────────────────────────────────────────────
function pickersLocalFilterAndSort(entity, ids, query, sortKey, sortDir){
  window.__pickerData = window.__pickerData || { candidates:{ itemsById:{} }, clients:{ itemsById:{} } };
  const itemsById = (window.__pickerData[entity]||{}).itemsById || {};
  const norm = (s)=> (s||'').toString().toLowerCase();
  const toks = norm(query||'').split(/\s+/).filter(Boolean);

  // Accept either an array of IDs or an array of row objects
  const rows = (ids && ids.length && typeof ids[0] === 'object')
    ? ids.slice()
    : (ids || []).map(id => itemsById[String(id)]).filter(Boolean);

  if (typeof window.__LOG_CONTRACTS === 'boolean' ? window.__LOG_CONTRACTS : true) {
    console.log('[PLFS:entry]', { entity, q: query, toks, in: rows.length, sortKey, sortDir });
  }

  const scoreRow = (r) => {
    if (!toks.length) return 0;
    let nameScore = 0, extraScore = 0;
    let allNameTokensMatch = true;
    if (entity === 'candidates') {
      const first = norm(r.first_name), last = norm(r.last_name);
      const disp  = norm(r.display_name || `${r.first_name||''} ${r.last_name||''}`);
      const role  = norm(r.roles_display);
      const email = norm(r.email);
      toks.forEach(t=>{
        let matched=false;
        if (first.startsWith(t)) { nameScore+=6; matched=true; }
        if (last.startsWith(t))  { nameScore+=6; matched=true; }
        if (disp.includes(t))    { nameScore+=5; matched=true; }
        if (first===t||last===t) { nameScore+=8; matched=true; }
        if (!matched) allNameTokensMatch=false;
        if (role.includes(t))    extraScore+=2;
        if (email.includes(t))   extraScore+=1;
      });
    } else {
      const name  = norm(r.name);
      const email = norm(r.primary_invoice_email);
      toks.forEach(t=>{
        let matched=false;
        if (name.includes(t))    { nameScore+=6; matched=true; }
        if (!matched) allNameTokensMatch=false;
        if (email.includes(t))   extraScore+=1;
      });
    }
    if (!allNameTokensMatch) return 0;
    if (nameScore <= 0) return 0;
    return nameScore + extraScore;
  };

  if (!toks.length) {
    const cmpAlpha = (a,b) => {
      const av = (a?.[sortKey] ?? '').toString().toLowerCase();
      const bv = (b?.[sortKey] ?? '').toString().toLowerCase();
      if (av < bv) return (sortDir==='asc'? -1 : 1);
      if (av > bv) return (sortDir==='asc'? 1 : -1);
      return 0;
    };
    const out = rows.slice().sort(cmpAlpha);
    if (typeof window.__LOG_CONTRACTS === 'boolean' ? window.__LOG_CONTRACTS : true) {
      console.log('[PLFS:noquery]', { entity, out: out.length, sample: out.slice(0,6).map(r=> r.display_name||r.name) });
    }
    return out;
  }

  const withScore = rows.map(r => ({ r, s: scoreRow(r) })).filter(x => x.s > 0);
  withScore.sort((A,B) => {
    if (A.s !== B.s) return B.s - A.s;
    const av = (A.r?.[sortKey] ?? '').toString().toLowerCase();
    const bv = (B.r?.[sortKey] ?? '').toString().toLowerCase();
    if (av < bv) return (sortDir==='asc'? -1 : 1);
    if (av > bv) return (sortDir==='asc'? 1 : -1);
    return 0;
  });
  const out = withScore.map(x => x.r);
  if (typeof window.__LOG_CONTRACTS === 'boolean' ? window.__LOG_CONTRACTS : true) {
    console.log('[PLFS:out]', { entity, q: query, out: out.length, sample: out.slice(0,6).map(r=> r.display_name||r.name) });
  }
  return out;
}

// ─────────────────────────────────────────────────────────────────────────────
// NEW: revalidateCandidateOnPick(id) / revalidateClientOnPick(id)
// Fetches current detail and refreshes dataset cache before accept
// ─────────────────────────────────────────────────────────────────────────────
async function revalidateCandidateOnPick(id){
  const url  = API(`/api/candidates/${encodeURIComponent(id)}`);
  const resp = await authFetch(url);
  if (!resp || !resp.ok) throw new Error('Could not fetch candidate.');
  const r = await resp.json();
  window.__pickerData = window.__pickerData || { candidates:{ itemsById:{} } };
  const ds = window.__pickerData.candidates ||= { itemsById:{} };
  const proj = {
    id: r.id,
    display_name: r.display_name || `${r.first_name||''} ${r.last_name||''}`.trim(),
    first_name: r.first_name || '',
    last_name: r.last_name || '',
    email: r.email || '',
    roles_display: Array.isArray(r.roles)? formatRolesSummary(r.roles) : (r.role||''),
    active: r.active !== false
  };
  ds.itemsById[String(r.id)] = proj;
}

async function revalidateClientOnPick(id){
  const url  = API(`/api/clients/${encodeURIComponent(id)}`);
  const resp = await authFetch(url);
  if (!resp || !resp.ok) throw new Error('Could not fetch client.');
  const r = await resp.json();
  window.__pickerData = window.__pickerData || { clients:{ itemsById:{} } };
  const ds = window.__pickerData.clients ||= { itemsById:{} };
  const proj = {
    id: r.id,
    name: r.name || '',
    primary_invoice_email: r.primary_invoice_email || '',
    active: r.active !== false
  };
  ds.itemsById[String(r.id)] = proj;
}

// ──────────────────────────────────────────────────────────────────────────────
// NEW: contractWeekCreateAdditional — POST /api/contract-weeks/:id/additional
// ──────────────────────────────────────────────────────────────────────────────
async function contractWeekCreateAdditional(week_id) {
  const r = await authFetch(API(`/api/contract-weeks/${encodeURIComponent(String(week_id))}/additional`), {
    method: 'POST'
  });
  if (!r?.ok) throw new Error('Create additional sheet failed');
  return r.json();
}

// ──────────────────────────────────────────────────────────────────────────────
// (Optional UX) NEW: openCandidatePicker / openClientPicker
// Lightweight pickers that call onPick({id,label}) and close
// ──────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: openCandidatePicker — delegated clicks + debounced live search with ranking
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: openCandidatePicker(onPick)
// - Uses summary-membership cache (ids) ∩ dataset cache (minimal rows)
// - Type-to-filter + header sort (Surname/First/Role/Email) locally
// - Revalidates on pick
// ─────────────────────────────────────────────────────────────────────────────

async function openCandidatePicker(onPick) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : true; // default ON

  if (LOGC) console.log('[PICKER][candidates] ensure dataset primed → start');
  await ensurePickerDatasetPrimed('candidates').catch(e=>{ if (LOGC) console.warn('[PICKER][candidates] priming failed', e); });

  let fp = getSummaryFingerprint('candidates');
  let mem = getSummaryMembership('candidates', fp);
  if (!mem?.ids?.length || mem?.stale) {
    if (LOGC) console.log('[PICKER][candidates] membership empty/stale → priming', { fp, mem });
    await primeSummaryMembership('candidates', fp);
    fp  = getSummaryFingerprint('candidates');
    mem = getSummaryMembership('candidates', fp);
  }

  const ds  = (window.__pickerData ||= {}).candidates || { since:null, itemsById:{} };
  const items = ds.itemsById || {};

  const baseIds  = (mem?.ids && mem.ids.length) ? mem.ids : Object.keys(items);
  const baseRows = baseIds.map(id => items[id]).filter(Boolean);

  if (LOGC) console.log('[PICKER][candidates] dataset snapshot', {
    fingerprint: fp, total: mem?.total, ids: baseIds.length, stale: !!mem?.stale, since: ds?.since,
    rowsBase: baseRows.length, missingItems: baseIds.length - baseRows.length
  });

  const renderRows = (rows) => rows.map(r => {
    const first = r.first_name || '';
    const last  = r.last_name || '';
    const label = (r.display_name || `${first} ${last}`).trim() || (r.tms_ref || r.id || '');
    const sub   = [r.email, r.roles_display].filter(Boolean).join(' • ');
    return `
      <tr data-id="${r.id||''}" data-label="${(label||'').replace(/"/g,'&quot;')}" class="pick-row">
        <td data-k="last_name">${(last)}</td>
        <td data-k="first_name">${(first)}</td>
        <td data-k="roles_display" class="mini">${(r.roles_display||'')}</td>
        <td data-k="email" class="mini">${(r.email||'')}</td>
      </tr>`;
  }).join('');

  const html = `
    <div class="tabc">
      <div class="row"><label>Search</label><div class="controls">
        <input class="input" type="text" id="pickerSearch" placeholder="${mem?.stale ? 'Priming list… type to narrow' : 'Type a first name, surname, role or email…'}"/>
      </div></div>
      <div class="hint">Showing candidates from the current summary list${mem?.total ? ` (${mem.total} total)` : ''}.</div>
      <table class="grid" id="pickerTable">
        <thead>
          <tr>
            <th data-sort="last_name">Surname</th>
            <th data-sort="first_name">First name</th>
            <th data-sort="roles_display">Role</th>
            <th data-sort="email">Email</th>
          </tr>
        </thead>
        <tbody id="pickerTBody">${renderRows(baseRows)}</tbody>
      </table>
    </div>`;

  if (LOGC) console.log('[PICKER][candidates] opening modal');
  showModal('Pick Candidate',[{key:'p',title:'Candidates'}],()=>html,async()=>true,false,()=>{

    const tbody   = document.getElementById('pickerTBody');
    const search  = document.getElementById('pickerSearch');
    const table   = document.getElementById('pickerTable');
    if (LOGC) console.log('[PICKER][candidates] onReturn', { hasTBody: !!tbody, hasSearch: !!search, hasTable: !!table });
    if (!tbody || !search || !table) return;

    let sortKey = 'last_name', sortDir = 'asc';
    let currentRows = baseRows.slice();

    const applyRows = (rows) => {
      tbody.innerHTML = renderRows(rows);
      if (LOGC) console.log('[PICKER][candidates] render()', { count: rows.length, sample: rows.slice(0,6).map(r=>r.display_name||`${r.first_name} ${r.last_name}`) });
      const first = tbody.querySelector('tr[data-id]');
      if (first) { first.classList.add('active'); }
    };
    const doFilter  = (q) => {
      const fn = (window.pickersLocalFilterAndSort || pickersLocalFilterAndSort);
      const out = fn('candidates', currentRows.length ? currentRows : baseRows, q, sortKey, sortDir);
      if (LOGC) console.log('[PICKER][candidates] doFilter()', { q, in: (currentRows.length||baseRows.length), out: out.length });
      return out;
    };

    if (!tbody.__wiredClick) {
      tbody.__wiredClick = true;
      const choose = async (tr) => {
        const id    = tr.getAttribute('data-id');
        const label = tr.getAttribute('data-label') || tr.textContent.trim();
        if (LOGC) console.log('[PICKER][candidates] select()', { id, label });
        try {
          await revalidateCandidateOnPick(id);
          if (typeof onPick==='function') onPick({ id, label });
        } catch (err) {
          console.warn('[PICKER][candidates] select() validation failed', err);
          alert(err?.message || 'Selection could not be validated.');
          return;
        }
        const closeBtn = document.getElementById('btnCloseModal'); if (closeBtn) closeBtn.click();
      };
      tbody.addEventListener('click', async (e) => {
        const tr = e.target && e.target.closest('tr[data-id]'); if (!tr) return;
        await choose(tr);
      });
      tbody.addEventListener('dblclick', async (e) => {
        const tr = e.target && e.target.closest('tr[data-id]'); if (!tr) return;
        await choose(tr);
      });
      if (LOGC) console.log('[PICKER][candidates] wired click + dblclick handler');
    }

    if (!table.__wiredSort) {
      table.__wiredSort = true;
      table.querySelector('thead').addEventListener('click', (e) => {
        const th = e.target && e.target.closest('th[data-sort]'); if (!th) return;
        const key = th.getAttribute('data-sort');
        sortDir = (sortKey === key && sortDir === 'asc') ? 'desc' : 'asc';
        sortKey = key;
        currentRows = doFilter(search.value.trim());
        applyRows(currentRows);
        if (LOGC) console.log('[PICKER][candidates] sort', { sortKey, sortDir, count: currentRows.length });
      });
      if (LOGC) console.log('[PICKER][candidates] wired sort header');
    }

    let t = 0;
    if (!search.__wiredInput) {
      search.__wiredInput = true;
      search.addEventListener('input', () => {
        const q = search.value.trim();
        if (LOGC) console.log('[PICKER][candidates] search input', { q });
        if (t) clearTimeout(t);
        t = setTimeout(() => { currentRows = doFilter(q); applyRows(currentRows); }, 150);
      });
      if (LOGC) console.log('[PICKER][candidates] wired search input');
    }

    if (!search.__wiredKey) {
      search.__wiredKey = true;
      search.addEventListener('keydown', async (e) => {
        const itemsEls = Array.from(tbody.querySelectorAll('tr[data-id]'));
        if (!itemsEls.length) {
          if (e.key === 'Escape') { const closeBtn = document.getElementById('btnCloseModal'); if (closeBtn) closeBtn.click(); }
          return;
        }
        const idx = itemsEls.findIndex(tr => tr.classList.contains('active'));
        const setActive = (i) => {
          itemsEls.forEach(tr=>tr.classList.remove('active'));
          itemsEls[i].classList.add('active');
          itemsEls[i].scrollIntoView({block:'nearest'});
        };
        if (e.key === 'ArrowDown') { e.preventDefault(); setActive(Math.min((idx<0?0:idx+1), itemsEls.length-1)); }
        if (e.key === 'ArrowUp')   { e.preventDefault(); setActive(Math.max((idx<0?0:idx-1), 0)); }
        if (e.key === 'Enter')     { e.preventDefault(); const target = itemsEls[Math.max(idx,0)]; if (target) target.click(); }
        if (e.key === 'Escape')    { e.preventDefault(); const closeBtn = document.getElementById('btnCloseModal'); if (closeBtn) closeBtn.click(); }
      });
      if (LOGC) console.log('[PICKER][candidates] wired search keydown');
    }

    setTimeout(() => { try { search.focus(); if (LOGC) console.log('[PICKER][candidates] search focused'); } catch {} }, 0);
  },{kind:'picker'});

  // 🔧 Post-render kick: ensure the picker's onReturn wiring runs once on first open
  setTimeout(() => {
    try {
      const fr = window.__getModalFrame?.();
      const willCall = !!(fr && fr.kind === 'picker' && typeof fr.onReturn === 'function' && !fr.__pickerInit);
      if (LOGC) console.log('[PICKER][candidates] post-render kick', { hasFrame: !!fr, kind: fr?.kind, hasOnReturn: typeof fr?.onReturn === 'function', already: !!fr?.__pickerInit, willCall });
      if (willCall) { fr.__pickerInit = true; fr.onReturn(); if (LOGC) console.log('[PICKER][candidates] initial onReturn() executed'); }
    } catch (e) { if (LOGC) console.warn('[PICKER][candidates] post-render kick failed', e); }
  }, 0);
}


// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: openClientPicker — delegated clicks + debounced live search
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: openClientPicker(onPick)
// - Uses summary-membership cache + dataset cache (clients)
// - Type-to-filter + header sort (Name/Email) locally
// - Revalidates on pick
// ─────────────────────────────────────────────────────────────────────────────


async function openClientPicker(onPick) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : true; // default ON

  if (LOGC) console.log('[PICKER][clients] ensure dataset primed → start');
  await ensurePickerDatasetPrimed('clients').catch(e=>{ if (LOGC) console.warn('[PICKER][clients] priming failed', e); });

  let fp = getSummaryFingerprint('clients');
  let mem = getSummaryMembership('clients', fp);
  if (!mem?.ids?.length || mem?.stale) {
    if (LOGC) console.log('[PICKER][clients] membership empty/stale → priming', { fp, mem });
    await primeSummaryMembership('clients', fp);
    fp  = getSummaryFingerprint('clients');
    mem = getSummaryMembership('clients', fp);
  }

  const ds  = (window.__pickerData ||= {}).clients || { since:null, itemsById:{} };
  const items = ds.itemsById || {};

  const baseIds  = (mem?.ids && mem.ids.length) ? mem.ids : Object.keys(items);
  const baseRows = baseIds.map(id => items[id]).filter(Boolean);

  if (LOGC) console.log('[PICKER][clients] dataset snapshot', {
    fingerprint: fp, total: mem?.total, ids: baseIds.length, stale: !!mem?.stale, since: ds?.since,
    rowsBase: baseRows.length, missingItems: baseIds.length - baseRows.length
  });

  const renderRows = (rows) => rows.map(r => {
    const label = (r.name || '').trim();
    const sub   = (r.primary_invoice_email || '').trim();
    return `
      <tr data-id="${r.id||''}" data-label="${(label||'').replace(/"/g,'&quot;')}" class="pick-row">
        <td data-k="name">${label}</td>
        <td data-k="primary_invoice_email" class="mini">${sub}</td>
      </tr>`;
  }).join('');

  const html = `
    <div class="tabc">
      <div class="row"><label>Search</label><div class="controls">
        <input class="input" type="text" id="pickerSearch" placeholder="${mem?.stale ? 'Priming list… type to narrow' : 'Type a client name or email…'}"/>
      </div></div>
      <div class="hint">Showing clients from the current summary list${mem?.total ? ` (${mem.total} total)` : ''}.</div>
      <table class="grid" id="pickerTable">
        <thead>
          <tr>
            <th data-sort="name">Name</th>
            <th data-sort="primary_invoice_email">Email</th>
          </tr>
        </thead>
        <tbody id="pickerTBody">${renderRows(baseRows)}</tbody>
      </table>
    </div>`;

  if (LOGC) console.log('[PICKER][clients] opening modal');
  showModal('Pick Client',[{key:'p',title:'Clients'}],()=>html,async()=>true,false,()=>{

    const tbody   = document.getElementById('pickerTBody');
    const search  = document.getElementById('pickerSearch');
    const table   = document.getElementById('pickerTable');
    if (LOGC) console.log('[PICKER][clients] onReturn', { hasTBody: !!tbody, hasSearch: !!search, hasTable: !!table });
    if (!tbody || !search || !table) return;

    let sortKey = 'name', sortDir = 'asc';
    let currentRows = baseRows.slice();

    const applyRows = (rows) => {
      tbody.innerHTML = renderRows(rows);
      if (LOGC) console.log('[PICKER][clients] render()', { count: rows.length, sample: rows.slice(0,6).map(r=>r.name) });
      const first = tbody.querySelector('tr[data-id]');
      if (first) { first.classList.add('active'); }
    };
    const doFilter  = (q) => {
      const fn = (window.pickersLocalFilterAndSort || pickersLocalFilterAndSort);
      const out = fn('clients', currentRows.length ? currentRows : baseRows, q, sortKey, sortDir);
      if (LOGC) console.log('[PICKER][clients] doFilter()', { q, in: (currentRows.length||baseRows.length), out: out.length });
      return out;
    };

    if (!tbody.__wiredClick) {
      tbody.__wiredClick = true;
      const choose = async (tr) => {
        const id    = tr.getAttribute('data-id');
        const label = tr.getAttribute('data-label') || tr.textContent.trim();
        if (LOGC) console.log('[PICKER][clients] select()', { id, label });
        try {
          await revalidateClientOnPick(id);
          if (typeof onPick==='function') onPick({ id, label });
        } catch (err) {
          console.warn('[PICKER][clients] select() validation failed', err);
          alert(err?.message || 'Selection could not be validated.');
          return;
        }
        const closeBtn = document.getElementById('btnCloseModal'); if (closeBtn) closeBtn.click();
      };
      tbody.addEventListener('click', async (e) => {
        const tr = e.target && e.target.closest('tr[data-id]'); if (!tr) return;
        await choose(tr);
      });
      tbody.addEventListener('dblclick', async (e) => {
        const tr = e.target && e.target.closest('tr[data-id]'); if (!tr) return;
        await choose(tr);
      });
      if (LOGC) console.log('[PICKER][clients] wired click + dblclick handler');
    }

    if (!table.__wiredSort) {
      table.__wiredSort = true;
      table.querySelector('thead').addEventListener('click', (e) => {
        const th = e.target && e.target.closest('th[data-sort]'); if (!th) return;
        const key = th.getAttribute('data-sort');
        sortDir = (sortKey === key && sortDir === 'asc') ? 'desc' : 'asc';
        sortKey = key;
        currentRows = doFilter(search.value.trim());
        applyRows(currentRows);
        if (LOGC) console.log('[PICKER][clients] sort', { sortKey, sortDir, count: currentRows.length });
      });
      if (LOGC) console.log('[PICKER][clients] wired sort header');
    }

    let t = 0;
    if (!search.__wiredInput) {
      search.__wiredInput = true;
      search.addEventListener('input', () => {
        const q = search.value.trim();
        if (LOGC) console.log('[PICKER][clients] search input', { q });
        if (t) clearTimeout(t);
        t = setTimeout(() => { currentRows = doFilter(q); applyRows(currentRows); }, 150);
      });
      if (LOGC) console.log('[PICKER][clients] wired search input');
    }

    if (!search.__wiredKey) {
      search.__wiredKey = true;
      search.addEventListener('keydown', async (e) => {
        const itemsEls = Array.from(tbody.querySelectorAll('tr[data-id]'));
        if (!itemsEls.length) {
          if (e.key === 'Escape') { const closeBtn = document.getElementById('btnCloseModal'); if (closeBtn) closeBtn.click(); }
          return;
        }
        const idx = itemsEls.findIndex(tr => tr.classList.contains('active'));
        const setActive = (i) => {
          itemsEls.forEach(tr=>tr.classList.remove('active'));
          itemsEls[i].classList.add('active');
          itemsEls[i].scrollIntoView({ block:'nearest' });
        };
        if (e.key === 'ArrowDown') { e.preventDefault(); setActive(Math.min((idx<0?0:idx+1), itemsEls.length-1)); }
        if (e.key === 'ArrowUp')   { e.preventDefault(); setActive(Math.max((idx<0?0:idx-1), 0)); }
        if (e.key === 'Enter')     { e.preventDefault(); const target = itemsEls[Math.max(idx,0)]; if (target) target.click(); }
        if (e.key === 'Escape')    { e.preventDefault(); const closeBtn = document.getElementById('btnCloseModal'); if (closeBtn) closeBtn.click(); }
      });
      if (LOGC) console.log('[PICKER][clients] wired search keydown');
    }

    setTimeout(() => { try { search.focus(); if (LOGC) console.log('[PICKER][clients] search focused'); } catch {} }, 0);
  },{kind:'picker'});

  // 🔧 Post-render kick: ensure the picker's onReturn wiring runs once on first open
  setTimeout(() => {
    try {
      const fr = window.__getModalFrame?.();
      const willCall = !!(fr && fr.kind === 'picker' && typeof fr.onReturn === 'function' && !fr.__pickerInit);
      if (LOGC) console.log('[PICKER][clients] post-render kick', { hasFrame: !!fr, kind: fr?.kind, hasOnReturn: typeof fr?.onReturn === 'function', already: !!fr?.__pickerInit, willCall });
      if (willCall) { fr.__pickerInit = true; fr.onReturn(); if (LOGC) console.log('[PICKER][clients] initial onReturn() executed'); }
    } catch (e) { if (LOGC) console.warn('[PICKER][clients] post-render kick failed', e); }
  }, 0);
}


// ===== NEW HELPERS / WRAPPERS =====

// Fetch one candidate by id (adjust endpoint to your API if needed)
async function getCandidate(candidate_id) {
  if (!candidate_id) throw new Error('candidate_id required');
  const r = await authFetch(API(`/api/candidates/${encodeURIComponent(String(candidate_id))}`));
  if (!r?.ok) throw new Error('Failed to load candidate');
  return r.json();
}

// Fetch one client by id (adjust endpoint to your API if needed)
async function getClient(client_id) {
  if (!client_id) throw new Error('client_id required');
  const r = await authFetch(API(`/api/clients/${encodeURIComponent(String(client_id))}`));
  if (!r?.ok) throw new Error('Failed to load client');
  return r.json();
}

// Set/update the non-blocking hint text in the contract modal footer
function showModalHint(text, tone /* 'ok' | 'warn' | 'fail' */) {
  const el = byId('modalHint'); if (!el) return;
  el.textContent = text || '';
  el.classList.remove('tag-ok','tag-warn','tag-fail');
  if (tone === 'ok')   el.classList.add('tag-ok');
  if (tone === 'warn') el.classList.add('tag-warn');
  if (tone === 'fail') el.classList.add('tag-fail');
}

// Generic live-filter helper for picker tables
function wirePickerLiveFilter(inputEl, tableEl) {
  const rows = Array.from(tableEl.querySelectorAll('tbody tr'));
  const norm = (s) => (s||'').toLowerCase();
  inputEl.addEventListener('input', () => {
    const q = norm(inputEl.value);
    rows.forEach(tr => {
      const show = !q || norm(tr.textContent).includes(q);
      tr.style.display = show ? '' : 'none';
    });
  });
}

// Convenience: set a form field inside the contract modal
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: setContractFormValue (adds logging)
// ─────────────────────────────────────────────────────────────────────────────
function setContractFormValue(name, value) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;

  // Do not stage ward_hint at all (deprecated)
  if (name === 'ward_hint') {
    if (LOGC) console.log('[CONTRACTS] setContractFormValue ignored (ward_hint deprecated)');
    return;
  }

  let targetName = (name === 'default_pay_method_snapshot') ? 'pay_method_snapshot' : name;

  try {
    const locked = !!(window.modalCtx?.formState?.main?.__pay_locked);
    if ((targetName === 'pay_method_snapshot' || name === 'default_pay_method_snapshot') && locked) {
      if (LOGC) console.log('[CONTRACTS] setContractFormValue ignored (pay method locked)', { name, value });
      return;
    }
  } catch {}

  const form =
    document.querySelector('#modalBody #contractForm') ||
    document.querySelector('#contractForm') ||
    null;
  const ratesRoot =
    document.getElementById('contractRatesTab') ||
    document.querySelector('#contractRatesTab') ||
    null;

  let el = null;
  if (ratesRoot) {
    el =
      ratesRoot.querySelector(`*[name="${CSS.escape(targetName)}"]`) ||
      ratesRoot.querySelector(`*[name="${CSS.escape(name)}"]`);
  }
  if (!el && form) {
    el =
      form.querySelector(`*[name="${CSS.escape(targetName)}"]`) ||
      form.querySelector(`*[name="${CSS.escape(name)}"]`);
  }

  // Validate & normalise *_start/*_end (empty allowed)
  if (/^(mon|tue|wed|thu|fri|sat|sun)_(start|end)$/.test(targetName)) {
    const raw = (value == null ? '' : String(value).trim());
    const isValidHHMM = (s)=>{
      if (!s) return true;
      if (!/^(\d{1,2}:\d{1,2}|\d{3,4})$/.test(s)) return false;
      let h,m;
      if (/^\d{3,4}$/.test(s)) { const p=s.padStart(4,'0'); h=+p.slice(0,2); m=+p.slice(2,4); }
      else { const a=s.split(':'); h=+a[0]; m=+a[1]; }
      return (h>=0 && h<=23 && m>=0 && m<=59);
    };
    if (raw && !isValidHHMM(raw)) {
      if (el) { el.value=''; el.setAttribute('data-invalid','1'); el.setAttribute('title','Enter HH:MM (00:00–23:59)'); }
      value = '';
    } else if (raw) {
      // normalise to HH:MM
      let h, m;
      if (/^\d{3,4}$/.test(raw)) { const p=raw.padStart(4,'0'); h=+p.slice(0,2); m=+p.slice(2,4); }
      else { const a=raw.split(':'); h=+a[0]; m=+a[1]; }
      value = String(h).padStart(2,'0') + ':' + String(m).padStart(2,'0');
      if (el) { el.value = value; el.removeAttribute('data-invalid'); el.removeAttribute('title'); }
    }
  }

  window.modalCtx = window.modalCtx || {};
  const fs = (window.modalCtx.formState ||= {
    __forId: (window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null),
    main:{},
    pay:{}
  });

  const isRate = /^(paye_|umb_|charge_)/.test(targetName);
  const prev = isRate ? fs.pay[targetName] : fs.main[targetName];

  let stored;
  if (el && el.type === 'checkbox') {
    el.checked = !!value && value !== 'false' && value !== '0';
    stored = el.checked ? 'on' : '';
  } else if (el && el.type === 'radio') {
    stored = String(value ?? '');
    const group = form ? Array.from(form.querySelectorAll(`input[type="radio"][name="${CSS.escape(el.name)}"]`)) : [];
    for (const r of group) r.checked = (String(r.value) === stored);
  } else {
    stored = (value == null ? '' : String(value));
    if (el) el.value = stored;
  }

  if (prev === stored) {
    if (LOGC) {
      console.log('[CONTRACTS] setContractFormValue no-op (unchanged)', {
        name: targetName,
        stored,
        prev,
        isRate
      });
    }
    return;
  }

  if (isRate) fs.pay[targetName] = stored;
  else        fs.main[targetName] = stored;

  if (LOGC) {
    let scope = 'none';
    try {
      if (el && ratesRoot && ratesRoot.contains(el)) scope = 'rates';
      else if (el && form && form.contains(el))      scope = 'form';
    } catch {}
    console.log('[CONTRACTS] setContractFormValue APPLY', {
      name: targetName,
      prev,
      stored,
      isRate,
      scope
    });
  }

  if (isRate || targetName === 'pay_method_snapshot') {
    try { computeContractMargins(); } catch {}
  }

  try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
}

function applyRatePresetToContractForm(preset, payMethod /* 'PAYE'|'UMBRELLA' */) {
  if (!preset) return;

  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;

  // Helper: resolve the nearest/open Contracts frame (parent or higher)
  const getContractsFrame = () => {
    const s = window.__modalStack || [];
    for (let i = s.length - 1; i >= 0; i--) {
      const f = s[i];
      if (f && (f.kind === 'contracts' || f.entity === 'contracts')) return f;
    }
    return null;
  };

  const form =
    document.querySelector('#modalBody #contractForm') ||
    document.querySelector('#contractForm') ||
    null;
  const ratesRoot =
    document.getElementById('contractRatesTab') ||
    document.querySelector('#contractRatesTab') ||
    null;
  const canTouchDom = !!(form || ratesRoot);

  const mc = window.modalCtx || (window.modalCtx = {});
  if (!mc.formState) {
    const baseId = (mc.data && mc.data.id) || mc.openToken || null;
    mc.formState = { __forId: baseId, main: {}, pay: {} };
  }
  const fs = mc.formState;
  fs.main = fs.main || {};
  fs.pay  = fs.pay  || {};

  const effectivePayMethod = String(
    payMethod ||
    fs.main.pay_method_snapshot ||
    (mc.data && mc.data.pay_method_snapshot) ||
    'PAYE'
  ).toUpperCase();

  if (LOGC) {
    console.log('[CONTRACTS] applyRatePresetToContractForm ENTER', {
      presetId: preset.id,
      payMethodParam: payMethod,
      effectivePayMethod
    });
  }

  // Format helper: 2 decimal places if numeric, otherwise leave as-is
  const as2dpRate = (raw) => {
    if (raw == null || raw === '') return '';
    const n = Number(raw);
    return Number.isFinite(n) ? n.toFixed(2) : String(raw);
  };

  const write = (name, raw) => {
    const isRate = /^(paye_|umb_|charge_)/.test(name) || /^mileage_(pay|charge)_rate$/.test(name);
    const v = (raw == null || raw === '')
      ? ''
      : (isRate ? as2dpRate(raw) : String(raw));

    const prev = /^(paye_|umb_|charge_)/.test(name) ? fs.pay[name] : fs.main[name];

    let el = null;
    let hit = 'none';
    if (ratesRoot) {
      el = ratesRoot.querySelector(`[name="${CSS.escape(name)}"]`);
      if (el) hit = 'rates';
    }
    if (!el && form) {
      el = form.querySelector(`[name="${CSS.escape(name)}"]`);
      if (el) hit = 'form';
    }

    if (LOGC) {
      console.log('[CONTRACTS] preset write BEFORE', { name, prev, next: v, hit });
    }

    if (el && canTouchDom) {
      el.value = v;
      try {
        el.dispatchEvent(new Event('input',  { bubbles:true }));
        el.dispatchEvent(new Event('change', { bubbles:true }));
      } catch {}
    }

    // single source of truth: let setContractFormValue stage + fire margins/dirty
    if (typeof setContractFormValue === 'function') {
      try { setContractFormValue(name, v); }
      catch (e) {
        if (LOGC) console.warn('[CONTRACTS] setContractFormValue from preset failed', { name, v, err: e && e.message });
      }
    }
  };

  // ─────────────────────────────────────────────────────────────
  // Identity fields: always overwrite from preset
  // ─────────────────────────────────────────────────────────────
  [['role','role'], ['band','band'], ['display_site','display_site']].forEach(([field, key]) => {
    const next = preset[key] != null ? String(preset[key]).trim() : '';
    write(field, next);
  });

  // ─────────────────────────────────────────────────────────────
  // Rates: copy all families the preset actually defines
  // ─────────────────────────────────────────────────────────────
  const BUCKETS  = ['day','night','sat','sun','bh'];
  const prefixes = ['paye','umb','charge'];

  BUCKETS.forEach(b => {
    prefixes.forEach(p => {
      const fieldName = `${p}_${b}`;
      if (!Object.prototype.hasOwnProperty.call(preset, fieldName)) return;
      const raw = preset[fieldName];
      const finalVal = as2dpRate(raw);
      write(fieldName, finalVal);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Mileage — copy if present on the preset; leave untouched if absent
  // ─────────────────────────────────────────────────────────────
  const as2dpMileage = as2dpRate; // same behaviour

  if (Object.prototype.hasOwnProperty.call(preset, 'mileage_pay_rate')) {
    write('mileage_pay_rate', as2dpMileage(preset.mileage_pay_rate));
  }
  if (Object.prototype.hasOwnProperty.call(preset, 'mileage_charge_rate')) {
    write('mileage_charge_rate', as2dpMileage(preset.mileage_charge_rate));
  }

  // ─────────────────────────────────────────────────────────────
  // Bucket labels: if any labels present, overwrite for those keys;
  // even blank values wipe existing labels.
  // ─────────────────────────────────────────────────────────────
  if (preset.bucket_labels_json) {
    const BL = preset.bucket_labels_json || {};
    const hasAnyLabel = BUCKETS.some(k => Object.prototype.hasOwnProperty.call(BL, k));

    if (hasAnyLabel) {
      fs.main.__bucket_labels = fs.main.__bucket_labels || {};
      BUCKETS.forEach(k => {
        if (!Object.prototype.hasOwnProperty.call(BL, k)) return; // leave others as-is
        const raw  = BL[k];
        const next = raw == null ? '' : String(raw).trim();
        write(`bucket_label_${k}`, next);
        write(`bucket_${k}`,       next);
        fs.main.__bucket_labels[k] = next;
      });
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Standard schedule:
  // - If NO days at all in std_schedule_json → do nothing
  // - If ANY day present → overwrite ALL 7 days (blanks where absent)
  // ─────────────────────────────────────────────────────────────
  const days = ['mon','tue','wed','thu','fri','sat','sun'];

  if (preset.std_schedule_json) {
    const sched = preset.std_schedule_json || {};
    const hasAnyDay = days.some(d => Object.prototype.hasOwnProperty.call(sched, d));

    if (hasAnyDay) {
      const template = {};
      const toStr = (v) => (v == null ? '' : String(v).trim());

      days.forEach(d => {
        const hasThisDay = Object.prototype.hasOwnProperty.call(sched, d);
        const src        = hasThisDay ? (sched[d] || {}) : {};
        const start      = toStr(src.start);
        const end        = toStr(src.end);

        let brStr = '';
        let brNum = 0;
        if (src.break_minutes != null && start && end) {
          brNum = Number(src.break_minutes) || 0;
          brStr = String(brNum);
        }

        // Always overwrite all 7 days (even to blanks)
        write(`${d}_start`, start);
        write(`${d}_end`,   end);
        write(`${d}_break`, brStr);

        if (start && end) {
          template[d] = { start, end, break_minutes: brNum };
        }
      });

      fs.main.__template = template;
    }
  }

  // Hours snapshot from preset or derived from template
  if (preset.std_hours_json) {
    fs.main.__hours = preset.std_hours_json;
  } else if (fs.main.__template) {
    const hours = {};
    const toMinutes = (hhmm) => {
      const m = String(hhmm || '').match(/^(\d{1,2}):(\d{2})$/);
      return m ? (+m[1] * 60 + +m[2]) : null;
    };

    days.forEach(d => {
      const slot = fs.main.__template[d];
      if (!slot || !slot.start || !slot.end) return;

      const startMin = toMinutes(slot.start);
      const endMin   = toMinutes(slot.end);
      if (startMin == null || endMin == null) return;

      let mins = endMin < startMin ? (endMin + 1440 - startMin) : (endMin - startMin);
      mins -= Number(slot.break_minutes || 0);
      if (mins <= 0) return;

      hours[d] = +(mins / 60).toFixed(2);
    });

    fs.main.__hours = Object.keys(hours).length ? hours : null;
  }

  // Non-blocking warnings for pay-method / family mismatches
  const hasFamily = (fam) => BUCKETS.some(k => {
    const v = preset[`${fam}_${k}`];
    return v !== undefined && v !== null && String(v).trim() !== '';
  });

  try {
    if (effectivePayMethod === 'UMBRELLA' && !hasFamily('umb')) {
      if (typeof showModalHint === 'function') {
        showModalHint(
          'No Umbrella rates are set for this preset rate card. Please enter the Umbrella pay rates manually',
          'warn'
        );
      } else if (window.__toast) {
        window.__toast('No Umbrella rates are set for this preset rate card. Please enter the Umbrella pay rates manually');
      }
    } else if (effectivePayMethod === 'PAYE' && !hasFamily('paye')) {
      if (typeof showModalHint === 'function') {
        showModalHint(
          'No PAYE rates are set for this preset rate card. Please enter the PAYE pay rates manually',
          'warn'
        );
      } else if (window.__toast) {
        window.__toast('No PAYE rates are set for this preset rate card. Please enter the PAYE pay rates manually');
      }
    }
  } catch {}

  // Recompute margins + mark modal dirty
  try { if (typeof computeContractMargins === 'function') computeContractMargins(); } catch {}

  try {
    const fr = getContractsFrame();
    if (fr) {
      fr.isDirty = true;
      if (typeof fr._updateButtons === 'function') fr._updateButtons();
    }
    window.dispatchEvent(new Event('modal-dirty'));
  } catch {}

  if (LOGC) {
    console.log('[CONTRACTS] applyRatePresetToContractForm EXIT', {
      presetId: preset.id,
      effectivePayMethod
    });
  }
}


function mergeContractStateIntoRow(row) {
  const base = { ...(row || {}) };
  const fs = (window.modalCtx && window.modalCtx.formState) || null;

  // Merge MAIN staged fields (text/selects/checkbox snapshots)
  if (fs && fs.main) {
    for (const [k, v] of Object.entries(fs.main)) {
      // For checkboxes we store 'on' or '', hydrate to boolean-like fields where appropriate
      if (k === 'auto_invoice' || k === 'require_reference_to_pay' || k === 'require_reference_to_invoice') {
        base[k] = v === 'on' || v === true;
      } else if (k === 'start_date' || k === 'end_date') {
        base[k] = v; // Keep as DD/MM/YYYY in the UI; conversion happens on save
      } else if (k === 'week_ending_weekday_snapshot') {
        base[k] = v;
      } else {
        base[k] = v;
      }
    }
  }

  // Merge PAY staged fields into rates_json without forcing number conversion (UI shows strings)
  const stagedRates = (fs && fs.pay) ? fs.pay : null;
  if (stagedRates) {
    const r = { ...(base.rates_json || {}) };
    for (const [k, v] of Object.entries(stagedRates)) r[k] = v;
    base.rates_json = r;
  }

  return base;
}
function snapshotContractForm() {
  const fs = (window.modalCtx.formState ||= { __forId: (window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null), main:{}, pay:{} });

  const form = document.querySelector('#contractForm');
  const fromMain  = form ? Array.from(form.querySelectorAll('input, select, textarea')) : [];

  const ratesTab  = document.querySelector('#contractRatesTab');
  const fromRates = ratesTab ? Array.from(ratesTab.querySelectorAll('input, select, textarea')) : [];

  const all = [...fromMain, ...fromRates];

  for (const el of all) {
    const name = el && el.name;
    if (!name) continue;
    if (el.disabled || el.readOnly || el.dataset.noCollect === 'true') continue;
    if (name === 'ward_hint') continue; // do not stage ward_hint

    let v;
    if (el.type === 'checkbox') {
      v = el.checked ? 'on' : '';
    } else if (el.type === 'radio') {
      if (!el.checked) continue;
      v = el.value;
    } else {
      v = el.value;
    }

    if (name === 'default_pay_method_snapshot') {
      fs.main.pay_method_snapshot = v;
      continue;
    }

    if (/^(paye_|umb_|charge_)/.test(name)) {
      fs.pay[name] = v;
    } else {
      fs.main[name] = v;
    }
  }
}


// Optional helper: align pay_method_snapshot to candidate; return hint if mismatch
function prefillPayMethodFromCandidate(candidate) {
  if (!candidate) return '';
  const method = (candidate.pay_method || candidate.pay_method_snapshot || '').toString().toUpperCase();
  if (!method || (method !== 'PAYE' && method !== 'UMBRELLA')) return '';

  const form = document.querySelector('#contractForm'); if (!form) return '';
  const sel = form.querySelector('select[name="pay_method_snapshot"]');
  if (!sel) return '';

  const current = (sel.value || '').toUpperCase();
  if (current === method) return '';

  // Preselect to match candidate to reduce mistakes (still editable)
  sel.value = method;
  const evt = new Event('change', { bubbles: true });
  sel.dispatchEvent(evt);

  return `Candidate pay method is ${method}; snapshot updated from ${current || 'N/A'}.`;
}

// Optional helper: return a friendly hint if client has no primary invoice email
function checkClientInvoiceEmailPresence(client) {
  if (!client) return '';
  const has = !!(client.primary_invoice_email && String(client.primary_invoice_email).trim());
  return has ? '' : 'Client has no primary invoice email set — auto-invoicing may be blocked.';
}

// ──────────────────────────────────────────────────────────────────────────────
// FIX 5: Guide hours UI (std_hours_json) added to Main tab
// (Mon–Sun numeric hours; optional, display-only helper)
// ──────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: renderContractMainTab (layout + logs; site under Client, Ward hint to right;
// Role with Band to the right; uses .form to pick up input styling)
// ─────────────────────────────────────────────────────────────────────────────

function renderContractMainTab(ctx) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : true;

  const d = mergeContractStateIntoRow(ctx?.data || {});
  const labelsBlock = renderBucketLabelsEditor({ data: d });

  const candVal   = d.candidate_id || '';
  const clientVal = d.client_id || '';

  const candLabel   = (d.candidate_display || '').trim();
  const clientLabel = (d.client_name || '').trim();

  // Derive labels from picker cache if missing but ids exist (and store into formState for persistence)
  let derivedCand = '';
  let derivedClient = '';
  try {
    const pickData = (window.__pickerData ||= {});
    if (!candLabel && candVal && pickData.candidates && pickData.candidates.itemsById) {
      const r = pickData.candidates.itemsById[candVal];
      if (r) {
        const first = (r.first_name||'').trim();
        const last  = (r.last_name||'').trim();
        const role  = ((r.roles_display||'').split(/[•;,]/)[0]||'').trim();
        derivedCand = `${last}${last?', ':''}${first}${role?` ${role}`:''}`.trim();
        const fs = (window.modalCtx.formState ||= { __forId:(window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null), main:{}, pay:{} });
        (fs.main ||= {}).candidate_display = derivedCand;
      }
    }
    if (!clientLabel && clientVal && pickData.clients && pickData.clients.itemsById) {
      const r = pickData.clients.itemsById[clientVal];
      if (r) {
        derivedClient = (r.name||'').trim();
        const fs = (window.modalCtx.formState ||= { __forId:(window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null), main:{}, pay:{} });
        (fs.main ||= {}).client_name = derivedClient;
      }
    }
  } catch {}

  const _candLabel   = candLabel   || derivedCand;
  const _clientLabel = clientLabel || derivedClient;

  const toUk = (iso) => {
    try { return (typeof formatIsoToUk === 'function') ? (formatIsoToUk(iso) || '') : (iso || ''); }
    catch { return iso || ''; }
  };
  const startUk = (d.start_date && /^\d{2}\/\d{2}\/\d{4}$/.test(d.start_date)) ? d.start_date : toUk(d.start_date);
  const endUk   = (d.end_date && /^\d{2}\/\d{2}\/\d{4}$/.test(d.end_date)) ? d.end_date : toUk(d.end_date);

  const SS = d.std_schedule_json || {};

  const pick = (day, part) => {
    const staged = d[`${day}_${part}`];
    if (staged !== undefined && staged !== null && String(staged).trim() !== '') return String(staged).trim();
    if (part === 'break') {
      const v = SS?.[day]?.break_minutes;
      return (v === 0 || v) ? String(v) : '';
    }
    return (SS?.[day]?.[part] || '');
  };

  const DAYS = [
    ['mon','Mon'],['tue','Tue'],['wed','Wed'],['thu','Thu'],
    ['fri','Fri'],['sat','Sat'],['sun','Sun']
  ];

  // disable Pay Method snapshot whenever a candidate is present or __pay_locked is set
  const payLocked = !!(d.__pay_locked || d.candidate_id);

  // Inline time normaliser/validator wired on blur + Tab (keydown)
  const timeEvents = () => `
    onblur="(function(el){
      var v=(el.value||'').trim(); v=v.replace(/[^0-9:]/g,'');
      if(!v){ try{ if(typeof setContractFormValue==='function') setContractFormValue(el.name,''); }catch(e){}; return; }
      if(v.indexOf(':')<0){
        if(v.length===3){ v='0'+v; }
        if(v.length!==4){ el.value=''; try{ if(typeof setContractFormValue==='function') setContractFormValue(el.name,''); }catch(e){}; try{ el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})); }catch(e){}; return; }
        v=v.slice(0,2)+':'+v.slice(2,4);
      }
      var p=v.split(':'), h=parseInt(p[0],10), m=parseInt(p[1],10);
      if(isNaN(h)||isNaN(m)||h<0||h>23||m<0||m>59){ el.value=''; }
      else { el.value=(h<10?'0'+h:h)+':' + (m<10?'0'+m:m); }
      try{ if(typeof setContractFormValue==='function') setContractFormValue(el.name, el.value);}catch(e){}
      try{ el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})); }catch(e){}
    })(this)"
    onkeydown="if(event.key==='Tab'){ (function(el){
      var v=(el.value||'').trim(); v=v.replace(/[^0-9:]/g,'');
      if(!v){ try{ if(typeof setContractFormValue==='function') setContractFormValue(el.name,''); }catch(e){}; return; }
      if(v.indexOf(':')<0){
        if(v.length===3){ v='0'+v; }
        if(v.length!==4){ el.value=''; try{ if(typeof setContractFormValue==='function') setContractFormValue(el.name,''); }catch(e){}; try{ el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})); }catch(e){}; return; }
        v=v.slice(0,2)+':'+v.slice(2,4);
      }
      var p=v.split(':'), h=parseInt(p[0],10), m=parseInt(p[1],10);
      if(isNaN(h)||isNaN(m)||h<0||h>23||m<0||m>59){ el.value=''; }
      else { el.value=(h<10?'0'+h:h)+':' + (m<10?'0'+m:m); }
      try{ if(typeof setContractFormValue==='function') setContractFormValue(el.name, el.value);}catch(e){}
      try{ el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})); }catch(e){}
    })(this) }"
  `;

  const dayRow = (k, label) => {
    const s  = pick(k,'start');
    const e  = pick(k,'end');
    const br = pick(k,'break');
    const num = (v) => (v == null ? '' : String(v));
    return `
      <div class="row sched" data-day="${k}">
        <label>${label}</label>
        <div class="controls" style="display:flex;align-items:flex-end;gap:8px;flex-wrap:wrap">
          <div class="grid-3" style="min-width:420px">
            <div class="split">
              <span class="mini">Start</span>
              <input class="input" name="${k}_start" value="${s}" placeholder="HH:MM" ${timeEvents()} />
            </div>
            <div class="split">
              <span class="mini">End</span>
              <input class="input" name="${k}_end" value="${e}" placeholder="HH:MM" ${timeEvents()} />
            </div>
            <div class="split">
              <span class="mini">Break (min)</span>
              <input class="input" type="number" min="0" step="1" name="${k}_break" value="${num(br)}" placeholder="0"
                oninput="try{ if(typeof setContractFormValue==='function') setContractFormValue(this.name, this.value); }catch(e){}" />
            </div>
          </div>
          <div class="row-actions" style="display:flex;gap:6px">
            <button type="button" class="btn mini"
              title="Copy this row’s Start/End/Break"
              onclick="(function(){
                try{
                  const f=document.querySelector('#contractForm'); if(!f) return;
                  const s=f['${k}_start']?.value||''; const e=f['${k}_end']?.value||''; const b=f['${k}_break']?.value||'';
                  window.__schedClipboard = { s, e, b };
                  try {
                    var day='${label}';
                    var range=(s||'—') + ((s||e)?'–':'') + (e||'');
                    var br=(b && String(b).trim()?(' + '+b+'m'):'');
                    if (window.__toast) window.__toast('Copied ' + day + ' ' + range + br);
                  } catch {}
                }catch(e){ console.warn('sched copy failed', e); }
              })()">Copy</button>
            <button type="button" class="btn mini"
              title="Paste to this row"
              onclick="(function(){
                try{
                  const clip = window.__schedClipboard || {};
                  const f=document.querySelector('#contractForm'); if(!f) return;
                  const S=f['${k}_start'], E=f['${k}_end'], B=f['${k}_break'];
                  if(S && clip.s!=null){ S.value = clip.s; S.dispatchEvent(new Event('blur', {bubbles:true})); }
                  if(E && clip.e!=null){ E.value = clip.e; E.dispatchEvent(new Event('blur', {bubbles:true})); }
                  if(B && clip.b!=null){
                    B.value = clip.b;
                    try{ if(typeof setContractFormValue==='function') setContractFormValue(B.name, B.value); }catch(e){}
                    B.dispatchEvent(new Event('input',{bubbles:true})); B.dispatchEvent(new Event('change',{bubbles:true}));
                  }
                }catch(e){ console.warn('sched paste failed', e); }
              })()">Paste</button>
          </div>
        </div>
      </div>`;
  };

  if (LOGC) console.log('[CONTRACTS] renderContractMainTab → Start/End/Breaks enabled + per-row Copy/Paste, auto-normalise on blur/Tab');

  const weekNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  const weVal = Number(d.week_ending_weekday_snapshot ?? 0);
  const weLabel = weekNames[isNaN(weVal) ? 0 : weVal];

  const schedGrid = `
    <div class="row"><label class="section">Proposed schedule (Mon–Sun)</label></div>
    <div class="sched-grid" style="min-width:0;flex:1">
      ${DAYS.map(([k,l]) => dayRow(k,l)).join('')}
    </div>
  `;

  if (LOGC) console.log('[CONTRACTS] renderContractMainTab snapshot', {
    candidate_id: candVal, client_id: clientVal,
    candidate_label: _candLabel, client_label: _clientLabel,
    week_ending_weekday_snapshot: d.week_ending_weekday_snapshot,
    mode: window.__getModalFrame?.()?.mode
  });

  // Inline, non-blocking overlap + timesheet-boundary checks on date changes
  const overlapChangeAttr = `
    onchange="(function(el){
      try{
        var form = document.querySelector('#contractForm');
        var cid  = form ? (form.querySelector('[name=\\'candidate_id\\']')?.value||'') : '';
        var sd   = form ? (form.querySelector('[name=\\'start_date\\']')?.value||'') : '';
        var ed   = form ? (form.querySelector('[name=\\'end_date\\']')?.value||'')  : '';
        var sIso = (window.parseUkDateToIso ? parseUkDateToIso(sd) : sd);
        var eIso = (window.parseUkDateToIso ? parseUkDateToIso(ed) : ed);
        var excl = (window.modalCtx && window.modalCtx.data && window.modalCtx.data.id) || null;

        // Overlap (non-blocking)
        if (cid && sIso && eIso && window.callCheckContractWindowOverlap) {
          callCheckContractWindowOverlap(cid, sIso, eIso, excl).then(function(res){
            if (res && res.has_overlap) {
              var msg = (res.overlaps||[]).map(function(o){
                var nm = o.client_name || o.client || 'Client';
                var a = o.overlap_from || '';
                var b = o.overlap_to   || '';
                return nm + ' ' + a + '→' + b;
              }).join(' • ');
              if (window.showModalHint) { showModalHint('Overlap with: ' + msg, 'warn'); }
              else if (window.__toast)  { __toast('Overlap with: ' + msg); }
            }
          });
        }

        // Timesheet boundary (non-blocking hint + cache for save eligibility). Skip in create (no contract id yet)
        if (excl && sIso && eIso && window.callCheckTimesheetBoundary) {
          callCheckTimesheetBoundary(excl, sIso, eIso).then(function(bres){
            window.__tsBoundaryResult = bres || null;
            if (bres && bres.ok === false) {
              var txt = 'Dates exclude existing timesheets.';
              try {
                var v = bres.violations || [];
                if (v.length) {
                  var sample = v.slice(0,3).map(function(x){
                    var nm = x.client_name || 'Client';
                    var dt = x.date || '';
                    var st = x.status || '';
                    return nm + ' ' + dt + (st?(' ('+st+')'):'');
                  }).join(' • ');
                  txt = 'Dates exclude existing timesheets: ' + sample + (v.length>3?'…':'');
                } else if (bres.min_ts_date || bres.max_ts_date) {
                  txt = 'Dates exclude timesheets in range ' + (bres.min_ts_date||'') + ' → ' + (bres.max_ts_date||'') + '.';
                }
              } catch {}
              if (window.showModalHint) { showModalHint(txt, 'warn'); } else if (window.__toast) { __toast(txt); }
            }
          });
        } else {
          if (!excl) window.__tsBoundaryResult = null;
        }
      }catch(e){}
    })(this)"`;

  return `
    <form id="contractForm" class="tabc form">
      <input type="hidden" name="candidate_id" value="${candVal}">
      <input type="hidden" name="client_id"    value="${clientVal}">
      <input type="hidden" name="week_ending_weekday_snapshot" value="${String(d.week_ending_weekday_snapshot ?? '')}">

      <div class="row">
        <label>Candidate</label>
        <div class="controls">
          <div class="split">
            <input class="input" type="text" id="candidate_name_display" value="${_candLabel}" placeholder="Type 3+ letters to search…" />
            <span>
              <button type="button" class="btn mini" id="btnPickCandidate">Pick…</button>
              <button type="button" class="btn mini" id="btnClearCandidate">Clear</button>
            </span>
          </div>
          <div class="mini" id="candidatePickLabel">${_candLabel ? `Chosen: ${_candLabel}` : ''}</div>
        </div>
      </div>

      <div class="row">
        <label>Client</label>
        <div class="controls">
          <div class="split">
            <input class="input" type="text" id="client_name_display" value="${_clientLabel}" placeholder="Type 3+ letters to search…" />
            <span>
              <button type="button" class="btn mini" id="btnPickClient">Pick…</button>
              <button type="button" class="btn mini" id="btnClearClient">Clear</button>
            </span>
          </div>
          <div class="mini" id="clientPickLabel">${_clientLabel ? `Chosen: ${_clientLabel}` : ''}</div>
        </div>
      </div>

      <div class="grid-2">
        <div class="row"><label>Display site</label><div class="controls"><input class="input" name="display_site" value="${d.display_site || ''}" /></div></div>
        <div class="row"><label>Week-ending day</label><div class="controls"><div class="mini" id="weLabel">${weLabel}</div></div></div>
      </div>

      <div class="grid-2">
        <div class="row"><label>Role</label><div class="controls"><input class="input" name="role" value="${d.role || ''}" /></div></div>
        <div class="row"><label>Band</label><div class="controls"><input class="input" name="band" value="${d.band || ''}" /></div></div>
      </div>

      <div class="grid-2">
        <div class="row"><label>Start date</label><div class="controls"><input class="input" name="start_date" value="${startUk}" placeholder="DD/MM/YYYY" required ${overlapChangeAttr} /></div></div>
        <div class="row"><label>End date</label><div class="controls"><input class="input" name="end_date" value="${endUk}" placeholder="DD/MM/YYYY" required ${overlapChangeAttr} /></div></div>
      </div>

      <div class="grid-3">
        <div class="row"><label>Pay method snapshot</label>
          <div class="controls">
            <select name="pay_method_snapshot" ${payLocked ? 'disabled' : ''}>
              <option value="PAYE" ${String(d.pay_method_snapshot||'PAYE').toUpperCase()==='PAYE'?'selected':''}>PAYE</option>
              <option value="UMBRELLA" ${String(d.pay_method_snapshot||'PAYE').toUpperCase()==='UMBRELLA'?'selected':''}>Umbrella</option>
            </select>
          </div>
        </div>
        <div class="row"><label>Default submission mode</label>
          <div class="controls">
            <select name="default_submission_mode">
              <option value="ELECTRONIC" ${String(d.default_submission_mode||'ELECTRONIC').toUpperCase()==='ELECTRONIC'?'selected':''}>Electronic</option>
              <option value="MANUAL" ${String(d.default_submission_mode||'ELECTRONIC').toUpperCase()==='MANUAL'?'selected':''}>Manual</option>
            </select>
          </div>
        </div>
        <div class="row"><label>Auto-invoice</label>
          <div class="controls"><input type="checkbox" name="auto_invoice" ${d.auto_invoice ? 'checked' : ''} /></div>
        </div>
      </div>

      <div class="grid-2">
        <div class="row"><label>Require reference to PAY</label><div class="controls"><input type="checkbox" name="require_reference_to_pay" ${d.require_reference_to_pay ? 'checked':''} /></div></div>
        <div class="row"><label>Require reference to INVOICE</label><div class="controls"><input type="checkbox" name="require_reference_to_invoice" ${d.require_reference_to_invoice ? 'checked':''} /></div></div>
      </div>

      ${schedGrid}

      ${labelsBlock}
    </form>`;
}


// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: renderContractRatesTab (adds logging only)
// ─────────────────────────────────────────────────────────────────────────────

function renderContractRatesTab(ctx) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;

  const merged = mergeContractStateIntoRow(ctx?.data || {});
  const R = (merged?.rates_json) || {};
  const payMethod = String(merged?.pay_method_snapshot || 'PAYE').toUpperCase();
  const showPAYE = (payMethod === 'PAYE');
  const num = (v) => (v == null ? '' : String(v));
  const LBL = merged?.bucket_labels_json || {};
  const labelOf = (k) => {
    if (k==='day') return (LBL.day||'Day');
    if (k==='night') return (LBL.night||'Night');
    if (k==='sat') return (LBL.sat||'Sat');
    if (k==='sun') return (LBL.sun||'Sun');
    if (k==='bh') return (LBL.bh||'BH');
    return k;
  };

  if (LOGC) console.log('[CONTRACTS] renderContractRatesTab', { payMethod, hasRates: !!merged?.rates_json });

  const html = `
    <div class="tabc" id="contractRatesTab" data-pay-method="${payMethod}">
      <div class="row" style="display:flex;justify-content:space-between;align-items:center">
        <label class="section">Rates</label>
        <div class="actions" style="gap:8px">
          <span class="pill" id="presetChip" style="display:none"></span>
          <button type="button" id="btnChoosePreset">Choose preset…</button>
          <button type="button" id="btnResetPreset">Reset preset</button>
        </div>
      </div>

      <div class="grid-3" id="ratesCards">
        <div class="card" id="cardPAYE" style="${showPAYE?'':'display:none'}">
          <div class="row"><label class="section">PAYE pay (visible if PAYE)</label></div>
          <div class="grid-5">
            <div class="row"><label>${labelOf('day')}</label><div class="controls"><input class="input" name="paye_day"  value="${num(R.paye_day)}" /></div></div>
            <div class="row"><label>${labelOf('night')}</label><div class="controls"><input class="input" name="paye_night" value="${num(R.paye_night)}" /></div></div>
            <div class="row"><label>${labelOf('sat')}</label><div class="controls"><input class="input" name="paye_sat"  value="${num(R.paye_sat)}" /></div></div>
            <div class="row"><label>${labelOf('sun')}</label><div class="controls"><input class="input" name="paye_sun"  value="${num(R.paye_sun)}" /></div></div>
            <div class="row"><label>${labelOf('bh')}</label><div class="controls"><input class="input" name="paye_bh"   value="${num(R.paye_bh)}" /></div></div>
          </div>
        </div>

        <div class="card" id="cardUMB" style="${showPAYE?'display:none':''}">
          <div class="row"><label class="section">Umbrella pay (visible if Umbrella)</label></div>
          <div class="grid-5">
            <div class="row"><label>${labelOf('day')}</label><div class="controls"><input class="input" name="umb_day"  value="${num(R.umb_day)}" /></div></div>
            <div class="row"><label>${labelOf('night')}</label><div class="controls"><input class="input" name="umb_night" value="${num(R.umb_night)}" /></div></div>
            <div class="row"><label>${labelOf('sat')}</label><div class="controls"><input class="input" name="umb_sat"  value="${num(R.umb_sat)}" /></div></div>
            <div class="row"><label>${labelOf('sun')}</label><div class="controls"><input class="input" name="umb_sun"  value="${num(R.umb_sun)}" /></div></div>
            <div class="row"><label>${labelOf('bh')}</label><div class="controls"><input class="input" name="umb_bh"   value="${num(R.umb_bh)}" /></div></div>
          </div>
        </div>

        <div class="card" id="cardCHG">
          <div class="row"><label class="section">Charge-out</label></div>
          <div class="grid-5">
            <div class="row"><label>${labelOf('day')}</label><div class="controls"><input class="input" name="charge_day"   value="${num(R.charge_day)}" /></div></div>
            <div class="row"><label>${labelOf('night')}</label><div class="controls"><input class="input" name="charge_night" value="${num(R.charge_night)}" /></div></div>
            <div class="row"><label>${labelOf('sat')}</label><div class="controls"><input class="input" name="charge_sat"   value="${num(R.charge_sat)}" /></div></div>
            <div class="row"><label>${labelOf('sun')}</label><div class="controls"><input class="input" name="charge_sun"   value="${num(R.charge_sun)}" /></div></div>
            <div class="row"><label>${labelOf('bh')}</label><div class="controls"><input class="input" name="charge_bh"    value="${num(R.charge_bh)}" /></div></div>
          </div>

          <!-- Mileage row -->
          <div class="grid-2" style="margin-top:10px">
            <div class="row"><label>Mileage charge</label><div class="controls"><input class="input" name="mileage_charge_rate" value="${num(merged?.mileage_charge_rate)}" /></div></div>
            <div class="row"><label>Mileage pay</label><div class="controls"><input class="input" name="mileage_pay_rate" value="${num(merged?.mileage_pay_rate)}" /></div></div>
          </div>
        </div>
      </div>

      <div class="row" style="margin-top:12px"><label class="section">Margins</label></div>
      <table class="grid" id="marginsTable">
        <thead><tr><th>Bucket</th><th>Pay</th><th>Charge</th><th>Margin</th></tr></thead>
        <tbody>
          <tr data-b="day"><td>${labelOf('day')}</td><td class="py"></td><td class="ch"></td><td class="mg"></td></tr>
          <tr data-b="night"><td>${labelOf('night')}</td><td class="py"></td><td class="ch"></td><td class="mg"></td></tr>
          <tr data-b="sat"><td>${labelOf('sat')}</td><td class="py"></td><td class="ch"></td><td class="mg"></td></tr>
          <tr data-b="sun"><td>${labelOf('sun')}</td><td class="py"></td><td class="ch"></td><td class="mg"></td></tr>
          <tr data-b="bh"><td>${labelOf('bh')}</td><td class="py"></td><td class="ch"></td><td class="mg"></td></tr>
        </tbody>
      </table>
    </div>`;

  setTimeout(() => {
    try {
      const root = document.getElementById('contractRatesTab');
      if (!root) return;
      const ev = new CustomEvent('contracts-rates-rendered', {
        detail: { payMethod }
      });
      if (LOGC) console.log('[CONTRACTS] dispatch contracts-rates-rendered', { payMethod });
      window.dispatchEvent(ev);
    } catch (e) {
      if (LOGC) console.warn('[CONTRACTS] contracts-rates-rendered dispatch failed', e);
    }
  }, 0);

  return html;
}



// ──────────────────────────────────────────────────────────────────────────────
// Week actions (drawer modals)
// ─────────────────────────────────────────────────────────────────────────────-

function openManualWeekEditor(week_id, contract_id /* optional but recommended */) {
  const main = `
    <div class="tabc">
      <div id="hoursGrid" class="grid-5 tight">
        ${['day','night','sat','sun','bh'].map(k => `
          <div data-bucket="${k}">
            <div class="lbl mini" style="margin-bottom:4px">${k.toUpperCase()}</div>
            <input class="input" type="number" step="0.01" min="0" name="h_${k}" placeholder="0.00" />
          </div>`).join('')}
      </div>
      <div class="row" style="margin-top:10px">
        <label>Reference (optional)</label>
        <div class="controls"><input class="input" name="reference_number" placeholder="PO / Ref" /></div>
      </div>
      <div class="row"><div class="hint">Tip: attach or replace a manual PDF in “Actions…”.</div></div>
    </div>
  `;

  showModal(
    `Manual Week — ${week_id}`,
    [{ key:'edit', title:'Edit hours' }],
    () => main,
    async () => {
      // Collect & post
      const root = document.querySelector('#modalRoot') || document;
      const v = (n) => Number(root.querySelector(`input[name="${n}"]`)?.value || 0);
      const ref = root.querySelector('input[name="reference_number"]')?.value?.trim() || '';

      const payload = { hours: { day:v('h_day'), night:v('h_night'), sat:v('h_sat'), sun:v('h_sun'), bh:v('h_bh') } };
      if (ref) payload.reference_number = ref;

      await contractWeekManualUpsert(week_id, payload);
      alert('Saved.');
      return true;
    },
    false,
    async () => {
      // Post-render: apply bucket labels if we have contract_id
      try {
        if (!contract_id) return;
        const cr = await getContract(contract_id);
        const L = getBucketLabelsForContract(cr?.contract || cr);
        applyBucketLabelsToHoursGrid(document.querySelector('#hoursGrid'), L);
      } catch {}
    },
    { kind:'manual-week' }
  );
}

// ──────────────────────────────────────────────────────────────────────────────
// openContractWeekActions (amended) — “Add additional sheet” now calls additional;
// add separate “Create expense sheet” button wired to expense-sheet endpoint
// ──────────────────────────────────────────────────────────────────────────────




// ──────────────────────────────────────────────────────────────────────────────
// FIX 2: Clone & Extend endpoint name mismatch (…/clone-and-extend)
// ──────────────────────────────────────────────────────────────────────────────
function openContractCloneAndExtend(contract_id) {
  const LOGM = !!window.__LOG_MODAL;
  const old = (window.modalCtx && window.modalCtx.data) ? window.modalCtx.data : {};
  if (LOGM) console.log('[CLONE] entry', { contract_id, hasOld: !!old?.id, oldPreview: old?.id ? { id: old.id, start: old.start_date, end: old.end_date } : null });

  const iso = (d)=> (typeof d === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(d)) ? d : toYmd(new Date());
  const oldStart = iso(old?.start_date);
  const oldEnd   = iso(old?.end_date);

  // Defaults for the wizard
  const defaultStart = (() => { const d=new Date((oldEnd||toYmd(new Date()))+'T00:00:00Z'); d.setUTCDate(d.getUTCDate()+1); return toYmd(d); })();
  const defaultEnd   = (() => { const d=new Date(defaultStart+'T00:00:00Z'); d.setUTCDate(d.getUTCDate()+84); return toYmd(d); })();
  const defaultEndOld= (() => { const d=new Date(defaultStart+'T00:00:00Z'); d.setUTCDate(d.getUTCDate()-1); return toYmd(d); })();

  const content = `
    <div class="tabc" id="cloneExtendForm">
      <div class="row"><label>New start</label>
        <div class="controls"><input class="input" type="text" name="new_start_date" placeholder="DD/MM/YYYY" value="${formatIsoToUk(defaultStart)}" /></div>
      </div>
      <div class="row"><label>New end</label>
        <div class="controls"><input class="input" type="text" name="new_end_date" placeholder="DD/MM/YYYY" value="${formatIsoToUk(defaultEnd)}" /></div>
      </div>

      <div class="row" style="margin-top:6px">
        <label style="display:flex;align-items:center;gap:6px">
          <input type="checkbox" name="end_existing_checked" checked />
          End existing contract on
        </label>
        <div class="controls" style="margin-top:6px">
          <input class="input" type="text" name="end_existing_on" placeholder="DD/MM/YYYY" value="${formatIsoToUk(defaultEndOld)}" />
          <div class="mini" style="margin-top:4px">Default is New start − 1 day. Untick to keep the existing contract running.</div>
        </div>
      </div>

      <div class="mini" style="margin-top:10px">
        After this, the successor opens in the normal contract modal (Create mode). You can edit Main / Rates / Calendar before saving.
      </div>
    </div>
  `;

  showModal(
    'Clone & Extend',
    [{ key:'c', title:'Successor window' }],
    () => content,
    async () => {
      const LOGM = !!window.__LOG_MODAL;
      const root = document.getElementById('cloneExtendForm') || document;

      const newStartUk  = root.querySelector('input[name="new_start_date"]')?.value?.trim() || '';
      const newEndUk    = root.querySelector('input[name="new_end_date"]')?.value?.trim()   || '';
      const endChk      = !!root.querySelector('input[name="end_existing_checked"]')?.checked;
      const endOldUk    = root.querySelector('input[name="end_existing_on"]')?.value?.trim() || '';

      const new_start_date = parseUkDateToIso(newStartUk);
      const new_end_date   = parseUkDateToIso(newEndUk);
      const end_existing_on= endChk ? parseUkDateToIso(endOldUk) : null;

      if (!new_start_date || !new_end_date) { alert('Enter both new start and new end.'); return false; }
      if (new_start_date > new_end_date)   { alert('New end must be on or after new start.'); return false; }

      try {
        const oldStartIso = (window.modalCtx?.data?.start_date) || '';
        if (endChk) {
          if (!end_existing_on) { alert('Pick a valid end date for the existing contract.'); return false; }
          if (oldStartIso && end_existing_on < oldStartIso) { alert('Existing contract cannot end before its original start.'); return false; }
          if (end_existing_on >= new_start_date) { alert('Existing contract end must be before the new start.'); return false; }
        }
      } catch {}

      // === NEW: pre-truncate the existing contract (blocking) BEFORE opening successor ===
      let effectiveOldEnd = end_existing_on;
      if (endChk) {
        const oldId = String(window.modalCtx?.data?.id || '');
        if (!oldId) { alert('Source contract id missing.'); return false; }

        try {
          console.groupCollapsed('[CLONE][pre-trim gate]');
          const url  = API(`/api/contracts/${encodeURIComponent(oldId)}/truncate-tail`);
          const init = {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ id: oldId, desired_end: end_existing_on })
          };
          console.log('request', { url, init, new_start_date, new_end_date });
          try { window.__LOG_API = true; } catch {}

          const res = await (typeof authFetch === 'function' ? authFetch(url, init) : fetch(url, init));
          let obj = null;
          try { obj = await res.clone().json(); } catch { obj = null; }

          const okField =
            (typeof obj?.ok === 'boolean' ? obj.ok :
             typeof obj?.success === 'boolean' ? obj.success :
             (typeof res?.ok === 'boolean' ? res.ok : undefined));

          const clamped  = !!obj?.clamped;
          const safe_end = obj?.safe_end || null;
          const status   = (typeof res?.status === 'number') ? res.status : (typeof obj?.status === 'number' ? obj.status : undefined);

          console.log('response', { status, ok: !!okField, clamped, safe_end, obj });

          if (!okField) {
            alert((obj && (obj.message || obj.error)) || res.statusText || 'Failed to end the existing contract.');
            console.groupEnd?.();
            return false;
          }

          if (clamped && typeof showTailClampWarning === 'function') {
            try { showTailClampWarning(safe_end, end_existing_on); } catch {}
          }

          effectiveOldEnd = safe_end || end_existing_on;

          if (effectiveOldEnd >= new_start_date) {
            alert(`Existing contract now ends on ${effectiveOldEnd}, which overlaps the new start (${new_start_date}). Adjust dates and try again.`);
            console.groupEnd?.();
            return false;
          }

          if (typeof refreshOldContractAfterTruncate === 'function') {
            try { await refreshOldContractAfterTruncate(oldId); } catch (e) { if (LOGM) console.warn('[CLONE] refresh after truncate failed', e); }
          }
          console.groupEnd?.();
        } catch (e) {
          console.warn('[CLONE][pre-trim gate] exception', e);
          alert(`Could not end the existing contract: ${e?.message || e}`);
          return false;
        }
      }

      // Build staged successor row from current contract (no staging of end_existing intent anymore)
      const old = window.modalCtx?.data || {};
      const newToken = `contract:new:${Date.now()}:${Math.random().toString(36).slice(2)}`;
      const stagedRow = {
        id: null,
        candidate_id: old.candidate_id || '',
        client_id:    old.client_id    || '',
        role:         old.role         || '',
        band:         (old.band ?? null),
        display_site: old.display_site || '',
        start_date:   new_start_date,
        end_date:     new_end_date,
        pay_method_snapshot: old.pay_method_snapshot || 'PAYE',
        default_submission_mode: old.default_submission_mode || 'ELECTRONIC',
        week_ending_weekday_snapshot: Number(old.week_ending_weekday_snapshot ?? 0),
        std_schedule_json: old.std_schedule_json || null,
        std_hours_json:    old.std_hours_json    || null,
        bucket_labels_json: old.bucket_labels_json || null,
        rates_json: (old.rates_json && typeof old.rates_json === 'object') ? old.rates_json : {}
      };

      // Open successor ONLY AFTER the pre-trim has finished successfully — as a ROOT modal
      try {
        if (LOGM) console.log('[CLONE] will open staged successor in Create mode (root, deferred)', { token: newToken, stagedRow, effectiveOldEnd });
        window.__preOpenToken = newToken;
        setTimeout(() => {
          try {
            // Tear down entire stack so successor opens as root (no parent to resurface)
            try { discardAllModalsAndState(); } catch {}
            openContract(stagedRow);
            // After the modal builds its own formState, force-align __forId with our token
            setTimeout(() => {
              try {
                if (window.modalCtx) {
                  window.modalCtx.openToken = newToken;
                  const fs2 = (window.modalCtx.formState ||= { __forId: newToken, main:{}, pay:{} });
                  fs2.__forId = newToken;
                  if (LOGM) console.log('[CLONE] bound token to create modal', { openToken: window.modalCtx.openToken, forId: fs2.__forId });
                }
              } catch (e) { console.warn('[CLONE] bind token failed', e); }
            }, 0);
          } catch (e) {
            console.error('[CLONE] openContract failed', e);
            try { renderAll(); } catch {}
          }
        }, 0);
      } catch (e) {
        console.error('[CLONE] schedule open failed', e);
      }

      return true;
    },
    false,
    () => {
      try { window.dispatchEvent(new Event('contracts-main-rendered')); } catch {}
    },
    { kind:'contract-clone-extend', forceEdit:true, noParentGate:true }
  );

  // Wire pickers & auto-sync after mount
  setTimeout(() => {
    const root = document.getElementById('cloneExtendForm');
    if (!root) return;

    const startEl = root.querySelector('input[name="new_start_date"]');
    const endEl   = root.querySelector('input[name="new_end_date"]');
    const endChk  = root.querySelector('input[name="end_existing_checked"]');
    const endOld  = root.querySelector('input[name="end_existing_on"]');

    attachUkDatePicker(startEl, { minDate: formatIsoToUk(oldStart) });
    attachUkDatePicker(endEl,   { minDate: startEl.value, maxDate: null });
    attachUkDatePicker(endOld,  { minDate: formatIsoToUk(oldStart), maxDate: startEl.value });

    const isoMinusOne = (isoStr) => { try { const d=new Date(isoStr+'T00:00:00Z'); d.setUTCDate(d.getUTCDate()-1); return toYmd(d); } catch { return null; } };

    const onStartChange = () => {
      const startIso = parseUkDateToIso(startEl.value || '') || defaultStart;
      const maxOldEndIso = isoMinusOne(startIso);
      const maxOldEndUk  = formatIsoToUk(maxOldEndIso);
      if (typeof endOld.setMinDate === 'function') endOld.setMinDate(formatIsoToUk(oldStart));
      endOld._maxIso = maxOldEndIso;
      if (typeof endOld.__ukdpRepaint === 'function') endOld.__ukdpRepaint();
      if (endChk.checked) endOld.value = maxOldEndUk;
      if (LOGM) console.log('[CLONE] onStartChange', { startIso, endOldUk: endOld.value, maxOldEndIso });
    };

    const onChkToggle = () => {
      const checked = !!endChk.checked;
      endOld.disabled = !checked;
      if (checked) {
        const sIso = parseUkDateToIso(startEl.value || '') || oldEnd;
        const maxIso = isoMinusOne(sIso);
        const maxUk  = formatIsoToUk(maxIso);
        const eIso   = parseUkDateToIso(endOld.value || '') || '';
        if (!eIso || eIso >= sIso || eIso < oldStart) endOld.value = maxUk;
      }
      if (LOGM) console.log('[CLONE] onChkToggle', { checked, endOldUk: endOld.value });
    };

    startEl.addEventListener('change', onStartChange, true);
    startEl.addEventListener('blur',   onStartChange, true);
    endChk.addEventListener('change',  onChkToggle,   true);

    onChkToggle();
    onStartChange();
  }, 0);
}


function openContractSkipWeeks(contract_id) {
  const content = `
    <div class="tabc">
      <div class="row"><label>From (W/E)</label><div class="controls"><input class="input" name="from" placeholder="YYYY-MM-DD" /></div></div>
      <div class="row"><label>To (W/E)</label><div class="controls"><input class="input" name="to" placeholder="YYYY-MM-DD" /></div></div>
      <div class="hint">Only OPEN/PLANNED weeks without timesheets will be cancelled.</div>
    </div>
  `;
  showModal(
    'Skip Weeks',
    [{ key:'s', title:'Cancel range'}],
    () => content,
    async () => {
      const root = document;
      const from = root.querySelector('input[name="from"]')?.value?.trim() || null;
      const to   = root.querySelector('input[name="to"]')?.value?.trim() || null;
      const r = await authFetch(API(`/api/contracts/${_enc(contract_id)}/skip-weeks`), {
        method:'POST', headers:{'content-type':'application/json'}, body:_json({ from, to })
      });
      if (!r?.ok) { alert('Skip weeks failed.'); return false; }
      alert('Weeks cancelled (where eligible).');
      return true;
    },
    false,
    null,
    { kind:'skip-weeks' }
  );
}


// ──────────────────────────────────────────────────────────────────────────────
// Helper — presign & upload a manual PDF to the week
// ─────────────────────────────────────────────────────────────────────────────-

async function presignAndAttachManualWeekPdf(week_id) {
  try {
    const { upload_url } = await contractWeekPresignPdf(week_id);

    // Spawn a file input + PUT the first file
    const inp = document.createElement('input');
    inp.type = 'file';
    inp.accept = '.pdf,.jpg,.jpeg,.png,.heic,.heif,application/pdf,image/*';
    inp.style.display = 'none';
    document.body.appendChild(inp);
    inp.onchange = async () => {
      const f = inp.files && inp.files[0];
      document.body.removeChild(inp);
      if (!f) return;

      const put = await fetch(upload_url, { method:'PUT', headers:{ 'content-type': f.type || 'application/octet-stream' }, body: f });
      if (!put.ok) throw new Error(`Upload failed (${put.status})`);
      alert('File uploaded and attached to the week.');
    };
    inp.click();
  } catch (e) {
    alert(e?.message || e);
  }
}














































// ─────────────────────────────────────────────────────────────
// NEW: Quick wrapper to focus current ticked selection
// ─────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────
// NEW: Render the tiny Selection toolbar under the grid
// Buttons: 🔍 Focus | 🔍 Save | 🔍 Load
// Call from renderSummary() after the pager, or anywhere you want.
// ─────────────────────────────────────────────────────────────
function renderSelectionToolbar(section, mountAfterEl) {
  // Locate a mount point
  const content = document.getElementById('content');
  const host = mountAfterEl || content;
  if (!host) return null;

  // Read selection
  window.__selection = window.__selection || {};
  const sel = (window.__selection[section] ||= { fingerprint: '', ids: new Set() });
  const hasSelection = sel.ids && sel.ids.size > 0;

  // Create bar
  const bar = document.createElement('div');
  bar.className = 'selection-toolbar';
  bar.style.cssText = 'display:flex;justify-content:flex-end;gap:8px;padding:6px 10px;border-top:1px dashed var(--line)';

  // Button factory
  const mkBtn = (title, text) => {
    const b = document.createElement('button');
    b.title = title;
    b.textContent = text;
    b.style.cssText = 'border:1px solid var(--line);background:#0b152a;color:var(--text);padding:4px 8px;border-radius:8px;cursor:pointer';
    return b;
  };

  const btnFocus = mkBtn('Focus on records', '🔍 Focus');
  const btnSave  = mkBtn('Save selection',   '🔍 Save');
  const btnLoad  = mkBtn('Load selection',   '🔍 Load');

  btnFocus.disabled = !hasSelection;
  btnSave.disabled  = !hasSelection;

  btnFocus.addEventListener('click', async () => {
    try { await focusCurrentSelection(section); } catch (e) { console.error('Focus failed', e); }
  });

  btnSave.addEventListener('click', async () => {
    try { await openSaveSelectionModal(section); } catch (e) { console.error('Save selection failed', e); }
  });

  btnLoad.addEventListener('click', async () => {
    try { await openLoadSelectionModal(section); } catch (e) { console.error('Load selection failed', e); }
  });

  bar.appendChild(btnFocus);
  bar.appendChild(btnSave);
  bar.appendChild(btnLoad);

  host.appendChild(bar);
  return bar;
}


// ─────────────────────────────────────────────────────────────
// NEW: Save the current ticked selection (IDs-only)
// Modes: Save as new, Append to existing (selection kind only)
// ─────────────────────────────────────────────────────────────
async function openSaveSelectionModal(section) {
  const sanitize = (typeof window !== 'undefined' && typeof window.sanitize === 'function')
    ? window.sanitize
    : (s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;')
                           .replace(/>/g,'&gt;').replace(/"/g,'&quot;')
                           .replace(/'/g,'&#39;'));

  window.__selection = window.__selection || {};
  const curSel = window.__selection[section] || { fingerprint:'', ids:new Set() };
  const idsNow = Array.from(new Set((Array.from(curSel.ids || []).map(String).filter(Boolean)))); // dedupe

  if (!idsNow.length) {
    alert('No records selected to save.');
    return;
  }

  // Load owned selection presets for Append
  const myId = currentUserId();
  const mine = await listReportPresets({ section, kind: 'selection', include_shared: false }).catch(() => []);
  const owned = (mine || []).filter(p => String(p.user_id) === String(myId));
  const optionsHtml = owned.map(p => `<option value="${p.id}">${sanitize(p.name || '(unnamed)')}</option>`).join('');

  const body = html(`
    <div class="form" id="saveSelectionForm" style="max-width:720px">
      <div class="row">
        <label for="selPresetName">Preset name</label>
        <div class="controls">
          <input id="selPresetName" class="input" placeholder="e.g. ‘Shortlist — RMNs’" />
        </div>
      </div>

      <div class="row">
        <label>Mode</label>
        <div class="controls" style="display:flex;flex-direction:column;gap:8px;min-width:0">
          <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
            <label class="inline">
              <input type="radio" name="mode" value="new" checked>
              <span>Save as new selection</span>
            </label>
            <label class="inline">
              <input type="radio" name="mode" value="append" ${owned.length ? '' : 'disabled'}>
              <span>Append to existing selection</span>
            </label>
          </div>
          <div id="selAppendWrap" style="display:none; width:100%; max-width:100%">
            <div class="hint" style="margin:2px 0 4px">
              ${owned.length ? 'Choose selection to append to' : 'You don’t own any selections to append'}
            </div>
            <select id="selAppendPresetId" class="input" style="width:100%; max-width:100%">
              ${optionsHtml}
            </select>
          </div>
        </div>
      </div>

      <div class="row">
        <label for="selPresetShared">Visibility</label>
        <div class="controls">
          <label class="inline">
            <input id="selPresetShared" type="checkbox">
            <span>Visible to all users</span>
          </label>
        </div>
      </div>
    </div>
  `);

  showModal(
    'Save selection',
    [{ key: 'form', label: 'Details' }],
    () => body,
    async () => {
      const name  = String(document.getElementById('selPresetName')?.value || '').trim();
      const share = !!document.getElementById('selPresetShared')?.checked;

      // Re-read & dedupe IDs at submit time
      const ids = Array.from(new Set((Array.from((window.__selection?.[section]?.ids) || []))
        .map(String).filter(Boolean)));

      if (!ids.length) { alert('No records selected.'); return false; }

      const mode = (document.querySelector('#saveSelectionForm input[name="mode"]:checked')?.value || 'new').toLowerCase();
      if (mode === 'append') {
        if (!owned.length) { alert('You don’t own any selections to append.'); return false; }
        const targetId = (document.getElementById('selAppendPresetId')?.value) || '';
        if (!targetId) { alert('Select a selection to append to.'); return false; }

        // Fetch target → dedupe-union → PATCH kind: 'selection'
        const targetList = await listReportPresets({ section, kind: 'selection', include_shared: false }).catch(()=>[]);
        const target = (targetList || []).find(p => String(p.id) === String(targetId));
        const targetIds = Array.isArray(target?.selection_json?.ids) ? target.selection_json.ids.map(String).filter(Boolean) : [];
        const targetSet = new Set(targetIds);

        // Only add what isn't there already
        const toAdd = ids.filter(id => !targetSet.has(id));
        if (toAdd.length === 0) {
          alert('Those records are already in that selection. Nothing to append.');
          return false;
        }

        const merged = Array.from(new Set([...targetIds, ...toAdd]));

        await updateReportPreset({
          id: targetId,
          kind: 'selection',
          selection: { ids: merged }
          // keep name/visibility as-is
        });
      } else {
        if (!name) { alert('Please enter a name'); return false; }
        await createReportPreset({
          section,
          kind: 'selection',
          name,
          is_shared: share,
          filters: {},                         // not used for selections
          selection: { ids }                   // already deduped
        });
      }

      try { invalidatePresetCache(section, 'selection'); } catch {}
      return true;  // ← tells saveForFrame to treat as success and close the modal
    },
    false,
    undefined,
    { noParentGate: true, forceEdit: true, kind: 'selection-save' }
  );

  // Wire append toggling + name field enable/disable
  setTimeout(() => {
    const formEl = document.getElementById('saveSelectionForm');
    if (!formEl || formEl.dataset.wired === '1') return;
    formEl.dataset.wired = '1';

    const appendWrap = document.getElementById('selAppendWrap');
    const nameInput  = document.getElementById('selPresetName');

    const syncModeUI = () => {
      const modeEl = formEl.querySelector('input[name="mode"]:checked');
      const isAppend = !!(modeEl && modeEl.value === 'append');

      if (appendWrap) appendWrap.style.display = isAppend ? 'block' : 'none';

      if (nameInput) {
        nameInput.disabled = isAppend;
        nameInput.readOnly = isAppend;
      }
    };

    formEl.querySelectorAll('input[name="mode"]').forEach(r => {
      r.addEventListener('change', () => {
        syncModeUI();
      });
    });

    // Initialise UI (default is "new" so name should be editable)
    syncModeUI();
  }, 0);
}



async function openLoadSelectionModal(section) {
  const sanitize = (typeof window !== 'undefined' && typeof window.sanitize === 'function')
    ? window.sanitize
    : (s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;')
                           .replace(/>/g,'&gt;').replace(/"/g,'&quot;')
                           .replace(/'/g,'&#39;'));

  let list = await listReportPresets({ section, kind: 'selection', include_shared: true }).catch(()=>[]);
  let selectedId = null;

  const renderList = () => {
    const myId = currentUserId();
    const mine   = (list || []).filter(r => String(r.user_id) === String(myId))
                     .sort((a,b)=>String(a.name||'').localeCompare(String(b.name||''), undefined, {sensitivity:'base'}));
    const shared = (list || []).filter(r => String(r.user_id) !== String(myId))
                     .sort((a,b)=>String(a.name||'').localeCompare(String(b.name||''), undefined, {sensitivity:'base'}));
    const rows = mine.concat(shared);

    const rowsHtml = rows.map(p => {
      const owned    = String(p.user_id) === String(myId);
      const nameHtml = `<span class="name">${sanitize(p.name || '(unnamed)')}</span>`;
      const creator  = (p.user && (p.user.display_name || p.user.email)) ? ` <span class="hint">• by ${sanitize(p.user.display_name || p.user.email)}</span>` : '';
      const badge    = p.is_shared ? `<span class="badge">shared</span>${creator}` : '';
      const trashBtn = owned ? `<button class="btn mini bin" title="Delete" data-act="delete">🗑</button>` : '';
      return `
        <tr data-id="${p.id}">
          <td class="pick">${nameHtml} ${badge}</td>
          <td>${new Date(p.updated_at || p.created_at).toLocaleString()}</td>
          <td class="actions" style="text-align:right">${trashBtn}</td>
        </tr>`;
    }).join('') || `<tr><td colspan="3" class="hint">No saved selections</td></tr>`;

    return html(`
      <div class="form">
        <div class="row" style="justify-content:space-between;align-items:center">
          <strong>Saved selections</strong>
          <span class="hint">Section: <code>${sanitize(section)}</code></span>
        </div>
        <div class="row">
          <table class="grid compact" id="selPresetTable">
            <thead>
              <tr>
                <th>Name</th>
                <th>Updated</th>
                <th style="text-align:right">Delete</th>
              </tr>
            </thead>
            <tbody>${rowsHtml}</tbody>
          </table>
        </div>
      </div>
    `);
  };

  function wireTable() {
    const tbl = document.getElementById('selPresetTable');
    if (!tbl || tbl.__wired) return;
    tbl.__wired = true;

    const frame = window.__getModalFrame?.();
    const updateButtons = () => {
      try {
        const fr = window.__getModalFrame?.();
        if (!fr) return;
        fr.isDirty = !!selectedId; // just to enable Save/Load
        fr._updateButtons?.();
      } catch {}
    };

    // click → select & enable Load
    tbl.addEventListener('click', (e) => {
      const tr = e.target && e.target.closest('tr[data-id]');
      const bin = e.target && e.target.closest('button[data-act="delete"]');
      if (bin) {
        // deletion handled in separate listener below
      }
      if (!tr) return;
      selectedId = tr.getAttribute('data-id');
      Array.from(tbl.querySelectorAll('tbody tr')).forEach(r => r.classList.toggle('selected', r === tr));
      updateButtons();
    });

    // dblclick → apply immediately
    tbl.addEventListener('dblclick', async (e) => {
      const tr = e.target && e.target.closest('tr[data-id]');
      if (!tr) return;
      const id = tr.getAttribute('data-id');
      const chosen = (list || []).find(p => p.id === id);
      if (!chosen) return;

      const ids = Array.isArray(chosen?.selection_json?.ids) ? chosen.selection_json.ids : [];
      await applySelectionAsFilter(section, { ids });
      const closeBtn = document.getElementById('btnCloseModal');
      if (closeBtn) closeBtn.click();
    });

    // delete owned selection
    tbl.addEventListener('click', async (e) => {
      const bin = e.target && e.target.closest('button[data-act="delete"]');
      if (!bin) return;
      const tr = e.target && e.target.closest('tr[data-id]');
      const id = tr && tr.getAttribute('data-id');
      const row = (list || []).find(p => p.id === id);
      if (!row) return;
      const myIdNow = currentUserId();
      if (String(row.user_id) !== String(myIdNow)) return;
      if (!confirm(`Delete saved selection “${row.name || '(unnamed)'}”?`)) return;

      try { await deleteReportPreset(id); } catch (err) { alert(String(err?.message || err || 'Failed to delete preset')); return; }
      try { invalidatePresetCache(section, 'selection'); } catch {}
      list = await listReportPresets({ section, kind:'selection', include_shared:true }).catch(()=>[]);
      selectedId = null;
      updateButtons();

      const body = document.getElementById('modalBody');
      if (body) {
        const markup = renderList();
        if (typeof markup === 'string') body.innerHTML = markup;
        else if (markup && typeof markup.nodeType === 'number') body.replaceChildren(markup);
        else body.innerHTML = String(markup ?? '');
        wireTable();
      }
    });

    updateButtons();
  }

  showModal(
    'Load selection',
    [{ key: 'list', label: 'Saved' }],
    renderList,
    async () => {
      if (!selectedId) { alert('Pick a selection to load'); return false; }
      const chosen = (list || []).find(p => p.id === selectedId);
      if (!chosen) { alert('Selection not found'); return false; }
      const ids = Array.isArray(chosen?.selection_json?.ids) ? chosen.selection_json.ids : [];
      await applySelectionAsFilter(section, { ids });
      return true;
    },
    false,
    undefined,
    { noParentGate: true, forceEdit: true, kind: 'selection-load' }
  );

  setTimeout(wireTable, 0);
}


// ======================================
// FRONTEND — buildSearchQS (UPDATED: support ids[] → id=in.(...))
// ======================================


// ──────────────────────────────────────────────────────────────────────────────
// buildSearchQS (amended) — map submission_mode → default_submission_mode;
// keep original too for safety; pass has_custom_labels through
// ──────────────────────────────────────────────────────────────────────────────

function buildSearchQS(section, filters = {}) {
  window.__listState = window.__listState || {};
  const st = (window.__listState[section] ||= {
    page: 1,
    pageSize: 50,
    total: null,
    hasMore: false,
    filters: null,
    sort: { key: null, dir: 'asc' }
  });

  // Ensure we always have a sort object
  if (!st.sort || typeof st.sort !== 'object') {
    st.sort = { key: null, dir: 'asc' };
  }

  const qs = new URLSearchParams();
  const add = (key, val) => {
    if (val == null || val === '') return;
    qs.append(key, String(val));
  };
  const addArr = (key, arr) => {
    if (!Array.isArray(arr)) return;
    arr.forEach(v => {
      if (v != null && v !== '') qs.append(key, String(v));
    });
  };

  // paging
  if (st.pageSize !== 'ALL') {
    add('page', st.page || 1);
    add('page_size', st.pageSize || 50);
    add('include_count', 'true');
  } else {
    add('page', 1);
    add('include_count', 'true');
  }

  // IDs filter (show only these records)
  if (Array.isArray(filters.ids) && filters.ids.length > 0) {
    qs.append('id', `in.(${filters.ids.map(String).join(',')})`);
  }

  switch (section) {
    case 'candidates': {
      const {
        first_name,
        last_name,
        email,
        phone,
        pay_method,
        roles_any,
        active,
        created_from,
        created_to,

        // extra filters
        primary_job_title_contains,
        job_title_contains,
        prof_reg_number,
        prof_reg_type,
        dob,
        gender,
        town_city,
        postcode,
        updated_from,
        updated_to,
        sort_code,
        account_number,
        umbrella_name,
        tms_ref
      } = filters || {};

      add('first_name', first_name);
      add('last_name', last_name);
      add('email', email);
      add('phone', phone);
      add('pay_method', pay_method); // PAYE / UMBRELLA / BLANK

      // Care Package Roles (rota roles)
      addArr('roles_any', roles_any);

      if (typeof active === 'boolean') add('active', active);

      add('created_from', created_from);
      add('created_to', created_to);

      // Primary vs all job titles
      add('primary_job_title_contains', primary_job_title_contains);
      add('job_title_contains', job_title_contains);

      // Professional registration
      add('prof_reg_number', prof_reg_number);
      add('prof_reg_type', prof_reg_type);

      // DOB exact
      add('dob', dob);

      // Demographics
      add('gender', gender);
      add('town_city', town_city);
      add('postcode', postcode);

      // Updated_at range
      add('updated_from', updated_from);
      add('updated_to', updated_to);

      // Banking / umbrella / ref
      add('sort_code', sort_code);
      add('account_number', account_number);
      add('umbrella_name', umbrella_name);
      add('tms_ref', tms_ref);

      break;
    }

    case 'clients': {
      const {
        name,
        cli_ref,
        primary_invoice_email,
        invoice_address,
        postcode,
        ap_phone,
        vat_chargeable,
        payment_terms_days,
        mileage_charge_rate,
        ts_queries_email,
        created_from,
        created_to,
        updated_from,
        updated_to
      } = filters || {};

      if (name) add('q', name);
      add('cli_ref', cli_ref);
      add('primary_invoice_email', primary_invoice_email);
      add('invoice_address', invoice_address);
      add('postcode', postcode);
      add('ap_phone', ap_phone);
      if (typeof vat_chargeable === 'boolean') add('vat_chargeable', vat_chargeable);
      add('payment_terms_days', payment_terms_days);
      add('mileage_charge_rate', mileage_charge_rate);
      add('ts_queries_email', ts_queries_email);
      add('created_from', created_from);
      add('created_to', created_to);
      add('updated_from', updated_from);
      add('updated_to', updated_to);
      break;
    }

    case 'umbrellas': {
      const {
        name,
        bank_name,
        sort_code,
        account_number,
        vat_chargeable,
        enabled,
        created_from,
        created_to
      } = filters || {};
      if (name) add('q', name);
      add('bank_name', bank_name);
      add('sort_code', sort_code);
      add('account_number', account_number);
      if (typeof vat_chargeable === 'boolean') add('vat_chargeable', vat_chargeable);
      if (typeof enabled === 'boolean') add('enabled', enabled);
      add('created_from', created_from);
      add('created_to', created_to);
      break;
    }

    case 'timesheets': {
      const {
        booking_id,
        occupant_key_norm,
        hospital_norm,
        worked_from,
        worked_to,
        week_ending_from,
        week_ending_to,
        status,
        created_from,
        created_to
      } = filters || {};
      add('booking_id', booking_id);
      add('occupant_key_norm', occupant_key_norm);
      add('hospital_norm', hospital_norm);
      add('worked_from', worked_from);
      add('worked_to', worked_to);
      add('week_ending_from', week_ending_from);
      add('week_ending_to', week_ending_to);
      addArr('status', status);
      add('created_from', created_from);
      add('created_to', created_to);
      break;
    }

    case 'invoices': {
      const {
        invoice_no,
        client_id,
        status,
        issued_from,
        issued_to,
        due_from,
        due_to,
        created_from,
        created_to
      } = filters || {};
      add('invoice_no', invoice_no);
      add('client_id', client_id);
      addArr('status', status);
      add('issued_from', issued_from);
      add('issued_to', issued_to);
      add('due_from', due_from);
      add('due_to', due_to);
      add('created_from', created_from);
      add('created_to', created_to);
      break;
    }

    case 'contracts': {
      const weekdayCodeMap = {
        MON: 1, TUE: 2, WED: 3, THU: 4, FRI: 5, SAT: 6, SUN: 0
      };

      const {
        q: qText,
        candidate_name,
        client_name,
        candidate_id,
        client_id,
        role,
        band,
        pay_method_snapshot,
        submission_mode,
        week_ending_weekday_snapshot,
        require_reference_to_pay,
        require_reference_to_invoice,
        has_custom_labels,
        active_on,
        start_date_from,
        start_date_to,
        end_date_from,
        end_date_to,
        created_from,
        created_to,
        updated_from,
        updated_to,
        auto_invoice,
        mileage_pay_rate,
        mileage_charge_rate,
        status
      } = filters || {};

      add('q', qText);
      add('candidate_name', candidate_name);
      add('client_name', client_name);
      add('candidate_id', candidate_id);
      add('client_id', client_id);
      add('role', role);
      add('band', band);
      add('pay_method_snapshot', pay_method_snapshot);

      if (submission_mode) {
        add('default_submission_mode', submission_mode);
        add('submission_mode', submission_mode);
      }

      if (week_ending_weekday_snapshot) {
        const codeUpper = String(week_ending_weekday_snapshot).toUpperCase();
        const mapped = weekdayCodeMap[codeUpper];
        add('week_ending_weekday_snapshot', mapped != null ? mapped : week_ending_weekday_snapshot);
      }

      if (typeof auto_invoice === 'boolean') add('auto_invoice', auto_invoice);
      if (typeof require_reference_to_pay === 'boolean') {
        add('require_reference_to_pay', require_reference_to_pay);
      }
      if (typeof require_reference_to_invoice === 'boolean') {
        add('require_reference_to_invoice', require_reference_to_invoice);
      }
      if (typeof has_custom_labels === 'boolean') add('has_custom_labels', has_custom_labels);
      add('active_on', active_on);

      // Date ranges
      add('start_date_from', start_date_from);
      add('start_date_to',   start_date_to);
      add('end_date_from',   end_date_from);
      add('end_date_to',     end_date_to);
      add('created_from',    created_from);
      add('created_to',      created_to);
      add('updated_from',    updated_from);
      add('updated_to',      updated_to);

      // Mileage
      add('mileage_pay_rate',    mileage_pay_rate);
      add('mileage_charge_rate', mileage_charge_rate);

      if (status) add('status', status);

      break;
    }
  }

  // Sorting (shared for all sections that support it)
  const sort = st.sort && typeof st.sort === 'object' ? st.sort : null;
  if (sort && sort.key) {
    qs.set('order_by', String(sort.key));
    qs.set('order_dir', sort.dir === 'desc' ? 'desc' : 'asc');
  }

  return qs.toString();
}



// -----------------------------
// NEW: Save search modal (new / overwrite / shared)
// -----------------------------

// === REPLACE: openSaveSearchModal (no currentWorked; built-in sanitize) ===
// === REPLACE: openSaveSearchModal (radio-safe + stable layout + full-width dropdown)
// ======================================
// FRONTEND — openSaveSearchModal (UPDATED)
// Behaviour:
// - If there IS a selection: show “Save Selection” UI only (Save new / Append).
// - If there is NO selection: show “Save Filters” UI only.
// ======================================
// Save selection/search modal — simplified choices per your spec
async function openSaveSearchModal(section, filters){
  const sanitize = (typeof window !== 'undefined' && typeof window.sanitize === 'function')
    ? window.sanitize
    : (s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;')
                           .replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'));

  // Selection present?
  window.__selection = window.__selection || {};
  const sel = window.__selection[section];
  const hasSelection = !!sel && sel.ids && sel.ids.size > 0;

  // If we’re going to append, we need the user’s presets list
  const mineServer = await listReportPresets({ section, kind: 'search', include_shared: false }).catch(()=>[]);
  const myId = currentUserId();
  const mine = (mineServer || []).filter(m => String(m.user_id) === String(myId));
  const hasOwned = Array.isArray(mine) && mine.length > 0;
  const optionsHtml = hasOwned
    ? mine.map(m => `<option value="${m.id}">${sanitize(m.name)}</option>`).join('')
    : '';

  let body;
  if (hasSelection) {
    body = html(`
      <div class="form" id="saveSearchForm" style="max-width:720px">
        <div class="row">
          <label for="presetName">Preset name</label>
          <div class="controls">
            <input id="presetName" name="preset_name" class="input" placeholder="e.g. ‘Shortlist — RMNs’" />
          </div>
        </div>

        <div class="row">
          <label>Mode</label>
          <div class="controls" style="display:flex;flex-direction:column;gap:8px;min-width:0">
            <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
              <label class="inline"><input type="radio" name="mode" value="new" checked> <span>Save selection as new</span></label>
              <label class="inline">
                <input type="radio" name="mode" value="append" ${hasOwned ? '' : 'disabled'}>
                <span>Append to existing selection</span>
              </label>
            </div>
            <div id="appendWrap" style="display:none; width:100%; max-width:100%">
              <div class="hint" style="margin:2px 0 4px">${hasOwned ? 'Choose selection to append to' : 'You don’t own any selections to append'}</div>
              <select id="appendPresetId" class="select" style="width:100%; max-width:100%">${optionsHtml}</select>
            </div>
          </div>
        </div>

        <div class="row">
          <label for="presetShared">Visibility</label>
          <div class="controls">
            <label class="inline"><input id="presetShared" type="checkbox"> <span>Visible to all users</span></label>
          </div>
        </div>
      </div>
    `);
  } else {
    body = html(`
      <div class="form" id="saveSearchForm" style="max-width:720px">
        <div class="row">
          <label for="presetName">Preset name</label>
          <div class="controls">
            <input id="presetName" name="preset_name" class="input" placeholder="e.g. ‘PAYE RMNs’" />
          </div>
        </div>

        <div class="row">
          <label>Visibility</label>
          <div class="controls">
            <label class="inline"><input id="presetShared" type="checkbox"> <span>Visible to all users</span></label>
          </div>
        </div>
      </div>
    `);
  }

  showModal(
    hasSelection ? 'Save selection' : 'Save search',
    [{ key: 'form', label: 'Details' }],
    () => body,
    async () => {
      const name  = String(document.getElementById('presetName')?.value || '').trim();
      const share = !!document.getElementById('presetShared')?.checked;
      if (!name && !hasSelection) { alert('Please enter a name'); return false; }

      // Recompute selection now
      const curSel = window.__selection[section];
      const hasSelectionNow = !!curSel && curSel.ids && curSel.ids.size>0;

      if (hasSelectionNow) {
        const modeInput = document.querySelector('#saveSearchForm input[name="mode"]:checked');
        const mode = (modeInput?.value || 'new').toLowerCase();
        if (mode === 'append') {
          if (!hasOwned) { alert('You don’t own any selections to append'); return false; }
          const targetId = (document.getElementById('appendPresetId')?.value) || '';
          if (!targetId) { alert('Select a selection to append to'); return false; }

          const target = (await listReportPresets({ section, kind:'search', include_shared:false }).catch(()=>[])).find(p => String(p.id) === String(targetId));
          const targetSel = target?.selection || target?.selection_json || null;
          const merged = mergeSelectionSnapshots(section,
            { section, fingerprint: targetSel?.fingerprint || '', ids: Array.from(new Set((targetSel?.ids||[]).map(String))) },
            { section, fingerprint: curSel.fingerprint || '',  ids: Array.from(curSel.ids || []) }
          );
          await updateReportPreset({ id: targetId, name: target?.name, section, kind:'search', selection: merged, is_shared: target?.is_shared });
        } else {
          const payload = {
            section, kind:'search', name, is_shared: share,
            selection: {
              fingerprint: curSel.fingerprint || '',
              ids: Array.from(curSel.ids || [])
            }
          };
          await createReportPreset(payload);
        }
      } else {
        const payload = { section, kind:'search', name, is_shared: share, filters: filters || {} };
        await createReportPreset(payload);
      }

      invalidatePresetCache(section, 'search');
      try { window.dispatchEvent(new Event('search-preset-updated')); } catch(_) {}
      return true;
    },
    false,
    undefined,
    { noParentGate: true, forceEdit: true, kind: hasSelection ? 'selection-save' : 'search-save' }
  );

  // Wire append toggling (only in selection mode)
  if (hasSelection) {
    setTimeout(() => {
      const formEl = document.getElementById('saveSearchForm');
      if (!formEl || formEl.dataset.wired === '1') return;
      formEl.dataset.wired = '1';
      const appendWrap = document.getElementById('appendWrap');
      formEl.querySelectorAll('input[name="mode"]').forEach(r =>
        r.addEventListener('change', () => {
          const isAppend = r.value === 'append' && r.checked;
          if (appendWrap) appendWrap.style.display = isAppend ? 'block' : 'none';
        })
      );
    }, 0);
  }
}

// === REPLACE: openLoadSearchModal (built-in sanitize; no globals required) ===
// FRONTEND — UPDATED
// openLoadSearchModal: emit event with filters (so parent re-applies after repaint),
// stage-delete UI kept; shows shared badge and (when present) creator.


// ======================================
// FRONTEND — openLoadSearchModal (UPDATED)
// Behaviour: list saved presets; double-click applies immediately & closes.
// - If preset has selection with explicit ids → show only those (applySelectionAsFilter).
// - If selection has allMatching=true → apply its filters instead.
// - If preset is a filters-only search → apply filters.
// ======================================

async function openLoadSearchModal(section){
  const sanitize = (typeof window !== 'undefined' && typeof window.sanitize === 'function')
    ? window.sanitize
    : (s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;')
                           .replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'));

  // Filters-only presets
  let list = await listReportPresets({ section, kind: 'search', include_shared: true }).catch(()=>[]);
  let selectedId = null;

  function sortMineThenShared(rows, myId) {
    const mine   = (rows || []).filter(r => String(r.user_id) === String(myId))
                     .sort((a,b)=>String(a.name||'').localeCompare(String(b.name||''), undefined, {sensitivity:'base'}));
    const shared = (rows || []).filter(r => String(r.user_id) !== String(myId))
                     .sort((a,b)=>String(a.name||'').localeCompare(String(b.name||''), undefined, {sensitivity:'base'}));
    return mine.concat(shared);
  }

  const renderList = () => {
    const myId = currentUserId();
    const rows = sortMineThenShared(list || [], myId);

    const rowsHtml = rows.map(p => {
      const owned    = String(p.user_id) === String(myId);
      const nameHtml = `<span class="name">${sanitize(p.name)}</span>`;
      const creator  = (p.user && (p.user.display_name || p.user.email)) ? ` <span class="hint">• by ${sanitize(p.user.display_name || p.user.email)}</span>` : '';
      const badge    = p.is_shared ? `<span class="badge">shared</span>${creator}` : '';
      const trashBtn = owned ? `<button class="bin btn btn-ghost btn-sm" title="Delete">🗑</button>` : '';
      return `
        <tr data-id="${p.id}">
          <td class="pick">${nameHtml} ${badge}</td>
          <td>${new Date(p.updated_at || p.created_at).toLocaleString()}</td>
          <td class="actions">${trashBtn}</td>
        </tr>`;
    }).join('') || `<tr><td colspan="3" class="hint">No saved searches</td></tr>`;

    return html(`
      <div class="form">
        <div class="row" style="justify-content:space-between;align-items:center">
          <strong>Saved searches</strong>
          <span class="hint">Section: <code>${sanitize(section)}</code></span>
        </div>
        <div class="row">
          <table class="grid compact" id="presetTable">
            <thead><tr><th>Name</th><th>Updated</th><th></th></tr></thead>
            <tbody>${rowsHtml}</tbody>
          </table>
        </div>
      </div>
    `);
  };

  function wirePresetTable() {
    const tbl = document.getElementById('presetTable');
    if (!tbl || tbl.__wired) return;
    tbl.__wired = true;

    // Select row on click
    tbl.addEventListener('click', (e) => {
      const tr = e.target && e.target.closest('tr[data-id]');
      if (!tr) return;
      selectedId = tr.getAttribute('data-id');
      Array.from(tbl.querySelectorAll('tbody tr')).forEach(r => r.classList.toggle('selected', r === tr));
    });

    // Double-click: apply filters immediately and close + notify parent
    tbl.addEventListener('dblclick', async (e) => {
      const tr = e.target && e.target.closest('tr[data-id]');
      if (!tr) return;
      const id = tr.getAttribute('data-id');
      const chosen = (list || []).find(p => p.id === id);
      if (!chosen) return;

      const filters = chosen.filters || chosen.filters_json || chosen.filtersJson || {};

      window.__listState = window.__listState || {};
      const st = (window.__listState[section] ||= { page:1, pageSize:50, total:null, hasMore:false, filters:null });
      st.page = 1; st.filters = filters || {};
      const rows = await search(section, st.filters);
      renderSummary(rows);

      // notify parent advanced-search to re-populate its form
      try { window.__PENDING_ADV_PRESET = { section, filters: st.filters || {} }; } catch {}

      const closeBtn = document.getElementById('btnCloseModal');
      if (closeBtn) closeBtn.click();
    });

    // Delete handler
    tbl.addEventListener('click', async (e) => {
      const bin = e.target && e.target.closest('button.bin');
      if (!bin) return;
      const tr = e.target && e.target.closest('tr[data-id]');
      const id = tr && tr.getAttribute('data-id');
      const row = (list || []).find(p => p.id === id);
      if (!row) return;
      const myIdNow = currentUserId();
      if (String(row.user_id) !== String(myIdNow)) return;
      if (!confirm(`Delete saved preset “${row.name}”? This cannot be undone.`)) return;

      try { await deleteReportPreset(id); } catch (err) { alert(String(err?.message || err || 'Failed to delete preset')); return; }
      try { invalidatePresetCache(section, 'search'); } catch {}
      list = await listReportPresets({ section, kind:'search', include_shared:true }).catch(()=>[]);

      const body = document.getElementById('modalBody');
      if (body) {
        const markup = renderList();
        if (typeof markup === 'string') body.innerHTML = markup;
        else if (markup && typeof markup.nodeType === 'number') body.replaceChildren(markup);
        else body.innerHTML = String(markup ?? '');
        wirePresetTable();
      }
    });
  }

  showModal(
    'Load saved search',
    [{ key: 'list', label: 'Saved' }],
    renderList,
    async () => {
      if (!selectedId) { alert('Pick a preset to load'); return false; }
      const chosen = (list || []).find(p => p.id === selectedId);
      if (!chosen) { alert('Preset not found'); return false; }

      const filters = chosen.filters || chosen.filters_json || chosen.filtersJson || {};
      window.__listState = window.__listState || {};
      const st = (window.__listState[section] ||= { page:1, pageSize:50, total:null, hasMore:false, filters:null });
      st.page = 1; st.filters = filters || {};
      const rows = await search(section, st.filters);
      renderSummary(rows);

      // notify parent advanced-search to re-populate its form
      try { window.__PENDING_ADV_PRESET = { section, filters: st.filters || {} }; } catch {}
      return true; // child closes; parent will onReturn and re-populate form
    },
    false,
    undefined,
    { noParentGate: true, forceEdit: true, kind: 'search-load' }
  );

  setTimeout(wirePresetTable, 0);
}



// ============================================================================
// Selection presets — wrappers to save/list/load selection presets via backend
// ============================================================================

/**
 * Save a selection preset.
 * By default uses kind: 'selection' to keep it distinct from pure filter presets,
 * but your backend can also store it under kind: 'search' with a `selection` block.
 */

/** List selection presets for a section (owned + shared if requested). */
async function listSelectionPresets(section, { include_shared = true } = {}) {
  if (typeof listReportPresets === 'function') {
    return await listReportPresets({ section, kind: 'selection', include_shared });
  }
  // Fallback, if needed:
  const qs = new URLSearchParams({ section, kind: 'selection', include_shared: include_shared ? 'true' : 'false' });
  const res = await authFetch(API(`/api/report-presets?${qs}`));
  return res?.ok ? res.json().catch(()=>[]) : [];
}

/**
 * Load a selection preset by ID or name. Returns the preset object (or null).
 * Note: this does not apply it — use applySelectionSnapshot() or mergeSelectionSnapshots().
 */
async function loadSelectionPreset(section, idOrName) {
  const all = await listSelectionPresets(section, { include_shared: true }) || [];
  if (!idOrName) return null;
  const match = all.find(p => String(p.id) === String(idOrName)) ||
                all.find(p => String(p.name || '').toLowerCase() === String(idOrName).toLowerCase());
  return match || null;
}













// -----------------------------
// UPDATED: openSearchModal()
// - Fix search submit path (object filters, not JSON string)
// - Provide inline Save/Load buttons that open child modals
// -----------------------------
// -----------------------------
// FIXED: openSearchModal()
// - No undefined variables
// - Uses attachUkDatePicker + extractFiltersFromForm
// - Section-aware fields that match buildSearchQS()
// -----------------------------
// === REPLACE: openSearchModal (icons + robust wiring + fallback for old text buttons) ===
// === FORCE-HIDE legacy buttons (one-time CSS) ===
// Run once: force-hide any legacy white buttons, and add compact button styles
(function ensureAdvancedSearchCSS(){
  if (document.getElementById('advSearchCSS')) return;
  const s = document.createElement('style');
  s.id = 'advSearchCSS';
  s.textContent = `
    /* never show legacy white buttons */
    #btnLoadSavedSearch, #btnSaveSearch { display: none !important; visibility: hidden !important; }

    /* compact, not-white text buttons */
    .adv-btn {
      height: 26px;
      padding: 0 10px;
      border: 1px solid #d1d5db;
      border-radius: 6px;
      background: #f3f4f6;          /* not white */
      color: #111827;
      font-size: 12px;
      font-weight: 500;
      cursor: pointer;
    }
    .adv-btn:hover { background: #e5e7eb; }
  `;
  document.head.appendChild(s);
})();

async function openSearchModal(opts = {}) {
  const TIMESHEET_STATUS = ['ERROR','RECEIVED','REVOKED','STORED','SAT','SUN','BH'];
  const INVOICE_STATUS   = ['DRAFT','ISSUED','ON_HOLD','PAID'];

  const row = (label, inner) => `
    <div class="row">
      <label>${label}</label>
      <div class="controls">${inner}</div>
    </div>`;

  const inputText = (name, placeholder='') =>
    `<input class="input" type="text" name="${name}" placeholder="${placeholder}" />`;

  const boolSelect = (name) => `
    <select name="${name}">
      <option value="">Any</option>
      <option value="true">Yes</option>
      <option value="false">No</option>
    </select>`;

  const datePair = (fromName, fromLabel, toName, toLabel) => `
    ${row(fromLabel, `<input class="input" type="text" name="${fromName}" placeholder="DD/MM/YYYY" />`)}
    ${row(toLabel,   `<input class="input" type="text" name="${toName}"   placeholder="DD/MM/YYYY" />`)}`;

  // Inline date range (From / To on one row)
  const dateRangeRow = (fromName, toName, label) => row(label, `
    <div class="split">
      <input class="input" type="text" name="${fromName}" placeholder="From DD/MM/YYYY" />
      <input class="input" type="text" name="${toName}"   placeholder="To DD/MM/YYYY" />
    </div>`);

  const multi = (name, values) =>
    `<select name="${name}" multiple size="6">${values.map(v=>`<option value="${v}">${v}</option>`).join('')}</select>`;

  const friendly = (section, key, fallback) => {
    try {
      if (typeof getFriendlyHeaderLabel === 'function') {
        const lbl = getFriendlyHeaderLabel(section, key);
        if (lbl && typeof lbl === 'string') return lbl;
      }
    } catch {}
    return fallback || key;
  };

  // Section-specific filters
  let inner = '';
  if (currentSection === 'candidates') {
    let roleOptions = [];
    try { roleOptions = await loadGlobalRoleOptions(); } catch { roleOptions = []; }

    inner = [
      // Basic identity / contact
      row('First name',           inputText('first_name')),
      row('Last name',            inputText('last_name')),
      row('Email',                `<input class="input" type="email" name="email" placeholder="name@domain" />`),
      row('Telephone',            inputText('phone')),

      // Pay type (including blank)
      row('Pay type', `
        <select name="pay_method">
          <option value="">Any</option>
          <option value="PAYE">PAYE</option>
          <option value="UMBRELLA">UMBRELLA</option>
          <option value="BLANK">Blank</option>
        </select>`),

      // Care Package Role (rota roles)
      row('Care Package Role (any)', `
        <select name="roles_any" multiple size="6">
          ${roleOptions.map(r => `<option value="${r}">${r}</option>`).join('')}
        </select>`),

      // Job titles
      row('Primary Job Title contains', inputText('primary_job_title_contains', 'e.g. CPN')),
      row('Any Job Title contains',     inputText('job_title_contains', 'includes primary and secondary')),

      // Professional registration
      row('Professional Reg Number', inputText('prof_reg_number')),
      row('Professional Reg Type', `
        <select name="prof_reg_type">
          <option value="">Any</option>
          <option value="NMC">NMC</option>
          <option value="GMC">GMC</option>
          <option value="HCPC">HCPC</option>
        </select>`),

      // DOB exact
      row('Date of birth', `<input class="input" type="text" name="dob" placeholder="DD/MM/YYYY" />`),

      // Gender / location
      row('Gender', `
        <select name="gender">
          <option value="">Any</option>
          <option value="Male">Male</option>
          <option value="Female">Female</option>
          <option value="Other">Other</option>
        </select>`),
      row('City',     inputText('town_city', 'Town / City')),
      row('Postcode', inputText('postcode', 'e.g. W7 3EE')),

      // Status + created range (inline)
      row('Active', boolSelect('active')),
      dateRangeRow('created_from','created_to','Created date (from / to)'),

      // Last updated range (inline)
      dateRangeRow('updated_from','updated_to','Last updated (from / to)'),

      // Banking / umbrella / ref
      row('Sort Code',       inputText('sort_code', '12-34-56')),
      row('Account Number',  inputText('account_number')),
      row('Umbrella Name',   inputText('umbrella_name')),
      row('TMS Ref',         inputText('tms_ref'))
    ].join('');
  } else if (currentSection === 'clients') {
    const lblName     = friendly('clients','name','Client name');
    const lblCliRef   = friendly('clients','cli_ref','Client ref');
    const lblEmail    = friendly('clients','primary_invoice_email','Invoice email');
    const lblInvAddr  = friendly('clients','invoice_address','Invoice address');
    const lblPost     = friendly('clients','postcode','Postcode');
    const lblApPhone  = friendly('clients','ap_phone','A/P phone');

    inner = [
      row(lblName,          inputText('name', 'partial match')),
      row(lblCliRef,        inputText('cli_ref')),
      row(lblEmail,         `<input class="input" type="email" name="primary_invoice_email" placeholder="ap@client" />`),
      row(lblInvAddr,       inputText('invoice_address')),
      row(lblPost,          inputText('postcode')),
      row(lblApPhone,       inputText('ap_phone')),
      row('VAT chargeable (Yes/No)', boolSelect('vat_chargeable')),
      row('Payment terms (days)', `<input class="input" type="number" name="payment_terms_days" min="0" />`),
      row('Mileage charge rate',  `<input class="input" type="number" step="0.01" name="mileage_charge_rate" />`),
      row('Timesheet queries email', `<input class="input" type="email" name="ts_queries_email" placeholder="ts@client" />`),
      dateRangeRow('created_from','created_to','Created date (from / to)'),
      dateRangeRow('updated_from','updated_to','Last updated (from / to)')
    ].join('');
  } else if (currentSection === 'umbrellas') {
    inner = [
      row('Name',                 inputText('name')),
      row('Bank',                 inputText('bank_name')),
      row('Sort code',            inputText('sort_code', '12-34-56')),
      row('Account number',       inputText('account_number')),
      row('VAT chargeable',       boolSelect('vat_chargeable')),
      row('Enabled',              boolSelect('enabled')),
      datePair('created_from','Created from','created_to','Created to')
    ].join('');
  } else if (currentSection === 'timesheets') {
    inner = [
      row('Booking ID',           inputText('booking_id')),
      row('Candidate key',        inputText('occupant_key_norm', 'candidate_id / key_norm')),
      row('Hospital',             inputText('hospital_norm')),
      datePair('worked_from','Worked from (date)','worked_to','Worked to (date)'),
      datePair('week_ending_from','Week ending from','week_ending_to','Week ending to'),
      row('Status',               multi('status', TIMESHEET_STATUS)),
      datePair('created_from','Created from','created_to','Created to')
    ].join('');
  } else if (currentSection === 'invoices') {
    inner = [
      row('Invoice no',           inputText('invoice_no')),
      row('Client ID',            inputText('client_id', 'UUID')),
      row('Status',               multi('status', INVOICE_STATUS)),
      datePair('issued_from','Issued from','issued_to','Issued to'),
      datePair('due_from','Due from','due_to','Due to'),
      datePair('created_from','Created from','created_to','Created to')
    ].join('');
  } else if (currentSection === 'contracts') {
    const weekdayOptions = [
      { value: '',    label: 'Any'    },
      { value: 'MON', label: 'Monday' },
      { value: 'TUE', label: 'Tuesday' },
      { value: 'WED', label: 'Wednesday' },
      { value: 'THU', label: 'Thursday' },
      { value: 'FRI', label: 'Friday' },
      { value: 'SAT', label: 'Saturday' },
      { value: 'SUN', label: 'Sunday' }
    ];

    const lblCandName  = friendly('contracts','candidate_display','Candidate name');
    const lblClient    = friendly('contracts','client_name','Client name');
    const lblBand      = friendly('contracts','band','Band');
    const lblRole      = friendly('contracts','role','Role');
    const lblPaySnap   = friendly('contracts','pay_method_snapshot','Pay method snapshot');
    const lblSubMode   = friendly('contracts','default_submission_mode','Submission mode');

    inner = [
      row('Free text',            inputText('q', 'client / candidate / role')),

      // Explicit name filters
      row(`${lblCandName} contains`, inputText('candidate_name', 'partial name match')),
      row(`${lblClient} contains`,   inputText('client_name', 'partial name match')),

      row('Candidate ID',         inputText('candidate_id', 'UUID')),
      row('Client ID',            inputText('client_id', 'UUID')),
      row(lblRole,                inputText('role', 'e.g. RMN')),
      row(lblBand,                inputText('band', 'e.g. 5 / 6 / 7')),
      row(lblPaySnap,  `
        <select name="pay_method_snapshot">
          <option value="">Any</option>
          <option value="PAYE">PAYE</option>
          <option value="UMBRELLA">UMBRELLA</option>
        </select>`),
      row(lblSubMode,      `
        <select name="submission_mode">
          <option value="">Any</option>
          <option value="MANUAL">Manual</option>
          <option value="ELECTRONIC">Electronic</option>
        </select>`),

      row('Start date (from / to)',
        `<div class="split">
           <input class="input" type="text" name="start_date_from" placeholder="From DD/MM/YYYY" />
           <input class="input" type="text" name="start_date_to"   placeholder="To DD/MM/YYYY" />
         </div>`),

      row('End date (from / to)',
        `<div class="split">
           <input class="input" type="text" name="end_date_from" placeholder="From DD/MM/YYYY" />
           <input class="input" type="text" name="end_date_to"   placeholder="To DD/MM/YYYY" />
         </div>`),

      row('Week-ending weekday',  `
        <select name="week_ending_weekday_snapshot">
          ${weekdayOptions.map(o=>`<option value="${o.value}">${o.label}</option>`).join('')}
        </select>`),

      row('Auto invoice',           boolSelect('auto_invoice')),
      row('Require ref to pay',     boolSelect('require_reference_to_pay')),
      row('Require ref to invoice', boolSelect('require_reference_to_invoice')),
      row('Has custom labels',      boolSelect('has_custom_labels')),
      row('Active on date',         `<input class="input" type="text" name="active_on" placeholder="DD/MM/YYYY" />`),

      dateRangeRow('created_from','created_to','Created date (from / to)'),
      dateRangeRow('updated_from','updated_to','Last updated (from / to)'),

      row('Mileage pay rate',    `<input class="input" type="number" step="0.01" name="mileage_pay_rate" />`),
      row('Mileage charge rate', `<input class="input" type="number" step="0.01" name="mileage_charge_rate" />`)
    ].join('');
  } else {
    inner = `<div class="tabc">No filters for this section.</div>`;
  }

  // Header: two small dark buttons, side by side, using .adv-btn styling
  const headerHtml = `
    <div class="row" id="searchHeaderRow" style="justify-content:flex-end;gap:6px;margin-bottom:.5rem">
      <div class="controls" style="display:flex;justify-content:flex-end;gap:6px">
        <button type="button" class="adv-btn" data-adv-act="load">Load saved search</button>
        <button type="button" class="adv-btn" data-adv-act="save">Save search</button>
      </div>
    </div>`;

  const formHtml = `
    <div class="form" id="searchForm">
      ${headerHtml}
      ${inner}
    </div>
  `;

  showModal(
    'Advanced Search',
    [{ key: 'filter', title: 'Filters' }],
    () => formHtml,
    async () => {
      window.__listState = window.__listState || {};
      const st = (window.__listState[currentSection] ||= {
        page: 1,
        pageSize: 50,
        total: null,
        hasMore: false,
        filters: null
      });
      st.page = 1;

      // Reset selection for the new dataset (IDs-only)
      window.__selection = window.__selection || {};
      const sel = (window.__selection[currentSection] ||= { fingerprint:'', ids:new Set() });
      const filters = extractFiltersFromForm('#searchForm');
      sel.fingerprint = JSON.stringify({ section: currentSection, filters });
      sel.ids.clear();

      const rows = await search(currentSection, filters);
      if (rows) renderSummary(rows);
      return true; // saveForFrame will close this advanced-search frame on success
    },
    false,
    () => {
      // Apply pending preset (if a child "Load search" just set it)
      const pending = (typeof window !== 'undefined') ? window.__PENDING_ADV_PRESET : null;
      if (pending && pending.section) {
        try {
          window.dispatchEvent(new CustomEvent('adv-search-apply-preset', { detail: pending }));
        } catch {}
      }
      if (typeof window !== 'undefined') delete window.__PENDING_ADV_PRESET;

      try { wireAdvancedSearch(); } catch {}

      // Prefill from current filters immediately on mount
      try {
        window.__listState = window.__listState || {};
        const st = (window.__listState[currentSection] ||= {
          page:1,
          pageSize:50,
          total:null,
          hasMore:false,
          filters:null
        });
        populateSearchFormFromFilters(st.filters || {}, '#searchForm');
      } catch {}

      // Wire datepickers to *all* DD/MM/YYYY fields (including From/To ranges)
      try {
        if (typeof attachUkDatePicker === 'function') {
          const root = document.getElementById('searchForm');
          if (root) {
            root.querySelectorAll('input[type="text"]').forEach(el => {
              const ph = (el.getAttribute('placeholder') || '').toUpperCase();
              if (ph.includes('DD/MM/YYYY')) {
                // Avoid double-wiring, in case modal is reopened
                if (!el.__ukPickerWired) {
                  attachUkDatePicker(el);
                  el.__ukPickerWired = true;
                }
              }
            });
          }
        }
      } catch {}
    },
    { noParentGate: true, forceEdit: true, kind: 'advanced-search' }
  );

  // Extra wiring (eg. load/save presets actions)
  setTimeout(wireAdvancedSearch, 0);
}



// === REPLACE: openSearchModal (icons only, legacy forced hidden, robust wiring) ===
// === REPLACE: openSearchModal (compact text buttons + robust delegated wiring) ===
// FRONTEND — UPDATED
// openSearchModal: compact text buttons + delegated wiring + listens for preset-apply event
// and re-applies filters AFTER parent repaint (onReturn hook).
// ======================================
// FRONTEND — openSearchModal (UPDATED)
// Branches: if there's a selection in the summary → go straight to Save Selection.
// Otherwise, open the Advanced Search (filters) modal as usual.
// ======================================


// ======================================
// FRONTEND — wireAdvancedSearch (UPDATED only to call the updated save/load modals)
// ======================================


function wireAdvancedSearch() {
  const bodyEl = document.getElementById('modalBody');
  const formEl = document.getElementById('searchForm');
  if (!bodyEl || !formEl) return;

  formEl.querySelectorAll('input[placeholder="DD/MM/YYYY"]').forEach(el => {
    try { attachUkDatePicker(el); } catch {}
  });

  // hide any legacy buttons
  formEl.querySelectorAll('#btnLoadSavedSearch,#btnSaveSearch').forEach(el => {
    el.style.display = 'none'; el.hidden = true; el.disabled = true;
  });

  // delegated click (survives re-renders)
  if (bodyEl._advSearchHandler) bodyEl.removeEventListener('click', bodyEl._advSearchHandler, true);
  bodyEl._advSearchHandler = async (e) => {
    const btn = e.target && e.target.closest('button[data-adv-act]');
    if (!btn) return;
    const act = btn.dataset.advAct;
    if (act === 'load') {
      await openLoadSearchModal(currentSection);
    } else if (act === 'save') {
      // ↳ Recompute filters at click time so we pass the *current* criteria
      const filters = extractFiltersFromForm('#searchForm');
      await openSaveSearchModal(currentSection, filters);
    }
  };
  bodyEl.addEventListener('click', bodyEl._advSearchHandler, true);

  // listen once for preset apply events (filters only — selection is handled in the load modal)
  if (!window.__advPresetListener) {
    window.__advPresetListener = (ev) => {
      const det = ev && ev.detail;
      const here = String(currentSection || '').toLowerCase();
      const inc  = String(det && det.section || '').toLowerCase();
      if (!det || !inc || inc !== here) return;
      try { window.__squelchDirty = true; } catch {}
      try { populateSearchFormFromFilters(det.filters || {}, '#searchForm'); }
      finally {
        setTimeout(() => { try { window.__squelchDirty = false; } catch {} }, 0);
      }
    };
    window.addEventListener('adv-search-apply-preset', window.__advPresetListener);
  }

  // NEW: Immediately populate from current filters when the modal mounts/re-renders
  try {
    window.__listState = window.__listState || {};
    const st = (window.__listState[currentSection] ||= { page:1, pageSize:50, total:null, hasMore:false, filters:null });
    populateSearchFormFromFilters(st.filters || {}, '#searchForm');
  } catch {}
}

// -----------------------------
// UPDATED: renderTools()
// - Keep Search…, add “Saved searches…” shortcut that opens Search modal pre-focused on loading presets
// -----------------------------

function renderTools(){
  const el = byId('toolButtons');
  const canCreate = ['candidates','clients','umbrellas','contracts'].includes(currentSection); // added contracts

  el.innerHTML = '';
  const addBtn = (txt, cb) => {
    const b = document.createElement('button');
    b.textContent = txt;
    b.onclick = cb;
    el.appendChild(b);
    return b;
  };

  const btnCreate = addBtn('Create New Record', () => openCreate());
  addBtn('Show all records', () => showAllRecords(currentSection));
  addBtn('Search…', () => openSearchModal()); // left toolbar search

  if (!canCreate) btnCreate.disabled = true;
}



async function showAllRecords(section = currentSection){
  // Reset paging & clear all filters
  window.__listState = window.__listState || {};
  const st = (window.__listState[section] ||= {
    page: 1, pageSize: 50, total: null, hasMore: false, filters: null,
  });
  st.page = 1;
  st.filters = null;

  // Forget any focused shortlist (IDs selection)
  const sel = ensureSelection(section);
  sel.ids.clear();
  sel.fingerprint = ''; // optional: allow renderSummary to recompute

  // Reload full list for the section and render
  const rows = await loadSection();
  renderSummary(rows);
}
// Optional alias if other code calls clearFilters()
function clearFilters(section = currentSection){
  return showAllRecords(section);
}


// ===================== NEW HELPERS (UI + data) =====================

// Cache for global roles
// Cache for global roles
let __GLOBAL_ROLE_CODES_CACHE__ = null;
let __GLOBAL_ROLE_CODES_CACHE_TS__ = 0;
function invalidateGlobalRoleOptionsCache(){
  __GLOBAL_ROLE_CODES_CACHE__ = null;
  __GLOBAL_ROLE_CODES_CACHE_TS__ = 0;
}

// Load and dedupe all role codes from client defaults across all clients
// 🔧 CHANGE: truly global roles list (de-duplicated across ALL clients), with a short TTL cache.
// Works even when there is no active client in context (e.g., Candidate create).
async function loadGlobalRoleOptions(){
  const now = Date.now();
  const TTL_MS = 60_000;

  // Prefer our global cache
  if (Array.isArray(window.__GLOBAL_ROLE_CODES_ALL__) &&
      (now - (window.__GLOBAL_ROLE_CODES_ALL_TS__ || 0) < TTL_MS)) {
    return window.__GLOBAL_ROLE_CODES_ALL__;
  }

  // Fallback to legacy '__fallback__' if it’s fresh
  if (window.__GLOBAL_ROLE_CODES_CACHE__ &&
      window.__GLOBAL_ROLE_CODES_CACHE__['__fallback__'] &&
      (now - (window.__GLOBAL_ROLE_CODES_CACHE_TS__?.['__fallback__'] || 0) < TTL_MS)) {
    const arr = window.__GLOBAL_ROLE_CODES_CACHE__['__fallback__'];
    window.__GLOBAL_ROLE_CODES_ALL__ = arr.slice();
    window.__GLOBAL_ROLE_CODES_ALL_TS__ = now;
    return arr;
  }

  // Aggregate roles across all clients (enabled client-default windows only)
  const roles = new Set();
  try {
    const clients = await listClientsBasic();
    for (const c of (clients || [])) {
      try {
        const rows = await listClientRates(c.id, { only_enabled: true });
        for (const r of (rows || [])) {
          if (r && r.role) roles.add(String(r.role));
        }
      } catch { /* ignore per-client errors */ }
    }
  } catch { /* ignore listClientsBasic error */ }

  const arr = [...roles].sort((a,b)=> a.localeCompare(b));

  // Save to both the new global cache AND the legacy fallback keys so existing invalidation hooks still help
  window.__GLOBAL_ROLE_CODES_ALL__ = arr;
  window.__GLOBAL_ROLE_CODES_ALL_TS__ = now;

  window.__GLOBAL_ROLE_CODES_CACHE__    = window.__GLOBAL_ROLE_CODES_CACHE__    || Object.create(null);
  window.__GLOBAL_ROLE_CODES_CACHE_TS__ = window.__GLOBAL_ROLE_CODES_CACHE_TS__ || Object.create(null);
  window.__GLOBAL_ROLE_CODES_CACHE__['__fallback__']    = arr;
  window.__GLOBAL_ROLE_CODES_CACHE_TS__['__fallback__'] = now;

  return arr;
}


// Render roles editor into a container; updates modalCtx.rolesState
function renderRolesEditor(container, rolesState, allRoleOptions){
  // Detect read-only (view mode) from the active modal frame
  const fr = (window.__modalStack || [])[ (window.__modalStack || []).length - 1 ] || null;
  // ✅ Treat 'create' same as 'edit' (editable)
  const readOnly = !fr || !(fr.mode === 'edit' || fr.mode === 'create');

  // Local, mutable copy of available options so we can refresh after adds/removes
  let roleOptions = Array.isArray(allRoleOptions) ? allRoleOptions.slice() : [];

  function markDirty() {
    try {
      const stack = window.__modalStack || [];
      const top = stack[stack.length - 1];
      if (top) top.isDirty = true;
      try { window.dispatchEvent(new CustomEvent('modal-dirty')); }
      catch { try { window.dispatchEvent(new Event('modal-dirty')); } catch(_) {} }
    } catch (_) {}
  }

  container.innerHTML = `
    <div class="roles-editor">
      <div class="roles-add" ${readOnly ? 'style="display:none"' : ''}>
        <select id="rolesAddSelect">
          <option value="">Add role…</option>
          ${roleOptions.map(code => `<option value="${code}">${code}</option>`).join('')}
        </select>
        <button id="rolesAddBtn" type="button">Add</button>
      </div>
      <ul id="rolesList" class="roles-list"></ul>
    </div>
  `;

  const sel = container.querySelector('#rolesAddSelect');
  const btn = container.querySelector('#rolesAddBtn');
  const ul  = container.querySelector('#rolesList');

  const byCode = (code) => (rolesState || []).find(r => String(r.code) === String(code));

  function availableOptions(){
    const picked = new Set((rolesState||[]).map(x => x.code));
    return roleOptions.filter(code => !picked.has(code));
  }

  function refreshAddSelect(){
    if (!sel) return;
    const opts = ['<option value="">Add role…</option>']
      .concat(availableOptions().map(code => `<option value="${code}">${code}</option>`))
      .join('');
    sel.innerHTML = opts;
  }

  function renderList(){
    ul.innerHTML = '';
    const arr = (rolesState||[]).slice().sort((a,b)=> (a.rank||0) - (b.rank||0));

    arr.forEach((item, idx) => {
      const li = document.createElement('li');
      li.className = 'role-item';
      li.draggable = !readOnly;
      li.dataset.index = String(idx);

      li.innerHTML = `
        <span class="drag" title="Drag to reorder" style="cursor:${readOnly?'default':'grab'}">⋮⋮</span>
        <span class="rank">${idx+1}.</span>
        <span class="code">${item.code}</span>
        <input class="label" type="text" placeholder="Optional label…" value="${item.label || ''}" ${readOnly?'disabled':''}/>
        <button class="remove" type="button" title="Remove" ${readOnly?'disabled style="display:none"':''}>✕</button>
      `;

      if (!readOnly) {
        li.querySelector('.remove').onclick = () => {
          rolesState = (rolesState || []).filter(r => r.code !== item.code);
          rolesState.forEach((r,i)=> r.rank = i+1);
          rolesState = normaliseRolesForSave(rolesState);
          window.modalCtx.rolesState = rolesState;
          markDirty();
          renderList(); refreshAddSelect();
        };

        li.querySelector('.label').oninput = (e) => {
          const rec = byCode(item.code);
          if (rec) rec.label = e.target.value;
          window.modalCtx.rolesState = rolesState;
          // dirty state handled by global tracker
        };

        // Drag payload
        li.addEventListener('dragstart', (e) => {
          const from = li.dataset.index || String(idx);
          try { e.dataTransfer.setData('text/x-role-index', from); } catch {}
          try { e.dataTransfer.setData('text/plain', from); } catch {}
          if (e.dataTransfer) e.dataTransfer.effectAllowed = 'move';
          li.classList.add('dragging');
        });
        li.addEventListener('dragend', () => {
          li.classList.remove('dragging');
          ul.querySelectorAll('.over').forEach(n => n.classList.remove('over'));
        });
      }

      ul.appendChild(li);
    });
  }

  // Delegate DnD only when editable
  if (!readOnly) {
    ul.addEventListener('dragover', (e) => {
      e.preventDefault();
      const overLi = e.target && e.target.closest('li.role-item');
      ul.querySelectorAll('.over').forEach(n => n.classList.remove('over'));
      if (overLi) overLi.classList.add('over');
      if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
    });

    ul.addEventListener('drop', (e) => {
      e.preventDefault();
      const toLi = e.target && e.target.closest('li.role-item');
      if (!toLi) return;

      let from = NaN;
      try { from = parseInt(e.dataTransfer.getData('text/x-role-index'), 10); } catch {}
      if (isNaN(from)) {
        try { from = parseInt(e.dataTransfer.getData('text/plain'), 10); } catch {}
      }
      const to = parseInt(toLi.dataset.index, 10);
      if (!Number.isInteger(from) || !Number.isInteger(to) || from === to) return;

      // Reorder against current rank-sorted view
      const view = (rolesState||[]).slice().sort((a,b)=> (a.rank||0) - (b.rank||0));
      const [moved] = view.splice(from, 1);
      view.splice(to, 0, moved);

      view.forEach((r,i)=> r.rank = i+1);
      rolesState = normaliseRolesForSave(view);
      window.modalCtx.rolesState = rolesState;

      markDirty();
      renderList();
      refreshAddSelect();
    });
  }

  // Add role (only in edit/create)
  if (!readOnly && btn) {
    btn.onclick = () => {
      const code = sel.value;
      if (!code) return;
      if ((rolesState||[]).some(r => r.code === code)) return; // no duplicates
      const nextRank = ((rolesState||[]).length || 0) + 1;
      rolesState = [...(rolesState||[]), { code, rank: nextRank }];
      rolesState = normaliseRolesForSave(rolesState);
      window.modalCtx.rolesState = rolesState;
      markDirty();
      renderList(); refreshAddSelect();
    };
  }

  // Expose a tiny API for refreshing options live
  container.__rolesEditor = {
    updateOptions(newOptions){
      roleOptions = Array.isArray(newOptions) ? newOptions.slice() : [];
      refreshAddSelect();
    }
  };

  // Initial paint
  refreshAddSelect();
  renderList();
}


// Drop dups (by code), sort by rank, rewrite rank 1..N
function normaliseRolesForSave(roles){
  const out = [];
  const seen = new Set();
  (Array.isArray(roles) ? roles : []).forEach(r => {
    const code = String(r.code || '').trim();
    if (!code) return;
    if (seen.has(code)) return;
    seen.add(code);
    out.push({ code, rank: Number(r.rank) || 0, label: r.label ? String(r.label) : undefined });
  });
  out.sort((a,b)=> a.rank - b.rank);
  out.forEach((r,i)=> r.rank = i+1);
  return out;
}

async function listClientHospitals(clientId){
  if (!clientId) return [];
  const url = API(`/api/clients/${clientId}/hospitals`);
  const APILOG = (typeof window !== 'undefined' && !!window.__LOG_API) || (typeof __LOG_API !== 'undefined' && !!__LOG_API);
  if (APILOG) console.log('[listClientHospitals] → GET', url);
  const r = await authFetch(url);
  if (APILOG) console.log('[listClientHospitals] ←', r.status, r.ok);
  const list = await toList(r);
  if (APILOG) console.log('[listClientHospitals] parsed length', Array.isArray(list) ? list.length : -1);
  return list;
}



function formatRolesSummary(roles){
  if (!Array.isArray(roles) || !roles.length) return '';
  const sorted = roles.slice().sort((a,b)=> (a.rank||0)-(b.rank||0));
  return sorted.map(r => `${ordinal(r.rank)} ${r.code}`).join(', ');
}

function ordinal(n){
  const s = ["th","st","nd","rd"], v = n % 100;
  return n + (s[(v-20)%10] || s[v] || s[0]);
}

// Basic clients list for dropdowns (id + name)
async function listClientsBasic(){
  const r = await authFetch(API('/api/clients'));
  const rows = await r.json().catch(()=>({items:[]}));
  const list = Array.isArray(rows?.items) ? rows.items : (Array.isArray(rows) ? rows : []);
  return list.map(x => ({ id: x.id, name: x.name })).filter(x => x.id && x.name);
}

// ===== UK date helpers & lightweight picker =====

function formatIsoToUk(iso){ // 'YYYY-MM-DD' -> 'DD/MM/YYYY'
  if (!iso || typeof iso !== 'string') return '';
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!m) return '';
  return `${m[3]}/${m[2]}/${m[1]}`;
}

function parseUkDateToIso(ddmmyyyy){ // 'DD/MM/YYYY' -> 'YYYY-MM-DD' or null
  if (!ddmmyyyy || typeof ddmmyyyy !== 'string') return null;
  const m = ddmmyyyy.trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return null;
  const d = parseInt(m[1],10), mo=parseInt(m[2],10), y=parseInt(m[3],10);
  if (mo<1||mo>12||d<1||d>31||y<1900||y>3000) return null;
  const dt = new Date(Date.UTC(y, mo-1, d));
  if (dt.getUTCFullYear()!==y || (dt.getUTCMonth()+1)!==mo || dt.getUTCDate()!==d) return null; // invalid date like 31/02
  const mm = String(mo).padStart(2,'0'), dd = String(d).padStart(2,'0');
  return `${y}-${mm}-${dd}`;
}

// Minimal calendar that sits above modals; ESC / outside closes; keyboard nav supported
function attachUkDatePicker(inputEl, opts) {
  if (!inputEl) return;
  inputEl.setAttribute('autocomplete','off');

  const setIso = (v) => {
    if (!v) return null;
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(String(v))) {
      try { return parseUkDateToIso(v) || null; } catch { return null; }
    }
    return String(v);
  };

  // allow dynamic (re)configuration
  if (opts && typeof opts === 'object') {
    if ('minDate' in opts) inputEl._minIso = setIso(opts.minDate);
    if ('maxDate' in opts) inputEl._maxIso = setIso(opts.maxDate);
  }

  // if already wired, just update constraints and return
  if (inputEl.__ukdpBound) {
    if (inputEl.__ukdpPortal) {
      // refresh current view with new bounds
      try { inputEl.__ukdpRepaint && inputEl.__ukdpRepaint(); } catch {}
    }
    return;
  }

  // Bounds helpers (YYYY-MM-DD or null)
  const getMinIso = () => inputEl._minIso || null;
  const getMaxIso = () => inputEl._maxIso || null;

  let portal = null;
  let current = null;

  function openPicker(){
    closePicker();

    let today = new Date();
    if (inputEl.value) {
      const iso = parseUkDateToIso(inputEl.value);
      if (iso) {
        const [y,m,d] = iso.split('-').map(Number);
        today = new Date(Date.UTC(y, m-1, d));
      }
    }
    current = { year: today.getUTCFullYear(), month: today.getUTCMonth() }; // 0-based

    portal = document.createElement('div');
    inputEl.__ukdpPortal = portal;
    portal.className = 'uk-datepicker-portal';
    portal.style.position = 'fixed';
    portal.style.zIndex = '99999';
    portal.style.background = '#fff';
    portal.style.border = '1px solid #ccc';
    portal.style.borderRadius = '8px';
    portal.style.boxShadow = '0 8px 24px rgba(0,0,0,0.15)';
    portal.style.padding = '8px';

    positionPortal();

    portal.innerHTML = renderCalendarHtml(current.year, current.month, inputEl.value);
    document.body.appendChild(portal);

    portal.addEventListener('click', onPortalClick);
    window.addEventListener('resize', positionPortal);
    document.addEventListener('keydown', onKeyDown, true);
    setTimeout(()=> document.addEventListener('click', onOutside, true), 0);
  }

  function positionPortal(){
    if (!portal) return;
    const r = inputEl.getBoundingClientRect();
    portal.style.left = `${Math.max(8, r.left)}px`;
    portal.style.top  = `${Math.max(8, r.top + window.scrollY + r.height + 6)}px`;
  }

  function closePicker(){
    if (!portal) return;
    document.removeEventListener('click', onOutside, true);
    document.removeEventListener('keydown', onKeyDown, true);
    window.removeEventListener('resize', positionPortal);
    portal.removeEventListener('click', onPortalClick);
    portal.remove();
    portal = null;
    inputEl.__ukdpPortal = null;
  }

  function onOutside(e){
    if (portal && !portal.contains(e.target) && e.target !== inputEl) closePicker();
  }

  function onKeyDown(e){
    if (!portal) return;
    if (e.key === 'Escape') { e.preventDefault(); closePicker(); return; }
  }

  function onPortalClick(e){
    const t = e.target;
    if (t.matches('.nav-prev')) { e.preventDefault(); navMonth(-1); return; }
    if (t.matches('.nav-next')) { e.preventDefault(); navMonth(+1); return; }
    const dayBtn = t.closest('button.day');
    if (dayBtn && !dayBtn.disabled) {
      const y = Number(dayBtn.dataset.y), m = Number(dayBtn.dataset.m), d = Number(dayBtn.dataset.d);
      const iso = `${y}-${String(m+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
      inputEl.value = formatIsoToUk(iso);
      closePicker();
      inputEl.dispatchEvent(new Event('change'));
    }
  }

  function dateAllowed(iso){
    const minIso = getMinIso(), maxIso = getMaxIso();
    if (minIso && iso < minIso) return false;
    if (maxIso && iso > maxIso) return false;
    return true;
  }

  function navMonth(delta){
    current.month += delta;
    if (current.month < 0) { current.month = 11; current.year--; }
    if (current.month > 11){ current.month = 0;  current.year++; }
    portal.innerHTML = renderCalendarHtml(current.year, current.month, inputEl.value);
  }

  function renderCalendarHtml(year, month0, selectedUk){
    const selectedIso = parseUkDateToIso(selectedUk || '') || '';
    const sel = selectedIso ? selectedIso.split('-').map(Number) : null;

    const first = new Date(Date.UTC(year, month0, 1));
    const startDow = first.getUTCDay(); // 0=Sun
    const daysInMonth = new Date(Date.UTC(year, month0+1, 0)).getUTCDate();
    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

    let grid = '<div class="cal-head" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">' +
      `<a href="#" class="nav-prev" aria-label="Previous month">‹</a>` +
      `<div class="title" style="font-weight:600">${monthNames[month0]} ${year}</div>` +
      `<a href="#" class="nav-next" aria-label="Next month">›</a>` +
      '</div>';

    grid += `<div class="cal-grid" style="display:grid;grid-template-columns:repeat(7,2em);gap:2px;justify-items:center;align-items:center">`;
    ['Su','Mo','Tu','We','Th','Fr','Sa'].forEach(d => grid += `<div style="font-size:12px;color:#666">${d}</div>`);

    // leading blanks
    for (let i=0;i<startDow;i++) grid += `<div></div>`;
    for (let d=1; d<=daysInMonth; d++){
      const iso = `${year}-${String(month0+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
      const isSel = sel && sel[0]===year && (sel[1]-1)===month0 && sel[2]===d;
      const allowed = dateAllowed(iso);
      grid += `<button type="button" class="day${isSel?' selected':''}" data-y="${year}" data-m="${month0}" data-d="${d}"` +
              ` style="width:2em;height:2em;border:1px solid #ddd;border-radius:4px;${allowed?'background:#fff':'background:#f4f4f4;color:#aaa'}" ${allowed?'':'disabled'}>${d}</button>`;
    }
    grid += `</div>`;
    return grid;
  }

   // public dynamic API for constraints refresh
  inputEl.setMinDate = (minDate) => {
    inputEl._minIso = setIso(minDate);
    if (inputEl.__ukdpPortal) { try { inputEl.__ukdpRepaint && inputEl.__ukdpRepaint(); } catch {} }
  };

  inputEl.setMaxDate = (maxDate) => {
    inputEl._maxIso = setIso(maxDate);
    if (inputEl.__ukdpPortal) { try { inputEl.__ukdpRepaint && inputEl.__ukdpRepaint(); } catch {} }
  };

  inputEl.addEventListener('focus', openPicker);

  inputEl.addEventListener('click', openPicker);

  inputEl.__ukdpBound = true;
  inputEl.__ukdpRepaint = ()=>{ if (portal && current) portal.innerHTML = renderCalendarHtml(current.year, current.month, inputEl.value); };
}


function headersFromRows(rows){
  if (!rows.length) return [];
  const keys = new Set(Object.keys(rows[0]));
  rows.forEach(r=> Object.keys(r).forEach(k=> keys.add(k)));
  return [...keys];
}



/* ===== UK date/time formatter helpers (display only) =====
   - Date-only 'YYYY-MM-DD' -> 'DD/MM/YYYY'
   - UTC timestamps -> 'DD/MM/YYYY HHMMhrs' (Europe/London, no seconds)
*/
function formatUkDate(isoDateStr){
  if (!isoDateStr || typeof isoDateStr !== 'string') return isoDateStr;
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(isoDateStr);
  if (!m) return isoDateStr;
  const [,y,mo,d] = m;
  return `${d}/${mo}/${y}`;
}

// ============================================================================
// NEW HELPERS
// ============================================================================

// Toggle enable/disable via backend endpoint
async function patchClientDefault(id, { disabled }) {
  const url = API(`/api/rates/client-defaults/${encodeURIComponent(id)}`);
  const res = await authFetch(url, {
    method: 'PATCH',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ disabled: !!disabled })
  });
  if (!res.ok) {
    const text = await res.text().catch(()=> '');
    throw new Error(text || `PATCH failed (${res.status})`);
  }
  const data = await res.json().catch(()=> ({}));
  // Endpoint returns { rate: {...} } or the row; support both
  const row = (data && (data.rate || data)) || null;
  return row;
}

// Format a user reference gracefully
function formatUserRef(u) {
  if (!u) return '';
  const s = String(u);
  if (s.includes('@')) return s;           // email
  if (s.length > 8) return s.slice(0,8)+'…';
  return s;
}

// Trivial HTML escape
function escapeHtml(x) {
  return String(x || '')
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'",'&#39;');
}
function formatUkTimestampFromUtc(isoLike){
  if (!isoLike) return isoLike;
  const dt = new Date(isoLike); // parse ISO / ISO-like (+00:00, with ms) to JS Date
  if (isNaN(dt.getTime())) return isoLike;
  const fmt = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year:'numeric', month:'2-digit', day:'2-digit',
    hour:'2-digit', minute:'2-digit', hour12:false
  });
  const parts = fmt.formatToParts(dt);
  const g = (type)=> (parts.find(p=>p.type===type)?.value || '');
  const dd = g('day'), mm = g('month'), yyyy = g('year');
  const hh = g('hour'), mi = g('minute');
  return `${dd}/${mm}/${yyyy} ${hh}${mi}hrs`;
}
function formatDisplayValue(key, val){
  if (val === null || val === undefined || val === '') return '—';
  if (typeof val === 'boolean') return val ? 'Yes' : 'No';

  if (typeof val === 'string'){
    // date-only?
    if (/^\d{4}-\d{2}-\d{2}$/.test(val)) return formatUkDate(val);
    // ISO-ish timestamp?
    if (/\d{4}-\d{2}-\d{2}T/.test(val))  return formatUkTimestampFromUtc(val);
  }

  // Heuristic by column name
  if (typeof key === 'string' && (
      key.endsWith('_at_utc') ||
      key === 'created_at' || key === 'updated_at' ||
      key.endsWith('_timestamp') || key.endsWith('_utc')
    )){
    return formatUkTimestampFromUtc(val);
  }

  return String(val);
}

// === UPDATED: Summary renders role summary for candidates (computed from JSON) ===


function renderAuditTable(content, rows){
  const cols = defaultColumnsFor('audit');
  const tbl = document.createElement('table'); tbl.className='grid';
  const thead = document.createElement('thead'); const trh=document.createElement('tr');
  cols.forEach(c=>{ const th=document.createElement('th'); th.textContent=c; trh.appendChild(th); });
  thead.appendChild(trh); tbl.appendChild(thead);
  const tb = document.createElement('tbody');
  rows.forEach(r=>{
    const tr=document.createElement('tr'); tr.ondblclick=()=> openAuditItem(r);
    cols.forEach(c=>{
      const td=document.createElement('td');
      let v=r[c];
      if (c==='status') td.innerHTML = `<span class="pill ${v==='SENT'?'tag-ok':v==='FAILED'?'tag-fail':'tag-warn'}">${v}</span>`;
      else td.textContent = formatDisplayValue(c, v);
      tr.appendChild(td);
    }); tb.appendChild(tr);
  }); tbl.appendChild(tb); content.appendChild(tbl);
}

// Column manager
const globalColsBtn = byId('btnColumns');
if (globalColsBtn) {
  globalColsBtn.onclick = () => {
    if (!currentSection || !Array.isArray(currentRows)) return;
    openColumnsDialog(currentSection);
  };
}


// ===== Data fetchers =====
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: listCandidates — supports { q, page, page_size } + best-effort server search
// Falls back to list+local filter if /api/search/candidates is unavailable.
// ─────────────────────────────────────────────────────────────────────────────
async function listCandidates(opts = {}) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;
  const q = (opts.q || '').trim();
  window.__listState = window.__listState || {};
  const st = (window.__listState['candidates'] ||= { page: 1, pageSize: 50, total: null, hasMore: false, filters: null });
  const page = Number(opts.page || st.page || 1);
  const ps   = String(opts.page_size || st.pageSize || 50);

  // Helper: choose a search strategy based on q
  const buildCandidateQS = (qText) => {
    const qs = new URLSearchParams();
    if (ps !== 'ALL') { qs.set('page', String(page)); qs.set('page_size', String(ps)); } else { qs.set('page','1'); }
    if (!qText) return qs;

    if (qText.includes('@'))        qs.set('email', qText);
    else if (qText.replace(/\D/g,'').length >= 7) qs.set('phone', qText);
    else if (qText.includes(' ')) {
      const parts = qText.split(/\s+/).filter(Boolean);
      const fn = parts.shift() || qText; const ln = parts.join(' ');
      qs.set('first_name', fn); if (ln) qs.set('last_name', ln);
    } else                          qs.set('first_name', qText);

    return qs;
  };

  // Try server-side search first when q is present
  if (q) {
    const qs = buildCandidateQS(q);
    const url = `/api/search/candidates?${qs.toString()}`;
    try {
      if (LOGC) console.log('[PICKER][candidates] server-search →', url);
      const r = await authFetch(API(url));
      if (r.ok) {
        const rows = await toList(r);
        if (LOGC) console.log('[PICKER][candidates] server-search OK', { count: rows.length });
        if (ps !== 'ALL') st.hasMore = Array.isArray(rows) && rows.length === Number(ps || 50);
        return rows;
      }
    } catch (e) {
      if (LOGC) console.warn('[PICKER][candidates] server-search failed, falling back', e);
    }
  }

  // Fallback: plain list, then optional local filter
  const qs = new URLSearchParams();
  if (ps !== 'ALL') { qs.set('page', String(page)); qs.set('page_size', String(ps)); } else { qs.set('page','1'); }
  const url = qs.toString() ? `/api/candidates?${qs}` : '/api/candidates';
  const r = await authFetch(API(url));
  const rows = await toList(r);
  if (ps !== 'ALL') st.hasMore = Array.isArray(rows) && rows.length === Number(ps || 50);

  if (!q) return rows;

  // Local best-match ranking when server search isn’t available
  const norm = (s) => (s||'').toString().toLowerCase();
  const toks = q.toLowerCase().split(/\s+/).filter(Boolean);
  const score = (row) => {
    const first = norm(row.first_name), last = norm(row.last_name);
    const disp  = norm(row.display_name || `${row.first_name||''} ${row.last_name||''}`);
    const email = norm(row.email), ref = norm(row.tms_ref);
    let s = 0;
    toks.forEach(t => {
      if (first.startsWith(t)) s += 6;
      if (last.startsWith(t))  s += 6;
      if (disp.startsWith(t))  s += 4;
      if (first === t || last === t) s += 8;
      if (disp.includes(t))    s += 2;
      if (email.includes(t))   s += 1;
      if (ref.includes(t))     s += 1;
    });
    if (toks.length >= 2) {
      // bonus if tokens cover first+last in any order
      const set = new Set(toks);
      if (set.has(first) && set.has(last)) s += 4;
    }
    return s;
  };
  const ranked = rows
    .map(r => ({ r, s: score(r) }))
    .filter(x => x.s > 0)
    .sort((a,b)=> b.s - a.s || String(a.r.display_name||'').localeCompare(String(b.r.display_name||'')))
    .map(x => x.r);

  if (LOGC) console.log('[PICKER][candidates] local-filter', { q, in: rows.length, out: ranked.length });
  return ranked;
}

// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: listClients — supports { q, page, page_size } with /api/search/clients?q=…
// Falls back to list when q is empty.
// ─────────────────────────────────────────────────────────────────────────────
async function listClients(opts = {}) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;
  const q = (opts.q || '').trim();
  window.__listState = window.__listState || {};
  const st = (window.__listState['clients'] ||= { page: 1, pageSize: 50, total: null, hasMore: false, filters: null });
  const page = Number(opts.page || st.page || 1);
  const ps   = String(opts.page_size || st.pageSize || 50);

  if (q) {
    const qs = new URLSearchParams();
    if (ps !== 'ALL') { qs.set('page', String(page)); qs.set('page_size', String(ps)); } else { qs.set('page','1'); }
    qs.set('q', q);
    const url = `/api/search/clients?${qs.toString()}`;
    try {
      if (LOGC) console.log('[PICKER][clients] server-search →', url);
      const r = await authFetch(API(url));
      if (r.ok) {
        const rows = await toList(r);
        if (LOGC) console.log('[PICKER][clients] server-search OK', { count: rows.length });
        if (ps !== 'ALL') st.hasMore = Array.isArray(rows) && rows.length === Number(ps || 50);
        return rows;
      }
    } catch (e) {
      if (LOGC) console.warn('[PICKER][clients] server-search failed, falling back', e);
    }
  }

  const qs = new URLSearchParams();
  if (ps !== 'ALL') { qs.set('page', String(page)); qs.set('page_size', String(ps)); } else { qs.set('page','1'); }
  const url = qs.toString() ? `/api/clients?${qs}` : '/api/clients';
  const r = await authFetch(API(url));
  const rows = await toList(r);
  if (ps !== 'ALL') st.hasMore = Array.isArray(rows) && rows.length === Number(ps || 50);

  // local filter when q present but search route unavailable
  if (!q) return rows;
  const qn = q.toLowerCase();
  const filtered = rows.filter(x => (x.name || '').toLowerCase().includes(qn) || (x.primary_invoice_email||'').toLowerCase().includes(qn));
  if (LOGC) console.log('[PICKER][clients] local-filter', { q, in: rows.length, out: filtered.length });
  return filtered;
}


async function listUmbrellas(){
  window.__listState = window.__listState || {};
  const st = (window.__listState['umbrellas'] ||= { page: 1, pageSize: 50, total: null, hasMore: false, filters: null });
  const ps = st.pageSize, pg = st.page;
  const qs = new URLSearchParams();
  if (ps !== 'ALL') { qs.set('page', String(pg || 1)); qs.set('page_size', String(ps || 50)); }
  else { qs.set('page', '1'); }
  const url = qs.toString() ? `/api/umbrellas?${qs}` : '/api/umbrellas';
  const r = await authFetch(API(url));
  const rows = toList(r);
  if (ps !== 'ALL') st.hasMore = Array.isArray(rows) && rows.length === Number(ps || 50);
  return rows;
}

async function listOutbox(){    const r = await authFetch(API('/api/email/outbox')); return toList(r); }
// ===================== API WRAPPERS (UPDATED) =====================

// GET /api/rates/client-defaults with optional filters:
//   clientId (path param), opts: { rate_type, role, band, active_on }
// Note: charge is shared; rate_type filter is optional and used by UI lists.
// ✅ UPDATED — unified FE model; grouping raw per-type rows into unified windows
//    Returned shape: [{ client_id, role, band|null, date_from, date_to|null,
//                       charge_day..bh, paye_day..bh, umb_day..bh }]
// ============================================================================
// LIST CLIENT RATES (adds only_enabled support)
// ============================================================================

async function listClientRates(clientId, opts = {}) {
  const APILOG = (typeof window !== 'undefined' && !!window.__LOG_API) || (typeof __LOG_API !== 'undefined' && !!__LOG_API);

  // 🔧 Guard: never fetch without a client_id (prevents global empty lists clobbering staged state)
  if (!clientId) {
    if (APILOG) console.warn('[listClientRates] called without clientId — returning [] to avoid clobbering state');
    return [];
  }

  const qp = new URLSearchParams();
  qp.set('client_id', clientId);
  if (opts.role) qp.set('role', String(opts.role));
  if (opts.band !== undefined && opts.band !== null && `${opts.band}` !== '') {
    qp.set('band', String(opts.band));
  }
  if (opts.active_on) qp.set('active_on', String(opts.active_on)); // YYYY-MM-DD
  if (opts.only_enabled) qp.set('only_enabled', 'true');

  const qs = `?${qp.toString()}`;
  const url = API(`/api/rates/client-defaults${qs}`);
  if (APILOG) console.log('[listClientRates] → GET', url);

  const res = await authFetch(url);
  if (APILOG) console.log('[listClientRates] ←', res.status, res.ok);
  const rows = await toList(res);
  if (APILOG) console.log('[listClientRates] parsed length', Array.isArray(rows) ? rows.length : -1);
  return Array.isArray(rows) ? rows : [];
}

// POST /api/rates/client-defaults — requires rate_type = 'PAYE' | 'UMBRELLA'

// POST /api/rates/candidate-overrides — requires rate_type
async function addCandidateRate(payload) {
  const rt = String(payload?.rate_type || '').toUpperCase();
  if (rt !== 'PAYE' && rt !== 'UMBRELLA') throw new Error("rate_type must be 'PAYE' or 'UMBRELLA'");
  const r = await authFetch(API('/api/rates/candidate-overrides'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...payload, rate_type: rt })
  });
  return r.ok;
}
// ✅ UPDATED — FE accepts a unified window payload; bridges to current per-type backend
//    Posts TWO rows: PAYE (pay from paye_*), UMBRELLA (pay from umb_*), both with same charge set
async function upsertClientRate(payload) {
  if (!payload || !payload.client_id || !payload.role || !payload.date_from) {
    throw new Error('upsertClientRate: client_id, role and date_from are required');
  }
  const APILOG = (typeof window !== 'undefined' && !!window.__LOG_API) || (typeof __LOG_API !== 'undefined' && !!__LOG_API);

  const body = {
    client_id : String(payload.client_id),
    role      : String(payload.role),
    band      : payload.band ?? null,
    date_from : payload.date_from,
    date_to   : payload.date_to ?? null,

    charge_day   : payload.charge_day   ?? null,
    charge_night : payload.charge_night ?? null,
    charge_sat   : payload.charge_sat   ?? null,
    charge_sun   : payload.charge_sun   ?? null,
    charge_bh    : payload.charge_bh    ?? null,

    paye_day     : payload.paye_day     ?? null,
    paye_night   : payload.paye_night   ?? null,
    paye_sat     : payload.paye_sat     ?? null,
    paye_sun     : payload.paye_sun     ?? null,
    paye_bh      : payload.paye_bh      ?? null,

    umb_day      : payload.umb_day      ?? null,
    umb_night    : payload.umb_night    ?? null,
    umb_sat      : payload.umb_sat      ?? null,
    umb_sun      : payload.umb_sun      ?? null,
    umb_bh       : payload.umb_bh       ?? null
  };

  if (APILOG) console.log('[upsertClientRate] → POST /api/rates/client-defaults', body);
  const res = await authFetch(
    API(`/api/rates/client-defaults`),
    { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }
  );
  if (APILOG) console.log('[upsertClientRate] ←', res.status, res.ok);
  if (!res.ok) {
    const msg = await res.text().catch(() => 'Failed to upsert client default window');
    if (APILOG) console.error('[upsertClientRate] error body', msg);
    throw new Error(msg);
  }
  const json = await res.json().catch(() => ({}));
  if (APILOG) console.log('[upsertClientRate] parsed', json);
  return json;
}


// Small helper: cached settings (for ERNI%)
// Falls back to 0 if missing; returns { erni_pct: number (e.g. 0.15), ... }
let __SETTINGS_CACHE__ = null;
async function getSettingsCached() {
  if (__SETTINGS_CACHE__) return __SETTINGS_CACHE__;
  try {
    const s = await getSettings();
    __SETTINGS_CACHE__ = s || {};
  } catch {
    __SETTINGS_CACHE__ = {};
  }
  return __SETTINGS_CACHE__;
}

// Small helper: pick best shared charge row for {client_id, role, band, active_on}
// Uses backend’s selection logic via filters; no rate_type filter here (shared).
// ✅ UPDATED — works against unified rows returned by listClientRates()
//    Picks exact band first, then band-null; newest start date wins
async function findBestChargeFor({ client_id, role, band, active_on }) {
  // Fetch ONLY enabled windows for this client/role/date
  const rows = await listClientRates(client_id, { role, band: undefined, active_on, only_enabled: true });
  if (!Array.isArray(rows) || !rows.length) return null;

  const ranked = rows
    // extra guard: ignore any disabled rows that might slip through
    .filter(r =>
      !r.disabled_at_utc &&
      r.role === role &&
      r.date_from &&
      r.date_from <= (active_on || '9999-12-31') &&
      (!r.date_to || r.date_to >= (active_on || '0000-01-01'))
    )
    .sort((a,b) => {
      const aExact = (String(a.band||'') === String(band||''));
      const bExact = (String(b.band||'') === String(band||''));
      if (aExact !== bExact) return aExact ? -1 : 1;          // exact band before band-null
      // newer start first
      return (a.date_from < b.date_from) ? 1 : (a.date_from > b.date_from ? -1 : 0);
    });

  const best = ranked[0];
  return best ? {
    charge_day  : best.charge_day  ?? null,
    charge_night: best.charge_night?? null,
    charge_sat  : best.charge_sat  ?? null,
    charge_sun  : best.charge_sun  ?? null,
    charge_bh   : best.charge_bh   ?? null
  } : null;
}


// Small helper: per-bucket margin
function calcMargin(charge, pay, rate_type, erni_pct = 0) {
  if (charge == null || pay == null) return null;
  const rt = String(rate_type || '').toUpperCase();
  if (rt === 'PAYE') {
    const factor = 1 + (Number.isFinite(erni_pct) ? erni_pct : 0);
    return +(charge - (pay * factor)).toFixed(2);
  }
  return +(charge - pay).toFixed(2);
}


// Helper: basic date overlap check for YYYY-MM-DD strings (null = open-ended)
function rangesOverlap(a_from, a_to, b_from, b_to) {
  const A0 = a_from || '0000-01-01';
  const A1 = a_to   || '9999-12-31';
  const B0 = b_from || '0000-01-01';
  const B1 = b_to   || '9999-12-31';
  return !(A1 < B0 || B1 < A0);
}

// ====================== listCandidateRates (unchanged API) ======================
async function listCandidateRates(candidate_id){
  const r = await authFetch(API(`/api/rates/candidate-overrides?candidate_id=${encodeURIComponent(candidate_id)}`));
  return toList(r);
}
// =========================== fetchRelated (unchanged API) ===========================
async function fetchRelated(entity, id, type){
  const url = API(`/api/related/${entity}/${id}/${type}`);
  let res;
  try {
    res = await authFetch(url);
  } catch (err) {
    console.error('fetchRelated network error:', { url, error: err });
    throw err;
  }

  if (!res.ok) {
    const text = await res.text().catch(()=> '');
    console.error('fetchRelated failed:', { status: res.status, url, server: text });
    throw new Error(`Request failed: ${res.status}`);
  }
  return toList(res);
}


// Settings (singleton)
async function getSettings(){
  const r = await authFetch(API('/api/settings/defaults'));
  if (!r.ok) throw new Error('Fetch settings failed');
  const j = await r.json();
  return j?.settings || j || {};   // <-- unwrap {settings: {...}}
}

async function saveSettings(payload){
  const r = await authFetch(API('/api/settings/defaults'), {
    method:'PUT',
    headers:{'content-type':'application/json'},
    body: JSON.stringify(payload)
  });
  if (!r.ok) throw new Error('Save failed');
  return true;
}

// Related counts (object map: {type: count, ...})
async function fetchRelatedCounts(entity, id){
  const r = await authFetch(API(`/api/related/${entity}/${id}/counts`));
  if (!r.ok) return {};
  return r.json();
}
// ===== UPDATED: upsertCandidate — normalize server response so we always return the created/updated object with an id
async function upsertCandidate(payload, id){
  if ('tms_ref' in payload) delete payload.tms_ref; // safety

  const url = id ? `/api/candidates/${id}` : '/api/candidates';
  const method = id ? 'PUT' : 'POST';

  let res;
  try {
    res = await authFetch(API(url), {
      method,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (err) {
    console.error('Candidate save request/network error:', { url, method, payload, error: err });
    throw err;
  }

  const text = await res.text().catch(() => '');
  if (!res.ok) {
    console.error('Candidate save failed:', { status: res.status, url, method, payload, server: text });
    throw new Error(text || `Save failed (${res.status})`);
  }

  try {
    const data = text ? JSON.parse(text) : null;
    // normalize common shapes: [row], { candidate: {...} }, or { ...row }
    let obj = null;
    if (Array.isArray(data)) obj = data[0] || null;
    else if (data && data.candidate) obj = data.candidate;
    else if (data && typeof data === 'object') obj = data;

    return obj || (id ? { id, ...payload } : {});
  } catch (e) {
    console.warn('Candidate save: non-JSON response body', { body: text });
    return id ? { id, ...payload } : {};
  }
}

// ===== UPDATED: upsertClient — normalize server response so we always return the created/updated object with an id

async function upsertClient(payload, id){
  // Never allow CLI to be sent from UI
  if ('cli_ref' in payload) delete payload.cli_ref;

  // Strip empty-string fields so we don't overwrite existing values with ''
  const clean = {};
  for (const [k, v] of Object.entries(payload || {})) {
    if (v === '' || v === undefined) continue;
    clean[k] = v;
  }

  const url    = id ? `/api/clients/${id}` : '/api/clients';
  const method = id ? 'PUT' : 'POST';
  const APILOG = (typeof window !== 'undefined' && !!window.__LOG_API) || (typeof __LOG_API !== 'undefined' && !!__LOG_API);
  if (APILOG) console.log('[upsertClient] →', { method, url: API(url), body: clean });

  const r = await authFetch(API(url), {
    method,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(clean)
  });

  if (APILOG) console.log('[upsertClient] ←', r.status, r.ok);
  if (!r.ok) {
    const msg = await r.text().catch(()=> '');
    if (APILOG) console.error('[upsertClient] error body', msg);
    throw new Error(msg || 'Save failed');
  }

  // Try to parse a full client row from the response
  try {
    const data = await r.json();
    if (APILOG) console.log('[upsertClient] parsed', data);
    let obj = null;
    if (Array.isArray(data)) obj = data[0] || null;
    else if (data && data.client) obj = data.client;
    else if (data && typeof data === 'object') obj = data;
    if (obj) return obj;
  } catch (_) { /* fall through to Location/GET fallback */ }

  // Fallbacks: try to extract id from Location, then GET the full row (to get cli_ref/name/etc.)
  let clientId = id || null;
  try {
    const loc = r.headers && r.headers.get('Location');
    if (!clientId && loc) {
      const m = loc.match(/\/api\/clients\/([^/?#]+)/i) || loc.match(/\/clients\/([^/?#]+)/i);
      if (m) clientId = m[1];
    }
  } catch (_) {}

  if (clientId) {
    try {
      const rr = await authFetch(API(`/api/clients/${encodeURIComponent(clientId)}`));
      if (rr.ok) {
        const dd = await rr.json().catch(()=> ({}));
        const obj = (dd && dd.client) ? dd.client : dd;
        if (obj && typeof obj === 'object') {
          if (APILOG) console.log('[upsertClient] GET backfill', obj);
          return obj;
        }
      }
    } catch (_) {}
  }

  // Last resort: return what we know
  const fallback = clientId ? { id: clientId, ...clean } : (id ? { id, ...clean } : { ...clean });
  if (APILOG) console.log('[upsertClient] fallback', fallback);
  return fallback;
}

// ================== FRONTEND: upsertUmbrella (UPDATED to return saved object) ==================
// ===== UPDATED: upsertUmbrella — normalize server response so we always return the created/updated object with an id
async function upsertUmbrella(payload, id){
  const url = id ? `/api/umbrellas/${id}` : '/api/umbrellas';
  const method = id ? 'PUT' : 'POST';
  const r = await authFetch(API(url), {method, headers:{'content-type':'application/json'}, body: JSON.stringify(payload)});
  if (!r.ok) { 
    const msg = await r.text().catch(()=> 'Save failed');
    throw new Error(msg || 'Save failed'); 
  }
  try {
    const data = await r.json();
    // normalize shapes: [row], { umbrella: {...} }, or { ...row }
    let obj = null;
    if (Array.isArray(data)) obj = data[0] || null;
    else if (data && data.umbrella) obj = data.umbrella;
    else if (data && typeof data === 'object') obj = data;

    return obj || (id ? { id, ...payload } : { ...payload });
  } catch (_) {
    return id ? { id, ...payload } : { ...payload };
  }
}


async function deleteCandidateRatesFor(candidate_id){
  const r = await authFetch(API(`/api/rates/candidate-overrides/${candidate_id}`), {method:'DELETE'}); return r.ok;
}



// ===== Details Modals =====
let modalCtx = { entity:null, data:null };


// ✅ CHANGED: make this async, always hydrate fresh from server before opening
function openDetails(rowOrId){
  if (!confirmDiscardChangesIfDirty()) return;

  let row = rowOrId;
  let id  = null;

  if (!row || typeof row !== 'object') {
    id = String(rowOrId || '');
    row = currentRows.find(x => String(x.id) === id) || null;
  } else {
    id = String(row.id || '');
  }

  if (!id) { alert('Record id not provided'); return; }

  if (currentSection === 'contracts') {
    (async () => {
      let fresh = null;
      try {
        fresh = await getContract(id);
      } catch (e) {
        console.debug('[OPEN] getContract failed, falling back to cached row', e);
      }

      const effective = fresh || row;
      if (!effective) { alert('Record not found'); return; }

      currentSelection = effective;
      console.debug('[OPEN] openDetails', { section: currentSection, id: effective.id });

      openContract(effective);
    })();
    return;
  }

  if (!row) { alert('Record not found'); return; }

  currentSelection = row;
  console.debug('[OPEN] openDetails', { section: currentSection, id: row.id });

  if (currentSection === 'candidates')      openCandidate(row);
  else if (currentSection === 'clients')    openClient(row);
  else if (currentSection === 'umbrellas')  openUmbrella(row);
  else if (currentSection === 'audit')      openAuditItem(row);
  else if (currentSection === 'contracts')  openContract(row);
}


function openCreate(){
  if (!confirmDiscardChangesIfDirty()) return;
  if (currentSection==='candidates') openCandidate({});
  else if (currentSection==='clients') openClient({});
  else if (currentSection==='umbrellas') openUmbrella({});
  else if (currentSection==='contracts') openContract({}); // new route
  else alert('Create not supported for this section yet.');
}

function openEdit(){
  if (!confirmDiscardChangesIfDirty()) return;
  if (!currentSelection) return alert('Select by double-clicking a row first.');
  openDetails(currentSelection);
}

async function openDelete(){
  const inModal = (window.__modalStack?.length || 0) > 0;
  const row = inModal ? (modalCtx?.data || null) : currentSelection;

  if (!row) return alert('Select a record (or open it) first.');
  if (!confirm('Delete (or disable) this record?')) return;

  console.debug('[DELETE] request', {
    section: inModal ? modalCtx?.entity : currentSection,
    inModal, id: row.id
  });

  const section = inModal ? modalCtx?.entity : currentSection;

  if (section === 'candidates'){
    await upsertCandidate({ ...row, active:false }, row.id);
  } else if (section === 'umbrellas'){
    await upsertUmbrella({ ...row, enabled:false }, row.id);
  } else if (section === 'contracts'){
    alert('Delete is not available for Contracts here. Open the contract and use the dedicated action if supported.'); // guarded, non-breaking
    return;
  } else {
    alert('Delete is not available for this section.');
    return;
  }

  if (inModal) {
    // Ensure no lingering state after delete
    discardAllModalsAndState();
  }
  await renderAll();
}

// ---- Candidate modal
// === UPDATED: Candidate open modal (mount roles editor; include roles on save) ===
// === UPDATED: openCandidate — save uses full persisted state + current tab values ===

// ========================= openCandidate (FIXED) =========================

// =================== CANDIDATE MODAL (unchanged save; ensures pay_method present) ===================
// ✅ UPDATED — staged candidate overrides model (apply vs save)
//    Parent Save commits staged deletes → edits → creates

// FRONTEND — UPDATED
// openCandidate: default Account holder from umbrella name if pay_method is UMBRELLA and empty.
// ================== FRONTEND: openCandidate (UPDATED) ==================
// ================== FIXED: openCandidate (hydrate before showModal) ==================
// ================== FIXED: openCandidate (hydrate before showModal) ==================
// ================== FIXED: openCandidate (hydrate before showModal) ==================


async function openCandidate(row) {
  // ===== Logging helpers (toggle with window.__LOG_MODAL = true/false) =====
  const LOG = (typeof window.__LOG_MODAL === 'boolean') ? window.__LOG_MODAL : false;
  const L  = (...a)=> { if (LOG) console.log('[OPEN_CANDIDATE]', ...a); };
  const W  = (...a)=> { if (LOG) console.warn('[OPEN_CANDIDATE]', ...a); };
  const E  = (...a)=> { if (LOG) console.error('[OPEN_CANDIDATE]', ...a); };

  const deep = (o)=> JSON.parse(JSON.stringify(o || {}));
  const incoming = deep(row || {});
  const seedId   = incoming?.id || null;

  L('ENTRY', { incomingKeys: Object.keys(incoming||{}), seedId });

  // helper to unwrap a single record from many common backend shapes
  const unwrapSingle = (data, key) => {
    if (Array.isArray(data)) return data[0] || null;
    if (data && key && data[key]) return unwrapSingle(data[key], null);
    if (data && Array.isArray(data.rows))  return data.rows[0]  || null;
    if (data && Array.isArray(data.items)) return data.items[0] || null;
    if (data && Array.isArray(data.data))  return data.data[0]  || null;
    return (data && typeof data === 'object') ? data : null;
  };

  // 1) Hydrate full record if we have an id
  let full = incoming;
  if (seedId) {
    try {
      const url = API(`/api/candidates/${encodeURIComponent(seedId)}`);
      L('[HTTP] GET', url);
      const res = await authFetch(url);
      L('[HTTP] status', res?.status, res?.ok);

      try {
        const raw = await res.clone().text();
        if (LOG) console.debug('[HTTP] raw body (≤2KB):', raw.slice(0, 2048));
      } catch (peekErr) { W('[HTTP] raw peek failed', peekErr?.message || peekErr); }

      if (res.ok) {
        const data = await res.json().catch((jErr)=>{ W('res.json() failed, using {}', jErr); return {}; });
        const candidate = data.candidate || unwrapSingle(data, 'candidate');
        const job_titles = Array.isArray(data.job_titles) ? data.job_titles : [];
        L('hydrated JSON keys', Object.keys(data||{}), 'candidate keys', Object.keys(candidate||{}));
        full = candidate ? { ...candidate, job_titles } : incoming;
      } else {
        W('non-OK response, using incoming row');
      }
    } catch (e) {
      W('hydrate failed; using summary row', e);
    }
  } else {
    L('no seedId — create mode');
  }

  // 2) Build modal context from hydrated data — ***IMPORTANT: seed window.modalCtx***
  const fullKeys = Object.keys(full || {});
  L('seeding window.modalCtx', { entity: 'candidates', fullId: full?.id, fullKeys });

  window.modalCtx = {
    entity: 'candidates',
    data:   deep(full),
    formState: { __forId: full?.id || null, main: {}, pay: {} },
    rolesState: Array.isArray(full?.roles) ? normaliseRolesForSave(full.roles) : [],
    overrides: { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() },
    clientSettingsState: null,
    openToken: ((full?.id) || 'new') + ':' + Date.now()
  };

  L('window.modalCtx seeded', {
    entity: window.modalCtx.entity,
    dataId: window.modalCtx.data?.id,
    dataKeys: Object.keys(window.modalCtx.data||{}),
    formStateForId: window.modalCtx.formState?.__forId,
    openToken: window.modalCtx.openToken
  });

  // 3) Render modal
  L('calling showModal with hasId=', !!full?.id, 'rawHasIdArg=', full?.id);
  showModal(
    'Candidate',
    [
      { key:'main',     label:'Main Details' },
      { key:'rates',    label:'Care Packages' },  // renamed tab
      { key:'pay',      label:'Payment details' },
      { key:'bookings', label:'Bookings' }
    ],
    (k, r) => {
      L('[renderCandidateTab] tab=', k, 'rowKeys=', Object.keys(r||{}), 'sample=', { first: r?.first_name, last: r?.last_name, id: r?.id });
      return renderCandidateTab(k, r);
    },
    async () => {
      L('[onSave] begin', { dataId: window.modalCtx?.data?.id, forId: window.modalCtx?.formState?.__forId });
      const isNew = !window.modalCtx?.data?.id;

      const fs   = window.modalCtx.formState || { __forId: null, main:{}, pay:{} };
      const hasId = !!window.modalCtx.data?.id;
      const same = hasId ? (fs.__forId === window.modalCtx.data.id)
                         : (fs.__forId === window.modalCtx.openToken || fs.__forId == null);
      const stateMain = same ? (fs.main || {}) : {};
      const statePay  = same ? (fs.pay  || {}) : {};
      const main      = document.querySelector('#tab-main') ? collectForm('#tab-main') : {};
      const pay       = document.querySelector('#tab-pay')  ? collectForm('#tab-pay')  : {};
      const roles     = normaliseRolesForSave(window.modalCtx.rolesState || window.modalCtx.data?.roles || []);
      const payload   = { ...stateMain, ...statePay, ...main, ...pay, roles };

      L('[onSave] collected', {
        same, stateMainKeys: Object.keys(stateMain||{}), statePayKeys: Object.keys(statePay||{}),
        mainKeys: Object.keys(main||{}), payKeys: Object.keys(pay||{}), rolesCount: roles?.length || 0
      });

      delete payload.umbrella_name;
      delete payload.tms_ref;

      if (!payload.first_name && full?.first_name) payload.first_name = full.first_name;
      if (!payload.last_name  && full?.last_name)  payload.last_name  = full.last_name;
      if (typeof payload.key_norm === 'undefined' && typeof full?.key_norm !== 'undefined') payload.key_norm = full.key_norm;

      if (!payload.display_name) {
        const dn = [payload.first_name, payload.last_name].filter(Boolean).join(' ').trim();
        payload.display_name = dn || full?.display_name || null;
      }

      if (!payload.pay_method) payload.pay_method = isNew ? 'PAYE' : (full?.pay_method || 'PAYE');

      if (payload.pay_method === 'UMBRELLA') {
        if ((!payload.umbrella_id || payload.umbrella_id === '') && full?.umbrella_id) {
          payload.umbrella_id = full.umbrella_id;
        }
        if (!payload.account_holder) {
          const umbNameEl = document.querySelector('#tab-pay #umbrella_name');
          if (umbNameEl && umbNameEl.value) payload.account_holder = umbNameEl.value;
        }
      }

      if (isNew && !payload.first_name && !payload.last_name) { alert('Enter at least a first or last name.'); return { ok:false }; }
      if (payload.pay_method === 'PAYE') payload.umbrella_id = null;
      else if (!payload.umbrella_id || payload.umbrella_id === '') { alert('Select an umbrella company for UMBRELLA pay.'); return { ok:false }; }
      if (payload.umbrella_id === '') payload.umbrella_id = null;

      for (const k of Object.keys(payload)) if (payload[k] === '') delete payload[k];

      // Sync Job Titles + Registration + DOB from candidateMainModel
         // Sync Job Titles + Registration + DOB from candidateMainModel
      try {
        const cm = window.modalCtx?.candidateMainModel;
        if (cm && typeof cm === 'object') {
          // Normalise + order job titles so primary comes first
          let jobs = Array.isArray(cm.job_titles) ? cm.job_titles.slice() : [];
          // drop any empty entries
          jobs = jobs.filter(j => j && j.job_title_id);
          if (jobs.length) {
            let primaryIdx = jobs.findIndex(j => j.is_primary);
            if (primaryIdx === -1) {
              // if no explicit primary, make the first one primary
              primaryIdx = 0;
            }
            // enforce exactly one primary at index 0
            jobs = jobs.map((j, idx) => ({
              ...j,
              is_primary: idx === primaryIdx
            }));
            if (primaryIdx !== 0) {
              const primary = jobs[primaryIdx];
              jobs.splice(primaryIdx, 1);
              jobs.unshift(primary);
            }
          }

          // keep the normalised list (with is_primary) on the model
          cm.job_titles = jobs;

          // derive the ordered list of IDs for the payload
          const jobIds = jobs.map(j => j.job_title_id).filter(Boolean);
          payload.job_titles = jobIds;
          payload.job_title_id = jobIds.length ? jobIds[0] : null;

          if (Object.prototype.hasOwnProperty.call(cm, 'prof_reg_type')) {
            payload.prof_reg_type = cm.prof_reg_type || null;
          }
          if (Object.prototype.hasOwnProperty.call(cm, 'prof_reg_number')) {
            payload.prof_reg_number = cm.prof_reg_number || '';
          }
          if (Object.prototype.hasOwnProperty.call(cm, 'date_of_birth')) {
            payload.date_of_birth = cm.date_of_birth || null;
          }
        }
      } catch (err) {
        W('sync from candidateMainModel failed', err);
      }


      const idForUpdate = window.modalCtx?.data?.id || full?.id || null;
      const tokenAtSave = window.modalCtx.openToken;
      L('[onSave] upsertCandidate', { idForUpdate, payloadKeys: Object.keys(payload||{}) });
      const saved = await upsertCandidate(payload, idForUpdate).catch(err => { E('upsertCandidate failed', err); return null; });
      const candidateId = idForUpdate || (saved && saved.id);
      L('[onSave] saved', { ok: !!saved, candidateId, savedKeys: Array.isArray(saved)?[]:Object.keys(saved||{}) });
      if (!candidateId) { alert('Failed to save candidate'); return { ok:false }; }

      // ===== Validate staged overrides (unchanged logic) =====
      const O = window.modalCtx.overrides || { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() };

      async function getCoveringDefault(client_id, role, band, date_from) {
        try {
          if (!client_id || !role || !date_from) return null;
          const list = await listClientRates(client_id, { active_on: date_from, only_enabled: true });
          const rows = Array.isArray(list) ? list.filter(w => !w.disabled_at_utc && String(w.role) === String(role)) : [];
          let win = rows.find(w => (w.band ?? null) === (band ?? null));
          if (!win && (band == null)) win = rows.find(w => w.band == null);
          return win || null;
        } catch { return null; }
      }
      const bucketLabel = { day:'Day', night:'Night', sat:'Sat', sun:'Sun', bh:'BH' };
      const erniMult = await (async ()=>{ if (typeof window.__ERNI_MULT__ === 'number') return window.__ERNI_MULT__; try { if (typeof getSettingsCached === 'function') { const s = await getSettingsCached(); let p = s?.erni_pct ?? s?.employers_ni_percent ?? 0; p = Number(p)||0; if (p>1) p=p/100; window.__ERNI_MULT__ = 1 + p; return window.__ERNI_MULT__; } } catch{} window.__ERNI_MULT__ = 1; return 1; })();

      // Validate EDITS
      for (const [editId, patchRaw] of Object.entries(O.stagedEdits || {})) {
        const original = (O.existing || []).find(x => String(x.id) === String(editId));
        if (!original) { alert('Cannot locate original override to validate'); return { ok:false }; }

        const eff = {
          client_id: patchRaw.client_id ?? original.client_id,
          role     : patchRaw.role      ?? original.role,
          band     : (patchRaw.hasOwnProperty('band') ? patchRaw.band : original.band),
          date_from: patchRaw.date_from ?? original.date_from,
          date_to  : patchRaw.hasOwnProperty('date_to') ? patchRaw.date_to : original.date_to,
          rate_type: (patchRaw.rate_type ?? original.rate_type ?? '').toUpperCase(),

          pay_day  : patchRaw.hasOwnProperty('pay_day')   ? patchRaw.pay_day   : original.pay_day,
          pay_night: patchRaw.hasOwnProperty('pay_night') ? patchRaw.pay_night : original.pay_night,
          pay_sat  : patchRaw.hasOwnProperty('pay_sat')   ? patchRaw.pay_sat   : original.pay_sat,
          pay_sun  : patchRaw.hasOwnProperty('pay_sun')   ? patchRaw.pay_sun   : original.pay_sun,
          pay_bh   : patchRaw.hasOwnProperty('pay_bh')    ? patchRaw.pay_bh    : original.pay_bh
        };

        const win = await getCoveringDefault(eff.client_id, eff.role, eff.band, eff.date_from);
        if (!win) { alert(`No active client default covers ${eff.role}${eff.band?` / ${eff.band}`:''} on ${formatIsoToUk(eff.date_from)}.`); return { ok:false }; }
        if (eff.date_to && win.date_to && eff.date_to > win.date_to) { alert(`Client rate ends on ${formatIsoToUk(win.date_to)} — override must end on or before this date.`); return { ok:false }; }

        for (const b of ['day','night','sat','sun','bh']) {
          const payB = eff[`pay_${b}`];
          const chg = win[`charge_${b}`];
          if (payB != null && chg == null) { alert(`No client charge for ${bucketLabel[b]} on ${formatIsoToUk(eff.date_from)}.`); return { ok:false }; }
          if (payB != null && chg != null) {
            const margin = (eff.rate_type==='PAYE') ? (chg - (payB * erniMult)) : (chg - payB);
            if (margin < 0) { alert(`Margin would be negative for ${bucketLabel[b]}.`); return { ok:false }; }
          }
        }
      }

      // Validate NEW rows
      for (const nv of (O.stagedNew || [])) {
        const win = await getCoveringDefault(nv.client_id, nv.role, nv.band ?? null, nv.date_from);
        if (!win) { alert(`No active client default covers ${nv.role}${nv.band?` / ${nv.band}`:''} on ${formatIsoToUk(nv.date_from)}.`); return { ok:false }; }
        if (nv.date_to && win.date_to && nv.date_to > win.date_to) { alert(`Client rate ends on ${formatIsoToUk(win.date_to)} — override must end on or before this date.`); return { ok:false }; }
        for (const b of ['day','night','sat','sun','bh']) {
          const payB = nv[`pay_${b}`]; const chg = win[`charge_${b}`];
          if (payB != null && chg == null) { alert(`No client charge for ${bucketLabel[b]} on ${formatIsoToUk(win.date_from)}.`); return { ok:false }; }
          if (payB != null && chg != null) {
            const margin = (String(nv.rate_type).toUpperCase()==='PAYE') ? (chg - (payB * erniMult)) : (chg - payB);
            if (margin < 0) { alert(`Margin would be negative for ${bucketLabel[b]}.`); return { ok:false }; }
          }
        }
      }

      // ===== Persist staged overrides (DELETE uses routed path with candidate_id) =====
      const overridesRef = window.modalCtx.overrides || { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() };
      L('[onSave] overrides', {
        deletes: Array.from(overridesRef.stagedDeletes || []),
        edits: Object.keys(overridesRef.stagedEdits || {}),
        newCount: (overridesRef.stagedNew || []).length
      });

      // Deletes — preferred by id; fallback to legacy filter keys
      for (const delId of overridesRef.stagedDeletes || []) {
        const rowDel = (overridesRef.existing || []).find(r => String(r.id) === String(delId));
        if (!rowDel) continue;

        const q = new URLSearchParams();
        if (rowDel.id) q.set('id', String(rowDel.id));
        else {
          if (rowDel.client_id) q.set('client_id', String(rowDel.client_id));
          if (rowDel.role != null) q.set('role', String(rowDel.role));
          q.set('band', (rowDel.band == null || rowDel.band === '') ? '' : String(rowDel.band));
          if (rowDel.rate_type) q.set('rate_type', String(rowDel.rate_type).toUpperCase());
          if (rowDel.date_from) q.set('date_from', String(rowDel.date_from));
        }

        const urlDel = API(`/api/rates/candidate-overrides/${encodeURIComponent(candidateId)}?${q.toString()}`);
        L('[onSave][DELETE override]', urlDel);
        const resDel = await authFetch(urlDel, { method: 'DELETE' });
        if (!resDel.ok) {
          const msg = await resDel.text().catch(()=> 'Delete override failed');
          alert(msg);
          return { ok:false }; }
      }

      // Edits — PATCH candidate_id in path + ORIGINAL keys in query, updates in body
      for (const [editId, patchRaw] of Object.entries(overridesRef.stagedEdits || {})) {
        const original = (overridesRef.existing || []).find(x => String(x.id) === String(editId));
        if (!original) { alert('Cannot locate original override to patch'); return { ok:false }; }

        const q = new URLSearchParams();
        if (original.client_id) q.set('client_id', original.client_id);
        if (original.role != null) q.set('role', String(original.role));
        q.set('band', (original.band == null || original.band === '') ? '' : String(original.band));
        if (original.rate_type) q.set('rate_type', String(original.rate_type).toUpperCase());

        const body = {};
        for (const [k,v] of Object.entries(patchRaw || {})) {
          if (v === '' || v === undefined) continue;
          body[k] = v;
        }
        body.candidate_id = candidateId;

        const urlPatch = API(`/api/rates/candidate-overrides/${encodeURIComponent(candidateId)}?${q.toString()}`);
        L('[onSave][PATCH override]', { url: urlPatch, body });
        const resPatch = await authFetch(urlPatch, {
          method:'PATCH',
          headers:{ 'content-type':'application/json' },
          body: JSON.stringify(body)
        });
        if (!resPatch.ok) {
          const msg = await resPatch.text().catch(()=> 'Update override failed');
          alert(msg);
          return { ok:false }; }
      }

      // Creates
      for (const nv of (overridesRef.stagedNew || [])) {
        if (!nv.client_id) { alert('Override must include client_id'); return { ok:false }; }
        const clean = {};
        for (const [k,v] of Object.entries(nv)) {
          if (k === '_tmpId' || v === '') continue;
          clean[k] = v;
        }
        const resCreate = await authFetch(
          API(`/api/rates/candidate-overrides`),
          { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ ...clean, candidate_id: candidateId }) }
        );
        if (!resCreate.ok) { const msg = await resCreate.text().catch(()=> 'Create override failed'); alert(msg); return { ok:false }; }
      }

      // Refresh overrides list from server and clear staging
      try {
        const latest = await listCandidateRates(candidateId);
        if (tokenAtSave === window.modalCtx.openToken && window.modalCtx.data?.id === candidateId) {
          window.modalCtx.overrides.existing = Array.isArray(latest) ? latest : [];
          window.modalCtx.overrides.stagedEdits = {};
          window.modalCtx.overrides.stagedNew = [];
          if (window.modalCtx.overrides.stagedDeletes?.clear) window.modalCtx.overrides.stagedDeletes.clear();
          const fr1 = window.__getModalFrame?.();
          if (fr1 && fr1.entity === 'candidates' && fr1.currentTabKey === 'rates') {
            await renderCandidateRatesTable();
          }
        }
      } catch (e) {
        W('post-save rates refresh failed', e);
      }

      const mergedRoles = (saved && saved.roles) || payload.roles || window.modalCtx.data?.roles || [];

      // Build a fresh job_titles array for modalCtx.data, based on the staged model
      let jobTitlesForCtx = [];
      try {
        const cm = window.modalCtx?.candidateMainModel;
        if (cm && Array.isArray(cm.job_titles)) {
          // Use the already normalised cm.job_titles with is_primary preserved
          jobTitlesForCtx = cm.job_titles.map((t) => ({
            job_title_id: t.job_title_id,
            is_primary: !!t.is_primary
          }));
        } else if (Array.isArray(payload.job_titles)) {
          jobTitlesForCtx = payload.job_titles.map((id, idx) => ({
            job_title_id: id,
            is_primary: idx === 0
          }));
        }
      } catch (e) {
        W('onSave: building jobTitlesForCtx failed', e);
      }

      window.modalCtx.data = {
        ...(window.modalCtx.data || {}),
        ...(saved || {}),
        id: candidateId,
        roles: mergedRoles,
        job_titles: jobTitlesForCtx
      };
      window.modalCtx.formState  = { __ForId: candidateId, main: {}, pay: {} };
      window.modalCtx.rolesState = mergedRoles;

      L('[onSave] final window.modalCtx', {
        dataId: window.modalCtx.data?.id,
        rolesCount: Array.isArray(window.modalCtx.data?.roles) ? window.modalCtx.data.roles.length : 0,
        formStateForId: window.modalCtx.formState?.__forId
      });

      if (isNew) window.__pendingFocus = { section: 'candidates', id: candidateId };

      return { ok: true, saved: window.modalCtx.data };

    },
    full?.id,
    () => {
      const fr = window.__getModalFrame?.();
      const isBookings = fr && fr.entity === 'candidates' && fr.currentTabKey === 'bookings';
      const candId = window.modalCtx?.data?.id;
      if (isBookings && candId) {
        try { renderCandidateCalendarTab(candId); } catch (e) { W('renderCandidateCalendarTab failed', e); }
      }
    }
  );
  L('showModal returned (sync)', { currentOpenToken: window.modalCtx.openToken });

  // 4) Optional async companion loads
  if (full?.id) {
    const token = window.modalCtx.openToken;
    const id    = full.id;

    try {
      L('[listCandidateRates] GET', { id, token });
      const existing = await listCandidateRates(id);
      L('[listCandidateRates] result', { count: Array.isArray(existing) ? existing.length : -1, sameToken: token === window.modalCtx.openToken, modalCtxId: window.modalCtx.data?.id });
      if (token === window.modalCtx.openToken && window.modalCtx.data?.id === id) {
        window.modalCtx.overrides.existing = Array.isArray(existing) ? existing : [];
        const fr2 = window.__getModalFrame?.();
        if (fr2 && fr2.entity === 'candidates' && fr2.currentTabKey === 'rates') {
          await renderCandidateRatesTable();
        }
      }
    } catch (e) { E('listCandidateRates failed', e); }

    // NOTE: Removed legacy fetchRelated('timesheets') + renderCalendar(ts) block.
    // The new calendar fetches its own data when the Bookings tab is active.
  } else {
    L('skip companion loads (no full.id)');
  }
}

function renderCandidateTab(key, row = {}) {
  if (key === 'main') return html(`
    <div class="form" id="tab-main">
      ${input('first_name','First name', row.first_name)}
      ${input('last_name','Last name', row.last_name)}
      ${input('email','Email', row.email, 'email')}
      ${input('phone','Telephone', row.phone)}
      ${select('pay_method','Pay method', row.pay_method || 'PAYE', ['PAYE','UMBRELLA'], {id:'pay-method'})}

      <!-- CCR: display-only, never posted -->
      <div class="row">
        <label>CloudTMS Candidate Reference (CCR)</label>
        <input id="tms_ref_display"
               value="${row.tms_ref ? String(row.tms_ref) : 'Awaiting CCR number from server'}"
               disabled
               readonly
               style="opacity:.7" />
      </div>

      ${input('display_name','Display name', row.display_name)}

      <!-- New: NI / DOB / Gender -->
      ${input('ni_number','National Insurance Number', row.ni_number)}
      ${input('date_of_birth','Date of birth', row.date_of_birth)}
      ${select('gender','Gender', row.gender || '', ['', 'Male', 'Female', 'Other'])}

      <!-- New: Job Titles (multi, with bins) -->
      <div class="row">
        <label>Job Titles</label>
        <div class="controls">
          <div id="jobTitlesList"
               style="display:flex;flex-wrap:wrap;gap:4px;min-height:24px;align-items:flex-start;"></div>
          <button type="button"
                  class="btn mini"
                  data-act="pick-job-title">
            Add Job Title…
          </button>
          <div class="hint">
            Right click a Job Title in Edit mode to select a Primary Job Role.
          </div>
        </div>
      </div>

      <!-- Professional registration number (NMC/GMC/HCPC) -->
      <div class="row"
           data-block="prof_reg"
           style="${row.prof_reg_type ? '' : 'display:none'}">
        <label data-field="prof_reg_label">
          ${row.prof_reg_type
            ? escapeHtml(`${row.prof_reg_type} Number`)
            : 'Registration Number'}
        </label>
        <div class="controls">
          <input class="input"
                 name="prof_reg_number"
                 value="${escapeHtml(row.prof_reg_number || '')}">
        </div>
      </div>

      <!-- Home address + postcode lookup -->
      <div class="row">
        <label>Home address</label>
        <div class="controls">
          <div class="grid-2">
            <input class="input"
                   name="address_line1"
                   placeholder="Address line 1"
                   value="${escapeHtml(row.address_line1 || '')}">
            <input class="input"
                   name="address_line2"
                   placeholder="Address line 2"
                   value="${escapeHtml(row.address_line2 || '')}">
            <input class="input"
                   name="address_line3"
                   placeholder="Address line 3"
                   value="${escapeHtml(row.address_line3 || '')}">
            <input class="input"
                   name="town_city"
                   placeholder="City / Town"
                   value="${escapeHtml(row.town_city || '')}">
            <input class="input"
                   name="county"
                   placeholder="County"
                   value="${escapeHtml(row.county || '')}">
            <div class="split">
              <input class="input"
                     name="postcode"
                     placeholder="Postcode"
                     value="${escapeHtml(row.postcode || '')}">
              <button type="button"
                      class="btn mini"
                      data-act="postcode-lookup"
                      title="Lookup by postcode">
                Lookup
              </button>
            </div>
          </div>
        </div>
      </div>

      <div class="row">
        <label>Notes</label>
        <textarea name="notes" placeholder="Free text…">${row.notes || ''}</textarea>
      </div>
    </div>
  `);

  // Care Packages tab (was "Rates")
  if (key === 'rates') return html(`
    <div class="form" id="tab-rates">
      <!-- Global Candidate Key (GCK) -->
      <div class="row">
        <label>Global Candidate Key (GCK)</label>
        <div class="controls">
          <input class="input"
                 name="key_norm"
                 value="${escapeHtml(row.key_norm || '')}"
                 placeholder="" />
          <div class="hint">
            This links the candidate record to the Google Sheets Rota.
          </div>
        </div>
      </div>

      <!-- Rota Roles editor -->
      <div class="row">
        <label>Rota Roles</label>
        <div id="rolesEditor" data-init="1"></div>
        <div class="hint">
          This links the candidate job role to a Care Package rota only.
          If this candidate is not working on Care Packages, you can ignore this.
        </div>
      </div>

      <!-- Candidate rate overrides table (unchanged wiring) -->
      <div class="row">
        <label>Care Package Rates</label>
        <div id="ratesTable"></div>
      </div>
    </div>
  `);

  if (key === 'pay') return html(`
    <div class="form" id="tab-pay">
      <div class="row">
        <label class="hint">
          PAYE bank fields are editable. If UMBRELLA is selected, bank details are taken from the umbrella and locked.
        </label>
      </div>

      ${input('account_holder','Account holder', row.account_holder)}
      ${input('bank_name','Bank name', row.bank_name)}
      ${input('sort_code','Sort code', row.sort_code)}
      ${input('account_number','Account number', row.account_number)}

      <!-- Umbrella chooser: text input + datalist + hidden canonical id -->
      <div class="row" id="umbRow">
        <label>Umbrella company</label>
        <input id="umbrella_name"
               list="umbList"
               placeholder="Type to search umbrellas…"
               value=""
               autocomplete="off"
               onclick="if (this.value) { this.dataset.prev=this.value; this.value=''; this.dispatchEvent(new Event('input',{bubbles:true})); }"
               onfocus="if (this.value) { this.dataset.prev=this.value; this.value=''; this.dispatchEvent(new Event('input',{bubbles:true})); }" />
        <datalist id="umbList"></datalist>
        <input type="hidden" name="umbrella_id" id="umbrella_id" value="${row.umbrella_id || ''}"/>
      </div>
    </div>
  `);

  // Candidate Calendar tab container
  if (key === 'bookings') return html(`
    <div id="candidateCalendarHolder" class="tabc">
      <div class="hint">Loading calendar…</div>
    </div>
  `);
}


// ====================== mountCandidatePayTab (FIXED) ======================
// FRONTEND — UPDATED
// mountCandidatePayTab: also keeps Account Holder in sync with umbrella name when UMNRELLA pay.
// ================== FIXED: openUmbrella (hydrate before showModal) ==================

async function renderCandidateRatesTable() {
  const LOG = !!window.__LOG_RATES;

  // Safety net: auto-create #ratesTable if missing
  let div = byId('ratesTable');
  if (!div) {
    const host = byId('modalBody');
    if (host) {
      div = document.createElement('div');
      div.id = 'ratesTable';
      host.appendChild(div);
      if (LOG) console.warn('[RATES][TABLE] created missing #ratesTable');
    } else {
      if (LOG) console.warn('[RATES][TABLE] no #ratesTable and no #modalBody; abort');
      return;
    }
  }

  const frame = _currentFrame();
  const parentEditable = frame && (frame.mode === 'edit' || frame.mode === 'create');
  if (LOG) console.log('[RATES][TABLE] parentEditable?', parentEditable, 'mode:', frame?.mode);

  // Resolve client names
  let clientsById = {};
  try {
    const clients = await listClientsBasic();
    clientsById = Object.fromEntries((clients || []).map(c => [c.id, c.name]));
  } catch (e) { console.error('[RATES][TABLE] load clients failed', e); }

  const O = (window.modalCtx.overrides ||= { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() });

  if (LOG) console.log('[RATES][TABLE snapshot]', {
    existing: (O.existing||[]).length,
    stagedNew: (O.stagedNew||[]).length,
    stagedEdits: Object.keys(O.stagedEdits||{}).length,
    stagedDeletes: (O.stagedDeletes && O.stagedDeletes.size) || 0,
    peekExisting: (O.existing||[])[0],
    peekNew: (O.stagedNew||[])[0]
  });

  // Merge view
  const pendingDeleteIds = (O.stagedDeletes instanceof Set) ? O.stagedDeletes : new Set();

  const rows = [];
  for (const ex of (O.existing || [])) {
    const isPendingDelete = !!(pendingDeleteIds && ex && pendingDeleteIds.has(ex.id));
    rows.push({
      ...ex,
      ...(O.stagedEdits?.[ex.id] || {}),
      _edited: !!O.stagedEdits?.[ex.id],
      _pendingDelete: isPendingDelete
    });
  }
  for (const n of (O.stagedNew || [])) rows.push({ ...n, _isNew: true });

  if (!rows.length) {
    div.innerHTML = `
      <div class="hint" style="margin-bottom:8px">No candidate-specific overrides. Client defaults will apply.</div>
      <div class="actions">
        <button id="btnAddRate" class="btn mini"${parentEditable ? '' : ' disabled'}>
          Add rate override
        </button>
        ${parentEditable
          ? '<span class="hint">Changes are staged. Click “Save” in the main dialog to persist.</span>'
          : '<span class="hint">Read-only. Click “Edit” in the main dialog to add/modify overrides.</span>'}
      </div>
    `;
    const addBtn = byId('btnAddRate');
    if (addBtn && parentEditable) addBtn.onclick = () => openCandidateRateModal(window.modalCtx.data?.id);
    if (LOG) console.log('[RATES][TABLE] rendered empty view');
    return;
  }

  const fmt = v => (v==null || Number.isNaN(v)) ? '—' : (Math.round(v*100)/100).toFixed(2);
  const mult = await (async ()=>{ if (typeof window.__ERNI_MULT__ === 'number') return window.__ERNI_MULT__; try { if (typeof getSettingsCached === 'function') { const s = await getSettingsCached(); let p = s?.erni_pct ?? s?.employers_ni_percent ?? 0; p = Number(p)||0; if (p>1) p=p/100; window.__ERNI_MULT__ = 1 + p; return window.__ERNI_MULT__; } } catch{} window.__ERNI_MULT__ = 1; return 1; })();

  const keyOf = r => [r.client_id, r.role || '', (r.band==null?'':String(r.band)), r.date_from || ''].join('|');
  const uniqueKeys = Array.from(new Set(rows.map(keyOf)));
  const chargeMap = Object.create(null);

  async function loadChargesForKey(key){
    const [client_id, role, bandKey, date_from] = key.split('|');
    const band = (bandKey === '' ? null : bandKey);
    if (!client_id || !role || !date_from) return null;
    try {
      const list = await listClientRates(client_id, { active_on: date_from, only_enabled: true });
      const filtered = Array.isArray(list) ? list.filter(w=>!w.disabled_at_utc && w.role===role) : [];
      let win = filtered.find(w => (w.band ?? null) === (band ?? null));
      if (!win && (band == null)) win = filtered.find(w => w.band == null);
      return win ? {
        day:   win.charge_day   ?? null,
        night: win.charge_night ?? null,
        sat:   win.charge_sat   ?? null,
        sun:   win.charge_sun   ?? null,
        bh:    win.charge_bh    ?? null
      } : null;
    } catch(e){ return null; }
  }
  await Promise.all(uniqueKeys.map(async k => { chargeMap[k] = await loadChargesForKey(k); }));

  // === NEW: check whether a covering client default exists TODAY (Europe/London), per (client,role,band)
  const todayIso = (() => {
    try {
      const s = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', year:'numeric', month:'2-digit', day:'2-digit' }).format(new Date());
      const [dd, mm, yyyy] = s.split('/');
      return `${yyyy}-${mm}-${dd}`;
    } catch { // fallback
      const d = new Date(); const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,'0'), day = String(d.getDate()).padStart(2,'0');
      return `${y}-${m}-${day}`;
    }
  })();
  const todayKeyOf = r => [r.client_id, r.role || '', (r.band==null?'':String(r.band))].join('|');
  const todayKeys = Array.from(new Set(rows.map(todayKeyOf)));
  const coverTodayMap = Object.create(null);
  async function loadCoverTodayForKey(key){
    const [client_id, role, bandKey] = key.split('|');
    const band = (bandKey === '' ? null : bandKey);
    if (!client_id || !role) return false;
    try {
      const list = await listClientRates(client_id, { active_on: todayIso, only_enabled: true });
      const filtered = Array.isArray(list) ? list.filter(w=>!w.disabled_at_utc && w.role===role) : [];
      let win = filtered.find(w => (w.band ?? null) === (band ?? null));
      if (!win && (band == null)) win = filtered.find(w => w.band == null);
      return !!win;
    } catch(e){ return false; }
  }
  await Promise.all(todayKeys.map(async k => { coverTodayMap[k] = await loadCoverTodayForKey(k); }));

  const cols    = ['client','role','band','rate_type','pay_day','pay_night','pay_sat','pay_sun','pay_bh','margin_day','margin_night','margin_sat','margin_sun','margin_bh','date_from','date_to','_state'];
  const headers = ['Client','Role','Band','Type','Pay Day','Pay Night','Pay Sat','Pay Sun','Pay BH','Margin Day','Margin Night','Margin Sat','Margin Sun','Margin BH','From','To','Status'];

  const tbl = document.createElement('table'); tbl.className = 'grid';
  const thead = document.createElement('thead'); const trh = document.createElement('tr');
  headers.forEach(h => { const th=document.createElement('th'); th.textContent=h; trh.appendChild(th); });
  thead.appendChild(trh); tbl.appendChild(thead);

  const tb = document.createElement('tbody');
  rows.forEach(r => {
    const tr = document.createElement('tr');
    if (parentEditable) tr.ondblclick = () => openCandidateRateModal(window.modalCtx.data?.id, r);

    const charges = chargeMap[keyOf(r)] || null;
    const isPAYE = String(r.rate_type || '').toUpperCase() === 'PAYE';

    const margin = {};
    ['day','night','sat','sun','bh'].forEach(b=>{
      const pay = r[`pay_${b}`]; const chg = charges ? charges[b] : null;
      margin[b] = (chg!=null && pay!=null) ? (isPAYE ? (chg - (pay * mult)) : (chg - pay)) : null;
    });

    let status =
      r._pendingDelete ? 'Pending delete (save to confirm)' :
      r._isNew        ? 'Staged (new)' :
      r._edited       ? 'Staged (edited)' : '';

    // Determine stale/orphan: (a) no covering client rate at override start OR (b) no covering client rate today
    const hasCoverAtStart = !!charges;
    const hasCoverToday = !!coverTodayMap[todayKeyOf(r)];
    const isStaleOrphan = (!hasCoverAtStart) || (!hasCoverToday);
    if (isStaleOrphan) status = 'Client rate no longer exists';

    // pay columns to 2dp
    const to2 = (v) => (v==null ? '—' : fmt(Number(v)));

    const pretty = {
      client: clientsById[r.client_id] || '', role: r.role || '', band: r.band ?? '', rate_type: r.rate_type || '',
      pay_day: to2(r.pay_day), pay_night: to2(r.pay_night), pay_sat: to2(r.pay_sat), pay_sun: to2(r.pay_sun), pay_bh: to2(r.pay_bh),
      margin_day: fmt(margin.day), margin_night: fmt(margin.night), margin_sat: fmt(margin.sat), margin_sun: fmt(margin.sun), margin_bh: fmt(margin.bh),
      date_from: formatDisplayValue('date_from', r.date_from), date_to: formatDisplayValue('date_to', r.date_to),
      _state   : status
    };

    cols.forEach(c => { const td=document.createElement('td'); td.textContent=String(pretty[c] ?? ''); tr.appendChild(td); });

    // Shade & disable open if stale/orphan
    if (isStaleOrphan) {
      tr.style.opacity = '.55';
      tr.style.cursor = 'not-allowed';
      tr.ondblclick = null;
    }

    tb.appendChild(tr);
  });
  tbl.appendChild(tb);

  const actions = document.createElement('div');
  actions.className = 'actions';
  actions.innerHTML = `
    <button id="btnAddRate" class="btn mini"${parentEditable ? '' : ' disabled'}>
      Add rate override
    </button>
    ${parentEditable
      ? '<span class="hint">Changes are staged. Click “Save” in the main dialog to persist.</span>'
      : '<span class="hint">Read-only. Click “Edit” in the main dialog to add/modify overrides.</span>'}
  `;

  div.innerHTML = '';
  div.appendChild(tbl);
  div.appendChild(actions);

  const addBtn = byId('btnAddRate');
  if (addBtn && parentEditable) addBtn.onclick = () => openCandidateRateModal(window.modalCtx.data?.id);

  if (LOG) console.log('[RATES][TABLE rendered]', { rows: rows.length, firstState: rows[0]?._state || '(none)' });
}



// Replaces your current function
// =================== renderCandidateRatesTable (FIXED) ===================
// =================== CANDIDATE RATES TABLE (UPDATED) ===================
// ✅ UPDATED — renders from modalCtx.overrides (existing ⊕ staged edits/new ⊖ staged deletes)

// ==================================
// 2) renderCandidateRatesTable(...)
// ==================================
// Now computes margins for each override row by resolving client charges at date_from (memoized per render)



// === UPDATED: Candidate modal tabs (adds Roles editor placeholder on 'main') ===


// === DIRTY NAVIGATION GUARDS (add) ===
function isAnyModalDirty(){
  const st = window.__modalStack || [];
  return st.some(f => f && f.isDirty);
}
// ==================== discardAllModalsAndState (kept with geometry reset) ====================

function discardAllModalsAndState(){
  try {
    if (modalCtx && modalCtx._rolesUpdatedHandler) {
      window.removeEventListener('global-roles-updated', modalCtx._rolesUpdatedHandler);
      modalCtx._rolesUpdatedHandler = undefined;
    }
    if (modalCtx && modalCtx._payMethodChangedHandler) {
      window.removeEventListener('pay-method-changed', modalCtx._payMethodChangedHandler);
      modalCtx._payMethodChangedHandler = undefined;
    }
  } catch (e) {
    console.warn('[MODAL] listener cleanup failed', e);
  }

  // Detach any remaining frame-level listeners (dirty/global) from all frames
  try {
    if (Array.isArray(window.__modalStack)) {
      while (window.__modalStack.length) {
        const fr = window.__modalStack.pop();
        if (fr && fr._detachDirty)  { try { fr._detachDirty();  } catch(_) {} }
        if (fr && fr._detachGlobal) { try { fr._detachGlobal(); } catch(_) {} }
      }
    }
  } catch (_) {}

  // Clear any staged calendar changes for open contracts (defensive sweep)
  try {
    if (window.__calStage && typeof clearContractCalendarStageState === 'function') {
      for (const contractId of Object.keys(window.__calStage)) {
        try { clearContractCalendarStageState(contractId); } catch {}
      }
    }
  } catch (e) {
    console.warn('[MODAL] calendar stage cleanup failed', e);
  }

  // Reset modal geometry to prevent "snap to right" on the next open
  const modal = byId('modal');
  if (modal) {
    modal.style.position = '';
    modal.style.left = '';
    modal.style.top = '';
    modal.style.right = '';
    modal.style.bottom = '';
    modal.style.transform = '';
    modal.classList.remove('dragging');
    // Cancel any document-level drag handlers that might still be live
    document.onmousemove = null;
    document.onmouseup   = null;
  }

  // 🔒 Clear any hidden modal DOM so stale inputs can't be read on next open
  const modalBody = document.getElementById('modalBody');
  if (modalBody) modalBody.replaceChildren();
  const modalTabs = document.getElementById('modalTabs');
  if (modalTabs) modalTabs.replaceChildren();
    const modalTitle = document.getElementById('modalTitle');
  if (modalTitle) modalTitle.textContent = '';

  // Clear any lingering bottom-right hint
  const modalHint = document.getElementById('modalHint');
  if (modalHint) {
    modalHint.textContent = '';
    modalHint.removeAttribute('data-tone');
    try { modalHint.classList.remove('ok', 'warn', 'err'); } catch {}
  }

  // Reset modal context
  modalCtx = {
    entity: null, data: null,
    formState: null, rolesState: null,
    ratesState: null, hospitalsState: null,
    clientSettingsState: null,
    openToken: null
  };

  // Hide overlay last
  const back = document.getElementById('modalBack');
  if (back) back.style.display = 'none';

  console.debug('[MODAL] hard reset complete');
}



function confirmDiscardChangesIfDirty(){
  if (!isAnyModalDirty()) return true; // not dirty → acts as plain "Close" guard
  const ok = window.confirm('You have unsaved changes. Discard them and continue?');
  if (!ok) return false;

  // Sanitize geometry before teardown
  const m = byId('modal');
  if (m) {
    m.style.position = '';
    m.style.left = '';
    m.style.top = '';
    m.style.right = '';
    m.style.bottom = '';
    m.style.transform = '';
    m.classList.remove('dragging');
  }
  document.onmousemove = null;
  document.onmouseup   = null;

  // Discard also clears any staged calendar changes (handled inside)
  discardAllModalsAndState();
  return true;
}


async function commitContractCalendarStageIfPending(contractId) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][commitIfPending]', ...a); };
  const W = (...a)=> { if ( LOG_CAL) console.warn('[CAL][commitIfPending]', ...a); };
  const E = (...a)=> { if ( LOG_CAL) console.error('[CAL][commitIfPending]', ...a); };

  try {
    const st = getContractCalendarStageState(contractId);
    const hasPending =
      !!st &&
      (st.add.size || st.remove.size || Object.keys(st.additional||{}).length || !!st.removeAll);
    if (!hasPending) {
      L('no pending calendar changes');
      return { ok: true, detail: 'no-op', removedAll: false };
    }

    // Build ranges + symmetry metadata
    const {
      addRanges,
      removeRanges,
      additionals,
      removeAll,
      needsLeftExtend,
      leftEdgeDate,
      rightEdgeDate
    } = buildPlanRangesFromStage(contractId);

    L('ranges from stage', { addRanges, removeRanges, additionals, removeAll });

    // Pull current contract & window from modal
    const contract     = (window.modalCtx && window.modalCtx.data) ? window.modalCtx.data : {};
    const contractStart = contract?.start_date   || null;
    const contractEnd   = contract?.end_date     || null;
    const candidateId   = contract?.candidate_id || null;

    // Special case: "remove all unsubmitted weeks" → single bulk unplan then exit.
    if (removeAll) {
      if (removeRanges.length) {
        const payload = {
          when_timesheet_exists: 'skip',
          empty_week_action: 'delete',   // hard delete empty weeks
          ranges: removeRanges           // expect [{ from, to, days: [] }]
        };
        L('DELETE /plan-ranges (removeAll, IfPending)', payload);
        try {
          const resp = await contractsUnplanRanges(contractId, payload);
          L('DELETE /plan-ranges (removeAll, IfPending) ←', resp);
        } catch (err) {
          E('unplan-ranges (removeAll, IfPending) failed', err);
          return { ok: false, message: err?.message || 'Calendar commit failed', removedAll: true };
        }
      } else {
        L('removeAll=true but no removeRanges built (IfPending)');
      }

      // Stage will be completely rebuilt by next view; normalizeContractWindowToShifts
      // will run after this from the caller.
      try { clearContractCalendarStageState(contractId); } catch {}
      L('calendar commit ok (removeAll, IfPending)');
      return { ok: true, detail: 'calendar saved', removedAll: true };
    }

    // Optional preflight overlap check when extending left
    if (needsLeftExtend && candidateId) {
      const newStart = leftEdgeDate && contractStart ? (leftEdgeDate < contractStart ? leftEdgeDate : contractStart)
                                                     : (leftEdgeDate || contractStart);
      const newEnd   = rightEdgeDate && contractEnd ? (rightEdgeDate > contractEnd ? rightEdgeDate : contractEnd)
                                                    : (rightEdgeDate || contractEnd || newStart);

      const payload = {
        candidate_id:       candidateId,
        start_date:         newStart,
        end_date:           newEnd,
        ignore_contract_id: contractId
      };

      L('preflight overlap check', payload);
      const overlapRes = await authFetch(API('/api/contracts/check-overlap'), {
        method: 'POST',
        headers: { 'Content-Type':'application/json' },
        body: JSON.stringify(payload)
      });

      // authFetch returns JSON
      if (!overlapRes || overlapRes.error) {
        const msg = overlapRes?.error || 'Overlap check failed';
        console.warn('[CAL][commitIfPending] overlap preflight failed', msg);
        return { ok: false, message: msg, removedAll: false };
      }

      if (overlapRes.has_overlap) {
        const first = Array.isArray(overlapRes.overlaps) && overlapRes.overlaps[0] ? overlapRes.overlaps[0] : null;
        const baseMsg = first
          ? `This extension overlaps ${first.client_name} (${first.role}${first.band ? ' Band ' + first.band : ''}) window ${first.existing_start_date} → ${first.existing_end_date}.`
          : 'This extension overlaps an existing contract window.';
        const proceed = confirm(`${baseMsg}\n\nProceed anyway and save with overlapping windows?`);
        if (!proceed) {
          L('overlap preflight: user cancelled save');
          return { ok: false, message: 'User cancelled due to overlap', removedAll: false, cancelled: true };
        }
        L('overlap preflight: user confirmed proceed');
      }
    }

    // === PLAN (adds) ===
    if (Array.isArray(addRanges) && addRanges.length) {
      const payload = {
        extend_contract_window: true,
        ranges: addRanges
      };
      L('POST /plan-ranges (IfPending)', { ranges: payload.ranges.length, extend_contract_window: true });
      try {
        const resp = await contractsPlanRanges(contractId, payload);
        L('POST /plan-ranges (IfPending) ←', resp);
      } catch (err) {
        E('plan-ranges (IfPending) failed', err);
        return { ok: false, message: err?.message || 'Calendar commit failed', removedAll: false };
      }
    } else {
      L('No addRanges to commit (IfPending)');
    }

    // === UNPLAN (removals) ===
    if (Array.isArray(removeRanges) && removeRanges.length) {
      const payload = {
        when_timesheet_exists: 'skip',
        empty_week_action: 'cancel',   // clear weeks + mark CANCELLED
        ranges: removeRanges
      };
      L('DELETE /plan-ranges (IfPending)', { ranges: payload.ranges.length });
      try {
        const resp = await contractsUnplanRanges(contractId, payload);
        L('DELETE /plan-ranges (IfPending) ←', resp);
      } catch (err) {
        E('unplan-ranges (IfPending) failed', err);
        return { ok: false, message: err?.message || 'Calendar commit failed', removedAll: false };
      }
    } else {
      L('No removeRanges to commit (IfPending)');
    }

    // === ADDITIONALS (split weeks) ===
    if (Array.isArray(additionals) && additionals.length) {
      L('Committing additional weeks… (IfPending)', { count: additionals.length });
      for (const g of additionals) {
        try {
          L('Create additional for baseWeekId (IfPending)', g.baseWeekId, 'dates=', g.dates);
          const addRow = await contractWeekCreateAdditional(g.baseWeekId);
          L('additional created (IfPending) ←', addRow);
          const payload = { add: g.dates.map(d => ({ date: d })), merge: 'append' };
          L('PATCH /contract-weeks/:id/plan (IfPending)', { week_id: addRow.id, payload });
          const resp = await contractWeekPlanPatch(addRow.id, payload);
          L('PATCH /contract-weeks/:id/plan (IfPending) ←', resp);
        } catch (err) {
          E('additional week flow (IfPending) failed', err);
          return { ok: false, message: err?.message || 'Calendar commit failed', removedAll: false };
        }
      }
    } else {
      L('No additionals to commit (IfPending)');
    }

    // Optimistic in-memory window nudge (for immediate UI consistency)
    try {
      if (window.modalCtx && window.modalCtx.data) {
        if (leftEdgeDate && (!window.modalCtx.data.start_date || leftEdgeDate < window.modalCtx.data.start_date)) {
          window.modalCtx.data.start_date = leftEdgeDate;
        }
        if (rightEdgeDate && (!window.modalCtx.data.end_date || rightEdgeDate > window.modalCtx.data.end_date)) {
          window.modalCtx.data.end_date = rightEdgeDate;
        }
      }
    } catch {}

    // Clear staged state after a successful commit
    try { clearContractCalendarStageState(contractId); } catch {}

    L('calendar commit ok (IfPending)');
    return { ok: true, detail: 'calendar saved', removedAll: false };

  } catch (e) {
    console.warn('[CAL][commitIfPending] failed', e);
    return { ok: false, message: e?.message || 'Calendar commit failed', removedAll: false };
  }
}



// Stage a full-window delete of all TS-free weeks (committed on Save).
async function removeAllUnsubmittedWeeks(contractId, bounds) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][removeAllUnsubmittedWeeks]', ...a); };

  const st = getContractCalendarStageState(contractId);

  const rawFrom = bounds?.from || window.modalCtx?.data?.start_date || null;
  const rawTo   = bounds?.to   || window.modalCtx?.data?.end_date   || null;

  const fromIso = (!rawFrom ? null : (rawFrom.includes('/') && typeof parseUkDateToIso === 'function') ? (parseUkDateToIso(rawFrom) || rawFrom) : rawFrom);
  const toIso   = (!rawTo   ? null : (rawTo.includes('/')   && typeof parseUkDateToIso === 'function') ? (parseUkDateToIso(rawTo)   || rawTo)   : rawTo);

  const wew = (window.modalCtx?.data?.week_ending_weekday_snapshot ?? 0);
  const endFrom = computeWeekEnding(fromIso || window.modalCtx?.data?.start_date, wew);
  const startFrom = addDays(endFrom, -6);
  const endTo = computeWeekEnding(toIso || window.modalCtx?.data?.end_date, wew);

  st.removeAll = { from: startFrom, to: endTo };
  st.remove.clear?.();
  st.add.clear?.();
  st.additional = {};

  L('staged removeAll', st.removeAll);
  try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
  return { ok: true, staged: true, bounds: st.removeAll };
}

// After a "remove-all" commit, rebound the contract dates to actuals
// Rule: if any timesheets exist => (start=min_ts, end=max_ts)
//       else => (end = start) leave start as-is.


async function normalizeContractWindowToShifts(contractId) {
  try {
    const r = await authFetch(API(`/api/contracts/${encodeURIComponent(contractId)}`), { method:'GET' });
    const data = await r.json().catch(()=>null);
    const contract = data?.contract || data || {};
    const currStart = contract.start_date || null;
    const currEnd   = contract.end_date   || null;
    if (!currStart || !currEnd) return { ok:false, reason:'no-window' };

    let minTs = null, maxTs = null;
    try {
      const bRes = await authFetch(API(`/api/contracts/check-timesheet-boundary`), {
        method:'POST',
        headers: { 'content-type':'application/json' },
        body: JSON.stringify({ contract_id: contractId, start_date: currStart, end_date: currEnd })
      });
      const b = await bRes.json().catch(()=>null);
      if (b) { minTs = b.min_ts_date || null; maxTs = b.max_ts_date || null; }
    } catch {}

    let newStart = currStart;
    let newEnd   = currEnd;

    if (minTs && maxTs) {
      newStart = minTs;
      newEnd   = maxTs;
    } else {
      try {
        const dRes = await authFetch(API(`/api/contracts/${encodeURIComponent(contractId)}/calendar?from=${encodeURIComponent(currStart)}&to=${encodeURIComponent(currEnd)}&granularity=day`), { method:'GET' });
        const d = await dRes.json().catch(()=>null);
        const items = Array.isArray(d?.items) ? d.items : [];
        const plannedDates = items
          .filter(it => String(it?.state||'').toUpperCase() === 'PLANNED')
          .map(it => it?.date)
          .filter(Boolean)
          .sort();
        if (plannedDates.length) {
          newStart = plannedDates[0];
          newEnd   = plannedDates[plannedDates.length - 1];
        } else {
          newEnd = currStart;
        }
      } catch {
        newEnd = currStart;
      }
    }

    if (newStart !== currStart || newEnd !== currEnd) {
      const payload = { id: contractId, start_date: newStart, end_date: newEnd };
      const saved = await upsertContract(payload, contractId);
      const savedContract = saved?.contract || saved || null;
      if (savedContract) {
        try { window.modalCtx.data = savedContract; } catch {}
        try {
          const fs = (window.modalCtx.formState ||= { __forId: (contractId || null), main:{}, pay:{} });
          fs.main ||= {};
          fs.main.start_date = savedContract.start_date || newStart;
          fs.main.end_date   = savedContract.end_date   || newEnd;
          const fr = window.__getModalFrame?.();
          const currentTab = fr?.currentTabKey || (document.querySelector('#modalTabs button.active')?.textContent?.toLowerCase() || '');
          if (currentTab === 'main' && typeof fr?.setTab === 'function') fr.setTab('main');
        } catch {}
      }
      return { ok:true, start_date: newStart, end_date: newEnd, changed:true };
    }

    return { ok:true, start_date: newStart, end_date: newEnd, changed:false };
  } catch (e) {
    return { ok:false, error: e?.message || String(e) };
  }
}



// ====================== mountCandidateRatesTab (FIXED) ======================
// =================== MOUNT CANDIDATE RATES TAB (unchanged flow) ===================

// ==============================
// 3) mountCandidateRatesTab(...)
// ==============================

// === UPDATED: Candidate Rate Override modal (Client→Role gated; bands; UK dates; date_to) ===
// ====================== openCandidateRateModal (FIXED) ======================
// =================== CANDIDATE OVERRIDE MODAL (UPDATED) ===================
// ==== CHILD MODAL (CANDIDATE RATE) — throw on errors; return true on success ====
// ✅ UPDATED — Apply (stage), gate against client defaults active at date_from,
//    auto-truncate incumbent of same rate_type at N−1 (staged), NO persistence here
async function mountCandidateRatesTab() {
  const LOG = !!window.__LOG_RATES;
  const token = window.modalCtx.openToken;
  const id    = window.modalCtx.data?.id || null;
  if (LOG) console.log('[RATES][mountCandidateRatesTab] ENTRY', { token, id });

  // Ensure a host exists (only on Rates tab; this function is called from showModal setTab('rates'))
  const host = byId('modalBody');
  if (host && !byId('ratesTable')) {
    const c = document.createElement('div');
    c.id = 'ratesTable';
    host.appendChild(c);
    if (LOG) console.log('[RATES][mountCandidateRatesTab] injected #ratesTable host');
  }

  // CREATE flow
  if (!id) {
    window.modalCtx.overrides = window.modalCtx.overrides || { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() };
    await renderCandidateRatesTable();
    if (LOG) console.log('[RATES][mountCandidateRatesTab] create-flow render (no id)');

    const btn = byId('btnAddRate');
    const frame = _currentFrame();
    if (btn && frame && (frame.mode === 'edit' || frame.mode === 'create')) btn.onclick = () => openCandidateRateModal(window.modalCtx.data?.id);
    return;
  }

  // EDIT flow — refresh ONLY existing; preserve stagedNew/Edits/Deletes
  const rates = await listCandidateRates(id);
  if (token !== window.modalCtx.openToken || window.modalCtx.data?.id !== id) {
    if (LOG) console.warn('[RATES][mountCandidateRatesTab] token/id changed mid-flight');
    return;
  }

  const O = window.modalCtx.overrides || (window.modalCtx.overrides = { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() });
  if (Array.isArray(rates)) O.existing = rates.slice();

  await renderCandidateRatesTable();
  if (LOG) console.log('[RATES][mountCandidateRatesTab] renderCandidateRatesTable() called');

  const btn = byId('btnAddRate');
  const frame = _currentFrame();
  if (btn && frame && (frame.mode === 'edit' || frame.mode === 'create')) btn.onclick = () => openCandidateRateModal(window.modalCtx.data?.id);
}

// Mount logic for Contracts → Rates tab (hide/show groups, compute margins, handle preset buttons)

function mountContractRatesTab() {
  const root = byId('contractRatesTab'); if (!root) return;

  const form = document.querySelector('#contractForm');
  const payMethodSel = form?.querySelector('select[name="pay_method_snapshot"], select[name="default_pay_method_snapshot"]');

  // Helper: normalise to 2dp if numeric, leave as-is otherwise
  const normaliseRateInput = (el) => {
    if (!el) return;
    let v = (el.value || '').trim();
    if (!v) return;
    const n = Number(v);
    if (!Number.isFinite(n)) return;
    const fixed = n.toFixed(2);
    if (fixed !== v) {
      el.value = fixed;
    }
  };

  try {
    const fs = (window.modalCtx.formState ||= { __forId:(window.modalCtx.data?.id ?? window.modalCtx.openToken ?? null), main:{}, pay:{} });
    if (!fs.pay || Object.keys(fs.pay).length === 0) {
      const saved = (window.modalCtx.data && window.modalCtx.data.rates_json) || {};
      const buckets = ['paye_day','paye_night','paye_sat','paye_sun','paye_bh','umb_day','umb_night','umb_sat','umb_sun','umb_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh'];
      fs.pay = fs.pay || {};
      for (const k of buckets) {
        const v = saved[k];
        if (v === 0 || (typeof v === 'number' && Number.isFinite(v))) {
          // store as 2dp string
          fs.pay[k] = Number(v).toFixed(2);
        }
      }
    }
  } catch {}

  const toggleCards = () => {
    const pm = (payMethodSel?.value || root.dataset.payMethod || 'PAYE').toUpperCase();
    const cardPAYE = byId('cardPAYE'), cardUMB = byId('cardUMB');
    if (cardPAYE) cardPAYE.style.display = (pm === 'PAYE') ? '' : 'none';
    if (cardUMB)  cardUMB.style.display  = (pm === 'PAYE') ? 'none' : '';
    return pm;
  };

  let payMethod = toggleCards();

  if (payMethodSel && !payMethodSel.__wired_pm) {
    payMethodSel.__wired_pm = true;
    payMethodSel.addEventListener('change', () => {
      payMethod = toggleCards();
      if (typeof computeContractMargins === 'function') computeContractMargins();
    });
  }

  const rateInputs = root.querySelectorAll('input[name^="paye_"], input[name^="umb_"], input[name^="charge_"]');

  rateInputs.forEach(el => {
    if (!el.__wired_mg) {
      el.__wired_mg = true;

      // Live margins on input
      el.addEventListener('input', () => {
        if (typeof computeContractMargins === 'function') computeContractMargins();
      });

      // Snap to 2dp on blur/tab away
      el.addEventListener('blur', () => {
        const before = el.value;
        normaliseRateInput(el);
        if (el.value !== before && typeof setContractFormValue === 'function') {
          setContractFormValue(el.name, el.value);
        } else if (typeof setContractFormValue === 'function') {
          // still stage even if unchanged, to keep formState in sync
          setContractFormValue(el.name, el.value);
        }
        if (typeof computeContractMargins === 'function') computeContractMargins();
      });
    }
  });

  // Mileage inputs: same behaviour (2dp on blur)
  const mileageInputs = root.querySelectorAll('input[name="mileage_pay_rate"], input[name="mileage_charge_rate"]');
  mileageInputs.forEach(el => {
    if (!el.__wired_mg) {
      el.__wired_mg = true;

      el.addEventListener('input', () => {
        // margins may or may not depend on mileage; cheap to recompute anyway
        if (typeof computeContractMargins === 'function') computeContractMargins();
      });

      el.addEventListener('blur', () => {
        const before = el.value;
        normaliseRateInput(el);
        if (typeof setContractFormValue === 'function') {
          setContractFormValue(el.name, el.value);
        }
        if (typeof computeContractMargins === 'function') computeContractMargins();
      });
    }
  });

  // One-time wiring for negative margin hints + bucket label updates
  if (!root.__wiredNeg) {
    root.__wiredNeg = true;
    window.addEventListener('contract-margins-updated', (ev) => {
      const s = ev?.detail || window.__contractMarginState || { hasNegativeMargins:false, negFlags:{} };
      Object.entries(s.negFlags||{}).forEach(([b,neg]) => {
        const row = document.querySelector(`#marginsTable tbody tr[data-b="${b}"]`);
        if (!row) return;
        const mgEl = row.querySelector('.mg');
        if (neg) {
          row.setAttribute('data-negative','1');
          if (mgEl && !mgEl.querySelector('.mini')) {
            const hint = document.createElement('div');
            hint.className='mini';
            hint.textContent='Margin can’t be negative';
            mgEl.appendChild(hint);
          }
        } else {
          row.removeAttribute('data-negative');
          if (mgEl) {
            const hint = mgEl.querySelector('.mini');
            if (hint) hint.remove();
          }
        }
      });
    });
    window.addEventListener('bucket-labels-changed', () => {
      const merged = mergeContractStateIntoRow(window.modalCtx?.data||{});
      const LBL = merged?.bucket_labels_json || (window.modalCtx?.formState?.main ? {
        day:   window.modalCtx.formState.main.bucket_day || window.modalCtx.formState.main.bucket_label_day,
        night: window.modalCtx.formState.main.bucket_night || window.modalCtx.formState.main.bucket_label_night,
        sat:   window.modalCtx.formState.main.bucket_sat || window.modalCtx.formState.main.bucket_label_sat,
        sun:   window.modalCtx.formState.main.bucket_sun || window.modalCtx.formState.main.bucket_label_sun,
        bh:    window.modalCtx.formState.main.bucket_bh || window.modalCtx.formState.main.bucket_label_bh,
      } : {});
      const labelOf = (k, def) => (LBL && LBL[k]) ? LBL[k] : def;
      const map = { day:'Day', night:'Night', sat:'Sat', sun:'Sun', bh:'BH' };
      Object.entries(map).forEach(([k,def])=>{
        const rows = document.querySelectorAll(`#contractRatesTab .row > label.section, #contractRatesTab .row > label`);
        rows.forEach(lab=>{
          const n = lab.textContent?.trim()||'';
          if (n === def) lab.textContent = labelOf(k, def);
        });
        const tr = document.querySelector(`#marginsTable tbody tr[data-b="${k}"] td:first-child`);
        if (tr) tr.textContent = labelOf(k, def);
      });
      if (typeof computeContractMargins === 'function') computeContractMargins();
    });
  }

  // Initial normalisation pass for values already in the DOM
  try {
    // Rates
    rateInputs.forEach(el => {
      normaliseRateInput(el);
      if (typeof setContractFormValue === 'function') {
        setContractFormValue(el.name, el.value);
      }
    });
    // Mileage
    mileageInputs.forEach(el => {
      normaliseRateInput(el);
      if (typeof setContractFormValue === 'function') {
        setContractFormValue(el.name, el.value);
      }
    });
  } catch {}

  if (typeof computeContractMargins === 'function') computeContractMargins();
}

// Open preset picker (card grid) and return chosen data




async function fetchClientRatePresets({ client_id, role, band, active_on }) {
  if (!client_id) return [];
  const qs = new URLSearchParams();
  qs.set('client_id', client_id);
  if (role) qs.set('role', role);
  if (band != null && band !== '') qs.set('band', band);
  if (active_on) qs.set('active_on', active_on);
  const r = await authFetch(API(`/api/rates/client-defaults?${qs.toString()}`));
  const rows = toList(r) || [];
  return rows;
}

async function fetchCandidateRateOverrides({ candidate_id, client_id, role, band, active_on }) {
  if (!candidate_id) return [];
  const qs = new URLSearchParams();
  qs.set('candidate_id', candidate_id);
  if (client_id) qs.set('client_id', client_id);
  if (role) qs.set('role', role);
  if (band != null && band !== '') qs.set('band', band);
  if (active_on) qs.set('active_on', active_on);
  const r = await authFetch(API(`/api/rates/candidate-overrides?${qs.toString()}`));
  const rows = toList(r) || [];
  return rows;
}




function computeContractMargins() {
  const fs = (window.modalCtx && window.modalCtx.formState) || { main:{}, pay:{} };
  const form = document.querySelector('#contractRatesTab')?.closest('form') || document.querySelector('#contractForm');

  const pmStaged = (fs.main && fs.main.pay_method_snapshot) || '';
  const payMethodSel = form ? form.querySelector('select[name="pay_method_snapshot"]') : null;
  const payMethod = ((payMethodSel && payMethodSel.value) || pmStaged || 'PAYE').toUpperCase();

  const get = (n) => {
    const domVal = form ? form.querySelector(`[name="${n}"]`)?.value : null;
    const staged = fs.pay ? fs.pay[n] : null;
    const v = (staged != null && staged !== '') ? staged : (domVal != null ? domVal : '');
    return Number(v || 0);
  };

  const buckets = ['day','night','sat','sun','bh'];
  const negFlags = {};
  buckets.forEach(b => {
    const ch = get(`charge_${b}`);
    const py = (payMethod==='PAYE') ? get(`paye_${b}`) : get(`umb_${b}`);
    let mg = ch - py;
    try { if (typeof window.calcDailyMargin === 'function') mg = window.calcDailyMargin({ bucket:b, charge:ch, pay:py, method:payMethod }); } catch {}

    const row = document.querySelector(`#marginsTable tbody tr[data-b="${b}"]`);
    if (row) {
      const chEl = row.querySelector('.ch'), pyEl = row.querySelector('.py'), mgEl=row.querySelector('.mg');
      if (pyEl) pyEl.textContent = (py || py===0) ? Number(py).toFixed(2) : '';
      if (chEl) chEl.textContent = (ch || ch===0) ? Number(ch).toFixed(2) : '';
      if (mgEl) {
        mgEl.textContent = (mg || mg===0) ? Number(mg).toFixed(2) : '';
        mgEl.style.color = (mg<0)? 'var(--fail)' : '';
        if (mg < 0) {
          mgEl.setAttribute('data-negative','1');
          if (!mgEl.querySelector('.mini')) {
            const hint = document.createElement('div'); hint.className='mini'; hint.textContent = 'Margin can’t be negative';
            mgEl.appendChild(hint);
          }
          row.setAttribute('data-negative','1');
        } else {
          mgEl.removeAttribute('data-negative');
          const hint = mgEl.querySelector('.mini'); if (hint) hint.remove();
          row.removeAttribute('data-negative');
        }
      }
    }
    negFlags[b] = (mg < 0);
  });

  const hasNegativeMargins = Object.values(negFlags).some(Boolean);
  window.__contractMarginState = { hasNegativeMargins, negFlags, method: payMethod };
  try { window.dispatchEvent(new CustomEvent('contract-margins-updated', { detail: window.__contractMarginState })); } catch {}
  try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
}



// ==============================
// 1) openCandidateRateModal(...)
// ==============================
// ========== CANDIDATE OVERRIDES ==========
// ========== CHILD MODAL (CANDIDATE OVERRIDE) WITH HEAVY LOGGING ==========


// ========== PARENT TABLE RENDER (WITH LOUD LOGS + SAFETY NET) ==========

async function openCandidateRateModal(candidate_id, existing) {
  const LOG = !!window.__LOG_RATES;
  const LOG_APPLY = (typeof window.__LOG_APPLY === 'boolean') ? window.__LOG_APPLY : LOG;
  const L  = (...a)=> { if (LOG) console.log('[RATES][openCandidateRateModal]', ...a); };
  const LG = (label, obj)=> { if (LOG) { console.groupCollapsed(`[RATES][openCandidateRateModal] ${label}`); console.log(obj); console.groupEnd(); } };

  L('ENTRY', { candidate_id, hasExisting: !!existing });

  const parentFrame   = _currentFrame();
  const parentEditable= parentFrame && (parentFrame.mode === 'edit' || parentFrame.mode === 'create');
  L('parent frame', { editable: !!parentEditable, mode: parentFrame?.mode });

  // ===== load clients =====
  const clients = await listClientsBasic().catch(e=>{ L('listClientsBasic failed', e); return []; });
  L('clients loaded', clients?.length || 0);
  const clientOptions = (clients||[]).map(c => `<option value="${c.id}">${c.name}</option>`).join('');
  const initialClientId = existing?.client_id || '';

  const defaultRateType = existing?.rate_type
    ? String(existing.rate_type).toUpperCase()
    : String(window.modalCtx?.data?.pay_method || 'PAYE').toUpperCase();

  const bucketLabel = { day:'Day', night:'Night', sat:'Sat', sun:'Sun', bh:'BH' };

  // Track the current in-form role/band so they don't reset when date/client changes
  let currentRole = existing?.role || '';
  let currentBand = (existing && existing.band != null) ? String(existing.band) : '';

  // ===== FORM =====
  const formHtml = html(`
    <div class="form" id="candRateForm">
      <div class="row">
        <label>Client (required)</label>
        <select name="client_id" id="cr_client_id" ${parentEditable ? '' : 'disabled'}>
          <option value="">Select client…</option>
          ${clientOptions}
        </select>
      </div>

      <div class="row">
        <label>Rate type (required)</label>
        <select name="rate_type" id="cr_rate_type" ${parentEditable ? '' : 'disabled'}>
          <option ${defaultRateType==='PAYE'?'selected':''}>PAYE</option>
          <option ${defaultRateType==='UMBRELLA'?'selected':''}>UMBRELLA</option>
        </select>
      </div>

      <div class="row">
        <label>Role (required)</label>
        <select name="role" id="cr_role" required ${parentEditable ? '' : 'disabled'}>
          <option value="">Select role…</option>
        </select>
      </div>

      <div class="row" id="cr_band_row">
        <label>Band (optional)</label>
        <select name="band" id="cr_band" ${parentEditable ? '' : 'disabled'}></select>
      </div>

      <div class="row">
        <label>Effective from (DD/MM/YYYY)</label>
        <input type="text" name="date_from" id="cr_date_from" placeholder="DD/MM/YYYY" ${parentEditable ? '' : 'disabled'} />
      </div>

      <div class="row">
        <label>Effective to (optional, DD/MM/YYYY)</label>
        <input type="text" name="date_to" id="cr_date_to" placeholder="DD/MM/YYYY" ${parentEditable ? '' : 'disabled'} />
        <div class="hint field-hint err" id="cr_date_to_err" style="display:none"></div>
      </div>

      ${['day','night','sat','sun','bh'].map(b => `
        <div class="row" data-bucket="${b}">
          <label>Pay (${b.toUpperCase()})</label>
          <div class="slot" id="slot_${b}">
            <div class="slot-input" id="slot_input_${b}" style="display:none">
              <input type="number" step="0.01" name="pay_${b}" id="pay_${b}"/>
            </div>
            <div class="slot-ph" id="slot_ph_${b}" style="opacity:.75">Not in client rate</div>
          </div>
        </div>`).join('')}

      <div class="row" style="grid-column:1 / -1; margin-top:10px">
        <table class="grid" id="cr_margins_tbl" style="width:100%">
          <thead><tr><th>Bucket</th><th>Margin</th><th class="hint">Uses client charge at start date</th></tr></thead>
          <tbody>
            ${['day','night','sat','sun','bh'].map(b=>`
              <tr><td>${b.toUpperCase()}</td><td><span id="cr_m_${b}">—</span></td><td></td></tr>`).join('')}
          </tbody>
        </table>
      </div>

      <div id="cr_err_panel" style="grid-column:1/-1; margin-top:10px; display:none; border:1px solid #7f1d1d; background:rgba(239,68,68,.08); color:#fecaca; padding:10px; border-radius:8px"></div>
    </div>
  `);

  // ===== helpers =====
  async function _erniMultiplier(){
    if (typeof window.__ERNI_MULT__ === 'number') return window.__ERNI_MULT__;
    try {
      if (typeof getSettingsCached === 'function') {
        const s = await getSettingsCached();
        let p = s?.erni_pct ?? s?.employers_ni_percent ?? 0;
        p = Number(p) || 0;
        if (p > 1) p = p / 100;
        window.__ERNI_MULT__ = 1 + p;
        return window.__ERNI_MULT__;
      }
    } catch {}
    window.__ERNI_MULT__ = 1;
    return 1;
  }
  function numOrNull(v){ if (v===undefined||v===null) return null; if (typeof v === 'string' && v.trim()==='') return null; const n=Number(v); return Number.isFinite(n) ? n : null; }
  const fmt = v => (v==null || Number.isNaN(v)) ? '—' : (Math.round(v*100)/100).toFixed(2);
  function showInlineError(html){ const p = byId('cr_err_panel'); if (!p) return; if (html && String(html).trim() !== '') { p.innerHTML = html; p.style.display = ''; } else { p.innerHTML = ''; p.style.display = 'none'; } }
  function setFieldError(bucket,msg){
    const rowInput = document.querySelector(`#candRateForm input[name="pay_${bucket}"]`);
    if (!rowInput) return;
    let hint = rowInput.parentElement?.querySelector?.(`.field-hint.err[data-bucket="${bucket}"]`);
    if (msg) {
      if (!hint) {
        hint = document.createElement('div');
        hint.className = 'hint field-hint err';
        hint.setAttribute('data-bucket', bucket);
        rowInput.parentElement.appendChild(hint);
      }
      hint.textContent = msg;
    } else if (hint) hint.remove();
  }
  function setDateToError(msg){ const el = byId('cr_date_to_err'); if (!el) return; if (msg) { el.textContent = msg; el.style.display = ''; } else { el.textContent = ''; el.style.display = 'none'; } }
  function clearAllFieldErrors(){ document.querySelectorAll('#candRateForm .field-hint.err').forEach(el=>el.remove()); setDateToError(''); }

  async function resolveCoveringWindow(client_id, role, band, active_on){
    try {
      const list = await listClientRates(client_id, { active_on, only_enabled: true });
      const rows = Array.isArray(list) ? rows = list.filter(w => !w.disabled_at_utc && w.role === role) : [];
      const wins = Array.isArray(list) ? list.filter(w => !w.disabled_at_utc && w.role === role) : [];
      const filtered = wins;
      let win = filtered.find(w => (w.band ?? null) === (band ?? null));
      if (!win && (band == null)) win = filtered.find(w => w.band == null);
      return win ? {
        charges: { day:win.charge_day??null, night:win.charge_night??null, sat:win.charge_sat??null, sun:win.charge_sun??null, bh:win.charge_bh??null },
        capIso: win.date_to || null
      } : null;
    } catch(e){ L('resolveCoveringWindow err', e); return null; }
  }

  let lastApplyState = null;
  function setApplyEnabled(enabled, reasonSummary){
    try {
      const btn = document.querySelector('#btnSave, #modal .actions .primary, #modal .btn-save, .modal .btn-save');
      if (btn) { btn.disabled = !enabled; btn.classList.toggle('disabled', !enabled); }
    } catch {}
    if (LOG_APPLY && lastApplyState !== enabled) {
      console.log('[RATES][APPLY] state →', enabled ? 'ENABLED' : 'DISABLED', reasonSummary || '');
      lastApplyState = enabled;
    }
    try { window.dispatchEvent(new CustomEvent('modal-apply-enabled', { detail:{ enabled } })); } catch {}
  }

  // ===== driver: recompute state (validations + overlap + preview) =====
  async function recomputeOverrideState(){
    const clientId = byId('cr_client_id')?.value || '';
    const role     = byId('cr_role')?.value || '';
    const bandSel  = byId('cr_band')?.value ?? '';
    const band     = (bandSel === '' ? null : bandSel);
    const isoFrom  = parseUkDateToIso(byId('cr_date_from')?.value || '');
    const isoTo    = parseUkDateToIso(byId('cr_date_to')?.value || '');
    const rateType = String(byId('cr_rate_type')?.value || '').toUpperCase();

    const buckets = ['day','night','sat','sun','bh'];
    const inputEl = (b)=> document.querySelector(`#candRateForm input[name="pay_${b}"]`);
    const slotIn  = (b)=> byId(`slot_input_${b}`);
    const slotPh  = (b)=> byId(`slot_ph_${b}`);

    showInlineError(''); clearAllFieldErrors();
    let canApply = true;

    const need = [];
    if (!clientId) need.push('clientId');
    if (!rateType) need.push('rateType');
    if (!role)     need.push('role');
    if (!isoFrom)  need.push('date_from');
    if (need.length) {
      buckets.forEach(b => {
        const inp = inputEl(b);
        if (inp) { inp.disabled = true; }
        if (slotIn(b)) slotIn(b).style.display = 'none';
        if (slotPh(b)) slotPh(b).style.display = '';
        const sp = byId(`cr_m_${b}`); if (sp) sp.textContent = '—';
      });
      setApplyEnabled(false, 'not_ready');
      return;
    }

    const win = await resolveCoveringWindow(clientId, role, band, isoFrom);
    if (!win) {
      buckets.forEach(b => {
        const inp = inputEl(b);
        if (inp) { inp.disabled = true; }
        if (slotIn(b)) slotIn(b).style.display = 'none';
        if (slotPh(b)) slotPh(b).style.display = '';
        const sp = byId(`cr_m_${b}`); if (sp) sp.textContent = '—';
      });
      showInlineError(`No active client default for <b>${escapeHtml(role)}</b>${band?` / <b>${escapeHtml(band)}</b>`:''} on <b>${formatIsoToUk(isoFrom)}</b>.`);
      setApplyEnabled(false, 'no_cover');
      return;
    }

    buckets.forEach(b => {
      const hasCharge = (win.charges[b] != null);
      const inp = inputEl(b);
      if (hasCharge) {
        if (slotPh(b)) slotPh(b).style.display = 'none';
        if (slotIn(b)) slotIn(b).style.display = '';
        if (inp) inp.disabled = false;
      } else {
        if (slotIn(b)) slotIn(b).style.display = 'none';
        if (slotPh(b)) slotPh(b).style.display = '';
        if (inp) { inp.value = ''; inp.disabled = true; }
      }
    });

    const mult = await _erniMultiplier();

    const invalid = [];
    buckets.forEach(b => {
      const el  = inputEl(b), chg = win.charges[b];
      if (!el || el.disabled) return;
      const pay = numOrNull(el.value);
      if (pay != null && chg == null) { invalid.push(b); setFieldError(b, `No client charge for ${bucketLabel[b]}.`); }
    });
    if (invalid.length) canApply = false;

    const neg = [];
    buckets.forEach(b => {
      const el  = inputEl(b), chg = win.charges[b];
      if (!el || el.disabled) return;
      const pay = numOrNull(el.value);
      if (pay == null || chg == null) return;
      const m = (rateType === 'PAYE') ? (chg - (pay * mult)) : (chg - pay);
      if (m < 0) { neg.push(b); setFieldError(b, `Margin would be negative for ${bucketLabel[b]}.`); }
    });
    if (neg.length) canApply = false;

    setDateToError('');
    if (isoTo && win.capIso && isoTo > win.capIso) {
      setDateToError(`Client rate ends on ${formatIsoToUk(win.capIso)} — override must end on/before this date.`);
      canApply = false;
    }

    const O = window.modalCtx.overrides || { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() };
    const deletedIds = O.stagedDeletes || new Set();
    const unify = [];
    (O.existing||[]).forEach(ex => { if (!deletedIds.has(ex.id)) unify.push({ ...(ex||{}), ...(O.stagedEdits?.[ex.id]||{}) }); });
    (O.stagedNew||[]).forEach(n => unify.push({ ...(n||{}) }));

    const sameKey = (o) =>
      String(o.client_id||'') === clientId &&
      String(o.role||'')      === role &&
      String((o.rate_type||'').toUpperCase()) === rateType &&
      String(o.band??'')      === String(band??'');

    const isSelf = (o) => {
      if (!existing) return false;
      if (existing.id && o.id) return String(o.id) === String(existing.id);
      if (existing._tmpId && o._tmpId) return String(o._tmpId) === String(existing._tmpId);
      return false;
    };

    const conflicts = unify.filter(o => sameKey(o) && !isSelf(o) &&
      !((o.date_to||'9999-12-31') < (isoFrom||'0000-01-01') || (isoTo||'9999-12-31') < (o.date_from||'0000-01-01')));

    if (conflicts.length) {
      canApply = false;
      const ov = conflicts[0];
      const cutThis  = (()=>{ const d=new Date((ov.date_from||'')+'T00:00:00Z'); if(!isNaN(d)) d.setUTCDate(d.getUTCDate()-1); return isNaN(d)?null:`${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`; })();
      const cutOther = (()=>{ const d=new Date((isoFrom||'')+'T00:00:00Z'); if(!isNaN(d)) d.setUTCDate(d.getUTCDate()-1); return isNaN(d)?null:`${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`; })();

      let fixButtons = '';
      if (cutThis)  fixButtons += `<button id="cr_fix_this"  class="btn mini" style="margin-right:8px">Fix: Shorten <b>THIS</b> to ${formatIsoToUk(cutThis)}</button>`;
      if (cutOther) fixButtons += `<button id="cr_fix_other" class="btn mini">Fix: Shorten <b>OTHER</b> to ${formatIsoToUk(cutOther)}</button>`;

      showInlineError(`
        <div style="font-weight:700;margin-bottom:6px">Overlap detected</div>
        <div style="margin-bottom:8px">Another rate card exists in this category.<br/><span class="hint">Use Fix or adjust dates.</span></div>
        <div>${fixButtons || '<i>No safe automatic fix available.</i>'}</div>
      `);

      setTimeout(()=> {
        const inTo = byId('cr_date_to');
        const fixThis = byId('cr_fix_this');
        if (fixThis && cutThis) fixThis.onclick = ()=> { inTo.value = formatIsoToUk(cutThis); recomputeOverrideState(); };
        const fixOther = byId('cr_fix_other');
        if (fixOther && cutOther) fixOther.onclick = ()=> {
          try {
            const target = conflicts[0];
            if (target.id) {
              O.stagedEdits[target.id] = { ...(O.stagedEdits[target.id]||{}), date_to: cutOther };
            } else if (target._tmpId) {
              const ix = (O.stagedNew||[]).findIndex(r=>r._tmpId===target._tmpId);
              if (ix>=0) O.stagedNew[ix] = { ...O.stagedNew[ix], date_to: cutOther };
            }
            renderCandidateRatesTable();
            recomputeOverrideState();
          } catch(e){ L('fixOther failed', e); }
        };
      }, 0);
    }

    // Preview margins (never NaN)
    buckets.forEach(b => {
      const sp  = byId(`cr_m_${b}`), el = inputEl(b), chg = win.charges[b];
      const pay = (el && !el.disabled) ? numOrNull(el.value) : null;
      const m   = (chg != null && pay != null) ? ((rateType === 'PAYE') ? (chg - (pay * mult)) : (chg - pay)) : null;
      if (sp) sp.textContent = (m==null ? '—' : fmt(m));
    });

    if (LOG_APPLY) console.log('[RATES][APPLY] canApply?', canApply, { clientId, role, band, isoFrom, isoTo, rateType });
    setApplyEnabled(canApply, canApply ? 'ok' : 'violations');
  }

  showModal(
    existing ? 'Edit Candidate Rate Override' : 'Add Candidate Rate Override',
    [{ key:'form', label:'Form' }],
    () => formHtml,
    async () => {
      await recomputeOverrideState();
      if (lastApplyState === false) { L('Apply blocked by recompute'); return false; }

      const raw = collectForm('#candRateForm');
      LG('Apply collected form', raw);

      const client_id = (raw.client_id || '').trim();
      const role      = (raw.role || '').trim();
      const band      = (raw.band || '').trim() || null;
      const rate_type = String(raw.rate_type || '').toUpperCase();

      const date_from = parseUkDateToIso(raw.date_from);
      const date_to   = raw.date_to ? parseUkDateToIso(raw.date_to) : null;

      const mapPay = (k) => (Object.prototype.hasOwnProperty.call(raw, k) && raw[k] !== '' ? Number(raw[k]) : null);

      const stagedAll = {
        id: existing?.id,
        candidate_id,
        client_id,
        role, band, rate_type,
        date_from, date_to,
        pay_day   : mapPay('pay_day'),
        pay_night : mapPay('pay_night'),
        pay_sat   : mapPay('pay_sat'),
        pay_sun   : mapPay('pay_sun'),
        pay_bh    : mapPay('pay_bh')
      };

      const O = (window.modalCtx.overrides ||= { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() });
      LG('STAGING before', { existing: (O.existing||[]).length, stagedNew:(O.stagedNew||[]).length, stagedEdits:Object.keys(O.stagedEdits||{}).length, stagedDeletes: O.stagedDeletes?.size || 0 });

      if (existing?.id) {
        O.stagedEdits[existing.id] = { ...(O.stagedEdits[existing.id]||{}), ...stagedAll };
      } else if (existing && !existing.id) {
        const tmpId = existing._tmpId || null;
        const idx   = tmpId ? (O.stagedNew||[]).findIndex(r => r._tmpId === tmpId) : -1;
        if (idx >= 0) O.stagedNew[idx] = { ...O.stagedNew[idx], ...stagedAll, _tmpId: tmpId };
        else          O.stagedNew.push({ ...stagedAll, _tmpId: tmpId || `tmp_${Date.now()}` });
      } else {
        O.stagedNew.push({ ...stagedAll, _tmpId: `tmp_${Date.now()}` });
      }

      LG('STAGING after', {
        existing: (O.existing||[]).length,
        stagedNew:(O.stagedNew||[]).length,
        stagedEdits:Object.keys(O.stagedEdits||{}).length,
        stagedDeletes: O.stagedDeletes?.size || 0,
        peekNew: (O.stagedNew||[])[(O.stagedNew||[]).length-1]
      });

      try { await renderCandidateRatesTable(); } catch {}
      try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
      return true;
    },
    false,
    () => {
      const parent = _currentFrame();
      if (parent) { parent.currentTabKey = 'rates'; parent.setTab('rates'); }
    },
    { kind: 'candidate-override' }
  );

  // ===== prefill & wire =====
  const selClient = byId('cr_client_id');
  const selRateT  = byId('cr_rate_type');
  const selRole   = byId('cr_role');
  const selBand   = byId('cr_band');
  const inFrom    = byId('cr_date_from');
  const inTo      = byId('cr_date_to');

  if (initialClientId) selClient.value = initialClientId;
  if (existing?.date_from) inFrom.value = formatIsoToUk(existing.date_from);
  if (existing?.date_to)   inTo.value   = formatIsoToUk(existing.date_to);

  // Prefill ALL buckets that have values (2dp)
  ['day','night','sat','sun','bh'].forEach(b=>{
    const val = (existing && Number.isFinite(existing[`pay_${b}`])) ? existing[`pay_${b}`] : null;
    const el  = document.querySelector(`#candRateForm input[name="pay_${b}"]`);
    if (el && val != null) {
      const num = Number(val);
      el.value = Number.isFinite(num) ? (Math.round(num*100)/100).toFixed(2) : String(val);
    }
  });

  attachUkDatePicker(inFrom); attachUkDatePicker(inTo);

  async function refreshClientRoles(clientId) {
    selRole.innerHTML = `<option value="">Select role…</option>`; selRole.disabled = true;
    selBand.innerHTML = `<option value=""></option>`;             selBand.disabled  = true;

    if (!clientId) { setApplyEnabled(false, 'no_client'); return; }

    const active_on = parseUkDateToIso(inFrom.value || '') || null;
    const list  = await listClientRates(clientId, { active_on, only_enabled: true }).catch(_=>[]);
    const wins  = (Array.isArray(list) ? list.filter(w => !w.disabled_at_utc) : []);
    const roles = new Set(); const bandsByRole = {};
    wins.forEach(w => {
      if (!w.role) return;
      roles.add(w.role);
      (bandsByRole[w.role] ||= new Set()).add(w.band==null ? '' : String(w.band));
    });

    const allowed = [...roles].sort((a,b)=> a.localeCompare(b));
    selRole.innerHTML = `<option value="">Select role…</option>` + allowed.map(code => `<option value="${code}">${code}</option>`).join('');
    selRole.disabled = !parentEditable;

    // Choose a role in this order:
    // 1) currentRole if still valid
    // 2) existing.role if still valid
    // 3) blank
    let chosenRole = '';
    if (currentRole && allowed.includes(currentRole)) {
      chosenRole = currentRole;
    } else if (existing?.role && allowed.includes(existing.role)) {
      chosenRole = existing.role;
    }

    if (chosenRole) {
      selRole.value = chosenRole;
      currentRole = chosenRole;

      const bandSet = [...(bandsByRole[chosenRole] || new Set())];
      const hasNull = bandSet.includes('');
      selBand.innerHTML =
        (hasNull ? `<option value="">(none)</option>` : '') +
        bandSet
          .filter(b=>b!=='')
          .sort((a,b)=> String(a).localeCompare(String(b)))
          .map(b => `<option value="${b}">${b}</option>`).join('');
      selBand.disabled = !parentEditable;

      // Pick band: prefer currentBand if still valid; else existing.band
      let desiredBand = '';
      if (currentBand && bandSet.includes(currentBand)) {
        desiredBand = currentBand;
      } else if (existing && existing.band != null) {
        const asStr = String(existing.band);
        if (bandSet.includes(asStr)) desiredBand = asStr;
      }

      if (desiredBand && bandSet.includes(desiredBand)) {
        selBand.value = desiredBand;
        currentBand = desiredBand;
      } else {
        // leave at (none)
        currentBand = '';
      }
    } else {
      selBand.innerHTML = `<option value=""></option>`;
      selBand.disabled  = true;
      currentRole = '';
      currentBand = '';
    }

    await recomputeOverrideState();
  }

  selClient.addEventListener('change', async () => {
    L('[EVENT] client change');
    if (parentEditable) {
      currentRole = ''; // changing client invalidates previous role
      currentBand = '';
      await refreshClientRoles(selClient.value);
    }
  });
  selRateT .addEventListener('change',        () => {
    L('[EVENT] rate_type change');
    if (parentEditable) recomputeOverrideState();
  });
  inFrom   .addEventListener('change',  async () => {
    L('[EVENT] date_from change');
    if (parentEditable) {
      // date change may change which client windows apply, but we keep role/band if still valid
      await refreshClientRoles(selClient.value);
    }
  });
  selRole  .addEventListener('change',  async () => {
    L('[EVENT] role change');
    if (!parentEditable) return;
    currentRole = selRole.value || '';
    // changing role recalculates margins
    await recomputeOverrideState();
  });
  selBand  .addEventListener('change',        () => {
    L('[EVENT] band change');
    if (!parentEditable) return;
    currentBand = selBand.value || '';
    recomputeOverrideState();
  });
  ['pay_day','pay_night','pay_sat','pay_sun','pay_bh'].forEach(n=>{
    const el = document.querySelector(`#candRateForm input[name="${n}"]`);
    if (el) el.addEventListener('input', () => {
      if (LOG_APPLY) console.log('[RATES][EVENT] pay change', n, el.value);
      recomputeOverrideState();
    });
  });

  // Initial state: if there is a client, load its roles, otherwise just compute state
  if (initialClientId) {
    await refreshClientRoles(initialClientId);
  } else {
    await recomputeOverrideState();
  }

  (function wireDeleteButton(){
    const delBtn = byId('btnDelete');
    if (!delBtn) return;
    if (!existing || !existing.id) { delBtn.style.display='none'; return; }
    delBtn.style.display = '';
    delBtn.disabled = false;
    delBtn.onclick = () => {
      try {
        const O = (window.modalCtx.overrides ||= { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() });
        (O.stagedDeletes ||= new Set()).add(existing.id);
        L('staged delete', { id: existing.id, size: O.stagedDeletes.size });
        try { renderCandidateRatesTable(); } catch {}
        try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
        const closeBtn = byId('btnCloseModal'); if (closeBtn) closeBtn.click();
      } catch (e) { L('stage delete failed', e); }
    };
  })();
}



// ---- Client modal

// =========================== openClient (FIXED) ==========================
// =================== CLIENT MODAL (UPDATED: rates rate_type + hospitals staged-CRUD) ===================
// ✅ UPDATED — unified FE model; on load, convert server rows to unified; on save, validate overlaps, bridge to per-type API

// ================== FRONTEND: openClient (UPDATED) ==================
// ================== FIXED: openClient (hydrate before showModal) ==================
// ================== FIXED: openClient (hydrate before showModal) ==================

// ============================================================================
// OPEN CLIENT (parent modal) — skip posting disabled windows on Save
// (No delete button is added here; ensure any existing parent delete UI is removed elsewhere.)
// ============================================================================

// =================== CLIENT RATES TABLE (UPDATED) ===================
// ✅ UPDATED — unified table view, dbl-click opens unified modal
// ============================================================================
// RENDER CLIENT RATES TABLE (adds "Status" col; shows disabled who/when)
// ============================================================================



// Now shows derived PAYE & Umbrella margins per bucket in the table
async function openClient(row) {
  // ===== Logging helpers (toggle with window.__LOG_MODAL = true/false) =====
  const LOG = (typeof window.__LOG_MODAL === 'boolean') ? window.__LOG_MODAL : true;
  const APILOG = (typeof window !== 'undefined' && !!window.__LOG_API) || (typeof __LOG_API !== 'undefined' && !!__LOG_API);
  const L  = (...a)=> { if (LOG) console.log('[OPEN_CLIENT]', ...a); };
  const W  = (...a)=> { if (LOG) console.warn('[OPEN_CLIENT]', ...a); };
  const E  = (...a)=> { if (LOG) console.error('[OPEN_CLIENT]', ...a); };

  const deep = (o)=> JSON.parse(JSON.stringify(o || {}));
  const incoming = deep(row || {});
  const seedId   = incoming?.id || null;

  L('ENTRY', { incomingKeys: Object.keys(incoming||{}), seedId });

  const unwrapSingle = (data, key) => {
    if (Array.isArray(data)) return data[0] || null;
    if (data && key && data[key]) return unwrapSingle(data[key], null);
    if (data && Array.isArray(data.rows))  return data.rows[0]  || null;
    if (data && Array.isArray(data.items)) return data.items[0] || null;
    if (data && Array.isArray(data.data))  return data.data[0]  || null;
    return (data && typeof data === 'object') ? data : null;
  };

  // 1) Hydrate full client if we have an id
  let full = incoming;
  let settingsSeed = null;
  if (seedId) {
    try {
      const url = API(`/api/clients/${encodeURIComponent(seedId)}`);
      L('[HTTP] GET', url);
      const r = await authFetch(url);
      L('[HTTP] status', r?.status, r?.ok);

      try {
        const raw = await r.clone().text();
        if (LOG) console.debug('[HTTP] raw body (≤2KB):', raw.slice(0, 2048));
      } catch (peekErr) { W('[HTTP] raw peek failed', peekErr?.message || peekErr); }

      if (r.ok) {
        const data = await r.json().catch(()=> ({}));
        const clientObj   = data?.client || unwrapSingle(data, 'client') || null;
        const settingsObj = data?.client_settings || data?.settings || null;
        settingsSeed = settingsObj ? deep(settingsObj) : null;
        full = clientObj || incoming;
        L('hydrated JSON keys', Object.keys(data||{}), 'client keys', Object.keys(clientObj||{}), 'hasSettingsSeed', !!settingsSeed);
      } else {
        W('non-OK response, using incoming row');
      }
    } catch (e) {
      W('openClient hydrate failed; using summary row', e);
    }
  } else {
    L('no seedId — create mode');
  }

  // 2) Seed modal context
  const fullKeys = Object.keys(full || {});
  L('seeding window.modalCtx', { entity: 'clients', fullId: full?.id, fullKeys });

  window.modalCtx = {
    entity: 'clients',
    data: deep(full),
    formState: { __forId: full?.id || null, main: {} },
    ratesState: [],
    ratesBaseline: [],
    hospitalsState: { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() },
    clientSettingsState: settingsSeed ? deep(settingsSeed) : {},
    openToken: ((full?.id) || 'new') + ':' + Date.now(),
    // NEW: persistent set for staged client-rate deletes (survives refresh/merge)
    ratesStagedDeletes: (window.modalCtx && window.modalCtx.ratesStagedDeletes instanceof Set)
      ? window.modalCtx.ratesStagedDeletes
      : new Set()
  };

  L('window.modalCtx seeded', {
    entity: window.modalCtx.entity,
    dataId: window.modalCtx.data?.id,
    dataKeys: Object.keys(window.modalCtx.data||{}),
    formStateForId: window.modalCtx.formState?.__forId,
    openToken: window.modalCtx.openToken,
    preseededSettings: Object.keys(window.modalCtx.clientSettingsState||{})
  });

  // 3) Render modal
  L('calling showModal with hasId=', !!full?.id, 'rawHasIdArg=', full?.id);
  showModal(
    'Client',
    [
      {key:'main',     label:'Main'},
      {key:'rates',    label:'Rates'},
      {key:'settings', label:'Client settings'},
      {key:'hospitals',label:'Hospitals & wards'}
    ],
    (k, r) => { L('[renderClientTab] tab=', k, 'rowKeys=', Object.keys(r||{}), 'sample=', { name: r?.name, id: r?.id }); return renderClientTab(k, r); },
    async ()=> {
      L('[onSave] begin', { dataId: window.modalCtx?.data?.id, forId: window.modalCtx?.formState?.__forId });
      const isNew = !window.modalCtx?.data?.id; // ← declared here (keep)

      // Collect "main" form
      const fs = window.modalCtx.formState || { __forId: null, main:{} };
      const hasId = !!window.modalCtx.data?.id;
      const same = hasId ? (fs.__forId === window.modalCtx.data.id)
                         : (fs.__forId === window.modalCtx.openToken || fs.__forId == null);

      const stagedMain = same ? (fs.main || {}) : {};
      const liveMain   = byId('tab-main') ? collectForm('#tab-main') : {};
      const payload    = { ...stagedMain, ...liveMain };

      L('[onSave] collected', { same, stagedKeys: Object.keys(stagedMain||{}), liveKeys: Object.keys(liveMain||{}) });

      delete payload.but_let_cli_ref;
      if (!payload.name && full?.name) payload.name = full.name;
      if (!payload.name) { alert('Please enter a Client name.'); return { ok:false }; }

      // Settings normalization (unchanged)
      const baseline = window.modalCtx.clientSettingsState || {};
      const hasFormMounted = !!byId('clientSettingsForm');
      const hasFullBaseline = ['day_start','day_end','night_start','night_end'].every(k => typeof baseline[k] === 'string' && baseline[k] !== '');
      const shouldValidateSettings = hasFormMounted || hasFullBaseline;

      let pendingSettings = null;
      if (shouldValidateSettings) {
        let csMerged = { ...(baseline || {}) };
        if (hasFormMounted) {
          const liveSettings = collectForm('#clientSettingsForm', false);
          ['day_start','day_end','night_start','night_end'].forEach(k=>{
            const v = _toHHMM(liveSettings[k]); if (v) csMerged[k] = v;
          });
          if (typeof liveSettings.timezone_id === 'string' && liveSettings.timezone_id.trim() !== '') {
            csMerged.timezone_id = liveSettings.timezone_id.trim();
          }
        }
        const { cleaned: csClean, invalid: csInvalid } = normalizeClientSettingsForSave(csMerged);
        if (APILOG) console.log('[OPEN_CLIENT] client_settings (merged→clean)', { csMerged, csClean, csInvalid, hasFormMounted, hasFullBaseline });
        if (csInvalid) { alert('Times must be HH:MM (24-hour).'); return { ok:false }; }
        if (Object.keys(csClean).length) pendingSettings = csClean;
      }

      // 3) Upsert client
      const idForUpdate = window.modalCtx?.data?.id || full?.id || null;
      if (APILOG) console.log('[OPEN_CLIENT] upsertClient → request', { idForUpdate, payload });
      delete payload.client_settings;

      const clientResp  = await upsertClient(payload, idForUpdate).catch(err => { E('upsertClient failed', err); return null; });
      const clientId    = idForUpdate || (clientResp && clientResp.id);
      if (APILOG) console.log('[OPEN_CLIENT] upsertClient ← response', { ok: !!clientResp, clientId });
      if (!clientId) { alert('Failed to save client'); return { ok:false }; }

      const savedClient = clientResp && typeof clientResp === 'object' ? clientResp : { id: clientId, ...payload };
      window.modalCtx.data = { ...(window.modalCtx.data || {}), ...savedClient, id: clientId };

      // 4) Save Client settings (after client exists)
      try {
        if (pendingSettings && Object.keys(pendingSettings).length) {
          if (APILOG) console.log('[OPEN_CLIENT] upsertClient (settings) → PUT /api/clients/:id', { clientId, pendingSettings });
          const upd = await upsertClient({ client_settings: pendingSettings }, clientId);
          if (!upd) throw new Error('Settings update failed');
          if (upd && typeof upd === 'object') window.modalCtx.data = { ...window.modalCtx.data, ...upd, id: clientId };
        }
      } catch (err) {
        alert(`Failed to save Client settings: ${String(err?.message || err)}`);
        return { ok:false };
      }

      // 5) Hospitals CRUD (typed-safe; skip when no staged changes)
      try {
        const hsRaw = window.modalCtx.hospitalsState || {};
        const hs = {
          existing     : Array.isArray(hsRaw.existing) ? hsRaw.existing : [],
          stagedNew    : Array.isArray(hsRaw.stagedNew) ? hsRaw.stagedNew : [],
          stagedEdits  : (hsRaw.stagedEdits && typeof hsRaw.stagedEdits === 'object') ? hsRaw.stagedEdits : {},
          stagedDeletes: (hsRaw.stagedDeletes instanceof Set)
            ? hsRaw.stagedDeletes
            : new Set(Array.isArray(hsRaw.stagedDeletes) ? hsRaw.stagedDeletes : Object.keys(hsRaw.stagedDeletes || {}))
        };

        const hasDel   = hs.stagedDeletes.size > 0;
        const hasEdits = Object.keys(hs.stagedEdits).length > 0;
        const hasNew   = hs.stagedNew.length > 0;

        if (hasDel || hasEdits || hasNew) {
          if (hasDel) {
            for (const hid of hs.stagedDeletes) {
              const url = API(`/api/clients/${encodeURIComponent(clientId)}/hospitals/${encodeURIComponent(hid)}`);
              if (APILOG) console.log('[OPEN_CLIENT] DELETE hospital →', url);
              const res = await authFetch(url, { method: 'DELETE' });
              if (!res.ok) throw new Error(await res.text());
            }
          }

          if (hasEdits) {
            for (const [hid, patchRaw] of Object.entries(hs.stagedEdits)) {
              const patch = {};
              if (patchRaw && Object.prototype.hasOwnProperty.call(patchRaw,'hospital_name_norm')) {
                const name = String(patchRaw.hospital_name_norm || '').trim();
                if (!name) throw new Error('Hospital name cannot be blank.');
                patch.hospital_name_norm = name;
              }
              if (patchRaw && Object.prototype.hasOwnProperty.call(patchRaw,'ward_hint')) {
                const hint = String(patchRaw.ward_hint ?? '').trim();
                patch.ward_hint = hint === '' ? null : hint;
              }
              if (Object.keys(patch).length === 0) continue;
              const url = API(`/api/clients/${encodeURIComponent(clientId)}/hospitals/${encodeURIComponent(hid)}`);
              if (APILOG) console.log('[OPEN_CLIENT] PATCH hospital →', url, patch);
              const res = await authFetch(url, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(patch) });
              if (!res.ok) throw new Error(await res.text());
            }
          }

          if (hasNew) {
            for (const n of hs.stagedNew) {
              const body = { hospital_name_norm: String(n?.hospital_name_norm || '').trim(), ward_hint: (String(n?.ward_hint ?? '').trim() || null) };
              if (!body.hospital_name_norm) throw new Error('Hospital name cannot be blank.');
              const url = API(`/api/clients/${encodeURIComponent(clientId)}/hospitals`);
              if (APILOG) console.log('[OPEN_CLIENT] POST hospital →', url, body);
              const res = await authFetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
              if (!res.ok) throw new Error(await res.text());
            }
          }

          // ⬇️ Cleanup (type-agnostic)
          window.modalCtx.hospitalsState.stagedDeletes = new Set();
          window.modalCtx.hospitalsState.stagedEdits   = {};
          window.modalCtx.hospitalsState.stagedNew     = [];

          // Refresh visible list
          try {
            const fresh = await listClientHospitals(clientId);
            window.modalCtx.hospitalsState.existing = Array.isArray(fresh) ? fresh : [];
            try { renderClientHospitalsTable(); } catch {}
          } catch (e) {
            W('[OPEN_CLIENT] hospitals refresh failed', e);
          }
        }
      } catch (err) {
        alert(`Failed to save Hospitals & wards: ${String(err?.message || err)}`);
        return { ok:false };
      }

      // 6) DELETE staged client windows BEFORE toggles/edits/inserts
      try {
        const allWindows = Array.isArray(window.modalCtx.ratesState) ? window.modalCtx.ratesState : [];
        L('[onSave] delete phase', { windows: allWindows.length, stagedDeleteSet: window.modalCtx.ratesStagedDeletes?.size || 0 });
        // Union of flags and Set
        const setIds = (window.modalCtx.ratesStagedDeletes instanceof Set) ? new Set([...window.modalCtx.ratesStagedDeletes]) : new Set();
        for (const w of allWindows) if (w && w.id && w.__delete === true) setIds.add(String(w.id));

        if (setIds.size) {
          if (APILOG) console.log('[OPEN_CLIENT] deleting staged client windows', [...setIds]);
          for (const id of setIds) {
            const url = API(`/api/rates/client-defaults/${encodeURIComponent(id)}`);
            const res = await authFetch(url, { method: 'DELETE' });
            if (res.status === 404) continue;
            if (!res.ok) {
              const body = await res.text().catch(()=> '');
              if (res.status === 409) alert(`Delete blocked: ${body || 'Associated data prevents deletion.'}`);
              else alert(`Failed to delete client rate window: ${body || res.status}`);
              return { ok:false };
            }
          }
          window.modalCtx.ratesState    = allWindows.filter(w => !(w && w.id && setIds.has(String(w.id))));
          window.modalCtx.ratesBaseline = (Array.isArray(window.modalCtx.ratesBaseline) ? window.modalCtx.ratesBaseline : [])
                                          .filter(b => !(b && b.id && setIds.has(String(b.id))));
          if (window.modalCtx.ratesStagedDeletes instanceof Set) {
            for (const id of setIds) window.modalCtx.ratesStagedDeletes.delete(id);
          }
        }
      } catch (errDel) {
        alert(`Failed to process deletions: ${String(errDel?.message || errDel || '')}`);
        return { ok:false };
      }

      // 7) Apply status toggles
      const baselineRates = Array.isArray(window.modalCtx.ratesBaseline) ? window.modalCtx.ratesBaseline : [];
      const prevById = new Map(baselineRates.filter(r => r && r.id).map(r => [String(r.id), r]));
      let windows = Array.isArray(window.modalCtx.ratesState) ? window.modalCtx.ratesState.slice() : [];

      const toggles = [];
      for (const w of windows) {
        if (!w?.id) continue;
        const prev = prevById.get(String(w.id));
        if (!prev) continue;
        if (!!prev.disabled_at_utc !== !!w.disabled_at_utc) toggles.push({ id: w.id, disabled: !!w.disabled_at_utc });
      }
      L('[onSave] toggle phase', { togglesCount: toggles.length, windows: windows.length, baseline: baselineRates.length });
      if (toggles.length) {
        if (APILOG) console.log('[OPEN_CLIENT] applying toggles', toggles);
        for (const t of toggles) {
          try { await patchClientDefault(t.id, { disabled: t.disabled }); }
          catch (e) {
            const msg = String(e?.message || e || '');
            if (msg.includes('duplicate')) alert('Cannot enable this window: another enabled window already starts on the same date for the same role/band.');
            else alert(`Failed to update status: ${msg}`);
            return { ok:false };
          }
        }
      }

      // 8) Final negative-margin guard (skip disabled)
      const erniMult = (async ()=> {
        if (typeof window.__ERNI_MULT__ === 'number') return window.__ERNI_MULT__;
        try {
          if (typeof getSettingsCached === 'function') {
            const s = await getSettingsCached();
            let p = s?.erni_pct ?? s?.employers_ni_percent ?? 0;
            p = Number(p) || 0; if (p > 1) p = p/100;
            window.__ERNI_MULT__ = 1 + p; return window.__ERNI_MULT__;
          }
        } catch {}
        return 1;
      })();
      const mult = await erniMult;
      for (const w of windows) {
        if (w.disabled_at_utc) continue;
        for (const b of ['day','night','sat','sun','bh']) {
          const chg  = w[`charge_${b}`], paye = w[`paye_${b}`], umb = w[`umb_${b}`];
          if (chg != null && paye != null && (chg - (paye * mult)) < 0) { alert(`PAYE margin would be negative for ${w.role}${w.band?` / ${w.band}`:''} (${b.toUpperCase()}). Fix before saving.`); return { ok:false }; }
          if (chg != null && umb  != null && (chg - umb) < 0)            { alert(`Umbrella margin would be negative for ${w.role}${w.band?` / ${w.band}`:''} (${b.toUpperCase()}). Fix before saving.`); return { ok:false }; }
        }
      }

      // 9) UPDATE existing, POST new (skip disabled)
      const toUpdate = windows.filter(w => w.id && !w.disabled_at_utc);
      const toCreate = windows.filter(w => !w.id && !w.disabled_at_utc);
      L('[onSave] upsert phase', { toUpdate: toUpdate.length, toCreate: toCreate.length });

      const buildBody = (w) => ({
        client_id : clientId,
        role      : w.role || '',
        band      : w.band ?? null,
        date_from : w.date_from || null,
        date_to   : w.date_to ?? null,

        charge_day   : w.charge_day   ?? null,
        charge_night : w.charge_night ?? null,
        charge_sat   : w.charge_sat   ?? null,
        charge_sun   : w.charge_sun   ?? null,
        charge_bh    : w.charge_bh    ?? null,

        paye_day     : w.paye_day     ?? null,
        paye_night   : w.paye_night   ?? null,
        paye_sat     : w.paye_sat     ?? null,
        paye_sun     : w.paye_sun     ?? null,
        paye_bh      : w.paye_bh      ?? null,

        umb_day      : w.umb_day      ?? null,
        umb_night    : w.umb_night    ?? null,
        umb_sat      : w.umb_sat      ?? null,
        umb_sun      : w.umb_sun      ?? null,
        umb_bh       : w.umb_bh       ?? null
      });

      // PUT updates
      for (const w of toUpdate) {
        if (!w.id) continue;
        try {
          const url = API(`/api/rates/client-defaults/${encodeURIComponent(w.id)}`);
          if (APILOG) console.log('[OPEN_CLIENT] PUT client-default window →', w.id);
          const res = await authFetch(url, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(buildBody(w))
          });
          if (!res.ok) {
            const body = await res.text().catch(()=> '');
            alert(`Failed to update client rate window: ${body || res.status}`);
            return { ok:false };
          }
        } catch (e) {
          alert(`Failed to update a client rate window: ${String(e?.message || e)}`); return { ok:false };
        }
      }

      // POST creates
      for (const w of toCreate) {
        try {
          if (APILOG) console.log('[OPEN_CLIENT] POST client-default window →', w);
          await upsertClientRate(buildBody(w));
        } catch (e) {
          E('Upsert client default window failed', w, e);
          alert('Failed to create a client rate window. See console for details.');
          return { ok:false };
        }
      }

      // 10) Refresh & rebuild baseline
      try {
        const refreshed = await listClientRates(clientId /* all incl. disabled */);
        const stagedDelIds = (window.modalCtx.ratesStagedDeletes instanceof Set) ? window.modalCtx.ratesStagedDeletes : new Set();
        window.modalCtx.ratesState = (Array.isArray(refreshed) ? refreshed.map(x => ({ ...x })) : []).map(x => {
          if (stagedDelIds.has(String(x.id))) x.__delete = true;
          return x;
        });
        window.modalCtx.ratesBaseline = JSON.parse(JSON.stringify(window.modalCtx.ratesState));
        L('[onSave] post-refresh', { refreshed: window.modalCtx.ratesState.length });
        try { renderClientRatesTable(); } catch {}
      } catch (e) {
        W('[openClient] post-save refresh failed', e);
      }

      window.modalCtx.formState = { __forId: clientId, main:{} };
      // ⬇️ FIX: reuse existing isNew; do not redeclare
      if (isNew) window.__pendingFocus = { section: 'clients', id: clientId };
      L('[onSave] EXIT ok=true');
      return { ok: true, saved: window.modalCtx.data };
    },
    full?.id
  );

  // 4) Post-paint async loads (merge metadata; preserve delete flags)
  if (full?.id) {
    const token = window.modalCtx.openToken;
    const id    = full.id;
    try {
      const unified = await listClientRates(id);
      if (token === window.modalCtx.openToken && window.modalCtx.data?.id === id) {
        const stagedDelIds = (window.modalCtx.ratesStagedDeletes instanceof Set) ? window.modalCtx.ratesStagedDeletes : new Set();
        const hasStaged = Array.isArray(window.modalCtx.ratesState) && window.modalCtx.ratesState.length > 0;
        if (!hasStaged) {
          window.modalCtx.ratesState = (Array.isArray(unified) ? unified.map(r => ({ ...r })) : []).map(r => {
            if (stagedDelIds.has(String(r.id))) r.__delete = true;
            return r;
          });
          window.modalCtx.ratesBaseline = JSON.parse(JSON.stringify(window.modalCtx.ratesState));
        } else {
          const staged = Array.isArray(window.modalCtx.ratesState) ? window.modalCtx.ratesState.slice() : [];
          const stagedById = new Map(staged.map(r => [String(r.id), r]));
          (Array.isArray(unified) ? unified : []).forEach(srv => {
            const s = stagedById.get(String(srv.id));
            if (s) {
              s.disabled_at_utc  = srv.disabled_at_utc ?? null;
              s.disabled_by_name = srv.disabled_by_name ?? null;
              if (stagedDelIds.has(String(srv.id))) s.__delete = true;
            } else {
              const row = { ...srv };
              if (stagedDelIds.has(String(row.id))) row.__delete = true;
              staged.push(row);
            }
          });
          window.modalCtx.ratesState = staged;
        }
        L('[POST-PAINT] rates merged', { count: window.modalCtx.ratesState.length });
        try { renderClientRatesTable(); } catch {}
      }
    } catch (e) { W('openClient POST-PAINT rates error', e); }

    try {
      const freshHosp = await listClientHospitals(id);
      if (token === window.modalCtx.openToken && window.modalCtx.data?.id === id) {
        window.modalCtx.hospitalsState.existing = Array.isArray(freshHosp) ? freshHosp : [];
        try { renderClientHospitalsTable(); } catch {}
      }
    } catch (e) { W('openClient POST-PAINT hospitals error', e); }

  } else {
    L('skip companion loads (no full.id)');
  }
}


function ensureSelectionStyles(){
  const ID = 'gridSelectionStyles';
  if (document.getElementById(ID)) return;
  const style = document.createElement('style');
  style.id = ID;
  style.textContent = `
    /* Subtle selected-row highlight — readable, not shouty */
    .grid tbody tr.selected {
      background: rgba(30,136,229,0.12) !important;  /* soft blue tint */
      color: inherit !important;                     /* keep text color */
      box-shadow: inset 0 0 0 1px rgba(29,78,216,.35); /* delicate rim */
    }
    .grid tbody tr.selected td { color: inherit !important; }

    /* Hover stays understated */
    .grid tbody tr:hover {
      background: rgba(0,0,0,0.04);
    }
  `;
  document.head.appendChild(style);
}

function renderClientTab(key, row = {}){
  if (key==='main') return html(`
    <div class="form" id="tab-main">
      ${input('name','Client name', row.name)}

      <!-- CLI: display-only, never posted -->
      <div class="row">
        <label>Client Ref (CLI-…)</label>
        <input id="cli_ref_display"
               value="${row.cli_ref ? String(row.cli_ref) : 'Awaiting CLI number from server'}"
               disabled
               readonly
               style="opacity:.7" />
      </div>

      <div class="row" style="grid-column:1/-1"><label>Invoice address</label><textarea name="invoice_address">${row.invoice_address || ''}</textarea></div>
      ${input('primary_invoice_email','Primary invoice email', row.primary_invoice_email,'email')}
      ${input('ap_phone','A/P phone', row.ap_phone)}
      ${select('vat_chargeable','VAT chargeable', row.vat_chargeable? 'Yes' : 'No', ['Yes','No'])}
      ${input('payment_terms_days','Payment terms (days)', row.payment_terms_days || 30, 'number')}
    </div>
  `);

  if (key==='rates')     return html(`<div id="clientRates"></div>`);
  if (key==='settings')  return html(`<div id="clientSettings"></div>`);

  if (key==='hospitals') {
    // Ensure initial render AND first-mount fetch if needed, then render the table
    setTimeout(async () => {
      try {
        const id = window.modalCtx?.data?.id || null;
        const hs = window.modalCtx?.hospitalsState || {};
        const hasExisting = Array.isArray(hs.existing) && hs.existing.length > 0;

        if (id && !hasExisting) {
          const fresh = await listClientHospitals(id);
          if (window.modalCtx?.data?.id === id) {
            window.modalCtx.hospitalsState.existing = Array.isArray(fresh) ? fresh : [];
          }
        }
        try { renderClientHospitalsTable(); } catch {}
      } catch (_) {}
    }, 0);

    return html(`<div id="clientHospitals"></div>`);
  }

  return '';
}


// ===========================
// 5) mountCandidatePayTab(...)
// (auto-populate Umbrella or PAYE bank fields + logging)
// ===========================
// ========== PAY TAB (just extra logs; logic unchanged) ==========
// This function now RESOLVES ONLY AFTER prefill (when umbrella_id is known).
// In your modal's setTab/renderTab: `return mountCandidatePayTab();` and await it.
async function mountCandidatePayTab(){
  const LOG = !!window.__LOG_PAYTAB;
  const fr = (window.__modalStack || [])[ (window.__modalStack || []).length - 1 ] || null;
  const mode = fr ? fr.mode : 'view';
  const isEdit = (mode === 'edit' || mode === 'create');
  if (LOG) console.log('[PAYTAB] ENTRY', { mode, isEdit });

  const payMethod   = (window.modalCtx?.payMethodState || window.modalCtx?.data?.pay_method || 'PAYE').toUpperCase();
  const currentUmbId = window.modalCtx?.data?.umbrella_id || '';

  const umbRow    = document.getElementById('umbRow');
  const nameInput = document.getElementById('umbrella_name');
  const listEl    = document.getElementById('umbList');
  const idHidden  = document.getElementById('umbrella_id');

  const accHolder = document.querySelector('#tab-pay input[name="account_holder"]');
  const bankName  = document.querySelector('#tab-pay input[name="bank_name"]');
  const sortCode  = document.querySelector('#tab-pay input[name="sort_code"]');
  const accNum    = document.querySelector('#tab-pay input[name="account_number"]');

  function setBankDisabled(disabled) {
    [accHolder, bankName, sortCode, accNum].forEach(el => { if (el) el.disabled = !!disabled; });
    const umbInput = document.getElementById('umbrella_name'); if (umbInput) umbInput.disabled = false;
    try { window.__BANK_FIELDS_DISABLED__ = !!disabled; } catch {}
    if (LOG) console.log('[PAYTAB] setBankDisabled', disabled);
  }

  const unwrapList = (data) => {
    if (Array.isArray(data)) return data;
    if (data && Array.isArray(data.items)) return data.items;
    if (data && Array.isArray(data.rows))  return data.rows;
    if (data && Array.isArray(data.data))  return data.data;
    return [];
  };
  function unwrapSingle(json) {
    if (!json) return null;
    if (Array.isArray(json)) return json[0] || null;
    if (json.data && Array.isArray(json.data)) return json.data[0] || null;
    if (Array.isArray(json.rows)) return json.rows[0] || null;
    if (json.item && typeof json.item === 'object' && !Array.isArray(json.item)) return json.item;
    const keys = Object.keys(json);
    if (keys.length === 1) {
      const only = json[keys[0]];
      if (only && typeof only === 'object' && !Array.isArray(only)) return only;
    }
    return json;
  }
  const normaliseSort = (v) => { if (!v) return ''; const digits = String(v).replace(/\D+/g, '').slice(0,6); if (digits.length !== 6) return v; return digits.replace(/(\d{2})(\d{2})(\d{2})/, '$1-$2-$3'); };

  function fillFromCandidate() {
    const d = window.modalCtx?.data || {};
    if (accHolder) accHolder.value = d.account_holder || '';
    if (bankName)  bankName.value  = d.bank_name || '';
    if (sortCode)  sortCode.value  = normaliseSort(d.sort_code || '');
    if (accNum)    accNum.value    = d.account_number || '';
    if (LOG) console.log('[PAYTAB] fillFromCandidate', { account_holder: accHolder?.value, bank_name: bankName?.value, sort_code: sortCode?.value, account_number: accNum?.value });
  }

  async function fetchUmbrellaById(id) {
    try {
      const res = await authFetch(API(`/api/umbrellas/${encodeURIComponent(id)}`));
      if (!res || !res.ok) return null;
      const json = await res.json().catch(() => null);
      const row = json && (json.umbrella || unwrapSingle(json));
      return row || null;
    } catch (_) { return null; }
  }
  function prefillUmbrellaBankFields(umb) {
    if (!umb) return;
    try {
      if (window.__LAST_UMB_PREFILL_ID__ && String(window.__LAST_UMB_PREFILL_ID__) === String(umb.id || '')) return;
      window.__LAST_UMB_PREFILL_ID__ = umb.id || null;
    } catch {}
    const bank = umb.bank_name || umb.bank || umb.bankName || '';
    const sc   = umb.sort_code || umb.bank_sort_code || umb.sortCode || '';
    const an   = umb.account_number || umb.bank_account_number || umb.accountNumber || '';
    const ah   = umb.name || umb.account_holder || umb.bank_account_name || umb.accountHolder || '';
    if (bankName)  bankName.value  = bank;
    if (sortCode)  sortCode.value  = normaliseSort(sc);
    if (accNum)    accNum.value    = an;
    if (accHolder) accHolder.value = ah;
    if (nameInput && !nameInput.value) nameInput.placeholder = umb.name || nameInput.placeholder || '';
    if (LOG) console.log('[PAYTAB] prefillUmbrellaBankFields', { umb_id: umb.id, name: umb.name, bank, sc, an, ah });
  }
  async function fetchAndPrefill(id) {
    if (!id) return;
    const umb = await fetchUmbrellaById(id);
    if (umb) {
      if (idHidden) idHidden.value = umb.id || idHidden.value || '';
      prefillUmbrellaBankFields(umb);
    } else if (LOG) {
      console.warn('[PAYTAB] fetchAndPrefill: umbrella not found', id);
    }
  }

  function syncUmbrellaSelection() {
    const val = (nameInput && nameInput.value) ? nameInput.value.trim() : '';
    if (!val) {
      if (idHidden) idHidden.value = '';
      if (LOG) console.log('[PAYTAB] selection cleared');
      return;
    }
    const allOpts = Array.from((listEl && listEl.options) ? listEl.options : []);
    const hitOpt = allOpts.find(o => o.value === val);
    const id = hitOpt && hitOpt.getAttribute('data-id');
    if (id) {
      if (LOG) console.log('[PAYTAB] selected umbrella', { label: val, id });
      if (idHidden) idHidden.value = id;
      fetchAndPrefill(id);
    } else {
      if (LOG) console.warn('[PAYTAB] no exact label match; clearing id & bank fields');
      if (idHidden) idHidden.value = '';
      if (bankName) bankName.value = '';
      if (sortCode) sortCode.value = '';
      if (accNum)   accNum.value = '';
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Deterministic prefill: await known-umbrella fetch BEFORE returning
  // ─────────────────────────────────────────────────────────────
  if (payMethod === 'UMBRELLA') {
    if (umbRow) umbRow.style.display = '';
    setBankDisabled(true);

    // Remember any typed label at mount (for later list-matching when NO id)
    const typedAtMount = (nameInput && nameInput.value) ? nameInput.value.trim() : '';

    // 1) If we KNOW the umbrella id, PREFILL and AWAIT before returning.
    if (currentUmbId) {
      if (LOG) console.log('[PAYTAB] deterministic prefill by umbrella_id', currentUmbId);
      await fetchAndPrefill(currentUmbId);
    }

    // 2) Load umbrella list in parallel (NOT awaited) for the datalist & label→id case.
    (async () => {
      let umbrellas = [];
      try {
        const res = await authFetch(API('/api/umbrellas'));
        if (res && res.ok) {
          const j = await res.json().catch(()=>[]);
          umbrellas = unwrapList(j);
        }
      } catch (_) { umbrellas = []; }
      if (LOG) console.log('[PAYTAB] umbrellas list loaded', umbrellas.length);

      if (listEl) {
        listEl.innerHTML = (umbrellas || []).map(u => {
          const label = u.name || u.remittance_email || u.id;
          return `<option data-id="${u.id}" value="${label}"></option>`;
        }).join('');
      }

      // If we DIDN'T have an id at mount but user already had a typed label, try to map it now.
      if (!currentUmbId && typedAtMount && umbrellas.length) {
        const hit = umbrellas.find(u => (u.name || '').trim() === typedAtMount);
        if (hit) {
          if (LOG) console.log('[PAYTAB] post-list typed match', { typedAtMount, id: hit.id });
          if (idHidden) idHidden.value = hit.id;
          fetchAndPrefill(hit.id);
        }
      }
    })().catch(()=>{});

    // Wiring for user selection & future changes
    if (nameInput) {
      nameInput.disabled = !isEdit;
      nameInput.oninput = syncUmbrellaSelection;
      nameInput.onchange = syncUmbrellaSelection;
    }
    if (idHidden) idHidden.addEventListener('change', () => fetchAndPrefill(idHidden.value));

    const onPmChanged = () => {
      const pm = (window.modalCtx?.payMethodState || window.modalCtx?.data?.pay_method || 'PAYE').toUpperCase();
      if (LOG) console.log('[PAYTAB] pay-method-changed', pm);
      if (pm !== 'UMBRELLA') {
        if (umbRow) umbRow.style.display = 'none';
        setBankDisabled(!isEdit);
        fillFromCandidate();
      } else {
        if (umbRow) umbRow.style.display = '';
        setBankDisabled(true);
        const id = (idHidden && idHidden.value) ? idHidden.value : (window.modalCtx?.data?.umbrella_id || '');
        if (id) fetchAndPrefill(id);
      }
    };
    try { window.addEventListener('pay-method-changed', onPmChanged, { once: true }); } catch {}

  } else {
    if (umbRow) umbRow.style.display = 'none';
    setBankDisabled(!isEdit);
    if (nameInput && idHidden) { nameInput.value = ''; idHidden.value = ''; }
    // PAYE path is immediate (no awaits needed)
    fillFromCandidate();
  }

  // IMPORTANT: The function is async; it resolves AFTER the umbrella prefill (when known).
  // If no umbrella_id or pay method isn’t UMBRELLA, it resolves immediately here.
}

// ============================================================================
// CALENDAR – SHARED HELPERS & STATE
// ============================================================================

window.__calState = window.__calState || {};     // per contract_id: { view, win, weekEndingWeekday }
window.__candCalState = window.__candCalState || {}; // per candidate_id
window.__calStage = window.__calStage || {};     // staged changes per contract_id

// ---------- Date utilities ----------
function ymd(d) { return (typeof d === 'string') ? d.slice(0,10) : (new Date(d)).toISOString().slice(0,10); }
function ymdToDate(ymdStr) { return new Date(ymdStr + 'T00:00:00Z'); }
function dateToYmd(dt) { return dt.toISOString().slice(0,10); }
// define once, both as window prop and local alias
// 1) Ensure a single global + local alias for toYmd
window.toYmd = window.toYmd || (
  (typeof dateToYmd === 'function')
    ? dateToYmd
    : (d) => {
        const y = d.getUTCFullYear();
        const m = String(d.getUTCMonth() + 1).padStart(2, '0');
        const day = String(d.getUTCDate()).padStart(2, '0');
        return `${y}-${m}-${day}`;
      }
);
var toYmd = window.toYmd; // avoids redeclare on re-exec

// 2) Define addDays (and expose globally)
window.addDays = window.addDays || function(ymdStr, delta){
  const d = ymdToDate(ymdStr);
  d.setUTCDate(d.getUTCDate() + Number(delta || 0));
  return toYmd(d);
};
var addDays = window.addDays; // optional local alias

function enumerateDates(fromYmd, toYmd) {
  const out = []; let d = ymdToDate(fromYmd), end = ymdToDate(toYmd);
  while (d <= end) { out.push(dateToYmd(d)); d.setUTCDate(d.getUTCDate() + 1); }
  return out;
}
function computeYearWindow(year)   { return { from: `${year}-01-01`, to: `${year}-12-31` }; }
function monthBounds(year, monthIndex) { const s = new Date(Date.UTC(year, monthIndex, 1)); const e = new Date(Date.UTC(year, monthIndex+1, 0)); return { from: dateToYmd(s), to: dateToYmd(e) }; }
function computeMonthWindow(year, monthIndex) { return monthBounds(year, monthIndex); }
function stepMonth(win, delta) { const s = ymdToDate(win.from); const n = new Date(Date.UTC(s.getUTCFullYear(), s.getUTCMonth()+delta, 1)); return monthBounds(n.getUTCFullYear(), n.getUTCMonth()); }

// ---------- Week ending ----------
function computeWeekEnding(ymdStr, weekEndingWeekday /* 0=Sun..6=Sat */) {
  const d = ymdToDate(ymdStr); const dow = d.getUTCDay(); const delta = (weekEndingWeekday - dow + 7) % 7; d.setUTCDate(d.getUTCDate() + delta); return dateToYmd(d);
}

// ---------- Colors / states ----------
function colorForState(state) {
  const s = String(state || 'EMPTY').toUpperCase();
  if (s === 'PLANNED')    return 'cal-planned';
  if (s === 'SUBMITTED')  return 'cal-submitted';
  if (s === 'AUTHORISED') return 'cal-authorised';
  if (s === 'INVOICED')   return 'cal-invoiced';
  if (s === 'PAID')       return 'cal-paid';
  return ''; // EMPTY -> white
}

// ---------- Selection helpers ----------
function initSelBucket(bucketKey) {
  const store = (bucketKey === 'cand') ? window.__candSel : window.__calSel;
  if (!store) {
    if (bucketKey === 'cand') window.__candSel = { set: new Set(), anchor: null };
    else window.__calSel = { set: new Set(), anchor: null };
  }
  return (bucketKey === 'cand') ? window.__candSel : window.__calSel;
}
function toggleDaySelected(bucketKey, ymdStr, additive = false) {
  const sel = initSelBucket(bucketKey);
  if (!additive) sel.set.clear();
  if (sel.set.has(ymdStr) && additive) sel.set.delete(ymdStr); else sel.set.add(ymdStr);
  sel.anchor = ymdStr;
}
function selectRange(bucketKey, fromYmd, toYmd, additive = false) {
  const sel = initSelBucket(bucketKey);
  if (!additive) sel.set.clear();
  const lo = (fromYmd < toYmd) ? fromYmd : toYmd;
  const hi = (fromYmd < toYmd) ? toYmd   : fromYmd;
  enumerateDates(lo, hi).forEach(d => sel.set.add(d));
}
function clearCalendarSelection(bucketKey) {
  const sel = initSelBucket(bucketKey); sel.set.clear(); sel.anchor = null;
}
function computeSelectionBounds(selection /* array of ymd */) {
  if (!selection?.length) return null;
  let lo = selection[0], hi = selection[0];
  for (const d of selection) { if (d < lo) lo = d; if (d > hi) hi = d; }
  return { from: lo, to: hi };
}

// ---------- Index builders ----------
function buildDateIndex(items) { const map = new Map(); for (const it of (items||[])) { const k = it.date; const arr = map.get(k) || []; arr.push(it); map.set(k, arr); } return map; }
function buildWeekIndex(weeks) {
  const m = new Map();
  for (const w of (weeks||[])) {
    const key = w.week_ending_date;
    const e = m.get(key) || { baseWeekId: null, baseHasTs: false, baseMode: 'ELECTRONIC', siblings: [] };
    e.siblings.push(w);
    if (Number(w.additional_seq||0) === 0) { e.baseWeekId = w.id; e.baseHasTs  = !!w.timesheet_id; e.baseMode   = w.submission_mode_snapshot || 'ELECTRONIC'; }
    m.set(key, e);
  }
  return m;
}

// ---------- Staging state ----------

function getContractCalendarStageState(contractId) {
  return (window.__calStage[contractId] ||= {
    add: new Set(),
    remove: new Set(),
    additional: {},
    weekEndingWeekday: (window.__calState[contractId]?.weekEndingWeekday || 0),
    removeAll: null
  });
}
function clearContractCalendarStageState(contractId) {
  window.__calStage[contractId] = {
    add: new Set(),
    remove: new Set(),
    additional: {},
    weekEndingWeekday: (window.__calState[contractId]?.weekEndingWeekday || 0),
    removeAll: null
  };
}
async function discardContractCalendarStage(contractId) {
  clearContractCalendarStageState(contractId);
  const last = window.__calState?.[contractId]?.win || computeYearWindow((new Date()).getUTCFullYear());
  await fetchAndRenderContractCalendar(contractId, { from: last.from, to: last.to, view: window.__calState?.[contractId]?.view || 'year' });
  return { ok:true };
}




function stageContractCalendarBookings(contractId, dates /* array of ymd */) {
  const st = getContractCalendarStageState(contractId);
  for (const d of dates) { st.remove.delete(d); st.add.add(d); }
}
function stageContractCalendarUnbookings(contractId, dates /* array of ymd */) {
  const st = getContractCalendarStageState(contractId);
  for (const d of dates) { st.add.delete(d); st.remove.add(d); }
}
function stageContractCalendarAdditional(contractId, baseWeekId, dates /* array of ymd */) {
  const st = getContractCalendarStageState(contractId);
  const set = (st.additional[baseWeekId] ||= new Set());
  for (const d of dates) set.add(d);
}

// Overlay staged colors onto fetched items (without persisting)
function applyStagedContractCalendarOverlay(contractId, itemsByDate /* Map<date, [items]> */, weekIndex) {
  const st = getContractCalendarStageState(contractId);
  const overlay = new Map(itemsByDate ? itemsByDate : []);
  const addDates = [...st.add];
  const remDates = [...st.remove];

  const ensureArr = (k) => { const a = overlay.get(k) || []; overlay.set(k, a); return a; };
  const strong = (x) => ['SUBMITTED','AUTHORISED','INVOICED','PAID'].includes(String(x.state||'EMPTY').toUpperCase());

  if (st.removeAll && (st.removeAll.from || st.removeAll.to)) {
    const from = st.removeAll.from || '0000-01-01';
    const to   = st.removeAll.to   || '9999-12-31';
    const dates = enumerateDates(from, to);
    for (const d of dates) {
      const we = computeWeekEnding(d, st.weekEndingWeekday || 0);
      let wi = weekIndex.get(we);

      if (!wi) {
        const weStart = addDays(we, -6);
        let hasStrongForThis = false;
        for (let i=0;i<7;i++){
          const dd = addDays(weStart, i);
          const arr = overlay.get(dd) || [];
          if (arr.some(x => String(x.contract_id||'')===String(contractId) && strong(x))) { hasStrongForThis = true; break; }
        }
        if (hasStrongForThis) {
          wi = { baseHasTs: true };
        } else {
          wi = { baseHasTs: false };
        }
      }

      const arr = ensureArr(d);
      if (wi.baseHasTs) continue;
      const filtered = arr.filter(x => !(String(x.contract_id || '') === String(contractId)
                                      && String(x.state || 'EMPTY').toUpperCase() === 'PLANNED'));
      overlay.set(d, filtered);
    }
  }

  for (const d of addDates) {
    const arr = ensureArr(d);
    const hasStrong = arr.some(strong);
    if (!hasStrong) {
      arr.push({ date:d, state:'PLANNED', contract_id: contractId });
    }
  }

  for (const d of remDates) {
    const arr = ensureArr(d);
    const top = topState(arr);
    if (top === 'PLANNED') overlay.set(d, []);
  }

  for (const [baseWeekId, set] of Object.entries(st.additional)) {
    for (const d of set) {
      const arr = ensureArr(d);
      const hasStrong = arr.some(strong);
      if (!hasStrong) {
        arr.push({ date:d, state:'PLANNED', contract_id: contractId });
      }
    }
  }

  return overlay;
}

// NEW — stage "Add missing weeks" (preview only)
// Decides dates from bounds and current template (std_schedule_json / __template),
// adds them into st.add; UI repaints via fetchAndRenderContractCalendar.
async function stageAddMissingWeeks(contractId, bounds) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][stageAddMissingWeeks]', ...a); };

  const st = getContractCalendarStageState(contractId);

  const rawFrom = bounds?.from || window.modalCtx?.data?.start_date || null;
  const rawTo   = bounds?.to   || window.modalCtx?.data?.end_date   || null;
  if (!rawFrom || !rawTo) return { ok:false, reason:'no-bounds' };

  const from = (rawFrom.includes('/') && typeof parseUkDateToIso === 'function') ? (parseUkDateToIso(rawFrom) || rawFrom) : rawFrom;
  const to   = (rawTo.includes('/')   && typeof parseUkDateToIso === 'function') ? (parseUkDateToIso(rawTo)   || rawTo)   : rawTo;

  let template = (window.modalCtx?.data?.std_schedule_json && typeof window.modalCtx.data.std_schedule_json === 'object')
    ? window.modalCtx.data.std_schedule_json
    : null;
  if (!template) {
    try {
      const fsT = window.modalCtx?.formState?.main?.__template;
      if (fsT && typeof fsT === 'object') template = fsT;
    } catch {}
  }
  if (!template || typeof template !== 'object') return { ok:false, reason:'no-template' };

  const activeDows = new Set();
  const valid = (d) => d && typeof d.start === 'string' && d.start && typeof d.end === 'string' && d.end;
  if (valid(template.sun)) activeDows.add(0);
  if (valid(template.mon)) activeDows.add(1);
  if (valid(template.tue)) activeDows.add(2);
  if (valid(template.wed)) activeDows.add(3);
  if (valid(template.thu)) activeDows.add(4);
  if (valid(template.fri)) activeDows.add(5);
  if (valid(template.sat)) activeDows.add(6);

  st.removeAll = null;

  const days = enumerateDates(from, to);
  let added = 0;
  for (const d of days) {
    const dow = ymdToDate(d).getUTCDay();
    if (!activeDows.has(dow)) continue;
    st.remove.delete(d);
    if (!st.add.has(d)) { st.add.add(d); added++; }
  }

  try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
  L('staged add-missing', { from, to, added });
  return { ok:true, added, from, to };
}


function topState(arr) {
  if (!arr?.length) return 'EMPTY';
  const prio = { EMPTY:0, PLANNED:1, SUBMITTED:2, AUTHORISED:3, INVOICED:4, PAID:5 };
  let s = 'EMPTY';
  for (const it of arr) { const st = String(it.state||'EMPTY').toUpperCase(); if ((prio[st]||0) > (prio[s]||0)) s = st; }
  return s;
}

// Build payloads for commit
function buildPlanRangesFromStage(contractId) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][buildRanges]', ...a); };

  const st = getContractCalendarStageState(contractId);
  const adds = [...st.add].sort();
  const rems = [...st.remove].sort();

  const boundsOf = (arr) => computeSelectionBounds(arr);

  const isConsecutiveDailyRun = (arr) => {
    if (arr.length < 2) return false;
    const ONE = 24*60*60*1000;
    for (let i = 1; i < arr.length; i++) {
      const prev = new Date(arr[i-1] + 'T00:00:00Z').getTime();
      const curr = new Date(arr[i]   + 'T00:00:00Z').getTime();
      if ((curr - prev) !== ONE) return false;
    }
    return true;
  };

  // Current contract window in modal (ISO YYYY-MM-DD)
  const contract = (window.modalCtx && window.modalCtx.data) ? window.modalCtx.data : {};
  const contractStart = contract?.start_date || null;
  const contractEnd   = contract?.end_date   || null;

  // Template (std_schedule_json) if present
  let template = (window.modalCtx?.data?.std_schedule_json && typeof window.modalCtx.data.std_schedule_json === 'object')
    ? window.modalCtx.data.std_schedule_json
    : null;
  if (!template) {
    try {
      const fsT = window.modalCtx?.formState?.main?.__template;
      if (fsT && typeof fsT === 'object') template = fsT;
    } catch {}
  }

  const activeDows = (() => {
    const s = new Set();
    if (!template) return s;
    const valid = (d) => d && typeof d.start === 'string' && d.start && typeof d.end === 'string' && d.end;
    if (valid(template.sun)) s.add(0);
    if (valid(template.mon)) s.add(1);
    if (valid(template.tue)) s.add(2);
    if (valid(template.wed)) s.add(3);
    if (valid(template.thu)) s.add(4);
    if (valid(template.fri)) s.add(5);
    if (valid(template.sat)) s.add(6);
    return s;
  })();

  // If 'Remove All' was staged, build a single removal range across the (possibly current) contract window.
  if (st.removeAll) {
    const removeRanges = [];
    const from = st.removeAll.from || contractStart || null;
    const to   = st.removeAll.to   || contractEnd   || null;
    removeRanges.push({ from, to, days: [] });
    L('removeRanges (removeAll)', { bounds: { from, to } });
    return {
      addRanges: [],
      removeRanges,
      additionals: [],
      removeAll: true,
      // meta for symmetry callers
      needsLeftExtend: false,
      leftEdgeDate: null,
      rightEdgeDate: null
    };
  }

  // ---- Add ranges (with left-extend symmetry) ----
  const addRanges = [];
  let leftEdgeDate  = null;
  let rightEdgeDate = null;
  let needsLeftExtend = false;

  if (adds.length) {
    leftEdgeDate  = adds[0];
    rightEdgeDate = adds[adds.length - 1];

    // Detect if any add is strictly before the current contract start
    if (contractStart && leftEdgeDate < contractStart) {
      needsLeftExtend = true; // <-- symmetry trigger
    }

    const b = boundsOf(adds);
    const LONG_CONSECUTIVE_THRESHOLD = 10;
    const consecutive = isConsecutiveDailyRun(adds);

    // IMPORTANT: never produce an empty explicitDays when no template exists.
    // Only use weekday-compression if a valid template is present.
    const haveTemplate = activeDows.size > 0;
    let explicitDays;

    if (consecutive && adds.length >= LONG_CONSECUTIVE_THRESHOLD && haveTemplate) {
      explicitDays = adds
        .filter(d => activeDows.has(new Date(d + 'T00:00:00Z').getUTCDay()))
        .map(d => ({ date: d }));
    } else {
      // Always send explicit days when:
      // - no template, or
      // - selection is short/not long-consecutive, or
      // - we need left-extend (ensure backend sees concrete pre-start dates)
      explicitDays = adds.map(d => ({ date: d }));
    }

    addRanges.push({
      from: b.from,
      to:   b.to,
      days: explicitDays,
      merge: 'append',
      when_timesheet_exists: 'create_additional'
    });
    L('addRanges', { bounds: b, count: explicitDays.length, sample: explicitDays.slice(0, 5), needsLeftExtend });
  } else {
    L('addRanges: none');
  }

  // ---- Remove ranges ----
  const removeRanges = [];
  if (rems.length) {
    const b = boundsOf(rems);
    removeRanges.push({
      from: b.from,
      to:   b.to,
      days: rems
    });
    L('removeRanges', { bounds: b, count: rems.length, sample: rems.slice(0, 5) });
  } else {
    L('removeRanges: none');
  }

  // ---- Additional days (for split weeks) ----
  const additionals = Object.entries(st.additional).map(([baseWeekId, set]) => ({
    baseWeekId, dates: [...set].sort()
  }));
  L('additionals', { count: additionals.length, sample: additionals.slice(0, 3) });

  return {
    addRanges,
    removeRanges,
    additionals,
    removeAll: false,
    // symmetry meta for the commit path
    needsLeftExtend,
    leftEdgeDate,
    rightEdgeDate
  };
}
function revertContractCalendarStage(contractId) {
  clearContractCalendarStageState(contractId);
}

// ============================================================================
// CALENDAR – API WRAPPERS
// ============================================================================

async function getContractCalendar(contract_id, opts) {
  const qs = new URLSearchParams();
  if (typeof opts === 'number') {
    qs.set('year', String(opts));
  } else if (opts && typeof opts === 'object') {
    if (opts.from) qs.set('from', String(opts.from));
    if (opts.to) qs.set('to',   String(opts.to));
    if (opts.granularity) qs.set('granularity', String(opts.granularity)); else qs.set('granularity', 'week');
    if (!opts.from && !opts.to && opts.year) qs.set('year', String(opts.year));
  } else {
    qs.set('year', String((new Date()).getUTCFullYear()));
  }
  const url = `/api/contracts/${_enc(contract_id)}/calendar?` + qs.toString();
  const r = await authFetch(API(url));
  if (!r?.ok) throw new Error('Calendar fetch failed');
  return r.json();
}
async function getContractCalendarRange(contract_id, from, to, granularity = 'day') {
  return getContractCalendar(contract_id, { from, to, granularity });
}
// Thin wrapper to match existing call sites.
// Backend returns day-level items; granularity is ignored for now.
async function getCandidateCalendarRange(candidate_id, from, to, granularity = 'day') {
  const qs = new URLSearchParams(); qs.set('from', from); qs.set('to', to);
  try {
    const r = await authFetch(API(`/api/candidates/${_enc(candidate_id)}/calendar?` + qs.toString()));
    if (!r || !r.ok) {
      try { const err = await r.json(); console.warn('[CAL][candidate] non-200', err); } catch {}
      return { from, to, items: [] };
    }
    const data = await r.json().catch(()=>null);
    if (!data || !Array.isArray(data.items)) return { from, to, items: [] };
    return data;
  } catch (e) {
    console.warn('[CAL][candidate] fetch failed', e);
    return { from, to, items: [] };
  }
}


async function getCandidateCalendar(candidate_id, from, to) {
  const qs = new URLSearchParams(); qs.set('from', from); qs.set('to', to);
  const r = await authFetch(API(`/api/candidates/${_enc(candidate_id)}/calendar?` + qs.toString()));
  if (!r?.ok) throw new Error('Candidate calendar fetch failed');
  return r.json();
}
async function contractsPlanRanges(contract_id, payload) {
  const r = await authFetch(API(`/api/contracts/${_enc(contract_id)}/plan-ranges`), { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(payload) });
  if (!r?.ok) throw new Error(await r.text()); return r.json();
}
async function contractsUnplanRanges(contract_id, payload) {
  const r = await authFetch(API(`/api/contracts/${_enc(contract_id)}/plan-ranges`), { method:'DELETE', headers:{'content-type':'application/json'}, body: JSON.stringify(payload) });
  if (!r?.ok) throw new Error(await r.text()); return r.json();
}
async function contractWeekPlanPatch(week_id, payload) {
  const r = await authFetch(API(`/api/contract-weeks/${_enc(week_id)}/plan`), { method:'PATCH', headers:{'content-type':'application/json'}, body: JSON.stringify(payload) });
  if (!r?.ok) throw new Error(await r.text()); return r.json();
}

// ============================================================================
// CALENDAR – CONTEXT MENU
// ============================================================================
function openCalendarContextMenu({ anchorEl, bucketKey, selection, capabilities, onAction }) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][ctx]', ...a); };

  // Remove any existing menu first
  document.getElementById('calCtxMenu')?.remove();

  // Create menu
  const menu = document.createElement('div');
  menu.id = 'calCtxMenu';
  menu.className = 'ctx-menu';

  // Position
  const r = anchorEl.getBoundingClientRect();
  menu.style.position = 'absolute';
  menu.style.zIndex = 10000;
  menu.style.top  = (window.scrollY + r.bottom) + 'px';
  menu.style.left = (window.scrollX + r.left) + 'px';

  // Dark, bordered, legible styling (inline so it works even without CSS)
  menu.style.minWidth = '180px';
  menu.style.padding = '6px';
  menu.style.border = '1px solid var(--line)';
  menu.style.borderRadius = '10px';
  menu.style.background = 'rgba(12, 21, 42, 0.98)'; // slightly lighter than page bg
  menu.style.boxShadow = '0 12px 28px rgba(0,0,0,.45), inset 0 0 0 1px rgba(255,255,255,.02)';
  menu.style.backdropFilter = 'blur(6px) saturate(120%)';

  // Items
  menu.innerHTML = `
    <div class="ctx-item ${capabilities.canBook ? '' : 'disabled'}"  data-act="book">Book</div>
    <div class="ctx-item ${capabilities.canUnbook ? '' : 'disabled'}" data-act="unbook">Unbook</div>
    <div class="ctx-item ${capabilities.canAddAdditional ? '' : 'disabled'}" data-act="additional">Add additional sheet</div>
  `;

  // Minimal inline item styles + hover
  [...menu.querySelectorAll('.ctx-item')].forEach((el, i) => {
    el.style.color = 'var(--text)';
    el.style.padding = '8px 10px';
    el.style.borderRadius = '8px';
    el.style.cursor = el.classList.contains('disabled') ? 'not-allowed' : 'pointer';
    el.style.userSelect = 'none';
    el.style.lineHeight = '1.25';
    if (i > 0) el.style.marginTop = '4px';
    el.addEventListener('mouseenter', () => { if (!el.classList.contains('disabled')) el.style.background = 'var(--hover)'; });
    el.addEventListener('mouseleave', () => { el.style.background = 'transparent'; });
    if (el.classList.contains('disabled')) { el.style.opacity = '.45'; el.style.filter = 'saturate(0.6) brightness(0.9)'; }
  });

  document.body.appendChild(menu);

  // Diagnostic log
  L('open', {
    bucketKey,
    selection: selection.slice(),
    capabilities
  });

  // Close + dispatch
  const close = () => { try { menu.remove(); } catch {} };
  menu.addEventListener('click', (e) => {
    const act = e.target?.getAttribute?.('data-act');
    if (!act || e.target.classList.contains('disabled')) return;
    close();
    onAction && onAction({ type: act, selection });
  });

  // Dismiss when clicking outside
  const onDoc = (e) => {
    if (!menu.contains(e.target)) {
      close();
      document.removeEventListener('mousedown', onDoc, true);
    }
  };
  setTimeout(() => document.addEventListener('mousedown', onDoc, true), 0);
}


// ============================================================================
// CALENDAR – GENERIC DAY GRID
// ============================================================================
function renderDayGrid(hostEl, opts) {
  if (!hostEl) return;
  const { from, to, itemsByDate, view, bucketKey } = opts;
  const sel = initSelBucket(bucketKey);
  const isContractBucket = (typeof bucketKey === 'string') && bucketKey.startsWith('c:');
  const currentKey = isContractBucket ? bucketKey.slice(2) : null;
  const interactive = (typeof opts.isInteractive === 'boolean') ? opts.isInteractive : true;

  hostEl.tripwire && hostEl.tripwire.abort?.();
  const controller = new AbortController();
  hostEl.tripwire = controller;

  hostEl.innerHTML = '';
  const toolbar = document.createElement('div');
  toolbar.className = 'row';
  toolbar.style.justifyContent = 'space-between';
  toolbar.style.alignItems = 'center';
  toolbar.innerHTML = `
    <div class="actions">
      <button id="calPrev">◀</button>
      <button id="calNext">▶</button>
      <button id="calToggle">${view === 'year' ? 'Month view' : 'Year view'}</button>
    </div>
    <div class="hint">${from} → ${to}</div>`;
  hostEl.appendChild(toolbar);

  const wrap = document.createElement('div');
  wrap.className = (view === 'year') ? 'year-wrap' : 'month-wrap';
  hostEl.appendChild(wrap);

  const months = [];
  if (view === 'year') {
    const y = ymdToDate(from).getUTCFullYear();
    for (let m = 0; m < 12; m++) months.push({ y, m });
  } else {
    const d0 = ymdToDate(from);
    months.push({ y: d0.getUTCFullYear(), m: d0.getUTCMonth() });
  }

  for (const { y, m } of months) {
    const box = document.createElement('div');
    box.className = 'month';
    box.innerHTML = `<h4>${new Date(Date.UTC(y, m, 1)).toLocaleString(undefined, { month: 'long' })} ${y}</h4>`;
    const days = document.createElement('div');
    days.className = (view === 'year') ? 'days' : 'days days-large';

    const first = new Date(Date.UTC(y, m, 1));
    for (let i = 0; i < first.getUTCDay(); i++) {
      days.appendChild(document.createElement('div'));
    }
    const daysInMonth = new Date(Date.UTC(y, m + 1, 0)).getUTCDate();

    for (let d = 1; d <= daysInMonth; d++) {
      const cell = document.createElement('div');
      cell.className = 'd';
      const dYmd = `${y}-${String(m + 1).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
      const items = itemsByDate.get(dYmd) || [];

      // ── NEW: colour source selection
      // Candidate view → derive colour from ALL items (unchanged).
      // Contract view  → derive colour ONLY from current contract's items.
      let finalState = 'EMPTY';
      let ownedByCurrent = false;
      if (isContractBucket) {
        const keyStr = String(currentKey || '');
        const owned = items.filter(it => String(it?.contract_id || '') === keyStr);
        ownedByCurrent = owned.length > 0;
        if (ownedByCurrent) {
          finalState = (typeof topState === 'function') ? topState(owned) : 'EMPTY';
        } else {
          // When a day is occupied only by other contracts, we keep the state as EMPTY
          // (no coloured class) and add a light-grey overlay class below.
          finalState = 'EMPTY';
        }
      } else {
        finalState = (typeof topState === 'function') ? topState(items) : 'EMPTY';
      }

      const stateClass = (typeof colorForState === 'function') ? colorForState(finalState) : null;
      if (stateClass) cell.classList.add(stateClass);

      // Only apply “occupied-other” greying in *contract* calendars and only when the day has
      // items but NONE belong to the current contract.
      if (isContractBucket) {
        const keyStr = String(currentKey || '');
        const occupiedByOtherOnly =
          !ownedByCurrent &&
          items.some(it => {
            const cid = String(it?.contract_id || '');
            return !!cid && cid !== keyStr;
          });
        if (occupiedByOtherOnly) {
          cell.classList.add('occupied-other'); // style this as your light grey
        }
      }

      if (sel.set.has(dYmd)) cell.className += ' selected';

      const dow = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][new Date(Date.UTC(y, m, d)).getUTCDay()];
      cell.innerHTML = `<div class="ico"><div class="dow">${dow.slice(0, 3)}</div><div class="num">${d}</div></div>`;
      cell.setAttribute('data-date', dYmd);

      if (interactive) {
        const onClick = (ev) => {
          if (controller.signal.aborted) return;
          const additive = ev.ctrlKey || ev.metaKey;
          const bucket = bucketKey;
          const anchor = initSelBucket(bucket).anchor;
          const useRange = ev.shiftKey && !!(anchor);
          if (useRange) {
            selectRange(bucket, anchor, dYmd, additive);
          } else {
            toggleDaySelected(bucket, dYmd, additive);
          }
          wrap.querySelectorAll('.d.selected').forEach(n => n.classList.remove('selected'));
          initSelBucket(bucket).set.forEach(s => {
            const dom = wrap.querySelector(`.d[data-date="${CSS.escape(s)}"]`);
            if (dom) dom.classList.add('selected');
          });
        };
        const onCtx = (ev) => {
          if (controller.signal.aborted) return;
          ev.preventDefault();
          const bucket = bucketKey;
          const selSet = initSelBucket(bucket).set;
          if (!selSet.has(dYmd)) {
            clearCalendarSelection(bucket);
            toggleDaySelected(bucket, dYmd, false);
            cell.classList.add('selected');
          }
          opts.onCellContextMenu && opts.onCellContextMenu(dYmd, ev);
        };
        cell.addEventListener('click', onClick, { signal: controller.signal });
        cell.addEventListener('contextmenu', onCtx, { signal: controller.signal });
      }

      days.appendChild(cell);
    }

    box.appendChild(days);
    wrap.appendChild(box);
  }

  toolbar.querySelector('#calPrev')?.addEventListener('click', () => {
    if (!controller.signal.aborted) opts.onNav && opts.onNav(-1);
  }, { signal: controller.signal });

  toolbar.querySelector('#calNext')?.addEventListener('click', () => {
    if (!controller.signal.aborted) opts.onNav && opts.onNav(1);
  }, { signal: controller.signal });

  toolbar.querySelector('#calToggle')?.addEventListener('click', () => {
    if (!controller.signal.aborted) opts.onToggleView && opts.onToggleView();
  }, { signal: controller.signal });
}

// ============================================================================
// CALENDAR – LEGEND
// ============================================================================
function renderCalendarLegend(container) {
  if (!container) return;
  container.innerHTML = `
    <div class="legend">
      <span class="chip cal-planned">Planned</span>
      <span class="chip cal-submitted">Submitted</span>
      <span class="chip cal-authorised">Authorised</span>
      <span class="chip cal-invoiced">Invoiced</span>
      <span class="chip cal-paid">Paid</span>
      <span class="chip">Not booked</span>
    </div>`;
}

// ============================================================================
// CONTRACTS – FETCH & RENDER (DAY CALENDAR) with STAGING
// ============================================================================


// Calendar Save/Discard is deprecated — keep as a no-op to avoid breaking older calls.
function wireContractCalendarSaveControls(contractId, holder, weekIndex) {
  // Remove old bar if present, and do not render anything new
  holder.querySelector('#calSaveBar')?.remove();
  // No-op: Big Save (modal) is responsible for committing calendar changes now.
}

// ============================================================================
// CONTRACTS – TAB RENDERER
// ============================================================================

function renderContractCalendarTab(ctx) {
  const LOGM = !!window.__LOG_MODAL;
  const c = ctx?.data || {};
  const holderId = 'contractCalendarHolder';

  const currentKey = (c.id || window.modalCtx?.openToken || null);
  const candId = c.candidate_id
              || (window.modalCtx?.formState?.main?.candidate_id || '').trim()
              || (document.querySelector('#contractForm input[name="candidate_id"]')?.value || '').trim();

  const weekEnding = (c.week_ending_weekday_snapshot ?? window.modalCtx?.formState?.main?.week_ending_weekday_snapshot ?? 0);

  const fr = (typeof window.__getModalFrame === 'function') ? window.__getModalFrame() : null;
  const inViewMode = !!(fr && fr.mode === 'view');

  // --- actions (now includes Duplicate)
  const actionsHtml = (c.id
    ? `<div class="actions" style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap">
         ${inViewMode ? `` : `<button id="btnAddMissing">Add missing weeks</button>
         <button id="btnRemoveAll">Remove all weeks</button>`}
         ${inViewMode ? `<button id="btnCloneExtend">Clone & Extend…</button>
         <button id="btnDuplicateContract">Duplicate Contract…</button>` : ``}
       </div>`
    : ``);

  // --- early hint ONLY for brand-new contracts with no candidate
  const hasId = !!c.id;
  if (!hasId && !candId) {
    if (LOGM) console.log('[CAL][contract] no candidate yet for NEW contract; render hint');
    return `
      <div id="${holderId}" class="tabc">
        <div class="info-row" style="margin:0 0 8px 0;font-size:13px;">
          <strong>Candidate:</strong>
          <span class="unassigned" style="color:var(--danger,#c0392b);margin-left:6px;">&lt;Unassigned&gt;</span>
        </div>
        <div class="hint">Pick a candidate to view and stage calendar dates.</div>
        ${actionsHtml}
      </div>`;
  }

  setTimeout(async () => {
    try {
      const y = (new Date()).getUTCFullYear();
      const win = computeYearWindow(y);
      const el = byId(holderId); if (!el) return;

      el.innerHTML = `
        <div class="tabc" style="display:flex;flex-direction:column;gap:8px;height:calc(72vh);max-height:calc(72vh)">
          <div class="info-row" style="font-size:13px;">
            <strong>Candidate:</strong>
            <span id="__calCandidateName" style="margin-left:6px;"></span>
          </div>
          <div id="__calScroll" style="flex:1;min-height:0;overflow:auto;border:1px solid var(--line,#e5e5e5);border-radius:8px;padding:4px;">
            <div id="__contractCal"></div>
          </div>
          ${actionsHtml}
        </div>`;

      // Render the candidate label (supports <Unassigned>)
      try {
        const nameEl = el.querySelector('#__calCandidateName');
        if (nameEl) {
          const stagedLabel = (window.modalCtx?.formState?.main?.candidate_display || '').trim();
          const rowLabel    = (c.candidate_display || c.candidate_name || '').trim();
          const label       = stagedLabel || rowLabel || '';
          if (label) {
            nameEl.textContent = label;
          } else {
            nameEl.textContent = '<Unassigned>';
            nameEl.style.color = 'var(--danger,#c0392b)';
          }
        }
      } catch {}

      if (c.id) {
        // EXISTING contract → always render calendar, even if candidate_id is null
        if (LOGM) console.log('[CAL][contract] render with real contract id', { id: c.id, win });
        await fetchAndRenderContractCalendar(c.id, { from: win.from, to: win.to, view: 'year' });
      } else {
        // CREATE mode with candidate → candidate-wide planner
        if (LOGM) console.log('[CAL][contract] render in CREATE mode (candidate-wide) with token bucket', { token: currentKey, candId, weekEnding });
        await fetchAndRenderCandidateCalendarForContract(
          currentKey,
          candId,
          { from: win.from, to: win.to, view: 'year', weekEnding: Number(weekEnding) }
        );
      }

      if (c.id) {
        const btnAdd = el.querySelector('#btnAddMissing');
        if (btnAdd && !btnAdd.__wired) {
          btnAdd.__wired = true;
          btnAdd.addEventListener('click', async () => {
            if (typeof stageAddMissingWeeks === 'function') {
              if (LOGM) console.log('[CAL][contract] stage add missing weeks', { id: c.id, from: c.start_date || win.from, to: c.end_date || win.to });
              await stageAddMissingWeeks(c.id, { from: c.start_date || win.from, to: c.end_date || win.to });
              try { showModalHint?.('Missing weeks staged (preview only). Save to persist.', 'warn'); } catch {}
            }
            const prev = byId('__calScroll')?.scrollTop || 0;
            await fetchAndRenderContractCalendar(
              c.id,
              {
                from: window.__calState[c.id]?.win?.from,
                to:   window.__calState[c.id]?.win?.to,
                view: window.__calState[c.id]?.view
              }
            );
            const sb = byId('__calScroll'); if (sb) sb.scrollTop = prev;
          });
        }

        const btnRem = el.querySelector('#btnRemoveAll');
        if (btnRem && !btnRem.__wired) {
          btnRem.__wired = true;
          btnRem.addEventListener('click', async () => {
            if (!window.confirm('Remove all unsubmitted weeks for this contract?')) return;
            if (typeof removeAllUnsubmittedWeeks === 'function') {
              if (LOGM) console.log('[CAL][contract] stage remove all unsubmitted weeks', { id: c.id, from: c.start_date || null, to: c.end_date || null });
              await removeAllUnsubmittedWeeks(c.id, { from: c.start_date || null, to: c.end_date || null });
              try { showModalHint?.('All unsubmitted weeks staged for removal (preview only). Save to persist.', 'warn'); } catch {}
            }
            const prev = byId('__calScroll')?.scrollTop || 0;
            await fetchAndRenderContractCalendar(
              c.id,
              {
                from: window.__calState[c.id]?.win?.from,
                to:   window.__calState[c.id]?.win?.to,
                view: window.__calState[c.id]?.view
              }
            );
            const sb = byId('__calScroll'); if (sb) sb.scrollTop = prev;
          });
        }

        const btnCE = el.querySelector('#btnCloneExtend');
        if (btnCE && !btnCE.__wired) {
          btnCE.__wired = true;
          btnCE.addEventListener('click', () => {
            if (LOGM) console.log('[CAL][contract] open clone & extend', { id: c.id });
            openContractCloneAndExtend(c.id);
          });
        }

        // NEW: Duplicate Contract…
        const btnDup = el.querySelector('#btnDuplicateContract');
        if (btnDup && !btnDup.__wired) {
          btnDup.__wired = true;
          btnDup.addEventListener('click', async () => {
            try {
              const countStr = window.prompt(
                'How many duplicate contracts do you require? (1–10)',
                '1'
              );
              if (countStr == null) return; // user hit Cancel

              const n = Number(countStr);
              if (!Number.isInteger(n) || n < 1 || n > 10) {
                alert('Please enter a whole number between 1 and 10.');
                return;
              }

              if (typeof duplicateContract !== 'function') {
                alert('Duplicate action is unavailable in this build.');
                return;
              }

              const res = await duplicateContract(c.id, { count: n });
              const ok  = !!(res && (res.ok === undefined ? true : res.ok));
              if (!ok) {
                const msg = res && res.message ? res.message : 'Duplicate failed';
                alert(msg);
                return;
              }

              const created = (res && Number.isInteger(res.count)) ? res.count : n;
              try {
                window.__toast?.(
                  `${created} duplicate contract${created > 1 ? 's' : ''} created`
                );
              } catch {}

              // Refresh list (and hence calendars/rows)
              try { await renderAll(); } catch {}
            } catch (e) {
              if (LOGM) console.warn('[CAL][contract] duplicate failed', e);
              alert(e?.message || 'Duplicate failed');
            }
          });
        }
      }
    } catch (e) {
      const el = byId(holderId); if (el) el.innerHTML = `<div class="error">Calendar load failed.</div>`;
      if (LOGM) console.warn('[CAL][contract] calendar render failed', e);
    }
  }, 0);

  return `
    <div id="${holderId}" class="tabc">
      <div class="info-row" style="margin:0 0 8px 0;font-size:13px;">
        <strong>Candidate:</strong>
        <span id="__calCandidateName" style="margin-left:6px;"></span>
      </div>
      <div class="hint">Loading calendar…</div>
      ${actionsHtml}
    </div>`;
}


function isConsecutiveDailyRun(dates) {
  if (!Array.isArray(dates) || dates.length < 2) return false;
  const arr = [...dates].sort();
  let hasPair = false;
  for (let i = 1; i < arr.length; i++) {
    const a = ymdToDate(arr[i - 1]);
    const b = ymdToDate(arr[i]);
    const diff = Math.round((b - a) / 86400000);
    if (diff === 1) hasPair = true;
  }
  return hasPair;
}

function updateCalendarInteractivity(isInteractive) {
  window.__calInteractive = !!isInteractive;
}


// ───────────────────────────────────────────────────────────────
// 5) Optional thin wrapper if you prefer not to call the API
//    directly throughout the UI (used by calendar wrapper).
// ───────────────────────────────────────────────────────────────
async function fetchCandidateCalendarForRange(candidateId, fromYmd, toYmd) {
  if (typeof getCandidateCalendarRange !== 'function') return { items: [] };
  try {
    return await getCandidateCalendarRange(candidateId, fromYmd, toYmd, 'day');
  } catch {
    return { items: [] };
  }
}
// ───────────────────────────────────────────────────────────────
// 4) Unbooking helper (present in some snippets, ensure available)
// ───────────────────────────────────────────────────────────────
function stageContractCalendarUnbookings(contractId, dates /* array of ymd */) {
  const st = getContractCalendarStageState(contractId);
  for (const d of dates) { st.add.delete(d); st.remove.add(d); }
}
// ───────────────────────────────────────────────────────────────
// 3) Stage adoption helper (token → real id) for create flow
//    You can use this inside openContract.onSave to simplify.
// ───────────────────────────────────────────────────────────────
function adoptCalendarStageFromToken(openToken, contractId) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : false;
  if (!openToken || !contractId || typeof getContractCalendarStageState !== 'function') return false;
  try {
    const stToken = getContractCalendarStageState(openToken);
    if (!stToken) return false;
    const has = (stToken.add?.size || stToken.remove?.size || Object.keys(stToken.additional||{}).length);
    if (!has) return false;
    const stId = getContractCalendarStageState(contractId);
    for (const d of stToken.add)    stId.add.add(d);
    for (const d of stToken.remove) stId.remove.add(d);
    stId.additional = { ...(stId.additional||{}), ...(stToken.additional||{}) };
    stToken.add?.clear?.(); stToken.remove?.clear?.(); stToken.additional = {};
    if (LOG_CAL) console.log('[CAL][adopt] token → id', { openToken, contractId });
    return true;
  } catch (e) {
    if (LOG_CAL) console.warn('[CAL][adopt] failed', e);
    return false;
  }
}
// ───────────────────────────────────────────────────────────────
// 2) Candidate-wide calendar renderer for the contract modal
//    Always renders ALL bookings for the candidate and stages
//    against currentKey (contract.id or openToken).
//    (Used by renderContractCalendarTab and openContract repaint)
// ───────────────────────────────────────────────────────────────

function computeContractSaveEligibility() {
  try {
    const fs   = (window.modalCtx && window.modalCtx.formState) || { main:{}, pay:{} };
    const data = (window.modalCtx && window.modalCtx.data) || {};
    const form = document.querySelector('#contractForm');

    const val = (name) => {
      const staged = fs.main && fs.main[name];
      if (staged !== undefined && staged !== null && String(staged).trim() !== '') return String(staged).trim();
      const el = form ? form.querySelector(`[name="${name}"]`) : null;
      return el ? String(el.value || '').trim() : '';
    };

    const hasText = (s) => !!(s && String(s).trim().length);

    // -------- Required entities with fallback to saved data
    const candidateOk = hasText(val('candidate_id')) || !!data.candidate_id;
    const clientOk    = hasText(val('client_id'))    || !!data.client_id;
    const roleOk      = hasText(val('role'))         || !!data.role;

    // -------- Dates (with fallback to saved row, and awareness of pending auto-expand)
    const toIso = (uk) => {
      if (!uk) return '';
      try { return (typeof parseUkDateToIso === 'function') ? (parseUkDateToIso(uk) || '') : uk; }
      catch { return uk; }
    };
    const vSd = val('start_date'), vEd = val('end_date');
    const sdIsoRaw = toIso(vSd) || (data.start_date || '');
    const edIsoRaw = toIso(vEd) || (data.end_date   || '');

    const bothDatesProvided = /^\d{4}-\d{2}-\d{2}$/.test(sdIsoRaw) && /^\d{4}-\d{2}-\d{2}$/.test(edIsoRaw);
    const dateOrderOk       = (!bothDatesProvided) || (sdIsoRaw <= edIsoRaw);

    const pendingExpand = (window.modalCtx && window.modalCtx.__windowExpand && window.modalCtx.__windowExpand.start && window.modalCtx.__windowExpand.end) ? window.modalCtx.__windowExpand : null;

    // -------- Is there any staged calendar?
    let hasStaged = false;
    try {
      const key = data.id || window.modalCtx?.openToken || null;
      if (key && typeof getContractCalendarStageState === 'function') {
        const st = getContractCalendarStageState(key);
        hasStaged = !!(st && (st.add?.size || st.remove?.size || (st.additional && Object.keys(st.additional).length)));
      }
    } catch {}

    // -------- Schedule validation (pending time format supported) with template fallback
    const reValidHHMM   = /^(\d{1,2}):(\d{2})$/;
    const rePendingOnly = /^\d{3,4}$/;
    const hhmm = (s) => {
      const m = String(s||'').match(reValidHHMM);
      if (!m) return null;
      const h = +m[1], mi = +m[2];
      if (h<0 || h>23 || mi<0 || mi>59) return null;
      return [h,mi];
    };

    const days = ['mon','tue','wed','thu','fri','sat','sun'];
    let hasValidPair   = false;
    let hasPendingPair = false;
    const pendingFields = [];
    const pendingDays   = [];

    for (const d of days) {
      const s = val(`${d}_start`);
      const e = val(`${d}_end`);
      if (!s || !e) continue;

      const sValid   = !!hhmm(s);
      const eValid   = !!hhmm(e);
      const sPending = rePendingOnly.test(s);
      const ePending = rePendingOnly.test(e);

      if (sValid && eValid) {
        hasValidPair = true;
      } else if ((sValid || sPending) && (eValid || ePending)) {
        hasPendingPair = true;
        if (sPending) pendingFields.push(`${d}_start`);
        if (ePending) pendingFields.push(`${d}_end`);
        pendingDays.push(d);
      }
    }

    const hasTemplate = !!(data.std_schedule_json && typeof data.std_schedule_json === 'object' && Object.keys(data.std_schedule_json).length);
    const scheduleOk = (hasValidPair || hasPendingPair || hasStaged || hasTemplate);
    const pendingTimeFormat = hasPendingPair || pendingFields.length > 0;

    // -------- Finance checks (with fallback to saved rates_json)
    const payMethod = ((val('pay_method_snapshot') || data.pay_method_snapshot || 'PAYE')).toUpperCase();

    const getNum = (n) => {
      if (fs.pay && Object.prototype.hasOwnProperty.call(fs.pay, n)) {
        const rawS = fs.pay[n];
        if (rawS !== '' && rawS !== null && rawS !== undefined) {
          const numS = Number(rawS);
          if (Number.isFinite(numS)) return numS;
        }
      }
      if (form) {
        const el = form.querySelector(`[name="${n}"]`);
        if (el && el.value !== '' && el.value !== null && el.value !== undefined) {
          const numD = Number(el.value);
          if (Number.isFinite(numD)) return numD;
        }
      }
      try {
        const saved = (window.modalCtx && window.modalCtx.data && window.modalCtx.data.rates_json) || {};
        const v = saved[n];
        if (v === 0 || (typeof v === 'number' && Number.isFinite(v))) return Number(v);
      } catch {}
      return null;
    };

    const payBuckets = (payMethod === 'PAYE')
      ? ['paye_day','paye_night','paye_sat','paye_sun','paye_bh']
      : ['umb_day','umb_night','umb_sat','umb_sun','umb_bh'];
    const chargeBuckets = ['charge_day','charge_night','charge_sat','charge_sun','charge_bh'];

    const anyPay    = payBuckets.some(b => getNum(b) !== null);
    const anyCharge = chargeBuckets.some(b => getNum(b) !== null);

    let hasNegativeMargins = false;
    if (window.__contractMarginState && typeof window.__contractMarginState.hasNegativeMargins === 'boolean') {
      hasNegativeMargins = !!window.__contractMarginState.hasNegativeMargins;
    } else {
      for (const cb of chargeBuckets) {
        const b  = cb.split('_')[1];
        const ch = getNum(`charge_${b}`);
        const py = getNum(`${payMethod === 'PAYE' ? 'paye' : 'umb'}_${b}`);
        if (ch !== null && py !== null && (ch - py) < 0) { hasNegativeMargins = true; break; }
      }
    }

    // -------- Timesheet boundary guard (uses cached result)
    let tsBoundaryViolation = false;
    let tsBoundaryMsg = null;
    try {
      const tsRes = window.__tsBoundaryResult;
      if (data?.id && tsRes && tsRes.ok === false) {
        tsBoundaryViolation = true;
        const v = tsRes.violations || [];
        if (v.length) {
          const sample = v.slice(0,3).map(x => {
            const nm = x.client_name || 'Client';
            const dt = x.date || '';
            const st = x.status || '';
            return `${nm} ${dt}${st?` (${st})`:''}`;
          }).join(' • ');
          tsBoundaryMsg = `Dates exclude existing timesheets: ${sample}${v.length>3? '…':''}`;
        } else if (tsRes.min_ts_date || tsRes.max_ts_date) {
          tsBoundaryMsg = `Dates exclude timesheets in range ${tsRes.min_ts_date||''} → ${tsRes.max_ts_date||''}.`;
        } else {
          tsBoundaryMsg = 'Dates exclude existing timesheets.';
        }
      }
    } catch {}

    // -------- Compose eligibility & reasons (with pending auto-expand awareness)
    const reasons = [];

    if (!candidateOk) reasons.push({ code:'MISSING_CANDIDATE', message:'Pick a candidate.' });
    if (!clientOk)    reasons.push({ code:'MISSING_CLIENT',    message:'Pick a client.' });
    if (!roleOk)      reasons.push({ code:'MISSING_ROLE',      message:'Enter a role.' });

    const hasWindowExpand = !!pendingExpand;

    if (!bothDatesProvided && !hasStaged && !hasWindowExpand) {
      reasons.push({ code:'DATES_OR_STAGE_REQUIRED', message:'Provide start & end dates or stage calendar changes.' });
    } else if (bothDatesProvided && !dateOrderOk) {
      reasons.push({ code:'DATE_ORDER_INVALID', message:'Start date must be on or before end date.' });
    }

    if (!scheduleOk) {
      reasons.push({ code:'SCHEDULE_REQUIRED', message:'Add at least one day with Start & End (or stage calendar changes).' });
    }

    if (!anyPay)    reasons.push({ code:'MISSING_PAY_RATES',    message:'Enter at least one pay bucket.' });
    if (!anyCharge) reasons.push({ code:'MISSING_CHARGE_RATES', message:'Enter at least one charge bucket.' });
    if (hasNegativeMargins) reasons.push({ code:'NEGATIVE_MARGIN', message:'One or more buckets produce a negative margin.' });

    if (tsBoundaryViolation) {
      reasons.push({ code:'TS_BOUNDARY_VIOLATION', message: tsBoundaryMsg || 'Dates exclude existing timesheets.' });
    }

    // ✅ FINAL ELIGIBILITY:
    //   • Candidate is NO LONGER a hard requirement here (still warned via reasons).
    const ok =
      /* candidateOk && */            // <-- removed from hard gate
      clientOk &&
      roleOk &&
      (
        (bothDatesProvided ? dateOrderOk : true) // dates ok if provided & ordered
        || hasStaged
        || hasWindowExpand
      ) &&
      scheduleOk &&
      anyPay &&
      anyCharge &&
      !hasNegativeMargins &&
      !tsBoundaryViolation;

    const detail = {
      ok,
      pendingTimeFormat,
      pending: {
        timeFormat: pendingTimeFormat,
        fields: pendingFields,
        days: pendingDays
      },
      checkpoints: {
        candidateOk, clientOk, roleOk,
        dates: { bothDatesProvided, dateOrderOk, hasStagedCalendar: hasStaged, tsBoundaryOk: !tsBoundaryViolation, willAutoExpand: !!hasWindowExpand },
        schedule: { hasValidPair, hasPendingPair, hasStagedCalendar: hasStaged, hasTemplate },
        finance: { anyPay, anyCharge, hasNegativeMargins, payMethod }
      },
      reasons,
      tip: pendingTimeFormat ? 'We’ll format times like 0900 → 09:00 when you tab out or save.' : null
    };

    window.__contractEligibility = detail;
    return ok;
  } catch (e) {
    window.__contractEligibility = { ok:false, reasons:[{ code:'INTERNAL_ERROR', message:String(e && e.message || e || 'unknown error') }] };
    return false;
  }
}


// ───────────────────────────────────────────────────────────────
// 1) Non-blocking date-window overlap checker (Main tab hint)
//    Call from renderContractMainTab on start/end date change.
// ───────────────────────────────────────────────────────────────
async function callCheckContractWindowOverlap(candidate_id, start_date_iso, end_date_iso, exclude_contract_id) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : false;
  const payload = {
    candidate_id: candidate_id || null,
    start_date  : start_date_iso || null,
    end_date    : end_date_iso || null,
    ignore_contract_id: exclude_contract_id || null
  };
  if (!candidate_id || !start_date_iso || !end_date_iso) return { has_overlap: false, overlaps: [] };
  if (typeof checkContractOverlap !== 'function') { if (LOGC) console.warn('[CONTRACTS] checkContractOverlap missing'); return { has_overlap:false, overlaps:[] }; }
  try {
    if (LOGC) console.log('[CONTRACTS] callCheckContractWindowOverlap → req', payload);
    const res = await checkContractOverlap(payload);
    if (LOGC) console.log('[CONTRACTS] callCheckContractWindowOverlap ← res', res);
    return res || { has_overlap:false, overlaps:[] };
  } catch (e) {
    if (LOGC) console.warn('[CONTRACTS] window overlap check failed', e);
    return { has_overlap:false, overlaps:[] };
  }
}


async function fetchAndRenderCandidateCalendarForContract(currentKey, candidateId, opts) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][candidate-wide]', ...a); };

  const state = (window.__calState[currentKey] ||= {
    view: (opts?.view || 'year'),
    win:  (opts?.from && opts?.to) ? { from: opts.from, to: opts.to } : computeYearWindow((new Date()).getUTCYear ? (new Date()).getUTCFullYear() : (new Date()).getFullYear()),
    weekEndingWeekday: (typeof opts?.weekEnding !== 'undefined' ? Number(opts.weekEnding) : (window.modalCtx?.data?.week_ending_weekday_snapshot ?? 0)),
    scrollTop: 0
  });

  if (opts?.view) state.view = opts.view;
  if (opts?.from && opts?.to) state.win = { from: opts.from, to: opts.to };
  if (typeof opts?.weekEnding !== 'undefined') state.weekEndingWeekday = Number(opts.weekEnding);

  const holder = byId('contractCalendarHolder'); if (!holder) return;
  const scrollBox = byId('__calScroll') || holder;
  state.scrollTop = scrollBox.scrollTop || 0;

  const contractId   = (window.modalCtx?.data?.id && String(currentKey) === String(window.modalCtx.data.id)) ? window.modalCtx.data.id : null;
  const currentStart = window.modalCtx?.data?.start_date || null;
  const currentEnd   = window.modalCtx?.data?.end_date   || null;

  let dayItems = [];
  try {
    if (typeof getCandidateCalendarRange === 'function') {
      const resp = await getCandidateCalendarRange(candidateId, state.win.from, state.win.to, 'day');
      dayItems = Array.isArray(resp?.items) ? resp.items : [];
    } else {
      dayItems = [];
    }
  } catch (e) {
    L('getCandidateCalendarRange failed; fallback to empty', e);
    dayItems = [];
  }

  const itemsByDate = buildDateIndex(dayItems);

  if (contractId && typeof getContractCalendar === 'function') {
    try {
      const cday = await getContractCalendar(contractId, { from: state.win.from, to: state.win.to, granularity:'day' });
      const cItems = Array.isArray(cday?.items) ? cday.items : [];
      for (const it of cItems) {
        const d = it?.date;
        if (!d) continue;
        const arr = itemsByDate.get(d) || [];
        if (it && !it.contract_id) it.contract_id = contractId;
        arr.push(it);
        itemsByDate.set(d, arr);
      }
      if (LOG_CAL) L('[CAL] merged contract-day items', { count: cItems.length });
    } catch (e) {
      L('getContractCalendar(day) failed; proceeding with candidate-only', e);
    }
  }

  let weekIndex = null;
  if (contractId && typeof getContractCalendar === 'function' && typeof buildWeekIndex === 'function') {
    try {
      const weeks = (await getContractCalendar(contractId, { from: state.win.from, to: state.win.to, granularity:'week' })).items || [];
      weekIndex = buildWeekIndex(weeks);
    } catch (e) {
      L('week index fetch failed; proceeding without TS lock context', e);
      weekIndex = null;
    }
  }

  const overlayedMap = applyStagedContractCalendarOverlay(currentKey, itemsByDate, new Map());

  const gridHost = document.createElement('div'); gridHost.id = 'contractDayGrid';
  const container = byId('__contractCal') || holder;
  if (container === holder) holder.innerHTML = '';
  container.innerHTML = '';
  container.appendChild(gridHost);

  renderDayGrid(gridHost, {
    from: state.win.from,
    to: state.win.to,
    itemsByDate: overlayedMap,
    view: state.view,
    bucketKey: `c:${currentKey}`,
    isInteractive: !!window.__calInteractive,
    onNav: async (delta) => {
      if (state.view === 'year') {
        const y = ymdToDate(state.win.from).getUTCFullYear() + delta;
        const nextWin = computeYearWindow(y);
        await fetchAndRenderCandidateCalendarForContract(currentKey, candidateId, { from: nextWin.from, to: nextWin.to, weekEnding: state.weekEndingWeekday, view: 'year' });
      } else {
        const nextWin = stepMonth(state.win, delta);
        await fetchAndRenderCandidateCalendarForContract(currentKey, candidateId, { from: nextWin.from, to: nextWin.to, weekEnding: state.weekEndingWeekday, view: 'month' });
      }
    },
    onToggleView: async () => {
      const newView = (state.view === 'year') ? 'month' : 'year';
      let win = state.win;
      if (newView === 'year') { const y = ymdToDate(state.win.from).getUTCFullYear(); win = computeYearWindow(y); }
      else { const dt = ymdToDate(state.win.from); win = computeMonthWindow(dt.getUTCFullYear(), dt.getUTCMonth()); }
      await fetchAndRenderCandidateCalendarForContract(currentKey, candidateId, { from: win.from, to: win.to, view: newView, weekEnding: state.weekEndingWeekday });
    },
    onCellContextMenu: (theDate, ev) => {
      if (!window.__calInteractive) return;
      const sel = initSelBucket(`c:${currentKey}`).set; const selArr = [...sel];

      const selMin = selArr.length ? selArr.reduce((a,b)=> a < b ? a : b) : null;
      const selMax = selArr.length ? selArr.reduce((a,b)=> a > b ? a : b) : null;
      let willExpand = false;
      let expStart = currentStart || selMin || null;
      let expEnd   = currentEnd   || selMax || null;

      if (currentStart && selMin && selMin < currentStart) { expStart = selMin; willExpand = true; }
      if (currentEnd   && selMax && selMax > currentEnd)   { expEnd   = selMax; willExpand = true; }

      if (willExpand && expStart && expEnd) {
        window.modalCtx.__windowExpand = { start: expStart, end: expEnd };
        const toUk = (iso) => { try { return (typeof formatIsoToUk === 'function') ? (formatIsoToUk(iso) || iso) : iso; } catch { return iso; } };
        const banner = byId('calExpandBanner');
        if (banner) banner.textContent = `These selections are outside the current window (${toUk(currentStart)||'—'} → ${toUk(currentEnd)||'—'}). We’ll extend to ${toUk(expStart)} → ${toUk(expEnd)} on Save.`;
      } else {
        window.modalCtx.__windowExpand = null;
        const banner = byId('calExpandBanner');
        if (banner) banner.textContent = '';
      }

      const itemsByDate = overlayedMap; // already overlaid

      const mine = String(window.modalCtx?.data?.id || currentKey);
      const ownedByCurrent = (d) => {
        const arr = itemsByDate.get(d) || [];
        return arr.some(it => String(it.contract_id || '') === mine);
      };
      const occupiedByOtherOnly = (d) => {
        const arr = itemsByDate.get(d) || [];
        return arr.some(it => {
          const cid = String(it.contract_id || '');
          return cid && cid !== mine;
        }) && !ownedByCurrent(d);
      };

      const anyGrey = selArr.some(occupiedByOtherOnly);
      const resolveFinalState = (d) => topState(overlayedMap.get(d) || []);

      const eligible = (d) => {
        const st = resolveFinalState(d);
        if (st === 'PLANNED') return true;
        return false;
      };
      const anyEligible = selArr.some(eligible);
      const allEligible = selArr.every(eligible);
      const blockMode = isConsecutiveDailyRun(selArr);

      const capabilities = {
        canBook: true,
        canUnbook: blockMode ? anyEligible : allEligible,
        canAddAdditional: false
      };

      openCalendarContextMenu({
        anchorEl: ev.target,
        bucketKey: `c:${currentKey}`,
        selection: selArr,
        capabilities,
        onAction: async ({ type, selection }) => {
          try {
            if (type === 'book') {
              if (anyGrey) {
                const names = [];
                for (const d of selection) {
                  if (!occupiedByOtherOnly(d)) continue;
                  const arr = itemsByDate.get(d) || [];
                  arr.forEach(it => {
                    const cid = String(it.contract_id || '');
                    if (cid && cid !== mine) names.push(it.client_name || 'Other client');
                  });
                }
                const hint = names.length
                  ? `This would clash with existing contract(s): ${[...new Set(names)].join(', ')}. Continue?`
                  : 'This would clash with an existing contract. Continue?';
                if (!window.confirm(hint)) return;
              }
              stageContractCalendarBookings(currentKey, selection);
            }
            if (type === 'unbook') {
              const toUnbook = selection.filter(eligible);
              if (toUnbook.length) stageContractCalendarUnbookings(currentKey, toUnbook);
            }
            try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
            const prev = (byId('__calScroll') || holder).scrollTop || 0;
            await fetchAndRenderCandidateCalendarForContract(currentKey, candidateId, { from: state.win.from, to: state.win.to, view: state.view, weekEnding: state.weekEndingWeekday });
            (byId('__calScroll') || holder).scrollTop = prev;
          } catch (e) {
            alert(e?.message || e);
          }
        }
      });
    }
  });

  (byId('__calScroll') || holder).scrollTop = state.scrollTop;
}



// ============================================================================
// CANDIDATE – RENDER CALENDAR TAB
// ============================================================================
async function fetchAndRenderContractCalendar(contractId, opts) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][contract]', ...a); };

  const state = (window.__calState[contractId] ||= { view:'year', win: computeYearWindow((new Date()).getUTCFullYear()), weekEndingWeekday: (window.modalCtx?.data?.week_ending_weekday_snapshot ?? 0), scrollTop: 0 });

  if (opts?.view) state.view = opts.view;
  if (opts?.from && opts?.to) state.win = { from: opts.from, to: opts.to };

  const holder = byId('contractCalendarHolder'); if (!holder) return;
  const scrollBox = byId('__calScroll') || holder;
  state.scrollTop = scrollBox.scrollTop || 0;

  const candidateId = window.modalCtx?.data?.candidate_id || window.modalCtx?.formState?.main?.candidate_id || null;

  let candidateItems = [];
  try {
    if (candidateId && typeof getCandidateCalendarRange === 'function') {
      const resp = await getCandidateCalendarRange(candidateId, state.win.from, state.win.to, 'day');
      candidateItems = Array.isArray(resp?.items) ? resp.items : [];
    }
  } catch (e) {
    L('getCandidateCalendarRange failed', e);
    candidateItems = [];
  }
  const itemsByDate = buildDateIndex(candidateItems);

  const dayResp = await getContractCalendarRange(contractId, state.win.from, state.win.to, 'day');
  const dayItems = Array.isArray(dayResp?.items) ? dayResp.items : [];
  for (const it of dayItems) {
    const d = it?.date; if (!d) continue;
    const arr = itemsByDate.get(d) || [];
    if (it && !it.contract_id) it.contract_id = contractId;
    arr.push(it);
    itemsByDate.set(d, arr);
  }

  const bufFrom = addDays(state.win.from, -7);
  const bufTo   = addDays(state.win.to,   +7);
  const weeksForIndex = (await getContractCalendar(contractId, { from: bufFrom, to: bufTo, granularity:'week' })).items || [];
  const weekIndex = buildWeekIndex(weeksForIndex);

  const overlayedMap = applyStagedContractCalendarOverlay(contractId, itemsByDate, weekIndex);

  const gridHost = document.createElement('div'); gridHost.id = 'contractDayGrid';
  const container = byId('__contractCal') || holder;
  if (container === holder) holder.innerHTML = '';
  container.innerHTML = '';
  container.appendChild(gridHost);

  renderDayGrid(gridHost, {
    from: state.win.from,
    to: state.win.to,
    itemsByDate: overlayedMap,
    view: state.view,
    bucketKey: `c:${contractId}`,
    isInteractive: !!window.__calInteractive,
    onNav: async (delta) => {
      if (state.view === 'year') {
        const y = ymdToDate(state.win.from).getUTCFullYear() + delta;
        const nextWin = computeYearWindow(y);
        await fetchAndRenderContractCalendar(contractId, { from: nextWin.from, to: nextWin.to, view: 'year' });
      } else {
        const nextWin = stepMonth(state.win, delta);
        await fetchAndRenderContractCalendar(contractId, { from: nextWin.from, to: nextWin.to, view: 'month' });
      }
    },
    onToggleView: async () => {
      const newView = (state.view === 'year') ? 'month' : 'year';
      let win = state.win;
      if (newView === 'year') { const y = ymdToDate(state.win.from).getUTCFullYear(); win = computeYearWindow(y); }
      else { const dt = ymdToDate(state.win.from); win = computeMonthWindow(dt.getUTCFullYear(), dt.getUTCMonth()); }
      await fetchAndRenderContractCalendar(contractId, { from: win.from, to: win.to, view: newView });
    },
    onCellContextMenu: (theDate, ev) => {
      if (!window.__calInteractive) return;
      const sel = initSelBucket(`c:${contractId}`).set; const selArr = [...sel];

      const ownedByCurrent = (d) => {
        const arr = itemsByDate.get(d) || [];
        return arr.some(it => String(it.contract_id || '') === String(contractId));
      };
      const occupiedByOtherOnly = (d) => {
        const arr = itemsByDate.get(d) || [];
        return arr.some(it => {
          const cid = String(it.contract_id || '');
          return cid && cid !== String(contractId);
        }) && !ownedByCurrent(d);
      };
      const anyGrey = selArr.some(occupiedByOtherOnly);

      const resolveFinalState = (d) => topState(overlayedMap.get(d) || []);

      const eligibleUnbook = (d) => {
        if (!ownedByCurrent(d)) return false;
        const we = computeWeekEnding(d, state.weekEndingWeekday);
        const w = weekIndex.get(we);
        const st = resolveFinalState(d);
        return st === 'PLANNED' && w && !w.baseHasTs;
      };
      const anyEligible = selArr.some(eligibleUnbook);
      const allEligible = selArr.every(eligibleUnbook);
      const blockMode = isConsecutiveDailyRun(selArr);

      const canBook = selArr.every(d => !ownedByCurrent(d));
      const canUnbook = blockMode ? anyEligible : allEligible;

      const canAddAdditional = selArr.some(d => {
        const st = resolveFinalState(d);
        const we = computeWeekEnding(d, state.weekEndingWeekday);
        const w = weekIndex.get(we);
        return st === 'EMPTY' && w && w.baseHasTs && w.baseWeekId;
      });

      openCalendarContextMenu({
        anchorEl: ev.target,
        bucketKey: `c:${contractId}`,
        selection: selArr,
        capabilities: { canBook, canUnbook, canAddAdditional },
        onAction: async ({ type, selection }) => {
          try {
            if (type === 'book') {
              if (anyGrey) {
                if (!window.confirm('This would clash with an existing contract on some selected dates. Continue?')) return;
              }
              stageContractCalendarBookings(contractId, selection);
            }
            if (type === 'unbook') {
              const toUnbook = selection.filter(eligibleUnbook);
              if (toUnbook.length) stageContractCalendarUnbookings(contractId, toUnbook);
            }
            if (type === 'additional') {
              const byBase = {};
              for (const d of selection) {
                const we = computeWeekEnding(d, state.weekEndingWeekday);
                const wi = weekIndex.get(we);
                if (!wi || !wi.baseWeekId || !wi.baseHasTs) continue;
                (byBase[wi.baseWeekId] ||= []).push(d);
              }
              for (const [baseWeekId, dates] of Object.entries(byBase)) {
                stageContractCalendarAdditional(contractId, baseWeekId, dates);
              }
            }
            try { window.dispatchEvent(new Event('modal-dirty')); } catch {}
            const prev = (byId('__calScroll') || holder).scrollTop || 0;
            await fetchAndRenderContractCalendar(contractId, { from: state.win.from, to: state.win.to, view: state.view });
            (byId('__calScroll') || holder).scrollTop = prev;
          } catch (e) {
            alert(e?.message || e);
          }
        }
      });
    }
  });

  (byId('__calScroll') || holder).scrollTop = state.scrollTop;
}
async function renderCandidateCalendarTab(candidateId) {
  const holderId = 'candidateCalendarHolder';
  const host = byId(holderId);
  if (!host) return;

  // Compute initial window (year view)
  const y = (new Date()).getUTCFullYear();
  const win = typeof computeYearWindow === 'function'
    ? computeYearWindow(y)
    : { from: `${y}-01-01`, to: `${y}-12-31` };

  // Build inner scaffold: scroll box + grid mount + legend
  host.innerHTML = `
    <div class="tabc" style="display:flex;flex-direction:column;gap:8px;height:calc(72vh);max-height:calc(72vh)">
      <div id="__candCalScroll" style="flex:1;min-height:0;overflow:auto;border:1px solid var(--line,#e5e5e5);border-radius:8px;padding:4px;">
        <div id="__candCal"></div>
      </div>
      <div id="__candCalLegend"></div>
    </div>
  `;

  // Render legend
  const legendHost = byId('__candCalLegend');
  if (legendHost && typeof renderCalendar === 'undefined') {
    // Reuse existing legend helper (does full overwrite of target node)
    if (typeof renderCalendarLegend === 'function') {
      renderCalendarLegend(legendHost);
    } else {
      // Fallback: minimal legend if helper is missing
      legendHost.innerHTML = `
        <div class="legend">
          <span class="chip cal-planned">Planned</span>
          <span class="chip cal-submitted">Submitted</span>
          <span class="chip cal-authorised">Certified</span>
          <span class="chip cal-invoiced">Invoiced</span>
          <span class="chip cal-paid">Paid</span>
          <span class="chip">Not booked</span>
        </div>`;
    }
  }

  // Draw initial window
  await fetchAndRenderCandidateCalendar(candidateId, { from: win.from, to: win.to, view: 'year' });
}
async function fetchAndRenderCandidateCalendar(candidateId, opts) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][candidate]', ...a); };

  const key = `cand:${candidateId}`;

  // Persist per-view state (separate from any contract bucket).
  const state = (window.__calState[key] ||= {
    view: (opts && typeof opts.view === 'string') ? opts.view : 'year',
    win:  (opts && opts.from && opts.to)
            ? { from: opts.from, to: opts.to }
            : (typeof computeYearWindow === 'function'
                ? computeYearWindow((new Date()).getUTCFullYear())
                : { from: `${(new Date()).getFullYear()}-01-01`, to: `${(new Date()).getFullYear()}-12-31` }),
    scrollTop: 0
  });

  // Allow caller to change view/window
  if (opts && typeof opts.view === 'string') state.view = opts.view;
  if (opts && opts.from && opts.to) state.win = { from: opts.from, to: opts.to };

  const holder = byId('candidateCalendarHolder');
  if (!holder) return;

  // Correct scroll box id (and legacy fallback to container)
  const scrollBox = byId('__candCalScroll') || holder;

  // Remember current scroll
  try { state.scrollTop = (typeof scrollBox.scrollTop === 'number') ? scrollBox.scrollTop : 0; } catch {}

  // Fetch candidate-wide day feed (all contracts; read-only render)
  let items = [];
  try {
    if (typeof getCandidateCalendarRange === 'function') {
      const r = await getCandidateCalendarRange(candidateId, state.win.from, state.win.to, 'day');
      items = Array.isArray(r?.items) ? r.items : [];
    } else if (typeof fetchCandidateCalendarForRange === 'function') {
      const r = await fetchCandidateCalendarForRange(candidateId, state.win.from, state.win.to);
      items = Array.isArray(r?.items) ? r.items : [];
    } else if (typeof getCandidateCalendar === 'function') {
      const r = await getCandidateCalendar(candidateId, state.win.from, state.win.to);
      items = Array.isArray(r?.items) ? r.items : [];
    }
  } catch (e) {
    L('candidate calendar fetch failed', e);
  }

  // Build map: date → [items]
  const itemsByDate = (typeof buildDateIndex === 'function')
    ? buildDateIndex(items)
    : (() => {
        const m = new Map();
        for (const it of items) {
          if (!it?.date) continue;
          const arr = m.get(it.date) || [];
          arr.push(it);
          m.set(it.date, arr);
        }
        return m;
      })();

  // Render grid (read-only)
  const gridHost = byId('__candCal') || (() => {
    const d = document.createElement('div'); d.id = '__candCal'; holder.appendChild(d); return d;
  })();
  gridHost.innerHTML = '';

  if (typeof renderDayGrid !== 'function') {
    gridHost.innerHTML = `<div class="hint">Calendar renderer not available.</div>`;
    return;
  }

  renderDayGrid(gridHost, {
    from: state.win.from,
    to:   state.win.to,
    itemsByDate,
    view: state.view,          // 'year' | 'month'
    bucketKey: key,            // not a contract id → no “other” greying
    isInteractive: false,      // candidate view is read-only
    onNav: async (delta) => {
      if (state.view === 'year') {
        const y = (new Date(state.win.from)).getUTCFullYear() + delta;
        const nextWin = (typeof computeYearWindow === 'function')
          ? computeYearWindow(y)
          : { from: `${y}-01-01`, to: `${y}-12-31` };
        await fetchAndRenderCandidateCalendar(candidateId, { from: nextWin.from, to: nextWin.to, view: 'year' });
      } else {
        const nextWin = (typeof stepMonth === 'function')
          ? stepMonth(state.win, delta)
          : (function () {
              const d = new Date(state.win.from);
              d.setUTCMonth(d.getUTCMonth() + delta, 1);
              const y = d.getUTCFullYear(), m = d.getUTCMonth();
              return (typeof computeMonthWindow === 'function')
                ? computeMonthWindow(y, m)
                : { from: `${y}-${String(m+1).padStart(2,'0')}-01`, to: `${y}-${String(m+1).padStart(2,'0')}-28` };
            })();
        await fetchAndRenderCandidateCalendar(candidateId, { from: nextWin.from, to: nextWin.to, view: 'month' });
      }
    },
    onToggleView: async () => {
      const newView = (state.view === 'year') ? 'month' : 'year';
      let nextWin;
      if (newView === 'year') {
        const y = (new Date(state.win.from)).getUTCFullYear();
        nextWin = (typeof computeYearWindow === 'function')
          ? computeYearWindow(y)
          : { from: `${y}-01-01`, to: `${y}-12-31` };
      } else {
        const d = new Date(state.win.from);
        const y = d.getUTCFullYear(), m = d.getUTCMonth();
        nextWin = (typeof computeMonthWindow === 'function')
          ? computeMonthWindow(y, m)
          : { from: `${y}-${String(m+1).padStart(2,'0')}-01`, to: `${y}-${String(m+1).padStart(2,'0')}-28` };
      }
      await fetchAndRenderCandidateCalendar(candidateId, { from: nextWin.from, to: nextWin.to, view: newView });
    }
  });

  // Restore scroll
  try { if (typeof scrollBox.scrollTop === 'number') scrollBox.scrollTop = state.scrollTop; } catch {}
}


function renderCandidateContractList(container, contractMap, handlers) {
  container.innerHTML = '';
  const title = document.createElement('div'); title.className='hint'; title.textContent='Contracts in view:'; container.appendChild(title);

  const list = document.createElement('div'); list.className='list';
  contractMap.forEach((v, cid) => {
    const row = document.createElement('div'); row.className='row item'; row.tabIndex = 0;
    row.innerHTML = `
      <span class="txt">${(v.client_name || 'Client')} • ${(v.role||'Role')}${v.band?` • ${v.band}`:''} • ${v.from} → ${v.to}</span>
      <span class="act"><button data-act="filter">Show only</button> <button data-act="open">Open</button></span>`;
    row.querySelector('[data-act="filter"]')?.addEventListener('click', () => handlers.onClick && handlers.onClick(cid));
    row.querySelector('[data-act="open"]')?.addEventListener('click', () => handlers.onDblClick && handlers.onDblClick(cid));
    row.addEventListener('dblclick', () => handlers.onDblClick && handlers.onDblClick(cid));
    list.appendChild(row);
  });
  container.appendChild(list);

  if (!contractMap.size) {
    const none = document.createElement('div'); none.className='hint'; none.textContent='No contracts in this window.'; container.appendChild(none);
  }

  const clear = document.createElement('div'); clear.className='actions';
  clear.innerHTML = `<button id="candClearFilter">Clear filter</button>`; container.appendChild(clear);
  clear.querySelector('#candClearFilter')?.addEventListener('click', () => handlers.onClear && handlers.onClear());
}


// =================== MOUNT CLIENT RATES TAB (unchanged glue) ===================
async function mountClientRatesTab() {
  const LOG_RATES = !!window.__LOG_RATES;
  const DBG = (...a)=> { if (LOG_RATES) console.log('[RATES][mountClientRatesTab]', ...a); };

  const ctx = window.modalCtx; // use canonical context
  DBG('ENTRY', { ctxEntity: ctx?.entity, ctxId: ctx?.data?.id });

  // render uses ctx.ratesState directly; no args needed
  try { await renderClientRatesTable(); DBG('renderClientRatesTable done'); } catch (e) { DBG('renderClientRatesTable error', e); }

  // Always resolve a real client id before opening the modal
  const btn = byId('btnAddClientRate');
  if (btn) {
    btn.onclick = () => {
      const cid = (ctx && ctx.data && (ctx.data.id || ctx.data.client_id)) || null;
      DBG('openClientRateModal from button', { cid });
      return openClientRateModal(cid);
    };
    DBG('wired btnAddClientRate');
  } else {
    DBG('btnAddClientRate not present');
  }
}



// =================== MOUNT HOSPITALS TAB (unchanged glue) ===================
function mountClientHospitalsTab() {
  const ctx = window.modalCtx; // 🔧 use canonical context
  const H = ctx.hospitalsState || (ctx.hospitalsState = { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() });

  // ⬇️ Key change: normalise stagedDeletes as a Set on mount (handles JSON-cloned arrays)
  if (!(H.stagedDeletes instanceof Set)) {
    H.stagedDeletes = new Set(Array.isArray(H.stagedDeletes) ? H.stagedDeletes : Object.keys(H.stagedDeletes || {}));
  }

  renderClientHospitalsTable();

  const addBtn = byId('btnAddClientHospital');
  if (addBtn) addBtn.onclick = () => openClientHospitalModal(ctx.data?.id);

  const wrap = byId('clientHospitals');
  if (wrap && !wrap.__wiredDelete) {
    wrap.addEventListener('click', (ev) => {
      const t = ev.target;
      const el = t && (t.closest('[data-action="delete"]') || t.closest('.btnDelHospital'));
      if (!el) return;

      const hid = el.getAttribute('data-hid') || el.getAttribute('data-id');
      if (!hid) return;

      // ⬇️ Ensure Set semantics before use
      if (!(H.stagedDeletes instanceof Set)) {
        H.stagedDeletes = new Set(Array.isArray(H.stagedDeletes) ? H.stagedDeletes : Object.keys(H.stagedDeletes || {}));
      }
      H.stagedDeletes.add(String(hid));

      try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
      renderClientHospitalsTable();
    }, true);
    wrap.__wiredDelete = true;
  }
}


async function openClientRateModal(client_id, existing) {
  const parentFrame = _currentFrame();
  const parentEditable = parentFrame && (parentFrame.mode === 'edit' || parentFrame.mode === 'create');
  const APILOG = (typeof window !== 'undefined' && !!window.__LOG_API) || (typeof __LOG_API !== 'undefined' && !!__LOG_API);
  const LOG_RATES = !!window.__LOG_RATES;
  const DBG = (...a)=> { if (LOG_RATES) console.log('[RATES][openClientRateModal]', ...a); };

  const ctx = window.modalCtx || {};
  const resolvedClientId =
    client_id ||
    (existing && existing.client_id) ||
    (ctx && ctx.data && (ctx.data.id || ctx.data.client_id)) ||
    null;

  if (APILOG) console.log('[openClientRateModal] resolvedClientId', resolvedClientId, { passed: client_id, existing });
  DBG('ENTRY', { parentEditable, hasExisting: !!existing, resolvedClientId, ctxEntity: ctx?.entity, ctxId: ctx?.data?.id });

  const globalRoles = await loadGlobalRoleOptions();
  const roleOptions = globalRoles.map(r => `<option value="${r}">${r}</option>`).join('') + `<option value="__OTHER__">+ Add new role…</option>`;
  DBG('loaded role options', { count: globalRoles.length });

  const ex  = existing || {};
  const who = ex.disabled_by_name || '';
  const when = ex.disabled_at_utc ? formatIsoToUk(String(ex.disabled_at_utc).slice(0,10)) : '';
  const isDisabled = !!ex.disabled_at_utc;
  DBG('existing status', { isDisabled, who, when, id: ex.id });

  const statusBlock = `
    <div class="row" id="cl_status_row" style="align-items:center; gap:8px;">
      <div>
        ${isDisabled ? `
          <span class="pill tag-fail" id="cl_status_pill">❌ Disabled</span>
          <div class="hint" id="cl_status_meta">${who ? `by ${escapeHtml(who)}` : ''} ${when ? `on ${escapeHtml(when)}` : ''}</div>`
      : `
          <span class="pill tag-ok" id="cl_status_pill">✓ Active</span>
          <div class="hint" id="cl_status_meta">&nbsp;</div>`}
      </div>
      ${parentEditable && ex.id ? `<div><button id="cl_toggle_btn" class="btn btn-outline btn-sm">${isDisabled ? 'Enable' : 'Disable'}</button></div>` : ''}
    </div>`;

  function sameRow(a, b) {
    const eq = !!(a && b && ((a === b) || (a.id && b.id && String(a.id) === String(b.id)) || (a.__localKey && b.__localKey && String(a.__localKey) === String(b.__localKey))));
    if (LOG_RATES) console.log('[RATES][sameRow]', { aId:a?.id, bId:b?.id, aKey:a?.__localKey, bKey:b?.__localKey, eq });
    return eq;
  }

  // INLINE panel for client-rate warnings/fixes
  const formHtml = html(`
    <div class="form" id="clientRateForm">
      ${statusBlock}
      <div class="row">
        <label>Role (required)</label>
        <select name="role" id="cl_role" required ${parentEditable ? '' : 'disabled'}>
          <option value="">Select role…</option>
          ${roleOptions}
        </select>
      </div>
      <div class="row" id="cl_role_new_row" style="display:none">
        <label>New role code</label>
        <input type="text" id="cl_role_new" placeholder="e.g. RMN-Lead" ${parentEditable ? '' : 'disabled'} />
        <div class="hint">Uppercase letters/numbers/[-_/ ] recommended.</div>
      </div>
      <div class="row"><label>VBR5809: Band (optional)</label>
        <input type="text" name="band" id="cl_band" value="${ex.band ?? ''}" ${parentEditable ? '' : 'disabled'} />
      </div>
      <div class="row"><label>Effective from (DD/MM/YYYY)</label>
        <input type="text" name="date_from" id="cl_date_from" placeholder="DD/MM/YYYY" ${parentEditable ? '' : 'disabled'} />
      </div>
      <div class="row"><label>Effective to (optional, DD/MM/YYYY)</label>
        <input type="text" name="date_to" id="cl_date_to" placeholder="DD/MM/YYYY" ${parentEditable ? '' : 'disabled'} />
      </div>
      <div class="row" style="grid-column: 1 / -1">
        <table class="grid" style="width:100%;border-collapse:collapse">
          <thead><tr><th>Bucket</th><th>PAYE pay</th><th>Umbrella pay</th><th>Charge</th></tr></thead>
          <tbody>
            ${['day','night','sat','sun','bh'].map(b => `
              <tr>
                <td style="white-space:nowrap">${b.toUpperCase()}</td>
                <td><input type="number" step="0.01" name="paye_${b}" ${parentEditable ? '' : 'disabled'} /></td>
                <td><input type="number" step="0.01" name="umb_${b}"  ${parentEditable ? '' : 'disabled'} /></td>
                <td><input type="number" step="0.01" name="charge_${b}" ${parentEditable ? '' : 'disabled'} /></td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>
      <div class="row" style="grid-column:1 / -1; margin-top:10px">
        <table class="grid" id="cl_margins_tbl" style="width:100%">
          <thead><tr><th>Bucket</th><th>PAYE margin</th><th>Umbrella margin</th></tr></thead>
          <tbody>
            ${['day','night','sat','sun','bh'].map(b=>`
              <tr><td>${b.toUpperCase()}</td><td><span id="m_paye_${b}">—</span></td><td><span id="m_umb_${b}">—</span></td></tr>`).join('')}
          </tbody>
        </table>
        <div class="hint" id="cl_delete_hint" style="display:none;margin-top:6px"></div>
      </div>

      <div id="cl_err_panel" style="grid-column:1/-1; margin-top:10px; display:none; border:1px solid #7f1d1d; background:rgba(239,68,68,.08); color:#fecaca; padding:10px; border-radius:8px"></div>
    </div>
  `);

  // ERNI
  async function _erniMultiplier(){
    if (typeof window.__ERNI_MULT__ === 'number') return window.__ERNI_MULT__;
    try {
      if (typeof getSettingsCached === 'function') {
        const s = await getSettingsCached();
        let p = s?.erni_pct ?? s?.employers_ni_percent ?? 0;
        p = Number(p) || 0; if (p > 1) p = p/100;
        window.__ERNI_MULT__ = 1 + p;
        return window.__ERNI_MULT__;
      }
    } catch {}
    window.__ERNI_MULT__ = 1;
    return 1;
  }

  function setApplyEnabled(enabled){
    if (LOG_RATES) console.log('[RATES][setApplyEnabled]', { enabled });
    try {
      const btn = document.querySelector('#modal .btn-save, #modal .actions .primary, #modal .actions .btn-primary, .modal .btn-save');
      if (btn) { btn.disabled = !enabled; btn.classList.toggle('disabled', !enabled); }
    } catch (e) { if (LOG_RATES) console.warn('[RATES][setApplyEnabled] button toggle failed', e); }
    // Inform parent showModal so child Save can proceed
    try { window.dispatchEvent(new CustomEvent('modal-apply-enabled', { detail: { enabled } })); } catch (e) { if (LOG_RATES) console.warn('[RATES][setApplyEnabled] dispatch failed', e); }
  }
  const numOrNull = v => { if (v===undefined||v===null) return null; if (typeof v === 'string' && v.trim()==='') return null; const n=Number(v); return Number.isFinite(n) ? n : null; };
  const fmt = v => (v==null || Number.isNaN(v)) ? '—' : (Math.round(v*100)/100).toFixed(2);

  function showClientInlineError(html){
    const p = byId('cl_err_panel');
    if (!p) return;
    if (html && String(html).trim() !== '') {
      p.innerHTML = html;
      p.style.display = '';
    } else {
      p.innerHTML = '';
      p.style.display = 'none';
    }
  }
  function isoMinusOneDay(iso){
    if (!iso) return null;
    const d = new Date(iso + 'T00:00:00Z');
    d.setUTCDate(d.getUTCDate() - 1);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth()+1).padStart(2,'0');
    const dd= String(d.getUTCDate()).padStart(2,'0');
    return `${y}-${m}-${dd}`;
  }
  function rangesOverlap(a0,a1,b0,b1){
    const A0 = a0 || '0000-01-01', A1 = a1 || '9999-12-31';
    const B0 = b0 || '0000-01-01', B1 = b1 || '9999-12-31';
    return !(A1 < B0 || B1 < A0);
  }

  // Gate inputs + margins + Apply + overlap inline fixes
  async function recomputeClientState(){
    const mult = await _erniMultiplier();
    const roleVal = (byId('cl_role')?.value || '').trim();
    const fromIso = parseUkDateToIso(byId('cl_date_from')?.value || '');
    const toIso   = parseUkDateToIso(byId('cl_date_to')?.value || '');

    DBG('recomputeClientState: ENTRY', { roleVal, fromIso, toIso, exId: ex?.id });

    const getIn = sel => document.querySelector(sel);
    const payeInputs = ['day','night','sat','sun','bh'].map(b => getIn(`#clientRateForm input[name="paye_${b}"]`));
    const umbInputs  = ['day','night','sat','sun','bh'].map(b => getIn(`#clientRateForm input[name="umb_${b}"]`));
    const chgInputs  = ['day','night','sat','sun','bh'].map(b => getIn(`#clientRateForm input[name="charge_${b}"]`));
    const allInputs  = [...payeInputs, ...umbInputs, ...chgInputs].filter(Boolean);

    let canApply = true;
    showClientInlineError('');

    if (!roleVal || !fromIso) {
      DBG('recomputeClientState: NOT_READY', { roleVal, fromIso });
      allInputs.forEach(inp => { inp.disabled = true; });
      // clear preview
      ['day','night','sat','sun','bh'].forEach(b=>{
        const spP = byId(`m_paye_${b}`), spU = byId(`m_umb_${b}`);
        if (spP) spP.textContent = '—';
        if (spU) spU.textContent = '—';
      });
      setApplyEnabled(false);
      return;
    }

    // keep inputs enabled
    allInputs.forEach(inp => { inp.disabled = true === false; });

    // Compute & render margins preview (2dp), while validating
    let hasNegative = false;
    const fmt2 = (v) => (v==null || Number.isNaN(v)) ? '—' : (Math.round(v*100)/100).toFixed(2);

    ['day','night','sat','sun','bh'].forEach(bucket=>{
      const paye = numOrNull(getIn(`#clientRateForm input[name="paye_${bucket}"]`)?.value);
      const umb  = numOrNull(getIn(`#clientRateForm input[name="umb_${bucket}"]`)?.value);
      const chg  = numOrNull(getIn(`#clientRateForm input[name="charge_${bucket}"]`)?.value);

      const payeMargin = (paye!=null && chg!=null) ? (chg - (paye * mult)) : null;
      const umbMargin  = (umb!=null  && chg!=null) ? (chg - umb)          : null;

      const spP = byId(`m_paye_${bucket}`), spU = byId(`m_umb_${bucket}`);
      if (spP) spP.textContent = fmt2(payeMargin);
      if (spU) spU.textContent = fmt2(umbMargin);

      if ((payeMargin != null && payeMargin < 0) || (umbMargin != null && umbMargin < 0)) hasNegative = true;
    });

    if (hasNegative) { canApply = false; showClientInlineError('One or more buckets would have a negative margin. Adjust pay/charge.'); }

    // Overlap detection + Fix buttons
    const staged = Array.isArray(ctx.ratesState) ? ctx.ratesState.slice() : [];
    const sameCat  = r => String(r.role||'') === roleVal && String(r.band??'') === String((byId('cl_band')?.value||'').trim()||'');
    const isSelf   = r => existing ? sameRow(r, existing) : false;

    const conflicts = staged
      .filter(r => !r.disabled_at_utc && sameCat(r) && !isSelf(r))
      .filter(r => rangesOverlap(r.date_from||null, r.date_to||null, fromIso, toIso||null));

    if (conflicts.length) {
      canApply = false;
      const ov = conflicts[0];
      const overlapStart = (fromIso > (ov.date_from||'0000-01-01')) ? fromIso : (ov.date_from||'');
      const overlapEnd   = ((toIso||'9999-12-31') < (ov.date_to||'9999-12-31')) ? (toIso||'') : (ov.date_to||'');
      const cutThis      = isoMinusOneDay(ov.date_from||'');
      const cutOther     = isoMinusOneDay(fromIso);

      let fixButtons = '';
      if (cutThis && (!fromIso || cutThis >= fromIso)) {
        fixButtons += `<button id="cl_fix_this" class="btn" style="margin-right:8px">Fix: Shorten <b>THIS</b> to ${formatIsoToUk(cutThis)}</button>`;
      }
      if (cutOther && (!ov.date_from || cutOther >= (ov.date_from))) {
        fixButtons += `<button id="cl_fix_other" class="btn">Fix: Shorten <b>OTHER</b> to ${formatIsoToUk(cutOther)}</button>`;
      }

      const msg = `
        <div style="font-weight:700;margin-bottom:6px">Overlap detected</div>
        <div style="margin-bottom:6px">
          Another client-default window exists for <b>${escapeHtml(roleVal)}</b>${(byId('cl_band')?.value||'').trim()?` / <b>${escapeHtml((byId('cl_band')?.value||'').trim())}</b>`:''}.<br/>
          <span class="hint" style="color:#fecaca">Overlap span:</span> <b>${formatIsoToUk(overlapStart)} – ${formatIsoToUk(overlapEnd)}</b>.
        </div>
        <div style="margin-bottom:8px">To proceed, adjust dates, or use a Fix:</div>
        <div>${fixButtons || '<i>No safe automatic fix available. Please adjust dates.</i>'}</div>
      `;
      showClientInlineError(msg);

      setTimeout(() => {
        const fromEl = byId('cl_date_from');
        const toEl   = byId('cl_date_to');
        const fixThis = byId('cl_fix_this');
        if (fixThis && cutThis) fixThis.onclick = () => {
          try { toEl.value = formatIsoToUk(cutThis); recomputeClientState(); } catch {}
        };
        const fixOther = byId('cl_fix_other');
        if (fixOther && cutOther) fixOther.onclick = () => {
          try {
            // stage OTHER truncate (stays staged until parent Save)
            if (ov.id) {
              const idx = staged.findIndex(r => r.id === ov.id);
              if (idx >= 0) ctx.ratesState[idx] = { ...ctx.ratesState[idx], date_to: cutOther };
            } else if (ov.__localKey) {
              const idx = staged.findIndex(r => r.__localKey === ov.__localKey);
              if (idx >= 0) ctx.ratesState[idx] = { ...ctx.ratesState[idx], date_to: cutOther };
            }
            try { renderClientRatesTable(); } catch {}
            recomputeClientState();
          } catch {}
        };
      }, 0);
    }

    DBG('recomputeClientState: EXIT', { canApply });
    setApplyEnabled(canApply);
  }

  const formTabLabel = `Form — ${isDisabled ? 'Inactive' : 'Active'}`;

  // showModal (child) — persist staged edits only; no popups for errors
  showModal(
    existing ? 'Edit Client Default Window' : 'Add/Upsert Client Default Window',
    [{ key:'form', label: formTabLabel }],
    () => formHtml,
    async () => {
      const stack = window.__modalStack || [];
      const pf = stack.length > 1 ? stack[stack.length - 2] : null;
      DBG('onSave ENTRY', { stackLen: stack.length, hasParent: !!pf, parentMode: pf?.mode });

      if (!pf || (pf.mode !== 'edit' && pf.mode !== 'create')) {
        DBG('onSave BLOCKED: parent not editable');
        return false;
      }

      // Re-validate live state; rely on validation result (do not read DOM disabled state)
      await recomputeClientState();
      DBG('onSave proceeding after recompute');

      const raw = collectForm('#clientRateForm');
      DBG('onSave collected', { raw });

      let role = (raw.role || '').trim();
      const newRole = (document.getElementById('cl_role_new')?.value || '').trim();
      if (role === '__OTHER__') {
        if (!newRole) { showClientInlineError('Enter a new role code.'); setApplyEnabled(false); DBG('onSave BLOCKED: newRole missing'); return false; }
        role = newRole.toUpperCase();
        if (typeof invalidateGlobalRoleOptionsCache === 'function') {
          try { invalidateGlobalRoleOptionsCache(); window.dispatchEvent(new CustomEvent('global-roles-updated')); } catch {}
        }
      }
      if (!role) { showClientInlineError('Role is required.'); setApplyEnabled(false); DBG('onSave BLOCKED: role missing'); return false; }

      const isoFrom = parseUkDateToIso(raw.date_from);
      if (!isoFrom) { showClientInlineError('Invalid “Effective from” date.'); setApplyEnabled(false); DBG('onSave BLOCKED: date_from invalid'); return false; }
      let isoTo = null;
      if (raw.date_to) {
        isoTo = parseUkDateToIso(raw.date_to);
        if (!isoTo) { showClientInlineError('Invalid “Effective to” date.'); setApplyEnabled(false); DBG('onSave BLOCKED: date_to invalid'); return false; }
        if (isoTo < isoFrom) { showClientInlineError('“Effective to” cannot be before “Effective from”.'); setApplyEnabled(false); DBG('onSave BLOCKED: date order invalid'); return false; }
      }

      const staged = {
        id: existing?.id || undefined,
        client_id: resolvedClientId,
        role,
        band: (raw.band || '').trim() || null,
        date_from: isoFrom,
        date_to:   isoTo,

        charge_day  : raw['charge_day']  !== '' ? Number(raw['charge_day'])  : null,
        charge_night: raw['charge_night']!== '' ? Number(raw['charge_night']) : null,
        charge_sat  : raw['charge_sat']  !== '' ? Number(raw['charge_sat'])  : null,
        charge_sun  : raw['charge_sun']  !== '' ? Number(raw['charge_sun'])  : null,
        charge_bh   : raw['charge_bh']   !== '' ? Number(raw['charge_bh'])   : null,

        paye_day    : raw['paye_day']    !== '' ? Number(raw['paye_day'])    : null,
        paye_night  : raw['paye_night']  !== '' ? Number(raw['paye_night'])  : null,
        paye_sat    : raw['paye_sat']    !== '' ? Number(raw['paye_sat'])    : null,
        paye_sun    : raw['paye_sun']    !== '' ? Number(raw['paye_sun'])    : null,
        paye_bh     : raw['paye_bh']     !== '' ? Number(raw['paye_bh'])     : null,

        umb_day     : raw['umb_day']     !== '' ? Number(raw['umb_day'])     : null,
        umb_night   : raw['umb_night']   !== '' ? Number(raw['umb_night'])   : null,
        umb_sat     : raw['umb_sat']     !== '' ? Number(raw['umb_sat'])     : null,
        umb_sun     : raw['umb_sun']     !== '' ? Number(raw['umb_sun'])     : null,
        umb_bh      : raw['umb_bh']      !== '' ? Number(raw['umb_bh'])      : null,

        // carry through disabled status + pending toggle marker (for UI/meta)
        disabled_at_utc : ex.disabled_at_utc ?? null,
        disabled_by_name: ex.disabled_by_name ?? null,
        __toggle        : ex.__toggle || undefined,
        __localKey      : existing?.__localKey || undefined,
        __delete        : existing?.__delete || false
      };

      if (ex.__toggle === 'enable') {
        staged.disabled_at_utc = null;
      } else if (ex.__toggle === 'disable') {
        staged.disabled_at_utc = staged.disabled_at_utc || new Date().toISOString().slice(0,10);
      }

      // Stage only (persist on parent Save)
      const before = { len: Array.isArray(ctx.ratesState) ? ctx.ratesState.length : 0 };
      ctx.ratesState = Array.isArray(ctx.ratesState) ? ctx.ratesState : [];
      if (existing) {
        const idx = ctx.ratesState.findIndex(r => sameRow(r, existing));
        if (idx >= 0) ctx.ratesState[idx] = staged; else ctx.ratesState.push(staged);
      } else {
        const already = ctx.ratesState.findIndex(r => sameRow(r, staged));
        if (already >= 0) ctx.ratesState[already] = staged; else ctx.ratesState.push(staged);
      }
      const after = { len: ctx.ratesState.length };
      DBG('onSave STAGED', { before, after, stagedId: staged.id, stagedRole: staged.role, toggle: staged.__toggle });

      try { const parent = _currentFrame(); if (parent && typeof parent.setTab === 'function') { parent.currentTabKey = 'rates'; parent.setTab('rates'); DBG('onSave: parent.setTab(rates)'); } } catch{}
      try { window.dispatchEvent(new CustomEvent('modal-dirty')); DBG('onSave: dispatched modal-dirty'); } catch {}
      try { await renderClientRatesTable(); DBG('onSave: renderClientRatesTable done'); } catch (e) { DBG('onSave: renderClientRatesTable error', e); }

      DBG('onSave EXIT ok=true');
      return true;
    },
    false,
    () => {
      const parent = _currentFrame();
      if (parent) { parent.currentTabKey = 'rates'; parent.setTab('rates'); DBG('onReturn: parent.setTab(rates)'); }
    },
    { kind: 'client-rate' }
  );

  // Hydrate & wire listeners
  const roleSel   = byId('cl_role');
  const roleNew   = byId('cl_role_new');
  const roleNewRow= byId('cl_role_new_row');
  const bandEl    = byId('cl_band');
  const fromEl    = byId('cl_date_from');
  const toEl      = byId('cl_date_to');

  attachUkDatePicker(fromEl); attachUkDatePicker(toEl);
  if (existing?.date_from) fromEl.value = formatIsoToUk(existing.date_from);
  if (existing?.date_to)   toEl.value   = formatIsoToUk(existing.date_to);

  if (existing?.role) {
    if (globalRoles.includes(existing.role)) {
      roleSel.value = existing.role;
      roleNewRow.style.display = 'none';
      roleNew.value = '';
    } else {
      roleSel.value = '__OTHER__';
      roleNewRow.style.display = '';
      roleNew.value = existing.role;
    }
  } else {
    roleNewRow.style.display = 'none';
  }

  // ===== Prefill with two-decimal rendering (no other changes) =====
  ['day','night','sat','sun','bh'].forEach(b=>{
    const set = (name, val) => {
      const el = document.querySelector(`#clientRateForm input[name="${name}_${b}"]`);
      if (el && typeof val !== 'undefined' && val !== null) {
        const num = Number(val);
        el.value = Number.isFinite(num) ? (Math.round(num*100)/100).toFixed(2) : String(val);
      }
    };
    set('paye',   existing?.[`paye_${b}`]);
    set('umb',    existing?.[`umb_${b}`]);
    set('charge', existing?.[`charge_${b}`]);
  });

  // Recompute once on mount
  DBG('mount: recomputeClientState');
  await recomputeClientState();

  roleSel.addEventListener('change', async ()=>{
    DBG('EVENT: role change', { value: roleSel.value });
    if (roleSel.value === '__OTHER__') { roleNewRow.style.display = ''; }
    else { roleNewRow.style.display = 'none'; roleNew.value=''; }
    await recomputeClientState();
  });
  bandEl.addEventListener('input',  () => { DBG('EVENT: band input', { value: bandEl.value }); recomputeClientState(); });
  fromEl.addEventListener('change', () => { DBG('EVENT: date_from change', { value: fromEl.value }); recomputeClientState(); });
  toEl.addEventListener('change',   () => { DBG('EVENT: date_to change', { value: toEl.value }); recomputeClientState(); });
  ['day','night','sat','sun','bh'].forEach(b=>{
    ['paye','umb','charge'].forEach(kind=>{
      const el = document.querySelector(`#clientRateForm input[name="${kind}_${b}"]`);
      if (el) el.addEventListener('input', () => { DBG('EVENT: pay input', { kind, bucket: b, value: el.value }); recomputeClientState(); });
    });
  });

  // Wire the Active/Inactive toggle button
  (function wireToggleButton(){
    const btn = byId('cl_toggle_btn');
    if (!btn || !parentEditable || !ex || !ex.id) return;
    DBG('wireToggleButton: ready', { id: ex.id, disabled: !!ex.disabled_at_utc });
    btn.onclick = () => {
      const pill = byId('cl_status_pill');
      const meta = byId('cl_status_meta');
      const currentlyDisabled = !!ex.disabled_at_utc;

      if (currentlyDisabled) {
        ex.__toggle = 'enable';
        ex.disabled_at_utc = null;
        if (pill) { pill.textContent = '✓ Active'; pill.className = 'pill tag-ok'; }
        if (meta) meta.innerHTML = '&nbsp;';
        btn.textContent = 'Disable';
      } else {
        ex.__toggle = 'disable';
        ex.disabled_at_utc = new Date().toISOString().slice(0,10);
        if (pill) { pill.textContent = '❌ Disabled'; pill.className = 'pill tag-fail'; }
        if (meta) meta.textContent = 'pending save';
        btn.textContent = 'Enable';
      }
      DBG('toggle clicked', { newToggle: ex.__toggle, newDisabledAt: ex.disabled_at_utc });
      setApplyEnabled(true);
    };
  })();

  // DELETE button logic (unchanged; staged delete until parent Save)
  (async function wireDeleteButton(){
    const delBtn = byId('btnDelete');
    if (!delBtn) return;
    if (!existing || !existing.id) { delBtn.style.display='none'; return; }

    const today = new Date(); const yyyy = today.getFullYear(); const mm = String(today.getMonth()+1).padStart(2,'0'); const dd = String(today.getDate()).padStart(2,'0');
    const todayIso = `${yyyy}-${mm}-${dd}`;
    const isFutureOrToday = !!existing.date_from && String(existing.date_from) >= todayIso;

    let deletable = isFutureOrToday;
    let reason = '';
    if (!deletable) {
      deletable = true; // allow delete in UI; real guard happens on Save server-side
      reason = '';
    }

    delBtn.style.display = '';
    delBtn.disabled = false;
    delBtn.onclick = () => {
      try {
        existing.__delete = true;
        DBG('delete staged', { id: existing.id });
        if (window.modalCtx && window.modalCtx.ratesStagedDeletes instanceof Set && existing.id) {
          window.modalCtx.ratesStagedDeletes.add(String(existing.id));
        }
        try { renderClientRatesTable(); } catch {}
        try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
        const closeBtn = byId('btnCloseModal'); if (closeBtn) closeBtn.click();
      } catch (e) { DBG('delete stage failed', e); }
    };
  })();
}


// === UPDATED: Client Default Rate modal (Role dropdown + new-role option; UK dates; date_to) ===
// ======================== openClientRateModal (FIXED) ========================
// =================== CLIENT DEFAULT RATE MODAL (UPDATED) ===================
// ✅ UPDATED — unified 3×5 grid (PAYE | Umbrella | Charge), date prefill, staged N−1 truncation of incumbent window

// ============================================================================
// CLIENT RATE MODAL (child) — adds status block + enable/disable button;
// overlap/rollback logic now IGNORES disabled rows
// ============================================================================

// ========== CLIENT DEFAULT RATES ==========

async function renderClientRatesTable() {
  const LOG_RATES = !!window.__LOG_RATES;
  const DBG = (...a)=> { if (LOG_RATES) console.log('[RATES][renderClientRatesTable]', ...a); };

  const div = byId('clientRates'); if (!div) { DBG('no #clientRates host, bail'); return; }

  const ctx = window.modalCtx;
  const staged = Array.isArray(ctx.ratesState) ? ctx.ratesState : [];
  const frame = _currentFrame();
  const parentEditable = frame && (frame.mode === 'edit' || frame.mode === 'create');

  DBG('ENTRY', { stagedLen: staged.length, parentEditable, ctxEntity: ctx?.entity });

  async function _erniMultiplier(){
    if (typeof window.__ERNI_MULT__ === 'number') return window.__ERNI_MULT__;
    try {
      if (typeof getSettingsCached === 'function') {
        const s = await getSettingsCached();
        let p = s?.erni_pct ?? s?.employers_ni_percent ?? 0;
        p = Number(p) || 0; if (p > 1) p = p/100;
        window.__ERNI_MULT__ = 1 + p;
        return window.__ERNI_MULT__;
      }
    } catch {}
    window.__ERNI_MULT__ = 1;
    return 1;
  }
  const mult = await _erniMultiplier();
  const fmt = v => (v==null || Number.isNaN(v)) ? '—' : (Math.round(v*100)/100).toFixed(2);

  div.innerHTML = '';

  if (!staged.length) {
    DBG('no staged rows → show empty state');
    div.innerHTML = `
      <div class="hint" style="margin-bottom:8px">No client default windows yet.</div>
      <div class="actions">
        <button id="btnAddClientRate" class="btn mini"${parentEditable ? '' : ' disabled'}>
          Add / Upsert client window
        </button>
        ${parentEditable
          ? '<span class="hint">Changes are staged. Click “Save” in the main dialog to persist.</span>'
          : '<span class="hint">Read-only. Click “Edit” in the main dialog to add/modify windows.</span>'}
      </div>
    `;
    const addBtn = byId('btnAddClientRate');
    if (addBtn && parentEditable) {
      addBtn.onclick = () => {
        const cid = (ctx && ctx.data && (ctx.data.id || ctx.data.client_id)) || null;
        return openClientRateModal(cid);
      };
    }
    return;
  }

  // Build a quick baseline lookup (used only for the new last-column status)
  const baseline = Array.isArray(ctx.ratesBaseline) ? ctx.ratesBaseline : [];
  const baselineById = new Map(baseline.filter(b => b && b.id).map(b => [String(b.id), b]));
  const stagedDelSet = (ctx.ratesStagedDeletes instanceof Set) ? ctx.ratesStagedDeletes : new Set();

  const cols = [
    'status',
    'role','band',
    'paye_day','paye_night','paye_sat','paye_sun','paye_bh',
    'umb_day','umb_night','umb_sat','umb_sun','umb_bh',
    'charge_day','charge_night','charge_sat','charge_sun','charge_bh',
    'paye_margin_day','paye_margin_night','paye_margin_sat','paye_margin_sun','paye_margin_bh',
    'umb_margin_day','umb_margin_night','umb_margin_sat','umb_margin_sun','umb_margin_bh',
    'date_from','date_to',
    // NEW: final column to reflect staged change status
    'change_status'
  ];
  const headers = [
    'Status',
    'Role','Band',
    'PAYE Day','PAYE Night','PAYE Sat','PAYE Sun','PAYE BH',
    'UMB Day','UMB Night','UMB Sat','UMB Sun','UMB BH',
    'Charge Day','Charge Night','Charge Sat','Charge Sun','Charge BH',
    'PAYE M Day','PAYE M Night','PAYE M Sat','PAYE M Sun','PAYE M BH',
    'UMB M Day','UMB M Night','UMB M Sat','UMB M Sun','UMB M BH',
    'From','To',
    // NEW header aligned with the final column
    'Change'
  ];

  const tbl   = document.createElement('table'); tbl.className='grid';
  const thead = document.createElement('thead');
  const trh   = document.createElement('tr');
  headers.forEach(h => { const th=document.createElement('th'); th.textContent = h; trh.appendChild(th); });
  thead.appendChild(trh);
  tbl.appendChild(thead);

  const tb = document.createElement('tbody');

  // Helper to detect field differences against baseline
  const DIFF_FIELDS = [
    'role','band','date_from','date_to',
    'charge_day','charge_night','charge_sat','charge_sun','charge_bh',
    'paye_day','paye_night','paye_sat','paye_sun','paye_bh',
    'umb_day','umb_night','umb_sat','umb_sun','umb_bh'
  ];
  const differsFromBaseline = (row) => {
    if (!row || !row.id) return false; // creations are handled separately
    const base = baselineById.get(String(row.id));
    if (!base) return true; // no baseline match means it's effectively different
    for (const f of DIFF_FIELDS) {
      const a = row[f]; const b = base[f];
      const na = (a === '' || a == null) ? null : a;
      const nb = (b === '' || b == null) ? null : b;
      if (String(na) !== String(nb)) return true;
    }
    return false;
  };

  // 2dp formatter for numeric cells that aren’t margins
  const to2 = v => (v==null || v==='') ? '—' : fmt(Number(v));

  staged.forEach((r, idx) => {
    const tr = document.createElement('tr');
    if (r.disabled_at_utc) tr.classList.add('row-disabled');
    if (r.__delete) tr.classList.add('row-delete-pending');

    if (parentEditable) tr.ondblclick = () => {
      const cid = (ctx && ctx.data && (ctx.data.id || ctx.data.client_id)) || r.client_id || null;
      return openClientRateModal(cid, r);
    };

    cols.forEach(c => {
      const td = document.createElement('td');

      if (c === 'status') {
        if (r.__delete) {
          td.innerHTML = `<span class="pill tag-fail" aria-label="Pending delete">🗑 Pending delete (save to confirm)</span>`;
        } else if (r.disabled_at_utc) {
          const pending = r.__toggle ? ' (pending save)' : '';
          td.innerHTML = `<span class="pill tag-fail" aria-label="Disabled">❌ Disabled${pending}</span>`;
        } else {
          const pending = r.__toggle ? ' (pending save)' : '';
          td.innerHTML = `<span class="pill tag-ok" aria-label="Active">✓ Active${pending}</span>`;
        }

      } else if (c.startsWith('paye_margin_') || c.startsWith('umb_margin_')) {
        const bucket = c.split('_').pop();
        const charge = r[`charge_${bucket}`];
        const paye   = r[`paye_${bucket}`];
        const umb    = r[`umb_${bucket}`];

        // ✅ Use global helper when present; fallback preserves existing behaviour
        let val = null;
        if (typeof calcDailyMargin === 'function') {
          if (c.startsWith('paye_margin_')) {
            val = calcDailyMargin({ bucket, charge, pay: paye, method: 'PAYE', erniMultiplier: mult });
          } else {
            val = calcDailyMargin({ bucket, charge, pay: umb,  method: 'UMBRELLA' });
          }
        } else {
          if (c.startsWith('paye_margin_')) val = (charge!=null && paye!=null) ? (charge - (paye * mult)) : null;
          else                               val = (charge!=null && umb!=null)  ? (charge - umb)          : null;
        }
        td.textContent = fmt(val);

      } else if (
        c.startsWith('charge_') ||
        c.startsWith('paye_')   ||
        c.startsWith('umb_')
      ) {
        // 2dp formatting for charge_* and paye_*/umb_* columns
        td.textContent = to2(r[c]);

      } else if (c === 'change_status') {
        let label = '';
        if (r.__delete || (r.id && stagedDelSet.has(String(r.id)))) {
          label = 'Pending delete (save to confirm)';
        } else if (r && r.__toggle === 'enable') {
          label = 'Pending enable';
        } else if (r && r.__toggle === 'disable') {
          label = 'Pending disable';
        } else if (!r.id) {
          label = 'Pending create';
        } else if (differsFromBaseline(r)) {
          label = 'Pending update';
        } else {
          label = '';
        }
        td.textContent = label;

      } else {
        td.textContent = formatDisplayValue(c, r[c]);
      }

      tr.appendChild(td);
    });

    tb.appendChild(tr);
    if (idx === 0) DBG('first row preview', r);
  });

  tbl.appendChild(tb);

  const actions = document.createElement('div');
  actions.className = 'actions';
  actions.innerHTML = `
    <button id="btnAddClientRate" class="btn mini"${parentEditable ? '' : ' disabled'}>
      Add / Upsert client window
    </button>
    ${parentEditable ? '' : '<span class="hint">Read-only. Click “Edit” in the main dialog to add/modify windows.</span>'}
  `;

  div.appendChild(tbl);
  div.appendChild(actions);
  DBG('EXIT render', { stagedLen: staged.length });
}


// ──────────────────────────────────────────────────────────────────────────────
// Global margin helpers (safe, contracts-only consumers can use immediately)
// - calcDailyMargin({ bucket, charge, pay, method, erniMultiplier? }) -> number|null
// - calcDailyMarginsForBuckets({ method, charge:{...}, pay:{...}, erniMultiplier? }) -> {day,night,sat,sun,bh}
// - ensureErniMultiplier() -> Promise<number>  (optional bootstrap to memoise ERNI)
// Notes:
//   • Pure calc (ex-VAT). No rounding, no styling. Callers format to 2dp.
//   • For PAYE, margin = charge − (pay × ERNI_MULTIPLIER). For Umbrella, margin = charge − pay.
//   • If any operand is missing/NaN returns null.
//   • Respects existing memo: window.__ERNI_MULT__ (fallback 1.0). Does not force async lookups.
//   • Non-breaking: only defines helpers if not already present.
// ──────────────────────────────────────────────────────────────────────────────
(() => {
  const W = (typeof window !== 'undefined') ? window : globalThis;

  // Normalise to number or null (treat '', undefined, NaN as null)
  const toNum = (v) => {
    if (v === '' || v === null || v === undefined) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  // Optional async bootstrap to memoise ERNI multiplier once (1 + percent)
  // Uses your existing getSettingsCached if available. Safe to call multiple times.
  if (typeof W.ensureErniMultiplier !== 'function') {
    W.ensureErniMultiplier = async function ensureErniMultiplier() {
      if (typeof W.__ERNI_MULT__ === 'number') return W.__ERNI_MULT__;
      try {
        if (typeof W.getSettingsCached === 'function') {
          const s = await W.getSettingsCached();
          let p = s?.erni_pct ?? s?.employers_ni_percent ?? 0;
          p = Number(p) || 0;
          if (p > 1) p = p / 100;           // support 13.8 vs 0.138
          W.__ERNI_MULT__ = 1 + p;
          return W.__ERNI_MULT__;
        }
      } catch {}
      W.__ERNI_MULT__ = 1;
      return 1;
    };
  }

  // Core per-bucket margin calculator (synchronous)
  if (typeof W.calcDailyMargin !== 'function') {
    /**
     * @param {Object} args
     * @param {'day'|'night'|'sat'|'sun'|'bh'} [args.bucket]
     * @param {number|null} args.charge
     * @param {number|null} args.pay    // PAYE pay or Umbrella pay for the bucket
     * @param {'PAYE'|'UMBRELLA'|string} args.method
     * @param {number} [args.erniMultiplier] // optional override; otherwise uses window.__ERNI_MULT__||1
     * @returns {number|null}
     */
    W.calcDailyMargin = function calcDailyMargin({ bucket, charge, pay, method, erniMultiplier } = {}) {
      const ch = toNum(charge);
      const py = toNum(pay);
      if (ch === null || py === null) return null;

      const m = (typeof erniMultiplier === 'number')
        ? erniMultiplier
        : (typeof W.__ERNI_MULT__ === 'number' ? W.__ERNI_MULT__ : 1);

      const meth = (method || 'PAYE').toString().toUpperCase();
      if (meth === 'PAYE') {
        return ch - (py * m);
      } else if (meth === 'UMBRELLA') {
        return ch - py;
      }
      // Unknown method → treat like Umbrella (no ERNI)
      return ch - py;
    };
  }

  // Convenience: compute margins for all five buckets in one call
  if (typeof W.calcDailyMarginsForBuckets !== 'function') {
    /**
     * @param {Object} args
     * @param {'PAYE'|'UMBRELLA'|string} args.method
     * @param {Object} args.charge  // {day,night,sat,sun,bh}
     * @param {Object} args.pay     // {day,night,sat,sun,bh}  (PAYE pay or Umbrella pay)
     * @param {number} [args.erniMultiplier]
     * @returns {{day:number|null,night:number|null,sat:number|null,sun:number|null,bh:number|null}}
     */
    W.calcDailyMarginsForBuckets = function calcDailyMarginsForBuckets({ method, charge = {}, pay = {}, erniMultiplier } = {}) {
      const buckets = ['day','night','sat','sun','bh'];
      const out = {};
      const meth = (method || 'PAYE').toString().toUpperCase();
      const m = (typeof erniMultiplier === 'number')
        ? erniMultiplier
        : (typeof W.__ERNI_MULT__ === 'number' ? W.__ERNI_MULT__ : 1);

      buckets.forEach(b => {
        out[b] = W.calcDailyMargin({
          bucket: b,
          charge: charge[b],
          pay:    pay[b],
          method: meth,
          erniMultiplier: m
        });
      });
      return out;
    };
  }
})();


// NEW
async function endContractSafely(contractId, desiredEnd) {
  const LOGC = (typeof window !== 'undefined' && window.__IS_TESTING_LOG) || (typeof window !== 'undefined' && !!window.__LOG_CONTRACTS);
  const url   = `${window.BROKER_BASE_URL}/api/contracts/${encodeURIComponent(String(contractId))}/truncate-tail`;
  const body  = { id: String(contractId), desired_end: String(desiredEnd) };

  if (LOGC) {
    console.groupCollapsed('[TRIM_CALL][frontend] dispatch');
    console.log('request', { url, body, hasAuthFetch: typeof window !== 'undefined' && typeof window.authFetch === 'function' });
  }

  let result;
  try {
    if (typeof window !== 'undefined' && typeof window.authFetch === 'function') {
      const resp = await window.authFetch({
        url,
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body)
      });
      // normalise possible shapes from authFetch
      if (resp && typeof resp === 'object' && 'ok' in resp && 'json' in resp && typeof resp.json === 'function') {
        const json = await resp.json().catch(() => null);
        result = (json && typeof json === 'object') ? { ...json } : { ok: !!resp.ok, status: resp.status ?? 200 };
      } else {
        result = resp;
      }
    } else {
      const r = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...(window?.sbHeaders || {}) },
        body: JSON.stringify(body)
      });
      const data = await r.json().catch(() => null);
      if (!r.ok) {
        const msg = (data && (data.error || data.message)) || `HTTP ${r.status}`;
        throw new Error(msg);
      }
      result = data ?? { ok: true };
    }

    if (LOGC) console.log('response', result);
    return result;
  } catch (err) {
    if (LOGC) console.warn('[TRIM_CALL][frontend] error', err);
    throw err;
  } finally {
    if (LOGC) console.groupEnd?.();
  }
}

// Ensure a window-bound handle exists even when this file is bundled as an ES module
if (typeof window !== 'undefined') {
  if (typeof window.endContractScrub === 'function' && !window.endContractSafely) {
    // legacy alias safeguard if you had a prior name
    window.endContractSafely = window.endContractScrub;
  } else if (typeof window.endContractSafely !== 'function') {
    window.endContractSafely = endContractSafely;
  }
}

// NEW
async function refetchContract(id) {
  const url = `${window.BROKER_BASE_URL}/api/contracts/${encodeURIComponent(id)}`;
  if (typeof authFetch === 'function') {
    const res = await authFetch({ url, method: 'GET', headers: { 'content-type': 'application/json' } });
    return res?.contract || res || null;
  }
  const r = await fetch(url, { headers: { ...(window.sbHeaders || {}) } });
  if (!r.ok) return null;
  const j = await r.json().catch(() => null);
  return j?.contract || j || null;
}

// NEW
function updateContractsListCache(id, row) {
  try {
    if (Array.isArray(window.currentRows)) {
      const i = window.currentRows.findIndex(x => String(x.id) === String(id));
      if (i >= 0) window.currentRows[i] = row;
      (window.__lastSavedAtById ||= {})[String(id)] = Date.now();
    }
  } catch {}
}

// NEW
function showTailClampWarning(safeEnd, desiredEnd) {
  const msg = `End date adjusted to ${safeEnd} due to existing timesheet(s). (Requested ${desiredEnd})`;
  if (typeof showModalHint === 'function') showModalHint(msg, 'warn');
  try { window.__toast?.(msg); } catch {}
}

// NEW
function clearCloneIntent() {
  try {
    const token = window.modalCtx?.openToken || null;
    if (window.modalCtx && window.modalCtx.__cloneIntent) delete window.modalCtx.__cloneIntent;
    if (token && window.__cloneIntents) delete window.__cloneIntents[token];
  } catch {}
}
// NEW
async function refreshOldContractAfterTruncate(oldContractId) {
  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : true;
  const row = await refetchContract(oldContractId);
  if (!row) return;

  updateContractsListCache(oldContractId, row);

  try {
    const fr = (typeof window.__getModalFrame === 'function') ? window.__getModalFrame() : null;
    if (fr && window.modalCtx && window.modalCtx.entity === 'contracts' && String(window.modalCtx?.data?.id || '') === String(oldContractId)) {
      window.modalCtx.data = { ...(window.modalCtx.data || {}), ...row };
      try { window.dispatchEvent(new Event('contracts-main-rendered')); } catch {}
    }
  } catch {}

  try {
    const st = (window.__calState || {})[oldContractId];
    const win = st && st.win ? st.win : null;
    if (typeof fetchAndRenderContractCalendar === 'function') {
      if (win) {
        await fetchAndRenderContractCalendar(oldContractId, { from: win.from, to: win.to, view: st.view });
      } else {
        const y = (new Date()).getUTCFullYear();
        const def = (typeof computeYearWindow === 'function') ? computeYearWindow(y) : { from: `${y}-01-01`, to: `${y}-12-31` };
        await fetchAndRenderContractCalendar(oldContractId, { from: def.from, to: def.to, view: 'year' });
      }
    }
  } catch (e) {
    if (LOGC) console.warn('[CONTRACTS] refreshOldContractAfterTruncate calendar refresh failed', e);
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: showModal (adds contract-modal class toggling for Contracts dialogs)
// ─────────────────────────────────────────────────────────────────────────────
function showModal(title, tabs, renderTab, onSave, hasId, onReturn, options) {

  const LOG = (typeof window.__LOG_MODAL === 'boolean') ? window.__LOG_MODAL : true;
  const L  = (...a)=> { if (LOG) console.log('[MODAL]', ...a); };
  const GC = (label)=> { if (LOG) console.groupCollapsed('[MODAL]', label); };
  const GE = ()=> { if (LOG) console.groupEnd(); };

  const stack = () => (window.__modalStack ||= []);
  const currentFrame = () => stack()[stack().length - 1] || null;
  const parentFrame  = () => (stack().length > 1 ? stack()[stack().length - 2] : null);
  const deep = (o) => JSON.parse(JSON.stringify(o));

  let opts = options || {};
  if (onReturn && typeof onReturn === 'object' && options === undefined) { opts = onReturn; onReturn = undefined; }

  const stripEmpty = (obj) => { const out={}; for (const [k,v] of Object.entries(obj||{})) { if (v===''||v==null) continue; out[k]=v; } return out; };

  function setFormReadOnly(root, ro) {
  if (!root || !document.contains(root)) { L('setFormReadOnly(skip: invalid root)', { ro }); return; }
  const _allBefore = root.querySelectorAll('input, select, textarea, button');
  const beforeDisabled = Array.from(_allBefore).filter(el => el.disabled).length;
  root.querySelectorAll('input, select, textarea, button').forEach((el) => {
    const isDisplayOnly = el.id === 'tms_ref_display' || el.id === 'cli_ref_display';
    if (el.type === 'button') {
      const allow = new Set(['btnCloseModal','btnDelete','btnEditModal','btnSave','btnRelated']);
      if (!allow.has(el.id)) el.disabled = !!ro;
      return;
    }
    if (isDisplayOnly) { el.setAttribute('disabled','true'); el.setAttribute('readonly','true'); return; }
    if (ro) { el.setAttribute('disabled','true'); el.setAttribute('readonly','true'); }
    else    { el.removeAttribute('disabled');   el.removeAttribute('readonly'); }
  });
  const _allAfter = root.querySelectorAll('input, select, textarea, button');
  const afterDisabled = Array.from(_allAfter).filter(el => el.disabled).length;
  try {
    const pc = root.querySelector('#btnPickCandidate');
    const pl = root.querySelector('#btnPickClient');
    L('setFormReadOnly snapshot', {
      ro,
      beforeDisabled,
      afterDisabled,
      picks: {
        btnPickCandidate: { exists: !!pc, disabled: !!(pc && pc.disabled) },
        btnPickClient:    { exists: !!pl, disabled: !!(pl && pl.disabled) }
      }
    });
  } catch {}
}


  function sanitizeModalGeometry() {
    const m = byId('modal');
    if (m) {
      m.classList.remove('dragging');
      const anchor = (window.__modalAnchor || null);
      if (anchor) {
        L('sanitizeModalGeometry: applying saved anchor', anchor);
        m.style.position = 'fixed';
        m.style.left     = anchor.left + 'px';
        m.style.top      = anchor.top  + 'px';
        m.style.right    = 'auto';
        m.style.bottom   = 'auto';
        m.style.transform= 'none';
      } else {
        L('sanitizeModalGeometry: reset to default (no anchor)');
        m.style.position = '';
        m.style.left = '';
        m.style.top = '';
        m.style.right = '';
        m.style.bottom = '';
        m.style.transform = '';
      }
    }
    document.onmousemove = null; document.onmouseup = null;
  }

  const modalEl = byId('modal');
  if (modalEl) {
    modalEl.style.position = ''; modalEl.style.left = ''; modalEl.style.top = '';
    modalEl.style.right = '';    modalEl.style.bottom = '';
    modalEl.style.transform = ''; modalEl.classList.remove('dragging');
    L('showModal: reset #modal initial geometry');
  }

     // Allow staging children (Clone & Extend) to be fully editable regardless of parent mode
  if (opts && opts.kind === 'contract-clone-extend') {
    opts.noParentGate = true;
    if (!opts.forceEdit) opts.forceEdit = true;
    L('showModal(kind=contract-clone-extend): enable noParentGate + forceEdit', { noParentGate: opts.noParentGate, forceEdit: opts.forceEdit });
  }
// Treat the Rate Presets **picker** as a normal, interactive child:
// - do NOT set noParentGate or _loadOnly here
// - dirty should propagate to the parent while the picker is open
if (opts && opts.kind === 'rate-presets-picker') {
  L('showModal(kind=rate-presets-picker): interactive child (noParentGate=false)');
}



const frame = {
  _token: `f:${Date.now()}:${Math.random().toString(36).slice(2)}`,
  _ctxRef: window.modalCtx,
  title,
  tabs: Array.isArray(tabs) ? tabs.slice() : [],
  renderTab,
  onSave,
  onReturn,
  hasId: !!hasId,
  entity: (window.modalCtx && window.modalCtx.entity) || null,

  noParentGate: !!opts.noParentGate,
  forceEdit:    !!opts.forceEdit,
  kind:         opts.kind || null,
  stayOpenOnSave: !!opts.stayOpenOnSave,
    currentTabKey: (Array.isArray(tabs) && tabs.length ? tabs[0].key : null),

    mode: (() => {
      if (opts.forceEdit) return 'edit';
      if (!hasId && opts.kind === 'rate-preset') return 'edit';
      return hasId ? 'view' : 'create';
    })(),
    isDirty:false, _snapshot:null, _detachDirty:null, _detachGlobal:null, _hasMountedOnce:false, _wired:false, _closing:false, _saving:false, _confirmingDiscard:false,
    _applyDesired:null,

persistCurrentTabState() {
  L('persistCurrentTabState ENTER', { mode: this.mode, currentTabKey: this.currentTabKey });
  if (!window.modalCtx || this.mode === 'view') { L('persist(skip)', { reason:'mode=view or no modalCtx', mode:this.mode }); return; }

  const sentinel = window.modalCtx?.openToken || null;
  const initial  = (window.modalCtx.data?.id ?? sentinel);
  const fs = window.modalCtx.formState || { __forId: initial, main:{}, pay:{} };
  if (fs.__forId == null) fs.__forId = initial;

  // Preserve schedule inputs even when blank ('') so cleared days don't get dropped
  const keepScheduleBlanks = (obj) => {
    const out = {};
    const days = ['mon','tue','wed','thu','fri','sat','sun'];
    const parts = ['start','end','break'];
    days.forEach(d => {
      parts.forEach(p => {
        const k = `${d}_${p}`;
        if (Object.prototype.hasOwnProperty.call(obj, k)) {
          out[k] = (obj[k] == null ? '' : String(obj[k]));
        }
      });
    });
    return out;
  };

   if (this.currentTabKey === 'main') {
    const sel = byId('tab-main') ? '#tab-main' : (byId('contractForm') ? '#contractForm' : null);
    if (sel) {
      const c = collectForm(sel);
      // keep existing behavior for most fields, then re-add schedule blanks explicitly
      const merged = { ...stripEmpty(c) };
      const sched  = keepScheduleBlanks(c);
      fs.main = { ...(fs.main||{}), ...merged, ...sched };
    }
  }

  if (this.currentTabKey === 'pay' && byId('tab-pay')) {
    const c = collectForm('#tab-pay');
    fs.pay  = { ...(fs.pay||{}), ...stripEmpty(c) };
  }

  // NEW: capture Care Packages tab (candidates/rates) into main form state
  if (this.entity === 'candidates' && this.currentTabKey === 'rates' && byId('tab-rates')) {
    const c = collectForm('#tab-rates');
    // GCK (key_norm) and any future Care Packages fields are treated
    // as part of the candidate "main" payload
    fs.main = { ...(fs.main || {}), ...stripEmpty(c) };
  }

  if (this.entity === 'contracts' && this.currentTabKey === 'rates') {
    try {
      const rt = byId('contractRatesTab');
      if (rt) {
        const rForm = {};
        rt.querySelectorAll('input, select, textarea').forEach(el => {
          if (el.name) rForm[el.name] = (el.type === 'checkbox' ? (el.checked ? 'on' : '') : el.value);
        });
        const onlyRates = {};
        for (const [k, v] of Object.entries(rForm)) if (/^(paye_|umb_|charge_)/.test(k)) onlyRates[k] = v;
        fs.pay = { ...(fs.pay || {}), ...stripEmpty(onlyRates) };
      }
      const mainSel = byId('contractForm') ? '#contractForm' : null;
      if (mainSel) {
        const m = collectForm(mainSel);
        // keep existing behavior for most fields, then re-add schedule blanks explicitly
        const mergedMain = { ...stripEmpty(m) };
        const sched      = keepScheduleBlanks(m);
        fs.main = { ...(fs.main || {}), ...mergedMain, ...sched };
      }
    } catch (e) {
      L('persistCurrentTabState contracts/rates failed', e);
    }
  }


  window.modalCtx.formState = fs;
  L('persistCurrentTabState EXIT', { forId: fs.__forId, mainKeys: Object.keys(fs.main||{}), payKeys: Object.keys(fs.pay||{}) });
},

// inside showModal(...), in the `const frame = { ... }` object:
mergedRowForTab(k) {
  L('mergedRowForTab ENTER', { k });
  const base = { ...(window.modalCtx?.data || {}) };
  const fs   = (window.modalCtx?.formState || {});
  const rid  = window.modalCtx?.data?.id ?? null;
  const fid  = fs.__forId ?? null;
  const sentinel = window.modalCtx?.openToken ?? null;
  const same = (fid===rid) || (rid==null && (fid===sentinel || fid==null));

  const mainStaged = same ? (fs.main || {}) : {};
  const payStaged  = same ? (fs.pay  || {}) : {};

  // Default merge (drops empty strings via stripEmpty)
  const out = { ...base, ...stripEmpty(mainStaged) };

  // Keep non-DOM baselines visible, but let staged template override base schedule
  try {
    // 🔹 CHANGE: always prefer the staged __template if present
    if (mainStaged.__template) {
      out.std_schedule_json = mainStaged.__template;
    }

    if (!out.std_hours_json && mainStaged.__hours) {
      out.std_hours_json = mainStaged.__hours;
    }

    if (Object.prototype.hasOwnProperty.call(mainStaged, '__bucket_labels')) {
      out.bucket_labels_json = mainStaged.__bucket_labels;
    }
  } catch {}

  // ✨ Preserve schedule fields even when blank
  // (so an applied preset with missing days truly overwrites prior values to empty)
  try {
    const days = ['mon','tue','wed','thu','fri','sat','sun'];
    const parts = ['start','end','break'];
    days.forEach(d => {
      parts.forEach(p => {
        const key = `${d}_${p}`;
        if (Object.prototype.hasOwnProperty.call(mainStaged, key)) {
          // Use the staged value verbatim, including ''
          out[key] = mainStaged[key];
        }
      });
    });
  } catch {}

  // Rates: merge staged pay/charge families into view row
  try {
    const mergedRates = { ...(out.rates_json || base.rates_json || {}) };
    for (const [kk, vv] of Object.entries(payStaged)) {
      if (/^(paye_|umb_|charge_)/.test(kk)) mergedRates[kk] = vv;
    }
    out.rates_json = mergedRates;
  } catch (e) {
    L('mergedRowForTab rates merge failed', e);
  }

  L('mergedRowForTab STATE', {
    rid, fid, sentinel, same,
    stagedMainKeys: Object.keys(mainStaged||{}),
    stagedPayKeys: Object.keys(payStaged||{}),
    ratesKeys: Object.keys(out.rates_json || {})
  });
  return out;
},


   _attachDirtyTracker() {
    if (this._detachDirty) { try { this._detachDirty(); } catch {} this._detachDirty = null; }
    const root = byId('modalBody'); if (!root) { L('_attachDirtyTracker(skip: no modalBody)'); return; }
    const onDirty = (ev) => {
      if (ev && !ev.isTrusted) return;

     // Allow presets picker to mark the *parent* dirty (only ignore truly load-only frames)
if (this._loadOnly === true) return;

      const isChild = (stack().length > 1);
      if (isChild) {
        if (this.noParentGate) {
          if (this.mode === 'edit' || this.mode === 'create') {
            this.isDirty = true;
            this._updateButtons && this._updateButtons();
          }
        } else {
          const p = parentFrame();
          if (p && (p.mode === 'edit' || p.mode === 'create')) {
            p.isDirty = true;
            p._updateButtons && p._updateButtons();
          }
        }
      } else {
        if (this.mode === 'edit' || this.mode === 'create') {
          this.isDirty = true;
          this._updateButtons && this._updateButtons();
        }
      }
      try { const t=currentFrame(); if (t && t.entity==='candidates' && t.currentTabKey==='rates') { renderCandidateRatesTable?.(); } } catch {}
    };
    root.addEventListener('input', onDirty, true);
    root.addEventListener('change',onDirty, true);
    this._detachDirty = ()=>{ root.removeEventListener('input',onDirty,true); root.removeEventListener('change',onDirty,true); };
    L('_attachDirtyTracker: attached');
  },



  async setTab(k) {
  GC(`setTab(${k})`);
  L('setTab ENTER', { k, prevKey: this.currentTabKey, entity: this.entity, mode: this.mode, hasMounted: this._hasMountedOnce });

  const prevDirty = this.isDirty;
  this._suppressDirty = true;

  const persist = this._hasMountedOnce; if (persist) this.persistCurrentTabState();

  const merged = this.mergedRowForTab(k);
  if (this.entity === 'contracts' && k === 'main' && this.mode !== 'edit' && this.mode !== 'create') {
    if (window.modalCtx?.data?.start_date) merged.start_date = window.modalCtx.data.start_date;
    if (window.modalCtx?.data?.end_date)   merged.end_date   = window.modalCtx.data.end_date;
    try {
      const fs = (window.modalCtx.formState ||= { __forId:(window.modalCtx?.data?.id || window.modalCtx?.openToken || null), main:{}, pay:{} });
      fs.main ||= {};
      if (merged.start_date) fs.main.start_date = merged.start_date;
      if (merged.end_date)   fs.main.end_date   = merged.end_date;
    } catch {}
  }
  byId('modalBody').innerHTML = this.renderTab(k, merged) || '';

  if (this.entity==='candidates' && k==='bookings') {
  const candId = window.modalCtx?.data?.id;
  if (candId) {
    try { renderCandidateCalendarTab(candId); } catch(e) { console.warn('renderCandidateCalendarTab failed', e); }
  }
}

// Care Packages tab (was 'rates'): mount rates table + Rota Roles
if (this.entity==='candidates' && k==='rates') {
  // Existing behaviour: mount candidate overrides / rates UI
  mountCandidateRatesTab?.();

  // NEW: wire the Rota Roles editor here instead of on the main tab
  const rolesHost = document.querySelector('#rolesEditor');
  if (rolesHost) {
    (async () => {
      try {
        const roleOptions = await loadGlobalRoleOptions();
        renderRolesEditor(rolesHost, window.modalCtx.rolesState || [], roleOptions);
        L('setTab(candidates/rates): roles editor mounted', { options: (roleOptions||[]).length });
      } catch (e) {
        console.error('[MODAL] roles mount failed', e);
      }
    })();
  }
}

if (this.entity==='candidates' && k==='pay') {
  if (!window.modalCtx?.payMethodState && window.modalCtx?.data?.pay_method) {
    window.modalCtx.payMethodState = String(window.modalCtx.data.pay_method);
    L('setTab(candidates/pay): seeded payMethodState', { payMethodState: window.modalCtx.payMethodState });
  }
  const p = mountCandidatePayTab?.();
  if (p && typeof p.then === 'function') { await p; }
}

if (this.entity==='candidates' && k==='main') {
  const pmSel = document.querySelector('#pay-method');
  if (pmSel) {
    const stagedPm   = window.modalCtx?.formState?.main?.pay_method;
    const preferred  = (window.modalCtx?.payMethodState || stagedPm || pmSel.value);
    pmSel.value = preferred;
    pmSel.addEventListener('change', () => {
      window.modalCtx.payMethodState = pmSel.value;
      try { window.dispatchEvent(new CustomEvent('pay-method-changed')); }
      catch { window.dispatchEvent(new Event('pay-method-changed')); }
    });
    window.modalCtx.payMethodState = pmSel.value;
    L('setTab(candidates/main): pay method wired', { preferred });
  }

  // ✅ Reuse existing candidateMainModel if present, so child modals don't lose changes
  try {
    const container = document.getElementById('tab-main');
    if (container &&
        typeof buildCandidateMainDetailsModel === 'function' &&
        typeof bindCandidateMainFormEvents === 'function') {

      let model = window.modalCtx?.candidateMainModel;
      if (!model || typeof model !== 'object') {
        // First time we hit the main tab: build from DB row
        model = buildCandidateMainDetailsModel(window.modalCtx?.data || {});
        window.modalCtx.candidateMainModel = model;
        L('setTab(candidates/main): created candidate main model', {
          keys: Object.keys(model || {})
        });
      } else {
        L('setTab(candidates/main): reusing candidate main model', {
          keys: Object.keys(model || {})
        });
      }

      // (Re)bind DOM to the model – inputs and job titles list will reflect current model values
      bindCandidateMainFormEvents(container, model);
    }
  } catch (e) {
    console.error('[MODAL] bindCandidateMainFormEvents failed', e);
  }
}


  if (this.entity === 'umbrellas' && k === 'main') {
    try {
      const container = document.getElementById('tab-main');
      if (container && typeof buildUmbrellaDetailsModel === 'function' && typeof bindUmbrellaAddressEvents === 'function') {
        const model = buildUmbrellaDetailsModel(window.modalCtx?.data || {});
        window.modalCtx.umbrellaModel = model;
        bindUmbrellaAddressEvents(container, model);
        L('setTab(umbrellas/main): bound umbrella model', { keys: Object.keys(model||{}) });
      }
    } catch (e) {
      console.error('[MODAL] bindUmbrellaAddressEvents failed', e);
    }
  }


  if (this.entity==='clients' && k==='rates')     { mountClientRatesTab?.(); }
  if (this.entity==='clients' && k==='hospitals') { mountClientHospitalsTab?.(); }
  if (this.entity==='clients' && k==='settings')  { renderClientSettingsUI?.(window.modalCtx.clientSettingsState||{}); }

  if (this.entity==='contracts' && k==='rates')   { mountContractRatesTab?.(); }

  this.currentTabKey = k;
  this._attachDirtyTracker();

  const isChild = (stack().length > 1);
  if (this.noParentGate) setFormReadOnly(byId('modalBody'), (this.mode==='view'||this.mode==='saving'));
  else if (isChild)      { const p=parentFrame(); setFormReadOnly(byId('modalBody'), !(p && (p.mode==='edit'||p.mode==='create'))); }
  else                   setFormReadOnly(byId('modalBody'), (this.mode==='view'||this.mode==='saving'));

  try {
    const pc = document.getElementById('btnPickCandidate');
    const pl = document.getElementById('btnPickClient');
    L('setTab EXIT snapshot', {
      currentTabKey: this.currentTabKey,
      pickButtons: {
        btnPickCandidate: { exists: !!pc, disabled: !!(pc && pc.disabled) },
        btnPickClient:    { exists: !!pl, disabled: !!(pl && pl.disabled) }
      }
    });
  } catch {}

  try {
    if (this.entity === 'contracts' && k === 'main') {
      window.dispatchEvent(new Event('contracts-main-rendered'));
    }
  } catch {}

  this._hasMountedOnce = true;

  this._suppressDirty = false;
  this.isDirty = prevDirty;

  if (typeof this._updateButtons === 'function') this._updateButtons();

  GE();
}

};
function setFrameMode(frameObj, mode) {
  L('setFrameMode ENTER', { prevMode: frameObj.mode, nextMode: mode, isChild: (stack().length>1), noParentGate: frameObj.noParentGate });
  const prev = frameObj.mode; frameObj.mode = mode;
  const isChild = (stack().length > 1);
  const isTop   = (currentFrame && currentFrame() === frameObj);

  // ▶ correct accidental 'view' on brand-new frames (e.g., successor create)
  if (!frameObj.hasId && mode === 'view') {
    mode = frameObj.forceEdit ? 'edit' : 'create';
    frameObj.mode = mode;
  }

  // ▶ Only toggle read-only on the DOM that actually belongs to the top frame.
  //    When updating a non-top frame (e.g., the parent while a picker is open),
  //    do not flip the global #modalBody to avoid UI flicker/regressions.
  if (isTop) {
    if (!isChild && (mode === 'create' || mode === 'edit')) {
      setFormReadOnly(byId('modalBody'), false);
    } else if (frameObj.noParentGate) {
      setFormReadOnly(byId('modalBody'), (mode==='view'||mode==='saving'));
    } else if (isChild) {
      const p = parentFrame();
      setFormReadOnly(byId('modalBody'), !(p && (p.mode==='edit'||p.mode==='create')));
    } else {
      setFormReadOnly(byId('modalBody'), (mode==='view'||mode==='saving'));
    }
  } else {
    L('setFrameMode (non-top): skipped read-only toggle to avoid affecting current child');
  }

  if (typeof frameObj._updateButtons === 'function') frameObj._updateButtons();

  try { const idx=stack().indexOf(frameObj); window.dispatchEvent(new CustomEvent('modal-frame-mode-changed',{detail:{frameIndex:idx,mode}})); } catch {}
  const repaint = !!(frameObj._hasMountedOnce && frameObj.currentTabKey);
  L('setFrameMode', { prevMode:prev, nextMode:mode, _hasMountedOnce:frameObj._hasMountedOnce, willRepaint:repaint });
  try {
    const pc = document.getElementById('btnPickCandidate');
    const pl = document.getElementById('btnPickClient');
    L('setFrameMode picker snapshot', {
      pickCandidate: { exists: !!pc, disabled: !!(pc && pc.disabled) },
      pickClient:    { exists: !!pl, disabled: !!(pl && pl.disabled) }
    });
  } catch {}
  updateCalendarInteractivity(mode==='edit' || mode==='create');
  if (repaint) {
    Promise.resolve(frameObj.setTab(frameObj.currentTabKey)).then(() => {
      try { frameObj.onReturn && frameObj.onReturn(); } catch {}
    });
  }
}

const parentOnOpen = currentFrame();
frame._parentModeOnOpen = parentOnOpen ? parentOnOpen.mode : null;

stack().push(frame);
byId('modalBack').style.display='flex';


function renderTop() {
  const LOG = (typeof window.__LOG_MODAL === 'boolean') ? window.__LOG_MODAL : true;
  const L  = (...a)=> { if (LOG) console.log('[MODAL]', ...a); };
  const GC = (label)=> { if (LOG) console.groupCollapsed('[MODAL]', label); };
  const GE = ()=> { if (LOG) console.groupEnd(); };

  GC('renderTop()');

  const hintEl = document.getElementById('modalHint');
  if (hintEl) {
    hintEl.textContent = '';
    hintEl.removeAttribute('data-tone');
    try { hintEl.classList.remove('ok','warn','err'); } catch {}
  }

  const isChild = (stack().length > 1);
  const top     = currentFrame();
  const parent  = parentFrame();

  // restore the parent/owner context for whatever frame is now on top
  if (top && top._ctxRef) window.modalCtx = top._ctxRef;

  if (typeof top._detachGlobal === 'function') { try { top._detachGlobal(); } catch {} top._wired = false; }

  L('renderTop state (global)', { entity: top?.entity, kind: top?.kind, mode: top?.mode, hasId: top?.hasId, currentTabKey: top?.currentTabKey });
  byId('modalTitle').textContent = top.title;

  const tabsEl = byId('modalTabs'); tabsEl.innerHTML='';
  (top.tabs||[]).forEach((t,i)=>{
    const b=document.createElement('button'); b.textContent = t.label||t.title||t.key;
    if (i===0 && !top.currentTabKey) top.currentTabKey = t.key;
    if (t.key===top.currentTabKey || (i===0 && !top.currentTabKey)) b.classList.add('active');
    b.onclick = ()=>{ if (top.mode==='saving') return; tabsEl.querySelectorAll('button').forEach(x=>x.classList.remove('active')); b.classList.add('active'); top.setTab(t.key); };
    tabsEl.appendChild(b);
  });
  L('renderTop tabs (global)', { count: (top.tabs||[]).length, active: top.currentTabKey });

  if (top.currentTabKey) top.setTab(top.currentTabKey);
  else if (top.tabs && top.tabs[0]) top.setTab(top.tabs[0].key);
  else byId('modalBody').innerHTML = top.renderTab('form',{})||'';

  const btnSave  = byId('btnSave');
  const btnClose = byId('btnCloseModal');
  const btnDel   = byId('btnDelete');
  const header   = byId('modalDrag');
  const modalNode= byId('modal');

  const LOGC = (typeof window.__LOG_CONTRACTS === 'boolean') ? window.__LOG_CONTRACTS : true;
  if (modalNode) {
    const parentIsContracts = !!(parent && ((parent.entity === 'contracts') || (parent.kind === 'contracts')));
    const isContracts = ((top.entity === 'contracts') || (top.kind === 'contracts') || parentIsContracts);
    modalNode.classList.toggle('contract-modal', !!isContracts);
    if (LOGC && isContracts) console.log('[CONTRACTS][MODAL] contract-modal class applied to #modal (inherited:', parentIsContracts, ')');

    // Job Titles: apply narrower modal sizing
    const isJobTitles = (top.kind === 'job-titles');
    modalNode.classList.toggle('jobtitles-modal', !!isJobTitles);
  }

  if (modalNode) {
    const anchor = (window.__modalAnchor || null);
    if (!anchor) {
      const R = modalNode.getBoundingClientRect();
      window.__modalAnchor = { left: R.left, top: R.top };
      modalNode.style.position = 'fixed';
      modalNode.style.left = R.left + 'px';
      modalNode.style.top  = R.top  + 'px';
      modalNode.style.right = 'auto';
      modalNode.style.bottom= 'auto';
      modalNode.style.transform = 'none';
      L('renderTop: anchored modal (global/new)', window.__modalAnchor);
    } else {
      modalNode.style.position = 'fixed';
      modalNode.style.left = window.__modalAnchor.left + 'px';
      modalNode.style.top  = window.__modalAnchor.top  + 'px';
      modalNode.style.right = 'auto';
      modalNode.style.bottom= 'auto';
      modalNode.style.transform = 'none';
      L('renderTop: anchored modal (global/reuse)', window.__modalAnchor);
    }
  }

  const showChildDelete = isChild && (top.kind==='client-rate' || top.kind==='candidate-override') && top.hasId;
  btnDel.style.display = showChildDelete ? '' : 'none'; btnDel.onclick = null;

  let btnEdit = byId('btnEditModal');
  if (!btnEdit) {
    btnEdit=document.createElement('button');
    btnEdit.id='btnEditModal'; btnEdit.type='button'; btnEdit.className='btn btn-outline btn-sm'; btnEdit.textContent='Edit';
    const bar = btnSave?.parentElement || btnClose?.parentElement; if (bar) bar.insertBefore(btnEdit, btnSave);
    L('renderTop (global): created btnEdit');
  }

  (function dragWire(){
    if(!header||!modalNode) return;
    const onDown = e=>{
      if ((e.button!==0 && e.type==='mousedown') || e.target.closest('button')) return;
      const R=modalNode.getBoundingClientRect();
      modalNode.style.position='fixed'; modalNode.style.left=R.left+'px'; modalNode.style.top=R.top+'px';
      modalNode.style.right='auto'; modalNode.style.bottom='auto'; modalNode.style.transform='none'; modalNode.classList.add('dragging');
      const ox=e.clientX-R.left, oy=e.clientY-R.top;
      document.onmousemove = ev=>{ let l=ev.clientX-ox, t=ev.clientY-oy; const ml=Math.max(0,window.innerWidth-R.width), mt=Math.max(0,window.innerHeight-R.height); if(l<0)l=0; if(t<0)t=0; if(l>ml)l=ml; if(t>mt)t=mt; modalNode.style.left=l+'px'; modalNode.style.top=t+'px'; };
      document.onmouseup   = ()=>{
        modalNode.classList.remove('dragging');
        const R2 = modalNode.getBoundingClientRect();
        window.__modalAnchor = { left: R2.left, top: R2.top };
        document.onmousemove=null; document.onmouseup=null;
        L('dragWire (global): saved new anchor', window.__modalAnchor);
      };
      e.preventDefault();
    };
    const onDbl = e=>{ if(!e.target.closest('button')) sanitizeModalGeometry(); };
    header.addEventListener('mousedown', onDown);
    header.addEventListener('dblclick',  onDbl);
    const prev=top._detachGlobal;
    top._detachGlobal = ()=>{ try{header.removeEventListener('mousedown',onDown);}catch{} try{header.removeEventListener('dblclick',onDbl);}catch{} document.onmousemove=null; document.onmouseup=null; if(typeof prev==='function'){ try{prev();}catch{} } };
  })();
  const wantApply = (isChild && !top.noParentGate) ||
                    (top.kind === 'client-rate' || top.kind === 'candidate-override' || top.kind === 'rate-presets-picker');

  const defaultPrimary =
    (top.kind === 'contract-clone-extend') ? 'Create'
  : (top.kind === 'advanced-search')       ? 'Search'
  : (top.kind === 'selection-load')        ? 'Load'
  : (wantApply ? 'Apply' : 'Save');


  btnSave.textContent = defaultPrimary; btnSave.setAttribute('aria-label', defaultPrimary);
  L('showModal defaultPrimary', { kind: top.kind, defaultPrimary });
  const setCloseLabel = ()=>{
    const label =
      (top.kind === 'advanced-search')
        ? 'Close'
        : (top.isDirty ? 'Discard' : 'Close');
    btnClose.textContent = label;
    btnClose.setAttribute('aria-label', label);
    btnClose.setAttribute('title', label);
  };


  top._updateButtons = ()=>{
    try {
      const h = document.getElementById('modalHint');
      if (h) {
        h.textContent = '';
        h.removeAttribute('data-tone');
        h.classList.remove('ok','warn','err');
      }
    } catch {}

    const parentEditable = top.noParentGate ? true : (parent ? (parent.mode==='edit' || parent.mode==='create') : true);
    const relatedBtn = byId('btnRelated');

    // NEW: hide Preset Manager's "New" button whenever a child is open or when the top frame isn't the manager
    try {
      const rpNew = byId('btnRpNew');
      if (rpNew) {
        const shouldShow = (!isChild && top.kind === 'rates-presets');
        rpNew.style.display = shouldShow ? '' : 'none';
      }
    } catch {}

    if (top.kind === 'advanced-search') {
      btnEdit.style.display='none';
      btnSave.style.display='';
      btnSave.disabled=!!top._saving;
      if (relatedBtn) {
        relatedBtn.style.display = 'none';
        relatedBtn.disabled = true;
      }
    } else if (top.kind === 'rates-presets') {
      btnEdit.style.display='none';
      btnSave.style.display='none';
      btnSave.disabled=true;
      if (relatedBtn) {
        relatedBtn.style.display = 'none';
        relatedBtn.disabled = true;
      }

      // Always show “Close” for the Preset Rates manager (never “Discard”)
      btnClose.textContent = 'Close';
      btnClose.setAttribute('aria-label', 'Close');
      btnClose.setAttribute('title', 'Close');

      L('_updateButtons snapshot (global)', {

        kind: top.kind, isChild, parentEditable, mode: top.mode,
        btnSave: { display: btnSave.style.display, disabled: btnSave.disabled },
        btnEdit: { display: btnEdit.style.display }
      });
      return;
    } else if (isChild && !top.noParentGate) {

      if (top.mode === 'view') {
        btnSave.style.display = 'none';
        btnSave.disabled = true;
        btnEdit.style.display = 'none';
        if (relatedBtn) {
          relatedBtn.style.display = 'none';
          relatedBtn.disabled = true;
        }
      } else {
        btnSave.style.display = parentEditable ? '' : 'none';

        // Child apply gating:
        // - rate-presets-picker: gate by __canSave
        // - client-rate / candidate-override: gate by _applyDesired
        // - address-lookup (and other simple children): always allow Apply if parent is editable
        let wantApply;
        if (top.kind === 'rate-presets-picker') {
          wantApply = !!top.__canSave;
        } else if (top.kind === 'client-rate' || top.kind === 'candidate-override') {
          wantApply = (top._applyDesired === true);
        } else {
          // e.g. address-lookup and other simple child modals
          wantApply = true;
        }

        btnSave.disabled = (!parentEditable) || top._saving || !wantApply;
        btnEdit.style.display='none';
        if (relatedBtn) {
          relatedBtn.style.display = 'none';
          relatedBtn.disabled = true;
        }
        if (LOG) console.log('[MODAL] child _updateButtons()', {
          parentEditable, wantApply, disabled: btnSave.disabled, kind: top.kind
        });
      }
    } else {

      btnEdit.style.display = (top.mode==='view' && top.hasId) ? '' : 'none';

      if (relatedBtn) {
        // Map modal entity → backend /api/related entity key
        const relatedEntity =
          top.entity === 'candidates' ? 'candidate' :
          top.entity === 'clients'    ? 'client'    :
          top.entity === 'contracts'  ? 'contract'  : // backend doesn’t use this yet, but harmless
          null;

        const showRelated =
          !isChild &&
          top.hasId &&
          !!relatedEntity;

        relatedBtn.style.display = showRelated ? '' : 'none';

        const canClick = showRelated && top.mode === 'view';
        relatedBtn.disabled = !canClick;

        if (canClick) {
          relatedBtn.onclick = async (ev) => {
            ev.preventDefault();
            ev.stopPropagation();

            // Guard against stale handlers if the top frame changed
            const fr = currentFrame && currentFrame();
            if (!fr || fr !== top) return;

            const ctx    = window.modalCtx || {};
            const entity = relatedEntity;
            const id     = ctx && ctx.data && ctx.data.id;

            if (!entity || !id) return;
            if (typeof fetchRelatedCounts !== 'function' || typeof showRelatedMenu !== 'function') {
              return;
            }

            let counts = {};
            try {
              counts = await fetchRelatedCounts(entity, id);
            } catch (e) {
              console.error('[RELATED] fetchRelatedCounts failed', e);
            }

            try {
              const rect = relatedBtn.getBoundingClientRect();
              const x = rect.left + rect.width / 2;
              const y = rect.top + rect.height;
              showRelatedMenu(x, y, counts || {}, entity, id);
            } catch (e) {
              console.error('[RELATED] showRelatedMenu failed', e);
            }
          };
        } else {
          relatedBtn.onclick = null;
        }
      }

      if (top.mode === 'create') {
        btnSave.style.display = '';
        btnSave.disabled = top._saving;
      } else if (top.mode==='view') {
        btnSave.style.display = 'none';
        btnSave.disabled = true;
      } else {

        btnSave.style.display='';

        let gateOK = true;
        let elig   = null;

        if (top.entity === 'contracts') {
          try {
            gateOK = (typeof computeContractSaveEligibility === 'function') ? !!computeContractSaveEligibility() : true;
            elig   = (typeof window !== 'undefined') ? (window.__contractEligibility || null) : null;

            if (elig && Array.isArray(elig.reasons)) {
              const tsReason = elig.reasons.find(r => r && r.code === 'TS_BOUNDARY_VIOLATION');
              if (tsReason) {
                gateOK = false;
                if (typeof showModalHint === 'function') showModalHint(tsReason.message || 'Dates exclude existing timesheets.', 'warn');
              }
            }

            // If Save would otherwise be allowed, surface a *polite* warning when candidate is missing.
            if ((top.mode==='edit' || top.mode==='create') && elig && elig.ok) {
              const missingCand = (elig.reasons || []).some(r => r && r.code === 'MISSING_CANDIDATE');
              if (missingCand && typeof showModalHint === 'function') {
                showModalHint('No candidate selected — this contract will be saved as <Unassigned>.', 'warn');
              }
            }

            if (!gateOK && elig && elig.pendingTimeFormat && (!elig.reasons || elig.reasons.length === 0)) {
              gateOK = true;
            }

            if (typeof showModalHint === 'function' && (top.mode==='edit' || top.mode==='create')) {
              if (elig && Array.isArray(elig.reasons) && elig.reasons.length && !elig.ok) {
                const hasTs = elig.reasons.some(r => r && r.code === 'TS_BOUNDARY_VIOLATION');
                if (!hasTs) {
                  const msg = elig.reasons.map(r => r && r.message).filter(Boolean).join(' • ');
                  if (msg) showModalHint(msg, 'warn');
                }
              } else if (elig && elig.pendingTimeFormat && elig.tip) {
                showModalHint(elig.tip, 'ok');
              }
            }
          } catch { gateOK = true; }
        }

        btnSave.disabled = (top.entity === 'contracts')
          ? (top._saving || ((top.kind !== 'contract-clone-extend') && !top.isDirty) || !gateOK)
          : (top._saving);
      }

    }

    setCloseLabel();
    L('_updateButtons snapshot (global)', {
      kind: top.kind, isChild, parentEditable, mode: top.mode,
      btnSave: { display: btnSave.style.display, disabled: btnSave.disabled },
      btnEdit: { display: btnEdit.style.display }
    });
  };



  top._updateButtons();
  btnEdit.onclick = ()=> {
    const isChildNow    = (stack().length > 1);
    const isRatePreset  = (top.kind === 'rate-preset');

    // Block Edit for search & normal child-apply modals,
    // but allow Edit for rate-preset even when opened as a child.
    if (!isRatePreset && (isChildNow || top.kind === 'advanced-search')) return;

    if (top.mode === 'view') {
      top._snapshot = {
        data               : deep(window.modalCtx?.data||null),
        formState          : deep(window.modalCtx?.formState||null),
        rolesState         : deep(window.modalCtx?.rolesState||null),
        ratesState         : deep(window.modalCtx?.ratesState||null),
        hospitalsState     : deep(window.modalCtx?.hospitalsState||null),
        clientSettingsState: deep(window.modalCtx?.clientSettingsState||null),
        overrides          : deep(window.modalCtx?.overrides || { existing:[], stagedNew:[], stagedEdits:{}, stagedDeletes:[] }),
        candidateMainModel : deep(window.modalCtx?.candidateMainModel || null)
      };
      top.isDirty = false;
      setFrameMode(top, 'edit');
      L('btnEdit (global) → switch to edit');
    }

  };


  const handleSecondary = (ev)=>{
    if (currentFrame && currentFrame() !== top) return;
    if (top._confirmingDiscard || top._closing) return;

    if (top.kind==='advanced-search') {
      top._closing=true;
      document.onmousemove=null; document.onmouseup=null; byId('modal')?.classList.remove('dragging'); sanitizeModalGeometry();
      const closing=stack().pop(); if (closing?._detachDirty){ try{closing._detachDirty();}catch{} closing._detachDirty=null; }
      if (closing?._detachGlobal){ try{closing._detachGlobal();}catch{} closing._detachGlobal=null; } top._wired=false;
      if (stack().length>0) {
        const p = currentFrame();
        if (p && p._ctxRef) window.modalCtx = p._ctxRef;

        const resumeMode =
          (typeof closing !== 'undefined' && closing && closing._parentModeOnOpen)
            ? closing._parentModeOnOpen
            : p.mode;

        try { setFrameMode(p, resumeMode); } catch {}
        p._updateButtons?.();

        renderTop();
        try { p.onReturn && p.onReturn(); } catch {}
      } else {
        discardAllModalsAndState(); if (window.__pendingFocus) { try{ renderAll(); } catch(e){ console.error('refresh after modal close failed',e); } }
      }
      return;
    }


    const isChildNow = (stack().length > 1);

    // Child frames with noParentGate: if dirty in edit/create, confirm discard before closing.
    // NEW: never prompt for the Rate Presets Picker — it must behave read-only for discard purposes.
    if (isChildNow && top.noParentGate && (top.mode === 'edit' || top.mode === 'create') && top.isDirty && top.kind !== 'rate-presets-picker') {
      let ok = false;
      try {
        top._confirmingDiscard = true;
        btnClose.disabled = true;
        ok = window.confirm('Discard changes and close?');
      } finally {
        top._confirmingDiscard = false;
        btnClose.disabled = false;
      }
      if (!ok) return;
    }


    if (!isChildNow && !top.noParentGate && top.mode==='edit' && top.kind!=='rates-presets') {
      if (!top.isDirty) {
        if (top._snapshot && window.modalCtx) {
          window.modalCtx.data                = deep(top._snapshot.data);
          window.modalCtx.formState           = deep(top._snapshot.formState);
          window.modalCtx.rolesState          = deep(top._snapshot.rolesState);
          window.modalCtx.ratesState          = deep(top._snapshot.ratesState);
          window.modalCtx.hospitalsState      = deep(top._snapshot.hospitalsState);
          window.modalCtx.clientSettingsState = deep(top._snapshot.clientSettingsState);
          if (top._snapshot.overrides) window.modalCtx.overrides = deep(top._snapshot.overrides);
          window.modalCtx.candidateMainModel  = deep(top._snapshot.candidateMainModel || null);
          try { renderCandidateRatesTable?.(); } catch {}
        }
        try {
          if (top.entity === 'contracts') {
            const cid = window.modalCtx?.data?.id;
            if (cid && typeof discardContractCalendarStage === 'function') discardContractCalendarStage(cid);
          }
        } catch {}
        top.isDirty=false; setFrameMode(top,'view'); top._snapshot=null;
        try{ window.__toast?.('No changes'); }catch{}; return;
      } else {
        let ok=false; try{ top._confirmingDiscard=true; btnClose.disabled=true; ok=window.confirm('Discard changes and return to view?'); } finally { top._confirmingDiscard=false; btnClose.disabled=false; }
        if (!ok) return;
        if (top._snapshot && window.modalCtx) {
          window.modalCtx.data                = deep(top._snapshot.data);
          window.modalCtx.formState           = deep(top._snapshot.formState);
          window.modalCtx.rolesState          = deep(top._snapshot.rolesState);
          window.modalCtx.ratesState          = deep(top._snapshot.ratesState);
          window.modalCtx.hospitalsState      = deep(top._snapshot.hospitalsState);
          window.modalCtx.clientSettingsState = deep(top._snapshot.clientSettingsState);
          if (top._snapshot.overrides) window.modalCtx.overrides = deep(top._snapshot.overrides);
          // 🔹 restore candidateMainModel as well so job titles (and primary) roll back
          window.modalCtx.candidateMainModel  = deep(top._snapshot.candidateMainModel || null);
          try { renderCandidateRatesTable?.(); } catch {}
        }
        try {
          if (top.entity === 'contracts') {
            const cid = window.modalCtx?.data?.id;
            if (cid && typeof discardContractCalendarStage === 'function') discardContractCalendarStage(cid);
          }
        } catch {}
        top.isDirty=false; top._snapshot=null; setFrameMode(top,'view'); return;
      }
    }


    if (top._closing) return;
    top._closing=true;
    document.onmousemove=null; document.onmouseup=null; byId('modal')?.classList.remove('dragging');

    if (!isChildNow && !top.noParentGate && top.mode==='create' && top.isDirty && top.kind!=='rates-presets') {
      let ok=false; try{ top._confirmingDiscard=true; btnClose.disabled=true; ok=window.confirm('You have unsaved changes. Discard them and close?'); } finally { top._confirmingDiscard=false; btnClose.disabled=false; }
      if (!ok) { top._closing=false; return; }
    }


    try {
      if (top.entity === 'contracts' && (top.mode==='edit' || top.mode==='create')) {
        const cid = window.modalCtx?.data?.id;
        if (cid && typeof discardContractCalendarStage === 'function') discardContractCalendarStage(cid);
      }
    } catch {}
    sanitizeModalGeometry();
    const closing=stack().pop(); if (closing?._detachDirty){ try{closing._detachDirty();}catch{} closing._detachDirty=null; }
    if (closing?._detachGlobal){ try{closing._detachGlobal();}catch{} closing._detachGlobal=null; } top._wired=false;
    if (stack().length>0) {
      const p=currentFrame();
      // restore parent context to ensure actions (Clone & Extend) render correctly
      if (p && p._ctxRef) window.modalCtx = p._ctxRef;
      // ▶ for most children, resume the original parent mode; for the rate-presets picker,
      //    keep whatever mode the parent is currently in (typically 'edit' after Apply).
      const resumeMode =
        (typeof closing !== 'undefined' &&
         closing &&
         closing._parentModeOnOpen &&
         closing.kind !== 'rate-presets-picker')
          ? closing._parentModeOnOpen
          : p.mode;

      try { setFrameMode(p, resumeMode); } catch {}
      p._updateButtons && p._updateButtons();
      renderTop();

      try{ p.onReturn && p.onReturn(); }catch{}
    } else {
      discardAllModalsAndState();
      if (window.__pendingFocus) { try{ renderAll(); } catch(e) { console.error('refresh after modal close failed', e); } }
    }

  };
  // AFTER
  const onCloseClick = (ev) => {
    const btn = ev?.currentTarget || byId('btnCloseModal');
    const bound = btn?.dataset?.ownerToken;
    const topNow = currentFrame();
    if (!topNow || bound !== topNow._token) return;
    handleSecondary(ev);
  };

  const bindClose = (btn, fr) => {
    if (!btn || !fr) return;
    btn.dataset.ownerToken = fr._token;
    btn.onclick = onCloseClick;
  };

  bindClose(btnClose, top);

  const hasStagedClientDeletes = ()=> {
    try {
      const anyFlag = Array.isArray(window.modalCtx?.ratesState) && window.modalCtx.ratesState.some(w => w && w.__delete === true);
      const anySet  = (window.modalCtx?.ratesStagedDeletes instanceof Set) && window.modalCtx.ratesStagedDeletes.size > 0;
      const ovDel   = (window.modalCtx?.overrides?.stagedDeletes instanceof Set) && window.modalCtx.overrides.stagedDeletes.size > 0;
      return !!(anyFlag || anySet || ovDel);
    } catch { return false; }
  };

  async function saveForFrame(fr) {
    if (!fr || fr._saving) return;
    const onlyDel   = hasStagedClientDeletes();
    const allowApply= (fr.kind==='candidate-override' || fr.kind==='client-rate') && fr._applyDesired===true;

    L('saveForFrame ENTER (global)', { kind: fr.kind, mode: fr.mode, noParentGate: fr.noParentGate, isDirty: fr.isDirty, onlyDel, allowApply });

    // Changed: do NOT block create saves; only no-op in EDIT when nothing changed
    const isChildNow = (window.__modalStack?.length > 1);
    const shouldNoop =
      (fr.kind!=='advanced-search') &&
      !fr.noParentGate &&
      fr.mode === 'edit' &&            // <-- was fr.mode!=='view'
      !fr.isDirty &&
      !onlyDel &&
      !allowApply;

    if (shouldNoop) {
      L('saveForFrame GUARD (global): no-op (no changes and apply not allowed)');
      if (isChildNow) {
        sanitizeModalGeometry(); window.__modalStack.pop();
        if (window.__modalStack.length>0) { const p=window.__modalStack[window.__modalStack.length-1]; renderTop(); try{ p.onReturn && p.onReturn(); }catch{} }
        else { /* keep open */ }
      } else {
        fr.isDirty=false; fr._snapshot=null; setFrameMode(fr,'view'); fr._updateButtons&&fr._updateButtons();
      }
      try{ window.__toast?.('No changes'); }catch{}; return;
    }

    fr.persistCurrentTabState();
    if (isChildNow && !fr.noParentGate && fr.kind!=='advanced-search') {
      const p=window.__modalStack[window.__modalStack.length-2];
      if (!p || !(p.mode==='edit'||p.mode==='create')) { L('saveForFrame GUARD (global): parent not editable'); return; }
    }
    fr._saving=true; fr._updateButtons&&fr._updateButtons();

    let ok=false, saved=null;
    if (typeof fr.onSave==='function') {
      try { const res=await fr.onSave(); ok = (res===true) || (res && res.ok===true); if (res&&res.saved) saved=res.saved; }
      catch (e) { L('saveForFrame onSave threw (global)', e); ok=false; }
    }
    fr._saving=false; if (!ok) { L('saveForFrame RESULT not ok (global)'); fr._updateButtons&&fr._updateButtons(); return; }

    if (fr.kind === 'advanced-search') {
      sanitizeModalGeometry();
      const closing = window.__modalStack.pop();
      if (closing?._detachDirty){ try{closing._detachDirty();}catch{} closing._detachDirty=null; }
      if (closing?._detachGlobal){ try{closing._detachGlobal();}catch{} closing._detachGlobal=null; }
      fr._wired = false;

      if (window.__modalStack.length > 0) {
        const p = window.__modalStack[window.__modalStack.length - 1];
        renderTop();
        try { p.onReturn && p.onReturn(); } catch {}
      } else {
        // 🔹 No more frames → fully tear down the modal & overlay
        discardAllModalsAndState();
      }

      L('saveForFrame EXIT (global advanced-search closed)');
      return;
    }

    if (isChildNow) {
      // If this child should remain open after save (successor contract),
      // flip it in-place to view mode and keep it on screen.
      if (fr.stayOpenOnSave) {
        try {
          if (saved && window.modalCtx) {
            window.modalCtx.data = { ...(window.modalCtx.data||{}), ...(saved.contract || saved) };
            fr.hasId = !!window.modalCtx.data?.id;
          }
          setFrameMode(fr, 'view');
          fr._updateButtons && fr._updateButtons();
          renderTop();
        } catch {}
        L('saveForFrame EXIT (child kept open)');
      } else {
        if (!fr.noParentGate) {
          try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
        }
        sanitizeModalGeometry(); window.__modalStack.pop();
        if (window.__modalStack.length>0) {
          const p=window.__modalStack[window.__modalStack.length-1];

          // ▶ for most children, resume the original parent mode; for the rate-presets picker,
          //    keep whatever mode the parent is currently in (typically 'edit' after Apply).
          const resumeMode =
            (typeof fr !== 'undefined' &&
             fr &&
             fr._parentModeOnOpen &&
             fr.kind !== 'rate-presets-picker')
              ? fr._parentModeOnOpen
              : p.mode; // keep whatever the parent already was

          try { setFrameMode(p, resumeMode); } catch {}
          p._updateButtons && p._updateButtons();
          renderTop();


          // ▶ nudge calendar/action bar re-wire if needed
          try { window.dispatchEvent(new Event('contracts-main-rendered')); } catch {}
          try { p.onReturn && p.onReturn(); }catch{}
        } else {
        }
        L('saveForFrame EXIT (global child)');
      }
    } else {

      try {
        const savedContract = (saved && (saved.contract || saved)) || null;
        const id = savedContract?.id || window.modalCtx?.data?.id || null;
        if (id && savedContract) {
          const idx = Array.isArray(currentRows) ? currentRows.findIndex(x => String(x.id) === String(id)) : -1;
          if (idx >= 0) currentRows[idx] = savedContract;
          (window.__lastSavedAtById ||= {})[String(id)] = Date.now();
        }
      } catch (e) { console.warn('[SAVE] list cache merge failed', e); }

      if (saved && window.modalCtx) { window.modalCtx.data = { ...(window.modalCtx.data||{}), ...(saved.contract || saved) }; fr.hasId = !!window.modalCtx.data?.id; }
      fr.isDirty=false; fr._snapshot=null; setFrameMode(fr,'view');
      L('saveForFrame EXIT (global parent, kept open)');
    }
  }

  const onSaveClick = async (ev)=>{
    const btn=ev?.currentTarget || byId('btnSave');
    const topNow=currentFrame(); const bound=btn?.dataset?.ownerToken;
    if (LOG) console.log('[MODAL] click #btnSave (global)', {boundToken:bound, topToken:topNow?._token, topKind:topNow?.kind, topTitle:topNow?.title});
    if(!topNow) return; if(bound!==topNow._token){ if(LOG) console.warn('[MODAL] token mismatch (global); using top frame'); }
    await saveForFrame(topNow);
  };

  const bindSave = (btn,fr)=>{ if(!btn||!fr) return; btn.dataset.ownerToken = fr._token; btn.onclick = onSaveClick; if(LOG) console.log('[MODAL] bind #btnSave → (global)',{ownerToken:fr._token,kind:fr.kind||'(parent)',title:fr.title,mode:fr.mode}); };
  bindSave(btnSave, top);
  // FIX: ignore programmatic "dirty" while suppression is active
  const onDirtyEvt = ()=>{
    const fr = currentFrame();
    if (fr && fr._suppressDirty) return;

    // Allow presets picker dirty → parent (ignore only truly load-only frames)
    if (fr && fr._loadOnly === true) return;


    const isChildNow = (stack().length > 1);
    if (isChildNow) {
      if (fr && fr.noParentGate) {
        if (fr.mode === 'edit' || fr.mode === 'create') {
          fr.isDirty = true;
          fr._updateButtons && fr._updateButtons();
        }
      } else {
        const p = parentFrame();
        if (p && (p.mode === 'edit' || p.mode === 'create')) {
          p.isDirty = true;
          p._updateButtons && p._updateButtons();
        }
      }
    } else if (fr && (fr.mode === 'edit' || fr.mode === 'create')) {
      fr.isDirty = true;
      fr._updateButtons && fr._updateButtons();
    }
    try{ const t=currentFrame(); if(t && t.entity==='candidates' && t.currentTabKey==='rates'){ renderCandidateRatesTable?.(); } }catch{}
  };


  const onApplyEvt = ev=>{
    const isChildNow=(stack().length>1); if(!isChildNow) return;
    const t=currentFrame(); if(!(t && (t.kind==='client-rate'||t.kind==='candidate-override'))) return;
    const enabled=!!(ev && ev.detail && ev.detail.enabled);
    t._applyDesired=enabled;
    t._updateButtons&&t._updateButtons();
    bindSave(byId('btnSave'), t);
    if(LOG) console.log('[MODAL] onApplyEvt (global) → _applyDesired =', enabled,'rebound save to top frame');
  };

  const onModeChanged = ev=>{
    const isChildNow=(stack().length>1); if(!isChildNow) return;
    const parentIdx=stack().length-2, changed=ev?.detail?.frameIndex ?? -1;
    if(changed===parentIdx){ if(LOG) console.log('[MODAL] parent mode changed (global) → child _updateButtons()'); const t=currentFrame(); t._updateButtons&&t._updateButtons(); bindSave(byId('btnSave'), t); }
  };

  const onMarginsEvt = ()=>{ try { const t=currentFrame(); if (t && (t.mode==='edit'||t.mode==='create')) t._updateButtons(); } catch {} };

  if (!top._wired) {
    window.addEventListener('modal-dirty', onDirtyEvt);
    window.addEventListener('modal-apply-enabled', onApplyEvt);
    window.addEventListener('modal-frame-mode-changed', onModeChanged);
    window.addEventListener('contract-margins-updated', onMarginsEvt);
    const onEsc=e=>{ if(e.key==='Escape'){ if(top._confirmingDiscard||top._closing) return; e.preventDefault(); byId('btnCloseModal').click(); } };
    window.addEventListener('keydown', onEsc);
    const onOverlayClick=e=>{ if(top._confirmingDiscard||top._closing) return; if(e.target===byId('modalBack')) { e.preventDefault(); e.stopPropagation(); return; } };
    byId('modalBack').addEventListener('click', onOverlayClick, true);

    top._detachGlobal = ()=>{ try{window.removeEventListener('modal-dirty',onDirtyEvt);}catch{} try{window.removeEventListener('modal-apply-enabled',onApplyEvt);}catch{} try{window.removeEventListener('modal-frame-mode-changed',onModeChanged);}catch{} try{window.removeEventListener('contract-margins-updated',onMarginsEvt);}catch{} try{window.removeEventListener('keydown',onEsc);}catch{} try{byId('modalBack').removeEventListener('click', onOverlayClick, true);}catch{}; };
    top._wired = true;
    L('renderTop (global): listeners wired');
  }


  const parentEditable = parent && (parent.mode==='edit' || parent.mode==='create');
  const isChildNow = (stack().length > 1);
  if (isChildNow && !top.noParentGate) setFormReadOnly(byId('modalBody'), !parentEditable);
  else                                 setFrameMode(top, top.mode);

  top._updateButtons && top._updateButtons();
  bindSave(btnSave, top);

  try {
    const pc = document.getElementById('btnPickCandidate');
    const pl = document.getElementById('btnPickClient');
    L('renderTop final snapshot (global)', {
      entity: top.entity, mode: top.mode, currentTabKey: top.currentTabKey,
      pickButtons: {
        btnPickCandidate: { exists: !!pc, disabled: !!(pc && pc.disabled) },
        btnPickClient:    { exists: !!pl, disabled: !!(pl && pl.disabled) }
      }
    });
  } catch {}

  GE();
}



  byId('modalBack').style.display='flex';
  window.__getModalFrame = currentFrame;
  L('showModal ENTER', { title, tabs: (tabs||[]).map(t=>t.key||t.title), hasId, entity: window.modalCtx?.entity, kind: opts.kind, forceEdit: !!opts.forceEdit });
  renderTop();
}

// Selection state helpers — simplified to explicit IDs only

// ──────────────────────────────────────────────────────────────────────────────
// IDs-only selection helpers (single source of truth: Set of selected IDs)
// ──────────────────────────────────────────────────────────────────────────────



function ensureSelection(section) {
  window.__selection = window.__selection || {};
  if (!window.__selection[section]) {
    window.__selection[section] = {
      fingerprint: '',
      ids: new Set(),
    };
  }
  return window.__selection[section];
}

function getSelectionSnapshot(section) {
  const sel = ensureSelection(section);
  return {
    fingerprint: sel.fingerprint || '',
    ids: Array.from(sel.ids || []),
    section,
  };
}
function applySelectionSnapshot(section, snapshot) {
  if (!snapshot || (snapshot.section && snapshot.section !== section)) {
    return getSelectionSnapshot(section);
  }
  const sel = ensureSelection(section);
  sel.fingerprint = String(snapshot.fingerprint || sel.fingerprint || '');
  sel.ids = new Set((snapshot.ids || []).map(String));
  return getSelectionSnapshot(section);
}

function isRowSelected(section, id) {
  if (!id) return false;
  return ensureSelection(section).ids.has(String(id));
}

function setRowSelected(section, id, selected) {
  if (!id) return getSelectionSnapshot(section);
  const sel = ensureSelection(section);
  const key = String(id);
  if (selected) sel.ids.add(key);
  else sel.ids.delete(key);
  return getSelectionSnapshot(section);
}


async function applyShortlistFilter(section, { ids }) {
  const cleanIds = Array.isArray(ids) ? ids.map(String).filter(Boolean) : [];
  if (!cleanIds.length) {
    alert('No records selected to focus.');
    return;
  }

  // Reset paging & REPLACE existing filters with IDs-only
  window.__listState = window.__listState || {};
  const st = (window.__listState[section] ||= {
    page: 1, pageSize: 50, total: null, hasMore: false, filters: null,
  });
  st.page = 1;
  st.filters = { ids: cleanIds }; // ← replace, don't merge

  // Mirror into selection for checkbox sync
  const sel = ensureSelection(section);
  sel.fingerprint = JSON.stringify({ section, filters: st.filters || {} });
  sel.ids = new Set(cleanIds);

  // Reload with the focused shortlist
  const rows = await search(section, st.filters);
  renderSummary(rows);
}


// Convenience: focus current selection by applying its IDs as a filter
async function focusCurrentSelection(section) {
  const sel = ensureSelection(section);
  const ids = Array.from(sel.ids || []);
  if (!ids.length) {
    alert('No records selected to focus.');
    return;
  }
  await applyShortlistFilter(section, { ids });
}

// Apply IDs-only selection as a filter and reload
// Apply IDs-only selection as a filter and reload
async function applySelectionAsFilter(section, selectionSnapshot) {
  const ids = Array.isArray(selectionSnapshot?.ids)
    ? selectionSnapshot.ids.map(String).filter(Boolean)
    : [];

  if (!ids.length) {
    alert('No records selected to focus.');
    return;
  }

  // Reset paging & REPLACE existing filters with IDs-only
  window.__listState = window.__listState || {};
  const st = (window.__listState[section] ||= {
    page: 1, pageSize: 50, total: null, hasMore: false, filters: null,
  });
  st.page = 1;
  st.filters = { ids }; // ← replace, don't merge

  // Mirror into selection for checkbox sync
  const sel = ensureSelection(section);
  sel.fingerprint = JSON.stringify({ section, filters: st.filters || {} });
  sel.ids = new Set(ids);

  // Reload data and re-render
  const rows = await search(section, st.filters);
  renderSummary(rows);
}





function clearSelection(section) {
  const sel = ensureSelection(section);
  sel.ids.clear();
  return getSelectionSnapshot(section);
}

function serializeSelection(section) {
  return getSelectionSnapshot(section);
}



function mergeSelectionSnapshots(section, baseSnapshot, addSnapshot) {
  const base = baseSnapshot || getSelectionSnapshot(section);
  const add  = addSnapshot  || {};
  const result = {
    fingerprint: base.fingerprint || add.fingerprint || '',
    ids: Array.from(new Set([
      ...(base.ids || []).map(String),
      ...(add.ids  || []).map(String),
    ])),
    section
  };
  return applySelectionSnapshot(section, result);
}

function dedupeIds(arr) {
  return Array.from(new Set((arr || []).map(String)));
}





// =================== ADD HOSPITAL MODAL (UPDATED: push into stagedNew) ===================
// ==== CHILD MODAL (ADD HOSPITAL) — throw on errors; return true on success ====

function openClientHospitalModal(client_id) {
  const parentFrame = _currentFrame();
  // ✅ Allow create OR edit to add hospitals pre-save
  const parentEditable = parentFrame && (parentFrame.mode === 'edit' || parentFrame.mode === 'create');
  const ctx = window.modalCtx; // 🔧 use canonical context

  const formHtml = html(`
    <div class="form" id="hospitalForm">
      ${input('hospital_name_norm','Hospital / Trust (normalised)','', 'text', parentEditable ? {} : { disabled:true })}
      ${input('ward_hint','Ward hint (optional)','', 'text', parentEditable ? {} : { disabled:true })}
    </div>
  `);

  // ⬇️ Key change: pass noParentGate so the child save isn't blocked by the "not dirty" guard
  showModal(
    'Add Hospital / Ward',
    [{key:'form',label:'Form'}],
    () => formHtml,
    async ()=> {
      const pf = _parentFrame();
      // ✅ Allow create OR edit to apply
      if (!pf || (pf.mode !== 'edit' && pf.mode !== 'create')) return false;

      const raw  = collectForm('#hospitalForm');
      const name = String(raw.hospital_name_norm || '').trim();
      if (!name) { alert('Hospital / Trust is required'); return false; }

      const H = ctx.hospitalsState || (ctx.hospitalsState = { existing: [], stagedNew: [], stagedEdits:{}, stagedDeletes: new Set() });
      H.stagedNew.push({ hospital_name_norm: name, ward_hint: (raw.ward_hint || '').trim() || null });

      try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
      try { renderClientHospitalsTable(); } catch {}
      return true; // Apply closes child
    },
    false,
    () => {
      const parent = _currentFrame();
      if (parent) { parent.currentTabKey = 'hospitals'; parent.setTab('hospitals'); }
    },
    // ⬇️ Options: bypass parent-gate guard for this child modal only
    { noParentGate: true, forceEdit: true, kind: 'client-hospital' }
  );
}

function renderClientHospitalsTable() {
  const el = byId('clientHospitals'); if (!el) return;

  const frame = _currentFrame();
  // ✅ Allow create OR edit to add/edit hospitals
  const parentEditable = frame && (frame.mode === 'edit' || frame.mode === 'create');

  const ctx = window.modalCtx; // 🔧 use canonical context
  const H = ctx.hospitalsState || (ctx.hospitalsState = { existing: [], stagedNew: [], stagedEdits: {}, stagedDeletes: new Set() });

  // ⬇️ Key change: normalise stagedDeletes back to a Set if it was JSON-cloned to an Array/object
  if (!(H.stagedDeletes instanceof Set)) {
    H.stagedDeletes = new Set(Array.isArray(H.stagedDeletes) ? H.stagedDeletes : Object.keys(H.stagedDeletes || {}));
  }
  H.stagedNew     = Array.isArray(H.stagedNew) ? H.stagedNew : [];
  H.existing      = Array.isArray(H.existing) ? H.existing : [];
  H.stagedEdits   = H.stagedEdits || {};

  el.innerHTML = '';

  const tbl = document.createElement('table'); tbl.className = 'grid';
  const cols = ['hospital_name_norm','ward_hint','status'];
  const thead = document.createElement('thead');
  const trh = document.createElement('tr');
  cols.forEach(c => { const th = document.createElement('th'); th.textContent = c; trh.appendChild(th); });
  thead.appendChild(trh); tbl.appendChild(thead);

  const tb = document.createElement('tbody');

  // Existing rows (DB)
  (H.existing || []).forEach((x) => {
    if (H.stagedDeletes.has(String(x.id))) return;

    const tr = document.createElement('tr');

    const nameTd = document.createElement('td');
    const nameInp = document.createElement('input');
    nameInp.type = 'text'; nameInp.value = x.hospital_name_norm || '';
    nameInp.disabled = !parentEditable;
    nameInp.oninput = () => {
      H.stagedEdits[String(x.id)] = { ...(H.stagedEdits[String(x.id)] || {}), hospital_name_norm: nameInp.value };
      try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
    };
    nameTd.appendChild(nameInp);

    const hintTd = document.createElement('td');
    const hintInp = document.createElement('input');
    hintInp.type = 'text'; hintInp.value = x.ward_hint || '';
    hintInp.disabled = !parentEditable;
    hintInp.oninput = () => {
      H.stagedEdits[String(x.id)] = { ...(H.stagedEdits[String(x.id)] || {}), ward_hint: hintInp.value || null };
      try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {}
    };
    hintTd.appendChild(hintInp);

    const actTd = document.createElement('td');
    const rmBtn = document.createElement('button');
    rmBtn.textContent = 'Remove';
    rmBtn.disabled = !parentEditable;
    rmBtn.setAttribute('data-action', 'delete');
    rmBtn.setAttribute('data-hid', String(x.id));
    rmBtn.className = 'btnDelHospital';
    actTd.appendChild(rmBtn);

    tr.appendChild(nameTd); tr.appendChild(hintTd); tr.appendChild(actTd);
    tb.appendChild(tr);
  });

  // New (unsaved) rows
  (H.stagedNew || []).forEach((x, idx) => {
    const tr = document.createElement('tr');
    const nameTd = document.createElement('td');
    const nameInp = document.createElement('input');
    nameInp.type = 'text'; nameInp.value = x.hospital_name_norm || '';
    nameInp.disabled = !parentEditable;
    nameInp.oninput = () => { x.hospital_name_norm = nameInp.value; try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {} };
    nameTd.appendChild(nameInp);

    const hintTd = document.createElement('td');
    const hintInp = document.createElement('input');
    hintInp.type = 'text'; hintInp.value = x.ward_hint || '';
    hintInp.disabled = !parentEditable;
    hintInp.oninput = () => { x.ward_hint = hintInp.value || null; try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {} };
    hintTd.appendChild(hintInp);

    const actTd = document.createElement('td');
    const rm = document.createElement('button'); rm.textContent = 'Remove (staged)';
    rm.disabled = !parentEditable;
    rm.onclick = () => { if (!parentEditable) return; H.stagedNew.splice(idx, 1); renderClientHospitalsTable(); try { window.dispatchEvent(new CustomEvent('modal-dirty')); } catch {} };
    actTd.appendChild(rm);

    tr.appendChild(nameTd); tr.appendChild(hintTd); tr.appendChild(actTd);
    tb.appendChild(tr);
  });

  tbl.appendChild(tb);

  const actions = document.createElement('div');
  actions.className = 'actions';
  actions.innerHTML = `<button id="btnAddClientHospital"${parentEditable ? '' : ' disabled'}>Add Hospital / Ward</button>
    ${parentEditable ? '' : '<span class="hint">Read-only. Click “Edit” in the main dialog to add/modify hospitals.</span>'}`;

  el.appendChild(tbl);
  el.appendChild(actions);

  const addBtn = byId('btnAddClientHospital');
  if (addBtn && parentEditable) addBtn.onclick = () => openClientHospitalModal(ctx.data?.id);
}


// =================== HOSPITALS TABLE (UPDATED: staged delete & edit) ===================


async function renderClientSettingsUI(settingsObj){
  const div = byId('clientSettings'); if (!div) return;

  const ctx = window.modalCtx;

  const initial = (ctx.clientSettingsState && typeof ctx.clientSettingsState === 'object')
    ? ctx.clientSettingsState
    : (settingsObj && typeof settingsObj === 'object' ? settingsObj : {});

  const _toHHMM = (v) => {
    if (!v) return '';
    const s = String(v).trim();
    if (/^\d{2}:\d{2}(:\d{2})?$/.test(s)) return s.slice(0,5);
    return s;
  };

  const s = {
    timezone_id : initial.timezone_id ?? 'Europe/London',
    day_start   : _toHHMM(initial.day_start)   || '06:00',
    day_end     : _toHHMM(initial.day_end)     || '20:00',
    night_start : _toHHMM(initial.night_start) || '20:00',
    night_end   : _toHHMM(initial.night_end)   || '06:00',
    week_ending_weekday: Number.isInteger(Number(initial.week_ending_weekday)) ? String(Math.min(6, Math.max(0, Number(initial.week_ending_weekday)))) : '0',
    // NEW defaults
    pay_reference_required: !!initial.pay_reference_required,
    invoice_reference_required: !!initial.invoice_reference_required,
    default_submission_mode: String(initial.default_submission_mode || 'ELECTRONIC').toUpperCase()
  };

  ctx.clientSettingsState = { ...initial, ...s };

  const input = (name,label,val,type='text') =>
    `<div class="row"><label>${label}</label><div class="controls"><input class="input" name="${name}" value="${String(val||'')}" ${type==='time'?'type="time" step="60"':''}/></div></div>`;

  const weekDaySelect = () => {
    const opts = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']
      .map((lab,idx)=>`<option value="${idx}" ${String(idx)===String(s.week_ending_weekday)?'selected':''}>${lab}</option>`).join('');
    return `<div class="row"><label>Week Ending Day</label><div class="controls"><select name="week_ending_weekday">${opts}</select></div></div>`;
  };

  // NEW: gates & default submission
  const gatesAndSubmission = () => {
    return `
      <div class="row">
        <label class="inline">
          <input type="checkbox" name="pay_reference_required" ${s.pay_reference_required?'checked':''}/>
          <span>Reference No. required to PAY</span>
        </label>
      </div>
      <div class="row">
        <label class="inline">
          <input type="checkbox" name="invoice_reference_required" ${s.invoice_reference_required?'checked':''}/>
          <span>Reference No. required to INVOICE</span>
        </label>
      </div>
      <div class="row">
        <label>Default Submission</label>
        <div class="controls">
          <select name="default_submission_mode">
            <option value="ELECTRONIC" ${s.default_submission_mode==='ELECTRONIC'?'selected':''}>ELECTRONIC</option>
            <option value="MANUAL" ${s.default_submission_mode==='MANUAL'?'selected':''}>MANUAL</option>
          </select>
        </div>
      </div>
    `;
  };

  div.innerHTML = `
    <div class="form" id="clientSettingsForm">
      ${input('timezone_id','Timezone', s.timezone_id)}
      ${input('day_start','Day shift starts (HH:MM)', s.day_start, 'time')}
      ${input('day_end','Day shift ends (HH:MM)', s.day_end, 'time')}
      ${input('night_start','Night shift starts (HH:MM)', s.night_start, 'time')}
      ${input('night_end','Night shift ends (HH:MM)', s.night_end, 'time')}
      ${weekDaySelect()}

      ${gatesAndSubmission()}

      <div class="hint" style="grid-column:1/-1">
        Example: Day 06:00–20:00, Night 20:00–06:00. These settings override global defaults for this client only.
      </div>
    </div>
  `;

  const root = document.getElementById('clientSettingsForm');
  const hhmm = /^([01]\d|2[0-3]):[0-5]\d$/;

  let lastValid = { ...s };
  if (root.__wired) {
    root.removeEventListener('input', root.__syncSoft, true);
    root.removeEventListener('change', root.__syncValidate, true);
    ['day_start','day_end','night_start','night_end'].forEach(k=>{
      const el = root.querySelector(`input[name="${k}"]`);
      if (el && el.__syncValidate) el.removeEventListener('blur', el.__syncValidate, true);
    });
  }

  const syncSoft = ()=>{
    const frame = _currentFrame();
    if (!frame || frame.mode !== 'edit') return;
    const vals = collectForm('#clientSettingsForm', false);
    const next = { ...ctx.clientSettingsState, ...vals };
    ['day_start','day_end','night_start','night_end'].forEach(k=>{
      const v = String(vals[k] ?? '').trim();
      if (v && !hhmm.test(v)) next[k] = lastValid[k];
    });
    // coerce week_ending_weekday to 0..6 (string)
    const w = Number(next.week_ending_weekday);
    next.week_ending_weekday = Number.isInteger(w) ? String(Math.min(6, Math.max(0, w))) : '0';

    // NEW: checkboxes & select coercion
    next.pay_reference_required = !!(vals.pay_reference_required === 'on' || vals.pay_reference_required === true || vals.pay_reference_required === 'true');
    next.invoice_reference_required = !!(vals.invoice_reference_required === 'on' || vals.invoice_reference_required === true || vals.invoice_reference_required === 'true');
    next.default_submission_mode = String((vals.default_submission_mode || next.default_submission_mode || 'ELECTRONIC')).toUpperCase();

    ctx.clientSettingsState = next;
  };

  let lastAlertAt = 0;
  const syncValidate = (ev)=>{
    const frame = _currentFrame();
    if (!frame || frame.mode !== 'edit') return;

    const vals = collectForm('#clientSettingsForm', false);
    let hadError = false;

    ['day_start','day_end','night_start','night_end'].forEach(k=>{
      const v = String(vals[k] ?? '').trim();
      if (v && !hhmm.test(v)) {
        hadError = true;
        const el = root.querySelector(`input[name="${k}"]`);
        if (el) el.value = lastValid[k] || '';
      }
    });

    let w = Number(vals.week_ending_weekday);
    if (!Number.isInteger(w) || w<0 || w>6) { hadError = true; w = Number(lastValid.week_ending_weekday) || 0; }

    // NEW coercions
    const payReq = !!(vals.pay_reference_required === 'on' || vals.pay_reference_required === true || vals.pay_reference_required === 'true');
    const invReq = !!(vals.invoice_reference_required === 'on' || vals.invoice_reference_required === true || vals.invoice_reference_required === 'true');
    const mode   = String(vals.default_submission_mode || '').toUpperCase();
    const modeOk = (mode === 'ELECTRONIC' || mode === 'MANUAL') ? mode : 'ELECTRONIC';

    ctx.clientSettingsState = {
      ...ctx.clientSettingsState,
      ...vals,
      week_ending_weekday: String(w),
      pay_reference_required: payReq,
      invoice_reference_required: invReq,
      default_submission_mode: modeOk
    };
    lastValid = { ...ctx.clientSettingsState };

    if (hadError) {
      const now = Date.now();
      if (now - lastAlertAt > 400) {
        alert('Please fix invalid values (times must be HH:MM, week ending must be 0–6).');
        lastAlertAt = now;
      }
    }
  };

  root.__syncSoft = syncSoft;
  root.__syncValidate = syncValidate;
  root.addEventListener('input',  syncSoft, true);
  root.addEventListener('change', syncValidate, true);
  ['day_start','day_end','night_start','night_end'].forEach(k=>{
    const el = root.querySelector(`input[name="${k}"]`);
    if (el) {
      el.__syncValidate = syncValidate;
      el.addEventListener('blur', syncValidate, true);
      el.setAttribute('step', '60');
    }
  });
  root.__wired = true;
}


// ---- Umbrella modal
// ========================= openUmbrella (FIXED) =========================
// ---- Umbrella modal
// ========================= openUmbrella (FIXED) =========================
// ================== FRONTEND: openUmbrella (UPDATED) ==================
// ================== FIXED: openUmbrella (hydrate before showModal) ==================
// ================== FIXED: openUmbrella (hydrate before showModal) ==================
async function openUmbrella(row){
  // ===== Logging helpers (toggle with window.__LOG_MODAL = true/false) =====
  const LOG = (typeof window.__LOG_MODAL === 'boolean') ? window.__LOG_MODAL : true;
  const L  = (...a)=> { if (LOG) console.log('[OPEN_UMBRELLA]', ...a); };
  const W  = (...a)=> { if (LOG) console.warn('[OPEN_UMBRELLA]', ...a); };
  const E  = (...a)=> { if (LOG) console.error('[OPEN_UMBRELLA]', ...a); };

  const deep = (o)=> JSON.parse(JSON.stringify(o || {}));
  const incoming = deep(row || {});
  const seedId   = incoming?.id || null;

  L('ENTRY', { incomingKeys: Object.keys(incoming||{}), seedId });

  const unwrapSingle = (data, key) => {
    if (Array.isArray(data)) return data[0] || null;
    if (data && key && data[key]) return unwrapSingle(data[key], null);
    if (data && Array.isArray(data.rows))  return data.rows[0]  || null;
    if (data && Array.isArray(data.items)) return data.items[0] || null;
    if (data && Array.isArray(data.data))  return data.data[0]  || null;
    return (data && typeof data === 'object') ? data : null;
  };

  // 1) Hydrate full umbrella if we have an id
  let full = incoming;
  if (seedId) {
    try {
      const url = API(`/api/umbrellas/${encodeURIComponent(seedId)}`);
      L('[HTTP] GET', url);
      const res = await authFetch(url);
      L('[HTTP] status', res?.status, res?.ok);

      try {
        const raw = await res.clone().text();
        if (LOG) console.debug('[HTTP] raw body (≤2KB):', raw.slice(0, 2048));
      } catch (peekErr) { W('[HTTP] raw peek failed', peekErr?.message || peekErr); }

      if (res.ok) {
        const data = await res.json().catch(()=> ({}));
        const unwrapped = unwrapSingle(data, 'umbrella');
        L('hydrated JSON keys', Object.keys(data||{}), 'unwrapped keys', Object.keys(unwrapped||{}));
        full = unwrapped || incoming;
      } else {
        W('non-OK response, using incoming row');
      }
    } catch (e) {
      W('openUmbrella hydrate failed; using summary row', e);
    }
  } else {
    L('no seedId — create mode');
  }

  // 2) Seed window.modalCtx and SHOW IMMEDIATELY
  const fullKeys = Object.keys(full || {});
  L('seeding window.modalCtx', { entity: 'umbrellas', fullId: full?.id, fullKeys });

  window.modalCtx = {
    entity: 'umbrellas',
    data: deep(full),
    formState: { __forId: full?.id || null, main: {} },
    rolesState: null,
    ratesState: null,
    clientSettingsState: null,
    openToken: ((full?.id) || 'new') + ':' + Date.now()
  };

  L('window.modalCtx seeded', {
    entity: window.modalCtx.entity,
    dataId: window.modalCtx.data?.id,
    dataKeys: Object.keys(window.modalCtx.data||{}),
    formStateForId: window.modalCtx.formState?.__forId,
    openToken: window.modalCtx.openToken
  });

  // 3) Render modal NOW
  L('calling showModal with hasId=', !!full?.id, 'rawHasIdArg=', full?.id);
  showModal(
    'Umbrella',
    [{ key:'main', label:'Main' }],
    (key, r)=> {
      L('[renderUmbrellaTab] tab=', key, 'rowKeys=', Object.keys(r||{}), 'sample=', { name: r?.name, id: r?.id });
      const u = r || {};
      return html(`
        <div class="form" id="tab-main">
          ${input('name','Name', u.name)}
          ${input('remittance_email','Remittance email', u.remittance_email, 'email')}
          ${input('bank_name','Bank', u.bank_name)}
          ${input('sort_code','Sort code', u.sort_code)}
          ${input('account_number','Account number', u.account_number)}

          <div class="row">
            <label>Company registration number</label>
            <div class="controls">
              <input class="input"
                     name="company_number"
                     value="${escapeHtml(u.company_number || '')}">
            </div>
          </div>

          <div class="row">
            <label>Address</label>
            <div class="controls">
              <div class="grid-2">
                <input class="input"
                       name="address_line1"
                       placeholder="Address line 1"
                       value="${escapeHtml(u.address_line1 || '')}">
                <input class="input"
                       name="address_line2"
                       placeholder="Address line 2"
                       value="${escapeHtml(u.address_line2 || '')}">
                <input class="input"
                       name="address_line3"
                       placeholder="Address line 3"
                       value="${escapeHtml(u.address_line3 || '')}">
                <input class="input"
                       name="town_city"
                       placeholder="City / Town"
                       value="${escapeHtml(u.town_city || '')}">
                <input class="input"
                       name="county"
                       placeholder="County"
                       value="${escapeHtml(u.county || '')}">
                <div class="split">
                  <input class="input"
                         name="postcode"
                         placeholder="Postcode"
                         value="${escapeHtml(u.postcode || '')}">
                  <button type="button"
                          class="btn mini"
                          data-act="umbrella-postcode-lookup"
                          title="Lookup by postcode">
                    Lookup
                  </button>
                </div>
                <input class="input"
                       name="country"
                       placeholder="Country"
                       value="${escapeHtml(u.country || '')}">
              </div>
            </div>
          </div>

          ${select('vat_chargeable','VAT chargeable', (u.vat_chargeable ? 'Yes' : 'No'), ['Yes','No'])}
          ${select('enabled','Enabled', (u.enabled === false) ? 'No' : 'Yes', ['Yes','No'])}
        </div>
      `);
    },
    async ()=> {
      L('[onSave] begin', { dataId: window.modalCtx?.data?.id, forId: window.modalCtx?.formState?.__forId });

      const fs = window.modalCtx.formState || { __forId: null, main:{} };
      const sameRecord = (!!window.modalCtx.data?.id && fs.__forId === window.modalCtx.data.id) ||
                         (!window.modalCtx.data?.id && fs.__forId == null);

      const staged = sameRecord ? (fs.main || {}) : {};
      const live   = collectForm('#tab-main');
      const payload = { ...staged, ...live };

      L('[onSave] collected', { sameRecord, stagedKeys: Object.keys(staged||{}), liveKeys: Object.keys(live||{}) });

      if (typeof payload.vat_chargeable !== 'boolean') {
        payload.vat_chargeable = (payload.vat_chargeable === 'Yes' || payload.vat_chargeable === 'true');
      }
      if (typeof payload.enabled !== 'boolean') {
        payload.enabled = (payload.enabled === 'Yes' || payload.enabled === 'true');
      }

      for (const k of Object.keys(payload)) {
        if (payload[k] === '') delete payload[k];
      }

      const idForUpdate = window.modalCtx?.data?.id || full?.id || null;
      L('[onSave] upsertUmbrella', { idForUpdate, payloadKeys: Object.keys(payload||{}) });
      const saved = await upsertUmbrella(payload, idForUpdate).catch(err => { E('upsertUmbrella failed', err); return null; });
      const umbrellaId = idForUpdate || (saved && saved.id);
      L('[onSave] saved', { ok: !!saved, umbrellaId, savedKeys: Object.keys(saved||{}) });
      if (!umbrellaId) { alert('Failed to save umbrella'); return { ok:false }; }

      window.modalCtx.data      = { ...(window.modalCtx.data || {}), ...(saved || {}), id: umbrellaId };
      window.modalCtx.formState = { __forId: umbrellaId, main: {} };

      if (!seedId && umbrellaId) window.__pendingFocus = { section: 'umbrellas', id: umbrellaId };

      L('[onSave] final window.modalCtx', {
        dataId: window.modalCtx.data?.id,
        dataKeys: Object.keys(window.modalCtx.data||{})
      });

      return { ok: true, saved: window.modalCtx.data };
    },
    full?.id,
    // onReturn: (re)bind address + company number to model + postcode lookup
    () => {
      try {
        const container = document.getElementById('tab-main');
        if (!container) return;
        const model = buildUmbrellaDetailsModel(window.modalCtx?.data || {});
        window.modalCtx.umbrellaModel = model;
        bindUmbrellaAddressEvents(container, model);
      } catch (e) {
        W('bindUmbrellaAddressEvents failed', e);
      }
    }
  );

  // (Umbrella has no heavy post-paint preloads in your snippet; if you add any later,
  // keep them here, after showModal, and guard with token/id like in openClient.)
}


// ---- Audit (Outbox)
function openAuditItem(row){
  const body = html(`
    <div class="form">
      ${readonly('Type', row.type)}
      ${readonly('To', row.to)}
      ${readonly('Subject', row.subject)}
      ${readonly('Status', row.status)}
      <div class="row" style="grid-column:1/-1"><label>Last error</label><textarea readonly>${row.last_error || ''}</textarea></div>
    </div>
  `);
  showModal('Outbox', [{key:'v',label:'View'}], ()=>body, async ()=>{ closeModal(); }, row?.id);
  const act = byId('modalActions');
  const retry = document.createElement('button'); retry.textContent='Retry send';
  retry.onclick = async ()=>{ await retryOutbox(row.id); alert('Retry queued'); }
  act.insertBefore(retry, byId('btnSave'));
}

// ---- Settings (global defaults)
async function renderSettingsPanel(content){
  // Load settings with a visible error if the call fails
  let s;
  try {
    s = await getSettings(); // unwraps {settings:{...}} → {...}
  } catch (e) {
    content.innerHTML = `
      <div class="tabc">
        <div class="error">Couldn’t load settings: ${e?.message || 'Unknown error'}</div>
      </div>`;
    return;
  }

  // Establish mode + snapshot for Cancel/Discard semantics
  let mode = 'view';            // 'view' | 'edit' | 'saving'
  let dirty = false;            // enable Save only when true
  let snapshot = JSON.parse(JSON.stringify(s)); // original values to restore on Discard

  // Prefer the new key; fall back to legacy if needed (still GLOBAL-only)
  const erniValue = (s.employers_ni_pct ?? s.erni_pct ?? 0);

  // Render panel shell with explicit Edit / Save / Cancel buttons
  content.innerHTML = `
    <div class="tabc">
      <div class="form" id="settingsForm">
        ${input('timezone_id','Timezone', s.timezone_id || 'Europe/London')}
        ${input('day_start','Day start', s.day_start || '06:00')}
        ${input('day_end','Day end', s.day_end || '20:00')}
        ${input('night_start','Night start', s.night_start || '20:00')}
        ${input('night_end','Night end', s.night_end || '06:00')}
        ${select('bh_source','Bank Holidays source', s.bh_source || 'MANUAL', ['MANUAL','FEED'])}
        <div class="row" style="grid-column:1/-1">
          <label>Bank Holidays list (JSON dates)</label>
          <textarea name="bh_list">${JSON.stringify(s.bh_list || [], null, 2)}</textarea>
        </div>
        ${input('bh_feed_url','BH feed URL', s.bh_feed_url || '')}
        ${input('vat_rate_pct','VAT %', s.vat_rate_pct ?? 20, 'number')}
        ${input('holiday_pay_pct','Holiday pay %', s.holiday_pay_pct ?? 0, 'number')}
        ${input('erni_pct','ERNI %', erniValue, 'number')}
        ${select('apply_holiday_to','Apply holiday to', s.apply_holiday_to || 'PAYE_ONLY', ['PAYE_ONLY','ALL','NONE'])}
        ${select('apply_erni_to','Apply ERNI to', s.apply_erni_to || 'PAYE_ONLY', ['PAYE_ONLY','ALL','NONE'])}
        <div class="row" style="grid-column:1/-1">
          <label>Margin includes (JSON)</label>
          <textarea name="margin_includes">${JSON.stringify(s.margin_includes || {}, null, 2)}</textarea>
        </div>

        <div class="row">
          <label>Effective from (DD/MM/YYYY)</label>
          <input type="text" name="effective_from" id="settings_effective_from" placeholder="DD/MM/YYYY"
                 value="${s.effective_from ? formatIsoToUk(s.effective_from) : ''}" />
        </div>
      </div>

      <div class="actions">
        <span class="hint" id="settingsHint"></span>
        <div class="spacer"></div>
        <button id="btnEditSettings">Edit</button>
        <button id="btnCancelSettings">Close</button>
        <button id="btnSaveSettings" class="primary" disabled>Save</button>
      </div>
    </div>
  `;

  // Elements
  const formEl   = byId('settingsForm');
  const effInput = byId('settings_effective_from');
  const btnEdit  = byId('btnEditSettings');
  const btnSave  = byId('btnSaveSettings');
  const btnCancel= byId('btnCancelSettings');
  const hintEl   = byId('settingsHint');

  // Attach date picker
  if (effInput) attachUkDatePicker(effInput);

  // Helpers for view/edit toggling
  function setReadOnly(ro) {
    formEl.querySelectorAll('input, select, textarea').forEach(el => {
      el.disabled = !!ro;
      el.readOnly = !!ro && el.tagName === 'INPUT';
    });
  }
  function repaintButtons() {
    // Secondary button label: Cancel (edit & not dirty) / Discard (edit & dirty) / Close (view)
    if (mode === 'view') {
      btnCancel.textContent = 'Close';
      hintEl.textContent = '';
      btnEdit.style.display = '';
      btnSave.style.display = 'none';
    } else if (mode === 'edit') {
      btnCancel.textContent = dirty ? 'Discard' : 'Cancel';
      hintEl.textContent = dirty ? 'You have unsaved changes' : '';
      btnEdit.style.display = 'none';
      btnSave.style.display = '';
      btnSave.disabled = !dirty;
    } else if (mode === 'saving') {
      btnCancel.textContent = 'Saving…';
      btnEdit.style.display = 'none';
      btnSave.style.display = '';
      btnSave.disabled = true;
    }
  }
  function toView() { mode = 'view'; dirty = false; setReadOnly(true);  repaintButtons(); }
  function toEdit() { mode = 'edit'; dirty = false; setReadOnly(false); repaintButtons(); }

  function refillFrom(obj) {
    // Reset form inputs from a settings object
    const map = new Map([...formEl.querySelectorAll('input,select,textarea')].map(el => [el.name, el]));
    if (map.has('timezone_id')) map.get('timezone_id').value = obj.timezone_id || 'Europe/London';
    if (map.has('day_start'))   map.get('day_start').value   = obj.day_start || '06:00';
    if (map.has('day_end'))     map.get('day_end').value     = obj.day_end || '20:00';
    if (map.has('night_start')) map.get('night_start').value = obj.night_start || '20:00';
    if (map.has('night_end'))   map.get('night_end').value   = obj.night_end || '06:00';
    if (map.has('bh_source'))   map.get('bh_source').value   = obj.bh_source || 'MANUAL';
    if (map.has('bh_list'))     map.get('bh_list').value     = JSON.stringify(obj.bh_list || [], null, 2);
    if (map.has('bh_feed_url')) map.get('bh_feed_url').value = obj.bh_feed_url || '';
    if (map.has('vat_rate_pct'))map.get('vat_rate_pct').value= obj.vat_rate_pct ?? 20;
    if (map.has('holiday_pay_pct')) map.get('holiday_pay_pct').value = obj.holiday_pay_pct ?? 0;
    if (map.has('erni_pct'))    map.get('erni_pct').value    = (obj.employers_ni_pct ?? obj.erni_pct ?? 0);
    if (map.has('apply_holiday_to')) map.get('apply_holiday_to').value = obj.apply_holiday_to || 'PAYE_ONLY';
    if (map.has('apply_erni_to'))    map.get('apply_erni_to').value    = obj.apply_erni_to || 'PAYE_ONLY';
    if (map.has('margin_includes'))  map.get('margin_includes').value  = JSON.stringify(obj.margin_includes || {}, null, 2);
    if (map.has('effective_from'))   map.get('effective_from').value   = obj.effective_from ? formatIsoToUk(obj.effective_from) : '';
  }

  // Initial view-only mode
  setReadOnly(true);
  repaintButtons();

  // Dirty tracking (only in edit mode)
  const onDirty = (e) => { if (mode !== 'edit') return; dirty = true; repaintButtons(); };
  formEl.addEventListener('input', onDirty, true);
  formEl.addEventListener('change', onDirty, true);

  // Edit
  btnEdit.onclick = () => {
    if (mode !== 'view') return;
    snapshot = JSON.parse(JSON.stringify(s)); // capture original
    toEdit();
  };

  // Cancel / Discard / Close
  btnCancel.onclick = () => {
    if (mode === 'edit') {
      if (!dirty) { toView(); return; }
      const ok = window.confirm('Discard changes and return to view?');
      if (!ok) return;
      s = JSON.parse(JSON.stringify(snapshot));
      refillFrom(s);
      toView();
      return;
    }
    // mode === 'view' → Close the settings panel view (navigate away)
    // Keep behaviour consistent with the rest of the app: go back to the main list.
    currentSection = 'candidates';
    renderAll();
  };

  // Save
  btnSave.onclick = async () => {
    if (mode !== 'edit' || !dirty) return;
    mode = 'saving'; repaintButtons();

    const payload = collectForm('#settingsForm', true);
    if (payload.effective_from) {
      const iso = parseUkDateToIso(payload.effective_from);
      if (!iso) { alert('Invalid Effective from date'); mode='edit'; repaintButtons(); return; }
      payload.effective_from = iso;
    }

    try {
      await saveSettings(payload);
      s = { ...s, ...payload };           // Update live settings + snapshot
      snapshot = JSON.parse(JSON.stringify(s));
      toView();
      hintEl.textContent = 'Saved.';
      setTimeout(()=> { if (hintEl.textContent === 'Saved.') hintEl.textContent=''; }, 1500);
    } catch (e) {
      mode = 'edit'; repaintButtons();
      alert('Save failed: ' + (e?.message || 'Unknown error'));
    }
  };
}

// ===== Generic modal plumbing =====

// =============================== showModal (FIXED) ===============================
// ==== FIXED MODAL FRAMEWORK: close only on explicit success from onSave ====
// ==== CHILD MODAL (CANDIDATE RATE) — throw on errors; return true on success ====



// =============================== closeModal (kept) ===============================
// ================== FRONTEND: closeModal (UPDATED to refresh if pending focus) ==================
function closeModal(){
  if (!window.__modalStack || !window.__modalStack.length) {
    // nothing to close; ensure overlay hidden and geometry clean
    discardAllModalsAndState();
    return;
  }

  // Sanitize geometry before changing frames
  const m = byId('modal');
  if (m) {
    m.style.position = '';
    m.style.left = '';
    m.style.top = '';
    m.style.right = '';
    m.style.bottom = '';
    m.style.transform = '';
    m.classList.remove('dragging');
  }
  document.onmousemove = null;
  document.onmouseup   = null;

  const closing = window.__modalStack.pop();
  if (closing) {
    // Detach per-frame listeners
    if (closing._detachDirty)  { try { closing._detachDirty();  } catch(_) {} closing._detachDirty  = null; }
    if (closing._detachGlobal) { try { closing._detachGlobal(); } catch(_) {} closing._detachGlobal = null; }
  }

  if (window.__modalStack.length > 0) {
    const parent = window.__modalStack[window.__modalStack.length - 1];
    byId('modalBack').style.display = 'flex';

    // Rebuild tabs
    const tabsEl = byId('modalTabs'); tabsEl.innerHTML = '';
    (parent.tabs || []).forEach((t) => {
      const b = document.createElement('button');
      b.textContent = t.label;
      if (t.key === parent.currentTabKey) b.classList.add('active');
      b.onclick = () => {
        tabsEl.querySelectorAll('button').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        parent.setTab(t.key);
      };
      tabsEl.appendChild(b);
    });

    // Re-show parent current tab
    if (parent.currentTabKey) parent.setTab(parent.currentTabKey);
    else if (parent.tabs && parent.tabs[0]) parent.setTab(parent.tabs[0].key);
  } else {
    // last frame closed -> full teardown so nothing lingers
    discardAllModalsAndState();

    // If a pending focus token exists, refresh the summary now so it can jump & highlight
    if (window.__pendingFocus) {
      try { renderAll(); } catch (e) { console.error('refresh after modal close failed', e); }
    }
  }
}

// === Helpers for modal mode & interactivity ===
function _currentFrame() {
  const stk = window.__modalStack || [];
  return stk[stk.length - 1] || null;
}
function _parentFrame() {
  const stk = window.__modalStack || [];
  return stk.length >= 2 ? stk[stk.length - 2] : null;
}
function _setFormReadOnly(root, ro) {
  if (!root) return;
  root.querySelectorAll('input, select, textarea, button').forEach(el => {
    const isDisplayOnly = el.id === 'tms_ref_display' || el.id === 'cli_ref_display';
    if (el.type === 'button') {
      // Buttons are generally disabled only when parent is view/child not editable
      if (ro && !isDisplayOnly) el.disabled = true;
      else if (!isDisplayOnly) el.disabled = false;
      return;
    }
    if (isDisplayOnly) {
      el.setAttribute('disabled', 'true');
      el.setAttribute('readonly', 'true');
      return;
    }
    if (ro) { el.setAttribute('disabled','true'); el.setAttribute('readonly','true'); }
    else    { el.removeAttribute('disabled'); el.removeAttribute('readonly'); }
  });
}
function _setFrameMode(frame, mode) {
  // mode: 'create' | 'view' | 'edit' | 'saving'
  frame.mode = mode;
  // read-only if view or saving
  const readOnly = (mode === 'view' || mode === 'saving');
  _setFormReadOnly(byId('modalBody'), readOnly);
  // buttons update
  if (typeof frame._updateButtons === 'function') frame._updateButtons();
}


// ===== Small helpers (fixed attribute serialization + HTML escaping) =====
const html = (s)=> s;

const _esc = (v) => String(v)
  .replace(/&/g,'&amp;')
  .replace(/</g,'&lt;')
  .replace(/>/g,'&gt;')
  .replace(/"/g,'&quot;')
  .replace(/'/g,'&#39;');

const _attrStr = (extra) => {
  if (!extra) return '';
  if (typeof extra === 'string') {
    const t = extra.trim();
    return t ? (' ' + t) : '';
  }
  if (typeof extra !== 'object') return '';
  let out = '';
  for (const [k, v] of Object.entries(extra)) {
    if (v === false || v == null) continue;      // skip false/null/undefined
    if (v === true) { out += ` ${k}`; continue; } // boolean attribute
    out += ` ${k}="${_esc(v)}"`;                  // key="value"
  }
  return out;
};

const input = (name, label, val = '', type = 'text', extra = '') => {
  const attrs = _attrStr(extra);
  const value = (val == null ? '' : val);
  return `<div class="row"><label>${_esc(label)}</label><input name="${_esc(name)}" type="${_esc(type)}" value="${_esc(value)}"${attrs}/></div>`;
};

const select = (name, label, val, options = [], extra = {}) => {
  const attrs = _attrStr(extra);
  const opts = options.map(o => {
    const selected = String(o) === String(val) ? ' selected' : '';
    return `<option${selected}>${_esc(o)}</option>`;
  }).join('');
  return `<div class="row"><label>${_esc(label)}</label><select name="${_esc(name)}"${attrs}>${opts}</select></div>`;
};

const readonly = (label, value) =>
  `<div class="row"><label>${_esc(label)}</label><input value="${_esc(value ?? '')}" readonly/></div>`;


/**
 * Safer form collector:
 * - Skips elements with no name
 * - Skips disabled/readonly or data-no-collect
 * - Converts Yes/No selects to booleans
 */
function collectForm(sel, jsonTry=false){
  const root = document.querySelector(sel);
  if (!root) return {};

  const out = {};
  root.querySelectorAll('input,select,textarea').forEach(el=>{
    if (!el.name) return;
    if (el.disabled || el.readOnly || el.dataset.noCollect === 'true') return;

    const k = el.name;
    let v;

    if (el.type === 'checkbox') {
      v = el.checked ? 'on' : '';
    } else if (el.type === 'radio') {
      if (!el.checked) return;
      v = el.value;
    } else if (el.type === 'number') {
      v = (el.value === '' ? '' : Number(el.value));
    } else {
      v = el.value;
    }

    if (el.tagName === 'SELECT' && (v === 'Yes' || v === 'No')) v = (v === 'Yes');

    if (jsonTry && (k === 'bh_list' || k === 'margin_includes')) {
      try { v = JSON.parse(v || (k === 'bh_list' ? '[]' : '{}')); } catch {}
    }

    out[k] = v;
  });
  return out;
}


// ================= NEW: Job titles client-side cache ==================
window.__jobTitlesCache = window.__jobTitlesCache || {
  items: [],
  byId: {},
  roots: [],
  loadedAt: 0
};

function normaliseJobTitles(items) {
  const byId = {};
  const roots = [];
  (items || []).forEach((r) => {
    if (!r || !r.id) return;
    const copy = { ...r, children: [] };
    byId[r.id] = copy;
  });
  Object.values(byId).forEach((node) => {
    if (node.parent_id && byId[node.parent_id]) {
      byId[node.parent_id].children.push(node);
    } else {
      roots.push(node);
    }
  });
  return { items, byId, roots };
}

// =============== NEW: loadJobTitlesTree (backend → cache) ===============
// =============== NEW: loadJobTitlesTree (backend → cache) ===============
async function loadJobTitlesTree(force = false, activeOnly = true) {
  // Ensure cache shape
  window.__jobTitlesCache = window.__jobTitlesCache || {
    items: [],
    byId: {},
    roots: [],
    loadedAt: 0
  };
  const C = window.__jobTitlesCache;
  const now = Date.now();

  // Reuse cache only for activeOnly=true
  if (
    activeOnly &&
    !force &&
    C.items &&
    C.items.length &&
    now - C.loadedAt < 60_000
  ) {
    return C;
  }

  const url = API(`/api/job-titles?activeOnly=${activeOnly ? 'true' : 'false'}`);
  const res = await authFetch(url);
  if (!res.ok) {
    console.error('[JOB_TITLES] list failed', res.status);
    throw new Error('Failed to load job titles');
  }
  const data = (await res.json().catch(() => ({}))) || {};
  const items = data.items || [];
  const norm = normaliseJobTitles(items);

  // Only mutate global cache when we’re in activeOnly mode
  if (activeOnly) {
    C.items = items;
    C.byId = norm.byId;
    C.roots = norm.roots;
    C.loadedAt = now;
    return C;
  }

  // For Settings (activeOnly=false) return a separate snapshot
  return {
    items,
    byId: norm.byId,
    roots: norm.roots,
    loadedAt: now
  };
}


// =============== NEW: buildJobTitlePathLabels ==========================
function buildJobTitlePathLabels(jobTitleId) {
  const C = window.__jobTitlesCache || {};
  const byId = C.byId || {};
  const chain = [];
  let cur = byId[jobTitleId];
  while (cur && chain.length < 16) {
    chain.push(cur.label || '');
    cur = cur.parent_id ? byId[cur.parent_id] : null;
  }
  return chain.reverse().filter(Boolean);
}

// =============== NEW: Job titles API helpers ===========================
async function apiCreateJobTitle(payload) {
  const url = API('/api/job-titles');
  const res = await authFetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {})
  });
  if (!res.ok) throw new Error('Failed to create job title');
  const data = (await res.json().catch(() => ({}))) || {};
  return data.item || null;
}

async function apiUpdateJobTitle(id, patch) {
  const url = API(`/api/job-titles/${encodeURIComponent(id)}`);
  const res = await authFetch(url, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(patch || {})
  });
  if (!res.ok) throw new Error('Failed to update job title');
  const data = (await res.json().catch(() => ({}))) || {};
  return data.item || null;
}

async function apiDeleteJobTitle(id) {
  const url = API(`/api/job-titles/${encodeURIComponent(id)}`);
  const res = await authFetch(url, { method: 'DELETE' });
  if (!res.ok) throw new Error('Failed to delete job title');
  const data = (await res.json().catch(() => ({}))) || {};
  return data;
}

function openJobTitleSettingsModal() {
  const S = {
    loading: false,
    error: '',
    items: [],
    byId: {},
    roots: [],
    selectedId: null,
    editing: null, // { id|null, parent_id|null, label, is_role, requires_prof_reg, prof_reg_type, active, isNew }
    collapsed: {}  // { [id]: true } for collapsed nodes
  };

  const profTypes = ['NMC', 'GMC', 'HCPC'];

  const makeEditingFromNode = (node) => {
    if (!node) return null;
    return {
      id: node.id,
      parent_id: node.parent_id || null,
      label: node.label || '',
      is_role: !!node.is_role,
      requires_prof_reg: !!node.requires_prof_reg,
      prof_reg_type: node.prof_reg_type || '',
      active: node.active !== false,
      isNew: false
    };
  };

  const makeEditingNew = (parentId) => ({
    id: null,
    parent_id: parentId || null,
    label: '',
    is_role: false, // default to Category
    requires_prof_reg: false,
    prof_reg_type: '',
    active: true,
    isNew: true
  });

  const renderTree = (nodes, level) => {
    if (!nodes || !nodes.length) return '';
    const pad = level * 16;
    return nodes
      .map((n) => {
        const isSelected =
          S.selectedId === n.id || (!S.selectedId && S.editing && S.editing.id === n.id);
        const kindLabel = n.is_role ? 'Role' : 'Category';
        const regBadge =
          n.is_role && n.requires_prof_reg
            ? `<span class="pill mini" style="margin-left:4px">${n.prof_reg_type || 'Reg'}</span>`
            : '';
        const inactiveTag =
          n.active === false
            ? `<span class="mini" style="margin-left:4px;opacity:.7">(inactive)</span>`
            : '';

        const hasChildren = Array.isArray(n.children) && n.children.length > 0;
        const isCollapsed = !!S.collapsed[n.id];

        const toggleHtml = hasChildren
          ? `<button type="button" class="btn mini" data-act="toggle" data-id="${n.id}" style="margin-right:4px;width:24px;text-align:center;padding:2px 0">${isCollapsed ? '+' : '−'}</button>`
          : `<span style="display:inline-block;width:24px"></span>`;

        const childrenHtml = !isCollapsed ? renderTree(n.children || [], level + 1) : '';

        return `
          <div class="jt-node${isSelected ? ' jt-node-active' : ''}" data-id="${n.id}"
               style="padding:4px 6px 4px ${pad + 6}px;cursor:pointer;border-radius:6px;">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:6px">
              <div style="display:flex;align-items:center;gap:4px">
                ${toggleHtml}
                <span class="jt-label" style="font-weight:600">${escapeHtml(n.label || '')}</span>
                <span class="mini" style="margin-left:6px;opacity:.75">${kindLabel}</span>
                ${regBadge}
                ${inactiveTag}
              </div>
              <div class="mini">
                <button type="button" class="btn mini" data-act="add-child" data-id="${n.id}">+ Child</button>
              </div>
            </div>
          </div>
          ${childrenHtml}
        `;
      })
      .join('');
  };

  const renderDetailsPanel = () => {
    const e = S.editing;
    if (!e) {
      return `
        <div class="hint" style="padding:8px">
          Select a node on the left, or click <strong>Add family</strong> to create a new top-level group.
        </div>
      `;
    }

    const node = e.id ? S.byId[e.id] : null;

    const hasRoleDescendants = (n) => {
      if (!n || !Array.isArray(n.children)) return false;
      const stack = [...n.children];
      let guard = 0;
      while (stack.length && guard++ < 1024) {
        const cur = stack.pop();
        if (!cur) continue;
        if (cur.is_role) return true;
        if (Array.isArray(cur.children) && cur.children.length) {
          stack.push(...cur.children);
        }
      }
      return false;
    };

    const depth = node ? (Number(node.depth) || 0) : (e.parent_id ? 1 : 0);
    const hasRoleDesc = node && hasRoleDescendants(node);

    // Determine title based on type + depth
    let title;
    if (e.isNew) {
      if (!e.parent_id) {
        title = 'New Family';
      } else if (!e.is_role) {
        title = 'New Sub Family';
      } else {
        title = 'New Role';
      }
    } else {
      if (e.is_role) {
        title = 'Edit Job Title';
      } else if (depth > 0 || e.parent_id) {
        title = 'Edit Sub Family Name';
      } else {
        title = 'Edit Family Name';
      }
    }

    // Category Type radios
    const nodeTypeGroupChecked = !e.is_role ? 'checked' : '';
    const nodeTypeRoleChecked = e.is_role ? 'checked' : '';

    // Show Role radio?
    let showRoleRadio = true;
    if (e.isNew && !e.parent_id) {
      // Add Family → cannot create a Role at top level
      showRoleRadio = false;
    } else if (!e.isNew && !e.is_role && hasRoleDesc) {
      // Existing Family/Subfamily with descendant Roles → cannot switch to Role
      showRoleRadio = false;
    }

    const requiresChecked = !!(e.is_role && e.requires_prof_reg);
    const regBlockStyle = e.is_role ? '' : 'display:none';

    // Parent path using local state (families > subfamilies)
    const parentPath = (() => {
      if (!e.parent_id) return '(top-level family)';
      const chain = [];
      let cur = S.byId[e.parent_id];
      let guard = 0;
      while (cur && guard++ < 16) {
        chain.push(cur.label || '');
        cur = cur.parent_id ? S.byId[cur.parent_id] : null;
      }
      return chain.length ? chain.reverse().join(' > ') : '(no parent)';
    })();

    const regOptions = profTypes
      .map((t) => `<option value="${t}" ${e.prof_reg_type === t ? 'selected' : ''}>${t}</option>`)
      .join('');

    const activeChecked = e.active ? 'checked' : '';

    return `
      <form id="jt_details_form" class="form" autocomplete="off">
        <div class="row" style="grid-column:1/-1">
          <div class="mini" style="opacity:.85">${title}</div>
        </div>

        <div class="row">
          <label>Label</label>
          <div class="controls">
            <input type="text" name="label" value="${escapeHtml(e.label || '')}" required />
            <div id="jt_cat_vis_row" style="margin-top:6px;display:flex;gap:12px;align-items:center;flex-wrap:wrap">
              <label class="inline">
                <input type="radio" name="node_type" value="group" ${nodeTypeGroupChecked} />
                <span>Category</span>
              </label>
              ${
                showRoleRadio
                  ? `
              <label class="inline">
                <input type="radio" name="node_type" value="role" ${nodeTypeRoleChecked} />
                <span>Role</span>
              </label>`
                  : ''
              }
              <label class="inline">
                <input type="checkbox" name="active" ${activeChecked} />
                <span>Visible to users</span>
              </label>
            </div>
          </div>
        </div>

        <div class="row">
          <label>Parent</label>
          <div class="mini" style="padding:6px 8px;border-radius:8px;background:#020617;border:1px solid var(--line)">
            ${escapeHtml(parentPath)}
          </div>
        </div>

        <div class="row" data-block="reg" style="${regBlockStyle}">
          <label class="inline">
            <input type="checkbox" name="requires_prof_reg" ${requiresChecked ? 'checked' : ''} />
            <span>Requires professional registration</span>
          </label>
          <select name="prof_reg_type" style="margin-top:6px;${requiresChecked ? '' : 'display:none'}">
            <option value="">-- Select type --</option>
            ${regOptions}
          </select>
        </div>

        <div class="row" style="grid-column:1/-1;margin-top:8px;display:flex;flex-direction:row;align-items:center;gap:6px;justify-content:flex-end">
          <button type="button" class="btn mini" style="padding:4px 8px" id="jt_btn_delete" ${(e.isNew || (node && Array.isArray(node.children) && node.children.length)) ? 'disabled' : ''}>Delete</button>
          <button type="button" class="btn mini primary" style="padding:4px 10px" id="jt_btn_save">Save</button>
        </div>
      </form>
    `;
  };

  const buildBody = () => {
    if (S.loading) {
      return html(`
        <div id="jobTitlesSettingsRoot" style="padding:10px">
          <div class="hint">Loading job titles…</div>
        </div>
      `);
    }

    const treeHtml = renderTree(S.roots || [], 0);

    return html(`
      <div id="jobTitlesSettingsRoot" style="display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,1.8fr);gap:12px;min-height:260px">
        <div>
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
            <div class="mini">Families / subfamilies / roles</div>
            <button type="button" class="btn mini" id="jt_add_root">+ Add family</button>
          </div>
          <div id="jt_tree"
               style="border:1px solid var(--line);border-radius:10px;max-height:420px;overflow:auto;padding:4px">
            ${
              treeHtml ||
              '<div class="hint" style="padding:8px">No job titles defined yet. Click <strong>Add family</strong> to create your first group.</div>'
            }
          </div>
          ${S.error ? `<div class="error" style="margin-top:8px">${escapeHtml(S.error)}</div>` : ''}
        </div>

        <div style="border:1px solid var(--line);border-radius:10px;padding:8px;min-height:240px">
          ${renderDetailsPanel()}
        </div>
      </div>
    `);
  };

  const repaint = () => {
    const bodyEl = document.getElementById('modalBody');
    if (!bodyEl) return;
    bodyEl.innerHTML = buildBody();
    wireEvents();
  };

  const showRoleUsageInCandidates = async (candidateIds) => {
    if (!Array.isArray(candidateIds) || !candidateIds.length) {
      alert('This job title is in use, but no candidate IDs were returned.');
      return;
    }
    const ok = window.confirm(
      `This role is assigned to ${candidateIds.length} candidates. Show them in the Candidates list?`
    );
    if (!ok) return;

    try {
      // Switch to Candidates section and filter by ids
      window.currentSection = 'candidates';
      window.__listState = window.__listState || {};
      const st = (window.__listState.candidates ||= {
        page: 1,
        pageSize: 50,
        total: null,
        hasMore: false,
        filters: null,
        sort: { key: null, dir: 'asc' }
      });
      st.page = 1;
      st.filters = { ...(st.filters || {}), ids: candidateIds };

      const rows = await search('candidates', st.filters);
      renderSummary(rows);

      // Close the Job Titles modal if it's open
      const btnClose = document.getElementById('btnCloseModal');
      if (btnClose) btnClose.click();
    } catch (e) {
      console.error('Failed to show candidates for job title', e);
      alert('Failed to open Candidates list for this role.');
    }
  };

  const refreshFromCache = async () => {
    S.loading = true;
    S.error = '';
    repaint();
    try {
      // Load ALL titles (including inactive) for Settings
      const cache = await loadJobTitlesTree(true, false);
      S.items = cache.items;
      S.byId = cache.byId;
      S.roots = cache.roots;

      // Seed collapsed state: collapse all non-role nodes that have children
      S.collapsed = {};
      const seedCollapsed = (nodes) => {
        if (!Array.isArray(nodes)) return;
        for (const n of nodes) {
          if (!n) continue;
          if (!n.is_role && Array.isArray(n.children) && n.children.length > 0) {
            S.collapsed[n.id] = true;
            seedCollapsed(n.children);
          }
        }
      };
      seedCollapsed(S.roots);

      // If nothing is selected, pick the first root if any
      if (!S.selectedId && S.roots.length) {
        S.selectedId = S.roots[0].id;
      }
      if (S.selectedId && S.byId[S.selectedId]) {
        S.editing = makeEditingFromNode(S.byId[S.selectedId]);
      } else {
        S.editing = null;
      }
    } catch (e) {
      console.error('[JOB_TITLES] load failed', e);
      S.error = 'Failed to load job titles';
    } finally {
      S.loading = false;
      repaint();
    }
  };

  const wireEvents = () => {
    const root = document.getElementById('jobTitlesSettingsRoot');
    if (!root) return;

    const treeBox = root.querySelector('#jt_tree');
    const addRootBtn = root.querySelector('#jt_add_root');
    const form = root.querySelector('#jt_details_form');
    const saveBtn = root.querySelector('#jt_btn_save');
    const deleteBtn = root.querySelector('#jt_btn_delete');

    if (addRootBtn) {
      addRootBtn.onclick = () => {
        S.selectedId = null;
        S.editing = makeEditingNew(null);
        repaint();
      };
    }

    if (treeBox) {
      treeBox.onclick = (e) => {
        const btn = e.target.closest('button[data-act]');
        if (btn) {
          const act = btn.getAttribute('data-act');
          const id = btn.getAttribute('data-id');
          const node = S.byId[id];
          if (!node) return;

          if (act === 'add-child') {
            S.selectedId = null;
            S.editing = makeEditingNew(node.id);
            repaint();
          } else if (act === 'toggle') {
            S.collapsed[id] = !S.collapsed[id];
            repaint();
          }
          return;
        }

        const nodeEl = e.target.closest('.jt-node[data-id]');
        if (!nodeEl) return;
        const id = nodeEl.getAttribute('data-id');
        const n = S.byId[id];
        if (!n) return;
        S.selectedId = id;
        S.editing = makeEditingFromNode(n);
        repaint();
      };
    }

    if (form && saveBtn) {
      saveBtn.onclick = async () => {
        const v = collectForm('#jt_details_form', false) || {};
        const label = (v.label || '').trim();
        if (!label) {
          alert('Label is required');
          return;
        }

        const nodeType = v.node_type === 'role' ? 'role' : 'group';
        const isRole = nodeType === 'role';

        const requiresProfReg = isRole && v.requires_prof_reg === 'on';
        const profRegType = requiresProfReg ? (v.prof_reg_type || '').trim().toUpperCase() : null;
        const active = v.active === 'on';

        if (requiresProfReg && !profRegType) {
          alert('Please choose a professional registration type (NMC / GMC / HCPC)');
          return;
        }

        const isNew = !S.editing || !S.editing.id;
        const parentId = S.editing ? S.editing.parent_id || null : null;

        try {
          let node;
          if (isNew) {
            node = await apiCreateJobTitle({
              label,
              parent_id: parentId,
              is_role: isRole,
              requires_prof_reg: requiresProfReg,
              prof_reg_type: profRegType,
              active
            });
          } else {
            node = await apiUpdateJobTitle(S.editing.id, {
              label,
              is_role: isRole,
              requires_prof_reg: requiresProfReg,
              prof_reg_type: profRegType,
              active
            });
          }

          // If backend signals that the role is in use, offer to show candidates
          if (node && node.error === 'JOB_TITLE_IN_USE' && Array.isArray(node.candidate_ids)) {
            await showRoleUsageInCandidates(node.candidate_ids);
            return;
          }

          // Refresh cache & select this node
          await refreshFromCache();
          if (node && node.id) {
            S.selectedId = node.id;
            S.editing = makeEditingFromNode(node);
            repaint();
          }
        } catch (err) {
          console.error('job title save failed', err);
          alert('Failed to save job title');
        }
      };
    }

    if (form && deleteBtn && !deleteBtn.disabled) {
      deleteBtn.onclick = async () => {
        if (!S.editing || !S.editing.id) return;
        const node = S.byId[S.editing.id];
        if (!node) return;

        if (!window.confirm(`Delete "${node.label}"?`)) return;

        try {
          const res = await apiDeleteJobTitle(node.id);

          // If backend says job title is in use, offer to show candidates
          if (res && res.error === 'JOB_TITLE_IN_USE' && Array.isArray(res.candidate_ids)) {
            await showRoleUsageInCandidates(res.candidate_ids);
            return;
          }

          await refreshFromCache();
        } catch (err) {
          console.error('job title delete failed', err);
          alert('Failed to delete job title – it may still be in use.');
        }
      };
    }

    // Category Type + registration block behaviour
    if (form) {
      const nodeTypeInputs = form.querySelectorAll('input[name="node_type"]');
      const regBlock = form.querySelector('[data-block="reg"]');
      const reqCb = form.querySelector('input[name="requires_prof_reg"]');
      const regSelect = form.querySelector('select[name="prof_reg_type"]');

      nodeTypeInputs.forEach((el) => {
        el.onchange = () => {
          const v = collectForm('#jt_details_form', false) || {};
          const isRole = v.node_type === 'role';
          if (regBlock) {
            regBlock.style.display = isRole ? '' : 'none';
          }
        };
      });

      if (reqCb && regSelect) {
        reqCb.onchange = () => {
          if (reqCb.checked) {
            regSelect.style.display = '';
          } else {
            regSelect.style.display = 'none';
            regSelect.value = '';
          }
        };
      }
    }
  };

  showModal(
    'Job Titles',
    [{ key: 'main', label: 'Job Titles' }],
    () => buildBody(),
    async () => ({ ok: true }), // All job title actions are immediate
    false,
    async () => {
      await refreshFromCache();
    },
    { kind: 'job-titles', noParentGate: false }
  );

  // Initial load after modal is mounted
  refreshFromCache().catch((e) => console.error('[JOB_TITLES] initial refresh failed', e));
}


// =============== NEW: Job Titles Settings modal (side panel) ===========
// =============== NEW: Job Titles Settings modal (side panel) ===========


function openJobTitlePickerModal(initialJobTitleId, onSelect) {
  const C = window.__jobTitlesCache || {};
  const roots = C.roots || [];
  const byId = C.byId || {};

  let selectedId = initialJobTitleId || null;
  const collapsedById = {};

  // Initial state: collapse all non-role nodes that have children
  const seedCollapsed = (nodes) => {
    if (!Array.isArray(nodes)) return;
    for (const n of nodes) {
      if (!n) continue;
      if (!n.is_role && Array.isArray(n.children) && n.children.length > 0) {
        collapsedById[n.id] = true;
        seedCollapsed(n.children);
      }
    }
  };
  seedCollapsed(roots);

  const renderOptions = (nodes, depth) => {
    if (!nodes || !nodes.length) return '';
    const pad = depth * 16;
    return nodes
      .map((n) => {
        const isRole = !!n.is_role;
        const isSelected = n.id === selectedId;
        const hasChildren = Array.isArray(n.children) && n.children.length > 0;
        const isCollapsed = !!collapsedById[n.id];

        const label = n.label || '';
        const regBadge =
          isRole && n.requires_prof_reg
            ? `<span class="pill mini" style="margin-left:4px">${n.prof_reg_type || 'Reg'}</span>`
            : '';
        const kind = isRole ? 'Role' : 'Group';

        const toggleHtml = hasChildren
          ? `<span class="mini" style="margin-right:4px">${isCollapsed ? '+' : '−'}</span>`
          : `<span style="display:inline-block;width:10px"></span>`;

        const childrenHtml =
          hasChildren && !isCollapsed ? renderOptions(n.children || [], depth + 1) : '';

        return `
          <div data-id="${n.id}"
               class="jt-pick-row${isSelected ? ' active' : ''}"
               style="padding:4px 8px;margin-left:${pad}px;cursor:pointer;display:flex;justify-content:space-between;align-items:center;border-radius:6px;">
            <div>
              ${toggleHtml}
              <strong>${escapeHtml(label)}</strong>
              <span class="mini" style="margin-left:6px;opacity:.7">${kind}</span>
            </div>
            <div>${regBadge}</div>
          </div>
          ${childrenHtml}
        `;
      })
      .join('');
  };

  const buildBody = () => {
    const node = selectedId ? byId[selectedId] : null;
    const pathLabels = node ? buildJobTitlePathLabels(selectedId) : [];

    const selectionSummary = (() => {
      if (!node) {
        return '<div class="mini">Nothing selected. Choose a <strong>Role</strong> from the left.</div>';
      }
      const isRole = !!node.is_role;
      if (!isRole) {
        return `
          <div class="mini">
            <div><strong>${escapeHtml(pathLabels.join(' > '))}</strong></div>
            <div style="margin-top:4px;color:#f97316">
              This is a <strong>Group</strong>. Please expand it and select a <strong>Role</strong> underneath.
            </div>
          </div>
        `;
      }
      const regBit = node.requires_prof_reg
        ? `Professional registration: ${node.prof_reg_type || ''} (number captured on candidate)`
        : 'No professional registration required for this role.';
      return `
        <div class="mini">
          <div><strong>${escapeHtml(pathLabels.join(' > '))}</strong></div>
          <div style="margin-top:4px">${escapeHtml(regBit)}</div>
        </div>
      `;
    })();

    return html(`
      <div id="jobTitlePickerRoot">
        <div class="hint" style="margin-bottom:8px">
          Choose <strong>Family → Subfamily → Role</strong>. Only <strong>Roles</strong> can be assigned to candidates.
        </div>

        <div style="display:grid;grid-template-columns:minmax(0,2fr) minmax(0,1.2fr);gap:10px;align-items:stretch">
          <div id="jtPickerTree"
               style="border:1px solid var(--line);border-radius:10px;max-height:360px;overflow:auto">
            ${
              renderOptions(roots, 0) ||
              '<div class="hint" style="padding:8px">No job titles defined yet. Add families and roles in Settings → Job Titles.</div>'
            }
          </div>
          <div style="border:1px solid var(--line);border-radius:10px;padding:8px;font-size:12px">
            <div style="font-weight:600;margin-bottom:4px">Selection</div>
            ${selectionSummary}
          </div>
        </div>
      </div>
    `);
  };

  const wireEvents = () => {
    const root = document.getElementById('jobTitlePickerRoot');
    if (!root) return;

    const tree = root.querySelector('#jtPickerTree');
    if (!tree || tree.__jtWired) return;
    tree.__jtWired = true;

    tree.onclick = (e) => {
      const row = e.target.closest('.jt-pick-row[data-id]');
      if (!row) return;
      const id = row.getAttribute('data-id');
      const node = byId[id];
      if (!node) return;

      if (!node.is_role) {
        // Group / Family / Subfamily – treat click as toggle expand/collapse
        const hasChildren = Array.isArray(node.children) && node.children.length > 0;
        if (!hasChildren) return;
        collapsedById[id] = !collapsedById[id];

        const bodyEl = document.getElementById('modalBody');
        if (!bodyEl) return;
        bodyEl.innerHTML = buildBody();
        const newRoot = document.getElementById('jobTitlePickerRoot');
        const newTree = newRoot && newRoot.querySelector('#jtPickerTree');
        if (newTree) newTree.__jtWired = false;
        wireEvents();
        return;
      }

      // Role – select
      selectedId = id;
      const bodyEl = document.getElementById('modalBody');
      if (!bodyEl) return;
      bodyEl.innerHTML = buildBody();
      const newRoot = document.getElementById('jobTitlePickerRoot');
      const newTree = newRoot && newRoot.querySelector('#jtPickerTree');
      if (newTree) newTree.__jtWired = false;
      wireEvents();
    };
  };

  showModal(
    'Select Job Title',
    [{ key: 'main', label: 'Job Title' }],
    () => buildBody(),
    async () => {
      const node = byId[selectedId];
      if (!node) {
        alert('Please select a job title first.');
        return { ok: false };
      }
      if (!node.is_role) {
        alert('Please select a Role (not just a Group/Category).');
        return { ok: false };
      }
      const pathLabels = buildJobTitlePathLabels(selectedId);
      if (typeof onSelect === 'function') {
        await onSelect({
          jobTitleId: selectedId,
          pathLabels,
          requiresProfReg: !!node.requires_prof_reg,
          profRegType: node.prof_reg_type || null
        });
      }
      return { ok: true };
    },
    false,
    () => {},
    { kind: 'job-title-picker', noParentGate: false }
  );

  // Wire events on initial open
  wireEvents();
}



// =============== NEW: Candidate Job Title picker =======================

// =============== NEW: applySelectedJobTitleToCandidate =================
function applySelectedJobTitleToCandidate(candidateModel, selection) {
  if (!candidateModel || !selection) return;
  candidateModel.job_title_id = selection.jobTitleId;
  candidateModel.job_title_path_display = (selection.pathLabels || []).join(' > ');
  candidateModel.prof_reg_type = selection.profRegType || null;

  if (!selection.requiresProfReg) {
    candidateModel.prof_reg_number = '';
  }
}

// =============== NEW: buildCandidateMainDetailsModel ===================
function buildCandidateMainDetailsModel(row) {
  const r = row || {};
  const model = {
    id: r.id || null,

    // Personal identifiers
    ni_number: r.ni_number || '',
    date_of_birth: r.date_of_birth || null, // ISO date string (YYYY-MM-DD)
    gender: r.gender || '',

    // Registration
    prof_reg_type: r.prof_reg_type || null,
    prof_reg_number: r.prof_reg_number || '',

    // Address
    address_line1: r.address_line1 || '',
    address_line2: r.address_line2 || '',
    address_line3: r.address_line3 || '',
    town_city: r.town_city || '',
    county: r.county || '',
    postcode: r.postcode || '',
    country: r.country || ''
  };

  // Multi job titles from backend (candidate_job_titles)
  // shape: [{ job_title_id, is_primary }, ...]
  let jobs = Array.isArray(r.job_titles)
    ? r.job_titles
        .map((jt) => ({
          job_title_id: jt.job_title_id,
          is_primary: !!jt.is_primary
        }))
        .filter((t) => t.job_title_id)
    : [];

  // Normalise:
  // - if none is primary, make the first primary
  // - if multiple are primary, keep the first as primary and clear the rest
  // - always move the primary to index 0
  if (jobs.length) {
    let primaryIdx = jobs.findIndex((t) => t.is_primary);
    if (primaryIdx === -1) {
      primaryIdx = 0;
    }

    jobs = jobs.map((t, idx) => ({
      ...t,
      is_primary: idx === primaryIdx
    }));

    if (primaryIdx !== 0) {
      const primary = jobs[primaryIdx];
      jobs.splice(primaryIdx, 1);
      jobs.unshift(primary);
    }
  }

  model.job_titles = jobs;
  return model;
}

// =============== NEW: bindCandidateMainFormEvents ======================
// =============== NEW: bindCandidateMainFormEvents ======================

function bindCandidateMainFormEvents(container, model) {
  if (!container || !model) return;

  const q = (sel) => container.querySelector(sel);

  const bind = (selector, key) => {
    const el = q(selector);
    if (!el) return;
    el.value = model[key] || '';
    el.addEventListener('input', () => {
      model[key] = el.value;
    });
  };

  // Small helper to mark current candidate frame dirty
  const markDirty = () => {
    try {
      const fr = window.__getModalFrame?.();
      if (fr && (fr.mode === 'edit' || fr.mode === 'create')) {
        fr.isDirty = true;
        fr._updateButtons?.();
      }
      window.dispatchEvent(new Event('modal-dirty'));
    } catch {}
  };

  // Normalise job_titles:
  // - drop any entries without job_title_id
  // - if none primary, first becomes primary
  // - if multiple primaries, only first stays primary
  // - primary always moved to index 0
  const normaliseJobTitles = () => {
    let items = Array.isArray(model.job_titles) ? model.job_titles.slice() : [];
    items = items.filter((t) => t && t.job_title_id);

    if (!items.length) {
      model.job_titles = [];
      return;
    }

    let primaryIdx = items.findIndex((t) => t.is_primary);
    if (primaryIdx === -1) {
      primaryIdx = 0;
    }

    items = items.map((t, idx) => ({
      ...t,
      is_primary: idx === primaryIdx
    }));

    if (primaryIdx !== 0) {
      const primary = items[primaryIdx];
      items.splice(primaryIdx, 1);
      items.unshift(primary);
    }

    model.job_titles = items;
  };

  // NI
  bind('input[name="ni_number"]', 'ni_number');

  // DOB (model holds ISO)
  const dobEl = q('input[name="date_of_birth"]');
  if (dobEl) {
    dobEl.value = model.date_of_birth
      ? (typeof formatIsoToUk === 'function' ? formatIsoToUk(model.date_of_birth) : model.date_of_birth)
      : '';
    dobEl.addEventListener('change', () => {
      const v = dobEl.value.trim();
      if (!v) {
        model.date_of_birth = null;
        markDirty();
        return;
      }
      if (typeof parseUkDateToIso === 'function') {
        const iso = parseUkDateToIso(v);
        model.date_of_birth = iso || null;
      } else {
        model.date_of_birth = v;
      }
      markDirty();
    });

    if (typeof attachUkDatePicker === 'function') {
      attachUkDatePicker(dobEl);
    }
  }

  // Gender
  const genderEl = q('select[name="gender"]');
  if (genderEl) {
    genderEl.value = model.gender || '';
    genderEl.addEventListener('change', () => {
      model.gender = genderEl.value || '';
      markDirty();
    });
  }

  // Professional registration number
  const profEl = q('input[name="prof_reg_number"]');
  if (profEl) {
    profEl.value = model.prof_reg_number || '';
    profEl.addEventListener('input', () => {
      model.prof_reg_number = profEl.value || '';
      markDirty();
    });
  }

  // Address fields
  const addrKeys = [
    'address_line1',
    'address_line2',
    'address_line3',
    'town_city',
    'county',
    'postcode',
    'country'
  ];
  addrKeys.forEach((k) => bind(`input[name="${k}"]`, k));

  // Helper to render current job_titles list
  const jobTitlesHost = q('#jobTitlesList');

  const renderJobTitlesList = () => {
    if (!jobTitlesHost) return;

    // Normalise before rendering
    normaliseJobTitles();

    const C = window.__jobTitlesCache || {};
    const byId = C.byId || {};
    const items = Array.isArray(model.job_titles) ? model.job_titles : [];

    // Check if we are in edit/create mode (for bins + context menu)
    const fr = window.__getModalFrame?.();
    const canEdit =
      !!fr &&
      fr.entity === 'candidates' &&
      fr.currentTabKey === 'main' &&
      (fr.mode === 'edit' || fr.mode === 'create');

    if (!items.length) {
      jobTitlesHost.innerHTML = `<div class="hint">No job titles selected yet.</div>`;
    } else {
      jobTitlesHost.innerHTML = items
        .map((t) => {
          const node = byId[t.job_title_id];
          const isPrimary = !!t.is_primary;
          // Just the leaf label (no full tree)
          const label = node ? (node.label || '') : String(t.job_title_id || '');
          const regBadge =
            node && node.requires_prof_reg
              ? `<span class="pill mini" style="margin-left:4px">${node.prof_reg_type || 'Reg'}</span>`
              : '';

          const pillBase =
            'display:inline-flex;align-items:center;gap:6px;margin:2px 4px 0 0;padding:2px 6px;border-radius:999px;';
          const pillStyle = isPrimary
            ? `${pillBase}border:1px solid var(--ok,#22c55e);background:rgba(34,197,94,0.08);`
            : `${pillBase}border:1px solid var(--line);`;

          const labelHtml = isPrimary
            ? `<span style="color:var(--ok,#22c55e);font-weight:600">${escapeHtml(label)}</span>`
            : `<span>${escapeHtml(label)}</span>`;

          const primaryTag = isPrimary
            ? `<span class="mini" style="margin-left:4px;opacity:.85;color:var(--ok,#22c55e)">Primary</span>`
            : '';

          const binHtml = canEdit
            ? `
              <button type="button"
                      class="btn mini"
                      data-act="remove-job-title"
                      data-id="${t.job_title_id}"
                      title="Remove">
                🗑
              </button>`
            : '';

          return `
            <div class="pill"
                 data-role-id="${t.job_title_id}"
                 style="${pillStyle}">
              ${labelHtml}
              ${primaryTag}
              ${regBadge}
              ${binHtml}
            </div>
          `;
        })
        .join('');
    }

    // Recompute whether any role requires registration
    const regWrapper = q('[data-block="prof_reg"]');
    const regLabel = q('[data-field="prof_reg_label"]');
    const anyRequires = items.some((t) => {
      const node = byId[t.job_title_id];
      return node && node.requires_prof_reg;
    });

    if (regWrapper) {
      if (anyRequires) {
        regWrapper.style.display = '';
        if (regLabel) {
          const type = model.prof_reg_type || '';
          regLabel.textContent = type ? `${type} Number` : 'Registration Number';
        }
      } else {
        regWrapper.style.display = 'none';
      }
    }
  };

  // Initial cache + list render
  (async () => {
    try {
      await loadJobTitlesTree(false);
    } catch {}
    renderJobTitlesList();
  })();

  // Wire bins (remove job title)
  if (jobTitlesHost && !jobTitlesHost.__wiredClick) {
    jobTitlesHost.__wiredClick = true;
    jobTitlesHost.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-act="remove-job-title"]');
      if (!btn) return;
      const id = btn.getAttribute('data-id');
      if (!id) return;

      model.job_titles = (model.job_titles || []).filter(
        (t) => String(t.job_title_id) !== String(id)
      );
      renderJobTitlesList();
      markDirty();
    });
  }

  // Right-click context menu for "Set Primary Job Title"
  if (jobTitlesHost && !jobTitlesHost.__wiredCtx) {
    jobTitlesHost.__wiredCtx = true;

    jobTitlesHost.addEventListener('contextmenu', (e) => {
      const pill = e.target.closest('.pill[data-role-id]');
      if (!pill) return;

      const fr = window.__getModalFrame?.();
      const canEdit =
        !!fr &&
        fr.entity === 'candidates' &&
        fr.currentTabKey === 'main' &&
        (fr.mode === 'edit' || fr.mode === 'create');

      if (!canEdit) return; // no menu in view mode

      e.preventDefault();

      const id = pill.getAttribute('data-role-id');
      if (!id) return;

      // Remove any existing job title context menu
      if (window.__jtContextMenu) {
        try { document.body.removeChild(window.__jtContextMenu); } catch {}
        window.__jtContextMenu = null;
      }

      const menu = document.createElement('div');
      menu.style.cssText =
        'position:fixed;z-index:10000;background:#0b1528;border:1px solid var(--line);' +
        'padding:6px;border-radius:8px;min-width:180px;font-size:12px;';
      menu.innerHTML = `
        <div data-act="set-primary"
             style="padding:6px 10px;cursor:pointer;">
          Set as primary job title
        </div>
      `;

      const closeMenu = () => {
        if (window.__jtContextMenu) {
          try { document.body.removeChild(window.__jtContextMenu); } catch {}
          window.__jtContextMenu = null;
          document.removeEventListener('click', onDocClick, true);
        }
      };

      const onDocClick = (ev) => {
        if (ev.target && ev.target.closest && ev.target.closest('#__jtContextMenu')) return;
        closeMenu();
      };

      menu.id = '__jtContextMenu';
      window.__jtContextMenu = menu;
      document.body.appendChild(menu);

      const x = e.clientX;
      const y = e.clientY;
      menu.style.left = `${x}px`;
      menu.style.top = `${y}px`;

      menu.addEventListener('click', (ev) => {
        const item = ev.target.closest('[data-act="set-primary"]');
        if (!item) return;

        // Set this job title as primary
        let items = Array.isArray(model.job_titles) ? model.job_titles.slice() : [];
        items = items.map((t) => ({
          ...t,
          is_primary: String(t.job_title_id) === String(id)
        }));
        model.job_titles = items;
        renderJobTitlesList();
        markDirty();
        closeMenu();
      });

      document.addEventListener('click', onDocClick, true);
    });
  }

  // Job Title picker button (add another role)
  const jobBtn = q('[data-act="pick-job-title"]');
  if (jobBtn && !jobBtn.__wired) {
    jobBtn.__wired = true;
    jobBtn.addEventListener('click', async () => {
      await loadJobTitlesTree(false).catch(() => {});
      openJobTitlePickerModal(null, (sel) => {
        const id = sel.jobTitleId;
        if (!id) return;

        const existing = Array.isArray(model.job_titles) ? model.job_titles.slice() : [];
        if (existing.some((t) => String(t.job_title_id) === String(id))) {
          alert('This role is already added for this candidate.');
          return;
        }

        const requires = !!sel.requiresProfReg;
        const type = sel.profRegType || null;

        if (requires) {
          if (model.prof_reg_type && model.prof_reg_type !== type) {
            alert('All roles must share the same registration type (NMC / GMC / HCPC).');
            return;
          }
          if (!model.prof_reg_type && type) {
            model.prof_reg_type = type;
          }
        }

        const hasPrimary = existing.some((t) => t.is_primary);
        const newItem = {
          job_title_id: id,
          // First job title ever becomes primary; otherwise only if there was no primary for some reason
          is_primary: !existing.length && !hasPrimary
        };

        model.job_titles = [...existing, newItem];
        renderJobTitlesList();
        markDirty();
      });
    });
  }

  // Postcode lookup icon
  const addrBtn = q('[data-act="postcode-lookup"]');
  if (addrBtn && !addrBtn.__wired) {
    addrBtn.__wired = true;
    addrBtn.addEventListener('click', () => {
      const current = {
        line1: model.address_line1,
        line2: model.address_line2,
        line3: model.address_line3,
        city: model.town_city,
        postcode: model.postcode
      };
      openAddressLookupModal(current, (chosen) => {
        applyAddressToCandidateModel(model, chosen);
        // reflect in inputs
        ['address_line1', 'address_line2', 'address_line3', 'town_city', 'postcode'].forEach((k) => {
          const el = q(`input[name="${k}"]`);
          if (el) el.value = model[k] || '';
        });
        markDirty();
      });
    });
  }
}


// =============== NEW: apiPostcodeLookup (frontend → backend) ===========
async function apiPostcodeLookup(postcode, house) {
  const params = new URLSearchParams();
  if (postcode) params.set('postcode', postcode);
  if (house) params.set('house', house);

  const url = API(`/api/tools/postcode-lookup?${params.toString()}`);
  const res = await authFetch(url);
  if (!res.ok) {
    throw new Error('Postcode lookup failed');
  }
  const data = (await res.json().catch(() => ({}))) || {};
  return data.addresses || [];
}

// =============== NEW: Address lookup modal (candidate/umbrella) ========
function openAddressLookupModal(initialAddress, onSave) {
  const S = {
    line1: initialAddress?.line1 || '',
    line2: initialAddress?.line2 || '',
    line3: initialAddress?.line3 || '',
    city: initialAddress?.city || '',
    postcode: initialAddress?.postcode || '',
    house: '',
    lookupInFlight: false,
    results: [],
    error: ''
  };

  const body = html(`
    <div id="addrLookupRoot" class="form">
      <div class="row">
        <label>Postcode</label>
        <input type="text" name="lookup_postcode" />
      </div>
      <div class="row">
        <label>House number / name (optional)</label>
        <input type="text" name="lookup_house" />
      </div>
      <div class="row">
        <button type="button" id="btnAddrLookup">Lookup</button>
      </div>

      <div class="row" style="grid-column:1/-1">
        <label>Results</label>
        <div id="addrLookupResults"
             style="border:1px solid var(--line);border-radius:8px;max-height:200px;overflow:auto">
          <div class="hint" style="padding:8px">No lookup results yet.</div>
        </div>
      </div>

      <div class="row" style="grid-column:1/-1;margin-top:10px">
        <label>Manual address (you can edit after choosing a result, or ignore lookup entirely)</label>
      </div>
      <div class="row">
        <label>Address line 1</label>
        <input type="text" name="addr_line1" />
      </div>
      <div class="row">
        <label>Address line 2</label>
        <input type="text" name="addr_line2" />
      </div>
      <div class="row">
        <label>Address line 3</label>
        <input type="text" name="addr_line3" />
      </div>
      <div class="row">
        <label>City / Town</label>
        <input type="text" name="addr_city" />
      </div>
      <div class="row">
        <label>Postcode</label>
        <input type="text" name="addr_postcode" />
      </div>
    </div>
  `);

  const renderResults = () => {
    const root = document.getElementById('addrLookupRoot');
    if (!root) return;
    const box = root.querySelector('#addrLookupResults');
    if (!box) return;

    if (S.error) {
      box.innerHTML = `<div class="error" style="padding:8px">${escapeHtml(S.error)}</div>`;
      return;
    }

    if (!S.results.length) {
      box.innerHTML = `<div class="hint" style="padding:8px">No lookup results yet.</div>`;
      return;
    }

    box.innerHTML = S.results
      .map(
        (a, idx) => `
        <div class="addr-row" data-i="${idx}"
             style="padding:6px 8px;border-bottom:1px solid var(--line);cursor:pointer">
          <div>${escapeHtml(a.line1 || '')}</div>
          <div class="mini">${escapeHtml(
            [a.line2, a.line3, a.city, a.postcode].filter(Boolean).join(', ')
          )}</div>
        </div>
      `
      )
      .join('');
  };

  const syncStateToForm = () => {
    const root = document.getElementById('addrLookupRoot');
    if (!root) return;
    const setVal = (name, v) => {
      const el = root.querySelector(`input[name="${name}"]`);
      if (el) el.value = v || '';
    };

    setVal('lookup_postcode', S.postcode);
    setVal('lookup_house', S.house);
    setVal('addr_line1', S.line1);
    setVal('addr_line2', S.line2);
    setVal('addr_line3', S.line3);
    setVal('addr_city', S.city);
    setVal('addr_postcode', S.postcode);
  };

  const syncFormToState = () => {
    const root = document.getElementById('addrLookupRoot');
    if (!root) return;
    const getVal = (name) => {
      const el = root.querySelector(`input[name="${name}"]`);
      return el ? el.value.trim() : '';
    };

    // For lookup purposes, postcode comes only from the lookup_postcode field
    S.postcode = getVal('lookup_postcode') || S.postcode;
    S.house = getVal('lookup_house') || S.house;
    S.line1 = getVal('addr_line1') || S.line1;
    S.line2 = getVal('addr_line2') || S.line2;
    S.line3 = getVal('addr_line3') || S.line3;
    S.city = getVal('addr_city') || S.city;
    // Do NOT overwrite S.postcode from addr_postcode here (Option A)
  };

  const onOpen = () => {
    const root = document.getElementById('addrLookupRoot');
    if (!root) return;

    syncStateToForm();
    renderResults();

    const btn = root.querySelector('#btnAddrLookup');
    if (btn && !btn.__wired) {
      btn.__wired = true;
      btn.addEventListener('click', async () => {
        syncFormToState();
        if (!S.postcode) {
          alert('Please enter a postcode first');
          return;
        }
        S.lookupInFlight = true;
        S.error = '';
        btn.disabled = true;
        btn.textContent = 'Looking up…';

        const wireResultClicks = () => {
          const box = root.querySelector('#addrLookupResults');
          if (!box || box.__wired) return;
          box.__wired = true;
          box.addEventListener('click', (e) => {
            const row = e.target.closest('.addr-row[data-i]');
            if (!row) return;
            const idx = +row.getAttribute('data-i');
            const a = S.results[idx];
            if (!a) return;
            S.line1 = a.line1 || '';
            S.line2 = a.line2 || '';
            S.line3 = a.line3 || '';
            S.city = a.city || '';
            S.postcode = a.postcode || '';
            syncStateToForm();
          });
        };

        try {
          const results = await apiPostcodeLookup(S.postcode, S.house);
          S.results = results || [];
          if (S.results.length === 1) {
            const a = S.results[0];
            S.line1 = a.line1 || S.line1;
            S.line2 = a.line2 || S.line2;
            S.line3 = a.line3 || S.line3;
            S.city = a.city || S.city;
            S.postcode = a.postcode || S.postcode;
            syncStateToForm();
          }
        } catch (e) {
          console.error('postcode lookup failed', e);
          S.error = 'Lookup failed. Please check the postcode or try again.';
        } finally {
          S.lookupInFlight = false;
          btn.disabled = false;
          btn.textContent = 'Lookup';
          renderResults();
          // ensure row clicks wired after rendering
          const box = root.querySelector('#addrLookupResults');
          if (box && !box.__wired) {
            wireResultClicks();
          }
        }
      });
    }

    // Initial result click wiring (in case results are present already)
    const box = root.querySelector('#addrLookupResults');
    if (box && !box.__wired) {
      box.__wired = true;
      box.addEventListener('click', (e) => {
        const row = e.target.closest('.addr-row[data-i]');
        if (!row) return;
        const idx = +row.getAttribute('data-i');
        const a = S.results[idx];
        if (!a) return;
        S.line1 = a.line1 || '';
        S.line2 = a.line2 || '';
        S.line3 = a.line3 || '';
        S.city = a.city || '';
        S.postcode = a.postcode || '';
        syncStateToForm();
      });
    }
  };

  showModal(
    'Address',
    [{ key: 'main', label: 'Address' }],
    () => body,
    async () => {
      const root = document.getElementById('addrLookupRoot');
      if (!root) return { ok: false };
      syncFormToState();
      const addr = {
        line1: S.line1 || '',
        line2: S.line2 || '',
        line3: S.line3 || '',
        city: S.city || '',
        postcode: S.postcode || ''
      };
      if (typeof onSave === 'function') {
        onSave(addr);
      }
      return { ok: true };
    },
    false,
    onOpen,
    { kind: 'address-lookup', noParentGate: false }
  );

  // Wire up button + results on first open
  onOpen();
}

// =============== NEW: applyAddress* helpers ============================
function applyAddressToCandidateModel(model, addr) {
  if (!model || !addr) return;
  model.address_line1 = addr.line1 || '';
  model.address_line2 = addr.line2 || '';
  model.address_line3 = addr.line3 || '';
  model.town_city = addr.city || '';
  model.postcode = addr.postcode || '';
}

function applyAddressToUmbrellaModel(model, addr) {
  if (!model || !addr) return;
  model.address_line1 = addr.line1 || '';
  model.address_line2 = addr.line2 || '';
  model.address_line3 = addr.line3 || '';
  model.town_city = addr.city || '';
  model.postcode = addr.postcode || '';
}

// =============== NEW: buildUmbrellaDetailsModel ========================
function buildUmbrellaDetailsModel(row) {
  const r = row || {};
  return {
    id: r.id || null,
    name: r.name || '',
    remittance_email: r.remittance_email || '',
    bank_name: r.bank_name || '',
    sort_code: r.sort_code || '',
    account_number: r.account_number || '',
    vat_chargeable: !!r.vat_chargeable,
    enabled: r.enabled === false ? false : true,

    address_line1: r.address_line1 || '',
    address_line2: r.address_line2 || '',
    address_line3: r.address_line3 || '',
    town_city: r.town_city || '',
    county: r.county || '',
    postcode: r.postcode || '',
    country: r.country || '',
    company_number: r.company_number || ''
  };
}

// =============== NEW: bindUmbrellaAddressEvents ========================
function bindUmbrellaAddressEvents(container, model) {
  if (!container || !model) return;
  const q = (sel) => container.querySelector(sel);

  const bind = (selector, key) => {
    const el = q(selector);
    if (!el) return;
    el.value = model[key] || '';
    el.addEventListener('input', () => {
      model[key] = el.value;
    });
  };

  // Address fields
  bind('input[name="address_line1"]', 'address_line1');
  bind('input[name="address_line2"]', 'address_line2');
  bind('input[name="address_line3"]', 'address_line3');
  bind('input[name="town_city"]', 'town_city');
  bind('input[name="county"]', 'county');
  bind('input[name="postcode"]', 'postcode');
  bind('input[name="country"]', 'country');

  // Company registration number
  bind('input[name="company_number"]', 'company_number');

  // Optional postcode lookup
  const btn = q('[data-act="umbrella-postcode-lookup"]');
  if (btn && !btn.__wired) {
    btn.__wired = true;
    btn.addEventListener('click', () => {
      const curr = {
        line1: model.address_line1,
        line2: model.address_line2,
        line3: model.address_line3,
        city: model.town_city,
        postcode: model.postcode
      };
      openAddressLookupModal(curr, (addr) => {
        applyAddressToUmbrellaModel(model, addr);
        ['address_line1', 'address_line2', 'address_line3', 'town_city', 'postcode'].forEach((k) => {
          const el = q(`input[name="${k}"]`);
          if (el) el.value = model[k] || '';
        });
      });
    });
  }
}


















// ==== FIXED MODAL FRAMEWORK: close only on explicit success from onSave ====
// ==== FIXED MODAL FRAMEWORK: close only on explicit success from onSave ====

// FRONTEND — UPDATED
// showModal: ignore non-trusted events for dirty; ensure drag handlers cleared early on close
// ================== FRONTEND: renderSummary (UPDATED to jump & highlight pending focus) ==================

// ===== UPDATED: renderSummary — if pending focus row isn't visible, try one auto-relax/reload pass, then highlight when found

// ──────────────────────────────────────────────────────────────────────────────
// FIX 4: Bucket labels preview derived for contracts listing
// (cosmetic only; other sections unchanged)
// ──────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: renderSummary(rows)
// - Writes/refreshes summary membership cache fingerprint for current section
// - Prepares candidate role projection as before
// - Hooks page-size change as before
// - Triggers background membership priming (ALL IDs for current filters)
// - (Sorting of summary grid can be added here if/when you enable header clicks)
// ─────────────────────────────────────────────────────────────────────────────
function renderSummary(rows){
  currentRows = rows;
  currentSelection = null;

  // ── paging state (per section)
  window.__listState = window.__listState || {};
  const st = (window.__listState[currentSection] ||= {
    page: 1,
    pageSize: 50,
    total: null,
    hasMore: false,
    filters: null,
    sort: { key: null, dir: 'asc' }
  });

  // Ensure we always have a sort object
  if (!st.sort || typeof st.sort !== 'object') {
    st.sort = { key: null, dir: 'asc' };
  }
  const sortState = st.sort;

  const page     = Number(st.page || 1);
  const pageSize = st.pageSize; // 50 | 100 | 200 | 'ALL'

  // ── selection state (per section) — explicit IDs only
  window.__selection = window.__selection || {};
  const ensureSel = (section)=>{ const init = { fingerprint:'', ids:new Set() }; return (window.__selection[section] ||= init); };
  const sel = ensureSel(currentSection);

  const isRowSelected = (id)=> sel.ids.has(String(id||''));    
  const setRowSelected = (id, selected)=>{
    id = String(id||''); if (!id) return;
    if (selected) sel.ids.add(id); else sel.ids.delete(id);
  };
  const clearSelection = ()=>{ sel.ids.clear(); };

  // Tie selection to dataset via fingerprint (filters + section)
  const computeFp = ()=> getSummaryFingerprint(currentSection);
  const fp = computeFp();
  if (sel.fingerprint !== fp) { sel.fingerprint = fp; clearSelection(); }

  // Section-specific pre-formatting
  if (currentSection === 'candidates') {
    rows.forEach(r => {
      // Rota roles only – do NOT use this for Job Titles
      r.role = (r && Array.isArray(r.roles)) ? formatRolesSummary(r.roles) : '';

      // Ensure job_titles_display exists as a string so the grid
      // can show it via prefs as its own column
      if (r.job_titles_display == null) {
        r.job_titles_display = '';
      } else {
        r.job_titles_display = String(r.job_titles_display);
      }
    });
  } else if (currentSection === 'contracts') {
    rows.forEach(r => {
      const j = r && r.bucket_labels_json;
      if (j && typeof j === 'object') {
        const day   = (j.day   || '').trim();
        const night = (j.night || '').trim();
        const sat   = (j.sat   || '').trim();
        const sun   = (j.sun   || '').trim();
        const bh    = (j.bh    || '').trim();
        const parts = [day,night,sat,sun,bh].filter(Boolean);
        r.bucket_labels_preview = parts.length === 5 ? parts.join('/') : '';
      } else {
        r.bucket_labels_preview = '';
      }
    });
  }

  const content = byId('content');
  byId('title').textContent = sections.find(s=>s.key===currentSection)?.label || '';

  // Preserve scroll position per section — for .summary-body, not #content
  window.__scrollMemory = window.__scrollMemory || {};
  const memKey = `summary:${currentSection}`;
  const prevScrollY = window.__scrollMemory[memKey] ?? 0;

  content.innerHTML = '';
  if (currentSection === 'settings') return renderSettingsPanel(content);
  if (currentSection === 'audit')    return renderAuditTable(content, rows);

  // ── top controls ────────────────────────────────────────────────────────────
  const topControls = document.createElement('div');
  topControls.style.cssText = 'display:flex;align-items:center;gap:10px;padding:8px 10px;border-bottom:1px solid var(--line)';

  // Page size
  const sizeLabel = document.createElement('span'); sizeLabel.className = 'mini'; sizeLabel.textContent = 'Page size:';
  const sizeSel = document.createElement('select'); sizeSel.id = 'summaryPageSize';
  ['50','100','200','ALL'].forEach(optVal => {
    const opt = document.createElement('option');
    opt.value = optVal; opt.textContent = (optVal === 'ALL') ? 'All' : `First ${optVal}`;
    if (String(pageSize) === optVal) opt.selected = true;
    sizeSel.appendChild(opt);
  });
  sizeSel.addEventListener('change', async () => {
    const val = sizeSel.value;
    window.__listState[currentSection].pageSize = (val === 'ALL') ? 'ALL' : Number(val);
    window.__listState[currentSection].page = 1;
    const data = await loadSection();
    renderSummary(data);
  });

  topControls.appendChild(sizeLabel);
  topControls.appendChild(sizeSel);

  // ── NEW: Contracts quick Status menu ────────────────────────────────────────
  let statusSel = null;
  if (currentSection === 'contracts') {
    const stFilters = window.__listState[currentSection].filters || {};
    if (!('status' in stFilters)) stFilters.status = 'active'; // default
    window.__listState[currentSection].filters = stFilters;

    const statusLabel = document.createElement('span'); statusLabel.className = 'mini'; statusLabel.textContent = 'Status:';
    statusSel = document.createElement('select');
    [['active','Active'], ['unassigned','Unassigned'], ['completed','Completed']].forEach(([v,l])=>{
      const o = document.createElement('option'); o.value=v; o.textContent=l;
      if ((stFilters.status||'').toLowerCase() === v) o.selected = true;
      statusSel.appendChild(o);
    });
    statusSel.addEventListener('change', async () => {
      window.__listState[currentSection].filters = { ...(window.__listState[currentSection].filters||{}), status: statusSel.value };
      window.__listState[currentSection].page = 1;
      const data = await loadSection();
      renderSummary(data);
    });

    topControls.appendChild(statusLabel);
    topControls.appendChild(statusSel);
  }

  // Columns button
  const btnCols = document.createElement('button');
  btnCols.textContent = 'Columns';
  btnCols.style.cssText = 'border:1px solid var(--line);background:#0b152a;color:var(--text);padding:4px 8px;border-radius:8px;cursor:pointer';
  btnCols.addEventListener('click', () => openColumnsDialog(currentSection));
  topControls.appendChild(btnCols);

  const spacerTop = document.createElement('div'); spacerTop.style.flex = '1';
  topControls.appendChild(spacerTop);

  // Selected info / clear
  const selInfo = document.createElement('div'); selInfo.className = 'mini';
  const renderSelInfo = ()=>{ selInfo.textContent = (sel.ids.size > 0) ? `${sel.ids.size} selected.` : ''; };
  renderSelInfo();

  const clearBtn = document.createElement('button');
  clearBtn.textContent = 'Clear selection';
  clearBtn.style.cssText = 'border:1px solid var(--line);background:#0b152a;color:var(--text);padding:4px 8px;border-radius:8px;cursor:pointer;display:none';
  clearBtn.onclick = ()=>{
    clearSelection(); renderSelInfo();
    Array.from(document.querySelectorAll('input.row-select')).forEach(cb=>{ cb.checked = false; });
    const hdr = byId('summarySelectAll'); if (hdr) { hdr.checked=false; hdr.indeterminate=false; }
    updateButtons();
  };

  topControls.appendChild(selInfo);
  topControls.appendChild(clearBtn);
  content.appendChild(topControls);

  // ── single table (header + body) inside scroll host ────────────────────────
  const bodyWrap = document.createElement('div');
  bodyWrap.className = 'summary-body';
  content.appendChild(bodyWrap);

  const tbl = document.createElement('table');
  tbl.className = 'grid';

  const thead = document.createElement('thead');
  thead.style.borderBottom = '1px solid var(--line)';
  const trh = document.createElement('tr');
  thead.appendChild(trh);
  tbl.appendChild(thead);

  const tb = document.createElement('tbody');
  tbl.appendChild(tb);
  bodyWrap.appendChild(tbl);

  let btnFocus, btnSave;

  const computeHeaderState = ()=>{
    const idsVisible = rows.map(r => String(r.id || ''));
    const selectedOfVisible = idsVisible.filter(id => sel.ids.has(id)).length;
    const hdrCbEl = byId('summarySelectAll');
    if (hdrCbEl) {
      hdrCbEl.checked = (idsVisible.length > 0 && selectedOfVisible === idsVisible.length);
      hdrCbEl.indeterminate = (selectedOfVisible > 0 && selectedOfVisible < idsVisible.length);
    }
  };

  const updateButtons = ()=>{
    const any = sel.ids.size > 0;
    if (btnFocus) btnFocus.disabled = !any;
    if (btnSave)  btnSave .disabled = !any;
    clearBtn.style.display = any ? '' : 'none';
    renderSelInfo();
  };

  // Determine columns (using server prefs)
  const cols = getVisibleColumnsForSection(currentSection, rows);

  // Header checkbox (first column)
  const thSel = document.createElement('th');
  thSel.style.width = '40px';
  thSel.style.minWidth = '40px';
  thSel.style.maxWidth = '40px';

  const hdrCb = document.createElement('input'); hdrCb.type='checkbox'; hdrCb.id='summarySelectAll';
  hdrCb.addEventListener('click', (e)=>{
    e.stopPropagation();
    const idsVisible = rows.map(r => String(r.id || ''));
    const wantOn = !!hdrCb.checked;
    idsVisible.forEach(id => { if (wantOn) sel.ids.add(id); else sel.ids.delete(id); });
    Array.from(document.querySelectorAll('input.row-select')).forEach(cb=>{ cb.checked = wantOn; });
    computeHeaderState();
    updateButtons();
  });
  thSel.appendChild(hdrCb);
  trh.appendChild(thSel);

  // Build header cells with friendly labels, resizer handles, and click-to-sort
  cols.forEach(c=>{
    const th = document.createElement('th');
    th.dataset.colKey = String(c);
    th.style.cursor = 'pointer';

    const label = getFriendlyHeaderLabel(currentSection, c);
    const isActive = sortState && sortState.key === c;
    const arrow = isActive ? (sortState.dir === 'asc' ? ' ▲' : ' ▼') : '';
    th.textContent = label + arrow;

    const res = document.createElement('div');
    res.className = 'col-resizer';
    res.title = 'Drag to resize. Double-click to reset.';
    res.style.cssText = 'position:absolute;right:0;top:0;width:6px;height:100%;cursor:col-resize;user-select:none;';
    th.appendChild(res);

    th.draggable = true;

    th.addEventListener('click', async (ev) => {
      if (ev.target && ev.target.closest && ev.target.closest('.col-resizer')) return;

      const colKey = th.dataset.colKey;
      if (!colKey) return;

      window.__listState = window.__listState || {};
      const st2 = (window.__listState[currentSection] ||= {
        page: 1,
        pageSize: 50,
        total: null,
        hasMore: false,
        filters: null,
        sort: { key: null, dir: 'asc' }
      });

      if (!st2.sort || typeof st2.sort !== 'object') {
        st2.sort = { key: null, dir: 'asc' };
      }

      const prevDir = (st2.sort && st2.sort.key === colKey) ? st2.sort.dir : null;
      const nextDir = (prevDir === 'asc') ? 'desc' : 'asc';

      st2.sort = { key: colKey, dir: nextDir };
      st2.page = 1;

      try {
        const data = await loadSection();
        renderSummary(data);
      } catch (e) {
        console.error('Failed to apply sort', e);
      }
    });

    trh.appendChild(th);
  });

  // Body rows
  if (currentSection === 'candidates') {
    tbl.style.width = 'auto';
  }

  rows.forEach(r=>{
    const tr = document.createElement('tr');
    tr.dataset.id = (r && r.id) ? String(r.id) : '';
    tr.dataset.section = currentSection;

    const tdSel = document.createElement('td');
    tdSel.style.width = '40px';
    tdSel.style.minWidth = '40px';
    tdSel.style.maxWidth = '40px';

    const cb = document.createElement('input'); cb.type='checkbox'; cb.className='row-select';
    cb.checked = isRowSelected(tr.dataset.id);
    cb.addEventListener('click', (e)=>{
      e.stopPropagation();
      const id = tr.dataset.id; setRowSelected(id, cb.checked);
      computeHeaderState();
      updateButtons();
    });
    tdSel.appendChild(cb); tr.appendChild(tdSel);

    cols.forEach(c=>{
      const td = document.createElement('td');
      td.dataset.colKey = String(c);
      const v = r[c];

      if (currentSection === 'candidates' && c === 'job_titles_display') {
        // Show only secondary job titles (primary is shown separately)
        const raw = typeof r.job_titles_display === 'string' ? r.job_titles_display : (v || '');
        if (!raw.trim()) {
          td.textContent = '';
        } else {
          const parts = raw.split(';').map(s => s.trim()).filter(Boolean);
          const rest  = parts.slice(1); // drop primary
          td.textContent = rest.join('; ');
        }
      } else {
        td.textContent = formatDisplayValue(c, v);
      }

      tr.appendChild(td);
    });

    tb.appendChild(tr);
  });

  tb.addEventListener('click', (ev) => {
    const tr = ev.target && ev.target.closest('tr'); if (!tr) return;
    if (ev.target && ev.target.classList && ev.target.classList.contains('row-select')) return;
    tb.querySelectorAll('tr.selected').forEach(n => n.classList.remove('selected'));
    tr.classList.add('selected');
    const id = tr.dataset.id;
    currentSelection = currentRows.find(x => String(x.id) === id) || null;
  });

  tb.addEventListener('dblclick', (ev) => {
    const tr = ev.target && ev.target.closest('tr'); if (!tr) return;
    if (!confirmDiscardChangesIfDirty()) return;
    tb.querySelectorAll('tr.selected').forEach(n => n.classList.remove('selected'));
    tr.classList.add('selected');
    const id = tr.dataset.id;
    const row = currentRows.find(x => String(x.id) === id) || null;
    if (!row) return;
    const beforeDepth = (window.__modalStack && window.__modalStack.length) || 0;
    openDetails(row);
    setTimeout(() => {
      const afterDepth = (window.__modalStack && window.__modalStack.length) || 0;
      if (afterDepth > beforeDepth) tb.querySelectorAll('tr.selected').forEach(n => n.classList.remove('selected'));
    }, 0);
  });

  // ── Apply widths + wire resize/reorder + header context menu ────────────────
  applyUserGridPrefs(currentSection, tbl, cols);
  wireGridColumnResizing(currentSection, tbl);
  wireGridColumnReorder(currentSection, tbl);
  attachHeaderContextMenu(currentSection, tbl);

  // Footer/pager
  const pager = document.createElement('div');
  pager.style.cssText = 'display:flex;align-items:center;gap:6px;padding:8px 10px;border-top:1px solid var(--line);';
  const info = document.createElement('span'); info.className = 'mini';

  const mkBtn = (label, disabled, onClick) => {
    const b = document.createElement('button');
    b.textContent = label; b.disabled = !!disabled;
    b.style.cssText = 'border:1px solid var(--line);background:#0b152a;color:var(--text);padding:4px 8px;border-radius:8px;cursor:pointer';
    if (!disabled) b.addEventListener('click', onClick);
    return b;
  };

  const hasMore = !!st.hasMore;
  const totalKnown = (typeof st.total === 'number');
  const current = page;
  let maxPageToShow;
  if (totalKnown && pageSize !== 'ALL') maxPageToShow = Math.max(1, Math.ceil(st.total / Number(pageSize)));
  else if (pageSize === 'ALL') maxPageToShow = 1;
  else maxPageToShow = hasMore ? (current + 1) : current;

  const prevBtn = mkBtn('Prev', current <= 1, async () => {
    window.__listState[currentSection].page = Math.max(1, current - 1);
    const data = await loadSection();
    renderSummary(data);
  });
  pager.appendChild(prevBtn);

  const makePageLink = (n) => mkBtn(String(n), n === current, async () => {
    window.__listState[currentSection].page = n;
    const data = await loadSection();
    renderSummary(data);
  });

  const pages = [];
  if (maxPageToShow <= 7) { for (let n=1; n<=maxPageToShow; n++) pages.push(n); }
  else {
    pages.push(1);
    if (current > 3) pages.push('…');
    for (let n=Math.max(2, current-1); n<=Math.min(maxPageToShow-1, current+1); n++) pages.push(n);
    if (hasMore || current+1 < maxPageToShow) pages.push('…');
    pages.push(maxPageToShow);
  }
  pages.forEach(pn => {
    if (pn === '…') { const span = document.createElement('span'); span.textContent = '…'; span.className = 'mini'; pager.appendChild(span); }
    else pager.appendChild(makePageLink(pn));
  });

  const nextBtn = mkBtn('Next', (pageSize === 'ALL') || (!hasMore && (!totalKnown || current >= maxPageToShow)), async () => {
    window.__listState[currentSection].page = current + 1;
    const data = await loadSection();
    renderSummary(data);
  });
  pager.appendChild(nextBtn);

  if (pageSize === 'ALL') info.textContent = `Showing all ${rows.length} ${currentSection}.`;
  else if (totalKnown) {
    const ps = Number(pageSize);
    const start = (current-1)*ps + 1;
    const end = Math.min(start + rows.length - 1, st.total || start - 1);
    info.textContent = `Showing ${start}–${end}${st.total!=null ? ` of ${st.total}` : ''}`;
  } else {
    const ps = Number(pageSize);
    const start = (current-1)*ps + 1;
    const end = start + rows.length - 1;
    info.textContent = `Showing ${start}–${end}${hasMore ? '+' : ''}`;
  }
  const spacer = document.createElement('div'); spacer.style.flex = '1';
  pager.appendChild(spacer); pager.appendChild(info);
  content.appendChild(pager);

  // Selection toolbar
  const selBar = document.createElement('div');
  selBar.style.cssText = 'display:flex;justify-content:flex-end;gap:8px;padding:6px 10px;border-top:1px dashed var(--line)';
  btnFocus = document.createElement('button');
  btnFocus.title = 'Focus on records';
  btnFocus.textContent = '🔍 Focus';
  btnFocus.style.cssText = 'border:1px solid var(--line);background:#0b152a;color:var(--text);padding:4px 8px;border-radius:8px;cursor:pointer';

  btnSave = document.createElement('button');
  btnSave.title = 'Save selection';
  btnSave.textContent = '🔍 Save';
  btnSave.style.cssText = btnFocus.style.cssText;

  const btnLoad = document.createElement('button');
  btnLoad.title = 'Load selection';
  btnLoad.textContent = '🔍 Load';
  btnLoad.style.cssText = btnFocus.style.cssText;

  btnFocus.addEventListener('click', async () => {
    if (sel.ids.size === 0) return;
    const ids = Array.from(sel.ids);
    try {
      if (typeof applySelectionAsFilter === 'function') {
        await applySelectionAsFilter(currentSection, { ids });
      } else {
        window.__listState = window.__listState || {};
        const st2 = (window.__listState[currentSection] ||= { page:1, pageSize:50, total:null, hasMore:false, filters:null, sort:{ key:null, dir:'asc' } });
        st2.page = 1; st2.filters = { ...(st2.filters||{}), ids };
        const rows2 = await search(currentSection, st2.filters);
        renderSummary(rows2);
      }
    } catch (e) { console.error('Focus failed', e); }
  });

  btnSave.addEventListener('click', async () => {
    if (sel.ids.size === 0) return;
    try { await openSaveSelectionModal ? openSaveSelectionModal(currentSection) : null; } catch {}
  });

  btnLoad.addEventListener('click', async () => {
    try {
      if (typeof openLoadSelectionModal === 'function') await openLoadSelectionModal(currentSection);
    } catch {}
  });

  selBar.appendChild(btnFocus);
  selBar.appendChild(btnSave);
  selBar.appendChild(btnLoad);
  content.appendChild(selBar);

  // Restore scroll memory on inner summary-body (data rows only)
  try {
    const scrollHost = content.querySelector('.summary-body');
    if (scrollHost) {
      scrollHost.__activeMemKey = memKey;
      scrollHost.scrollTop = prevScrollY;
      if (!scrollHost.__scrollMemHooked) {
        scrollHost.addEventListener('scroll', () => {
          const k = scrollHost.__activeMemKey || memKey;
          window.__scrollMemory[k] = scrollHost.scrollTop || 0;
        });
        scrollHost.__scrollMemHooked = true;
      }
    }
  } catch {}

  // Initial states
  computeHeaderState();
  updateButtons();

  try { primeSummaryMembership(currentSection, fp); } catch (e) { /* non-blocking */ }
}


// Close any existing floating menu
function closeRelatedMenu(){
  const m = document.getElementById('relatedMenu');
  if (m) m.remove();
  document.removeEventListener('click', closeRelatedMenu, { capture: true });
  document.removeEventListener('keydown', escCloseRelatedMenu, true);
}
function escCloseRelatedMenu(ev){
  if (ev.key === 'Escape') closeRelatedMenu();
}

// Create & show a context menu near (x,y)
// ========================= showRelatedMenu (FIXED) =========================
function showRelatedMenu(x, y, counts, entity, id){
  closeRelatedMenu();

  const entries = counts && typeof counts === 'object'
    ? Object.entries(counts).filter(([k])=>k && k.trim().length>0)
    : [];

  const menu = document.createElement('div');
  menu.id = 'relatedMenu';
  menu.style.position = 'fixed';
  menu.style.left = x + 'px';
  menu.style.top  = y + 'px';
  menu.style.zIndex = 1000;
  menu.style.minWidth = '220px';
  menu.style.maxWidth = '280px';
  menu.style.background = '#0b1221';
  menu.style.border = '1px solid #334155';
  menu.style.borderRadius = '10px';
  menu.style.boxShadow = '0 10px 24px rgba(0,0,0,.35)';
  menu.style.padding = '6px';
  menu.style.color = '#f8fafc';
  menu.style.font = '14px/1.4 system-ui,Segoe UI,Roboto,Helvetica,Arial';

  function item(label, disabled, onClick){
    const it = document.createElement('div');
    it.textContent = label;
    it.style.padding = '8px 10px';
    it.style.borderRadius = '8px';
    it.style.cursor = disabled ? 'default' : 'pointer';
    it.style.opacity = disabled ? '.6' : '1';
    it.onmouseenter = ()=>{ if (!disabled) it.style.background = 'rgba(255,255,255,.06)'; };
    it.onmouseleave = ()=>{ it.style.background = 'transparent'; };
    if (!disabled) it.onclick = async (ev)=>{
      ev.stopPropagation();
      const rows = await fetchRelated(entity, id, onClick.type);
      if (rows) {
        // When jumping sections via Related, tear down any open modal to avoid stale state
        if ((window.__modalStack?.length || 0) > 0 || modalCtx?.entity) {
          discardAllModalsAndState();
        }
        if (onClick.type === 'timesheets' || onClick.type === 'invoices') {
          currentSection = onClick.type;
        }
        renderSummary(rows);
      }
      closeRelatedMenu();
    };
    menu.appendChild(it);
  }

  if (!entries.length) {
    item('No related records', true, {});
  } else {
    entries.sort((a,b)=> (b[1]||0)-(a[1]||0));
    entries.forEach(([type, count])=>{
      const label = `${count} related ${type}`;
      item(label, count===0, { type });
    });
  }

  document.body.appendChild(menu);
  setTimeout(()=>{
    document.addEventListener('click', closeRelatedMenu, { capture: true, once: true });
    document.addEventListener('keydown', escCloseRelatedMenu, true);
  }, 0);

  menu.addEventListener('click', ev => ev.stopPropagation());
}

// ===== Quick search =====
// ✅ Quick search: build minimal per-section filters (with timesheet heuristic)
// ✅ Quick search: build minimal per-section filters (with timesheet heuristic)
// Helper to build minimal quick-search filters per section
function buildQuickFilters(section, text) {
  const q = String(text || '').trim();
  if (!q) return {};

  switch (section) {
    case 'clients':
    case 'umbrellas':
    case 'invoices':
      // backend supports ?q=... (ilike on name / invoice_no depending on endpoint)
      return { q: q };

    case 'timesheets': {
      // Pick ONE field to avoid AND-ing and missing matches
      const looksLikeUUID   = /^[0-9a-f-]{10,}$/i.test(q);
      const looksLikeBkId   = /^[A-Za-z0-9-]{6,}$/.test(q);
      const looksLikeOccKey = /^[A-Za-z0-9_.-]{4,}$/.test(q);

      if (looksLikeBkId || looksLikeUUID) return { booking_id: q };
      if (looksLikeOccKey)                return { occupant_key_norm: q };
      return { hospital_norm: q };
    }

    case 'candidates':
      return { first_name: q, last_name: q, email: q, phone: q };

    default:
      return {};
  }
}

// ✅ Quick search: use heuristic builder (includes timesheets fix)

async function openSearch() {
  const q = prompt('Search text:');
  if (!q) return;

  // reflect in the quick box for consistency
  const box = byId('quickSearch');
  if (box) box.value = q;

  const filters = buildQuickFilters(currentSection, q);
  const rows = await search(currentSection, filters);
  if (rows) renderSummary(rows);
}

// OPTIONAL: open ALT+F for fast search
document.addEventListener('keydown', (e) => {
  if (e.altKey && (e.key === 'f' || e.key === 'F')) {
    e.preventDefault();
    openSearchModal();
  }
});

// ================== NEW: openSettings (parent modal; opens in View) ==================
async function openSettings() {
  const deep = (o)=> JSON.parse(JSON.stringify(o || {}));

  // Hydrate settings first
  let settings;
  try {
    settings = await getSettings(); // unwraps {settings:{...}} → {...}
  } catch (e) {
    alert('Could not load settings.');
    return;
  }

  // Seed modal context
  modalCtx = {
    entity: 'settings',
    data: deep(settings),                    // single source of truth for showModal
    formState: { __forId: 'global', main:{} },
    openToken: 'settings:' + Date.now()
  };

  // Open in VIEW mode (hasId=true) and let showModal manage Edit/Cancel/Discard/Save
  showModal(
    'Settings',
    [{ key:'main', label:'Defaults' }],
    renderSettingsTab,
    handleSaveSettings,
    true // hasId → opens in View mode
  );
}


// ================== NEW: renderSettingsTab (tab renderer; showModal controls read-only) ==================
function renderSettingsTab(key, s = {}) {
  if (key !== 'main') return '';

  const erniValue = (s.employers_ni_pct ?? s.erni_pct ?? 0);

  return html(`
    <div class="form" id="settingsForm">
      ${input('timezone_id','Timezone', s.timezone_id || 'Europe/London')}
      ${input('day_start','Day start', s.day_start || '06:00')}
      ${input('day_end','Day end', s.day_end || '20:00')}
      ${input('night_start','Night start', s.night_start || '20:00')}
      ${input('night_end','Night end', s.night_end || '06:00')}
      ${select('bh_source','Bank Holidays source', s.bh_source || 'MANUAL', ['MANUAL','FEED'])}
      <div class="row" style="grid-column:1/-1">
        <label>Bank Holidays list (JSON dates)</label>
        <textarea name="bh_list">${JSON.stringify(s.bh_list || [], null, 2)}</textarea>
      </div>
      ${input('bh_feed_url','BH feed URL', s.bh_feed_url || '')}
      ${input('vat_rate_pct','VAT %', s.vat_rate_pct ?? 20, 'number')}
      ${input('holiday_pay_pct','Holiday pay %', s.holiday_pay_pct ?? 0, 'number')}
      ${input('erni_pct','ERNI %', erniValue, 'number')}
      ${select('apply_holiday_to','Apply holiday to', s.apply_holiday_to || 'PAYE_ONLY', ['PAYE_ONLY','ALL','NONE'])}
      ${select('apply_erni_to','Apply ERNI to', s.apply_erni_to || 'PAYE_ONLY', ['PAYE_ONLY','ALL','NONE'])}
      <div class="row" style="grid-column:1/-1">
        <label>Margin includes (JSON)</label>
        <textarea name="margin_includes">${JSON.stringify(s.margin_includes || {}, null, 2)}</textarea>
      </div>

      <div class="row">
        <label>Effective from (DD/MM/YYYY)</label>
        <input type="text" name="effective_from" id="settings_effective_from" placeholder="DD/MM/YYYY"
               value="${s.effective_from ? formatIsoToUk(s.effective_from) : ''}" />
      </div>
    </div>
  `);
}


// ================== NEW: handleSaveSettings (parent onSave; persist then stay open in View) ==================
async function handleSaveSettings() {
  // Collect with JSON parsing for bh_list / margin_includes
  const payload = collectForm('#settingsForm', true) || {};

  // Normalise date
  if (payload.effective_from) {
    const iso = parseUkDateToIso(payload.effective_from);
    if (!iso) {
      alert('Invalid “Effective from” date');
      return { ok:false };
    }
    payload.effective_from = iso;
  }

  try {
    await saveSettings(payload);
  } catch (e) {
    alert('Save failed: ' + (e?.message || 'Unknown error'));
    return { ok:false };
  }

  // Return the merged saved state so showModal flips to View and repaints with new values
  const saved = { ...(modalCtx.data || {}), ...payload };
  return { ok:true, saved };
}

async function commitContractCalendarStage(contractId) {
  const LOG_CAL = (typeof window.__LOG_CAL === 'boolean') ? window.__LOG_CAL : true;
  const L = (...a)=> { if (LOG_CAL) console.log('[CAL][commit]', ...a); };
  const W = (...a)=> { if (LOG_CAL) console.warn('[CAL][commit]', ...a); };
  const E = (...a)=> { if (LOG_CAL) console.error('[CAL][commit]', ...a); };

  const { addRanges, removeRanges, additionals, removeAll } = buildPlanRangesFromStage(contractId);
  L('BEGIN', { contractId, addRanges, removeRanges, additionals, removeAll });

  // If "remove all" is staged, we only perform the single bulk unplan and exit.
  if (removeAll) {
    if (removeRanges.length) {
      const payload = {
        when_timesheet_exists: 'skip',
        empty_week_action: 'delete',
        ranges: removeRanges
      };
      L('DELETE /plan-ranges (removeAll)', payload);
      try {
        const resp = await contractsUnplanRanges(contractId, payload);
        L('DELETE /plan-ranges ←', resp);
      } catch (err) {
        E('unplan-ranges (removeAll) failed', err);
        throw err;
      }
    } else {
      L('removeAll=true but no removeRanges built');
    }
    clearContractCalendarStageState(contractId);
    L('DONE: stage cleared for', contractId);
    return { ok: true, detail: 'calendar saved', removedAll: true };
  }

  // Otherwise, proceed with normal sequence: adds → removes → additionals.
  if (addRanges.length) {
    const payload = {
      extend_contract_window: true,
      ranges: addRanges
    };
    L('POST /plan-ranges', payload);
    try {
      const resp = await contractsPlanRanges(contractId, payload);
      L('POST /plan-ranges ←', resp);
      if (!resp || typeof resp !== 'object') W('plan-ranges returned unexpected response', resp);
    } catch (err) {
      E('plan-ranges failed', err);
      throw err;
    }
  } else {
    L('No addRanges to commit');
  }

  if (removeRanges.length) {
    const payload = {
      when_timesheet_exists: 'skip',
      empty_week_action: 'cancel',
      ranges: removeRanges
    };
    L('DELETE /plan-ranges', payload);
    try {
      const resp = await contractsUnplanRanges(contractId, payload);
      L('DELETE /plan-ranges ←', resp);
    } catch (err) {
      E('unplan-ranges failed', err);
      throw err;
    }
  } else {
    L('No removeRanges to commit');
  }

  if (additionals.length) {
    L('Committing additional weeks…', { count: additionals.length });
    for (const g of additionals) {
      try {
        L('Create additional for baseWeekId', g.baseWeekId, 'dates=', g.dates);
        const addRow = await contractWeekCreateAdditional(g.baseWeekId);
        L('additional created ←', addRow);
        const payload = { add: g.dates.map(d => ({ date: d })), merge: 'append' };
        L('PATCH /contract-weeks/:id/plan', { week_id: addRow.id, payload });
        const resp = await contractWeekPlanPatch(addRow.id, payload);
        L('PATCH /contract-weeks/:id/plan ←', resp);
      } catch (err) {
        E('additional week flow failed', err);
        throw err;
      }
    }
  } else {
    L('No additional week patches to commit');
  }

  clearContractCalendarStageState(contractId);
  L('DONE: stage cleared for', contractId);
  return { ok: true, detail: 'calendar saved', removedAll: false };
}


async function duplicateContract(contractId, { count } = {}) {
  const n = Number(count || 1);
  if (!Number.isInteger(n) || n < 1 || n > 10) {
    return { ok: false, message: 'count must be an integer between 1 and 10' };
  }

  try {
    const url = API(`/api/contracts/${encodeURIComponent(contractId)}/duplicate`);
    const res = await authFetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count: n })
    });

    // Non-2xx → try to surface a useful error
    if (!res.ok) {
      let msg = `Duplicate failed (${res.status})`;
      try {
        const txt = await res.text();
        if (txt) msg = txt;
      } catch {}
      return { ok: false, message: msg };
    }

    let data = null;
    try {
      data = await res.json();
    } catch {
      data = null;
    }

    // Backend returns: { source_contract_id, count, duplicates: [...] }
    return {
      ok: true,
      ...(data || {}),
    };
  } catch (e) {
    return {
      ok: false,
      message: e?.message || 'Duplicate failed'
    };
  }
}


// ===== Boot =====
async function renderAll(){
  // seed defaults for first login / first visit to section
  window.__listState = window.__listState || {};
  if (!window.__listState[currentSection]) {
    window.__listState[currentSection] = {
      page: 1,
      pageSize: 50,
      total: null,
      hasMore: false,
      filters: null,
      sort: { key: null, dir: 'asc' }
    };
  }
  renderTopNav();
  renderTools();
  const data = await loadSection();
  renderSummary(data);
}

async function bootstrapApp(){
  // Belt & braces: if loadSession() ran but globals are not mirrored, mirror now
  try {
    if (typeof window !== 'undefined') {
      if (window.SESSION !== SESSION) window.SESSION = SESSION;
      window.__auth = window.__auth || {};
      if (!window.__auth.user && SESSION?.user) window.__auth.user = SESSION.user;
      if (!window.__USER_ID && SESSION?.user?.id) window.__USER_ID = SESSION.user.id;
    }

    // If token exists but user.id is missing, hydrate via /api/me (non-blocking safety)
    if (SESSION?.accessToken && (!SESSION.user || !SESSION.user.id)) {
      try {
        const meRes = await fetch(API('/api/me'), { headers: { 'Authorization': `Bearer ${SESSION.accessToken}` } });
        if (meRes.ok) {
          const meJson = await meRes.json().catch(()=> ({}));
          const profile = meJson && (meJson.user || meJson);
          if (profile && profile.id) {
            saveSession({ ...SESSION, user: profile }); // also re-mirrors globals
          }
        }
      } catch {}
    }
  } catch {}

  ensureSelectionStyles();   // ← ensure the highlight is clearly visible
  renderTopNav();
  renderTools();
  await renderAll();
}


// Initialize
initAuthUI();
if (loadSession()) { scheduleRefresh(); bootstrapApp(); }
else openLogin();
