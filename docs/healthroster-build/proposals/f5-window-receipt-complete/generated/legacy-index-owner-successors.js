import {validateZeroRoundingSuppressions} from './invoice-presentation-contract.js';
export function validateLegacyInvoiceManifestSuppressions(manifest) {
  if (manifest.source_rounding_suppressions==null) return manifest;
  const invoice=manifest.invoice||manifest.invoice_row;
  if (!invoice?.id || !Array.isArray(manifest.lines)) throw new Error('INVOICE_ZERO_ROUNDING_SUPPRESSION_INVALID');
  validateZeroRoundingSuppressions({invoice_id:invoice.id,source_rounding_suppressions:manifest.source_rounding_suppressions,
    lines:manifest.lines.map(line=>({source_invoice_line_id:line.id}))});
  return manifest;
}


async function handleGetInvoice(env, req, invoiceId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    // ✅ Single RPC call for full invoice context (manifest includes reference_rows + tsfin map + reference sources)
    const man = await sbRpc(env, 'invoice_render_manifest', { p_invoice_id: invoiceId });
    const manRows = Array.isArray(man) ? man : (man?.data || []);
    const manifest = (manRows && manRows.length) ? manRows[0] : man;

    if (!manifest || typeof manifest !== 'object') {
      return withCORS(env, req, serverError('Failed to fetch invoice manifest'));
    }

    const invoice = manifest.invoice || manifest.invoice_row || null;
    if (!invoice || !invoice.id) return withCORS(env, req, notFound('Invoice not found'));

    let invoiceAsyncDetail = null;
    if (isInvoiceAsyncPipelineEnabled(env)) {
      try {
        invoiceAsyncDetail = unwrapRpcJsonb(await sbRpc(env, 'invoice_detail_get', {
          p_invoice_id: invoiceId,
          p_actor_user_id: user.id
        }), 'invoice_detail_get');
      } catch {
        invoiceAsyncDetail = null;
      }
    }
    const emailDeliveryPolicy = invoiceEmailDeliveryPolicySummary(invoiceAsyncDetail);
    const evidence = Array.isArray(invoiceAsyncDetail?.evidence)
      ? invoiceAsyncDetail.evidence
      : (Array.isArray(manifest.evidence) ? manifest.evidence : []);
    const timesheetEvidence = Array.isArray(invoiceAsyncDetail?.timesheet_evidence)
      ? invoiceAsyncDetail.timesheet_evidence
      : (Array.isArray(manifest.timesheet_evidence) ? manifest.timesheet_evidence : []);
    const evidenceOther = Array.isArray(invoiceAsyncDetail?.evidence_other)
      ? invoiceAsyncDetail.evidence_other
      : (Array.isArray(manifest.evidence_other) ? manifest.evidence_other : []);
    const evidenceReadiness = (
      invoiceAsyncDetail?.document_readiness
      && typeof invoiceAsyncDetail.document_readiness === 'object'
    ) ? invoiceAsyncDetail.document_readiness : null;

    const header_snapshot_json =
      (manifest.header_snapshot_json && typeof manifest.header_snapshot_json === 'object')
        ? manifest.header_snapshot_json
        : (invoice.header_snapshot_json && typeof invoice.header_snapshot_json === 'object' ? invoice.header_snapshot_json : {});

    let weeklySourceInvoice = null;
    let weeklySourceEvidence = [];
    if (String(header_snapshot_json?.schema_version || '').trim()
        === 'WEEKLY_SOURCE_SELF_BILL_INVOICE_V1') {
      weeklySourceInvoice = unwrapRpcJsonb(await sbRpc(
        env,
        'weekly_source_invoice_edit_context_v1',
        { p_request: { actor_user_id: user.id, invoice_id: invoiceId } }
      ), 'weekly_source_invoice_edit_context_v1');
      if (weeklySourceInvoice?.is_weekly_source_invoice !== true) {
        throw new Error('WEEKLY_SOURCE_INVOICE_EDIT_CONTEXT_INVALID');
      }
      weeklySourceEvidence = (await weeklySourceInvoiceEvidenceRows(env, invoiceId, user.id))
        .map(({ upload_id, filename, uploaded_at_utc, byte_count, download_available }) => ({
          upload_id, filename, uploaded_at_utc, byte_count, download_available
        }));
    }

    validateLegacyInvoiceManifestSuppressions(manifest);
    const lineRows = Array.isArray(manifest.lines) ? manifest.lines : [];

    const items = lineRows.map(l => ({
      invoice_line_id: l.id ?? null,
      source_key: l.source_key ?? null,
      booking_id: l.booking_id ?? null,
      timesheet_id: l.timesheet_id ?? null,

      is_adjustment: (l.is_adjustment != null)
        ? !!l.is_adjustment
        : (!l.timesheet_id || String(l?.meta_json?.line_type || '').toUpperCase() === 'ADJUSTMENT'),
      line_type_norm: (typeof l.line_type_norm === 'string' && l.line_type_norm)
        ? l.line_type_norm
        : String(l?.meta_json?.line_type || '').toUpperCase(),

      qty: {
        day: l.hours_day, night: l.hours_night, sat: l.hours_sat, sun: l.hours_sun, bh: l.hours_bh
      },
      pay_rate: {
        day: l.pay_day, night: l.pay_night, sat: l.pay_sat, sun: l.pay_sun, bh: l.pay_bh
      },
      charge_rate: {
        day: l.charge_day, night: l.charge_night, sat: l.charge_sat, sun: l.charge_sun, bh: l.charge_bh
      },

      total_pay_ex_vat: l.total_pay_ex_vat ?? null,
      total_charge_ex_vat: l.total_charge_ex_vat ?? null,
      margin_ex_vat: l.margin_ex_vat ?? null,

      vat_rate_pct: l.vat_rate_pct ?? null,
      vat_amount: l.vat_amount ?? null,
      total_inc_vat: l.total_inc_vat ?? null,

      description: l.description ?? null,
      paper_ts_r2_key: l.paper_ts_r2_key ?? l.effective_paper_ts_r2_key ?? null,

      meta_json: (l.meta_json && typeof l.meta_json === 'object') ? l.meta_json : {},

      // ✅ filled in below (precheck diagnostics)
      precheck_status: null,
      has_timesheet_evidence_pdf: null,
    }));

    // ✅ Precheck diagnostics for all timesheets on this invoice (batched)
    try {
      const tsIds = Array.from(
        new Set(items.map(it => it?.timesheet_id).filter(Boolean).map(String))
      );

      if (tsIds.length) {
        const chunkSize = 200;
        const pcMap = new Map();

        for (let i = 0; i < tsIds.length; i += chunkSize) {
          const chunk = tsIds.slice(i, i + chunkSize);
          const { rows: pcRows } = await sbFetch(
            env,
            `${env.SUPABASE_URL}/rest/v1/v_ts_invoice_precheck` +
              `?select=timesheet_id,precheck_status,has_timesheet_evidence_pdf` +
              `&timesheet_id=in.(${chunk.map(encodeURIComponent).join(',')})`
          );
          for (const r of (pcRows || [])) {
            if (!r?.timesheet_id) continue;
            pcMap.set(String(r.timesheet_id), {
              precheck_status: r.precheck_status ?? null,
              has_timesheet_evidence_pdf: (r.has_timesheet_evidence_pdf === true),
            });
          }
        }

        for (const it of items) {
          const tsId = it?.timesheet_id ? String(it.timesheet_id) : null;
          if (!tsId) continue;
          const pc = pcMap.get(tsId);
          if (!pc) continue;
          it.precheck_status = pc.precheck_status;
          it.has_timesheet_evidence_pdf = pc.has_timesheet_evidence_pdf;
        }
      }
    } catch {
      // omit diagnostics on failure
    }

    // ✅ Pass-through: segment structures + history from manifest (single-source-of-truth support)
    const segments_on_invoice_by_timesheet =
      (manifest.segments_on_invoice_by_timesheet && typeof manifest.segments_on_invoice_by_timesheet === 'object')
        ? manifest.segments_on_invoice_by_timesheet
        : ((manifest.segments_by_timesheet && typeof manifest.segments_by_timesheet === 'object')
            ? manifest.segments_by_timesheet
            : null);

    const history = Array.isArray(manifest.history) ? manifest.history : [];

    // ✅ Additional pass-throughs for frontend convenience (already present in manifest, but exposed top-level)
    const tsfin_id_by_timesheet_id =
      (manifest.tsfin_id_by_timesheet_id && typeof manifest.tsfin_id_by_timesheet_id === 'object')
        ? manifest.tsfin_id_by_timesheet_id
        : {};

    const reference_rows = Array.isArray(manifest.reference_rows) ? manifest.reference_rows : [];

    // ✅ UPDATED: take reference sources directly from the manifest (RPC now embeds this)
    // Also: ensure any timesheet_ids mentioned by items/reference_rows have a key in the map.
    const timesheet_reference_sources_by_id = (() => {
      const out = Object.create(null);

      try {
        const src =
          (manifest.timesheet_reference_sources_by_id && typeof manifest.timesheet_reference_sources_by_id === 'object')
            ? manifest.timesheet_reference_sources_by_id
            : null;

        if (src) {
          for (const [k, v] of Object.entries(src)) {
            const tid = String(k || '').trim();
            if (!tid) continue;

            const obj = (v && typeof v === 'object') ? v : {};
            out[tid] = {
              reference_number: (obj.reference_number != null) ? obj.reference_number : null,
              day_references_json: (obj.day_references_json != null) ? obj.day_references_json : null,
              actual_schedule_json: (obj.actual_schedule_json != null) ? obj.actual_schedule_json : null
            };
          }
        }

        const idSet = new Set();

        for (const it of (items || [])) {
          const tid = it?.timesheet_id != null ? String(it.timesheet_id).trim() : '';
          if (tid) idSet.add(tid);
        }

        for (const rr of (reference_rows || [])) {
          if (!rr || typeof rr !== 'object') continue;
          const tid = rr.timesheet_id != null ? String(rr.timesheet_id).trim() : '';
          if (tid) idSet.add(tid);
        }

        for (const tid of idSet) {
          if (!out[tid]) {
            out[tid] = {
              reference_number: null,
              day_references_json: null,
              actual_schedule_json: null
            };
          }
        }
      } catch {}

      return out;
    })();

    // Optional correspondence (legacy query param)
    const includeCorr = new URL(req.url).searchParams.get('include_correspondence');
    if (includeCorr) {
      let correspondence = [];
      try {
        const { rows: corrRows } = await sbFetch(
          env,
          `${env.SUPABASE_URL}/rest/v1/audit_events` +
          `?object_type=eq.invoice` +
          `&object_id_text=eq.${encodeURIComponent(invoiceId)}` +
          `&or=(action.eq.EMAIL_QUEUED,action.eq.EMAIL_SENT)` +
          `&select=ts_utc,action,after_json,correlation_id` +
          `&order=ts_utc.desc`
        );

        const mailIds = [...new Set((corrRows || []).map(r => r.correlation_id).filter(Boolean))];
        let mailMap = {};
        if (mailIds.length) {
          const { rows: mailRows } = await sbFetch(
            env,
            `${env.SUPABASE_URL}/rest/v1/mail_outbox` +
            `?id=in.(${mailIds.map(encodeURIComponent).join(',')})` +
            `&select=id,to,cc,subject,status,created_at_utc,sent_at,failed_at,reference,provider_message_id`
          );
          mailMap = Object.fromEntries((mailRows || []).map(m => [m.id, m]));
        }

        correspondence = (corrRows || []).map(ev => ({
          ts_utc: ev.ts_utc,
          action: ev.action,
          correlation_id: ev.correlation_id || null,
          after_json: ev.after_json ?? null,
          email: ev.correlation_id && mailMap[ev.correlation_id] ? mailMap[ev.correlation_id] : null
        }));
      } catch {
        correspondence = [];
      }

      return withCORS(env, req, ok({
        manifest,

        invoice,
        items,
        header_snapshot_json,
        email_summary: manifest.email_summary ?? null,
        email_delivery_policy: emailDeliveryPolicy,

        attach_policy: manifest.attach_policy ?? null,

        evidence,
        timesheet_evidence: timesheetEvidence,
        evidence_other: evidenceOther,
        evidence_readiness: evidenceReadiness,

        hr_source_rows_cache: Array.isArray(manifest.hr_source_rows_cache) ? manifest.hr_source_rows_cache : [],
        tsfin_external_source_rows: Array.isArray(manifest.tsfin_external_source_rows) ? manifest.tsfin_external_source_rows : [],

        segments_on_invoice_by_timesheet,
        segments_by_timesheet: segments_on_invoice_by_timesheet,
        history,
        tsfin_id_by_timesheet_id,
        reference_rows,

        // ✅ reference sources now come from the RPC (no extra DB reads here)
        timesheet_reference_sources_by_id,

        weekly_source_invoice: weeklySourceInvoice,
        weekly_source_evidence: weeklySourceEvidence,

        correspondence
      }));
    }

    return withCORS(env, req, ok({
      manifest,

      invoice,
      items,
      header_snapshot_json,
      email_summary: manifest.email_summary ?? null,
      email_delivery_policy: emailDeliveryPolicy,

      attach_policy: manifest.attach_policy ?? null,

      evidence,
      timesheet_evidence: timesheetEvidence,
      evidence_other: evidenceOther,
      evidence_readiness: evidenceReadiness,

      hr_source_rows_cache: Array.isArray(manifest.hr_source_rows_cache) ? manifest.hr_source_rows_cache : [],
      tsfin_external_source_rows: Array.isArray(manifest.tsfin_external_source_rows) ? manifest.tsfin_external_source_rows : [],

      segments_on_invoice_by_timesheet,
      segments_by_timesheet: segments_on_invoice_by_timesheet,
      history,
      tsfin_id_by_timesheet_id,
      reference_rows,

      // ✅ reference sources now come from the RPC (no extra DB reads here)
      timesheet_reference_sources_by_id,

      weekly_source_invoice: weeklySourceInvoice,
      weekly_source_evidence: weeklySourceEvidence
    }));
  } catch {
    return withCORS(env, req, serverError('Failed to fetch invoice'));
  }
}

async function _renderInvoiceBundleAndStore(env, req, invoiceId, userForAudit, opts, testRuntime = null) {
  const LOG = (typeof wranglerimportlog !== 'undefined' && wranglerimportlog === true);

  const enc = encodeURIComponent;
  const t0 = Date.now();

  const options = (opts && typeof opts === 'object') ? opts : {};
  const forceRegen = !!(options.force_regen === true || options.forceRegen === true);

  let step = 'INIT';
  const log = (level, msg, extra) => {
    if (!LOG) return;
    try {
      const payload = Object.assign(
        {
          tag: 'INVOICE_RENDER_BUNDLE',
          at_utc: new Date().toISOString(),
          cf_ray: req?.headers?.get?.('cf-ray') || null,
          invoice_id: invoiceId || null,
          step: step || null,
          msg: String(msg || '')
        },
        (extra && typeof extra === 'object') ? extra : {}
      );
      (level === 'error' ? console.error : console.log)(JSON.stringify(payload));
    } catch (e) {
      try { (level === 'error' ? console.error : console.log)(msg); } catch {}
    }
  };

  log('log', 'start', { force_regen: forceRegen });

  const r2HeadForInvoiceRender = async (key) => {
    try {
      const cleanKey = normalizeKey(String(key || ''));
      if (!cleanKey) return null;
      const bucket = env.R2_BUCKET || env.R2;
      if (!bucket || typeof bucket.head !== 'function') return null;
      return await bucket.head(cleanKey);
    } catch {
      return null;
    }
  };

  function toMarginsObj(m) {
    const dflt = { top: 32, right: 12, bottom: 20, left: 12 };
    if (Array.isArray(m) && m.length === 4) {
      return {
        top: Number(m[0] ?? dflt.top),
        right: Number(m[1] ?? dflt.right),
        bottom: Number(m[2] ?? dflt.bottom),
        left: Number(m[3] ?? dflt.left),
      };
    }
    if (m && typeof m === "object") {
      return {
        top: Number(m.top ?? dflt.top),
        right: Number(m.right ?? dflt.right),
        bottom: Number(m.bottom ?? dflt.bottom),
        left: Number(m.left ?? dflt.left),
      };
    }
    return dflt;
  }

  // Date parsing used for HR/NHSP sorting.
  // - Supports YYYY-MM-DD, ISO strings, and DD/MM/YYYY.
  // - Invalid/missing => Infinity (sorted last).
  function _parseSortDateToMs(v) {
    if (v == null) return Number.POSITIVE_INFINITY;
    const s0 = String(v).trim();
    if (!s0) return Number.POSITIVE_INFINITY;

    // DD/MM/YYYY
    const m = s0.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
    if (m) {
      const dd = Number(m[1]), mm = Number(m[2]), yyyy = Number(m[3]);
      if (dd >= 1 && dd <= 31 && mm >= 1 && mm <= 12 && yyyy >= 1900 && yyyy <= 2100) {
        const ms = Date.parse(`${yyyy}-${String(mm).padStart(2, '0')}-${String(dd).padStart(2, '0')}T00:00:00Z`);
        return Number.isFinite(ms) ? ms : Number.POSITIVE_INFINITY;
      }
    }

    // YYYY-MM-DD (or ISO)
    const ms = Date.parse(s0);
    return Number.isFinite(ms) ? ms : Number.POSITIVE_INFINITY;
  }

  function _safeGetDateFromAnyRow(raw) {
    if (!raw || typeof raw !== 'object') return '';
    return (
      raw.work_date ??
      raw.WorkDate ??
      raw.workDate ??
      raw.date ??
      raw.Date ??
      raw.shift_date ??
      raw.shiftDate ??
      raw['Work Date'] ??
      raw['WORK DATE'] ??
      ''
    );
  }
function buildHrReportHTML(inv, header, hrRows) {
  const safe = (v) => (v == null ? '' : String(v));
  const h = header || {};
  const clientName = safe(h.client_name || h.client || '');
  const invNo = safe(inv.invoice_no || '');
  const issued = safe(inv.issued_at_utc || '');

  // Use global escapeHtml if present (it is used elsewhere), otherwise fall back safely.
  const esc = (typeof escapeHtml === 'function')
    ? (v) => escapeHtml(safe(v))
    : (v) => safe(v)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');

  const pad2 = (n) => String(n).padStart(2, '0');

  const toNum = (v) => {
    if (typeof v === 'number') return Number.isFinite(v) ? v : null;
    const s = String(v ?? '').trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  };

  // Excel time fraction / serial date-time -> HH:MM
  // - If v is 0..2 => treat as day-fraction (durations up to 48h supported)
  // - If v is a typical Excel serial date (~20000..90000) => use fractional part for time-of-day
  // - If v already looks like HH:MM(/:SS) => normalise to HH:MM
  const excelToHHMM = (v) => {
    const n = toNum(v);
    if (n == null) {
      const s = String(v ?? '').trim();
      if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(s)) {
        const parts = s.split(':');
        const hh = pad2(Number(parts[0]) || 0);
        const mm = pad2(Number(parts[1]) || 0);
        return `${hh}:${mm}`;
      }
      return safe(v);
    }

    let frac = null;

    // fraction-of-day (common)
    if (n >= 0 && n <= 2) {
      frac = n;
    } else if (n > 2 && n < 90000) {
      // serial date-time -> time fraction
      frac = n - Math.floor(n);
    }

    if (frac == null || frac < 0) return safe(v);

    const mins = Math.round(frac * 1440);
    const hh = Math.floor(mins / 60);
    const mm = mins % 60;
    return `${pad2(hh)}:${pad2(mm)}`;
  };

  const normH = (s) => String(s || '').trim().toLowerCase().replace(/\s+/g, ' ');

  const tables = [];

  const unwrapRow = (r) => {
    if (!r) return null;
    if (r.raw_row && typeof r.raw_row === 'object') return r.raw_row;
    if (r.payload && typeof r.payload === 'object') return r.payload;
    if (r.raw && typeof r.raw === 'object') return r.raw;
    return r;
  };

  // Accept either legacy array form (hrRows = rows array) OR new form (hrRows = { tables: [...] }).
  if (Array.isArray(hrRows)) {
    const rows = hrRows.map(unwrapRow).filter(r => r && typeof r === 'object');
    if (rows.length) tables.push({ header_rows: [], header_columns: [], rows });
  } else if (hrRows && typeof hrRows === 'object') {
    const tList = Array.isArray(hrRows.tables) ? hrRows.tables : [hrRows];
    for (const t of tList) {
      const headerRows = Array.isArray(t?.header_rows)
        ? t.header_rows.map(row => Array.isArray(row) ? row.map(safe) : [])
        : [];
      const headerCols = Array.isArray(t?.header_columns) ? t.header_columns.map(safe) : [];

      const rowsRaw = Array.isArray(t?.rows_json)
        ? t.rows_json
        : (Array.isArray(t?.rows) ? t.rows : []);
      const rows = rowsRaw.map(unwrapRow).filter(r => r && typeof r === 'object');

      if (rows.length) tables.push({ header_rows: headerRows, header_columns: headerCols, rows });
    }
  }

  const hasTables = tables.length > 0;

  const calcColCount = (hdrRows, hdrCols, rows) => {
    let n = 0;

    if (Array.isArray(hdrRows) && hdrRows.length) {
      for (const r of hdrRows) {
        if (Array.isArray(r)) n = Math.max(n, r.length);
      }
      if (n > 0) return n;
    }

    if (Array.isArray(hdrCols) && hdrCols.length) return hdrCols.length;

    const firstWithRaw = (Array.isArray(rows) ? rows : []).find(r => Array.isArray(r?.raw_columns));
    if (firstWithRaw && Array.isArray(firstWithRaw.raw_columns)) return firstWithRaw.raw_columns.length;

    // Last resort: keys (only if no raw_columns exists anywhere)
    const firstRow = (Array.isArray(rows) ? rows : [])[0];
    return firstRow && typeof firstRow === 'object' ? Object.keys(firstRow).length : 0;
  };

  const buildColumnMeta = (hdrRows, hdrCols, colCount, rows) => {
    const hdrRowsArr = Array.isArray(hdrRows) ? hdrRows : [];
    const hdrColsArr = Array.isArray(hdrCols) ? hdrCols : [];

    const labelAt = (i) => {
      for (let r = hdrRowsArr.length - 1; r >= 0; r--) {
        const row = hdrRowsArr[r];
        if (!Array.isArray(row)) continue;
        const v = (i < row.length) ? String(row[i] ?? '').trim() : '';
        if (v) return v;
      }
      if (i < hdrColsArr.length) {
        const v2 = String(hdrColsArr[i] ?? '').trim();
        if (v2) return v2;
      }
      const firstWithRaw = (Array.isArray(rows) ? rows : []).find(rr => Array.isArray(rr?.raw_columns));
      if (firstWithRaw && colCount > 0) return `Column ${i + 1}`;
      return '';
    };

    const meta = [];
    for (let i = 0; i < colCount; i++) {
      const label = labelAt(i);
      const hn = normH(label);

      let kind = 'text';

      // ✅ Requested: treat From/To/Start/End as time columns
      if (
        hn === 'from' || hn === 'to' || hn === 'start' || hn === 'end' ||
        hn === 'from time' || hn === 'to time' || hn === 'start time' || hn === 'end time' ||
        hn.endsWith(' from') || hn.endsWith(' to') || hn.endsWith(' start') || hn.endsWith(' end')
      ) {
        kind = 'time';
      }

      meta.push({ label, hn, kind });
    }
    return meta;
  };

  const renderThead = (hdrRows, hdrCols, colCount, rows) => {
    if (Array.isArray(hdrRows) && hdrRows.length) {
      const trs = hdrRows.map(rowArr => {
        const cells = [];
        for (let i = 0; i < colCount; i++) {
          const v = (Array.isArray(rowArr) && i < rowArr.length) ? rowArr[i] : '';
          cells.push(`<th>${esc(v)}</th>`);
        }
        return `<tr>${cells.join('')}</tr>`;
      }).join('');
      return `<thead>${trs}</thead>`;
    }

    if (Array.isArray(hdrCols) && hdrCols.length) {
      const cells = hdrCols.map(c => `<th>${esc(c)}</th>`).join('');
      return `<thead><tr>${cells}</tr></thead>`;
    }

    // If no stored headers, prefer "Column 1..N" when raw_columns exists
    const firstWithRaw = (Array.isArray(rows) ? rows : []).find(r => Array.isArray(r?.raw_columns));
    if (firstWithRaw && colCount > 0) {
      const cols = Array.from({ length: colCount }, (_, i) => `Column ${i + 1}`);
      const cells = cols.map(c => `<th>${esc(c)}</th>`).join('');
      return `<thead><tr>${cells}</tr></thead>`;
    }

    // Absolute last resort: keys from first row
    const firstRow = (Array.isArray(rows) ? rows : [])[0] || {};
    const keys = Object.keys(firstRow);
    const cells = keys.map(k => `<th>${esc(k)}</th>`).join('');
    return `<thead><tr>${cells}</tr></thead>`;
  };

  const renderTbody = (rows, colCount, colMeta) => {
    const bodyRows = (Array.isArray(rows) ? rows : []).map(r => {
      // Canonical: render from raw_columns if present
      const arr = Array.isArray(r?.raw_columns)
        ? r.raw_columns
        : (Array.isArray(r?.payload_json?.raw_columns) ? r.payload_json.raw_columns : null);

      if (arr) {
        const cells = [];
        for (let i = 0; i < colCount; i++) {
          const v0 = (i < arr.length) ? arr[i] : '';
          const kind = (colMeta && colMeta[i] && colMeta[i].kind) ? colMeta[i].kind : 'text';

          let out = '';
          if (kind === 'time') out = excelToHHMM(v0);
          else out = safe(v0);

          cells.push(`<td>${esc(out)}</td>`);
        }
        return `<tr>${cells.join('')}</tr>`;
      }

      // Fallback only if raw_columns absent (legacy/unexpected)
      const keys = Object.keys(r || {});
      const cells = [];
      for (let i = 0; i < colCount; i++) {
        const k = keys[i];
        const v = (k != null) ? (r?.[k]) : '';
        cells.push(`<td>${esc(v)}</td>`);
      }
      return `<tr>${cells.join('')}</tr>`;
    }).join('');

    return `<tbody>${bodyRows}</tbody>`;
  };

  const renderTable = (t, idx) => {
    const hdrRows = Array.isArray(t.header_rows) ? t.header_rows : [];
    const hdrCols = Array.isArray(t.header_columns) ? t.header_columns : [];
    const rows = Array.isArray(t.rows) ? t.rows : [];
    const colCount = calcColCount(hdrRows, hdrCols, rows);

    const sep = (idx > 0) ? `<div class="page-break"></div>` : '';

    if (rows.length === 0) return '';

    const colMeta = buildColumnMeta(hdrRows, hdrCols, colCount, rows);
    const theadHtml = renderThead(hdrRows, hdrCols, colCount, rows);
    const tbodyHtml = renderTbody(rows, colCount, colMeta);

    return `${sep}
      <table class="hr">
        ${theadHtml}
        ${tbodyHtml}
      </table>
    `;
  };

  const tablesHtml = hasTables ? tables.map(renderTable).join('') : '';
  const hasAnyRows = hasTables && tables.some(t => Array.isArray(t.rows) && t.rows.length > 0);

  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>HealthRoster Report</title>
  <style>
    @page { size: A4 landscape; }

    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 11px; margin: 24px; }
    h1 { font-size: 18px; margin-bottom: 4px; }
    h2 { font-size: 13px; margin-top: 4px; margin-bottom: 12px; }
    .meta { margin-bottom: 12px; font-size: 10px; }
    .meta span { display: inline-block; margin-right: 16px; }

    .page-break { break-before: page; page-break-before: always; }

    table.hr { width: 100%; border-collapse: collapse; font-size: 9px; table-layout: fixed; }
    table.hr thead { display: table-header-group; }
    table.hr th, table.hr td { border: 1px solid #ccc; padding: 3px 4px; text-align: left; vertical-align: top; word-break: break-word; overflow-wrap: anywhere; }
    table.hr th { background: #f2f2f2; }

    .no-data { margin-top: 12px; font-style: italic; }
  </style>
</head>
<body>
  <h1>HealthRoster report</h1>
  <h2>Invoice ${esc(invNo)}</h2>
  <div class="meta">
    <span><strong>Client:</strong> ${esc(clientName)}</span>
    <span><strong>Issued:</strong> ${esc(issued)}</span>
  </div>

  ${hasAnyRows ? tablesHtml : `
    <div class="no-data">No HealthRoster rows captured for this invoice.</div>
  `}
</body>
</html>
  `;
}


function buildNhspReportHTML(inv, header, nhspData) {
  const safe = (v) => (v == null ? '' : String(v));
  const h = header || {};
  const clientName = safe(h.client_name || h.client || '');
  const invNo = safe(inv.invoice_no || '');
  const issued = safe(inv.issued_at_utc || '');

  const pad2 = (n) => String(n).padStart(2, '0');

  const toNum = (v) => {
    if (typeof v === 'number') return Number.isFinite(v) ? v : null;
    const s = String(v ?? '').trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  };

  // Excel serial date (days since 1899-12-30, with Excel leap-year bug baked into that base)
  const excelSerialToDmy = (v) => {
    const n = toNum(v);
    if (n == null) return safe(v);
    // keep this conservative: serial dates are typically large (~45000+ for 2023+)
    if (n < 20000 || n > 90000) return safe(v);

    const base = Date.UTC(1899, 11, 30, 0, 0, 0, 0);
    const ms = base + Math.round(n) * 86400000;
    if (!Number.isFinite(ms)) return safe(v);

    const d = new Date(ms);
    if (Number.isNaN(d.getTime())) return safe(v);

    const dd = pad2(d.getUTCDate());
    const mm = pad2(d.getUTCMonth() + 1);
    const yy = String(d.getUTCFullYear());
    return `${dd}/${mm}/${yy}`;
  };

  // Excel day-fraction time/duration: 0.375 => 09:00, 0.3125 => 07:30
  const excelFracToHHMM = (v) => {
    const n = toNum(v);
    if (n == null) {
      const s = String(v ?? '').trim();
      if (/^\d{1,2}:\d{2}(:\d{2})?$/.test(s)) {
        const parts = s.split(':');
        const hh = pad2(Number(parts[0]) || 0);
        const mm = pad2(Number(parts[1]) || 0);
        return `${hh}:${mm}`;
      }
      return safe(v);
    }
    // treat 0..2 as fractions of a day (covers durations up to 48h)
    if (n < 0 || n > 2) return safe(v);
    const mins = Math.round(n * 1440);
    const hh = Math.floor(mins / 60);
    const mm = mins % 60;
    return `${pad2(hh)}:${pad2(mm)}`;
  };

  const fmtInt = (v) => {
    const n = toNum(v);
    if (n == null) return safe(v);
    return String(Math.round(n));
  };

  const fmtMoney = (v) => {
    // If already a string like "50.7", keep it as-is per brief
    if (typeof v === 'string') {
      const s = v.trim();
      if (s && Number.isFinite(Number(s))) return s;
      return safe(v);
    }
    const n = toNum(v);
    if (n == null) return safe(v);
    return n.toFixed(2);
  };

  const normH = (s) => String(s || '').trim().toLowerCase().replace(/\s+/g, ' ');

  const tables = [];

  const unwrapRow = (r) => {
    if (!r) return null;
    if (r.raw_row && typeof r.raw_row === 'object') return r.raw_row;
    if (r.raw && typeof r.raw === 'object') return r.raw;
    return r;
  };

  if (Array.isArray(nhspData)) {
    const rows = nhspData.map(unwrapRow).filter(r => r && typeof r === 'object');
    if (rows.length) tables.push({ header_rows: [], header_columns: [], rows });
  } else if (nhspData && typeof nhspData === 'object') {
    const tList = Array.isArray(nhspData.tables) ? nhspData.tables : [nhspData];
    for (const t of tList) {
      const headerRows = Array.isArray(t?.header_rows)
        ? t.header_rows.map(row => Array.isArray(row) ? row.map(safe) : [])
        : [];
      const headerCols = Array.isArray(t?.header_columns) ? t.header_columns.map(safe) : [];
      const rowsRaw = Array.isArray(t?.rows_json)
        ? t.rows_json
        : (Array.isArray(t?.rows) ? t.rows : []);
      const rows = rowsRaw.map(unwrapRow).filter(r => r && typeof r === 'object');
      if (rows.length) tables.push({ header_rows: headerRows, header_columns: headerCols, rows });
    }
  }

  const hasTables = tables.length > 0;

  const calcColCount = (hdrRows, hdrCols, rows) => {
    let n = 0;

    if (Array.isArray(hdrRows) && hdrRows.length) {
      for (const r of hdrRows) {
        if (Array.isArray(r)) n = Math.max(n, r.length);
      }
      if (n > 0) return n;
    }

    if (Array.isArray(hdrCols) && hdrCols.length) return hdrCols.length;

    const firstWithRaw = (Array.isArray(rows) ? rows : []).find(r => Array.isArray(r?.raw_columns));
    if (firstWithRaw && Array.isArray(firstWithRaw.raw_columns)) return firstWithRaw.raw_columns.length;

    const firstRow = (Array.isArray(rows) ? rows : [])[0];
    return firstRow && typeof firstRow === 'object' ? Object.keys(firstRow).length : 0;
  };

  const buildColumnMeta = (hdrRows, hdrCols, colCount, rows) => {
    // Determine the effective column label per index:
    // prefer the last non-empty cell across header_rows (bottom-up), else header_columns, else ''
    const hdrRowsArr = Array.isArray(hdrRows) ? hdrRows : [];
    const hdrColsArr = Array.isArray(hdrCols) ? hdrCols : [];

    const labelAt = (i) => {
      for (let r = hdrRowsArr.length - 1; r >= 0; r--) {
        const row = hdrRowsArr[r];
        if (!Array.isArray(row)) continue;
        const v = (i < row.length) ? String(row[i] ?? '').trim() : '';
        if (v) return v;
      }
      if (i < hdrColsArr.length) {
        const v2 = String(hdrColsArr[i] ?? '').trim();
        if (v2) return v2;
      }
      // last resort: generate "Column N" if raw_columns exist
      const firstWithRaw = (Array.isArray(rows) ? rows : []).find(rr => Array.isArray(rr?.raw_columns));
      if (firstWithRaw && colCount > 0) return `Column ${i + 1}`;
      return '';
    };

    const meta = [];
    for (let i = 0; i < colCount; i++) {
      const label = labelAt(i);
      const hn = normH(label);

      let kind = 'text';
      if (hn.includes('date')) kind = 'date';
      else if (hn === 'start' || hn === 'end' || hn === 'total') kind = 'time';
      else if (hn.includes('break') && hn.includes('minute')) kind = 'break';
      else if (hn === 'commission' || hn.includes('commission') || hn === 'total cost' || hn.includes('total cost')) kind = 'money';

      meta.push({ label, hn, kind });
    }
    return meta;
  };

  const renderThead = (hdrRows, hdrCols, colCount, rows) => {
    if (Array.isArray(hdrRows) && hdrRows.length) {
      const trs = hdrRows.map(rowArr => {
        const cells = [];
        for (let i = 0; i < colCount; i++) {
          const v = (Array.isArray(rowArr) && i < rowArr.length) ? rowArr[i] : '';
          cells.push(`<th>${escapeHtml(safe(v))}</th>`);
        }
        return `<tr>${cells.join('')}</tr>`;
      }).join('');
      return `<thead>${trs}</thead>`;
    }

    if (Array.isArray(hdrCols) && hdrCols.length) {
      const cells = hdrCols.map(col => `<th>${escapeHtml(safe(col))}</th>`).join('');
      return `<thead><tr>${cells}</tr></thead>`;
    }

    const firstWithRaw = (Array.isArray(rows) ? rows : []).find(r => Array.isArray(r?.raw_columns));
    if (firstWithRaw && colCount > 0) {
      const cols = Array.from({ length: colCount }, (_, i) => `Column ${i + 1}`);
      const cells = cols.map(col => `<th>${escapeHtml(safe(col))}</th>`).join('');
      return `<thead><tr>${cells}</tr></thead>`;
    }

    const firstRow = (Array.isArray(rows) ? rows : [])[0] || {};
    const keys = Object.keys(firstRow);
    const cells = keys.map(col => `<th>${escapeHtml(safe(col))}</th>`).join('');
    return `<thead><tr>${cells}</tr></thead>`;
  };

  const renderTbody = (rows, colCount, colMeta) => {
    const rowsHtml = (Array.isArray(rows) ? rows : []).map(r => {
      const arr = Array.isArray(r?.raw_columns)
        ? r.raw_columns
        : (Array.isArray(r?.payload_json?.raw_columns) ? r.payload_json.raw_columns : null);

      const cells = [];

      if (arr) {
        for (let i = 0; i < colCount; i++) {
          const v0 = (i < arr.length) ? arr[i] : '';
          const k = colMeta?.[i]?.kind || 'text';

          let out = '';
          if (k === 'date') out = excelSerialToDmy(v0);
          else if (k === 'time') out = excelFracToHHMM(v0);
          else if (k === 'break') out = fmtInt(v0);
          else if (k === 'money') out = fmtMoney(v0);
          else out = safe(v0);

          cells.push(`<td>${escapeHtml(out)}</td>`);
        }
        return `<tr>${cells.join('')}</tr>`;
      }

      // fallback only if raw_columns absent
      const keys = Object.keys(r || {});
      for (let i = 0; i < colCount; i++) {
        const kk = keys[i];
        const vv = (kk != null) ? (r?.[kk]) : '';
        cells.push(`<td>${escapeHtml(safe(vv))}</td>`);
      }
      return `<tr>${cells.join('')}</tr>`;
    }).join('');

    return `<tbody>${rowsHtml}</tbody>`;
  };

  const renderTable = (t, idx) => {
    const hdrRows = Array.isArray(t.header_rows) ? t.header_rows : [];
    const hdrCols = Array.isArray(t.header_columns) ? t.header_columns : [];
    const rows = Array.isArray(t.rows) ? t.rows : [];
    const colCount = calcColCount(hdrRows, hdrCols, rows);

    if (!rows.length) return '';

    const colMeta = buildColumnMeta(hdrRows, hdrCols, colCount, rows);

    // Widen Assignment + Commission columns (as requested) via colgroup.
    // Keep everything else auto within table-layout: fixed.
    const idxAssignment = colMeta.findIndex(m => m && m.hn === 'assignment');
    const idxCommission = colMeta.findIndex(m => m && (m.hn === 'commission' || m.hn.includes('commission')));

    const colgroup = (() => {
      const cols = [];
      for (let i = 0; i < colCount; i++) {
        let w = '';
        if (i === idxAssignment) w = '140px';
        if (i === idxCommission) w = '90px';
        cols.push(`<col${w ? ` style="width:${w};"` : ''}>`);
      }
      return `<colgroup>${cols.join('')}</colgroup>`;
    })();

    const theadHtml = renderThead(hdrRows, hdrCols, colCount, rows);
    const tbodyHtml = renderTbody(rows, colCount, colMeta);

    const sep = (idx > 0) ? `<div class="page-break"></div>` : '';

    return `${sep}
      <table class="nhsp">
        ${colgroup}
        ${theadHtml}
        ${tbodyHtml}
      </table>
    `;
  };

  return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>NHSP Attachment</title>
  <style>
    @page { size: A4 landscape; margin: 0; }
    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 10px;
      margin: 0;
    }
    h1 { font-size: 18px; margin-bottom: 4px; }
    h2 { font-size: 13px; margin-top: 4px; margin-bottom: 12px; }
    .meta { margin-bottom: 12px; font-size: 10px; }
    .meta span { display: inline-block; margin-right: 16px; }

    .page-break { break-before: page; page-break-before: always; }

    table.nhsp {
      width: 100%;
      border-collapse: collapse;
      font-size: 8.5px;
      table-layout: fixed;
    }

    table.nhsp thead { display: table-header-group; }

    table.nhsp th,
    table.nhsp td {
      border: 1px solid #ccc;
      padding: 2px 3px;
      text-align: left;
      vertical-align: top;
      word-break: break-word;
      overflow-wrap: anywhere;
    }

    table.nhsp th { background: #f2f2f2; }

    .no-data { margin-top: 12px; font-style: italic; }
  </style>
</head>
<body>
  <h1>NHSP attachment</h1>
  <h2>Invoice ${escapeHtml(invNo)}</h2>
  <div class="meta">
    <span><strong>Client:</strong> ${escapeHtml(clientName)}</span>
    <span><strong>Issued:</strong> ${escapeHtml(issued)}</span>
  </div>

  ${hasTables ? tables.map(renderTable).join('') : `
    <div class="no-data">No NHSP rows captured for this invoice.</div>
  `}
</body>
</html>
  `;
}


  // ==========================================================
  // UPDATED: buildHTML (render FREEFORM reference rows as single-column list/table)
  // ==========================================================
  function buildHTML(payload) {
  const {
    header = {},
    meta: payloadMeta = {},
    invoice_no = "",
    issued_at_utc,
    due_at_utc,
    totals = { subtotal_ex_vat: 0, vat_amount: 0, total_inc_vat: 0 },
    items = [],
    reference_rows = []
   } = payload || {};

  const isDraftInvoice = !(issued_at_utc && String(issued_at_utc).trim());

  const clientName = pick(header, "client_name", "");

  const clientAddress = (pick(header, "client_invoice_address", "") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);

  const vatChargeable = !!pick(header, "vat_chargeable", true);
  const appliedVatPct = Number(pick(header, "applied_vat_rate_pct", 0));
  const termsDays = pick(header, "payment_terms_days", null);
  const bank = pick(header, "bank", {}) || {};

  const agencyName =
    pick(header, "agency_name", "") ||
    pick(header, "agency_display_name", "") ||
    pick(header, "company_name", "") ||
    "";

  const agencyLogoUrl =
    pick(header, "agency_logo_url", "") ||
    pick(header, "logo_url", "") ||
    "";

  const registeredAddressLines = (pick(header, "registered_address", "") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);

  const companyRegNumber =
    pick(header, "company_reg_number", "") ||
    pick(header, "company_registration_number", "") ||
    "";

  const DEFAULT_VAT_REG = "363 6805 80";
  const vatReg = pick(header, "vat_registration_number", "") || DEFAULT_VAT_REG;

  const stationeryUrl = pick(header, "stationery_url", "");

  const defaultMargins = stationeryUrl
    ? { top: 32, right: 12, bottom: 20, left: 12 }
    : { top: 18, right: 12, bottom: 34, left: 12 };
  const mgIn = pick(header, "stationery_margins_mm", {}) || {};
  const mg = {
    top: Number(pick(mgIn, "top", defaultMargins.top)),
    right: Number(pick(mgIn, "right", defaultMargins.right)),
    bottom: Number(pick(mgIn, "bottom", defaultMargins.bottom)),
    left: Number(pick(mgIn, "left", defaultMargins.left))
  };

  const hideBankFooter = !!pick(header, "hide_bank_footer", false);

  // Reserve space so fixed header/footer NEVER collide with flowing content.
  // Header repeats on every page; footer repeats when shown.
  const FIXED_HEADER_MM = 52; // tuned for your header block height (logo/name + invoice + bill-to + meta)
  const FIXED_FOOTER_MM = hideBankFooter ? 0 : 26;

  const pageTopMm = Number(mg.top) + FIXED_HEADER_MM;
  const pageBottomMm = Number(mg.bottom) + FIXED_FOOTER_MM;

  const headerPo = pick(header, "po_number", null);
  const itemPos = items.map((i) => i?.meta?.po_number).filter(Boolean);
  const uniquePos = Array.from(new Set([...(headerPo ? [headerPo] : []), ...itemPos]));
  const poNo = uniquePos.length === 1 ? uniquePos[0] : "";

  const showVatCols = vatChargeable && (appliedVatPct > 0 || Number(totals.vat_amount) > 0);

  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };
  const round2 = (v) => Math.round(num(v) * 100) / 100;
  const fmtQty = (v, decimals = 2) => {
    const n = num(v);
    if (Math.abs(n - Math.round(n)) < 1e-9) return String(Math.round(n));
    return n.toFixed(decimals);
  };
  const eqRate = (a, b) => round2(a) === round2(b);

  const getLineTypeNorm = (it) => {
    const m = it?.meta || {};
    return String(m.line_type_norm || m.line_type || "").toUpperCase();
  };

  // ===== AMENDMENT: treat HOURS_* as HOURS everywhere =====
  const isHoursType = (t) => {
    const tt = String(t || "").toUpperCase();
    return tt === "HOURS" || tt.startsWith("HOURS_");
  };
  // ========================================================

  const getTimesheetId = (it) => {
    return (it && it.timesheet_id != null) ? String(it.timesheet_id)
      : (it?.meta?.timesheet_id != null ? String(it.meta.timesheet_id) : null);
  };

  const isAdjustmentItem = (it) => {
    const t = getLineTypeNorm(it);
    const tsId = getTimesheetId(it);
    return (!tsId || t === "ADJUSTMENT");
  };

  const DEFAULT_LABELS = { day: "Day", night: "Night", sat: "Sat", sun: "Sun", bh: "BH" };
  const bucketLabelOf = (labels, key) => String((labels && labels[key]) || DEFAULT_LABELS[key] || key);

  const groupMap = new Map();
  const adjustments = [];

  for (const it of (items || [])) {
    if (isAdjustmentItem(it)) {
      adjustments.push(it);
      continue;
    }
    const tsId = getTimesheetId(it);
    if (!tsId) {
      adjustments.push(it);
      continue;
    }
    if (!groupMap.has(tsId)) groupMap.set(tsId, { tsId, items: [] });
    groupMap.get(tsId).items.push(it);
  }

  const groups = Array.from(groupMap.values()).sort((a, b) => {
    // ===== AMENDMENT: find HOURS or HOURS_* meta for ordering =====
    const aMeta = (a.items.find(x => isHoursType(getLineTypeNorm(x)))?.meta) || a.items[0]?.meta || {};
    const bMeta = (b.items.find(x => isHoursType(getLineTypeNorm(x)))?.meta) || b.items[0]?.meta || {};
    // =============================================================
    const awe = String(aMeta.week_ending_date || aMeta.week_ending || aMeta.weekEnding || "");
    const bwe = String(bMeta.week_ending_date || bMeta.week_ending || bMeta.weekEnding || "");
    if (awe !== bwe) return awe < bwe ? 1 : -1;
    const ac = String(aMeta.candidate_display || aMeta.candidate || "");
    const bc = String(bMeta.candidate_display || bMeta.candidate || "");
    if (ac !== bc) return ac.localeCompare(bc);
    return String(a.tsId).localeCompare(String(b.tsId));
  });

  const groupBreakdownTableHtml = (grp) => {
    const its = grp.items || [];
    const hoursItem = its.find((x) => {
      const t = getLineTypeNorm(x);
      return (t === "HOURS" || String(t || "").startsWith("HOURS_"));
    }) || null;

    const metaBase = (hoursItem?.meta || its[0]?.meta || {}) || {};
    const labels = (metaBase.bucket_labels && typeof metaBase.bucket_labels === "object") ? metaBase.bucket_labels : DEFAULT_LABELS;

    const hDay = num(metaBase.hours_day);
    const hNgt = num(metaBase.hours_night);
    const hSat = num(metaBase.hours_sat);
    const hSun = num(metaBase.hours_sun);
    const hBh  = num(metaBase.hours_bh);

    const rDay = num(metaBase.charge_day);
    const rNgt = num(metaBase.charge_night);
    const rSat = num(metaBase.charge_sat);
    const rSun = num(metaBase.charge_sun);
    const rBh  = num(metaBase.charge_bh);

    const groupFlag = !!pick(payloadMeta, "group_nightsat_sunbh", false);
    const canMerge = groupFlag && eqRate(rNgt, rSat) && eqRate(rSun, rBh);

    const rows = [];

    const pushRow = (desc, qty, unit, charge) => {
      const q = num(qty);
      const u = num(unit);
      const c = round2(charge);
      if (c === 0) return;
      rows.push({ desc: String(desc || ""), qty: q, unit: u, charge: c });
    };

    if (hoursItem) {
      if (canMerge) {
        const nightSatQty = hNgt + hSat;
        const sunBhQty = hSun + hBh;
        pushRow(bucketLabelOf(labels, "day"), hDay, rDay, hDay * rDay);
        pushRow(`${bucketLabelOf(labels, "night")}/${bucketLabelOf(labels, "sat")}`, nightSatQty, rNgt, nightSatQty * rNgt);
        pushRow(`${bucketLabelOf(labels, "sun")}/${bucketLabelOf(labels, "bh")}`, sunBhQty, rSun, sunBhQty * rSun);
      } else {
        pushRow(bucketLabelOf(labels, "day"), hDay, rDay, hDay * rDay);
        pushRow(bucketLabelOf(labels, "night"), hNgt, rNgt, hNgt * rNgt);
        pushRow(bucketLabelOf(labels, "sat"), hSat, rSat, hSat * rSat);
        pushRow(bucketLabelOf(labels, "sun"), hSun, rSun, hSun * rSun);
        pushRow(bucketLabelOf(labels, "bh"), hBh, rBh, hBh * rBh);
      }
    }

    for (const it of its) {
      const t = getLineTypeNorm(it);
      // ===== AMENDMENT: skip HOURS and HOURS_* (prevents duplicate/incorrect rows) =====
      if (isHoursType(t)) continue;
      // ============================================================================
      if (t === "ADJUSTMENT") continue;

      const m = it.meta || {};
      const totalEx = num(it.total_ex_vat);

      const unitLabel = String(m.unit_label || it.description || t || "").trim() || t;

      let qty = null;
      let unitCharge = null;

      if (t === "MILEAGE") {
        if (m?.mileage && m.mileage.mileage_units != null) qty = num(m.mileage.mileage_units);
        else if (m.mileage_units != null) qty = num(m.mileage_units);
        else if (m.qty != null) qty = num(m.qty);

        if (m?.mileage && m.mileage.charge_rate != null) unitCharge = num(m.mileage.charge_rate);
        else if (m.unit_charge_ex_vat != null) unitCharge = num(m.unit_charge_ex_vat);
      } else if (t.startsWith("ADDITIONAL_RATE")) {
        if (m?.units && m.units.unit_count != null) qty = num(m.units.unit_count);
        else if (m.unit_count != null) qty = num(m.unit_count);
        else if (m.qty != null) qty = num(m.qty);

        if (m?.units && m.units.charge_rate != null) unitCharge = num(m.units.charge_rate);
        else if (m.unit_charge_ex_vat != null) unitCharge = num(m.unit_charge_ex_vat);
      } else if (t.startsWith("EXPENSE")) {
        qty = (m.qty != null) ? num(m.qty) : 1;
        unitCharge = (m.unit_charge_ex_vat != null)
          ? num(m.unit_charge_ex_vat)
          : (qty !== 0 ? totalEx / qty : totalEx);
      } else {
        qty = (m.qty != null) ? num(m.qty) : 1;
        unitCharge = (m.unit_charge_ex_vat != null)
          ? num(m.unit_charge_ex_vat)
          : (qty !== 0 ? totalEx / qty : totalEx);
      }

      if (qty == null) qty = 1;
      if (round2(qty) === 0) continue;

      if (unitCharge == null) unitCharge = (qty !== 0) ? (totalEx / qty) : totalEx;

      pushRow(unitLabel, qty, unitCharge, totalEx);
    }

    if (!rows.length) return "";

    const head = `
      <table class="breakdown">
        <thead>
          <tr>
            <th class="b-desc">Unit Description</th>
            <th class="b-qty">Quantity</th>
            <th class="b-unit">Unit Charge (ex VAT)</th>
            <th class="b-charge">Charge (ex VAT)</th>
          </tr>
        </thead>
        <tbody>
    `;

    const body = rows.map(r => `
      <tr>
        <td class="b-desc">${escapeHtml(r.desc)}</td>
        <td class="b-qty mono">${fmtQty(r.qty, 2)}</td>
        <td class="b-unit mono">${fmtGBP(r.unit)}</td>
        <td class="b-charge mono">${fmtGBP(r.charge)}</td>
      </tr>
    `).join("");

    const tail = `
        </tbody>
      </table>
    `;

    return head + body + tail;
  };

  const refsByTsId = new Map();
  for (const r of (reference_rows || [])) {
    const tsId = (r && r.timesheet_id != null) ? String(r.timesheet_id) : null;
    if (!tsId) continue;
    if (!refsByTsId.has(tsId)) refsByTsId.set(tsId, []);
    refsByTsId.get(tsId).push(r);
  }

  const refsTableHtmlForTs = (tsId) => {
    const refs = refsByTsId.get(String(tsId || "")) || [];
    if (!refs.length) return "";

    const isStructured = (r) => {
      const d = (r?.day_ymd != null) ? String(r.day_ymd).trim() : '';
      const st = (r?.start_utc != null) ? String(r.start_utc).trim() : '';
      const en = (r?.end_utc != null) ? String(r.end_utc).trim() : '';
      return !!(d || st || en);
    };

    const structured = [];
    const freeform = [];
    refs.forEach((r) => {
      (isStructured(r) ? structured : freeform).push(r);
    });

    const hasRefTxt = (r) => (r?.current_reference != null) && String(r.current_reference).trim();
    const refText = (r) => hasRefTxt(r) ? String(r.current_reference).trim() : (r?.is_required ? "MISSING" : "—");
    const refCls = (r) => (!hasRefTxt(r) && r?.is_required) ? "ref-missing" : "";

    const structuredTable = structured.length ? (() => {
      const rowsHtml = structured.map((r) => {
        const day = r?.day_ymd ? fmtDateGB(r.day_ymd) : "";
        const st = r?.start_utc ? fmtUKTime(r.start_utc) : "";
        const en = r?.end_utc ? fmtUKTime(r.end_utc) : "";
        return `
        <tr>
          <td class="r-day mono">${escapeHtml(day)}</td>
          <td class="r-time mono">${escapeHtml(st)}</td>
          <td class="r-time mono">${escapeHtml(en)}</td>
          <td class="r-ref mono ${refCls(r)}">${escapeHtml(refText(r))}</td>
        </tr>
      `;
      }).join("");

      return `
        <table class="refs">
          <thead>
            <tr>
              <th class="r-day">Day</th>
              <th class="r-time">Start</th>
              <th class="r-time">End</th>
              <th class="r-ref">Reference</th>
            </tr>
          </thead>
          <tbody>
            ${rowsHtml}
          </tbody>
        </table>
      `;
    })() : "";

    const freeformTable = freeform.length ? (() => {
      const rowsHtml = freeform.map((r) => `
        <tr>
          <td class="r-ref mono ${refCls(r)}">${escapeHtml(refText(r))}</td>
        </tr>
      `).join("");

      return `
        <table class="refs refs-freeform">
          <thead>
            <tr>
              <th class="r-ref">Reference</th>
            </tr>
          </thead>
          <tbody>
            ${rowsHtml}
          </tbody>
        </table>
      `;
    })() : "";

    return `
      <div class="refs-wrap">
        <div class="refs-title">Booking references</div>
        ${structuredTable || ""}
        ${freeformTable ? `<div style="height:6px;"></div>${freeformTable}` : ""}
      </div>
    `;
  };

  const groupRowsHtml = groups.map((grp, idx) => {
    const its = grp.items || [];
    // ===== AMENDMENT: metaFirst should prefer HOURS or HOURS_* =====
    const metaFirst = (its.find(x => isHoursType(getLineTypeNorm(x)))?.meta) || its[0]?.meta || {};
    // ============================================================
    const we = metaFirst.week_ending_date || metaFirst.week_ending || metaFirst.weekEnding || null;

    const sublineParts = [
      metaFirst.candidate_display || metaFirst.candidate || null,
      metaFirst.role || metaFirst.job_title || null,
      metaFirst.hospital || metaFirst.hospital_norm || null,
      metaFirst.ward || metaFirst.ward_norm || null,
      we ? `W/E ${fmtDateGB(we)}` : null,
      metaFirst.po_number ? `PO ${metaFirst.po_number}` : null
    ].filter(Boolean).join(" • ");

    const grpEx = round2(its.reduce((a, it) => a + num(it.total_ex_vat), 0));
    const grpVat = round2(its.reduce((a, it) => a + num(it.vat_amount), 0));
    const grpInc = round2(its.reduce((a, it) => a + num(it.total_inc_vat), 0));

    const title = escapeHtml(metaFirst.candidate_display || metaFirst.candidate || `Timesheet ${idx + 1}`);
    const breakdown = groupBreakdownTableHtml(grp);

    const isNhspGroup = (its || []).some((it) => {
      const m = (it && it.meta && typeof it.meta === "object") ? it.meta : {};
      const basis = String(m.basis || m.cur_basis || m.timesheet_basis || "").toUpperCase();
      const route = String(m.route_type || m.route || "").toUpperCase();
      const flagNhsp = (m.client_is_nhsp === true) || (m.is_nhsp === true);
      return flagNhsp || basis === "NHSP" || basis === "NHSP_ADJUSTMENT" || route.includes("NHSP");
    });

    const refsHtml = isNhspGroup ? "" : refsTableHtmlForTs(grp.tsId);

    return `
      <tr class="line">
        <td class="desc">
          <div class="desc-title">${title}</div>
          <div class="desc-meta">
            ${escapeHtml(sublineParts)}
            ${breakdown ? `<div class="breakdown-wrap">${breakdown}</div>` : ""}
            ${refsHtml || ""}
          </div>
        </td>
        <td class="money exvat">${fmtGBP(grpEx)}</td>
        ${showVatCols ? `<td class="money vat">${fmtGBP(grpVat)}</td>` : ""}
        <td class="money totalinc">${fmtGBP(grpInc)}</td>
      </tr>
    `;
  }).join("");

  const adjustmentsRowsHtml = (adjustments && adjustments.length)
    ? (() => {
        const head = `
          <tr class="section-row">
            <td class="desc" colspan="${showVatCols ? 4 : 3}">
              <div class="section-title">Adjustments</div>
            </td>
          </tr>
        `;

        const rows = adjustments.map((it, idx) => {
          const meta = it.meta || {};
          const desc = escapeHtml(it.description || meta.unit_label || `Adjustment ${idx + 1}`);
          const sub = [
            meta.po_number ? `PO ${meta.po_number}` : null
          ].filter(Boolean).join(" • ");

          return `
            <tr class="line">
              <td class="desc">
                <div class="desc-title">${desc}</div>
                ${sub ? `<div class="desc-meta">${escapeHtml(sub)}</div>` : ""}
              </td>
              <td class="money exvat">${fmtGBP(it.total_ex_vat)}</td>
              ${showVatCols ? `<td class="money vat">${fmtGBP(it.vat_amount)}</td>` : ""}
              <td class="money totalinc">${fmtGBP(it.total_inc_vat)}</b></td>
            </tr>
          `;
        }).join("");

        return head + rows;
      })()
    : "";

  const lineRows = (groupRowsHtml || adjustmentsRowsHtml)
    ? (groupRowsHtml + adjustmentsRowsHtml)
    : "";

      return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Invoice ${escapeHtml(invoice_no || "")}</title>
  <style>
    /* Body must NOT reserve header/footer space here.
       Puppeteer displayHeaderFooter + pdf margins will handle that. */
@page { size: A4; }


    html, body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif;
      color: #111;
      font-size: 11px;
      line-height: 1.35;
      -webkit-print-color-adjust: exact;
      margin: 0;
      padding: 0;
    }

    .stationery {
      position: fixed;
      inset: 0;
      z-index: 0;
      background-repeat: no-repeat;
      background-position: center;
      background-size: cover;
      opacity: 1;
      pointer-events: none;
    }

    .wrap { position: relative; z-index: 1; width: 100%; }

    .mono { font-variant-numeric: tabular-nums; }

    .lines {
      width: 100%;
      border-collapse: collapse;
      border: 1px solid #e5e7eb;
      border-radius: 10px;
      overflow: hidden;
    }
    .lines thead th {
      background: #f9fafb;
      padding: 8px 10px;
      text-align: left;
      font-weight: 600;
    }
    .lines th, .lines td {
      border: 1px solid #e5e7eb;
      padding: 8px 10px;
      vertical-align: top;
    }
    .lines thead th.money, .lines td.money { text-align: right; }
    .desc-title { font-weight: 600; margin-bottom: 2px; }
    .desc-meta { color: #555; }
    .money { font-variant-numeric: tabular-nums; }
    .lines tfoot td { background: #fcfcfd; font-weight: 600; }
    .lines tfoot .label { text-align: right; color: #333; font-weight: 600; }

    .breakdown-wrap { margin-top: 6px; }
    table.breakdown {
      width: 100%;
      border-collapse: collapse;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      overflow: hidden;
      font-size: 10px;
      background: #fff;
    }
    table.breakdown th, table.breakdown td {
      border: 1px solid #e5e7eb;
      padding: 6px 8px;
      vertical-align: top;
    }
    table.breakdown thead th {
      background: #f9fafb;
      font-weight: 600;
      text-align: left;
    }
    table.breakdown .b-qty,
    table.breakdown .b-unit,
    table.breakdown .b-charge { text-align: right; white-space: nowrap; }
    table.breakdown .b-desc { text-align: left; }

    .refs-wrap { margin-top: 6px; }
    .refs-title { font-weight: 600; margin: 2px 0 4px; color: #333; }
    table.refs {
      width: 100%;
      border-collapse: collapse;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      overflow: hidden;
      font-size: 10px;
      background: #fff;
    }
    table.refs th, table.refs td {
      border: 1px solid #e5e7eb;
      padding: 6px 8px;
      vertical-align: top;
    }
    table.refs thead th {
      background: #f9fafb;
      font-weight: 600;
      text-align: left;
    }
    table.refs .r-day { width: 90px; white-space: nowrap; }
    table.refs .r-time { width: 60px; white-space: nowrap; text-align: right; }
    table.refs .r-ref { text-align: left; }
    table.refs td.r-time { text-align: right; }
    table.refs.refs-freeform .r-ref { width: auto; }
    .ref-missing { color: #b91c1c; font-weight: 700; }

    .section-row td {
      background: #f9fafb;
      border-left: 1px solid #e5e7eb;
      border-right: 1px solid #e5e7eb;
    }
    .section-title { font-weight: 700; color: #333; }
 .draft-watermark {
  position: fixed;
  left: 0;
  right: 0;
  top: 45%;
  text-align: center;
  font-size: 64px;
  font-weight: 800;
  letter-spacing: 2px;
  color: rgba(0,0,0,0.12);
  transform: rotate(-18deg);
  z-index: 2;
  pointer-events: none;
  user-select: none;
}

  </style>
</head>
<body>
  ${stationeryUrl ? `<div class="stationery" style="background-image:url('${escapeUrl(stationeryUrl)}');"></div>` : ""}

  ${isDraftInvoice ? `<div class="draft-watermark">DRAFT INVOICE</div>` : ""}

  <div class="wrap">

    <table class="lines">
      <thead>
        <tr>
          <th>Description</th>
          <th class="money">Ex VAT</th>
          ${showVatCols ? `<th class="money">VAT</th>` : ""}
          <th class="money">Total</th>
        </tr>
      </thead>
      <tbody>
        ${lineRows || `<tr><td colspan="${showVatCols ? 4 : 3}">No lines.</td></tr>`}
      </tbody>
      <tfoot>
        <tr>
          <td class="label">Subtotal (ex VAT)</td>
          <td class="money mono">${fmtGBP(totals.subtotal_ex_vat)}</td>
          ${showVatCols ? `<td></td>` : ""}
          <td></td>
        </tr>
        ${showVatCols ? `
        <tr>
          <td class="label">VAT</td>
          <td></td>
          <td class="money mono">${fmtGBP(totals.vat_amount)}</td>
          <td></td>
        </tr>` : ""}
        <tr>
          <td class="label"><b>Total due</b></td>
          <td></td>
          ${showVatCols ? `<td></td>` : ""}
          <td class="money mono"><b>${fmtGBP(totals.total_inc_vat)}</b></td>
        </tr>
      </tfoot>
    </table>
  </div>
</body>
</html>`;
}


  // Helpers local to this function
  const isTimesheetKind = (k) => String(k || '').toUpperCase() === 'TIMESHEET';
  const isDefaultDocsPdfKeyFor = (tsId, key) => {
    if (!tsId || !key) return false;
    const norm = normalizeKey(String(key));
    return norm === normalizeKey(`docs-pdf/timesheets/ts_${String(tsId)}.pdf`);
  };

  const attachPageDebug = (page, label) => {
    if (!LOG || !page) return;
    try {
      page.on('console', (m) => {
        try {
          const type = m?.type?.() || 'log';
          const text = m?.text?.() || '';
          if (type === 'error' || type === 'warning') {
            log('error', 'page_console', { page: label, console_type: type, console_text: text });
          } else {
            log('log', 'page_console', { page: label, console_type: type, console_text: text });
          }
        } catch {}
      });
    } catch {}
    try {
      page.on('pageerror', (e) => {
        log('error', 'pageerror', { page: label, err_message: e?.message || String(e || ''), err_stack: e?.stack || null });
      });
    } catch {}
    try {
      page.on('requestfailed', (r) => {
        const url = r?.url?.() || null;
        const failure = r?.failure?.() || null;
        log('error', 'requestfailed', { page: label, url, failure });
      });
    } catch {}
  };

  try {
    // 1) Manifest (single RPC)
    step = 'MANIFEST_RPC';
    const tMan0 = Date.now();
    // The optional runtime is reachable only through the exported test seam at
    // the bottom of this module. Production callers always omit it. This lets
    // the release proof execute this exact renderer/storage owner while using
    // an in-memory Browser Rendering/R2 fixture instead of a second formatter.
    const renderSbRpc = testRuntime?.sbRpc || sbRpc;
    const renderWithBrowser = testRuntime?.withBrowser || withBrowser;
    const man = await renderSbRpc(env, 'invoice_render_manifest', { p_invoice_id: invoiceId });
    log('log', 'manifest_rpc_ok', { ms: Date.now() - tMan0 });

    const manRows = Array.isArray(man) ? man : (man?.data || []);
    const manifest = (manRows && manRows.length) ? manRows[0] : man;

    if (!manifest || typeof manifest !== 'object') {
      log('error', 'manifest_invalid', { ms: Date.now() - t0 });
      return { ok: false, error: 'Failed to load invoice render manifest' };
    }

    validateLegacyInvoiceManifestSuppressions(manifest);
    const inv = manifest.invoice || manifest.invoice_row || null;
    if (!inv || !inv.id) {
      log('error', 'invoice_not_found_in_manifest', { ms: Date.now() - t0 });
      return { ok: false, error: 'Invoice not found' };
    }
    const docFlagsById = (manifest && typeof manifest === 'object' && manifest.timesheet_doc_flags_by_id && typeof manifest.timesheet_doc_flags_by_id === 'object')
      ? manifest.timesheet_doc_flags_by_id
      : {};

    const regenTimesheetIds = [];
    const qrRefsChangedTimesheetIds = [];

    try {
      for (const [k, v] of Object.entries(docFlagsById)) {
        const tsid = String(k || '').trim();
        if (!tsid) continue;
        const obj = (v && typeof v === 'object') ? v : {};
        if (obj.electronic_pdf_regen_required === true || obj.electronic_refs_changed === true) {
          regenTimesheetIds.push(tsid);
        }
        if (obj.qr_refs_changed === true) {
          qrRefsChangedTimesheetIds.push(tsid);
        }
      }
    } catch {}

    const hasRegenRequired = (regenTimesheetIds.length > 0);

    log('log', 'manifest_counts', {
      ms: Date.now() - t0,
      lines_len: Array.isArray(manifest.lines) ? manifest.lines.length : 0,
      evidence_len: Array.isArray(manifest.evidence) ? manifest.evidence.length : 0,
      timesheet_evidence_len: Array.isArray(manifest.timesheet_evidence) ? manifest.timesheet_evidence.length : 0,
      evidence_other_len: Array.isArray(manifest.evidence_other) ? manifest.evidence_other.length : 0,
      tsfin_blocks: Array.isArray(manifest.tsfin_external_source_rows) ? manifest.tsfin_external_source_rows.length : 0,
      hr_cache_blocks: Array.isArray(manifest.hr_source_rows_cache) ? manifest.hr_source_rows_cache.length : 0,
      regen_timesheets: regenTimesheetIds.length,
      qr_refs_changed_timesheets: qrRefsChangedTimesheetIds.length
    });

      // ✅ Cache short-circuit INSIDE render (covers email path too)
    // IMPORTANT: if any ELECTRONIC timesheet needs regen, do NOT return cached bundle
    try {
      if (!forceRegen && !hasRegenRequired) {
        const key = (inv && typeof inv.invoice_pdf_r2_key === 'string') ? inv.invoice_pdf_r2_key.trim() : '';
        const genAt = inv ? inv.invoice_pdf_generated_at_utc : null;
        const updAt = inv ? inv.updated_at : null;

        if (key && genAt && updAt) {
          const tUpd = new Date(updAt).getTime();
          const tGen = new Date(genAt).getTime();

          if (Number.isFinite(tUpd) && Number.isFinite(tGen) && tUpd <= tGen) {
            const cleanCachedKey = key.replace(/^\/+/, '');
            const cachedHead = await r2HeadForInvoiceRender(cleanCachedKey);
            if (cachedHead) {
              log('log', 'bundle_cache_hit', { ms: Date.now() - t0, pdf_key: cleanCachedKey });
              return {
                ok: true,
                pdf_key: cleanCachedKey,
                cached: true,
                attached_timesheets: 0,
                attached_hr: false,
                attached_nhsp: false,
                attached_evidence: 0,
                attached_timesheet_evidence: 0,
                attached_manual_timesheets: 0,
                regen_timesheet_ids: regenTimesheetIds,
                qr_refs_changed_timesheet_ids: qrRefsChangedTimesheetIds
              };
            }

            log('error', 'bundle_cache_missing_r2_regenerating', {
              ms: Date.now() - t0,
              pdf_key: cleanCachedKey
            });
          }
        }
      }
    } catch (errCache) {
      log('error', 'bundle_cache_check_failed_nonfatal', {
        ms: Date.now() - t0,
        err_name: errCache?.name || null,
        err_message: errCache?.message || String(errCache || ''),
        err_stack: errCache?.stack || null
      });
    }

    step = 'READ_HEADER_ATTACH_POLICY';
    const header = (manifest.header_snapshot_json && typeof manifest.header_snapshot_json === 'object')
      ? manifest.header_snapshot_json
      : (inv.header_snapshot_json && typeof inv.header_snapshot_json === 'object' ? inv.header_snapshot_json : {});

    const attachPolicy = (manifest.attach_policy && typeof manifest.attach_policy === 'object')
      ? manifest.attach_policy
      : (header.attach_policy && typeof header.attach_policy === 'object' ? header.attach_policy : null);

    const requiresHr = !!(attachPolicy && attachPolicy.requires_hr === true);
    const hrAttach = !!(attachPolicy && Object.prototype.hasOwnProperty.call(attachPolicy, 'hr_attach_to_invoice') && attachPolicy.hr_attach_to_invoice !== false);
    const hrAllowed = (requiresHr === true && hrAttach === true);

    const tsAttach = (attachPolicy && Object.prototype.hasOwnProperty.call(attachPolicy, 'ts_attach_to_invoice'))
      ? (attachPolicy.ts_attach_to_invoice !== false)
      : true;

    log('log', 'attach_policy_effective', {
      requires_hr: requiresHr,
      hr_attach_to_invoice: hrAttach,
      hr_allowed: hrAllowed,
      ts_attach_to_invoice: tsAttach,
      attach_policy_present: !!attachPolicy
    });

  // Stationery resolution + presign
step = 'PRESIGN_STATIONERY';
let stationeryKey =
  (typeof header.stationery_key === "string" && header.stationery_key.trim()) ||
  env.INVOICE_STATIONERY_KEY ||
  "Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png";
if (/\.pdf$/i.test(stationeryKey)) stationeryKey = stationeryKey.replace(/\.pdf$/i, "@300dpi.png");
stationeryKey = normalizeKey(stationeryKey);

const tPresign0 = Date.now();
const stationeryUrl = await presignR2Url(
  env,
  req,
  stationeryKey,
  Number(env.PRESIGN_EXPIRES_SECONDS || 600)
);
log('log', 'presign_ok', {
  ms: Date.now() - tPresign0,
  stationery_key: stationeryKey,
  stationery_url_len: (stationeryUrl ? String(stationeryUrl).length : 0),
  stationery_origin: (() => { try { return stationeryUrl ? (new URL(stationeryUrl)).origin : null; } catch { return null; } })()
});
// ✅ NEW: Logo embed (match timesheet robustness: bytes from R2 → data: URI; no Chromium fetch)
step = 'EMBED_LOGO_DATA_URI';
try {
  const bucket = env.R2_BUCKET || env.R2;

  const rawLogo =
    (typeof header.agency_logo === 'string' && header.agency_logo.trim()) ||
    (typeof header.agency_logo_url === 'string' && header.agency_logo_url.trim()) ||
    '';

  const looksLikeRealUrl = (s) => {
    const x = String(s || '').trim();
    return /^https?:\/\//i.test(x) || /^data:/i.test(x);
  };

  if (bucket?.get && rawLogo && !looksLikeRealUrl(rawLogo)) {
    const logoKey = normalizeKey(String(rawLogo).trim());
    const obj = await bucket.get(logoKey).catch(() => null);

    if (obj) {
      const bytesU8 = new Uint8Array(await new Response(obj.body).arrayBuffer());

      // match timesheet renderer safety cap
      const MAX_LOGO_BYTES = 250_000;

      if (bytesU8.length > 0 && bytesU8.length <= MAX_LOGO_BYTES) {
        const ct0 = String(obj.httpMetadata?.contentType || "").toLowerCase();

        const ct =
          ct0 ||
          (logoKey.toLowerCase().endsWith(".png") ? "image/png" :
           logoKey.toLowerCase().endsWith(".jpg") || logoKey.toLowerCase().endsWith(".jpeg") ? "image/jpeg" :
           "application/octet-stream");

        const dataUrl = `data:${ct};base64,${toBase64(bytesU8)}`;

        // this is what buildHTML() and headerTemplate will read
        header.agency_logo_url = dataUrl;

        log('log', 'embed_logo_ok', {
          logo_key: logoKey,
          logo_bytes: bytesU8.length,
          logo_ct: ct
        });
      } else {
        log('error', 'embed_logo_skip_too_large_or_empty', {
          logo_key: logoKey,
          logo_bytes: bytesU8.length
        });
      }
    } else {
      log('error', 'embed_logo_not_found', { logo_key: logoKey });
    }
  } else {
    log('log', 'embed_logo_skip', { has_logo: !!rawLogo, looks_like_url: !!(rawLogo && looksLikeRealUrl(rawLogo)) });
  }
} catch (eLogo) {
  log('error', 'embed_logo_failed_nonfatal', {
    err_name: eLogo?.name || null,
    err_message: eLogo?.message || String(eLogo || ''),
    err_stack: eLogo?.stack || null
  });
}


const marginsObj = toMarginsObj(header.stationery_margins_mm);
const hideBankFooter = header.hide_bank_footer === true;


    const lineRows = Array.isArray(manifest.lines) ? manifest.lines : [];

    const evidenceAll = Array.isArray(manifest.evidence) ? manifest.evidence : [];
    const timesheetEvidenceRows = Array.isArray(manifest.timesheet_evidence)
      ? manifest.timesheet_evidence
      : evidenceAll.filter(ev => isTimesheetKind(ev?.kind));
    const otherEvidenceRows = Array.isArray(manifest.evidence_other)
      ? manifest.evidence_other
      : evidenceAll.filter(ev => !isTimesheetKind(ev?.kind));
    const tsfinRows = Array.isArray(manifest.tsfin_external_source_rows) ? manifest.tsfin_external_source_rows : [];
    const hrCacheRows = Array.isArray(manifest.hr_source_rows_cache) ? manifest.hr_source_rows_cache : [];

    // ----------------------------------------------------------
    // ✅ Mandatory: NEVER use TSFIN external_source_rows_json fallback for NHSP/HR.
    // If cache is empty OR missing headers, force-build invoice-scoped rows via RPC.
    // ----------------------------------------------------------
    function _hasUsableHeadersForBlock(b) {
      if (!b || typeof b !== 'object') return false;
      const rows = Array.isArray(b.rows_json) ? b.rows_json : [];
      if (!rows.length) return true; // no rows => header presence not required for usability check
      const hr = Array.isArray(b.header_rows) ? b.header_rows : null;
      if (hr && hr.length > 0) return true;
      const hc = Array.isArray(b.header_columns) ? b.header_columns : null;
      return !!(hc && hc.length > 0);
    }

    function _cacheNeedsRefresh(blocks) {
      const arr = Array.isArray(blocks) ? blocks : [];
      if (arr.length === 0) return true;
      // If ANY block that has rows lacks usable headers -> refresh
      for (const b of arr) {
        if (!_hasUsableHeadersForBlock(b)) return true;
      }
      return false;
    }

    let hrCacheRowsEffective = hrCacheRows;

    if (_cacheNeedsRefresh(hrCacheRowsEffective)) {
      step = 'HR_SOURCE_ROWS_COLLECT_RPC';
      const tHr0 = Date.now();
      const fresh0 = await renderSbRpc(env, 'invoice_source_rows_collect', { p_invoice_id: invoiceId, p_force_refresh: true });
      const freshRows = Array.isArray(fresh0) ? fresh0 : (fresh0?.data || []);

      hrCacheRowsEffective = Array.isArray(freshRows) ? freshRows : [];

      log('log', 'invoice_source_rows_collect_done', {
        ms: Date.now() - tHr0,
        hr_cache_blocks_before: Array.isArray(hrCacheRows) ? hrCacheRows.length : 0,
        hr_cache_blocks_after: hrCacheRowsEffective.length
      });

      // If still missing headers, treat as unusable (skip attachments later)
      if (_cacheNeedsRefresh(hrCacheRowsEffective)) {
        log('error', 'hr_source_rows_cache_still_missing_headers_or_empty', {
          hr_cache_blocks_after: hrCacheRowsEffective.length
        });
        hrCacheRowsEffective = [];
      }
    }

    let groupNightsat = false;
    if (typeof header.group_nightsat_sunbh === 'boolean') groupNightsat = header.group_nightsat_sunbh;
    else if (typeof header?.meta?.group_nightsat_sunbh === 'boolean') groupNightsat = header.meta.group_nightsat_sunbh;

    // Build invoice HTML now (outside browser) for reuse
    step = 'BUILD_INVOICE_HTML';
    const invoiceData = {
      header: {
        ...header,
        stationery_url: stationeryUrl,
        stationery_margins_mm: marginsObj,
        hide_bank_footer: hideBankFooter,
      },
      meta: {
        kind: 'invoice',
        generated_at_utc: new Date().toISOString(),
        group_nightsat_sunbh: groupNightsat,
      },
      invoice_no: inv.invoice_no || null,
      issued_at_utc: inv.issued_at_utc,
      due_at_utc: inv.due_at_utc,
      totals: {
        subtotal_ex_vat: Number(inv.subtotal_ex_vat || 0),
        vat_amount: Number(inv.vat_amount || 0),
        total_inc_vat: Number(inv.total_inc_vat || 0),
      },
      items: (lineRows || []).map((l) => {
        const meta0 = (l.meta_json && typeof l.meta_json === 'object') ? { ...l.meta_json } : {};
        if (!meta0.week_ending_date && meta0.week_ending_date_local) meta0.week_ending_date = meta0.week_ending_date_local;

        if (!meta0.line_type_norm && l.line_type_norm) meta0.line_type_norm = String(l.line_type_norm);
        if (!meta0.line_type_norm) meta0.line_type_norm = String(meta0.line_type || '').toUpperCase();
        if (meta0.timesheet_id == null && l.timesheet_id) meta0.timesheet_id = String(l.timesheet_id);

        const hourKeys = ['hours_day','hours_night','hours_sat','hours_sun','hours_bh'];
        for (const k of hourKeys) {
          if (meta0[k] == null && l[k] != null) meta0[k] = Number(l[k] || 0);
        }
        const rateKeys = ['charge_day','charge_night','charge_sat','charge_sun','charge_bh'];
        for (const k of rateKeys) {
          if (meta0[k] == null && l[k] != null) meta0[k] = Number(l[k] || 0);
        }

        return {
          description: l.description,
          timesheet_id: l.timesheet_id || null,
          meta: meta0,
          total_ex_vat: Number(l.total_charge_ex_vat || 0),
          vat_rate_pct: Number(l.vat_rate_pct || 0),
          vat_amount: Number(l.vat_amount || 0),
          total_inc_vat: Number(l.total_inc_vat || 0),
        };
      }),
      reference_rows: Array.isArray(manifest.reference_rows) ? manifest.reference_rows : [],
    };

    const invoiceHtml = buildHTML(invoiceData);
    // ==========================================================
// Option A: Use Puppeteer header/footer templates to prevent collisions
// Header at top, footer at bottom, body cannot overlap.
// ==========================================================
const headerForTemplate = invoiceData.header || {};
const metaForTemplate = invoiceData.meta || {};

const clientNameTpl = String(pick(headerForTemplate, "client_name", "") || "");
const clientAddrLinesTpl = String(pick(headerForTemplate, "client_invoice_address", "") || "")
  .split("\n")
  .map(s => String(s || "").trim())
  .filter(Boolean);
const clientAddrHtmlTpl = clientAddrLinesTpl.length
  ? `<br>${clientAddrLinesTpl.map(escapeHtml).join("<br>")}`
  : "";

const agencyNameTpl =
  String(pick(headerForTemplate, "agency_name", "") ||
  pick(headerForTemplate, "agency_display_name", "") ||
  pick(headerForTemplate, "company_name", "") ||
  "");

const agencyLogoTpl =
  String(pick(headerForTemplate, "agency_logo_url", "") || pick(headerForTemplate, "logo_url", "") || "");

const termsDaysTpl = pick(headerForTemplate, "payment_terms_days", null);

const issuedTxt = fmtDateGB(invoiceData.issued_at_utc);
const dueTxt = fmtDateGB(invoiceData.due_at_utc);

const headerPoTpl = pick(headerForTemplate, "po_number", null);
const itemPosTpl = (invoiceData.items || []).map(i => i?.meta?.po_number).filter(Boolean);
const uniquePosTpl = Array.from(new Set([...(headerPoTpl ? [headerPoTpl] : []), ...itemPosTpl]));
const poNoTpl = uniquePosTpl.length === 1 ? String(uniquePosTpl[0]) : "";
const backingReportNumbersTpl = Array.from(new Set(
  (Array.isArray(headerForTemplate?.meta?.backing_report_numbers)
    ? headerForTemplate.meta.backing_report_numbers
    : [])
    .map(value => String(value || '').trim())
    .filter(Boolean)
));
const backingReportLabelTpl = backingReportNumbersTpl.join(', ');

const bankTpl = (pick(headerForTemplate, "bank", {}) || {});
const registeredAddressLinesTpl = String(pick(headerForTemplate, "registered_address", "") || "")
  .split("\n").map(s => String(s || "").trim()).filter(Boolean);

const companyRegNumberTpl =
  String(pick(headerForTemplate, "company_reg_number", "") ||
  pick(headerForTemplate, "company_registration_number", "") ||
  "");

const DEFAULT_VAT_REG = "363 6805 80";
const vatRegTpl = String(pick(headerForTemplate, "vat_registration_number", "") || DEFAULT_VAT_REG);

// Use same sizing as invoice HTML currently uses
const headerTemplate = `
<style>
  .hdr-wrap {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif;
    font-size: 11px;
    color: #111;
    width: 100%;
    box-sizing: border-box;
    padding: 0 ${marginsObj.right}mm 0 ${marginsObj.left}mm;
  }
  .hdr {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    align-items: start;
  }
.brand { display:flex; align-items:center; gap:10px; margin: 0 0 6px 0; }
.brand-logo { height: 68px; max-width: 360px; object-fit: contain; }
.brand-name { font-weight: 700; font-size: 16.25px; line-height: 1.1; }


  .title { font-size: 20px; font-weight: 700; letter-spacing: .5px; margin: 0; }
  .muted { color: #666; }
  .mono { font-variant-numeric: tabular-nums; }
  .panel { border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; }
  .billto-title { font-weight: 600; margin-bottom: 4px; }
  .billto { white-space: pre-wrap; }
  .meta-table { width: 100%; border-collapse: collapse; }
  .meta-table th { text-align: left; font-weight: 600; padding: 0 0 2px 0; }
  .meta-table td { padding: 2px 0; }
</style>
<div class="hdr-wrap">
  <div class="hdr">
    <div>
      <div class="brand">
        ${agencyLogoTpl ? `<img class="brand-logo" src="${escapeUrl(agencyLogoTpl)}" />` : ""}
        ${agencyNameTpl ? `<div class="brand-name">${escapeHtml(agencyNameTpl)}</div>` : ""}
      </div>

      <div class="title">INVOICE ${invoiceData.invoice_no ? `<span class="muted mono">#${escapeHtml(String(invoiceData.invoice_no))}</span>` : ""}</div>

      <div class="panel" style="margin-top:8px;">
        <div class="billto-title">Bill To</div>
        <div class="billto"><b>${escapeHtml(clientNameTpl)}</b>${clientAddrHtmlTpl}</div>
      </div>
    </div>

    <div class="panel">
      <table class="meta-table">
        <tr><th>Issue date</th><td class="mono">${escapeHtml(issuedTxt)}</td></tr>
        <tr><th>Due date</th><td class="mono">${escapeHtml(dueTxt)}</td></tr>
        ${termsDaysTpl != null ? `<tr><th>Payment terms</th><td class="mono">${escapeHtml(String(termsDaysTpl))} days</td></tr>` : ""}
        ${poNoTpl ? `<tr><th>PO Number</th><td class="mono">${escapeHtml(poNoTpl)}</td></tr>` : ""}
        ${backingReportLabelTpl ? `<tr><th>Backing report</th><td class="mono">${escapeHtml(backingReportLabelTpl)}</td></tr>` : ""}
      </table>
    </div>
  </div>
</div>
`;

const footerTemplate = hideBankFooter ? `<div></div>` : `
<style>
  .ftr-wrap {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif;
    font-size: 10px;
    color: #333;
    width: 100%;
    box-sizing: border-box;
    padding: 0 ${marginsObj.right}mm 0 ${marginsObj.left}mm;
  }
  .ftr {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 12px;
    align-items: start;
  }
  .mono { font-variant-numeric: tabular-nums; }
  .right { text-align: right; }
</style>
<div class="ftr-wrap">
  <div class="ftr">
    <div>
      <div><b>BACS Payment Details</b></div>
      <div>Banker: <span class="mono">${escapeHtml(pick(bankTpl, "name", ""))}</span></div>
      <div>Sort Code: <span class="mono">${escapeHtml(pick(bankTpl, "sort_code", ""))}</span> &nbsp;&nbsp; Account No.: <span class="mono">${escapeHtml(pick(bankTpl, "account_number", ""))}</span></div>
    </div>
    <div class="right">
      ${vatRegTpl ? `<div>VAT Reg: <b class="mono">${escapeHtml(vatRegTpl)}</b></div>` : ""}
      ${companyRegNumberTpl ? `<div>Company Reg: <b class="mono">${escapeHtml(companyRegNumberTpl)}</b></div>` : ""}
      ${registeredAddressLinesTpl.length ? `<div style="margin-top:4px;">Registered address:<br>${registeredAddressLinesTpl.map(escapeHtml).join("<br>")}</div>` : ""}
    </div>
  </div>
</div>
`;

// Header/footer heights used by PDF margins.
// Keep your existing tuned values.
const FIXED_HEADER_MM = 52;
const FIXED_FOOTER_MM = hideBankFooter ? 0 : 26;

// IMPORTANT: Header at top of page (no extra blank band above header).
// Body starts after header; footer sits at bottom; body cannot overlap.
const pdfMargins = {
  top: `${FIXED_HEADER_MM}mm`,
  right: `${marginsObj.right}mm`,
  bottom: `${FIXED_FOOTER_MM}mm`,
  left: `${marginsObj.left}mm`
};

    log('log', 'invoice_html_built', {
      ms: Date.now() - t0,
      html_len: (invoiceHtml ? String(invoiceHtml).length : 0),
      line_count: lineRows.length,
      evidence_timesheet_count: timesheetEvidenceRows.length,
      evidence_other_count: otherEvidenceRows.length
    });

// Timesheet IDs from invoice lines
step = 'COLLECT_TS_IDS';
const tsIds = [...new Set(lineRows.map(r => r?.timesheet_id).filter(Boolean).map(String))];
log('log', 'timesheets_collected', { ts_ids: tsIds.length });

// ✅ NEW: Ensure ALL attached ELECTRONIC timesheet PDFs are clean before attaching.
// - Not only manifest-flagged ones.
// - Determine ELECTRONIC by timesheets.submission_mode === 'ELECTRONIC' AND manual_pdf_r2_key IS NULL.
// - If manifest indicates a regen is required, force_regen=true; otherwise force_regen=false (dirty-check in renderer decides).
if (tsAttach && tsIds.length) {
  step = 'ENSURE_ELECTRONIC_TS_PDFS_BEFORE_ATTACH';

  const regenSet = new Set((regenTimesheetIds || []).map(x => String(x || '').trim()).filter(Boolean));
  const tsInfoById = new Map();

  // Batch load minimal TS fields needed to identify ELECTRONIC safely
  const chunkSize = 200;
  for (let i = 0; i < tsIds.length; i += chunkSize) {
    const chunk = tsIds.slice(i, i + chunkSize);
    const { rows: tsRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/timesheets` +
        `?is_current=eq.true` +
        `&select=timesheet_id,submission_mode,manual_pdf_r2_key` +
        `&timesheet_id=in.(${chunk.map(enc).join(',')})`
    );
    for (const r of (tsRows || [])) {
      const id = (r?.timesheet_id != null) ? String(r.timesheet_id).trim() : '';
      if (!id) continue;
      tsInfoById.set(id, r);
    }
  }

  const ensureFailures = [];

  for (const tsId of tsIds) {
    const row = tsInfoById.get(String(tsId)) || null;
    if (!row) {
      // If we can't classify it, we must NOT attach a potentially stale PDF
      ensureFailures.push({
        timesheet_id: String(tsId),
        error: 'Missing timesheet row (cannot verify submission_mode/manual_pdf_r2_key)'
      });
      continue;
    }

    const mode = String(row?.submission_mode || '').toUpperCase();
    const manualKey = (row?.manual_pdf_r2_key != null) ? String(row.manual_pdf_r2_key).trim() : '';
    const isElectronic = (mode === 'ELECTRONIC') && !manualKey;

    if (!isElectronic) continue;

    const forceNow = regenSet.has(String(tsId));

    try {
      // prefer_generated is NOT needed here because manual_pdf_r2_key is verified null for ELECTRONIC.
      await ensureTimesheetPdf(env, tsId, { force_regen: forceNow });
    } catch (eEnsure) {
      ensureFailures.push({
        timesheet_id: String(tsId),
        error: eEnsure?.message || String(eEnsure || 'Failed to ensure timesheet PDF')
      });
    }
  }

  if (ensureFailures.length) {
    log('error', 'ensure_electronic_ts_pdfs_failed', {
      count: ensureFailures.length,
      failures: ensureFailures
    });

    return {
      ok: false,
      error: `Invoice render failed: could not prepare ${ensureFailures.length} electronic timesheet PDF(s) for attachment.`
    };
  }

  log('log', 'ensure_electronic_ts_pdfs_ok', {
    ts_ids: tsIds.length,
    forced_regen_count: (regenTimesheetIds || []).length
  });
}

const docsKeys = tsIds.map(tsId => normalizeKey(`docs-pdf/timesheets/ts_${tsId}.pdf`));


    const manualKeyByTsId = new Map();
    for (const l of lineRows) {
      const tsId = l?.timesheet_id ? String(l.timesheet_id) : null;
      if (!tsId) continue;
      const k = (l.paper_ts_r2_key || l.effective_paper_ts_r2_key || null);
      if (!k) continue;
      const nk = normalizeKey(String(k));
      if (isDefaultDocsPdfKeyFor(tsId, nk)) continue;
      if (!manualKeyByTsId.has(tsId)) manualKeyByTsId.set(tsId, nk);
    }
    const manualKeys = tsIds.map(tsId => manualKeyByTsId.get(tsId) || null).filter(Boolean);

    const expenseOnlySet = new Set();
    const noPhysicalTimesheetSet = new Set();

    if ((tsAttach || manualKeys.length) && tsIds.length) {
      const byTs = new Map();
      for (const l of (lineRows || [])) {
        const tsId = l?.timesheet_id ? String(l.timesheet_id) : null;
        if (!tsId) continue;
        if (!byTs.has(tsId)) byTs.set(tsId, []);
        byTs.get(tsId).push(l);
      }

      for (const [tsId, lines] of byTs.entries()) {
        const metas = (lines || []).map(l => (l?.meta_json && typeof l.meta_json === 'object') ? l.meta_json : {});
        const anyTrue = (key) => metas.some(m => m && m[key] === true);

        const basisVals = metas.map(m => String(m?.basis || m?.cur_basis || m?.timesheet_basis || '').toUpperCase());
        const routeVals = metas.map(m => String(m?.route_type || m?.route || '').toUpperCase());

        const isNhsp = anyTrue('client_is_nhsp') || anyTrue('is_nhsp') || basisVals.includes('NHSP') || routeVals.some(v => v.includes('NHSP'));
        const isHrImport = basisVals.includes('HEALTHROSTER') || routeVals.some(v => v.includes('HEALTHROSTER') || v.includes('HR_') || v === 'HR');
        const noTsReq = anyTrue('no_timesheet_required') || anyTrue('client_no_timesheet_required') || anyTrue('contract_no_timesheet_required');

        if (isNhsp || isHrImport || noTsReq) noPhysicalTimesheetSet.add(String(tsId));
      }

      const typeOf = (l) => {
        const m = (l?.meta_json && typeof l.meta_json === 'object') ? l.meta_json : {};
        return String(l?.line_type_norm || m?.line_type_norm || m?.line_type || '').toUpperCase();
      };

      const isHoursType = (t) => {
        const x = String(t || '').toUpperCase();
        return (x === 'HOURS' || x.startsWith('HOURS_'));
      };

      for (const tsId of tsIds) {
        const lines = byTs.get(String(tsId)) || [];
        if (!lines.length) continue;

        const types = lines.map(typeOf);

        const hasHours = types.some(isHoursType);
        const hasAdditional = types.some(t => String(t || '').toUpperCase().startsWith('ADDITIONAL_RATE'));
        const hasExpenseOrMileage = types.some(t => {
          const x = String(t || '').toUpperCase();
          return (x === 'MILEAGE' || x.startsWith('EXPENSE'));
        });

        if (!hasHours && !hasAdditional && hasExpenseOrMileage) {
          expenseOnlySet.add(String(tsId));
        }
      }
    }

    log('log', 'timesheet_sets', {
      expense_only_count: expenseOnlySet.size,
      no_physical_count: noPhysicalTimesheetSet.size,
      ts_attach_to_invoice: tsAttach
    });

    const missing = [];
    const dedupeKeys = new Set();
    const tsEvidenceFetchedSet = new Set();

    step = 'FETCH_TIMESHEET_EVIDENCE';
    const timesheetEvidenceBytesList = [];
    for (const ev of timesheetEvidenceRows) {
      const key = ev?.storage_key;
      if (!key) continue;
      const norm = normalizeKey(String(key));
      if (dedupeKeys.has(norm)) continue;

      const bytes = await r2GetBytes(env, norm);
      if (!bytes || !bytes.length) {
        missing.push({ kind: 'TIMESHEET_EVIDENCE_PDF', timesheet_id: ev?.timesheet_id || null, storage_key: norm });
        continue;
      }
      let safeBytes = bytes;
      try {
        safeBytes = await ensureInvoiceEvidenceBytesArePdf(bytes, {
          sourceLabel: 'Timesheet evidence',
          storageKey: norm,
          displayName: ev?.display_name || null,
          mimeType: ev?.mime_type || ev?.content_type || null
        });
      } catch (e) {
        return {
          ok: false,
          error: `Timesheet evidence attachment failed: ${e?.message || e}`,
          deterministic_render_failure: true,
          render_failure_kind: 'INVOICE_EVIDENCE_ATTACHMENT_PARSE'
        };
      }
      dedupeKeys.add(norm);
      timesheetEvidenceBytesList.push(safeBytes);
      if (ev?.timesheet_id) tsEvidenceFetchedSet.add(String(ev.timesheet_id));
    }

    step = 'FETCH_TS_PDFS';
    const tsBytesList = [];
    if (tsAttach) {
      for (let i = 0; i < tsIds.length; i++) {
        const tsId = tsIds[i];
        const key = docsKeys[i];
        const norm = normalizeKey(String(key));
        if (dedupeKeys.has(norm)) continue;

        const bytes = await r2GetBytes(env, norm);
        if (!bytes || !bytes.length) {
          if (
            expenseOnlySet.has(String(tsId)) ||
            tsEvidenceFetchedSet.has(String(tsId)) ||
            noPhysicalTimesheetSet.has(String(tsId)) ||
            manualKeyByTsId.has(String(tsId))
          ) continue;
          missing.push({ kind: 'TIMESHEET_PDF', timesheet_id: tsId, storage_key: norm });
          continue;
        }
        dedupeKeys.add(norm);
        tsBytesList.push(bytes);
      }
    }

    step = 'FETCH_MANUAL_TS_PDFS';
    const manualTsBytesList = [];
    for (let i = 0; i < tsIds.length; i++) {
      const tsId = tsIds[i];
      const key = manualKeyByTsId.get(String(tsId));
      if (!key) continue;

      const norm = normalizeKey(String(key));
      if (dedupeKeys.has(norm)) continue;

      const bytes = await r2GetBytes(env, norm);
      if (!bytes || !bytes.length) {
        if (expenseOnlySet.has(String(tsId)) || tsEvidenceFetchedSet.has(String(tsId))) continue;
        missing.push({ kind: 'MANUAL_TIMESHEET_PDF', timesheet_id: tsId, storage_key: norm });
        continue;
      }
      let safeBytes = bytes;
      try {
        safeBytes = await ensureManualTimesheetBytesArePdf(bytes, {
          env,
          sourceLabel: 'Manual timesheet document',
          storageKey: norm
        });
      } catch (e) {
        return {
          ok: false,
          error: `Unsupported manual timesheet document type: ${e?.message || e}`,
          deterministic_render_failure: true,
          render_failure_kind: 'MANUAL_TIMESHEET_ATTACHMENT_PARSE'
        };
      }
      dedupeKeys.add(norm);
      manualTsBytesList.push(safeBytes);
    }

    step = 'FETCH_OTHER_EVIDENCE';
    const evidenceBytesList = [];
    for (const ev of otherEvidenceRows) {
      const key = ev?.storage_key;
      if (!key) continue;
      const norm = normalizeKey(String(key));
      if (dedupeKeys.has(norm)) continue;

      const bytes = await r2GetBytes(env, norm);
      if (!bytes || !bytes.length) {
        missing.push({ kind: 'EVIDENCE_PDF', timesheet_id: ev?.timesheet_id || null, storage_key: norm });
        continue;
      }
      let safeBytes = bytes;
      try {
        safeBytes = await ensureInvoiceEvidenceBytesArePdf(bytes, {
          sourceLabel: 'Invoice evidence',
          storageKey: norm,
          displayName: ev?.display_name || null,
          mimeType: ev?.mime_type || ev?.content_type || null
        });
      } catch (e) {
        return {
          ok: false,
          error: `Invoice evidence attachment failed: ${e?.message || e}`,
          deterministic_render_failure: true,
          render_failure_kind: 'INVOICE_EVIDENCE_ATTACHMENT_PARSE'
        };
      }
      dedupeKeys.add(norm);
      evidenceBytesList.push(safeBytes);
    }

    log('log', 'artefact_fetch_summary', {
      missing_count: missing.length,
      timesheet_evidence_pdfs: timesheetEvidenceBytesList.length,
      ts_pdfs: tsBytesList.length,
      manual_ts_pdfs: manualTsBytesList.length,
      other_evidence_pdfs: evidenceBytesList.length
    });

    if (missing.length) {
      try {
        if (userForAudit) {
          await writeAudit(
            env,
            userForAudit,
            'INVOICE_RENDER_FAILED_MISSING_ARTEFACTS',
            { invoice_id: invoiceId, missing },
            { entity: 'invoice', subject_id: invoiceId, req }
          );
        }
      } catch (errAudit) {
        log('error', 'writeAudit_missing_artefacts_failed', {
          err_name: errAudit?.name || null,
          err_message: errAudit?.message || String(errAudit || ''),
          err_stack: errAudit?.stack || null
        });
      }
      return { ok: false, error: `Invoice render failed: missing required artefacts (${missing.length}).` };
    }
     // Build HR rows/html (outside browser)
    step = 'BUILD_HR_REPORT';
    let hrHtml = null;
    if (hrAllowed) {
      const hrTables = [];

      if (hrCacheRowsEffective.length) {
        for (const r of hrCacheRowsEffective) {
          const ssU = String(r.source_system || '').toUpperCase();
          if (ssU !== 'HEALTHROSTER' && ssU !== 'HEALTHROSTER_DAILY') continue;

          const rows = Array.isArray(r.rows_json) ? r.rows_json : [];
          if (!rows.length) continue;

          hrTables.push({
            import_id: r.import_id || null,
            header_rows: Array.isArray(r.header_rows) ? r.header_rows : [],
            header_columns: Array.isArray(r.header_columns) ? r.header_columns : [],
            rows_json: rows
          });
        }
      }

      const totalRows = hrTables.reduce((a, t) => a + (Array.isArray(t.rows_json) ? t.rows_json.length : 0), 0);

      log('log', 'hr_tables_collected', {
        hr_tables: hrTables.length,
        hr_total_rows: totalRows,
        hr_cache_blocks: hrCacheRowsEffective.length
      });

      // ✅ Mandatory: if still no invoice-scoped rows, SKIP attachment (do NOT fall back to TSFIN snapshots)
      if (hrTables.length && totalRows > 0) {
        hrHtml = buildHrReportHTML(inv, header, { tables: hrTables }); // header_rows now passed through
        log('log', 'hr_html_built', { hr_html_len: (hrHtml ? String(hrHtml).length : 0) });
      } else {
        log('log', 'hr_skipped_no_invoice_scoped_rows', { hr_allowed: true, hr_tables: hrTables.length, hr_total_rows: totalRows });
      }
    } else {
      log('log', 'hr_skipped_not_allowed', { requires_hr: requiresHr, hr_attach_to_invoice: hrAttach });
    }

    // Build NHSP tables/html (outside browser)
    step = 'BUILD_NHSP_REPORT';
    let nhspHtml = null;
    {
      const nhspTables = [];

      if (hrCacheRowsEffective.length) {
        for (const r of hrCacheRowsEffective) {
          if (String(r.source_system || '').toUpperCase() !== 'NHSP') continue;

          const rows = Array.isArray(r.rows_json) ? r.rows_json : [];
          if (!rows.length) continue;

          nhspTables.push({
            import_id: r.import_id || null,
            header_rows: Array.isArray(r.header_rows) ? r.header_rows : [],
            header_columns: Array.isArray(r.header_columns) ? r.header_columns : [],
            rows_json: rows
          });
        }
      }

      const totalRows = nhspTables.reduce((a, t) => a + (Array.isArray(t.rows_json) ? t.rows_json.length : 0), 0);

      log('log', 'nhsp_tables_collected', {
        nhsp_tables: nhspTables.length,
        nhsp_total_rows: totalRows,
        nhsp_cache_blocks: hrCacheRowsEffective.length
      });

      // ✅ Mandatory: if still no invoice-scoped rows, SKIP attachment (do NOT fall back to TSFIN snapshots)
      if (nhspTables.length && totalRows > 0) {
        nhspHtml = buildNhspReportHTML(inv, header, { tables: nhspTables }); // header_rows now passed through
        log('log', 'nhsp_html_built', { nhsp_html_len: (nhspHtml ? String(nhspHtml).length : 0) });
      } else {
        log('log', 'nhsp_skipped_no_invoice_scoped_rows', { nhsp_tables: nhspTables.length, nhsp_total_rows: totalRows });
      }
    }


    // === EFFICIENCY FIX: single browser session per invoice render ===
    step = 'BROWSER_RENDER_ALL';
    const renderAll = await renderWithBrowser(env, async (browser) => {
      const out = { invoicePdfU8: null, hrBytes: null, nhspBytes: null };

      // Invoice PDF
      {
        const page = await browser.newPage();
        attachPageDebug(page, 'invoice');
        try {
          log('log', 'invoice_setContent_start', { ms: Date.now() - t0 });
          await page.setContent(invoiceHtml, { waitUntil: "networkidle0" });
          log('log', 'invoice_setContent_ok', { ms: Date.now() - t0 });

          await page.emulateMediaType("screen");

          log('log', 'invoice_pdf_start', { ms: Date.now() - t0 });
 const pdfArrayBuffer = await page.pdf({
  format: "a4",
  printBackground: true,
  displayHeaderFooter: true,
  headerTemplate,
  footerTemplate,
  margin: pdfMargins
});

          log('log', 'invoice_pdf_ok', { ms: Date.now() - t0, pdf_bytes: (pdfArrayBuffer ? pdfArrayBuffer.byteLength : 0) });

          out.invoicePdfU8 = new Uint8Array(pdfArrayBuffer);
        } finally {
          try { await page.close(); } catch {}
        }
      }

        // HR PDF (only if allowed and rows exist)
      if (hrHtml) {
        const page = await browser.newPage();
        attachPageDebug(page, 'hr');
        try {
          log('log', 'hr_setContent_start');
          await page.setContent(hrHtml, { waitUntil: 'networkidle0' });
          log('log', 'hr_setContent_ok');

          await page.emulateMediaType('screen');

          log('log', 'hr_pdf_start');
          const pdfArrayBuffer = await page.pdf({
            format: 'a4',
            landscape: true,
            printBackground: true,
            margin: { top: 24, right: 24, bottom: 24, left: 24 }
          });
          log('log', 'hr_pdf_ok', { hr_pdf_bytes: (pdfArrayBuffer ? pdfArrayBuffer.byteLength : 0) });

          out.hrBytes = new Uint8Array(pdfArrayBuffer);
        } finally {
          try { await page.close(); } catch {}
        }
      }

      // NHSP PDF (always if NHSP rows exist)
      if (nhspHtml) {
        const page = await browser.newPage();
        attachPageDebug(page, 'nhsp');
        try {
          log('log', 'nhsp_setContent_start');
          await page.setContent(nhspHtml, { waitUntil: 'networkidle0' });
          log('log', 'nhsp_setContent_ok');

          await page.emulateMediaType('screen');

          log('log', 'nhsp_pdf_start');
          const pdfArrayBuffer = await page.pdf({
            format: 'a4',
            landscape: true,
            printBackground: true,
            margin: { top: 24, right: 24, bottom: 24, left: 24 }
          });
          log('log', 'nhsp_pdf_ok', { nhsp_pdf_bytes: (pdfArrayBuffer ? pdfArrayBuffer.byteLength : 0) });

          out.nhspBytes = new Uint8Array(pdfArrayBuffer);
        } finally {
          try { await page.close(); } catch {}
        }
      }

      return out;
    });

    step = 'BROWSER_RENDER_VERIFY';
    if (!renderAll || !renderAll.invoicePdfU8 || !renderAll.invoicePdfU8.length) {
      throw new Error('Invoice PDF renderer did not return PDF bytes');
    }

    const invoicePdfU8 = renderAll.invoicePdfU8;
    const hrBytes = renderAll.hrBytes;
    const nhspBytes = renderAll.nhspBytes;

    log('log', 'browser_render_done', {
      invoice_pdf_u8_len: invoicePdfU8?.length || 0,
      hr_pdf_u8_len: hrBytes?.length || 0,
      nhsp_pdf_u8_len: nhspBytes?.length || 0
    });

    // ==========================================================
    // Merge PDFs (UPDATED ORDER):
    // 1) Invoice
    // 2) Timesheet PDFs (canonical, if tsAttach)
    // 3) Manual/override Timesheet PDFs (always when present)
    // 4) HR report (if present)
    // 5) NHSP report (if present)
    // 6) Evidence PDFs (timesheet evidence + other evidence)
    // ==========================================================
    step = 'PDF_MERGE_START';
    const merged = await PDFDocument.create();

    // 1) Invoice
    step = 'PDFLIB_LOAD_INVOICE';
    const invDoc = await PDFDocument.load(invoicePdfU8);
    (await merged.copyPages(invDoc, invDoc.getPageIndices())).forEach(p => merged.addPage(p));

    // 2) Timesheet PDFs (canonical)
    step = 'PDFLIB_ATTACH_TS_PDFS';
    let attachedTsCount = 0;
    if (tsAttach) {
      for (const b of tsBytesList) {
        const doc = await PDFDocument.load(b);
        (await merged.copyPages(doc, doc.getPageIndices())).forEach(p => merged.addPage(p));
        attachedTsCount++;
      }
    }

    // 3) Manual/override Timesheet PDFs (always included when present)
    step = 'PDFLIB_ATTACH_MANUAL_TS_PDFS';
    let attachedManualTsCount = 0;
    for (const b of manualTsBytesList) {
      const doc = await PDFDocument.load(b);
      (await merged.copyPages(doc, doc.getPageIndices())).forEach(p => merged.addPage(p));
      attachedManualTsCount++;
    }

    // 4) HR report
    step = 'PDFLIB_ATTACH_HR';
    if (hrBytes?.length) {
      const doc = await PDFDocument.load(hrBytes);
      (await merged.copyPages(doc, doc.getPageIndices())).forEach(p => merged.addPage(p));
    }

    // 5) NHSP report
    step = 'PDFLIB_ATTACH_NHSP';
    if (nhspBytes?.length) {
      const doc = await PDFDocument.load(nhspBytes);
      (await merged.copyPages(doc, doc.getPageIndices())).forEach(p => merged.addPage(p));
    }

    // 6) Evidence PDFs (timesheet evidence + other evidence)
    step = 'PDFLIB_ATTACH_TIMESHEET_EVIDENCE';
    let attachedTimesheetEvidenceCount = 0;
    for (const b of timesheetEvidenceBytesList) {
      const doc = await PDFDocument.load(b);
      (await merged.copyPages(doc, doc.getPageIndices())).forEach(p => merged.addPage(p));
      attachedTimesheetEvidenceCount++;
    }

    step = 'PDFLIB_ATTACH_OTHER_EVIDENCE';
    let attachedEvidenceCount = 0;
    for (const b of evidenceBytesList) {
      const doc = await PDFDocument.load(b);
      (await merged.copyPages(doc, doc.getPageIndices())).forEach(p => merged.addPage(p));
      attachedEvidenceCount++;
    }

    step = 'PDFLIB_SAVE_MERGED';
    const combinedU8 = await merged.save();
    log('log', 'merge_ok', {
      combined_len: combinedU8?.length || 0,
      attached_timesheets: attachedTsCount,
      attached_manual_timesheets: attachedManualTsCount,
      attached_timesheet_evidence: attachedTimesheetEvidenceCount,
      attached_evidence: attachedEvidenceCount,
      attached_hr: !!(hrBytes && hrBytes.length),
      attached_nhsp: !!(nhspBytes && nhspBytes.length)
    });

    // Store combined PDF in R2 and patch invoice row
    step = 'R2_PUT';
    const pdfKey = normalizeKey(`docs-pdf/invoices/invoice_${invoiceId}.pdf`);
    await r2Put(env, pdfKey, combinedU8, { httpMetadata: { contentType: "application/pdf" } });

    step = 'R2_PUT_VERIFY';
    const storedHead = await r2HeadForInvoiceRender(pdfKey);
    if (!storedHead) {
      throw new Error('Invoice PDF R2 upload verification failed');
    }

    const nowIso = new Date().toISOString();

    step = 'DB_PATCH_INVOICE';
    const patchRes = await fetch(
      `${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${enc(invoiceId)}`,
      {
        method: "PATCH",
        headers: sbHeaders(env),
        body: JSON.stringify({
          invoice_pdf_r2_key: pdfKey,
          invoice_pdf_generated_at_utc: nowIso,
          paper_ts_r2_manifest: docsKeys,
          updated_at: nowIso,
        }),
      }
    );

    if (!patchRes.ok) {
      let patchText = '';
      try { patchText = await patchRes.text(); } catch {}
      patchText = String(patchText || '').replace(/\s+/g, ' ').trim().slice(0, 800);
      throw new Error(`Invoice PDF DB patch failed (${patchRes.status}): ${patchText || patchRes.statusText || 'unknown error'}`);
    }

    step = 'DONE';
    log('log', 'success', { ms: Date.now() - t0, pdf_key: pdfKey });

    return {
      ok: true,
      pdf_key: pdfKey,
      cached: false,
      attached_timesheets: attachedTsCount,
      attached_hr: !!(hrBytes && hrBytes.length),
      attached_nhsp: !!(nhspBytes && nhspBytes.length),
      attached_evidence: attachedEvidenceCount,
      attached_timesheet_evidence: attachedTimesheetEvidenceCount,
      attached_manual_timesheets: attachedManualTsCount
    };
  } catch (err) {
    const errMessage = err?.message || String(err || '');
    const errStack = err?.stack || null;

    log('error', 'exception', {
      ms: Date.now() - t0,
      err_name: err?.name || null,
      err_message: errMessage,
      err_stack: errStack
    });

    try {
      if (userForAudit) {
        const stackStr = (errStack == null) ? null : String(errStack);
        const stackTrim = (stackStr && stackStr.length > 8000) ? stackStr.slice(0, 8000) : stackStr;

        await writeAudit(
          env,
          userForAudit,
          'INVOICE_RENDER_BUNDLE_EXCEPTION',
          {
            invoice_id: invoiceId,
            step,
            err_name: err?.name || null,
            err_message: errMessage,
            err_stack: stackTrim,
            ms: Date.now() - t0
          },
          { entity: 'invoice', subject_id: invoiceId, req }
        );
      }
    } catch (errAudit) {
      log('error', 'writeAudit_exception_failed', {
        err_name: errAudit?.name || null,
        err_message: errAudit?.message || String(errAudit || ''),
        err_stack: errAudit?.stack || null
      });
    }

    const safePublicError = (() => {
      let msg = String(errMessage || '').trim() || 'Unknown render error';
      msg = msg
        .replace(/(authorization\s*:?\s*bearer\s+)[^\s,;]+/ig, '$1[redacted]')
        .replace(/(token=)[^&\s]+/ig, '$1[redacted]')
        .replace(/(apikey=)[^&\s]+/ig, '$1[redacted]')
        .replace(/(service[_-]?role[_-]?key\s*[:=]\s*)[^\s,;]+/ig, '$1[redacted]')
        .replace(/(X-Amz-Signature=)[^&\s]+/ig, '$1[redacted]')
        .replace(/(X-Amz-Credential=)[^&\s]+/ig, '$1[redacted]')
        .replace(/\s+/g, ' ')
        .trim();

      const safeStep = String(step || 'UNKNOWN')
        .replace(/[^A-Za-z0-9_:-]/g, '_')
        .slice(0, 80) || 'UNKNOWN';

      return `Failed to render invoice bundle at ${safeStep}: ${(msg.slice(0, 500) || 'Unknown render error')}`;
    })();

    return { ok: false, error: safePublicError, render_step: step || null };
  }
}
