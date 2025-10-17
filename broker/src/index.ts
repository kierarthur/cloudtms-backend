/**
 * CloudTMS Broker (Cloudflare Worker) â€” Auth + Timesheets API (no Google Sheets)
 *
 * Endpoints:
 *  - POST   /auth/login
 *  - POST   /auth/refresh
 *  - POST   /auth/logout
 *  - POST   /auth/forgot
 *  - POST   /auth/reset
 *
 *  - POST   /timesheets/presign
 *  - PUT    /upload?key=...&booking_id=...&role=nurse|authoriser&token=...
 *  - POST   /timesheets/submit
 *  - POST   /timesheets/revoke
 *  - POST   /timesheets/revoke-and-presign
 *  - GET    /timesheets/:booking_id           (current; add ?version=2 or ?current_only=false)
 *  - GET    /timesheets
 *  - POST   /timesheets/query
 *  - POST   /timesheets/authorised-status
 *  - POST   /signatures/presign-get 
 *  - POST   /signatures/presign-get/batch
 *  - GET    /signatures/get?key=...&booking_id=...&role=...&token=...
 *  - GET    /healthz
 *  - GET    /readyz
 *  - GET    /version
 */

const JSON_HEADERS = { "content-type": "application/json; charset=utf-8" };
const TEXT_PLAIN = { "content-type": "text/plain; charset=utf-8" };

// worker.mjs (Cloudflare Worker, module syntax)
import puppeteer from "@cloudflare/puppeteer";

// --- Helpers ---------------------------------------------------------------
const fmtGBP = (n) =>
  new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: "GBP",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(Number(n || 0));

const fmtDateGB = (iso) => {
  if (!iso) return "";
  const d = new Date(iso);
  return new Intl.DateTimeFormat("en-GB", { timeZone: "UTC" }).format(d);
};

const pick = (o, k, d = undefined) => (o && o[k] != null ? o[k] : d);

const escapeHtml = (s = "") =>
  String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");

const escapeUrl = (s = "") =>
  String(s).replace(/['")\\]/g, (m) => ({ "'": "%27", '"': "%22", ")": "%29", "\\": "%5C" }[m]));

// Safer base64 for large Uint8Array in Workers (chunked)
function toBase64(u8) {
  let binary = "";
  const chunk = 0x8000;
  for (let i = 0; i < u8.length; i += chunk) {
    binary += String.fromCharCode(...u8.subarray(i, i + chunk));
  }
  return btoa(binary);
}

// --- HTML builder ----------------------------------------------------------
function buildHTML(payload) {
  const {
    header = {},
    invoice_no = "",
    issued_at_utc,
    due_at_utc,
    totals = { subtotal_ex_vat: 0, vat_amount: 0, total_inc_vat: 0 },
    items = []
  } = payload || {};

  // Snapshot fields (populated by your issuing worker)
  const clientName = pick(header, "client_name", "");
  const clientAddress = (pick(header, "client_invoice_address", "") || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);

  const vatChargeable = !!pick(header, "vat_chargeable", true);
  const appliedVatPct = Number(pick(header, "applied_vat_rate_pct", 0)); // NOTE: never displayed
  const termsDays = pick(header, "payment_terms_days", null);
  const bank = pick(header, "bank", {}) || {};

  // VAT reg: use settings value if present; otherwise fallback to hard-wired number
  const DEFAULT_VAT_REG = "363 6805 80";
  const vatReg = pick(header, "vat_registration_number", "") || DEFAULT_VAT_REG;

  // Stationery (letterhead) — expects PNG/JPG URL (signed) + safe-area margins (mm)
  const stationeryUrl = pick(header, "stationery_url", ""); // PNG/JPG URL or data: URI
  const defaultMargins = stationeryUrl
    ? { top: 32, right: 12, bottom: 20, left: 12 } // safe defaults with artwork
    : { top: 18, right: 12, bottom: 34, left: 12 }; // plain layout defaults
  const mgIn = pick(header, "stationery_margins_mm", {}) || {};
  const mg = {
    top: Number(pick(mgIn, "top", defaultMargins.top)),
    right: Number(pick(mgIn, "right", defaultMargins.right)),
    bottom: Number(pick(mgIn, "bottom", defaultMargins.bottom)),
    left: Number(pick(mgIn, "left", defaultMargins.left))
  };

  // Hide transactional footer if your artwork already includes it
  const hideBankFooter = !!pick(header, "hide_bank_footer", false);

  // Header-level PO only if a single unique PO exists across header + all items
  const headerPo = pick(header, "po_number", null);
  const itemPos = items.map((i) => i?.meta?.po_number).filter(Boolean);
  const uniquePos = Array.from(new Set([...(headerPo ? [headerPo] : []), ...itemPos]));
  const poNo = uniquePos.length === 1 ? uniquePos[0] : "";

  const showVatCols = vatChargeable && (appliedVatPct > 0 || Number(totals.vat_amount) > 0);

  // Build line rows
  const lineRows = items
    .map((it, idx) => {
      const meta = it.meta || {};
      const we = meta.week_ending_date || meta.week_ending || meta.weekEnding || null;

      const sublineParts = [
        meta.candidate_display || meta.candidate || null,
        meta.role || meta.job_title || null,
        meta.hospital || meta.hospital_norm || null,
        meta.ward || meta.ward_norm || null,
        we ? `W/E ${fmtDateGB(we)}` : null,
        meta.po_number ? `PO ${meta.po_number}` : null
      ]
        .filter(Boolean)
        .join(" • ");

      const hours = { d: meta.hours_day, n: meta.hours_night, sa: meta.hours_sat, su: meta.hours_sun, bh: meta.hours_bh };
      const hourPills = Object.entries(hours)
        .filter(([, v]) => Number(v) > 0)
        .map(([k, v]) => `<span class="pill">${k.toUpperCase()}: ${Number(v).toFixed(2)}</span>`)
        .join("");

      return `
        <tr class="line">
          <td class="desc">
            <div class="desc-title">${escapeHtml(it.description || `Line ${idx + 1}`)}</div>
            <div class="desc-meta">
              ${escapeHtml(sublineParts)}
              ${hourPills ? `<div class="pills">${hourPills}</div>` : ""}
            </div>
          </td>
          <td class="money exvat">${fmtGBP(it.total_ex_vat)}</td>
          ${showVatCols ? `<td class="money vat">${fmtGBP(it.vat_amount)}</td>` : ""}
          <td class="money totalinc">${fmtGBP(it.total_inc_vat)}</td>
        </tr>
      `;
    })
    .join("");

  // Build full HTML (stationery background + reserved margins + fixed footer)
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Invoice ${escapeHtml(invoice_no || "")}</title>
  <style>
    /* Reserve safe areas so content never overlaps header/footer artwork */
    @page { size: A4; margin: ${mg.top}mm ${mg.right}mm ${mg.bottom}mm ${mg.left}mm; }

    html, body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif;
      color: #111;
      font-size: 11px; /* fixed size per spec */
      line-height: 1.35;
      -webkit-print-color-adjust: exact;
    }

    /* Stationery (full page) */
    .stationery {
      position: fixed;
      inset: 0;
      z-index: 0;
      background-repeat: no-repeat;
      background-position: center;
      background-size: cover; /* A4 PNG should fill edge-to-edge */
      opacity: 1;
      pointer-events: none;
    }

    .wrap { position: relative; z-index: 1; width: 100%; }

    .header {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-bottom: 16px;
    }
    .title { font-size: 20px; font-weight: 700; letter-spacing: .5px; }
    .muted { color: #666; }
    .mono { font-variant-numeric: tabular-nums; }
    .panel { border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; }
    .billto-title { font-weight: 600; margin-bottom: 4px; }
    .billto { white-space: pre-wrap; }

    .meta-table { width: 100%; border-collapse: collapse; }
    .meta-table th { text-align: left; font-weight: 600; padding: 0 0 2px 0; }
    .meta-table td { padding: 2px 0; }

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
      border: 1px solid #e5e7eb;  /* full grid */
      padding: 8px 10px;
      vertical-align: top;
    }
    .lines thead th.money, .lines td.money { text-align: right; }
    .desc-title { font-weight: 600; margin-bottom: 2px; }
    .desc-meta { color: #555; }
    .pills { margin-top: 3px; }
    .pill {
      display: inline-block; border: 1px solid #e5e7eb; border-radius: 999px;
      padding: 1px 6px; font-size: 10px; margin-right: 4px; margin-top: 2px;
    }
    .money { font-variant-numeric: tabular-nums; }

    .lines tfoot td { background: #fcfcfd; font-weight: 600; }
    .lines tfoot .label { text-align: right; color: #333; font-weight: 600; }

    /* Transactional footer pinned above bottom margin */
    .footer {
      position: fixed;
      left: ${mg.left}mm; right: ${mg.right}mm; bottom: ${mg.bottom}mm;
      font-size: 10px; color: #333;
      display: ${hideBankFooter ? "none" : "grid"};
      grid-template-columns: 2fr 1fr; gap: 12px;
    }
    .right { text-align: right; }
  </style>
</head>
<body>
  ${stationeryUrl ? `<div class="stationery" style="background-image:url('${escapeUrl(stationeryUrl)}');"></div>` : ""}

  <div class="wrap">
    <div class="header">
      <div>
        <div class="title">INVOICE ${invoice_no ? `<span class="muted mono">#${escapeHtml(invoice_no)}</span>` : ""}</div>
        <div class="panel" style="margin-top:8px;">
          <div class="billto-title">Bill To</div>
          <div class="billto"><b>${escapeHtml(clientName)}</b>${clientAddress.length ? `<br>${clientAddress.map(escapeHtml).join("<br>")}` : ""}</div>
        </div>
      </div>
      <div class="panel">
        <table class="meta-table">
          <tr><th>Issue date</th><td class="mono">${fmtDateGB(issued_at_utc)}</td></tr>
          <tr><th>Due date</th><td class="mono">${fmtDateGB(due_at_utc)}</td></tr>
          ${termsDays != null ? `<tr><th>Payment terms</th><td class="mono">${termsDays} days</td></tr>` : ""}
          ${poNo ? `<tr><th>PO Number</th><td class="mono">${escapeHtml(poNo)}</td></tr>` : ""}
          <!-- VAT % intentionally NOT displayed -->
        </table>
      </div>
    </div>

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

  <div class="footer">
    <div>
      <div><b>BACS Payment Details</b></div>
      <div>Banker: <span class="mono">${escapeHtml(pick(bank, "name", ""))}</span></div>
      <div>Sort Code: <span class="mono">${escapeHtml(pick(bank, "sort_code", ""))}</span> &nbsp;&nbsp; Account No.: <span class="mono">${escapeHtml(pick(bank, "account_number", ""))}</span></div>
    </div>
    <div class="right">
      ${vatReg ? `<div>VAT Reg: <b class="mono">${escapeHtml(vatReg)}</b></div>` : ""}
    </div>
  </div>
</body>
</html>`;
}



/*
  Email Outbox Broker – queue + drain + provider integration (Power Automate)
  ---------------------------------------------------------------------------
  Drop this file into your Worker codebase (or merge the functions into your existing
  handler module). It uses your existing helper utilities and DB conventions.

  Assumptions / dependencies already present in your codebase (as seen in snippets):
    - requireUser(env, req, roles?) -> returns user or null
    - withCORS(env, req, response)
    - ok/json helpers: ok(data), badRequest(msg), unauthorized(), notFound(msg), serverError(msg)
    - sbHeaders(env) -> { 'apikey': ..., 'Authorization': 'Bearer ...', 'Content-Type': 'application/json' }
    - sbFetch(env, url, single = false, init?) -> { rows } JSON wrapper for Supabase REST
    - writeAudit(env, user, action, afterJson, opts) -> writes audit_events
    - handleInvoiceRender(env, req, invoiceId) -> returns Response JSON { pdf_url }

  New exported handlers in this module:
    - handleOutboxDrain
    - handleEmailSend
    - handleQueueTsoFailureEmail
    - handleOutboxRetry
    - handleListOutbox
    - handleGetOutboxItem
    - handleOutboxMarkSent
    - handleOutboxMarkFailed

  Helpers (pure logic or provider-facing):
    - drainEmailOutboxOnce
    - buildEmailPayloadFromOutboxRow
    - postToPowerAutomate
    - fetchAttachmentBase64FromR2
    - limitOrLinkAttachments
    - normalizeEmailPayload, validateEmailPayload, estimatePayloadSizeBytes
    - resolveTsoRecipientForTimesheet
    - recordEmailAudit (thin wrapper over writeAudit)
    - signDownloadUrlForR2Key

  Revised functions from your snippets:
    - handleRemittanceEmailForCandidate (queues REMITTANCE table row)
    - handleInvoiceEmail (queues INVOICE row; ensures PDF exists; audit)

  Router wiring helper:
    - wireEmailRoutes(router)

  Scheduled drain (optional):
    - scheduled(event, env, ctx) example at bottom – call drainEmailOutboxOnce periodically.
*/

// ------------------------------
// Config knobs (override via env if you like)
// ------------------------------
const DEFAULT_DRAIN_LIMIT = 10;                 // how many queued rows to pick per drain
const EMAIL_MAX_PAYLOAD_BYTES = 18 * 1024 * 1024; // 18MB safety cap before trimming/links
const SIGNED_LINK_TTL_SECS = 7 * 24 * 60 * 60;  // 7 days

// ------------------------------
// Small utilities
// ------------------------------
const enc = (s) => encodeURIComponent(String(s ?? ''));
const nowIso = () => new Date().toISOString();

// best-effort byte size estimate for JSON payload
function estimatePayloadSizeBytes(obj) {
  try {
    return new TextEncoder().encode(JSON.stringify(obj)).byteLength;
  } catch {
    return 0;
  }
}

function isNonEmptyString(x) {
  return typeof x === 'string' && x.trim().length > 0;
}

function toArray(x) {
  if (!x) return [];
  if (Array.isArray(x)) return x;
  return [x];
}

function splitCsvMaybe(s) {
  if (!isNonEmptyString(s)) return [];
  return s.split(',').map((p) => p.trim()).filter(Boolean);
}

// robust base64 from ArrayBuffer for Workers (avoid large String.fromCharCode spread)
function base64FromArrayBuffer(buf) {
  let binary = '';
  const bytes = new Uint8Array(buf);
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

// ------------------------------
// Provider integration (Power Automate)
// ------------------------------
async function postToPowerAutomate(env, payload) {
  const url = env.POWER_AUTOMATE_EMAIL_WEBHOOK_URL;
  if (!isNonEmptyString(url)) {
    return { ok: false, status: 0, body: 'POWER_AUTOMATE_EMAIL_WEBHOOK_URL not configured' };
  }

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  let body;
  try { body = await res.text(); } catch { body = ''; }

  const ok = res.ok;
  // transparently try to parse a provider_message_id if present in JSON
  let provider_message_id = undefined;
  try {
    const j = JSON.parse(body);
    provider_message_id = j.provider_message_id || j.id || j.messageId || undefined;
  } catch {}

  return { ok, status: res.status, body, provider_message_id };
}

// ------------------------------
// Attachments
// ------------------------------
async function fetchAttachmentBase64FromR2(env, r2Key) {
  // Try common binding names – adjust if your binding name differs.
  const bucket = env.DOCS_BUCKET || env.R2 || env.R2_BUCKET || env.FILES_BUCKET;
  if (!bucket || typeof bucket.get !== 'function') {
    throw new Error('R2 binding not available on env (expected DOCS_BUCKET or R2)');
  }
  const obj = await bucket.get(r2Key);
  if (!obj) throw new Error(`R2 object not found for key: ${r2Key}`);
  const arrBuf = await obj.arrayBuffer();
  const base64 = base64FromArrayBuffer(arrBuf);
  // if the filename is not known here, caller should supply display name
  return base64;
}

function normalizeEmailPayload(raw) {
  const to = Array.isArray(raw.to) ? raw.to : splitCsvMaybe(raw.to);
  const cc = Array.isArray(raw.cc) ? raw.cc : splitCsvMaybe(raw.cc);
  const bcc = Array.isArray(raw.bcc) ? raw.bcc : splitCsvMaybe(raw.bcc);
  const replyTo = Array.isArray(raw.replyTo) ? raw.replyTo : splitCsvMaybe(raw.replyTo);

  const html = raw.html || raw.body_html || undefined;
  const text = raw.text || raw.body_text || undefined;

  let attachments = [];
  // Support two shapes:
  //  1) legacy: [{ r2_key, filename }]
  //  2) direct: [{ name, contentBase64 }]
  for (const a of toArray(raw.attachments)) {
    if (!a) continue;
    if (a.contentBase64 && a.name) {
      attachments.push({ name: String(a.name), contentBase64: String(a.contentBase64) });
    } else if (a.r2_key) {
      attachments.push({ r2_key: String(a.r2_key), name: a.filename || 'attachment' });
    }
  }

  return { to, cc, bcc, replyTo, subject: raw.subject, html, text, attachments, reference: raw.reference };
}

function validateEmailPayload(p) {
  if (!Array.isArray(p.to) || p.to.length === 0) return 'Missing recipient(s)';
  if (!isNonEmptyString(p.subject)) return 'Missing subject';
  if (!isNonEmptyString(p.html) && !isNonEmptyString(p.text)) return 'Missing html/text body';
  return null;
}

async function buildEmailPayloadFromOutboxRow(env, outboxRow) {
  const base = normalizeEmailPayload({
    to: outboxRow.to,
    cc: outboxRow.cc,
    subject: outboxRow.subject,
    body_html: outboxRow.body_html,
    body_text: outboxRow.body_text,
    attachments: outboxRow.attachments,
    reference: outboxRow.reference,
  });

  const err = validateEmailPayload(base);
  if (err) throw new Error(err);

  // Resolve any R2 attachments to base64
  const resolved = [];
  for (const a of base.attachments) {
    if (a.contentBase64 && a.name) { resolved.push(a); continue; }
    if (a.r2_key) {
      const contentBase64 = await fetchAttachmentBase64FromR2(env, a.r2_key);
      resolved.push({ name: a.name || 'attachment', contentBase64 });
    }
  }

  // Compose canonical payload expected by the Flow
  let payload = {
    to: base.to,
    cc: base.cc,
    bcc: base.bcc,
    replyTo: base.replyTo,
    subject: base.subject,
    html: base.html,
    text: base.text,
    attachments: resolved,
    reference: base.reference,
    // Include a type hint for easier templating in Power Automate
    meta: { type: outboxRow.type, outbox_id: outboxRow.id }
  };

  // Trim or link heavy payloads
  const sized = await limitOrLinkAttachments(env, payload);
  return sized.payload;
}

async function limitOrLinkAttachments(env, payload) {
  const limitBytes = Number(env.EMAIL_MAX_PAYLOAD_BYTES) || EMAIL_MAX_PAYLOAD_BYTES;
  let currentBytes = estimatePayloadSizeBytes(payload);
  if (currentBytes <= limitBytes) return { payload, trimmed: false };

  // Prefer to keep a PDF invoice if present, trim the rest and inject links.
  const kept = [];
  const trimmed = [];
  for (const a of payload.attachments) {
    const isInvoice = /invoice/i.test(a.name || '') && /\.pdf$/i.test(a.name || '');
    if (isInvoice && kept.length === 0) { kept.push(a); } else { trimmed.push(a); }
  }
  if (kept.length === 0 && payload.attachments.length > 0) {
    // keep the first as a compromise
    kept.push(payload.attachments[0]);
    trimmed.splice(0, 1);
  }

  let linksHtml = '';
  let linksText = '';

  // If original attachments came from R2 we may have lost their original keys.
  // Encourage callers to include `r2_key` in attachments for graceful linking.
  for (const a of trimmed) {
    const key = a.r2_key || (a.meta && a.meta.r2_key) || null;
    if (!key) continue;
    const url = await signDownloadUrlForR2Key(env, key, { ttlSecs: SIGNED_LINK_TTL_SECS });
    linksHtml += `<li><a href="${url}">${a.name || key}</a></li>`;
    linksText += `\n- ${a.name || key}: ${url}`;
  }

  let html = payload.html || '';
  let text = payload.text || '';

  if (linksHtml) {
    html += `\n<hr/><p>Some large attachments were replaced with links:</p><ul>${linksHtml}</ul>`;
  }
  if (linksText) {
    text += `\n\nSome large attachments were replaced with links:${linksText}`;
  }

  const newPayload = { ...payload, attachments: kept, html, text };
  return { payload: newPayload, trimmed: true };
}

async function signDownloadUrlForR2Key(env, r2Key, { ttlSecs }) {
  // If you already expose a download endpoint like /files?key=...
  // prefer to sign that instead of S3-style presign here.
  // Configure env.PUBLIC_DOWNLOAD_BASE_URL to your existing endpoint.
  const base = env.PUBLIC_DOWNLOAD_BASE_URL;
  if (isNonEmptyString(base)) {
    const u = new URL(base);
    u.searchParams.set('key', r2Key);
    u.searchParams.set('exp', String(Math.floor(Date.now() / 1000) + ttlSecs));
    // Optionally: include a dummy HMAC if you already verify it your side
    return u.toString();
  }
  // Fallback: return plain key token – your Flow can resolve it.
  return `r2://${r2Key}`;
}

// ------------------------------
// Queue drain logic
// ------------------------------
async function drainEmailOutboxOnce(env, { limit, types } = {}) {
  const take = Math.max(1, Math.min(Number(limit) || Number(env.EMAIL_DRAIN_LIMIT_DEFAULT) || DEFAULT_DRAIN_LIMIT, 100));
  const typeFilter = Array.isArray(types) && types.length ? types : null;

  let url = `${env.SUPABASE_URL}/rest/v1/mail_outbox?select=*` +
            `&status=eq.QUEUED` +
            `&order=created_at_utc.asc` +
            `&limit=${take}`;
  if (typeFilter) {
    // in.("A","B")
    const t = typeFilter.map((t) => `"${enc(t)}"`).join(',');
    url += `&type=in.(${t})`;
  }

  const { rows } = await sbFetch(env, url, false);
  const picked = rows || [];
  if (picked.length === 0) {
    return { picked: 0, sent: 0, failed: 0, errors: [] };
  }

  let sent = 0; let failed = 0; const errors = [];

  for (const row of picked) {
    try {
      const payload = await buildEmailPayloadFromOutboxRow(env, row);
      const res = await postToPowerAutomate(env, payload);

      if (res.ok) {
        // mark SENT
        const upd = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${enc(row.id)}`, {
          method: 'PATCH',
          headers: sbHeaders(env),
          body: JSON.stringify({ status: 'SENT', sent_at: nowIso(), provider_message_id: res.provider_message_id || null, last_error: null, failed_at: null })
        });
        if (!upd.ok) {
          const errTxt = await upd.text();
          throw new Error(`Sent but failed to update status: ${errTxt}`);
        }
        sent += 1;
        await recordEmailAudit(env, null, 'EMAIL_SENT', { outbox_id: row.id, provider_message_id: res.provider_message_id, type: row.type });
      } else {
        // mark FAILED
        const upd = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${enc(row.id)}`, {
          method: 'PATCH', headers: sbHeaders(env),
          body: JSON.stringify({ status: 'FAILED', failed_at: nowIso(), last_error: String(res.body || res.status) })
        });
        if (!upd.ok) {
          const errTxt = await upd.text();
          throw new Error(`Provider fail and update fail: ${errTxt}`);
        }
        failed += 1;
        errors.push({ id: row.id, error: res.body || `HTTP ${res.status}` });
        await recordEmailAudit(env, null, 'EMAIL_FAILED', { outbox_id: row.id, error: res.body || `HTTP ${res.status}`, type: row.type });
      }
    } catch (e) {
      // defensive failure
      try {
        await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${enc(row.id)}`, {
          method: 'PATCH', headers: sbHeaders(env),
          body: JSON.stringify({ status: 'FAILED', failed_at: nowIso(), last_error: String(e?.message || e) })
        });
      } catch {}
      failed += 1;
      errors.push({ id: row.id, error: String(e?.message || e) });
      await recordEmailAudit(env, null, 'EMAIL_FAILED', { outbox_id: row.id, error: String(e?.message || e), type: row.type });
    }
  }

  return { picked: picked.length, sent, failed, errors };
}

// ------------------------------
// HTTP handlers – Outbox ops
// ------------------------------
export async function handleOutboxDrain(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body = {};
  try { body = await parseJSONBody(req); } catch {}

  try {
    const report = await drainEmailOutboxOnce(env, { limit: body?.limit, types: body?.types });
    return withCORS(env, req, ok(report));
  } catch (e) {
    return withCORS(env, req, serverError(String(e?.message || e)));
  }
}

export async function handleEmailSend(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }

  const normalized = normalizeEmailPayload(body);
  const err = validateEmailPayload(normalized);
  if (err) return withCORS(env, req, badRequest(err));

  // If caller asks to queue instead of immediate send
  if (body?.queue === true) {
    const insert = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox`, {
      method: 'POST', headers: { ...sbHeaders(env), Prefer: 'return=representation' },
      body: JSON.stringify({
        type: body?.type || 'BROADCAST',
        to: normalized.to.join(','), cc: normalized.cc?.join(',') || null,
        subject: normalized.subject,
        body_html: normalized.html || null,
        body_text: normalized.text || null,
        attachments: body.attachments || null,
        status: 'QUEUED',
        reference: body?.reference || null,
        created_by: user?.id || null,
      })
    });
    if (!insert.ok) {
      return withCORS(env, req, serverError(`Failed to queue: ${await insert.text()}`));
    }
    const rows = await insert.json().catch(() => []);
    const row = Array.isArray(rows) ? rows[0] : rows;

    await recordEmailAudit(env, user, 'EMAIL_QUEUED', { outbox_id: row?.id, type: body?.type || 'BROADCAST' });
    return withCORS(env, req, ok({ queued: true, id: row?.id }));
  }

  // Immediate send via provider
  // Resolve any R2 attachments
  const resolved = [];
  for (const a of normalized.attachments) {
    if (a.contentBase64 && a.name) { resolved.push(a); }
    else if (a.r2_key) {
      const contentBase64 = await fetchAttachmentBase64FromR2(env, a.r2_key);
      resolved.push({ name: a.name || 'attachment', contentBase64 });
    }
  }

  const outgoing = await limitOrLinkAttachments(env, { payload: { ...normalized, attachments: resolved } });
  const resp = await postToPowerAutomate(env, outgoing.payload);
  if (!resp.ok) {
    return withCORS(env, req, serverError(`Provider rejected: ${resp.status} ${resp.body || ''}`));
  }

  await recordEmailAudit(env, user, 'EMAIL_SENT', { ad_hoc: true, provider_message_id: resp.provider_message_id });
  return withCORS(env, req, ok({ sent: true, provider_message_id: resp.provider_message_id }));
}

export async function handleOutboxRetry(env, req, outboxId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  try {
    const upd = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${enc(outboxId)}`, {
      method: 'PATCH', headers: sbHeaders(env),
      body: JSON.stringify({ status: 'QUEUED', failed_at: null, last_error: null })
    });
    if (!upd.ok) return withCORS(env, req, serverError(`Failed to retry: ${await upd.text()}`));
    await recordEmailAudit(env, user, 'EMAIL_RETRY', { outbox_id: outboxId });
    return withCORS(env, req, ok({ queued: true }));
  } catch (e) {
    return withCORS(env, req, serverError(String(e?.message || e)));
  }
}

export async function handleListOutbox(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const url = new URL(req.url);
  const status = url.searchParams.get('status');
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '50', 10), 200);
  const type = url.searchParams.get('type');

  let q = `${env.SUPABASE_URL}/rest/v1/mail_outbox?select=*`;
  if (status) q += `&status=eq.${enc(status)}`;
  if (type) q += `&type=eq.${enc(type)}`;
  q += `&order=created_at_utc.desc&limit=${limit}`;

  const { rows } = await sbFetch(env, q, false);
  return withCORS(env, req, ok({ items: rows }));
}

export async function handleGetOutboxItem(env, req, outboxId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/mail_outbox?select=*&id=eq.${enc(outboxId)}`, false);
  if (!rows?.length) return withCORS(env, req, notFound('Outbox item not found'));
  return withCORS(env, req, ok(rows[0]));
}

export async function handleOutboxMarkSent(env, req) {
  // Optional callback for provider -> system reconciliation
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }
  const { id, provider_message_id } = body || {};
  if (!isNonEmptyString(id)) return withCORS(env, req, badRequest('id is required'));

  const upd = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${enc(id)}`, {
    method: 'PATCH', headers: sbHeaders(env),
    body: JSON.stringify({ status: 'SENT', sent_at: nowIso(), provider_message_id: provider_message_id || null, last_error: null, failed_at: null })
  });
  if (!upd.ok) return withCORS(env, req, serverError(`Failed to mark sent: ${await upd.text()}`));

  await recordEmailAudit(env, user, 'EMAIL_MARK_SENT', { outbox_id: id, provider_message_id });
  return withCORS(env, req, ok({ ok: true }));
}

export async function handleOutboxMarkFailed(env, req) {
  // Optional callback for provider -> system reconciliation
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }
  const { id, error } = body || {};
  if (!isNonEmptyString(id)) return withCORS(env, req, badRequest('id is required'));

  const upd = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${enc(id)}`, {
    method: 'PATCH', headers: sbHeaders(env),
    body: JSON.stringify({ status: 'FAILED', failed_at: nowIso(), last_error: String(error || 'Unknown error') })
  });
  if (!upd.ok) return withCORS(env, req, serverError(`Failed to mark failed: ${await upd.text()}`));

  await recordEmailAudit(env, user, 'EMAIL_MARK_FAILED', { outbox_id: id, error: String(error || 'Unknown error') });
  return withCORS(env, req, ok({ ok: true }));
}

// ------------------------------
// HTTP handler – TSO failure email queueing
// ------------------------------
export async function handleQueueTsoFailureEmail(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }

  const timesheet_id = body?.timesheet_id || null;
  const booking_id = body?.booking_id || null;
  if (!timesheet_id && !booking_id) return withCORS(env, req, badRequest('Provide { timesheet_id } or { booking_id }'));

  try {
    const { to, client_id } = await resolveTsoRecipientForTimesheet(env, { timesheet_id, booking_id });
    if (!to) return withCORS(env, req, badRequest('Client TS queries email not configured'));

    const subject = body?.subject || 'Timesheet Query (TSO Failure)';
    const text = body?.body_text || 'A timesheet requires your attention.';
    const html = body?.body_html || `<p>${text}</p>`;

    // Optional attachments supplied by caller – we just pass through to the queue
    const attachments = Array.isArray(body?.attachments) ? body.attachments : null;

    const insert = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox`, {
      method: 'POST', headers: { ...sbHeaders(env), Prefer: 'return=representation' },
      body: JSON.stringify({
        type: 'TSO_FAILURE',
        to,
        cc: null,
        subject,
        body_html: html,
        body_text: text,
        attachments,
        status: 'QUEUED',
        reference: booking_id ? `tso_failure:booking:${booking_id}` : `tso_failure:timesheet:${timesheet_id}`,
        created_by: user?.id || null,
      })
    });
    if (!insert.ok) return withCORS(env, req, serverError(`Failed to queue: ${await insert.text()}`));

    const rows = await insert.json().catch(() => []);
    const row = Array.isArray(rows) ? rows[0] : rows;
    await recordEmailAudit(env, user, 'EMAIL_QUEUED', { outbox_id: row?.id, type: 'TSO_FAILURE', client_id });

    return withCORS(env, req, ok({ queued: true, id: row?.id }));
  } catch (e) {
    return withCORS(env, req, badRequest(String(e?.message || e)));
  }
}

// ------------------------------
// Recipient resolution for TSO mails
// ------------------------------
export async function resolveTsoRecipientForTimesheet(env, { timesheet_id, booking_id }) {
  // Prefer to resolve via current financials -> client_id
  if (timesheet_id) {
    const url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
                `?select=client_id,timesheet:timesheets(hospital_norm)` +
                `&timesheet_id=eq.${enc(timesheet_id)}` +
                `&is_current=eq.true` +
                `&limit=1`;
    const { rows } = await sbFetch(env, url, false);
    if (rows?.length) {
      const row = rows[0];
      if (row.client_id) {
        const to = await fetchTsQueriesEmailForClient(env, row.client_id);
        if (!to) throw new Error('NO_EMAIL_CONFIGURED');
        return { to, client_id: row.client_id };
      }
      // fallback: try hospital mapping
      const hosp = row?.timesheet?.hospital_norm || null;
      return await resolveRecipientViaHospital(env, hosp);
    }
  }

  if (booking_id) {
    // resolve via hospital mapping from the timesheets table (current version)
    const url = `${env.SUPABASE_URL}/rest/v1/timesheets` +
                `?select=hospital_norm` +
                `&booking_id=eq.${enc(booking_id)}` +
                `&is_current=eq.true` +
                `&limit=1`;
    const { rows } = await sbFetch(env, url, false);
    const hosp = rows?.[0]?.hospital_norm || null;
    return await resolveRecipientViaHospital(env, hosp);
  }

  throw new Error('CLIENT_UNKNOWN');
}

async function resolveRecipientViaHospital(env, hospital_norm) {
  if (!isNonEmptyString(hospital_norm)) throw new Error('CLIENT_UNKNOWN');
  const url = `${env.SUPABASE_URL}/rest/v1/client_hospitals?select=client_id&hospital_name_norm=eq.${enc(hospital_norm)}`;
  const { rows } = await sbFetch(env, url, false);
  if (!rows?.length) throw new Error('CLIENT_UNKNOWN');
  if (rows.length > 1) throw new Error('AMBIGUOUS_HOSPITAL');
  const client_id = rows[0].client_id;
  const to = await fetchTsQueriesEmailForClient(env, client_id);
  if (!to) throw new Error('NO_EMAIL_CONFIGURED');
  return { to, client_id };
}

async function fetchTsQueriesEmailForClient(env, client_id) {
  const url = `${env.SUPABASE_URL}/rest/v1/clients?id=eq.${enc(client_id)}&select=ts_queries_email`;
  const { rows } = await sbFetch(env, url, false);
  const to = rows?.[0]?.ts_queries_email || null;
  if (!isNonEmptyString(to)) return null;
  return to;
}

// ------------------------------
// Audit helper
// ------------------------------
async function recordEmailAudit(env, userOrNull, action, meta) {
  try {
    await writeAudit(env, userOrNull, action, meta, {
      entity: 'email', subject_id: (meta && meta.outbox_id) || null, reason: 'MAIL_OUTBOX'
    });
  } catch {}
}

// ------------------------------
// REVISED: Remittance & Invoice email queueing
// ------------------------------
export async function handleRemittanceEmailForCandidate(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }

  const timesheetIds = Array.isArray(body?.timesheet_ids) ? body.timesheet_ids.filter(Boolean) : [];
  const candidateId = body?.candidate_id || null;
  const startDate = body?.period_start || null; // YYYY-MM-DD
  const endDate   = body?.period_end   || null; // YYYY-MM-DD

  if (!timesheetIds.length && !(candidateId && startDate && endDate)) {
    return withCORS(env, req, badRequest('Provide either timesheet_ids[] OR { candidate_id, period_start, period_end }'));
  }

  try {
    let url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
              `?select=` + [
                'timesheet_id','candidate_id','client_id',
                'hours_day','hours_night','hours_sat','hours_sun','hours_bh',
                'pay_day','pay_night','pay_sat','pay_sun','pay_bh',
                'total_hours','total_pay_ex_vat',
                'timesheet:timesheets(timesheet_id,booking_id,week_ending_date,hospital_norm,ward_norm,shift_label_norm)',
                'client:clients(name)'
              ].join(',') +
              `&is_current=eq.true`;

    if (timesheetIds.length) {
      const ids = timesheetIds.map((id) => enc(id)).join(',');
      url += `&timesheet_id=in.(${ids})`;
    } else {
      url += `&candidate_id=eq.${enc(candidateId)}`;
      url += `&timesheet.week_ending_date=gte.${enc(startDate)}`;
      url += `&timesheet.week_ending_date=lte.${enc(endDate)}`;
    }
    url += `&order=timesheet.week_ending_date.asc`;

    const { rows: finRows } = await sbFetch(env, url, false);
    if (!finRows?.length) return withCORS(env, req, notFound('No timesheets found for the selection'));

    const candIds = [...new Set(finRows.map((r) => r.candidate_id).filter(Boolean))];
    if (candIds.length !== 1) return withCORS(env, req, badRequest('Selection spans multiple candidates; send one remittance per candidate.'));
    const candId = candIds[0];

    const { rows: candRows } = await sbFetch(env,
      `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${enc(candId)}&select=id,email,display_name,first_name,last_name`, false);
    if (!candRows?.length) return withCORS(env, req, notFound('Candidate not found'));

    const cand = candRows[0];
    const toEmail = (cand.email || '').trim();
    if (!toEmail) return withCORS(env, req, badRequest('Candidate email is missing'));

    const candName = cand.display_name || [cand.first_name, cand.last_name].filter(Boolean).join(' ') || 'Candidate';
    const dates = finRows.map((r) => r?.timesheet?.week_ending_date).filter(Boolean).sort();

    let periodLabel = '', periodKey = '';
    if (startDate && endDate) {
      periodLabel = `${startDate} to ${endDate}`;
      periodKey = `${startDate}_${endDate}`;
    } else if (dates.length) {
      const first = dates[0]; const last = dates[dates.length - 1];
      periodLabel = first === last ? `WE ${first}` : `WE ${first}–${last}`;
      periodKey = first === last ? `${first}` : `${first}_${last}`;
    } else {
      periodLabel = 'Selected timesheets';
      periodKey = 'selected';
    }

    const reference = `remit:candidate:${candId}:${periodKey}`;

    const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));
    const fmt = (n) => (n == null ? '' : Number(n).toFixed(2));

    const rowsHtml = finRows.map((r) => {
      const ts = r.timesheet || {}; const cli = r.client || {};
      return `
        <tr>
          <td>${esc(ts.week_ending_date || '')}</td>
          <td>${esc(cli.name || '')}</td>
          <td>${esc(ts.hospital_norm || '')}</td>
          <td>${esc(ts.ward_norm || '')}</td>
          <td>${esc(ts.shift_label_norm || '')}</td>
          <td style="text-align:right">${fmt(r.hours_day)}</td>
          <td style="text-align:right">${fmt(r.pay_day)}</td>
          <td style="text-align:right">${fmt(r.hours_night)}</td>
          <td style="text-align:right">${fmt(r.pay_night)}</td>
          <td style="text-align:right">${fmt(r.hours_sat)}</td>
          <td style="text-align:right">${fmt(r.pay_sat)}</td>
          <td style="text-align:right">${fmt(r.hours_sun)}</td>
          <td style="text-align:right">${fmt(r.pay_sun)}</td>
          <td style="text-align:right">${fmt(r.hours_bh)}</td>
          <td style="text-align:right">${fmt(r.pay_bh)}</td>
          <td style="text-align:right"><strong>${fmt(r.total_pay_ex_vat)}</strong></td>
        </tr>`;
    }).join('');

    const grandTotal = finRows.reduce((a, r) => a + Number(r.total_pay_ex_vat || 0), 0);

    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;font-size:14px;line-height:1.4">
        <h2 style="margin:0 0 8px">Remittance Advice</h2>
        <p style="margin:0 0 12px"><strong>${esc(candName)}</strong></p>
        <p style="margin:0 0 16px">Period: ${esc(periodLabel)}</p>
        <table width="100%" border="0" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead>
            <tr style="background:#f5f5f5">
              <th align="left">Week Ending</th>
              <th align="left">Client</th>
              <th align="left">Hospital</th>
              <th align="left">Ward</th>
              <th align="left">Shift</th>
              <th align="right">Hrs Day</th>
              <th align="right">Pay Day</th>
              <th align="right">Hrs Night</th>
              <th align="right">Pay Night</th>
              <th align="right">Hrs Sat</th>
              <th align="right">Pay Sat</th>
              <th align="right">Hrs Sun</th>
              <th align="right">Pay Sun</th>
              <th align="right">Hrs BH</th>
              <th align="right">Pay BH</th>
              <th align="right">Total (ex VAT)</th>
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
          <tfoot>
            <tr>
              <td colspan="15" align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>Grand Total:</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${fmt(grandTotal)}</strong></td>
            </tr>
          </tfoot>
        </table>
        <p style="margin-top:16px;color:#666">Note: This remittance reflects pay-exclusive amounts based on authorised timesheets.</p>
      </div>`;

    const text = [
      'Remittance Advice', `${candName}`, `Period: ${periodLabel}`, '',
      ...finRows.map((r) => {
        const ts = r.timesheet || {}; const cli = r.client || {};
        return [
          `WE ${ts.week_ending_date || ''} — ${cli.name || ''} / ${ts.hospital_norm || ''} / ${ts.ward_norm || ''} / ${ts.shift_label_norm || ''}`,
          `Day: ${fmt(r.hours_day)} @ ${fmt(r.pay_day)}`,
          `Night: ${fmt(r.hours_night)} @ ${fmt(r.pay_night)}`,
          `Sat: ${fmt(r.hours_sat)} @ ${fmt(r.pay_sat)}`,
          `Sun: ${fmt(r.hours_sun)} @ ${fmt(r.pay_sun)}`,
          `BH: ${fmt(r.hours_bh)} @ ${fmt(r.pay_bh)}`,
          `Total (ex VAT): ${fmt(r.total_pay_ex_vat)}`,
          ''
        ].join('\n');
      }),
      `Grand Total (ex VAT): ${fmt(grandTotal)}`
    ].join('\n');

    // Queue email in mail_outbox
    const outRes = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox`, {
      method: 'POST',
      headers: { ...sbHeaders(env), Prefer: 'return=representation' },
      body: JSON.stringify({
        type: 'REMITTANCE', to: toEmail, cc: null,
        subject: `Remittance Advice – ${periodLabel}`,
        body_html: html, body_text: text,
        attachments: null,
        status: 'QUEUED', reference,
        created_by: user?.id || null,
      })
    });

    if (!outRes.ok) {
      const err = await outRes.text();
      return withCORS(env, req, serverError(`Failed to queue remittance email: ${err}`));
    }

    const outJson = await outRes.json().catch(() => []);
    const mail = Array.isArray(outJson) ? outJson[0] : outJson;
    const mailId = mail?.id || null;

    await writeAudit(env, user, 'EMAIL_QUEUED', {
      to: toEmail,
      subject: `Remittance Advice – ${periodLabel}`,
      period: { start: startDate || dates[0] || null, end: endDate || dates[dates.length - 1] || null },
      mail_id: mailId,
      timesheets: finRows.map((r) => r.timesheet_id)
    }, { entity: 'candidate', subject_id: candId, reason: 'REMITTANCE', correlation_id: mailId, req });

    for (const r of finRows) {
      await writeAudit(env, user, 'EMAIL_QUEUED', { to: toEmail, subject: `Remittance Advice – ${periodLabel}`, mail_id: mailId }, { entity: 'timesheet', subject_id: r.timesheet_id, reason: 'REMITTANCE', correlation_id: mailId, req });
    }

    return withCORS(env, req, ok({ queued: true, mail_id: mailId, items: finRows.length }));
  } catch (e) {
    return withCORS(env, req, serverError('Failed to build/queue remittance email'));
  }
}

export async function handleInvoiceEmail(env, req, invoiceId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  try {
    const { rows } = await sbFetch(env,
      `${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${enc(invoiceId)}&select=invoice_no,invoice_pdf_r2_key,client:clients(primary_invoice_email,name)`, false);
    if (!rows?.length) return withCORS(env, req, notFound('Invoice not found'));

    const inv = rows[0];
    const to = inv.client?.primary_invoice_email || null;
    if (!to) return withCORS(env, req, badRequest('Client invoice email not configured'));

    // Ensure we have a PDF; if not, render now
    let pdfKey = inv.invoice_pdf_r2_key;
    if (!pdfKey) {
      const renderResp = await handleInvoiceRender(env, req, invoiceId);
      if (!renderResp.ok) return renderResp;
      const payload = await renderResp.json();
      const dlUrl = new URL(payload.pdf_url);
      pdfKey = dlUrl.searchParams.get('key');
    }

    const subject = `Invoice ${inv.invoice_no}`;

    // Queue in mail_outbox
    const out = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox`, {
      method: 'POST',
      headers: { ...sbHeaders(env), Prefer: 'return=representation' },
      body: JSON.stringify({
        type: 'INVOICE',
        to, cc: null,
        subject,
        body_text: `Please find Invoice ${inv.invoice_no} attached.`,
        attachments: [{ r2_key: pdfKey, filename: `Invoice_${inv.invoice_no}.pdf` }],
        status: 'QUEUED', reference: `invoice:${invoiceId}`,
        created_at_utc: nowIso(), created_by: user?.id || null,
      })
    });

    if (!out.ok) {
      const err = await out.text();
      return withCORS(env, req, serverError(`Failed to queue email: ${err}`));
    }

    const outJson = await out.json().catch(() => ({}));
    const mailRow = Array.isArray(outJson) ? outJson[0] : outJson;
    const mailId = mailRow?.id || null;

    await writeAudit(env, user, 'EMAIL_QUEUED', { to, subject, invoice_pdf_r2_key: pdfKey, mail_id: mailId }, { entity: 'invoice', subject_id: invoiceId, correlation_id: mailId, req });
    return withCORS(env, req, ok({ queued: true, mail_id: mailId }));
  } catch {
    return withCORS(env, req, serverError('Failed to queue invoice email'));
  }
}


function ok(data, headers = {}) {
  return new Response(JSON.stringify(data), { status: 200, headers: { ...JSON_HEADERS, ...headers } });
}
function badRequest(msg, details) {
  return new Response(JSON.stringify({ error: msg, details }), { status: 400, headers: JSON_HEADERS });
}
function unauthorized(msg = "Unauthorized") {
  return new Response(JSON.stringify({ error: msg }), { status: 401, headers: JSON_HEADERS });
}
function forbidden(msg = "Forbidden") {
  return new Response(JSON.stringify({ error: msg }), { status: 403, headers: JSON_HEADERS });
}
function notFound(msg = "Not found") {
  return new Response(JSON.stringify({ error: msg }), { status: 404, headers: JSON_HEADERS });
}
function conflict(msg = "Conflict") {
  return new Response(JSON.stringify({ error: msg }), { status: 409, headers: JSON_HEADERS });
}
function tooLarge(msg = "Payload too large") {
  return new Response(JSON.stringify({ error: msg }), { status: 413, headers: JSON_HEADERS });
}
function unsupported(msg = "Unsupported Media Type") {
  return new Response(JSON.stringify({ error: msg }), { status: 415, headers: JSON_HEADERS });
}
function unprocessable(msg = "Unprocessable Entity") {
  return new Response(JSON.stringify({ error: msg }), { status: 422, headers: JSON_HEADERS });
}
function serverError(msg = "Internal Server Error") {
  return new Response(JSON.stringify({ error: msg }), { status: 500, headers: JSON_HEADERS });
}

function parseJSONBody(req) {
  return req.json().catch(() => null);
}
function splitCsv(s) {
  return String(s || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}
function buildCORSHeaders(env, reqOrigin) {
  const allowed = splitCsv(env.ALLOWED_ORIGINS || "");
  const h = {
    "Access-Control-Allow-Credentials": "true",
    "Access-Control-Allow-Headers": "authorization,content-type,content-md5,x-requested-with,idempotency-key,x-idempotency-key",
    "Access-Control-Allow-Methods": "GET,POST,PUT,OPTIONS",
    "Vary": "Origin",
  };
  if (!allowed.length) return h;
  if (reqOrigin && allowed.includes(reqOrigin)) h["Access-Control-Allow-Origin"] = reqOrigin;
  return h;
}
function withCORS(env, req, res) {
  const origin = req.headers.get("origin");
  const headers = buildCORSHeaders(env, origin);
  const newHeaders = new Headers(res.headers);
  for (const [k, v] of Object.entries(headers)) newHeaders.set(k, v);
  return new Response(res.body, { status: res.status, headers: newHeaders });
}
function preflightIfNeeded(env, req) {
  if (req.method === "OPTIONS") return withCORS(env, req, new Response(null, { status: 204 }));
  return null;
}

// ---------------------- Crypto helpers ----------------------
async function hmacSign(secret, data) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  return bufToBase64Url(sig);
}
async function hmacVerify(secret, data, signatureB64url) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"]
  );
  const sig = base64UrlToUint8(signatureB64url);
  return crypto.subtle.verify("HMAC", key, sig, new TextEncoder().encode(data));
}
async function sha256Hex(s) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  const arr = Array.from(new Uint8Array(d));
  return arr.map((b) => b.toString(16).padStart(2, "0")).join("");
}
function bufToBase64Url(buf) {
  let binary = "";
  const bytes = new Uint8Array(buf);
  for (let i = 0; i < bytes.byteLength; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}
function base64UrlToUint8(b64url) {
  const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/") + "==".slice((2 - (b64url.length * 3) % 4) % 4);
  const bin = atob(b64);
  const arr = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
  return arr;
}

// Tokens (compact): base64url(JSON).base64url(HMAC) with exp
async function createToken(secret, payload) {
  const data = bufToBase64Url(new TextEncoder().encode(JSON.stringify(payload)));
  const sig = await hmacSign(secret, data);
  return `${data}.${sig}`;
}
async function verifyToken(secret, token) {
  const [data, sig] = String(token || "").split(".");
  if (!data || !sig) return { ok: false, error: "Malformed token" };
  const ok = await hmacVerify(secret, data, sig);
  if (!ok) return { ok: false, error: "Invalid signature" };
  const payload = JSON.parse(new TextDecoder().decode(base64UrlToUint8(data)));
  const now = Math.floor(Date.now() / 1000);
  if (typeof payload.exp === "number" && now > payload.exp) return { ok: false, error: "Token expired" };
  return { ok: true, payload };
}

// ---------------------- Date / Time ----------------------
const LONDON_TZ = "Europe/London";
function londonDate(isoOrDate) {
  const d = new Date(isoOrDate);
  const fmt = new Intl.DateTimeFormat("en-GB", { timeZone: LONDON_TZ, year: "numeric", month: "2-digit", day: "2-digit" });
  const parts = fmt.formatToParts(d).reduce((acc, p) => ((acc[p.type] = p.value), acc), {});
  return `${parts.year}-${parts.month}-${parts.day}`;
}
function weekEndingSunday(dateYmd) {
  const [y, m, d] = dateYmd.split("-").map((x) => parseInt(x, 10));
  const dt = new Date(Date.UTC(y, m - 1, d));
  const dow = new Intl.DateTimeFormat("en-GB", { timeZone: LONDON_TZ, weekday: "short" }).format(dt).toLowerCase();
  const dayNum = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"].indexOf(dow.slice(0, 3));
  const add = 6 - (dayNum === -1 ? 0 : dayNum);
  const sunday = new Date(dt);
  sunday.setUTCDate(sunday.getUTCDate() + add);
  return londonDate(sunday);
}

// ---------------------- Booking ID ----------------------
function norm(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s\-@&\/,.:]/g, "");
}
async function makeBookingId(candidate_id, date_of_shift, hospital, ward, job_title, shift_label = "") {
  const base = `${norm(candidate_id)}|${date_of_shift}|${norm(hospital)}|${norm(ward)}|${norm(job_title)}|${norm(shift_label)}`;
  const hash = await sha256Hex(base);
  return `bk_${hash.slice(0, 16)}`;
}

// ---------------------- R2 helpers ----------------------
async function r2Head(env, key) {
  try { return await env.R2.head(key); } catch { return null; }
}
async function r2Get(env, key) {
  try { return await env.R2.get(key); } catch { return null; }
}
async function r2Put(env, key, body, opts) {
  return await env.R2.put(key, body, opts);
}

// ---------------------- Supabase REST ----------------------
function sbHeaders(env) {
  const key = env.SUPABASE_SERVICE_ROLE_KEY;
  return { "apikey": key, "Authorization": `Bearer ${key}`, "Content-Type": "application/json" };
}
async function sbFetch(env, url, includeCount = false) {
  const res = await fetch(url, { headers: { ...sbHeaders(env), ...(includeCount ? { Prefer: "count=exact" } : {}) } });
  const text = await res.text();
  let json = [];
  try { json = text ? JSON.parse(text) : []; } catch { json = []; }
  const countHeader = res.headers.get("content-range");
  const total = countHeader && /\/(\d+)$/.exec(countHeader) ? parseInt(/\/(\d+)$/.exec(countHeader)[1], 10) : undefined;
  if (!res.ok) throw new Error(`Supabase fetch failed ${res.status}: ${text}`);
  return { rows: json, total };
}
async function sbUpsertTimesheet(env, row) {
  const url = `${env.SUPABASE_URL}/rest/v1/timesheets?on_conflict=booking_id,version`;
  const res = await fetch(url, {
    method: "POST",
    headers: { ...sbHeaders(env), "Prefer": "resolution=merge-duplicates,return=representation" },
    body: JSON.stringify(row),
  });
  if (!res.ok) throw new Error(`Supabase upsert timesheet failed ${res.status}`);
  const json = await res.json().catch(() => ({}));
  return Array.isArray(json) ? json[0] : json;
}

// Helpers to read timesheets
async function sbGetTimesheetCurrent(env, booking_id) {
  const url = `${env.SUPABASE_URL}/rest/v1/timesheets?booking_id=eq.${encodeURIComponent(booking_id)}&is_current=eq.true&select=*`;
  const { rows } = await sbFetch(env, url);
  return rows[0] || null;
}
async function sbGetTimesheetByVersion(env, booking_id, version) {
  const url = `${env.SUPABASE_URL}/rest/v1/timesheets?booking_id=eq.${encodeURIComponent(booking_id)}&version=eq.${encodeURIComponent(version)}&select=*`;
  const { rows } = await sbFetch(env, url);
  return rows[0] || null;
}
async function sbMaxVersion(env, booking_id) {
  const url = `${env.SUPABASE_URL}/rest/v1/timesheets?booking_id=eq.${encodeURIComponent(booking_id)}&select=version&order=version.desc&limit=1`;
  const { rows } = await sbFetch(env, url);
  return rows.length ? rows[0].version : 0;
}

// ---------------------- Business rules ----------------------
function isEligibleWindow(worked_end_iso) {
  // Shift must be happening now OR finished within last 4 hours.
  const now = Date.now();
  const end = new Date(worked_end_iso).getTime();
  return now >= end - 1000 * 60 * 60 * 12 && now <= end + 1000 * 60 * 60 * 4;
}
function minutesBetween(aIso, bIso) {
  const a = new Date(aIso).getTime();
  const b = new Date(bIso).getTime();
  return Math.round((b - a) / 60000);
}
function isPng(contentType) {
  return /^image\/png(?:;|$)/i.test(contentType || "");
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// AUTH SECTION (login/forgot/reset/refresh/logout)
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

// ENV expected (Option A: Cloudflare Pages frontend + Worker API):
// - SUPABASE_URL
// - SUPABASE_SERVICE_ROLE_KEY
// - ALLOWED_ORIGINS                 (CSV of exact origins for CORS, e.g. "https://tms.example.com")
// - SESSION_TOKEN_SECRET            (HMAC secret for access/refresh tokens)
// - COOKIE_NAME                     (default 'ctms_refresh')
// - COOKIE_DOMAIN                   (e.g. '.example.com' for same-site subdomains)
// - COOKIE_SAME_SITE                ('Lax' recommended for same-site; 'None' if truly cross-site)
// - ACCESS_TTL_SECONDS              (default 900 = 15m)
// - REFRESH_TTL_SECONDS             (default 1209600 = 14d)
// - PASSWORD_RESET_TTL_SECONDS      (default 3600)
// - UPLOAD_TOKEN_SECRET             (HMAC secret for upload/download token mint/verify)
// Bindings:
// - SESSIONS (KV namespace)         (KV for refresh sessions)
// - R2 (bucket for signatures)

const AUTH = {
  USERS_TABLE: 'tms_users',
  RESETS_TABLE: 'tms_password_resets',
};

function pickCookieSameSite(env) {
  const v = String(env.COOKIE_SAME_SITE || 'Lax');
  return (v === 'None' || v === 'Lax' || v === 'Strict') ? v : 'Lax';
}
function cookieName(env){ return String(env.COOKIE_NAME || 'ctms_refresh'); }

function setCookie(headers, name, value, { maxAgeSec, domain, sameSite='Lax', secure=true, httpOnly=true, path='/' } = {}) {
  const parts = [`${name}=${value || ''}`];
  if (path) parts.push(`Path=${path}`);
  if (domain) parts.push(`Domain=${domain}`);
  if (Number.isFinite(maxAgeSec)) parts.push(`Max-Age=${Math.max(0, maxAgeSec|0)}`);
  if (sameSite) parts.push(`SameSite=${sameSite}`);
  if (secure) parts.push('Secure');
  if (httpOnly) parts.push('HttpOnly');
  headers.append('Set-Cookie', parts.join('; '));
}
function parseCookies(req) {
  const raw = req.headers.get('cookie') || '';
  const out = {};
  raw.split(';').map(s => s.trim()).filter(Boolean).forEach(p=>{
    const i = p.indexOf('=');
    const k = i>=0 ? p.slice(0,i).trim() : p;
    const v = i>=0 ? p.slice(i+1) : '';
    out[k] = v;
  });
  return out;
}

// ── Password hashing (PBKDF2-SHA256) ─────────────────────────
const MAX_PBKDF2_ITER = 100000; // Cloudflare limit

async function pbkdf2Hash(password, iterations = 100000) {
  const iters = Math.min(Number(iterations) || 100000, MAX_PBKDF2_ITER);
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(password), { name:'PBKDF2' }, false, ['deriveBits']);
  const bits = await crypto.subtle.deriveBits({ name:'PBKDF2', hash:'SHA-256', salt, iterations: iters }, key, 256);
  const hashB64 = bufToBase64Url(bits);
  const saltB64 = bufToBase64Url(salt);
  return `pbkdf2:sha256$${iters}$${saltB64}$${hashB64}`;
}

async function pbkdf2Verify(password, stored) {
  // format: pbkdf2:sha256$ITER$SALT$HASH
  const m = /^pbkdf2:sha256\$(\d+)\$([A-Za-z0-9\-_]+)\$([A-Za-z0-9\-_]+)$/.exec(String(stored||''));
  if (!m) return false;
  const iterations = parseInt(m[1],10);
  const salt = base64UrlToUint8(m[2]);
  const want = base64UrlToUint8(m[3]);
  if (!Number.isFinite(iterations) || iterations <= 0 || iterations > MAX_PBKDF2_ITER) {
    // Iteration count not supported on this platform → treat as mismatch (avoids 500)
    console.warn('PBKDF2 iteration count not supported:', iterations);
    return false;
  }
  try {
    const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(password), { name:'PBKDF2' }, false, ['deriveBits']);
    const bits = await crypto.subtle.deriveBits({ name:'PBKDF2', hash:'SHA-256', salt, iterations }, key, want.byteLength*8);
    const got = new Uint8Array(bits);
    if (got.byteLength !== want.byteLength) return false;
    let diff = 0;
    for (let i=0;i<got.byteLength;i++) diff |= (got[i]^want[i]);
    return diff === 0;
  } catch (e) {
    console.warn('PBKDF2 verify failed:', e);
    return false;
  }
}

// â”€â”€ Supabase helpers for users / resets â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function sbAuthHeaders(env){
  const k = env.SUPABASE_SERVICE_ROLE_KEY;
  return { 'apikey': k, 'Authorization': `Bearer ${k}`, 'Content-Type': 'application/json' };
}
async function sbGetUserByEmail(env, email) {
  const url = `${env.SUPABASE_URL}/rest/v1/${AUTH.USERS_TABLE}?email=eq.${encodeURIComponent(email)}&select=id,email,role,is_active,password_hash,session_version`;
  const res = await fetch(url, { headers: sbAuthHeaders(env) });
  const json = await res.json().catch(()=>[]);
  return Array.isArray(json) && json[0] ? json[0] : null;
}
async function sbGetUserById(env, id) {
  const url = `${env.SUPABASE_URL}/rest/v1/${AUTH.USERS_TABLE}?id=eq.${encodeURIComponent(id)}&select=id,email,role,is_active,password_hash,session_version`;
  const res = await fetch(url, { headers: sbAuthHeaders(env) });
  const json = await res.json().catch(()=>[]);
  return Array.isArray(json) && json[0] ? json[0] : null;
}
async function sbUpdateUserPassword(env, user_id, newHash) {
  const url = `${env.SUPABASE_URL}/rest/v1/${AUTH.USERS_TABLE}?id=eq.${encodeURIComponent(user_id)}`;
  const res = await fetch(url, { method:'PATCH', headers: { ...sbAuthHeaders(env), 'Prefer':'return=representation' }, body: JSON.stringify({ password_hash: newHash }) });
  if (!res.ok) throw new Error(`password update failed ${res.status}`);
  // bump session_version:
  const current = await sbGetUserById(env, user_id);
  const svRes = await fetch(url, { method:'PATCH', headers: { ...sbAuthHeaders(env), 'Prefer':'return=representation' }, body: JSON.stringify({ session_version: (current.session_version|0) + 1 }) });
  if (!svRes.ok) throw new Error(`session_version bump failed ${svRes.status}`);
  const j = await svRes.json().catch(()=>[]);
  return Array.isArray(j) && j[0] ? j[0] : null;
}
async function sbInsertResetToken(env, user_id, ttlSec) {
  const token = bufToBase64Url(crypto.getRandomValues(new Uint8Array(32)));
  const expires_at = new Date(Date.now() + (ttlSec*1000)).toISOString();
  const url = `${env.SUPABASE_URL}/rest/v1/${AUTH.RESETS_TABLE}`;
  const row = { user_id, token, expires_at, used_at: null };
  const res = await fetch(url, { method:'POST', headers: { ...sbAuthHeaders(env), 'Prefer':'return=representation' }, body: JSON.stringify(row) });
  if (!res.ok) throw new Error(`insert reset token failed ${res.status}`);
  return token;
}
async function sbConsumeResetToken(env, token) {
  const selUrl = `${env.SUPABASE_URL}/rest/v1/${AUTH.RESETS_TABLE}?token=eq.${encodeURIComponent(token)}&select=id,user_id,expires_at,used_at`;
  const r = await fetch(selUrl, { headers: sbAuthHeaders(env) });
  const arr = await r.json().catch(()=>[]);
  const row = Array.isArray(arr) ? arr[0] : null;
  if (!row) return { ok:false, error:'INVALID_OR_EXPIRED_RESET' };
  if (row.used_at) return { ok:false, error:'INVALID_OR_EXPIRED_RESET' };
  if (new Date(row.expires_at).getTime() < Date.now()) return { ok:false, error:'INVALID_OR_EXPIRED_RESET' };
  const updUrl = `${env.SUPABASE_URL}/rest/v1/${AUTH.RESETS_TABLE}?id=eq.${encodeURIComponent(row.id)}`;
  const u = await fetch(updUrl, { method:'PATCH', headers: { ...sbAuthHeaders(env), 'Prefer':'return=representation' }, body: JSON.stringify({ used_at: new Date().toISOString() }) });
  if (!u.ok) return { ok:false, error:'RESET_CONSUME_FAILED' };
  return { ok:true, user_id: row.user_id };
}

// â”€â”€ Access/Refresh tokens (HMAC) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function accessTtl(env){ return parseInt(env.ACCESS_TTL_SECONDS || '900', 10) || 900; }          // 15m
function refreshTtl(env){ return parseInt(env.REFRESH_TTL_SECONDS || '1209600', 10) || 1209600; } // 14d
function resetTtl(env){ return parseInt(env.PASSWORD_RESET_TTL_SECONDS || '3600', 10) || 3600; }   // 60m
function sessionSecret(env){ return String(env.SESSION_TOKEN_SECRET); }

async function mintAccessToken(env, { user_id, email, role, sv, sid }) {
  const exp = Math.floor(Date.now()/1000) + accessTtl(env);
  const payload = { typ:'access', sub:user_id, email, role, sv, sid, iat: Math.floor(Date.now()/1000), exp };
  const token = await createToken(sessionSecret(env), payload);
  return { token, exp };
}
async function mintRefreshToken(env, { sid, sv }) {
  const exp = Math.floor(Date.now()/1000) + refreshTtl(env);
  const payload = { typ:'refresh', sid, sv, iat: Math.floor(Date.now()/1000), exp };
  const token = await createToken(sessionSecret(env), payload);
  return { token, exp };
}

// Bearer access-token guard for /api/* routes
async function requireUser(env, req, allowedRoles = []) {
  const hdr = req.headers.get('authorization') || '';
  const m = hdr.match(/^Bearer\s+(.+)$/i);
  if (!m) return null;

  const ver = await verifyToken(sessionSecret(env), m[1]);
  if (!ver.ok) return null;

  const p = ver.payload || {};
  if (p.typ !== 'access' || !p.sub) return null;

  // Ensure the user still exists, is active, and session_version matches
  const user = await sbGetUserById(env, p.sub);
  if (!user || user.is_active !== true) return null;
  if ((user.session_version|0) !== (p.sv|0)) return null;

  if (Array.isArray(allowedRoles) && allowedRoles.length && !allowedRoles.includes(user.role)) return null;
  return { id: user.id, email: user.email, role: user.role, sv: user.session_version|0, sid: p.sid };
}

// KV session helpers (store sid → { user_id, sv, exp })
async function kvPutSession(env, sid, data, ttlSec) {

  await env.SESSIONS.put(`sid:${sid}`, JSON.stringify(data), { expirationTtl: ttlSec });
}
async function kvGetSession(env, sid) {
  const t = await env.SESSIONS.get(`sid:${sid}`);
  return t ? JSON.parse(t) : null;
}
async function kvDelSession(env, sid) {
  await env.SESSIONS.delete(`sid:${sid}`);
}

// â”€â”€ Auth handlers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function handleAuthLogin(env, req) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;
  const body = await parseJSONBody(req);
  if (!body) return badRequest('invalid_json');

  const email = String((body.email||'')).trim().toLowerCase();
  const pw    = String(body.password||'');
  if (!email || !pw) return badRequest('email_and_password_required');

  const user = await sbGetUserByEmail(env, email);
  if (!user || user.is_active !== true) return unauthorized('Invalid credentials');

  const okPw = await pbkdf2Verify(pw, user.password_hash || '');
  if (!okPw) return unauthorized('Invalid credentials');

  // Create KV session + tokens
  const sid = bufToBase64Url(crypto.getRandomValues(new Uint8Array(16)));
  const sv  = user.session_version|0 || 1;
  const refresh = await mintRefreshToken(env, { sid, sv });
  const access  = await mintAccessToken(env, { user_id: user.id, email: user.email, role: user.role, sv, sid });

  await kvPutSession(env, sid, { user_id: user.id, sv, exp: refresh.exp }, refreshTtl(env));

  const headers = new Headers(JSON_HEADERS);
  setCookie(headers, cookieName(env), refresh.token, {
    maxAgeSec: refreshTtl(env),
    domain: env.COOKIE_DOMAIN || undefined,
    sameSite: pickCookieSameSite(env),
    secure: true,
    httpOnly: true,
    path: '/'
  });

  return new Response(JSON.stringify({
    ok: true,
    access_token: access.token,
    expires_in: accessTtl(env),
    user: { id: user.id, email: user.email, role: user.role }
  }), { status: 200, headers });
}

async function handleAuthRefresh(env, req) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;
  const cookies = parseCookies(req);
  const raw = cookies[cookieName(env)];
  if (!raw) return unauthorized('No refresh cookie');

  const ver = await verifyToken(sessionSecret(env), raw);
  if (!ver.ok) return unauthorized('Invalid refresh token');
  const { typ, sid, sv, exp } = ver.payload || {};
  if (typ !== 'refresh' || !sid) return unauthorized('Invalid refresh claims');
  if ((exp|0) <= Math.floor(Date.now()/1000)) return unauthorized('Refresh expired');

  const sess = await kvGetSession(env, sid);
  if (!sess) return unauthorized('Session not found');

  // Check session_version still valid
  const user = await sbGetUserById(env, sess.user_id);
  if (!user || user.is_active !== true) return unauthorized('User disabled');
  if ((user.session_version|0) !== (sv|0)) {
    await kvDelSession(env, sid);
    return unauthorized('Session version changed');
  }

  const access = await mintAccessToken(env, {
    user_id: user.id, email: user.email, role: user.role, sv, sid
  });

  const headers = new Headers(JSON_HEADERS);
  // Optional: rotate refresh if near expiry (<3d)
  const secondsLeft = exp - Math.floor(Date.now()/1000);
  if (secondsLeft < (3*24*60*60)) {
    const next = await mintRefreshToken(env, { sid, sv });
    await kvPutSession(env, sid, { user_id: user.id, sv, exp: next.exp }, refreshTtl(env));
    setCookie(headers, cookieName(env), next.token, {
      maxAgeSec: refreshTtl(env),
      domain: env.COOKIE_DOMAIN || undefined,
      sameSite: pickCookieSameSite(env),
      secure: true, httpOnly: true, path:'/'
    });
  }

  return new Response(JSON.stringify({
    access_token: access.token,
    expires_in: accessTtl(env),
    user: { id: user.id, email: user.email, role: user.role }
  }), { status: 200, headers });
}

async function handleAuthLogout(env, req) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;
  const cookies = parseCookies(req);
  const raw = cookies[cookieName(env)];
  if (raw) {
    const ver = await verifyToken(sessionSecret(env), raw);
    if (ver.ok && ver.payload && ver.payload.sid) {
      await kvDelSession(env, ver.payload.sid);
    }
  }
  const headers = new Headers(JSON_HEADERS);
  setCookie(headers, cookieName(env), '', {
    maxAgeSec: 0, domain: env.COOKIE_DOMAIN || undefined, sameSite: pickCookieSameSite(env), secure:true, httpOnly:true, path:'/'
  });
  return new Response(JSON.stringify({ ok:true }), { status:200, headers });
}

async function handleAuthForgot(env, req) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;
  const body = await parseJSONBody(req);
  if (!body) return badRequest('invalid_json');
  const email = String((body.email||'')).trim().toLowerCase();
  if (!email) return badRequest('email_required');

  const user = await sbGetUserByEmail(env, email);
  if (user && user.is_active === true) {
    await sbInsertResetToken(env, user.id, resetTtl(env));
    // Send email via your mailer with a link containing ?k=<token> (not implemented here)
  }
  return ok({ ok:true }); // privacy-safe
}

async function handleAuthReset(env, req) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;
  const body = await parseJSONBody(req);
  if (!body) return badRequest('invalid_json');

  const token = String(body.token||'');
  const newPw = String(body.new_password||'');
  if (!token || !newPw) return badRequest('token_and_new_password_required');

  const strong = newPw.length>=8 && /[a-z]/.test(newPw) && /[A-Z]/.test(newPw) && /[0-9]/.test(newPw);
  if (!strong) return new Response(JSON.stringify({ ok:false, error:'WEAK_PASSWORD' }), { status:400, headers: JSON_HEADERS });

  const consumed = await sbConsumeResetToken(env, token);
  if (!consumed.ok) return new Response(JSON.stringify({ ok:false, error: consumed.error }), { status:400, headers: JSON_HEADERS });

  const hash = await pbkdf2Hash(newPw);
  await sbUpdateUserPassword(env, consumed.user_id, hash);

  return ok({ ok:true });
}

// ---------------------- UK timezone check ----------------------
async function handleUKTimeCheck(env, req) {
  const body = await parseJSONBody(req);
  if (!body) return withCORS(env, req, badRequest('invalid_json'));

  const phone_tz = String(body.phone_tz || '');
  const phone_epoch_ms = Number(body.phone_epoch_ms);
  const tolerance_ms = Math.max(0, Number(env.UK_TZ_SKEW_MS ?? 180000));

  const broker_epoch_ms = Date.now();
  const broker_tz = 'Europe/London';

  const tzOk = phone_tz === broker_tz;
  const skew_ms = Number.isFinite(phone_epoch_ms)
    ? Math.abs(broker_epoch_ms - phone_epoch_ms)
    : NaN;
  const skewOk = Number.isFinite(skew_ms) && skew_ms <= tolerance_ms;

  const valid = tzOk && skewOk;
  const reason = valid
    ? 'ok'
    : (!tzOk ? 'tz_mismatch' : (!Number.isFinite(skew_ms) ? 'invalid_phone_epoch' : 'clock_skew'));

  return withCORS(env, req, ok({
    valid,
    reason,
    tolerance_ms,
    skew_ms,
    broker_epoch_ms,
    broker_tz,
  }));
}

// ---------------------- Query builder for list ----------------------
function buildTimesheetsQuery(env, q) {
  const url = new URL(`${env.SUPABASE_URL}/rest/v1/timesheets`);
  url.searchParams.set("select", "*");

  const add = (k, v) => url.searchParams.set(k, v);

  if (q.booking_id) add("booking_id", `eq.${q.booking_id}`);
  if (q.booking_ids && q.booking_ids.length) add("booking_id", `in.(${q.booking_ids.map(encodeURIComponent).join(",")})`);

  if (q.candidate_id) add("occupant_key_norm", `eq.${q.candidate_id}`);
  if (q.candidate_ids && q.candidate_ids.length) add("occupant_key_norm", `in.(${q.candidate_ids.map(encodeURIComponent).join(",")})`);

  if (q.week_ending) add("week_ending_date", `eq.${q.week_ending}`);
  if (q.week_endings && q.week_endings.length) add("week_ending_date", `in.(${q.week_endings.map(encodeURIComponent).join(",")})`);

  if (q.status) add("status", `eq.${q.status}`);
  if (q.statuses && q.statuses.length) add("status", `in.(${q.statuses.map(encodeURIComponent).join(",")})`);

  if (q.job_title) add("job_title_norm", `eq.${q.job_title}`);
  if (q.job_titles && q.job_titles.length) add("job_title_norm", `in.(${q.job_titles.map(encodeURIComponent).join(",")})`);

  const hospMode = q.hospital_match || "contains";
  if (q.hospital) {
    if (hospMode === "exact") add("hospital_norm", `eq.${q.hospital.toLowerCase()}`);
    else if (hospMode === "prefix") add("hospital_norm", `like.${q.hospital.toLowerCase()}%`);
    else add("hospital_norm", `like.%${q.hospital.toLowerCase()}%`);
  }
  const wardMode = q.ward_match || "contains";
  if (q.ward) {
    if (wardMode === "exact") add("ward_norm", `eq.${q.ward.toLowerCase()}`);
    else if (wardMode === "prefix") add("ward_norm", `like.${q.ward.toLowerCase()}%`);
    else add("ward_norm", `like.%${q.ward.toLowerCase()}%`);
  }

  if (q.version) add("version", `eq.${q.version}`);
  else if (String(q.current_only ?? "true").toLowerCase() !== "false") add("is_current", "eq.true");

  if (q.sort) {
    const col = q.sort;
    const ord = q.order === "asc" ? "asc" : "desc";
    url.searchParams.set("order", `${col}.${ord}`);
  }
  const limit = Math.min(parseInt(q.limit || "50", 10) || 50, 200);
  const offset = parseInt(q.offset || "0", 10) || 0;
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));

  return url.toString();
}

// ---------------------- Upload/submit/presign ----------------------
async function handlePresign(env, req) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;

  const body = await parseJSONBody(req);
  if (!body) return withCORS(env, req, badRequest("Invalid JSON"));

  const {
    occupant_key: candidate_id,
    date_start_local,  // YYYY-MM-DD (local)
    hospital,
    ward,
    job_title,
    shift_label = "",
    resubmission_of,
  } = body;

  if (!candidate_id || !date_start_local || !hospital || !ward || !job_title) {
    return withCORS(env, req, badRequest("Missing required fields"));
  }

  const booking_id = resubmission_of || await makeBookingId(candidate_id, date_start_local, hospital, ward, job_title, shift_label);
  const week_ending_date = weekEndingSunday(date_start_local);
  const weCompact = week_ending_date.replace(/-/g, "");

  const maxV = await sbMaxVersion(env, booking_id);
  const version = maxV > 0 ? maxV + 1 : 1;

  const nurseKey = `/we=${weCompact}/${booking_id}/v${version}/nurse.png`;
  const authKey  = `/we=${weCompact}/${booking_id}/v${version}/authoriser.png`;

  const maxBytes = parseInt(env.UPLOAD_MAX_BYTES || "300000", 10);
  const expiresSec = parseInt(env.PRESIGN_EXPIRES_SECONDS || "600", 10);
  const exp = Math.floor(Date.now() / 1000) + expiresSec;
  const secret = env.UPLOAD_TOKEN_SECRET;

  const nurseToken = await createToken(secret, { typ: "upload", booking_id, version, role: "nurse", key: nurseKey, exp });
  const authToken  = await createToken(secret, { typ: "upload", booking_id, version, role: "authoriser", key: authKey, exp });

  const uploadBase = new URL(req.url);
  uploadBase.pathname = "/upload";
  const mkUrl = (key, token, role) => {
    const u = new URL(uploadBase);
    u.searchParams.set("key", key);
    u.searchParams.set("booking_id", booking_id);
    u.searchParams.set("version", String(version));
    u.searchParams.set("role", role);
    u.searchParams.set("token", token);
    return u.toString();
  };

  return withCORS(env, req, ok({
    booking_id,
    version,
    week_ending_date,
    upload: {
      content_type: "image/png",
      max_bytes: maxBytes,
      expires_at: new Date(exp * 1000).toISOString(),
      nurse: { key: nurseKey, put_url: mkUrl(nurseKey, nurseToken, "nurse"), token: nurseToken },
      authoriser: { key: authKey, put_url: mkUrl(authKey, authToken, "authoriser"), token: authToken },
    },
  }));
}

async function handleUpload(env, req, url) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;

  const key = url.searchParams.get("key") || "";
  const booking_id = url.searchParams.get("booking_id") || "";
  const role = url.searchParams.get("role") || "";
  const token = url.searchParams.get("token") || "";

  const secret = env.UPLOAD_TOKEN_SECRET;
  const ver = await verifyToken(secret, token);
  if (!ver.ok) return withCORS(env, req, unauthorized("Invalid token"));
  const p = ver.payload;
  if (p.typ !== "upload" || p.booking_id !== booking_id || p.role !== role || p.key !== key) {
    return withCORS(env, req, unauthorized("Token mismatch"));
  }

  const keyOk = /^\/we=\d{8}\/bk_[a-f0-9]{16}(?:\/v\d+)?\/(nurse|authoriser)\.png$/.test(key);
  if (!keyOk) return withCORS(env, req, badRequest("Invalid key"));

  const ct = req.headers.get("content-type") || "";
  if (!isPng(ct)) return withCORS(env, req, unsupported("Only image/png allowed"));

  const maxBytes = parseInt(env.UPLOAD_MAX_BYTES || "300000", 10);
  const contentLength = parseInt(req.headers.get("content-length") || "0", 10);
  if (contentLength > maxBytes) return withCORS(env, req, tooLarge(`Max ${maxBytes} bytes`));

  const requireMd5 = String(env.REQUIRE_MD5 || "false").toLowerCase() === "true";
  const md5 = req.headers.get("content-md5");
  if (requireMd5 && !md5) return withCORS(env, req, badRequest("Content-MD5 required"));

  const we = (key.match(/^\/we=(\d{8})\//) || [])[1];
  const versionMatch = key.match(/\/v(\d+)\//);
  const version = versionMatch ? parseInt(versionMatch[1], 10) : 1;
  const week_ending_date = we ? `${we.slice(0,4)}-${we.slice(4,6)}-${we.slice(6,8)}` : undefined;

  const putRes = await r2Put(env, key, req.body, {
    httpMetadata: { contentType: "image/png" },
    customMetadata: {
      bookingid: booking_id,
      weekending: week_ending_date || "",
      role,
      version: String(version),
    },
  });
  const size = contentLength || undefined;
  return withCORS(env, req, ok({ ok: true, role, key, etag: putRes?.etag, size, version }));
}

async function handleSubmit(env, req) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;

  const body = await parseJSONBody(req);
  if (!body) return withCORS(env, req, badRequest("Invalid JSON"));

  const required = [
    "booking_id", "scheduled_start_iso", "scheduled_end_iso",
    "worked_start_iso", "worked_end_iso",
    "break_start_iso", "break_end_iso",
    "auth_name", "auth_job_title", "nurse_key", "authoriser_key", "idempotency_key"
  ];
  for (const k of required) if (!body[k]) return withCORS(env, req, badRequest(`Missing ${k}`));

  if (!isEligibleWindow(body.worked_end_iso)) {
    return withCORS(env, req, new Response(JSON.stringify({ error: "Shift not in eligible window (must be ongoing or ended â‰¤ 4h)", code: "INELIGIBLE" }), { status: 422, headers: JSON_HEADERS }));
  }

  const nurseHead = await r2Head(env, body.nurse_key);
  const authHead  = await r2Head(env, body.authoriser_key);
  if (!nurseHead || !authHead) return withCORS(env, req, badRequest("Signatures not uploaded"));

  const worked_date_local = londonDate(body.worked_start_iso);
  const week_ending_date = weekEndingSunday(worked_date_local);
  const break_minutes = minutesBetween(body.break_start_iso, body.break_end_iso);
  const worked_minutes = minutesBetween(body.worked_start_iso, body.worked_end_iso);
  const break_expected = parseInt(env.BREAK_EXPECTED_MINUTES || "60", 10);

  let version = parseInt(body.version || "0", 10);
  if (!version || Number.isNaN(version)) {
    const m = body.nurse_key.match(/\/v(\d+)\//) || body.authoriser_key.match(/\/v(\d+)\//);
    version = m ? parseInt(m[1], 10) : 1;
  }

  const current = await sbGetTimesheetCurrent(env, body.booking_id);
  if (current && current.is_current === true) {
    const maxV = await sbMaxVersion(env, body.booking_id);
    if (maxV >= 1) return withCORS(env, req, conflict("A current timesheet exists for this booking. Revoke before resubmitting."));
  }

  const row = {
    booking_id: body.booking_id,
    version,
    is_current: true,

    occupant_key_norm: (body.candidate_id || body.occupant_key || "").toLowerCase(),
    hospital_norm: (body.hospital || "").toLowerCase(),
    ward_norm: (body.ward || "").toLowerCase(),
    job_title_norm: (body.job_title || "").toLowerCase(),
    shift_label_norm: (body.shift_label || "").toLowerCase() || null,

    scheduled_start_iso: body.scheduled_start_iso,
    scheduled_end_iso: body.scheduled_end_iso,
    worked_start_iso: body.worked_start_iso,
    worked_end_iso: body.worked_end_iso,
    break_start_iso: body.break_start_iso,
    break_end_iso: body.break_end_iso,
    break_minutes,
    worked_minutes,

    week_ending_date,

    auth_name: body.auth_name,
    auth_job_title: body.auth_job_title,
    authorised_at_server: new Date().toISOString(),

    r2_nurse_key: body.nurse_key,
    r2_auth_key: body.authoriser_key,

    status: "SUBMITTED",
    idempotency_key: body.idempotency_key,
    client_hash: body.client_hash || null,
    client_ua: body.client_user_agent || req.headers.get("user-agent") || "",
  };

  let ts;
  try {
    ts = await sbUpsertTimesheet(env, row);
  } catch (e) {
    return withCORS(env, req, serverError(`DB upsert failed: ${e.message}`));
  }

  const ts_id = ts?.timesheet_id || null;
  return withCORS(env, req, ok({ ok: true, timesheet_id: ts_id, status: "SUBMITTED", break_ok: break_minutes === break_expected, version }));
}

// ---------------------- Revoke flows ----------------------
async function handleRevoke(env, req) {
  const body = await parseJSONBody(req);
  if (!body) return badRequest("Invalid JSON");
  const { booking_id, reason = null, actor = "candidate" } = body;
  if (!booking_id) return badRequest("booking_id required");

  const current = await sbGetTimesheetCurrent(env, booking_id);
  if (!current) return notFound("No current timesheet to revoke");

  const url = `${env.SUPABASE_URL}/rest/v1/timesheets?booking_id=eq.${encodeURIComponent(booking_id)}&is_current=eq.true`;
  const patch = {
    is_current: false,
    status: "REVOKED",
    revoked_at: new Date().toISOString(),
    revoked_reason: reason,
    revoked_by: actor,
  };
  const res = await fetch(url, {
    method: "PATCH",
    headers: { ...sbHeaders(env), "Prefer": "return=representation" },
    body: JSON.stringify(patch),
  });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    return serverError(`Revoke failed: ${res.status} ${t}`);
  }
  const json = await res.json().catch(() => []);
  const revoked = Array.isArray(json) ? json[0] : json;

  const next_version = (await sbMaxVersion(env, booking_id)) + 1;
  return ok({ ok: true, booking_id, current_revoked: true, timesheet_id: revoked?.timesheet_id || null, next_version });
}

async function handleRevokeAndPresign(env, req) {
  const pre = preflightIfNeeded(env, req); if (pre) return pre;
  const body = await parseJSONBody(req);
  if (!body) return withCORS(env, req, badRequest("Invalid JSON"));
  const { booking_id, reason = null, actor = "candidate" } = body;
  if (!booking_id) return withCORS(env, req, badRequest("booking_id required"));

  const current = await sbGetTimesheetCurrent(env, booking_id);
  if (!current) return withCORS(env, req, notFound("No current timesheet to revoke"));

  const url = `${env.SUPABASE_URL}/rest/v1/timesheets?booking_id=eq.${encodeURIComponent(booking_id)}&is_current=eq.true`;
  const patch = { is_current: false, status: "REVOKED", revoked_at: new Date().toISOString(), revoked_reason: reason, revoked_by: actor };
  const res = await fetch(url, { method: "PATCH", headers: { ...sbHeaders(env), "Prefer": "return=representation" }, body: JSON.stringify(patch) });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    return withCORS(env, req, serverError(`Revoke failed: ${res.status} ${t}`));
  }

  const next_version = (await sbMaxVersion(env, booking_id)) + 1;
  const week_ending_date = current.week_ending_date;
  const weCompact = week_ending_date.replace(/-/g, "");
  const nurseKey = `/we=${weCompact}/${booking_id}/v${next_version}/nurse.png`;
  const authKey  = `/we=${weCompact}/${booking_id}/v${next_version}/authoriser.png`;

  const maxBytes = parseInt(env.UPLOAD_MAX_BYTES || "300000", 10);
  const expiresSec = parseInt(env.PRESIGN_EXPIRES_SECONDS || "600", 10);
  const exp = Math.floor(Date.now() / 1000) + expiresSec;
  const secret = env.UPLOAD_TOKEN_SECRET;
  const nurseToken = await createToken(secret, { typ: "upload", booking_id, version: next_version, role: "nurse", key: nurseKey, exp });
  const authToken  = await createToken(secret, { typ: "upload", booking_id, version: next_version, role: "authoriser", key: authKey, exp });

  const base = new URL(req.url); base.pathname = "/upload";
  const mkUrl = (key, token, role) => {
    const u = new URL(base);
    u.searchParams.set("key", key);
    u.searchParams.set("booking_id", booking_id);
    u.searchParams.set("version", String(next_version));
    u.searchParams.set("role", role);
    u.searchParams.set("token", token);
    return u.toString();
  };

  return withCORS(env, req, ok({
    ok: true,
    booking_id,
    version: next_version,
    week_ending_date,
    upload: {
      content_type: "image/png",
      max_bytes: maxBytes,
      expires_at: new Date(exp * 1000).toISOString(),
      nurse: { key: nurseKey, put_url: mkUrl(nurseKey, nurseToken, "nurse"), token: nurseToken },
      authoriser: { key: authKey, put_url: mkUrl(authKey, authToken, "authoriser"), token: authToken },
    },
  }));
}

// ---------------------- Reads ----------------------
async function handleGetOne(env, req, booking_id, url) {
  const version = url.searchParams.get("version");
  const currentOnly = String(url.searchParams.get("current_only") ?? "true").toLowerCase() !== "false";

  let row = null;
  if (version) row = await sbGetTimesheetByVersion(env, booking_id, parseInt(version, 10));
  else if (currentOnly) row = await sbGetTimesheetCurrent(env, booking_id);
  else {
    const u = `${env.SUPABASE_URL}/rest/v1/timesheets?booking_id=eq.${encodeURIComponent(booking_id)}&select=*&order=version.desc&limit=10`;
    const { rows } = await sbFetch(env, u);
    row = rows;
  }
  if (!row || (Array.isArray(row) && !row.length)) return withCORS(env, req, notFound("Timesheet not found"));
  return withCORS(env, req, ok(row));
}

// ─────────────────────────────────────────────────────────────────────────────
// Timesheets: list with optional sign keys/urls
// ─────────────────────────────────────────────────────────────────────────────
async function handleList(env, req, url) {
  // Admin-only (exposes keys/URLs when requested)
  const user = await requireUser(env, req, ["admin"]);
  if (!user) return withCORS(env, req, unauthorized());

  const q = Object.fromEntries(url.searchParams.entries());
  const sbUrl = buildTimesheetsQuery(env, q);

  const includeCount = String(q.include_count ?? "false").toLowerCase() === "true";
  const { rows, total } = await sbFetch(env, sbUrl, includeCount);

  const include = new Set(String(q.include || "").split(",").map(s => s.trim()).filter(Boolean));
  const sign_which = String(q.sign_which || "both").toLowerCase();

  // Clamp expiry: 60s–900s, default 180s
  const expReq = parseInt(q.sign_expires_seconds || "180", 10);
  const sign_exp = Math.max(60, Math.min(Number.isFinite(expReq) ? expReq : 180, 900));

  const secret = env.UPLOAD_TOKEN_SECRET;
  if (!secret && include.has("sign_urls")) {
    return withCORS(env, req, serverError("UPLOAD_TOKEN_SECRET not configured"));
  }

  const items = await Promise.all(
    rows.map(async (r) => {
      const have = { nurse: !!r.r2_nurse_key, authoriser: !!r.r2_auth_key };
      const out = { ...r, signatures: { have } };

      if (include.has("sign_keys")) {
        out.signatures.keys = {
          nurse: r.r2_nurse_key || null,
          authoriser: r.r2_auth_key || null
        };
      }

      if (include.has("sign_urls")) {
        const addUrl = async (which, key) => {
          if (!key) return null;
          const exp = Math.floor(Date.now() / 1000) + sign_exp;
          const token = await createToken(secret, {
            typ: "dl",
            booking_id: r.booking_id,
            role: which,
            key,
            exp,
          });
          const u = new URL(req.url);
          u.pathname = "/signatures/get";
          u.search = "";
          u.searchParams.set("key", key);
          u.searchParams.set("booking_id", r.booking_id);
          u.searchParams.set("role", which);
          u.searchParams.set("token", token);
          return u.toString();
        };

        const urls = {};
        if (sign_which === "both" || sign_which === "nurse") {
          urls.nurse = await addUrl("nurse", r.r2_nurse_key);
        }
        if (sign_which === "both" || sign_which === "authoriser") {
          urls.authoriser = await addUrl("authoriser", r.r2_auth_key);
        }
        out.signatures.urls = urls;
      }

      return out;
    })
  );

  const resp = includeCount ? { items, count: total ?? undefined } : { items };
  return withCORS(env, req, ok(resp));
}

async function handleQuery(env, req) {
  const body = await parseJSONBody(req);
  if (!body) return badRequest("Invalid JSON");
  const q = { ...body };
  if (Array.isArray(q.booking_ids) && q.booking_ids.length > 0) q.booking_ids = q.booking_ids.slice(0, 500);
  const sbUrl = buildTimesheetsQuery(env, q);
  const { rows } = await sbFetch(env, sbUrl, !!q.include_count);
  return ok({ items: rows });
}

async function handleAuthorisedStatus(env, req) {
  const body = await parseJSONBody(req);
  if (!body || !Array.isArray(body.booking_ids) || !body.booking_ids.length) return badRequest("booking_ids array required");
  const ids = body.booking_ids.slice(0, 1000);
  const url = `${env.SUPABASE_URL}/rest/v1/timesheets?booking_id=in.(${ids.map(encodeURIComponent).join(",")})&select=booking_id,authorised_at_server`;
  const { rows } = await sbFetch(env, url);
  const map = {};
  for (const r of rows) map[r.booking_id] = !!r.authorised_at_server;
  for (const id of ids) if (!(id in map)) map[id] = false;
  return ok({ statuses: map });
}

// ---------------------- Signatures: presign GET + proxy GET ----------------------
async function handleSignPresignGet(env, req) {
  const body = await parseJSONBody(req);
  if (!body) return badRequest("Invalid JSON");
  const { booking_id, which = "nurse", version = null, expires_seconds = 180 } = body;
  if (!booking_id || !["nurse", "authoriser"].includes(which)) return badRequest("booking_id and which required");

  const row = version ? await sbGetTimesheetByVersion(env, booking_id, parseInt(version, 10))
                      : await sbGetTimesheetCurrent(env, booking_id);
  if (!row) return notFound("Timesheet not found");
  const key = which === "nurse" ? row.r2_nurse_key : row.r2_auth_key;
  if (!key) return notFound("Signature not found");

  const exp = Math.min(parseInt(expires_seconds, 10) || 180, 900);
  const tokenExp = Math.floor(Date.now() / 1000) + exp;
  const secret = env.UPLOAD_TOKEN_SECRET;
  const token = await createToken(secret, { typ: "dl", booking_id, role: which, key, exp: tokenExp });

  const u = new URL(req.url);
  u.pathname = "/signatures/get"; u.search = "";
  u.searchParams.set("key", key);
  u.searchParams.set("booking_id", booking_id);
  u.searchParams.set("role", which);
  u.searchParams.set("token", token);

  return ok({ booking_id, which, get_url: u.toString(), expires_at: new Date(tokenExp * 1000).toISOString() });
}

async function handleSignPresignGetBatch(env, req) {
  const body = await parseJSONBody(req);
  if (!body || !Array.isArray(body.items) || !body.items.length) return badRequest("items array required");
  const items = body.items.slice(0, 100);
  const exp = Math.min(parseInt(body.expires_seconds || "300", 10) || 300, 900);
  const secret = env.UPLOAD_TOKEN_SECRET;

  const out = [];
  const not_found = [];

  for (const it of items) {
    const booking_id = it.booking_id;
    const which = it.which || "nurse";
    const version = it.version ? parseInt(it.version, 10) : null;
    if (!booking_id || !["nurse", "authoriser"].includes(which)) continue;

    const row = version ? await sbGetTimesheetByVersion(env, booking_id, version)
                        : await sbGetTimesheetCurrent(env, booking_id);
    if (!row) { not_found.push(booking_id); continue; }
    const key = which === "nurse" ? row.r2_nurse_key : row.r2_auth_key;
    if (!key) { not_found.push(booking_id); continue; }

    const tokenExp = Math.floor(Date.now() / 1000) + exp;
    const token = await createToken(secret, { typ: "dl", booking_id, role: which, key, exp: tokenExp });

    const u = new URL(req.url);
    u.pathname = "/signatures/get"; u.search = "";
    u.searchParams.set("key", key);
    u.searchParams.set("booking_id", booking_id);
    u.searchParams.set("role", which);
    u.searchParams.set("token", token);

    out.push({ booking_id, which, version: row.version, get_url: u.toString(), expires_at: new Date(tokenExp * 1000).toISOString() });
  }

  return ok({ links: out, not_found });
}

async function handleSignGet(env, req, url) {
  const key = url.searchParams.get("key") || "";
  const booking_id = url.searchParams.get("booking_id") || "";
  const role = url.searchParams.get("role") || "";
  const token = url.searchParams.get("token") || "";
  const secret = env.UPLOAD_TOKEN_SECRET;
  const ver = await verifyToken(secret, token);
  if (!ver.ok) return unauthorized("Invalid token");
  const p = ver.payload;
  if (p.typ !== "dl" || p.booking_id !== booking_id || p.role !== role || p.key !== key) return unauthorized("Token mismatch");
  const obj = await r2Get(env, key);
  if (!obj) return notFound("Not found");
  const headers = new Headers({ "content-type": "image/png", "cache-control": "private, max-age=300" });
  return new Response(obj.body, { status: 200, headers });
}
// ====================== SETTINGS (DEFAULTS) ======================
/**
 * @openapi
 * /api/settings/defaults:
 *   get:
 *     summary: Get global defaults (singleton)
 *     tags: [Settings]
 *     security:
 *       - bearerAuth: []
 *     responses:
 *       200:
 *         description: settings_defaults row
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 settings:
 *                   type: object
 *   put:
 *     summary: Update global defaults (singleton)
 *     tags: [Settings]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             description: Keys from settings_defaults to update
 *     responses:
 *       200:
 *         description: Updated settings_defaults row
 */
// -------------------------------------------
// SETTINGS (surface/save bank + VAT reg no.)
// -------------------------------------------
async function handleGetSettings(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized('Unauthorized');

  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=*`
    );
    if (!rows.length) {
      const { rows: alt } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/settings_defaults?select=*&limit=1`
      );
      if (!alt.length) return notFound("settings_defaults not found");
      const s2 = { ...alt[0] };
      delete s2.id;
      return withCORS(env, req, ok({ settings: s2 }));
    }
    const settings = { ...rows[0] };
    delete settings.id;
    return withCORS(env, req, ok({ settings }));
  } catch (e) {
    return withCORS(env, req, serverError("Failed to fetch settings_defaults"));
  }
}

async function handleUpdateSettings(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized('Unauthorized');

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  // Only allow known fields (but keep flexible)
  const allowed = [
    'timezone_id','day_start','day_end','night_start','night_end',
    'bh_source','bh_list','bh_feed_url',
    'vat_rate_pct','holiday_pay_pct','erni_pct','apply_holiday_to','apply_erni_to','margin_includes','effective_from',
    'bank_name','bank_sort_code','bank_account_number','vat_registration_number'
  ];
  const payload: any = { updated_at: new Date().toISOString() };
  for (const k of allowed) if (k in data) payload[k] = data[k];

  try {
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1`, {
      method: "PATCH",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Update failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const settings = Array.isArray(json) ? json[0] : json;
    delete settings.id;
    return withCORS(env, req, ok({ settings }));
  } catch {
    return withCORS(env, req, serverError("Failed to update settings_defaults"));
  }
}

// ====================== CLIENTS ======================
/**
 * @openapi
 * /api/clients:
 *   get:
 *     summary: List clients
 *     tags: [Clients]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: query
 *         name: include_count
 *         schema: { type: boolean }
 *       - in: query
 *         name: limit
 *         schema: { type: integer, minimum: 1, maximum: 200 }
 *       - in: query
 *         name: offset
 *         schema: { type: integer, minimum: 0 }
 *     responses:
 *       200:
 *         description: List of clients
 *   post:
 *     summary: Create client
 *     tags: [Clients]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *     responses:
 *       200:
 *         description: Created client
 */
async function handleListClients(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const url = new URL(`${env.SUPABASE_URL}/rest/v1/clients`);
  // Include all columns so ts_queries_email is returned
  url.searchParams.set("select", "*");

  const params = new URL(req.url).searchParams;
  const includeCount = params.get("include_count") === "true";
  const limit = Math.min(parseInt(params.get("limit") || "50", 10), 200);
  const offset = parseInt(params.get("offset") || "0", 10);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("order", "name.asc");

  try {
    const { rows, total } = await sbFetch(env, url.toString(), includeCount);
    const resp = includeCount ? { items: rows, count: total ?? undefined } : { items: rows };
    return withCORS(env, req, ok(resp));
  } catch {
    return withCORS(env, req, serverError("Failed to list clients"));
  }
}

async function handleCreateClient(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  // mileage_charge_rate and ts_queries_email are accepted by DB; other fields are passed through
  try {
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/clients`, {
      method: "POST",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data, created_at: new Date().toISOString() })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Client creation failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const client = Array.isArray(json) ? json[0] : json;
    return withCORS(env, req, ok({ client }));
  } catch {
    return withCORS(env, req, serverError("Failed to create client"));
  }
}

/**
 * @openapi
 * /api/clients/{id}:
 *   get:
 *     summary: Get client
 *     tags: [Clients]
 *     security:
 *       - bearerAuth: []
 *   put:
 *     summary: Update client
 *     tags: [Clients]
 *     security:
 *       - bearerAuth: []
 */
async function handleGetClient(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/clients?id=eq.${encodeURIComponent(clientId)}`
    );
    if (!rows.length) return withCORS(env, req, notFound("Client not found"));
    return withCORS(env, req, ok({ client: rows[0] }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch client"));
  }
}

// --------------------------------------------------
// UPDATE CLIENT (mark stale/enqueue on policy change)
// --------------------------------------------------
async function handleUpdateClient(env: any, req: Request, clientId: string) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  try {
    // Load existing for comparison (policy/mileage fields only; ts_queries_email is updated pass-through)
    const { rows: beforeRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/clients?id=eq.${encodeURIComponent(clientId)}&select=vat_chargeable,payment_terms_days,mileage_charge_rate,ts_queries_email`
    );
    const before = beforeRows?.[0] || {};

    const url = `${env.SUPABASE_URL}/rest/v1/clients?id=eq.${encodeURIComponent(clientId)}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data, updated_at: new Date().toISOString() })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Update failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const client = Array.isArray(json) ? json[0] : json;

    const policyChanged =
      (data.vat_chargeable != null && !!data.vat_chargeable !== !!before.vat_chargeable) ||
      (data.payment_terms_days != null && Number(data.payment_terms_days) !== Number(before.payment_terms_days));
    const mileageChargeChanged =
      (data.mileage_charge_rate != null && Number(data.mileage_charge_rate) !== Number(before.mileage_charge_rate));

    if (policyChanged || mileageChargeChanged) {
      // Mark related current/uninvoiced TSFIN as stale & enqueue
      await fetch(
        `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
        `?client_id=eq.${encodeURIComponent(clientId)}` +
        `&is_current=eq.true` +
        `&locked_by_invoice_id=is.null`,
        {
          method: "PATCH",
          headers: { ...sbHeaders(env), "Prefer": "return=minimal" },
          body: JSON.stringify({
            is_stale: true,
            stale_reason: 'CLIENT_SETTINGS_CHANGED',
            updated_at: new Date().toISOString()
          })
        }
      );

      const { rows: tsfins } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
        `?select=timesheet_id` +
        `&client_id=eq.${encodeURIComponent(clientId)}` +
        `&is_current=eq.true` +
        `&locked_by_invoice_id=is.null`
      );
      const toEnqueue = (tsfins || []).map((r: any) => ({ timesheet_id: r.timesheet_id, reason: 'POLICY_CHANGED' }));

      if (toEnqueue.length) {
        await fetch(
          `${env.SUPABASE_URL}/rest/v1/ts_financials_outbox?on_conflict=timesheet_id,reason`,
          {
            method: "POST",
            headers: { ...sbHeaders(env), "Prefer": "resolution=ignore-duplicates" },
            body: JSON.stringify(toEnqueue)
          }
        );
      }
    }

    return withCORS(env, req, ok({ client }));
  } catch {
    return withCORS(env, req, serverError("Failed to update client"));
  }
}


// ====================== CLIENT HOSPITALS ======================
/**
 * @openapi
 * /api/clients/{client_id}/hospitals:
 *   get:
 *     summary: List client hospitals
 *     tags: [Client Hospitals]
 *     security:
 *       - bearerAuth: []
 *   post:
 *     summary: Create client hospital
 *     tags: [Client Hospitals]
 *     security:
 *       - bearerAuth: []
 * /api/clients/{client_id}/hospitals/{hospital_id}:
 *   get:
 *     summary: Get client hospital
 *     tags: [Client Hospitals]
 *     security:
 *       - bearerAuth: []
 *   put:
 *     summary: Update client hospital
 *     tags: [Client Hospitals]
 *     security:
 *       - bearerAuth: []
 */
async function handleListHospitals(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/client_hospitals?client_id=eq.${encodeURIComponent(clientId)}&select=*`
    );
    return withCORS(env, req, ok({ items: rows }));
  } catch {
    return withCORS(env, req, serverError("Failed to list client hospitals"));
  }
}

async function handleCreateHospital(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  try {
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/client_hospitals`, {
      method: "POST",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({
        ...data,
        client_id: clientId,
        hospital_name_norm: data.hospital_name_norm || data.name || data.hospital,
        created_at: new Date().toISOString()
      })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Hospital creation failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const hospital = Array.isArray(json) ? json[0] : json;
    return withCORS(env, req, ok({ hospital }));
  } catch {
    return withCORS(env, req, serverError("Failed to create client hospital"));
  }
}

async function handleGetHospital(env, req, clientId, hospitalId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/client_hospitals?id=eq.${encodeURIComponent(hospitalId)}&client_id=eq.${encodeURIComponent(clientId)}&select=*`
    );
    if (!rows.length) return withCORS(env, req, notFound("Hospital not found"));
    return withCORS(env, req, ok({ hospital: rows[0] }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch client hospital"));
  }
}

async function handleUpdateHospital(env, req, clientId, hospitalId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  try {
    const url = `${env.SUPABASE_URL}/rest/v1/client_hospitals?id=eq.${encodeURIComponent(hospitalId)}&client_id=eq.${encodeURIComponent(clientId)}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data, updated_at: new Date().toISOString() })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Hospital update failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const hospital = Array.isArray(json) ? json[0] : json;
    return withCORS(env, req, ok({ hospital }));
  } catch {
    return withCORS(env, req, serverError("Failed to update client hospital"));
  }
}

// ====================== UMBRELLAS ======================
/**
 * @openapi
 * /api/umbrellas:
 *   get:
 *     summary: List umbrella companies
 *     tags: [Umbrellas]
 *     security:
 *       - bearerAuth: []
 *   post:
 *     summary: Create umbrella
 *     tags: [Umbrellas]
 *     security:
 *       - bearerAuth: []
 * /api/umbrellas/{umbrella_id}:
 *   get:
 *     summary: Get umbrella
 *     tags: [Umbrellas]
 *     security:
 *       - bearerAuth: []
 *   put:
 *     summary: Update umbrella
 *     tags: [Umbrellas]
 *     security:
 *       - bearerAuth: []
 */
async function handleListUmbrellas(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/umbrellas?select=*`);
    return withCORS(env, req, ok({ items: rows }));
  } catch {
    return withCORS(env, req, serverError("Failed to list umbrellas"));
  }
}

async function handleCreateUmbrella(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  try {
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/umbrellas`, {
      method: "POST",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data, created_at: new Date().toISOString() })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Umbrella creation failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const umbrella = Array.isArray(json) ? json[0] : json;
    return withCORS(env, req, ok({ umbrella }));
  } catch {
    return withCORS(env, req, serverError("Failed to create umbrella"));
  }
}

async function handleGetUmbrella(env: any, req: Request, umbrellaId: string) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/umbrellas?id=eq.${encodeURIComponent(umbrellaId)}&select=*`
    );
    if (!rows.length) return withCORS(env, req, notFound("Umbrella not found"));
    return withCORS(env, req, ok({ umbrella: rows[0] }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch umbrella"));
  }
}

async function handleUpdateUmbrella(env: any, req: Request, umbrellaId: string) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  try {
    // 1) Load current umbrella (for change detection)
    const { rows: beforeRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/umbrellas?id=eq.${encodeURIComponent(umbrellaId)}&select=name,bank_name,sort_code,account_number`
    );
    const before = beforeRows?.[0] || {};

    // 2) Update
    const url = `${env.SUPABASE_URL}/rest/v1/umbrellas?id=eq.${encodeURIComponent(umbrellaId)}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data, updated_at: new Date().toISOString() })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Umbrella update failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const umbrella = Array.isArray(json) ? json[0] : json;

    // 3) Detect pay-channel impacting changes
    const watched = ['name','bank_name','sort_code','account_number'] as const;
    const changed = watched.some(k => umbrella?.[k] !== before?.[k]);

    if (changed) {
      // Enqueue recompute for all candidates on this umbrella (current & unlocked TSFIN only)
      const { rows: candidateRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/candidates` +
        `?select=id` +
        `&umbrella_id=eq.${encodeURIComponent(umbrellaId)}` +
        `&pay_method=eq.UMBRELLA`
      );
      const candIds = (candidateRows || []).map((r: any) => r.id);
      if (candIds.length) {
        const idsParam = candIds.map(encodeURIComponent).join(',');
        const { rows: tsfins } = await sbFetch(
          env,
          `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
          `?select=timesheet_id` +
          `&candidate_id=in.(${idsParam})` +
          `&is_current=eq.true` +
          `&locked_by_invoice_id=is.null`
        );
        const toEnqueue = (tsfins || []).map((r: any) => ({ timesheet_id: r.timesheet_id, reason: 'CONTEXT_CHANGED' }));
        if (toEnqueue.length) {
          await fetch(
            `${env.SUPABASE_URL}/rest/v1/ts_financials_outbox?on_conflict=timesheet_id,reason`,
            {
              method: "POST",
              headers: { ...sbHeaders(env), "Prefer": "resolution=ignore-duplicates" },
              body: JSON.stringify(toEnqueue)
            }
          );
        }
      }
    }

    return withCORS(env, req, ok({ umbrella }));
  } catch {
    return withCORS(env, req, serverError("Failed to update umbrella"));
  }
}

// ====================== CANDIDATES ======================
/**
 * @openapi
 * /api/candidates:
 *   get:
 *     summary: List candidates
 *     tags: [Candidates]
 *     security:
 *       - bearerAuth: []
 *   post:
 *     summary: Create candidate
 *     tags: [Candidates]
 *     security:
 *       - bearerAuth: []
 * /api/candidates/{candidate_id}:
 *   get:
 *     summary: Get candidate
 *     tags: [Candidates]
 *     security:
 *       - bearerAuth: []
 *   put:
 *     summary: Update candidate
 *     tags: [Candidates]
 *     security:
 *       - bearerAuth: []
 */
async function handleListCandidates(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/candidates?select=*`);
    return withCORS(env, req, ok({ items: rows }));
  } catch {
    return withCORS(env, req, serverError("Failed to list candidates"));
  }
}

// ------------------------------------------------------
// CREATE CANDIDATE / CLIENT (accept mileage fields too)
// ------------------------------------------------------
async function handleCreateCandidate(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  // mileage_pay_rate is accepted by DB; other fields are passed through
  try {
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/candidates`, {
      method: "POST",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data, created_at: new Date().toISOString() })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Candidate creation failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const candidate = Array.isArray(json) ? json[0] : json;
    return withCORS(env, req, ok({ candidate }));
  } catch {
    return withCORS(env, req, serverError("Failed to create candidate"));
  }
}

async function handleGetCandidate(env: any, req: Request, candidateId: string) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    // Fetch candidate
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(candidateId)}&select=*`
    );
    if (!rows.length) return withCORS(env, req, notFound("Candidate not found"));
    const candidate = rows[0];

    // If umbrella, fetch umbrella minimal fields
    let umbrella: any = undefined;
    if (candidate.pay_method === 'UMBRELLA' && candidate.umbrella_id) {
      const { rows: umbRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/umbrellas?id=eq.${encodeURIComponent(candidate.umbrella_id)}&select=id,name,bank_name,sort_code,account_number`
      );
      umbrella = umbRows?.[0];
    }

    const effective_pay_channel = resolveEffectivePayChannel({
      pay_method: candidate.pay_method,
      candidate,
      umbrella
    });

    return withCORS(env, req, ok({ candidate, effective_pay_channel }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch candidate"));
  }
}

// -------------------------------------------------------
// UPDATE CANDIDATE (enqueue non-invoiced TSFIN on change)
// -------------------------------------------------------
async function handleUpdateCandidate(env: any, req: Request, candidateId: string) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  try {
    // 1) Load current candidate to detect changes
    const sel = [
      'pay_method',
      'mileage_pay_rate',
      'umbrella_id',
      'account_holder',
      'bank_name',
      'sort_code',
      'account_number'
    ].join(',');
    const { rows: beforeRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(candidateId)}&select=${sel}`
    );
    const before = beforeRows?.[0] || {};

    // 2) Update
    const url = `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(candidateId)}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data, updated_at: new Date().toISOString() })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Candidate update failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const candidate = Array.isArray(json) ? json[0] : json;

    // 3) Change detection
    const payMethodChanged   = (data.pay_method != null)   && data.pay_method !== before.pay_method;
    const umbrellaChanged    = (data.umbrella_id !== undefined) && data.umbrella_id !== before.umbrella_id;
    const mileagePayChanged  = (data.mileage_pay_rate != null) && Number(data.mileage_pay_rate) !== Number(before.mileage_pay_rate);

    const bankKeys = ['account_holder','bank_name','sort_code','account_number'] as const;
    const bankChanged = bankKeys.some(k => Object.prototype.hasOwnProperty.call(data, k) && data[k] !== before[k]);

    // 4) Enqueue recompute for non-invoiced, current TSFIN for this candidate
    if (payMethodChanged || umbrellaChanged || bankChanged || mileagePayChanged) {
      const { rows: tsfins } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
        `?select=timesheet_id` +
        `&candidate_id=eq.${encodeURIComponent(candidateId)}` +
        `&is_current=eq.true` +
        `&locked_by_invoice_id=is.null`
      );
      const items: Array<{ timesheet_id: string, reason: string }> = [];
      for (const r of (tsfins || [])) {
        if (payMethodChanged || umbrellaChanged || bankChanged) {
          items.push({ timesheet_id: r.timesheet_id, reason: 'CONTEXT_CHANGED' });
        }
        if (mileagePayChanged) {
          items.push({ timesheet_id: r.timesheet_id, reason: 'RATE_CHANGED' });
        }
      }
      if (items.length) {
        await fetch(
          `${env.SUPABASE_URL}/rest/v1/ts_financials_outbox?on_conflict=timesheet_id,reason`,
          {
            method: "POST",
            headers: { ...sbHeaders(env), "Prefer": "resolution=ignore-duplicates" },
            body: JSON.stringify(items)
          }
        );
      }
    }

    return withCORS(env, req, ok({ candidate }));
  } catch {
    return withCORS(env, req, serverError("Failed to update candidate"));
  }
}

// ====================== RATES (FIVE-WAY) ======================
/**
 * @openapi
 * /api/rates/client-defaults:
 *   get:
 *     summary: List client default rates
 *     tags: [Rates]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: query
 *         name: client_id
 *         required: true
 *         schema: { type: string }
 *   post:
 *     summary: Upsert client default rates (five-way)
 *     tags: [Rates]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               client_id: { type: string }
 *               role: { type: string }
 *               band: { type: string }
 *               date_from: { type: string, format: date }
 *               date_to: { type: string, format: date, nullable: true }
 *               charge_day: { type: number }
 *               charge_night: { type: number }
 *               charge_sat: { type: number }
 *               charge_sun: { type: number }
 *               charge_bh: { type: number }
 *               pay_day: { type: number, nullable: true }
 *               pay_night: { type: number, nullable: true }
 *               pay_sat: { type: number, nullable: true }
 *               pay_sun: { type: number, nullable: true }
 *               pay_bh: { type: number, nullable: true }
 * /api/rates/candidate-overrides:
 *   get:
 *     summary: List candidate override rates
 *     tags: [Rates]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: query
 *         name: candidate_id
 *         required: true
 *         schema: { type: string }
 *   post:
 *     summary: Create candidate override (five-way)
 *     tags: [Rates]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               candidate_id: { type: string }
 *               client_id: { type: string, nullable: true }
 *               role: { type: string }
 *               band: { type: string }
 *               date_from: { type: string, format: date }
 *               date_to: { type: string, format: date, nullable: true }
 *               pay_day: { type: number }
 *               pay_night: { type: number }
 *               pay_sat: { type: number }
 *               pay_sun: { type: number }
 *               pay_bh: { type: number }
 * /api/rates/resolve-preview:
 *   post:
 *     summary: Resolve five-way pay/charge for a candidate/client/date/role/band
 *     tags: [Rates]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [candidate_id, client_id, date]
 *             properties:
 *               candidate_id: { type: string }
 *               client_id: { type: string }
 *               role: { type: string }
 *               band: { type: string }
 *               date: { type: string, format: date }
 *     responses:
 *       200:
 *         description: Resolved five-way snapshot
 */

// ─────────────────────────────────────────────────────────────────────────────
// Client defaults: list with optional role/band + active_on filters,
// and correct NULL/default semantics. Supports pagination.
// ─────────────────────────────────────────────────────────────────────────────
async function handleListClientRates(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const sp   = new URL(req.url).searchParams;
  const cid  = clientId || sp.get("client_id");
  const role = sp.get("role");     // single value; if UI needs multi, extend to CSV/array handling
  const band = sp.get("band");
  const on   = sp.get("active_on"); // YYYY-MM-DD (or ISO)
  const limit  = Math.min(Math.max(parseInt(sp.get('limit')  || '100', 10) || 100, 1), 500);
  const offset = Math.max(parseInt(sp.get('offset') || '0',   10) || 0, 0);

  if (!cid) return withCORS(env, req, badRequest("client_id required"));

  try {
    let q = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?select=*&client_id=eq.${encodeURIComponent(cid)}`;

    const andParts = [];
    // Treat provided role/band as "specific or default" (include NULL)
    if (role) andParts.push(`or(role.eq.${encodeURIComponent(role)},role.is.null)`);
    if (band) andParts.push(`or(band.eq.${encodeURIComponent(band)},band.is.null)`);
    if (on) {
      andParts.push(`date_from=lte.${encodeURIComponent(on)}`);
      andParts.push(`or(date_to.gte.${encodeURIComponent(on)},date_to.is.null)`);
    }
    if (andParts.length) q += `&and=(${andParts.join(',')})`;

    q += `&order=date_from.desc,role.nullslast,band.nullslast&limit=${limit}&offset=${offset}`;

    const { rows } = await sbFetch(env, q);
    return withCORS(env, req, ok({ items: rows }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch client default rates"));
  }
}

async function handleUpsertClientRate(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const body = await parseJSONBody(req);
  if (!body) return withCORS(env, req, badRequest("Invalid JSON"));

  const record = {
    ...body,
    client_id: body.client_id || clientId
  };
  if (!record.client_id) return withCORS(env, req, badRequest("client_id required"));

  // Uniqueness upsert basis: client_id + role + band + date_from
  const role = record.role || null;
  const band = record.band || null;
  const dateFrom = record.date_from || null;

  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?client_id=eq.${encodeURIComponent(record.client_id)}${role ? `&role=eq.${encodeURIComponent(role)}` : ""}${band ? `&band=eq.${encodeURIComponent(band)}` : ""}${dateFrom ? `&date_from=eq.${encodeURIComponent(dateFrom)}` : ""}&select=id`
    );

    let result;
    if (rows.length) {
      const url = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?id=eq.${encodeURIComponent(rows[0].id)}`;
      const res = await fetch(url, {
        method: "PATCH",
        headers: { ...sbHeaders(env), "Prefer": "return=representation" },
        body: JSON.stringify({ ...record, updated_at: new Date().toISOString() })
      });
      if (!res.ok) {
        const err = await res.text();
        return withCORS(env, req, badRequest(`Rate update failed: ${err}`));
      }
      const json = await res.json().catch(() => ({}));
      result = Array.isArray(json) ? json[0] : json;
    } else {
      const res = await fetch(`${env.SUPABASE_URL}/rest/v1/rates_client_defaults`, {
        method: "POST",
        headers: { ...sbHeaders(env), "Prefer": "return=representation" },
        body: JSON.stringify({ ...record, created_at: new Date().toISOString() })
      });
      if (!res.ok) {
        const err = await res.text();
        return withCORS(env, req, badRequest(`Rate insert failed: ${err}`));
      }
      const json = await res.json().catch(() => ({}));
      result = Array.isArray(json) ? json[0] : json;
    }
    return withCORS(env, req, ok({ rate: result }));
  } catch {
    return withCORS(env, req, serverError("Failed to upsert client rate"));
  }
}
// ─────────────────────────────────────────────────────────────────────────────
// Rates: list candidate overrides by client
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// Overrides: list by CLIENT with optional role/band + active_on filters,
// correct NULL/default semantics, pagination, and candidate join.
// ─────────────────────────────────────────────────────────────────────────────
async function handleListOverridesByClient(env, req, clientId) {
  const user = await requireUser(env, req, ["admin"]);
  if (!user) return withCORS(env, req, unauthorized());

  const sp   = new URL(req.url).searchParams;
  const cid  = clientId || sp.get("client_id");
  const role = sp.get("role");
  const band = sp.get("band");
  const on   = sp.get("active_on");
  const limit  = Math.min(Math.max(parseInt(sp.get('limit')  || '100', 10) || 100, 1), 500);
  const offset = Math.max(parseInt(sp.get('offset') || '0',   10) || 0, 0);

  if (!cid) return withCORS(env, req, badRequest("client_id required"));

  try {
    let q = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides` +
            `?select=*,candidate:candidates(id,display_name)`; // display_name exists on candidates

    // Base constraint: overrides tied to this client OR default (NULL) only?
    // For listing "by client", we typically want rows where client_id equals this client.
    // If you also want defaults that might apply to all clients, extend with an OR group.
    q += `&client_id=eq.${encodeURIComponent(cid)}`;

    const andParts = [];
    if (role) andParts.push(`or(role.eq.${encodeURIComponent(role)},role.is.null)`);
    if (band) andParts.push(`or(band.eq.${encodeURIComponent(band)},band.is.null)`);
    if (on) {
      andParts.push(`date_from=lte.${encodeURIComponent(on)}`);
      andParts.push(`or(date_to.gte.${encodeURIComponent(on)},date_to.is.null)`);
    }
    if (andParts.length) q += `&and=(${andParts.join(',')})`;

    q += `&order=date_from.desc,client_id.nullslast,role.nullslast,band.nullslast&limit=${limit}&offset=${offset}`;

    const { rows } = await sbFetch(env, q);

    const items = rows.map(r => ({
      ...r,
      candidate_name: r.candidate ? r.candidate.display_name : null
    }));

    return withCORS(env, req, ok({ items }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch candidate overrides for client"));
  }
}
// ─────────────────────────────────────────────────────────────────────────────
// Overrides: list by CANDIDATE with optional role/band + active_on filters,
// correct NULL/default semantics, pagination, and client join.
// ─────────────────────────────────────────────────────────────────────────────
async function handleListOverridesByCandidate(env, req, candidateId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const sp    = new URL(req.url).searchParams;
  const cand  = candidateId || sp.get("candidate_id");
  const role  = sp.get("role");
  const band  = sp.get("band");
  const on    = sp.get("active_on");
  const limit  = Math.min(Math.max(parseInt(sp.get('limit')  || '100', 10) || 100, 1), 500);
  const offset = Math.max(parseInt(sp.get('offset') || '0',   10) || 0, 0);

  if (!cand) return withCORS(env, req, badRequest("candidate_id required"));

  try {
    let q = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?select=*,client:clients(id,name)`;
    q += `&candidate_id=eq.${encodeURIComponent(cand)}`;

    const andParts = [];
    if (role) andParts.push(`or(role.eq.${encodeURIComponent(role)},role.is.null)`);
    if (band) andParts.push(`or(band.eq.${encodeURIComponent(band)},band.is.null)`);
    if (on) {
      andParts.push(`date_from=lte.${encodeURIComponent(on)}`);
      andParts.push(`or(date_to.gte.${encodeURIComponent(on)},date_to.is.null)`);
    }
    if (andParts.length) q += `&and=(${andParts.join(',')})`;

    q += `&order=date_from.desc,client_id.nullslast,role.nullslast,band.nullslast&limit=${limit}&offset=${offset}`;

    const { rows } = await sbFetch(env, q);

    const items = rows.map(r => ({
      ...r,
      client_name: r.client ? r.client.name : null
    }));

    return withCORS(env, req, ok({ items }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch overrides for candidate"));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Rates: create a candidate override
// ─────────────────────────────────────────────────────────────────────────────
async function handleCreateOverride(env, req, candidateId, clientIdParam = null) {
  const user = await requireUser(env, req, ["admin"]);
  if (!user) return withCORS(env, req, unauthorized());

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  const candidate_id = candidateId || data.candidate_id;
  if (!candidate_id) return withCORS(env, req, badRequest("candidate_id required"));

  // (Light validation; keep strict rules in DB where possible)
  const record = {
    candidate_id,
    client_id: clientIdParam || data.client_id || null,
    role: data.role || null,
    band: data.band || null,
    date_from: data.date_from || null,
    date_to: data.date_to || null,
    pay_day: data.pay_day,
    pay_night: data.pay_night,
    pay_sat: data.pay_sat,
    pay_sun: data.pay_sun,
    pay_bh: data.pay_bh,
    created_at: new Date().toISOString(),
  };

  try {
    const res = await fetch(
      `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides`,
      {
        method: "POST",
        headers: { ...sbHeaders(env), Prefer: "return=representation" },
        body: JSON.stringify(record),
      }
    );

    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Override creation failed: ${err}`));
    }

    const json = await res.json().catch(() => ({}));
    const override = Array.isArray(json) ? json[0] : json;
    return withCORS(env, req, ok({ override }));
  } catch {
    return withCORS(env, req, serverError("Failed to create override"));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Overrides: UPDATE with safe targeting. Supports targeting NULL/default for
// client/role/band by passing the literal "null" in those parameters.
// Requires at least one discriminator beyond candidate_id to avoid broad patch.
// ─────────────────────────────────────────────────────────────────────────────
async function handleUpdateOverride(env, req, candidateId, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  const sp = new URL(req.url).searchParams;
  // Allow route params to be complemented/overridden by query params
  const cand = candidateId || sp.get("candidate_id");
  const cid  = (clientId !== undefined && clientId !== null) ? clientId : sp.get("client_id");
  const role = sp.get("role");
  const band = sp.get("band");

  if (!cand) return withCORS(env, req, badRequest("candidate_id required"));

  // Guard: require at least one discriminator in addition to candidate_id
  if (!cid && !role && !band) {
    return withCORS(env, req, badRequest("Provide at least one of client_id, role, or band to target an override"));
  }

  try {
    let url = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?candidate_id=eq.${encodeURIComponent(cand)}`;

    // Target specific or NULL/default rows explicitly
    if (cid !== undefined && cid !== null) {
      if (String(cid).toLowerCase() === 'null') url += `&client_id=is.null`;
      else url += `&client_id=eq.${encodeURIComponent(cid)}`;
    }
    if (role) {
      if (String(role).toLowerCase() === 'null') url += `&role=is.null`;
      else url += `&role=eq.${encodeURIComponent(role)}`;
    }
    if (band) {
      if (String(band).toLowerCase() === 'null') url += `&band=is.null`;
      else url += `&band=eq.${encodeURIComponent(band)}`;
    }

    const res = await fetch(url, {
      method: "PATCH",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data })
    });

    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Override update failed: ${err}`));
    }

    const json = await res.json().catch(() => []);
    if (Array.isArray(json) && !json.length) {
      return withCORS(env, req, notFound("Override not found"));
    }
    const override = Array.isArray(json) ? json[0] : json;
    return withCORS(env, req, ok({ override }));
  } catch {
    return withCORS(env, req, serverError("Failed to update override"));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Overrides: DELETE with safe targeting. Supports targeting NULL/default for
// client/role/band via literal "null". Requires at least one discriminator
// beyond candidate_id to avoid broad deletes.
// ─────────────────────────────────────────────────────────────────────────────
async function handleDeleteOverride(env, req, candidateId, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const sp = new URL(req.url).searchParams;
  const cand = candidateId || sp.get("candidate_id");
  const cid  = (clientId !== undefined && clientId !== null) ? clientId : sp.get("client_id");
  const role = sp.get("role");
  const band = sp.get("band");

  if (!cand) return withCORS(env, req, badRequest("candidate_id required"));

  // Guard to prevent mass-delete
  if (!cid && !role && !band) {
    return withCORS(env, req, badRequest("Provide at least one of client_id, role, or band to target an override for delete"));
  }

  try {
    let url = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?candidate_id=eq.${encodeURIComponent(cand)}`;

    if (cid !== undefined && cid !== null) {
      if (String(cid).toLowerCase() === 'null') url += `&client_id=is.null`;
      else url += `&client_id=eq.${encodeURIComponent(cid)}`;
    }
    if (role) {
      if (String(role).toLowerCase() === 'null') url += `&role=is.null`;
      else url += `&role=eq.${encodeURIComponent(role)}`;
    }
    if (band) {
      if (String(band).toLowerCase() === 'null') url += `&band=is.null`;
      else url += `&band=eq.${encodeURIComponent(band)}`;
    }

    const res = await fetch(url, { method: "DELETE", headers: sbHeaders(env) });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Override delete failed: ${err}`));
    }
    return withCORS(env, req, ok({ ok: true }));
  } catch {
    return withCORS(env, req, serverError("Failed to delete override"));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Rates: resolve one effective rate with correct NULL/default semantics,
// date-window filtering, and deterministic tie-break ordering
// ─────────────────────────────────────────────────────────────────────────────
async function handleResolveRate(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const payload = req.method === "GET"
    ? Object.fromEntries(new URL(req.url).searchParams)
    : await parseJSONBody(req);

  const client_id    = payload.client_id;
  const candidate_id = payload.candidate_id;
  const role         = payload.role || null;
  const band         = payload.band || null;
  const date         = payload.date || payload.on || null;

  if (!client_id || !candidate_id) {
    return withCORS(env, req, badRequest("client_id and candidate_id are required"));
  }

  try {
    // ── 1) Candidate override (most specific). Allow NULL-or-specific for each dimension,
    //     apply date window if provided, and tie-break by specificity + latest date_from.
    {
      let q = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?select=*&candidate_id=eq.${encodeURIComponent(candidate_id)}`;

      const andParts = [];
      if (client_id) andParts.push(`or(client_id.eq.${encodeURIComponent(client_id)},client_id.is.null)`);
      if (role)      andParts.push(`or(role.eq.${encodeURIComponent(role)},role.is.null)`);
      if (band)      andParts.push(`or(band.eq.${encodeURIComponent(band)},band.is.null)`);
      if (date) {
        andParts.push(`date_from=lte.${encodeURIComponent(date)}`);
        andParts.push(`or(date_to.gte.${encodeURIComponent(date)},date_to.is.null)`);
      }
      if (andParts.length) q += `&and=(${andParts.join(',')})`;

      // Most specific first, then most recent starting window
      q += `&order=client_id.nullslast,role.nullslast,band.nullslast,date_from.desc&limit=1`;

      const { rows: candRows } = await sbFetch(env, q);
      const activeCand = candRows && candRows[0];

      if (activeCand) {
        return withCORS(env, req, ok({
          source: "candidate_override",
          pay: {
            day:   activeCand.pay_day,
            night: activeCand.pay_night,
            sat:   activeCand.pay_sat,
            sun:   activeCand.pay_sun,
            bh:    activeCand.pay_bh
          }
        }));
      }
    }

    // ── 2) Client defaults (charge + optional default pay). Allow NULL-or-specific role/band,
    //       apply date window if provided, and tie-break by specificity + latest date_from.
    {
      let q = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?select=*&client_id=eq.${encodeURIComponent(client_id)}`;

      const andParts = [];
      if (role) andParts.push(`or(role.eq.${encodeURIComponent(role)},role.is.null)`);
      if (band) andParts.push(`or(band.eq.${encodeURIComponent(band)},band.is.null)`);
      if (date) {
        andParts.push(`date_from=lte.${encodeURIComponent(date)}`);
        andParts.push(`or(date_to.gte.${encodeURIComponent(date)},date_to.is.null)`);
      }
      if (andParts.length) q += `&and=(${andParts.join(',')})`;

      q += `&order=role.nullslast,band.nullslast,date_from.desc&limit=1`;

      const { rows: defRows } = await sbFetch(env, q);
      const activeDef = defRows && defRows[0];

      if (activeDef) {
        return withCORS(env, req, ok({
          source: "client_defaults",
          charge: {
            day:   activeDef.charge_day,
            night: activeDef.charge_night,
            sat:   activeDef.charge_sat,
            sun:   activeDef.charge_sun,
            bh:    activeDef.charge_bh
          },
          pay: (activeDef.pay_day != null) ? {
            day:   activeDef.pay_day,
            night: activeDef.pay_night,
            sat:   activeDef.pay_sat,
            sun:   activeDef.pay_sun,
            bh:    activeDef.pay_bh
          } : null
        }));
      }
    }

    return withCORS(env, req, notFound("No applicable rate found"));
  } catch {
    return withCORS(env, req, serverError("Failed to resolve rates"));
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// Files: secure download via short-lived token
// GET /api/files/download?key=...&token=...[&filename=...]
// ─────────────────────────────────────────────────────────────────────────────
async function handleFilesDownload(env, req) {
  try {
    const url = new URL(req.url);
    const key = url.searchParams.get('key');
    const token = url.searchParams.get('token');
    const overrideName = url.searchParams.get('filename') || null;

    if (!key || !token) {
      return withCORS(env, req, badRequest("key and token are required"));
    }

    // Basic key sanitisation + prefix allow-list
    if (key.includes('..') || key.startsWith('/')) {
      return withCORS(env, req, unauthorized());
    }
    const ALLOWED_PREFIXES = ['invoices/', 'remittances/', 'paper_ts/', 'signatures/', 'docs/'];
    if (!ALLOWED_PREFIXES.some(p => key.startsWith(p))) {
      return withCORS(env, req, unauthorized());
    }

    // Verify token
    const secret = env.UPLOAD_TOKEN_SECRET;
    if (!secret) return withCORS(env, req, serverError("Server not configured"));

    // verifyToken should return the payload or throw/reject on invalid
    let payload;
    try {
      payload = await verifyToken(secret, token);
    } catch {
      await writeAudit(env, null, 'FILE_DOWNLOAD_DENIED', { key, reason: 'invalid_token' }, { entity: 'r2', subject_id: key, req });
      return withCORS(env, req, unauthorized());
    }

    // Claims checks
    if (!payload || payload.typ !== 'dl' || payload.key !== key) {
      await writeAudit(env, null, 'FILE_DOWNLOAD_DENIED', { key, reason: 'claims_mismatch' }, { entity: 'r2', subject_id: key, req });
      return withCORS(env, req, unauthorized());
    }

    // Expiry check (payload.exp is unix seconds)
    const now = Math.floor(Date.now() / 1000);
    if (typeof payload.exp === 'number' && now >= payload.exp) {
      await writeAudit(env, null, 'FILE_DOWNLOAD_DENIED', { key, reason: 'expired' }, { entity: 'r2', subject_id: key, req });
      return withCORS(env, req, new Response("Link expired", { status: 410 }));
    }

    // Fetch from R2
    const bucket = env.R2_BUCKET || env.R2;
    if (!bucket || !bucket.get) {
      return withCORS(env, req, serverError("Storage not configured"));
    }
    const obj = await bucket.get(key);
    if (!obj) {
      await writeAudit(env, null, 'FILE_DOWNLOAD_NOT_FOUND', { key }, { entity: 'r2', subject_id: key, req });
      return withCORS(env, req, notFound("File not found"));
    }

    // Build headers
    const headers = new Headers();
    const ct = obj.httpMetadata?.contentType || 'application/octet-stream';
    headers.set('Content-Type', ct);
    if (typeof obj.size === 'number') headers.set('Content-Length', String(obj.size));
    headers.set('X-Content-Type-Options', 'nosniff');
    headers.set('Cache-Control', 'no-store');

    // Choose safe filename
    const metaName = obj.customMetadata?.originalName || null;
    const baseName = key.split('/').pop() || 'download.bin';
    const chosen = (overrideName || metaName || baseName)
      .replace(/[/\\]/g, '_')
      .replace(/[\r\n"]/g, ''); // basic header injection hardening
    headers.set('Content-Disposition', `attachment; filename="${chosen}"`);

    await writeAudit(env, null, 'FILE_DOWNLOAD_OK', { key, size: obj.size || null }, { entity: 'r2', subject_id: key, req });
    return withCORS(env, req, new Response(obj.body, { status: 200, headers }));
  } catch {
    return withCORS(env, req, serverError("Failed to download file"));
  }
}

// ====================== HEALTHROSTER ======================
/**
 * @openapi
 * /api/healthroster/import:
 *   post:
 *     summary: Register an HR import (file already in R2)
 *     tags: [HealthRoster]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [file_key]
 *             properties:
 *               file_key: { type: string, description: "R2 key of uploaded file" }
 *     responses:
 *       200:
 *         description: import_id returned
 * /api/healthroster/{import_id}/rows:
 *   get:
 *     summary: List parsed rows for an import
 *     tags: [HealthRoster]
 *     security:
 *       - bearerAuth: []
 * /api/healthroster/{import_id}/mapping:
 *   get:
 *     summary: List active HR name mappings (global)
 *     tags: [HealthRoster]
 *     security:
 *       - bearerAuth: []
 *   post:
 *     summary: Upsert HR name mappings (global)
 *     tags: [HealthRoster]
 *     security:
 *       - bearerAuth: []
 * /api/healthroster/{import_id}/validate:
 *   post:
 *     summary: Trigger validation run (delegates to Flow if configured)
 *     tags: [HealthRoster]
 *     security:
 *       - bearerAuth: []
 */
async function handleHRRows(env, req, importId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  if (!importId) {
    return withCORS(env, req, badRequest("import_id is required"));
  }

  try {
    const { searchParams } = new URL(req.url);
    const limit  = Math.min(Math.max(parseInt(searchParams.get('limit')  || '500', 10) || 500, 1), 5000);
    const offset = Math.max(parseInt(searchParams.get('offset') || '0',   10) || 0, 0);
    const order  = searchParams.get('order') || 'id.asc';

    const url =
      `${env.SUPABASE_URL}/rest/v1/hr_rows` +
      `?import_id=eq.${encodeURIComponent(importId)}` +
      `&select=*` +
      `&order=${encodeURIComponent(order)}` +
      `&limit=${limit}` +
      `&offset=${offset}`;

    const { rows } = await sbFetch(env, url, false);
    return withCORS(env, req, ok({ rows }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch HR rows"));
  }
}




// ====================== TIMESHEETS FINANCE PREVIEW ======================
/**
 * @openapi
 * /api/timesheets/finance-preview:
 *   post:
 *     summary: Finance preview for selected authorised timesheets (read-only)
 *     tags: [Timesheets]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [timesheet_ids]
 *             properties:
 *               timesheet_ids:
 *                 type: array
 *                 items: { type: string }
 *     responses:
 *       200:
 *         description: Aggregate totals & margin (ex VAT)
 */
// ─────────────────────────────────────────────────────────────────────────────
// Timesheets: finance preview (aggregate five-way totals)
// ─────────────────────────────────────────────────────────────────────────────

// ---------------------- Email Outbox, Broadcast, and Files (unified /api/*) ----------------------
/**
 * @openapi
 * /api/email/outbox:
 *   get:
 *     summary: List email outbox items (most recent first)
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: query
 *         name: status
 *         schema:
 *           type: string
 *           enum: [QUEUED, SENT, FAILED]
 *       - in: query
 *         name: limit
 *         schema:
 *           type: integer
 *           default: 100
 *           minimum: 1
 *           maximum: 500
 *
 * /api/email/outbox/mark-sent:
*   post:
*     summary: Mark a queued outbox item as sent (callback from mail provider/Flow)
*     security:
*       - bearerAuth: []
*     requestBody:
*       required: true
*       content:
*         application/json:
*           schema:
*             type: object
*             required: [id]
*             properties:
*               id:
*                 type: string
*               sent_at:
*                 type: string
*                 format: date-time
*               provider_message_id:
*                 type: string
 *
 * /api/email/outbox/mark-failed:
*   post:
*     summary: Mark a queued outbox item as failed (callback from mail provider/Flow)
*     security:
*       - bearerAuth: []
*     requestBody:
*       required: true
*       content:
*         application/json:
*           schema:
*             type: object
*             required: [id, error]
*             properties:
*               id:
*                 type: string
*               error:
*                 type: string
*               failed_at:
*                 type: string
*                 format: date-time
 *
 * /api/email/broadcast:
 *   post:
 *     summary: Send broadcast email (enqueues via webhook and records a summary outbox row)
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               group:
 *                 type: string
 *                 description: One of "candidates" or "clients". Ignored if "emails" is provided.
 *                 enum: [candidates, clients]
 *               emails:
 *                 type: array
 *                 items:
 *                   type: string
 *                 description: Explicit recipient list. If present, overrides "group".
 *               subject:
 *                 type: string
 *               body_text:
 *                 type: string
 *
 * /api/files/presign-download:
 *   post:
 *     summary: Create a short-lived, tokenized download URL for a stored file
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [key]
 *             properties:
 *               key:
 *                 type: string
 *                 description: Object key/path in storage (e.g. "invoices/abc.pdf")
 *               expires_seconds:
 *                 type: integer
 *                 default: 180
 *                 minimum: 60
 *                 maximum: 900
 *     responses:
 *       200:
 *         description: URL created
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 url:
 *                   type: string
 *
 * /api/files/download:
 *   get:
 *     summary: Token-verified file download (serves with correct Content-Type and Content-Disposition)
 *     parameters:
 *       - in: query
 *         name: key
 *         required: true
 *         schema:
 *           type: string
 *       - in: query
 *         name: token
 *         required: true
 *         schema:
 *           type: string
 *       - in: query
 *         name: inline
 *         required: false
 *         schema:
 *           type: boolean
 *           default: false
 *         description: If true, attempt inline display (e.g. PDFs in browser). Otherwise download attachment.
 */

async function handleOutboxList(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  try {
    const { searchParams } = new URL(req.url);
    const status = searchParams.get('status');
    const limit = Math.min(Math.max(parseInt(searchParams.get('limit') || '100', 10) || 100, 1), 500);
    const where = status ? `&status=eq.${encodeURIComponent(status)}` : '';

    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/mail_outbox?select=*&order=created_at_utc.desc&limit=${limit}${where}`
    );

    return withCORS(env, req, ok({ items: rows }));
  } catch (e) {
    return withCORS(env, req, serverError("Failed to fetch mail_outbox"));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Broadcast email: send via webhook, write summary row to mail_outbox
// ─────────────────────────────────────────────────────────────────────────────
async function handleBroadcastEmail(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  let emails = [];
  if (Array.isArray(data.emails) && data.emails.length) {
    emails = data.emails;
  } else if (data.group === "candidates") {
    const { rows: candRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/candidates?select=email`);
    emails = candRows.map(c => c.email).filter(Boolean);
  } else if (data.group === "clients") {
    // Use primary_invoice_email only (clients table has no generic 'email' column)
    const { rows: clientRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/clients?select=primary_invoice_email`);
    emails = clientRows.map(c => c.primary_invoice_email).filter(Boolean);
  }

  // Normalise + de-duplicate
  emails = [...new Set(emails.map(e => String(e || '').trim()).filter(Boolean))];
  if (!emails.length) return withCORS(env, req, badRequest("No recipients found"));

  const subject = data.subject || "(No Subject)";
  const bodyText = data.body_text || "";

  try {
    const map = JSON.parse(env.TSO_WEBHOOK_MAP || "{}");
    const url = map["broadcast_email"];
    if (!url) return withCORS(env, req, serverError("Broadcast email webhook not configured"));

    const payload = { to_emails: emails, subject, body_text: bodyText };

    const flowRes = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    if (!flowRes.ok) {
      const err = await flowRes.text();
      return withCORS(env, req, serverError(`Broadcast failed: ${err}`));
    }

    // Record a single summary row in mail_outbox
    const nowIso = new Date().toISOString();
    const insRes = await fetch(`${env.SUPABASE_URL}/rest/v1/mail_outbox`, {
      method: "POST",
      headers: sbHeaders(env),
      body: JSON.stringify({
        type: "BROADCAST",
        reference: data.group ? String(data.group).toUpperCase() : "CUSTOM",
        to: `${emails.length} recipients`,
        subject,
        body_text: bodyText.slice(0, 2000),
        status: "SENT",
        created_at_utc: nowIso,
        sent_at: nowIso
      })
    });

    if (!insRes.ok) {
      const err = await insRes.text();
      return withCORS(env, req, serverError(`Outbox insert failed: ${err}`));
    }

    await writeAudit(env, user, 'EMAIL_BROADCAST', { subject, count: emails.length });

    return withCORS(env, req, ok({ ok: true, recipients: emails.length }));
  } catch (e) {
    return withCORS(env, req, serverError("Failed to send broadcast email"));
  }
}

/**
 * Persist a single audit event. Never throws; logs a warning on failure.
 *
 * @param {Object} env                    - Worker env (must include SUPABASE_URL + sbHeaders())
 * @param {Object|null} user              - From requireUser(); may be null
 * @param {string} action                 - e.g. 'EMAIL_BROADCAST', 'OUTBOX_MARK_SENT'
 * @param {Object|null} details           - JSON-serialisable details (kept small)
 * @param {Object} [opts]
 * @param {string|null} [opts.entity]     - Domain entity (e.g. 'mail_outbox','invoice','timesheet')
 * @param {string|number|null} [opts.subject_id] - Primary key of affected record (if any)
 * @param {Request|null} [opts.req]       - Incoming Request to capture headers (optional)
 */
async function writeAudit(env, user, action, details = null, opts = {}) {
  try {
    const req = opts.req || null;

    // Request meta (all optional)
    const ip  = req?.headers?.get('cf-connecting-ip')
             || req?.headers?.get('x-forwarded-for')
             || null;
    const ua  = req?.headers?.get('user-agent') || null;

    // Prefer an explicit correlation_id (e.g., mail_outbox.id), then fallbacks
    const correlationId =
      opts.correlation_id ??
      (details && details.mail_id) ??
      req?.headers?.get('x-correlation-id') ??
      req?.headers?.get('x-request-id') ??
      req?.headers?.get('idempotency-key') ??
      req?.headers?.get('x-idempotency-key') ??
      null;

    // Actor normalization (support id or sub; include role/email if present)
    const actor_user_id       = user?.id ?? user?.sub ?? null;
    const actor_display       = user?.email ?? null;
    const actor_role_at_time  = user?.role ?? null;

    // Object targeting
    const object_type     = opts.entity || opts.object_type || 'generic';
    const object_id_text  =
      opts.subject_id != null
        ? String(opts.subject_id)
        : (opts.object_id_text != null ? String(opts.object_id_text) : null);

    // Build payload aligned to audit_events schema
    const payload = {
      object_type,                 // e.g. 'invoice' | 'timesheet' | 'candidate'
      object_id_text,              // target id as text
      action: String(action),      // e.g. 'EMAIL_QUEUED' | 'EMAIL_SENT'
      before_json: opts.before ?? null,
      after_json: details ?? null,  // include extra context (e.g., {to, subject, invoice_pdf_r2_key, mail_id})
      reason: opts.reason ?? null,
      actor_user_id,
      actor_display,
      actor_role_at_time,
      ip,
      user_agent: ua,
      correlation_id: correlationId
      // ts_utc is implicit (DEFAULT now()) – no need to send
    };

    // Best-effort POST; do not throw on failure
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/audit_events`, {
      method: 'POST',
      headers: { ...sbHeaders(env), Prefer: 'return=minimal' },
      body: JSON.stringify(payload)
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      console.warn('writeAudit failed', res.status, txt);
    }
  } catch (err) {
    console.warn('writeAudit error', err);
  }
}


// ---------------------- HealthRoster Endpoints (expanded) ----------------------
/**
 * Canonical HR pipeline:
 * 1) /healthroster/import      -> create import record (file key + metadata)
 * 2) /healthroster/{id}/rows   -> list parsed rows (assumes a separate parser populated hr_rows)
 * 3) /healthroster/{id}/mapping (GET/POST) -> view/save sticky name mappings (candidate resolution)
 * 4) /healthroster/{id}/validate -> run validation, write hr_results + timesheet_validations
 *
 * Validation rules (per brief):
 * - Staff name -> candidate via sticky mappings; else fuzzy; allow surname<->firstname inversion
 * - Request Grade must map to RMN or HCA (ignore band)
 * - Treat Date/Start/End as UK local; convert to UTC and compare to authorised timesheets
 * - Overnights if End < Start => add 1 day
 * - Ignore breaks (not present in HR)
 * - PASS: write VALIDATION_OK with hr_reference; never mutate timesheets/signatures
 * - FAIL: write reason codes (STAFF_NOT_FOUND, TIME_MISMATCH, ROLE_MISMATCH, TIMESHEET_NOT_FOUND, MULTIPLE_MATCHES, REQUEST_GRADE_UNKNOWN)
 */

/** Utility: basic normalisation */
function normName(s){
  return String(s||'')
    .toUpperCase()
    .replace(/\([^)]*\)/g,'') // remove parentheticals like (RMN)
    .replace(/[^A-Z\s]/g,' ')  // strip punctuation/accents (assumes upstream already de-accented if needed)
    .replace(/\s+/g,' ')
    .trim();
}
function nameVariants(raw){
  const n = normName(raw);
  const parts = n.split(' ');
  if (parts.length < 2) return [n];
  const first = parts[0], last = parts[parts.length-1];
  const full = `${last} ${first}`; // surname first
  return [n, full];
}

/** Utility: derive RMN/HCA from Request Grade */
function roleFromRequestGrade(grade){
  const g = String(grade||'').toUpperCase();
  if (/HCA|HEALTH\s*CARE/i.test(g)) return 'HCA';
  if (/RMN|MENTAL|NURSE/i.test(g)) return 'RMN';
  return null;
}

/** Utility: UK DST (BST) check; last Sunday of Mar to last Sunday of Oct */
function ukLocalToUtcISO(ymd, hhmm = "00:00") {
  // ymd: "YYYY-MM-DD" (UK local date), hhmm: "HH:MM" (UK local clock time)
  const [Y, Mo, D] = String(ymd || "").split("-").map(Number);
  const [H, Mi]    = String(hhmm || "00:00").split(":").map(Number);
  if (!Y || !Mo || !D || Number.isNaN(H) || Number.isNaN(Mi)) return null;

  const offsetHours = isBSTLocalDate(Y, Mo, D) ? 1 : 0; // UK summer = UTC+1
  const dt = new Date(Date.UTC(Y, Mo - 1, D, H - offsetHours, Mi, 0));
  return dt.toISOString();
}

function isBSTLocalDate(Y, Mo, D) {
  // Decide DST for that calendar date using a noon sentinel to avoid boundary weirdness.
  const noonUtcGuess = Date.UTC(Y, Mo - 1, D, 12, 0, 0);
  const start = bstStartUtc(Y); // last Sunday in March, 01:00 UTC
  const end   = bstEndUtc(Y);   // last Sunday in October, 01:00 UTC
  return noonUtcGuess >= start && noonUtcGuess < end;
}

function bstStartUtc(year) {
  // Last Sunday in March at 01:00 UTC
  const day = lastSundayUtc(year, 2); // March = 2 (0-indexed)
  return Date.UTC(year, 2, day, 1, 0, 0);
}

function bstEndUtc(year) {
  // Last Sunday in October at 01:00 UTC
  const day = lastSundayUtc(year, 9); // October = 9 (0-indexed)
  return Date.UTC(year, 9, day, 1, 0, 0);
}

function lastSundayUtc(year, monthIndex /* 0-11 */) {
  // Date-of-month for the last Sunday of the given month (UTC).
  const d = new Date(Date.UTC(year, monthIndex + 1, 0)); // last day of month
  const dow = d.getUTCDay(); // 0 = Sun
  return d.getUTCDate() - dow; // backtrack to Sunday
}

async function getStickyMappings(env, importId) {
  // Global active mappings only; use existing columns
  const { rows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/hr_name_mappings` +
      `?active=eq.true` +
      `&select=hr_name_norm,hospital_or_trust,candidate_id` +
      `&order=last_used_at.desc,created_at.desc`
  );

  // Prefer most recently used; key by hr_name_norm (hospital dimension optional/not supplied here)
  const map = new Map();
  for (const r of rows) {
    if (r.hr_name_norm && r.candidate_id && !map.has(r.hr_name_norm)) {
      map.set(r.hr_name_norm, r.candidate_id);
    }
  }
  return map;
}

async function resolveCandidateId(env, staffRaw, stickyMap) {
  const variants = nameVariants(staffRaw); // includes "FIRST LAST" and "LAST FIRST" normalized
  for (const v of variants) {
    const cid = stickyMap.get(v);
    if (cid) return cid;
  }

  // Fallback fuzzy using available candidate fields
  const { rows: cands } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/candidates?select=id,first_name,last_name,display_name,key_norm`
  );

  const normCandidates = cands.map((c) => {
    const key1 = normName(`${c.last_name || ''} ${c.first_name || ''}`);
    const key2 = normName(`${c.first_name || ''} ${c.last_name || ''}`);
    const key3 = normName(c.display_name || '');
    const key4 = (c.key_norm || '').toUpperCase().trim(); // already-normalized key if present
    return { id: c.id, keys: new Set([key1, key2, key3, key4].filter(Boolean)) };
  });

  for (const v of variants) {
    const hit = normCandidates.find((c) => c.keys.has(v));
    if (hit) return hit.id;
  }

  // Also try direct normalized raw (covers single-name display cases)
  const rawNorm = normName(staffRaw);
  const hitRaw = normCandidates.find((c) => c.keys.has(rawNorm));
  return hitRaw ? hitRaw.id : null;
}
async function findAuthorisedTimesheet(env, { candidate_id, ymd, start_hhmm, end_hhmm, unit }) {
  // Convert UK local to UTC boundaries
  const startUtcIso = ukLocalToUtcISO(ymd, start_hhmm);

  // Handle overnights: if end < start => add 1 day before converting
  let endYmd = ymd;
  const [sh, sm] = start_hhmm.split(':').map(Number);
  const [eh, em] = end_hhmm.split(':').map(Number);
  const isOvernight = (eh * 60 + em) <= (sh * 60 + sm);
  if (isOvernight) {
    const dt = new Date(Date.UTC(...ymd.split('-').map((n, i) => (i === 1 ? Number(n) - 1 : Number(n)))));
    dt.setUTCDate(dt.getUTCDate() + 1);
    endYmd = `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, '0')}-${String(dt.getUTCDate()).padStart(2, '0')}`;
  }
  const endUtcIso = ukLocalToUtcISO(endYmd, end_hhmm);

  // ✅ Query the HR view (not base timesheets) and use "unit" (not hospital/ward)
  // View columns: id, candidate_id, start_utc, end_utc, unit, role_code, authorised, date_ymd
  let q =
    `${env.SUPABASE_URL}/rest/v1/timesheets_hr_view` +
    `?select=id,candidate_id,start_utc,end_utc,unit,role_code,authorised,date_ymd` +
    `&authorised=eq.true` +
    `&candidate_id=eq.${encodeURIComponent(candidate_id)}` +
    `&start_utc=eq.${encodeURIComponent(startUtcIso)}` +
    `&end_utc=eq.${encodeURIComponent(endUtcIso)}`;

  if (unit) {
    q += `&unit=eq.${encodeURIComponent(unit)}`;
  }

  const { rows } = await sbFetch(env, q);

  if (rows.length === 1) return rows[0];
  if (rows.length > 1) return { MULTIPLE: rows };
  // If exact match not found, try loose match on date + role later in validator
  return null;
}


async function upsertValidation(env, user, { timesheet_id, status, reason, hr_reference, import_id }) {
  // Upsert into timesheet_validations keyed by timesheet_id
  const nowIso = new Date().toISOString();
  const shouldStamp =
    typeof status === 'string' && /ok|pass|valid/i.test(status);

  const payload = {
    timesheet_id,
    status,
    reason_code: reason || null,
    hr_request_id: hr_reference || null,
    validated_at_utc: shouldStamp ? nowIso : null,
    last_source: import_id || null
  };

  const res = await fetch(
    `${env.SUPABASE_URL}/rest/v1/timesheet_validations?on_conflict=timesheet_id`,
    {
      method: 'POST',
      headers: { ...sbHeaders(env), 'Prefer': 'resolution=merge-duplicates,return=minimal' },
      body: JSON.stringify(payload)
    }
  );
  if (!res.ok){
    const err = await res.text();
    throw new Error(`timesheet_validations upsert failed: ${err}`);
  }
}

// --- FIXED: hr_results insert uses correct columns only ---
async function writeHRResult(env, importId, rowId, payload) {
  const nowIso = new Date().toISOString();

  const rec = {
    row_id: rowId,
    status: payload?.status,                 // 'SUCCESS' | 'FAIL'
    reason_code: payload?.reason_code ?? null,
    details_json: payload?.details_json ?? null,
    created_at_utc: nowIso
  };

  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/hr_results`, {
    method: 'POST',
    headers: sbHeaders(env),
    body: JSON.stringify(rec),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`hr_results insert failed: ${err}`);
  }
}


/**
 * @openapi
 * /healthroster/import:
 *   post:
 *     summary: Import HealthRoster data file
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               file_key: { type: string, description: R2 key of uploaded file }
 *               original_name: { type: string }
 *               tz_assumption: { type: string, description: Optional IANA tz, defaults to Europe/London }
 *     responses:
 *       200:
 *         description: Import record created
 */
async function handleHRImport(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const body = await parseJSONBody(req);
  const filename = (body?.original_name || body?.file_key || '').trim();
  if (!filename) {
    return withCORS(env, req, badRequest("original_name or file_key is required"));
  }

  try {
    const nowIso = new Date().toISOString();
    // hr_imports columns: filename, uploaded_by, uploaded_at_utc, tz_assumption, parse_summary_json
    const payload = {
      filename,
      uploaded_by: user.id,
      uploaded_at_utc: nowIso,
      tz_assumption: body?.tz_assumption || 'Europe/London',
    ...(body?.parse_summary_json ? { parse_summary_json: body.parse_summary_json } : {}),
    };

    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/hr_imports`, {
      method: 'POST',
      headers: { ...sbHeaders(env), Prefer: 'return=representation' },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Import record create failed: ${err}`));
    }

    const json = await res.json().catch(() => ([]));
    const rec = Array.isArray(json) ? json[0] : json;

    if (!rec || !rec.id) {
      return withCORS(env, req, serverError("Import record create returned no id"));
    }

    // Best-effort audit
    await writeAudit(
      env,
      user,
      'HR_IMPORT_CREATED',
      { import_id: rec.id, filename },
      { entity: 'hr_imports', subject_id: rec.id, req }
    );

    return withCORS(env, req, ok({ import_id: rec.id }));
  } catch {
    return withCORS(env, req, serverError("Failed to create HR import"));
  }
}

/**
 * @openapi
 * /healthroster/{import_id}/mapping:
 *   get:
 *     summary: Get mapping suggestions
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: query
 *         name: q
 *         schema: { type: string }
 *         description: Filter by hr_name_norm (ILIKE)
 *   post:
 *     summary: Save mapping adjustments (array or object)
 *     security:
 *       - bearerAuth: []
 */

async function handleHRMapping(env, req, importId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  if (!importId) {
    // Path keeps importId for routing; table has no import_id (global mappings).
  }

  if (req.method === 'GET') {
    try {
      const { searchParams } = new URL(req.url);
      const limit = Math.min(Math.max(parseInt(searchParams.get('limit') || '500', 10) || 500, 1), 5000);
      const offset = Math.max(parseInt(searchParams.get('offset') || '0', 10) || 0, 0);
      const order = searchParams.get('order') || 'created_at.desc';
      const q = (searchParams.get('q') || '').trim();

      let url =
        `${env.SUPABASE_URL}/rest/v1/hr_name_mappings?select=*` +
        `&active=eq.true` +
        `&order=${encodeURIComponent(order)}` +
        `&limit=${limit}` +
        `&offset=${offset}`;

      if (q) url += `&hr_name_norm=ilike.${encodeURIComponent(`%${q}%`)}`;

      const { rows } = await sbFetch(env, url, false);
      return withCORS(env, req, ok({ mappings: rows }));
    } catch {
      return withCORS(env, req, serverError('Failed to fetch mappings'));
    }
  }

  if (req.method === 'POST') {
    try {
      const data = await parseJSONBody(req);
      if (!data) return withCORS(env, req, badRequest('Invalid JSON'));

      // hr_name_mappings requires: hr_name_norm (text), candidate_id (uuid), optional hospital_or_trust
      const nowUser = user.id;
      const input = Array.isArray(data) ? data : [data];

      const payload = input
        .map((m) => ({
          hr_name_norm: (m?.hr_name_norm || m?.staff_norm || '').trim(),
          hospital_or_trust: m?.hospital_or_trust ?? null,
          candidate_id: m?.candidate_id || null,
          active: m?.active === false ? false : true,
          created_by: nowUser,
          notes: m?.notes ?? null,
        }))
        .filter((m) => m.hr_name_norm && m.candidate_id);

      if (!payload.length) {
        return withCORS(env, req, badRequest('No valid mappings to upsert'));
      }

      // Upsert against global active uniqueness (hr_name_norm, hospital_or_trust) with active=true rows
      const res = await fetch(
        `${env.SUPABASE_URL}/rest/v1/hr_name_mappings?on_conflict=hr_name_norm,hospital_or_trust`,
        {
          method: 'POST',
          headers: { ...sbHeaders(env), Prefer: 'resolution=merge-duplicates,return=representation' },
          body: JSON.stringify(payload),
        }
      );

      if (!res.ok) {
        const err = await res.text();
        return withCORS(env, req, badRequest(`Mapping upsert failed: ${err}`));
      }

      const json = await res.json().catch(() => []);

      await writeAudit(
        env,
        user,
        'HR_MAPPINGS_UPSERT',
        { import_id: importId, count: payload.length },
        { entity: 'hr_name_mappings', subject_id: importId, req }
      );

      return withCORS(env, req, ok({ mappings: json }));
    } catch {
      return withCORS(env, req, serverError('Failed to upsert mappings'));
    }
  }

  return withCORS(env, req, badRequest('Unsupported method'));
}

/**
 * @openapi
 * /healthroster/{import_id}/validate:
 *   post:
 *     summary: Validate imported data -> writes hr_results + timesheet_validations
 *     security:
 *       - bearerAuth: []
 */
async function handleHRValidate(env, req, importId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  try {
    // Mark the import as "validation requested" inside hr_imports.parse_summary_json
    // (since there is no status/requested_at_utc column on hr_imports)
    const nowIso = new Date().toISOString();

    // Load current parse_summary_json
    const selUrl = `${env.SUPABASE_URL}/rest/v1/hr_imports?id=eq.${encodeURIComponent(importId)}&select=parse_summary_json&limit=1`;
    const { rows: curRows } = await sbFetch(env, selUrl, false);
    const cur = (curRows?.[0]?.parse_summary_json && typeof curRows[0].parse_summary_json === 'object')
      ? curRows[0].parse_summary_json
      : {};

    const patchBody = {
      parse_summary_json: {
        ...cur,
        validation_requested_at_utc: nowIso,
      },
    };

    const patchRes = await fetch(
      `${env.SUPABASE_URL}/rest/v1/hr_imports?id=eq.${encodeURIComponent(importId)}`,
      {
        method: "PATCH",
        headers: sbHeaders(env),
        body: JSON.stringify(patchBody),
      }
    );

    if (!patchRes.ok) {
      const err = await patchRes.text().catch(() => "");
      return withCORS(env, req, serverError(`Failed to update import: ${err}`));
    }

    // Optionally trigger downstream validation flow (Power Automate, etc.)
    const map = JSON.parse(env.TSO_WEBHOOK_MAP || "{}");
    const hook = map["hr_validate"];
    if (hook) {
      const resp = await fetch(hook, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ import_id: importId }),
      });
      if (!resp.ok) {
        const err = await resp.text().catch(() => "");
        return withCORS(env, req, serverError(`Validation trigger failed: ${err}`));
      }
    }

    // Audit (best-effort)
    await writeAudit(
      env,
      user,
      "HR_VALIDATE_REQUESTED",
      { import_id: importId },
      { entity: "hr_imports", subject_id: importId, req }
    );

    return withCORS(env, req, ok({ ok: true }));
  } catch {
    return withCORS(env, req, serverError("Validation trigger failed"));
  }
}

// ---------------------- Router wiring hint (additions) ----------------------
// if (req.method === 'GET'  && p === '/emails/outbox')            return withCORS(env, req, await handleOutboxList(env, req));
// if (req.method === 'POST' && p === '/emails/outbox/mark-sent')  return withCORS(env, req, await handleOutboxMarkSent(env, req));
// if (req.method === 'POST' && p === '/emails/outbox/mark-failed')return withCORS(env, req, await handleOutboxMarkFailed(env, req));
// if (req.method === 'POST' && p === '/broadcast/email')          return withCORS(env, req, await handleBroadcastEmail(env, req));
// if (req.method === 'POST' && p === '/healthroster/import')      return withCORS(env, req, await handleHRImport(env, req));
// if (req.method === 'GET'  && p.startsWith('/healthroster/') && p.endsWith('/rows')){
//   const importId = p.split('/')[2];
//   return withCORS(env, req, await handleHRRows(env, req, importId));
// }
// if (p.startsWith('/healthroster/') && p.endsWith('/mapping')){
//   const importId = p.split('/')[2];
//   return withCORS(env, req, await handleHRMapping(env, req, importId));
// }
// if (req.method === 'POST' && p.startsWith('/healthroster/') && p.endsWith('/validate')){
//   const importId = p.split('/')[2];
//   return withCORS(env, req, await handleHRValidate(env, req, importId));
// }

// ---------------------- Notes ----------------------
// - mail_outbox table expected columns: id, type, reference, to, subject, body_text, status, sent_at, failed_at, last_error, provider_message_id, created_at
// - hr_* tables expected columns:
//   hr_imports: id, file_key, original_name, status, created_at, created_by, validated_at, pass_count, fail_count
//   hr_rows: id, import_id, date_ymd (YYYY-MM-DD), start_hhmm, end_hhmm, unit, request_grade, request_id, staff
//   hr_name_mappings: import_id (nullable), staff_norm, candidate_id, locked, confidence, updated_at, updated_by
//   hr_results: id, import_id, row_id, status (OK/FAIL), reason, request_id, timesheet_id, candidate_id, created_at
// - timesheet_validations: timesheet_id, status (VALIDATION_OK/ERROR_* or OVERRIDDEN), reason, hr_reference, updated_at, updated_by
// - timesheets: id, candidate_id, start_utc, end_utc, hospital, ward, authorised, role_code (HCA/RMN)
// - All timestamps stored in UTC; UK local used only for classification/HR comparison.

// ====================== INVOICES ======================
/**
 * @openapi
 * /api/invoices:
 *   get:
 *     summary: List invoices
 *     tags: [Invoices]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: query
 *         name: status
 *         schema: { type: string, enum: [paid, unpaid] }
 *       - in: query
 *         name: include_count
 *         schema: { type: boolean }
 *       - in: query
 *         name: limit
 *         schema: { type: integer, minimum: 1, maximum: 200 }
 *       - in: query
 *         name: offset
 *         schema: { type: integer, minimum: 0 }
 *   post:
 *     summary: Create invoice from eligible timesheets
 *     tags: [Invoices]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [client_id, timesheet_ids]
 *             properties:
 *               client_id: { type: string }
 *               issue_date: { type: string, format: date, nullable: true }
 *               timesheet_ids:
 *                 type: array
 *                 items: { type: string }
 * /api/invoices/{invoice_id}:
 *   get:
 *     summary: Get invoice with lines
 *     tags: [Invoices]
 *     security:
 *       - bearerAuth: []
 * /api/invoices/{invoice_id}/render:
 *   post:
 *     summary: Render invoice PDF (stores to R2 and records key)
 *     tags: [Invoices]
 *     security:
 *       - bearerAuth: []
 * /api/invoices/{invoice_id}/email:
 *   post:
 *     summary: Queue invoice email in mail_outbox
 *     tags: [Invoices]
 *     security:
 *       - bearerAuth: []
 * /api/invoices/{invoice_id}/credit-note:
 *   post:
 *     summary: Create a credit note referencing original invoice
 *     tags: [Invoices]
 *     security:
 *       - bearerAuth: []
 * /api/invoices/{invoice_id}/mark-paid:
 *   post:
 *     summary: Mark invoice as paid (sets paid_at_utc)
 *     tags: [Invoices]
 *     security:
 *       - bearerAuth: []
 * /api/invoices/{invoice_id}/unpay:
 *   post:
 *     summary: Revert invoice to unpaid
 *     tags: [Invoices]
 *     security:
 *       - bearerAuth: []
 */
// ------------------------
// LIST INVOICES (light UI)
// ------------------------
async function handleListInvoices(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const params = new URL(req.url).searchParams;
  const statusFilter = params.get("status");
  const includeCount = params.get("include_count") === "true";
  const limit = Math.min(parseInt(params.get("limit") || "50", 10), 200);
  const offset = parseInt(params.get("offset") || "0", 10);

  let filter = "";
  if (statusFilter === "paid") filter = "&paid_at_utc=not.is.null";
  if (statusFilter === "unpaid") filter = "&paid_at_utc=is.null";

  // Keep things light but include header snapshot for UI
  const select =
    [
      'id','invoice_no','client_id','issued_at_utc','due_at_utc',
      'status','subtotal_ex_vat','vat_amount','total_inc_vat',
      'invoice_pdf_r2_key','header_snapshot_json'
    ].join(',');

  const url = `${env.SUPABASE_URL}/rest/v1/invoices?select=${select}&order=issued_at_utc.desc&limit=${limit}&offset=${offset}${filter}`;

  try {
    const { rows, total } = await sbFetch(env, url, includeCount);
    const resp = includeCount ? { items: rows, count: total ?? undefined } : { items: rows };
    return withCORS(env, req, ok(resp));
  } catch {
    return withCORS(env, req, serverError("Failed to list invoices"));
  }
}


// New: one-email-per-candidate remittance composer + queue + audit

/**
 * @openapi
 * /api/remittances/email-for-candidate:
 *   post:
 *     summary: Queue a remittance email for a candidate (HTML; one email per pay-run)
 *     description: >
 *       Queues a single HTML remittance email to a candidate, either for an explicit list
 *       of timesheets or for a date range. The email is queued in `mail_outbox` and an audit
 *       trail is written to `audit_events` for the candidate and each included timesheet.
 *     tags: [Remittances]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             oneOf:
 *               - type: object
 *                 required: [timesheet_ids]
 *                 properties:
 *                   timesheet_ids:
 *                     type: array
 *                     items: { type: string, format: uuid }
 *                     description: Explicit list of timesheet IDs to include.
 *               - type: object
 *                 required: [candidate_id, period_start, period_end]
 *                 properties:
 *                   candidate_id: { type: string, format: uuid }
 *                   period_start: { type: string, format: date }
 *                   period_end:   { type: string, format: date }
 *     responses:
 *       "200":
 *         description: Email queued successfully
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 queued: { type: boolean }
 *                 mail_id: { type: string, format: uuid, nullable: true }
 *                 items: { type: integer, description: "Number of timesheets included" }
 *       "400":
 *         description: Bad request (missing selector or invalid input)
 *       "401":
 *         description: Unauthorized
 *       "404":
 *         description: No matching timesheets for the selection
 */

// -------------------
// GET INVOICE (+meta)
// -------------------
async function handleGetInvoice(env: any, req: Request, invoiceId: string) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoiceId)}&select=*,client:clients(name,primary_invoice_email)`
    );
    if (!rows.length) return withCORS(env, req, notFound("Invoice not found"));
    const invoice = rows[0];

    const { rows: lineRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoice_lines` +
        `?invoice_id=eq.${encodeURIComponent(invoiceId)}` +
        `&select=` +
        [
          'id','invoice_id','timesheet_id','booking_id','description',
          'hours_day','hours_night','hours_sat','hours_sun','hours_bh',
          'pay_day','pay_night','pay_sat','pay_sun','pay_bh',
          'charge_day','charge_night','charge_sat','charge_sun','charge_bh',
          'total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat',
          'vat_rate_pct','vat_amount','total_inc_vat','paper_ts_r2_key',
          'meta_json'
        ].join(',')
    );

    const items = lineRows.map((l: any) => ({
      booking_id: l.booking_id ?? null,
      timesheet_id: l.timesheet_id ?? null,
      qty: { day: l.hours_day, night: l.hours_night, sat: l.hours_sat, sun: l.hours_sun, bh: l.hours_bh },
      rate: { day: l.charge_day, night: l.charge_night, sat: l.charge_sat, sun: l.charge_sun, bh: l.charge_bh },
      total_ex_vat: l.total_charge_ex_vat,
      description: l.description,
      meta_json: l.meta_json ?? {}
    }));

    // Optional correspondence
    const includeCorr = new URL(req.url).searchParams.get('include_correspondence');
    if (includeCorr) {
      let correspondence: any[] = [];
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

        const mailIds = [...new Set((corrRows || []).map((r: any) => r.correlation_id).filter(Boolean))];
        let mailMap: Record<string, any> = {};
        if (mailIds.length) {
          const { rows: mailRows } = await sbFetch(
            env,
            `${env.SUPABASE_URL}/rest/v1/mail_outbox` +
              `?id=in.(${mailIds.map(encodeURIComponent).join(',')})` +
              `&select=id,to,cc,subject,status,created_at_utc,sent_at,failed_at,reference,provider_message_id`
          );
          mailMap = Object.fromEntries((mailRows || []).map((m: any) => [m.id, m]));
        }

        correspondence = (corrRows || []).map((ev: any) => ({
          ts_utc: ev.ts_utc,
          action: ev.action,
          correlation_id: ev.correlation_id || null,
          after_json: ev.after_json ?? null,
          email: ev.correlation_id && mailMap[ev.correlation_id]
            ? mailMap[ev.correlation_id]
            : null
        }));
      } catch {
        correspondence = [];
      }
      return withCORS(env, req, ok({ invoice, items, header_snapshot_json: invoice.header_snapshot_json ?? {}, correspondence }));
    }

    return withCORS(env, req, ok({ invoice, items, header_snapshot_json: invoice.header_snapshot_json ?? {} }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch invoice"));
  }
}



// ----------------------
// RENDER INVOICE (SNAP)
// ----------------------
async function handleInvoiceRender(env: any, req: Request, invoiceId: string) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    // Pull header snapshot + bare client join for name/email (optional)
    const { rows: invRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoices` +
      `?id=eq.${encodeURIComponent(invoiceId)}` +
      `&select=id,invoice_no,issued_at_utc,due_at_utc,subtotal_ex_vat,vat_amount,total_inc_vat,header_snapshot_json`
    );
    if (!invRows?.length) return withCORS(env, req, notFound("Invoice not found"));
    const inv = invRows[0];

    // Fetch lines with meta_json only (no live joins)
    const { rows: lineRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoice_lines` +
      `?invoice_id=eq.${encodeURIComponent(invoiceId)}` +
      `&select=id,description,total_charge_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,meta_json`
    );

    const invoiceData = {
      header: inv.header_snapshot_json || {},
      invoice_no: inv.invoice_no || null,
      issued_at_utc: inv.issued_at_utc,
      due_at_utc: inv.due_at_utc,
      totals: {
        subtotal_ex_vat: Number(inv.subtotal_ex_vat || 0),
        vat_amount: Number(inv.vat_amount || 0),
        total_inc_vat: Number(inv.total_inc_vat || 0)
      },
      items: (lineRows || []).map((l: any) => ({
        description: l.description,
        meta: l.meta_json ?? {},
        total_ex_vat: Number(l.total_charge_ex_vat || 0),
        vat_rate_pct: Number(l.vat_rate_pct || 0),
        vat_amount: Number(l.vat_amount || 0),
        total_inc_vat: Number(l.total_inc_vat || 0)
      }))
    };

    const map = JSON.parse(env.TSO_WEBHOOK_MAP || "{}");
    const url = map["invoice_render"];
    if (!url) return withCORS(env, req, serverError("Invoice render webhook not configured"));

    const flowRes = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(invoiceData)
    });
    if (!flowRes.ok) {
      const err = await flowRes.text();
      return withCORS(env, req, serverError(`Render failed: ${err}`));
    }
    const flowJson = await flowRes.json().catch(() => ({}));
    const pdfBase64 = flowJson.file || flowJson.pdf;
    if (!pdfBase64) return withCORS(env, req, serverError("No PDF returned from renderer"));

    // Store in R2
    const bin = atob(pdfBase64);
    const buffer = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) buffer[i] = bin.charCodeAt(i);

    const pdfKey = `/docs-pdf/invoices/invoice_${invoiceId}.pdf`;
    await r2Put(env, pdfKey, buffer, { httpMetadata: { contentType: "application/pdf" } });

    await fetch(`${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoiceId)}`, {
      method: "PATCH",
      headers: sbHeaders(env),
      body: JSON.stringify({ invoice_pdf_r2_key: pdfKey })
    });

    // Short-lived signed URL via token
    const exp = Math.floor(Date.now()/1000) + 300;
    const token = await createToken(env.UPLOAD_TOKEN_SECRET, { typ: "dl", key: pdfKey, exp });
    const downloadUrl = new URL(req.url);
    downloadUrl.pathname = "/api/files/download";
    downloadUrl.search = "";
    downloadUrl.searchParams.set("key", pdfKey);
    downloadUrl.searchParams.set("token", token);

    return withCORS(env, req, ok({ pdf_url: downloadUrl.toString() }));
  } catch {
    return withCORS(env, req, serverError("Failed to render invoice"));
  }
}

async function handleInvoiceCredit(env, req, invoiceId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoiceId)}&select=invoice_no,client_id`
    );
    if (!rows.length) return withCORS(env, req, notFound("Invoice not found"));
    const orig = rows[0];

    const { rows: latest } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoices?select=invoice_no&order=invoice_no.desc&limit=1`
    );
    const nextNo = latest.length ? ((latest[0].invoice_no | 0) + 1) : 1001;

    const now = new Date().toISOString();
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/invoices`, {
      method: "POST",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({
        client_id: orig.client_id,
        invoice_no: nextNo,
        type: "CREDIT_NOTE",
        status: "ISSUED",
        original_invoice_id: invoiceId,
        issued_at_utc: now
      })
    });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, serverError(`Credit note creation failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const creditInv = Array.isArray(json) ? json[0] : json;

    // Mirror original lines with negative monetary amounts (hours unchanged)
    const { rows: origLines } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoice_lines?invoice_id=eq.${encodeURIComponent(invoiceId)}&select=id,invoice_id,timesheet_id,booking_id,description,hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_day,pay_night,pay_sat,pay_sun,pay_bh,charge_day,charge_night,charge_sat,charge_sun,charge_bh,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,paper_ts_r2_key`
    );

    const creditLines = origLines.map((l) => ({
      invoice_id: creditInv.id,
      timesheet_id: l.timesheet_id || null,
      booking_id: l.booking_id || null,
      description: l.description || null,
      hours_day: Number(l.hours_day || 0),
      hours_night: Number(l.hours_night || 0),
      hours_sat: Number(l.hours_sat || 0),
      hours_sun: Number(l.hours_sun || 0),
      hours_bh: Number(l.hours_bh || 0),
      pay_day: l.pay_day == null ? null : -Math.abs(Number(l.pay_day)),
      pay_night: l.pay_night == null ? null : -Math.abs(Number(l.pay_night)),
      pay_sat: l.pay_sat == null ? null : -Math.abs(Number(l.pay_sat)),
      pay_sun: l.pay_sun == null ? null : -Math.abs(Number(l.pay_sun)),
      pay_bh: l.pay_bh == null ? null : -Math.abs(Number(l.pay_bh)),
      charge_day: l.charge_day == null ? null : -Math.abs(Number(l.charge_day)),
      charge_night: l.charge_night == null ? null : -Math.abs(Number(l.charge_night)),
      charge_sat: l.charge_sat == null ? null : -Math.abs(Number(l.charge_sat)),
      charge_sun: l.charge_sun == null ? null : -Math.abs(Number(l.charge_sun)),
      charge_bh: l.charge_bh == null ? null : -Math.abs(Number(l.charge_bh)),
      total_pay_ex_vat: -Math.abs(Number(l.total_pay_ex_vat || 0)),
      total_charge_ex_vat: -Math.abs(Number(l.total_charge_ex_vat || 0)),
      margin_ex_vat: -Math.abs(Number(l.margin_ex_vat || 0)),
      vat_rate_pct: l.vat_rate_pct == null ? 20.0 : Number(l.vat_rate_pct),
      vat_amount: -Math.abs(Number(l.vat_amount || 0)),
      total_inc_vat: -Math.abs(Number(l.total_inc_vat || 0)),
      paper_ts_r2_key: l.paper_ts_r2_key || null
    }));

    if (creditLines.length) {
      const liRes = await fetch(`${env.SUPABASE_URL}/rest/v1/invoice_lines`, {
        method: "POST",
        headers: sbHeaders(env),
        body: JSON.stringify(creditLines)
      });
      if (!liRes.ok) {
        const err = await liRes.text();
        return withCORS(env, req, serverError(`Failed to insert credit lines: ${err}`));
      }
    }

    return withCORS(env, req, ok({ credit_invoice_id: creditInv.id, invoice_no: creditInv.invoice_no }));
  } catch {
    return withCORS(env, req, serverError("Failed to create credit note"));
  }
}

async function handleInvoiceMarkPaid(env, req, invoiceId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req).catch(() => null);
  let paidAt = new Date();
  if (data && data.paid_date) {
    const pd = new Date(data.paid_date);
    if (!isNaN(pd.getTime())) paidAt = pd;
  }

  try {
    await fetch(`${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoiceId)}`, {
      method: "PATCH",
      headers: sbHeaders(env),
      body: JSON.stringify({ paid_at_utc: paidAt.toISOString() })
    });
    return withCORS(env, req, ok({ ok: true }));
  } catch {
    return withCORS(env, req, serverError("Failed to mark invoice paid"));
  }
}

async function handleInvoiceMarkUnpaid(env, req, invoiceId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    await fetch(`${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoiceId)}`, {
      method: "PATCH",
      headers: sbHeaders(env),
      body: JSON.stringify({ paid_at_utc: null })
    });
    return withCORS(env, req, ok({ ok: true }));
  } catch {
    return withCORS(env, req, serverError("Failed to mark invoice unpaid"));
  }
}

// ====================== RELATED: COUNTS (generic) ======================
/**
 * @openapi
 * /api/related/{entity}/{id}/counts:
 *   get:
 *     summary: Get counts of related records for an entity (candidate, timesheet, invoice, remittance)
 *     tags: [Related]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: entity
 *         required: true
 *         schema:
 *           type: string
 *           enum: [candidate, timesheet, invoice, remittance]
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *           description: The entity identifier (UUID for candidate/timesheet/invoice; mail_outbox.id for remittance)
 *     responses:
 *       200:
 *         description: Counts keyed by related type for the given entity.
 *         content:
 *           application/json:
 *             schema:
 *               oneOf:
 *                 - type: object
 *                   properties:
 *                     timesheets: { type: integer }
 *                     invoices:   { type: integer }
 *                     remittances:{ type: integer }
 *                 - type: object
 *                   properties:
 *                     candidate:  { type: integer }
 *                     invoice:    { type: integer }
 *                     remittances:{ type: integer }
 *                 - type: object
 *                   properties:
 *                     timesheets:     { type: integer }
 *                     candidates:     { type: integer }
 *                     correspondence: { type: integer }
 *                 - type: object
 *                   properties:
 *                     timesheets: { type: integer }
 *                     candidate:  { type: integer }
 */
async function handleRelatedCounts(env, req, entity, id) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  // Small helper to read exact count via PostgREST Content-Range, if available
  const countOrLen = (res) => (typeof res.count === 'number' ? res.count : (res.rows?.length || 0));

  try {
    if (entity === 'candidate') {
      // timesheets count (current snapshots for candidate)
      const tsq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?candidate_id=eq.${encodeURIComponent(id)}&is_current=eq.true&select=id`;
      const tsfin = await sbFetch(env, tsq, { preferExactCount: true });

      // invoices count (distinct locked_by_invoice_id where not null)
      const invq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?candidate_id=eq.${encodeURIComponent(id)}&is_current=eq.true&locked_by_invoice_id=not.is.null&select=locked_by_invoice_id`;
      const invfin = await sbFetch(env, invq);
      const invDistinct = new Set((invfin.rows || []).map(r => r.locked_by_invoice_id).filter(Boolean));

      // remittances count (mail_outbox by reference prefix)
      const refPrefix = `remit:candidate:${id}:`;
      const remq = `${env.SUPABASE_URL}/rest/v1/mail_outbox?type=eq.REMITTANCE&reference=like.${encodeURIComponent(refPrefix + '%')}&select=id`;
      const rem = await sbFetch(env, remq, { preferExactCount: true });

      return withCORS(env, req, ok({
        timesheets: countOrLen(tsfin),
        invoices:   invDistinct.size,
        remittances: countOrLen(rem),
      }));
    }

    if (entity === 'timesheet') {
      // candidate (0/1) & invoice (0/1) from current tsfin row
      const curq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true&select=candidate_id,locked_by_invoice_id`;
      const cur = await sbFetch(env, curq);
      const row = (cur.rows || [])[0] || {};
      const hasCandidate = row.candidate_id ? 1 : 0;
      const hasInvoice   = row.locked_by_invoice_id ? 1 : 0;

      // remittances count via audit correlation_ids
      const audq = `${env.SUPABASE_URL}/rest/v1/audit_events?object_type=eq.timesheet&object_id_text=eq.${encodeURIComponent(id)}&reason=eq.REMITTANCE&action=in.(EMAIL_QUEUED,EMAIL_SENT)&select=correlation_id`;
      const aud = await sbFetch(env, audq);
      const remDistinct = new Set((aud.rows || []).map(r => r.correlation_id).filter(Boolean));

      return withCORS(env, req, ok({
        candidate: hasCandidate,
        invoice:   hasInvoice,
        remittances: remDistinct.size,
      }));
    }

    if (entity === 'invoice') {
      // timesheets & candidates from current tsfin rows linked to invoice
      const tsq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?locked_by_invoice_id=eq.${encodeURIComponent(id)}&is_current=eq.true&select=candidate_id,id`;
      const tsfin = await sbFetch(env, tsq, { preferExactCount: true });
      const candDistinct = new Set((tsfin.rows || []).map(r => r.candidate_id).filter(Boolean));

      // correspondence (audit) on invoice
      const audq = `${env.SUPABASE_URL}/rest/v1/audit_events?object_type=eq.invoice&object_id_text=eq.${encodeURIComponent(id)}&action=in.(EMAIL_QUEUED,EMAIL_SENT)&select=id`;
      const aud = await sbFetch(env, audq, { preferExactCount: true });

      return withCORS(env, req, ok({
        timesheets: countOrLen(tsfin),
        candidates: candDistinct.size,
        correspondence: countOrLen(aud),
      }));
    }

    if (entity === 'remittance') {
      // A "remittance" here is a mail_outbox row (id = correlation_id)
      // Count related timesheets from audit trail and whether a candidate exists.
      const base = `correlation_id=eq.${encodeURIComponent(id)}&reason=eq.REMITTANCE`;
      const tsAudQ = `${env.SUPABASE_URL}/rest/v1/audit_events?${base}&object_type=eq.timesheet&select=object_id_text`;
      const tsAud = await sbFetch(env, tsAudQ);
      const tsDistinct = new Set((tsAud.rows || []).map(r => r.object_id_text).filter(Boolean));

      const candAudQ = `${env.SUPABASE_URL}/rest/v1/audit_events?${base}&object_type=eq.candidate&select=id`;
      const candAud = await sbFetch(env, candAudQ);
      const hasCand = (candAud.rows || []).length > 0 ? 1 : 0;

      return withCORS(env, req, ok({
        timesheets: tsDistinct.size,
        candidate: hasCand,
      }));
    }

    return withCORS(env, req, badRequest("Unsupported entity"));
  } catch (e) {
    console.error('handleRelatedCounts error', e);
    return withCORS(env, req, serverError("Failed to load related counts"));
  }
}
// ====================== RELATED: LIST (generic) ======================
/**
 * @openapi
 * /api/related/{entity}/{id}/{type}:
 *   get:
 *     summary: List related records for an entity (candidate, timesheet, invoice, remittance)
 *     tags: [Related]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: entity
 *         required: true
 *         schema:
 *           type: string
 *           enum: [candidate, timesheet, invoice, remittance]
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *       - in: path
 *         name: type
 *         required: true
 *         schema:
 *           type: string
 *           description: >
 *             For candidate: timesheets | invoices | remittances
 *             For timesheet: candidate | invoice | remittances
 *             For invoice:   timesheets | candidates | correspondence
 *             For remittance: timesheets | candidate
 *       - in: query
 *         name: limit
 *         schema: { type: integer, minimum: 1, maximum: 100, default: 20 }
 *       - in: query
 *         name: offset
 *         schema: { type: integer, minimum: 0, default: 0 }
 *     responses:
 *       200:
 *         description: A lightweight list of related records (shape depends on entity/type).
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 items:
 *                   type: array
 *                   items:
 *                     type: object
 *                 total:
 *                   type: integer
 */
async function handleRelatedList(env, req, entity, id) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const url = new URL(req.url);
  const type = (matchPath(url.pathname, '/api/related/:entity/:id/:type') || {}).type;
  const limit = Math.max(1, Math.min(100, parseInt(url.searchParams.get('limit') || '20', 10)));
  const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0', 10));

  const okList = (items, total = undefined) => withCORS(env, req, ok({ items, ...(typeof total === 'number' ? { total } : {}) }));

  try {
    // -------- CANDIDATE --------
    if (entity === 'candidate') {
      if (type === 'timesheets') {
        // Current timesheets for candidate (from tsfin) + basic timesheet summary
        const base = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?candidate_id=eq.${encodeURIComponent(id)}&is_current=eq.true`;
        const sel  = `select=timesheet:timesheets(timesheet_id,booking_id,week_ending_date),processing_status,total_pay_ex_vat,total_hours,client_id`;
        const ord  = `order=timesheet.week_ending_date.desc`;
        const rng  = `&limit=${limit}&offset=${offset}`;
        const res  = await sbFetch(env, `${base}&${sel}&${ord}${rng}`, { preferExactCount: true });
        // Normalize output
        const items = (res.rows || []).map(r => ({
          timesheet_id: r.timesheet?.timesheet_id || r.timesheet_id,
          booking_id:   r.timesheet?.booking_id || null,
          week_ending_date: r.timesheet?.week_ending_date || null,
          processing_status: r.processing_status,
          total_pay_ex_vat:  r.total_pay_ex_vat,
          total_hours:       r.total_hours,
          client_id:         r.client_id || null,
        }));
        return okList(items, typeof res.count === 'number' ? res.count : undefined);
      }

      if (type === 'invoices') {
        // Distinct invoice ids from tsfin, then fetch invoice summaries
        const finq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?candidate_id=eq.${encodeURIComponent(id)}&is_current=eq.true&locked_by_invoice_id=not.is.null&select=locked_by_invoice_id`;
        const fin = await sbFetch(env, finq);
        const invIds = [...new Set((fin.rows || []).map(r => r.locked_by_invoice_id).filter(Boolean))];

        // Page the final list (apply pagination after distinct)
        const pageIds = invIds.slice(offset, offset + limit);
        if (pageIds.length === 0) return okList([], invIds.length);

        const invq = `${env.SUPABASE_URL}/rest/v1/invoices?id=in.(${pageIds.map(encodeURIComponent).join(',')})&select=id,invoice_no,issued_at_utc,status,total_inc_vat,client_id&order=issued_at_utc.desc`;
        const inv  = await sbFetch(env, invq);
        const items = (inv.rows || []).map(r => ({
          invoice_id:    r.id,
          invoice_no:    r.invoice_no,
          issued_at_utc: r.issued_at_utc,
          status:        r.status,
          total_inc_vat: r.total_inc_vat,
          client_id:     r.client_id,
        }));
        return okList(items, invIds.length);
      }

      if (type === 'remittances') {
        const refPrefix = `remit:candidate:${id}:`;
        const base = `${env.SUPABASE_URL}/rest/v1/mail_outbox?type=eq.REMITTANCE&reference=like.${encodeURIComponent(refPrefix + '%')}`;
        const sel  = `select=id,to,subject,status,created_at_utc,sent_at,reference`;
        const ord  = `order=created_at_utc.desc`;
        const rng  = `&limit=${limit}&offset=${offset}`;
        const res  = await sbFetch(env, `${base}&${sel}&${ord}${rng}`, { preferExactCount: true });
        const items = (res.rows || []).map(r => ({
          mail_id: r.id,
          to: r.to,
          subject: r.subject,
          status: r.status,
          created_at_utc: r.created_at_utc,
          sent_at: r.sent_at,
          reference: r.reference,
        }));
        return okList(items, typeof res.count === 'number' ? res.count : undefined);
      }

      return withCORS(env, req, badRequest("Unsupported type for candidate"));
    }

    // -------- TIMESHEET --------
    if (entity === 'timesheet') {
      if (type === 'candidate') {
        const curq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true&select=candidate_id`;
        const cur = await sbFetch(env, curq);
        const candId = (cur.rows || [])[0]?.candidate_id;
        if (!candId) return okList([], 0);
        const cq = `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(candId)}&select=id,display_name,email`;
        const cr = await sbFetch(env, cq);
        const c = (cr.rows || [])[0];
        return okList(c ? [{ id: c.id, display_name: c.display_name, email: c.email }] : [], c ? 1 : 0);
      }

      if (type === 'invoice') {
        const curq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?timesheet_id=eq.${encodeURIComponent(id)}&is_current=eq.true&select=locked_by_invoice_id`;
        const cur = await sbFetch(env, curq);
        const invId = (cur.rows || [])[0]?.locked_by_invoice_id;
        if (!invId) return okList([], 0);
        const iq = `${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invId)}&select=id,invoice_no,issued_at_utc,status,total_inc_vat,client_id`;
        const ir = await sbFetch(env, iq);
        const i = (ir.rows || [])[0];
        return okList(i ? [{
          invoice_id:    i.id,
          invoice_no:    i.invoice_no,
          issued_at_utc: i.issued_at_utc,
          status:        i.status,
          total_inc_vat: i.total_inc_vat,
          client_id:     i.client_id,
        }] : [], i ? 1 : 0);
      }

      if (type === 'remittances') {
        const audq = `${env.SUPABASE_URL}/rest/v1/audit_events?object_type=eq.timesheet&object_id_text=eq.${encodeURIComponent(id)}&reason=eq.REMITTANCE&action=in.(EMAIL_QUEUED,EMAIL_SENT)&select=correlation_id,ts_utc&order=ts_utc.desc`;
        const aud = await sbFetch(env, audq);
        const mailIds = [...new Set((aud.rows || []).map(r => r.correlation_id).filter(Boolean))];
        const total = mailIds.length;
        const pageIds = mailIds.slice(offset, offset + limit);
        if (!pageIds.length) return okList([], total);

        const mq = `${env.SUPABASE_URL}/rest/v1/mail_outbox?id=in.(${pageIds.map(encodeURIComponent).join(',')})&select=id,to,subject,status,created_at_utc,sent_at,reference&order=created_at_utc.desc`;
        const mr = await sbFetch(env, mq);
        const items = (mr.rows || []).map(r => ({
          mail_id: r.id,
          to: r.to,
          subject: r.subject,
          status: r.status,
          created_at_utc: r.created_at_utc,
          sent_at: r.sent_at,
          reference: r.reference,
        }));
        return okList(items, total);
      }

      return withCORS(env, req, badRequest("Unsupported type for timesheet"));
    }

    // -------- INVOICE --------
    if (entity === 'invoice') {
      if (type === 'timesheets') {
        const base = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?locked_by_invoice_id=eq.${encodeURIComponent(id)}&is_current=eq.true`;
        const sel  = `select=timesheet:timesheets(timesheet_id,booking_id,week_ending_date),processing_status,total_pay_ex_vat,total_hours,candidate_id,client_id`;
        const ord  = `order=timesheet.week_ending_date.desc`;
        const rng  = `&limit=${limit}&offset=${offset}`;
        const res  = await sbFetch(env, `${base}&${sel}&${ord}${rng}`, { preferExactCount: true });
        const items = (res.rows || []).map(r => ({
          timesheet_id: r.timesheet?.timesheet_id || r.timesheet_id,
          booking_id:   r.timesheet?.booking_id || null,
          week_ending_date: r.timesheet?.week_ending_date || null,
          processing_status: r.processing_status,
          total_pay_ex_vat:  r.total_pay_ex_vat,
          total_hours:       r.total_hours,
          candidate_id:      r.candidate_id || null,
          client_id:         r.client_id || null,
        }));
        return okList(items, typeof res.count === 'number' ? res.count : undefined);
      }

      if (type === 'candidates') {
        const cq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?locked_by_invoice_id=eq.${encodeURIComponent(id)}&is_current=eq.true&select=candidate_id`;
        const cr = await sbFetch(env, cq);
        const candIds = [...new Set((cr.rows || []).map(r => r.candidate_id).filter(Boolean))];
        const total = candIds.length;
        const pageIds = candIds.slice(offset, offset + limit);
        if (!pageIds.length) return okList([], total);

        const cdetq = `${env.SUPABASE_URL}/rest/v1/candidates?id=in.(${pageIds.map(encodeURIComponent).join(',')})&select=id,display_name,email`;
        const cdet = await sbFetch(env, cdetq);
        const items = (cdet.rows || []).map(c => ({ id: c.id, display_name: c.display_name, email: c.email }));
        return okList(items, total);
      }

      if (type === 'correspondence') {
        // Audit trail for the invoice; enrich with mail_outbox by correlation_id (mail_id)
        const audq = `${env.SUPABASE_URL}/rest/v1/audit_events?object_type=eq.invoice&object_id_text=eq.${encodeURIComponent(id)}&action=in.(EMAIL_QUEUED,EMAIL_SENT)&select=id,action,ts_utc,correlation_id&order=ts_utc.desc`;
        const aud = await sbFetch(env, audq);
        const mailIds = [...new Set((aud.rows || []).map(r => r.correlation_id).filter(Boolean))];
        const total = mailIds.length;
        const pageIds = mailIds.slice(offset, offset + limit);
        if (!pageIds.length) {
          // Return just audit rows if no paged mail records
          const items = (aud.rows || []).map(a => ({ audit_id: a.id, action: a.action, ts_utc: a.ts_utc, mail_id: a.correlation_id || null }));
          return okList(items, total);
        }

        const mq = `${env.SUPABASE_URL}/rest/v1/mail_outbox?id=in.(${pageIds.map(encodeURIComponent).join(',')})&select=id,to,subject,status,created_at_utc,sent_at,reference&order=created_at_utc.desc`;
        const mr = await sbFetch(env, mq);
        const mById = new Map((mr.rows || []).map(m => [m.id, m]));
        // Compose items in mail order (newest first)
        const items = pageIds.map(mid => {
          const m = mById.get(mid);
          return m ? {
            mail_id: m.id, to: m.to, subject: m.subject, status: m.status,
            created_at_utc: m.created_at_utc, sent_at: m.sent_at, reference: m.reference
          } : { mail_id: mid };
        });
        return okList(items, total);
      }

      return withCORS(env, req, badRequest("Unsupported type for invoice"));
    }

    // -------- REMITTANCE (mail_outbox row) --------
    if (entity === 'remittance') {
      if (type === 'timesheets') {
        const audq = `${env.SUPABASE_URL}/rest/v1/audit_events?correlation_id=eq.${encodeURIComponent(id)}&reason=eq.REMITTANCE&object_type=eq.timesheet&select=object_id_text,ts_utc&order=ts_utc.desc`;
        const aud = await sbFetch(env, audq);
        const tsIds = [...new Set((aud.rows || []).map(r => r.object_id_text).filter(Boolean))];
        const total = tsIds.length;
        const pageIds = tsIds.slice(offset, offset + limit);
        if (!pageIds.length) return okList([], total);

        const tsq = `${env.SUPABASE_URL}/rest/v1/timesheets?timesheet_id=in.(${pageIds.map(encodeURIComponent).join(',')})&select=timesheet_id,booking_id,week_ending_date&order=week_ending_date.desc`;
        const tsr = await sbFetch(env, tsq);
        const items = (tsr.rows || []).map(t => ({
          timesheet_id: t.timesheet_id,
          booking_id: t.booking_id,
          week_ending_date: t.week_ending_date,
        }));
        return okList(items, total);
      }

      if (type === 'candidate') {
        // Prefer audit record; fallback to parsing mail_outbox.reference
        const audq = `${env.SUPABASE_URL}/rest/v1/audit_events?correlation_id=eq.${encodeURIComponent(id)}&reason=eq.REMITTANCE&object_type=eq.candidate&select=object_id_text&limit=1`;
        const aud = await sbFetch(env, audq);
        let candId = (aud.rows || [])[0]?.object_id_text;

        if (!candId) {
          const mq = `${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${encodeURIComponent(id)}&select=reference&limit=1`;
          const mr = await sbFetch(env, mq);
          const ref = (mr.rows || [])[0]?.reference || '';
          // pattern remit:candidate:{uuid}:{...}
          const m = ref.match(/^remit:candidate:([0-9a-fA-F-]{36}):/);
          if (m) candId = m[1];
        }

        if (!candId) return okList([], 0);
        const cq = `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(candId)}&select=id,display_name,email&limit=1`;
        const cr = await sbFetch(env, cq);
        const c = (cr.rows || [])[0];
        return okList(c ? [{ id: c.id, display_name: c.display_name, email: c.email }] : [], c ? 1 : 0);
      }

      return withCORS(env, req, badRequest("Unsupported type for remittance"));
    }

    return withCORS(env, req, badRequest("Unsupported entity"));
  } catch (e) {
    console.error('handleRelatedList error', e);
    return withCORS(env, req, serverError("Failed to load related list"));
  }
}

// ====================== OUTBOX: GET ONE (full email) ======================
/**
 * @openapi
 * /api/outbox/{mail_id}:
 *   get:
 *     summary: Get a full email from the outbox (headers, body, attachments)
 *     tags: [Email]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: mail_id
 *         required: true
 *         schema:
 *           type: string
 *           description: mail_outbox.id (UUID)
 *     responses:
 *       200:
 *         description: Full outbox message
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 id: { type: string }
 *                 type: { type: string, enum: [INVOICE, REMITTANCE, TSO_FAILURE, BROADCAST] }
 *                 to: { type: string }
 *                 cc: { type: string, nullable: true }
 *                 subject: { type: string }
 *                 body_html: { type: string, nullable: true }
 *                 body_text: { type: string, nullable: true }
 *                 attachments:
 *                   type: array
 *                   items:
 *                     type: object
 *                     properties:
 *                       r2_key:   { type: string }
 *                       filename: { type: string }
 *                 status: { type: string, enum: [QUEUED, SENT, FAILED] }
 *                 created_at_utc: { type: string, format: date-time }
 *                 sent_at:        { type: string, format: date-time, nullable: true }
 *                 failed_at:      { type: string, format: date-time, nullable: true }
 *                 last_error: { type: string, nullable: true }
 *                 reference:  { type: string, nullable: true }
 *                 provider_message_id: { type: string, nullable: true }
 *       404:
 *         description: Not found
 */
async function handleOutboxGet(env, req, mailId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const q = `${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${encodeURIComponent(mailId)}&select=id,type,to,cc,subject,body_html,body_text,attachments,status,last_error,created_at_utc,sent_at,failed_at,reference,provider_message_id&limit=1`;
    const res = await sbFetch(env, q);
    const row = (res.rows || [])[0];
    if (!row) return withCORS(env, req, notFound("Outbox message not found"));
    return withCORS(env, req, ok(row));
  } catch (e) {
    console.error('handleOutboxGet error', e);
    return withCORS(env, req, serverError("Failed to fetch outbox message"));
  }
}




// ====================== FILES (R2, SIGNED) ======================
/**
 * @openapi
 * /api/files/presign-upload:
 *   post:
 *     summary: Get pre-signed upload URL (tokened)
 *     tags: [Files]
 *     security:
 *       - bearerAuth: []
 * /api/files/upload:
 *   put:
 *     summary: Upload to R2 using pre-signed URL
 *     tags: [Files]
 * /api/files/presign-download:
 *   post:
 *     summary: Get pre-signed download URL (short-lived)
 *     tags: [Files]
 *     security:
 *       - bearerAuth: []
 * /api/files/download:
 *   get:
 *     summary: Download file by signed token
 *     tags: [Files]
 */
async function handleFilePresignUpload(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data || !data.content_type) return withCORS(env, req, badRequest("content_type required"));

  const filename = data.filename || "";
  const contentType = String(data.content_type);

  let ext = "";
  if (filename && filename.includes(".")) {
    ext = filename.substring(filename.lastIndexOf("."));
  } else {
    const ctMap = {
      "image/png": ".png",
      "image/jpeg": ".jpg",
      "image/gif": ".gif",
      "application/pdf": ".pdf",
      "text/plain": ".txt",
      "text/csv": ".csv",
      "application/vnd.ms-excel": ".xls",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx"
    };
    if (contentType in ctMap) ext = ctMap[contentType];
  }

  const dateTag = new Date().toISOString().slice(0,10).replace(/-/g,"");
  const randBytes = crypto.getRandomValues(new Uint8Array(8));
  let randHex = "";
  randBytes.forEach(b => randHex += b.toString(16).padStart(2, "0"));
  const fileKey = `/files/${dateTag}/file_${randHex}${ext || ""}`;

  const expiresSec = Math.min(parseInt(env.PRESIGN_EXPIRES_SECONDS || "600", 10), 900);
  const exp = Math.floor(Date.now() / 1000) + expiresSec;
  const token = await createToken(env.UPLOAD_TOKEN_SECRET, { typ: "file_upload", key: fileKey, exp });

  const baseUrl = new URL(req.url);
  baseUrl.pathname = "/api/files/upload";
  baseUrl.search = "";
  baseUrl.searchParams.set("key", fileKey);
  baseUrl.searchParams.set("token", token);

  return withCORS(env, req, ok({
    key: fileKey,
    upload_url: baseUrl.toString(),
    token,
    expires_at: new Date(exp * 1000).toISOString(),
    max_bytes: parseInt(env.FILE_MAX_BYTES || "5000000", 10),
    content_type: contentType
  }));
}

async function handleFileUpload(env, req, url) {
  const key = url.searchParams.get("key") || "";
  const token = url.searchParams.get("token") || "";
  const ver = await verifyToken(env.UPLOAD_TOKEN_SECRET, token);
  if (!ver.ok) return withCORS(env, req, unauthorized("Invalid token"));
  const p = ver.payload;
  if (p.typ !== "file_upload" || p.key !== key) {
    return withCORS(env, req, unauthorized("Token mismatch"));
  }
  const keyOk = /^\/files\/\d{8}\/file_[0-9a-f]{16}(\.[A-Za-z0-9]{3,10})?$/.test(key);
  if (!keyOk) return withCORS(env, req, badRequest("Invalid key"));

  const ct = req.headers.get("content-type") || "";
  const allowedTypes = /^(image\/|application\/pdf|text\/|application\/vnd\.openxmlformats|application\/vnd\.ms-excel)/i;
  if (!allowedTypes.test(ct)) return withCORS(env, req, unsupported("File type not allowed"));

  const maxBytes = parseInt(env.FILE_MAX_BYTES || "5000000", 10);
  const contentLength = parseInt(req.headers.get("content-length") || "0", 10);
  if (contentLength > maxBytes) return withCORS(env, req, tooLarge(`Max ${maxBytes} bytes`));

  const putRes = await r2Put(env, key, req.body, { httpMetadata: { contentType: ct }, customMetadata: {} });
  const size = contentLength || undefined;
  return withCORS(env, req, ok({ ok: true, key, etag: putRes?.etag, size }));
}

async function handleFilePresignDownload(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data || !data.key) return withCORS(env, req, badRequest("key is required"));

  const key = String(data.key);
  const head = await r2Head(env, key);
  if (!head) return withCORS(env, req, notFound("File not found"));

  const exp = Math.floor(Date.now()/1000) + 300;
  const token = await createToken(env.UPLOAD_TOKEN_SECRET, { typ: "file_dl", key, exp });
  const baseUrl = new URL(req.url);
  baseUrl.pathname = "/api/files/download";
  baseUrl.search = "";
  baseUrl.searchParams.set("key", key);
  baseUrl.searchParams.set("token", token);
  return withCORS(env, req, ok({ download_url: baseUrl.toString(), expires_at: new Date(exp * 1000).toISOString() }));
}

async function handleFileDownload(env, req, url) {
  const key = url.searchParams.get("key") || "";
  const token = url.searchParams.get("token") || "";
  const ver = await verifyToken(env.UPLOAD_TOKEN_SECRET, token);
  if (!ver.ok) return unauthorized("Invalid token");
  const p = ver.payload;
  if (p.typ !== "file_dl" || p.key !== key) return unauthorized("Token mismatch");

  const obj = await r2Get(env, key);
  if (!obj) return notFound("Not found");

  let contentType = "application/octet-stream";
  if (obj.httpMetadata?.contentType) contentType = obj.httpMetadata.contentType;

  const headers = new Headers({
    "content-type": contentType,
    "cache-control": "private, max-age=300"
  });
  const dispName = obj.customMetadata?.originalName || key.split("/").pop();
  headers.set("Content-Disposition", `attachment; filename="${dispName}"`);

  return new Response(obj.body, { status: 200, headers });
}



// ---------------------- Health ----------------------
async function handleHealth(env) {
  try { await env.R2.head("__healthcheck__" + Date.now()); } catch {}
  return new Response("ok", { status: 200, headers: TEXT_PLAIN });
}
async function handleReady(env) {
  const missing = [];
  for (const k of ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"]) if (!env[k]) missing.push(k);
  if (!env.R2) missing.push("R2");
  for (const k of ["SESSION_TOKEN_SECRET","UPLOAD_TOKEN_SECRET"]) if (!env[k]) missing.push(k);
  if (missing.length) return new Response("missing: " + missing.join(","), { status: 503, headers: TEXT_PLAIN });
  return new Response("ready", { status: 200, headers: TEXT_PLAIN });
}
function handleVersion() {
  return new Response(JSON.stringify({ version: "1.2.0", built_at: new Date().toISOString() }), { status: 200, headers: JSON_HEADERS });
}

/*
  CloudTMS – Timesheet Financials: worker + API additions (drop‑in)
  -----------------------------------------------------------------
  Purpose
    - Drain `ts_financials_outbox` (via Supabase RPC) and compute/store `timesheets_financials` snapshots.
    - Provide minimal API endpoints to:
        • Manually drain the queue once (/api/tsfin/queue/drain)
        • Recompute a timesheet (/api/tsfin/recompute)
        • Read current snapshot(s) (/api/tsfin/financials)
        • Promote READY_FOR_HR → READY_FOR_INVOICE (/api/tsfin/mark-ready)
        • Invoice creation & locking based on financial snapshots (replacement for old invoices)
        • Credit-note flow that unlocks and marks snapshots stale
    - All business math lives here; DB remains source-of-truth for raw timesheets and the outbox.

  Integration notes
    - Keep your existing Worker file intact. Import (or paste) these functions and wire routes.
    - This code uses the same helper style as your Worker: sbFetch/sbHeaders/withCORS/ok/badRequest/unauthorized.
    - Where your base file already defines helpers with the same names, delete the duplicates below.

  Router wiring (example)
    // Queue/worker
    if (req.method === 'POST' && p === '/tsfin/queue/drain')     return withCORS(env, req, await handleTsfinDrain(env, req));
    if (req.method === 'POST' && p === '/tsfin/recompute')       return withCORS(env, req, await handleTsfinRecompute(env, req));
    if (req.method === 'GET'  && p === '/tsfin/financials')      return withCORS(env, req, await handleTsfinFinancials(env, req));
    if (req.method === 'POST' && p === '/tsfin/mark-ready')      return withCORS(env, req, await handleTsfinMarkReady(env, req));

    // Invoices (replace your old handlers if present)
    if (req.method === 'GET'  && p === '/invoices')              return withCORS(env, req, await handleListInvoices(env, req));
    if (req.method === 'POST' && p === '/invoices')              return withCORS(env, req, await handleCreateInvoiceTsfin(env, req));
    if (req.method === 'GET'  && p.startsWith('/invoices/'))     return withCORS(env, req, await handleGetInvoice(env, req, p.split('/')[2]));
    if (req.method === 'POST' && p.endsWith('/credit-note'))     return withCORS(env, req, await handleCreateCreditNoteTsfin(env, req, p.split('/')[2]));

    // Finance preview (replace your old preview if desired to use snapshots)
    if (req.method === 'POST' && p === '/timesheets/finance-preview') return withCORS(env, req, await handleFinancePreviewTsfin(env, req));
*/

// ---------------------------
// Shared type mirrors
// ---------------------------

type TsFinReason = 'NEW_AUTHORISED'|'VERSION_ROTATED'|'REVOKED'|'RATE_CHANGED'|'POLICY_CHANGED'|'CONTEXT_CHANGED'|'MANUAL';

type CandidateAssignment = 'UNASSIGNED'|'ASSIGNED';

type ProcessingStatus = 'UNASSIGNED'|'CLIENT_UNRESOLVED'|'RATE_MISSING'|'PAY_CHANNEL_MISSING'|'READY_FOR_HR'|'READY_FOR_INVOICE';

type PayMethod = 'PAYE'|'UMBRELLA';

// ---------------------------
// Minimal helpers (reuse your base ones if present)
// ---------------------------

function pick<T>(obj: any, keys: string[], defaults: Partial<Record<string, any>> = {}): T {
  const out: any = { ...defaults };
  for (const k of keys) out[k] = obj?.[k] ?? defaults[k];
  return out as T;
}

function asNumber(x: any, d = 0): number {
  if (x === null || x === undefined) return d;
  const n = typeof x === 'number' ? x : Number(x);
  return Number.isFinite(n) ? n : d;
}

function round2(n: number) { return Math.round(n * 100) / 100; }

function splitCsv(s: string | null | undefined): string[] {
  return String(s || '').split(',').map((x) => x.trim()).filter(Boolean);
}

function ymd(iso: string): string {
  const d = new Date(iso);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const da = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${da}`;
}

// --- Basic UK DST handling (same style as your base file) ---
function isBSTLocal(ymdStr: string) {
  const [y, m, d] = ymdStr.split('-').map(Number);
  const lastSunday = (year: number, month: number) => {
    const dt = new Date(Date.UTC(year, month, 0));
    const dow = dt.getUTCDay();
    dt.setUTCDate(dt.getUTCDate() - dow);
    return dt.getUTCDate();
  };
  const lastSunMar = lastSunday(y, 3);
  const lastSunOct = lastSunday(y, 10);
  const n = Date.UTC(y, m - 1, d);
  const bstStart = Date.UTC(y, 2, lastSunMar, 1);
  const bstEnd = Date.UTC(y, 9, lastSunOct, 1);
  return n >= bstStart && n < bstEnd;
}

function toLocalParts(iso: string, tz: string): { ymd: string; hh: number; mm: number } {
  // For Europe/London only; treat other tz as UTC fallback.
  const inYmd = ymd(iso);
  const offset = (tz === 'Europe/London' && isBSTLocal(inYmd)) ? 1 : 0; // hours ahead of UTC
  const d = new Date(iso);
  let hh = d.getUTCHours() + offset;
  let mm = d.getUTCMinutes();
  let y = d.getUTCFullYear();
  let m = d.getUTCMonth() + 1;
  let da = d.getUTCDate();
  if (hh >= 24) { hh -= 24; const dt = new Date(Date.UTC(y, m - 1, da)); dt.setUTCDate(dt.getUTCDate() + 1); y = dt.getUTCFullYear(); m = dt.getUTCMonth() + 1; da = dt.getUTCDate(); }
  const ymdStr = `${y}-${String(m).padStart(2, '0')}-${String(da).padStart(2, '0')}`;
  return { ymd: ymdStr, hh, mm };
}

function minutesBetween(isoA: string, isoB: string) {
  return Math.round((new Date(isoB).getTime() - new Date(isoA).getTime()) / 60000);
}

// ---------------------------
// Supabase helpers (RPC + REST)
// ---------------------------

function sbHeaders(env: any) {
  const key = env.SUPABASE_SERVICE_ROLE_KEY;
  return { apikey: key, Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' };
}

async function sbFetch(env: any, url: string, includeCount = false) {
  const res = await fetch(url, { headers: { ...sbHeaders(env), ...(includeCount ? { Prefer: 'count=exact' } : {}) } });
  const text = await res.text();
  let json: any = [];
  try { json = text ? JSON.parse(text) : []; } catch { json = []; }
  const countHeader = res.headers.get('content-range');
  const total = countHeader && /\/(\d+)$/.exec(countHeader) ? parseInt(/\/(\d+)$/.exec(countHeader)![1], 10) : undefined;
  if (!res.ok) throw new Error(`Supabase fetch failed ${res.status}: ${text}`);
  return { rows: json, total };
}

async function sbRpc(env: any, fn: string, args: any) {
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${encodeURIComponent(fn)}`;
  const res = await fetch(url, { method: 'POST', headers: sbHeaders(env), body: JSON.stringify(args || {}) });
  const txt = await res.text();
  let json: any;
  try { json = txt ? JSON.parse(txt) : null; } catch { json = null; }
  if (!res.ok) throw new Error(`RPC ${fn} failed ${res.status}: ${txt}`);
  return json;
}

// ---------------------------
// Context loaders
// ---------------------------

async function loadCurrentTimesheet(env: any, timesheet_id: string) {
  const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/timesheets?timesheet_id=eq.${encodeURIComponent(timesheet_id)}&is_current=eq.true&select=*`);
  return rows[0] || null;
}

async function loadCandidate(env: any, key_norm: string | null) {
  if (!key_norm) return null;
  const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/candidates?key_norm=eq.${encodeURIComponent(key_norm)}&active=eq.true&select=*`);
  return rows[0] || null;
}

async function resolveClientId(env: any, hospital_norm: string | null) {
  if (!hospital_norm) return null;
  const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/client_hospitals?hospital_name_norm=eq.${encodeURIComponent(hospital_norm)}&select=client_id&limit=1`);
  return rows[0]?.client_id || null;
}

async function loadPolicy(env: any, client_id: string | null, workedDateYmd: string | null) {
  const { rows: defRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=*`);
  const def = defRows[0] || {};

  let cs: any = null;
  if (client_id) {
    const w = workedDateYmd ? `&and=(or(effective_from.lte.${encodeURIComponent(workedDateYmd)},effective_from.is.null))` : '';
    const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/client_settings?client_id=eq.${encodeURIComponent(client_id)}&select=*&order=effective_from.desc,nullsLast=true&limit=1${w}`);
    cs = rows[0] || null;
  }

  const tz = cs?.timezone_id || def?.timezone_id || 'Europe/London';
  const bh = (cs?.bh_list && Array.isArray(cs.bh_list)) ? cs.bh_list : (def?.bh_list || []);
  return {
    timezone_id: tz,
    day_start: cs?.day_start || def?.day_start || '06:00:00',
    day_end: cs?.day_end || def?.day_end || '20:00:00',
    night_start: cs?.night_start || def?.night_start || '20:00:00',
    night_end: cs?.night_end || def?.night_end || '06:00:00',
    vat_rate_pct: asNumber(cs?.vat_rate_pct ?? def?.vat_rate_pct ?? 20),
    holiday_pay_pct: asNumber(cs?.holiday_pay_pct ?? def?.holiday_pay_pct ?? 12.07),
    erni_pct: asNumber(cs?.erni_pct ?? def?.erni_pct ?? 13.8),
    apply_holiday_to: cs?.apply_holiday_to || def?.apply_holiday_to || 'PAYE_ONLY',
    apply_erni_to: cs?.apply_erni_to || def?.apply_erni_to || 'PAYE_ONLY',
    margin_includes: { expenses: !!(cs?.margin_includes?.expenses ?? def?.margin_includes?.expenses) },
    bh_list: Array.isArray(bh) ? bh : [],
  };
}

// ---------------------------
// Classification (minutes → five buckets) with precedence BH > Sun > Sat > Night > Day
// ---------------------------

function hhmmToMin(hhmm: string) {
  const [h, m] = (hhmm || '00:00:00').split(':').map((x) => parseInt(x, 10) || 0);
  return h * 60 + m;
}

function subtractBreak(segments: [string, string][], breakStartIso: string | null, breakEndIso: string | null, breakMin: number | null) {
  if (breakStartIso && breakEndIso) {
    // Clip each segment against [breakStart, breakEnd]
    const bs = new Date(breakStartIso).getTime();
    const be = new Date(breakEndIso).getTime();
    const out: [string, string][] = [];
    for (const [a, b] of segments) {
      const A = new Date(a).getTime();
      const B = new Date(b).getTime();
      if (B <= bs || A >= be) { out.push([a, b]); continue; }
      if (A < bs) out.push([a, new Date(bs).toISOString()]);
      if (B > be) out.push([new Date(be).toISOString(), b]);
    }
    return out;
  }
  if (!breakMin || breakMin <= 0) return segments;
  // Remove breakMin from the middle of the (single) segment (fallback)
  if (!segments.length) return segments;
  let [a, b] = segments[0];
  const total = minutesBetween(a, b);
  if (breakMin >= total) return [];
  const startCut = Math.floor((total - breakMin) / 2);
  const mid = new Date(new Date(a).getTime() + startCut * 60000).toISOString();
  const midEnd = new Date(new Date(mid).getTime() + breakMin * 60000).toISOString();
  return [[a, mid], [midEnd, b]];
}

function classifyMinutes(env: any, policy: any, segments: [string, string][]) {
  const out = { day: 0, night: 0, sat: 0, sun: 0, bh: 0 };
  const tz = policy.timezone_id || 'Europe/London';
  const dayStartMin = hhmmToMin(policy.day_start);
  const dayEndMin = hhmmToMin(policy.day_end);
  const bhSet = new Set<string>(policy.bh_list || []);

  for (const [isoA, isoB] of segments) {
    // Iterate by local-day slices to apply precedence per day
    let cur = new Date(isoA);
    const end = new Date(isoB);

    while (cur < end) {
      const { ymd: curYmd, hh, mm } = toLocalParts(cur.toISOString(), tz);
      const offsetMin = (tz === 'Europe/London' && isBSTLocal(curYmd)) ? 60 : 0; // informational only

      // End of this local day (midnight local)
      const dayEnd = new Date(Date.UTC(cur.getUTCFullYear(), cur.getUTCMonth(), cur.getUTCDate() + 1));
      const sliceEnd = end < dayEnd ? end : dayEnd;
      const mins = minutesBetween(cur.toISOString(), sliceEnd.toISOString());

      const dow = new Date(`${curYmd}T00:00:00Z`).getUTCDay(); // 0=Sun..6=Sat (using UTC midnight aligns with our ymd)
      const curIsBh = bhSet.has(curYmd);

      // Precedence: BH wins, then Sun/Sat, else split day vs night by window
      if (curIsBh) {
        out.bh += mins;
      } else if (dow === 0) {
        out.sun += mins;
      } else if (dow === 6) {
        out.sat += mins;
      } else {
        // Weekday: split by day window
        // Compute local minute-of-day for slice boundaries (approx: use start minute; assume slice within same day)
        const startLocalMin = hh * 60 + mm;
        const endLocalMin = startLocalMin + mins;
        // Portion in [dayStart, dayEnd]
        const dayOverlap = Math.max(0, Math.min(endLocalMin, dayEndMin) - Math.max(startLocalMin, dayStartMin));
        const nightOverlap = mins - dayOverlap;
        out.day += dayOverlap;
        out.night += nightOverlap;
      }

      cur = sliceEnd;
    }
  }

  // Round to hours
  return {
    hours_day: round2(out.day / 60),
    hours_night: round2(out.night / 60),
    hours_sat: round2(out.sat / 60),
    hours_sun: round2(out.sun / 60),
    hours_bh: round2(out.bh / 60),
  };
}

// ---------------------------
// Rates resolution (candidate override -> client default). Most specific + latest.
// ---------------------------

async function resolveRates(env: any, { candidate_id, client_id, role, band, dateYmd }: any) {
  // 1) Candidate override (allow NULL-or-specific per dimension)
  if (candidate_id) {
    let q1 = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?select=*` +
             `&candidate_id=eq.${encodeURIComponent(candidate_id)}` +
             `&order=client_id.nullslast,role.nullslast,band.nullslast,date_from.desc&limit=1`;

    const andParts: string[] = [];
    if (client_id) andParts.push(`or(client_id.eq.${encodeURIComponent(client_id)},client_id.is.null)`);
    if (role) andParts.push(`or(role.eq.${encodeURIComponent(role)},role.is.null)`);
    if (band) andParts.push(`or(band.eq.${encodeURIComponent(band)},band.is.null)`);
    if (dateYmd) { andParts.push(`date_from=lte.${encodeURIComponent(dateYmd)}`); andParts.push(`or(date_to.gte.${encodeURIComponent(dateYmd)},date_to.is.null)`); }
    if (andParts.length) q1 += `&and=(${andParts.join(',')})`;

    const { rows: cand } = await sbFetch(env, q1);
    if (cand[0]) {
      const r = cand[0];
      return {
        source: { kind: 'CANDIDATE_OVERRIDE', id: r.id },
        pay: { day: r.pay_day, night: r.pay_night, sat: r.pay_sat, sun: r.pay_sun, bh: r.pay_bh },
        charge: null,
      };
    }
  }

  // 2) Client default
  if (client_id) {
    let q2 = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?select=*` +
             `&client_id=eq.${encodeURIComponent(client_id)}` +
             `&order=role.nullslast,band.nullslast,date_from.desc&limit=1`;

    const and2: string[] = [];
    if (role) and2.push(`or(role.eq.${encodeURIComponent(role)},role.is.null)`);
    if (band) and2.push(`or(band.eq.${encodeURIComponent(band)},band.is.null)`);
    if (dateYmd) { and2.push(`date_from=lte.${encodeURIComponent(dateYmd)}`); and2.push(`or(date_to.gte.${encodeURIComponent(dateYmd)},date_to.is.null)`); }
    if (and2.length) q2 += `&and=(${and2.join(',')})`;

    const { rows: def } = await sbFetch(env, q2);
    if (def[0]) {
      const r = def[0];
      return {
        source: { kind: 'CLIENT_DEFAULT', id: r.id },
        pay: r.pay_day != null ? { day: r.pay_day, night: r.pay_night, sat: r.pay_sat, sun: r.pay_sun, bh: r.pay_bh } : null,
        charge: { day: r.charge_day, night: r.charge_night, sat: r.charge_sat, sun: r.charge_sun, bh: r.charge_bh },
      };
    }
  }

  return { source: { kind: 'NONE', id: null }, pay: null, charge: null };
}

function anyMissingRates(hours: any, pay: any, charge: any) {
  // Require a charge rate for any non-zero hours. Pay may fall back to 0 only if explicitly allowed; we mark missing for safety.
  const buckets: (keyof typeof hours)[] = ['day', 'night', 'sat', 'sun', 'bh'] as any;
  for (const b of buckets) {
    if (hours[b] > 0) {
      if (!charge || charge[b] == null) return true;
      if (!pay || pay[b] == null) return true;
    }
  }
  return false;
}

// ---------------------------
// Snapshot writer (guards invoice locks, flips current → false, inserts new)
// ---------------------------

async function writeSnapshot(env: any, snapshot: any) {
  // Guard with tsfin_prepare_write; throws if locked
  await sbRpc(env, 'tsfin_prepare_write', { timesheet_id: snapshot.timesheet_id });

  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/timesheets_financials`, {
    method: 'POST',
    headers: { ...sbHeaders(env), Prefer: 'return=representation' },
    body: JSON.stringify(snapshot)
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`timesheets_financials insert failed: ${txt}`);
  }
  const json = await res.json().catch(() => ([]));
  return Array.isArray(json) ? json[0] : json;
}
// ---------- helpers used by patch handlers ----------
const toNum = (v: any) => (v === null || v === undefined ? null : Number(v));
const nonneg = (n: any) => (n === null || n === undefined ? true : Number(n) >= 0);
const nowIso = () => new Date().toISOString();

async function fetchCurrentTsfin(env: any, timesheetId: string) {
  const q =
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?timesheet_id=eq.${encodeURIComponent(timesheetId)}` +
    `&is_current=eq.true` +
    `&select=` + [
      'timesheet_id','timesheet_version','candidate_id','client_id',
      'processing_status','locked_by_invoice_id','is_current',
      'expenses_pay_ex_vat','expenses_charge_ex_vat','expenses_description','expenses_evidence_r2_key',
      'mileage_pay_ex_vat','mileage_charge_ex_vat','mileage_evidence_r2_key','mileage_pay_rate','mileage_charge_rate',
      'po_number'
    ].join(',');
  const { rows } = await sbFetch(env, q);
  return (rows || [])[0] || null;
}

async function enqueueManualTsfinRecalc(env: any, timesheetId: string) {
  const id = (globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`).toString();
  const body = [{
    id,
    timesheet_id: timesheetId,
    reason: 'MANUAL',
    attempt_count: 0,
    next_attempt_at: nowIso(),
    last_error: null,
    created_at: nowIso(),
  }];
  await fetch(
    `${env.SUPABASE_URL}/rest/v1/ts_financials_outbox?on_conflict=timesheet_id,reason`,
    {
      method: 'POST',
      headers: { ...sbHeaders(env), 'Prefer': 'resolution=merge-duplicates' },
      body: JSON.stringify(body),
    }
  );
}

async function insertAuditEvent(env: any, req: Request, args: {
  object_type: string,
  object_id_text: string | null,
  action: string,
  before_json: any,
  after_json: any,
  reason?: string | null
}) {
  // Best-effort actor/IP extraction; table should accept nulls.
  const user = await requireUser(env, req, ['admin']).catch(() => null);
  const ip = req.headers.get('CF-Connecting-IP') || req.headers.get('x-forwarded-for') || null;
  const ua = req.headers.get('user-agent') || null;
  const correlation_id = (globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`).toString();

  const payload = [{
    actor_user_id: user?.id ?? user?.user_id ?? null,
    actor_display: user?.email ?? null,
    actor_role_at_time: user?.role ?? null,
    object_type: args.object_type,
    object_id_text: args.object_id_text,
    action: args.action,
    before_json: args.before_json ?? null,
    after_json: args.after_json ?? null,
    reason: args.reason ?? null,
    ip,
    user_agent: ua,
    correlation_id,
  }];

  await fetch(`${env.SUPABASE_URL}/rest/v1/audit_events`, {
    method: 'POST',
    headers: { ...sbHeaders(env), 'Prefer': 'return=minimal' },
    body: JSON.stringify(payload),
  });
}

/**
 * Shared patcher. Accepts a partial payload targeting expenses/mileage/po fields.
 * Enforces "unlocked", evidence rules, sets is_stale, enqueues recompute, and audits.
 */
async function patchTsfinCommon(
  env: any,
  req: Request,
  timesheetId: string,
  patch: {
    reason?: string | null,
    expenses?: {
      pay_ex_vat?: number | null,
      charge_ex_vat?: number | null,
      description?: string | null,
      evidence_r2_key?: string | null,
    },
    mileage?: {
      pay_ex_vat?: number | null,
      charge_ex_vat?: number | null,
      evidence_r2_key?: string | null,
      pay_rate?: number | null,
      charge_rate?: number | null,
    },
    po?: { number?: string | null }
  }
) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  // Validate basic numeric constraints up-front
  if (patch.expenses) {
    const xp = patch.expenses;
    if (!nonneg(xp.pay_ex_vat) || !nonneg(xp.charge_ex_vat)) {
      return badRequest('Expense values must be >= 0');
    }
  }
  if (patch.mileage) {
    const ml = patch.mileage;
    if (!nonneg(ml.pay_ex_vat) || !nonneg(ml.charge_ex_vat) || !nonneg(ml.pay_rate) || !nonneg(ml.charge_rate)) {
      return badRequest('Mileage values must be >= 0');
    }
  }

  // Load current snapshot (and guard invoice lock)
  const before = await fetchCurrentTsfin(env, timesheetId);
  if (!before) return notFound('TSFIN current row not found');
  if (before.locked_by_invoice_id) return conflict('Timesheet financials are locked by an invoice');

  // Evidence rules (mirror DB CHECKs)
  if (patch.expenses && patch.expenses.charge_ex_vat != null) {
    const ch = Number(patch.expenses.charge_ex_vat);
    if (ch > 0 && !patch.expenses.evidence_r2_key && !before.expenses_evidence_r2_key) {
      return unprocessable('Expenses charge requires evidence_r2_key');
    }
  }
  if (patch.mileage && patch.mileage.charge_ex_vat != null) {
    const ch = Number(patch.mileage.charge_ex_vat);
    if (ch > 0 && !patch.mileage.evidence_r2_key && !before.mileage_evidence_r2_key) {
      return unprocessable('Mileage charge requires evidence_r2_key');
    }
  }

  // Prepare final payload for PostgREST PATCH.
  const upd: any = {};

  if (patch.expenses) {
    const xp = patch.expenses;
    if (xp.pay_ex_vat !== undefined)      upd.expenses_pay_ex_vat = toNum(xp.pay_ex_vat) ?? 0;
    if (xp.charge_ex_vat !== undefined)   upd.expenses_charge_ex_vat = toNum(xp.charge_ex_vat) ?? 0;
    if (xp.description !== undefined)     upd.expenses_description = xp.description ?? null;
    if (xp.evidence_r2_key !== undefined) upd.expenses_evidence_r2_key = xp.evidence_r2_key ?? null;
  }

  // Mileage, including default rate fallback behavior:
  if (patch.mileage) {
    const ml = patch.mileage;
    if (ml.pay_ex_vat !== undefined)      upd.mileage_pay_ex_vat = toNum(ml.pay_ex_vat) ?? 0;
    if (ml.charge_ex_vat !== undefined)   upd.mileage_charge_ex_vat = toNum(ml.charge_ex_vat) ?? 0;
    if (ml.evidence_r2_key !== undefined) upd.mileage_evidence_r2_key = ml.evidence_r2_key ?? null;

    // Explicit rate updates
    if (ml.pay_rate !== undefined)        upd.mileage_pay_rate = ml.pay_rate === null ? null : toNum(ml.pay_rate);
    if (ml.charge_rate !== undefined)     upd.mileage_charge_rate = ml.charge_rate === null ? null : toNum(ml.charge_rate);

    // If either rate was omitted (undefined), mirror the Node behavior:
    // set to existing value or fallback defaults (candidate/client).
    if (ml.pay_rate === undefined || ml.charge_rate === undefined) {
      // Pull defaults only if needed
      const needPay = ml.pay_rate === undefined;
      const needChg = ml.charge_rate === undefined;

      if (needPay || needChg) {
        // get candidate/client default rates
        let candRate: number | null = null;
        let clientRate: number | null = null;
        try {
          if (needPay && before.candidate_id) {
            const { rows: candRows } = await sbFetch(
              env,
              `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(before.candidate_id)}&select=mileage_pay_rate&limit=1`
            );
            candRate = candRows?.[0]?.mileage_pay_rate ?? null;
          }
          if (needChg && before.client_id) {
            const { rows: cliRows } = await sbFetch(
              env,
              `${env.SUPABASE_URL}/rest/v1/clients?id=eq.${encodeURIComponent(before.client_id)}&select=mileage_charge_rate&limit=1`
            );
            clientRate = cliRows?.[0]?.mileage_charge_rate ?? null;
          }
        } catch { /* ignore */ }

        if (needPay) {
          upd.mileage_pay_rate = toNum(before.mileage_pay_rate) ?? (candRate == null ? null : Number(candRate));
        }
        if (needChg) {
          upd.mileage_charge_rate = toNum(before.mileage_charge_rate) ?? (clientRate == null ? null : Number(clientRate));
        }
      }
    }
  }

  if (patch.po) {
    if (patch.po.number !== undefined) upd.po_number = patch.po.number ?? null;
  }

  // If no actual fields to change, return unchanged (mirror Node behavior).
  const hasFieldChange = Object.keys(upd).length > 0;
  if (!hasFieldChange) {
    return ok({ updated: false, tsfin: before });
  }

  // Always mark stale & bump updated_at
  upd.is_stale = true;
  upd.updated_at = nowIso();

  // Apply update: limit to current & unlocked
  const url =
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?timesheet_id=eq.${encodeURIComponent(timesheetId)}` +
    `&is_current=eq.true` +
    `&locked_by_invoice_id=is.null`;

  const res = await fetch(url, {
    method: 'PATCH',
    headers: { ...sbHeaders(env), 'Prefer': 'return=representation' },
    body: JSON.stringify(upd),
  });

  if (!res.ok) {
    const t = await res.text();
    return serverError(`TSFIN update failed: ${t}`);
  }
  const json = await res.json().catch(() => []);
  const after = Array.isArray(json) ? json[0] : json;

  // Enqueue manual recompute + audit
  await enqueueManualTsfinRecalc(env, timesheetId).catch(() => {});
  await insertAuditEvent(env, req, {
    object_type: 'timesheets_financials',
    object_id_text: timesheetId,
    action: 'PATCH',
    reason: patch.reason ?? null,
    before_json: {
      expenses: {
        pay_ex_vat: before.expenses_pay_ex_vat,
        charge_ex_vat: before.expenses_charge_ex_vat,
        description: before.expenses_description,
        evidence_r2_key: before.expenses_evidence_r2_key,
      },
      mileage: {
        pay_ex_vat: before.mileage_pay_ex_vat,
        charge_ex_vat: before.mileage_charge_ex_vat,
        evidence_r2_key: before.mileage_evidence_r2_key,
        pay_rate: before.mileage_pay_rate,
        charge_rate: before.mileage_charge_rate,
      },
      po: { number: before.po_number },
    },
    after_json: {
      expenses: {
        pay_ex_vat: after?.expenses_pay_ex_vat,
        charge_ex_vat: after?.expenses_charge_ex_vat,
        description: after?.expenses_description,
        evidence_r2_key: after?.expenses_evidence_r2_key,
      },
      mileage: {
        pay_ex_vat: after?.mileage_pay_ex_vat,
        charge_ex_vat: after?.mileage_charge_ex_vat,
        evidence_r2_key: after?.mileage_evidence_r2_key,
        pay_rate: after?.mileage_pay_rate,
        charge_rate: after?.mileage_charge_rate,
      },
      po: { number: after?.po_number },
    }
  }).catch(() => {});

  return ok({ updated: true, tsfin: after });
}

// -------------------- public handlers --------------------

// PATCH /api/tsfin/:timesheet_id/expenses
async function handleTsfinPatchExpenses(env: any, req: Request, timesheetId: string) {
  const body = await parseJSONBody(req).catch(() => ({}));
  return withCORS(env, req, await patchTsfinCommon(env, req, timesheetId, {
    reason: body?.reason ?? null,
    expenses: {
      pay_ex_vat: body?.pay_ex_vat,
      charge_ex_vat: body?.charge_ex_vat,
      description: body?.description,
      evidence_r2_key: body?.evidence_r2_key,
    }
  }));
}

// PATCH /api/tsfin/:timesheet_id/mileage
async function handleTsfinPatchMileage(env: any, req: Request, timesheetId: string) {
  const body = await parseJSONBody(req).catch(() => ({}));
  return withCORS(env, req, await patchTsfinCommon(env, req, timesheetId, {
    reason: body?.reason ?? null,
    mileage: {
      pay_ex_vat: body?.pay_ex_vat,
      charge_ex_vat: body?.charge_ex_vat,
      evidence_r2_key: body?.evidence_r2_key,
      pay_rate: body?.pay_rate,          // undefined ⇒ fallback behavior, null ⇒ explicit null
      charge_rate: body?.charge_rate,
    }
  }));
}

// PATCH /api/tsfin/:timesheet_id/po
async function handleTsfinPatchPO(env: any, req: Request, timesheetId: string) {
  const body = await parseJSONBody(req).catch(() => ({}));
  return withCORS(env, req, await patchTsfinCommon(env, req, timesheetId, {
    reason: body?.reason ?? null,
    po: { number: body?.number }
  }));
}

// ---------------------------
// Worker: dequeue → compute → store → ack
// ---------------------------
// Helper: resolve effective pay channel (pure, no I/O)
function resolveEffectivePayChannel(input: {
  pay_method?: string | null,
  candidate?: {
    account_holder?: string | null,
    bank_name?: string | null,
    sort_code?: string | null,
    account_number?: string | null,
    umbrella_id?: string | null
  },
  umbrella?: {
    name?: string | null,
    bank_name?: string | null,
    sort_code?: string | null,
    account_number?: string | null
  }
}) {
  const pm = (input.pay_method || '').toUpperCase();
  const cand = input.candidate || {};
  const umb = input.umbrella || {};

  const trim = (v?: string | null) => (v ?? '').toString().trim() || null;

  if (pm === 'PAYE') {
    const out = {
      pay_method: 'PAYE',
      source: 'CANDIDATE' as const,
      account_holder: trim(cand.account_holder),
      bank_name: trim(cand.bank_name),
      sort_code: trim(cand.sort_code),
      account_number: trim(cand.account_number)
    };
    const missing: string[] = [];
    if (!out.sort_code) missing.push('sort_code');
    if (!out.account_number) missing.push('account_number');
    return { ...out, ok: missing.length === 0, missing };
  }

  if (pm === 'UMBRELLA') {
    const out = {
      pay_method: 'UMBRELLA',
      source: 'UMBRELLA' as const,
      account_holder: trim(umb.name) || null,
      bank_name: trim(umb.bank_name),
      sort_code: trim(umb.sort_code),
      account_number: trim(umb.account_number)
    };
    const missing: string[] = [];
    if (!trim(cand.umbrella_id)) missing.push('umbrella_id');
    if (!out.sort_code) missing.push('sort_code');
    if (!out.account_number) missing.push('account_number');
    return { ...out, ok: missing.length === 0, missing };
  }

  return {
    pay_method: pm || null,
    source: 'MISSING' as const,
    account_holder: null,
    bank_name: null,
    sort_code: null,
    account_number: null,
    ok: false,
    missing: ['pay_method']
  };
}

async function runTsfinWorkerOnce(env: any, { limit = 50 } = {}) {
  const lease: { id: string; timesheet_id: string; reason: TsFinReason }[] = await sbRpc(env, 'tsfin_dequeue_batch', { limit });
  if (!Array.isArray(lease) || !lease.length) return { picked: 0, ok: 0, fail: 0 };

  let ok = 0, fail = 0;
  for (const item of lease) {
    try {
      const ts = await loadCurrentTimesheet(env, item.timesheet_id);
      if (!ts) {
        await sbRpc(env, 'tsfin_mark_revoked', { timesheet_id: item.timesheet_id });
        await sbRpc(env, 'tsfin_work_success', { id: item.id });
        ok++; continue;
      }

      if (!ts.authorised_at_server) {
        await sbRpc(env, 'tsfin_work_success', { id: item.id });
        ok++; continue;
      }

      const occupantKey = ts.occupant_key_norm || null;
      const candidate = await loadCandidate(env, occupantKey);
      const candidate_assignment: CandidateAssignment = candidate ? 'ASSIGNED' : 'UNASSIGNED';

      const client_id = await resolveClientId(env, ts.hospital_norm || null);
      const policy = await loadPolicy(env, client_id, ts.worked_start_iso ? ymd(ts.worked_start_iso) : null);

      // Build working segments and subtract break
      let segments: [string, string][] = [];
      if (ts.worked_start_iso && ts.worked_end_iso) segments.push([ts.worked_start_iso, ts.worked_end_iso]);
      segments = subtractBreak(segments, ts.break_start_iso || null, ts.break_end_iso || null, ts.break_minutes || null);

      const hours = classifyMinutes(env, policy, segments);

      const workedDate = ts.worked_start_iso ? toLocalParts(ts.worked_start_iso, policy.timezone_id).ymd : null;
      const rates = await resolveRates(env, { candidate_id: candidate?.id || null, client_id, role: ts.job_title_norm || null, band: ts.band || null, dateYmd: workedDate });

      const missing = anyMissingRates({ day: hours.hours_day, night: hours.hours_night, sat: hours.hours_sat, sun: hours.hours_sun, bh: hours.hours_bh }, rates.pay, rates.charge);

      // Determine pay channel requirement
      const pay_method: PayMethod | null = (candidate?.pay_method === 'UMBRELLA') ? 'UMBRELLA' : (candidate?.pay_method === 'PAYE' ? 'PAYE' : (ts.pay_method || null));
      let processing_status: ProcessingStatus = 'READY_FOR_HR';
      if (!candidate) processing_status = 'UNASSIGNED';
      else if (!client_id) processing_status = 'CLIENT_UNRESOLVED';
      else if (missing) processing_status = 'RATE_MISSING';
      else if (pay_method === 'UMBRELLA' && !candidate?.umbrella_id) processing_status = 'PAY_CHANNEL_MISSING';
      else processing_status = 'READY_FOR_HR';

      // Compute totals (guard against nulls)
      const pay = rates.pay || { day: 0, night: 0, sat: 0, sun: 0, bh: 0 };
      const charge = rates.charge || { day: 0, night: 0, sat: 0, sun: 0, bh: 0 };
      const total_pay_ex_vat = round2(
        hours.hours_day * asNumber(pay.day) +
        hours.hours_night * asNumber(pay.night) +
        hours.hours_sat * asNumber(pay.sat) +
        hours.hours_sun * asNumber(pay.sun) +
        hours.hours_bh * asNumber(pay.bh)
      );
      const total_charge_ex_vat = round2(
        hours.hours_day * asNumber(charge.day) +
        hours.hours_night * asNumber(charge.night) +
        hours.hours_sat * asNumber(charge.sat) +
        hours.hours_sun * asNumber(charge.sun) +
        hours.hours_bh * asNumber(charge.bh)
      );
      const margin_ex_vat = round2(total_charge_ex_vat - total_pay_ex_vat);

      const snapshot = {
        // identity
        timesheet_id: item.timesheet_id,
        timesheet_version: ts.version || 1,
        basis: 'SELF_REPORTED',

        // inputs
        occupant_key_norm: ts.occupant_key_norm || null,
        worked_start_iso: ts.worked_start_iso || null,
        worked_end_iso: ts.worked_end_iso || null,
        break_start_iso: ts.break_start_iso || null,
        break_end_iso: ts.break_end_iso || null,
        break_minutes: ts.break_minutes || null,

        candidate_id: candidate?.id || null,
        client_id: client_id || null,
        role: ts.job_title_norm || null,
        band: ts.band || null,
        pay_method,

        policy_snapshot_json: policy,
        rate_source_refs_json: rates.source,

        // buckets
        hours_day: hours.hours_day,
        hours_night: hours.hours_night,
        hours_sat: hours.hours_sat,
        hours_sun: hours.hours_sun,
        hours_bh: hours.hours_bh,

        // rates
        pay_day: rates.pay?.day ?? null,
        pay_night: rates.pay?.night ?? null,
        pay_sat: rates.pay?.sat ?? null,
        pay_sun: rates.pay?.sun ?? null,
        pay_bh: rates.pay?.bh ?? null,
        charge_day: rates.charge?.day ?? null,
        charge_night: rates.charge?.night ?? null,
        charge_sat: rates.charge?.sat ?? null,
        charge_sun: rates.charge?.sun ?? null,
        charge_bh: rates.charge?.bh ?? null,

        // totals
        total_hours: round2(hours.hours_day + hours.hours_night + hours.hours_sat + hours.hours_sun + hours.hours_bh),
        total_pay_ex_vat,
        total_charge_ex_vat,
        margin_ex_vat,

        // gating
        candidate_assignment,
        processing_status,
      };

      await writeSnapshot(env, snapshot);
      await sbRpc(env, 'tsfin_work_success', { id: item.id });
      ok++;
    } catch (e: any) {
      await sbRpc(env, 'tsfin_work_fail', { id: item.id, error_text: String(e?.message || e) });
      fail++;
    }
  }

  return { picked: lease.length, ok, fail };
}

// ---------------------------
// API: Manual drain (admin)
// ---------------------------

async function handleTsfinDrain(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();
  const body = await parseJSONBody(req).catch(() => null);
  const limit = Math.min(Math.max(parseInt(body?.limit || '50', 10) || 50, 1), 500);
  const res = await runTsfinWorkerOnce(env, { limit });
  return ok(res);
}

// ---------------------------
// API: Recompute (enqueue MANUAL) – accepts timesheet_id(s)
// ---------------------------

async function handleTsfinRecompute(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();
  const body = await parseJSONBody(req).catch(() => null);
  const ids: string[] = Array.isArray(body?.timesheet_ids) ? body.timesheet_ids.slice(0, 200) : [];
  if (!ids.length) return badRequest('timesheet_ids array required');
  for (const tsid of ids) await sbRpc(env, 'enqueue_ts_financials', { timesheet_id: tsid, reason: 'MANUAL' });
  return ok({ enqueued: ids.length });
}

// ---------------------------
// API: Read current snapshots by timesheet_id or by client/date filters
// ---------------------------

// ----------------------------------------
// GET TSFIN (include exp/mileage/PO fields)
// ----------------------------------------
async function handleTsfinFinancials(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const sp = new URL(req.url).searchParams;
  const ids = splitCsv(sp.get('timesheet_ids'));

  let url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?is_current=eq.true`;
  const select = [
    '*',
    'expenses_pay_ex_vat','expenses_charge_ex_vat','expenses_description','expenses_evidence_r2_key',
    'mileage_pay_ex_vat','mileage_charge_ex_vat','mileage_evidence_r2_key','mileage_pay_rate','mileage_charge_rate',
    'po_number',
    'candidate_id',
    'pay_method'
  ];
  url += `&select=${select.join(',')}`;
  if (ids.length) url += `&timesheet_id=in.(${ids.map(encodeURIComponent).join(',')})`;

  const { rows } = await sbFetch(env, url, false);

  // Batch-resolve effective pay channel for each row
  const candIds = Array.from(new Set((rows || []).map((r: any) => r.candidate_id).filter(Boolean)));
  let candidatesById = new Map<string, any>();
  let umbrellasById = new Map<string, any>();

  if (candIds.length) {
    const candParam = candIds.map(encodeURIComponent).join(',');
    const { rows: candRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/candidates` +
      `?select=id,umbrella_id,account_holder,bank_name,sort_code,account_number` +
      `&id=in.(${candParam})`
    );
    candidatesById = new Map((candRows || []).map((c: any) => [c.id, c]));

    const umbIds = Array.from(new Set((candRows || [])
      .map((c: any) => c.umbrella_id)
      .filter(Boolean)));
    if (umbIds.length) {
      const umbParam = umbIds.map(encodeURIComponent).join(',');
      const { rows: umbRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/umbrellas` +
        `?select=id,name,bank_name,sort_code,account_number` +
        `&id=in.(${umbParam})`
      );
      umbrellasById = new Map((umbRows || []).map((u: any) => [u.id, u]));
    }
  }

  const items = (rows || []).map((r: any) => {
    const cand = candidatesById.get(r.candidate_id);
    const umb  = cand?.umbrella_id ? umbrellasById.get(cand.umbrella_id) : undefined;
    const effective_pay_channel = resolveEffectivePayChannel({
      pay_method: r.pay_method,
      candidate: cand,
      umbrella: umb
    });
    return { ...r, effective_pay_channel };
  });

  return ok({ items });
}

// ---------------------------
// API: Mark READY_FOR_HR → READY_FOR_INVOICE (after HR validation OK)
// ---------------------------

// REPLACE the whole handler with this
// ------------------------------------------------------
// MARK READY (validate evidence rules before promotion)
// ------------------------------------------------------
async function handleTsfinMarkReady(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized('Unauthorized');

  const payload = await parseJSONBody(req);
  if (!payload || !Array.isArray(payload.timesheet_ids) || payload.timesheet_ids.length === 0) {
    return badRequest("timesheet_ids[] required");
  }
  const ids = [...new Set(payload.timesheet_ids)].filter(Boolean);
  if (ids.length === 0) return badRequest("No valid timesheet_ids");

  const idsParam = ids.map(encodeURIComponent).join(',');

  // 1) Latest validation per TS
  const valUrl =
    `${env.SUPABASE_URL}/rest/v1/timesheet_validations` +
    `?select=timesheet_id,status,updated_at` +
    `&timesheet_id=in.(${idsParam})` +
    `&order=timesheet_id.asc,updated_at.desc` +
    `&limit=10000`;

  const { rows: allVals } = await sbFetch(env, valUrl);
  const latestById = new Map<string, any>();
  for (const v of allVals) if (!latestById.has(v.timesheet_id)) latestById.set(v.timesheet_id, v);

  const OK = new Set(['VALIDATION_OK','OVERRIDDEN']);

  // 2) Load current/unlocked TSFIN snapshots (need candidate_id + pay_method for channel gating)
  const { rows: tsfinRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?select=timesheet_id,processing_status,candidate_id,pay_method,` +
      `expenses_charge_ex_vat,expenses_evidence_r2_key,mileage_charge_ex_vat,mileage_evidence_r2_key` +
      `&timesheet_id=in.(${idsParam})` +
      `&is_current=eq.true` +
      `&locked_by_invoice_id=is.null`
  );

  const eligibleIds: string[] = [];
  const blocked: Array<{ id: string, reason: string }> = [];

  // 2a) Fetch candidates (bank + umbrella link)
  const candIds = Array.from(new Set((tsfinRows || []).map((r: any) => r.candidate_id).filter(Boolean)));
  let candidatesById = new Map<string, any>();
  let umbrellasById = new Map<string, any>();

  if (candIds.length) {
    const candParam = candIds.map(encodeURIComponent).join(',');
    const { rows: candRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/candidates` +
      `?select=id,umbrella_id,account_holder,bank_name,sort_code,account_number` +
      `&id=in.(${candParam})`
    );
    candidatesById = new Map((candRows || []).map((c: any) => [c.id, c]));

    // 2b) Fetch umbrellas used by those candidates (only those needed)
    const umbIds = Array.from(new Set((candRows || [])
      .map((c: any) => c.umbrella_id)
      .filter(Boolean)));
    if (umbIds.length) {
      const umbParam = umbIds.map(encodeURIComponent).join(',');
      const { rows: umbRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/umbrellas` +
        `?select=id,name,bank_name,sort_code,account_number` +
        `&id=in.(${umbParam})`
      );
      umbrellasById = new Map((umbRows || []).map((u: any) => [u.id, u]));
    }
  }

  for (const id of ids) {
    const v = latestById.get(id);
    if (!v || !OK.has(v.status)) {
      blocked.push({ id, reason: 'validation_not_ok' });
      continue;
    }

    const row = (tsfinRows || []).find((r: any) => r.timesheet_id === id);
    if (!row) {
      blocked.push({ id, reason: 'tsfin_missing_or_locked' });
      continue;
    }
    if (row.processing_status !== 'READY_FOR_HR') {
      blocked.push({ id, reason: `bad_status_${row.processing_status}` });
      continue;
    }

    // Evidence rules: if charge>0 → evidence required
    const expChg = Number(row.expenses_charge_ex_vat || 0);
    const milChg = Number(row.mileage_charge_ex_vat || 0);
    if (expChg > 0 && !row.expenses_evidence_r2_key) {
      blocked.push({ id, reason: 'expenses_evidence_missing' });
      continue;
    }
    if (milChg > 0 && !row.mileage_evidence_r2_key) {
      blocked.push({ id, reason: 'mileage_evidence_missing' });
      continue;
    }

    // Pay-channel gating
    const cand = candidatesById.get(row.candidate_id);
    const umb  = cand?.umbrella_id ? umbrellasById.get(cand.umbrella_id) : undefined;
    const channel = resolveEffectivePayChannel({
      pay_method: row.pay_method,  // prefer TSFIN snapshot
      candidate: cand,
      umbrella: umb
    });
    if (!channel.ok) {
      blocked.push({ id, reason: 'pay_channel_missing' });
      continue;
    }

    eligibleIds.push(id);
  }

  if (!eligibleIds.length) {
    return badRequest("No timesheets are eligible to mark READY_FOR_INVOICE (validation/evidence/pay-channel rules failed).");
  }

  // 3) Promote
  const updUrl =
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?timesheet_id=in.(${eligibleIds.map(encodeURIComponent).join(',')})` +
    `&is_current=eq.true` +
    `&locked_by_invoice_id=is.null` +
    `&processing_status=eq.READY_FOR_HR`;

  const res = await fetch(updUrl, {
    method: "PATCH",
    headers: { ...sbHeaders(env), "Prefer": "return=representation" },
    body: JSON.stringify({
      processing_status: "READY_FOR_INVOICE",
      updated_at: new Date().toISOString()
    })
  });

  if (!res.ok) {
    const t = await res.text();
    return serverError(`Failed to mark READY_FOR_INVOICE: ${t}`);
  }

  const promoted = await res.json().catch(() => []);
  return ok({
    promoted_count: promoted.length,
    promoted_ids: promoted.map((r: any) => r.timesheet_id),
    blocked_ids: blocked
  });
}
// ---------------------------
// Finance Preview (replacement that uses snapshots)
// ---------------------------

// ---------------------------------------
// FINANCE PREVIEW (now adds exp/mileage)
// ---------------------------------------
async function handleFinancePreviewTsfin(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const body = await parseJSONBody(req).catch(() => null);
  const ids: string[] = Array.isArray(body?.timesheet_ids) ? [...new Set(body.timesheet_ids)].slice(0, 200) : [];
  if (!ids.length) return badRequest('timesheet_ids array required');

  const { rows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?is_current=eq.true&timesheet_id=in.(${ids.map(encodeURIComponent).join(',')})` +
      `&select=timesheet_id,client_id,` +
      [
        'hours_day','hours_night','hours_sat','hours_sun','hours_bh',
        'pay_day','pay_night','pay_sat','pay_sun','pay_bh',
        'charge_day','charge_night','charge_sat','charge_sun','charge_bh',
        'total_pay_ex_vat','total_charge_ex_vat','total_hours',
        'expenses_pay_ex_vat','expenses_charge_ex_vat',
        'mileage_pay_ex_vat','mileage_charge_ex_vat'
      ].join(',')
  );
  if (!rows?.length) return notFound('No current snapshots');

  // Fetch VAT context per client
  const clientIds = [...new Set(rows.map((r: any) => r.client_id).filter(Boolean))];
  const mapClientVat: Record<string, { vat_chargeable: boolean, vat_rate_pct: number }> = {};
  if (clientIds.length) {
    const { rows: cRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/clients?select=id,vat_chargeable&id=in.(${clientIds.map(encodeURIComponent).join(',')})`
    );
    const vatChargeableById: Record<string, boolean> = Object.fromEntries((cRows || []).map((c: any) => [c.id, !!c.vat_chargeable]));
    const { rows: def } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=vat_rate_pct`);
    const defaultVat = Number(def?.[0]?.vat_rate_pct ?? 20);

    // latest client_settings per client
    const { rows: cs } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/client_settings?select=client_id,vat_rate_pct,effective_from&client_id=in.(${clientIds.map(encodeURIComponent).join(',')})&order=client_id.asc,effective_from.desc`
    );
    const latest = new Map<string, number>();
    for (const r of cs || []) if (!latest.has(r.client_id)) latest.set(r.client_id, Number(r.vat_rate_pct ?? defaultVat));
    for (const cid of clientIds) {
      const chargeable = vatChargeableById[cid] ?? true;
      const rate = chargeable ? (latest.get(cid) ?? defaultVat) : 0;
      mapClientVat[cid] = { vat_chargeable: chargeable, vat_rate_pct: rate };
    }
  }

  const agg = {
    total_timesheets: rows.length,
    hours: { day: 0, night: 0, sat: 0, sun: 0, bh: 0, total: 0 },
    totals: {
      pay_ex_vat: 0,
      charge_ex_vat: 0,
      expenses_charge_ex_vat: 0,
      mileage_charge_ex_vat: 0,
      subtotal_ex_vat: 0,
      vat_amount: 0,
      total_inc_vat: 0
    }
  };

  for (const r of rows) {
    const h = { day: +r.hours_day || 0, night: +r.hours_night || 0, sat: +r.hours_sat || 0, sun: +r.hours_sun || 0, bh: +r.hours_bh || 0 };
    const p = { day: +r.pay_day || 0, night: +r.pay_night || 0, sat: +r.pay_sat || 0, sun: +r.pay_sun || 0, bh: +r.pay_bh || 0 };
    const c = { day: +r.charge_day || 0, night: +r.charge_night || 0, sat: +r.charge_sat || 0, sun: +r.charge_sun || 0, bh: +r.charge_bh || 0 };

    const payTotal = round2(h.day*p.day + h.night*p.night + h.sat*p.sat + h.sun*p.sun + h.bh*p.bh);
    const chgTotal = round2(h.day*c.day + h.night*c.night + h.sat*c.sat + h.sun*c.sun + h.bh*c.bh);

    const expChg = Number(r.expenses_charge_ex_vat || 0);
    const milChg = Number(r.mileage_charge_ex_vat || 0);

    const vatCtx = mapClientVat[r.client_id] || { vat_chargeable: true, vat_rate_pct: 20 };
    const lineEx = round2(chgTotal + expChg + milChg);
    const lineVat = round2(lineEx * (vatCtx.vat_rate_pct / 100));
    const lineInc = round2(lineEx + lineVat);

    agg.hours.day += h.day; agg.hours.night += h.night; agg.hours.sat += h.sat; agg.hours.sun += h.sun; agg.hours.bh += h.bh;
    agg.hours.total += (+r.total_hours || (h.day+h.night+h.sat+h.sun+h.bh));

    agg.totals.pay_ex_vat += payTotal;
    agg.totals.charge_ex_vat += chgTotal;
    agg.totals.expenses_charge_ex_vat += expChg;
    agg.totals.mileage_charge_ex_vat += milChg;
    agg.totals.subtotal_ex_vat += lineEx;
    agg.totals.vat_amount += lineVat;
    agg.totals.total_inc_vat += lineInc;
  }

  // Final rounding
  for (const k of Object.keys(agg.totals) as (keyof typeof agg['totals'])[]) {
    // @ts-ignore
    agg.totals[k] = round2(agg.totals[k]);
  }

  return ok(agg);
}

// ---------------------------
// Invoices (TSFIN) – create from READY_FOR_INVOICE snapshots, lock them, build invoice_lines
// ---------------------------

// REPLACE your handleCreateInvoiceTsfin with this
// -----------------------------
// CREATE INVOICE (TSFIN → INV)
// -----------------------------
async function handleCreateInvoiceTsfin(env: any, req: Request) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized('Unauthorized');

  const body = await parseJSONBody(req).catch(() => null);
  if (!body || !Array.isArray(body.timesheet_ids) || body.timesheet_ids.length === 0) {
    return badRequest("timesheet_ids[] required");
  }

  const timesheetIds: string[] = [...new Set(body.timesheet_ids)].filter(Boolean);
  if (timesheetIds.length === 0) return badRequest("No valid timesheet_ids");

  const inIds = timesheetIds.map(encodeURIComponent).join(',');

  // 1) Load eligible TSFIN: current, READY_FOR_INVOICE, unlocked
  const snapUrl =
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?select=*` +
    `&timesheet_id=in.(${inIds})` +
    `&is_current=eq.true` +
    `&locked_by_invoice_id=is.null` +
    `&processing_status=eq.READY_FOR_INVOICE`;

  const { rows: snaps } = await sbFetch(env, snapUrl);
  if (!snaps?.length) {
    return badRequest("No eligible timesheets (need READY_FOR_INVOICE & unlocked).");
  }

  // Ensure exactly one client across the snapshots.
  const clientIds = [...new Set(snaps.map((s: any) => s.client_id).filter(Boolean))];
  if (clientIds.length !== 1) {
    return badRequest(`Expected exactly one client across snapshots, found ${clientIds.length}.`);
  }
  const client_id = clientIds[0];

  // 2) VAT inputs (defaults + client + latest client_settings)
  const { rows: defRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=vat_rate_pct,bank_name,bank_sort_code,bank_account_number,vat_registration_number`
  );
  const defaultVat = Number(defRows?.[0]?.vat_rate_pct ?? 20);

  const { rows: cliRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/clients?select=id,name,invoice_address,primary_invoice_email,vat_chargeable,payment_terms_days&id=eq.${encodeURIComponent(client_id)}`
  );
  const client = cliRows?.[0] || null;
  if (!client) return badRequest("Client not found for snapshots.");

  const { rows: csRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/client_settings` +
    `?select=client_id,vat_rate_pct,effective_from` +
    `&client_id=eq.${encodeURIComponent(client_id)}` +
    `&order=effective_from.desc` +
    `&limit=1`
  );
  const cs = csRows?.[0] || null;

  const vatRatePct: number = client.vat_chargeable === false
    ? 0
    : Number(cs?.vat_rate_pct ?? defaultVat);

  // Also fetch base timesheet data for line meta (no joins at render time later)
  const { rows: tsRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets` +
    `?select=timesheet_id,booking_id,week_ending_date,r2_auth_key,r2_nurse_key,reference_number` +
    `&timesheet_id=in.(${inIds})`
  );
  const tsMetaMap: Record<string, any> = Object.fromEntries(
    (tsRows || []).map((t: any) => [t.timesheet_id, t])
  );

  // 3) Create the invoice header (DRAFT)
  const issuedAt = new Date().toISOString();
  const termsDays = Number(client.payment_terms_days ?? 30);
  const dueAt = new Date(Date.now() + termsDays * 86_400_000).toISOString();

  // Build header snapshot JSON now (used for rendering later)
  const header_snapshot_json = {
    client_id,
    client_name: client.name,
    client_invoice_address: client.invoice_address ?? null,
    client_primary_invoice_email: client.primary_invoice_email ?? null,
    vat_chargeable: !!client.vat_chargeable,
    applied_vat_rate_pct: vatRatePct,
    payment_terms_days: termsDays,
    issued_at_utc: issuedAt,
    due_at_utc: dueAt,
    bank: {
      name: defRows?.[0]?.bank_name ?? null,
      sort_code: defRows?.[0]?.bank_sort_code ?? null,
      account_number: defRows?.[0]?.bank_account_number ?? null,
    },
    vat_registration_number: defRows?.[0]?.vat_registration_number ?? null,
    meta: {
      source: "TSFIN",
      timesheet_count: snaps.length
    }
  };

  const invIns = await fetch(`${env.SUPABASE_URL}/rest/v1/invoices`, {
    method: "POST",
    headers: { ...sbHeaders(env), "Prefer": "return=representation" },
    body: JSON.stringify({
      client_id,
      status: 'DRAFT',
      issued_at_utc: issuedAt,
      due_at_utc: dueAt,
      subtotal_ex_vat: 0,
      vat_amount: 0,
      total_inc_vat: 0,
      header_snapshot_json
    })
  });
  if (!invIns.ok) {
    const t = await invIns.text();
    return serverError(`Failed to create invoice: ${t}`);
  }
  const invoice = (await invIns.json())[0];

  // 4) Build invoice_lines (HOURS + EXPENSES + MILEAGE if applicable)
  let sumEx = 0, sumVat = 0, sumInc = 0;
  const lines: any[] = [];

  for (const s of snaps) {
    // Per-timesheet hours line
    const h = {
      day: Number(s.hours_day || 0),
      night: Number(s.hours_night || 0),
      sat: Number(s.hours_sat || 0),
      sun: Number(s.hours_sun || 0),
      bh: Number(s.hours_bh || 0),
    };
    const pay = {
      day: s.pay_day == null ? null : Number(s.pay_day),
      night: s.pay_night == null ? null : Number(s.pay_night),
      sat: s.pay_sat == null ? null : Number(s.pay_sat),
      sun: s.pay_sun == null ? null : Number(s.pay_sun),
      bh: s.pay_bh == null ? null : Number(s.pay_bh),
    };
    const chg = {
      day: s.charge_day == null ? null : Number(s.charge_day),
      night: s.charge_night == null ? null : Number(s.charge_night),
      sat: s.charge_sat == null ? null : Number(s.charge_sat),
      sun: s.charge_sun == null ? null : Number(s.charge_sun),
      bh: s.charge_bh == null ? null : Number(s.charge_bh),
    };

    const line_pay_ex = round2(
      (h.day * (pay.day ?? 0)) +
      (h.night * (pay.night ?? 0)) +
      (h.sat * (pay.sat ?? 0)) +
      (h.sun * (pay.sun ?? 0)) +
      (h.bh * (pay.bh ?? 0))
    );
    const line_charge_ex = round2(
      (h.day * (chg.day ?? 0)) +
      (h.night * (chg.night ?? 0)) +
      (h.sat * (chg.sat ?? 0)) +
      (h.sun * (chg.sun ?? 0)) +
      (h.bh * (chg.bh ?? 0))
    );
    const margin_ex = round2(line_charge_ex - line_pay_ex);
    const vat_amount = round2(line_charge_ex * vatRatePct / 100);
    const total_inc_vat = round2(line_charge_ex + vat_amount);

    sumEx += line_charge_ex;
    sumVat += vat_amount;
    sumInc += total_inc_vat;

    const tsMeta = tsMetaMap[s.timesheet_id] || {};
    const hoursLineMeta = {
      line_type: "HOURS",
      timesheet_id: s.timesheet_id,
      timesheet_version: s.timesheet_version,
      booking_id: tsMeta.booking_id ?? null,
      week_ending_date_local: tsMeta.week_ending_date ?? null,
      po_number: s.po_number ?? null,
      evidence: {
        r2_auth_key: tsMeta.r2_auth_key ?? null,
        r2_nurse_key: tsMeta.r2_nurse_key ?? null
      },
      policy_snapshot_json: s.policy_snapshot_json ?? {},
      rate_source_refs_json: s.rate_source_refs_json ?? {},
      breakdown: { hours: h, pay, charge: chg },
      totals: {
        line_pay_ex_vat: line_pay_ex,
        line_charge_ex_vat: line_charge_ex,
        margin_ex_vat: margin_ex,
        vat_rate_pct: vatRatePct,
        vat_amount,
        total_inc_vat
      }
    };

    lines.push({
      invoice_id: invoice.id,
      timesheet_id: s.timesheet_id,
      booking_id: tsMeta.booking_id ?? null,
      description: `Timesheet ${s.timesheet_id} (v${s.timesheet_version})`,
      hours_day: h.day, hours_night: h.night, hours_sat: h.sat, hours_sun: h.sun, hours_bh: h.bh,
      pay_day: pay.day, pay_night: pay.night, pay_sat: pay.sat, pay_sun: pay.sun, pay_bh: pay.bh,
      charge_day: chg.day, charge_night: chg.night, charge_sat: chg.sat, charge_sun: chg.sun, charge_bh: chg.bh,
      total_pay_ex_vat: line_pay_ex,
      total_charge_ex_vat: line_charge_ex,
      margin_ex_vat: margin_ex,
      vat_rate_pct: vatRatePct,
      vat_amount,
      total_inc_vat,
      meta_json: hoursLineMeta
    });

    // Expenses line (optional)
    const expCharge = Number(s.expenses_charge_ex_vat || 0);
    if (expCharge > 0) {
      const expPay = Number(s.expenses_pay_ex_vat || 0);
      const expMargin = round2(expCharge - expPay);
      const expVat = round2(expCharge * vatRatePct / 100);
      const expInc = round2(expCharge + expVat);

      sumEx += expCharge; sumVat += expVat; sumInc += expInc;

      const expMeta = {
        line_type: "EXPENSES",
        timesheet_id: s.timesheet_id,
        timesheet_version: s.timesheet_version,
        booking_id: tsMeta.booking_id ?? null,
        week_ending_date_local: tsMeta.week_ending_date ?? null,
        po_number: s.po_number ?? null,
        description: s.expenses_description ?? null,
        evidence_r2_key: s.expenses_evidence_r2_key ?? null,
        totals: {
          pay_ex_vat: expPay,
          charge_ex_vat: expCharge,
          margin_ex_vat: expMargin,
          vat_rate_pct: vatRatePct,
          vat_amount: expVat,
          total_inc_vat: expInc
        }
      };

      lines.push({
        invoice_id: invoice.id,
        timesheet_id: s.timesheet_id,
        booking_id: tsMeta.booking_id ?? null,
        description: `Expenses – ${s.expenses_description || 'Receipted'}`,
        hours_day: 0, hours_night: 0, hours_sat: 0, hours_sun: 0, hours_bh: 0,
        pay_day: null, pay_night: null, pay_sat: null, pay_sun: null, pay_bh: null,
        charge_day: null, charge_night: null, charge_sat: null, charge_sun: null, charge_bh: null,
        total_pay_ex_vat: expPay,
        total_charge_ex_vat: expCharge,
        margin_ex_vat: expMargin,
        vat_rate_pct: vatRatePct,
        vat_amount: expVat,
        total_inc_vat: expInc,
        meta_json: expMeta
      });
    }

    // Mileage line (optional)
    const milCharge = Number(s.mileage_charge_ex_vat || 0);
    if (milCharge > 0) {
      const milPay = Number(s.mileage_pay_ex_vat || 0);
      const milMargin = round2(milCharge - milPay);
      const milVat = round2(milCharge * vatRatePct / 100);
      const milInc = round2(milCharge + milVat);

      sumEx += milCharge; sumVat += milVat; sumInc += milInc;

      const milMeta = {
        line_type: "MILEAGE",
        timesheet_id: s.timesheet_id,
        timesheet_version: s.timesheet_version,
        booking_id: tsMeta.booking_id ?? null,
        week_ending_date_local: tsMeta.week_ending_date ?? null,
        po_number: s.po_number ?? null,
        evidence_r2_key: s.mileage_evidence_r2_key ?? null,
        pay_rate: s.mileage_pay_rate ?? null,
        charge_rate: s.mileage_charge_rate ?? null,
        totals: {
          pay_ex_vat: milPay,
          charge_ex_vat: milCharge,
          margin_ex_vat: milMargin,
          vat_rate_pct: vatRatePct,
          vat_amount: milVat,
          total_inc_vat: milInc
        }
      };

      lines.push({
        invoice_id: invoice.id,
        timesheet_id: s.timesheet_id,
        booking_id: tsMeta.booking_id ?? null,
        description: `Mileage – Receipted`,
        hours_day: 0, hours_night: 0, hours_sat: 0, hours_sun: 0, hours_bh: 0,
        pay_day: null, pay_night: null, pay_sat: null, pay_sun: null, pay_bh: null,
        charge_day: null, charge_night: null, charge_sat: null, charge_sun: null, charge_bh: null,
        total_pay_ex_vat: milPay,
        total_charge_ex_vat: milCharge,
        margin_ex_vat: milMargin,
        vat_rate_pct: vatRatePct,
        vat_amount: milVat,
        total_inc_vat: milInc,
        meta_json: milMeta
      });
    }
  }

  // 5) Persist lines
  if (lines.length) {
    const resLines = await fetch(`${env.SUPABASE_URL}/rest/v1/invoice_lines`, {
      method: "POST",
      headers: { ...sbHeaders(env), "Prefer": "return=minimal" },
      body: JSON.stringify(lines)
    });
    if (!resLines.ok) {
      const t = await resLines.text();
      return serverError(`Failed to insert invoice_lines: ${t}`);
    }
  }

  // 6) Update invoice header totals
  const updInv = await fetch(`${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoice.id)}`, {
    method: "PATCH",
    headers: { ...sbHeaders(env), "Prefer": "return=representation" },
    body: JSON.stringify({
      subtotal_ex_vat: round2(sumEx),
      vat_amount: round2(sumVat),
      total_inc_vat: round2(sumInc),
      updated_at: new Date().toISOString()
    })
  });
  if (!updInv.ok) {
    const t = await updInv.text();
    return serverError(`Failed to update invoice totals: ${t}`);
  }

  // 7) Lock the snapshots to this invoice
  const lockUrl =
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?timesheet_id=in.(${inIds})` +
    `&is_current=eq.true` +
    `&locked_by_invoice_id=is.null` +
    `&processing_status=eq.READY_FOR_INVOICE`;

  const lockRes = await fetch(lockUrl, {
    method: "PATCH",
    headers: { ...sbHeaders(env), "Prefer": "return=minimal" },
    body: JSON.stringify({
      locked_by_invoice_id: invoice.id,
      locked_at_utc: new Date().toISOString()
    })
  });
  if (!lockRes.ok) {
    const t = await lockRes.text();
    return serverError(`Failed to lock snapshots: ${t}`);
  }

  return ok({
    invoice_id: invoice.id,
    client_id,
    lines: lines.length,
    totals: { ex_vat: round2(sumEx), vat: round2(sumVat), inc_vat: round2(sumInc) }
  });
}


// ---------------------------
// Credit note: create credit for an invoice and unlock associated snapshots
// ---------------------------

async function handleCreateCreditNoteTsfin(env: any, req: Request, invoiceId: string) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  // Create credit note header referencing original
  const { rows: invRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoiceId)}&select=*`);
  const inv = invRows[0]; if (!inv) return notFound('Invoice not found');

  const now = new Date().toISOString();
  const cnRes = await fetch(`${env.SUPABASE_URL}/rest/v1/invoices`, { method: 'POST', headers: { ...sbHeaders(env), Prefer: 'return=representation' }, body: JSON.stringify({
  client_id: inv.client_id,
  type: 'CREDIT_NOTE',
  status: 'ISSUED',
  issued_at_utc: now,
  original_invoice_id: inv.id
})
 });
  if (!cnRes.ok) { const t = await cnRes.text(); return serverError(`Credit note create failed: ${t}`); }
  const cnJson = await cnRes.json().catch(() => ([]));
  const credit = Array.isArray(cnJson) ? cnJson[0] : cnJson;

  // Unlock snapshots
  const { rows: snaps } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/timesheets_financials?is_current=eq.true&locked_by_invoice_id=eq.${encodeURIComponent(invoiceId)}&select=timesheet_id`);
  if (snaps.length) {
    const url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?is_current=eq.true&locked_by_invoice_id=eq.${encodeURIComponent(invoiceId)}`;
    const body = { locked_by_invoice_id: null, locked_at_utc: null, unlocked_by_credit_note_id: credit.id, is_stale: true, stale_reason: 'UNLOCKED_BY_CREDIT' };
    const res = await fetch(url, { method: 'PATCH', headers: sbHeaders(env), body: JSON.stringify(body) });
    if (!res.ok) { const t = await res.text(); return serverError(`Unlock failed: ${t}`); }

    // Optionally enqueue recompute for those timesheets
    for (const r of snaps) await sbRpc(env, 'enqueue_ts_financials', { timesheet_id: r.timesheet_id, reason: 'VERSION_ROTATED' });
  }

  return ok({ credit_note_id: credit.id, unlocked_snapshots: snaps.length });
}

// ---------------------------
// Auth, CORS, JSON helpers stubs – remove these if you already have them globally
// ---------------------------

// ---------------------- Router ----------------------
function matchPath(pathname, pattern) {
  const pa = pathname.split("/").filter(Boolean);
  const pb = pattern.split("/").filter(Boolean);
  if (pa.length !== pb.length) return null;
  const params = {};
  for (let i = 0; i < pb.length; i++) {
    if (pb[i].startsWith(":")) params[pb[i].slice(1)] = decodeURIComponent(pa[i]);
    else if (pb[i] !== pa[i]) return null;
  }
  return params;
}

export default {
  async fetch(req, env) {
    const pre = preflightIfNeeded(env, req);
    if (pre) return pre;

    const url = new URL(req.url);
    const p = url.pathname;

    try {
      // Auth routes
      if (req.method === 'POST' && p === '/auth/login')   return withCORS(env, req, await handleAuthLogin(env, req));
      if (req.method === 'POST' && p === '/auth/refresh') return withCORS(env, req, await handleAuthRefresh(env, req));
      if (req.method === 'POST' && p === '/auth/logout')  return withCORS(env, req, await handleAuthLogout(env, req));
      if (req.method === 'POST' && p === '/auth/forgot')  return withCORS(env, req, await handleAuthForgot(env, req));
      if (req.method === 'POST' && p === '/auth/reset')   return withCORS(env, req, await handleAuthReset(env, req));

      // Health
      if (req.method === "GET" && p === "/healthz") return handleHealth(env);
      if (req.method === "GET" && p === "/readyz")  return handleReady(env);
      if (req.method === "GET" && p === "/version") return handleVersion();

      // Core write flow (public/mobile)
      if (req.method === "POST" && p === "/timesheets/presign")           return handlePresign(env, req);
      if (req.method === "PUT"  && p === "/upload")                        return handleUpload(env, req, url);
      if (req.method === "POST" && p === "/timesheets/submit")             return handleSubmit(env, req);

      // Time / TZ checks
      if (req.method === "POST" && p === "/time/uk-check")                 return handleUKTimeCheck(env, req);

      // Revoke flows
      if (req.method === "POST" && p === "/timesheets/revoke")             return handleRevoke(env, req);
      if (req.method === "POST" && p === "/timesheets/revoke-and-presign") return handleRevokeAndPresign(env, req);

      // Reads
      const one = matchPath(p, "/timesheets/:booking_id");
      if (req.method === "GET" && one)                                     return handleGetOne(env, req, one.booking_id, url);
      if (req.method === "GET" && p === "/timesheets")                     return handleList(env, req, url);
      if (req.method === "POST" && p === "/timesheets/query")              return handleQuery(env, req);
      if (req.method === "POST" && p === "/timesheets/authorised-status")  return handleAuthorisedStatus(env, req);

      // Signatures
      if (req.method === "POST" && p === "/signatures/presign-get")        return handleSignPresignGet(env, req);
      if (req.method === "POST" && p === "/signatures/presign-get/batch")  return handleSignPresignGetBatch(env, req);
      if (req.method === "GET"  && p === "/signatures/get")                return handleSignGet(env, req, url);

      // ====================== ADMIN/BACKOFFICE API ROUTES ======================

      // Settings (singleton)
      if (req.method === 'GET' && p === '/api/settings/defaults')          return handleGetSettings(env, req);
      if (req.method === 'PUT' && p === '/api/settings/defaults')          return handleUpdateSettings(env, req);

      // Clients
      if (req.method === 'GET' && p === '/api/clients')                    return handleListClients(env, req);
      if (req.method === 'POST' && p === '/api/clients')                   return handleCreateClient(env, req);
      const client = matchPath(p, '/api/clients/:id');
      if (client && req.method === 'GET')                                  return handleGetClient(env, req, client.id);
      if (client && req.method === 'PUT')                                  return handleUpdateClient(env, req, client.id);

      // Client Hospitals
      const chList = matchPath(p, '/api/clients/:client_id/hospitals');
      if (chList && req.method === 'GET')                                  return handleListHospitals(env, req, chList.client_id);
      if (chList && req.method === 'POST')                                 return handleCreateHospital(env, req, chList.client_id);
      const chOne = matchPath(p, '/api/clients/:client_id/hospitals/:hospital_id');
      if (chOne && req.method === 'GET')                                   return handleGetHospital(env, req, chOne.client_id, chOne.hospital_id);
      if (chOne && req.method === 'PUT')                                   return handleUpdateHospital(env, req, chOne.client_id, chOne.hospital_id);

      // Umbrellas
      if (req.method === 'GET' && p === '/api/umbrellas')                  return handleListUmbrellas(env, req);
      if (req.method === 'POST' && p === '/api/umbrellas')                 return handleCreateUmbrella(env, req);
      const umb = matchPath(p, '/api/umbrellas/:umbrella_id');
      if (umb && req.method === 'GET')                                     return handleGetUmbrella(env, req, umb.umbrella_id);
      if (umb && req.method === 'PUT')                                     return handleUpdateUmbrella(env, req, umb.umbrella_id);

      // Candidates
      if (req.method === 'GET' && p === '/api/candidates')                 return handleListCandidates(env, req);
      if (req.method === 'POST' && p === '/api/candidates')                return handleCreateCandidate(env, req);
      const cand = matchPath(p, '/api/candidates/:candidate_id');
      if (cand && req.method === 'GET')                                    return handleGetCandidate(env, req, cand.candidate_id);
      if (cand && req.method === 'PUT')                                    return handleUpdateCandidate(env, req, cand.candidate_id);

      // Rates
      if (req.method === 'GET' && p === '/api/rates/client-defaults')      return handleListClientRates(env, req);
      if (req.method === 'POST' && p === '/api/rates/client-defaults')     return handleUpsertClientRate(env, req);
      if (req.method === 'GET' && p === '/api/rates/candidate-overrides')  return handleListOverridesByCandidate(env, req);
      if (req.method === 'GET' && p === '/api/rates/client-overrides')     return handleListOverridesByClient(env, req); // expects client_id query param
      if (req.method === 'POST' && p === '/api/rates/candidate-overrides') return handleCreateOverride(env, req);
      if (req.method === 'POST' && p === '/api/rates/resolve-preview')     return handleResolveRate(env, req);
      // Targeted UPDATE + DELETE for candidate overrides by path param
      {
        const cov = matchPath(p, '/api/rates/candidate-overrides/:candidate_id');
        if (cov && req.method === 'PATCH') {
          return handleUpdateOverride(env, req, cov.candidate_id);
        }
        if (cov && req.method === 'DELETE') {
          return handleDeleteOverride(env, req, cov.candidate_id);
        }
      }
      // Add alias path to match the spec (keeps existing /api/rates/client-overrides too)
      if (req.method === 'GET' && p === '/api/rates/candidate-overrides/by-client') {
        return handleListOverridesByClient(env, req); // expects ?client_id=...
      }

      // HealthRoster
      if (req.method === 'POST' && p === '/api/healthroster/import')       return handleHRImport(env, req);
      const hrRows = matchPath(p, '/api/healthroster/:import_id/rows');
      if (hrRows && req.method === 'GET')                                   return handleHRRows(env, req, hrRows.import_id);
      const hrMap = matchPath(p, '/api/healthroster/:import_id/mapping');
      if (hrMap && (req.method === 'GET' || req.method === 'POST'))         return handleHRMapping(env, req, hrMap.import_id);
      const hrVal = matchPath(p, '/api/healthroster/:import_id/validate');
      if (hrVal && req.method === 'POST')                                   return handleHRValidate(env, req, hrVal.import_id);

      // Timesheets finance preview
      if (req.method === 'POST' && p === '/api/timesheets/finance-preview') return handleFinancePreviewTsfin(env, req);

      // TSFIN worker & utilities
      if (req.method === 'POST' && p === '/api/tsfin/queue/drain')          return handleTsfinDrain(env, req);
      if (req.method === 'POST' && p === '/api/tsfin/recompute')            return handleTsfinRecompute(env, req);
      if (req.method === 'GET'  && p === '/api/tsfin/financials')           return handleTsfinFinancials(env, req);
      if (req.method === 'POST' && p === '/api/tsfin/mark-ready')           return handleTsfinMarkReady(env, req);

      // === TSFIN editing (UI-driven)
      {
        const tsfinOne = matchPath(p, '/api/tsfin/:timesheet_id');
        if (tsfinOne && req.method === 'PATCH') {
          return handleTsfinPatch(env, req, tsfinOne.timesheet_id);
        }

        const tsfinExp = matchPath(p, '/api/tsfin/:timesheet_id/expenses');
        if (tsfinExp && req.method === 'PATCH') {
          return handleTsfinPatchExpenses(env, req, tsfinExp.timesheet_id);
        }

        const tsfinMil = matchPath(p, '/api/tsfin/:timesheet_id/mileage');
        if (tsfinMil && req.method === 'PATCH') {
          return handleTsfinPatchMileage(env, req, tsfinMil.timesheet_id);
        }

        const tsfinPO = matchPath(p, '/api/tsfin/:timesheet_id/po');
        if (tsfinPO && req.method === 'PATCH') {
          return handleTsfinPatchPO(env, req, tsfinPO.timesheet_id);
        }
      }

      // Invoices
      if (req.method === 'GET'  && p === '/api/invoices')                   return handleListInvoices(env, req);
      if (req.method === 'POST' && p === '/api/invoices')                   return handleCreateInvoiceTsfin(env, req);

      const inv = matchPath(p, '/api/invoices/:invoice_id');
      if (inv && req.method === 'GET')                                      return handleGetInvoice(env, req, inv.invoice_id);

      // Existing: render by invoice ID (uses stored header/items/totals)
      const invRender = matchPath(p, '/api/invoices/:invoice_id/render');
      if (invRender && req.method === 'POST')                               return handleInvoiceRender(env, req, invRender.invoice_id);

      // NEW: render directly from posted payload (preview / ad-hoc)
      if (req.method === 'POST' && p === '/api/invoices/render')            return handleInvoiceRenderFromPayload(env, req);

      const invEmail = matchPath(p, '/api/invoices/:invoice_id/email');
      if (invEmail && req.method === 'POST')                                return handleInvoiceEmail(env, req, invEmail.invoice_id);

      const invCredit = matchPath(p, '/api/invoices/:invoice_id/credit-note');
      if (invCredit && req.method === 'POST')                               return handleCreateCreditNoteTsfin(env, req, invCredit.invoice_id);

      const invPaid = matchPath(p, '/api/invoices/:invoice_id/mark-paid');
      if (invPaid && req.method === 'POST')                                 return handleInvoiceMarkPaid(env, req, invPaid.invoice_id);

      const invUnpay = matchPath(p, '/api/invoices/:invoice_id/unpay');
      if (invUnpay && req.method === 'POST')                                return handleInvoiceMarkUnpaid(env, req, invUnpay.invoice_id);

      // Remittances — new composer/queue (one email per candidate)
      if (req.method === 'POST' && p === '/api/remittances/email-for-candidate') {
        return handleRemittanceEmailForCandidate(env, req);
      }

      // ====================== EMAIL (OUTBOX, SEND, TSO) ======================

      // List outbox
      if (req.method === 'GET'  && p === '/api/email/outbox')               return handleListOutbox(env, req);
      // Get one outbox item (canonical)
      const outOne = matchPath(p, '/api/email/outbox/:id');
      if (outOne && req.method === 'GET')                                   return handleGetOutboxItem(env, req, outOne.id);
      // Back-compat alias to fetch single outbox item
      const outbox = matchPath(p, '/api/outbox/:mail_id');
      if (outbox && req.method === 'GET')                                   return handleGetOutboxItem(env, req, outbox.mail_id);

      // Drain outbox queue
      if (req.method === 'POST' && p === '/api/email/outbox/drain')         return handleOutboxDrain(env, req);
      // Retry a failed item
      const outRetry = matchPath(p, '/api/email/outbox/:id/retry');
      if (outRetry && req.method === 'POST')                                return handleOutboxRetry(env, req, outRetry.id);

      // Provider callbacks / manual marks
      if (req.method === 'POST' && p === '/api/email/outbox/mark-sent')     return handleOutboxMarkSent(env, req);
      if (req.method === 'POST' && p === '/api/email/outbox/mark-failed')   return handleOutboxMarkFailed(env, req);

      // Ad-hoc direct send / broadcast (canonical)
      if (req.method === 'POST' && p === '/api/email/send')                 return handleEmailSend(env, req);
      // Back-compat alias for broadcast
      if (req.method === 'POST' && p === '/api/email/broadcast')            return handleEmailSend(env, req);

      // ====================== RELATED (generic) ======================
      // Counts for an entity (place before the generic list matcher)
      const relCounts = matchPath(p, '/api/related/:entity/:id/counts');
      if (relCounts && req.method === 'GET') {
        return handleRelatedCounts(env, req, relCounts.entity, relCounts.id);
      }
      // List a related type for an entity (newest-first, with limit/offset)
      const relList = matchPath(p, '/api/related/:entity/:id/:type');
      if (relList && req.method === 'GET') {
        return handleRelatedList(env, req, relList.entity, relList.id, relList.type);
      }

      // Files (R2, signed)
      if (req.method === 'POST' && p === '/api/files/presign-upload')       return handleFilePresignUpload(env, req);
      if (req.method === 'PUT'  && p === '/api/files/upload')               return handleFileUpload(env, req, url);
      if (req.method === 'POST' && p === '/api/files/presign-download')     return handleFilePresignDownload(env, req);
      if (req.method === 'GET'  && p === '/api/files/download')             return handleFilesDownload(env, req); // token-verified download

      return new Response("Not found", { status: 404, headers: TEXT_PLAIN });
    } catch (e) {
      console.error("Unhandled error:", e);
      return serverError("Unexpected error");
    }
  },

  /// Cron handler for TSFIN queue processing + Email outbox drain
  async scheduled(event, env, ctx) {
    const maxBatches = parseInt(env.TSFIN_MAX_BATCHES || '10', 10);
    const batchSize  = parseInt(env.TSFIN_BATCH_SIZE  || '50', 10);

    // Run the TSFIN worker loop until we drain this cycle or hit maxBatches
    ctx.waitUntil((async () => {
      for (let i = 0; i < maxBatches; i++) {
        const res = await runTsfinWorkerOnce(env, { limit: batchSize });
        if (!res || res.picked === 0) break; // nothing left to do this tick
      }
    })());

    // Drain the EMAIL outbox queue on a cadence as well
    const emailBatchLimit = parseInt(env.EMAIL_DRAIN_LIMIT_DEFAULT || '10', 10);
    ctx.waitUntil(drainEmailOutboxOnce(env, { limit: emailBatchLimit }));
  }
}
