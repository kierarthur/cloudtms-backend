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

async function withBrowser(env, fn) {
  const browser = await puppeteer.launch(env.BROWSER); // <-- BROWSER binding from wrangler.toml
  try { return await fn(browser); }
  finally { await browser.close(); }
}


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
// KEEP ONLY THIS (unified helper)
function pick(obj, keyOrKeys, defaults = undefined) {
  // Single key form: pick(obj, 'key', defaultVal)
  if (!Array.isArray(keyOrKeys)) {
    return obj && obj[keyOrKeys] != null ? obj[keyOrKeys] : defaults;
  }
  // Multi-key form: pick(obj, ['a','b'], { a: 1 }) -> { a: valOr1, b: valOrUndefined }
  const out = { ...(defaults || {}) };
  for (const k of keyOrKeys) {
    out[k] = obj && obj[k] != null ? obj[k] : (defaults ? defaults[k] : undefined);
  }
  return out;
}

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
// ====== PDF + TS RENDER HELPERS ======
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';

// MM→points for A4 placement
const MM_TO_PT = 72 / 25.4;
const mmToPt = (mm) => mm * MM_TO_PT;

// UK time formatting helpers
const UK_TZ = 'Europe/London';
const fmtUKDate = (d) => {
  // accepts ISO string or YYYY-MM-DD
  if (!d) return '';
  if (/^\d{4}-\d{2}-\d{2}$/.test(d)) {
    const [y, m, dd] = d.split('-').map(Number);
    // dd/mm/yy
    return `${String(dd).padStart(2,'0')}/${String(m).padStart(2,'0')}/${String(y % 100).padStart(2,'0')}`;
  }
  const dt = new Date(d);
  const parts = new Intl.DateTimeFormat('en-GB', { timeZone: UK_TZ, day: '2-digit', month: '2-digit', year: '2-digit' }).formatToParts(dt);
  const day = parts.find(p => p.type === 'day')?.value ?? '01';
  const mon = parts.find(p => p.type === 'month')?.value ?? '01';
  const yr2 = parts.find(p => p.type === 'year')?.value ?? '00';
  return `${day}/${mon}/${yr2}`;
};
const fmtUKTime = (d) => {
  if (!d) return '';
  const dt = new Date(d);
  return new Intl.DateTimeFormat('en-GB', { timeZone: UK_TZ, hour: '2-digit', minute: '2-digit', hour12: false }).format(dt);
};
// Monday=0 … Sunday=6 based on UK-local start time
const ukWeekdayIndexMon0 = (iso) => {
  if (!iso) return 0;
  const name = new Intl.DateTimeFormat('en-GB', { timeZone: UK_TZ, weekday: 'short' }).format(new Date(iso));
  const map = { Mon: 0, Tue: 1, Wed: 2, Thu: 3, Fri: 4, Sat: 5, Sun: 6 };
  return map[name] ?? 0;
};

// === R2 JSON convenience ===
const TS_LAYOUT_R2_KEY = 'Assets/Stationery/Timesheet/layout.json';

async function r2Put(env, key, bytesOrString, opts = {}) {
  const bucket = env.R2_BUCKET || env.R2;
  const cleanKey = normalizeKey(key);
  const body =
    bytesOrString instanceof Uint8Array ? bytesOrString
    : bytesOrString instanceof ArrayBuffer ? new Uint8Array(bytesOrString)
    : typeof bytesOrString === 'string' ? bytesOrString
    : new Uint8Array(); // fallback to empty

  return bucket.put(cleanKey, body, opts);
}


async function r2GetJSON(env, key) {
  const u8 = await r2GetBytes(env, key);
  if (!u8) return null;
  try { return JSON.parse(new TextDecoder().decode(u8)); }
  catch { return null; }
}

async function r2PutJSON(env, key, obj, opts = {}) {
  const json = JSON.stringify(obj, null, 2);
  return r2Put(env, key, json, { httpMetadata: { contentType: 'application/json' }, ...opts });
}

// Minimal safe fallback (edit once via calibrator UI; persisted copy lives at TS_LAYOUT_R2_KEY)
const TS_LAYOUT_FALLBACK = {
  page: { width_mm: 210, height_mm: 297 },
  rows: [
    { date:{x_mm:22,y_mm:210}, start:{x_mm:60,y_mm:210}, finish:{x_mm:85,y_mm:210}, brkStart:{x_mm:110,y_mm:210}, brkEnd:{x_mm:130,y_mm:210}, role:{x_mm:155,y_mm:210} },
    { date:{x_mm:22,y_mm:200}, start:{x_mm:60,y_mm:200}, finish:{x_mm:85,y_mm:200}, brkStart:{x_mm:110,y_mm:200}, brkEnd:{x_mm:130,y_mm:200}, role:{x_mm:155,y_mm:200} },
    { date:{x_mm:22,y_mm:190}, start:{x_mm:60,y_mm:190}, finish:{x_mm:85,y_mm:190}, brkStart:{x_mm:110,y_mm:190}, brkEnd:{x_mm:130,y_mm:190}, role:{x_mm:155,y_mm:190} },
    { date:{x_mm:22,y_mm:180}, start:{x_mm:60,y_mm:180}, finish:{x_mm:85,y_mm:180}, brkStart:{x_mm:110,y_mm:180}, brkEnd:{x_mm:130,y_mm:180}, role:{x_mm:155,y_mm:180} },
    { date:{x_mm:22,y_mm:170}, start:{x_mm:60,y_mm:170}, finish:{x_mm:85,y_mm:170}, brkStart:{x_mm:110,y_mm:170}, brkEnd:{x_mm:130,y_mm:170}, role:{x_mm:155,y_mm:170} },
    { date:{x_mm:22,y_mm:160}, start:{x_mm:60,y_mm:160}, finish:{x_mm:85,y_mm:160}, brkStart:{x_mm:110,y_mm:160}, brkEnd:{x_mm:130,y_mm:160}, role:{x_mm:155,y_mm:160} },
    { date:{x_mm:22,y_mm:150}, start:{x_mm:60,y_mm:150}, finish:{x_mm:85,y_mm:150}, brkStart:{x_mm:110,y_mm:150}, brkEnd:{x_mm:130,y_mm:150}, role:{x_mm:155,y_mm:150} },
  ],
  fields: {
    hospital:{x_mm:20,y_mm:270}, ward:{x_mm:110,y_mm:270},
    candidate:{x_mm:20,y_mm:260}, job_title:{x_mm:110,y_mm:260}, band:{x_mm:180,y_mm:260},
    booking_ref:{x_mm:20,y_mm:250}, week_ending:{x_mm:110,y_mm:250}, ts_number:{x_mm:180,y_mm:250},
    nurse_sign_date:{x_mm:45,y_mm:85}, nurse_signature:{x_mm:20,y_mm:90,w_mm:60,h_mm:20},
    auth_name:{x_mm:120,y_mm:85}, auth_job_title:{x_mm:120,y_mm:80}, auth_sign_date:{x_mm:165,y_mm:85},
    auth_signature:{x_mm:120,y_mm:90,w_mm:60,h_mm:20},
  },
  text: { fontSize: 10 },
  debug: { enabled: false, grid_mm: 5 },
};

function validateTsLayout(l) {
  const e = (msg) => new Error(`Invalid timesheet layout: ${msg}`);
  if (!l || typeof l !== 'object') throw e('missing object');
  const { page, rows, fields } = l;
  if (!page || typeof page.width_mm !== 'number' || typeof page.height_mm !== 'number') throw e('page dims required');
  if (!Array.isArray(rows) || rows.length !== 7) throw e('rows must be length=7 (Mon..Sun)');
  const needPoint = (o, n) => (o && typeof o.x_mm === 'number' && typeof o.y_mm === 'number') || (()=>{throw e(`row field ${n} missing x_mm/y_mm`)})();
  rows.forEach((r, i) => {
    needPoint(r.date, `rows[${i}].date`);
    needPoint(r.start, `rows[${i}].start`);
    needPoint(r.finish, `rows[${i}].finish`);
    needPoint(r.brkStart, `rows[${i}].brkStart`);
    needPoint(r.brkEnd, `rows[${i}].brkEnd`);
    needPoint(r.role, `rows[${i}].role`);
  });
  const needBox = (o, n) => (o && typeof o.x_mm==='number' && typeof o.y_mm==='number' && typeof o.w_mm==='number' && typeof o.h_mm==='number' && o.w_mm>0 && o.h_mm>0) || (()=>{throw e(`field ${n} needs x/y/w/h (w/h>0)`)} )();
  ['hospital','ward','candidate','job_title','band','booking_ref','week_ending','ts_number','nurse_sign_date','auth_name','auth_job_title','auth_sign_date'].forEach(k=>{
    needPoint(fields[k], `fields.${k}`);
  });
  needBox(fields.nurse_signature, 'fields.nurse_signature');
  needBox(fields.auth_signature, 'fields.auth_signature');
  return l;
}

async function loadTsLayout(env) {
  const fromR2 = await r2GetJSON(env, TS_LAYOUT_R2_KEY);
  try { return validateTsLayout(fromR2 || TS_LAYOUT_FALLBACK); }
  catch { return TS_LAYOUT_FALLBACK; }
}

// Draw an image inside a fixed box while preserving aspect ratio (no distortion).
async function drawImageInBox(page, pdfDoc, bytesU8, box, contentType) {
  if (!bytesU8 || bytesU8.length === 0) return;
  let img;
  if (contentType && /png/i.test(contentType)) {
    img = await pdfDoc.embedPng(bytesU8);
  } else {
    // try JPG if not PNG
    try { img = await pdfDoc.embedJpg(bytesU8); }
    catch { img = await pdfDoc.embedPng(bytesU8); }
  }
  const { width, height } = img;
  const boxW = mmToPt(box.w_mm);
  const boxH = mmToPt(box.h_mm);
  const scale = Math.min(boxW / width, boxH / height, 1); // never scale up
  const drawW = width * scale;
  const drawH = height * scale;
  const x = mmToPt(box.x_mm) + (boxW - drawW) / 2;
  const y = mmToPt(box.y_mm) + (boxH - drawH) / 2;
  page.drawImage(img, { x, y, width: drawW, height: drawH });
}

// Basic R2 helpers
const normalizeKey = (k) => (k || '').replace(/^\/+/, '');
async function r2Exists(env, key) {
  const bucket = env.R2_BUCKET || env.R2;
  const obj = await bucket.head(normalizeKey(key)).catch(() => null);
  return !!obj;
}
async function r2GetBytes(env, key) {
  const bucket = env.R2_BUCKET || env.R2;
  const obj = await bucket.get(normalizeKey(key));
  if (!obj) return null;
  const ab = await new Response(obj.body).arrayBuffer();
  return new Uint8Array(ab);
}

// Signed public download URL (for stationery fetching inside invoice HTML)
function presignR2Url(env, req, key, ttlSeconds = 300) {
  const cleanKey = normalizeKey(key);
  const exp = Math.floor(Date.now() / 1000) + (ttlSeconds | 0);
  const tokenPayload = { typ: "dl", key: cleanKey, exp };
  const token = createToken(env.UPLOAD_TOKEN_SECRET, tokenPayload);
  const base = env.PUBLIC_DOWNLOAD_BASE_URL || new URL(new URL(req.url).origin + '/api/files/download').toString();
  const u = new URL(base);
  u.searchParams.set('key', cleanKey);
  u.searchParams.set('token', token);
  return u.toString();
}

// ===== Timesheet field layout (edit these mm coordinates once, keep forever) =====
// A4: 210 × 297mm, origin at bottom-left (pdf-lib)
// Fill in real coordinates with debug overlay once.
const TS_FIELD_MAP = {
  page: { width_mm: 210, height_mm: 297 },
  // Row anchors for Mon..Sun (index 0..6). Replace with your exact positions.
  rows: [
    { // Monday
      date: { x_mm: 22, y_mm: 210 },
      start: { x_mm: 60, y_mm: 210 },
      finish:{ x_mm: 85, y_mm: 210 },
      brkStart:{ x_mm: 110, y_mm: 210 },
      brkEnd:  { x_mm: 130, y_mm: 210 },
      role:    { x_mm: 155, y_mm: 210 },
    },
    { date: { x_mm: 22, y_mm: 200 }, start:{ x_mm: 60, y_mm: 200 }, finish:{ x_mm:85, y_mm:200 }, brkStart:{ x_mm:110, y_mm:200 }, brkEnd:{ x_mm:130, y_mm:200 }, role:{ x_mm:155, y_mm:200 } },
    { date: { x_mm: 22, y_mm: 190 }, start:{ x_mm: 60, y_mm: 190 }, finish:{ x_mm:85, y_mm:190 }, brkStart:{ x_mm:110, y_mm:190 }, brkEnd:{ x_mm:130, y_mm:190 }, role:{ x_mm:155, y_mm:190 } },
    { date: { x_mm: 22, y_mm: 180 }, start:{ x_mm: 60, y_mm: 180 }, finish:{ x_mm:85, y_mm:180 }, brkStart:{ x_mm:110, y_mm:180 }, brkEnd:{ x_mm:130, y_mm:180 }, role:{ x_mm:155, y_mm:180 } },
    { date: { x_mm: 22, y_mm: 170 }, start:{ x_mm: 60, y_mm: 170 }, finish:{ x_mm:85, y_mm:170 }, brkStart:{ x_mm:110, y_mm:170 }, brkEnd:{ x_mm:130, y_mm:170 }, role:{ x_mm:155, y_mm:170 } },
    { date: { x_mm: 22, y_mm: 160 }, start:{ x_mm: 60, y_mm: 160 }, finish:{ x_mm:85, y_mm:160 }, brkStart:{ x_mm:110, y_mm:160 }, brkEnd:{ x_mm:130, y_mm:160 }, role:{ x_mm:155, y_mm:160 } },
    { date: { x_mm: 22, y_mm: 150 }, start:{ x_mm: 60, y_mm: 150 }, finish:{ x_mm:85, y_mm:150 }, brkStart:{ x_mm:110, y_mm:150 }, brkEnd:{ x_mm:130, y_mm:150 }, role:{ x_mm:155, y_mm:150 } },
  ],
  fields: {
    hospital:      { x_mm: 20,  y_mm: 270 },
    ward:          { x_mm: 110, y_mm: 270 },
    candidate:     { x_mm: 20,  y_mm: 260 },
    job_title:     { x_mm: 110, y_mm: 260 },
    band:          { x_mm: 180, y_mm: 260 },
    booking_ref:   { x_mm: 20,  y_mm: 250 },
    week_ending:   { x_mm: 110, y_mm: 250 },
    ts_number:     { x_mm: 180, y_mm: 250 },

    nurse_sign_date: { x_mm: 45,  y_mm: 85 },
    nurse_signature: { x_mm: 20,  y_mm: 90, w_mm: 60, h_mm: 20 },

    auth_name:       { x_mm: 120, y_mm: 85 },
    auth_job_title:  { x_mm: 120, y_mm: 80 },
    auth_sign_date:  { x_mm: 165, y_mm: 85 },
    auth_signature:  { x_mm: 120, y_mm: 90, w_mm: 60, h_mm: 20 },
  },
  text: { fontSize: 10 },
  debug: { enabled: false, grid_mm: 5 }, // flip enabled=true temporarily for calibration
};

// Draw debug grid + labels (dev-only)
// Draw debug grid + labels (dev-only). Pass the runtime layout.
function drawDebugOverlay(page, font, layout) {
  if (!layout?.debug?.enabled) return;
  const mmToPt = (mm) => mm * (72 / 25.4);
  const light = rgb(0.8, 0.8, 0.8);
  const wPt = mmToPt(layout.page.width_mm);
  const hPt = mmToPt(layout.page.height_mm);
  const step = mmToPt(layout.debug.grid_mm || 5);

  for (let x = 0; x <= wPt; x += step) page.drawLine({ start: { x, y: 0 }, end: { x, y: hPt }, color: light, thickness: 0.3 });
  for (let y = 0; y <= hPt; y += step) page.drawLine({ start: { x: 0, y }, end: { x: wPt, y }, color: light, thickness: 0.3 });

  const label = (txt, mm) => page.drawText(txt, { x: mmToPt(mm.x_mm), y: mmToPt(mm.y_mm), size: 6, font, color: rgb(0.2, 0.2, 0.2) });
  // Markers on row anchors
  layout.rows.forEach((r, i) => {
    label(`Row${i} date`, r.date); label(`Row${i} start`, r.start); label(`Row${i} finish`, r.finish);
    label(`Row${i} brkStart`, r.brkStart); label(`Row${i} brkEnd`, r.brkEnd); label(`Row${i} role`, r.role);
  });
  // Field labels
  Object.entries(layout.fields).forEach(([k, v]) => { if ('x_mm' in v) label(k, v); });

  // Signature rectangles with size readout (so you can eyeball sizing)
  const drawBox = (b, name) => {
    const x = mmToPt(b.x_mm), y = mmToPt(b.y_mm), w = mmToPt(b.w_mm), h = mmToPt(b.h_mm);
    page.drawRectangle({ x, y, width: w, height: h, borderColor: rgb(0.2,0.2,0.2), borderWidth: 0.5, color: undefined, opacity: 0.2 });
    page.drawText(`${name} ${Math.round(b.w_mm)}×${Math.round(b.h_mm)}mm`, { x: x + 1, y: y + h + 2, size: 6, font, color: rgb(0.2,0.2,0.2) });
  };
  if (layout.fields.nurse_signature?.w_mm) drawBox(layout.fields.nurse_signature, 'nurse_signature');
  if (layout.fields.auth_signature?.w_mm) drawBox(layout.fields.auth_signature, 'auth_signature');
}

// Numeric short ref rule
function printableShortRef(s) {
  if (typeof s !== 'string') return '';
  const clean = s.trim();
  if (!/^\d{1,10}$/.test(clean)) return '';
  return clean;
}

// Render a single timesheet to PDF, save to R2 (idempotent), return the R2 key.
// Render a single timesheet to PDF, save to R2 (idempotent), return the R2 key.
async function renderTimesheetPDFAndSave(env, timesheetId) {
  const bucket = env.R2_BUCKET || env.R2;
  if (!bucket?.get || !bucket?.put) throw new Error('Storage not configured');

  const outKey = normalizeKey(`docs-pdf/timesheets/ts_${timesheetId}.pdf`);
  if (await r2Exists(env, outKey)) return outKey;

  // Load runtime layout (R2 → fallback)
  const layout = await loadTsLayout(env);

  // Load TS row
  const { rows: tsRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/timesheets?timesheet_id=eq.${encodeURIComponent(timesheetId)}&select=*`);
  const ts = tsRows?.[0];
  if (!ts) throw new Error('Timesheet not found');

  // Candidate display
  const { rows: finRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/timesheets_financials?timesheet_id=eq.${encodeURIComponent(timesheetId)}&is_current=eq.true&select=candidate_id,band`);
  const fin = finRows?.[0] || {};
  let candidateName = ts.occupant_key_norm || '';
  if (fin.candidate_id) {
    const { rows: cRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(fin.candidate_id)}&select=display_name,first_name,last_name`);
    const c = cRows?.[0];
    if (c) candidateName = c.display_name || [c.first_name, c.last_name].filter(Boolean).join(' ') || candidateName;
  }

  // Load template
  const templateKey = normalizeKey(env.TIMESHEET_TEMPLATE_KEY || 'Assets/Stationery/Timesheet/Blank Timesheet.pdf');
  const templateBytes = await r2GetBytes(env, templateKey);
  if (!templateBytes) throw new Error('Timesheet template not found in R2');

  const pdfDoc = await PDFDocument.load(templateBytes);
  const page = pdfDoc.getPages()[0] || pdfDoc.addPage([mmToPt(layout.page.width_mm), mmToPt(layout.page.height_mm)]);
  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const fz = layout.text?.fontSize || 10;
  const drawText = (txt, mm, size = fz) => {
    if (!txt) return;
    page.drawText(String(txt), { x: mmToPt(mm.x_mm), y: mmToPt(mm.y_mm), size, font, color: rgb(0,0,0) });
  };

  // Header fields
  drawText(ts.hospital_norm || '', layout.fields.hospital);
  drawText(ts.ward_norm || '', layout.fields.ward);
  drawText(candidateName || '', layout.fields.candidate);
  drawText(ts.job_title_norm || '', layout.fields.job_title);
  drawText(fin.band || '', layout.fields.band);
  drawText(ts.booking_id || '', layout.fields.booking_ref);
  drawText(fmtUKDate(ts.week_ending_date), layout.fields.week_ending);
  drawText(printableShortRef(ts.reference_number || '') || '', layout.fields.ts_number);

  // Day row (UK-local)
  const rowIdx = typeof ts.worked_start_iso === 'string' ? ukWeekdayIndexMon0(ts.worked_start_iso) : 0;
  const row = layout.rows[rowIdx] || layout.rows[0];

  drawText(fmtUKDate(ts.worked_start_iso), row.date);
  drawText(fmtUKTime(ts.worked_start_iso), row.start);
  drawText(fmtUKTime(ts.worked_end_iso), row.finish);
  if (ts.break_start_iso && ts.break_end_iso) {
    drawText(fmtUKTime(ts.break_start_iso), row.brkStart);
    drawText(fmtUKTime(ts.break_end_iso), row.brkEnd);
  } else if (typeof ts.break_minutes === 'number') {
    drawText(`${ts.break_minutes}m`, row.brkStart);
  }
  drawText(ts.job_title_norm || '', row.role);

  // Signatures (fit=contain, never distort, never scale up)
  if (ts.r2_nurse_key) {
    const nk = normalizeKey(ts.r2_nurse_key);
    const nurseObj = await (env.R2_BUCKET || env.R2).get(nk);
    if (nurseObj) {
      const nurseBytes = new Uint8Array(await new Response(nurseObj.body).arrayBuffer());
      await drawImageInBox(page, pdfDoc, nurseBytes, layout.fields.nurse_signature, nurseObj.httpMetadata?.contentType || 'image/png');
    }
  }
  if (ts.r2_auth_key) {
    const ak = normalizeKey(ts.r2_auth_key);
    const authObj = await (env.R2_BUCKET || env.R2).get(ak);
    if (authObj) {
      const authBytes = new Uint8Array(await new Response(authObj.body).arrayBuffer());
      await drawImageInBox(page, pdfDoc, authBytes, layout.fields.auth_signature, authObj.httpMetadata?.contentType || 'image/png');
    }
  }

  // Sign dates (UK-local)
  if (ts.authorised_at_server) drawText(fmtUKDate(ts.authorised_at_server), layout.fields.auth_sign_date);
  drawText(fmtUKDate(ts.worked_end_iso || ts.worked_start_iso), layout.fields.nurse_sign_date);

  // Debug overlay (calibration-only)
  drawDebugOverlay(page, font, layout);

  const outBytes = await pdfDoc.save();
  await (env.R2_BUCKET || env.R2).put(outKey, outBytes, { httpMetadata: { contentType: 'application/pdf' } });
  return outKey;
}

// Ensure a TS PDF exists; return its key (render/snapshot if missing)
async function ensureTimesheetPdf(env, timesheetId) {
  const key = normalizeKey(`docs-pdf/timesheets/ts_${timesheetId}.pdf`);
  if (await r2Exists(env, key)) return key;
  return await renderTimesheetPDFAndSave(env, timesheetId);
}

//
// NEW HANDLERS ONLY — per your request
// Assumes shared helpers exist in your codebase: requireUser, withCORS, ok, badRequest, notFound, unauthorized, serverError, parseJSONBody, sbFetch, sbHeaders, writeAudit, sbRpc (optional).
//

// ───────────────────────────────────────────────────────────────────────────────
// Shared local helpers (lightweight, self-contained)
// ───────────────────────────────────────────────────────────────────────────────

const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;
const csvEsc = (v) => {
  if (v == null) return '';
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
const csvJoin = (cols) => cols.map(csvEsc).join(',');

async function getDefaultSettings(env) {
  const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=vat_rate_pct,holiday_pay_pct`);
  return {
    vat: Number(rows?.[0]?.vat_rate_pct ?? 20),
    wtr: Number(rows?.[0]?.holiday_pay_pct ?? 12.07),
  };
}

async function getClientHolidayPctMap(env, clientIds) {
  if (!clientIds?.length) return {};
  const url = `${env.SUPABASE_URL}/rest/v1/client_settings` +
    `?select=client_id,holiday_pay_pct,apply_holiday_to,effective_from` +
    `&client_id=in.(${clientIds.map(enc).join(',')})` +
    `&order=client_id.asc,effective_from.desc`;
  const { rows } = await sbFetch(env, url);
  const map = {};
  for (const r of rows || []) {
    if (map[r.client_id]) continue; // first (latest) only
    map[r.client_id] = {
      pct: (r.holiday_pay_pct == null ? null : Number(r.holiday_pay_pct)),
      applyTo: (r.apply_holiday_to || '').toUpperCase(), // PAYE_ONLY | ALL | NONE
    };
  }
  return map;
}

function resolveWtrPctForRow(row, defaults, clientHolidayMap) {
  // Prefer snapshot if present (already frozen)
  if (row?.pay_wtr_rate_pct_snapshot != null && row?.pay_wtr_rate_pct_snapshot !== '')
    return Number(row.pay_wtr_rate_pct_snapshot);

  // Then prefer policy snapshot values
  const pol = row?.policy_snapshot_json || {};
  const polPct = pol.holiday_pay_pct;
  const apply = String(pol.apply_holiday_to || '').toUpperCase();
  if (apply === 'NONE') return 0;
  if (polPct != null && isFinite(Number(polPct))) return Number(polPct);

  // Else check client settings
  const cs = clientHolidayMap[row.client_id];
  if (cs) {
    if (cs.applyTo === 'NONE') return 0;
    if (isFinite(Number(cs.pct))) return Number(cs.pct);
  }

  // Fallback to defaults
  return Number(defaults.wtr);
}

function deriveUmbrellaVatSnapshots(rowEx, hintRatePct, umbrellaVatChargeable) {
  if (!umbrellaVatChargeable) {
    return { rate: null, vat: 0, inc: rowEx };
  }
  const rate = Number(hintRatePct ?? 0);
  const vat = round2(rowEx * (rate / 100));
  const inc = round2(rowEx + vat);
  return { rate, vat, inc };
}

// ───────────────────────────────────────────────────────────────────────────────
// 1) PAYMENTS & REMITTANCES
// ───────────────────────────────────────────────────────────────────────────────

// ───────────────────────────────────────────────────────────────────────────────
// PAYMENTS — CSV (authorised gate + 16-char payment reference cap)
// ───────────────────────────────────────────────────────────────────────────────

export async function handlePaymentsGenerateCsv(env, req) {
  // ==== Local helpers (pure JS)
  const enc = encodeURIComponent;
  const round2 = (n) => Math.round((Number(n) || 0) * 100) / 100;
  const capRef = (s, max = 16) => (s ? String(s).slice(0, max) : '');
  const csvEsc = (v) => {
    const s = String(v ?? '');
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };

  // ==== Admin auth
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  // ==== Body parsing
  let body;
  try {
    body = await parseJSONBody(req);
  } catch {
    return withCORS(env, req, badRequest('Invalid JSON'));
  }

  // ==== Filters (all optional)
  const from         = body?.week_ending_from || null; // YYYY-MM-DD
  const to           = body?.week_ending_to   || null; // YYYY-MM-DD
  const clientIds    = Array.isArray(body?.client_ids)    ? body.client_ids.filter(Boolean)    : [];
  const candidateIds = Array.isArray(body?.candidate_ids) ? body.candidate_ids.filter(Boolean) : [];
  const payMethod    = body?.pay_method ? String(body.pay_method).toUpperCase() : null; // 'PAYE'|'UMBRELLA'|null
  const umbrellaIds  = Array.isArray(body?.umbrella_ids) ? body.umbrella_ids.filter(Boolean) : [];

  // ==== Query TSFIN — only current, unpaid, not on hold, AUTHORISED timesheets
  let url =
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?select=` +
    [
      'id','timesheet_id','candidate_id','client_id','pay_method',
      'total_pay_ex_vat','expenses_pay_ex_vat','mileage_pay_ex_vat',
      'pay_wtr_rate_pct_snapshot','policy_snapshot_json',
      'pay_vat_rate_pct_snapshot','pay_vat_amount_snapshot','pay_total_inc_vat_snapshot',
      'paid_at_utc','pay_on_hold',
      'timesheet:timesheets(week_ending_date,authorised_at_server)',
    ].join(',') +
    `&is_current=eq.true&paid_at_utc=is.null&pay_on_hold=eq.false` +
    `&timesheet.authorised_at_server=not.is.null`;

  if (from)                url += `&timesheet.week_ending_date=gte.${enc(from)}`;
  if (to)                  url += `&timesheet.week_ending_date=lte.${enc(to)}`;
  if (clientIds.length)    url += `&client_id=in.(${clientIds.map(enc).join(',')})`;
  if (candidateIds.length) url += `&candidate_id=in.(${candidateIds.map(enc).join(',')})`;
  if (payMethod)           url += `&pay_method=eq.${enc(payMethod)}`;

  const { rows: tsRows } = await sbFetch(env, url, false);
  if (!tsRows?.length) {
    return withCORS(env, req, notFound('No eligible timesheets for payment.'));
  }

  // ==== Candidate and Umbrella lookups (for bank + method + VAT)
  const candIds = [...new Set(tsRows.map(r => r.candidate_id).filter(Boolean))];
  const { rows: candRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/candidates?select=id,display_name,first_name,last_name,email,account_holder,bank_name,sort_code,account_number,pay_method,umbrella_id&id=in.(${candIds.map(enc).join(',')})`
  );
  const mapCand = Object.fromEntries((candRows || []).map(c => [c.id, c]));

  const umbIds = [...new Set((candRows || []).map(c => c.umbrella_id).filter(Boolean))];
  const { rows: umbRows } = umbIds.length
    ? await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/umbrellas?id=in.(${umbIds.map(enc).join(',')})&select=id,name,enabled,vat_chargeable,bank_name,sort_code,account_number`
      )
    : { rows: [] };
  const mapUmb = Object.fromEntries((umbRows || []).map(u => [u.id, u]));

  // ==== Optional umbrella filter + safety gates
  const filtered = tsRows.filter(r => {
    const cand = mapCand[r.candidate_id];
    if (!cand) return false;
    if (payMethod && String(cand.pay_method || '').toUpperCase() !== payMethod) return false;

    if (umbrellaIds.length) {
      const u = cand.umbrella_id;
      if (!u || !umbrellaIds.includes(u)) return false;
    }
    // Umbrella enabled gate if pay_method=UMBRELLA
    if (String(cand.pay_method || '').toUpperCase() === 'UMBRELLA') {
      const umb = mapUmb[cand.umbrella_id];
      if (!umb?.enabled) return false;
    }
    // Minimal bank info for PAYE
    if (String(cand.pay_method || '').toUpperCase() === 'PAYE') {
      if (!cand.sort_code || !cand.account_number || !cand.account_holder) return false;
    }
    // Authorised safety (embedded filter above handles most cases)
    if (!r?.timesheet?.authorised_at_server) return false;

    return true;
  });

  if (!filtered.length) {
    return withCORS(env, req, notFound('No eligible timesheets after payment channel checks.'));
  }

  // ==== Defaults / client WTR
  const clientIdSet = [...new Set(filtered.map(r => r.client_id).filter(Boolean))];
  const defaults = await getDefaultSettings(env);
  const clientHolidayMap = await getClientHolidayPctMap(env, clientIdSet);

  // ==== Group by (candidate_id, week_ending_date)
  const groups = new Map(); // key -> { candidate_id, week_ending_date, rows: [], pay_method }
  for (const r of filtered) {
    const we = r?.timesheet?.week_ending_date || null;
    const key = `${r.candidate_id}__${we}`;
    let g = groups.get(key);
    if (!g) {
      g = {
        candidate_id: r.candidate_id,
        week_ending_date: we,
        rows: [],
        pay_method: String(r.pay_method || '').toUpperCase()
      };
      groups.set(key, g);
    }
    g.rows.push(r);
  }

  // ==== CSV compose (Monzo format)
  // Row 1 header MUST match the sample exactly.
  const csvHeader = ['Payment reference','Payee name','Sort code','Bank account number','Bank account type','Amount'];
  const csvRows = [csvHeader.map(csvEsc).join(',')];

  const nowIso = new Date().toISOString();
  const batchRef = `PAY:${nowIso.slice(0,10)}`; // used only for audit correlation

  const affectedTsIds = [];
  const patchQueue = [];

  for (const g of groups.values()) {
    const cand = mapCand[g.candidate_id];
    const payMethodEff = String(cand?.pay_method || g.pay_method || '').toUpperCase();

    // 1) Compute group amount
    let sumEx = 0;
    let sumInc = 0;
    for (const r of g.rows) {
      const payEx = Number(r.total_pay_ex_vat || 0);
      const expEx = Number(r.expenses_pay_ex_vat || 0);
      const milEx = Number(r.mileage_pay_ex_vat || 0);
      const rowEx = round2(payEx + expEx + milEx);

      if (payMethodEff === 'PAYE') {
        // Ensure WTR snapshot if missing
        const wtrPct = resolveWtrPctForRow(r, defaults, clientHolidayMap);
        if (r.pay_wtr_rate_pct_snapshot == null) {
          patchQueue.push({ id: r.id, body: { pay_wtr_rate_pct_snapshot: wtrPct } });
        }
        sumEx += rowEx;
      } else {
        // Umbrella — determine VAT snapshots/INC if umbrella VAT chargeable
        const umb = mapUmb[cand?.umbrella_id];
        const vatChargeable = !!umb?.vat_chargeable;
        let rate   = r.pay_vat_rate_pct_snapshot;
        let vatAmt = r.pay_vat_amount_snapshot;
        let incAmt = r.pay_total_inc_vat_snapshot;

        if (vatChargeable) {
          if (incAmt == null || Number(incAmt) === 0) {
            rate = (rate == null ? defaults.vat : Number(rate));
            const derived = deriveUmbrellaVatSnapshots(rowEx, rate, true);
            rate = derived.rate; vatAmt = derived.vat; incAmt = derived.inc;
            patchQueue.push({
              id: r.id,
              body: {
                pay_vat_rate_pct_snapshot: rate,
                pay_vat_amount_snapshot: vatAmt,
                pay_total_inc_vat_snapshot: incAmt
              }
            });
          }
          sumInc += Number(incAmt || 0);
        } else {
          // Not VAT chargeable — snapshots should be neutral
          if ((r.pay_vat_amount_snapshot && Number(r.pay_vat_amount_snapshot) !== 0) ||
              (r.pay_total_inc_vat_snapshot && Number(r.pay_total_inc_vat_snapshot) !== round2(rowEx))) {
            patchQueue.push({
              id: r.id,
              body: {
                pay_vat_rate_pct_snapshot: null,
                pay_vat_amount_snapshot: 0,
                pay_total_inc_vat_snapshot: rowEx
              }
            });
          }
          sumInc += rowEx;
        }
      }
    }

    const amount = (payMethodEff === 'PAYE') ? round2(sumEx) : round2(sumInc);

    // 2) Determine payee + bank details + account type
    let payeeName, sortCode, accountNumber, accountType;
    if (payMethodEff === 'UMBRELLA') {
      const umb = mapUmb[cand?.umbrella_id];
      if (!umb?.enabled) continue; // safety
      payeeName     = umb.name || 'Umbrella';
      sortCode      = umb.sort_code || '';
      accountNumber = umb.account_number || '';
      accountType   = 'Business';
    } else {
      // PAYE
      const first   = (cand?.first_name || '').trim();
      const last    = (cand?.last_name  || '').trim();
      const display = [first, last].filter(Boolean).join(' ').trim();
      payeeName     = display || cand?.account_holder || cand?.display_name || 'Candidate';
      sortCode      = cand?.sort_code || '';
      accountNumber = cand?.account_number || '';
      accountType   = 'Personal';
    }

    // 3) PAYMENT REFERENCE: Candidate "Surname Firstname", capped to 16 chars
    const refLast  = (cand?.last_name  || '').trim();
    const refFirst = (cand?.first_name || '').trim();
    const lineRef  = capRef([refLast, refFirst].filter(Boolean).join(' ').trim(), 16);

    // 4) CSV row
    csvRows.push([
      csvEsc(lineRef),
      csvEsc(payeeName),
      csvEsc(sortCode),
      csvEsc(accountNumber),
      csvEsc(accountType),
      csvEsc(amount.toFixed(2)),
    ].join(','));

    // 5) Mark all rows in group as paid + store the actual reference used
    for (const r of g.rows) {
      affectedTsIds.push(r.timesheet_id);
      patchQueue.push({
        id: r.id,
        body: {
          paid_at_utc: nowIso,
          paid_by_user_id: user.id || null,
          payment_reference: lineRef
        }
      });
    }
  }

  if (!affectedTsIds.length) {
    return withCORS(env, req, notFound('Nothing to pay.'));
  }

  // ==== Apply patches
  for (const p of patchQueue) {
    await fetch(
      `${env.SUPABASE_URL}/rest/v1/timesheets_financials?id=eq.${enc(p.id)}`,
      {
        method: 'PATCH',
        headers: { ...sbHeaders(env), Prefer: 'return=minimal' },
        body: JSON.stringify(p.body)
      }
    );
  }

  // ==== Audit
  await writeAudit(
    env,
    user,
    'PAY_CSV_GENERATED',
    { batch_reference: batchRef, items: affectedTsIds.length, timesheets: affectedTsIds },
    { entity: 'timesheet', subject_id: null, reason: 'PAYMENT', correlation_id: batchRef, req }
  );

  // ==== Return CSV file text + summary
  const csv = csvRows.join('\n');
  return withCORS(env, req, ok({
    csv,
    affected_timesheet_ids: affectedTsIds,
    groups: groups.size,
    batch_reference: batchRef
  }));
}



export async function handleRemittancesSend(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }

  const ids = Array.isArray(body?.timesheet_ids) ? [...new Set(body.timesheet_ids)].filter(Boolean) : [];
  const resend = body?.resend === true;
  if (!ids.length) return withCORS(env, req, badRequest('timesheet_ids[] required'));

  // Pull current snapshots for those timesheets
  const url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?is_current=eq.true&timesheet_id=in.(${ids.map(enc).join(',')})&select=` + [
      'id','timesheet_id','candidate_id','client_id',
      'pay_method',
      'hours_day','hours_night','hours_sat','hours_sun','hours_bh',
      'pay_day','pay_night','pay_sat','pay_sun','pay_bh',
      'total_hours','total_pay_ex_vat',
      'expenses_pay_ex_vat','mileage_pay_ex_vat',
      'pay_wtr_rate_pct_snapshot','policy_snapshot_json',
      'pay_vat_rate_pct_snapshot','pay_vat_amount_snapshot','pay_total_inc_vat_snapshot',
      'remittance_last_sent_at_utc','remittance_send_count',
      'timesheet:timesheets(timesheet_id,booking_id,week_ending_date,hospital_norm,ward_norm,shift_label_norm)',
      'client:clients(name)'
    ].join(',');
  const { rows: finRowsRaw } = await sbFetch(env, url, false);
  if (!finRowsRaw?.length) return withCORS(env, req, notFound('No matching current timesheets.'));

  const finRows = resend ? finRowsRaw : finRowsRaw.filter(r => !r.remittance_last_sent_at_utc && !r.remittance_send_count);
  if (!finRows.length) return withCORS(env, req, ok({ queued: 0, skipped: finRowsRaw.length, reason: 'already_sent' }));

  const candIds = [...new Set(finRows.map(r => r.candidate_id).filter(Boolean))];
  const { rows: candRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/candidates?id=in.(${candIds.map(enc).join(',')})&select=id,email,display_name,first_name,last_name,pay_method,umbrella_id`
  );
  const mapCand = Object.fromEntries((candRows || []).map(c => [c.id, c]));

  const umbIds = [...new Set((candRows || []).map(c => c.umbrella_id).filter(Boolean))];
  const { rows: umbRows } = umbIds.length
    ? await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/umbrellas?id=in.(${umbIds.map(enc).join(',')})&select=id,vat_chargeable`)
    : { rows: [] };
  const mapUmb = Object.fromEntries((umbRows || []).map(u => [u.id, u]));

  // Period labels per candidate
  const groups = new Map(); // candId -> rows[]
  for (const r of finRows) {
    const arr = groups.get(r.candidate_id) || [];
    arr.push(r);
    groups.set(r.candidate_id, arr);
  }

  const defaults = await getDefaultSettings(env);
  const clientIdSet = [...new Set(finRows.map(r => r.client_id).filter(Boolean))];
  const clientHolidayMap = await getClientHolidayPctMap(env, clientIdSet);

  let totalQueued = 0;
  const outboxIds = [];

  for (const [candId, rows] of groups) {
    const cand = mapCand[candId];
    if (!cand) continue;
    const toEmail = (cand.email || '').trim();
    if (!toEmail) continue;

    // Build period label from date range in rows
    const dates = rows.map(r => r?.timesheet?.week_ending_date).filter(Boolean).sort();
    const first = dates[0], last = dates[dates.length - 1];
    const periodLabel = (first && last) ? (first === last ? `WE ${first}` : `WE ${first}–${last}`) : 'Selected timesheets';
    const periodKey = (first && last) ? (first === last ? `${first}` : `${first}_${last}`) : 'selected';
    const reference = `remit:candidate:${candId}:${periodKey}`;

    // HTML rows
    const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));
    const fmt = (n) => (n == null ? '' : Number(n).toFixed(2));
    const toNum = (v) => (v == null ? 0 : Number(v) || 0);

    const hasPAYE = rows.some(r => String(r.pay_method || '').toUpperCase() === 'PAYE');
    const hasUmb  = rows.some(r => String(r.pay_method || '').toUpperCase() === 'UMBRELLA');

    let totalPayEx = 0, totalExpEx = 0, totalMilEx = 0, totalEx = 0;
    let totalWtrBasic = 0, totalWtrElem = 0;
    let totalVat = 0, totalInc = 0;

    const rowsHtml = rows.map((r) => {
      const ts = r.timesheet || {}; const cli = r.client || {};
      const payMethod = String(r.pay_method || '').toUpperCase();
      const payEx = toNum(r.total_pay_ex_vat);
      const expEx = toNum(r.expenses_pay_ex_vat);
      const milEx = toNum(r.mileage_pay_ex_vat);
      const rowEx = round2(payEx + expEx + milEx);

      totalPayEx += payEx; totalExpEx += expEx; totalMilEx += milEx; totalEx += rowEx;

      // PAYE: informational WTR split
      let wtrInfoHtml = '—';
      if (payMethod === 'PAYE') {
        const wtrPct = resolveWtrPctForRow(r, defaults, clientHolidayMap);
        const base = (payEx > 0) ? (payEx / (1 + (wtrPct / 100))) : 0;
        const wtr = payEx - base;
        totalWtrBasic += base; totalWtrElem += wtr;
        wtrInfoHtml = `${fmt(base)} basic + ${fmt(wtr)} WTR @ ${fmt(wtrPct)}%`;
      }

      // Umbrella VAT
      let vatHtml = '';
      let incHtml = '';
      if (payMethod === 'UMBRELLA') {
        const umb = mapUmb[cand.umbrella_id];
        const vatChargeable = !!umb?.vat_chargeable;
        let rate = r.pay_vat_rate_pct_snapshot;
        let vatAmt = r.pay_vat_amount_snapshot;
        let incAmt = r.pay_total_inc_vat_snapshot;

        if (vatChargeable) {
          if (!incAmt || Number(incAmt) === 0) {
            rate = (rate == null ? defaults.vat : Number(rate));
            const derived = deriveUmbrellaVatSnapshots(rowEx, rate, true);
            rate = derived.rate; vatAmt = derived.vat; incAmt = derived.inc;
          }
          totalVat += Number(vatAmt || 0);
          totalInc += Number(incAmt || 0);
          vatHtml = `${fmt(vatAmt)}${(rate || rate === 0) ? ` @ ${fmt(rate)}%` : ''}`;
          incHtml = `${fmt(incAmt)}`;
        } else {
          totalInc += rowEx;
          vatHtml = '—';
          incHtml = `${fmt(rowEx)}`;
        }
      }

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
          <td style="text-align:right">${fmt(payEx)}</td>
          <td style="text-align:right">${fmt(expEx)}</td>
          <td style="text-align:right">${fmt(milEx)}</td>
          <td style="text-align:right"><strong>${fmt(rowEx)}</strong></td>
          ${hasPAYE ? `<td style="text-align:right">${wtrInfoHtml}</td>` : ''}
          ${hasUmb ? `<td style="text-align:right">${vatHtml}</td>` : ''}
          ${hasUmb ? `<td style="text-align:right"><strong>${incHtml}</strong></td>` : ''}
        </tr>`;
    }).join('');

    const extraPAYECol = hasPAYE ? '<th align="right">Basic + WTR (info)</th>' : '';
    const extraUmbCols = hasUmb ? '<th align="right">VAT</th><th align="right">Total (inc VAT)</th>' : '';
    const candName = cand.display_name || [cand.first_name, cand.last_name].filter(Boolean).join(' ') || 'Candidate';
    const nowIso = new Date().toISOString();
    const titleSuffix = hasUmb ? ' – Umbrella' : (hasPAYE ? ' – PAYE' : '');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;font-size:14px;line-height:1.4">
        <h2 style="margin:0 0 8px">Remittance Advice${titleSuffix}</h2>
        <p style="margin:0 0 12px"><strong>${esc(candName)}</strong></p>
        <p style="margin:0 0 12px">Period: ${esc(periodLabel)}</p>
        <p style="margin:0 0 16px;color:#666">Generated: ${esc(nowIso)}</p>

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
              <th align="right">Pay (ex VAT)</th>
              <th align="right">Expenses</th>
              <th align="right">Mileage</th>
              <th align="right">Total (ex VAT)</th>
              ${extraPAYECol}
              ${extraUmbCols}
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
          <tfoot>
            <tr>
              <td colspan="${hasPAYE ? 19 : 18}" align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>Totals:</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${round2(totalPayEx).toFixed(2)}</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${round2(totalExpEx).toFixed(2)}</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${round2(totalMilEx).toFixed(2)}</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${round2(totalEx).toFixed(2)}</strong></td>
              ${hasPAYE ? `<td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${round2(totalWtrBasic).toFixed(2)} basic + ${round2(totalWtrElem).toFixed(2)} WTR</strong></td>` : ''}
              ${hasUmb ? `<td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${round2(totalVat).toFixed(2)}</strong></td>` : ''}
              ${hasUmb ? `<td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${round2(totalInc).toFixed(2)}</strong></td>` : ''}
            </tr>
          </tfoot>
        </table>

        ${hasPAYE ? `<p style="margin-top:12px;color:#666">Note: For PAYE, the pay rate is WTR-inclusive. The “Basic + WTR” split is informational only and is included in your payment.</p>` : ''}
        ${hasUmb ? `<p style="margin-top:8px;color:#666">Note: For Umbrella assignments where VAT applies, totals show ex VAT and inc VAT amounts using the VAT rate captured at the time of payment/lock.</p>` : ''}
      </div>`;

    // Plain text
    const tlines = [
      `Remittance Advice${titleSuffix}`,
      `${candName}`,
      `Period: ${periodLabel}`,
      `Generated: ${nowIso}`,
      ''
    ];
    for (const r of rows) {
      const ts = r.timesheet || {}; const cli = r.client || {};
      const pm = String(r.pay_method || '').toUpperCase();
      const payEx = Number(r.total_pay_ex_vat || 0);
      const expEx = Number(r.expenses_pay_ex_vat || 0);
      const milEx = Number(r.mileage_pay_ex_vat || 0);
      const rowEx = round2(payEx + expEx + milEx);

      tlines.push(`WE ${ts.week_ending_date || ''} — ${cli.name || ''} / ${ts.hospital_norm || ''} / ${ts.ward_norm || ''} / ${ts.shift_label_norm || ''}`);
      tlines.push(`Day: ${fmt(r.hours_day)} @ ${fmt(r.pay_day)}, Night: ${fmt(r.hours_night)} @ ${fmt(r.pay_night)}, Sat: ${fmt(r.hours_sat)} @ ${fmt(r.pay_sat)}, Sun: ${fmt(r.hours_sun)} @ ${fmt(r.pay_sun)}, BH: ${fmt(r.hours_bh)} @ ${fmt(r.pay_bh)}`);
      tlines.push(`Pay ex VAT: ${fmt(payEx)}  |  Expenses: ${fmt(expEx)}  |  Mileage: ${fmt(milEx)}  |  Total ex VAT: ${fmt(rowEx)}`);
      if (pm === 'PAYE') {
        const wtrPct = resolveWtrPctForRow(r, defaults, clientHolidayMap);
        const base = (payEx > 0) ? (payEx / (1 + (wtrPct / 100))) : 0;
        const wtr = payEx - base;
        tlines.push(`(PAYE) Basic + WTR (info): ${fmt(base)} basic + ${fmt(wtr)} WTR @ ${fmt(wtrPct)}% (included)`);
      } else if (pm === 'UMBRELLA') {
        const umb = mapUmb[cand.umbrella_id];
        const vatChargeable = !!umb?.vat_chargeable;
        let rate = r.pay_vat_rate_pct_snapshot;
        let vatAmt = r.pay_vat_amount_snapshot;
        let incAmt = r.pay_total_inc_vat_snapshot;
        if (vatChargeable && (!incAmt || Number(incAmt) === 0)) {
          rate = (rate == null ? defaults.vat : Number(rate));
          const derived = deriveUmbrellaVatSnapshots(rowEx, rate, true);
          rate = derived.rate; vatAmt = derived.vat; incAmt = derived.inc;
        }
        if (vatChargeable) {
          tlines.push(`(Umbrella) VAT: ${fmt(vatAmt)} @ ${fmt(rate)}%  |  Total inc VAT: ${fmt(incAmt)}`);
        } else {
          tlines.push(`(Umbrella) VAT: 0.00  |  Total inc VAT: ${fmt(rowEx)}`);
        }
      }
      tlines.push('');
    }
    tlines.push(`Totals — Pay ex VAT: ${round2(totalPayEx).toFixed(2)}, Expenses: ${round2(totalExpEx).toFixed(2)}, Mileage: ${round2(totalMilEx).toFixed(2)}, Total ex VAT: ${round2(totalEx).toFixed(2)}`);
    if (hasPAYE) tlines.push(`PAYE Basic + WTR (info totals): ${round2(totalWtrBasic).toFixed(2)} basic + ${round2(totalWtrElem).toFixed(2)} WTR`);
    if (hasUmb)  tlines.push(`Umbrella VAT Total: ${round2(totalVat).toFixed(2)}  |  Total inc VAT: ${round2(totalInc).toFixed(2)}`);
    const text = tlines.join('\n');

    // Queue a single email per-candidate
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
    if (!outRes.ok) continue;
    const outJson = await outRes.json().catch(() => []);
    const mail = Array.isArray(outJson) ? outJson[0] : outJson;
    const mailId = mail?.id || null;
    if (mailId) outboxIds.push(mailId);

    // Audit (candidate)
    await writeAudit(env, user, 'EMAIL_QUEUED', {
      to: toEmail,
      subject: `Remittance Advice – ${periodLabel}`,
      period: { start: first || null, end: last || null },
      mail_id: mailId,
      timesheets: rows.map(r => r.timesheet_id)
    }, { entity: 'candidate', subject_id: candId, reason: 'REMITTANCE', correlation_id: mailId, req });

    // Audit (each timesheet) + update remittance counters
    for (const r of rows) {
      await writeAudit(env, user, 'EMAIL_QUEUED',
        { to: toEmail, subject: `Remittance Advice – ${periodLabel}`, mail_id: mailId },
        { entity: 'timesheet', subject_id: r.timesheet_id, reason: 'REMITTANCE', correlation_id: mailId, req });

      const newCount = Number(r.remittance_send_count || 0) + 1;
      await fetch(`${env.SUPABASE_URL}/rest/v1/timesheets_financials?id=eq.${enc(r.id)}`, {
        method: 'PATCH',
        headers: { ...sbHeaders(env), Prefer: 'return=minimal' },
        body: JSON.stringify({ remittance_last_sent_at_utc: nowIso, remittance_send_count: newCount })
      });
    }

    totalQueued += 1;
  }

  return withCORS(env, req, ok({ queued: totalQueued, outbox_ids: outboxIds }));
}

// ───────────────────────────────────────────────────────────────────────────────
// 2) TIMESHEETS — PAY STATE
// ───────────────────────────────────────────────────────────────────────────────

export async function handleTimesheetPayHold(env, req, timesheetId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }
  const onHold = body?.on_hold === true;
  const reason = (body?.reason || '').trim() || null;

  // Update the current financial snapshot for the timesheet
  const patch = onHold
    ? { pay_on_hold: true, pay_on_hold_reason: reason, pay_on_hold_since_utc: new Date().toISOString() }
    : { pay_on_hold: false, pay_on_hold_reason: null, pay_on_hold_since_utc: null };

  const res = await fetch(
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials?is_current=eq.true&timesheet_id=eq.${enc(timesheetId)}`,
    { method: 'PATCH', headers: { ...sbHeaders(env), Prefer: 'return=representation' }, body: JSON.stringify(patch) }
  );
  if (!res.ok) {
    const t = await res.text();
    return withCORS(env, req, serverError(`Failed to update pay hold: ${t}`));
  }
  const json = await res.json().catch(() => []);
  const row = Array.isArray(json) ? json[0] : json;

  await writeAudit(env, user, onHold ? 'PAY_HOLD_SET' : 'PAY_HOLD_CLEARED', {
    reason,
  }, { entity: 'timesheet', subject_id: timesheetId, reason: 'PAYMENT', correlation_id: null, req });

  return withCORS(env, req, ok({ updated: true, on_hold: onHold, row }));
}

export async function handleTimesheetMarkPaid(env, req, timesheetId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }
  const paid = body?.paid === true;
  const paymentRef = (body?.payment_reference || '').trim() || `MANUAL:${new Date().toISOString().slice(0,10)}`;

  // Load current row
  const { rows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials?is_current=eq.true&timesheet_id=eq.${enc(timesheetId)}` +
    `&select=id,candidate_id,client_id,pay_method,total_pay_ex_vat,expenses_pay_ex_vat,mileage_pay_ex_vat,policy_snapshot_json,` +
    `pay_wtr_rate_pct_snapshot,pay_vat_rate_pct_snapshot,pay_vat_amount_snapshot,pay_total_inc_vat_snapshot`
  );
  const row = rows?.[0];
  if (!row) return withCORS(env, req, notFound('Timesheet financial snapshot not found'));

  const patch = {};
  if (paid) {
    patch.paid_at_utc = new Date().toISOString();
    patch.paid_by_user_id = user.id || null;
    patch.payment_reference = paymentRef;

    // Fill snapshots if missing (WTR for PAYE, VAT for Umbrella)
    const defaults = await getDefaultSettings(env);
    if (String(row.pay_method || '').toUpperCase() === 'PAYE') {
      if (row.pay_wtr_rate_pct_snapshot == null) {
        const clientMap = await getClientHolidayPctMap(env, [row.client_id].filter(Boolean));
        patch.pay_wtr_rate_pct_snapshot = resolveWtrPctForRow(row, defaults, clientMap);
      }
    } else {
      // Umbrella VAT
      // Need umbrella VAT-chargeable. Fetch candidate -> umbrella
      const { rows: cRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${enc(row.candidate_id)}&select=umbrella_id`);
      const umbId = cRows?.[0]?.umbrella_id || null;
      let vatChargeable = false;
      if (umbId) {
        const { rows: uRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/umbrellas?id=eq.${enc(umbId)}&select=vat_chargeable`);
        vatChargeable = !!uRows?.[0]?.vat_chargeable;
      }
      const payEx = Number(row.total_pay_ex_vat || 0);
      const expEx = Number(row.expenses_pay_ex_vat || 0);
      const milEx = Number(row.mileage_pay_ex_vat || 0);
      const rowEx = round2(payEx + expEx + milEx);

      if (vatChargeable) {
        const rate = (row.pay_vat_rate_pct_snapshot == null) ? defaults.vat : Number(row.pay_vat_rate_pct_snapshot);
        const derived = deriveUmbrellaVatSnapshots(rowEx, rate, true);
        patch.pay_vat_rate_pct_snapshot = derived.rate;
        patch.pay_vat_amount_snapshot = derived.vat;
        patch.pay_total_inc_vat_snapshot = derived.inc;
      } else {
        patch.pay_vat_rate_pct_snapshot = null;
        patch.pay_vat_amount_snapshot = 0;
        patch.pay_total_inc_vat_snapshot = rowEx;
      }
    }
  } else {
    patch.paid_at_utc = null;
    patch.paid_by_user_id = null;
    patch.payment_reference = null;
  }

  const res = await fetch(
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials?id=eq.${enc(row.id)}`,
    { method: 'PATCH', headers: { ...sbHeaders(env), Prefer: 'return=representation' }, body: JSON.stringify(patch) }
  );
  if (!res.ok) {
    const t = await res.text();
    return withCORS(env, req, serverError(`Failed to update paid state: ${t}`));
  }
  const json = await res.json().catch(() => []);
  const updated = Array.isArray(json) ? json[0] : json;

  await writeAudit(env, user, paid ? 'MARK_PAID' : 'MARK_UNPAID', {
    payment_reference: paymentRef
  }, { entity: 'timesheet', subject_id: timesheetId, reason: 'PAYMENT', correlation_id: null, req });

  // Optionally enqueue worker to re-evaluate eligibility on UNPAID
  if (!paid) {
    try {
      await sbRpc(env, 'enqueue_ts_financials', { timesheet_id: timesheetId, reason: 'CONTEXT_CHANGED' });
    } catch { /* noop */ }
  }

  return withCORS(env, req, ok({ updated }));
}

// ───────────────────────────────────────────────────────────────────────────────
// 3) REPORTS (screen/print/CSV) — minimal but complete implementations
// ───────────────────────────────────────────────────────────────────────────────

// ───────────────────────────────────────────────────────────────────────────────
// REPORTS — Timesheets (unchanged; already supports print/csv)
// ───────────────────────────────────────────────────────────────────────────────

export async function handleReportTimesheets(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q = (k) => urlObj.searchParams.get(k);
  const qs = (k) => urlObj.searchParams.getAll(k);
  const format = (q('format') || 'json').toLowerCase(); // 'json' | 'csv' | 'print'
  const includeOnHold = (q('include_on_hold') === 'true');

  const from = q('week_ending_from');
  const to = q('week_ending_to');
  const payMethod = q('pay_method') ? q('pay_method').toUpperCase() : null;
  const clientIds = qs('client_id');
  const candidateIds = qs('candidate_id');
  const paid = q('paid'); // 'true' | 'false' | null
  const invoiced = q('invoiced'); // 'true' | 'false' | null

  let url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?select=` + [
      'timesheet_id','candidate_id','client_id','pay_method','locked_by_invoice_id',
      'total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat',
      'expenses_charge_ex_vat','mileage_charge_ex_vat',
      'paid_at_utc','pay_on_hold',
      'timesheet:timesheets(week_ending_date)',
      'client:clients(name)',
    ].join(',') +
    `&is_current=eq.true`;

  if (from) url += `&timesheet.week_ending_date=gte.${enc(from)}`;
  if (to)   url += `&timesheet.week_ending_date=lte.${enc(to)}`;
  if (payMethod) url += `&pay_method=eq.${enc(payMethod)}`;
  if (clientIds.length) url += `&client_id=in.(${clientIds.map(enc).join(',')})`;
  if (candidateIds.length) url += `&candidate_id=in.(${candidateIds.map(enc).join(',')})`;
  if (!includeOnHold) url += `&pay_on_hold=eq.false`;
  if (paid === 'true') url += `&paid_at_utc=not.is.null`;
  if (paid === 'false') url += `&paid_at_utc=is.null`;
  if (invoiced === 'true') url += `&locked_by_invoice_id=not.is.null`;
  if (invoiced === 'false') url += `&locked_by_invoice_id=is.null`;

  const { rows } = await sbFetch(env, url);
  if (!rows?.length) return withCORS(env, req, ok({ rows: [], totals: {} }));

  // Totals
  const totals = rows.reduce((a, r) => {
    a.pay_ex_vat += Number(r.total_pay_ex_vat || 0);
    a.charge_ex_vat += Number(r.total_charge_ex_vat || 0);
    a.margin_ex_vat += Number(r.margin_ex_vat || 0);
    a.expenses_charge_ex_vat += Number(r.expenses_charge_ex_vat || 0);
    a.mileage_charge_ex_vat += Number(r.mileage_charge_ex_vat || 0);
    return a;
  }, { pay_ex_vat:0, charge_ex_vat:0, margin_ex_vat:0, expenses_charge_ex_vat:0, mileage_charge_ex_vat:0 });
  Object.keys(totals).forEach(k => totals[k] = round2(totals[k]));

  if (format === 'csv') {
    const header = ['WeekEnding','Client','PayMethod','Paid','Invoiced','PayExVAT','ChargeExVAT','MarginExVAT','ExpensesChargeExVAT','MileageChargeExVAT'];
    const out = [csvJoin(header)];
    for (const r of rows) {
      out.push(csvJoin([
        r?.timesheet?.week_ending_date || '',
        r?.client?.name || '',
        (r.pay_method || '').toUpperCase(),
        r.paid_at_utc ? 'Y' : 'N',
        r.locked_by_invoice_id ? 'Y' : 'N',
        round2(r.total_pay_ex_vat).toFixed(2),
        round2(r.total_charge_ex_vat).toFixed(2),
        round2(r.margin_ex_vat).toFixed(2),
        round2(r.expenses_charge_ex_vat).toFixed(2),
        round2(r.mileage_charge_ex_vat).toFixed(2),
      ]));
    }
    return withCORS(env, req, ok({ csv: out.join('\n'), totals, count: rows.length }));
  }

  if (format === 'print') {
    const rowsHtml = rows.map(r => `
      <tr>
        <td>${r?.timesheet?.week_ending_date || ''}</td>
        <td>${r?.client?.name || ''}</td>
        <td>${(r.pay_method || '').toUpperCase()}</td>
        <td>${r.paid_at_utc ? 'Y' : 'N'}</td>
        <td>${r.locked_by_invoice_id ? 'Y' : 'N'}</td>
        <td style="text-align:right">${round2(r.total_pay_ex_vat).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.total_charge_ex_vat).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.margin_ex_vat).toFixed(2)}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Timesheets Report</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5"><th>Week Ending</th><th>Client</th><th>Pay Method</th><th>Paid</th><th>Invoiced</th><th>Pay ex VAT</th><th>Charge ex VAT</th><th>Margin ex VAT</th></tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
        <p><strong>Totals —</strong> Pay ex VAT: ${totals.pay_ex_vat.toFixed(2)}, Charge ex VAT: ${totals.charge_ex_vat.toFixed(2)}, Margin ex VAT: ${totals.margin_ex_vat.toFixed(2)}</p>
      </div>`;
    return withCORS(env, req, ok({ html, totals, count: rows.length }));
  }

  return withCORS(env, req, ok({ rows, totals, count: rows.length }));
}


// ───────────────────────────────────────────────────────────────────────────────
// REPORTS — Invoices (add print)
// ───────────────────────────────────────────────────────────────────────────────

export async function handleReportInvoices(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q = (k) => urlObj.searchParams.get(k);
  const qs = (k) => urlObj.searchParams.getAll(k);
  const format = (q('format') || 'json').toLowerCase();

  const from = q('issued_from');
  const to   = q('issued_to');
  const status = q('status'); // DRAFT | ISSUED | ON_HOLD | PAID
  const clientIds = qs('client_id');

  let url = `${env.SUPABASE_URL}/rest/v1/invoices?select=id,client_id,type,status,issued_at_utc,subtotal_ex_vat,vat_amount,total_inc_vat,invoice_no&order=issued_at_utc.desc`;
  if (from) url += `&issued_at_utc=gte.${enc(from)}`;
  if (to)   url += `&issued_at_utc=lte.${enc(to)}`;
  if (status) url += `&status=eq.${enc(status)}`;
  if (clientIds.length) url += `&client_id=in.(${clientIds.map(enc).join(',')})`;

  const { rows: invs } = await sbFetch(env, url);
  if (!invs?.length) return withCORS(env, req, ok({ rows: [], totals: {} }));

  // Get margin by summing invoice_lines.margin_ex_vat
  const invIds = invs.map(i => i.id);
  const { rows: lines } = await sbFetch(env,
    `${env.SUPABASE_URL}/rest/v1/invoice_lines?select=invoice_id,margin_ex_vat&invoice_id=in.(${invIds.map(enc).join(',')})`
  );
  const marginByInv = {};
  for (const ln of lines || []) {
    marginByInv[ln.invoice_id] = round2((marginByInv[ln.invoice_id] || 0) + Number(ln.margin_ex_vat || 0));
  }

  const rows = invs.map(i => ({
    ...i,
    margin_ex_vat: marginByInv[i.id] || 0
  }));

  const totals = rows.reduce((a, r) => {
    a.subtotal_ex_vat += Number(r.subtotal_ex_vat || 0);
    a.vat_amount += Number(r.vat_amount || 0);
    a.total_inc_vat += Number(r.total_inc_vat || 0);
    a.margin_ex_vat += Number(r.margin_ex_vat || 0);
    return a;
  }, { subtotal_ex_vat:0, vat_amount:0, total_inc_vat:0, margin_ex_vat:0 });
  Object.keys(totals).forEach(k => totals[k] = round2(totals[k]));

  if (format === 'csv') {
    const header = ['InvoiceNo','Status','IssuedAt','SubtotalExVAT','VAT','TotalIncVAT','MarginExVAT'];
    const out = [csvJoin(header)];
    for (const r of rows) {
      out.push(csvJoin([
        r.invoice_no || '',
        r.status || '',
        r.issued_at_utc || '',
        round2(r.subtotal_ex_vat).toFixed(2),
        round2(r.vat_amount).toFixed(2),
        round2(r.total_inc_vat).toFixed(2),
        round2(r.margin_ex_vat).toFixed(2)
      ]));
    }
    return withCORS(env, req, ok({ csv: out.join('\n'), totals, count: rows.length }));
  }

  if (format === 'print') {
    const rowsHtml = rows.map(r => `
      <tr>
        <td>${r.invoice_no || ''}</td>
        <td>${r.status || ''}</td>
        <td>${r.issued_at_utc || ''}</td>
        <td style="text-align:right">${round2(r.subtotal_ex_vat).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.vat_amount).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.total_inc_vat).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.margin_ex_vat).toFixed(2)}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Invoices Report</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5">
            <th>Invoice No</th><th>Status</th><th>Issued At</th>
            <th>Subtotal ex VAT</th><th>VAT</th><th>Total inc VAT</th><th>Margin ex VAT</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
        <p><strong>Totals —</strong> Subtotal: ${totals.subtotal_ex_vat.toFixed(2)}, VAT: ${totals.vat_amount.toFixed(2)}, Total: ${totals.total_inc_vat.toFixed(2)}, Margin: ${totals.margin_ex_vat.toFixed(2)}</p>
      </div>`;
    return withCORS(env, req, ok({ html, totals, count: rows.length }));
  }

  return withCORS(env, req, ok({ rows, totals, count: rows.length }));
}


// ───────────────────────────────────────────────────────────────────────────────
// REPORTS — Candidates (add print)
// ───────────────────────────────────────────────────────────────────────────────

export async function handleReportCandidates(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q = (k) => urlObj.searchParams.get(k);
  const qs = (k) => urlObj.searchParams.getAll(k);
  const format = (q('format') || 'json').toLowerCase();

  const from = q('week_ending_from');
  const to   = q('week_ending_to');
  const candidateIds = qs('candidate_id');

  let url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?select=candidate_id,total_charge_ex_vat,total_pay_ex_vat,margin_ex_vat,timesheet:timesheets(week_ending_date)` +
    `&is_current=eq.true`;
  if (from) url += `&timesheet.week_ending_date=gte.${enc(from)}`;
  if (to)   url += `&timesheet.week_ending_date=lte.${enc(to)}`;
  if (candidateIds.length) url += `&candidate_id=in.(${candidateIds.map(enc).join(',')})`;

  const { rows } = await sbFetch(env, url);
  const agg = {};
  for (const r of rows || []) {
    if (!r.candidate_id) continue;
    const a = (agg[r.candidate_id] ||= { candidate_id: r.candidate_id, charge_ex_vat:0, pay_ex_vat:0, margin_ex_vat:0 });
    a.charge_ex_vat += Number(r.total_charge_ex_vat || 0);
    a.pay_ex_vat += Number(r.total_pay_ex_vat || 0);
    a.margin_ex_vat += Number(r.margin_ex_vat || 0);
  }
  const outRows = Object.values(agg).map((a) => ({
    ...a,
    charge_ex_vat: round2(a.charge_ex_vat),
    pay_ex_vat: round2(a.pay_ex_vat),
    margin_ex_vat: round2(a.margin_ex_vat)
  }));

  if (format === 'csv') {
    const header = ['CandidateId','ChargeExVAT','PayExVAT','MarginExVAT'];
    const out = [csvJoin(header)];
    for (const r of outRows) out.push(csvJoin([r.candidate_id, r.charge_ex_vat.toFixed(2), r.pay_ex_vat.toFixed(2), r.margin_ex_vat.toFixed(2)]));
    return withCORS(env, req, ok({ csv: out.join('\n'), count: outRows.length }));
  }

  if (format === 'print') {
    const rowsHtml = outRows.map(r => `
      <tr>
        <td>${r.candidate_id}</td>
        <td style="text-align:right">${r.charge_ex_vat.toFixed(2)}</td>
        <td style="text-align:right">${r.pay_ex_vat.toFixed(2)}</td>
        <td style="text-align:right">${r.margin_ex_vat.toFixed(2)}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Candidates Report</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5"><th>Candidate</th><th>Charge ex VAT</th><th>Pay ex VAT</th><th>Margin ex VAT</th></tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    return withCORS(env, req, ok({ html, count: outRows.length }));
  }

  return withCORS(env, req, ok({ rows: outRows, count: outRows.length }));
}


// ───────────────────────────────────────────────────────────────────────────────
// REPORTS — Clients (add print)
// ───────────────────────────────────────────────────────────────────────────────

export async function handleReportClients(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q = (k) => urlObj.searchParams.get(k);
  const qs = (k) => urlObj.searchParams.getAll(k);
  const format = (q('format') || 'json').toLowerCase();

  const from = q('week_ending_from');
  const to   = q('week_ending_to');
  const clientIds = qs('client_id');

  let url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?select=client_id,total_charge_ex_vat,total_pay_ex_vat,margin_ex_vat,timesheet:timesheets(week_ending_date)` +
    `&is_current=eq.true`;
  if (from) url += `&timesheet.week_ending_date=gte.${enc(from)}`;
  if (to)   url += `&timesheet.week_ending_date=lte.${enc(to)}`;
  if (clientIds.length) url += `&client_id=in.(${clientIds.map(enc).join(',')})`;

  const { rows } = await sbFetch(env, url);
  const agg = {};
  for (const r of rows || []) {
    if (!r.client_id) continue;
    const a = (agg[r.client_id] ||= { client_id: r.client_id, charge_ex_vat:0, pay_ex_vat:0, margin_ex_vat:0 });
    a.charge_ex_vat += Number(r.total_charge_ex_vat || 0);
    a.pay_ex_vat += Number(r.total_pay_ex_vat || 0);
    a.margin_ex_vat += Number(r.margin_ex_vat || 0);
  }
  const outRows = Object.values(agg).map((a) => ({
    ...a,
    charge_ex_vat: round2(a.charge_ex_vat),
    pay_ex_vat: round2(a.pay_ex_vat),
    margin_ex_vat: round2(a.margin_ex_vat)
  }));

  if (format === 'csv') {
    const header = ['ClientId','ChargeExVAT','PayExVAT','MarginExVAT'];
    const out = [csvJoin(header)];
    for (const r of outRows) out.push(csvJoin([r.client_id, r.charge_ex_vat.toFixed(2), r.pay_ex_vat.toFixed(2), r.margin_ex_vat.toFixed(2)]));
    return withCORS(env, req, ok({ csv: out.join('\n'), count: outRows.length }));
  }

  if (format === 'print') {
    const rowsHtml = outRows.map(r => `
      <tr>
        <td>${r.client_id}</td>
        <td style="text-align:right">${r.charge_ex_vat.toFixed(2)}</td>
        <td style="text-align:right">${r.pay_ex_vat.toFixed(2)}</td>
        <td style="text-align:right">${r.margin_ex_vat.toFixed(2)}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Clients Report</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5"><th>Client</th><th>Charge ex VAT</th><th>Pay ex VAT</th><th>Margin ex VAT</th></tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    return withCORS(env, req, ok({ html, count: outRows.length }));
  }

  return withCORS(env, req, ok({ rows: outRows, count: outRows.length }));
}


// ───────────────────────────────────────────────────────────────────────────────
// REPORTS — Umbrellas (add charge/margin + print)
// ───────────────────────────────────────────────────────────────────────────────

export async function handleReportUmbrellas(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q = (k) => urlObj.searchParams.get(k);
  const format = (q('format') || 'json').toLowerCase();

  // Pull candidate <- umbrella mapping (and names for display)
  const { rows: candUmb } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/candidates?select=id,umbrella_id&umbrella_id=not.is.null`);
  const mapCandUmb = Object.fromEntries((candUmb || []).map(c => [c.id, c.umbrella_id]));
  const umbIds = [...new Set((candUmb || []).map(c => c.umbrella_id))];
  if (!umbIds.length) return withCORS(env, req, ok({ rows: [], count: 0 }));

  const { rows: umbMeta } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/umbrellas?id=in.(${umbIds.map(enc).join(',')})&select=id,name`);
  const umbName = Object.fromEntries((umbMeta || []).map(u => [u.id, u.name || u.id]));

  // Get ts-fin grouped for those candidates (include pay, charge, margin)
  const { rows } = await sbFetch(env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials?select=candidate_id,total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat` +
    `&is_current=eq.true&candidate_id=in.(${Object.keys(mapCandUmb).map(enc).join(',')})`
  );

  const agg = {};
  for (const r of rows || []) {
    const umb = mapCandUmb[r.candidate_id];
    const a = (agg[umb] ||= { umbrella_id: umb, umbrella_name: umbName[umb] || umb, pay_ex_vat: 0, charge_ex_vat: 0, margin_ex_vat: 0 });
    a.pay_ex_vat += Number(r.total_pay_ex_vat || 0);
    a.charge_ex_vat += Number(r.total_charge_ex_vat || 0);
    a.margin_ex_vat += Number(r.margin_ex_vat || 0);
  }
  for (const k of Object.keys(agg)) {
    agg[k].pay_ex_vat = round2(agg[k].pay_ex_vat);
    agg[k].charge_ex_vat = round2(agg[k].charge_ex_vat);
    agg[k].margin_ex_vat = round2(agg[k].margin_ex_vat);
  }

  const outRows = Object.values(agg);

  if (format === 'csv') {
    const header = ['UmbrellaId','UmbrellaName','PayExVAT','ChargeExVAT','MarginExVAT'];
    const out = [csvJoin(header)];
    for (const r of outRows) out.push(csvJoin([r.umbrella_id, r.umbrella_name, r.pay_ex_vat.toFixed(2), r.charge_ex_vat.toFixed(2), r.margin_ex_vat.toFixed(2)]));
    return withCORS(env, req, ok({ csv: out.join('\n'), count: outRows.length }));
  }

  if (format === 'print') {
    const rowsHtml = outRows.map(r => `
      <tr>
        <td>${r.umbrella_name}</td>
        <td style="text-align:right">${r.pay_ex_vat.toFixed(2)}</td>
        <td style="text-align:right">${r.charge_ex_vat.toFixed(2)}</td>
        <td style="text-align:right">${r.margin_ex_vat.toFixed(2)}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Umbrellas Report</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5"><th>Umbrella</th><th>Pay ex VAT</th><th>Charge ex VAT</th><th>Margin ex VAT</th></tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    return withCORS(env, req, ok({ html, count: outRows.length }));
  }

  return withCORS(env, req, ok({ rows: outRows, count: outRows.length }));
}
// ───────────────────────────────────────────────────────────────────────────────
// 4) SEARCH (rich filters per section) — pragmatic implementations
// ───────────────────────────────────────────────────────────────────────────────

// ───────────────────────────────────────────────────────────────────────────────
// SEARCH — Timesheets (richer filters + csv/print)
// ───────────────────────────────────────────────────────────────────────────────

export async function handleSearchTimesheets(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q = (k) => urlObj.searchParams.get(k);
  const qa = (k) => urlObj.searchParams.getAll(k); // for repeated params (e.g., status)
  const page = Math.max(1, parseInt(q('page') || '1', 10));
  const pageSize = Math.max(1, Math.min(200, parseInt(q('page_size') || '50', 10)));

  const format = (q('format') || 'json').toLowerCase(); // 'json'|'csv'|'print'

  // Existing booleans
  const includeOnHold = q('include_on_hold') === 'true';
  const paid      = q('paid');         // 'true' | 'false' | null
  const invoiced  = q('invoiced');     // 'true' | 'false' | null

  // Existing range filters
  const weFrom    = q('week_ending_from');
  const weTo      = q('week_ending_to');
  const clientId  = q('client_id');
  const candidateId = q('candidate_id');
  const payMethod = q('pay_method') ? q('pay_method').toUpperCase() : null;

  // NEW filters to match FE
  const bookingId = q('booking_id');
  const occKey    = q('occupant_key_norm');
  const hospital  = q('hospital_norm');
  const workedFrom = q('worked_from');
  const workedTo   = q('worked_to');
  const createdFrom = q('created_from');
  const createdTo   = q('created_to');
  const statuses    = qa('status'); // repeated: status=A&status=B

  const orderBy = (q('order_by') || 'week_ending_date').toLowerCase(); // week_ending_date|margin|charge|pay
  const orderDir = (q('order_dir') || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';

  // Base select: join needed timesheet bits for filter/sort on week_ending_date & status/hospital/occupant
  let url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
    `?select=timesheet_id,candidate_id,client_id,pay_method,` +
    `total_charge_ex_vat,total_pay_ex_vat,margin_ex_vat,paid_at_utc,locked_by_invoice_id,pay_on_hold,created_at,` +
    `timesheet:timesheets(week_ending_date,status,booking_id,occupant_key_norm,hospital_norm),` +
    `client:clients(name)` +
    `&is_current=eq.true`;

  // Existing filters
  if (weFrom) url += `&timesheet.week_ending_date=gte.${enc(weFrom)}`;
  if (weTo)   url += `&timesheet.week_ending_date=lte.${enc(weTo)}`;
  if (clientId)    url += `&client_id=eq.${enc(clientId)}`;
  if (candidateId) url += `&candidate_id=eq.${enc(candidateId)}`;
  if (payMethod)   url += `&pay_method=eq.${enc(payMethod)}`;
  if (!includeOnHold) url += `&pay_on_hold=eq.false`;
  if (paid === 'true')  url += `&paid_at_utc=not.is.null`;
  if (paid === 'false') url += `&paid_at_utc=is.null`;
  if (invoiced === 'true')  url += `&locked_by_invoice_id=not.is.null`;
  if (invoiced === 'false') url += `&locked_by_invoice_id=is.null`;

  // NEW: extra filters aligned with FE buildSearchQS()
  if (bookingId) url += `&timesheet.booking_id=eq.${enc(bookingId)}`;
  if (occKey)    url += `&timesheet.occupant_key_norm=eq.${enc(occKey)}`;
  if (hospital)  url += `&timesheet.hospital_norm=eq.${enc(hospital)}`;
  if (workedFrom) url += `&worked_start_iso=gte.${enc(workedFrom)}`;
  if (workedTo)   url += `&worked_end_iso=lte.${enc(workedTo)}`;
  if (createdFrom) url += `&created_at=gte.${enc(createdFrom)}`;
  if (createdTo)   url += `&created_at=lte.${enc(createdTo)}`;
  if (Array.isArray(statuses) && statuses.length) {
    // PostgREST in() — for enums, unquoted tokens are fine: in.(RECEIVED,STORED)
    const inList = statuses.map(s => String(s).toUpperCase().replace(/[(),]/g,'')).join(',');
    url += `&timesheet.status=in.(${inList})`;
  }

  const orderMap = {
    week_ending_date: 'timesheet.week_ending_date',
    margin: 'margin_ex_vat',
    charge: 'total_charge_ex_vat',
    pay: 'total_pay_ex_vat',
  };
  const orderCol = orderMap[orderBy] || orderMap.week_ending_date;
  url += `&order=${enc(orderCol)}.${orderDir}&limit=${pageSize}&offset=${(page-1)*pageSize}`;

  const { rows } = await sbFetch(env, url);

  if (format === 'csv') {
    const header = ['WeekEnding','Client','PayMethod','OnHold','Paid','Invoiced','PayExVAT','ChargeExVAT','MarginExVAT'];
    const out = [csvJoin(header)];
    for (const r of rows || []) {
      out.push(csvJoin([
        r?.timesheet?.week_ending_date || '',
        r?.client?.name || '',
        (r.pay_method || '').toUpperCase(),
        r.pay_on_hold ? 'Y' : 'N',
        r.paid_at_utc ? 'Y' : 'N',
        r.locked_by_invoice_id ? 'Y' : 'N',
        round2(r.total_pay_ex_vat || 0).toFixed(2),
        round2(r.total_charge_ex_vat || 0).toFixed(2),
        round2(r.margin_ex_vat || 0).toFixed(2),
      ]));
    }
    return withCORS(env, req, ok({ csv: out.join('\n'), count: rows?.length || 0, page, page_size: pageSize }));
  }

  if (format === 'print') {
    const rowsHtml = (rows || []).map(r => `
      <tr>
        <td>${r?.timesheet?.week_ending_date || ''}</td>
        <td>${r?.client?.name || ''}</td>
        <td>${(r.pay_method || '').toUpperCase()}</td>
        <td>${r.pay_on_hold ? 'Y' : 'N'}</td>
        <td>${r.paid_at_utc ? 'Y' : 'N'}</td>
        <td>${r.locked_by_invoice_id ? 'Y' : 'N'}</td>
        <td style="text-align:right">${round2(r.total_pay_ex_vat || 0).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.total_charge_ex_vat || 0).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.margin_ex_vat || 0).toFixed(2)}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Timesheets — Search Results</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5">
            <th>Week Ending</th><th>Client</th><th>Pay Method</th>
            <th>On Hold</th><th>Paid</th><th>Invoiced</th>
            <th>Pay ex VAT</th><th>Charge ex VAT</th><th>Margin ex VAT</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    return withCORS(env, req, ok({ html, count: rows?.length || 0, page, page_size: pageSize }));
  }

  return withCORS(env, req, ok({ rows, page, page_size: pageSize, count: rows?.length || 0 }));
}



// ───────────────────────────────────────────────────────────────────────────────
// SEARCH — Invoices (richer filters + csv/print)
// ───────────────────────────────────────────────────────────────────────────────
export async function handleSearchInvoices(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q  = (k) => urlObj.searchParams.get(k);
  const qa = (k) => urlObj.searchParams.getAll(k);
  const page = Math.max(1, parseInt(q('page') || '1', 10));
  const pageSize = Math.max(1, Math.min(200, parseInt(q('page_size') || '50', 10)));

  const format = (q('format') || 'json').toLowerCase(); // 'json'|'csv'|'print'

  // Filters (now accepting arrays and extra ranges)
  const statuses  = qa('status');             // repeated
  const clientId  = q('client_id');
  const invNo     = q('invoice_no');
  const invQ      = q('q');                   // partial match on invoice_no
  const issuedFrom = q('issued_from');
  const issuedTo   = q('issued_to');
  const dueFrom    = q('due_from');
  const dueTo      = q('due_to');
  const createdFrom = q('created_from');
  const createdTo   = q('created_to');

  const orderBy = (q('order_by') || 'issued_at_utc').toLowerCase();
  const orderDir = (q('order_dir') || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';
  const orderAllowed = new Set(['issued_at_utc','invoice_no','total_inc_vat','subtotal_ex_vat','created_at']);

  let url = `${env.SUPABASE_URL}/rest/v1/invoices` +
    `?select=id,invoice_no,client_id,status,issued_at_utc,due_at_utc,created_at,total_inc_vat,subtotal_ex_vat,vat_amount` +
    `&limit=${pageSize}&offset=${(page-1)*pageSize}`;

  // Apply filters
  if (Array.isArray(statuses) && statuses.length) {
    const inList = statuses.map(s => String(s).toUpperCase().replace(/[(),]/g,'')).join(',');
    url += `&status=in.(${inList})`;
  }
  if (clientId) url += `&client_id=eq.${enc(clientId)}`;
  if (invNo)    url += `&invoice_no=eq.${enc(invNo)}`;
  if (invQ)     url += `&invoice_no=ilike.*${enc(invQ)}*`;
  if (issuedFrom) url += `&issued_at_utc=gte.${enc(issuedFrom)}`;
  if (issuedTo)   url += `&issued_at_utc=lte.${enc(issuedTo)}`;
  if (dueFrom)    url += `&due_at_utc=gte.${enc(dueFrom)}`;
  if (dueTo)      url += `&due_at_utc=lte.${enc(dueTo)}`;
  if (createdFrom) url += `&created_at=gte.${enc(createdFrom)}`;
  if (createdTo)   url += `&created_at=lte.${enc(createdTo)}`;

  url += `&order=${orderAllowed.has(orderBy) ? enc(orderBy) : 'issued_at_utc'}.${orderDir}`;

  const { rows } = await sbFetch(env, url);

  if (format === 'csv') {
    const header = ['InvoiceNo','Status','IssuedAt','DueAt','CreatedAt','SubtotalExVAT','VAT','TotalIncVAT'];
    const out = [csvJoin(header)];
    for (const r of rows || []) {
      out.push(csvJoin([
        r.invoice_no || '',
        r.status || '',
        r.issued_at_utc || '',
        r.due_at_utc || '',
        r.created_at || '',
        round2(r.subtotal_ex_vat || 0).toFixed(2),
        round2(r.vat_amount || 0).toFixed(2),
        round2(r.total_inc_vat || 0).toFixed(2),
      ]));
    }
    return withCORS(env, req, ok({ csv: out.join('\n'), count: rows?.length || 0, page, page_size: pageSize }));
  }

  if (format === 'print') {
    const rowsHtml = (rows || []).map(r => `
      <tr>
        <td>${r.invoice_no || ''}</td>
        <td>${r.status || ''}</td>
        <td>${r.issued_at_utc || ''}</td>
        <td>${r.due_at_utc || ''}</td>
        <td>${r.created_at || ''}</td>
        <td style="text-align:right">${round2(r.subtotal_ex_vat || 0).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.vat_amount || 0).toFixed(2)}</td>
        <td style="text-align:right">${round2(r.total_inc_vat || 0).toFixed(2)}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Invoices — Search Results</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5">
            <th>Invoice No</th><th>Status</th><th>Issued At</th><th>Due At</th><th>Created At</th>
            <th>Subtotal ex VAT</th><th>VAT</th><th>Total inc VAT</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    return withCORS(env, req, ok({ html, count: rows?.length || 0, page, page_size: pageSize }));
  }

  return withCORS(env, req, ok({ rows, page, page_size: pageSize, count: rows?.length || 0 }));
}


// ───────────────────────────────────────────────────────────────────────────────
// SEARCH — Candidates (richer filters + csv/print)
// ───────────────────────────────────────────────────────────────────────────────




// ───────────────────────────────────────────────────────────────────────────────
// SEARCH — Clients (richer filters + csv/print)
// ───────────────────────────────────────────────────────────────────────────────
export async function handleSearchClients(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q = (k) => urlObj.searchParams.get(k);
  const page = Math.max(1, parseInt(q('page') || '1', 10));
  const pageSize = Math.max(1, Math.min(200, parseInt(q('page_size') || '50', 10)));
  const format = (q('format') || 'json').toLowerCase(); // 'json'|'csv'|'print'

  // Filters expanded to match FE
  const text = q('q'); // name partial
  const cliRef = q('cli_ref');
  const primaryEmail = q('primary_invoice_email');
  const apPhone = q('ap_phone');
  const vatChargeable = q('vat_chargeable'); // 'true'|'false'|null
  const createdFrom = q('created_from');
  const createdTo   = q('created_to');

  let url = `${env.SUPABASE_URL}/rest/v1/clients` +
            `?select=id,name,vat_chargeable,payment_terms_days,primary_invoice_email,ap_phone,cli_ref,created_at` +
            `&order=name.asc` +
            `&limit=${pageSize}&offset=${(page-1)*pageSize}`;

  if (text)        url += `&name=ilike.*${enc(text)}*`;
  if (cliRef)      url += `&cli_ref=ilike.*${enc(cliRef)}*`;
  if (primaryEmail) url += `&primary_invoice_email=ilike.*${enc(primaryEmail)}*`;
  if (apPhone)     url += `&ap_phone=ilike.*${enc(apPhone)}*`;
  if (vatChargeable === 'true')  url += `&vat_chargeable=eq.true`;
  if (vatChargeable === 'false') url += `&vat_chargeable=eq.false`;
  if (createdFrom) url += `&created_at=gte.${enc(createdFrom)}`;
  if (createdTo)   url += `&created_at=lte.${enc(createdTo)}`;

  const { rows } = await sbFetch(env, url);

  if (format === 'csv') {
    const header = ['ClientId','Name','VATChargeable','PaymentTermsDays','PrimaryInvoiceEmail','APPhone','CliRef','CreatedAt'];
    const out = [csvJoin(header)];
    for (const r of rows || []) {
      out.push(csvJoin([
        r.id, r.name || '', r.vat_chargeable ? 'Y' : 'N', Number(r.payment_terms_days ?? ''),
        r.primary_invoice_email || '', r.ap_phone || '', r.cli_ref || '', r.created_at || ''
      ]));
    }
    return withCORS(env, req, ok({ csv: out.join('\n'), count: rows?.length || 0, page, page_size: pageSize }));
  }

  if (format === 'print') {
    const rowsHtml = (rows || []).map(r => `
      <tr>
        <td>${r.name || ''}</td>
        <td>${r.vat_chargeable ? 'Y' : 'N'}</td>
        <td>${Number(r.payment_terms_days ?? '')}</td>
        <td>${r.primary_invoice_email || ''}</td>
        <td>${r.ap_phone || ''}</td>
        <td>${r.cli_ref || ''}</td>
        <td>${r.created_at || ''}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Clients — Search Results</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5">
            <th>Name</th><th>VAT Chargeable</th><th>Payment Terms (days)</th>
            <th>Primary Invoice Email</th><th>A/P Phone</th><th>Client Ref</th><th>Created At</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    return withCORS(env, req, ok({ html, count: rows?.length || 0, page, page_size: pageSize }));
  }

  return withCORS(env, req, ok({ rows, page, page_size: pageSize, count: rows?.length || 0 }));
}


// ───────────────────────────────────────────────────────────────────────────────
// SEARCH — Umbrellas (richer filters + csv/print)
// ───────────────────────────────────────────────────────────────────────────────
export async function handleSearchUmbrellas(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q = (k) => urlObj.searchParams.get(k);
  const page = Math.max(1, parseInt(q('page') || '1', 10));
  const pageSize = Math.max(1, Math.min(200, parseInt(q('page_size') || '50', 10)));
  const format = (q('format') || 'json').toLowerCase(); // 'json'|'csv'|'print'

  // Expanded filters to match FE
  const text         = q('q'); // name partial
  const bankName     = q('bank_name');
  const sortCode     = q('sort_code');
  const accountNo    = q('account_number');
  const enabled      = q('enabled');        // 'true'|'false'|null
  const vatChargeable = q('vat_chargeable'); // 'true'|'false'|null
  const createdFrom  = q('created_from');
  const createdTo    = q('created_to');

  let url = `${env.SUPABASE_URL}/rest/v1/umbrellas` +
            `?select=id,name,vat_chargeable,enabled,bank_name,sort_code,account_number,created_at` +
            `&order=name.asc` +
            `&limit=${pageSize}&offset=${(page-1)*pageSize}`;

  if (text)      url += `&name=ilike.*${enc(text)}*`;
  if (bankName)  url += `&bank_name=ilike.*${enc(bankName)}*`;
  if (sortCode)  url += `&sort_code=ilike.*${enc(sortCode)}*`;
  if (accountNo) url += `&account_number=ilike.*${enc(accountNo)}*`;
  if (enabled === 'true')  url += `&enabled=eq.true`;
  if (enabled === 'false') url += `&enabled=eq.false`;
  if (vatChargeable === 'true')  url += `&vat_chargeable=eq.true`;
  if (vatChargeable === 'false') url += `&vat_chargeable=eq.false`;
  if (createdFrom) url += `&created_at=gte.${enc(createdFrom)}`;
  if (createdTo)   url += `&created_at=lte.${enc(createdTo)}`;

  const { rows } = await sbFetch(env, url);

  if (format === 'csv') {
    const header = ['UmbrellaId','Name','Enabled','VATChargeable','Bank','SortCode','AccountNumber','CreatedAt'];
    const out = [csvJoin(header)];
    for (const r of rows || []) {
      out.push(csvJoin([
        r.id, r.name || '', r.enabled ? 'Y' : 'N', r.vat_chargeable ? 'Y' : 'N',
        r.bank_name || '', r.sort_code || '', r.account_number || '', r.created_at || ''
      ]));
    }
    return withCORS(env, req, ok({ csv: out.join('\n'), count: rows?.length || 0, page, page_size: pageSize }));
  }

  if (format === 'print') {
    const rowsHtml = (rows || []).map(r => `
      <tr>
        <td>${r.name || ''}</td>
        <td>${r.enabled ? 'Y' : 'N'}</td>
        <td>${r.vat_chargeable ? 'Y' : 'N'}</td>
        <td>${r.bank_name || ''}</td>
        <td>${r.sort_code || ''}</td>
        <td>${r.account_number || ''}</td>
        <td>${r.created_at || ''}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Umbrellas — Search Results</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5">
            <th>Name</th><th>Enabled</th><th>VAT Chargeable</th>
            <th>Bank</th><th>Sort code</th><th>Account number</th><th>Created At</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    return withCORS(env, req, ok({ html, count: rows?.length || 0, page, page_size: pageSize }));
  }

  return withCORS(env, req, ok({ rows, page, page_size: pageSize, count: rows?.length || 0 }));
}

// ───────────────────────────────────────────────────────────────────────────────
// REPORT PRESETS — Create / List / Update / Delete
// Table: report_presets (id, user_id, section, name, filters_json, is_default, is_shared, created_at, updated_at)
// ───────────────────────────────────────────────────────────────────────────────
export async function handleReportPresetsList(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q  = (k) => urlObj.searchParams.get(k);
  const page      = Math.max(1, parseInt(q('page') || '1', 10));
  const pageSize  = Math.max(1, Math.min(200, parseInt(q('page_size') || '50', 10)));
  const section   = q('section');                 // optional: filter to a section
  const kind      = q('kind');                    // optional: 'search' | 'report' | 'dashboard'
  const text      = q('q');                       // optional name search
  const includeShared = q('include_shared') === 'true';
  const orderBy   = (q('order_by') || 'created_at').toLowerCase(); // created_at|name|updated_at
  const orderDir  = (q('order_dir') || 'desc').toLowerCase() === 'asc' ? 'asc' : 'desc';

  const orderAllowed = new Set(['created_at','name','updated_at']);
  const orderExpr = `&order=${orderAllowed.has(orderBy) ? enc(orderBy) : 'created_at'}.${orderDir}`;
  const pageExpr  = `&limit=${pageSize}&offset=${(page-1)*pageSize}`;

  // 1) Fetch my presets
  let urlMine = `${env.SUPABASE_URL}/rest/v1/report_presets` +
    `?select=id,user_id,section,kind,name,filters_json,is_default,is_shared,created_at,updated_at` +
    `&user_id=eq.${enc(user.id)}`;
  if (section) urlMine += `&section=eq.${enc(section)}`;
  if (kind)    urlMine += `&kind=eq.${enc(kind)}`;
  if (text)    urlMine += `&name=ilike.*${enc(text)}*`;
  urlMine += orderExpr + pageExpr;

  const { rows: mine = [] } = await sbFetch(env, urlMine);

  // 2) Optionally fetch shared presets (not owned by user, but visible)
  let shared = [];
  if (includeShared) {
    let urlShared = `${env.SUPABASE_URL}/rest/v1/report_presets` +
      `?select=id,user_id,section,kind,name,filters_json,is_default,is_shared,created_at,updated_at` +
      `&is_shared=eq.true` +
      `&user_id=neq.${enc(user.id)}`; // 🔧 exclude my own shared presets to avoid duplicates
    if (section) urlShared += `&section=eq.${enc(section)}`;
    if (kind)    urlShared += `&kind=eq.${enc(kind)}`;
    if (text)    urlShared += `&name=ilike.*${enc(text)}*`;
    urlShared += orderExpr + pageExpr;
    const { rows } = await sbFetch(env, urlShared);
    shared = rows || [];
  }

  return withCORS(env, req, ok({
    rows: mine.concat(shared),
    page, page_size: pageSize,
    count: (mine?.length || 0) + (shared?.length || 0)
  }));
}

export async function handleReportPresetsCreate(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  let body;
  try { body = await parseJSONBody(req); } catch { return withCORS(env, req, badRequest('Invalid JSON')); }

  const section = (body?.section || '').trim();
  const name    = (body?.name || '').trim();
  const filters = body?.filters || {};
  const isDefault = !!body?.is_default;
  const isShared  = !!body?.is_shared; // keep false unless you genuinely want global visibility
  const kindRaw   = (body?.kind ?? 'search');
  const kind      = String(kindRaw).trim().toLowerCase();

  const KIND_ALLOWED = new Set(['search','report','dashboard']);

  if (!section) return withCORS(env, req, badRequest('section is required'));
  if (!name)    return withCORS(env, req, badRequest('name is required'));
  if (typeof filters !== 'object') return withCORS(env, req, badRequest('filters must be an object'));
  if (!KIND_ALLOWED.has(kind))     return withCORS(env, req, badRequest('kind must be one of search|report|dashboard'));

  // If setting as default, clear any previous defaults for this user + section + kind
  if (isDefault) {
    await fetch(
      `${env.SUPABASE_URL}/rest/v1/report_presets` +
      `?user_id=eq.${enc(user.id)}` +
      `&section=eq.${enc(section)}` +
      `&kind=eq.${enc(kind)}` +
      `&is_default=eq.true`,
      { method: 'PATCH', headers: { ...sbHeaders(env), Prefer: 'return=minimal' }, body: JSON.stringify({ is_default: false }) }
    );
  }

  // Create
  const { rows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/report_presets`,
    true, // representation
    {
      method: 'POST',
      headers: { ...sbHeaders(env), Prefer: 'return=representation' },
      body: JSON.stringify({
        user_id: user.id,
        section,
        kind,
        name,
        filters_json: filters,
        is_default: isDefault,
        is_shared: isShared
      })
    }
  );

  return withCORS(env, req, ok({ row: rows?.[0] || null }));
}
export async function handleReportPresetsUpdate(env, req, routeId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj  = new URL(req.url);
  const qsId    = urlObj.searchParams.get('id'); // optional ?id=...
  let body;
  try { body = await parseJSONBody(req); } catch { body = {}; }

  // Prefer route param, then query, then body
  const id = routeId || qsId || body?.id;
  if (!id) return withCORS(env, req, badRequest('id is required'));

  // Fetch to validate ownership and obtain existing section/kind
  const { rows: existingRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/report_presets?select=id,user_id,section,kind,is_default&id=eq.${enc(id)}`
  );
  const existing = existingRows?.[0];
  if (!existing) return withCORS(env, req, notFound('Preset not found'));
  if (existing.user_id !== user.id) return withCORS(env, req, unauthorized());

  const KIND_ALLOWED = new Set(['search','report','dashboard']);

  const patch = {};
  if (typeof body.name === 'string')    patch.name = body.name.trim();
  if (typeof body.section === 'string') patch.section = body.section.trim();
  if (body.filters && typeof body.filters === 'object') patch.filters_json = body.filters;
  if (typeof body.is_shared === 'boolean')  patch.is_shared = body.is_shared;
  if (typeof body.is_default === 'boolean') patch.is_default = body.is_default;
  if (typeof body.kind === 'string') {
    const k = body.kind.trim().toLowerCase();
    if (!KIND_ALLOWED.has(k)) return withCORS(env, req, badRequest('kind must be one of search|report|dashboard'));
    patch.kind = k;
  }

  // If becoming default, clear others for (user, section, kind)
  if (patch.is_default === true) {
    const sectionEff = patch.section || existing.section;
    const kindEff    = patch.kind    || existing.kind;
    await fetch(
      `${env.SUPABASE_URL}/rest/v1/report_presets` +
      `?user_id=eq.${enc(user.id)}` +
      `&section=eq.${enc(sectionEff)}` +
      `&kind=eq.${enc(kindEff)}` +
      `&is_default=eq.true` +
      `&id=neq.${enc(id)}`,
      { method: 'PATCH', headers: { ...sbHeaders(env), Prefer: 'return=minimal' }, body: JSON.stringify({ is_default: false }) }
    );
  }

  const { rows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/report_presets?id=eq.${enc(id)}`,
    true,
    { method: 'PATCH', headers: { ...sbHeaders(env), Prefer: 'return=representation' }, body: JSON.stringify(patch) }
  );

  return withCORS(env, req, ok({ row: rows?.[0] || null }));
}

export async function handleReportPresetsDelete(env, req, routeId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const qsId   = urlObj.searchParams.get('id'); // optional ?id=...

  // Prefer route param, then query
  const id = routeId || qsId;
  if (!id) return withCORS(env, req, badRequest('id is required'));

  // Ownership check
  const { rows: existingRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/report_presets?select=id,user_id&id=eq.${enc(id)}`
  );
  const existing = existingRows?.[0];
  if (!existing) return withCORS(env, req, notFound('Preset not found'));
  if (existing.user_id !== user.id) return withCORS(env, req, unauthorized());

  await fetch(
    `${env.SUPABASE_URL}/rest/v1/report_presets?id=eq.${enc(id)}`,
    { method: 'DELETE', headers: { ...sbHeaders(env), Prefer: 'return=minimal' } }
  );

  return withCORS(env, req, ok({ deleted_id: id }));
}

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

  // helpers
  const nowIso = new Date().toISOString();
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));
  const fmt = (n) => (n == null ? '' : Number(n).toFixed(2));
  const toNum = (v) => (v == null ? 0 : Number(v) || 0);

  try {
    // 1) Pull current ts-fin snapshots (+ joined timesheet + client) with the new fields we need
    let url = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
              `?select=` + [
                'id','timesheet_id','candidate_id','client_id','pay_method',
                'hours_day','hours_night','hours_sat','hours_sun','hours_bh',
                'pay_day','pay_night','pay_sat','pay_sun','pay_bh',
                'total_hours','total_pay_ex_vat',
                'expenses_pay_ex_vat','mileage_pay_ex_vat',
                'pay_wtr_rate_pct_snapshot','policy_snapshot_json',
                'pay_vat_rate_pct_snapshot','pay_vat_amount_snapshot','pay_total_inc_vat_snapshot',
                'remittance_send_count',
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

    // Candidate details
    const { rows: candRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${enc(candId)}&select=id,email,display_name,first_name,last_name`,
      false
    );
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

    // Determine if selection is PAYE and/or UMBRELLA (in theory a candidate is one channel, but be robust)
    const hasPAYE = finRows.some((r) => String(r.pay_method || '').toUpperCase() === 'PAYE');
    const hasUmbrella = finRows.some((r) => String(r.pay_method || '').toUpperCase() === 'UMBRELLA');

    // Load defaults (for WTR fallback)
    let defaultWTR = 0;
    {
      const { rows: defRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=holiday_pay_pct`
      );
      defaultWTR = Number(defRows?.[0]?.holiday_pay_pct ?? 0);
    }

    // WTR helper — prefer snapshot, else policy, else default
    function resolveWtrPct(row) {
      const snap = row?.pay_wtr_rate_pct_snapshot;
      if (snap !== null && snap !== undefined && Number.isFinite(Number(snap))) return Number(snap);
      const pol = row?.policy_snapshot_json || {};
      let pct = Number(pol.holiday_pay_pct ?? NaN);
      const applyTo = String(pol.apply_holiday_to || '').toUpperCase();
      if (applyTo === 'NONE') return 0;
      // If explicitly scoped to PAYE or ALL, use; otherwise fallback
      if (!Number.isFinite(pct)) return defaultWTR;
      return pct;
    }

    // 2) Build table rows with:
    // - Per band hours/rates (as before)
    // - PAYE: Basic/WTR informational split (included in pay) using WTR%
    // - UMBRELLA: VAT & inc-VAT using snapshot (or derived from rate if snapshot missing)
    let totalPayEx = 0, totalExpEx = 0, totalMilEx = 0, totalEx = 0;
    let totalWtrBasic = 0, totalWtrElem = 0;
    let totalVat = 0, totalInc = 0;

    const rowsHtml = finRows.map((r) => {
      const ts = r.timesheet || {}; const cli = r.client || {};
      const payMethod = String(r.pay_method || '').toUpperCase();

      // Ex-VAT components
      const payEx = toNum(r.total_pay_ex_vat);
      const expEx = toNum(r.expenses_pay_ex_vat);
      const milEx = toNum(r.mileage_pay_ex_vat);
      let rowEx = payEx + expEx + milEx;

      totalPayEx += payEx;
      totalExpEx += expEx;
      totalMilEx += milEx;
      totalEx += rowEx;

      // PAYE WTR split (informational)
      let wtrInfoHtml = '—';
      if (payMethod === 'PAYE') {
        const wtrPct = resolveWtrPct(r);
        const base = rowEx > 0 ? (payEx / (1 + (wtrPct / 100))) : 0; // split only the hourly pay portion
        const wtr = payEx - base;
        totalWtrBasic += base;
        totalWtrElem += wtr;
        wtrInfoHtml = `${fmt(base)} basic + ${fmt(wtr)} WTR @ ${fmt(wtrPct)}%`;
      }

      // Umbrella VAT (from snapshot if present)
      let vatHtml = '';
      let incHtml = '';
      if (payMethod === 'UMBRELLA') {
        let vatAmt = toNum(r.pay_vat_amount_snapshot);
        let incAmt = toNum(r.pay_total_inc_vat_snapshot);
        const rate = r.pay_vat_rate_pct_snapshot == null ? null : Number(r.pay_vat_rate_pct_snapshot);

        // If snapshot absent, derive from ex + rate (if available)
        if (!incAmt && rate && rate > 0) {
          vatAmt = (rowEx * rate) / 100;
          incAmt = rowEx + vatAmt;
        }
        // Prefer snapshot-consistent ex for display where possible
        if (incAmt && vatAmt) {
          // Recompute rowEx for display consistency
          const snapshotEx = incAmt - vatAmt;
          if (snapshotEx > 0) rowEx = snapshotEx;
        }

        totalVat += vatAmt;
        totalInc += incAmt;

        vatHtml = `${fmt(vatAmt)}${(rate || rate === 0) ? ` @ ${fmt(rate)}%` : ''}`;
        incHtml = `${fmt(incAmt)}`;
      }

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

          <td style="text-align:right">${fmt(payEx)}</td>
          <td style="text-align:right">${fmt(expEx)}</td>
          <td style="text-align:right">${fmt(milEx)}</td>
          <td style="text-align:right"><strong>${fmt(rowEx)}</strong></td>

          ${hasPAYE ? `<td style="text-align:right">${wtrInfoHtml}</td>` : ''}

          ${hasUmbrella ? `<td style="text-align:right">${vatHtml || '—'}</td>` : ''}
          ${hasUmbrella ? `<td style="text-align:right"><strong>${incHtml || '—'}</strong></td>` : ''}
        </tr>`;
    }).join('');

    // 3) Build HTML (header adapts to PAYE/Umbrella columns)
    const extraPAYECol = hasPAYE ? '<th align="right">Basic + WTR (info)</th>' : '';
    const extraUmbCols = hasUmbrella
      ? '<th align="right">VAT</th><th align="right">Total (inc VAT)</th>'
      : '';

    const periodTitleSuffix = hasUmbrella ? ' – Umbrella' : (hasPAYE ? ' – PAYE' : '');

    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;font-size:14px;line-height:1.4">
        <h2 style="margin:0 0 8px">Remittance Advice${periodTitleSuffix}</h2>
        <p style="margin:0 0 12px"><strong>${esc(candName)}</strong></p>
        <p style="margin:0 0 12px">Period: ${esc(periodLabel)}</p>
        <p style="margin:0 0 16px;color:#666">Generated: ${esc(nowIso)}</p>

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

              <th align="right">Pay (ex VAT)</th>
              <th align="right">Expenses</th>
              <th align="right">Mileage</th>
              <th align="right">Total (ex VAT)</th>

              ${extraPAYECol}
              ${extraUmbCols}
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
          <tfoot>
            <tr>
              <td colspan="${hasPAYE ? 19 : 18}${hasUmbrella ? '' : ''}" align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>Totals:</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${fmt(totalPayEx)}</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${fmt(totalExpEx)}</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${fmt(totalMilEx)}</strong></td>
              <td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${fmt(totalEx)}</strong></td>

              ${hasPAYE ? `<td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${fmt(totalWtrBasic)} basic + ${fmt(totalWtrElem)} WTR</strong></td>` : ''}

              ${hasUmbrella ? `<td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${fmt(totalVat)}</strong></td>` : ''}
              ${hasUmbrella ? `<td align="right" style="padding-top:10px;border-top:1px solid #e5e5e5"><strong>${fmt(totalInc)}</strong></td>` : ''}
            </tr>
          </tfoot>
        </table>

        ${hasPAYE ? `<p style="margin-top:12px;color:#666">Note: For PAYE, the pay rate is WTR-inclusive. The “Basic + WTR” split is informational only and is included in your payment.</p>` : ''}
        ${hasUmbrella ? `<p style="margin-top:8px;color:#666">Note: For Umbrella assignments where VAT applies, totals show ex VAT and inc VAT amounts using the VAT rate captured at the time of payment/lock.</p>` : ''}
      </div>`;

    // 4) Plain-text version (short, but with the new info)
    const textLines = [];
    textLines.push(`Remittance Advice${periodTitleSuffix}`);
    textLines.push(`${candName}`);
    textLines.push(`Period: ${periodLabel}`);
    textLines.push(`Generated: ${nowIso}`);
    textLines.push('');

    for (const r of finRows) {
      const ts = r.timesheet || {}; const cli = r.client || {};
      const pm = String(r.pay_method || '').toUpperCase();

      const payEx = toNum(r.total_pay_ex_vat);
      const expEx = toNum(r.expenses_pay_ex_vat);
      const milEx = toNum(r.mileage_pay_ex_vat);
      const rowEx = payEx + expEx + milEx;

      textLines.push(`WE ${ts.week_ending_date || ''} — ${cli.name || ''} / ${ts.hospital_norm || ''} / ${ts.ward_norm || ''} / ${ts.shift_label_norm || ''}`);
      textLines.push(`Day: ${fmt(r.hours_day)} @ ${fmt(r.pay_day)}, Night: ${fmt(r.hours_night)} @ ${fmt(r.pay_night)}, Sat: ${fmt(r.hours_sat)} @ ${fmt(r.pay_sat)}, Sun: ${fmt(r.hours_sun)} @ ${fmt(r.pay_sun)}, BH: ${fmt(r.hours_bh)} @ ${fmt(r.pay_bh)}`);
      textLines.push(`Pay ex VAT: ${fmt(payEx)}  |  Expenses: ${fmt(expEx)}  |  Mileage: ${fmt(milEx)}  |  Total ex VAT: ${fmt(rowEx)}`);

      if (pm === 'PAYE') {
        const wtrPct = resolveWtrPct(r);
        const base = payEx / (1 + (wtrPct / 100));
        const wtr = payEx - base;
        textLines.push(`(PAYE) Basic + WTR (info): ${fmt(base)} basic + ${fmt(wtr)} WTR @ ${fmt(wtrPct)}% (included)`);
      } else if (pm === 'UMBRELLA') {
        const vatAmt = toNum(r.pay_vat_amount_snapshot);
        const incAmt = toNum(r.pay_total_inc_vat_snapshot);
        const rate = r.pay_vat_rate_pct_snapshot == null ? null : Number(r.pay_vat_rate_pct_snapshot);
        const vatStr = rate || rate === 0 ? `VAT: ${fmt(vatAmt)} @ ${fmt(rate)}%  |  Total inc VAT: ${fmt(incAmt)}` : `VAT: ${fmt(vatAmt)}  |  Total inc VAT: ${fmt(incAmt)}`;
        textLines.push(`(Umbrella) ${vatStr}`);
      }
      textLines.push('');
    }

    textLines.push(`Totals — Pay ex VAT: ${fmt(totalPayEx)}, Expenses: ${fmt(totalExpEx)}, Mileage: ${fmt(totalMilEx)}, Total ex VAT: ${fmt(totalEx)}`);
    if (hasPAYE) textLines.push(`PAYE Basic + WTR (info totals): ${fmt(totalWtrBasic)} basic + ${fmt(totalWtrElem)} WTR`);
    if (hasUmbrella) textLines.push(`Umbrella VAT Total: ${fmt(totalVat)}  |  Total inc VAT: ${fmt(totalInc)}`);

    const text = textLines.join('\n');

    // 5) Queue email in mail_outbox
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

    // 6) Audit logs
    await writeAudit(
      env, user, 'EMAIL_QUEUED',
      {
        to: toEmail,
        subject: `Remittance Advice – ${periodLabel}`,
        period: { start: startDate || dates[0] || null, end: endDate || dates[dates.length - 1] || null },
        mail_id: mailId,
        timesheets: finRows.map((r) => r.timesheet_id)
      },
      { entity: 'candidate', subject_id: candId, reason: 'REMITTANCE', correlation_id: mailId, req }
    );
    for (const r of finRows) {
      await writeAudit(
        env, user, 'EMAIL_QUEUED',
        { to: toEmail, subject: `Remittance Advice – ${periodLabel}`, mail_id: mailId },
        { entity: 'timesheet', subject_id: r.timesheet_id, reason: 'REMITTANCE', correlation_id: mailId, req }
      );
    }

    // 7) Update remittance counters on the snapshots (timestamp + increment count)
    //    Do per-row patch to safely increment the counter value we just read.
    for (const r of finRows) {
      const newCount = (Number(r.remittance_send_count || 0) + 1);
      await fetch(
        `${env.SUPABASE_URL}/rest/v1/timesheets_financials?id=eq.${enc(r.id)}`,
        {
          method: 'PATCH',
          headers: { ...sbHeaders(env), Prefer: 'return=minimal' },
          body: JSON.stringify({
            remittance_last_sent_at_utc: nowIso,
            remittance_send_count: newCount
          })
        }
      );
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


// ---------------------- Supabase REST ----------------------
function sbHeaders(env) {
  const key = env.SUPABASE_SERVICE_ROLE_KEY;
  return { "apikey": key, "Authorization": `Bearer ${key}`, "Content-Type": "application/json" };
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
async function handleGetSettings(env, req) {
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
async function handleUpdateSettings(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized('Unauthorized');

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  // Allow new validation flags
  const allowed = [
    'timezone_id','day_start','day_end','night_start','night_end',
    'bh_source','bh_list','bh_feed_url',
    'vat_rate_pct','holiday_pay_pct','erni_pct','apply_holiday_to','apply_erni_to','margin_includes','effective_from',
    'bank_name','bank_sort_code','bank_account_number','vat_registration_number',
    // NEW
    'hr_validation_required','ts_reference_required'
  ];
  const payload = { updated_at: new Date().toISOString() };
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
async function handleCreateClient(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  try {
    // Create client first
    const clientRes = await fetch(`${env.SUPABASE_URL}/rest/v1/clients`, {
      method: "POST",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify({ ...data, created_at: new Date().toISOString() })
    });
    if (!clientRes.ok) {
      const err = await clientRes.text();
      return withCORS(env, req, badRequest(`Client creation failed: ${err}`));
    }
    const clientJson = await clientRes.json().catch(() => ({}));
    const client = Array.isArray(clientJson) ? clientJson[0] : clientJson;

    // Optionally seed client_settings with new validation flags if provided
    const csInput = {
      ...(typeof data.client_settings === 'object' ? data.client_settings : {}),
    };
    if ('hr_validation_required' in data) csInput.hr_validation_required = !!data.hr_validation_required;
    if ('ts_reference_required' in data) csInput.ts_reference_required = !!data.ts_reference_required;

    let client_settings;
    if (Object.keys(csInput).length) {
      const csPayload = {
        client_id: client.id,
        ...csInput,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      };
      const csRes = await fetch(`${env.SUPABASE_URL}/rest/v1/client_settings`, {
        method: "POST",
        headers: { ...sbHeaders(env), "Prefer": "return=representation" },
        body: JSON.stringify(csPayload)
      });
      if (!csRes.ok) {
        const err = await csRes.text();
        // don't fail client creation if settings insert fails; return warning
        return withCORS(env, req, ok({ client, warning: `Client created but client_settings insert failed: ${err}` }));
      }
      const csJson = await csRes.json().catch(() => ({}));
      client_settings = Array.isArray(csJson) ? csJson[0] : csJson;
    }

    return withCORS(env, req, ok({ client, client_settings }));
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
    // Client
    const { rows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/clients?id=eq.${encodeURIComponent(clientId)}`
    );
    if (!rows.length) return withCORS(env, req, notFound("Client not found"));
    const client = rows[0];

    // Latest client_settings (include validation flags)
    const { rows: csRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/client_settings` +
      `?client_id=eq.${encodeURIComponent(clientId)}` +
      `&select=id,client_id,vat_rate_pct,holiday_pay_pct,erni_pct,apply_holiday_to,apply_erni_to,margin_includes,effective_from,` +
      `timezone_id,day_start,day_end,night_start,night_end,bh_source,bh_list,bh_feed_url,` +
      `hr_validation_required,ts_reference_required,created_at,updated_at` +
      `&order=effective_from.desc,created_at.desc&limit=1`
    );
    const client_settings = csRows?.[0] || null;

    return withCORS(env, req, ok({ client, client_settings }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch client"));
  }
}


// --------------------------------------------------
// UPDATE CLIENT (mark stale/enqueue on policy change)
// --------------------------------------------------
// --------------------------------------------------
// UPDATE CLIENT (mark stale/enqueue on policy change)
// --------------------------------------------------
async function handleUpdateClient(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  try {
    // --- Load existing client + latest client_settings for comparison
    const { rows: beforeClientRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/clients` +
      `?id=eq.${encodeURIComponent(clientId)}` +
      `&select=vat_chargeable,payment_terms_days,mileage_charge_rate,ts_queries_email`
    );
    const beforeClient = beforeClientRows?.[0] || {};

    const { rows: beforeCsRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/client_settings` +
      `?client_id=eq.${encodeURIComponent(clientId)}` +
      `&select=id,hr_validation_required,ts_reference_required,effective_from,created_at` +
      `&order=effective_from.desc,created_at.desc&limit=1`
    );
    const beforeCs = beforeCsRows?.[0] || null;

    // --- Split incoming payload between clients and client_settings
    const csInput = {
      ...(typeof data.client_settings === 'object' ? data.client_settings : {})
    };
    if ('hr_validation_required' in data) csInput.hr_validation_required = !!data.hr_validation_required;
    if ('ts_reference_required' in data) csInput.ts_reference_required = !!data.ts_reference_required;

    const { hr_validation_required, ts_reference_required, client_settings, ...clientPatchRaw } = data;
    const clientPatch = { ...clientPatchRaw, updated_at: new Date().toISOString() };

    // --- Update clients table (only if anything other than updated_at is being set)
    const res = await fetch(
      `${env.SUPABASE_URL}/rest/v1/clients?id=eq.${encodeURIComponent(clientId)}`,
      {
        method: "PATCH",
        headers: { ...sbHeaders(env), "Prefer": "return=representation" },
        body: JSON.stringify(clientPatch)
      }
    );
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Update failed: ${err}`));
    }
    const json = await res.json().catch(() => ({}));
    const client = Array.isArray(json) ? json[0] : json;

    // --- Upsert/patch client_settings if provided
    let csChanged = false;
    let client_settings_updated = null;
    if (Object.keys(csInput).length) {
      const desired = {
        ...(beforeCs || { client_id: clientId }),
        ...csInput
      };
      const hasBefore = !!beforeCs?.id;

      // Detect change in toggles
      const beforeHr = !!(beforeCs?.hr_validation_required ?? false);
      const beforeRef = !!(beforeCs?.ts_reference_required ?? false);
      const nextHr = !!(desired.hr_validation_required ?? false);
      const nextRef = !!(desired.ts_reference_required ?? false);
      csChanged = (beforeHr !== nextHr) || (beforeRef !== nextRef);

      if (hasBefore) {
        // Patch the latest row
        const csRes = await fetch(
          `${env.SUPABASE_URL}/rest/v1/client_settings?id=eq.${encodeURIComponent(beforeCs.id)}`,
          {
            method: "PATCH",
            headers: { ...sbHeaders(env), "Prefer": "return=representation" },
            body: JSON.stringify({ ...csInput, updated_at: new Date().toISOString() })
          }
        );
        if (!csRes.ok) {
          const err = await csRes.text();
          return withCORS(env, req, badRequest(`Client settings update failed: ${err}`));
        }
        const csJson = await csRes.json().catch(() => ({}));
        client_settings_updated = Array.isArray(csJson) ? csJson[0] : csJson;
      } else {
        // Insert fresh row
        const csRes = await fetch(`${env.SUPABASE_URL}/rest/v1/client_settings`, {
          method: "POST",
          headers: { ...sbHeaders(env), "Prefer": "return=representation" },
          body: JSON.stringify({
            client_id: clientId,
            ...csInput,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
          })
        });
        if (!csRes.ok) {
          const err = await csRes.text();
          return withCORS(env, req, badRequest(`Client settings insert failed: ${err}`));
        }
        const csJson = await csRes.json().catch(() => ({}));
        client_settings_updated = Array.isArray(csJson) ? csJson[0] : csJson;
      }
    }

    // --- Change detection for clients table
    const policyChanged =
      (data.vat_chargeable != null && !!data.vat_chargeable !== !!beforeClient.vat_chargeable) ||
      (data.payment_terms_days != null && Number(data.payment_terms_days) !== Number(beforeClient.payment_terms_days));
    const mileageChargeChanged =
      (data.mileage_charge_rate != null && Number(data.mileage_charge_rate) !== Number(beforeClient.mileage_charge_rate));

    // If any client-level policy OR client_settings validation flags changed, mark stale & enqueue recompute
    if (policyChanged || mileageChargeChanged || csChanged) {
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
      const toEnqueue = (tsfins || []).map(r => ({ timesheet_id: r.timesheet_id, reason: 'POLICY_CHANGED' }));
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

    return withCORS(env, req, ok({ client, client_settings: client_settings_updated || beforeCs || null }));
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
 *   patch:
 *     summary: Update client hospital
 *     tags: [Client Hospitals]
 *     security:
 *       - bearerAuth: []
 *   delete:
 *     summary: Delete client hospital
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
    // Build safe patch: normalise name, tidy empties, block immutable fields
    const patch = { ...data };

    // Normalise hospital_name_norm if caller sent alternate keys
    if (typeof patch.hospital_name_norm === 'undefined' && (data.name || data.hospital)) {
      patch.hospital_name_norm = data.name || data.hospital;
    }

    // Treat empty string as null for optional fields
    if ('ward_hint' in patch && (patch.ward_hint === '' || patch.ward_hint == null)) {
      patch.ward_hint = null;
    }

    // Prevent updates to immutable/owner fields
    delete patch.id;
    delete patch.client_id;
    delete patch.created_at;

    patch.updated_at = new Date().toISOString();

    const url = `${env.SUPABASE_URL}/rest/v1/client_hospitals` +
                `?id=eq.${encodeURIComponent(hospitalId)}` +
                `&client_id=eq.${encodeURIComponent(clientId)}`;

    const res = await fetch(url, {
      method: "PATCH",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" },
      body: JSON.stringify(patch)
    });

    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Hospital update failed: ${err}`));
    }

    const json = await res.json().catch(() => []);
    const hospital = Array.isArray(json) ? json[0] : json;

    if (!hospital) {
      return withCORS(env, req, notFound("Hospital not found"));
    }

    return withCORS(env, req, ok({ hospital }));
  } catch {
    return withCORS(env, req, serverError("Failed to update client hospital"));
  }
}


async function handleDeleteHospital(env, req, clientId, hospitalId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const url = `${env.SUPABASE_URL}/rest/v1/client_hospitals` +
      `?id=eq.${encodeURIComponent(hospitalId)}` +
      `&client_id=eq.${encodeURIComponent(clientId)}`;

    const res = await fetch(url, {
      method: "DELETE",
      headers: { ...sbHeaders(env), "Prefer": "return=representation" }
    });

    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Hospital delete failed: ${err}`));
    }

    // With Prefer:return=representation PostgREST returns the deleted row(s)
    const json = await res.json().catch(() => []);
    if (!Array.isArray(json) || json.length === 0) {
      return withCORS(env, req, notFound("Hospital not found"));
    }

    return withCORS(env, req, ok({ deleted: true, hospital: json[0] }));
  } catch {
    return withCORS(env, req, serverError("Failed to delete client hospital"));
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
async function handleCreateUmbrella(env, req) {
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
async function handleGetUmbrella(env, req, umbrellaId) {
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

async function handleUpdateUmbrella(env, req, umbrellaId) {
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
    const watched = ['name','bank_name','sort_code','account_number'];
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
      const candIds = (candidateRows || []).map(r => r.id);
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
        const toEnqueue = (tsfins || []).map(r => ({ timesheet_id: r.timesheet_id, reason: 'CONTEXT_CHANGED' }));
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
export async function handleSearchCandidates(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const urlObj = new URL(req.url);
  const q  = (k) => urlObj.searchParams.get(k);
  const qa = (k) => urlObj.searchParams.getAll(k); // for repeated params e.g. roles_any

  const page     = Math.max(1, parseInt(q('page') || '1', 10));
  const pageSize = Math.max(1, Math.min(200, parseInt(q('page_size') || '50', 10)));
  const format   = (q('format') || 'json').toLowerCase(); // 'json'|'csv'|'print'

  // ---------- Named filters (from FE) ----------
  const firstName  = q('first_name');
  const lastName   = q('last_name');
  const email      = q('email');
  const phone      = q('phone');
  const payMethod  = q('pay_method') ? q('pay_method').toUpperCase() : null; // PAYE|UMBRELLA
  const active     = q('active'); // 'true'|'false'|null
  const createdFrom = q('created_from');
  const createdTo   = q('created_to');

  // roles_any / roles_all as REPEATED params
  let rolesAny = qa('roles_any').filter(Boolean).map(s => s.trim()).filter(Boolean);
  let rolesAll = qa('roles_all').filter(Boolean).map(s => s.trim()).filter(Boolean);

  // ---------- Back-compat: support JSON inside q= for roles_any/roles_all and optional text ----------
  const rawQ = q('q'); // may be JSON: {"roles_any":["RMN","HCA"], "roles_all":["RMN"], "text":"ann"}
  let text = null;     // display_name partial or free-text
  if (rawQ) {
    try {
      const parsed = JSON.parse(rawQ);
      if (parsed && typeof parsed === 'object') {
        if (Array.isArray(parsed.roles_any)) {
          rolesAny = rolesAny.concat(parsed.roles_any.filter(v => typeof v === 'string' && v.trim()).map(v => v.trim()));
        }
        if (Array.isArray(parsed.roles_all)) {
          rolesAll = rolesAll.concat(parsed.roles_all.filter(v => typeof v === 'string' && v.trim()).map(v => v.trim()));
        }
        if (typeof parsed.text === 'string') text = parsed.text.trim();
        else if (typeof parsed.display_name === 'string') text = parsed.display_name.trim();
        else if (typeof parsed.q === 'string') text = parsed.q.trim();
      } else {
        text = String(rawQ || '').trim();
      }
    } catch {
      text = String(rawQ || '').trim();
    }
  }

  // De-duplicate roles arrays after merging sources
  rolesAny = Array.from(new Set(rolesAny));
  rolesAll = Array.from(new Set(rolesAll));

  // ---------- Build PostgREST URL ----------
  let url =
    `${env.SUPABASE_URL}/rest/v1/candidates` +
    `?select=id,display_name,first_name,last_name,email,phone,pay_method,active,created_at` +
    `&order=display_name.asc` +
    `&limit=${pageSize}&offset=${(page - 1) * pageSize}`;

  // Free-text: by default apply to display_name; named fields (first_name etc.) are handled below
  if (text) url += `&display_name=ilike.*${enc(text)}*`;

  // Named partials
  if (firstName) url += `&first_name=ilike.*${enc(firstName)}*`;
  if (lastName)  url += `&last_name=ilike.*${enc(lastName)}*`;
  if (email)     url += `&email=ilike.*${enc(email)}*`;
  if (phone)     url += `&phone=ilike.*${enc(phone)}*`;

  // Exact-ish enumerations / booleans
  if (payMethod)    url += `&pay_method=eq.${enc(payMethod)}`;   // PAYE|UMBRELLA
  if (active === 'true')  url += `&active=eq.true`;
  if (active === 'false') url += `&active=eq.false`;

  // Created range
  if (createdFrom) url += `&created_at=gte.${enc(createdFrom)}`;
  if (createdTo)   url += `&created_at=lte.${enc(createdTo)}`;

  // roles_all (AND semantics) — repeat cs filter (roles @> '[{"code":"X"}]') for each code
  if (rolesAll.length) {
    for (const code of rolesAll) {
      const val = JSON.stringify([{ code }]); // [{"code":"RMN"}]
      url += `&roles=cs.${enc(val)}`;
    }
  }

  // roles_any (OR semantics) — or=(roles.cs.[{"code":"RMN"}],roles.cs.[{"code":"HCA"}],...)
  if (rolesAny.length) {
    const parts = rolesAny.map(code => {
      const val = enc(JSON.stringify([{ code }])); // encode [{"code":"HCA"}]
      return `roles=cs.${val}`;
    });
    url += `&or=(${parts.join(',')})`;
  }

  const { rows } = await sbFetch(env, url);

  if (format === 'csv') {
    const header = ['CandidateId','DisplayName','Email','Phone','PayMethod','Active','CreatedAt'];
    const out = [csvJoin(header)];
    for (const r of rows || []) {
      out.push(csvJoin([
        r.id,
        r.display_name || [r.first_name, r.last_name].filter(Boolean).join(' '),
        r.email || '',
        r.phone || '',
        (r.pay_method || '').toUpperCase(),
        r.active ? 'Y' : 'N',
        r.created_at || ''
      ]));
    }
    return withCORS(env, req, ok({ csv: out.join('\n'), count: rows?.length || 0, page, page_size: pageSize }));
  }

  if (format === 'print') {
    const rowsHtml = (rows || []).map(r => `
      <tr>
        <td>${r.display_name || [r.first_name, r.last_name].filter(Boolean).join(' ')}</td>
        <td>${r.email || ''}</td>
        <td>${r.phone || ''}</td>
        <td>${(r.pay_method || '').toUpperCase()}</td>
        <td>${r.active ? 'Y' : 'N'}</td>
        <td>${r.created_at || ''}</td>
      </tr>`).join('');
    const html = `
      <div style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif">
        <h3>Candidates — Search Results</h3>
        <table width="100%" cellspacing="0" cellpadding="6" style="border-collapse:collapse">
          <thead><tr style="background:#f5f5f5">
            <th>Candidate</th><th>Email</th><th>Phone</th><th>Pay Method</th><th>Active</th><th>Created At</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>`;
    return withCORS(env, req, ok({ html, count: rows?.length || 0, page, page_size: pageSize }));
  }

  return withCORS(env, req, ok({ rows, page, page_size: pageSize, count: rows?.length || 0 }));
}

export async function handleListCandidates(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  try {
    const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/candidates?select=*`);
    return withCORS(env, req, ok({ items: rows }));
  } catch {
    return withCORS(env, req, serverError("Failed to list candidates"));
  }
}

export async function handleCreateCandidate(env, req) {
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

export async function handleGetCandidate(env, req, candidateId) {
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
    let umbrella = undefined;
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

export async function handleUpdateCandidate(env, req, candidateId) {
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
    const payMethodChanged  = (data.pay_method != null) && data.pay_method !== before.pay_method;
    const umbrellaChanged   = (data.umbrella_id !== undefined) && data.umbrella_id !== before.umbrella_id;
    const mileagePayChanged = (data.mileage_pay_rate != null) && Number(data.mileage_pay_rate) !== Number(before.mileage_pay_rate);

    const bankKeys = ['account_holder','bank_name','sort_code','account_number'];
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
      const items = [];
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


// ─────────────────────────────────────────────────────────────────────────────
// Files: secure download via short-lived token
// GET /api/files/download?key=...&token=...[&filename=...]
// ─────────────────────────────────────────────────────────────────────────────
async function handleFilesDownload(env, req) {
  try {
    const url = new URL(req.url);
    const keyParam = url.searchParams.get('key');
    const token = url.searchParams.get('token');
    const overrideName = url.searchParams.get('filename') || null;

    if (!keyParam || !token) {
      return withCORS(env, req, badRequest("key and token are required"));
    }

    // Normalize the incoming key to a bare R2 key (no leading slash)
    const key = keyParam.replace(/^\/+/, '');

    // Basic key sanitisation + prefix allow-list (check after normalization)
    if (key.includes('..')) {
      return withCORS(env, req, unauthorized());
    }

    // Add stationery & rendered-docs prefixes
    const ALLOWED_PREFIXES = [
      'invoices/', 'remittances/', 'paper_ts/', 'signatures/', 'docs/',
      'docs-pdf/',                 // rendered PDFs (e.g. docs-pdf/invoices/…)
      'Assets/', 'assets/'         // stationery & other assets (PNG letterhead lives here)
    ];
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

    // Claims checks (normalize payload key as well)
    const payloadKey = (payload && typeof payload.key === 'string') ? payload.key.replace(/^\/+/, '') : '';
    if (!payload || payload.typ !== 'dl' || payloadKey !== key) {
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
// ------------------------
// LIST INVOICES (light UI)
// ------------------------
async function handleListInvoices(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized(); // keep current behavior

  const params = new URL(req.url).searchParams;

  // legacy values: "paid" | "unpaid"
  // new values: exact enum status: DRAFT | ISSUED | ON_HOLD | PAID
  const statusFilter = (params.get("status") || "").toUpperCase();
  const clientId     = params.get("client_id") || null;
  const issuedFrom   = params.get("issued_from") || null; // ISO date/time
  const issuedTo     = params.get("issued_to")   || null; // ISO date/time

  const includeCount = params.get("include_count") === "true";
  const limit  = Math.min(parseInt(params.get("limit")  || "50", 10), 200);
  const offset = Math.max(0,   parseInt(params.get("offset") || "0", 10));

  // Base select (kept identical to your original)
  const select = [
    'id','invoice_no','client_id','issued_at_utc','due_at_utc',
    'status','subtotal_ex_vat','vat_amount','total_inc_vat',
    'invoice_pdf_r2_key','header_snapshot_json'
  ].join(',');

  // Build URL w/ filters
  let url = `${env.SUPABASE_URL}/rest/v1/invoices?select=${encodeURIComponent(select)}&order=issued_at_utc.desc&limit=${limit}&offset=${offset}`;

  // Status: support both legacy "paid|unpaid" and exact enum statuses
  if (statusFilter === 'PAID') {
    url += `&status=eq.PAID`;
  } else if (statusFilter === 'UNPAID') {
    // keep legacy meaning: not yet marked as paid (no paid_at_utc) — matches your current behavior
    url += `&paid_at_utc=is.null`;
  } else if (statusFilter === 'PAID_LEGACY' || statusFilter === 'PAID_LEGACY_PLACEHOLDER') {
    // not used; kept here just to illustrate how you'd branch if you ever needed it
  } else if (statusFilter === 'DRAFT' || statusFilter === 'ISSUED' || statusFilter === 'ON_HOLD') {
    url += `&status=eq.${encodeURIComponent(statusFilter)}`;
  } else if (statusFilter === 'PAID' /* already handled above */) {
    // noop
  } else if (statusFilter === 'PAID' /* duplicate guard */) {
    // noop
  } else if (statusFilter === 'PAID' /* legacy alias 'paid' handled below */) {
    // noop
  } else if (statusFilter === 'PAID' /* keep symmetrical branches tidy */) {
    // noop
  } else {
    // legacy "paid" | "unpaid" (lowercase)
    const legacy = params.get("status");
    if (legacy === 'paid')   url += `&paid_at_utc=not.is.null`;
    if (legacy === 'unpaid') url += `&paid_at_utc=is.null`;
  }

  // Optional: filter by client
  if (clientId) {
    url += `&client_id=eq.${encodeURIComponent(clientId)}`;
  }

  // Optional: issued date range
  if (issuedFrom) url += `&issued_at_utc=gte.${encodeURIComponent(issuedFrom)}`;
  if (issuedTo)   url += `&issued_at_utc=lte.${encodeURIComponent(issuedTo)}`;

  try {
    const { rows, total } = await sbFetch(env, url, includeCount);
    const resp = includeCount ? { items: rows, count: total ?? undefined } : { items: rows };
    return withCORS(env, req, ok(resp));
  } catch (e) {
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
// -------------------
// GET INVOICE (+meta)
// -------------------
async function handleGetInvoice(env, req, invoiceId) {
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

    const items = lineRows.map(l => ({
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


// === AMENDMENT inside broker/src/index.js ===
// Replace your existing handleInvoiceRender with this version
async function handleInvoiceRender(env, req, invoiceId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  function toMarginsObj(m) {
    const dflt = { top: 32, right: 12, bottom: 20, left: 12 };
    if (Array.isArray(m) && m.length === 4) {
      return { top: Number(m[0] ?? dflt.top), right: Number(m[1] ?? dflt.right), bottom: Number(m[2] ?? dflt.bottom), left: Number(m[3] ?? dflt.left) };
    }
    if (m && typeof m === 'object') {
      return { top: Number(m.top ?? dflt.top), right: Number(m.right ?? dflt.right), bottom: Number(m.bottom ?? dflt.bottom), left: Number(m.left ?? dflt.left) };
    }
    return dflt;
  }

  try {
    // 1) Load invoice + lines
    const { rows: invRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoices` +
      `?id=eq.${encodeURIComponent(invoiceId)}` +
      `&select=id,invoice_no,issued_at_utc,due_at_utc,subtotal_ex_vat,vat_amount,total_inc_vat,header_snapshot_json`
    );
    if (!invRows?.length) return withCORS(env, req, notFound("Invoice not found"));
    const inv = invRows[0];

    const { rows: lineRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/invoice_lines` +
      `?invoice_id=eq.${encodeURIComponent(invoiceId)}` +
      `&select=id,timesheet_id,paper_ts_r2_key,description,total_charge_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,meta_json`
    );

    // 2) Stationery resolution (auto-swap PDF→PNG), signed URL under header.*
    const header = inv.header_snapshot_json || {};
    let stationeryKey =
      (typeof header.stationery_key === 'string' && header.stationery_key.trim()) ||
      env.INVOICE_STATIONERY_KEY ||
      'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
    if (/\.pdf$/i.test(stationeryKey)) {
      stationeryKey = stationeryKey.replace(/\.pdf$/i, '@300dpi.png');
    }
    stationeryKey = normalizeKey(stationeryKey);

    const stationeryUrl = presignR2Url(env, req, stationeryKey, Number(env.PRESIGN_EXPIRES_SECONDS || 600));
    const marginsObj = toMarginsObj(header.stationery_margins_mm);
    const hideBankFooter = header.hide_bank_footer === true;

    // 3) Build payload for HTML builder
    const invoiceData = {
      header: {
        ...header,
        stationery_url: stationeryUrl,
        stationery_margins_mm: marginsObj,
        hide_bank_footer: hideBankFooter,
      },
      invoice_no: inv.invoice_no || null,
      issued_at_utc: inv.issued_at_utc,
      due_at_utc: inv.due_at_utc,
      totals: {
        subtotal_ex_vat: Number(inv.subtotal_ex_vat || 0),
        vat_amount: Number(inv.vat_amount || 0),
        total_inc_vat: Number(inv.total_inc_vat || 0),
      },
      items: (lineRows || []).map(l => ({
        description: l.description,
        meta: l.meta_json ?? {},
        total_ex_vat: Number(l.total_charge_ex_vat || 0),
        vat_rate_pct: Number(l.vat_rate_pct || 0),
        vat_amount: Number(l.vat_amount || 0),
        total_inc_vat: Number(l.total_inc_vat || 0),
      })),
    };

    // 4) Build invoice HTML & render PDF in Workers Browser
    const html = buildHTML(invoiceData);
    const invoicePdfU8 = await withBrowser(env, async (browser) => {
      const page = await browser.newPage();
      await page.setContent(html, { waitUntil: 'networkidle0' });
      await page.emulateMediaType('screen');
      const pdfArrayBuffer = await page.pdf({
        format: 'a4',
        printBackground: true,
        margin: { top: 0, right: 0, bottom: 0, left: 0 },
      });
      return new Uint8Array(pdfArrayBuffer);
    });

    // 5) Ensure all timesheet PDFs exist; collect their bytes
    const tsIds = [...new Set((lineRows || []).map(r => r.timesheet_id).filter(Boolean))];
    const tsKeys = [];
    for (const tsId of tsIds) {
      const ensuredKey = await ensureTimesheetPdf(env, tsId);
      tsKeys.push(ensuredKey);

      // Update the line rows in case key was missing before
      await fetch(`${env.SUPABASE_URL}/rest/v1/invoice_lines?invoice_id=eq.${encodeURIComponent(inv.id)}&timesheet_id=eq.${encodeURIComponent(tsId)}`, {
        method: "PATCH",
        headers: { ...sbHeaders(env), "Prefer": "return=minimal" },
        body: JSON.stringify({ paper_ts_r2_key: ensuredKey })
      });
    }

    const tsBytesList = [];
    for (const k of tsKeys) {
      const bytes = await r2GetBytes(env, k);
      if (bytes) tsBytesList.push(bytes);
    }

    // 6) Merge: invoice pages first, then timesheets (single combined artifact)
    const merged = await PDFDocument.create();
    // add invoice
    const invDoc = await PDFDocument.load(invoicePdfU8);
    const invPages = await merged.copyPages(invDoc, invDoc.getPageIndices());
    invPages.forEach(p => merged.addPage(p));
    // add each TS
    for (const tsBytes of tsBytesList) {
      const tsDoc = await PDFDocument.load(tsBytes);
      const pages = await merged.copyPages(tsDoc, tsDoc.getPageIndices());
      pages.forEach(p => merged.addPage(p));
    }
    const combinedU8 = await merged.save();

    // 7) Store combined in R2 and update invoice row
    const pdfKey = normalizeKey(`docs-pdf/invoices/invoice_${invoiceId}.pdf`);
    await r2Put(env, pdfKey, combinedU8, { httpMetadata: { contentType: "application/pdf" } });

    await fetch(`${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoiceId)}`, {
      method: "PATCH",
      headers: sbHeaders(env),
      body: JSON.stringify({
        invoice_pdf_r2_key: pdfKey,
        paper_ts_r2_manifest: tsKeys, // good for audit & re-renders
        updated_at: new Date().toISOString()
      }),
    });

    // 8) Return signed URL to the combined PDF
    const token = await createToken(env.UPLOAD_TOKEN_SECRET, { typ: "dl", key: pdfKey, exp: Math.floor(Date.now()/1000) + Number(env.PRESIGN_EXPIRES_SECONDS || 600) });
    const downloadUrl = new URL(env.PUBLIC_DOWNLOAD_BASE_URL || new URL(new URL(req.url).origin + '/api/files/download').toString());
    downloadUrl.searchParams.set("key", pdfKey);
    downloadUrl.searchParams.set("token", token);

    return withCORS(env, req, ok({ pdf_url: downloadUrl.toString(), attached_timesheets: tsKeys.length }));
  } catch (e) {
    return withCORS(env, req, serverError("Failed to render invoice bundle"));
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

// ─────────────────────────────────────────────────────────────────────────────
// Rates: list client defaults (now supports optional rate_type filter)
// ─────────────────────────────────────────────────────────────────────────────
async function handleListClientRates(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const sp      = new URL(req.url).searchParams;
  const cid     = clientId || sp.get("client_id");        // optional: allow cross-client listing
  const role    = sp.get("role");                         // exact match (role is NOT NULL)
  const bandRaw = sp.get("band");                         // exact match; treat ''/null as band IS NULL
  const on      = sp.get("active_on");                    // YYYY-MM-DD (or ISO)
  const limit   = Math.min(Math.max(parseInt(sp.get('limit')  || '100', 10) || 100, 1), 500);
  const offset  = Math.max(parseInt(sp.get('offset') || '0',   10) || 0, 0);

  try {
    // Unified defaults: one row per window (paye_*, umb_*, charge_*). No rate_type filtering.
    let q = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?select=*`;

    if (cid)  q += `&client_id=eq.${encodeURIComponent(cid)}`;
    if (role) q += `&role=eq.${encodeURIComponent(role)}`;

    // Band filter: explicit '' or 'null' → IS NULL; otherwise exact match. If no band param, return all.
    if (bandRaw !== null) {
      if (bandRaw === '' || bandRaw.toLowerCase() === 'null') {
        q += `&band=is.null`;
      } else {
        q += `&band=eq.${encodeURIComponent(bandRaw)}`;
      }
    }

    // Active-on-date filter: date_from <= on AND (date_to >= on OR date_to IS NULL)
    if (on) {
      q += `&date_from=lte.${encodeURIComponent(on)}`;
      q += `&or=(date_to.gte.${encodeURIComponent(on)},date_to.is.null)`;
    }

    // Deterministic ordering; role is NOT NULL, band may be NULL
    q += `&order=date_from.desc,role.asc,band.nullsfirst&limit=${limit}&offset=${offset}`;

    const { rows } = await sbFetch(env, q);
    return withCORS(env, req, ok({ items: rows }));
  } catch {
    return withCORS(env, req, serverError("Failed to fetch client default rates"));
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// Rates: upsert client default (now requires rate_type; uniqueness includes it)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Rates: upsert client default (require rate_type; enqueue RATE_CHANGED by rate_type)
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// Rates: CLIENT DEFAULTS (UNIFIED WINDOW)
// POST /api/rates/client-defaults
// - Unified payload: one row holds 5×charge + 5×PAYE + 5×Umbrella
// - No rate_type in API
// - On insert with start N: truncate incumbent window (same category) to N−1
// - If a later window exists and new.date_to is null or overlaps, clamp new.date_to to (nextStart−1)
// - Unique key: (client_id, role, band|null, date_from)
// - Enqueue TSFIN recompute for current, unlocked rows for this client (all pay methods)
// ─────────────────────────────────────────────────────────────────────────────
async function handleUpsertClientRate(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const body = await parseJSONBody(req);
  if (!body) return withCORS(env, req, badRequest("Invalid JSON"));

  // Expect unified fields in the body
  const rec = {
    client_id: body.client_id || clientId,
    role:      body.role ?? null,
    band:      (body.band === '' ? null : body.band ?? null),
    date_from: body.date_from ?? null,
    date_to:   (body.date_to === '' ? null : body.date_to ?? null),

    // charge (5)
    charge_day:   body.charge_day   ?? null,
    charge_night: body.charge_night ?? null,
    charge_sat:   body.charge_sat   ?? null,
    charge_sun:   body.charge_sun   ?? null,
    charge_bh:    body.charge_bh    ?? null,

    // PAYE (5)
    paye_day:   body.paye_day   ?? null,
    paye_night: body.paye_night ?? null,
    paye_sat:   body.paye_sat   ?? null,
    paye_sun:   body.paye_sun   ?? null,
    paye_bh:    body.paye_bh    ?? null,

    // Umbrella (5)
    umb_day:    body.umb_day    ?? null,
    umb_night:  body.umb_night  ?? null,
    umb_sat:    body.umb_sat    ?? null,
    umb_sun:    body.umb_sun    ?? null,
    umb_bh:     body.umb_bh     ?? null
  };

  if (!rec.client_id) return withCORS(env, req, badRequest("client_id required"));
  if (!rec.role)       return withCORS(env, req, badRequest("role required"));
  if (!rec.date_from)  return withCORS(env, req, badRequest("date_from required"));

  const client_id = rec.client_id;
  const role      = rec.role;
  const band      = rec.band;
  const dateFrom  = rec.date_from;

  // Helper: encode "(band is null)" vs "band = value"
  const bandFilter = (b) => (b == null ? 'band=is.null' : `band=eq.${encodeURIComponent(b)}`);

  try {
    // 0) UPDATE path? (exact unique key exists) -> PATCH that row, skip truncation logic
    {
      let q = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
              `?client_id=eq.${encodeURIComponent(client_id)}` +
              `&role=eq.${encodeURIComponent(role)}` +
              `&${bandFilter(band)}` +
              `&date_from=eq.${encodeURIComponent(dateFrom)}` +
              `&select=id`;
      const { rows: exactRows } = await sbFetch(env, q);
      if (Array.isArray(exactRows) && exactRows.length === 1) {
        const id = exactRows[0].id;
        const res = await fetch(
          `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?id=eq.${encodeURIComponent(id)}`,
          { method: "PATCH", headers: { ...sbHeaders(env), "Prefer": "return=representation" }, body: JSON.stringify({ ...rec, updated_at: nowIso() }) }
        );
        if (!res.ok) {
          const err = await res.text();
          return withCORS(env, req, badRequest(`Rate update failed: ${err}`));
        }
        const json = await res.json().catch(() => ({}));
        const result = Array.isArray(json) ? json[0] : json;

        // Enqueue recompute for all current, unlocked rows for this client (both pay methods)
        await enqueueTsfinRecomputeForClient(env, client_id);

        return withCORS(env, req, ok({ rate: result }));
      }
    }

    // 1) INSERT path — ensure rollover behavior & no overlaps
    // 1a) Find incumbent window (same category) active at new start N = dateFrom
    {
      let qInc = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
                 `?client_id=eq.${encodeURIComponent(client_id)}` +
                 `&role=eq.${encodeURIComponent(role)}` +
                 `&${bandFilter(band)}` +
                 `&date_from=lte.${encodeURIComponent(dateFrom)}` +
                 `&or=(date_to.gte.${encodeURIComponent(dateFrom)},date_to.is.null)` +
                 `&select=id,date_from,date_to` +
                 `&order=date_from.desc&limit=2`;
      const { rows: incumbents } = await sbFetch(env, qInc);

      if (incumbents.length > 1) {
        return withCORS(env, req, badRequest("Multiple incumbent windows found at the proposed start date. Please tidy overlaps first."));
      }
      if (incumbents.length === 1) {
        const inc = incumbents[0];
        if (String(inc.date_from) === String(dateFrom)) {
          return withCORS(env, req, badRequest("A window for this role/band already starts on the same date. Edit that window instead or choose a different start."));
        }
        const cut = dayBeforeYmd(dateFrom);
        if (inc.date_from && cut < inc.date_from) {
          return withCORS(env, req, badRequest("Proposed start would back-cut before the incumbent's start. Adjust dates."));
        }
        // Truncate incumbent to N−1
        const p = await fetch(
          `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?id=eq.${encodeURIComponent(inc.id)}`,
          { method: "PATCH", headers: { ...sbHeaders(env), "Prefer": "return=representation" }, body: JSON.stringify({ date_to: cut, updated_at: nowIso() }) }
        );
        if (!p.ok) {
          const err = await p.text();
          return withCORS(env, req, badRequest(`Failed to truncate incumbent: ${err}`));
        }
      }
    }

    // 1b) If a later window exists, clamp new.date_to to the day before the next start to avoid overlap
    {
      let qNext = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
                  `?client_id=eq.${encodeURIComponent(client_id)}` +
                  `&role=eq.${encodeURIComponent(role)}` +
                  `&${bandFilter(band)}` +
                  `&date_from=gt.${encodeURIComponent(dateFrom)}` +
                  `&select=date_from` +
                  `&order=date_from.asc&limit=1`;
      const { rows: nexts } = await sbFetch(env, qNext);
      if (Array.isArray(nexts) && nexts.length === 1) {
        const nextStart = nexts[0].date_from;
        const clampTo = dayBeforeYmd(nextStart);
        if (!rec.date_to || rec.date_to > clampTo) {
          rec.date_to = clampTo;
        }
      }
    }

    // 2) Insert the new unified window
    {
      const res = await fetch(`${env.SUPABASE_URL}/rest/v1/rates_client_defaults`, {
        method: "POST",
        headers: { ...sbHeaders(env), "Prefer": "return=representation" },
        body: JSON.stringify({ ...rec, created_at: nowIso() })
      });
      if (!res.ok) {
        const err = await res.text();
        return withCORS(env, req, badRequest(`Rate insert failed: ${err}`));
      }
      const json = await res.json().catch(() => ({}));
      const result = Array.isArray(json) ? json[0] : json;

      // Enqueue recompute for all current, unlocked rows for this client (both pay methods)
      await enqueueTsfinRecomputeForClient(env, client_id);

      return withCORS(env, req, ok({ rate: result }));
    }
  } catch (e) {
    return withCORS(env, req, serverError("Failed to upsert client default window"));
  }

  // Helpers (local)
  function dayBeforeYmd(ymd) {
    if (!ymd) return null;
    const d = new Date(`${ymd}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() - 1);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${dd}`;
  }
}

async function enqueueTsfinRecomputeForClient(env, client_id) {
  try {
    const urlList =
      `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?select=timesheet_id` +
      `&client_id=eq.${encodeURIComponent(client_id)}` +
      `&is_current=eq.true` +
      `&locked_by_invoice_id=is.null`;
    const { rows: tsfins } = await sbFetch(env, urlList);
    if (tsfins && tsfins.length) {
      const now = nowIso();
      const items = tsfins.map(r => ({
        timesheet_id: r.timesheet_id,
        reason: 'RATE_CHANGED',
        attempt_count: 0,
        next_attempt_at: now,
        last_error: null,
        created_at: now,
      }));
      await fetch(
        `${env.SUPABASE_URL}/rest/v1/ts_financials_outbox?on_conflict=timesheet_id,reason`,
        {
          method: "POST",
          headers: { ...sbHeaders(env), "Prefer": "resolution=ignore-duplicates" },
          body: JSON.stringify(items)
        }
      );
    }
  } catch (_) { /* non-fatal */ }
}





// ─────────────────────────────────────────────────────────────────────────────
// Overrides: list by CLIENT (now supports optional rate_type filter)
// ─────────────────────────────────────────────────────────────────────────────
async function handleListOverridesByClient(env, req, clientId) {
  const user = await requireUser(env, req, ["admin"]);
  if (!user) return withCORS(env, req, unauthorized());

  const sp      = new URL(req.url).searchParams;
  const cid     = clientId || sp.get("client_id");
  const role    = sp.get("role");
  const band    = sp.get("band");
  const on      = sp.get("active_on");
  const rateType = sp.get("rate_type"); // optional filter
  const limit   = Math.min(Math.max(parseInt(sp.get('limit')  || '100', 10) || 100, 1), 500);
  const offset  = Math.max(parseInt(sp.get('offset') || '0',   10) || 0, 0);

  if (!cid) return withCORS(env, req, badRequest("client_id required"));

  try {
    let q = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides` +
            `?select=*,candidate:candidates(id,display_name)` +
            `&client_id=eq.${encodeURIComponent(cid)}`;

    if (rateType) q += `&rate_type=eq.${encodeURIComponent(rateType)}`;

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
// Overrides: list by CANDIDATE (now supports optional rate_type filter)
// ─────────────────────────────────────────────────────────────────────────────
async function handleListOverridesByCandidate(env, req, candidateId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const sp       = new URL(req.url).searchParams;
  const cand     = candidateId || sp.get("candidate_id");
  const role     = sp.get("role");
  const band     = sp.get("band");
  const on       = sp.get("active_on");
  const rateType = sp.get("rate_type"); // optional filter
  const limit    = Math.min(Math.max(parseInt(sp.get('limit')  || '100', 10) || 100, 1), 500);
  const offset   = Math.max(parseInt(sp.get('offset') || '0',   10) || 0, 0);

  if (!cand) return withCORS(env, req, badRequest("candidate_id required"));

  try {
    let q = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?select=*,client:clients(id,name)`;
    q += `&candidate_id=eq.${encodeURIComponent(cand)}`;
    if (rateType) q += `&rate_type=eq.${encodeURIComponent(rateType)}`;

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
// Overrides: CREATE (now requires rate_type)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Overrides: CREATE (require rate_type; enqueue RATE_CHANGED for candidate scope)
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// Overrides: CREATE (server enforces hard gate + server-side truncate)
// POST /api/rates/candidate-overrides
// - client_id REQUIRED
// - Gate: client must have an active default window for (role, band|null) at date_from
// - Truncate same-type incumbent (candidate, client, role, band, rate_type) to N−1
// - Insert new override
// - Enqueue RATE_CHANGED for current, unlocked TSFIN (candidate [+client], pay_method=rate_type)
// ─────────────────────────────────────────────────────────────────────────────
async function handleCreateOverride(env, req, candidateId, clientIdParam = null) {
  const user = await requireUser(env, req, ["admin"]);
  if (!user) return withCORS(env, req, unauthorized());

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  const candidate_id = candidateId || data.candidate_id;
  if (!candidate_id) return withCORS(env, req, badRequest("candidate_id required"));

  const rate_type = (data.rate_type || '').toUpperCase();
  if (rate_type !== 'PAYE' && rate_type !== 'UMBRELLA') {
    return withCORS(env, req, badRequest("rate_type must be 'PAYE' or 'UMBRELLA'"));
  }

  const client_id = clientIdParam || data.client_id || null;
  if (!client_id) return withCORS(env, req, badRequest("client_id required"));

  const role      = data.role || null;
  const band      = (data.band === '' ? null : data.band || null);
  const date_from = data.date_from || null;
  const date_to   = (data.date_to === '' ? null : data.date_to || null);

  if (!role)      return withCORS(env, req, badRequest("role required"));
  if (!date_from) return withCORS(env, req, badRequest("date_from required"));

  try {
    // Gate: require an active client default (unified window) for (client, role, band|null) at date_from
    {
      const gateQ =
        `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
        `?client_id=eq.${encodeURIComponent(client_id)}` +
        `&role=eq.${encodeURIComponent(role)}` +
        (band == null ? `&band=is.null` : `&band=eq.${encodeURIComponent(band)}`) +
        `&date_from=lte.${encodeURIComponent(date_from)}` +
        `&or=(date_to.gte.${encodeURIComponent(date_from)},date_to.is.null)` +
        `&select=id&limit=1`;
      const { rows: gateRows } = await sbFetch(env, gateQ);
      if (!gateRows || !gateRows.length) {
        return withCORS(env, req, badRequest("Client has no active default window for the selected role/band at the override start date."));
      }
    }

    // Server-side truncate of incumbent (same candidate+client+role+band+rate_type) active at date_from
    {
      const incQ =
        `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides` +
        `?candidate_id=eq.${encodeURIComponent(candidate_id)}` +
        `&client_id=eq.${encodeURIComponent(client_id)}` +
        `&rate_type=eq.${encodeURIComponent(rate_type)}` +
        (role ? `&role=eq.${encodeURIComponent(role)}` : `&role=is.null`) +
        (band == null ? `&band=is.null` : `&band=eq.${encodeURIComponent(band)}`) +
        `&date_from=lte.${encodeURIComponent(date_from)}` +
        `&or=(date_to.gte.${encodeURIComponent(date_from)},date_to.is.null)` +
        `&select=id,date_from,date_to&order=date_from.desc&limit=2`;

      const { rows: incumbents } = await sbFetch(env, incQ);
      if (incumbents.length > 1) {
        return withCORS(env, req, badRequest("Multiple incumbent overrides found. Please tidy overlaps first."));
      }
      if (incumbents.length === 1) {
        const inc = incumbents[0];
        if (String(inc.date_from) === String(date_from)) {
          return withCORS(env, req, badRequest("An override with the same start date already exists. Edit that override instead."));
        }
        const cut = dayBeforeYmd(date_from);
        if (inc.date_from && cut < inc.date_from) {
          return withCORS(env, req, badRequest("Proposed start would back-cut before the incumbent's start. Adjust dates."));
        }
        const p = await fetch(
          `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?id=eq.${encodeURIComponent(inc.id)}`,
          { method: "PATCH", headers: { ...sbHeaders(env), "Prefer": "return=representation" }, body: JSON.stringify({ date_to: cut, updated_at: nowIso() }) }
        );
        if (!p.ok) {
          const err = await p.text();
          return withCORS(env, req, badRequest(`Failed to truncate incumbent override: ${err}`));
        }
      }
    }

    const record = {
      candidate_id,
      client_id,
      role,
      band,
      date_from,
      date_to,
      rate_type,
      pay_day:   data.pay_day   ?? null,
      pay_night: data.pay_night ?? null,
      pay_sat:   data.pay_sat   ?? null,
      pay_sun:   data.pay_sun   ?? null,
      pay_bh:    data.pay_bh    ?? null,
      created_at: nowIso(),
    };

    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides`, {
      method: "POST",
      headers: { ...sbHeaders(env), Prefer: "return=representation" },
      body: JSON.stringify(record),
    });

    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Override creation failed: ${err}`));
    }

    const json = await res.json().catch(() => ({}));
    const override = Array.isArray(json) ? json[0] : json;

    // Enqueue recompute for this candidate (+client if provided), scoped by pay_method
    await enqueueTsfinRecomputeForCandidate(env, candidate_id, rate_type, client_id);

    return withCORS(env, req, ok({ override }));
  } catch {
    return withCORS(env, req, serverError("Failed to create override"));
  }

  function dayBeforeYmd(ymd) {
    if (!ymd) return null;
    const d = new Date(`${ymd}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() - 1);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${dd}`;
  }
}

async function enqueueTsfinRecomputeForCandidate(env, candidate_id, rate_type, client_id /* optional */) {
  try {
    let q =
      `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?select=timesheet_id` +
      `&candidate_id=eq.${encodeURIComponent(candidate_id)}` +
      `&pay_method=eq.${encodeURIComponent(rate_type)}` +
      `&is_current=eq.true` +
      `&locked_by_invoice_id=is.null`;
    if (client_id) q += `&client_id=eq.${encodeURIComponent(client_id)}`;
    const { rows: tsfins } = await sbFetch(env, q);

    if (tsfins && tsfins.length) {
      const now = nowIso();
      const items = tsfins.map(r => ({
        timesheet_id: r.timesheet_id,
        reason: 'RATE_CHANGED',
        attempt_count: 0,
        next_attempt_at: now,
        last_error: null,
        created_at: now,
      }));
      await fetch(
        `${env.SUPABASE_URL}/rest/v1/ts_financials_outbox?on_conflict=timesheet_id,reason`,
        {
          method: "POST",
          headers: { ...sbHeaders(env), "Prefer": "resolution=ignore-duplicates" },
          body: JSON.stringify(items)
        }
      );
    }
  } catch (_) { /* non-fatal */ }
}

// ─────────────────────────────────────────────────────────────────────────────
// Overrides: UPDATE (targeting now supports rate_type in the WHERE side)
// Enqueue: RATE_CHANGED for current, unlocked TSFIN scoped by rate_type (if given)
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// Overrides: UPDATE (server enforces no-overlap; gate if date_from/role/band/client changes)
// PATCH /api/rates/candidate-overrides?candidate_id=...&client_id=...&role=...&band=...&rate_type=...
// - Require client_id not to become NULL
// - If start date / role / band / client changes: gate against client defaults at new start
// - If new start overlaps same-type incumbent (excluding the row itself), truncate incumbent to N−1
// - If later same-type window would overlap new (date_to null/too far), clamp date_to to day before next-start
// - Enqueue RATE_CHANGED for current, unlocked TSFIN (candidate, optional client, pay_method if known)
// ─────────────────────────────────────────────────────────────────────────────
async function handleUpdateOverride(env, req, candidateId, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const data = await parseJSONBody(req);
  if (!data) return withCORS(env, req, badRequest("Invalid JSON"));

  const sp   = new URL(req.url).searchParams;
  const cand = candidateId || sp.get("candidate_id");
  const cidQ = (clientId !== undefined && clientId !== null) ? clientId : sp.get("client_id");
  const roleQ = sp.get("role");
  const bandQ = sp.get("band");
  const rateTypeFilter = sp.get("rate_type");

  if (!cand) return withCORS(env, req, badRequest("candidate_id required"));
  if (!cidQ && !roleQ && !bandQ && !rateTypeFilter) {
    return withCORS(env, req, badRequest("Provide at least one of client_id, role, band, or rate_type to target an override"));
  }

  if (data.rate_type) {
    const rt = String(data.rate_type).toUpperCase();
    if (rt !== 'PAYE' && rt !== 'UMBRELLA') {
      return withCORS(env, req, badRequest("rate_type must be 'PAYE' or 'UMBRELLA'"));
    }
  }

  // Query target rows first
  let selectUrl = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?candidate_id=eq.${encodeURIComponent(cand)}`;
  if (cidQ !== undefined && cidQ !== null) {
    if (String(cidQ).toLowerCase() === 'null') selectUrl += `&client_id=is.null`;
    else selectUrl += `&client_id=eq.${encodeURIComponent(cidQ)}`;
  }
  if (roleQ) {
    if (String(roleQ).toLowerCase() === 'null') selectUrl += `&role=is.null`;
    else selectUrl += `&role=eq.${encodeURIComponent(roleQ)}`;
  }
  if (bandQ) {
    if (String(bandQ).toLowerCase() === 'null') selectUrl += `&band=is.null`;
    else selectUrl += `&band=eq.${encodeURIComponent(bandQ)}`;
  }
  if (rateTypeFilter) {
    selectUrl += `&rate_type=eq.${encodeURIComponent(rateTypeFilter)}`;
  }
  selectUrl += `&select=id,candidate_id,client_id,role,band,date_from,date_to,rate_type`;

  try {
    const { rows: targets } = await sbFetch(env, selectUrl);
    if (!targets || !targets.length) {
      return withCORS(env, req, notFound("Override not found"));
    }

    const patched = [];

    for (const t of targets) {
      const newRow = {
        ...t,
        ...data,
        client_id: (data.client_id === '' ? null : (data.client_id ?? t.client_id)),
        role:      (data.role      === '' ? null : (data.role      ?? t.role)),
        band:      (data.band      === '' ? null : (data.band      ?? t.band)),
        date_from: (data.date_from === '' ? null : (data.date_from ?? t.date_from)),
        date_to:   (data.date_to   === '' ? null : (data.date_to   ?? t.date_to)),
        rate_type: (data.rate_type ? String(data.rate_type).toUpperCase() : t.rate_type)
      };

      if (!newRow.client_id) {
        return withCORS(env, req, badRequest("client_id cannot be NULL on overrides"));
      }
      if (!newRow.role) {
        return withCORS(env, req, badRequest("role required on overrides"));
      }
      if (!newRow.date_from) {
        return withCORS(env, req, badRequest("date_from required on overrides"));
      }
      // Gate against client defaults for (client, role, band|null) at new start
      {
        const gateQ =
          `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
          `?client_id=eq.${encodeURIComponent(newRow.client_id)}` +
          `&role=eq.${encodeURIComponent(newRow.role)}` +
          (newRow.band == null ? `&band=is.null` : `&band=eq.${encodeURIComponent(newRow.band)}`) +
          `&date_from=lte.${encodeURIComponent(newRow.date_from)}` +
          `&or=(date_to.gte.${encodeURIComponent(newRow.date_from)},date_to.is.null)` +
          `&select=id&limit=1`;
        const { rows: gateRows } = await sbFetch(env, gateQ);
        if (!gateRows || !gateRows.length) {
          return withCORS(env, req, badRequest("Client has no active default window for the updated role/band at the override start date."));
        }
      }

      // Truncate same-type incumbent (excluding this id) active at new start
      {
        const incQ =
          `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides` +
          `?candidate_id=eq.${encodeURIComponent(newRow.candidate_id)}` +
          `&client_id=eq.${encodeURIComponent(newRow.client_id)}` +
          `&rate_type=eq.${encodeURIComponent(newRow.rate_type)}` +
          (newRow.role ? `&role=eq.${encodeURIComponent(newRow.role)}` : `&role=is.null`) +
          (newRow.band == null ? `&band=is.null` : `&band=eq.${encodeURIComponent(newRow.band)}`) +
          `&date_from=lte.${encodeURIComponent(newRow.date_from)}` +
          `&or=(date_to.gte.${encodeURIComponent(newRow.date_from)},date_to.is.null)` +
          `&select=id,date_from,date_to&order=date_from.desc&limit=2`;
        const { rows: incs } = await sbFetch(env, incQ);

        const filtered = (incs || []).filter(r => String(r.id) !== String(t.id));
        if (filtered.length > 1) {
          return withCORS(env, req, badRequest("Multiple incumbent overrides found that would overlap. Please tidy overlaps first."));
        }
        if (filtered.length === 1) {
          const inc = filtered[0];
          if (String(inc.date_from) === String(newRow.date_from)) {
            return withCORS(env, req, badRequest("Another override already starts on the same date. Adjust dates or edit the other override."));
          }
          const cut = dayBeforeYmd(newRow.date_from);
          if (inc.date_from && cut < inc.date_from) {
            return withCORS(env, req, badRequest("Updated start would back-cut before the other override's start. Adjust dates."));
          }
          const p = await fetch(
            `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?id=eq.${encodeURIComponent(inc.id)}`,
            { method: "PATCH", headers: { ...sbHeaders(env), "Prefer": "return=representation" }, body: JSON.stringify({ date_to: cut, updated_at: nowIso() }) }
          );
          if (!p.ok) {
            const err = await p.text();
            return withCORS(env, req, badRequest(`Failed to truncate conflicting override: ${err}`));
          }
        }
      }

      // Clamp date_to if a later same-type override would overlap
      {
        let laterQ =
          `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides` +
          `?candidate_id=eq.${encodeURIComponent(newRow.candidate_id)}` +
          `&client_id=eq.${encodeURIComponent(newRow.client_id)}` +
          `&rate_type=eq.${encodeURIComponent(newRow.rate_type)}` +
          (newRow.role ? `&role=eq.${encodeURIComponent(newRow.role)}` : `&role=is.null`) +
          (newRow.band == null ? `&band=is.null` : `&band=eq.${encodeURIComponent(newRow.band)}`) +
          `&date_from=gt.${encodeURIComponent(newRow.date_from)}` +
          `&select=id,date_from&order=date_from.asc&limit=1`;
        const { rows: nexts } = await sbFetch(env, laterQ);
        if (nexts && nexts.length === 1) {
          const nextStart = nexts[0].date_from;
          const clampTo = dayBeforeYmd(nextStart);
          if (!newRow.date_to || newRow.date_to > clampTo) {
            newRow.date_to = clampTo;
          }
        }
      }

      // Persist this row by id
      const patch = {
        client_id: newRow.client_id,
        role:      newRow.role,
        band:      newRow.band,
        date_from: newRow.date_from,
        date_to:   newRow.date_to,
        rate_type: newRow.rate_type,
        pay_day:   newRow.pay_day   ?? null,
        pay_night: newRow.pay_night ?? null,
        pay_sat:   newRow.pay_sat   ?? null,
        pay_sun:   newRow.pay_sun   ?? null,
        pay_bh:    newRow.pay_bh    ?? null,
        updated_at: nowIso()
      };

      const res = await fetch(
        `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?id=eq.${encodeURIComponent(t.id)}`,
        { method: "PATCH", headers: { ...sbHeaders(env), "Prefer": "return=representation" }, body: JSON.stringify(patch) }
      );

      if (!res.ok) {
        const err = await res.text();
        return withCORS(env, req, badRequest(`Override update failed: ${err}`));
      }

      const json = await res.json().catch(() => []);
      patched.push(Array.isArray(json) ? json[0] : json);
    }

    // Enqueue recompute for affected TSFIN (if a unique rate_type emerged, use it; else enqueue both)
    const rt =
      (rateTypeFilter && rateTypeFilter.toUpperCase()) ||
      (data.rate_type && String(data.rate_type).toUpperCase()) ||
      (patched[0] && patched[0].rate_type && String(patched[0].rate_type).toUpperCase()) ||
      null;

    if (rt) {
      await enqueueTsfinRecomputeForCandidate(env, cand, rt, (cidQ && String(cidQ).toLowerCase() !== 'null') ? cidQ : null);
    } else {
      await enqueueTsfinRecomputeForCandidate(env, cand, 'PAYE',     (cidQ && String(cidQ).toLowerCase() !== 'null') ? cidQ : null);
      await enqueueTsfinRecomputeForCandidate(env, cand, 'UMBRELLA', (cidQ && String(cidQ).toLowerCase() !== 'null') ? cidQ : null);
    }

    return withCORS(env, req, ok({ override: patched[0] || null }));
  } catch (e) {
    return withCORS(env, req, serverError("Failed to update override"));
  }

  function dayBeforeYmd(ymd) {
    if (!ymd) return null;
    const d = new Date(`${ymd}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() - 1);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${dd}`;
  }
}



// ─────────────────────────────────────────────────────────────────────────────
// Overrides: DELETE (targeting now supports rate_type in the WHERE side)
// Enqueue: RATE_CHANGED for current, unlocked TSFIN scoped by rate_type (if given)
// ─────────────────────────────────────────────────────────────────────────────
async function handleDeleteOverride(env, req, candidateId, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return withCORS(env, req, unauthorized());

  const sp   = new URL(req.url).searchParams;
  const cand = candidateId || sp.get("candidate_id");
  const cid  = (clientId !== undefined && clientId !== null) ? clientId : sp.get("client_id");
  const role = sp.get("role");
  const band = sp.get("band");
  const rateTypeFilter = sp.get("rate_type"); // WHERE-side filter (optional)

  if (!cand) return withCORS(env, req, badRequest("candidate_id required"));

  // Guard to prevent mass-delete
  if (!cid && !role && !band && !rateTypeFilter) {
    return withCORS(env, req, badRequest("Provide at least one of client_id, role, band, or rate_type to target an override for delete"));
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
    if (rateTypeFilter) {
      url += `&rate_type=eq.${encodeURIComponent(rateTypeFilter)}`;
    }

    const res = await fetch(url, { method: "DELETE", headers: sbHeaders(env) });
    if (!res.ok) {
      const err = await res.text();
      return withCORS(env, req, badRequest(`Override delete failed: ${err}`));
    }

    // ── Enqueue recompute for affected TSFIN (current & unlocked), scoped by rate_type if available
    try {
      const enqRateType = rateTypeFilter ? rateTypeFilter.toUpperCase() : null;

      let qTsfin = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
                   `?select=timesheet_id` +
                   `&candidate_id=eq.${encodeURIComponent(cand)}` +
                   `&is_current=eq.true` +
                   `&locked_by_invoice_id=is.null`;
      if (enqRateType) qTsfin += `&pay_method=eq.${encodeURIComponent(enqRateType)}`;

      const { rows: tsfins } = await sbFetch(env, qTsfin);
      const toEnqueue = (tsfins || []).map(r => ({ timesheet_id: r.timesheet_id, reason: 'RATE_CHANGED' }));
      if (toEnqueue.length) {
        await fetch(`${env.SUPABASE_URL}/rest/v1/ts_financials_outbox?on_conflict=timesheet_id,reason`, {
          method: "POST",
          headers: { ...sbHeaders(env), "Prefer": "resolution=ignore-duplicates" },
          body: JSON.stringify(toEnqueue)
        });
      }
    } catch (_) { /* best-effort enqueue; ignore enqueue failures here */ }

    return withCORS(env, req, ok({ ok: true }));
  } catch {
    return withCORS(env, req, serverError("Failed to delete override"));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Rates: resolve-preview (now derives rate_type from candidate.pay_method
// if not provided; PAY from override/def filtered by rate_type; CHARGE
// from client defaults without rate_type filtering)
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// Rates: resolve-preview (UNIFIED DEFAULTS)
// - Derive rate_type from candidate if not provided
// - PAY: candidate override (same type) if present (exact band → else band-null); else from unified client default (paye_* or umb_*)
// - CHARGE: always from unified client default (exact band → else band-null)
// - Return both PAY and CHARGE even when override is used
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
  const band         = (payload.band === '' ? null : payload.band || null);
  const date         = payload.date || payload.on || null;

  if (!client_id || !candidate_id) {
    return withCORS(env, req, badRequest("client_id and candidate_id are required"));
  }
  if (!date) {
    return withCORS(env, req, badRequest("date (YYYY-MM-DD) is required"));
  }

  // Determine effective rate_type
  let rate_type = (payload.rate_type || '').toUpperCase();
  if (rate_type !== 'PAYE' && rate_type !== 'UMBRELLA') {
    try {
      const { rows: cand } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(candidate_id)}&select=pay_method&limit=1`
      );
      const pm = (cand && cand[0] && (cand[0].pay_method || '')).toUpperCase();
      rate_type = (pm === 'PAYE' || pm === 'UMBRELLA') ? pm : 'UMBRELLA';
    } catch {
      rate_type = 'UMBRELLA';
    }
  }

  try {
    // 1) Try candidate override pay (exact band → band-null)
    const override = await fetchActiveOverride(env, { candidate_id, client_id, role, band, date, rate_type });

    // 2) Charge from unified client default (exact band → band-null)
    const windowCharge = await fetchUnifiedDefaultWindow(env, { client_id, role, band, date });
    const charge = windowCharge ? pickCharge(windowCharge) : null;

    if (override) {
      return withCORS(env, req, ok({
        source: "candidate_override",
        rate_type,
        charge,
        pay: pickPay(windowCharge /* may be null */, override, rate_type, /* preferOverride */ true)
      }));
    }

    // 3) No override → pay from unified defaults (if any)
    const windowPay = windowCharge || await fetchUnifiedDefaultWindow(env, { client_id, role, band, date }); // reuse if same
    if (windowPay) {
      return withCORS(env, req, ok({
        source: "client_defaults",
        rate_type,
        charge: pickCharge(windowPay),
        pay:    pickPay(windowPay, null, rate_type, /* preferOverride */ false)
      }));
    }

    return withCORS(env, req, notFound("No applicable client default was found for pay/charge resolution."));
  } catch {
    return withCORS(env, req, serverError("Failed to resolve rates"));
  }

  // Helpers (local)
  function pickCharge(w) {
    if (!w) return null;
    return {
      day:   w.charge_day,
      night: w.charge_night,
      sat:   w.charge_sat,
      sun:   w.charge_sun,
      bh:    w.charge_bh
    };
  }

  function pickPay(windowRow, overrideRow, rt, preferOverride) {
    if (preferOverride && overrideRow) {
      return {
        day:   overrideRow.pay_day,
        night: overrideRow.pay_night,
        sat:   overrideRow.pay_sat,
        sun:   overrideRow.pay_sun,
        bh:    overrideRow.pay_bh
      };
    }
    if (!windowRow) return null;
    if (rt === 'PAYE') {
      return {
        day: windowRow.paye_day, night: windowRow.paye_night, sat: windowRow.paye_sat, sun: windowRow.paye_sun, bh: windowRow.paye_bh
      };
    }
    return {
      day: windowRow.umb_day, night: windowRow.umb_night, sat: windowRow.umb_sat, sun: windowRow.umb_sun, bh: windowRow.umb_bh
    };
  }
}

async function fetchActiveOverride(env, { candidate_id, client_id, role, band, date, rate_type }) {
  // We still allow legacy rows with client_id NULL to match (if present), but creation now requires client_id.
  let q = `${env.SUPABASE_URL}/rest/v1/rates_candidate_overrides?select=*` +
          `&candidate_id=eq.${encodeURIComponent(candidate_id)}` +
          `&rate_type=eq.${encodeURIComponent(rate_type)}` +
          `&date_from=lte.${encodeURIComponent(date)}` +
          `&or=(date_to.gte.${encodeURIComponent(date)},date_to.is.null)` +
          `&order=client_id.nullslast,role.nullslast,band.nullslast,date_from.desc&limit=1`;
  if (client_id) q += `&or=(client_id.eq.${encodeURIComponent(client_id)},client_id.is.null)`;
  if (role)      q += `&or=(role.eq.${encodeURIComponent(role)},role.is.null)`;
  if (band != null) q += `&band=eq.${encodeURIComponent(band)}`;
  // If band is null, prefer exact "band is null"; filter applied by order

  const { rows } = await sbFetch(env, q);
  return rows && rows[0] ? rows[0] : null;
}

async function fetchUnifiedDefaultWindow(env, { client_id, role, band, date }) {
  // Try exact band first
  let qExact =
    `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
    `?client_id=eq.${encodeURIComponent(client_id)}` +
    `&role=eq.${encodeURIComponent(role)}` +
    (band == null ? `&band=is.null` : `&band=eq.${encodeURIComponent(band)}`) +
    `&date_from=lte.${encodeURIComponent(date)}` +
    `&or=(date_to.gte.${encodeURIComponent(date)},date_to.is.null)` +
    `&select=*` +
    `&order=date_from.desc&limit=1`;

  const { rows: exact } = await sbFetch(env, qExact);
  if (exact && exact[0]) return exact[0];

  // If we asked for a specific band and none found, fallback to band-null
  if (band != null) {
    let qNull =
      `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
      `?client_id=eq.${encodeURIComponent(client_id)}` +
      `&role=eq.${encodeURIComponent(role)}` +
      `&band=is.null` +
      `&date_from=lte.${encodeURIComponent(date)}` +
      `&or=(date_to.gte.${encodeURIComponent(date)},date_to.is.null)` +
      `&select=*` +
      `&order=date_from.desc&limit=1`;
    const { rows: nullRows } = await sbFetch(env, qNull);
    if (nullRows && nullRows[0]) return nullRows[0];
  }

  return null;
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
  const limit  = Math.max(1, Math.min(100, parseInt(url.searchParams.get('limit')  || '20', 10)));
  const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0', 10));

  const okList = (items, total = undefined) =>
    withCORS(env, req, ok({ items, ...(typeof total === 'number' ? { total } : {}) }));

  // Helpers for week-ending (Sunday) computation without touching timesheets table
  const MS_DAY = 24 * 60 * 60 * 1000;
  const toISODate = (d) => {
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const day = String(d.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  };
  const parseDate = (s) => {
    // Accept 'YYYY-MM-DD' or ISO-like; return Date (UTC midnight for date-only)
    if (!s) return null;
    // If it's exactly YYYY-MM-DD, build UTC date at midnight
    const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (m) return new Date(Date.UTC(+m[1], +m[2]-1, +m[3]));
    const d = new Date(s);
    return isNaN(d.getTime()) ? null : d;
  };
  const computeWeekEndingSunday = (baseStr) => {
    const d = parseDate(baseStr);
    if (!d) return null;
    const dow = d.getUTCDay(); // 0=Sun
    const add = (7 - dow) % 7; // if already Sunday (0), add 0
    const sunday = new Date(d.getTime() + add * MS_DAY);
    return toISODate(sunday);
  };
  const computeWEFromRow = (r) => {
    // Priority: use existing week_ending_date if present, else any reasonable base date, else null
    const base =
      r.week_ending_date ||
      r.worked_date ||
      r.worked_from_date ||
      r.shift_date ||
      r.created_at_utc ||
      r.created_at ||
      null;
    const we = computeWeekEndingSunday(base);
    return we || null;
  };

  try {
    // -------- CANDIDATE --------
    if (entity === 'candidate') {
      if (type === 'timesheets') {
        // Only use timesheets_financials; compute week ending in Worker
        const finQ = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
          `?candidate_id=eq.${encodeURIComponent(id)}` +
          `&is_current=eq.true` +
          `&select=*`;
        const fin = await sbFetch(env, finQ, { preferExactCount: true });
        const finRows = fin.rows || [];
        const total = typeof fin.count === 'number' ? fin.count : finRows.length;

        if (!finRows.length) return okList([], total);

        const mapped = finRows.map(r => ({
          timesheet_id:     r.timesheet_id,
          week_ending_date: computeWEFromRow(r),
          processing_status: r.processing_status,
          total_pay_ex_vat:  r.total_pay_ex_vat,
          total_hours:       r.total_hours,
          client_id:         r.client_id || null,
        }));

        // Sort by computed week ending desc, then page
        mapped.sort((a, b) => {
          const ax = a.week_ending_date || '';
          const bx = b.week_ending_date || '';
          return ax < bx ? 1 : ax > bx ? -1 : 0;
        });

        const page = mapped.slice(offset, offset + limit);
        return okList(page, total);
      }

      if (type === 'invoices') {
        // Distinct invoice ids from tsfin, then fetch invoice summaries (unchanged)
        const finq = `${env.SUPABASE_URL}/rest/v1/timesheets_financials?candidate_id=eq.${encodeURIComponent(id)}&is_current=eq.true&locked_by_invoice_id=not.is.null&select=locked_by_invoice_id`;
        const fin = await sbFetch(env, finq);
        const invIds = [...new Set((fin.rows || []).map(r => r.locked_by_invoice_id).filter(Boolean))];

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
        // Use only timesheets_financials; compute week ending here; no booking_id
        const finQ = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
          `?locked_by_invoice_id=eq.${encodeURIComponent(id)}` +
          `&is_current=eq.true` +
          `&select=*`;
        const fin = await sbFetch(env, finQ, { preferExactCount: true });
        const finRows = fin.rows || [];
        const total = typeof fin.count === 'number' ? fin.count : finRows.length;

        if (!finRows.length) return okList([], total);

        const mapped = finRows.map(r => ({
          timesheet_id:     r.timesheet_id,
          week_ending_date: computeWEFromRow(r),
          processing_status: r.processing_status,
          total_pay_ex_vat:  r.total_pay_ex_vat,
          total_hours:       r.total_hours,
          candidate_id:      r.candidate_id || null,
          client_id:         r.client_id || null,
        }));

        mapped.sort((a, b) => {
          const ax = a.week_ending_date || '';
          const bx = b.week_ending_date || '';
          return ax < bx ? 1 : ax > bx ? -1 : 0;
        });

        const page = mapped.slice(offset, offset + limit);
        return okList(page, total);
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
        const audq = `${env.SUPABASE_URL}/rest/v1/audit_events?object_type=eq.invoice&object_id_text=eq.${encodeURIComponent(id)}&action=in.(EMAIL_QUEUED,EMAIL_SENT)&select=id,action,ts_utc,correlation_id&order=ts_utc.desc`;
        const aud = await sbFetch(env, audq);
        const mailIds = [...new Set((aud.rows || []).map(r => r.correlation_id).filter(Boolean))];
        const total = mailIds.length;
        const pageIds = mailIds.slice(offset, offset + limit);
        if (!pageIds.length) {
          const items = (aud.rows || []).map(a => ({ audit_id: a.id, action: a.action, ts_utc: a.ts_utc, mail_id: a.correlation_id || null }));
          return okList(items, total);
        }

        const mq = `${env.SUPABASE_URL}/rest/v1/mail_outbox?id=in.(${pageIds.map(encodeURIComponent).join(',')})&select=id,to,subject,status,created_at_utc,sent_at,reference&order=created_at_utc.desc`;
        const mr = await sbFetch(env, mq);
        const mById = new Map((mr.rows || []).map(m => [m.id, m]));
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
        // Get timesheet_ids from audit trail, then fetch current tsfin rows and compute week ending
        const audq = `${env.SUPABASE_URL}/rest/v1/audit_events?correlation_id=eq.${encodeURIComponent(id)}&reason=eq.REMITTANCE&object_type=eq.timesheet&select=object_id_text,ts_utc&order=ts_utc.desc`;
        const aud = await sbFetch(env, audq);
        const tsIds = [...new Set((aud.rows || []).map(r => r.object_id_text).filter(Boolean))];
        const total = tsIds.length;
        const pageIds = tsIds.slice(offset, offset + limit);
        if (!pageIds.length) return okList([], total);

        const tsfinQ = `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
          `?timesheet_id=in.(${pageIds.map(encodeURIComponent).join(',')})` +
          `&is_current=eq.true&select=*`;
        const tsfinR = await sbFetch(env, tsfinQ);
        const items = (tsfinR.rows || []).map(r => ({
          timesheet_id:     r.timesheet_id,
          week_ending_date: computeWEFromRow(r),
        })).sort((a,b) => {
          const ax = a.week_ending_date || '';
          const bx = b.week_ending_date || '';
          return ax < bx ? 1 : ax > bx ? -1 : 0;
        });
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
/** @typedef {'NEW_AUTHORISED'|'VERSION_ROTATED'|'REVOKED'|'RATE_CHANGED'|'POLICY_CHANGED'|'CONTEXT_CHANGED'|'MANUAL'} TsFinReason */
/** @typedef {'UNASSIGNED'|'ASSIGNED'} CandidateAssignment */
/** @typedef {'UNASSIGNED'|'CLIENT_UNRESOLVED'|'RATE_MISSING'|'PAY_CHANNEL_MISSING'|'READY_FOR_HR'|'READY_FOR_INVOICE'} ProcessingStatus */
/** @typedef {'PAYE'|'UMBRELLA'} PayMethod */

// ---------------------------
// Minimal helpers (reuse your base ones if present)
// ---------------------------


function asNumber(x, d = 0) {
  if (x === null || x === undefined) return d;
  const n = typeof x === 'number' ? x : Number(x);
  return Number.isFinite(n) ? n : d;
}



function ymd(iso) {
  const d = new Date(iso);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const da = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${da}`;
}

// --- Basic UK DST handling (same style as your base file) ---
function isBSTLocal(ymdStr) {
  const [y, m, d] = ymdStr.split('-').map(Number);
  const lastSunday = (year, month) => {
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

function toLocalParts(iso, tz) {
  // For Europe/London only; treat other tz as UTC fallback.
  const inYmd = ymd(iso);
  const offset = (tz === 'Europe/London' && isBSTLocal(inYmd)) ? 1 : 0; // hours ahead of UTC
  const d = new Date(iso);
  let hh = d.getUTCHours() + offset;
  let mm = d.getUTCMinutes();
  let y = d.getUTCFullYear();
  let m = d.getUTCMonth() + 1;
  let da = d.getUTCDate();
  if (hh >= 24) {
    hh -= 24;
    const dt = new Date(Date.UTC(y, m - 1, da));
    dt.setUTCDate(dt.getUTCDate() + 1);
    y = dt.getUTCFullYear(); m = dt.getUTCMonth() + 1; da = dt.getUTCDate();
  }
  const ymdStr = `${y}-${String(m).padStart(2, '0')}-${String(da).padStart(2, '0')}`;
  return { ymd: ymdStr, hh, mm };
}

// ---------------------------
// Supabase helpers (RPC + REST)
// ---------------------------

// === REPLACE: sbFetch (supports GET + POST/PATCH/DELETE + optional exact count)
async function sbFetch(env, url, third, fourth) {
  // Back-compat:
  //  sbFetch(env, url)                  -> simple GET
  //  sbFetch(env, url, true)            -> GET with Prefer: count=exact
  //  sbFetch(env, url, { ...init })     -> custom fetch init (method/headers/body)
  //  sbFetch(env, url, true, { ...init }) -> exact count + custom init

  let includeCount = false;
  let init = {};

  if (typeof third === 'boolean') {
    includeCount = third;
  } else if (third && typeof third === 'object') {
    // treat as fetch init; allow { preferExactCount:true } too
    const { preferExactCount, ...rest } = third;
    if (typeof preferExactCount === 'boolean') includeCount = preferExactCount;
    init = { ...rest };
  }

  if (fourth && typeof fourth === 'object') {
    init = { ...init, ...fourth };
  }

  // Merge headers; respect any explicit Prefer the caller set (e.g. return=representation)
  const callerPrefer = init.headers && (init.headers.Prefer || init.headers['Prefer']);
  const headers = {
    ...sbHeaders(env),
    ...(callerPrefer ? {} : (includeCount ? { Prefer: 'count=exact' } : {})),
    ...(init.headers || {})
  };

  const res = await fetch(url, { ...init, headers });
  const text = await res.text();

  let json;
  try { json = text ? JSON.parse(text) : null; } catch { json = null; }

  if (!res.ok) throw new Error(`Supabase fetch failed ${res.status}: ${text}`);

  // Normalise to { rows, total } like before
  let rows;
  if (Array.isArray(json)) rows = json;
  else if (json === null) rows = [];
  else if (typeof json === 'object' && Array.isArray(json.rows)) rows = json.rows;
  else rows = [json]; // fall back to single-object → [obj]

  const cr = res.headers.get('content-range');
  const m = cr && /\/(\d+)$/.exec(cr);
  const total = m ? parseInt(m[1], 10) : undefined;

  return { rows, total };
}

async function sbRpc(env, fn, args) {
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${encodeURIComponent(fn)}`;
  const res = await fetch(url, { method: 'POST', headers: sbHeaders(env), body: JSON.stringify(args || {}) });
  const txt = await res.text();
  let json;
  try { json = txt ? JSON.parse(txt) : null; } catch { json = null; }
  if (!res.ok) throw new Error(`RPC ${fn} failed ${res.status}: ${txt}`);
  return json;
}

// ---------------------------
// Context loaders
// ---------------------------
async function loadCurrentTimesheet(env, timesheet_id) {
  const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/timesheets?timesheet_id=eq.${encodeURIComponent(timesheet_id)}&is_current=eq.true&select=*`);
  return rows[0] || null;
}

async function loadCandidate(env, key_norm) {
  if (!key_norm) return null;
  const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/candidates?key_norm=eq.${encodeURIComponent(key_norm)}&active=eq.true&select=*`);
  return rows[0] || null;
}

async function resolveClientId(env, hospital_norm) {
  if (!hospital_norm) return null;
  const { rows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/client_hospitals?hospital_name_norm=eq.${encodeURIComponent(hospital_norm)}&select=client_id&limit=1`);
  return rows[0]?.client_id || null;
}

async function loadPolicy(env, client_id, workedDateYmd) {
  const { rows: defRows } = await sbFetch(env, `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=*`);
  const def = defRows[0] || {};

  let cs = null;
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
// Classification helpers
// ---------------------------
function hhmmToMin(hhmm) {
  const [h, m] = (hhmm || '00:00:00').split(':').map((x) => parseInt(x, 10) || 0);
  return h * 60 + m;
}

function subtractBreak(segments, breakStartIso, breakEndIso, breakMin) {
  if (breakStartIso && breakEndIso) {
    const bs = new Date(breakStartIso).getTime();
    const be = new Date(breakEndIso).getTime();
    const out = [];
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
  if (!segments.length) return segments;
  let [a, b] = segments[0];
  const total = minutesBetween(a, b);
  if (breakMin >= total) return [];
  const startCut = Math.floor((total - breakMin) / 2);
  const mid = new Date(new Date(a).getTime() + startCut * 60000).toISOString();
  const midEnd = new Date(new Date(mid).getTime() + breakMin * 60000).toISOString();
  return [[a, mid], [midEnd, b]];
}

function classifyMinutes(env, policy, segments) {
  const out = { day: 0, night: 0, sat: 0, sun: 0, bh: 0 };
  const tz = policy.timezone_id || 'Europe/London';
  const dayStartMin = hhmmToMin(policy.day_start);
  const dayEndMin = hhmmToMin(policy.day_end);
  const bhSet = new Set(policy.bh_list || []);

  for (const [isoA, isoB] of segments) {
    let cur = new Date(isoA);
    const end = new Date(isoB);

    while (cur < end) {
      const { ymd: curYmd, hh, mm } = toLocalParts(cur.toISOString(), tz);
      const dayEnd = new Date(Date.UTC(cur.getUTCFullYear(), cur.getUTCMonth(), cur.getUTCDate() + 1));
      const sliceEnd = end < dayEnd ? end : dayEnd;
      const mins = minutesBetween(cur.toISOString(), sliceEnd.toISOString());

      const dow = new Date(`${curYmd}T00:00:00Z`).getUTCDay(); // 0=Sun..6=Sat
      const curIsBh = bhSet.has(curYmd);

      if (curIsBh) {
        out.bh += mins;
      } else if (dow === 0) {
        out.sun += mins;
      } else if (dow === 6) {
        out.sat += mins;
      } else {
        const startLocalMin = hh * 60 + mm;
        const endLocalMin = startLocalMin + mins;
        const dayOverlap = Math.max(0, Math.min(endLocalMin, dayEndMin) - Math.max(startLocalMin, dayStartMin));
        const nightOverlap = mins - dayOverlap;
        out.day += dayOverlap;
        out.night += nightOverlap;
      }

      cur = sliceEnd;
    }
  }

  return {
    hours_day: round2(out.day / 60),
    hours_night: round2(out.night / 60),
    hours_sat: round2(out.sat / 60),
    hours_sun: round2(out.sun / 60),
    hours_bh: round2(out.bh / 60),
  };
}

// ---------------------------
// Rates resolution
// ---------------------------

// ─────────────────────────────────────────────────────────────────────────────
// Internal resolution used by worker (now derives rate_type from candidate,
// filters PAY by rate_type; selects CHARGE without rate_type)
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// Internal resolver used by the worker (UNIFIED DEFAULTS)
// - Same logic as handleResolveRate but returns a plain object (no HTTP response)
// ─────────────────────────────────────────────────────────────────────────────
async function resolveRates(env, { candidate_id, client_id, role, band, dateYmd }) {
  // Determine effective rate_type from candidate if available
  let rate_type = null;
  if (candidate_id) {
    try {
      const { rows: cand } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/candidates?id=eq.${encodeURIComponent(candidate_id)}&select=pay_method&limit=1`
      );
      const pm = (cand && cand[0] && (cand[0].pay_method || '')).toUpperCase();
      rate_type = (pm === 'PAYE' || pm === 'UMBRELLA') ? pm : 'UMBRELLA';
    } catch {
      rate_type = 'UMBRELLA';
    }
  } else {
    rate_type = 'UMBRELLA';
  }

  // 1) Candidate override PAY (exact band → band-null)
  const override = await fetchActiveOverride(env, { candidate_id, client_id, role, band, date: dateYmd, rate_type });

  // 2) Client default window for CHARGE (and PAY if needed)
  const windowDef = await fetchUnifiedDefaultWindow(env, { client_id, role, band, date: dateYmd });

  // Compose result
  const charge = windowDef ? {
    day: windowDef.charge_day, night: windowDef.charge_night, sat: windowDef.charge_sat, sun: windowDef.charge_sun, bh: windowDef.charge_bh
  } : null;

  const pay = override ? {
    day: override.pay_day, night: override.pay_night, sat: override.pay_sat, sun: override.pay_sun, bh: override.pay_bh
  } : (windowDef ? (
    rate_type === 'PAYE'
      ? { day: windowDef.paye_day, night: windowDef.paye_night, sat: windowDef.paye_sat, sun: windowDef.paye_sun, bh: windowDef.paye_bh }
      : { day: windowDef.umb_day,  night: windowDef.umb_night,  sat: windowDef.umb_sat,  sun: windowDef.umb_sun,  bh: windowDef.umb_bh  }
  ) : null);

  return {
    source: override
      ? { kind: 'CANDIDATE_OVERRIDE', id: override.id, rate_type }
      : (windowDef ? { kind: 'CLIENT_DEFAULT', id: windowDef.id, rate_type } : { kind: 'NONE', id: null, rate_type }),
    pay,
    charge
  };
}


function anyMissingRates(hours, pay, charge) {
  const buckets = ['day', 'night', 'sat', 'sun', 'bh'];
  for (const b of buckets) {
    if (hours[b] > 0) {
      if (!charge || charge[b] == null) return true;
      if (!pay || pay[b] == null) return true;
    }
  }
  return false;
}

// ---------------------------
// Snapshot writer
// ---------------------------
async function writeSnapshot(env, snapshot) {
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
const toNum = (v) => (v === null || v === undefined ? null : Number(v));
const nonneg = (n) => (n === null || n === undefined ? true : Number(n) >= 0);

async function fetchCurrentTsfin(env, timesheetId) {
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

async function enqueueManualTsfinRecalc(env, timesheetId) {
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

async function insertAuditEvent(env, req, args) {
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
 * Shared patcher (JS)
 */
async function patchTsfinCommon(env, req, timesheetId, patch) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

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

  const before = await fetchCurrentTsfin(env, timesheetId);
  if (!before) return notFound('TSFIN current row not found');
  if (before.locked_by_invoice_id) return conflict('Timesheet financials are locked by an invoice');

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

  const upd = {};

  if (patch.expenses) {
    const xp = patch.expenses;
    if (xp.pay_ex_vat !== undefined)      upd.expenses_pay_ex_vat = toNum(xp.pay_ex_vat) ?? 0;
    if (xp.charge_ex_vat !== undefined)   upd.expenses_charge_ex_vat = toNum(xp.charge_ex_vat) ?? 0;
    if (xp.description !== undefined)     upd.expenses_description = xp.description ?? null;
    if (xp.evidence_r2_key !== undefined) upd.expenses_evidence_r2_key = xp.evidence_r2_key ?? null;
  }

  if (patch.mileage) {
    const ml = patch.mileage;
    if (ml.pay_ex_vat !== undefined)      upd.mileage_pay_ex_vat = toNum(ml.pay_ex_vat) ?? 0;
    if (ml.charge_ex_vat !== undefined)   upd.mileage_charge_ex_vat = toNum(ml.charge_ex_vat) ?? 0;
    if (ml.evidence_r2_key !== undefined) upd.mileage_evidence_r2_key = ml.evidence_r2_key ?? null;

    if (ml.pay_rate !== undefined)        upd.mileage_pay_rate = ml.pay_rate === null ? null : toNum(ml.pay_rate);
    if (ml.charge_rate !== undefined)     upd.mileage_charge_rate = ml.charge_rate === null ? null : toNum(ml.charge_rate);

    if (ml.pay_rate === undefined || ml.charge_rate === undefined) {
      const needPay = ml.pay_rate === undefined;
      const needChg = ml.charge_rate === undefined;

      if (needPay || needChg) {
        let candRate = null;
        let clientRate = null;
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

  const hasFieldChange = Object.keys(upd).length > 0;
  if (!hasFieldChange) {
    return ok({ updated: false, tsfin: before });
  }

  upd.is_stale = true;
  upd.updated_at = nowIso();

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
async function handleTsfinPatchExpenses(env, req, timesheetId) {
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

async function handleTsfinPatchMileage(env, req, timesheetId) {
  const body = await parseJSONBody(req).catch(() => ({}));
  return withCORS(env, req, await patchTsfinCommon(env, req, timesheetId, {
    reason: body?.reason ?? null,
    mileage: {
      pay_ex_vat: body?.pay_ex_vat,
      charge_ex_vat: body?.charge_ex_vat,
      evidence_r2_key: body?.evidence_r2_key,
      pay_rate: body?.pay_rate,
      charge_rate: body?.charge_rate,
    }
  }));
}

async function handleTsfinPatchPO(env, req, timesheetId) {
  const body = await parseJSONBody(req).catch(() => ({}));
  return withCORS(env, req, await patchTsfinCommon(env, req, timesheetId, {
    reason: body?.reason ?? null,
    po: { number: body?.number }
  }));
}

// ---------------------------
// Pay channel resolution (pure)
// ---------------------------
function resolveEffectivePayChannel(input) {
  const pm = (input.pay_method || '').toUpperCase();
  const cand = input.candidate || {};
  const umb = input.umbrella || {};

  const trim = (v) => (v ?? '').toString().trim() || null;

  if (pm === 'PAYE') {
    const out = {
      pay_method: 'PAYE',
      source: 'CANDIDATE',
      account_holder: trim(cand.account_holder),
      bank_name: trim(cand.bank_name),
      sort_code: trim(cand.sort_code),
      account_number: trim(cand.account_number)
    };
    const missing = [];
    if (!out.sort_code) missing.push('sort_code');
    if (!out.account_number) missing.push('account_number');
    return { ...out, ok: missing.length === 0, missing };
  }

  if (pm === 'UMBRELLA') {
    const out = {
      pay_method: 'UMBRELLA',
      source: 'UMBRELLA',
      account_holder: trim(umb.name) || null,
      bank_name: trim(umb.bank_name),
      sort_code: trim(umb.sort_code),
      account_number: trim(umb.account_number)
    };
    const missing = [];
    if (!trim(cand.umbrella_id)) missing.push('umbrella_id');
    if (!out.sort_code) missing.push('sort_code');
    if (!out.account_number) missing.push('account_number');
    return { ...out, ok: missing.length === 0, missing };
  }

  return {
    pay_method: pm || null,
    source: 'MISSING',
    account_holder: null,
    bank_name: null,
    sort_code: null,
    account_number: null,
    ok: false,
    missing: ['pay_method']
  };
}

// ---------------------------
// Worker: dequeue → compute
// ---------------------------
// ---------------------------
// Worker: dequeue → compute
// ---------------------------
async function runTsfinWorkerOnce(env, { limit = 50 } = {}) {
  // NOTE: Rename RPC arg -> p_limit
  const lease = await sbRpc(env, 'tsfin_dequeue_batch', { p_limit: limit });
  if (!Array.isArray(lease) || !lease.length) return { picked: 0, ok: 0, fail: 0 };

  let ok = 0, fail = 0;

  for (const item of lease) {
    try {
      const ts = await loadCurrentTimesheet(env, item.timesheet_id);
      if (!ts) {
        // NOTE: Rename RPC args -> p_timesheet_id, p_id
        await sbRpc(env, 'tsfin_mark_revoked', { p_timesheet_id: item.timesheet_id });
        await sbRpc(env, 'tsfin_work_success', { p_id: item.id });
        ok++; continue;
      }

      // Must be authorised to proceed with financials
      if (!ts.authorised_at_server) {
        // NOTE: Rename RPC arg -> p_id
        await sbRpc(env, 'tsfin_work_success', { p_id: item.id });
        ok++; continue;
      }

      const occupantKey = ts.occupant_key_norm || null;
      const candidate = await loadCandidate(env, occupantKey);
      const candidate_assignment = candidate ? 'ASSIGNED' : 'UNASSIGNED';

      const client_id = await resolveClientId(env, ts.hospital_norm || null);
      const workedDateYmd = ts.worked_start_iso ? toLocalParts(ts.worked_start_iso, null).ymd : null;
      const policy = await loadPolicy(env, client_id, workedDateYmd); // includes time bands + rates like vat, holiday pct etc.

      // Build minutes -> hour buckets and subtract breaks
      let segments = [];
      if (ts.worked_start_iso && ts.worked_end_iso) segments.push([ts.worked_start_iso, ts.worked_end_iso]);
      segments = subtractBreak(segments, ts.break_start_iso || null, ts.break_end_iso || null, ts.break_minutes || null);

      const hours = classifyMinutes(env, policy, segments);

      // Resolve pay/charge rates (PAY filtered by rate_type=pay_method; CHARGE shared)
      const rates = await resolveRates(env, {
        candidate_id: candidate?.id || null,
        client_id,
        role: ts.job_title_norm || null,
        band: ts.band || null,
        dateYmd: workedDateYmd
      });

      const missingRates = anyMissingRates(
        { day: hours.hours_day, night: hours.hours_night, sat: hours.hours_sat, sun: hours.hours_sun, bh: hours.hours_bh },
        rates.pay,
        rates.charge
      );

      // PAY method: default from candidate with fallback (kept for compatibility)
      const pay_method =
        (candidate?.pay_method === 'UMBRELLA') ? 'UMBRELLA' :
        (candidate?.pay_method === 'PAYE') ? 'PAYE' :
        (ts.pay_method || null);

      // Processing status logic (unchanged semantics, but tightened)
      let processing_status = 'READY_FOR_HR';
      if (!candidate) processing_status = 'UNASSIGNED';
      else if (!client_id) processing_status = 'CLIENT_UNRESOLVED';
      else if (missingRates) processing_status = 'RATE_MISSING';
      else if (pay_method === 'UMBRELLA' && !candidate?.umbrella_id) processing_status = 'PAY_CHANNEL_MISSING';
      else processing_status = 'READY_FOR_HR'; // keep existing external contract

      // Totals
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

      // Determine “payment-eligibility lite” for WTR snapshot timing:
      const channelOK =
        (pay_method === 'PAYE' && hasPayeBank(candidate)) ||
        (pay_method === 'UMBRELLA' && !!candidate?.umbrella_id);

      const paymentReadyLite = !!(channelOK && !missingRates); // ts is authorised earlier

      // WTR snapshot rule: set for PAYE when effectively payment-ready; margin unchanged
      const pay_wtr_rate_pct_snapshot =
        (pay_method === 'PAYE' && paymentReadyLite) ? asNumber(policy?.holiday_pay_pct) ?? null : null;

      const snapshot = {
        timesheet_id: item.timesheet_id,
        timesheet_version: ts.version || 1,
        basis: 'SELF_REPORTED',

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

        hours_day: hours.hours_day,
        hours_night: hours.hours_night,
        hours_sat: hours.hours_sat,
        hours_sun: hours.hours_sun,
        hours_bh: hours.hours_bh,

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

        total_hours: round2(hours.hours_day + hours.hours_night + hours.hours_sat + hours.hours_sun + hours.hours_bh),
        total_pay_ex_vat,
        total_charge_ex_vat,
        margin_ex_vat,

        // Persisted hints/state
        pay_wtr_rate_pct_snapshot, // may be null; only set for PAYE when ready-lite
        candidate_assignment,
        processing_status
      };

      await writeSnapshot(env, snapshot);
      // NOTE: Rename RPC arg -> p_id
      await sbRpc(env, 'tsfin_work_success', { p_id: item.id });
      ok++;

    } catch (e) {
      // NOTE: Rename RPC args -> p_id, p_error
      await sbRpc(env, 'tsfin_work_fail', { p_id: item.id, p_error: String(e?.message || e) });
      fail++;
    }
  }

  return { picked: lease.length, ok, fail };
}

// helpers used above
function hasPayeBank(c) {
  if (!c) return false;
  return !!(c.account_number && c.sort_code); // minimal signal; holder/bank_name optional
}


// ---------------------------
// API: Manual drain
// ---------------------------
async function handleTsfinDrain(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();
  const body = await parseJSONBody(req).catch(() => null);
  const limit = Math.min(Math.max(parseInt(body?.limit || '50', 10) || 50, 1), 500);
  const res = await runTsfinWorkerOnce(env, { limit });
  return ok(res);
}

// ---------------------------
// API: Recompute (enqueue)
// ---------------------------
async function handleTsfinRecompute(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();
  const body = await parseJSONBody(req).catch(() => null);
  const ids = Array.isArray(body?.timesheet_ids) ? body.timesheet_ids.slice(0, 200) : [];
  if (!ids.length) return badRequest('timesheet_ids array required');
  for (const tsid of ids) await sbRpc(env, 'enqueue_ts_financials', { timesheet_id: tsid, reason: 'MANUAL' });
  return ok({ enqueued: ids.length });
}

// ----------------------------------------
// GET TSFIN (include exp/mileage/PO fields)
// ----------------------------------------
async function handleTsfinFinancials(env, req) {
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

  const candIds = Array.from(new Set((rows || []).map((r) => r.candidate_id).filter(Boolean)));
  let candidatesById = new Map();
  let umbrellasById = new Map();

  if (candIds.length) {
    const candParam = candIds.map(encodeURIComponent).join(',');
    const { rows: candRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/candidates` +
      `?select=id,umbrella_id,account_holder,bank_name,sort_code,account_number` +
      `&id=in.(${candParam})`
    );
    candidatesById = new Map((candRows || []).map((c) => [c.id, c]));

    const umbIds = Array.from(new Set((candRows || [])
      .map((c) => c.umbrella_id)
      .filter(Boolean)));
    if (umbIds.length) {
      const umbParam = umbIds.map(encodeURIComponent).join(',');
      const { rows: umbRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/umbrellas` +
        `?select=id,name,bank_name,sort_code,account_number` +
        `&id=in.(${umbParam})`
      );
      umbrellasById = new Map((umbRows || []).map((u) => [u.id, u]));
    }
  }

  const items = (rows || []).map((r) => {
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

// ------------------------------------------------------
// MARK READY (validate evidence rules before promotion)
// ------------------------------------------------------
// ------------------------------------------------------
// MARK READY (validate rules before promotion; supports
// settings_defaults.hr_validation_required and
// settings_defaults.ts_reference_required)
// ------------------------------------------------------
async function handleTsfinMarkReady(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized('Unauthorized');

  const payload = await parseJSONBody(req);
  if (!payload || !Array.isArray(payload.timesheet_ids) || payload.timesheet_ids.length === 0) {
    return badRequest("timesheet_ids[] required");
  }
  const ids = [...new Set(payload.timesheet_ids)].filter(Boolean);
  if (ids.length === 0) return badRequest("No valid timesheet_ids");

  const idsParam = ids.map(encodeURIComponent).join(',');

  // Load feature flags from defaults (fallbacks chosen to preserve existing behavior)
  // - hr_validation_required defaults to true (old behavior gated on validation)
  // - ts_reference_required defaults to false (new optional gating)
  let hrRequired = true;
  let tsRefRequired = false;
  try {
    const { rows: defRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=hr_validation_required,ts_reference_required`
    );
    if (defRows && defRows[0]) {
      if (typeof defRows[0].hr_validation_required === 'boolean') {
        hrRequired = defRows[0].hr_validation_required;
      }
      if (typeof defRows[0].ts_reference_required === 'boolean') {
        tsRefRequired = defRows[0].ts_reference_required;
      }
    }
  } catch {
    // if settings lookup fails, proceed with defaults above
  }

  // Optional: Latest validation per TS (only if HR validation is required)
  let latestById = new Map();
  if (hrRequired) {
    const valUrl =
      `${env.SUPABASE_URL}/rest/v1/timesheet_validations` +
      `?select=timesheet_id,status,updated_at` +
      `&timesheet_id=in.(${idsParam})` +
      `&order=timesheet_id.asc,updated_at.desc` +
      `&limit=10000`;

    const { rows: allVals } = await sbFetch(env, valUrl);
    for (const v of allVals || []) {
      if (!latestById.has(v.timesheet_id)) latestById.set(v.timesheet_id, v);
    }
  }

  // Current/unlocked TSFIN snapshots we might promote
  const { rows: tsfinRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?select=timesheet_id,processing_status,candidate_id,pay_method,` +
      `expenses_charge_ex_vat,expenses_evidence_r2_key,mileage_charge_ex_vat,mileage_evidence_r2_key` +
      `&timesheet_id=in.(${idsParam})` +
      `&is_current=eq.true` +
      `&locked_by_invoice_id=is.null`
  );

  // (If required) load timesheet reference numbers for gating
  let tsMetaMap = new Map();
  if (tsRefRequired) {
    const { rows: tsRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/timesheets` +
        `?select=timesheet_id,reference_number` +
        `&timesheet_id=in.(${idsParam})`
    );
    for (const t of tsRows || []) {
      tsMetaMap.set(t.timesheet_id, t);
    }
  }

  const eligibleIds = [];
  const blocked = [];

  // Candidate → umbrella lookups for pay-channel gating
  const candIds = Array.from(new Set((tsfinRows || []).map((r) => r.candidate_id).filter(Boolean)));
  let candidatesById = new Map();
  let umbrellasById = new Map();

  if (candIds.length) {
    const candParam = candIds.map(encodeURIComponent).join(',');
    const { rows: candRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/candidates` +
      `?select=id,umbrella_id,account_holder,bank_name,sort_code,account_number` +
      `&id=in.(${candParam})`
    );
    candidatesById = new Map((candRows || []).map((c) => [c.id, c]));

    const umbIds = Array.from(new Set((candRows || [])
      .map((c) => c.umbrella_id)
      .filter(Boolean)));
    if (umbIds.length) {
      const umbParam = umbIds.map(encodeURIComponent).join(',');
      const { rows: umbRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/umbrellas` +
        `?select=id,name,bank_name,sort_code,account_number` +
        `&id=in.(${umbParam})`
      );
      umbrellasById = new Map((umbRows || []).map((u) => [u.id, u]));
    }
  }

  const OK = new Set(['VALIDATION_OK','OVERRIDDEN']);

  for (const id of ids) {
    const row = (tsfinRows || []).find((r) => r.timesheet_id === id);
    if (!row) {
      blocked.push({ id, reason: 'tsfin_missing_or_locked' });
      continue;
    }
    if (row.processing_status !== 'READY_FOR_HR') {
      blocked.push({ id, reason: `bad_status_${row.processing_status}` });
      continue;
    }

    // 1) HR Validation gating (if required)
    if (hrRequired) {
      const v = latestById.get(id);
      if (!v || !OK.has(v.status)) {
        blocked.push({ id, reason: 'validation_not_ok' });
        continue;
      }
    }

    // 2) Evidence rules: if charge>0 → evidence required
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

    // 3) Pay-channel gating
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

    // 4) Timesheet reference gating (if required)
    if (tsRefRequired) {
      const tsMeta = tsMetaMap.get(id);
      const ref = (tsMeta?.reference_number || '').toString().trim();
      if (!ref) {
        blocked.push({ id, reason: 'reference_missing' });
        continue;
      }
    }

    // Passed all gates
    eligibleIds.push(id);
  }

  if (!eligibleIds.length) {
    return badRequest("No timesheets are eligible to mark READY_FOR_INVOICE (validation/evidence/pay-channel/reference rules failed).");
  }

  // Promote READY_FOR_HR → READY_FOR_INVOICE for just the eligible set
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
    promoted_ids: promoted.map((r) => r.timesheet_id),
    blocked_ids: blocked
  });
}

// ---------------------------
// Finance Preview (replacement that uses snapshots)
// ---------------------------

// ---------------------------------------
// FINANCE PREVIEW (now adds exp/mileage)
// ---------------------------------------
// ---------------------------------------
// FINANCE PREVIEW (now adds exp/mileage)
// ---------------------------------------
async function handleFinancePreviewTsfin(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const body = await parseJSONBody(req).catch(() => null);
  const ids = Array.isArray(body?.timesheet_ids) ? [...new Set(body.timesheet_ids)].slice(0, 200) : [];
  if (!ids.length) return badRequest('timesheet_ids array required');

  const { rows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?is_current=eq.true&timesheet_id=in.(${ids.map(encodeURIComponent).join(',')})` +
      `&select=timesheet_id,client_id,pay_method,policy_snapshot_json,pay_wtr_rate_pct_snapshot,` +
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

  // VAT context per client
  const clientIds = [...new Set(rows.map((r) => r.client_id).filter(Boolean))];
  const mapClientVat = {};
  let defaultVat = 20;
  let defaultWtr = 0;

  // Pull defaults once (VAT and WTR)
  {
    const { rows: def } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1&select=vat_rate_pct,holiday_pay_pct`
    );
    defaultVat = Number(def?.[0]?.vat_rate_pct ?? 20);
    defaultWtr = Number(def?.[0]?.holiday_pay_pct ?? 0);
  }

  if (clientIds.length) {
    const { rows: cRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/clients?select=id,vat_chargeable&id=in.(${clientIds.map(encodeURIComponent).join(',')})`
    );
    const vatChargeableById = Object.fromEntries((cRows || []).map((c) => [c.id, !!c.vat_chargeable]));

    const { rows: cs } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/client_settings?select=client_id,vat_rate_pct,effective_from&client_id=in.(${clientIds.map(encodeURIComponent).join(',')})&order=client_id.asc,effective_from.desc`
    );
    const latest = new Map();
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
    },
    // New: PAYE WTR informational split across the selection
    paye_wtr: {
      timesheets: 0,
      hours_total: 0,
      pay_inclusive_ex_vat: 0,
      basic_ex_wtr_ex_vat: 0,
      wtr_element_ex_vat: 0,
      effective_rate_pct_weighted: 0
    }
  };

  // Trackers for weighted WTR%
  let wtrWeightedNum = 0;
  let wtrWeightedDen = 0;

  for (const r of rows) {
    const h = { day: +r.hours_day || 0, night: +r.hours_night || 0, sat: +r.hours_sat || 0, sun: +r.hours_sun || 0, bh: +r.hours_bh || 0 };
    const p = { day: +r.pay_day || 0, night: +r.pay_night || 0, sat: +r.pay_sat || 0, sun: +r.pay_sun || 0, bh: +r.pay_bh || 0 };
    const c = { day: +r.charge_day || 0, night: +r.charge_night || 0, sat: +r.charge_sat || 0, sun: +r.charge_sun || 0, bh: +r.charge_bh || 0 };

    const payTotal = round2(h.day*p.day + h.night*p.night + h.sat*p.sat + h.sun*p.sun + h.bh*p.bh);
    const chgTotal = round2(h.day*c.day + h.night*c.night + h.sat*c.sat + h.sun*c.sun + h.bh*c.bh);

    const expChg = Number(r.expenses_charge_ex_vat || 0);
    const milChg = Number(r.mileage_charge_ex_vat || 0);

    const vatCtx = mapClientVat[r.client_id] || { vat_chargeable: true, vat_rate_pct: defaultVat };
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

    // ---- PAYE WTR informational split (does not affect margin) ----
    if ((r.pay_method || '').toUpperCase() === 'PAYE') {
      // Prefer the snapshot if present; else derive from policy; else fall back to default
      let wtrPct = (r.pay_wtr_rate_pct_snapshot == null) ? null : Number(r.pay_wtr_rate_pct_snapshot);
      if (wtrPct == null || !Number.isFinite(wtrPct)) {
        const pol = r.policy_snapshot_json || {};
        let polPct = Number(pol.holiday_pay_pct ?? NaN);
        const applyTo = String(pol.apply_holiday_to || '').toUpperCase();
        // If policy explicitly disables WTR, treat as zero
        if (applyTo === 'NONE') polPct = 0;
        // Only apply PAYE or ALL; otherwise zero
        if (applyTo && !['PAYE_ONLY', 'ALL', 'NONE'].includes(applyTo)) {
          // unknown -> leave as-is
        }
        wtrPct = Number.isFinite(polPct) ? polPct : defaultWtr;
      }

      const baseExWtr = payTotal / (1 + (wtrPct / 100));
      const wtrElem = payTotal - baseExWtr;

      agg.paye_wtr.timesheets += 1;
      agg.paye_wtr.hours_total += (+r.total_hours || (h.day+h.night+h.sat+h.sun+h.bh));
      agg.paye_wtr.pay_inclusive_ex_vat += payTotal;
      agg.paye_wtr.basic_ex_wtr_ex_vat += baseExWtr;
      agg.paye_wtr.wtr_element_ex_vat += wtrElem;

      if (payTotal > 0 && Number.isFinite(wtrPct)) {
        wtrWeightedNum += (wtrPct * payTotal);
        wtrWeightedDen += payTotal;
      }
    }
  }

  // Final rounding
  Object.keys(agg.totals).forEach((k) => { agg.totals[k] = round2(agg.totals[k]); });

  // Round WTR aggregates and compute weighted effective WTR%
  agg.paye_wtr.pay_inclusive_ex_vat = round2(agg.paye_wtr.pay_inclusive_ex_vat);
  agg.paye_wtr.basic_ex_wtr_ex_vat = round2(agg.paye_wtr.basic_ex_wtr_ex_vat);
  agg.paye_wtr.wtr_element_ex_vat = round2(agg.paye_wtr.wtr_element_ex_vat);
  agg.paye_wtr.hours_total = round2(agg.paye_wtr.hours_total);
  agg.paye_wtr.effective_rate_pct_weighted = round2(wtrWeightedDen > 0 ? (wtrWeightedNum / wtrWeightedDen) : 0);

  return ok(agg);
}


// ---------------------------
// Invoices (TSFIN) – create from READY_FOR_INVOICE snapshots, lock them, build invoice_lines
// ---------------------------

// REPLACE your handleCreateInvoiceTsfin with this
// -----------------------------
// CREATE INVOICE (TSFIN → INV)
// -----------------------------
// -----------------------------
// CREATE INVOICE (TSFIN → INV)
// -----------------------------
// === AMENDMENT inside broker/src/index.js ===
// Keep your existing handleCreateInvoiceTsfin; replace it fully with this version
// -----------------------------
// CREATE INVOICE (TSFIN → INV)
// Adds ts reference number into meta for hours lines
// -----------------------------
async function handleCreateInvoiceTsfin(env, req) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized('Unauthorized');

  const body = await parseJSONBody(req).catch(() => null);
  if (!body || !Array.isArray(body.timesheet_ids) || body.timesheet_ids.length === 0) {
    return badRequest("timesheet_ids[] required");
  }

  const timesheetIds = [...new Set(body.timesheet_ids)].filter(Boolean);
  if (timesheetIds.length === 0) return badRequest("No valid timesheet_ids");

  const inIds = timesheetIds.map(encodeURIComponent).join(',');

  // 1) Eligible snapshots
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

  // Must be single client
  const clientIds = [...new Set(snaps.map((s) => s.client_id).filter(Boolean))];
  if (clientIds.length !== 1) {
    return badRequest(`Expected exactly one client across snapshots, found ${clientIds.length}.`);
  }
  const client_id = clientIds[0];

  // VAT + bank defaults
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

  const vatRatePct = client.vat_chargeable === false ? 0 : Number(cs?.vat_rate_pct ?? defaultVat);

  // Base timesheet meta (now also grab reference_number for meta)
  const { rows: tsRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets` +
    `?select=timesheet_id,booking_id,week_ending_date,r2_auth_key,r2_nurse_key,reference_number` +
    `&timesheet_id=in.(${inIds})`
  );
  const tsMetaMap = Object.fromEntries((tsRows || []).map((t) => [t.timesheet_id, t]));

  // Stationery defaults (PNG)
  let DEFAULT_STATIONERY_KEY =
    env.INVOICE_STATIONERY_KEY ||
    'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
  if (/\.pdf$/i.test(DEFAULT_STATIONERY_KEY)) {
    DEFAULT_STATIONERY_KEY = DEFAULT_STATIONERY_KEY.replace(/\.pdf$/i, '@300dpi.png');
  }
  const DEFAULT_STATIONERY_MARGINS_MM = { top: 32, right: 12, bottom: 20, left: 12 };
  const DEFAULT_HIDE_BANK_FOOTER = true;

  try {
    const exists = await r2Exists(env, DEFAULT_STATIONERY_KEY);
    if (!exists) {
      console.warn(`[handleCreateInvoiceTsfin] Stationery asset missing in R2: ${DEFAULT_STATIONERY_KEY}`);
    }
  } catch {
    // non-fatal
  }

  // 3) Create header (DRAFT)
  const issuedAt = new Date().toISOString();
  const termsDays = Number(client.payment_terms_days ?? 30);
  const dueAt = new Date(Date.now() + termsDays * 86_400_000).toISOString();

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

    // stationery snapshot
    stationery_key: DEFAULT_STATIONERY_KEY,
    stationery_margins_mm: DEFAULT_STATIONERY_MARGINS_MM,
    hide_bank_footer: DEFAULT_HIDE_BANK_FOOTER,

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

  // 4) Build lines
  let sumEx = 0, sumVat = 0, sumInc = 0;
  const lines = [];

  for (const s of snaps) {
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
      ts_reference_number: (tsMeta.reference_number ?? null),
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

    // Expenses
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

    // Mileage
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
    body: JSON.stringify({ locked_by_invoice_id: invoice.id, locked_at_utc: new Date().toISOString() })
  });
  if (!lockRes.ok) {
    const t = await lockRes.text();
    return serverError(`Failed to lock snapshots: ${t}`);
  }

  // 7b) Back-fill candidate (umbrella) VAT snapshots at lock time (first action wins semantics)
  try {
    // Focus only on UM BRELLA items with a candidate
    const umbSnaps = snaps.filter(s => s.pay_method === 'UMBRELLA' && s.candidate_id);

    if (umbSnaps.length) {
      // Load candidates -> umbrellas
      const candIds = [...new Set(umbSnaps.map(s => s.candidate_id))].map(encodeURIComponent).join(',');
      const { rows: candRows } = await sbFetch(
        env,
        `${env.SUPABASE_URL}/rest/v1/candidates?select=id,umbrella_id&id=in.(${candIds})`
      );
      const candMap = Object.fromEntries((candRows || []).map(c => [c.id, c]));
      const umbrellaIds = [...new Set((candRows || []).map(c => c.umbrella_id).filter(Boolean))];
      let umbMap = {};
      if (umbrellaIds.length) {
        const umbIn = umbrellaIds.map(encodeURIComponent).join(',');
        const { rows: umbRows } = await sbFetch(
          env,
          `${env.SUPABASE_URL}/rest/v1/umbrellas?select=id,enabled,vat_chargeable&id=in.(${umbIn})`
        );
        umbMap = Object.fromEntries((umbRows || []).map(u => [u.id, u]));
      }

      // Patch each row individually with its snapshot if not already set by earlier PAY
      for (const s of umbSnaps) {
        // Skip if already set (earlier PAY path may have set it)
        const already =
          (s.pay_vat_rate_pct_snapshot != null) ||
          Number(s.pay_vat_amount_snapshot || 0) > 0 ||
          Number(s.pay_total_inc_vat_snapshot || 0) > 0;
        if (already) continue;

        const cand = candMap[s.candidate_id];
        if (!cand?.umbrella_id) continue; // no umbrella to snapshot against

        const umb = umbMap[cand.umbrella_id];
        // If umbrella not found, or disabled, or not VAT-chargeable, set neutral values (rate null, amount 0, total = ex)
        if (!umb || umb.enabled === false || umb.vat_chargeable === false) {
          const neutralPayload = {
            pay_vat_rate_pct_snapshot: null,
            pay_vat_amount_snapshot: 0,
            pay_total_inc_vat_snapshot: Number(s.total_pay_ex_vat || 0)
          };
          await fetch(
            `${env.SUPABASE_URL}/rest/v1/timesheets_financials?timesheet_id=eq.${encodeURIComponent(s.timesheet_id)}&is_current=eq.true`,
            {
              method: "PATCH",
              headers: { ...sbHeaders(env), "Prefer": "return=minimal" },
              body: JSON.stringify(neutralPayload)
            }
          );
          continue;
        }

        // VAT rate for pay-side snapshot: use policy snapshot if present, else default
        const payVatPct =
          (s.policy_snapshot_json && s.policy_snapshot_json.vat_rate_pct != null)
            ? Number(s.policy_snapshot_json.vat_rate_pct)
            : defaultVat;

        const base = Number(s.total_pay_ex_vat || 0);
        const vatAmt = round2(base * (Number(payVatPct) || 0) / 100);
        const totalInc = round2(base + vatAmt);

        const payload = {
          pay_vat_rate_pct_snapshot: Number(payVatPct),
          pay_vat_amount_snapshot: vatAmt,
          pay_total_inc_vat_snapshot: totalInc
        };

        await fetch(
          `${env.SUPABASE_URL}/rest/v1/timesheets_financials?timesheet_id=eq.${encodeURIComponent(s.timesheet_id)}&is_current=eq.true`,
          {
            method: "PATCH",
            headers: { ...sbHeaders(env), "Prefer": "return=minimal" },
            body: JSON.stringify(payload)
          }
        );
      }
    }
  } catch (e) {
    // Non-fatal: do not block invoice creation if pay-side VAT snapshotting fails
    console.warn(`[handleCreateInvoiceTsfin] umbrella VAT snapshot fill failed: ${String(e?.message || e)}`);
  }

  // Compute partial eligibility (for response diagnostics)
  const eligibleTsIdSet = new Set(snaps.map(s => s.timesheet_id));
  const skipped_timesheet_ids = timesheetIds.filter(id => !eligibleTsIdSet.has(id));

  // 8) Ensure all TS PDFs exist and write keys back to invoice_lines
  const uniqueTsIds = [...new Set(lines.map(l => l.timesheet_id).filter(Boolean))];

  const concurrency = Math.max(1, Number(env.TIMESHEET_RENDER_CONCURRENCY || 4));
  async function mapWithLimit(arr, limit, iterator) {
    let idx = 0;
    const results = new Array(arr.length);
    const workers = Array.from({ length: Math.min(limit, arr.length) }, async () => {
      while (true) {
        const current = idx++;
        if (current >= arr.length) break;
        results[current] = await iterator(arr[current], current);
      }
    });
    await Promise.all(workers);
    return results;
  }

  await mapWithLimit(uniqueTsIds, concurrency, async (tsId) => {
    const key = await ensureTimesheetPdf(env, tsId);
    await fetch(
      `${env.SUPABASE_URL}/rest/v1/invoice_lines?invoice_id=eq.${encodeURIComponent(invoice.id)}&timesheet_id=eq.${encodeURIComponent(tsId)}`,
      {
        method: "PATCH",
        headers: { ...sbHeaders(env), "Prefer": "return=minimal" },
        body: JSON.stringify({ paper_ts_r2_key: key })
      }
    );
    return key;
  });

  return ok({
    invoice_id: invoice.id,
    client_id,
    lines: lines.length,
    totals: { ex_vat: round2(sumEx), vat: round2(sumVat), inc_vat: round2(sumInc) },
    skipped_timesheet_ids
  });
}




// ---------------------------
// Credit note: create credit for an invoice and unlock associated snapshots
// ---------------------------
// ---------------------------
// Credit note: create credit for an invoice and unlock associated snapshots
// ---------------------------
async function handleCreateCreditNoteTsfin(env, req, invoiceId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  // Load the original invoice we’re crediting
  const { rows: invRows } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/invoices?id=eq.${encodeURIComponent(invoiceId)}&select=*`
  );
  const inv = invRows?.[0];
  if (!inv) return notFound('Invoice not found');

  // Pull any existing snapshot from the original invoice (preferred),
  // and top-up with sane defaults where needed so the credit note is deterministic.
  const baseHeader = inv.header_snapshot_json || {};

  // Resolve stationery key (prefer snapshot → env → fallback), and auto-swap PDF → PNG
  let stationeryKey =
    (typeof baseHeader.stationery_key === 'string' && baseHeader.stationery_key.trim()) ||
    env.INVOICE_STATIONERY_KEY ||
    'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
  if (/\.pdf$/i.test(stationeryKey)) {
    stationeryKey = stationeryKey.replace(/\.pdf$/i, '@300dpi.png');
  }
  stationeryKey = stationeryKey.replace(/^\/+/, ''); // normalize (no leading slash)

  // Normalize margins (accept array [t,r,b,l] or object {top,right,bottom,left})
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
    if (m && typeof m === 'object') {
      return {
        top: Number(m.top ?? dflt.top),
        right: Number(m.right ?? dflt.right),
        bottom: Number(m.bottom ?? dflt.bottom),
        left: Number(m.left ?? dflt.left),
      };
    }
    return dflt;
  }
  const stationeryMarginsObj = toMarginsObj(baseHeader.stationery_margins_mm);
  const hideBankFooter = baseHeader.hide_bank_footer === true || true; // default true

  // Ensure we have bank + VAT details (prefer original snapshot; else settings)
  let bank = baseHeader.bank || null;
  let vatReg = baseHeader.vat_registration_number || null;

  if (!bank || !vatReg) {
    const { rows: defRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/settings_defaults` +
        `?id=eq.1&select=bank_name,bank_sort_code,bank_account_number,vat_registration_number`
    );
    const def = defRows?.[0] || {};
    bank = bank || {
      name: def.bank_name ?? null,
      sort_code: def.bank_sort_code ?? null,
      account_number: def.bank_account_number ?? null,
    };
    vatReg = vatReg || def.vat_registration_number || null;
  }

  // Ensure client info exists (prefer snapshot; else fetch)
  let clientName = baseHeader.client_name || null;
  let clientAddr = baseHeader.client_invoice_address || null;
  let clientEmail = baseHeader.client_primary_invoice_email || null;
  let vatChargeable =
    typeof baseHeader.vat_chargeable === 'boolean' ? baseHeader.vat_chargeable : true;
  let termsDays =
    typeof baseHeader.payment_terms_days === 'number'
      ? baseHeader.payment_terms_days
      : 30;
  let appliedVatPct =
    typeof baseHeader.applied_vat_rate_pct === 'number'
      ? baseHeader.applied_vat_rate_pct
      : 0;

  if (!clientName || !clientAddr || clientEmail == null) {
    const { rows: cliRows } = await sbFetch(
      env,
      `${env.SUPABASE_URL}/rest/v1/clients` +
        `?select=id,name,invoice_address,primary_invoice_email,vat_chargeable,payment_terms_days` +
        `&id=eq.${encodeURIComponent(inv.client_id)}`
    );
    const cli = cliRows?.[0] || {};
    clientName = clientName || cli.name || null;
    clientAddr = clientAddr || cli.invoice_address || null;
    clientEmail = clientEmail ?? cli.primary_invoice_email ?? null;
    if (typeof cli.vat_chargeable === 'boolean') vatChargeable = cli.vat_chargeable;
    if (typeof cli.payment_terms_days === 'number') termsDays = cli.payment_terms_days;
  }

  // Issue & (optional) due dates for the credit note
  const now = new Date().toISOString();
  const dueAt = new Date(Date.now() + (termsDays || 0) * 86_400_000).toISOString();

  // Build a snapshot for the credit note so future re-renders remain identical
  const header_snapshot_json = {
    client_id: inv.client_id,
    client_name: clientName,
    client_invoice_address: clientAddr,
    client_primary_invoice_email: clientEmail,
    vat_chargeable: !!vatChargeable,
    applied_vat_rate_pct: Number(appliedVatPct || 0),
    payment_terms_days: Number(termsDays || 0),
    issued_at_utc: now,
    due_at_utc: dueAt,

    // Stationery snapshot (PNG key + margins + footer policy)
    stationery_key: stationeryKey,
    stationery_margins_mm: stationeryMarginsObj,
    hide_bank_footer: !!hideBankFooter,

    bank,
    vat_registration_number: vatReg,

    // Tag this snapshot as a credit note and reference the original invoice
    meta: {
      source: "CREDIT_NOTE",
      original_invoice_id: inv.id
    }
  };

  // Create the credit note row (snapshot included)
  const cnRes = await fetch(`${env.SUPABASE_URL}/rest/v1/invoices`, {
    method: 'POST',
    headers: { ...sbHeaders(env), Prefer: 'return=representation' },
    body: JSON.stringify({
      client_id: inv.client_id,
      type: 'CREDIT_NOTE',
      status: 'ISSUED',
      issued_at_utc: now,
      original_invoice_id: inv.id,
      header_snapshot_json
    })
  });
  if (!cnRes.ok) {
    const t = await cnRes.text();
    return serverError(`Credit note create failed: ${t}`);
  }
  const cnJson = await cnRes.json().catch(() => ([]));
  const credit = Array.isArray(cnJson) ? cnJson[0] : cnJson;

  // Unlock snapshots that were locked by the original invoice
  const { rows: snaps } = await sbFetch(
    env,
    `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?is_current=eq.true&locked_by_invoice_id=eq.${encodeURIComponent(invoiceId)}` +
      `&select=timesheet_id`
  );

  if (snaps.length) {
    const url =
      `${env.SUPABASE_URL}/rest/v1/timesheets_financials` +
      `?is_current=eq.true&locked_by_invoice_id=eq.${encodeURIComponent(invoiceId)}`;
    const body = {
      locked_by_invoice_id: null,
      locked_at_utc: null,
      unlocked_by_credit_note_id: credit.id,
      is_stale: true,
      stale_reason: 'UNLOCKED_BY_CREDIT'
    };
    const res = await fetch(url, {
      method: 'PATCH',
      headers: sbHeaders(env),
      body: JSON.stringify(body)
    });
    if (!res.ok) {
      const t = await res.text();
      return serverError(`Unlock failed: ${t}`);
    }

    // Enqueue recompute for those timesheets
    for (const r of snaps) {
      await sbRpc(env, 'enqueue_ts_financials', {
        timesheet_id: r.timesheet_id,
        reason: 'VERSION_ROTATED'
      });
    }
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
      // ====================== AUTH ======================
      if (req.method === 'POST' && p === '/auth/login')   return withCORS(env, req, await handleAuthLogin(env, req));
      if (req.method === 'POST' && p === '/auth/refresh') return withCORS(env, req, await handleAuthRefresh(env, req));
      if (req.method === 'POST' && p === '/auth/logout')  return withCORS(env, req, await handleAuthLogout(env, req));
      if (req.method === 'POST' && p === '/auth/forgot')  return withCORS(env, req, await handleAuthForgot(env, req));
      if (req.method === 'POST' && p === '/auth/reset')   return withCORS(env, req, await handleAuthReset(env, req));

      // ====================== HEALTH ======================
      if (req.method === "GET" && p === "/healthz") return handleHealth(env);
      if (req.method === "GET" && p === "/readyz")  return handleReady(env);
      if (req.method === "GET" && p === "/version") return handleVersion();

      // ====================== PUBLIC (mobile) WRITE FLOW ======================
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
      {
        const client = matchPath(p, '/api/clients/:id');
        if (client && req.method === 'GET')                                return handleGetClient(env, req, client.id);
        if (client && req.method === 'PUT')                                return handleUpdateClient(env, req, client.id);
      }

      // Client Hospitals
      {
        const chList = matchPath(p, '/api/clients/:client_id/hospitals');
        if (chList && req.method === 'GET')    return handleListHospitals(env, req, chList.client_id);
        if (chList && req.method === 'POST')   return handleCreateHospital(env, req, chList.client_id);

        const chOne = matchPath(p, '/api/clients/:client_id/hospitals/:hospital_id');
        if (chOne && req.method === 'GET')     return handleGetHospital(env, req, chOne.client_id, chOne.hospital_id);
        if (chOne && (req.method === 'PATCH' || req.method === 'PUT'))
                                               return handleUpdateHospital(env, req, chOne.client_id, chOne.hospital_id);
        if (chOne && req.method === 'DELETE')  return handleDeleteHospital(env, req, chOne.client_id, chOne.hospital_id);
      }

      // Umbrellas
      if (req.method === 'GET' && p === '/api/umbrellas')                  return handleListUmbrellas(env, req);
      if (req.method === 'POST' && p === '/api/umbrellas')                 return handleCreateUmbrella(env, req);
      {
        const umb = matchPath(p, '/api/umbrellas/:umbrella_id');
        if (umb && req.method === 'GET')                                   return handleGetUmbrella(env, req, umb.umbrella_id);
        if (umb && req.method === 'PUT')                                   return handleUpdateUmbrella(env, req, umb.umbrella_id);
      }

      // Candidates
      if (req.method === 'GET' && p === '/api/candidates')                 return handleListCandidates(env, req);
      if (req.method === 'POST' && p === '/api/candidates')                return handleCreateCandidate(env, req);
      {
        const cand = matchPath(p, '/api/candidates/:candidate_id');
        if (cand && req.method === 'GET')                                  return handleGetCandidate(env, req, cand.candidate_id);
        if (cand && req.method === 'PUT')                                  return handleUpdateCandidate(env, req, cand.candidate_id);
      }

      // Rates
      if (req.method === 'GET' && p === '/api/rates/client-defaults')      return handleListClientRates(env, req);
      if (req.method === 'POST' && p === '/api/rates/client-defaults')     return handleUpsertClientRate(env, req);

      if (req.method === 'GET' && p === '/api/rates/candidate-overrides')  return handleListOverridesByCandidate(env, req);
      if (req.method === 'GET' && p === '/api/rates/client-overrides')     return handleListOverridesByClient(env, req); // expects client_id query param
      if (req.method === 'POST' && p === '/api/rates/candidate-overrides') return handleCreateOverride(env, req);

      // NEW: support GET as well as POST for resolve-preview
      if (req.method === 'GET'  && p === '/api/rates/resolve-preview')     return handleResolveRate(env, req);
      if (req.method === 'POST' && p === '/api/rates/resolve-preview')     return handleResolveRate(env, req);

      {
        const cov = matchPath(p, '/api/rates/candidate-overrides/:candidate_id');
        if (cov && req.method === 'PATCH')                                 return handleUpdateOverride(env, req, cov.candidate_id);
        if (cov && req.method === 'DELETE')                                return handleDeleteOverride(env, req, cov.candidate_id);
      }
      if (req.method === 'GET' && p === '/api/rates/candidate-overrides/by-client') {
        return handleListOverridesByClient(env, req); // expects ?client_id=...
      }

      // HealthRoster
      if (req.method === 'POST' && p === '/api/healthroster/import')       return handleHRImport(env, req);
      {
        const hrRows = matchPath(p, '/api/healthroster/:import_id/rows');
        if (hrRows && req.method === 'GET')                                 return handleHRRows(env, req, hrRows.import_id);
      }
      {
        const hrMap = matchPath(p, '/api/healthroster/:import_id/mapping');
        if (hrMap && (req.method === 'GET' || req.method === 'POST'))       return handleHRMapping(env, req, hrMap.import_id);
      }
      {
        const hrVal = matchPath(p, '/api/healthroster/:import_id/validate');
        if (hrVal && req.method === 'POST')                                 return handleHRValidate(env, req, hrVal.import_id);
      }
      // NEW: queue a TSO failure email (Power Automate)
      if (req.method === 'POST' && p === '/api/hr/tso-failure-email')       return handleQueueTsoFailureEmail(env, req);

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
        if (tsfinOne && req.method === 'PATCH')                             return handleTsfinPatch(env, req, tsfinOne.timesheet_id);

        const tsfinExp = matchPath(p, '/api/tsfin/:timesheet_id/expenses');
        if (tsfinExp && req.method === 'PATCH')                             return handleTsfinPatchExpenses(env, req, tsfinExp.timesheet_id);

        const tsfinMil = matchPath(p, '/api/tsfin/:timesheet_id/mileage');
        if (tsfinMil && req.method === 'PATCH')                             return handleTsfinPatchMileage(env, req, tsfinMil.timesheet_id);

        const tsfinPO = matchPath(p, '/api/tsfin/:timesheet_id/po');
        if (tsfinPO && req.method === 'PATCH')                              return handleTsfinPatchPO(env, req, tsfinPO.timesheet_id);
      }

      // NEW: Timesheets – pay state
      {
        const payHold = matchPath(p, '/api/timesheets/:id/pay-hold');
        if (payHold && req.method === 'PATCH')                              return handleTimesheetPayHold(env, req, payHold.id);

        const markPaid = matchPath(p, '/api/timesheets/:id/mark-paid');
        if (markPaid && req.method === 'PATCH')                             return handleTimesheetMarkPaid(env, req, markPaid.id);
      }

      // Invoices
      if (req.method === 'GET'  && p === '/api/invoices')                   return handleListInvoices(env, req);
      if (req.method === 'POST' && p === '/api/invoices')                   return handleCreateInvoiceTsfin(env, req);

      {
        const inv = matchPath(p, '/api/invoices/:invoice_id');
        if (inv && req.method === 'GET')                                    return handleGetInvoice(env, req, inv.invoice_id);
      }

      // Existing: render by invoice ID (uses stored header/items/totals)
      {
        const invRender = matchPath(p, '/api/invoices/:invoice_id/render');
        if (invRender && req.method === 'POST')                             return handleInvoiceRender(env, req, invRender.invoice_id);
      }

      // NEW: render directly from posted payload (preview / ad-hoc)
      if (req.method === 'POST' && p === '/api/invoices/render')            return handleInvoiceRenderFromPayload(env, req);

      {
        const invEmail = matchPath(p, '/api/invoices/:invoice_id/email');
        if (invEmail && req.method === 'POST')                              return handleInvoiceEmail(env, req, invEmail.invoice_id);
      }

      {
        const invCredit = matchPath(p, '/api/invoices/:invoice_id/credit-note');
        if (invCredit && req.method === 'POST')                             return handleCreateCreditNoteTsfin(env, req, invCredit.invoice_id);
      }

      {
        const invPaid = matchPath(p, '/api/invoices/:invoice_id/mark-paid');
        if (invPaid && req.method === 'POST')                               return handleInvoiceMarkPaid(env, req, invPaid.invoice_id);
      }

      {
        const invUnpay = matchPath(p, '/api/invoices/:invoice_id/unpay');
        if (invUnpay && req.method === 'POST')                              return handleInvoiceMarkUnpaid(env, req, invUnpay.invoice_id);
      }

      // Remittances — existing single-candidate composer
      if (req.method === 'POST' && p === '/api/remittances/email-for-candidate') {
        return handleRemittanceEmailForCandidate(env, req);
      }
      // NEW: bulk remittance send (list-driven)
      if (req.method === 'POST' && p === '/api/remittances/send')           return handleRemittancesSend(env, req);

      // ====================== SEARCH (exportable) ======================
      if (req.method === 'GET' && p === '/api/search/timesheets')           return handleSearchTimesheets(env, req);
      if (req.method === 'GET' && p === '/api/search/invoices')             return handleSearchInvoices(env, req);

      // Revised logic: /api/search/candidates supports JSON q= with roles_any (OR) and roles_all (AND)
      if (req.method === 'GET' && p === '/api/search/candidates')           return handleSearchCandidates(env, req);

      if (req.method === 'GET' && p === '/api/search/clients')              return handleSearchClients(env, req);
      if (req.method === 'GET' && p === '/api/search/umbrellas')            return handleSearchUmbrellas(env, req);

      // ====================== REPORTS (json/csv/print) ======================
      if (req.method === 'GET' && p === '/api/reports/timesheets')          return handleReportTimesheets(env, req);
      if (req.method === 'GET' && p === '/api/reports/invoices')            return handleReportInvoices(env, req);
      if (req.method === 'GET' && p === '/api/reports/candidates')          return handleReportCandidates(env, req);
      if (req.method === 'GET' && p === '/api/reports/clients')             return handleReportClients(env, req);
      if (req.method === 'GET' && p === '/api/reports/umbrellas')           return handleReportUmbrellas(env, req);

      // ====================== PAYMENTS (Bank CSV) ======================
      if (req.method === 'POST' && p === '/api/payments/generate-csv')      return handlePaymentsGenerateCsv(env, req);

      // ====================== REPORT PRESETS (CRUD) ======================
      if (req.method === 'POST' && p === '/api/report-presets')             return handleReportPresetsCreate(env, req);
      if (req.method === 'GET'  && p === '/api/report-presets')             return handleReportPresetsList(env, req);
      {
        const rp = matchPath(p, '/api/report-presets/:preset_id');
        if (rp && req.method === 'PATCH')                                   return handleReportPresetsUpdate(env, req, rp.preset_id);
        if (rp && req.method === 'DELETE')                                  return handleReportPresetsDelete(env, req, rp.preset_id);
      }

      // ====================== EMAIL (OUTBOX, SEND, TSO) ======================
      // List outbox
      if (req.method === 'GET'  && p === '/api/email/outbox')               return handleListOutbox(env, req);
      // Get one outbox item (canonical)
      {
        const outOne = matchPath(p, '/api/email/outbox/:id');
        if (outOne && req.method === 'GET')                                 return handleGetOutboxItem(env, req, outOne.id);
      }
      // Back-compat alias to fetch single outbox item
      {
        const outbox = matchPath(p, '/api/outbox/:mail_id');
        if (outbox && req.method === 'GET')                                 return handleGetOutboxItem(env, req, outbox.mail_id);
      }

      // Drain outbox queue
      if (req.method === 'POST' && p === '/api/email/outbox/drain')         return handleOutboxDrain(env, req);
      // Retry a failed item
      {
        const outRetry = matchPath(p, '/api/email/outbox/:id/retry');
        if (outRetry && req.method === 'POST')                              return handleOutboxRetry(env, req, outRetry.id);
      }

      // Provider callbacks / manual marks
      if (req.method === 'POST' && p === '/api/email/outbox/mark-sent')     return handleOutboxMarkSent(env, req);
      if (req.method === 'POST' && p === '/api/email/outbox/mark-failed')   return handleOutboxMarkFailed(env, req);

      // Ad-hoc direct send / broadcast (canonical)
      if (req.method === 'POST' && p === '/api/email/send')                 return handleEmailSend(env, req);
      // Back-compat alias for broadcast
      if (req.method === 'POST' && p === '/api/email/broadcast')            return handleEmailSend(env, req);

      // ====================== RELATED (generic) ======================
      // Counts for an entity (place before the generic list matcher)
      {
        const relCounts = matchPath(p, '/api/related/:entity/:id/counts');
        if (relCounts && req.method === 'GET') {
          return handleRelatedCounts(env, req, relCounts.entity, relCounts.id);
        }
      }
      // List a related type for an entity (newest-first, with limit/offset)
      {
        const relList = matchPath(p, '/api/related/:entity/:id/:type');
        if (relList && req.method === 'GET') {
          return handleRelatedList(env, req, relList.entity, relList.id, relList.type);
        }
      }

      // ====================== FILES (R2, signed) ======================
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
