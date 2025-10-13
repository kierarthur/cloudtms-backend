```javascript
/**
 * CloudTMS Broker (Cloudflare Worker) — Auth + Timesheets API (no Google Sheets)
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
    "Access-Control-Allow-Headers": "authorization,content-type,content-md5,x-requested-with",
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

// ─────────────────────────────────────────────────────────────
// AUTH SECTION (login/forgot/reset/refresh/logout)
// ─────────────────────────────────────────────────────────────

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
async function pbkdf2Hash(password, iterations=210000) {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(password), { name:'PBKDF2' }, false, ['deriveBits']);
  const bits = await crypto.subtle.deriveBits({ name:'PBKDF2', hash:'SHA-256', salt, iterations }, key, 256);
  const hashB64 = bufToBase64Url(bits);
  const saltB64 = bufToBase64Url(salt);
  return `pbkdf2:sha256$${iterations}$${saltB64}$${hashB64}`;
}
async function pbkdf2Verify(password, stored) {
  // format: pbkdf2:sha256$ITER$SALT$HASH
  const m = /^pbkdf2:sha256\$(\d+)\$([A-Za-z0-9\-_]+)\$([A-Za-z0-9\-_]+)$/.exec(String(stored||''));
  if (!m) return false;
  const iterations = parseInt(m[1],10);
  const salt = base64UrlToUint8(m[2]);
  const want = base64UrlToUint8(m[3]);
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(password), { name:'PBKDF2' }, false, ['deriveBits']);
  const bits = await crypto.subtle.deriveBits({ name:'PBKDF2', hash:'SHA-256', salt, iterations }, key, want.byteLength*8);
  const got = new Uint8Array(bits);
  if (got.byteLength !== want.byteLength) return false;
  // constant-time compare
  let diff = 0;
  for (let i=0;i<got.byteLength;i++) diff |= (got[i]^want[i]);
  return diff === 0;
}

// ── Supabase helpers for users / resets ──────────────────────
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

// ── Access/Refresh tokens (HMAC) ────────────────────────────
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

// ── Auth handlers ───────────────────────────────────────────
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
    return withCORS(env, req, new Response(JSON.stringify({ error: "Shift not in eligible window (must be ongoing or ended ≤ 4h)", code: "INELIGIBLE" }), { status: 422, headers: JSON_HEADERS }));
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

async function handleList(env, req, url) {
  const q = Object.fromEntries(url.searchParams.entries());
  const sbUrl = buildTimesheetsQuery(env, q);
  const includeCount = String(q.include_count || "false").toLowerCase() === "true";
  const { rows, total } = await sbFetch(env, sbUrl, includeCount);

  const include = new Set((q.include || "").split(",").map(s => s.trim()).filter(Boolean));
  const sign_which = (q.sign_which || "both").toLowerCase();
  const sign_exp = Math.min(parseInt(q.sign_expires_seconds || "180", 10) || 180, 900);
  const secret = env.UPLOAD_TOKEN_SECRET;

  const items = await Promise.all(rows.map(async (r) => {
    const have = { nurse: !!r.r2_nurse_key, authoriser: !!r.r2_auth_key };
    const out = { ...r, signatures: { have } };

    if (include.has("sign_keys")) {
      out.signatures.keys = { nurse: r.r2_nurse_key || null, authoriser: r.r2_auth_key || null };
    }
    if (include.has("sign_urls")) {
      const addUrl = async (which, key) => {
        if (!key) return null;
        const exp = Math.floor(Date.now() / 1000) + sign_exp;
        const token = await createToken(secret, { typ: "dl", booking_id: r.booking_id, role: which, key, exp });
        const u = new URL(url);
        u.pathname = "/signatures/get"; u.search = "";
        u.searchParams.set("key", key);
        u.searchParams.set("booking_id", r.booking_id);
        u.searchParams.set("role", which);
        u.searchParams.set("token", token);
        return u.toString();
      };
      const urls = {};
      if (sign_which === "both" || sign_which === "nurse") urls.nurse = await addUrl("nurse", r.r2_nurse_key);
      if (sign_which === "both" || sign_which === "authoriser") urls.authoriser = await addUrl("authoriser", r.r2_auth_key);
      out.signatures.urls = urls;
    }
    return out;
  }));

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

      // Core write flow
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

      return new Response("Not found", { status: 404, headers: TEXT_PLAIN });
    } catch (e) {
      console.error("Unhandled error:", e);
      return serverError("Unexpected error");
    }
  },

  // No cron needed (Sheets disabled); keep for future if desired.
  async scheduled(controller, env, ctx) {}
};
```
