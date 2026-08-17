
/** ========= Availability API (Standalone) =========
 * Uses Script Properties:
 *  SPREADSHEET_ID, API_SHARED_TOKEN, ALLOWED_ORIGIN, TIMEZONE
 *  COLOR_BOOKED, COLOR_UNAVAIL, COLOR_BLOCKED, COLOR_AVAILABLE
 *  URL_TOKEN_SECRET (optional, no longer used for deterministic tokens)
 *  PWA_BASE_URL (e.g., https://yourapp.com/ or http://localhost:3000/)
 *  LINKS_SPREADSHEET_ID (optional) -> if omitted, links live in THIS spreadsheet
 *
 * ROTA spreadsheet (SPREADSHEET_ID) must contain:
 *  - "Candidate List"  (Telephone col D in local 0XXXXXXXXXX)
 *  - "Availability"    (A..F meta, G→ rolling 14-day headers "EEE dd/MM/yyyy")
 *  - "EmailHistory"    (OccupantKey, Date dd/MM/yyyy, Shift, Hospital, Ward, Notes, … Booking Ref (col D), Job Title (col H))
 *
 * This script only WRITES to:
 *  - Rota."Availability" (on POST)
 *  - LinksSS."Availability API Links" (menu-driven update)
 *
 * It NEVER writes to Rota."Candidate List" or Rota."EmailHistory".
 */

// ---------- Config helpers ----------
function _P() {
  const p = PropertiesService.getScriptProperties().getProperties();
  if (!p.SPREADSHEET_ID) throw new Error("SPREADSHEET_ID missing");
  if (!p.API_SHARED_TOKEN) throw new Error("API_SHARED_TOKEN missing");

  const tzVal =
    p.TIMEZONE ||
    Session.getScriptTimeZone() ||
    "Europe/London";

  // Prefer explicit links IDs; fall back safely
  const linksId =
    p.LINKS_SSID ||
    p.LINKS_SPREADSHEET_ID ||
    (function () {
      try { return SpreadsheetApp.getActive().getId(); }
      catch (_) { return p.SPREADSHEET_ID; }
    })();

  // Normalise LOGINTESTMODE to boolean
  const loginTestMode = (function (v) {
    if (v == null) return false;
    const s = String(v).trim().toLowerCase();
    return s === "true" || s === "1" || s === "yes";
  })(p.LOGINTESTMODE);

  return {
    // ---- IDs & tokens (keep legacy names) ----
    SPREADSHEET_ID: p.SPREADSHEET_ID,
    SSID: p.SPREADSHEET_ID,                 // legacy alias used in older code
    LINKS_SSID: linksId,
    TOKEN: p.API_SHARED_TOKEN,

    // ---- General config ----
    ORIGIN: p.ALLOWED_ORIGIN || "*",
    TZ: tzVal,
    URL_TOKEN_SECRET: p.URL_TOKEN_SECRET || "",
    PWA_BASE_URL: (p.PWA_BASE_URL || "http://localhost:3000/").replace(/\s+$/, ""),
    ROTA_APP_URL: p.ROTA_APP_URL || "",
    LOGINTESTMODE: loginTestMode,

    // ---- Sheet names (unchanged) ----
    SH_CAND: "Candidate List",
    SH_AV:   "Availability",
    SH_EH:   "EmailHistory",
    SH_LINKS:"Availability API Links",

    // ---- Colours (unchanged keys) ----
    COL_BOOKED:  (p.COLOR_BOOKED    || "#4a86e8").toLowerCase(),
    COL_UNAV:    (p.COLOR_UNAVAIL   || "#e06666").toLowerCase(),
    COL_BLOCKED: (p.COLOR_BLOCKED   || "#6aa84f").toLowerCase(),
    COL_AVAIL:   (p.COLOR_AVAILABLE || "#fff2cc").toLowerCase(),

    // ---- Cache TTLs ----
    TTL_TILES:   Number(p.TTL_TILES   || 3600),
    TTL_HEADERS: Number(p.TTL_HEADERS || 86400),

    // ---- Emergency / comms (pass-through; helpers coerce types later) ----
    EMERGENCY_CONTACTS:                p.EMERGENCY_CONTACTS,
    EMERGENCY_TEMPLATE_NAMESPACE:      p.EMERGENCY_TEMPLATE_NAMESPACE,
    EMERGENCY_TEMPLATE_NAME:           p.EMERGENCY_TEMPLATE_NAME,
    EMERGENCY_RESPONDED_TEMPLATE_NAME: p.EMERGENCY_RESPONDED_TEMPLATE_NAME,
    EMERGENCY_WEBHOOK_SECRET:          p.EMERGENCY_WEBHOOK_SECRET,
    EMERGENCY_FROM_EMAIL:              p.EMERGENCY_FROM_EMAIL,

    // ClickSend
    CLICKSEND_USER:                    p.CLICKSEND_USER,
    CLICKSEND_API_KEY:                 p.CLICKSEND_API_KEY,
    CLICKSEND_FROM:                    p.CLICKSEND_FROM,

    // Late-running templates (new)
    LATE_SAME_TEMPLATE_NAME:           p.LATE_SAME_TEMPLATE_NAME,
    LATE_PREV_TEMPLATE_NAME:           p.LATE_PREV_TEMPLATE_NAME,
    LATE_EMERGENCY_TEMPLATE_NAME:      p.LATE_EMERGENCY_TEMPLATE_NAME,

    // Emergency timing + test gates
    EMERGENCY_ACK_TIMEOUT_MIN:         p.EMERGENCY_ACK_TIMEOUT_MIN,
    EMERGENCY_CALL_GAP_MIN:            p.EMERGENCY_CALL_GAP_MIN,
    TEST_MODE:                         p.TEST_MODE,
    TEST_PHONE:                        p.TEST_PHONE,

    // WATI (new endpoint + token + optional channel)
    WATI_TOKEN:                        p.WATI_TOKEN,
    WATI_ENDPOINT:                     p.WATI_ENDPOINT,          // e.g. "https://eu-api.wati.io/601205"
    WATI_CHANNEL_NUMBER:               p.WATI_CHANNEL_NUMBER     // optional
  };
}



// ---------- Memory & state (ScriptProperties + CacheService) ----------
function _sp() { return PropertiesService.getScriptProperties(); }
function _nowIso() { return new Date().toISOString(); }

// ScriptProperties keys
const K_ROTABUSY = "ROTABUSY";            // "1" | ""
const K_ROTABUSY_RUN = "ROTABUSY_RUNID";
const K_ROTABUSY_SINCE = "ROTABUSY_SINCE";
const K_WRITELOCK = "WRITELOCK";          // "1" | ""
const K_WRITELOCK_SINCE = "WRITELOCK_SINCE";
const K_HEADERS_JSON = "HEADERS_JSON";    // [{ymd,displayDay,displayDate,col?}] (col optional)
const K_HEADERS_TS = "HEADERS_TS";
const K_TILES_REV = "TILES_REV";          // integer string; bump to invalidate all tiles
const K_PENDING_WRITES = "PENDING_WRITES";// JSON array [{msisdn,changes:[{ymd,code}],queuedAt}]
const K_EH_HASH = "EH_HASH";              // last audit checksum
const K_EH_REV = "EH_REV";                // integer string; bump when EMAILHISTORY_UPDATED

function _getInt(k, dflt) {
  const v = _sp().getProperty(k);
  return v == null || v === "" ? (dflt|0) : (parseInt(v,10) || 0);
}
function _bump(k) {
  const n = _getInt(k, 0) + 1;
  _sp().setProperty(k, String(n));
  return n;
}

function _isRotaBusy() { return _sp().getProperty(K_ROTABUSY) === "1"; }
function _setRotaBusy(runId, busy) {
  const sp = _sp();

  // Be tolerant of constant names vs legacy literals
  const KEY_BUSY  = (typeof K_ROTABUSY !== 'undefined')       ? K_ROTABUSY       : 'ROTABUSY';
  const KEY_RUN   = (typeof K_ROTABUSY_RUN !== 'undefined')   ? K_ROTABUSY_RUN   :
                    (typeof K_ROTABUSY_RUNID !== 'undefined') ? K_ROTABUSY_RUNID : 'ROTABUSY_RUNID';
  const KEY_SINCE = (typeof K_ROTABUSY_SINCE !== 'undefined') ? K_ROTABUSY_SINCE : 'ROTABUSY_SINCE';

  // Snapshot current state
  const before = {
    busy:  sp.getProperty(KEY_BUSY)  || null,
    run:   sp.getProperty(KEY_RUN)   || null,
    since: sp.getProperty(KEY_SINCE) || null
  };

  if (busy) {
    sp.setProperty(KEY_BUSY, "1");
    if (runId) sp.setProperty(KEY_RUN, String(runId));
    const since = (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString();
    sp.setProperty(KEY_SINCE, since);

    try {
      _log('rotabusy', 'set:true', {
        source: '_setRotaBusy',
        runId: String(runId || ''),
        before,
        after: {
          busy:  sp.getProperty(KEY_BUSY)  || null,
          run:   sp.getProperty(KEY_RUN)   || null,
          since: sp.getProperty(KEY_SINCE) || null
        }
      });
    } catch (_) {}
  } else {
    sp.deleteProperty(KEY_BUSY);
    sp.deleteProperty(KEY_RUN);
    sp.deleteProperty(KEY_SINCE);

    try {
      _log('rotabusy', 'set:false', {
        source: '_setRotaBusy',
        runId: String(runId || ''),
        before,
        after: {
          busy:  sp.getProperty(KEY_BUSY)  || null,
          run:   sp.getProperty(KEY_RUN)   || null,
          since: sp.getProperty(KEY_SINCE) || null
        }
      });
    } catch (_) {}
  }
}

function _isWriteLocked() { return _sp().getProperty(K_WRITELOCK) === "1"; }
function _setWriteLock(on) {
  if (on) {
    _sp().setProperty(K_WRITELOCK, "1");
    _sp().setProperty(K_WRITELOCK_SINCE, _nowIso());
  } else {
    _sp().deleteProperty(K_WRITELOCK);
    _sp().deleteProperty(K_WRITELOCK_SINCE);
  }
}

function _cache() { return CacheService.getScriptCache(); }
function _cacheKeyTiles(msisdn) { return "tiles:" + String(msisdn); }
function _cacheGetJSON(key) {
  try { const v = _cache().get(key); return v ? JSON.parse(v) : null; } catch(e){ return null; }
}
function _cachePutJSON(key, obj, ttl) {
  try { _cache().put(key, JSON.stringify(obj), ttl); } catch (_) {}
}

// HEADERS memory
function _headersGet() {
  const raw = _sp().getProperty(K_HEADERS_JSON);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch(_) { return null; }
}
function _headersSet(arr) {
  _sp().setProperty(K_HEADERS_JSON, JSON.stringify(arr || []));
  _sp().setProperty(K_HEADERS_TS, _nowIso());
  _bump(K_TILES_REV); // invalidate tiles globally whenever headers change
}
// ✅ Fixed: ensure "NewUser" exists immediately before AdminAction


// Ensure we have headers; if rota is NOT busy, we may read the sheet and refresh memory

function _headersStale() {
  const ts = _sp().getProperty(K_HEADERS_TS);
  if (!ts) return true;
  const ageSec = (Date.now() - new Date(ts).getTime())/1000;
  return ageSec > _P().TTL_HEADERS;
}
function _ensureHeaders(ss) {
  const mem = _headersGet();

  // If rota is busy, never touch live; serve whatever we have.
  if (_isRotaBusy()) return mem;

  // Decide if we should consider refreshing.
  const needRefresh = !mem || _headersStale();
  if (needRefresh) {
    const live = _readAvailabilityHeaders(ss);
    if (live && live.length === 14) {
      const arr = live.map(h => ({ ymd: h.ymd, displayDay: h.displayDay, displayDate: h.displayDate }));

      // If we have NO memory yet, initialise it from live immediately (housekeeping, no bump),
      // even if staged headers exist — staging is for the next flip.
      if (!mem) {
        const sp = _sp();
        sp.setProperty(K_HEADERS_JSON, JSON.stringify(arr));
        sp.setProperty(K_HEADERS_TS, _nowIso());
        return arr;
      }

      // If the current memory matches live exactly, just refresh the timestamp (no global bump).
      const same =
        Array.isArray(mem) &&
        mem.length === 14 &&
        mem.every((m, i) =>
          m.ymd === arr[i].ymd &&
          m.displayDay === arr[i].displayDay &&
          m.displayDate === arr[i].displayDate
        );

      if (same) {
        _sp().setProperty(K_HEADERS_TS, _nowIso());
        return mem;
      }

      // If there are staged headers awaiting publish, do not overwrite memory here.
      // Keep serving the existing memory until _publishStagedHeadersAndFlip() runs.
      // Refresh TS so we don't keep rechecking on every call.
      const staged = (typeof _headersStageGet === 'function') ? _headersStageGet() : null;
      if (staged && Array.isArray(staged) && staged.length === 14) {
        _sp().setProperty(K_HEADERS_TS, _nowIso());
        return mem;
      }

      // Otherwise, update memory to live WITHOUT bumping tiles rev (housekeeping refresh).
      const sp = _sp();
      sp.setProperty(K_HEADERS_JSON, JSON.stringify(arr));
      sp.setProperty(K_HEADERS_TS, _nowIso());
      return arr;
    }
  }
  return mem;
}




// Pending writes queue
function _getPendingWrites() {
  const raw = _sp().getProperty(K_PENDING_WRITES);
  if (!raw) return [];
  try { return JSON.parse(raw) || []; } catch(_) { return []; }
}
function _setPendingWrites(arr) {
  _sp().setProperty(K_PENDING_WRITES, JSON.stringify(arr || []));
}
function _queueChanges(msisdn, changes) {
  // Normalize identity
  const tel = _normaliseLocalTel(msisdn || "");
  if (!tel) return;

  // Normalize & validate incoming changes (last-write-wins within the incoming batch)
  const ALLOWED = new Set(["", "N/A", "LD", "N", "LD/N"]);
  const byYmdIncoming = {};
  (Array.isArray(changes) ? changes : []).forEach(c => {
    const ymd  = String(c && c.ymd || "");
    // cheap YYYY-MM-DD check
    if (!/^\d{4}-\d{2}-\d{2}$/.test(ymd)) return;
    const codeUp = (c.code == null ? "" : String(c.code).toUpperCase());
    if (!ALLOWED.has(codeUp)) return;
    byYmdIncoming[ymd] = { ymd, code: codeUp };
  });

  const incomingList = Object.values(byYmdIncoming);
  if (!incomingList.length) return;

  // Load existing queue and coalesce all entries for this msisdn (last-write-wins across old + new)
  const q = _getPendingWrites();
  const others = [];
  const byYmdExisting = {};

  q.forEach(item => {
    const t = _normaliseLocalTel(item && item.msisdn || "");
    if (t !== tel) {
      others.push(item);
      return;
    }
    const arr = Array.isArray(item.changes) ? item.changes : [];
    arr.forEach(ch => {
      const ymd = String(ch && ch.ymd || "");
      const codeUp = (ch && ch.code != null) ? String(ch.code).toUpperCase() : "";
      if (/^\d{4}-\d{2}-\d{2}$/.test(ymd) && ALLOWED.has(codeUp)) {
        byYmdExisting[ymd] = { ymd, code: codeUp };
      }
    });
  });

  // Merge: new overrides existing
  const mergedByYmd = Object.assign({}, byYmdExisting, byYmdIncoming);
  const mergedList = Object.values(mergedByYmd);
  if (!mergedList.length) {
    // Nothing to store for this tel; just persist others
    _setPendingWrites(others);
    return;
  }

  const newEntry = { msisdn: String(tel), changes: mergedList, queuedAt: _nowIso() };

  // Prevent unbounded growth: keep only the most recent 500 entries overall
  const newQueue = others.concat([newEntry]);
  if (newQueue.length > 500) {
    // drop oldest, keep last 500
    _setPendingWrites(newQueue.slice(newQueue.length - 500));
  } else {
    _setPendingWrites(newQueue);
  }
}

function _ssRota()  { return SpreadsheetApp.openById(_P().SSID); }
function _ssLinks() { return SpreadsheetApp.openById(_P().LINKS_SSID); }

// ---------- Phone normalisation ----------
function _normaliseLocalTel(raw) {
  // Return UK local form 0XXXXXXXXXX, or null if cannot.
  const digits = String(raw || "").replace(/\D/g, "");
  if (!digits) return null;
  if (digits.startsWith("0") && digits.length === 11) return digits;
  if (digits.startsWith("44") && digits.length === 12) return "0" + digits.slice(2);
  if (digits.startsWith("0044") && digits.length === 14) return "0" + digits.slice(4);
  if (digits.length === 10 && digits.startsWith("7")) return "0" + digits; // tolerate missing leading 0
  return null;
}



// ---------- CORS / responses ----------
// ---------- CORS helpers (fixed) ----------


function _ok(body) {
  return ContentService
    .createTextOutput(JSON.stringify(body))
    .setMimeType(ContentService.MimeType.JSON);
}

function _err(msg, code, extra) {
  const body = Object.assign(
    { ok: false, error: String(msg), status: Number(code || 400) },
    extra || {}
  );
  return ContentService
    .createTextOutput(JSON.stringify(body))
    .setMimeType(ContentService.MimeType.JSON);
}

// Either remove doOptions entirely, or keep it trivial:
function doOptions(e) {
  return ContentService.createTextOutput('')
    .setMimeType(ContentService.MimeType.TEXT);
}

// Keep _withCors if you like the call sites, but it must NOT set headers:
function _withCors(output) {
  // Only ensure JSON mime-type if you still call it.
  return output.setMimeType(ContentService.MimeType.JSON);
}

// ---------- Routing ----------







/* ======================= NEW HELPERS FOR EAGER WARMING ======================= */

// ScriptProperties key for pending warm list (msisdns)
const K_PENDING_WARMS = "PENDING_WARMS";

/** Get pending warms array from Script Properties */
function _getPendingWarms() {
  try {
    const raw = _sp().getProperty(K_PENDING_WARMS);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.filter(Boolean) : [];
  } catch (_) {
    return [];
  }
}

/** Set pending warms array into Script Properties */
function _setPendingWarms(arr) {
  try {
    const uniq = Array.from(new Set((Array.isArray(arr) ? arr : []).filter(Boolean)));
    _sp().setProperty(K_PENDING_WARMS, JSON.stringify(uniq));
  } catch (_) {}
}

/** Add msisdns to the pending warm queue (de-duped, bounded) */
function _queueWarms(msisdns) {
  try {
    const add = Array.from(new Set((Array.isArray(msisdns) ? msisdns : []).filter(Boolean)));
    if (!add.length) return;
    const cur = _getPendingWarms();
    const set = new Set(cur.concat(add));
    // prevent unbounded growth
    const out = Array.from(set).slice(-500);
    _setPendingWarms(out);
    _logWarm('QUEUE_WARMS', { queuedNow: add.length, totalQueued: out.length });
  } catch (_) {}
}

/** Drain pending warms and perform warming (called at AVAILABILITY_UPDATE_END) */
function _drainPendingWarms() {
  const msisdns = _getPendingWarms();
  if (!msisdns.length) return { attempted: 0, warmed: 0, failed: 0 };
  _setPendingWarms([]);
  const res = _warmTilesForMsisdns(msisdns);
  _logWarm('DRAIN_WARMS', Object.assign({ count: msisdns.length }, res));
  return res;
}

/** Map occupant keys (e.g., "surname firstname" lowercased) to msisdns from Candidate List */
function _msisdnListFromOccupantKeys(keys) {
  const ss = _ssRota();
  const sh = ss.getSheetByName(_P().SH_CAND);
  if (!sh) return [];

  const vals = sh.getDataRange().getDisplayValues();
  if (vals.length < 2) return [];

  const want = new Set((keys || []).map(k => String(k || '').toLowerCase().trim()).filter(Boolean));
  if (!want.size) return [];

  const result = new Set();
  for (let i = 1; i < vals.length; i++) {
    const sur = String(vals[i][0] || '').trim();
    const fir = String(vals[i][1] || '').trim();
    const tel = _normaliseLocalTel(String(vals[i][3] || ''));
    if (!tel) continue;
    const key = (sur + ' ' + fir).toLowerCase().trim();
    if (want.has(key)) result.add(tel);
  }
  const arr = Array.from(result);
  _logWarm('KEYS_TO_MSISDN', { keysCount: keys.length, resolved: arr.length });
  return arr;
}


/** Lightweight logger for warm events into Logs sheet */
function _logWarm(event, data) {
  // logging disabled – no-op
}




function _flushPendingWrites() {
  const q = _getPendingWrites();
  if (!q.length) return { count: 0, applied: 0, failed: 0 };

  if (_isRotaBusy()) return { count: q.length, applied: 0, failed: q.length, reason: "ROTA_BUSY" };

  const ss = _ssRota();

  // ✅ Use LIVE headers/columns from the sheet (not assumed 7+i)
  const headers = _readAvailabilityHeaders(ss);
  if (!headers || headers.length !== 14) {
    return { count: q.length, applied: 0, failed: q.length, reason: "HEADERS_INVALID" };
  }
  const ymdToCol   = Object.fromEntries(headers.map(h => [h.ymd, h.col]));
  const headersYmd = headers.map(h => h.ymd);
  const idxByYmd   = Object.fromEntries(headersYmd.map((y, i) => [y, i]));

  let applied = 0, failed = 0;

  // Group by msisdn and merge last-write-wins per ymd across the whole queue
  const grouped = {}; // tel -> { ymd -> code }
  q.forEach(item => {
    const tel = _normaliseLocalTel(item.msisdn || "");
    if (!tel) return;
    if (!grouped[tel]) grouped[tel] = {};
    (item.changes || []).forEach(ch => {
      const ymd  = String(ch.ymd || "");
      const code = (ch.code == null ? "" : String(ch.code));
      grouped[tel][ymd] = code; // last write wins
    });
  });

  try {
    _setWriteLock(true);

    // Apply per candidate
    for (const tel of Object.keys(grouped)) {
      const cand = _findCandidateByMobile(ss, tel);
      if (!cand) { failed += Object.keys(grouped[tel]).length; continue; }

      const avRow = _findAvailabilityRowByTelephone(ss, cand.telephone);
      if (!avRow) { failed += Object.keys(grouped[tel]).length; continue; }

      const nameKey = (cand.surname + " " + cand.firstname).toLowerCase().trim();
      const ymds    = Object.keys(grouped[tel]);
      const bookedMap = _readBookedMap(ss, nameKey, ymds);

      const toApply = []; // { r, c, value, bg }

      ymds.forEach(ymd => {
        const col = ymdToCol[ymd];
        if (!col) { failed++; return; } // outside 14-day window

        // Booking/block guards
        if (bookedMap[ymd]) { failed++; return; }

        const cell = _readOneAvailabilityCell(ss, avRow.rowIndex, col);
        if (_isBlockedCell(cell.value, cell.bg)) { failed++; return; }

        const codeUp = String(grouped[tel][ymd] || "").toUpperCase();
        if (!["", "N/A", "LD", "N", "LD/N"].includes(codeUp)) { failed++; return; }

        const m = _mapWrite(codeUp);
        toApply.push({ r: avRow.rowIndex, c: col, value: m.value, bg: m.bg });
        applied++;
      });

      if (toApply.length) {
        const av = ss.getSheetByName(_P().SH_AV);
        const list = av.getRangeList(toApply.map(x => av.getRange(x.r, x.c, 1, 1).getA1Notation()));
        // values then backgrounds to minimize API calls and keep order deterministic
        list.getRanges().forEach((range, i) => range.setValue(toApply[i].value));
        list.getRanges().forEach((range, i) => range.setBackground(toApply[i].bg));
      }

      // Refresh candidate cache slice post-flush if cache aligns with current headers
      const tilesObj = _tilesGet(tel);
      if (tilesObj && JSON.stringify(tilesObj.headers) === JSON.stringify(headersYmd)) {
        const bookedFresh = _readBookedMap(ss, nameKey, ymds);
        const avSh = ss.getSheetByName(_P().SH_AV);

        ymds.forEach(ymd => {
          const idx = idxByYmd[ymd];
          const col = ymdToCol[ymd];
          if (idx == null || !col) return;

          const cell = _readOneAvailabilityCell(ss, avRow.rowIndex, col);
          const H = headers[idx];
          const b = bookedFresh[ymd];

          let tile;
          if (b) {
            tile = {
              ymd,
              displayDay: H.displayDay,
              displayDate: H.displayDate,
              booked: true, editable: false, status: "BOOKED",
              shiftInfo: b.notes || b.shift,
              hospital: b.hospital || "",
              ward: b.ward || "",
              jobTitle: b.jobTitle || "",
              bookingRef: b.bookingRef || ""
            };
          } else if (_isBlockedCell(cell.value, cell.bg)) {
            tile = { ymd, displayDay: H.displayDay, displayDate: H.displayDate, booked: false, editable: false, status: "BLOCKED" };
          } else {
            const stat = _statusFromCell(cell.value, cell.bg);
            tile = { ymd, displayDay: H.displayDay, displayDate: H.displayDate, booked: false, editable: true, status: stat };
          }

          tilesObj.tiles[idx] = tile;
        });

        _tilesPut(tel, tilesObj.tiles, headersYmd);
      }
    }
  } finally {
    _setWriteLock(false);
    _setPendingWrites([]); // Clear queue regardless; any failures will be retried on next user action
  }

  return { count: q.length, applied, failed };
}

// Rebuild/refresh tiles incrementally when not busy (optional: bind to hourly trigger)
function hourlyRefresh() {
  if (_isRotaBusy()) return;
  const ss = _ssRota();
  _ensureHeaders(ss); // keep header memory fresh
  // Light-touch: we don't rebuild all tiles (can be heavy). The cache populates on demand.
  // If you want a full warm-up, iterate all Availability rows here and call doGet for each msisdn.
}

// Compute a checksum for EmailHistory and bump EH_REV if drift is detected (bind to 4-hour trigger)
function ehAudit() {
  if (_isRotaBusy()) return;
  const ss = _ssRota();
  const sh = ss.getSheetByName(_P().SH_EH);
  if (!sh) return;
  const range = sh.getDataRange().getDisplayValues();
  if (range.length < 2) return;

  // Cheap checksum (order-sensitive)
  const flat = range.slice(0, Math.min(range.length, 20000)) // cap rows for safety
                   .map(r => r.join("|")).join("\n");
  const hash = Utilities.base64EncodeWebSafe(Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, flat));
  const prev = _sp().getProperty(K_EH_HASH) || "";
  if (hash !== prev) {
    _sp().setProperty(K_EH_HASH, hash);
    _bump(K_EH_REV);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _applyChangesToTilesCache — still patches tiles, but after write it
// opportunistically refreshes peers tri-state for the nearest booked anchor,
// so peers don’t remain stale when availability is edited.
// (Conservative: best-effort; if no booked anchor we skip.)
// ─────────────────────────────────────────────────────────────────────────────

// ---------- Identity resolution (msisdn or token k) ----------


// Normalise to UK local 0XXXXXXXXXX; accept +44 / 44 / 0 and tolerate 10-digit missing leading 0
function _normaliseMsisdn(raw) {
  const s = String(raw || "").replace(/\D/g, "");
  if (!s) return null;
  if (s.startsWith("44")  && s.length === 12) return "0" + s.slice(2);
  if (s.startsWith("0044")&& s.length === 14) return "0" + s.slice(4);
  if (s.startsWith("0")   && s.length === 11) return s;
  if (s.length === 10 && s.startsWith("7")) return "0" + s; // tolerate missing leading 0
  return null;
}

function _enforceToken(e) {
  // prefer query param
  let t = (e && e.parameter && e.parameter.t) ? String(e.parameter.t) : '';

  // fallback: sniff from raw body without JSON.parse
  if (!t && e && e.postData && typeof e.postData.contents === 'string') {
    const raw = e.postData.contents;
    // very small, safe sniffers:
    let m = raw.match(/"(?:t)"\s*:\s*"([^"]+)"/); // JSON-ish
    if (!m) m = raw.match(/\bt=([A-Za-z0-9._-]+)/); // form-ish
    if (m && m[1]) t = m[1];
  }

  if (t !== _P().TOKEN) {
    const err = new Error("FORBIDDEN");
    err.status = 403;            // let outer handlers map this to 403
    throw err;
  }
}
function onEdit(e) {
  try {
    // Basic guards
    if (!e || !e.range) return;

    var sh = e.range.getSheet();
    var linksName = (_P && typeof _P === 'function' ? _P().SH_LINKS : 'Availability API Links');
    if (sh.getName() !== linksName) return; // only police the Links sheet

    var r = e.range;
    var row = r.getRow();
    var col = r.getColumn();
    var numRows = r.getNumRows();
    var numCols = r.getNumColumns();

    // Always block header edits
    if (row === 1) {
      // Revert header edits: clear or restore old value when available
      if (numRows === 1 && numCols === 1 && 'oldValue' in e) {
        r.setValue(e.oldValue != null ? e.oldValue : '');
      } else {
        r.clearContent();
      }
      e.source.toast('Header is protected.', 'Protected', 4);
      return;
    }

    // Resolve column indexes dynamically
    var info = _linksHeaderInfo(sh);
    var m = info && info.m ? info.m : null;

    // If schema missing, block all edits until schema is healthy
    if (!m || m.STA == null || m.ACT == null || m.STA < 0 || m.ACT < 0) {
      if (numRows === 1 && numCols === 1 && 'oldValue' in e) {
        r.setValue(e.oldValue != null ? e.oldValue : '');
      } else {
        r.clearContent();
      }
      e.source.toast('Sheet is system-managed. Please try again later.', 'Protected', 4);
      return;
    }

    // Allowed columns (1-based) — allow Status, AdminAction, and NewUser
    var allowedCols = new Set([m.STA + 1, m.ACT + 1]);
    if (m.NEW != null && m.NEW >= 0) {
      allowedCols.add(m.NEW + 1);
    }

    // Check if the whole edited range is within allowed columns AND body rows (>=2)
    var allColsAllowed = true;
    for (var c = col; c < col + numCols; c++) {
      if (!allowedCols.has(c)) { allColsAllowed = false; break; }
    }
    var inBody = row >= 2;

    if (allColsAllowed && inBody) {
      // Let the edit through (data validation already enforces allowed values)
      return;
    }

    // Otherwise: REVERT the user edit immediately
    if (numRows === 1 && numCols === 1 && 'oldValue' in e) {
      // Single-cell: restore previous value when available
      r.setValue(e.oldValue != null ? e.oldValue : '');
    } else {
      // Multi-cell or unknown old values: clear content
      r.clearContent();
    }

    e.source.toast('This sheet is system-managed. Only “Status”, “AdminAction”, and “NewUser” can be edited.', 'Protected', 5);
  } catch (err) {
    // Silent fail to avoid blocking user flow
    try { Logger.log('onEdit guard error: ' + (err && err.message ? err.message : err)); } catch (_) {}
  }
}

// ---------- Token & Links helpers ----------
function _ensureLinksSheet() {
  const ss  = _ssLinks();
  const p   = _P();
  const name = p.SH_LINKS || "Availability API Links";

  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
  }

  // If the sheet is empty or header row is not fully populated, seed the full header set.
  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  const hasAnyData = (lastRow >= 1 && lastCol >= 1);

  // Read current header row (if present)
  let hdr = [];
  if (hasAnyData) {
    hdr = sh.getRange(1, 1, 1, Math.max(1, lastCol)).getDisplayValues()[0].map(v => String(v || ''));
  }

  // Full, canonical header list (includes new auth columns)
  const canonicalHeaders = [
    "Surname",
    "First",
    "Telephone",
    "Email",
    "Token",
    "URL",
    "Status",
    "CreatedAt",
    "RevokedAt",
    "NewUser",
    "AdminAction",
    "PasswordHash",
    "PasswordSalt",
    "PasswordUpdatedAt",
    "ResetToken",
    "ResetExpiresAt"
  ];

  const needsSeeding =
    !hdr.length ||
    // If any canonical header is missing, we’ll reseed the row cleanly
    canonicalHeaders.some(h => hdr.map(x => x.trim().toLowerCase()).indexOf(h.toLowerCase()) < 0);

  if (needsSeeding) {
    // Clear existing contents and write the canonical header row
    sh.clear();
    sh.getRange(1, 1, 1, canonicalHeaders.length).setValues([canonicalHeaders]);

    // Freeze header
    sh.setFrozenRows(1);

    // Light formatting niceties
    // Telephone should be plain text
    const telCol = canonicalHeaders.indexOf("Telephone") + 1;
    if (telCol > 0) {
      sh.getRange(1, telCol, Math.max(1000, sh.getMaxRows()), 1).setNumberFormat('@');
    }

    // Optional: Autosize header width
    sh.autoResizeColumns(1, canonicalHeaders.length);
  }

  // Now retrofit/validate (safe on both fresh and existing sheets)
  _ensureLinksSchema(sh);     // will no-op for already-correct sheets
  _applyLinksValidations(sh); // Status/NewUser/AdminAction data validation

  return sh;
}







function _randomToken() {
  const raw = Utilities.getUuid() + "|" + new Date().getTime() + "|" + Math.random();
  const hash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, raw);
  return Utilities.base64EncodeWebSafe(hash).replace(/=+$/,'');
}



// ---------- UPDATE TOKENS (menu action) ----------


// ---------- Domain helpers (ROTA) ----------
function _findCandidateByMobile(ss, msisdn0) {
  const sh = ss.getSheetByName(_P().SH_CAND);
  if (!sh) throw new Error("Candidate List sheet missing");
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return null;
  const vals = sh.getRange(2, 1, lastRow - 1, 4).getDisplayValues();
  for (let i = 0; i < vals.length; i++) {
    const row = vals[i];
    const telephone = _normaliseLocalTel((row[3] || "").toString());
    if (telephone === msisdn0) {
      return {
        rowIndex: i + 2,
        surname: String(row[0] || ""),
        firstname: String(row[1] || ""),
        email: String(row[2] || ""),
        telephone: telephone
      };
    }
  }
  return null;
}

function _readAvailabilityHeaders(ss) {
  const sh = ss.getSheetByName(_P().SH_AV);
  if (!sh) throw new Error("Availability sheet missing");
  const lastCol = sh.getLastColumn();
  if (lastCol < 7) return [];
  const headers = sh.getRange(1, 7, 1, lastCol - 6).getDisplayValues()[0];
  const tz = _P().TZ;
  const out = [];
  for (let i = 0; i < headers.length; i++) {
    const h = String(headers[i] || "");
    const m = h.match(/\b(\d{2}\/\d{2}\/\d{4})\b/);
    if (!m) continue;
    const [dd, mm, yyyy] = m[1].split("/");
    const d = new Date(Number(yyyy), Number(mm) - 1, Number(dd));
    const ymd = Utilities.formatDate(d, tz, "yyyy-MM-dd");
    out.push({
      col: 7 + i,
      headerText: h,
      ymd,
      displayDay: Utilities.formatDate(d, tz, "EEE").toUpperCase(),
      displayDate: Utilities.formatDate(d, tz, "d MMMM")
    });
  }
  return out;
}
function _findAvailabilityRowByTelephone(ss, msisdn0) {
  const sh = ss.getSheetByName(_P().SH_AV);
  if (!sh) throw new Error("Availability sheet missing");
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return null;
  const vals = sh.getRange(2, 4, lastRow - 1, 1).getDisplayValues();
  for (let i = 0; i < vals.length; i++) {
    const tel = _normaliseLocalTel(String(vals[i][0] || ""));
    if (tel === msisdn0) return { rowIndex: i + 2 };
  }
  return null;
}
function _readAvailabilityCells(ss, rowIndex, headers) {
  const sh = ss.getSheetByName(_P().SH_AV);
  const cols = headers.map(h => h.col);
  const minCol = Math.min(...cols);
  const count = headers.length;
  const vals = sh.getRange(rowIndex, minCol, 1, count).getDisplayValues()[0];
  const bgs  = sh.getRange(rowIndex, minCol, 1, count).getBackgrounds()[0].map(s => String(s || "").toLowerCase());
  return vals.map((v, i) => ({ value: String(v || ""), bg: bgs[i] }));
}
function _readOneAvailabilityCell(ss, rowIndex, colIndex) {
  const sh = ss.getSheetByName(_P().SH_AV);
  const v = sh.getRange(rowIndex, colIndex).getDisplayValue();
  const bg = sh.getRange(rowIndex, colIndex).getBackground().toLowerCase();
  return { value: String(v || ""), bg };
}

// Mapping status
function _statusFromCell(value, bg, pOpt) {
  const p = pOpt || _P();
  const v = String(value || "").toUpperCase().trim();
  const b = String(bg || "").toLowerCase();
  if (!v && (b === "#ffffff" || !b)) return "PENDING AVAILABILITY";
  if (b === p.COL_UNAV || v === "N/A") return "NOT AVAILABLE";
  if (b === p.COL_AVAIL) {
    if (v === "LD") return "LONG DAY";
    if (v === "N")  return "NIGHT";
    if (v === "LD/N" || v === "LD+N") return "LONG DAY/NIGHT";
  }
  return "PENDING AVAILABILITY";
}
function _isBlockedCell(value, bg, pOpt) {
  const p = pOpt || _P();
  const v = String(value || "").toUpperCase().trim();
  const b = String(bg || "").toLowerCase();
  return v === "BLOCKED" || b === p.COL_BLOCKED;
}

/**
 * Build a map of booked days for the given ymds:
 *   map[ymd] = { shift, notes, hospital, ward, location, jobTitle, bookingRef }
 * Notes: 
 *  - jobTitle comes from EmailHistory column H (fallback by header name).
 *  - bookingRef comes from EmailHistory column D (fallback by header name).
 */
/**
 * Build a map of booked days for the given ymds:
 *   map[ymd] = { shift, notes, hospital, ward, location, jobTitle, bookingRef }
 * Notes:
 *  - jobTitle comes from EmailHistory column H (fallback by header name).
 *  - bookingRef comes from EmailHistory column D (fallback by header name).
 */
function _readBookedMap(ss, nameKey, ymds) {
  const p = _P();
  const sh = ss.getSheetByName(p.SH_EH);
  const map = {};
  if (!sh) return map;

  const rows = sh.getDataRange().getDisplayValues();
  if (rows.length < 2) return map;

  const hdr = rows[0].map(s => String(s || "").toLowerCase());
  const idxByHeader = (h) => hdr.indexOf(h);

  const iOcc   = idxByHeader("occupantkey");
  const iDate  = idxByHeader("date");
  const iShift = idxByHeader("shift");
  const iHosp  = idxByHeader("hospital");
  const iWard  = idxByHeader("ward");
  const iNotes = idxByHeader("notes");

  // Booking Reference (prefer fixed column D if plausible; else by header name)
  let iRef = idxByHeader("booking reference");
  if (iRef < 0 && rows[0].length >= 4) iRef = 3; // column D (0-based)

  // Job Title (prefer fixed column H; else by header name)
  let iJob = idxByHeader("job title");
  if (iJob < 0 && rows[0].length >= 8) iJob = 7; // column H (0-based)

  const set = new Set(ymds || []);
  const tz  = p.TZ;

  for (let r = 1; r < rows.length; r++) {
    const occ = (iOcc >= 0 ? String(rows[r][iOcc] || "") : "").toLowerCase().trim();
    if (!occ || occ !== nameKey) continue;

    const dateStr = (iDate >= 0 ? String(rows[r][iDate] || "") : "").trim();
    const m = dateStr.match(/\b(\d{2}\/\d{2}\/\d{4})\b/);
    const ddmmyyyy = m ? m[1] : dateStr;
    const parts = ddmmyyyy.split("/");
    if (parts.length !== 3) continue;
    const [dd, mm, yyyy] = parts;
    const d = new Date(Number(yyyy), Number(mm) - 1, Number(dd));
    const ymd = Utilities.formatDate(d, tz, "yyyy-MM-dd");

    // Restrict to requested ymds for tiles
    if (set.size && !set.has(ymd)) continue;

    const shiftRaw = iShift >= 0 ? String(rows[r][iShift] || "").toUpperCase() : "";
    const shift = (shiftRaw.includes("NIGHT") || shiftRaw === "N") ? "NIGHT" : "LONG DAY";

    const hospitalRaw = iHosp >= 0 ? String(rows[r][iHosp] || "") : "";
    const hospitalNorm = _normalizeHospitalName(hospitalRaw);

    const ward = iWard >= 0 ? String(rows[r][iWard] || "") : "";
    const notes = iNotes >= 0 ? String(rows[r][iNotes] || "").trim() : "";

    const bookingRef = iRef >= 0 ? String(rows[r][iRef] || "").trim() : "";
    let jobTitle = iJob >= 0 ? String(rows[r][iJob] || "").trim().toUpperCase() : "";
    if (jobTitle !== "RMN" && jobTitle !== "HCA") jobTitle = "";

    // Prefer the first entry that has Notes; otherwise allow overwrite
    if (!map[ymd] || (notes && !map[ymd].notes)) {
      map[ymd] = {
        shift,
        notes,
        hospital: hospitalNorm,            // <-- normalized value
        ward,
        location: [hospitalNorm, ward].filter(Boolean).join(" – "), // <-- uses normalized
        jobTitle,
        bookingRef
      };
    }
  }
  return map;
}


// Past shifts list for nameKey between start..end (inclusive).
// Past shifts list for nameKey between start..end (inclusive).
function _listPastShifts(ss, nameKey, startDateYmd, endDateYmd) {
  const p = _P();
  const sh = ss.getSheetByName(p.SH_EH);
  const out = [];
  if (!sh) return out;

  const rows = sh.getDataRange().getDisplayValues();
  if (rows.length < 2) return out;

  const hdr = rows[0].map(s => String(s || "").toLowerCase());
  const idxByHeader = (h) => hdr.indexOf(h);

  const iOcc   = idxByHeader("occupantkey");
  const iDate  = idxByHeader("date");
  const iShift = idxByHeader("shift");
  const iHosp  = idxByHeader("hospital");
  const iWard  = idxByHeader("ward");
  const iNotes = idxByHeader("notes");

  let iRef = idxByHeader("booking reference");
  if (iRef < 0 && rows[0].length >= 4) iRef = 3; // column D
  let iJob = idxByHeader("job title");
  if (iJob < 0 && rows[0].length >= 8) iJob = 7; // column H

  const tz = p.TZ;
  const start = startDateYmd;
  const end   = endDateYmd;

  for (let r = 1; r < rows.length; r++) {
    const occ = (iOcc >= 0 ? String(rows[r][iOcc] || "") : "").toLowerCase().trim();
    if (!occ || occ !== nameKey) continue;

    const dateStr = (iDate >= 0 ? String(rows[r][iDate] || "") : "").trim();
    const m = dateStr.match(/\b(\d{2}\/\d{2}\/\d{4})\b/);
    const ddmmyyyy = m ? m[1] : dateStr;
    const parts = ddmmyyyy.split("/");
    if (parts.length !== 3) continue;
    const [dd, mm, yyyy] = parts;
    const d = new Date(Number(yyyy), Number(mm) - 1, Number(dd));
    const ymd = Utilities.formatDate(d, tz, "yyyy-MM-dd");

    if (ymd < start || ymd > end) continue;

    const shiftRaw = iShift >= 0 ? String(rows[r][iShift] || "").toUpperCase() : "";
    const shift = (shiftRaw.includes("NIGHT") || shiftRaw === "N") ? "NIGHT" : "LONG DAY";

    const notes = iNotes >= 0 ? String(rows[r][iNotes] || "").trim() : "";

    const hospitalRaw = iHosp >= 0 ? String(rows[r][iHosp] || "") : "";
    const hospitalNorm = _normalizeHospitalName(hospitalRaw);

    const ward = iWard >= 0 ? String(rows[r][iWard] || "") : "";
    const bookingRef = iRef >= 0 ? String(rows[r][iRef] || "").trim() : "";

    let jobTitle = iJob >= 0 ? String(rows[r][iJob] || "").trim().toUpperCase() : "";
    if (jobTitle !== "RMN" && jobTitle !== "HCA") jobTitle = "";

    const dateDisplay = Utilities.formatDate(d, tz, "EEE d MMM");
    const canonical = _canonicalTimes(shift);

    out.push({
      ymd,
      dateDisplay,
      shiftType: shift,
      notes,
      canonicalTimes: canonical,
      hospital: hospitalNorm,      // <-- normalized value
      ward,
      bookingRef,
      jobTitle
    });
  }

  // Sort descending by date, then by source order
  out.sort((a, b) => (a.ymd < b.ymd ? 1 : (a.ymd > b.ymd ? -1 : 0)));
  return out;
}



// ---------- Helper: normalize hospital name before sending to UI ----------
function _normalizeHospitalName(name) {
  // Trim and work case-insensitively
  const s = String(name || "").trim();
  if (!s) return s;

  const low = s.toLowerCase();

  // 1) Prefix rewrites (highest priority)
  if (low.startsWith("worthing")) {
    return "Worthing Hospital";
  }
  if (low.startsWith("royal alexandra")) {
    return "RACH Brighton";
  }
  // Accept "st richard", "st richards", "st richard's"
  if (low.startsWith("st richard")) {
    return "St Richards Hospital";
  }

  // 2) Generic "Hospital" capture (first whole-word occurrence)
  const m = s.match(/\bHospital\b/i);
  if (m && typeof m.index === "number") {
    return s.slice(0, m.index + m[0].length).trim();
  }

  // 3) No match → return as-is
  return s;
}

function _canonicalTimes(shift) {
  return (shift === "NIGHT") ? "19:30–08:00" : "07:30–20:00";
}

// ---------- Date helpers ----------
function _dateOnly(d, tz) {
  const y = Utilities.formatDate(d, tz, "yyyy");
  const m = Utilities.formatDate(d, tz, "MM");
  const day = Utilities.formatDate(d, tz, "dd");
  return `${y}-${m}-${day}`;
}
function _addDays(ymd, delta, tz) {
  const [Y, M, D] = ymd.split("-").map(Number);
  const dt = new Date(Y, M - 1, D);
  dt.setDate(dt.getDate() + delta);
  return Utilities.formatDate(dt, tz, "yyyy-MM-dd");
}

function _mapWrite(code, pOpt) {
  const p = pOpt || _P();
  switch (code) {
    case "":     return { value: "",    bg: "#ffffff" };
    case "N/A":  return { value: "N/A", bg: p.COL_UNAV };
    case "LD":   return { value: "LD",  bg: p.COL_AVAIL };
    case "N":    return { value: "N",   bg: p.COL_AVAIL };
    case "LD/N": return { value: "LD/N",bg: p.COL_AVAIL };
    default:     return { value: "",    bg: "#ffffff" };
  }
}

// ---------- Misc ----------
function myFunction() {
  // updateTokens();
}
// ========== STAGED-HEADERS + ZERO-LATENCY FLIP HELPERS (Option A) ==========

// Store a staged 14-day headers array (no global flip yet).
// Each entry should look like: { ymd, displayDay, displayDate }
function _headersStageSet(arr) {
  if (!Array.isArray(arr) || arr.length !== 14) {
    throw new Error("headersStageSet: expected 14 staged headers");
  }
  const norm = arr.map(h => ({
    ymd: String(h.ymd || ""),
    displayDay: String(h.displayDay || ""),
    displayDate: String(h.displayDate || "")
  }));
  const sp = _sp();
  sp.setProperty("HEADERS_STAGE_JSON", JSON.stringify(norm));
  sp.setProperty("HEADERS_STAGE_TS", _nowIso());
}

// Return the staged headers (or null if none).
function _headersStageGet() {
  const sp = _sp();
  const raw = sp.getProperty("HEADERS_STAGE_JSON");
  if (!raw) return null;
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : null;
  } catch (_) {
    return null;
  }
}

// Clear any staged headers + prepared next rev marker.
function _headersStageClear() {
  const sp = _sp();
  sp.deleteProperty("HEADERS_STAGE_JSON");
  sp.deleteProperty("HEADERS_STAGE_TS");
  sp.deleteProperty("TILES_REV_NEXT");
}

// Set TILES_REV explicitly (no bump side-effects).
function _setTilesRevExplicit(n) {
  const v = parseInt(n, 10);
  if (!isFinite(v) || v < 0) throw new Error("setTilesRevExplicit: invalid rev");
  _sp().setProperty(K_TILES_REV, String(v));
}

// Get current TILES_REV as integer.
function _revGet() {
  return _getInt(K_TILES_REV, 0);
}

// Get prepared next_rev (if any). If not prepared, returns current+1.
function _revNextGet() {
  const cur = _revGet();
  const next = _getInt("TILES_REV_NEXT", 0);
  return next > 0 ? next : (cur + 1);
}

// Compute & store next_rev based on current TILES_REV; return it.
// If a next_rev already exists and is ahead of current, keep it.
function _prepareNextTilesRev() {
  const sp = _sp();
  const cur = _revGet();
  const existing = _getInt("TILES_REV_NEXT", 0);
  const next = existing > cur ? existing : (cur + 1);
  sp.setProperty("TILES_REV_NEXT", String(next));
  return next;
}

// Publish staged headers into HEADERS_JSON and atomically flip TILES_REV → next_rev.
// Clears staged headers and the NEXT marker afterwards.
function _publishStagedHeadersAndFlip() {
  const sp = _sp();
  const staged = _headersStageGet();
  if (!staged || staged.length !== 14) {
    return { ok: false, reason: "NO_STAGED_HEADERS" };
  }

  // Commit headers memory (do NOT bump here; we flip explicitly below)
  sp.setProperty(K_HEADERS_JSON, JSON.stringify(staged));
  sp.setProperty(K_HEADERS_TS, _nowIso());

  // Flip global rev to the prepared value (or current+1 if missing)
  const nextRev = _revNextGet();
  _setTilesRevExplicit(nextRev);

  // Clear staging artifacts
  _headersStageClear();

  return { ok: true, nextRev, headersCount: 14 };
}

function _listActiveTokenMsisdns() {
  const ss = _ssLinks();
  const sh = ss.getSheetByName(_P().SH_LINKS);
  if (!sh) return [];

  const rows = sh.getDataRange().getDisplayValues();
  if (rows.length <= 1) return [];

  // Header indexes by the standard schema:
  // ["Surname","First","Telephone","Email","Token","URL","Status","CreatedAt","RevokedAt","AdminAction"]
  const hdr = rows[0].map(s => String(s || "").toLowerCase());
  const idxTEL = hdr.indexOf("telephone"); // 2
  const idxSTA = hdr.indexOf("status");    // 6
  // NOTE: do NOT require a non-empty token anymore (post-reset users may have none)

  const out = new Set();
  for (let i = 1; i < rows.length; i++) {
    const status = (idxSTA >= 0 ? String(rows[i][idxSTA] || "") : "").trim();
    if (status !== "Active") continue;

    const tel = _normaliseLocalTel((idxTEL >= 0 ? rows[i][idxTEL] : rows[i][2]) || "");
    if (!tel) continue;

    out.add(tel);
  }
  return Array.from(out);
}

// Return list of msisdns (local 0XXXXXXXXXX) for all rows with Status="Active" in Links sheet.


// Warm a single msisdn’s tiles using a specific revision (revOverride) and the STAGED headers.
// Returns true on success, false otherwise.



// Orchestrator for zero-latency flip (Option A).
// 1) Prepare next_rev
// 2) Fetch active-token msisdns
// 3) Warm everyone with revOverride = next_rev, using staged headers
// 4) Publish staged headers + flip global TILES_REV to next_rev



function _setLinksNewUser(tel, value /* 'YES'|'ALERT'|'NO' */) {
  const rec = _getLinksRowByTel(tel);
  if (!rec || rec.m.NEW < 0) return false;
  rec.sh.getRange(rec.rowIndex, rec.m.NEW + 1).setValue(String(value || 'NO'));
  return true;
}







/** ===================== Message Editor (Change Messages) ===================== **/

// Property keys (Script Properties)

// Add these to your onOpen() menu builder (or extend your existing one)
// ============================
// Menu
// ============================
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Availability API')
    .addItem('Emergency Contacts Settings', 'showEmergencyContactsUI')
    .addSeparator()
    .addItem('Update tokens', 'updateTokens')
    .addSeparator()
    .addSubMenu(
      ui.createMenu('Change Messages')
        .addItem('Apple Welcome on First Load',   'openMessageEditor_WelcomeApple')
        .addItem('Android Welcome on First Load', 'openMessageEditor_WelcomeAndroid')
        .addItem('Unknown Device Welcome on First Load', 'openMessageEditor_WelcomeUnknown')
        .addItem('Welcome Email (New User + Reset)', 'openMessageEditor_WelcomeEmail')
        .addItem('Alert Message', 'openMessageEditor_Alert')
        .addSeparator()
        .addItem('Accommodation Contacts', 'openMessageEditor_Accommodation')
        .addItem('Hospital Addresses',     'openMessageEditor_Hospital')
        .addItem('Timesheet email message','openMessageEditor_Timesheet')
    )
    .addSeparator()
    .addItem('Script Properties…', 'spManageScriptProperties') // <-- added
    .addToUi();
}

function _openMessageEditorDialog_(kind) {
  const t = String(kind || '').toUpperCase();
  const template = HtmlService.createTemplateFromFile('MessageEditor');
  template.kind = t;

  const title =
    t === 'WELCOME_APPLE'   ? 'Apple Welcome on First Load' :
    t === 'WELCOME_ANDROID' ? 'Android Welcome on First Load' :
    t === 'WELCOME_UNKNOWN' ? 'Unknown Device Welcome on First Load' :
    t === 'WELCOME_EMAIL'   ? 'Welcome Email (New User + Reset)' :
    t === 'ALERT'           ? 'Alert Message' :
    t === 'ACCOMMODATION'   ? 'Accommodation Contacts' :
    t === 'HOSPITAL'        ? 'Hospital Addresses' :
    t === 'TIMESHEET'       ? 'Timesheet Email Message' :
                              'Message';

  const html = template.evaluate()
    .setSandboxMode(HtmlService.SandboxMode.IFRAME)
    .setWidth(720)
    .setHeight(620);

  SpreadsheetApp.getUi().showModalDialog(html, title);
}

// ============================
// Dialog launcher
// ============================


// ============================
// Openers for each message kind
// ============================
// New wrappers
function openMessageEditor_WelcomeApple()   { _openMessageEditorDialog_('WELCOME_APPLE'); }
function openMessageEditor_WelcomeAndroid() { _openMessageEditorDialog_('WELCOME_ANDROID'); }
function openMessageEditor_WelcomeUnknown() { _openMessageEditorDialog_('WELCOME_UNKNOWN'); }
function openMessageEditor_WelcomeEmail()   { _openMessageEditorDialog_('WELCOME_EMAIL'); }

function openMessageEditor_Accommodation()  { _openMessageEditorDialog_('ACCOMMODATION'); }
function openMessageEditor_Hospital()       { _openMessageEditorDialog_('HOSPITAL'); }
function openMessageEditor_Timesheet()      { _openMessageEditorDialog_('TIMESHEET'); }
function openMessageEditor_Alert() {_openMessageEditorDialog_('ALERT');}


// ============================
// Key mapping for load/save (used by your existing get/save helpers)
// ============================

/** Update your global keys object */
const _MSG_KEYS = {
  WELCOME_APPLE:          'WELCOME_MESSAGE_APPLE_HTML',     // Apple Welcome on First Load
  WELCOME_ANDROID:        'WELCOME_MESSAGE_ANDROID_HTML',   // Android Welcome on First Load
  WELCOME_UNKNOWN:        'WELCOME_MESSAGE_HTML',           // Unknown Device Welcome on First Load
  WELCOME_EMAIL_NEW_USER: 'WELCOME_TO_APP_HTML',            // Welcome Email New User with password reset

  ALERT:                  'ALERT_MESSAGE_HTML',
  ACCOMMODATION:          'ACCOMMODATION_CONTACTS_HTML',
  HOSPITAL:               'HOSPITAL_ADDRESSES_HTML',
  TIMESHEET:              'TIMESHEET_EMAIL_HTML'
};

/** Map menu kind → Script Property key */
function _resolveKindToKey_(kind) {
  switch (String(kind || '').toUpperCase()) {
    case 'WELCOME_APPLE':   return 'WELCOME_MESSAGE_APPLE_HTML';
    case 'WELCOME_ANDROID': return 'WELCOME_MESSAGE_ANDROID_HTML';
    case 'WELCOME_UNKNOWN': return 'WELCOME_MESSAGE_HTML';
    case 'WELCOME_EMAIL':   return 'WELCOME_TO_APP_HTML';          // ✅ New user welcome email + reset
    case 'ALERT':           return 'ALERT_MESSAGE_HTML';
    case 'ACCOMMODATION':   return 'ACCOMMODATION_CONTACTS_HTML';
    case 'HOSPITAL':        return 'HOSPITAL_ADDRESSES_HTML';
    case 'TIMESHEET':       return 'TIMESHEET_EMAIL_HTML';
    default: return null;
  }
}



/** Strict sanitizer:
 *  - Keeps only: <b>, <strong>, <u>, <p>, <div>, <br>
 *  - Drops ALL attributes
 *  - Normalizes <strong> → <b>
 *  - Allows text nodes and line breaks
 */

/**
 * Extract a token from a variety of user inputs:
 *  - Full URL: "https://availability.arthur-rai.co.uk?k=XYZ..."
 *  - Query string fragment: "?k=XYZ..." or "k=XYZ..."
 *  - Raw token: "XYZ..."
 * Returns the token string or null if not found.
 */


/**
 * Basic sanity check for your web‑safe base64-ish token shape.
 * Your generator uses base64url of a SHA-256 digest (no padding).
 */
function _isPlausibleToken(s) {
  const t = String(s || '').trim();
  // 20+ chars, URL‑safe Base64 chars only
  return /^[A-Za-z0-9_-]{20,}$/.test(t);
}

/**
 * Accept a user-pasted string (full URL or token), extract k, validate it
 * against the Links sheet (must be Active), and return the resolved msisdn.
 *
 * Returns:
 *  { ok:true, token, msisdn }   on success
 *  { ok:false, error:"..." }    on failure
 */


/**
 * (Optional) Redact a token for logs.
 * Example: abcdef...wxyz (keep 6 + 4 visible)
 */
function _maskTokenForLog(token) {
  const t = String(token || '');
  if (t.length <= 12) return '***';
  return t.slice(0, 6) + '…' + t.slice(-4);
}
/**
 * Sanitize arbitrary HTML while allowing a tiny, email‑friendly subset:
 * Allowed tags: <b>, <i>, <u>, <p>, <div>, <br>, <a>, <ul>, <ol>, <li>
 * - All attributes are dropped except safe `href` on <a>
 * - href schemes allowed: http, https, mailto
 * - Normalizes <strong>→<b>, <em>→<i>, and <br/> variants → <br>
 * - Removes comments, <script>, and <style> blocks
 */
function _sanitizeLimitedHtml(input) {
  if (input == null || input === '') return '';

  // 1) Strip comments and dangerous blocks entirely
  let s = String(input)
    .replace(/<!--[\s\S]*?-->/g, '')                           // HTML comments
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, '')       // script blocks
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, '');        // style blocks

  // 2) Normalize common markup variants
  s = s
    .replace(/<\s*strong\b[^>]*>/gi, '<b>')
    .replace(/<\s*\/\s*strong\s*>/gi, '</b>')
    .replace(/<\s*em\b[^>]*>/gi, '<i>')
    .replace(/<\s*\/\s*em\s*>/gi, '</i>')
    .replace(/<\s*br\s*\/?>/gi, '<br>');

  // 3) Whitelist pass: rebuild tags, drop disallowed, scrub attributes
  // Allowed tags (open + close); treat <br> as a special self-closing
  var ALLOWED = new Set(['b','i','u','p','div','br','a','ul','ol','li']);
  var SELF_CLOSING = new Set(['br']);

  // Only keep href on <a>, and only if it’s http(s) or mailto
  function _sanitizeHref(raw) {
    var v = String(raw || '').trim().replace(/"/g, '&quot;'); // minimal escaping
    // decode basic entities that might hide schemes (defensive but simple)
    v = v.replace(/&amp;/gi, '&');
    // Reject javascript:, data:, vbscript:, etc.
    if (!/^(https?:|mailto:)/i.test(v)) return '';
    return v;
  }

  // Replace every tag with a sanitized version or nothing
  s = s.replace(/<\s*\/?\s*([a-zA-Z0-9]+)([^>]*)>/g, function (_, tag, attrs) {
    var t = String(tag || '').toLowerCase();

    // Handle closing tags
    if (/^\//.test(tag)) {
      var tn = t.replace(/^\//, '');
      return ALLOWED.has(tn) ? ('</' + tn + '>') : '';
    }

    // Self-closing normalization (we don’t keep attributes on br anyway)
    if (!ALLOWED.has(t)) return '';
    if (SELF_CLOSING.has(t)) return '<' + t + '>';

    // Keep only safe href on <a>
    if (t === 'a') {
      // Try to find href=... in the original attrs
      var m = String(attrs || '').match(/\bhref\s*=\s*(['"])(.*?)\1/i) ||
              String(attrs || '').match(/\bhref\s*=\s*([^'"\s>]+)/i);
      var href = m ? _sanitizeHref(m[2] != null ? m[2] : m[1]) : '';
      return href ? '<a href="' + href + '">' : '<a>';
    }

    // All other allowed tags: drop attributes
    return '<' + t + '>';
  });

  // 4) Lightweight final guard: if any rogue <script> slipped through, nuke it
  if (/<\s*script\b/i.test(s)) {
    s = s.replace(/<\s*script[\s\S]*?>[\s\S]*?<\/script>/gi, '');
  }

  // 5) Normalize whitespace a bit; keep user-intended line breaks
  s = s.replace(/\r\n/g, '\n').trim();

  return s;
}
/** Lightweight logger for token/auth/editor/attachment events into Logs sheet */
function _logTokenEvent(phase, data) {
  // logging disabled – no-op
}

/** Try to give a clear human explanation for token-lookup failures */
function _explainTokenLookupFailure(reason, ctx) {
  switch (reason) {
    case 'NO_TOKEN_IN_TEXT':
      return 'No token found in pasted text. Expected full URL containing "?k=…", a "k=…" snippet, or a bare token (20+ URL-safe chars).';
    case 'NOT_ACTIVE_OR_UNKNOWN':
      return 'Token was parsed, but not found with Status="Active" in Links sheet (or telephone missing/invalid).';
    case 'LINKS_SHEET_MISSING':
      return 'Links sheet not found or missing required headers (Token/Status/Telephone).';
    default:
      return 'Token lookup failed (generic).';
  }
}

/** If a Drive file is a native Google type, convert to PDF and fix the filename. */
function _coerceBlobToSendablePDF_(file) {
  if (!file) return null;
  const mime = file.getMimeType();
  // Native Google types require conversion
  const isGoogleType = String(mime || '').indexOf('application/vnd.google-apps') === 0;
  if (isGoogleType) {
    const pdfBlob = file.getAs(MimeType.PDF);
    // Ensure filename has .pdf
    const base = file.getName().replace(/\.[^.]+$/,'');
    pdfBlob.setName(base + '.pdf');
    return pdfBlob;
  }
  // Non-Google binaries (e.g., PDF, DOCX) can be sent as-is
  return file.getBlob ? file.getBlob() : file;
}

/** Ensure a filename (string) ends with given extension; used for attachment naming. */
function _ensureExt(name, ext) {
  const e = String(ext || '').replace(/^\./,'');
  const n = String(name || 'attachment');
  return /\.[A-Za-z0-9]+$/.test(n) ? n : (n + '.' + e);
}


function _extractTokenFromUserString(input) {
  const raw = String(input || "").trim();
  if (!raw) return null;

  // 1) If it's a full/partial URL, try URLSearchParams
  try {
    // Works for absolute URLs; for fragments, try to coerce
    let urlObj = null;

    if (/^https?:\/\//i.test(raw)) {
      urlObj = new URL(raw);
    } else if (raw.startsWith('?') || raw.includes('k=')) {
      // Coerce a query-only string into a URL for parsing
      urlObj = new URL('https://x.invalid/' + (raw.startsWith('?') ? raw : ('?' + raw)));
    }

    if (urlObj) {
      const k = urlObj.searchParams.get('k');
      if (k && _isPlausibleToken(k)) return k.trim();
    }
  } catch (_) {
    // fall through to regex parsing
  }

  // 2) Look for k=XYZ anywhere in the text
  const m = raw.match(/[?&]k=([A-Za-z0-9_-]+)/) || raw.match(/\bk=([A-Za-z0-9_-]+)/);
  if (m && m[1] && _isPlausibleToken(m[1])) return m[1].trim();

  // 3) If the whole string looks like a token, accept it
  if (_isPlausibleToken(raw)) return raw;

  return null;
}


function _getLinksRowByTel(tel) {
  const ss = _ssLinks();
  const sh = ss.getSheetByName(_P().SH_LINKS);
  if (!sh) return null;
  const { m } = _linksHeaderInfo(sh);
  if (m.TEL < 0) return null;

  const vals = sh.getDataRange().getDisplayValues();
  for (let r = 1; r < vals.length; r++) {
    const rowTel = _normaliseLocalTel(vals[r][m.TEL]);
    if (rowTel === tel) {
      return { sh, rowIndex: r + 1, row: vals[r], m };
    }
  }
  return null;
}


function _sendTimesheetForMsisdn(msisdn) {
  try {
    const rec = _getLinksRowByTel(msisdn);
    if (!rec) return { ok: false, error: "LINKS_ROW_NOT_FOUND" };
    const email = String(rec.row[rec.m.EMA] || "").trim();
    const first = String(rec.row[rec.m.FIR] || "").trim();
    if (!email) return { ok: false, error: "NO_EMAIL_IN_LINKS" };

    const props = PropertiesService.getScriptProperties();
    const hook = props.getProperty("POWER_EMAIL");
    if (!hook) return { ok: false, error: "POWER_EMAIL_NOT_CONFIGURED" };

    // Timesheet template message (HTML) with {{firstName}}
    let html = props.getProperty("TIMESHEET_EMAIL_HTML") || "";
    if (!html) {
      html = _sanitizeLimitedHtml(
        'Dear {{firstName}},<br><br>Please find attached a blank timesheet as requested.<br><br>Many thanks,<br>Sussex Packages of Care Team.<br>Arthur Rai Medical Services.'
      );
    }
    html = html.replace(/\{\{\s*firstName\s*\}\}/gi, first || "");

    // Subject
    const subject = "Timesheet";

    // Attachment: by property TIMESHEET_FILE_ID (preferred) or TIMESHEET_FILE_URL
    let attachBlob = _getTimesheetBlob();
    if (!attachBlob) {
      _logTokenEvent('TIMESHEET_ATTACHMENT_MISSING', {
        msisdn,
        note: 'No file found via TIMESHEET_FILE_ID / TIMESHEET_FILE_URL; sending without attachment (will likely fail schema).'
      });
    }

    // Build attachmentsV2 per schema
    const attachmentsV2 = [];
    if (attachBlob) {
      let name = attachBlob.getName() || 'timesheet';
      // If the blob claims a Google Apps MIME, we likely coerced it already; still ensure .pdf
      name = _ensureExt(name, 'pdf');
      attachmentsV2.push({
        Name: name,
        ContentBytes: Utilities.base64Encode(attachBlob.getBytes())
      });
    }

    const payload = {
      to: email,
      subject,
      htmlBody: html,
      body: html,           // fallback
      attachmentsV2
    };

    const res = _postToPowerEmail(hook, payload);

    _logTokenEvent('TIMESHEET_SEND_RESULT', {
      msisdn,
      to: email,
      hasAttachment: attachmentsV2.length > 0,
      attachName: attachmentsV2.length ? attachmentsV2[0].Name : '',
      status: res && res.status,
      ok: !!(res && res.ok),
      error: res && res.error,
      note: (attachmentsV2.length ? 'Email payload had an attachment.' : 'Email payload had NO attachment.')
    });

    return res;
  } catch (e) {
    const err = String(e && e.message || e);
    _logTokenEvent('TIMESHEET_SEND_EXCEPTION', {
      msisdn,
      error: err,
      note: 'Unexpected error while preparing/sending timesheet.'
    });
    return { ok: false, error: err };
  }
}
function _getTimesheetBlob() {
  const props = PropertiesService.getScriptProperties();
  const fileId = props.getProperty("TIMESHEET_FILE_ID");

  // Try by explicit file ID first
  if (fileId) {
    try {
      const file = DriveApp.getFileById(fileId);
      const blob = _coerceBlobToSendablePDF_(file);
      if (blob) return blob;
      _logTokenEvent('ATTACHMENT_COERCE_FAIL', {
        fileId,
        note: 'Could not convert fileId to sendable blob.'
      });
    } catch (e) {
      _logTokenEvent('ATTACHMENT_GET_BY_ID_FAIL', {
        fileId,
        error: String(e && e.message || e),
        note: 'DriveApp.getFileById failed.'
      });
    }
  }

  // Fallback: URL that looks like Google Drive "file/d/{id}/view"
  const url = props.getProperty("TIMESHEET_FILE_URL");
  if (url) {
    const m = String(url).match(/\/d\/([A-Za-z0-9_-]{20,})\//);
    const id = m ? m[1] : null;
    if (id) {
      try {
        const file = DriveApp.getFileById(id);
        const blob = _coerceBlobToSendablePDF_(file);
        if (blob) return blob;
        _logTokenEvent('ATTACHMENT_COERCE_FAIL', {
          urlFragment: String(url).slice(0, 120),
          fileId: id,
          note: 'Could not convert URL-derived fileId to sendable blob.'
        });
      } catch (e2) {
        _logTokenEvent('ATTACHMENT_GET_BY_URL_FAIL', {
          urlFragment: String(url).slice(0, 120),
          error: String(e2 && e2.message || e2),
          note: 'DriveApp.getFileById failed for URL-derived ID.'
        });
      }
    } else {
      _logTokenEvent('ATTACHMENT_URL_PARSE_FAIL', {
        urlFragment: String(url).slice(0, 120),
        note: 'Could not parse file ID from TIMESHEET_FILE_URL.'
      });
    }
  }

  return null;
}

// Finds the most-recent unacknowledged emergency alert by scanning the sheet.
// Returns an object shaped like _emergencyFindById(), or null if none.
function _emergencyFindMostRecentUnacked() {
  // Use the same sheet accessor your EA helpers use
  var sh = _eaSheet_();
  if (!sh) return null;

  var lastRow = sh.getLastRow();
  var lastCol = sh.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return null; // no data rows

  // Read headers exactly as stored (must match _EA_HEADERS)
  var headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(function (h) { return String(h || '').trim(); });
  function col(name) {
    var i = headers.indexOf(name);
    if (i >= 0) return i;
    // case-insensitive fallback
    var lower = headers.map(function (h) { return h.toLowerCase(); });
    return lower.indexOf(String(name).toLowerCase());
  }

  var cAlertId = col('alert_id');
  var cStatus  = col('status');
  var cAck     = col('acknowledged');

  if (cAlertId < 0 || cStatus < 0 || cAck < 0) return null;

  // Scan from the bottom (newest appended rows first)
  var vals = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
  for (var r = vals.length - 1; r >= 0; r--) {
    var row = vals[r];
    var status = String(row[cStatus] || '').toUpperCase();
    var ackRaw = row[cAck];
    var ack = (ackRaw === true) || (String(ackRaw).toUpperCase() === 'TRUE');

    // Consider unacknowledged and not already terminal
    if (!ack && status !== 'ACKED' && status !== 'CLOSED') {
      // Build object with the same keys as the sheet headers (like _emergencyFindById)
      var obj = {};
      for (var c = 0; c < headers.length; c++) {
        obj[headers[c]] = row[c];
      }

      // Also ensure canonical fields present/normalized
      obj.alert_id      = String(row[cAlertId] || '');
      obj.status        = status;
      obj.acknowledged  = ack;
      obj.row_index     = r + 2; // actual sheet row number

      // Expand JSON fields for parity with _emergencyFindById
      try { obj.wati_message_ids = JSON.parse(String(obj.wati_message_ids_json || '[]')); } catch(_){ obj.wati_message_ids = []; }
      try { obj.call_queue        = JSON.parse(String(obj.call_queue_json || '[]')); }     catch(_){ obj.call_queue = []; }
      try { obj.call_map          = JSON.parse(String(obj.call_map_json || '{}')); }       catch(_){ obj.call_map = {}; }
      try { obj.log               = JSON.parse(String(obj.log_json || '[]')); }            catch(_){ obj.log = []; }

      return obj;
    }
  }

  return null;
}


/***** ============================================================
 * EMERGENCY ALERTS — NEW FUNCTIONS (Additive / Non-invasive)
 * ============================================================ *****/

/** ===== Sheet schema & helpers ===== **/

const _EA_SHEET_NAME = 'EmergencyAlerts';
const _EA_HEADERS = [
  'alert_id', 'created_iso', 'updated_iso',
  'candidate_msisdn', 'candidate_name', 'candidate_email',
  'shift_ymd', 'shift_type', 'shift_start_iso', 'shift_end_iso',
  'hospital', 'ward', 'job_title', 'booking_ref',
  'date_label', 'time_range_label',
  'issue_type', 'eta_or_leave_time_label', 'reason_text',
  'subject_name', 'subject_msisdn',
  'status', 'acknowledged', 'ack_source',
  'ack_name', 'ack_number', 'ack_when_iso',
  'wati_message_ids_json',
  'call_queue_json', 'call_index', 'call_next_at_iso',
  'escalation_trigger_id', 'stepper_trigger_id',
  'call_map_json',        // { callId: {to, placed_at_iso} }
  'log_json',             // [{ts,msg}]
];

function _eaSheet_() {
  const ss = SpreadsheetApp.openById(_P().SPREADSHEET_ID);
  let sh = ss.getSheetByName(_EA_SHEET_NAME);
  if (!sh) {
    sh = ss.insertSheet(_EA_SHEET_NAME);
    sh.appendRow(_EA_HEADERS);
    return sh;
  }

  // Ensure required headers exist; append any missing (non-destructive).
  const lastCol = sh.getLastColumn() || 1;
  const hdrRange = sh.getRange(1, 1, 1, lastCol);
  const existing = hdrRange.getValues()[0].map(h => String(h || '').trim());

  let changed = false;
  _EA_HEADERS.forEach((need) => {
    if (!existing.includes(need)) {
      sh.getRange(1, sh.getLastColumn() + 1).setValue(need);
      changed = true;
    }
  });

  // If headers row was empty, write full header set once.
  if (!existing.some(Boolean)) {
    sh.getRange(1, 1, 1, _EA_HEADERS.length).setValues([_EA_HEADERS]);
    return sh;
  }

  // No destructive re-ordering; _eaIndexMap_ resolves names → indices dynamically.
  if (changed) {
    // Expand header range length so _eaIndexMap_ can see the new columns immediately.
    sh.getRange(1, 1, 1, sh.getLastColumn()).getValues();
  }
  return sh;
}


function _eaFindRowById_(alert_id) {
  const { sh, m } = _eaIndexMap_();
  const rng = sh.getRange(2, m.alert_id+1, sh.getLastRow()-1, 1).getValues();
  for (let i=0;i<rng.length;i++) {
    if (String(rng[i][0]||'') === String(alert_id)) {
      return { sh, m, rowIndex: i+2 };
    }
  }
  return null;
}
function _eaAppend_(obj) {
  const { sh, m } = _eaIndexMap_();
  const row = new Array(Object.keys(m).length).fill('');
  Object.keys(obj).forEach(k => {
    if (m[k] != null) row[m[k]] = obj[k];
  });
  sh.appendRow(row);
  return { sh, m, rowIndex: sh.getLastRow() };
}
function _eaPatch_(alert_id, patch) {
  const rec = _eaFindRowById_(alert_id);
  if (!rec) return false;
  const { sh, m, rowIndex } = rec;
  Object.keys(patch).forEach(k => {
    if (m[k] != null) sh.getRange(rowIndex, m[k]+1).setValue(patch[k]);
  });
  sh.getRange(rowIndex, m.updated_iso+1).setValue(_nowIso());
  return true;
}
function _eaAppendLog_(alert_id, msg) {
  const rec = _eaFindRowById_(alert_id);
  if (!rec) return;
  const { sh, m, rowIndex } = rec;
  const cur = sh.getRange(rowIndex, m.log_json+1).getValue();
  let arr = [];
  try { arr = cur ? JSON.parse(cur) : []; } catch(_) {}
  arr.push({ ts:_nowIso(), msg: String(msg||'') });
  sh.getRange(rowIndex, m.log_json+1).setValue(JSON.stringify(arr));
}

/** ===== Core ===== **/



/** ===== Timing & formatting ===== **/

function _parseNotesToTimes(notesString, ymd, tz) {
  if (!notesString) return null;
  const txt = String(notesString);
  const rx = /Start:\s*([0-2]?\d:\d{2})\s*Finish:\s*([0-2]?\d:\d{2})/i;
  const m = rx.exec(txt);
  if (!m) return null;

  const [Y, M, D] = ymd.split('-').map(Number);
  const [sh, sm] = m[1].split(':').map(Number);
  const [eh, em] = m[2].split(':').map(Number);

  function localTimeToIsoZ(Y, M, D, h, m, tz) {
    const noonUtc = Date.UTC(Y, M - 1, D, 12, 0, 0);
    const z = Utilities.formatDate(new Date(noonUtc), tz || 'Europe/London', 'Z'); // e.g. +0100
    const sign = z[0] === '-' ? -1 : 1;
    const offMin = sign * (parseInt(z.slice(1,3),10)*60 + parseInt(z.slice(3,5),10));
    const utcMidnight = Date.UTC(Y, M-1, D, 0,0,0);
    const localMin = h*60 + m;
    const ms = utcMidnight + (localMin - offMin) * 60000;
    return _toIsoZ_(new Date(ms));
  }

  const startIso = localTimeToIsoZ(Y, M, D, sh, sm, tz);
  let endDay = D;
  if (eh*60 + em <= sh*60 + sm) endDay += 1; // overnight
  const endIso = localTimeToIsoZ(Y, M, endDay, eh, em, tz);

  return {
    startAtIso: startIso,
    endAtIso: endIso,
    overnight: endDay !== D
  };
}
function _resolveShiftTimesForBooking(booking, tz) {
  const ymd   = String(booking.ymd);
  const notes = booking.shiftInfo || '';

  // 1) Honour explicit times in notes: "Start: HH:mm Finish: HH:mm"
  const parsed = _parseNotesToTimes(notes, ymd, tz);
  if (parsed) {
    return {
      startAtIso: parsed.startAtIso,
      endAtIso:   parsed.endAtIso,
      // prefer explicit shiftType when provided; else infer from overnight
      type: (booking.shiftType || '').toUpperCase() || (parsed.overnight ? 'NIGHT' : 'LONG DAY')
    };
  }

  // 2) Fallback to canonical default times
  //    detect NIGHT if either shiftType or shiftInfo contains "NIGHT"
  const [Y, M, D] = ymd.split('-').map(Number);
  const shift = /\bNIGHT\b/.test(`${(booking.shiftType||'')} ${(booking.shiftInfo||'')}`.toUpperCase())
    ? 'NIGHT'
    : 'LONG DAY';

  const start = shift === 'NIGHT' ? { h: 19, m: 30 } : { h: 7,  m: 30 };
  const end   = shift === 'NIGHT' ? { h: 8,  m: 0  } : { h: 20, m: 0  };

  function localTimeToIsoZ(Y, M, D, h, m, tz) {
    const noonUtc = Date.UTC(Y, M - 1, D, 12, 0, 0);
    const z = Utilities.formatDate(new Date(noonUtc), tz || 'Europe/London', 'Z'); // +0100 / +0000
    const sign = z[0] === '-' ? -1 : 1;
    const offMin = sign * (parseInt(z.slice(1,3),10) * 60 + parseInt(z.slice(3,5),10));
    const utcMidnight = Date.UTC(Y, M - 1, D, 0, 0, 0);
    const localMin = h * 60 + m;
    const ms = utcMidnight + (localMin - offMin) * 60000;
    return _toIsoZ_(new Date(ms));
  }

  const startIso = localTimeToIsoZ(Y, M, D, start.h, start.m, tz);
  const endIso   = localTimeToIsoZ(Y, M, D + (shift === 'NIGHT' ? 1 : 0), end.h, end.m, tz);

  return {
    startAtIso: startIso,
    endAtIso:   endIso,
    type: shift
  };
}

function _formatDateLabel(ymd, tz) {
  tz = tz || _P().TZ || 'Europe/London';
  const [Y,M,D]= ymd.split('-').map(Number);
  const d = new Date(Y, M-1, D);
  const now = _nowInTZ_(tz);
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const that  = new Date(Y, M-1, D);
  const deltaDays = Math.round((that - today)/(24*3600*1000));
  const prefix = (deltaDays === 0) ? 'TODAY' : (deltaDays === 1 ? 'TOMORROW' : '');
  const dateTxt = Utilities.formatDate(d, tz, 'EEEE d MMMM yyyy');
  return (prefix ? (prefix+' '+dateTxt) : dateTxt);
}

function _formatTimeRangeLabel(startIso, endIso, tz) {
  tz = tz || _P().TZ || 'Europe/London';
  const s = _formatHhmmHrs_(startIso, tz);
  const e = _formatHhmmHrs_(endIso, tz);
  return `${s}–${e}`;
}

function _formatHhmmHrs_(iso, tz) {
  const d = new Date(iso);
  return Utilities.formatDate(d, tz, 'HH:mm') + 'hrs';
}

function _normaliseUkMsisdnTo447(msisdn) {
  if (!msisdn) return '';
  let s = String(msisdn).replace(/\s+/g,'').replace(/[^\d+]/g,'');
  if (s.startsWith('+')) s = s.slice(1);
  if (s.startsWith('0044')) s = '44' + s.slice(4);
  if (s.startsWith('0')) s = '44' + s.slice(1);
  if (!s.startsWith('44')) s = '44' + s;
  return s;
}

/** ===== Config & data ===== **/

function _getEmergencyContacts() {
  // ────────────────────────────────────────────────────────────────
  // Logging helper (route-scoped)
  // ────────────────────────────────────────────────────────────────
  function L(event, data) { try { _log('EMERG_CONTACTS', event, data || {}); } catch (_) {} }
  function maskNum(n) {
    if (!n) return '';
    var s = String(n).replace(/[^\d]/g, '');
    if (s.length <= 6) return s;
    return s.slice(0, 3) + '…' + s.slice(-3);
  }

  // Read Script Properties
  const P   = (typeof _P === 'function') ? _P() : {};
  const raw = (P && P.EMERGENCY_CONTACTS) || '[]';
  const tzDefault = (P && P.TZ) || 'Europe/London';

  L('prop_read', {
    raw_len: raw.length,
    tzDefault
  });

  // Parse JSON (with error logging)
  let list = [];
  try {
    list = JSON.parse(raw);
    L('prop_parsed_ok', { count: Array.isArray(list) ? list.length : 0, type: typeof list });
  } catch (e) {
    L('prop_parse_error', { error: String(e && e.message || e) });
    list = [];
  }

  // Helpers
  function todayYmdInTz(d, tz) {
    return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
  }
  function isBlackoutToday(contact, tz, ymdToday) {
    const bl = Array.isArray(contact.blackouts) ? contact.blackouts : [];
    for (let i = 0; i < bl.length; i++) {
      const item = bl[i] || {};
      const one  = String(item.date || '').trim();
      const from = String(item.from || '').trim();
      const to   = String(item.to   || '').trim();
      if (one) {
        if (ymdToday === one) return true;
      } else if (from && to) {
        if (ymdToday >= from && ymdToday <= to) return true; // inclusive
      }
    }
    return false;
  }

  // Normaliser dependency: log if missing
  if (typeof _normaliseUkMsisdnTo447 !== 'function') {
    L('normaliser_missing', { hint: '_normaliseUkMsisdnTo447 is not defined' });
  }

  const now = new Date();

  // Pre-map diagnostics
  L('map_begin', { input_count: Array.isArray(list) ? list.length : 0 });

  const mapped = (Array.isArray(list) ? list : []).map((x, idx) => {
    const firstname = String(x.firstname || '').trim();
    const surname   = String(x.surname   || '').trim();

    const sourceNumberRaw = x.mobile || x.msisdn || x.whatsappNumber || '';
    const mobile447 = (typeof _normaliseUkMsisdnTo447 === 'function')
      ? _normaliseUkMsisdnTo447(sourceNumberRaw)
      : String(sourceNumberRaw);

    const cw = (x.call_windows && typeof x.call_windows === 'object') ? x.call_windows : null;

    let bl = [];
    try { if (Array.isArray(x.blackouts)) bl = x.blackouts.map(b => (b || {})); } catch (_) {}

    let pr = 9999;
    if (typeof x.priority === 'number' && isFinite(x.priority)) pr = x.priority;

    const enabled = (x.enabled !== false);
    const tz = String(x.timezone || tzDefault);
    const display_name = [firstname, surname].filter(Boolean).join(' ').trim();

    // Per-row trace of normalisation outcome
    L('map_row', {
      idx,
      in_firstname: firstname,
      in_surname: surname,
      in_number_raw_masked: maskNum(sourceNumberRaw),
      out_mobile447_masked: maskNum(mobile447),
      enabled,
      priority: pr,
      tz,
      blackouts_count: bl.length,
      has_call_windows: !!cw
    });

    return {
      firstname,
      surname,
      display_name,
      name: display_name,

      mobile447,
      whatsappNumber: mobile447, // alias for WATI

      enabled,
      priority: pr,
      timezone: tz,
      call_windows: cw,
      blackouts: bl
    };
  });

  // Filter 1: must have a normalised UK number (explicit UK check)
  const withNumber = mapped.filter(x => /^447\d{9}$/.test(String(x.mobile447 || '')));
  L('filter_number', {
    before: mapped.length,
    after: withNumber.length,
    dropped: mapped.length - withNumber.length,
    sample_kept: withNumber.slice(0, 3).map(c => ({ name: c.display_name, n: maskNum(c.mobile447) })),
    sample_dropped: mapped
      .filter(x => !/^447\d{9}$/.test(String(x.mobile447 || '')))
      .slice(0, 3)
      .map(c => ({ name: c.display_name, n_raw: maskNum(c.mobile447) }))
  });

  // Filter 2: enabled !== false
  const enabledOnly = withNumber.filter(x => x.enabled !== false);
  L('filter_enabled', {
    before: withNumber.length,
    after: enabledOnly.length,
    dropped: withNumber.length - enabledOnly.length
  });

  // Filter 3: blackout TODAY (in their timezone)
  const final = [];
  const droppedBlackout = [];
  enabledOnly.forEach(c => {
    const tz = c.timezone || tzDefault;
    const ymd = todayYmdInTz(now, tz);
    const blackout = isBlackoutToday(c, tz, ymd);
    if (blackout) {
      droppedBlackout.push({ name: c.display_name, n: maskNum(c.mobile447), tz, today: ymd });
    } else {
      final.push(c);
    }
  });
  L('filter_blackout_today', {
    before: enabledOnly.length,
    after: final.length,
    dropped: droppedBlackout.length,
    dropped_sample: droppedBlackout.slice(0, 5)
  });

  // Final summary
  L('result', {
    final_count: final.length,
    sample: final.slice(0, 5).map(c => ({
      name: c.display_name,
      n: maskNum(c.mobile447),
      tz: c.timezone,
      priority: c.priority
    }))
  });

  return final;
}



// Add/extend inside _getEmergencyProps()
function _getEmergencyProps() {
  const p = _P();
  const watiRaw = String(p.WATI_TOKEN || '').trim();
  let watiToken = '';
  const m = /Bearer\s+\S+/.exec(watiRaw);
  if (m) watiToken = m[0];
  const tz = p.TZ || 'Europe/London';

  let timeoutMin = Number(p.TIMEOUT_ALERTS);
  if (!(timeoutMin > 0)) timeoutMin = 45;

  return {
    TZ: tz,

    // WATI / WhatsApp
    WATI_TOKEN: watiToken,
    WATI_TENANT: '601205',
    WATI_ENDPOINT: p.WATI_ENDPOINT || '',
    DANGEROUS_FULL_WATI_LOGS: p.DANGEROUS_FULL_WATI_LOGS || '',

    TEMPLATE_NAMESPACE: p.EMERGENCY_TEMPLATE_NAMESPACE || 'Emergency',
    TEMPLATE_NAME_EMERGENCY: p.EMERGENCY_TEMPLATE_NAME || 'APPEMERGENCY',
    TEMPLATE_NAME_RESPONDED: p.EMERGENCY_RESPONDED_TEMPLATE_NAME || 'EMERGENCY_RESPONDED',

    // Used by responder-confirm WhatsApp in the updated flows
    TEMPLATE_NAME_EMERGACCEPTCONFIRM: p.EMERGENCY_ACCEPTED_CONFIRM_TEMPLATE_NAME || 'emergacceptconfirm01',
    TEMPLATE_NAME_EMERGTHANKS:        p.EMERGENCY_ALREADY_ACK_TEMPLATE_NAME     || 'emergthanks',

    // Running-late templates (unchanged)
    TEMPLATE_NAME_LATE_SAME:      p.LATE_SAME_TEMPLATE_NAME      || 'sameshiftrunninglate',
    TEMPLATE_NAME_LATE_PREV:      p.LATE_PREV_TEMPLATE_NAME      || 'previousshiftrunninglate',
    TEMPLATE_NAME_LATE_EMERGENCY: p.LATE_EMERGENCY_TEMPLATE_NAME || 'lateemergency',

// New for the "ack all" flow
TEMPLATE_NAME_CONFIRMALLALERTS:   p.EMERGENCY_CONFIRM_ALL_ALERTS_TEMPLATE_NAME   || 'emergencyconfirmallalerts',
TEMPLATE_NAME_ALLALERTSRESPONDED: p.EMERGENCY_ALL_ALERTS_RESPONDED_TEMPLATE_NAME || 'allalertsrespondedother',

    // Email + webhook
    WEBHOOK_SECRET: p.EMERGENCY_WEBHOOK_SECRET || '',
    FROM_EMAIL: p.EMERGENCY_FROM_EMAIL || 'sussex@arthur-rai.co.uk',

    // ClickSend
    CLICKSEND_USER: p.CLICKSEND_USER || '',
    CLICKSEND_API_KEY: p.CLICKSEND_API_KEY || '',
    CLICKSEND_FROM: p.CLICKSEND_FROM || '',

    // Emergency timing
    ACK_TIMEOUT_MIN: Number(p.EMERGENCY_ACK_TIMEOUT_MIN || 5),
    CALL_GAP_MIN: Number(p.EMERGENCY_CALL_GAP_MIN || 2),

    TIMEOUT_ALERTS_MIN: Number(timeoutMin),

    TEST_MODE: String(p.TEST_MODE || '').toUpperCase() === 'TRUE',
    TEST_PHONE: _normaliseUkMsisdnTo447(p.TEST_PHONE || '')
  };
}




/** ===== Notifications ===== **/

/**
 * Update WATI contact attribute "alert_id" for a list of contacts.
 * Each contact can have whatsappNumber | phone | mobile447 | mobile | msisdn.
 * Runs updates in parallel; returns a compact summary for logging.
 */
function _watiUpdateAlertIdForContacts(alert_id, contacts) {
  var cfg = _getEmergencyProps ? _getEmergencyProps() : {};
  var tenant = cfg.WATI_TENANT || '601205';
  var token  = cfg.WATI_TOKEN;

  if (!token) {
    try { _log('EMERG_INIT', 'wati_update_skipped_no_token'); } catch(_) {}
    return { ok: false, reason: 'NO_WATI_TOKEN', updated: 0, total: (contacts && contacts.length) || 0, results: [] };
  }

  // Build parallel requests (one contact per request; one attribute per request)
  var requests = (contacts || []).map(function(c) {
    var raw = c && (c.whatsappNumber || c.phone || c.mobile447 || c.mobile || c.msisdn || '');
    var msisdn = _normaliseUkMsisdnTo447(raw); // <-- your existing normaliser
    if (!msisdn || String(msisdn).trim() === '') {
      return null; // skip empties; logged below
    }

    var url = 'https://eu-api.wati.io/' + tenant + '/api/v1/updateContactAttributes/' + encodeURIComponent(msisdn);
    var payload = JSON.stringify({
      customParams: [
        { name: 'alert_id', value: String(alert_id) }
      ]
    });

    return {
      url: url,
      method: 'post',
      contentType: 'application/json',
      headers: { Authorization: token },
      payload: payload,
      muteHttpExceptions: true,
      __meta: { msisdn: msisdn }
    };
  }).filter(Boolean);

  if (!requests.length) {
    try { _log('EMERG_INIT', 'wati_update_no_valid_numbers'); } catch(_) {}
    return { ok: false, reason: 'NO_VALID_NUMBERS', updated: 0, total: 0, results: [] };
  }

  // Fire them all in parallel
  var rawResponses = [];
  try {
    rawResponses = UrlFetchApp.fetchAll(requests);
  } catch (e) {
    try { _log('EMERG_INIT', 'wati_update_fetchAll_exception', String(e && e.message || e)); } catch(_) {}
    return { ok: false, reason: 'FETCHALL_EXCEPTION', updated: 0, total: requests.length, results: [] };
  }

  // Parse & summarise
  var updated = 0;
  var results = rawResponses.map(function(resp, i) {
    var http = (resp && typeof resp.getResponseCode === 'function') ? resp.getResponseCode() : null;
    var text = (resp && typeof resp.getContentText === 'function') ? resp.getContentText() : '';
    var json = null;
    try { json = text ? JSON.parse(text) : null; } catch(_) {}

    var ok = (http >= 200 && http < 300);
    if (ok) updated++;

    return {
      msisdn: requests[i].__meta.msisdn,
      http: http,
      ok: ok,
      json: json,
      raw: ok ? undefined : (text || '')
    };
  });

  var errors = results.filter(function(r){ return !r.ok; })
                      .map(function(r){ return { msisdn: r.msisdn, http: r.http, err: (r.json && (r.json.error || r.json.message)) || r.raw || '' }; });

  try {
    _log('EMERG_INIT', 'wati_update_alert_id_summary', {
      total: results.length,
      updated: updated,
      errors: errors.slice(0, 5)
    });
  } catch(_) {}

  return { ok: true, updated: updated, total: results.length, results: results };
}


/**
 * UPDATED: Sends initial emergency notifications.
 * Now FIRST updates each contact’s WATI contact attribute "alert_id",
 * then sends the WATI template broadcast, then schedules escalation.
 */



function _sendWatiBulkTemplate_EMERGENCY_RESPONDED(alert, responderName, whenIso) {
  const cfg = _getEmergencyProps();
  const tz  = (cfg && cfg.TZ) || 'Europe/London';

  // Normalise to 447xxxxxxxxx for reliable comparisons & WATI
  function to447(n) {
    if (!n) return '';
    let s = String(n).replace(/[^\d+]/g, '');
    if (s.startsWith('+')) s = s.slice(1);
    if (s.startsWith('07') && s.length === 11) return '44' + s.slice(1);
    if (s.startsWith('447') && s.length === 12) return s;
    if (s.startsWith('44') && s.length === 12)  return s;
    return '';
  }

  // Blackout check (helper may exist; otherwise check common flags)
  function isInBlackout(contact) {
    try {
      if (typeof _isEmergencyContactInBlackout === 'function') {
        return !!_isEmergencyContactInBlackout(contact);
      }
      const flags = [
        contact && contact.blackout,
        contact && contact.blackout_today,
        contact && contact.isBlackout,
        contact && contact.is_blackout,
        contact && contact.blackoutNow
      ];
      return flags.some(v => v === true || v === 1 || String(v).toLowerCase() === 'true');
    } catch (_) {
      return false;
    }
  }

  // Acceptor’s number (exclude from broadcast) — taken from the alert record
  const exclusion447 = to447(alert && alert.ack_number);

  // Build recipient list from emergency contacts, validate numbers, exclude acceptor, exclude blackouts
  const contacts = (typeof _getEmergencyContacts === 'function') ? (_getEmergencyContacts() || []) : [];
  const valid = [];
  const invalid = [];
  contacts.forEach(c => {
    if (isInBlackout(c)) return; // exclude blackout contacts
    const n = to447(c && (c.mobile447 || c.whatsappNumber || c.msisdn || ''));
    if (n && /^447\d{9}$/.test(n)) {
      if (exclusion447 && n === exclusion447) return; // exclude acceptor
      valid.push(n);
    } else {
      invalid.push(String(c && (c.mobile447 || c.whatsappNumber || c.msisdn || '')));
    }
  });

  try {
    if (alert && alert.alert_id) {
      _eaAppendLog_(alert.alert_id, 'EMERGENCY_RESPONDED recipients valid=' + JSON.stringify(valid) + ' invalid=' + JSON.stringify(invalid));
    }
  } catch (_) {}

  // Nothing to send to (don’t hard-fail the flow)
  if (!valid.length) {
    return { httpCode: 200, json: { result: false, error: 'No valid receivers after filtering (blackouts/exclusions)' } };
  }

  const respondedAtLabel =
    Utilities.formatDate(new Date(whenIso || Date.now()), tz, 'EEEE d MMMM yyyy HH:mm') + 'hrs';

  const nameForTemplate = String(
    responderName ||
    (alert && alert.ack_name) ||
    'A team member'
  );

  const receivers = valid.map(n => ({
    whatsappNumber: n,
    customParams: [
      // ⬇⬇ EXACT to template placeholders ⬇⬇
      { name: 'Responder_name',     value: nameForTemplate },
      { name: 'Responded_at_label', value: respondedAtLabel },
      { name: 'alert_id',           value: (alert && alert.alert_id) || '' }
    ]
  }));

  // Prefer your generic sender
  if (typeof _sendWATIBulkTemplate_Generic === 'function') {
    return _sendWATIBulkTemplate_Generic(
      (cfg && cfg.TEMPLATE_NAME_RESPONDED) || 'EMERGENCY_RESPONDED',
      receivers,
      'EMERGENCY_RESPONDED',
      tz
    );
  }

  // Fallback direct API call
  const url = `https://eu-api.wati.io/${cfg.WATI_TENANT}/api/v2/sendTemplateMessages`;
  const payload = {
    template_namespace: cfg.TEMPLATE_NAMESPACE,
    template_name:      (cfg && cfg.TEMPLATE_NAME_RESPONDED) || 'EMERGENCY_RESPONDED',
    broadcast_name:     `EmergencyResp_${Utilities.formatDate(new Date(), tz, 'yyyyMMdd_HHmmss')}`,
    receivers
  };
  const resp = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: { Authorization: cfg.WATI_TOKEN },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  let json = null; try { json = JSON.parse(resp.getContentText() || ''); } catch (_) {}
  return { httpCode: resp.getResponseCode(), json };
}



function _emailEmergency(alert) {
  const cfg = _getEmergencyProps();
  const to = cfg.FROM_EMAIL;
  const subj = `[EMERGENCY] ${alert.candidate_name || 'Unknown'} — ${alert.shift_type} — ${alert.date_label}`;
  const body = [
    `Candidate: ${alert.candidate_name} (${alert.candidate_msisdn})`,
    `Role: ${alert.job_title}`,
    `Hospital: ${alert.hospital}`,
    `Ward: ${alert.ward}`,
    `Date: ${alert.date_label}`,
    `Time: ${alert.time_range_label}`,
    `Shift: ${alert.shift_type}`,
    `Issue: ${alert.issue_type}`,
    alert.eta_or_leave_time_label ? `ETA/Leave time: ${alert.eta_or_leave_time_label}` : '',
    alert.reason_text ? `Reason: ${alert.reason_text}` : '',
    `Alert ID: ${alert.alert_id}`
  ].filter(Boolean).join('\n');

  MailApp.sendEmail({
    to,
    subject: subj,
    replyTo: cfg.FROM_EMAIL,
    name: 'Emergency Alerts',
    body
  });
  _eaAppendLog_(alert.alert_id, 'Email sent to ops');
}

function _emailAck(alert, responderName, whenIso) {
  const cfg = _getEmergencyProps();
  const to = cfg.FROM_EMAIL;
  const subj = `[EMERGENCY ACK] ${responderName||'Responder'} — ${alert.candidate_name} — ${alert.date_label}`;
  const body = [
    `Responder: ${responderName||''}`,
    `When: ${Utilities.formatDate(new Date(whenIso||Date.now()), cfg.TZ, 'EEEE d MMMM yyyy HH:mm') + 'hrs'}`,
    `Candidate: ${alert.candidate_name} (${alert.candidate_msisdn})`,
    `Hospital/Ward: ${alert.hospital} / ${alert.ward}`,
    `Shift: ${alert.shift_type} ${alert.time_range_label} on ${alert.date_label}`,
    `Issue: ${alert.issue_type}`,
    alert.eta_or_leave_time_label ? `ETA/Leave time: ${alert.eta_or_leave_time_label}` : '',
    alert.reason_text ? `Reason: ${alert.reason_text}` : '',
    `Alert ID: ${alert.alert_id}`
  ].filter(Boolean).join('\n');

  MailApp.sendEmail({
    to,
    subject: subj,
    replyTo: cfg.FROM_EMAIL,
    name: 'Emergency Alerts',
    body
  });
  _eaAppendLog_(alert.alert_id, 'ACK email sent to ops');
}


// (unchanged, but kept here for completeness)


/** ===== Webhooks ===== **/
/** ===== Webhooks ===== **/




/** ===== Small internal utilities (safe) ===== **/


function _toIsoZ_(d) {
  return (d instanceof Date ? d : new Date(d)).toISOString();
}
function _nowIso() { return new Date().toISOString(); }
function _nowInTZ_(tz) {
  const now = new Date();
  // Utilities.formatDate returns string; use a trick to get tz components
  const s = Utilities.formatDate(now, tz, 'yyyy/MM/dd HH:mm:ss');
  const [Y,M,D,h,m,sec] = s.split(/[\/ :]/).map(Number);
  return new Date(Y, M-1, D, h, m, sec);
}
function _ok(payload){ return ContentService.createTextOutput(JSON.stringify(payload||{ok:true})).setMimeType(ContentService.MimeType.JSON); }
function _err(msg, code){ const out = ContentService.createTextOutput(JSON.stringify({ ok:false, error:String(msg||'ERROR') })).setMimeType(ContentService.MimeType.JSON); return out; }
function _safeParseBody_(e) {
  // Always return an object; prefer JSON if clearly JSON, otherwise fall back to form params.
  try {
    const hasContents = e && e.postData && typeof e.postData.contents === 'string';
    const ctype = (e && e.postData && e.postData.type) || '';

    if (hasContents) {
      const raw = e.postData.contents;
      const looksJson = /^\s*[\{\[]/.test(raw || '');
      if ((ctype && ctype.indexOf('application/json') === 0) || looksJson) {
        try {
          return JSON.parse(raw || '{}');
        } catch (jsonErr) {
          // fall through to params below
        }
      }
    }
  } catch (_) {
    // swallow and fall through to params
  }

  // x-www-form-urlencoded (ClickSend) or querystring: Apps Script exposes these in e.parameter
  try {
    const p = (e && e.parameter) ? e.parameter : {};
    return (p && typeof p === 'object') ? p : {};
  } catch (_) {
    return {};
  }
}

function _validateWebhook_(e){
  const want = (_P().EMERGENCY_WEBHOOK_SECRET || '').trim();
  if (!want) return true; // if not set, accept (dev mode)

  const p = (e && e.parameter) || {};
  const hdrs = (e && e.headers) || {};

  // Try param/header first (preferred in production)
  let got = (p.secret || hdrs['x-webhook-secret'] || hdrs['X-Webhook-Secret'] || '').trim();

  // Also accept a secret in the JSON body for providers that post it there
  if (!got && e && e.postData && e.postData.contents) {
    try {
      const body = JSON.parse(e.postData.contents || '{}');
      got = String(body.secret || body.EMERGENCY_WEBHOOK_SECRET || '').trim();
    } catch (_) { /* ignore parse errors */ }
  }

  return got && got === want;
}


/** ===== (Optional) helper to recover alert_id from trigger event ===== **/

function _getTriggerSourceAlertId_(e){
  // Prefer explicit payload if present (manual calls/tests)
  if (e && typeof e === 'object' && e.alert_id) return String(e.alert_id);

  // Time-based triggers provide a trigger UID for installable triggers.
  // We persist the trigger uniqueId into the alert record when creating it,
  // so we can reverse-lookup the owning alert here.
  try {
    const uid = e && e.triggerUid ? String(e.triggerUid) : '';
    if (!uid) return null;

    const { sh, m } = _eaIndexMap_();
    const lastRow = sh.getLastRow();
    if (lastRow < 2) return null;

    // Read all rows once to minimize calls
    const escCol = m.escalation_trigger_id + 1;
    const stepCol = m.stepper_trigger_id + 1;
    const idCol = m.alert_id + 1;
    const rng = sh.getRange(2, 1, lastRow - 1, sh.getLastColumn()).getValues();

    for (let i = 0; i < rng.length; i++) {
      const row = rng[i];
      const escId = String(row[escCol - 1] || '');
      const steId = String(row[stepCol - 1] || '');
      if (uid && (uid === escId || uid === steId)) {
        return String(row[idCol - 1] || '');
      }
    }
  } catch (_) { /* ignore */ }

  return null;
}


function flushQueuedLogsToSheet() {
  // Prevent overlap if the trigger fires while a previous run is still working
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30 * 1000)) {
    // Another flusher is running; just skip this tick
    return;
  }

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName('Logs');
    if (!sh) {
      sh = ss.insertSheet('Logs');
      sh.appendRow([
        'Timestamp',    // when the request started
        'RID',          // unique request ID
        'Action',       // e.g. AUTH_LOGIN
        'Status',       // HTTP status
        'TotalMs',      // total time spent
        'RespBytes',    // size of response JSON (approx)
        'HadK',         // whether ?k token was provided
        'EmailMask',
        'MsisdnMask',
        'SpansJSON'     // JSON of spans timings
      ]);
    }

    const cache = CacheService.getScriptCache();
    const registryKey = 'logq:registry';

    // Load the registry of queued cache keys
    let keys = [];
    const reg = cache.get(registryKey);
    if (reg) {
      try { keys = JSON.parse(reg) || []; } catch (e) { keys = []; }
    }
    if (!keys.length) return;

    // Process a limited batch each run to respect quotas
    const BATCH_SIZE = 200; // tune as needed
    const toProcess = keys.slice(0, BATCH_SIZE);
    const remaining = keys.slice(toProcess.length);

    const rows = [];
    const stillKeys = []; // keys we couldn't process this time (e.g., parse errors)

    for (const k of toProcess) {
      const val = cache.get(k);
      if (!val) {
        // key expired or already removed; skip
        continue;
      }
      try {
        const entry = JSON.parse(val);

        rows.push([
          entry.when ? new Date(entry.when) : new Date(),
          entry.rid || '',
          entry.action || '',
          entry.status || '',
          entry.totalMs || '',
          entry.respBytes || '',
          entry.hadK ? 'Y' : '',
          entry.emailMask || '',
          entry.msisdnMask || '',
          JSON.stringify(entry.spans || [])
        ]);
      } catch (e) {
        // keep the key around to retry next time
        stillKeys.push(k);
      }
    }

    if (rows.length) {
      // Append in one go. Get last row BEFORE inserting.
      const oldLast = sh.getLastRow(); // there is at least the header (>=1)
      sh.insertRowsAfter(oldLast || 1, rows.length);
      sh.getRange((oldLast || 1) + 1, 1, rows.length, rows[0].length).setValues(rows);
    }

    // Remove successfully processed keys from cache
    if (toProcess.length) {
      try { cache.removeAll(toProcess); } catch (e) { /* ignore */ }
    }

    // Rebuild registry with the keys we didn't process + any parse failures
    const newRegistry = remaining.concat(stillKeys);
    cache.put(registryKey, JSON.stringify(newRegistry), 6 * 60 * 60);
  } finally {
    lock.releaseLock();
  }
}




function _linksHeaderInfo(sh) {
  const lastCol = sh.getLastColumn();
  const hdr = sh.getRange(1, 1, 1, Math.max(1, lastCol)).getDisplayValues()[0];
  const norm = hdr.map(h => String(h || '').trim().toLowerCase());

  function _idxAny(names){
    const keys = [].concat(names).map(s => String(s).trim().toLowerCase());
    for (const key of keys) {
      const i = norm.indexOf(key);
      if (i >= 0) return i;
    }
    return -1;
  }

  // Standard names & common variants
  const m = {
    SUR: _idxAny(['surname']),
    FIR: _idxAny(['first','first name','firstname']),
    TEL: _idxAny(['telephone','phone','mobile']),
    EMA: _idxAny(['email','e-mail']),
    TOK: _idxAny(['token']),
    URL: _idxAny(['url','link']),
    STA: _idxAny(['status']),
    CRT: _idxAny(['createdat','created at','created']),
    REV: _idxAny(['revokedat','revoked at','revoked']),
    NEW: _idxAny(['newuser','new user']),
    ACT: _idxAny(['adminaction','admin action','action']),

    // NEW: Auth fields
    PWH: _idxAny(['passwordhash','password hash','pwdhash']),
    PWS: _idxAny(['passwordsalt','password salt','pwdsalt']),
    PWD: _idxAny(['passwordupdatedat','password updated at','pwdupdatedat','pwdupdated']),
    RST: _idxAny(['resettoken','reset token','pwdresettoken']),
    REX: _idxAny(['resetexpiresat','reset expires at','resetexpiry','reset expiration'])
  };

  return { hdr, norm, m, lastCol };
}
function _ensureLinksSchema(sh) {
  const info = _linksHeaderInfo(sh);
  let { hdr, m } = info;

  // Ensure RevokedAt exists (position after CreatedAt)
  if (m.REV < 0) {
    const insAt = (m.CRT >= 0 ? m.CRT + 2 : (hdr.length + 1)); // 1-based for insertAfter CreatedAt
    sh.insertColumnAfter(m.CRT + 1);
    sh.getRange(1, m.CRT + 2).setValue("RevokedAt");
    // Refresh map
    ({ hdr, m } = _linksHeaderInfo(sh));
  }

  // Ensure NewUser exists immediately BEFORE AdminAction
  if (m.NEW < 0) {
    if (m.ACT < 0) throw new Error("Links sheet is missing required header: AdminAction");
    sh.insertColumnBefore(m.ACT + 1);
    sh.getRange(1, m.ACT + 1).setValue("NewUser");
    ({ hdr, m } = _linksHeaderInfo(sh));

    // Initialise existing rows to NO
    const lastRow = Math.max(sh.getLastRow(), 2);
    if (lastRow > 1) {
      sh.getRange(2, m.NEW + 1, lastRow - 1, 1).setValues(
        Array.from({ length: lastRow - 1 }, () => ["NO"])
      );
    }
  }

  // NEW: Ensure password / reset columns exist (append to the right sensibly)
  function ensureColAfter(refIdx, title) {
    // Inserts AFTER the given 0-based index; if ref missing, append at end
    const after = (refIdx >= 0 ? refIdx + 1 : sh.getLastColumn());
    sh.insertColumnAfter(after);
    sh.getRange(1, after + 1).setValue(title);
  }

  if (m.PWH < 0) { ensureColAfter(Math.max(m.URL, m.ACT, m.REV, m.CRT), "PasswordHash"); ({ hdr, m } = _linksHeaderInfo(sh)); }
  if (m.PWS < 0) { ensureColAfter(m.PWH, "PasswordSalt"); ({ hdr, m } = _linksHeaderInfo(sh)); }
  if (m.PWD < 0) { ensureColAfter(m.PWS, "PasswordUpdatedAt"); ({ hdr, m } = _linksHeaderInfo(sh)); }
  if (m.RST < 0) { ensureColAfter(m.PWD, "ResetToken"); ({ hdr, m } = _linksHeaderInfo(sh)); }
  if (m.REX < 0) { ensureColAfter(m.RST, "ResetExpiresAt"); ({ hdr, m } = _linksHeaderInfo(sh)); }

  // Re-apply validations (including ALERT option)
  _applyLinksValidations(sh);
}
function updateTokens() {
  const p  = _P();
  // ---- UI is not available in time-driven triggers; guard it ----
  let ui = null;
  try { ui = SpreadsheetApp.getUi(); } catch (_) { ui = null; }
  const t0 = new Date();

  const rota  = _ssRota();
  const links = _ensureLinksSheet(); // ensures sheet exists

  // ✅ Retrofit schema on existing sheets (RevokedAt + NewUser; validations + NEW auth columns)
  _ensureLinksSchema(links);

  // --- Ensure "Candidate_ID" column exists immediately to the right of "ResetExpiresAt" ---
  (function ensureCandidateIdColumnRightOfREX() {
    const hdrRange = links.getRange(1, 1, 1, links.getLastColumn());
    const hdrVals  = hdrRange.getDisplayValues()[0].map(String);
    const idxREX   = hdrVals.findIndex(h => h.trim().toLowerCase() === 'resetexpiresat');
    const idxCID   = hdrVals.findIndex(h => h.trim().toLowerCase() === 'candidate_id');
    if (idxREX >= 0 && idxCID === -1) {
      const insertAt = idxREX + 2; // insert immediately after ResetExpiresAt (1-based)
      links.insertColumns(insertAt, 1);
      const newHdrRange = links.getRange(1, 1, 1, links.getLastColumn());
      const newHdrVals  = newHdrRange.getValues()[0];
      newHdrVals[idxREX + 1] = 'Candidate_ID';
      newHdrRange.setValues([newHdrVals]);
    }
  })();

  // Read candidate list
  const candSh = rota.getSheetByName(p.SH_CAND);
  if (!candSh) throw new Error('Candidate List sheet missing in rota');
  const cRows = candSh.getDataRange().getDisplayValues();

  // Build map tel -> profile
  const candidates = new Map();
  // Detect "Candidate_ID" column in Candidate List header (if present)
  const candHdr = cRows.length ? cRows[0].map(h => String(h || '').trim()) : [];
  const cIdxCID = candHdr.findIndex(h => h.trim().toLowerCase() === 'candidate_id');

  for (let i = 1; i < cRows.length; i++) {
    const surname = String(cRows[i][0] || '');
    const first   = String(cRows[i][1] || '');
    const email   = String(cRows[i][2] || '');
    const telRaw  = String(cRows[i][3] || '');
    const tel     = _normaliseLocalTel(telRaw);
    const candidateId  = cIdxCID >= 0 ? String(cRows[i][cIdxCID] || '') : '';
    if (tel) candidates.set(tel, { surname, first, email, tel, candidateId });
  }

  // Header info (after schema ensured and Candidate_ID inserted if needed)
  let { hdr, m, lastCol } = _linksHeaderInfo(links);
  // Map Candidate_ID index (non-invasive; does not affect other columns)
  if (typeof m.CID === 'undefined') {
    m.CID = hdr.findIndex(h => String(h).trim().toLowerCase() === 'candidate_id');
  }

  // Guard: ensure required columns exist (unchanged)
  const required = ['SUR','FIR','TEL','EMA','TOK','URL','STA','CRT','REV','NEW','ACT','PWH','PWS','PWD','RST','REX'];
  required.forEach(key => {
    if (m[key] < 0) throw new Error('Links sheet is missing required header: ' + key);
  });

  // Read all rows
  const raw = links.getDataRange().getDisplayValues();
  const rows = raw.length > 1 ? raw.slice(1) : [];

  // Normalise telephone + group by tel
  const byTel = new Map();
  let fixedTelFormat = 0;

  rows.forEach(r => {
    const telNorm = _normaliseLocalTel(r[m.TEL]);
    if (telNorm && r[m.TEL] !== telNorm) {
      r[m.TEL] = telNorm;
      fixedTelFormat++;
    }
    const key = telNorm || String(r[m.TEL] || '').replace(/\s/g, '');
    if (!byTel.has(key)) byTel.set(key, []);
    byTel.get(key).push({ r });
  });

  let created = 0, profileUpdated = 0, revoked = 0, reissued = 0, fixedDup = 0;

  // 📧 prepare outbound onboarding emails (send AFTER successful write)
  const props = PropertiesService.getScriptProperties();
  const testMode = String(props.getProperty('LOGINTESTMODE') || '').toLowerCase() === 'true';
  const webhook = props.getProperty('POWER_EMAIL') || '';
  const tmpl = (props.getProperty('WELCOME_TO_APP_HTML') || '').trim();
  const pendingEmails = []; // { email, url, tel, first }

  // 1) Ensure at most one Active per tel (keep newest by CreatedAt)
  byTel.forEach(list => {
    const active = list.filter(x => String(x.r[m.STA]) === 'Active');
    if (active.length > 1) {
      active.sort((a,b) => new Date(a.r[m.CRT] || 0) - new Date(b.r[m.CRT] || 0));
      active.pop(); // keep newest
      active.forEach(x => {
        if (String(x.r[m.STA]) !== 'Revoked') {
          x.r[m.STA] = 'Revoked';
          x.r[m.REV] = new Date().toISOString();
          revoked++;
        }
      });
      fixedDup += (list.length - 1);
    }
  });

  // 2) Sync profile fields from Candidate List (and populate Candidate_ID if available)
  byTel.forEach((list, tel) => {
    const cand = candidates.get(tel);
    if (!cand) return;
    list.forEach(x => {
      const r = x.r;
      let changed = false;
      if (r[m.SUR] !== cand.surname) { r[m.SUR] = cand.surname; changed = true; }
      if (r[m.FIR] !== cand.first)   { r[m.FIR] = cand.first;   changed = true; }
      if (r[m.EMA] !== cand.email)   { r[m.EMA] = cand.email;   changed = true; }
      if (r[m.TEL] !== cand.tel)     { r[m.TEL] = cand.tel;     changed = true; }
      if (m.CID >= 0) {
        const curCid = String(r[m.CID] || '');
        const nextCid = String(cand.candidateId || '');
        if (curCid !== nextCid) { r[m.CID] = nextCid; changed = true; }
      }
      if (changed) profileUpdated++;
    });
  });

  // 3) Apply AdminAction per row
  rows.forEach(r => {
    const tel = _normaliseLocalTel(r[m.TEL]);
    const act = String(r[m.ACT] || 'None');
    const sta = String(r[m.STA] || '');
    if (!tel || act === 'None') return;

    if (act === 'Revoke only') {
      if (sta === 'Active') {
        r[m.STA] = 'Revoked';
        r[m.REV] = new Date().toISOString();
        revoked++;
      }
      r[m.ACT] = 'None';
    }

    if (act === 'Revoke & Reissue') {
      if (sta === 'Active') r[m.REV] = new Date().toISOString();
      const tok = _randomToken();
      r[m.TOK] = tok;
      r[m.URL] = _buildUrlForToken(tok);
      r[m.STA] = 'Active';
      r[m.CRT] = new Date().toISOString();
      r[m.ACT] = 'None';

      // Treat reissue as a fresh onboarding link → reset token valid for 30 days
      r[m.RST] = tok;
      r[m.REX] = _nowPlusDaysIso(30);

      reissued++;

      // Queue onboarding email (only if not in test mode and we have essentials)
      const email = String(r[m.EMA] || '').trim();
      if (!testMode && webhook && email && r[m.URL]) {
        const first = String(r[m.FIR] || '').trim();
        const url   = String(r[m.URL] || '').trim();
        pendingEmails.push({ email, url, tel, first });
      }
    }
  });

  // 4) Create links for new candidates not present at all
  candidates.forEach((cand, tel) => {
    if (!byTel.has(tel)) {
      const newRow = new Array(lastCol).fill('');
      newRow[m.SUR] = cand.surname;
      newRow[m.FIR] = cand.first;
      newRow[m.TEL] = cand.tel;
      newRow[m.EMA] = cand.email;

      // Candidate_ID from Candidate List, if column exists
      if (m.CID >= 0) newRow[m.CID] = String(cand.candidateId || '');

      // First-time login URL/token (acts as password setup link)
      newRow[m.TOK] = _randomToken();
      newRow[m.URL] = _buildUrlForToken(newRow[m.TOK]);
      newRow[m.STA] = 'Active';
      newRow[m.CRT] = new Date().toISOString();
      newRow[m.NEW] = 'YES';
      newRow[m.ACT] = 'None';

      // NEW: initialize auth fields
      newRow[m.PWH] = '';
      newRow[m.PWS] = '';
      newRow[m.PWD] = '';

      // First-time reset token = same token; **valid for 30 days**
      newRow[m.RST] = newRow[m.TOK];
      newRow[m.REX] = _nowPlusDaysIso(30);

      rows.push(newRow);
      created++;

      // Queue onboarding email (only if not in test mode and we have essentials)
      const email = String(newRow[m.EMA] || '').trim();
      if (!testMode && webhook && email && newRow[m.URL]) {
        const first = String(newRow[m.FIR] || '').trim();
        const url   = String(newRow[m.URL] || '').trim();
        pendingEmails.push({ email, url, tel, first });
      }
    }
  });

  // 5) Final safety: enforce one Active per tel again after mutations
  const byTel2 = new Map();
  rows.forEach(r => {
    const tel = _normaliseLocalTel(r[m.TEL]);
    const key = tel || String(r[m.TEL] || '').replace(/\s/g, '');
    if (!byTel2.has(key)) byTel2.set(key, []);
    byTel2.get(key).push({ r });
  });
  byTel2.forEach(list => {
    const active = list.filter(x => String(x.r[m.STA]) === 'Active');
    if (active.length > 1) {
      active.sort((a,b) => new Date(a.r[m.CRT] || 0) - new Date(b.r[m.CRT] || 0));
      active.pop();
      active.forEach(x => {
        x.r[m.STA] = 'Revoked';
        if (!x.r[m.REV]) x.r[m.REV] = new Date().toISOString();
        revoked++;
      });
    }
  });

  // 6) Write back — preserve the exact header structure and column count (width-safe)
  const physicalWidth = links.getLastColumn();

  // Build a header row that matches the physical width exactly (no corruption)
  const hdrOut = new Array(physicalWidth).fill('');
  for (let i = 0; i < Math.min(physicalWidth, hdr.length); i++) hdrOut[i] = hdr[i];

  // Clear contents, then write header deterministically to row 1
  links.clear();
  links.getRange(1, 1, 1, physicalWidth).setValues([hdrOut]);

  const writeRows = Array.isArray(rows[0]) ? rows : rows.map(x => x.r);
  if (writeRows.length) {
    // Coerce every row to exactly the physical width (pad/trim) to avoid mismatch
    const padded = writeRows.map(row => {
      const src = Array.isArray(row) ? row : [];
      const out = new Array(physicalWidth).fill('');
      for (let i = 0; i < Math.min(physicalWidth, src.length); i++) out[i] = src[i];
      return out;
    });

    // Format Telephone column as plain text (dynamic index)
    if (m.TEL >= 0) {
      const telCol = m.TEL + 1;
      const maxRows = Math.max(padded.length + 10, Math.max(1000, links.getMaxRows()));
      links.getRange(1, telCol, maxRows, 1).setNumberFormat('@');
    }

    links.getRange(2, 1, padded.length, physicalWidth).setValues(padded);
  }

  // Re-apply validations
  _applyLinksValidations(links);

  // 7) Send queued onboarding emails (only if not in test mode)
  if (!testMode && pendingEmails.length && webhook) {
    pendingEmails.forEach(({ email, url, tel, first }) => {
      try {
        // Build HTML from WELCOME_TO_APP_HTML, ensuring clickable <a>
        const safeFirst = String(first || '').trim() || 'there';
        let html = tmpl || (
          'Dear {{firstName}},<br><br>' +
          'Please use the link below to set your password and access the app:<br>' +
          '<a href="{{webappUrlHref}}">{{webappUrl}}</a><br><br>' +
          'Kind regards,<br>Sussex Packages of Care Team.<br>Arthur Rai Medical Services.'
        );

        // Fill tokens safely
        const hrefEsc = _escapeHtmlAttribute(String(url || ''));
        const textEsc = _escapeHtmlForEmail(String(url || ''));
        html = String(html)
          .replace(/\{\{\s*firstName\s*\}\}/gi, _escapeHtmlForEmail(safeFirst))
          .replace(/\{\{\s*webappUrlHref\s*\}\}/gi, hrefEsc)
          .replace(/\{\{\s*webappUrl\s*\}\}/gi, textEsc);

        // Plain text fallback
        const plain =
          'Dear ' + (safeFirst) + ',\n\n' +
          'Please use the link below to set your password and access the app:\n' +
          String(url || '') + '\n\n' +
          'Kind regards,\nSussex Packages of Care Team.\nArthur Rai Medical Services.';

        const payload = {
          to: String(email || ''),
          subject: 'Set your password – Arthur Rai Medical Services',
          htmlBody: html,
          body: plain
        };
        const res = _postToPowerEmail(webhook, payload);
        _logTokenEvent('ONBOARDING_EMAIL_SENT', { email, tel, ok: !!(res && res.ok) });
      } catch (e) {
        _logTokenEvent('ONBOARDING_EMAIL_FAIL', { email, tel, error: String(e && e.message || e) });
      }
    });
  } else if (testMode && pendingEmails.length) {
    _logTokenEvent('ONBOARDING_EMAIL_SKIPPED_TESTMODE', { count: pendingEmails.length });
  }

  // ───── NEW: persist fast lookup index (tel → Candidate_ID) in Script Properties ─────
  try {
    const telToCid = {};
    const rowsFinal = Array.isArray(rows[0]) ? rows : rows.map(x => x.r);
    rowsFinal.forEach(r => {
      const tel = _normaliseLocalTel(r[m.TEL]);
      const cid = (m.CID >= 0) ? String(r[m.CID] || '').trim() : '';
      if (tel && cid) telToCid[tel] = cid;
    });
    props.setProperty('LINKS_TEL_TO_CANDIDATE_ID_JSON', JSON.stringify(telToCid));
    props.setProperty('LINKS_TEL_TO_CANDIDATE_ID_TS', _nowIso());
    props.setProperty('LINKS_TEL_TO_CANDIDATE_ID_COUNT', String(Object.keys(telToCid).length));
  } catch (_) {}

  const secs = ((new Date()) - t0) / 1000;
  const summary =
    `Done in ${secs.toFixed(1)}s\n` +
    `New: ${created}\nReissued: ${reissued}\nRevoked: ${revoked}\n` +
    `Profile updates: ${profileUpdated}\nDuplicates fixed: ${fixedDup}\n` +
    `Telephones normalised (fixed): ${fixedTelFormat}`;

  if (ui && ui.alert) {
    ui.alert('Availability API → Update tokens', summary, ui.ButtonSet.OK);
  } else {
    Logger.log('Availability API → Update tokens\n' + summary);
  }
}

function _acceptTokenPaste(userText) {
  try {
    // 1) Extract token (or explain failure)
    const token = _extractTokenFromUserString(userText);
    if (!token) {
      const msg = _explainTokenLookupFailure('NO_TOKEN_IN_TEXT');
      _logTokenEvent('TOKEN_CLAIM_FAIL', {
        error: 'TOKEN_NOT_FOUND_IN_TEXT',
        providedSnippet: String(userText || '').slice(0, 120),
        note: msg
      });
      return { ok: false, error: "TOKEN_NOT_FOUND_IN_TEXT" };
    }

    // 2) Look it up — now supports two modes:
    //    - RESET tokens (time-limited): match Links.ResetToken + not expired
    //    - LEGACY tokens (Active): match Links.Token + Status=Active
    const resetRec = _findLinksRowByResetToken(token); // NEW helper
    if (resetRec) {
      // Check expiry (covers 30‑minute forgot links and 30‑day onboarding links)
      if (resetRec.m.REX >= 0) {
        const expiresIso = String(resetRec.row[resetRec.m.REX] || "");
        if (_isFutureIso(expiresIso)) { // NEW helper
          _logTokenEvent('TOKEN_CLAIM_OK', {
            providedKMasked: _maskTokenForLog(token),
            resolvedMsisdn: _normaliseLocalTel(String(resetRec.row[resetRec.m.TEL] || "")),
            note: 'Reset token accepted (mode=RESET)'
          });
          return { ok: true, mode: 'RESET' };
        }
      }
      // Expired
      _logTokenEvent('TOKEN_CLAIM_FAIL', {
        error: 'TOKEN_NOT_ACTIVE_OR_UNKNOWN',
        providedKMasked: _maskTokenForLog(token),
        note: 'Reset token expired'
      });
      return { ok: false, error: "TOKEN_NOT_ACTIVE_OR_UNKNOWN" };
    }

    // Legacy (backwards-compat: Active k→msisdn)
    const tel = _lookupTelephoneByActiveToken(token);
    if (!tel) {
      const msg = _explainTokenLookupFailure('NOT_ACTIVE_OR_UNKNOWN');
      _logTokenEvent('TOKEN_CLAIM_FAIL', {
        error: 'TOKEN_NOT_ACTIVE_OR_UNKNOWN',
        providedKMasked: _maskTokenForLog(token),
        note: msg
      });
      return { ok: false, error: "TOKEN_NOT_ACTIVE_OR_UNKNOWN" };
    }

    _logTokenEvent('TOKEN_CLAIM_OK', {
      providedKMasked: _maskTokenForLog(token),
      resolvedMsisdn: tel,
      note: 'Legacy token accepted and mapped to msisdn'
    });
    return { ok: true, token, msisdn: tel, mode: 'LEGACY' };
  } catch (e) {
    const err = String(e && e.message || e);
    _logTokenEvent('TOKEN_CLAIM_EXCEPTION', {
      error: err,
      note: 'Unexpected error during token claim.'
    });
    return { ok: false, error: err };
  }
}

function _lookupTelephoneByActiveToken(token) {
  try {
    const ss = _ssLinks();
    const sh = ss.getSheetByName(_P().SH_LINKS);
    if (!sh) {
      _logTokenEvent('TOKEN_LOOKUP_ABORT', {
        error: 'LINKS_SHEET_MISSING',
        note: _explainTokenLookupFailure('LINKS_SHEET_MISSING')
      });
      return null;
    }

    const { m } = _linksHeaderInfo(sh);
    if (m.TOK < 0 || m.STA < 0 || m.TEL < 0) {
      _logTokenEvent('TOKEN_LOOKUP_ABORT', {
        error: 'MISSING_REQUIRED_HEADERS',
        headersPresent: Object.keys(m).filter(k => m[k] >= 0),
        note: 'Links sheet missing Token/Status/Telephone columns.'
      });
      return null;
    }

    const vals = sh.getDataRange().getDisplayValues();
    if (vals.length < 2) return null;

    for (let i = 1; i < vals.length; i++) {
      const row = vals[i];
      const theToken = String(row[m.TOK] || '').trim();
      const status   = String(row[m.STA] || '').trim();
      const telNorm  = _normaliseLocalTel(String(row[m.TEL] || ''));
      if (theToken === token && status === 'Active' && telNorm) {
        return telNorm;
      }
    }

    // Not found or not active
    _logTokenEvent('TOKEN_LOOKUP_NOT_FOUND', {
      providedKMasked: _maskTokenForLog(token),
      note: _explainTokenLookupFailure('NOT_ACTIVE_OR_UNKNOWN')
    });
    return null;
  } catch (e) {
    _logTokenEvent('TOKEN_LOOKUP_EXCEPTION', {
      providedKMasked: _maskTokenForLog(token),
      error: String(e && e.message || e),
      note: 'Unexpected error while scanning Links sheet.'
    });
    return null;
  }
}

function _resolveMsisdnFromRequest(eOrBody) {
  const param = eOrBody.parameter || {};
  const body  = eOrBody.postData ? JSON.parse(eOrBody.postData.contents || "{}") : eOrBody;

  // 1) direct msisdn if present
  const raw = (param.msisdn || body.msisdn || "").trim();
  let msisdn = _normaliseMsisdn(raw);
  if (msisdn) return msisdn;

  // 2) token k -> look up in Links sheet (must be Active)
  const k = (param.k || body.k || "").trim();
  if (k) {
    const tel = _lookupTelephoneByActiveToken(k);
    if (tel) return tel;
  }
  return null;
}

function _applyLinksValidations(sh) {
  const { m } = _linksHeaderInfo(sh);
  const lastRow = Math.max(sh.getLastRow(), 2);

  // Status
  if (m.STA >= 0) {
    const col1 = m.STA + 1;
    const range = sh.getRange(2, col1, Math.max(1, lastRow - 1), 1);
    const rule = SpreadsheetApp.newDataValidation()
      .requireValueInList(['Active', 'Revoked'], true)
      .setAllowInvalid(false)
      .build();
    range.setDataValidation(rule);
  }

  // NewUser (now supports ALERT)
  if (m.NEW >= 0) {
    const col1 = m.NEW + 1;
    const range = sh.getRange(2, col1, Math.max(1, lastRow - 1), 1);
    const rule = SpreadsheetApp.newDataValidation()
      .requireValueInList(['YES','ALERT','NO'], true)
      .setAllowInvalid(false)
      .build();
    range.setDataValidation(rule);
  }

  // AdminAction
  if (m.ACT >= 0) {
    const col1 = m.ACT + 1;
    const range = sh.getRange(2, col1, Math.max(1, lastRow - 1), 1);
    const rule = SpreadsheetApp.newDataValidation()
      .requireValueInList(['None','Revoke only','Revoke & Reissue'], true)
      .setAllowInvalid(false)
      .build();
    range.setDataValidation(rule);
  }
}

function _buildUrlForToken(token) {
  const base = _P().PWA_BASE_URL;
  const sep = base.includes("?") ? "&" : "?";
  return `${base}${sep}k=${encodeURIComponent(token)}`;
}


function _postToPowerEmail(webhookUrl, payload) {
  const res = UrlFetchApp.fetch(String(webhookUrl), {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  const code = res.getResponseCode();
  const text = res.getContentText() || "";
  if (code < 200 || code >= 300) {
    _logTokenEvent('POWER_EMAIL_NON_2XX', {
      status: code,
      bodyFragment: text.slice(0, 500),
      note: 'Power Automate returned non-2xx.'
    });
    return { ok: false, status: code, error: text.substring(0, 1000) };
  }
  try {
    const parsed = JSON.parse(text);
    return Object.assign({ ok: true, status: code }, parsed);
  } catch (_) {
    _logTokenEvent('POWER_EMAIL_PARSE_FALLBACK', {
      status: code,
      bodyFragment: text.slice(0, 500),
      note: 'Response was not JSON; returning raw body.'
    });
    return { ok: true, status: code, body: text };
  }
}
/***** =========================
 *  LINKS lookups & selectors
 *  ========================= */

/**
 * Find a Links row by email (case-insensitive).
 * Returns { sh, rowIndex, row, m } or null if not found.
 */
function _linksEmailRowCacheKey_(normalisedEmail) {
  const bytes = Utilities.newBlob(String(normalisedEmail || '')).getBytes();
  const digest = _passwordSha256Bytes_(bytes);
  return 'links-email-row-v1:' + _toHex(digest).slice(0, 40);
}

function _findLinksRowByEmail(email) {
  try {
    const needle = String(email || '').trim().toLowerCase();
    if (!needle) return null;

    const ss = _ssLinks();
    const sh = ss.getSheetByName(_P().SH_LINKS);
    if (!sh) return null;

    const info = _linksHeaderInfo(sh);
    const m = info.m;
    const lastRow = sh.getLastRow();

    if (m.EMA < 0 || info.lastCol < 1 || lastRow < 2) return null;

    const emails = sh
      .getRange(2, m.EMA + 1, lastRow - 1, 1)
      .getDisplayValues();

    for (let i = 0; i < emails.length; i++) {
      const rowEmail = String(emails[i][0] || '').trim().toLowerCase();
      if (rowEmail !== needle) continue;

      const rowIndex = i + 2;
      const row = sh
        .getRange(rowIndex, 1, 1, info.lastCol)
        .getDisplayValues()[0];

      return { sh, rowIndex, row, m };
    }

    return null;
  } catch (_) {
    return null;
  }
}


/**
 * Find a Links row by reset token (exact match).
 * Returns { sh, rowIndex, row, m } or null if not found.
 */
function _findLinksRowByResetToken(token) {
  try {
    const ss = _ssLinks();
    const sh = ss.getSheetByName(_P().SH_LINKS);
    if (!sh) return null;

    const info = _linksHeaderInfo(sh);
    const m = info.m;
    if (m.RST < 0) return null;

    const vals = sh.getDataRange().getDisplayValues();
    if (vals.length < 2) return null;

    const needle = String(token || '').trim();
    for (let i = 1; i < vals.length; i++) {
      const row = vals[i];
      const rst = String(row[m.RST] || '').trim();
      if (rst && rst === needle) {
        return { sh, rowIndex: i + 1, row, m };
      }
    }
    return null;
  } catch (_) {
    return null;
  }
}
/***** =========================
 *  Password hashing & verify
 *  ========================= */

/**
 * Return a URL-safe base64 string from bytes.
 */
function _b64url(bytes) {
  const b64 = Utilities.base64Encode(bytes);
  return b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

/**
 * Convert byte[] to lowercase hex string.
 */
function _toHex(bytes) {
  return bytes.map(b => ('0' + (b & 0xFF).toString(16)).slice(-2)).join('');
}

/**
 * Generate a cryptographically strong random salt (32 bytes), returned as hex.
 */
// 32-byte (256‑bit) salt as lowercase hex (64 chars)
function _generateSaltHex() {
  // UUID gives ~122 bits of randomness; we expand to 256 bits via SHA‑256
  const material = Utilities.getUuid().replace(/-/g, '') + '|' + Date.now();
  const digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    material
  ); // byte[]
  return _toHex(Array.from(digest)); // uses your existing _toHex
}
/**
 * Basic PBKDF2-like derivation using HMAC-SHA256 iterations.
 * NOTE: Apps Script has no PBKDF2 built-in; this is a pragmatic approach.
 */
const _PASSWORD_SHA256_K_ = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
  0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
  0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
  0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
  0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
  0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
];

function _passwordNormaliseBytes_(bytes) {
  const out = new Array(bytes ? bytes.length : 0);
  for (let i = 0; i < out.length; i++) out[i] = Number(bytes[i]) & 0xFF;
  return out;
}

function _passwordRotateRight_(value, count) {
  return (value >>> count) | (value << (32 - count));
}

/** Pure in-process SHA-256 over byte values; returns bytes in the range 0..255. */
function _passwordSha256Bytes_(inputBytes) {
  const bytes = _passwordNormaliseBytes_(inputBytes);
  const originalLength = bytes.length;
  const bitLengthLow = (originalLength * 8) >>> 0;
  const bitLengthHigh = Math.floor((originalLength * 8) / 0x100000000) >>> 0;

  bytes.push(0x80);
  while ((bytes.length % 64) !== 56) bytes.push(0);

  bytes.push((bitLengthHigh >>> 24) & 0xFF);
  bytes.push((bitLengthHigh >>> 16) & 0xFF);
  bytes.push((bitLengthHigh >>> 8) & 0xFF);
  bytes.push(bitLengthHigh & 0xFF);
  bytes.push((bitLengthLow >>> 24) & 0xFF);
  bytes.push((bitLengthLow >>> 16) & 0xFF);
  bytes.push((bitLengthLow >>> 8) & 0xFF);
  bytes.push(bitLengthLow & 0xFF);

  const state = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
  ];
  const words = new Array(64);

  for (let offset = 0; offset < bytes.length; offset += 64) {
    let i;
    for (i = 0; i < 16; i++) {
      const p = offset + (i * 4);
      words[i] = (
        (bytes[p] << 24) |
        (bytes[p + 1] << 16) |
        (bytes[p + 2] << 8) |
        bytes[p + 3]
      ) >>> 0;
    }

    for (i = 16; i < 64; i++) {
      const x = words[i - 15];
      const y = words[i - 2];
      const sigma0 = (_passwordRotateRight_(x, 7) ^ _passwordRotateRight_(x, 18) ^ (x >>> 3)) >>> 0;
      const sigma1 = (_passwordRotateRight_(y, 17) ^ _passwordRotateRight_(y, 19) ^ (y >>> 10)) >>> 0;
      words[i] = (words[i - 16] + sigma0 + words[i - 7] + sigma1) >>> 0;
    }

    let a = state[0];
    let b = state[1];
    let c = state[2];
    let d = state[3];
    let e = state[4];
    let f = state[5];
    let g = state[6];
    let h = state[7];

    for (i = 0; i < 64; i++) {
      const upperSigma1 = (_passwordRotateRight_(e, 6) ^ _passwordRotateRight_(e, 11) ^ _passwordRotateRight_(e, 25)) >>> 0;
      const choose = ((e & f) ^ ((~e) & g)) >>> 0;
      const temp1 = (h + upperSigma1 + choose + _PASSWORD_SHA256_K_[i] + words[i]) >>> 0;
      const upperSigma0 = (_passwordRotateRight_(a, 2) ^ _passwordRotateRight_(a, 13) ^ _passwordRotateRight_(a, 22)) >>> 0;
      const majority = ((a & b) ^ (a & c) ^ (b & c)) >>> 0;
      const temp2 = (upperSigma0 + majority) >>> 0;

      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }

    state[0] = (state[0] + a) >>> 0;
    state[1] = (state[1] + b) >>> 0;
    state[2] = (state[2] + c) >>> 0;
    state[3] = (state[3] + d) >>> 0;
    state[4] = (state[4] + e) >>> 0;
    state[5] = (state[5] + f) >>> 0;
    state[6] = (state[6] + g) >>> 0;
    state[7] = (state[7] + h) >>> 0;
  }

  const digest = [];
  for (let i = 0; i < state.length; i++) {
    digest.push((state[i] >>> 24) & 0xFF);
    digest.push((state[i] >>> 16) & 0xFF);
    digest.push((state[i] >>> 8) & 0xFF);
    digest.push(state[i] & 0xFF);
  }
  return digest;
}

/** Prepare the constant HMAC pads once for all 1,000 rounds. */
function _passwordPrepareHmacSha256_(keyBytes) {
  const blockSize = 64;
  let key = _passwordNormaliseBytes_(keyBytes);
  if (key.length > blockSize) key = _passwordSha256Bytes_(key);
  while (key.length < blockSize) key.push(0);

  const innerPad = new Array(blockSize);
  const outerPad = new Array(blockSize);
  for (let i = 0; i < blockSize; i++) {
    innerPad[i] = key[i] ^ 0x36;
    outerPad[i] = key[i] ^ 0x5C;
  }
  return { innerPad, outerPad };
}

function _passwordHmacSha256Prepared_(messageBytes, prepared) {
  const message = _passwordNormaliseBytes_(messageBytes);
  const innerDigest = _passwordSha256Bytes_(prepared.innerPad.concat(message));
  return _passwordSha256Bytes_(prepared.outerPad.concat(innerDigest));
}

/** Byte-compatible replacement used by existing login and password-reset flows. */
function _deriveKeyHmacSha256(passwordUtf8, saltHex, iterations) {
  const saltBytes = saltHex.match(/.{1,2}/g).map(h => parseInt(h, 16));
  const prepared = _passwordPrepareHmacSha256_(saltBytes);
  // Start with first digest of (salt || password)
  let msg = saltBytes.concat(Array.from(passwordUtf8));
  let u = _passwordHmacSha256Prepared_(msg, prepared);
  let out = new Uint8Array(u);

  for (let i = 1; i < iterations; i++) {
    u = _passwordHmacSha256Prepared_(u, prepared);
    for (let j = 0; j < out.length; j++) {
      out[j] = out[j] ^ u[j];
    }
  }
  return _toHex(Array.from(out));
}

/** Original calculation retained only for the manual compatibility test. */
function _deriveKeyHmacSha256Legacy_(passwordUtf8, saltHex, iterations) {
  const saltBytes = saltHex.match(/.{1,2}/g).map(h => parseInt(h, 16));
  let msg = saltBytes.concat(Array.from(passwordUtf8));
  let u = Utilities.computeHmacSha256Signature(msg, saltBytes);
  let out = new Uint8Array(u);

  for (let i = 1; i < iterations; i++) {
    u = Utilities.computeHmacSha256Signature(u, saltBytes);
    for (let j = 0; j < out.length; j++) {
      out[j] = out[j] ^ u[j];
    }
  }
  return _toHex(Array.from(out));
}

/**
 * Hash a password: returns { salt, hash, algo, iter, v }
 *  - algo: "HMAC-SHA256"
 *  - iter: iteration count
 *  - v:    a version marker for future upgrades
 */
function _hashPassword(plain) {
  const password = String(plain || '');
  const salt = _generateSaltHex();
  const iter = 1000; // balance strength vs. GAS quotas
  const bytes = Utilities.newBlob(password).getBytes();
  const dkHex = _deriveKeyHmacSha256(bytes, salt, iter);
  return {
    salt: salt,
    hash: dkHex,
    algo: "HMAC-SHA256",
    iter: iter,
    v: 1
  };
}

/**
 * Verify a password against stored salt/hash.
 */
function _verifyPassword(plain, salt, hash) {
  try {
    if (!plain || !salt || !hash) return false;
    const iter = 1000; // keep in sync with _hashPassword
    const bytes = Utilities.newBlob(String(plain)).getBytes();
    const dkHex = _deriveKeyHmacSha256(bytes, String(salt), iter);
    return dkHex === String(hash);
  } catch (_) {
    return false;
  }
}

/**
 * Manual, no-write regression test for the Apps Script editor.
 * The 1,000-round legacy case is intentionally slow and runs only on request.
 */
function testPasswordKdfCompatibility() {
  const cases = [
    {
      label: 'one-round-ascii',
      password: 'Compatibility-Test-1!',
      salt: '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f',
      iterations: 1,
      expected: '64e18062b675f6a86206def8dd228e4fceb3c553dcf6a8018b6d3657319dffb0'
    },
    {
      label: 'two-round-unicode',
      password: 'T\u00ebst-\u5bc6\u7801-\ud83d\udd10',
      salt: 'ffeeddccbbaa9988776655443322110001020304050607080910111213141516',
      iterations: 2,
      expected: '56b192455357de8eb507c689134b1efc530680fe6fb190a2139ead417e1d4114'
    },
    {
      label: 'ten-round-ascii',
      password: 'Another-Dummy-Password-2!',
      salt: '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
      iterations: 10,
      expected: '686c2ee70cb15cd8fe6de503a1a6d810b596d4a8c8676b888c6de8c78765bdf2'
    },
    {
      label: 'full-legacy-1000-rounds',
      password: 'Dummy-Existing-Password-3!',
      salt: '89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567',
      iterations: 1000,
      expected: 'a5318ecc1dd2ecce3fcfaea4739eb376863fa9f853dabfe2f0a8258b3fdda5f7'
    }
  ];

  const report = [];
  for (let i = 0; i < cases.length; i++) {
    const item = cases[i];
    const bytes = Utilities.newBlob(item.password).getBytes();

    const legacyStarted = Date.now();
    const legacyHex = _deriveKeyHmacSha256Legacy_(bytes, item.salt, item.iterations);
    const legacyMs = Date.now() - legacyStarted;

    const fastStarted = Date.now();
    const fastHex = _deriveKeyHmacSha256(bytes, item.salt, item.iterations);
    const fastMs = Date.now() - fastStarted;

    if (legacyHex !== item.expected) {
      throw new Error('PASSWORD_KDF_LEGACY_VECTOR_FAILED:' + item.label);
    }
    if (fastHex !== item.expected) {
      throw new Error('PASSWORD_KDF_FAST_VECTOR_FAILED:' + item.label);
    }
    if (legacyHex !== fastHex) {
      throw new Error('PASSWORD_KDF_COMPATIBILITY_FAILED:' + item.label);
    }

    report.push({
      label: item.label,
      iterations: item.iterations,
      compatible: true,
      legacyMs,
      fastMs
    });
  }

  const result = { ok: true, cases: report };
  console.log(JSON.stringify(result));
  return result;
}

/** Fast-only fixed-vector timing check; no sheets or user records are touched. */
function benchmarkPasswordKdfFast() {
  const salt = '89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567';
  const bytes = Utilities.newBlob('Dummy-Benchmark-Password-4!').getBytes();
  const expected = '2f0709a4d5b90ec9c330f70f4f7760dfd207971ef3c1dde71309883d54827244';
  const started = Date.now();
  const hash = _deriveKeyHmacSha256(bytes, salt, 1000);
  const elapsedMs = Date.now() - started;
  const result = { ok: hash === expected, iterations: 1000, elapsedMs };
  console.log(JSON.stringify(result));
  if (!result.ok) throw new Error('PASSWORD_KDF_BENCHMARK_VECTOR_FAILED');
  return result;
}

/** No-write check for the new/reset-password hash and verification path. */
function testPasswordHashRoundTrip() {
  const dummyPassword = 'Dummy-Round-Trip-Password-5!';
  const created = _hashPassword(dummyPassword);
  const correctAccepted = _verifyPassword(dummyPassword, created.salt, created.hash);
  const incorrectRejected = !_verifyPassword(dummyPassword + '-wrong', created.salt, created.hash);
  const result = {
    ok: correctAccepted && incorrectRejected,
    correctAccepted,
    incorrectRejected
  };
  console.log(JSON.stringify(result));
  if (!result.ok) throw new Error('PASSWORD_HASH_ROUND_TRIP_FAILED');
  return result;
}
/***** =========================
 *  Reset tokens & time helpers
 *  ========================= */

/**
 * A plausible token format check used by reset endpoints, URLs, etc.
 */
function _isPlausibleToken(k) {
  return /^[A-Za-z0-9_-]{20,}$/.test(String(k || ''));
}

/**
 * Generate a URL-safe reset token (256-bit).
 */
// URL‑safe 256‑bit reset token (Base64URL, no padding)
function _generateResetToken() {
  // Mix UUID + time + a little non‑crypto jitter to diversify inputs
  const material =
    Utilities.getUuid() + '|' + Date.now() + '|' + Math.random();
  const digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    material
  ); // byte[]
  return _b64url(Array.from(digest)); // uses your existing _b64url
}
/**
 * ISO string now + N minutes.
 */
function _nowPlusMinutesIso(mins) {
  const d = new Date(Date.now() + (Number(mins) || 0) * 60 * 1000);
  return d.toISOString();
}

/**
 * ISO string now + N days.
 */
function _nowPlusDaysIso(days) {
  const d = new Date(Date.now() + (Number(days) || 0) * 24 * 60 * 60 * 1000);
  return d.toISOString();
}

/**
 * ISO string for now (UTC).
 */
function _nowIso() {
  return new Date().toISOString();
}

/**
 * Check if an ISO timestamp is in the future.
 */
function _isFutureIso(iso) {
  if (!iso) return false;
  const t = Date.parse(iso);
  if (!isFinite(t)) return false;
  return t > Date.now();
}

/**
 * Minimal password strength policy.
 * Adjust as needed (length, classes, etc.).
 */
function _isStrongPassword(pw) {
  const s = String(pw || '');
  if (s.length < 8) return false;
  if (!/[a-z]/.test(s)) return false;
  if (!/[A-Z]/.test(s)) return false;
  if (!/[0-9]/.test(s)) return false;
  // Optionally enforce a symbol:
  // if (!/[^\w]/.test(s)) return false;
  return true;
}

/***** =========================
 *  Password hashing & verify
 *  ========================= */

/**
 * Return a URL-safe base64 string from bytes.
 */

/***** =========================
 *  Reset tokens & time helpers
 *  ========================= */

/**
 * A plausible token format check used by reset endpoints, URLs, etc.
 */

/***** =========================
 *  Email dispatch (Power Automate)
 *  ========================= */

/**
 * Send the password reset email via Power Automate.
 * Expects a Script Property: POWER_AUTOMATE_RESET_URL
 * Payload fields can be adapted to your flow’s contract.
 */
/***** =========================
 *  Email dispatch (Power Automate)
 *  ========================= */

/**
 * Send the password reset email via Power Automate.
 * Requires Script Property: POWER_AUTOMATE_RESET_URL
 * Payload fields can be adapted to your Flow’s contract.
 */
/**
 * Send the password reset email via Power Automate.
 * Requires Script Property: POWER_AUTOMATE_RESET_URL
 * Payload fields can be adapted to your Flow’s contract.
 */
function _sendPasswordResetEmail(email, url) {
  try {
    // ---- Validate inputs ----
    email = String(email || '').trim().toLowerCase();
    url   = String(url || '').trim();
    if (!email) return { ok: false, error: 'MISSING_EMAIL' };
    if (!url)   return { ok: false, error: 'MISSING_RESET_LINK' };

    // ---- Resolve first name via canonical helper used elsewhere ----
    var first = '';
    try {
      var rec = null;
      if (typeof _findLinksRowByEmail === 'function') {
        rec = _findLinksRowByEmail(email);
      } else if (typeof _getLinksRowByEmail === 'function') {
        // Fallback only if the canonical finder is unavailable
        rec = _getLinksRowByEmail(email);
      }
      if (rec && rec.row && rec.m && rec.m.FIR != null && rec.m.FIR >= 0) {
        first = String(rec.row[rec.m.FIR] || '').trim();
      } else {
        // Diagnostics to help spot data issues (masked email)
        var masked = (function (s) {
          try {
            var m = String(s || '').trim().toLowerCase();
            var parts = m.split('@');
            if (parts.length !== 2) return '•@•';
            var u = parts[0], d = parts[1];
            var head = u ? u[0] : '';
            return head + '•••@' + d;
          } catch (_) { return '•@•'; }
        })(email);
        _logTokenEvent('PASSWORD_RESET_EMAIL_NO_ROW_OR_NO_FIR', { emailMask: masked });
      }
      if (!first) {
        var masked2 = (function (s) {
          try {
            var m = String(s || '').trim().toLowerCase();
            var parts = m.split('@');
            if (parts.length !== 2) return '•@•';
            var u = parts[0], d = parts[1];
            var head = u ? u[0] : '';
            return head + '•••@' + d;
          } catch (_) { return '•@•'; }
        })(email);
        _logTokenEvent('PASSWORD_RESET_EMAIL_MISSING_FIRSTNAME', { emailMask: masked2 });
      }
    } catch (ign) {
      // Keep going with empty first (we’ll use a neutral salutation)
    }

    // ---- Script Properties: webhook + (optional) subject/html overrides ----
    var props = PropertiesService.getScriptProperties();
    var hook  = props.getProperty('POWER_EMAIL');
    if (!hook) return { ok: false, error: 'POWER_EMAIL_NOT_CONFIGURED' };

    var subject = (props.getProperty('RESET_EMAIL_SUBJECT') || 'Reset your password').trim();

    // If RESET_EMAIL_HTML is provided, use it; otherwise use a safe, anchor-including default
    var html = props.getProperty('RESET_EMAIL_HTML') || '';
    if (!html) {
      // Default HTML contains a real anchor so it is always clickable
      html =
        'Dear {{firstName}},<br><br>' +
        'You requested to reset your password. Please click the link below to set a new password:<br>' +
        '{{resetLinkAnchor}}<br><br>' +
        'If you did not request this, you can ignore this message.<br><br>' +
        'Many thanks,<br>Sussex Packages of Care Team.<br>Arthur Rai Medical Services.';
    }

    // ---- Safely build anchor + fill tokens ----
    var escapedUrlAttr = _escapeHtmlAttribute(url); // for href=""
    var escapedUrlText = _escapeHtmlForEmail(url);  // for link text
    var anchor = '<a href="' + escapedUrlAttr + '">' + escapedUrlText + '</a>';

    // Choose a neutral salutation if first name is empty to avoid "Dear ,"
    var safeFirst = first ? first : 'there';

    // Fill {{firstName}} first
    html = String(html)
      .replace(/\{\{\s*firstName\s*\}\}/gi, _escapeHtmlForEmail(safeFirst));

    // If template provides {{resetLinkAnchor}}, prefer it.
    var hadAnchorToken = /\{\{\s*resetLinkAnchor\s*\}\}/i.test(html);
    if (hadAnchorToken) {
      html = html.replace(/\{\{\s*resetLinkAnchor\s*\}\}/gi, anchor);
    }

    // Always also replace {{resetLink}}.
    // If there is NO <a ...> present anywhere and NO {{resetLinkAnchor}} token was used,
    // upgrade {{resetLink}} to an anchor so it's clickable.
    var htmlHasAnyAnchor = /<a\b[^>]*>/i.test(html);
    if (!hadAnchorToken && !htmlHasAnyAnchor) {
      // Promote raw token to anchor
      html = html.replace(/\{\{\s*resetLink\s*\}\}/gi, anchor);
    } else {
      // Otherwise insert raw URL text (useful if template already contains its own <a> markup)
      html = html.replace(/\{\{\s*resetLink\s*\}\}/gi, escapedUrlText);
    }

    // ---- Plain text fallback (no HTML tags) ----
    var plain =
      'Dear ' + (safeFirst || '') + ',\n\n' +
      'You requested to reset your password. Please use the link below to set a new password:\n' +
      url + '\n\n' +
      'If you did not request this, you can ignore this message.\n\n' +
      'Many thanks,\nSussex Packages of Care Team.\nArthur Rai Medical Services.';

    // ---- Payload mirrors your timesheet sender (keep key names identical) ----
    var payload = {
      to: email,
      subject: subject,
      htmlBody: html,  // HTML version (with clickable anchor)
      body: plain      // text-only fallback
      // attachments: none for reset emails
    };

    // ---- Send via shared helper ----
    var res = _postToPowerEmail(hook, payload);

    // ---- Log for observability ----
    _logTokenEvent('PASSWORD_RESET_EMAIL_RESULT', {
      to: email,
      status: res && res.status,
      ok: !!(res && res.ok),
      error: res && res.error
    });

    return res;
  } catch (e) {
    var err = String((e && e.message) || e);
    _logTokenEvent('PASSWORD_RESET_EMAIL_EXCEPTION', { to: email || '', error: err });
    return { ok: false, error: err };
  }
}


/**
 * Escape for HTML element text nodes.
 */
function _escapeHtmlForEmail(s) {
  s = String(s || '');
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/**
 * Escape for HTML attribute values (e.g., href="...").
 */
function _escapeHtmlAttribute(s) {
  s = String(s || '');
  return s
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;');
}

function __enqueueLog(entry) {
  const cache = CacheService.getScriptCache();
  const key = 'logq:' + new Date().toISOString() + ':' + Utilities.getUuid();
  cache.put(key, JSON.stringify(entry), 6 * 60 * 60);

  let reg = cache.get('logq:registry');
  let arr = [];
  try { arr = JSON.parse(reg) || []; } catch (e) { arr = []; }
  arr.push(key);
  cache.put('logq:registry', JSON.stringify(arr), 6 * 60 * 60);
}

function saveMessageForKind(kind, html) {
  const key = _resolveKindToKey_(kind);
  if (!key) throw new Error('Unknown message kind');

  const inputLen = String(html || '').length;
  const safe = _sanitizeBoldUnderlineHtml_(String(html || '')); // now preserves b/u/p/div/br/a/ul/ol/li
  PropertiesService.getScriptProperties().setProperty(key, safe);

  // Optional telemetry — keep if you still want it
  try {
    _logTokenEvent && _logTokenEvent('MESSAGE_SAVE', {
      kind: String(kind || '').toUpperCase(),
      inputLength: inputLen,
      savedLength: safe.length,
      note: 'Message saved after rich sanitizer.'
    });
  } catch (_){}

  return { ok: true, key, length: safe.length };
}

function getMessageForKind(kind) {
  const key = _resolveKindToKey_(kind);
  if (!key) throw new Error('Unknown message kind');
  // Return stored HTML verbatim; the editor assigns it via innerHTML.
  const raw = PropertiesService.getScriptProperties().getProperty(key) || '';
  return String(raw || '');
}

/**
 * Rich but safe sanitizer that preserves:
 *  - <b>, <u>, <br>, <p>, <div>, <ul>, <ol>, <li>, and <a href target rel>
 *  - Converts style-based bold/underline (<span style="..."> etc.) into <b>/<u>
 *  - Normalizes <strong> → <b>; balances unclosed <b>/<u>; strips disallowed tags/attrs
 */
function _sanitizeBoldUnderlineHtml_(input) {
  if (!input) return '';

  var s = String(input);

  // 1) Remove comments, script/style blocks entirely
  s = s
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, '');

  // 2) Normalize common tags/voids
  s = s
    .replace(/<\s*strong\b[^>]*>/gi, '<b>')
    .replace(/<\s*\/\s*strong\s*>/gi, '</b>')
    .replace(/<\s*br\s*\/?>/gi, '<br>');

  // 3) Convert style-based wrappers to <b>/<u> (span/div/p only)
  //    Detect font-weight:bold/700+/800+/900 and text-decoration: underline
  s = s.replace(
    /<\s*(span|div|p)\b([^>]*)>([\s\S]*?)<\/\s*\1\s*>/gi,
    function (_, tag, attrs, inner) {
      var a = String(attrs || '');
      var hasBold = /font-weight\s*:\s*(bold|[7-9]00)/i.test(a);
      var hasUnderline = /text-decoration(?:-line)?\s*:\s*[^";]*underline/i.test(a);

      // Strip inline style/class/etc. on these containers
      var content = inner;

      if (hasBold && hasUnderline) return '<b><u>' + content + '</u></b>';
      if (hasBold)                return '<b>' + content + '</b>';
      if (hasUnderline)           return '<u>' + content + '</u>';
      return '<' + tag.toLowerCase() + '>' + content + '</' + tag.toLowerCase() + '>';
    }
  );

  // 4) Whitelist tags and clean attributes
  //    Allowed tags: b, u, br, p, div, ul, ol, li, a
  //    For <a> keep only href/target/rel (validate href)
  var ALLOWED = { b:1, u:1, br:1, p:1, div:1, ul:1, ol:1, li:1, a:1 };
  s = s.replace(/<\s*\/?\s*([a-z0-9]+)([^>]*)>/gi, function (m, tn, rawAttrs) {
    var tag = String(tn || '').toLowerCase();

    // void tag
    if (tag === 'br') return '<br>';

    // closing tag
    if (m[1] === '/' || /^<\s*\//.test(m)) {
      return ALLOWED[tag] ? ('</' + tag + '>') : '';
    }

    // opening tags
    if (!ALLOWED[tag]) return '';

    if (tag === 'a') {
      // keep only href/target/rel + sanitize href
      var hrefMatch   = /\bhref\s*=\s*("([^"]*)"|'([^']*)'|([^\s"'=<>`]+))/i.exec(rawAttrs) || [];
      var hrefRaw     = hrefMatch[2] || hrefMatch[3] || hrefMatch[4] || '';
      var href        = String(hrefRaw || '').trim();

      // Disallow javascript: and other dangerous schemes
      var safeHref = '';
      if (/^(https?:|mailto:|tel:|#|\/)/i.test(href)) {
        safeHref = href;
      } else if (href) {
        // fallback to plain text anchor with no link
        safeHref = '';
      }

      var targetMatch = /\btarget\s*=\s*("([^"]*)"|'([^']*)')/i.exec(rawAttrs) || [];
      var target      = (targetMatch[2] || targetMatch[3] || '').trim();
      target = target && /^_blank|_self|_parent|_top$/i.test(target) ? target : '_blank';

      var relMatch = /\brel\s*=\s*("([^"]*)"|'([^']*)')/i.exec(rawAttrs) || [];
      var rel      = (relMatch[2] || relMatch[3] || '').trim();
      if (!rel) rel = 'noopener';

      return '<a' + (safeHref ? ' href="' + _escapeHtmlAttr_(safeHref) + '"' : '') +
             ' target="' + _escapeHtmlAttr_(target) + '"' +
             ' rel="' + _escapeHtmlAttr_(rel) + '">';
    }

    // all other allowed tags: strip attributes
    return '<' + tag + '>';
  });

  // 5) Strip any remaining disallowed tags defensively (if any slipped through)
  s = s.replace(/<\s*\/?\s*(?!b\b|u\b|br\b|p\b|div\b|ul\b|ol\b|li\b|a\b)[a-z0-9]+[^>]*>/gi, '');

  // 6) Balance simple inline tags (<b>, <u>) in case editor produced unclosed tags
  s = _balanceInlineTags_(s, ['b','u']);

  // 7) Clean empty wrappers like <p></p> and normalize whitespace
  s = s.replace(/<(p|div)>\s*<\/\1>/gi, '');
  s = s.replace(/\r\n/g, '\n').trim();

  return s;

  // --- helpers (scoped) ---
  function _escapeHtmlAttr_(v) {
    return String(v || '')
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;');
  }

  function _balanceInlineTags_(html, tagNames) {
    var stack = [];
    // tokenise tags; rebuild with simple stack discipline for listed tags
    return String(html).replace(/<\s*\/?\s*([a-z0-9]+)[^>]*>/gi, function (m, tn) {
      var tag = String(tn || '').toLowerCase();

      // pass through non-target tags unmodified
      if (tagNames.indexOf(tag) === -1 && !(tag === 'br')) return m;

      var isClose = /^<\s*\//.test(m);
      if (!isClose) {
        // opening
        stack.push(tag);
        return '<' + tag + '>';
      } else {
        // closing
        if (stack.length && stack[stack.length - 1] === tag) {
          stack.pop();
          return '</' + tag + '>';
        } else {
          // stray closing → drop it
          return '';
        }
      }
    }) + stack.reverse().map(function(t){ return '</' + t + '>'; }).join('');
  }
}



function hourlyBackupAvailabilityAPI() {
  // ── CONFIG ─────────────────────────────────────────────────────────────
  const FOLDER_ID = '1quRd3rmmYnAFCW-E9PggRQvOvwajhpjJ';   // destination folder (can be a shortcut)
  const TARGET_SPREADSHEET_ID = '1BSomZL0jRse5SGfTgADwswVmIjY4mCMfvDAfQxxIUA8'; // source spreadsheet
  const KEEP_DAYS = 5;
  const NAME_PREFIX  = 'BACKUP Availability API do not use ';
  const PROPS_PREFIX = 'BACKUP Availability API ScriptProperties ';
  const MAX_RETRIES = 5, BASE_SLEEP_MS = 400;
  // ───────────────────────────────────────────────────────────────────────

  const tz  = Session.getScriptTimeZone();
  const now = new Date();
  const stamp = Utilities.formatDate(now, tz, 'ddMMyyyy_HHmm');
  const backupName    = NAME_PREFIX  + stamp;
  const propsFileName = PROPS_PREFIX + stamp + '.txt';

  const log = (m, ...a) => { try { Logger.log(m, ...a); } catch(_) {} };
  const logErr = (label, e) => {
    const msg = (e && e.message) || String(e);
    const name = (e && e.name) || 'Error';
    const stack = (e && e.stack) || '(no stack)';
    log('[ERROR] %s\nname=%s\nmessage=%s\nstack=%s', label, name, msg, stack);
    try { if (e && e.details) log('[Drive API details] %s', JSON.stringify(e.details)); } catch(_) {}
  };
  const withRetry = (label, fn) => {
    let lastErr;
    for (let i = 0; i < MAX_RETRIES; i++) {
      try {
        if (i > 0) Utilities.sleep(BASE_SLEEP_MS * Math.pow(2, i - 1));
        return fn();
      } catch (e) {
        lastErr = e;
        const msg = (e && e.message) || String(e);
        const transient = /Internal error|Backend Error|We're sorry|Rate Limit|Timeout|500|502|503|504/i.test(msg);
        log('[retry %d/%d] %s: %s', i + 1, MAX_RETRIES, label, msg);
        if (!transient && i >= 1) break;
      }
    }
    throw new Error(label + ' failed after retries: ' + (lastErr && lastErr.message));
  };

  // Resolve folder (handle shortcuts) — v3 fields
  function resolveFolderIdOrThrow(id) {
    const f = withRetry('Drive.Files.get(folder)', () => Drive.Files.get(id, { supportsAllDrives: true }));
    if (f.mimeType === 'application/vnd.google-apps.shortcut') {
      const targetId = f.shortcutDetails && f.shortcutDetails.targetId;
      if (!targetId) throw new Error('Shortcut has no targetId.');
      const tf = withRetry('Drive.Files.get(shortcut target)', () =>
        Drive.Files.get(targetId, { supportsAllDrives: true })
      );
      if (tf.mimeType !== 'application/vnd.google-apps.folder') {
        throw new Error('Shortcut target is not a folder (mimeType=' + tf.mimeType + ').');
      }
      log('Resolved folder shortcut → targetId=%s', targetId);
      return targetId;
    }
    if (f.mimeType !== 'application/vnd.google-apps.folder') {
      throw new Error('Provided FOLDER_ID is not a folder (mimeType=' + f.mimeType + ').');
    }
    return id;
  }

  log('Backup start @ %s (tz=%s)', Utilities.formatDate(now, tz, 'yyyy-MM-dd HH:mm:ss'), tz);
  const srcId = (TARGET_SPREADSHEET_ID || '').trim();
  if (!srcId) throw new Error('Standalone script: set TARGET_SPREADSHEET_ID.');

  const destFolderId = resolveFolderIdOrThrow(FOLDER_ID);

  // 1) Copy spreadsheet into destination (v3: name + parents)
  let backupFile;
  try {
    const resource = { name: backupName, parents: [destFolderId] };
    backupFile = withRetry('Drive.Files.copy(spreadsheet)', () =>
      Drive.Files.copy(resource, srcId, { supportsAllDrives: true })
    );
    log('Copy OK: id=%s name="%s"', backupFile.id, backupFile.name);
  } catch (e) {
    logErr('Drive.Files.copy', e);
    throw e;
  }

  // 2) Create snapshot text file (v3: Files.create)
  try {
    const allProps = PropertiesService.getScriptProperties().getProperties() || {};
    const jsonPretty = JSON.stringify(allProps, null, 2);
    const restoreSnippet =
`// === Restore Script Properties ===
function restoreScriptPropertiesFromSnapshot() {
  const payload = ${jsonPretty};
  PropertiesService.getScriptProperties().setProperties(payload, true);
  Logger.log('Restored %s keys', Object.keys(payload).length);
}`;
    const content = [
      `# Availability API — Script Properties Snapshot`,
      `# Created: ${Utilities.formatDate(now, tz, 'yyyy-MM-dd HH:mm:ss z')}`,
      `# Keys: ${Object.keys(allProps).length}`,
      ``,
      `## JSON Payload`,
      jsonPretty,
      ``,
      `## Restore Helper`,
      restoreSnippet,
      ``
    ].join('\n');

    const blob = Utilities.newBlob(content, 'text/plain', propsFileName);
    const meta = { name: propsFileName, mimeType: 'text/plain', parents: [destFolderId] };
    const snap = withRetry('Drive.Files.create(snapshot)', () =>
      Drive.Files.create(meta, blob, { supportsAllDrives: true })
    );
    log('Snapshot OK: id=%s name="%s" bytes=%s', snap.id, snap.name, content.length);
  } catch (e) {
    logErr('Writing snapshot', e);
    // continue
  }

  // 3) Cleanup old backups (v3: name + modifiedTime; trash via update)
  try {
    const cutoffIso = new Date(now.getTime() - KEEP_DAYS * 24 * 60 * 60 * 1000).toISOString();
    const q = [
      `'${destFolderId}' in parents`,
      `and (name contains '${NAME_PREFIX}' or name contains '${PROPS_PREFIX}')`,
      `and modifiedTime < '${cutoffIso}'`,
      'and trashed = false'
    ].join(' ');
    let pageToken, checked = 0, trashed = 0;
    do {
      const res = withRetry('Drive.Files.list(cleanup)', () =>
        Drive.Files.list({
          q,
          pageToken,
          pageSize: 100,
          supportsAllDrives: true,
          includeItemsFromAllDrives: true, // tolerated by v3 in Apps Script
          fields: 'files(id,name),nextPageToken'
        })
      );
      const items = (res.files || []);
      items.forEach(file => {
        checked++;
        try {
          withRetry(`Drive.Files.update(trashed:${file.name})`, () =>
            Drive.Files.update({ trashed: true }, file.id, null, { supportsAllDrives: true })
          );
          trashed++;
          log('Trashed old backup: %s', file.name);
        } catch (e) {
          logErr('Trash ' + file.name, e);
        }
      });
      pageToken = res.nextPageToken;
    } while (pageToken);
    log('Cleanup done: checked=%s trashed=%s', checked, trashed);
  } catch (e) {
    logErr('Cleanup loop', e);
  }

  log('Backup complete: "%s" (id=%s)', backupFile.name, backupFile.id);
}


/**
 * Send the initial EMERGENCY email via Power Automate (no attachments).
 * Uses Script Property POWER_EMAIL and EMERGENCY_FROM_EMAIL.
 * Expects an "alert" object created by _emergencyCreateRecord().
 */



/**
 * Send the ACKNOWLEDGEMENT email via Power Automate (no attachments).
 * Called when WATI quick-reply is tapped or when ClickSend DTMF=1 is received.
 */

function _sendEmergencyAckEmailPA(alert, responderName, whenIso) {
  const props = PropertiesService.getScriptProperties();
  const hook = props.getProperty('POWER_EMAIL');
  if (!hook) return { ok: false, error: 'POWER_EMAIL_NOT_CONFIGURED' };

  const tz = props.getProperty('TIMEZONE') || 'Europe/London';
  const to = props.getProperty('EMERGENCY_FROM_EMAIL') || 'sussex@arthur-rai.co.uk';
  const whenLabel = Utilities.formatDate(new Date(whenIso || new Date()), tz, 'EEEE d MMMM yyyy HH:mm') + 'hrs';

  // ───── helpers for display-only transformations ─────
  function stripHrs(t) { return String(t || '').trim().replace(/\s*hrs?$/i, ''); }
  function mapIssueTypeLabel(issue) {
    const r = String(issue || '').toUpperCase();
    if (r === 'CANNOT_ATTEND') return 'Cancelling Shift';
    if (r === 'LEAVE_EARLY')   return 'Needs to leave their current shift early';
    if (r === 'DNA')           return 'Did Not Arrive';
    return String(issue || '');
  }
  // (kept as-is: display tidy for free-text reason if it happens to equal the enums)
  function mapReasonForEmail(reason) {
    const r = String(reason || '').toUpperCase();
    if (r === 'CANNOT_ATTEND') return 'Cancelling Shift';
    if (r === 'LEAVE_EARLY')   return 'Needs to leave their current shift early';
    if (r === 'DNA')           return 'Did Not Arrive';
    return reason || '';
  }
  function formatLeaveEarlyEmailLabel(src) {
    const s = String(src || '').trim();
    if (!s) return '';
    const up = s.toUpperCase();
    if (up === 'NOW' || up === 'ASAP') return 'This candidate needs to leave immediately';
    const core = stripHrs(s);
    if (/^\d{1,2}:\d{2}$/.test(core)) return `This candidate needs to leave at ${core}hrs`;
    return stripHrs(s);
  }

  // ───── DNA-aware display values (subject + body) ─────
  const isDNA = String(alert.issue_type || (alert.issue && alert.issue.type) || '')
    .toUpperCase() === 'DNA';

  // For DNA: headline absentee (subject_*); else reporter (candidate_*)
  const displayNamePrimary = isDNA
    ? (String(alert.subject_name || '').trim() || String(alert.candidate_name || '').trim() || '')
    : (String(alert.candidate_name || '').trim() || '');

  const displayTelPrimary = isDNA
    ? (String(alert.subject_msisdn || (alert.issue && alert.issue.subject_msisdn) || '').trim() ||
       String(alert.candidate_msisdn || '').trim() || '')
    : (String(alert.candidate_msisdn || '').trim() || '');

  // Subject uses DNA-aware primary name
  var subject = `[EMERGENCY ACK] ${responderName || 'Responder'} — ${displayNamePrimary} — ${alert.date_label || ''}`.trim();

  const issueLabel = mapIssueTypeLabel(alert.issue_type || (alert.issue && alert.issue.type) || '');
  const isLeaveEarly = String(alert.issue_type || (alert.issue && alert.issue.type) || '')
    .toUpperCase() === 'LEAVE_EARLY';
  const displayEta = isLeaveEarly
    ? formatLeaveEarlyEmailLabel(alert.eta_or_leave_time_label || '')
    : (alert.eta_or_leave_time_label || '');
  const displayReason = mapReasonForEmail(alert.reason_text || '');

  var html =
    '<div style="font-family:Arial,Helvetica,sans-serif;line-height:1.45;font-size:14px;color:#111;">' +
      `<p><b>Emergency acknowledgement received</b></p>` +
      `<p><b>Responder:</b> ${_escapeHtml(responderName || '')}</p>` +
      `<p><b>When:</b> ${_escapeHtml(whenLabel)}</p>` +
      // DNA-aware candidate/subject line:
      `<p><b>Candidate:</b> ${_escapeHtml(displayNamePrimary)} (${_escapeHtml(displayTelPrimary || '')})</p>` +
      `<p><b>Hospital/Ward:</b> ${_escapeHtml(alert.hospital || '')} / ${_escapeHtml(alert.ward || '')}</p>` +
      `<p><b>Shift:</b> ${_escapeHtml(alert.shift_type || '')} — ${_escapeHtml(alert.time_range_label || '')} on ${_escapeHtml(alert.date_label || '')}</p>` +
      `<p><b>Issue:</b> ${_escapeHtml(issueLabel)}</p>` +
      (alert.eta_or_leave_time_label ? `<p><b>ETA/Leave time:</b> ${_escapeHtml(displayEta)}</p>` : '') +
      (alert.reason_text ? `<p><b>Reason:</b> ${_escapeHtml(displayReason)}</p>` : '') +
      `<p><b>Alert ID:</b> ${_escapeHtml(alert.alert_id || '')}</p>` +
      `<p>Timezone: ${_escapeHtml(tz)}</p>` +
    '</div>';

  var payload = {
    to: to,
    subject: subject,
    htmlBody: html,
    body: html // fallback
  };

  var res = _postToPowerEmail(hook, payload);
  try {
    _eaAppendLog_(alert.alert_id, 'PowerEmail ACK: ' + JSON.stringify({ status: res && res.status, ok: !!(res && res.ok) }));
  } catch (_) {}
  return res;
}



/** Minimal HTML escaper (same spirit as your _sanitizeLimitedHtml usage) */
function _escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/// ───────────────────────────────────── helper (unchanged) ─────────────────────────────────────



function __listOtherOutstandingCannotAttend(excludeAlertId) {
  try {
    const out = [];
    const idx = _eaIndexMap_(); // { sh, m }
    const sh  = idx.sh, m = idx.m;
    const last = sh.getLastRow();
    for (let r = 2; r <= last; r++) {
      const alert_id = String(sh.getRange(r, m.alert_id + 1).getValue() || '');
      if (!alert_id || alert_id === excludeAlertId) continue;

      const acknowledged = sh.getRange(r, m.acknowledged + 1).getValue();
      const status = String(sh.getRange(r, m.status + 1).getValue() || '').toUpperCase();
      const issue  = String(sh.getRange(r, m.issue_type + 1).getValue() || '').toUpperCase();

      if (acknowledged === true || String(acknowledged) === 'true') continue;
      if (status === 'TIMEOUT' || status === 'CANCELLED') continue;
      if (issue !== 'CANNOT_ATTEND') continue;

      const a = _emergencyFindById(alert_id);
      if (a) out.push(a);
    }
    return out;
  } catch (ex) {
    return [];
  }
}



/**
 * Determine if the user may declare "running late" NOW for a given shift.
 * Enforces the 4-hour rule, uses non-standard times when present.
 * Returns: { eligible, reason?, startAtIso, endAtIso, type }
 */
function _runningLateResolveEligibility(input) {
  const P  = _P();
  const tz = P.TZ || 'Europe/London';

  const ymd         = String(input.ymd || '');
  const shift_type  = String(input.shift_type || '');
  const hospital    = _normalizeHospitalName(String(input.hospital || ''));
  const ward        = String(input.ward || '');
  const job_title   = String(input.job_title || '');
  const booking_ref = String(input.booking_ref || '');
  const shiftInfo   = String(input.shiftInfo || '');

  if (!ymd || !shift_type || !hospital) {
    return { eligible: false, reason: 'MISSING_FIELDS' };
  }

  // 1) Resolve shift start/end (notes override, otherwise standard LD/N times per spec)
  const times = _runningLateResolveShiftTimes({ ymd, shift_type, shiftInfo, tz });
  let startAtIso = times.startAtIso;
  let endAtIso   = times.endAtIso;
  const type     = times.type; // 'LONG DAY' or 'NIGHT'

  // ── FIX: if shiftInfo already carries UK local times, do NOT push them forward.
  // We align the ISO so that, when formatted in tz, it exactly matches the hh:mm in shiftInfo.
  // (No other behaviour is changed.)
  try {
    if (shiftInfo) {
      // Extract "Start: HH:mm" and "Finish: HH:mm" from the note
      const m = /Start:\s*([0-2]?\d:[0-5]\d)\b[\s\S]*?Finish:\s*([0-2]?\d:[0-5]\d)\b/i.exec(shiftInfo);
      if (m) {
        const intendedStart = m[1]; // HH:mm (UK local)
        const intendedEnd   = m[2]; // HH:mm (UK local)

        // Helper: shift a given ISO so its LOCAL (tz) clock reads intended HH:mm
        const alignIsoToLocalHHmm = (iso, hhmm) => {
          try {
            const d     = new Date(iso);
            const local = Utilities.formatDate(d, tz, 'HH:mm');
            if (local === hhmm) return iso; // already correct
            // compute minute delta (local - intended); then move ISO backwards by that delta
            const toMin = s => {
              const [H,M] = s.split(':').map(x=>parseInt(x,10));
              return (H*60 + M) % (24*60);
            };
            const localMin    = toMin(local);
            const intendedMin = toMin(hhmm);
            let diff = localMin - intendedMin; // minutes
            // Normalize large wraparounds to the shortest adjustment (handle midnight boundaries safely)
            if (diff >  12*60) diff -= 24*60;
            if (diff < -12*60) diff += 24*60;
            const fixed = new Date(d.getTime() - diff * 60000);
            return fixed.toISOString();
          } catch (_) {
            return iso;
          }
        };

        startAtIso = alignIsoToLocalHHmm(startAtIso, intendedStart);
        endAtIso   = alignIsoToLocalHHmm(endAtIso, intendedEnd);
      }
    }
  } catch (_) {
    // swallow — eligibility logic below remains unchanged
  }

  // 2) Enforce 4-hour rule:
  //    - If shift already started: OK any time until shift finishes.
  //    - If in future: allowed only when within 4 hours of start.
  const now   = _nowInTZ_(tz);
  const start = new Date(startAtIso);
  const end   = new Date(endAtIso);

  if (now > end) {
    return { eligible: false, reason: 'SHIFT_FINISHED', startAtIso, endAtIso, type };
  }

  // If future: must be within 4h of start
  const fourHoursMs = 4 * 60 * 60 * 1000;
  if (now < start) {
    const diff = start.getTime() - now.getTime();
    if (diff > fourHoursMs) {
      return { eligible: false, reason: 'MORE_THAN_4H_BEFORE_START', startAtIso, endAtIso, type };
    }
  }

  return { eligible: true, startAtIso, endAtIso, type };
}

/**
 * Compute the "arrive no later than" time label, rounding UP to the nearest 5 minutes.
 * Logic: take shiftStart + minutes; take max(current time, result); round up to 5.
 * Returns a Date instance (ISO string formatting happens at call-site).
 */
function _runningLateComputeArrivalBy(args) {
  const tz = (_P().TZ || 'Europe/London');
  const startAtIso = String(args.startAtIso || '');
  const minutes = Math.max(1, Number(args.minutes || 0));
  const nowIso  = String(args.nowIso || _nowIso());

  // If start unknown, base on "now" but keep deterministic rounding
  const base = startAtIso ? new Date(startAtIso) : new Date(nowIso);

  // Deterministic: arrival = start + minutes (no max with now)
  const chosen = new Date(base.getTime() + minutes * 60 * 1000);

  // Round UP to nearest 5 minutes
  chosen.setSeconds(0, 0);
  const mm = chosen.getMinutes();
  const up = Math.ceil(mm / 5) * 5;
  if (up === 60) { chosen.setHours(chosen.getHours() + 1); chosen.setMinutes(0); }
  else { chosen.setMinutes(up); }

  return chosen;
}


/**
 * Resolve shift times honoring non-standard times in notes (“Start: HH:mm Finish: HH:mm”).
 * Standard fallback per spec:
 *  - LONG DAY: 07:30–20:30 (same day)
 *  - NIGHT:    20:30–07:30 (overnight)
 */





/**
 * Build cohorts (same-shift & previous-shift) from EmailHistory,
 * and attach contact numbers from Candidate List.
 * Returns: { sameShift:[{nameKey, msisdn447, jobTitle}], previousShift:[...] }
 */
function _runningLateFindCohortsFromEmailHistory({
  ymd,
  shift_type,            // may be a display string like "07:30–20:00hrs"
  hospital,
  ward,
  msisdn,
  candidate_name,

  // OPTIONAL (if available, lets us derive shift canonically from times)
  shift_start_iso,
  shift_end_iso,
  time_range_label,
  shift_type_raw        // OPTIONAL canonical hint: "LONG DAY" | "NIGHT"
}) {
  const P  = _P();
  const tz = P.TZ || 'Europe/London';
  const ss = _ssRota();

  // ────────────────────── helpers (local only) ──────────────────────
  // Prefer deriving canonical shift from start/end times; fall back to hints/labels.
  function _canonicalShiftFromInputs() {
    try {
      const startIso =
        shift_start_iso || null;
      const endIso   =
        shift_end_iso   || null;

      const start = startIso ? new Date(startIso) : null;
      const end   = endIso   ? new Date(endIso)   : null;

      // If we have concrete times: NIGHT if crosses midnight, else LONG DAY.
      if (start && end && !isNaN(start) && !isNaN(end)) {
        const sY = Utilities.formatDate(start, tz, 'yyyy-MM-dd');
        const eY = Utilities.formatDate(end,   tz, 'yyyy-MM-dd');
        const crossesMidnight = (sY !== eY);
        return crossesMidnight ? 'NIGHT' : 'LONG DAY';
      }

      // Otherwise try explicit raw tag
      const raw = String(shift_type_raw || '').trim().toUpperCase();
      if (raw === 'NIGHT' || raw === 'N') return 'NIGHT';
      if (raw === 'LONG DAY' || raw === 'LONGDAY' || raw === 'DAY') return 'LONG DAY';

      // Fallback to time-range heuristics if provided
      const tr = String(time_range_label || '').toLowerCase();
      if (tr) {
        // crude but safe: night if clearly late hours appear
        if (/(22|23|00|01|02|03|04|05)/.test(tr)) return 'NIGHT';
        if (/(07|08|09).*(19|20)/.test(tr)) return 'LONG DAY';
      }

      // Final fallback: original behaviour based on display shift_type
      const disp = String(shift_type || '').toUpperCase();
      return (disp.includes('N') || disp.includes('NIGHT')) ? 'NIGHT' : 'LONG DAY';
    } catch (_) {
      // ultra-safe fallback
      return (String(shift_type || '').toUpperCase().includes('N') ? 'NIGHT' : 'LONG DAY');
    }
  }

  // Format 447XXXXXXXXX → 07XXXXXXXXX (for human-facing display)
  function _format447To07(n447) {
    const s = String(n447 || '').replace(/\s+/g, '');
    if (/^447\d{9}$/.test(s)) return '0' + s.slice(2);
    if (/^07\d{9}$/.test(s))  return s;          // already local
    if (/^\+447\d{9}$/.test(s)) return '0' + s.slice(3);
    // If unknown format, try best-effort:
    const onlyDigits = s.replace(/[^\d]/g, '');
    if (onlyDigits.startsWith('447') && onlyDigits.length === 12) return '0' + onlyDigits.slice(2);
    return onlyDigits || '';
  }

  // ────────────────────── inputs & normalization ──────────────────────
  const targetHospital = _normalizeHospitalName(String(hospital || ''));
  const targetWard     = String(ward || '').trim();
  const shift          = _canonicalShiftFromInputs();

  // 1) Read EmailHistory sheet
  const ehSh = ss.getSheetByName(P.SH_EH);
  if (!ehSh) return { sameShift: [], previousShift: [], candidate_msisdn_07: _format447To07(_normaliseUkMsisdnTo447(msisdn || '')) };
  const rows = ehSh.getDataRange().getDisplayValues();
  if (rows.length < 2) return { sameShift: [], previousShift: [], candidate_msisdn_07: _format447To07(_normaliseUkMsisdnTo447(msisdn || '')) };

  const hdr = rows[0].map(s => String(s || "").toLowerCase());
  const iOcc   = hdr.indexOf('occupantkey');
  const iDate  = hdr.indexOf('date');
  const iShift = hdr.indexOf('shift');
  const iHosp  = hdr.indexOf('hospital');
  const iWard  = hdr.indexOf('ward');
  const iNotes = hdr.indexOf('notes');

  // 2) Candidate index from Candidate List
  const candIdx = _buildCandidateIndex_(ss); // { nameKey => { msisdn447, jobTitle } }

  // 3) Helper: convert EH date text to ymd
  function _ehDateToYmd(s) {
    const m = String(s || '').match(/\b(\d{2}\/\d{2}\/\d{4})\b/);
    const ddmmyyyy = m ? m[1] : String(s || '');
    const parts = ddmmyyyy.split('/');
    if (parts.length !== 3) return '';
    const [dd, mm, yyyy] = parts.map(Number);
    const d = new Date(yyyy, mm - 1, dd);
    return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
  }

  // 4) Determine previous shift selector
  const [Y, M, D] = String(ymd).split('-').map(Number);
  const curDate = new Date(Y, M - 1, D);
  const prevNightDate = new Date(curDate); prevNightDate.setDate(prevNightDate.getDate() - 1);
  const prevNightYmd = Utilities.formatDate(prevNightDate, tz, 'yyyy-MM-dd');
  const prevSelector = (shift === 'LONG DAY')
    ? { ymd: prevNightYmd, shift: 'NIGHT' }
    : { ymd: ymd,         shift: 'LONG DAY' };

  // Also compute today/yesterday (for diagnostics filter)
  const now = new Date();
  const todayYmd    = Utilities.formatDate(now, tz, 'yyyy-MM-dd');
  const yesterday   = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 1);
  const yesterdayYmd= Utilities.formatDate(yesterday, tz, 'yyyy-MM-dd');

  // 5) Sweep EmailHistory
  const sameShift = [];
  const previousShift = [];
  const diag = [];

  for (let r = 1; r < rows.length; r++) {
    const occ = iOcc >= 0 ? String(rows[r][iOcc] || '').toLowerCase().trim() : '';
    const dateYmd = iDate >= 0 ? _ehDateToYmd(rows[r][iDate]) : '';
    if (!occ || !dateYmd) continue;

    const hospRaw = iHosp >= 0 ? String(rows[r][iHosp] || '') : '';
    const hospNorm = _normalizeHospitalName(hospRaw);
    const wardVal = iWard >= 0 ? String(rows[r][iWard] || '') : '';

    const sRaw = iShift >= 0 ? String(rows[r][iShift] || '').toUpperCase() : '';
    const ehShift = (sRaw.includes('NIGHT') || sRaw === 'N') ? 'NIGHT' : 'LONG DAY';

    const hospitalMatch = (hospNorm === targetHospital);
    const wardMatch     = ((wardVal || '').trim() === targetWard);
    const isCurrentDate = (dateYmd === ymd);
    const isPrevDate    = (dateYmd === prevSelector.ymd);
    const shiftMatchCurrent = (ehShift === shift);
    const shiftMatchPrev    = (ehShift === prevSelector.shift);

    if (isCurrentDate && shiftMatchCurrent && hospitalMatch && wardMatch) {
      const c = candIdx[occ];
      if (c && c.msisdn447) {
        sameShift.push({ nameKey: occ, msisdn447: _format447To07(c.msisdn447), jobTitle: c.jobTitle || '' });
      }
    }

    if (isPrevDate && shiftMatchPrev && hospitalMatch && wardMatch) {
      const c = candIdx[occ];
      if (c && c.msisdn447) {
        previousShift.push({ nameKey: occ, msisdn447: _format447To07(c.msisdn447), jobTitle: c.jobTitle || '' });
      }
    }
  }

  function uniqByMsisdn(list) {
    const seen = new Set();
    const out = [];
    for (const x of list) {
      if (!x || !x.msisdn447) continue;
      if (seen.has(x.msisdn447)) continue;
      seen.add(x.msisdn447);
      out.push(x);
    }
    return out;
  }

  const me447 = _normaliseUkMsisdnTo447(msisdn || '');
  const sameClean = uniqByMsisdn(sameShift).filter(x => x.msisdn447 !== _format447To07(me447));
  const prevClean = uniqByMsisdn(previousShift).filter(x => x.msisdn447 !== _format447To07(me447));

  const candidate_msisdn_07 = _format447To07(me447);

  return { sameShift: sameClean, previousShift: prevClean, candidate_msisdn_07 };
}


/**
 * Send all required WATI notifications for a running-late declaration:
 *  - Same-shift → TEMPLATE_NAME_LATE_SAME
 *  - Previous-shift → TEMPLATE_NAME_LATE_PREV
 *  - Emergency contacts (WATI only, no calls) → TEMPLATE_NAME_LATE_EMERGENCY
 * Expects model:
 * {
 *   candidate_msisdn_447, candidate_name, role, hospital, ward,
 *   date_label, shift_type, time_range_label,
 *   late_by_minutes, late_by_label, arrival_by_label,
 *   sameShift:[{msisdn447}], previousShift:[{msisdn447}],
 *   notifyEmergencyContacts:true/false
 * }
 * Returns brief send stats.
 */
function _runningLateSendNotifications(model) {
  const cfg = _getEmergencyProps();
  const tz  = cfg.TZ || 'Europe/London';

  // ─────────────────────────── logging helpers (adaptive) ───────────────────────────
  const __RID = Utilities.getUuid();
  const __T0  = Date.now();
function __logsSheet() {
  /*
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName('Logs');
    if (!sh) {
      // Default to the 6-col doPost-style header (widest deployment)
      sh = ss.insertSheet('Logs');
      sh.getRange(1,1,1,6).setValues([['Timestamp','ReqId','Route','Event','Action','DataJSON']]);
      sh.setFrozenRows(1);
    }
    return sh;
  } catch (_) { return null; }
  */
  // logging disabled
  return null;
}

  // stringify with truncation to avoid 50k cell limit issues
  function __safeStr(obj, max) {
    const s = (typeof obj === 'string') ? obj : JSON.stringify(obj || {}, null, 0);
    const MAX = (typeof max === 'number' && max > 0) ? max : 20000; // generous but safe
    return s.length > MAX ? (s.slice(0, MAX) + '…[truncated]') : s;
  }

  function __mask447(n) {
    try {
      const s = String(n || '').trim();
      if (!s) return '';
      if (s.length <= 7) return s[0] + '•••';
      return s.slice(0,5) + '•••••' + s.slice(-2);
    } catch(_) { return '•••'; }
  }

  function __headReceiversSample(arr, take) {
    const list = Array.isArray(arr) ? arr : [];
    const N = Math.max(0, Math.min(Number(take || 5), 10));
    return list.slice(0, N).map(r => ({
      whatsappNumber: __mask447(r && r.whatsappNumber),
      customParams: (r && r.customParams) ? (r.customParams.slice(0,6)) : []
    }));
  }
function __log(eventName, dataObj, httpCode) {
  // try {
  //   const sh = __logsSheet(); if (!sh) return;
  //   const stamp  = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
  //
  //   // Detect header shape
  //   const header = sh.getRange(1,1,1,Math.max(6, sh.getLastColumn() || 6)).getValues()[0];
  //   const has6   = header && header[0] === 'Timestamp' && header[1] === 'ReqId' && header[2] === 'Route';
  //   const has7   = header && header[0] === 'Timestamp' && header[1] === 'Route' && header[2] === 'Event' && header[3] === 'Template';
  //
  //   const payload = Object.assign({ ms: Date.now() - __T0, trace: __RID }, dataObj || {});
  //   if (has6) {
  //     // doPost-style
  //     sh.appendRow([ stamp, __RID, '_runningLateSendNotifications', String(eventName || ''), 'RUNNING_LATE_SEND', __safeStr(payload) ]);
  //   } else if (has7) {
  //     // WATI helper-style
  //     sh.appendRow([ stamp, '_runningLateSendNotifications', String(eventName || ''), '', /* Template */// '', /* Broadcast */// (httpCode || ''), /* HttpCode */// __safeStr(payload) ]);
  //   } else {
  //     // Fallback: write a simple 2-cell line to avoid throwing
  //     sh.appendRow([stamp, __safeStr({ route: '_runningLateSendNotifications', event: eventName, payload })]);
  //   }
  // } catch(_) {}
}



function __mark(eventName, extra) {
  /*
  __log(eventName, extra || {});
  */
  // logging disabled
}

  // ─────────────────────────── local helpers ───────────────────────────
  function onlyTime(label) {
    const s = String(label || '').trim();
    return s.replace(/\s*hrs?$/i, '');
  }
  function titleCaseNameKey(nameKey) {
    const s = String(nameKey || '').trim();
    if (!s) return '';
    return s.split(/\s+/).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
  }
  // NEW: invert "surname firstname" → "Firstname Surname" (title-cased)
  function invertNameKeyToFull(nameKey) {
    const s = String(nameKey || '').trim();
    if (!s) return '';
    const parts = s.split(/\s+/);
    if (parts.length < 2) return titleCaseNameKey(s); // fallback
    const surname  = parts[0];
    const firstname= parts.slice(1).join(' ');
    const tcFirst  = firstname.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
    const tcSur    = surname.charAt(0).toUpperCase() + surname.slice(1);
return `${tcFirst} ${tcSur}`.trim();

  }

  // Format 447XXXXXXXXX/+447XXXXXXXXX → 07XXXXXXXXX (for human-facing display / template tokens)
  function _format447To07(n447) {
    const s = String(n447 || '').replace(/\s+/g, '');
    if (/^\+447\d{9}$/.test(s)) return '0' + s.slice(3);
    if (/^447\d{9}$/.test(s))   return '0' + s.slice(2);
    if (/^07\d{9}$/.test(s))    return s; // already local
    const digits = s.replace(/[^\d]/g, '');
    if (digits.startsWith('447') && digits.length === 12) return '0' + digits.slice(2);
    return digits;
  }

  // ---- param builders for each template family ----
  function buildParamsEmergency(m) {
    return [
      { name: 'Candidate_name',   value: m.candidate_name || '' },
      { name: 'Role',             value: m.role || '' },
      { name: 'Shift_type',       value: m.shift_type || '' },
      { name: 'Hospital',         value: m.hospital || '' },
      { name: 'Ward',             value: m.ward || '' },
      { name: 'Date_label',       value: m.date_label || '' },
      { name: 'Arrival_by_label', value: onlyTime(m.arrival_by_label || m.arrivalLabel || '') },
      // ⬇︎ Use local 07 format for display token
      { name: 'Candidate_msisdn', value: m.candidate_msisdn_07 || _format447To07(m.candidate_msisdn_447 || '') }
    ];
  }
function buildParamsPrevOrSame(m, cohortEntry) {
  const firstName =
    (cohortEntry && cohortEntry.firstName) ||
    (m.recipientFirstName) ||
    (cohortEntry && cohortEntry.nameKey ? titleCaseNameKey(cohortEntry.nameKey).split(' ').slice(-1)[0] : '') ||
    '';

  return [
    { name: 'firstName',          value: firstName },
    { name: 'lateWorkerName',     value: m.candidate_name || '' },
    { name: 'lateWorkerJobTitle', value: m.role || '' },
    { name: 'shiftType',          value: m.shift_type || '' },
    { name: 'hospital',           value: m.hospital || '' },
    { name: 'ward',               value: m.ward || '' },
    { name: 'hhmm',               value: onlyTime(m.arrival_by_label || m.arrivalLabel || '') },
    // ⬇︎ Use local 07 format for display token
    { name: 'mobile',             value: m.candidate_msisdn_07 || _format447To07(m.candidate_msisdn_447 || '') }
  ];
}


  function toReceivers(list, paramBuilder) {
    return (list || [])
      .map(x => ({ whatsappNumber: x.msisdn447, customParams: paramBuilder(model, x) }))
      .filter(r => !!r.whatsappNumber);
  }

  // ─────────────────────────── function start ───────────────────────────
  try {
    // High-level model summary (redacted)
    __mark('start', {
      model_summary: {
        candidate_msisdn_447: __mask447(model && model.candidate_msisdn_447),
        candidate_name:       model && model.candidate_name || '',
        role:                 model && model.role || '',
        hospital:             model && model.hospital || '',
        ward:                 model && model.ward || '',
        date_label:           model && model.date_label || '',
        shift_type:           model && model.shift_type || '',
        time_range_label:     model && model.time_range_label || '',
        late_by_minutes:      model && model.late_by_minutes || null,
        arrival_by_label:     model && model.arrival_by_label || '',
        sameShift_count:      (model && Array.isArray(model.sameShift)) ? model.sameShift.length : 0,
        previousShift_count:  (model && Array.isArray(model.previousShift)) ? model.previousShift.length : 0,
        notifyEmergencyContacts: !!(model && model.notifyEmergencyContacts)
      },
      cfg_flags: { TEST_MODE: !!cfg.TEST_MODE }
    });
  } catch(_) {}

  // ─────────────────────────── 1) Same-shift ───────────────────────────
  let sameStats = { attempted: 0, success: 0, ids: [] };
  try {
    const receivers = toReceivers(model.sameShift, buildParamsPrevOrSame);
    sameStats.attempted = receivers.length;
    __mark('sameShift:receivers', { attempted: receivers.length, sample: __headReceiversSample(receivers, 5) });

    if (!receivers.length) {
      __mark('sameShift:skip:no_receivers');
    } else if (typeof _sendWATIBulkTemplate_Generic !== 'function') {
      __mark('sameShift:skip:no_generic_sender');
    } else {
      const t0  = Date.now();
      const out = _sendWATIBulkTemplate_Generic(
        cfg.TEMPLATE_NAME_LATE_SAME,
        receivers,
        'RUNNING_LATE_SAME',
        tz
      );
      const dt = Date.now() - t0;
      sameStats.success = (out && out.httpCode >= 200 && out.httpCode < 300) ? receivers.length : 0;
      try {
        sameStats.ids = (out && out.json && (out.json.data || out.json.messageIds || out.json.ids)) || [];
      } catch (_) {}
      __log('sameShift:send:result', {
        httpCode: out && out.httpCode,
        duration_ms: dt,
        ids_count: (sameStats.ids || []).length,
        json_has_error: !!(out && out.json && (out.json.error || out.json.message === 'error')),
      }, out && out.httpCode);
    }
  } catch (e1) {
    __mark('sameShift:error', { error: String(e1 && e1.message || e1) });
  }

  // ─────────────────────────── 2) Previous-shift ───────────────────────────
  let prevStats = { attempted: 0, success: 0, ids: [] };
  try {
    const receivers = toReceivers(model.previousShift, buildParamsPrevOrSame);
    prevStats.attempted = receivers.length;
    __mark('previousShift:receivers', { attempted: receivers.length, sample: __headReceiversSample(receivers, 5) });

    if (!receivers.length) {
      __mark('previousShift:skip:no_receivers');
    } else if (typeof _sendWATIBulkTemplate_GENERIC !== 'function' && typeof _sendWATIBulkTemplate_Generic === 'function') {
      // safeguard if someone typoed the helper name elsewhere; keep original flow unchanged otherwise
    }

    if (!prevStats.attempted) {
      // keep behavior consistent
    } else if (typeof _sendWATIBulkTemplate_Generic !== 'function') {
      __mark('previousShift:skip:no_generic_sender');
    } else {
      const t0  = Date.now();
      const out = _sendWATIBulkTemplate_Generic(
        cfg.TEMPLATE_NAME_LATE_PREV,
        receivers,
        'RUNNING_LATE_PREV',
        tz
      );
      const dt = Date.now() - t0;
      prevStats.success = (out && out.httpCode >= 200 && out.httpCode < 300) ? receivers.length : 0;
      try {
        prevStats.ids = (out && out.json && (out.json.data || out.json.messageIds || out.json.ids)) || [];
      } catch (_) {}
      __log('previousShift:send:result', {
        httpCode: out && out.httpCode,
        duration_ms: dt,
        ids_count: (prevStats.ids || []).length,
        json_has_error: !!(out && out.json && (out.json.error || out.json.message === 'error')),
      }, out && out.httpCode);
    }
  } catch (e2) {
    __mark('previousShift:error', { error: String(e2 && e2.message || e2) });
  }

  // ─────────────────────────── 3) Emergency contacts ───────────────────────────
  let ecStats = { attempted: 0, success: 0, ids: [], filtered: { empty: 0, malformed: 0, deduped: 0 } };
  try {
    if (model.notifyEmergencyContacts) {
      const contacts = _getEmergencyContacts() || [];
      const cand447  = String(model.candidate_msisdn_447 || '').trim();

      const TEST_MODE        = String(cfg.TEST_MODE).toUpperCase() === 'TRUE' || cfg.TEST_MODE === true;
      const ALLOW_SELF_NOTIFY= !!TEST_MODE;

      const seen = new Set();
      const receivers = [];

      for (const c of contacts) {
        const raw = (c && (c.mobile447 || c.mobile || c.msisdn || c.whatsappNumber || '')).toString().trim();
        if (!raw) { ecStats.filtered.empty++; continue; }
        const n447 = _normaliseUkMsisdnTo447(raw);
        if (!n447) { ecStats.filtered.malformed++; continue; }

        if (!ALLOW_SELF_NOTIFY && cand447 && n447 === cand447) { ecStats.filtered.deduped++; continue; }
        if (seen.has(n447)) { ecStats.filtered.deduped++; continue; }
        seen.add(n447);

        receivers.push({ whatsappNumber: n447, customParams: buildParamsEmergency(model) });
      }

      ecStats.attempted = receivers.length;
      __mark('ec:receivers', {
        contact_source_count: contacts.length,
        attempted: receivers.length,
        filtered: ecStats.filtered,
        sample: __headReceiversSample(receivers, 5),
        TEST_MODE: TEST_MODE,
        ALLOW_SELF_NOTIFY: ALLOW_SELF_NOTIFY
      });

      if (!receivers.length) {
        __mark('ec:skip:no_receivers');
      } else if (typeof _sendWATIBulkTemplate_Generic !== 'function') {
        __mark('ec:skip:no_generic_sender');
      } else {
        const t0  = Date.now();
        const out = _sendWATIBulkTemplate_Generic(
          cfg.TEMPLATE_NAME_LATE_EMERGENCY, // exactly "lateemergency" in WATI
          receivers,
          'RUNNING_LATE_EC',
          tz
        );
        const dt = Date.now() - t0;
        ecStats.success = (out && out.httpCode >= 200 && out.httpCode < 300) ? receivers.length : 0;
        try {
          ecStats.ids = (out && out.json && (out.json.data || out.json.messageIds || out.json.ids)) || [];
        } catch (_) {}
        __log('ec:send:result', {
          httpCode: out && out.httpCode,
          duration_ms: dt,
          ids_count: (ecStats.ids || []).length,
          json_has_error: !!(out && out.json && (out.json.error || out.json.message === 'error')),
        }, out && out.httpCode);
      }
    } else {
      __mark('ec:skip:flag_off');
    }
  } catch (e3) {
    __mark('ec:error', { error: String(e3 && e3.message || e3) });
  }

  // ─────────────────────────── 4) Send informational email (no action required) ───────────────────────────
  try {
    const props = PropertiesService.getScriptProperties();
    const hook  = props.getProperty('POWER_EMAIL');
    const to    = props.getProperty('EMERGENCY_FROM_EMAIL') || 'sussex@arthur-rai.co.uk';

    if (hook && to) {
      // Build colleague name lists from cohorts (invert "surname firstname" → "Firstname Surname")
      const sameNames = (Array.isArray(model.sameShift) ? model.sameShift : [])
        .map(x => x && x.nameKey ? invertNameKeyToFull(x.nameKey) : '')
        .filter(Boolean);
      const prevNames = (Array.isArray(model.previousShift) ? model.previousShift : [])
        .map(x => x && x.nameKey ? invertNameKeyToFull(x.nameKey) : '')
        .filter(Boolean);

      const candName = String(model.candidate_name || '').trim();
      const candMs07 = _format447To07(model.candidate_msisdn_447 || '');
      const role     = String(model.role || '').trim();
      const hospital = String(model.hospital || '').trim();
      const ward     = String(model.ward || '').trim();
      const dateLabel= String(model.date_label || '').trim();
      const shiftType= String(model.shift_type || '').trim().toUpperCase();
      const timeRange= String(model.time_range_label || '').trim();
      const arrivalLbl=String(model.arrival_by_label || '').trim();
      const alertId  = String(model.alert_id || '').trim();
      const tzUse    = tz;

      // Subject in your established format:
      // [EMERGENCY] <Candidate_name> — <Shift_type> — <Date_label> (Running Late - no action required)
      const subject = `[EMERGENCY] ${candName || 'Candidate'} — ${shiftType || ''} — ${dateLabel || ''} (Running Late - no action required)`.trim();

      // Plain-text style body wrapped in HTML (keep consistent with your other emails)
      const html =
        '<div style="font-family:Arial,Helvetica,sans-serif;line-height:1.45;font-size:14px;color:#111;">' +
          `<p><b>Emergency running late notification</b></p>` +
          `<p><b>Candidate:</b> ${_escapeHtml(candName)} (${_escapeHtml(candMs07)})</p>` +
          (role ? `<p><b>Role:</b> ${_escapeHtml(role)}</p>` : '') +
          `<p><b>Hospital/Ward:</b> ${_escapeHtml(hospital)} / ${_escapeHtml(ward)}</p>` +
          (dateLabel ? `<p><b>Date:</b> ${_escapeHtml(dateLabel)}</p>` : '') +
          `<p><b>Shift:</b> ${_escapeHtml(shiftType)} — ${_escapeHtml(timeRange)}</p>` +
          `<p><b>Status:</b> Candidate is running late.<br>` +
            (arrivalLbl ? `Candidate has advised they will arrive no later than <b>${_escapeHtml(arrivalLbl)}</b>.` : '') +
          `</p>` +
          `<p><b>Colleagues notified:</b></p>` +
          `<ul>` +
            (sameNames.length ? `<li><i>Same shift:</i> ${_escapeHtml(sameNames.join(', '))}</li>` : '') +
            (prevNames.length ? `<li><i>Previous shift:</i> ${_escapeHtml(prevNames.join(', '))}</li>` : '') +
            (!sameNames.length && !prevNames.length ? `<li>(none)</li>` : '') +
          `</ul>` +
          (alertId ? `<p><b>Alert ID:</b> ${_escapeHtml(alertId)}</p>` : '') +
          `<p>Timezone: ${_escapeHtml(tzUse)}</p>` +
        '</div>';

      const payload = { to, subject, htmlBody: html, body: html };
      const res     = _postToPowerEmail(hook, payload);
      __mark('email:sent', {
        to_masked: to.replace(/^[^@]+/, m => (m && m[0]) ? (m[0] + '•••') : '•••'),
        ok: !!(res && res.ok),
        status: res && res.status
      });
    } else {
      __mark('email:skip:no_hook_or_to', { hasHook: !!hook, hasTo: !!to });
    }
  } catch (e4) {
    __mark('email:error', { error: String(e4 && e4.message || e4) });
  }

  // ─────────────────────────── wrap up ───────────────────────────
  try {
    __mark('done', { summary: { sameShift: sameStats, previousShift: prevStats, emergencyContacts: ecStats } });
  } catch(_) {}

  return { sameShift: sameStats, previousShift: prevStats, emergencyContacts: ecStats };
}


/**
 * Candidate index for quick lookup by nameKey ("surname firstname") → { msisdn447, jobTitle }.
 * Reads Candidate List once; tolerates partial data.
 */
function _buildCandidateIndex_(ss) {
  const P = _P();
  const sh = ss.getSheetByName(P.SH_CAND);
  const out = {};
  if (!sh) return out;

  const vals = sh.getDataRange().getDisplayValues();
  if (vals.length < 2) return out;

  // Map headers (we know some expected names from your sample)
  const hdr = vals[0].map(s => String(s || '').toLowerCase().trim());
  const iFirst = hdr.indexOf('first name');
  const iSur   = hdr.indexOf('surname');
  const iTel   = hdr.indexOf('telephone');
  const iJob   = hdr.indexOf('job title');

  for (let r = 1; r < vals.length; r++) {
    const fn = iFirst >= 0 ? String(vals[r][iFirst] || '').trim() : '';
    const sn = iSur   >= 0 ? String(vals[r][iSur]   || '').trim() : '';
    if (!fn && !sn) continue;
    const nameKey = (sn + ' ' + fn).toLowerCase().trim();

    const telRaw = iTel >= 0 ? String(vals[r][iTel] || '') : '';
    const ms447  = _normaliseUkMsisdnTo447(telRaw);
    const job    = iJob >= 0 ? String(vals[r][iJob] || '').trim().toUpperCase() : '';

    out[nameKey] = { msisdn447: ms447, jobTitle: job };
  }
  return out;
}
/**
 * List booked shifts the user can declare "running late" for *right now*,
 * honoring the 4-hour rule and non-standard times in EmailHistory Notes.
 *
 * Returns an array of:
 *  {
 *    ymd,                      // "YYYY-MM-DD" (shift start date)
 *    shift_type,               // "LONG DAY" | "NIGHT" (resolved)
 *    hospital, ward,           // normalized hospital; exact ward text
 *    job_title, booking_ref,   // from EmailHistory (cols H/D or headers)
 *    shiftInfo,                // raw Notes (may hold non-standard times)
 *    startAtIso, endAtIso,     // resolved actual times (ISO Z)
 *    date_label,               // e.g. "TODAY Tuesday 18 September 2025"
 *    time_range_label          // e.g. "07:30–20:30hrs"
 *  }
 *
 * @param {string} msisdn  The caller’s mobile (local "0XXXXXXXXXX" or 44… tolerated)
 * @param {string=} nowIso Optional ISO timestamp to evaluate "now" (for tests)
 */
function _getEmergencyEligibleShifts(msisdn, nowIso) {
  try {
    const P   = (typeof _P === 'function') ? _P() : {};
    const tz  = P.TZ || 'Europe/London';
    const now = nowIso ? new Date(nowIso) : new Date();

    // ── Grace windows for including already-started (or soon-to-start) shifts
    const GRACE_MIN_AFTER_START = 600; // include if within 30m after official start
    // const PRE_MIN_BEFORE_START = 60; // (optional) include up to 60m before start

    // ───────────────────────── Date helpers (robust conversions) ─────────────────────────
    function __asDate(x) {
      if (x instanceof Date) return x;
      if (typeof x === 'string' || typeof x === 'number') {
        const d = new Date(x);
        if (!isNaN(d)) return d;
      }
      try {
        const d2 = new Date(String(x || ''));
        if (!isNaN(d2)) return d2;
      } catch {}
      return new Date(); // safe fallback
    }
    function fmtYmd(d) {
      const D = __asDate(d);
      return Utilities.formatDate(D, tz, 'yyyy-MM-dd');
    }
    function asYmd(anyVal) {
      // Return a yyyy-MM-dd string regardless of input type
      if (anyVal instanceof Date) return fmtYmd(anyVal);
      if (typeof anyVal === 'string') {
        if (/^\d{4}-\d{2}-\d{2}$/.test(anyVal)) return anyVal;
        return fmtYmd(__asDate(anyVal));
      }
      return fmtYmd(__asDate(anyVal));
    }
    function ymdMinMax(list) {
      const ys = (list || []).filter(Boolean).map(String);
      if (!ys.length) return { min: '', max: '' };
      const sorted = ys.slice().sort();
      return { min: sorted[0], max: sorted[sorted.length - 1] };
    }
    // Format "HHmmhrs - HHmmhrs" in local tz from ISO instants
    function __formatHrsRange(startIso, endIso) {
      try {
        const s = Utilities.formatDate(new Date(startIso), tz, 'HHmm') + 'hrs';
        const e = Utilities.formatDate(new Date(endIso),   tz, 'HHmm') + 'hrs';
        return s + ' - ' + e;
      } catch (_) {
        return '';
      }
    }

    // ─────────────────────────────── enter + cache probe ───────────────────────────────
    const cached = (typeof _tilesGet === 'function') ? _tilesGet(msisdn) : null;
    const hasCache = !!(cached && Array.isArray(cached.tiles) && Array.isArray(cached.headers));
    const headersLen = hasCache ? cached.headers.length : 0;
    const tilesLen   = hasCache ? cached.tiles.length   : 0;

    if (!hasCache) {
      return []; // no cache → keep button hidden
    }

    // Hard guard: headers/tiles must align
    if (headersLen !== tilesLen) {
      return [];
    }

    const H = cached.headers.map(String);
    const T = cached.tiles;

    // Pair by index into a lookup map for exact date→tile inspection
    const byYmd = {};
    for (let i = 0; i < H.length; i++) {
      const y = String(H[i] || '');
      if (y) byYmd[y] = T[i];
    }

    // ── Compute the 3-day run-late window (yesterday/today/tomorrow in tz) robustly ──
    // 1) Anchor "today" at local midnight in tz
    const todayMidnightStr = Utilities.formatDate(now, tz, 'yyyy/MM/dd'); // string
    const today = __asDate(new Date(todayMidnightStr));                   // Date
    const ymdToday = fmtYmd(today);                                       // 'yyyy-MM-dd'

    // 2) Use _addDays IF PRESENT but always pass a YMD STRING
    const ymdYesterday = (typeof _addDays === 'function')
      ? asYmd(_addDays(ymdToday, -1, tz))
      : fmtYmd(new Date(today.getFullYear(), today.getMonth(), today.getDate() - 1));
    const ymdTomorrow  = (typeof _addDays === 'function')
      ? asYmd(_addDays(ymdToday,  1, tz))
      : fmtYmd(new Date(today.getFullYear(), today.getMonth(), today.getDate() + 1));
    const nearSet = new Set([ymdYesterday, ymdToday, ymdTomorrow]);
    const cancelWindow = new Set([ymdToday, ymdTomorrow]); // ← only allow cancel today/tomorrow

    const runLateEligible = [];   // items eligible for "I'm running late"
    let nextUpcoming = null;      // first FUTURE booked shift (strictly > now)
    let nextCancelable = null;    // first FUTURE shift that is in cancelWindow (today/tomorrow only)

    // ─────────────────────────────── full window scan (booked days) ───────────────────────────────
    for (let i = 0; i < H.length; i++) {
      const ymd  = String(H[i] || '');
      const tile = T[i];
      if (!ymd || !tile || !tile.booked) {
        continue;
      }

      const shiftInfo   = String(tile.shiftInfo || '');
      const hospital    = String(tile.hospital  || '');
      const ward        = String(tile.ward      || '');
      const job_title   = String(tile.jobTitle  || '');
      const booking_ref = String(tile.bookingRef|| '');

      // Resolve concrete times
      const times = (typeof _runningLateResolveShiftTimes === 'function')
        ? _runningLateResolveShiftTimes({ ymd, shift_type: '', shiftInfo, tz })
        : null;
      if (!times || !times.startAtIso || !times.endAtIso) {
        continue;
      }

      const startTs = Date.parse(times.startAtIso) || 0;

      // ➊ Compute grace-window checks relative to start
      const startMs = startTs;
      const nowMs   = now.getTime();
      const withinAfterStartGrace =
        nowMs >= startMs && nowMs <= (startMs + GRACE_MIN_AFTER_START * 60 * 1000);

      // (optional) allow a pre-start grace window as well
      // const withinBeforeStartGrace =
      //   nowMs >= (startMs - PRE_MIN_BEFORE_START * 60 * 1000) && nowMs < startMs;

      // Run-late eligibility only for y/t/t
      let canRunLate = false;
      let eligReason = '';
      if (nearSet.has(ymd) && typeof _runningLateResolveEligibility === 'function') {
        const elig = _runningLateResolveEligibility({
          ymd,
          shift_type: String(times.type || ''),
          hospital, ward, job_title, booking_ref, shiftInfo, msisdn
        }) || { eligible: false };
        canRunLate = !!elig.eligible;
        eligReason = String(elig.reason || '');
      }

      // ⬅️ include if we’re within the post-start grace window
      if (withinAfterStartGrace) {
        canRunLate = true;
        if (!eligReason) eligReason = `within ${GRACE_MIN_AFTER_START}m after start`;
      }

      // (optional) include the pre-start window too
      // if (withinBeforeStartGrace) {
      //   canRunLate = true;
      //   if (!eligReason) eligReason = `within ${PRE_MIN_BEFORE_START}m before start`;
      // }

      // Build item. We KEEP a canonical raw type for logic, but expose times for the UI via shift_type.
      const canonicalType = String(times.type || (shiftInfo.toUpperCase().includes('N') ? 'NIGHT' : 'LONG DAY'));
      const hrsRange = __formatHrsRange(times.startAtIso, times.endAtIso); // e.g., "1930hrs - 0800hrs"

      const item = {
        ymd,
        // Show exact times to the app (no front-end change needed)
        shift_type: hrsRange || canonicalType,
        // Preserve the raw canonical type for any logic/diagnostics that may need it
        shift_type_raw: canonicalType,
        hospital, ward, job_title, booking_ref, shiftInfo,
        startAtIso: times.startAtIso,
        endAtIso:   times.endAtIso,
        date_label: (typeof _formatDateLabel === 'function') ? _formatDateLabel(ymd, tz) : ymd,
        time_range_label: (typeof _formatTimeRangeLabel === 'function') ? _formatTimeRangeLabel(times.startAtIso, times.endAtIso, tz) : hrsRange,
        canRunLate: canRunLate,
        canCancel:  false
      };

      if (canRunLate) runLateEligible.push(item);

      // Track next FUTURE shift
      const isFuture = startTs > now.getTime();
      if (isFuture) {
        // Keep earliest future (any day)
        if (!nextUpcoming || startTs < Date.parse(nextUpcoming.startAtIso || '')) {
          nextUpcoming = item;
        }
        // Keep earliest future that is *today or tomorrow* for cancel
        if (cancelWindow.has(ymd)) {
          if (!nextCancelable || startTs < Date.parse(nextCancelable.startAtIso || '')) {
            nextCancelable = item;
          }
        }
      }
    }

    // Sort run-late options by soonest start
    runLateEligible.sort((a, b) => (Date.parse(a.startAtIso || '') || 0) - (Date.parse(b.startAtIso || '') || 0));

    // Build final output (merge cancel into matching item or append cancel-only)
    let out;
    if (runLateEligible.length) {
      out = runLateEligible.slice();

      if (nextCancelable) {
        const match = (x) =>
          x.ymd === nextCancelable.ymd &&
          x.hospital === nextCancelable.hospital &&
          x.ward === nextCancelable.ward &&
          x.booking_ref === nextCancelable.booking_ref;

        const already = out.some(match);
        if (already) {
          out = out.map(x => (match(x) ? Object.assign({}, x, { canCancel: true }) : x));
        } else {
          out.push(Object.assign({}, nextCancelable, { canCancel: true }));
        }
      }
    } else {
      // No run-late cards → only show cancel if the nearest future shift is today/tomorrow
      out = nextCancelable ? [Object.assign({}, nextCancelable, { canCancel: true })] : [];
    }

    return out;
  } catch (e) {
    return [];
  }
}


/**
 * Register a trigger's creation timestamp in Script Properties so we can later
 * enforce a hard wall-clock cap even if we lose the alert reference.
 */
/**
 * Register a trigger UID with metadata so we can safely cancel per-alert later.
 * Flexible signature:
 *   _registerTrigger(uid, alert_id, handler)
 *   _registerTrigger(uid, { alert_id, handler, created_at, note })
 *   (legacy) _registerTrigger(uid, isoString)  // will be wrapped as {created_at: iso}
 */
function _registerTrigger(uid, a, b) {
  try {
    if (!uid) return;

    // Build meta flexibly
    let meta;
    if (typeof a === 'object' && a !== null) {
      meta = Object.assign({ created_at: _nowIso() }, a);
    } else if (typeof a === 'string' && typeof b === 'string') {
      meta = { alert_id: a, handler: b, created_at: _nowIso() };
    } else if (typeof a === 'string' && !b) {
      // Back-compat path: previous code passed an ISO timestamp as 2nd arg
      meta = { created_at: a };
    } else {
      meta = { created_at: _nowIso() };
    }

    // Normalize fields
    meta.alert_id = String(meta.alert_id || '');
    meta.handler  = String(meta.handler  || '');
    meta.created_at = String(meta.created_at || _nowIso());

    const sp = PropertiesService.getScriptProperties();
    sp.setProperty('EMERG_TRIG_' + uid, JSON.stringify(meta));
  } catch (_) {}
}

/** Remove a trigger from the registry (script properties) by UID. */
function _unregisterTrigger(uid) {
  try {
    if (!uid) return;
    const sp = PropertiesService.getScriptProperties();
    sp.deleteProperty('EMERG_TRIG_' + uid);
  } catch (_) {}
}

/** (Optional helper) Read back registered trigger meta by UID. */
function _getRegisteredTrigger(uid) {
  try {
    if (!uid) return null;
    const sp = PropertiesService.getScriptProperties();
    const raw = sp.getProperty('EMERG_TRIG_' + uid);
    if (!raw) return null;
    try { return JSON.parse(raw); } catch (_) { return { created_at: String(raw) }; } // legacy string
  } catch (_) { return null; }
}

/**
 * Ensure there is exactly one stepper trigger for this alert.
 * - If alert.stepper_trigger_id exists and still present → reuse it.
 * - Else create a new trigger, store its uniqueId on the alert, and register it.
 */
function _ensureStepperTrigger(alert_id) {
  let existingById = null;
  let existingUid  = '';
  try {
    const alert = _emergencyFindById(alert_id);
    const haveUid = alert && alert.stepper_trigger_id;
    const triggers = ScriptApp.getProjectTriggers() || [];
    if (haveUid) {
      existingById = triggers.find(t => t.getUniqueId && t.getUniqueId() === alert.stepper_trigger_id);
      if (existingById) {
        existingUid = existingById.getUniqueId ? existingById.getUniqueId() : '';
        // Re-register in case registry was lost — bind UID → alert_id + handler
        _registerTrigger(existingUid, alert_id, 'emergencyCallStepper');  // ← changed
        return existingById;
      }
    }
  } catch (_) {}

  // ── Create a fresh trigger (per-alert stepper) ──
  const trig = ScriptApp.newTrigger('emergencyCallStepper')
    .timeBased()
    .everyMinutes(1)
    .create();

  const uid = (trig.getUniqueId ? trig.getUniqueId() : '');
  _emergencyUpdateById(alert_id, { stepper_trigger_id: uid });

  // Bind UID → alert_id + handler for precise cancellation later
  _registerTrigger(uid, alert_id, 'emergencyCallStepper');               // ← changed

  try {
    _log('EMERG', `_ensureStepperTrigger: created stepper trigger (uid=${uid}) for alert ${alert_id}`);
  } catch (_) {}

  return trig;
}


/**
 * Defensive canceller:
 * - Deletes triggers by stored IDs AND by handler names (emergencyEscalate/emergencyCallStepper).
 * - Unregisters IDs from the registry.
 * - Clears stored trigger IDs on the alert.
 * NOTE: This DELETES triggers; Apps Script has no "disable" — delete is permanent.
 */
function _cancelTriggersForAlert(alert) {
  try {
    if (!alert || !alert.alert_id) return;

    const targetAlertId = String(alert.alert_id);
    const wantIds = new Set(
      [alert.escalation_trigger_id, alert.stepper_trigger_id].filter(Boolean)
    );

    const allowedHandlers = new Set(['emergencyEscalate', 'emergencyCallStepper']);
    const REG_PREFIX = 'EMERG_TRIG_';

    const sp = PropertiesService.getScriptProperties();
    const allProps = sp.getProperties(); // bulk read for perf
    const triggers = ScriptApp.getProjectTriggers() || [];

    // Diagnostics
    const deletedUids = [];
    const inspected = [];               // per-trigger inspection records
    const reasonsSkipped = [];          // [{uid, reason, meta, handler}]
    const wantedButMissing = new Set(wantIds); // remove when seen
    let deleted = 0;
    let deletedForThisAlert = 0;

    _log('EMERG', 'cancelTriggers:scan_start', {
      alert_id: targetAlertId,
      want_ids: Array.from(wantIds),
      total_triggers: triggers.length
    });

    for (const t of triggers) {
      try {
        const uid = (t.getUniqueId && t.getUniqueId()) || '';
        if (!uid) continue;

        const handler = (t.getHandlerFunction && t.getHandlerFunction()) || '';
        const regRaw = allProps[REG_PREFIX + uid] || '';
        let meta = null;
        if (regRaw) { try { meta = JSON.parse(regRaw); } catch (_) {} }

        let shouldDelete = false;
        let reason = '';

        // Fast path: explicit match to the IDs held on the alert row
        if (wantIds.has(uid)) {
          shouldDelete = true;
          reason = 'EXPLICIT_MATCH';
        } else {
          // Fallback: consult the registry to confirm ownership
          if (meta && String(meta.alert_id) === targetAlertId) {
            if (!meta.handler || allowedHandlers.has(String(meta.handler))) {
              shouldDelete = true;
              reason = 'REGISTRY_OWNED_BY_ALERT';
            } else {
              reasonsSkipped.push({
                uid, handler, meta,
                reason: 'REGISTRY_OWNED_BUT_HANDLER_NOT_ALLOWED'
              });
              reason = 'REGISTRY_OWNED_BUT_HANDLER_NOT_ALLOWED';
            }
          } else if (meta && String(meta.alert_id) !== targetAlertId) {
            reasonsSkipped.push({
              uid, handler, meta,
              reason: 'REGISTRY_OWNED_BY_DIFFERENT_ALERT'
            });
            reason = 'REGISTRY_OWNED_BY_DIFFERENT_ALERT';
          } else if (!meta && allowedHandlers.has(handler)) {
            // Conservative: we don’t delete if owner is unknown, even if handler matches.
            reasonsSkipped.push({
              uid, handler, meta: null,
              reason: 'NO_REGISTRY_META_HANDLER_MATCHED_BUT_UNOWNED'
            });
            reason = 'NO_REGISTRY_META_HANDLER_MATCHED_BUT_UNOWNED';
          } else {
            reasonsSkipped.push({
              uid, handler, meta: meta || null,
              reason: 'NOT_MATCHED_TO_ALERT'
            });
            reason = 'NOT_MATCHED_TO_ALERT';
          }
        }

        inspected.push({ uid, handler, meta, shouldDelete, reason });

        if (shouldDelete) {
          // bookkeeping before delete
          wantedButMissing.delete(uid);

          _log('EMERG', 'cancelTriggers:delete_trigger', {
            uid, handler, reason, registry_meta: meta || null
          });

          ScriptApp.deleteTrigger(t);
          deleted++;
          deletedUids.push(uid);

          // consider it "for this alert" iff we had an explicit match or the registry proved ownership
          if (reason === 'EXPLICIT_MATCH' || reason === 'REGISTRY_OWNED_BY_ALERT') {
            deletedForThisAlert++;
          }

          // tidy the registry
          try { _unregisterTrigger(uid); } catch (_) {}
        } else {
          // ✅ use the local reason we computed for THIS trigger
          _log('EMERG', 'cancelTriggers:skip_trigger', { uid, handler, reason });
        }
      } catch (inner) {
        _log('EMERG', 'cancelTriggers:inspect_error', { err: String(inner) });
      }
    }

    // Only clear columns on the alert row if we actually removed at least one of its triggers
    if (deletedForThisAlert > 0) {
      try {
        _emergencyUpdateById(targetAlertId, {
          escalation_trigger_id: '',
          stepper_trigger_id: ''
        });
        _log('EMERG', 'cancelTriggers:cleared_alert_trigger_columns', {
          alert_id: targetAlertId,
          deletedForThisAlert
        });
      } catch (e) {
        _log('EMERG', 'cancelTriggers:clear_columns_error', { err: String(e) });
      }
    } else {
      // Surface helpful diagnostics if we didn’t delete the alert’s own triggers
      _log('EMERG', 'cancelTriggers:no_deletes_for_alert', {
        alert_id: targetAlertId,
        wanted_but_missing: Array.from(wantedButMissing)
      });
    }

    // Final summary (include a capped sample of inspected for deep dives)
    const INSPECT_SAMPLE_LIMIT = 20;
    _log('EMERG', `_cancelTriggersForAlert: deleted=${deleted} for alert ${targetAlertId}`, {
      deleted_uids: deletedUids,
      deleted_for_this_alert: deletedForThisAlert,
      wanted_ids: Array.from(wantIds),
      wanted_but_missing: Array.from(wantedButMissing),
      skipped: reasonsSkipped,
      inspected_count: inspected.length,
      inspected_sample: inspected.slice(0, INSPECT_SAMPLE_LIMIT)
    });

  } catch (e) {
    _log('EMERG', `_cancelTriggersForAlert error`, { err: String(e && e.message || e) });
  }
}
function _log(routeOrEvent, eventOrData, dataObj) {
  // NO-OP: logging disabled. Original implementation kept below for reference.
  /*
  try {
    // Ensure sheet exists with standard headers
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName('Logs');
    if (!sh) {
      sh = ss.insertSheet('Logs');
      sh.getRange(1,1,1,6).setValues([[
        'Timestamp','ReqId','Route','Event','Action','DataJSON'
      ]]);
      sh.setFrozenRows(1);
    }

    // Resolve args (overloaded)
    let route = 'app';
    let event = '';
    let data  = {};
    if (arguments.length === 1) {
      event = String(routeOrEvent || '');
    } else if (arguments.length === 2) {
      route = String(routeOrEvent || '');
      if (eventOrData && typeof eventOrData === 'object') {
        data = eventOrData || {};
      } else {
        event = String(eventOrData || '');
      }
    } else {
      route = String(routeOrEvent || '');
      event = String(eventOrData || '');
      data  = dataObj || {};
    }

    // Timestamp & context
    const tz = (typeof _P === 'function' && _P() && _P().TZ) ? _P().TZ : 'Europe/London';
    const stamp = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
    const reqId = Utilities.getUuid();
    const action = (typeof __action !== 'undefined') ? String(__action || '') : '';

    // Append row
    sh.appendRow([
      stamp,
      reqId,
      route,
      event,
      action,
      JSON.stringify(data)
    ]);
  } catch (e) {
    // Never throw from logger
    try { Logger.log('[_log] failed: ' + (e && e.message || e)); } catch (_) {}
  }
  */
}
function logminimal(routeOrEvent, eventOrData, dataObj) {
  // Logging disabled — no-op.
  /*
  try {
    // Ensure sheet exists with standard headers
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName('Logs');
    if (!sh) {
      sh = ss.insertSheet('Logs');
      sh.getRange(1, 1, 1, 6).setValues([[
        'Timestamp', 'ReqId', 'Route', 'Event', 'Action', 'DataJSON'
      ]]);
      sh.setFrozenRows(1);
    }

    // Resolve args (overloaded)
    let route = 'app';
    let event = '';
    let data = {};
    if (arguments.length === 1) {
      event = String(routeOrEvent || '');
    } else if (arguments.length === 2) {
      route = String(routeOrEvent || '');
      if (eventOrData && typeof eventOrData === 'object') {
        data = eventOrData || {};
      } else {
        event = String(eventOrData || '');
      }
    } else {
      route = String(routeOrEvent || '');
      event = String(eventOrData || '');
      data = dataObj || {};
    }

    // Timestamp & context
    const tz = (typeof _P === 'function' && _P() && _P().TZ) ? _P().TZ : 'Europe/London';
    const stamp = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
    const reqId = (typeof __RID !== 'undefined' ? __RID : Utilities.getUuid());
    const action = (typeof __action !== 'undefined') ? String(__action || '') : '';

    // Append row
    sh.appendRow([
      stamp,
      reqId,
      route,
      event,
      action,
      JSON.stringify(data)
    ]);
  } catch (e) {
    // Never throw from logger
    try { Logger.log('[logminimal] failed: ' + (e && e.message || e)); } catch (_) {}
  }
  */
}





/**
 * Start escalation:
 * - Places first TTS call.
 * - Seeds queue/index/next call time/map.
 * - Stamps stepper_started_at_iso (for wall-clock cap).
 * - Ensures a per-alert minute stepper trigger exists (and registers it).
 */

// ─────────────────────────────────────────────────────────────────────────────
// NEW: Priority + windows + whole-date blackouts respected for CALLS
// ─────────────────────────────────────────────────────────────────────────────





/**
 * Minute stepper:
 * - Enforces both wall-clock cap and attempts-based cap using TIMEOUT_ALERTS_MIN.
 * - Robustly cancels & unregisters triggers on ACK, empty queue, exhausted, or timeout.
 * - Finally-block ensures no leaked triggers when terminal conditions are met.
 */
function emergencyCallStepper(e) {
  // ─────────────────────────────
  // Lightweight logger to Logs sheet + Stackdriver
  // ─────────────────────────────
  function _logStep(msg, data) {
  try {
    var suffix = '';
    if (data !== undefined) {
      try {
        var s = JSON.stringify(data);
        suffix = ' ' + (s.length > 1200 ? s.slice(0, 1200) + '…' : s);
      } catch (_) {
        suffix = ' ' + String(data);
      }
    }
    try { _log('EMERG_STEPPER', msg + suffix); } catch (_) {}
    try { Logger.log('[EMERG_STEPPER] ' + msg + suffix); } catch (_) {}
  } catch (_) {}
}


  const alertId = _getTriggerSourceAlertId_(e) || (e && e.alert_id);
  if (!alertId) { _logStep('No alertId on stepper invocation', { haveE: !!e }); return; }

  let alert = _emergencyFindById(alertId);
  if (!alert) { _logStep('Alert not found for stepper', { alertId }); return; }

  const cfg = _getEmergencyProps();
  const TIMEOUT_MIN = Number(cfg.TIMEOUT_ALERTS_MIN || 0) || 45;
  let terminalStop = false;

  _logStep('Stepper tick: loaded', {
    alertId,
    status: alert.status,
    acknowledged: String(alert.acknowledged),
    call_index: alert.call_index,
    call_next_at_iso: alert.call_next_at_iso || '',
    queue_len: (alert.call_queue || []).length,
    TIMEOUT_MIN,
    CALL_GAP_MIN: cfg.CALL_GAP_MIN
  });

  // Helper to resolve responder name for the current callee
  function resolveResponderName(msisdn447) {
    try {
      const list = _getEmergencyContacts() || [];
      const hit = list.find(c => String(c.mobile447 || '').trim() === String(msisdn447));
      if (!hit) return 'there';
      const dn = hit.display_name ||
                 [hit.firstname, hit.surname].filter(Boolean).join(' ').trim() ||
                 hit.name ||
                 hit.fullname ||
                 null;
      if (dn && String(dn).trim()) return String(dn).trim();
      const s = String(msisdn447 || '');
      return s ? (s.slice(0,3) + ' ' + s.slice(3,7) + ' ' + s.slice(7)) : 'there';
    } catch (_) { return 'there'; }
  }

  // Same "when" derivation as in escalate
  function deriveSpeakableWhenLabel(a) {
  try {
    const tz = (cfg && cfg.TZ) || 'Europe/London';

    const now = new Date();
    const todayYmd    = Utilities.formatDate(now, tz, 'yyyy-MM-dd');
    const tomorrowYmd = Utilities.formatDate(new Date(now.getTime() + 86400000), tz, 'yyyy-MM-dd');

    // Prefer explicit ISO start/end if present (either flat or under a.shift)
    const startIso = a.shift_start_iso || a.startAtIso || (a.shift && a.shift.startAtIso) || null;
    const endIso   = a.shift_end_iso   || a.endAtIso   || (a.shift && a.shift.endAtIso)   || null;

    // Compute start/end YMDs in TZ. If end is missing, assume same-day (long day).
    const start    = startIso ? new Date(startIso) : null;
    const end      = endIso   ? new Date(endIso)   : null;
    const startYmd = start ? Utilities.formatDate(start, tz, 'yyyy-MM-dd')
                           : (a.shift_ymd || (a.shift && a.shift.ymd) || null);
    const endYmd   = end   ? Utilities.formatDate(end,   tz, 'yyyy-MM-dd')
                           : startYmd;

    // Core rule: crosses midnight → NIGHT; same calendar day → LONG DAY
    const crossesMidnight = !!(startYmd && endYmd && endYmd !== startYmd);

    if (startYmd === todayYmd) {
      return crossesMidnight ? 'tonight' : 'today long day';
    }
    if (startYmd === tomorrowYmd) {
      return crossesMidnight ? 'tomorrow night' : 'tomorrow long day';
    }

    // Fallback: explicit date label, annotated with night/long day when we can
    const dateLabel =
      a.date_label ||
      (a.shift_ymd && typeof _formatDateLabel === 'function' ? _formatDateLabel(a.shift_ymd, tz) : '') ||
      (a.shift && a.shift.ymd && typeof _formatDateLabel === 'function' ? _formatDateLabel(a.shift.ymd, tz) : '') ||
      '';

    if (dateLabel) {
      return crossesMidnight ? (dateLabel + ' night') : (dateLabel + ' long day');
    }

    return 'the scheduled time';
  } catch (_) {
    return 'the scheduled time';
  }
}

  try {
    // Stop immediately if already ACKed
    if (String(alert.acknowledged) === 'true' || alert.acknowledged === true) {
      _logStep('Stop: already ACKed — cancelling triggers and clearing id');
      _cancelTriggersForAlert(alert); // ensure stepper trigger deleted
      _emergencyUpdateById(alertId, { stepper_trigger_id: '' });
      try { _eaAppendLog_(alertId, 'Stepper stopped (ACKed)'); } catch (_) {}
      terminalStop = true;
      return;
    }

    const queue = alert.call_queue || [];
    const idx   = Number(alert.call_index || 0);
    const nextIso = alert.call_next_at_iso;

    if (!queue.length) {
      _logStep('Stop: empty queue — cancelling triggers and clearing id');
      _cancelTriggersForAlert(alert);
      _emergencyUpdateById(alertId, { stepper_trigger_id: '' });
      try { _eaAppendLog_(alertId, 'Stepper: empty queue → stopped'); } catch (_) {}
      terminalStop = true;
      return;
    }

    // ── Hard wall-clock cap (independent of attempts)
    (function enforceWallClockCap() {
      try {
        var started = alert.stepper_started_at_iso && new Date(alert.stepper_started_at_iso).getTime();
        if (started) {
          var elapsedMin = (Date.now() - started) / 60000;
          _logStep('Wall-clock check', { started_iso: alert.stepper_started_at_iso, elapsed_min: Math.floor(elapsedMin), limit_min: TIMEOUT_MIN });
          if (elapsedMin >= TIMEOUT_MIN) {
            _logStep('Stop: wall-clock timeout — cancelling triggers, marking TIMEOUT');
            _cancelTriggersForAlert(alert);
            _emergencyUpdateById(alertId, {
              status: 'TIMEOUT',
              call_next_at_iso: '',
              stepper_stopped_at_iso: _nowIso(),
              stepper_trigger_id: ''
            });
            try { _eaAppendLog_(alertId, 'Stepper: hard wall-clock timeout (' + Math.floor(elapsedMin) + ' min; limit ' + TIMEOUT_MIN + ' min) → stopped'); } catch (_) {}
            terminalStop = true;
            throw new Error('WALL_CLOCK_TIMEOUT'); // ensure we exit this run
          }
        } else {
          _logStep('Wall-clock start missing — skipping elapsed check');
        }
      } catch (eWC) {
        _logStep('Wall-clock cap branch threw (expected on timeout)', { err: String(eWC && eWC.message || eWC) });
        throw eWC;
      }
    })();

    if (terminalStop) return;

    // ---- Attempts-based cap (derived from TIMEOUT_MIN and CALL_GAP_MIN)
    var GAP = Number(cfg.CALL_GAP_MIN || 2);
    if (!(GAP > 0)) GAP = 2; // safety
    var MAX_ATTEMPTS = Math.max(1, Math.ceil(TIMEOUT_MIN / GAP));
    _logStep('Attempts cap check', { idx, queue_len: queue.length, GAP, MAX_ATTEMPTS });

    if (idx >= MAX_ATTEMPTS) {
      _logStep('Stop: attempts cap reached — cancelling triggers, marking TIMEOUT');
      _cancelTriggersForAlert(alert); // deletes stepper trigger
      _emergencyUpdateById(alertId, {
        status: 'TIMEOUT',
        call_next_at_iso: '',
        stepper_stopped_at_iso: _nowIso(),
        stepper_trigger_id: ''
      });
      try { _eaAppendLog_(alertId, `Stepper: timed out after ~${TIMEOUT_MIN} minutes (attempts ${idx}/${queue.length}); giving up`); } catch (_) {}
      terminalStop = true;
      return;
    }
    // --------------------------------------------------------------------------

    if (idx >= queue.length) {
      _logStep('Stop: exhausted contacts — cancelling triggers and clearing id', { idx, queue_len: queue.length });
      _cancelTriggersForAlert(alert);
      _emergencyUpdateById(alertId, { stepper_trigger_id: '' });
      try { _eaAppendLog_(alertId, 'Stepper: exhausted contacts → stopped'); } catch (_) {}
      terminalStop = true;
      return;
    }

    // Respect next scheduled time
    if (nextIso && Date.now() < new Date(nextIso).getTime()) {
      _logStep('Skip: not due yet', { now_iso: _nowIso(), next_due_iso: nextIso, ms_until: (new Date(nextIso).getTime() - Date.now()) });
      return;
    }

    const to  = queue[idx];
    const responderName = resolveResponderName(to);
    const speakableWhen = deriveSpeakableWhenLabel(alert);

    const tts = _composeTts_(alert, cfg, {
      responderName,
      speakableWhenLabel: speakableWhen,
      callGapMin: GAP
    });

    _logStep('Placing TTS call', { to, idx_plus_one: idx + 1, of: queue.length, require_input: true, dtmf_ack: '1' });

    let callRes = {};
    try {
      // PASS alertId so the webhook can bind the ACK deterministically
      callRes = _clickSendPlaceTTSCall(to, tts, { require_input: true, dtmf_ack: '1', alert_id: alertId }) || {};
      _logStep('TTS call placed', { to, call_id: callRes.call_id || '(none)' });
      try { _eaAppendLog_(alertId, `Stepper: placed call to ${to} (index ${idx + 1}/${queue.length})`); } catch (_) {}
    } catch (callErr) {
      _logStep('TTS call failed (continuing to schedule next)', { to, error: String(callErr && callErr.message || callErr) });
    }

    // Update per-call map
    const map = alert.call_map || {};
    map[callRes.call_id || Utilities.getUuid()] = { to, placed_at_iso: _nowIso() };

    // Schedule next attempt
    const nextAt = new Date(Date.now() + GAP * 60 * 1000);

    _emergencyUpdateById(alertId, {
      call_index: idx + 1,
      call_next_at_iso: _toIsoZ_(nextAt),
      call_map_json: JSON.stringify(map)
    });

    _logStep('Stepper state advanced', {
      new_call_index: idx + 1,
      next_call_due_iso: _toIsoZ_(nextAt),
      call_map_size: Object.keys(map).length
    });
    try { _eaAppendLog_(alertId, `Stepper: called ${to} (index ${idx + 1}/${queue.length})`); } catch (_) {}

  } finally {
    // Ensure we don't leak triggers in terminal states
    try {
      // Refresh alert (status might have changed above)
      alert = _emergencyFindById(alertId) || alert;
      if (!alert) { _logStep('Finally: alert vanished after run', { alertId }); return; }

      const isAcked   = (String(alert.acknowledged) === 'true' || alert.acknowledged === true);
      const isTimeout = (String(alert.status || '').toUpperCase() === 'TIMEOUT');
      const noQueue   = !(alert.call_queue && alert.call_queue.length);

      _logStep('Finally: terminal check', { terminalStop, isAcked, isTimeout, noQueue });

      if (terminalStop || isAcked || isTimeout || noQueue) {
        _logStep('Finally: cancelling triggers & clearing id (defensive)');
        _cancelTriggersForAlert(alert);
        _emergencyUpdateById(alert.alert_id, { stepper_trigger_id: '' });
      } else {
        _logStep('Finally: still active — leaving trigger in place');
      }
    } catch (finErr) {
      _logStep('Finally: cleanup error (ignored)', { error: String(finErr && finErr.message || finErr) });
    }
  }
}




/**
 * Nightly janitor:
 * - Deletes (not disables) ANY project triggers for emergency handlers that are past TIMEOUT_ALERTS_MIN
 *   according to our registry (EMERG_TRIG_<uid> → ISO).
 * - If a trigger for those handlers has no registry entry, we delete it as a defensive cleanup.
 *   (Safer for leak prevention; remove this behavior if you want to keep unregistered ones.)
 * - Clears registry entries for any deleted triggers.
 */
function emergencyTriggerJanitor() {
  // Cleans up stale emergency triggers using the JSON registry written by _registerTrigger.
  // Handles both NEW (JSON) and LEGACY (plain ISO string) registry formats.
  const cfg = _getEmergencyProps();
  const TIMEOUT_MIN = Number(cfg.TIMEOUT_ALERTS_MIN || 0) || 45;
  const cutoffMs = TIMEOUT_MIN * 60 * 1000;

  const sp = PropertiesService.getScriptProperties();
  const all = ScriptApp.getProjectTriggers() || [];
  const allowedHandlers = new Set(['emergencyCallStepper', 'emergencyEscalate']);

  let deleted = 0, kept = 0;

  for (const t of all) {
    try {
      const fn = (t.getHandlerFunction && t.getHandlerFunction()) || '';
      if (!allowedHandlers.has(fn)) { kept++; continue; }

      const uid = (t.getUniqueId && t.getUniqueId()) || '';
      const key = uid ? ('EMERG_TRIG_' + uid) : '';
      const raw = key ? sp.getProperty(key) : null;

      // Parse registry entry:
      // - NEW format: JSON with { alert_id, handler, created_at, ... }
      // - LEGACY format: plain ISO string (e.g., "2025-09-28T14:03:00Z")
      let createdAtIso = null;
      if (raw) {
        try {
          const meta = JSON.parse(raw); // NEW format
          createdAtIso =
            (meta && (meta.created_at || meta.created_iso || meta.created || meta.ts)) || null;
          // If JSON parsed but has no timestamp, treat as "fresh" and keep.
        } catch (_) {
          // LEGACY: raw is a plain ISO timestamp string
          createdAtIso = raw;
        }
      }

      if (createdAtIso) {
        const startedMs = Date.parse(createdAtIso);
        if (!Number.isNaN(startedMs) && (Date.now() - startedMs) >= cutoffMs) {
          ScriptApp.deleteTrigger(t);       // hard delete
          deleted++;
          if (uid) _unregisterTrigger(uid); // drop registry entry
          continue;
        }
        // Fresh enough → keep
        kept++;
        continue;
      }

      // No registry info at all → delete defensively to prevent leaks.
      ScriptApp.deleteTrigger(t);
      deleted++;
      if (uid) _unregisterTrigger(uid);

    } catch (_) {
      // Swallow and continue with next trigger
    }
  }

  try {
    _log('EMERG',
      `emergencyTriggerJanitor: deleted=${deleted}, kept=${kept}, timeout_min=${TIMEOUT_MIN}`);
  } catch (_) {}
}


/**
 * Quick test sender for WATI template: `appemergencyurl`
 * Sends to 447545970472 with all variables populated and {{1}} set to the alert_id.
 * Run this from the Apps Script editor: Run ▶ testSend_AppEmergencyUrl
 */
function testSend_AppEmergencyUrl() {
  // --- Try to read existing config; fall back to placeholders you can edit ---
  var cfg = (typeof _getEmergencyProps === 'function') ? _getEmergencyProps() : {};
  var TOKEN   = cfg.WATI_TOKEN || (typeof WATI_TOKEN !== 'undefined' ? WATI_TOKEN : ''); // ⚠️ must be set
  var TENANT  = cfg.WATI_TENANT || '601205';  // change if different
  var NAMESPACE = cfg.TEMPLATE_NAMESPACE || cfg.WATI_TEMPLATE_NAMESPACE || ''; // optional on some accounts
  var TZ      = cfg.TZ || 'Europe/London';

  if (!TOKEN) throw new Error('WATI token missing. Set _getEmergencyProps().WATI_TOKEN or global WATI_TOKEN.');

  // --- Test data (edit as you wish) ---
  var to447          = '447545970472';
  var alertId        = 'ALERT_TEST_1359';
  var candidateName  = 'Kier Arthur';
  var role           = 'RMN';
  var hospital       = 'Worthing';
  var ward           = 'Bluefin';
  var dateLabel      = Utilities.formatDate(new Date(), TZ, 'EEE d MMMM yyyy');
  var timeRangeLabel = '0730-2000hrs';
  var shiftType      = 'LONG DAY';
  var issueType      = 'Running late';
  var etaLabel       = '8am';
  var reasonText     = 'Car broke down';

  // {{1}} in your CTA URL — you said you want it to carry the alert_id
  var ctaVar1 = alertId;

  // --- Build WATI v2 payload ---
  var url = 'https://eu-api.wati.io/' + TENANT + '/api/v2/sendTemplateMessages';
  var payload = {
    // Namespace can be omitted on some setups; include if your account requires it
    template_namespace: NAMESPACE || undefined,
    template_name: 'appemergencyurl',
    broadcast_name: 'AppEmergencyURL_Test_' + Utilities.formatDate(new Date(), TZ, 'yyyyMMdd_HHmmss'),
    receivers: [{
      whatsappNumber: to447,
      customParams: [
        { name: 'Candidate_name',           value: candidateName },
        { name: 'Role',                     value: role },
        { name: 'Hospital',                 value: hospital },
        { name: 'Ward',                     value: ward },
        { name: 'Date_label',               value: dateLabel },
        { name: 'Time_range_label',         value: timeRangeLabel },
        { name: 'Shift_type',               value: shiftType },
        { name: 'Issue_type',               value: issueType },
        { name: 'Eta_or_leave_time_label',  value: etaLabel },
        { name: 'Reason_text',              value: reasonText },
        { name: 'alert_id',                 value: alertId },

        // 👇 This fills the CTA URL placeholder {{1}} in your approved template
        { name: '1',                        value: ctaVar1 }
      ]
    }]
  };

  // --- Fire request ---
  var resp = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
    headers: { Authorization: TOKEN }
  });

  // --- Inspect result in logs ---
  var http = resp.getResponseCode();
  var text = resp.getContentText();
  var json = (function(){ try { return JSON.parse(text); } catch(_) { return null; } })();

  Logger.log('WATI send http=%s', http);
  Logger.log('WATI response (preview)=%s', text.slice(0, 2000));

  // If you want to see message IDs (when returned) for correlation:
  var ids = json && (json.data || json.messageIds || json.ids);
  if (ids) Logger.log('WATI message ids=%s', JSON.stringify(ids));

  return { http: http, json: json };
}

/**
 * Seed N fake emergency alerts that are ALL CANNOT_ATTEND (outstanding + active).
 * Usage: addFakeCannotAttendAlerts({ count: 3, msisdn: '7545970472' })
 */
function addFakeCannotAttendAlerts(opts) {
  opts = opts || {};
  var COUNT  = Number(opts.count || 3);
  var MSISDN = String(opts.msisdn || '7545970472'); // your test number
  var NOWISO = (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString();

  var idx = _eaIndexMap_(); // { sh, m }
  var sh  = idx.sh, m = idx.m;

  function setIf(colKey, rowIndex, value) {
    if (m.hasOwnProperty(colKey) && m[colKey] >= 0) {
      sh.getRange(rowIndex, m[colKey] + 1).setValue(value);
    }
  }

  var hospitals = [
    { hospital: 'St Richards Hospital', ward: 'Acute Ward' },
    { hospital: 'Royal Sussex County',  ward: 'ICU' },
    { hospital: 'Princess Royal Hospital', ward: 'Paediatrics' },
    { hospital: 'Worthing Hospital', ward: 'AAU' }
  ];
  var roles = ['RMN', 'RGN', 'HCA'];
  var reasons = ['Unwell', 'Family emergency', 'Transport failure', 'Unexpected childcare'];

  var tz = (_P() && _P().TZ) || 'Europe/London';

  for (var i = 0; i < COUNT; i++) {
    var rowIndex = sh.getLastRow() + 1;
    var id       = Utilities.getUuid();

    var h  = hospitals[i % hospitals.length];
    var rl = roles[i % roles.length];
    var reason = reasons[i % reasons.length];

    // Make them look current/imminent
    var start = new Date();
    start.setMinutes(start.getMinutes() + (i * 15)); // stagger into near future
    var end = new Date(start.getTime() + (11.5 * 60 * 60 * 1000)); // ~11.5h

    var ymd = Utilities.formatDate(start, tz, 'yyyy-MM-dd');
    var dateLabel  = Utilities.formatDate(start, tz, 'EEEE d MMMM yyyy'); // e.g., Sunday 21 September 2025
    var startLabel = Utilities.formatDate(start, tz, 'HH:mm') + 'hrs';
    var endLabel   = Utilities.formatDate(end,   tz, 'HH:mm') + 'hrs';
    var timeRange  = startLabel + '–' + endLabel;

    // Required to be "outstanding"
    setIf('alert_id',     rowIndex, id);
    setIf('acknowledged', rowIndex, false);
    setIf('status',       rowIndex, 'ACTIVE');

    // Basic identity
    setIf('created_iso',        rowIndex, NOWISO);
    setIf('updated_iso',        rowIndex, NOWISO);
    setIf('candidate_msisdn',   rowIndex, MSISDN);
    setIf('candidate_name',     rowIndex, 'Test Candidate');

    // Shift info
    setIf('shift_ymd',       rowIndex, ymd);
    setIf('shift_type',      rowIndex, (i % 2 === 0) ? 'NIGHT' : 'DAY');
    setIf('shift_start_iso', rowIndex, start.toISOString());
    setIf('shift_end_iso',   rowIndex, end.toISOString());

    // Location & role
    setIf('hospital',   rowIndex, h.hospital);
    setIf('ward',       rowIndex, h.ward);
    setIf('job_title',  rowIndex, rl);

    // Display labels
    setIf('date_label',             rowIndex, dateLabel);
    setIf('time_range_label',       rowIndex, timeRange);

    // 🚨 Only CANNOT_ATTEND
    setIf('issue_type',             rowIndex, 'CANNOT_ATTEND');
    setIf('eta_or_leave_time_label',rowIndex, 'N/A');
    setIf('reason_text',            rowIndex, reason);

    // Ensure no triggers tied
    setIf('escalation_trigger_id',  rowIndex, '');
    setIf('stepper_trigger_id',     rowIndex, '');

    // Clear ack-related fields
    setIf('ack_source',   rowIndex, '');
    setIf('ack_name',     rowIndex, '');
    setIf('ack_number',   rowIndex, '');
    setIf('ack_when_iso', rowIndex, '');

    // Optional numeric/JSON
    setIf('attempt_count', rowIndex, 0);
    setIf('meta_json',     rowIndex, '{}');

    // Optional log
    if (m.hasOwnProperty('log_json') && m['log_json'] >= 0) {
      var logArr = [{ ts: NOWISO, msg: 'Created fake CANNOT_ATTEND emergency record for testing' }];
      sh.getRange(rowIndex, m['log_json'] + 1).setValue(JSON.stringify(logArr));
    }
  }

  return { ok: true, added: COUNT, type: 'CANNOT_ATTEND', at: NOWISO };
}


/**
 * Menu entrypoint (you already added this, included here for completeness).
 * Adds a top-level item to open the Emergency Contacts UI.
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Availability API')
    .addItem('Emergency Contacts Settings', 'showEmergencyContactsUI')
    .addSeparator()
    .addItem('Update tokens', 'updateTokens')
    .addSeparator()
    .addSubMenu(
      ui.createMenu('Change Messages')
        .addItem('Apple Welcome on First Load',   'openMessageEditor_WelcomeApple')
        .addItem('Android Welcome on First Load', 'openMessageEditor_WelcomeAndroid')
        .addItem('Unknown Device Welcome on First Load', 'openMessageEditor_WelcomeUnknown')
        .addItem('Welcome Email (New User + Reset)', 'openMessageEditor_WelcomeEmail')
        .addItem('Alert Message', 'openMessageEditor_Alert')
        .addSeparator()
        .addItem('Accommodation Contacts', 'openMessageEditor_Accommodation')
        .addItem('Hospital Addresses',     'openMessageEditor_Hospital')
        .addItem('Timesheet email message','openMessageEditor_Timesheet')
    )
    .addSeparator()
    .addItem('Script Properties…', 'spManageScriptProperties') // <-- added
    .addToUi();
}


/**
 * UI entry point — opens the main Emergency Contacts modal (list/sort/add/edit/delete).
 * The HTML file name must be 'EmergencyContactsModal'.
 */
function showEmergencyContactsUI() {
  const html = HtmlService.createHtmlOutputFromFile('EmergencyContactsModal')
    .setTitle('Emergency Contacts')
    .setWidth(1100)
    .setHeight(700);
  SpreadsheetApp.getUi().showModelessDialog(html, 'Emergency Contacts');
  // Clear transient UI state for a fresh session
  _setUiState_({ editorPayload: null, lastUpsert: null });
}

/**
 * Opens the Contact Details editor modal (create or edit).
 * payload: null (create) OR { index:number|null, contact:Object }
 * The HTML file name must be 'ContactDetailsModal'.
 */
function showContactEditor(payload) {
  _setUiState_({ editorPayload: payload || { index: null, contact: {} } });
  const html = HtmlService.createHtmlOutputFromFile('ContactDetailsModal')
    .setTitle('Contact Details & Availability')
    .setWidth(1000)
    .setHeight(750);
  SpreadsheetApp.getUi().showModalDialog(html, 'Contact');
}


/**
 * Lightweight per-user state to pass data between HTML dialogs.
 */
function _setUiState_(obj) {
  const cp = PropertiesService.getUserProperties();
  cp.setProperty('EMERG_UI_STATE', JSON.stringify(obj || {}));
}

function _getUiState_() {
  const cp = PropertiesService.getUserProperties();
  const raw = cp.getProperty('EMERG_UI_STATE') || '{}';
  try { return JSON.parse(raw); } catch (e) { return {}; }
}

/**
 * Modal B (editor) fetches its initial payload from here.
 * Returns { index, contact } or a default empty payload.
 */
function getEditorPayload() {
  const s = _getUiState_();
  return (s && s.editorPayload) ? s.editorPayload : { index: null, contact: {} };
}

/**
 * Modal B posts its upsert result here. We then notify Modal A (opener)
 * by injecting a tiny modeless dialog that calls window.applyContactUpsert(payload).
 */
function applyContactFromEditor(payload) {
  _setUiState_({ lastUpsert: payload, editorPayload: null });
}


function getAndClearLastUpsert() {
  const s = _getUiState_() || {};
  const payload = s.lastUpsert || null;
  _setUiState_({ editorPayload: s.editorPayload || null, lastUpsert: null });
  return payload;
}
/**
 * Modal A initial load: returns the current contacts array (raw JSON parsed).
 */
function getEmergencyContactsForUi() {
  return _readEmergencyContactsArray_();
}

/**
 * Modal A final save: validate/normalise/dedupe/merge and write to Script Properties.
 * Returns a brief summary for user feedback.
 */
function saveEmergencyContacts(list) {
  const result = _saveContacts_(Array.isArray(list) ? list : []);
  return {
    ok: true,
    message: `Saved ${result.count} contact(s). Added ${result.added}, updated ${result.updated}, deleted ${result.deleted}.`
  };
}

/* ========================================================================== */
/*                         Core read / write helpers                          */
/* ========================================================================== */

function _readEmergencyContactsArray_() {
  const P = _P();
  const raw = (P && P.EMERGENCY_CONTACTS) || '[]';
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    return [];
  }
}

function _writeEmergencyContactsArray_(arr) {
  const sp = PropertiesService.getScriptProperties();
  sp.setProperty('EMERGENCY_CONTACTS', JSON.stringify(arr));
}

/**
 * Save pipeline:
 * - Coerce & validate each incoming contact
 * - Key by normalised mobile (447XXXXXXXXX)
 * - Preserve stable order seed from the previous file for tie-breaking
 * - Sort by priority asc; ties remain stable by original file order
 */
function _saveContacts_(incoming) {
  const before = _readEmergencyContactsArray_();

  // stable order seed = existing keys in file order
  const byKey = {};
  const order = [];
  before.forEach(c => {
    const k = _normKey_(c.mobile);
    if (k && !(k in byKey)) {
      byKey[k] = c;
      order.push(k);
    }
  });

  let added = 0, updated = 0;

  // Apply incoming (UI intent wins: simple replace)
  incoming.forEach(c => {
    const clean = _cleanContact_(c);
    const k = _normKey_(clean.mobile);
    if (!k) return; // invalid mobile -> skip
    if (!(k in byKey)) {
      byKey[k] = clean;
      order.push(k);
      added++;
    } else {
      byKey[k] = clean;
      updated++;
    }
  });

  // Deletions: keys present before but missing from incoming set
  let deleted = 0;
  const incomingKeys = {};
  incoming.forEach(c => {
    const k = _normKey_(c.mobile);
    if (k) incomingKeys[k] = true;
  });
  before.forEach(c => {
    const k = _normKey_(c.mobile);
    if (k && !incomingKeys[k]) deleted++;
  });

  // Build final array in seeded order, filtered to incomingKeys
  const arr = order
    .filter(k => incomingKeys[k])
    .map(k => byKey[k]);

  // Priority sort asc; ties stable by seeded order
  arr.sort((a, b) => {
    const pa = Number(a.priority || 9999), pb = Number(b.priority || 9999);
    if (pa < pb) return -1;
    if (pa > pb) return 1;
    return 0; // stable by seeded order
  });

  _writeEmergencyContactsArray_(arr);
  try { _log('EMERG_CONTACTS_SAVE', 'result', { count: arr.length, added, updated, deleted }); } catch (_) {}
  return { count: arr.length, added, updated, deleted };
}

/**
 * Normalised mobile key: returns 447XXXXXXXXX (12 digits) or '' if invalid.
 * - Accepts "07xxxxxxxxx", "+447xxxxxxxxx", "447xxxxxxxxx".
 */
function _normKey_(mobile) {
  const s = String(mobile || '').replace(/\D+/g, '');
  if (!s) return '';
  let out = s;

  // Normalize common UK mobile formats
  if (s.startsWith('0')) {
    out = '44' + s.slice(1);            // 07xxxxxxxxx -> 447xxxxxxxxx
  } else if (s.startsWith('7') && s.length === 10) {
    out = '44' + s;                     // 7xxxxxxxxx -> 447xxxxxxxxx
  }

  // Final validation on normalized string
  if (out.startsWith('447') && out.length === 12) {
    return out; // correct normalized form
  }

  return '';
}

/**
 * Coerces and validates a single contact object to the contract you defined.
 * - Trims names, mobile; default enabled true; priority number or 9999; timezone default.
 * - call_windows: only keep valid HH:MM pairs; omit entirely if effectively empty.
 * - blackouts: only valid {date} and {from,to} (inclusive); drop invalid.
 * - notes (optional) is preserved if non-empty.
 */
function _cleanContact_(c) {
  const out = {};
  out.firstname = String((c.firstname || '').trim());
  out.surname   = String((c.surname   || '').trim());
  out.mobile    = String((c.mobile    || '').trim());
  out.enabled   = (c.enabled !== false);
  out.priority  = (isFinite(Number(c.priority)) ? Math.max(1, Math.floor(Number(c.priority))) : 9999);
  out.timezone  = String(c.timezone || (_P().TZ || 'Europe/London'));

  // call_windows
  if (c.call_windows && typeof c.call_windows === 'object') {
    const cw = {};
    function validWin(w) {
      if (!w || typeof w !== 'object') return false;
      if (!/^\d{2}:\d{2}$/.test(w.start || '')) return false;
      if (!/^\d{2}:\d{2}$/.test(w.end   || '')) return false;
      if (w.start === '24:00') return false; // start cannot be 24:00
      const [hs, ms] = w.start.split(':').map(Number);
      const [he, me] = w.end.split(':').map(Number);
      if (hs < 0 || hs > 24 || ms < 0 || ms > 59 || he < 0 || he > 24 || me < 0 || me > 59) return false;
      return true; // allow overnight (start > end) and end == 24:00
    }
    const copyArr = (arr) => (Array.isArray(arr) ? arr.filter(validWin).map(w => ({ start: w.start, end: w.end })) : []);
    if (Array.isArray(c.call_windows.default)) {
      const a = copyArr(c.call_windows.default);
      if (a.length) cw.default = a;
    }
    ['mon','tue','wed','thu','fri','sat','sun'].forEach(d => {
      if (Array.isArray(c.call_windows[d])) {
        const a = copyArr(c.call_windows[d]);
        if (a.length) cw[d] = a;
      }
    });
    if (Object.keys(cw).length) out.call_windows = cw; // else omit (Any time allowed)
  }

  // blackouts (Annual Leave)
  if (Array.isArray(c.blackouts)) {
    const bl = [];
    c.blackouts.forEach(b => {
      if (b && b.date && /^\d{4}-\d{2}-\d{2}$/.test(b.date)) {
        bl.push({ date: b.date });
      } else if (
        b && b.from && b.to &&
        /^\d{4}-\d{2}-\d{2}$/.test(b.from) &&
        /^\d{4}-\d{2}-\d{2}$/.test(b.to) &&
        b.from <= b.to
      ) {
        bl.push({ from: b.from, to: b.to });
      }
    });
    if (bl.length) out.blackouts = bl;
  }

  if (c.notes) out.notes = String(c.notes);

  return out;
}
/* ========================= NEW HELPERS (Peers) ========================= */

/**
 * Compute peers for a user across previous/current/next relative to the user’s
 * anchor shift (if on-shift now, use that; else the closest within the 14-day window;
 * else return empty arrays). Best-effort and side-effect free.
 *
 * Returns:
 * {
 *   previous: [{ firstName, surnameInitial, role, msisdn07 }],
 *   current:  [{ ... }],
 *   next:     [{ ... }]
 * }
 */
function _computePeersForUser(arg) {
  // Logs-rich version to diagnose why "today's shift" isn't being picked
  // and why peers may be empty. Uses _log at each decision point.

  const safeEmpty = { previous: [], current: [], next: [] };

  // ---- tiny local helpers (no external deps) ----
  function maskMsisdn(s) {
    try {
      const m = String(s || "").replace(/\s+/g, "");
      if (!m) return "";
      return m.slice(0, 3) + "•••••" + m.slice(-4);
    } catch (_) { return ""; }
  }
  function samplePeers(list, n) {
    const L = Array.isArray(list) ? list : [];
    return L.slice(0, n).map(p => ({
      firstName: (p && (p.firstName || p.firstname)) || "",
      surnameInitial: (p && (p.surnameInitial || (String(p.surname || "").trim().charAt(0).toUpperCase()))) || "",
      role: (p && (p.role || p.jobTitle)) || "",
      msisdn_masked: maskMsisdn(p && (p.msisdn07 || p.mobile || p.msisdn || p.tel || ""))
    }));
  }
  function keyToLog(k) {
    if (!k) return null;
    return {
      ymd: k.ymd || "",
      hospital: k.hospital || "",
      ward: k.ward || "",
      shiftType: k.shiftType || k.type || ""
    };
  }
  function headersSummary(headersYmds) {
    if (!Array.isArray(headersYmds) || !headersYmds.length) return { count: 0, range: [] };
    const h = headersYmds;
    const range = h.length <= 6 ? h.slice(0) : [h[0], h[1], "…", h[h.length - 2], h[h.length - 1]];
    return { count: h.length, range: range };
  }
  function bookedMapSummary(bookedMap) {
    try {
      const keys = bookedMap ? Object.keys(bookedMap) : [];
      const out = { totalDays: keys.length, daysSample: keys.slice(0, 5) };
      // include "today" if present for easier diagnosis
      try {
        const tz = (typeof _P === 'function' && _P() && _P().TZ) ? _P().TZ : 'Europe/London';
        const today = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');
        if (bookedMap && bookedMap[today]) {
          out.today = {
            ymd: today,
            raw: {
              hospital: bookedMap[today].hospital || "",
              ward: bookedMap[today].ward || "",
              jobTitle: bookedMap[today].jobTitle || "",
              shift: bookedMap[today].shift || bookedMap[today].notes || ""
            }
          };
        }
      } catch (_) {}
      return out;
    } catch (_) { return { totalDays: 0, daysSample: [] }; }
  }

  try {
    // ───────────────────────── adapter path ─────────────────────────
    if (arg && typeof arg === 'object' && arg.ymd && arg.hospital && arg.ward) {
      _log('DNA', 'peers:compute:start', { mode: 'adapter', key: keyToLog(arg) });

      const res = (typeof _computePeersFor === 'function') ? _computePeersFor(arg) : null;
      const out = (res && typeof res === 'object')
        ? { previous: res.previous || [], current: res.current || [], next: res.next || [] }
        : safeEmpty;

      _log('DNA', 'peers:compute:adapter:result', {
        key: keyToLog(arg),
        previous_len: (out.previous || []).length,
        current_len:  (out.current  || []).length,
        next_len:     (out.next     || []).length,
        current_sample: samplePeers(out.current, 3)
      });

      return out;
    }

    // ───────────────────────── rich-context path ─────────────────────────
    const { ss, user, headersYmds, bookedMap } = (arg || {});
    const msisdnMasked = maskMsisdn(user && (user.tel || user.msisdn || user.mobile));

    if (!ss || !user || !Array.isArray(headersYmds)) {
      _log('DNA', 'peers:compute:bad_args', {
        hasSS: !!ss,
        hasUser: !!user,
        hasHeaders: Array.isArray(headersYmds),
        msisdn_masked: msisdnMasked
      });
      return safeEmpty;
    }

    _log('DNA', 'peers:compute:start', {
      mode: 'rich',
      msisdn_masked: msisdnMasked,
      headers: headersSummary(headersYmds),
      booked: bookedMapSummary(bookedMap)
    });

    // 1) Pick anchor shift (this is where "today’s shift" can fail to be selected)
    const anchor = (typeof _pickAnchorShiftForUser === 'function')
      ? _pickAnchorShiftForUser({ headersYmds, bookedMap })
      : null;

    if (!anchor) {
      _log('DNA', 'peers:anchor:none', {
        reason: 'no_anchor_from_pickAnchorShiftForUser',
        headers: headersSummary(headersYmds),
        booked: bookedMapSummary(bookedMap)
      });
      return safeEmpty;
    }

    _log('DNA', 'peers:anchor', { anchor: keyToLog(anchor) });

    // 2) Find adjacent shift keys
    const adj = (typeof _findAdjacentShiftKeys === 'function')
      ? _findAdjacentShiftKeys(anchor)
      : { previous: null, next: null };

    _log('DNA', 'peers:adj', {
      anchor: keyToLog(anchor),
      previous: keyToLog(adj && adj.previous),
      next: keyToLog(adj && adj.next)
    });

    // 3) List peers per key
    let currentPeers = [];
    let previousPeers = [];
    let nextPeers = [];

    // Current
    if (typeof _listPeersForShiftKey === 'function') {
      _log('DNA', 'peers:list:enter', { role: 'current', key: keyToLog(anchor) });
      currentPeers = _listPeersForShiftKey(ss, anchor, user) || [];
      _log('DNA', 'peers:list:result', {
        role: 'current',
        key: keyToLog(anchor),
        count: currentPeers.length,
        sample: samplePeers(currentPeers, 5)
      });
    } else {
      _log('DNA', 'peers:list:skip', { role: 'current', reason: '_listPeersForShiftKey not defined' });
    }

    // Previous
    if (adj && adj.previous && typeof _listPeersForShiftKey === 'function') {
      _log('DNA', 'peers:list:enter', { role: 'previous', key: keyToLog(adj.previous) });
      previousPeers = _listPeersForShiftKey(ss, adj.previous, user) || [];
      _log('DNA', 'peers:list:result', {
        role: 'previous',
        key: keyToLog(adj.previous),
        count: previousPeers.length,
        sample: samplePeers(previousPeers, 3)
      });
    } else if (adj && !adj.previous) {
      _log('DNA', 'peers:list:skip', { role: 'previous', reason: 'no_previous_key' });
    }

    // Next
    if (adj && adj.next && typeof _listPeersForShiftKey === 'function') {
      _log('DNA', 'peers:list:enter', { role: 'next', key: keyToLog(adj.next) });
      nextPeers = _listPeersForShiftKey(ss, adj.next, user) || [];
      _log('DNA', 'peers:list:result', {
        role: 'next',
        key: keyToLog(adj.next),
        count: nextPeers.length,
        sample: samplePeers(nextPeers, 3)
      });
    } else if (adj && !adj.next) {
      _log('DNA', 'peers:list:skip', { role: 'next', reason: 'no_next_key' });
    }

    const out = { previous: previousPeers, current: currentPeers, next: nextPeers };

    // 4) Final summary (what will feed cache/save)
    _log('DNA', 'peers:compute:done', {
      anchor: keyToLog(anchor),
      previous_len: previousPeers.length,
      current_len:  currentPeers.length,
      next_len:     nextPeers.length,
      current_names: samplePeers(currentPeers, 10).map(p => (p.firstName || '') + (p.surnameInitial ? (' ' + p.surnameInitial) : ''))
    });

    return out;

  } catch (ex) {
    _log('DNA', 'peers:compute:error', { error: String(ex && ex.message || ex) });
    return safeEmpty;
  }
}

/**
 * Decide the user’s “anchor” shift within the window:
 * Priority:
 *  1) If today’s ymd is booked: anchor = today’s booking
 *  2) Else the nearest upcoming booked day
 *  3) Else the most recent past booked day
 * Returns shiftKey: { ymd, hospital, ward, shiftType }
 */
function _pickAnchorShiftForUser({ headersYmds, bookedMap }) {
  const P = _P();
  const tz = P && P.TZ ? P.TZ : 'Europe/London';
  const todayYmd = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');

  function mkKey(ymd) {
    const b = bookedMap && bookedMap[ymd];
    if (!b) return null;
    return {
      ymd,
      hospital: b.hospital || '',
      ward: b.ward || '',
      shiftType: (String(b.shift || b.notes || '').toUpperCase().includes('NIGHT'))
        ? 'NIGHT'
        : 'LONG DAY'
    };
  }

  try {
    _log('DNA', 'pickAnchor:enter', {
      todayYmd,
      headersYmds: headersYmds || [],
      bookedKeys: Object.keys(bookedMap || {})
    });
  } catch (_) {}

  // 1) today
  if (bookedMap && bookedMap[todayYmd]) {
    const k = mkKey(todayYmd);
    try { _log('DNA', 'pickAnchor:today_match', { todayYmd, anchor: k }); } catch (_) {}
    if (k) return k;
  }

  // 2) next upcoming
  const upcoming = headersYmds.filter(ymd => ymd >= todayYmd);
  for (const y of upcoming) {
    const k = mkKey(y);
    if (k) {
      try { _log('DNA', 'pickAnchor:upcoming_match', { y, anchor: k }); } catch (_) {}
      return k;
    }
  }

  // 3) most recent past
  const past = headersYmds.filter(ymd => ymd < todayYmd).reverse();
  for (const y of past) {
    const k = mkKey(y);
    if (k) {
      try { _log('DNA', 'pickAnchor:past_match', { y, anchor: k }); } catch (_) {}
      return k;
    }
  }

  try { _log('DNA', 'pickAnchor:none_found', {}); } catch (_) {}
  return null;
}

/**
 * Given an anchor shiftKey, derive the immediately-adjacent shift blocks at
 * the same hospital+ward.
 *
 * Convention used:
 *  - LD (LONG DAY) is followed by NIGHT on the same ymd
 *  - NIGHT is followed by LONG DAY on the next calendar day
 *  - previous is the inverse (NIGHT ← LD same day; LD ← NIGHT previous day)
 */
function _findAdjacentShiftKeys(anchor) {
  const out = { previous: null, next: null };
  if (!anchor) return out;

  const { ymd, hospital, ward, shiftType } = anchor;
  function ymdAdd(ymd, deltaDays) {
    try {
      const [Y,M,D] = ymd.split('-').map(Number);
      const d = new Date(Y, (M||1)-1, D || 1);
      d.setDate(d.getDate() + (deltaDays | 0));
      const tz = (_P() && _P().TZ) ? _P().TZ : 'Europe/London';
      return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
    } catch (_) { return ymd; }
  }

  if (shiftType === 'LONG DAY') {
    out.previous = { ymd, hospital, ward, shiftType: 'NIGHT' };       // prior block (same day night)
    out.next     = { ymd, hospital, ward, shiftType: 'NIGHT' };       // next block (same day night)
  } else {
    // NIGHT
    out.previous = { ymd: ymdAdd(ymd, -1), hospital, ward, shiftType: 'LONG DAY' };
    out.next     = { ymd: ymdAdd(ymd, 1),  hospital, ward, shiftType: 'LONG DAY' };
  }
  return out;
}

/**
 * List peers for a specific shiftKey at the same hospital+ward+shiftType,
 * excluding the requesting user. Returns array of:
 *   { firstName, surnameInitial, role, msisdn07 }
 */
function _listPeersForShiftKey(ss, shiftKey, user) {
  try {
    if (!ss || !shiftKey) return [];
    const p = _P();
    const sh = ss.getSheetByName(p.SH_EH);
    if (!sh) return [];

    const rows = sh.getDataRange().getDisplayValues();
    if (rows.length < 2) return [];

    const hdr = rows[0].map(s => String(s || "").toLowerCase());
    const idxBy = (h) => hdr.indexOf(h);

    const iOcc   = idxBy("occupantkey");
    const iDate  = idxBy("date");
    const iShift = idxBy("shift");
    const iHosp  = idxBy("hospital");
    const iWard  = idxBy("ward");
    let   iJob   = idxBy("job title"); if (iJob < 0 && rows[0].length >= 8) iJob = 7;

    const wantYmd = String(shiftKey.ymd || '');
    const wantHosp = _normalizeHospitalName(String(shiftKey.hospital || ''));
    const wantWard = String(shiftKey.ward || '');
    const wantShiftType = String(shiftKey.shiftType || '').toUpperCase();

    const out = [];
    for (let r = 1; r < rows.length; r++) {
      const dateStr = (iDate >= 0 ? String(rows[r][iDate] || "") : "").trim();
      const m = dateStr.match(/\b(\d{2}\/\d{2}\/\d{4})\b/);
      const ddmmyyyy = m ? m[1] : dateStr;
      const parts = ddmmyyyy.split("/");
      if (parts.length !== 3) continue;

      const [dd, mm, yyyy] = parts.map(Number);
      const ymd = Utilities.formatDate(new Date(yyyy, mm - 1, dd), p.TZ, "yyyy-MM-dd");
      if (ymd !== wantYmd) continue;

      const hospNorm = _normalizeHospitalName(iHosp >= 0 ? String(rows[r][iHosp] || "") : "");
      if (hospNorm !== wantHosp) continue;

      const ward = iWard >= 0 ? String(rows[r][iWard] || "") : "";
      if (ward !== wantWard) continue;

      const shiftRaw = iShift >= 0 ? String(rows[r][iShift] || "").toUpperCase() : "";
      const rowType = (shiftRaw.includes("NIGHT") || shiftRaw === "N") ? "NIGHT" : "LONG DAY";
      if (rowType !== wantShiftType) continue;

      // Occupant (to resolve name → contact/role)
      const occ = (iOcc >= 0 ? String(rows[r][iOcc] || "") : "").trim();
      if (!occ) continue;

      // Resolve candidate by occupant key; fallback to name split.
      const cand = _findCandidateByOccupantKeyOrName(ss, occ);
      if (!cand) continue;

      // Exclude self by telephone match if available, else by name match.
      const isSelf =
        (user && user.tel && cand.telephone && _normaliseLocalTel(user.tel) === _normaliseLocalTel(cand.telephone)) ||
        ((user && user.firstname && user.surname) &&
          ((cand.firstname || '').toLowerCase() === String(user.firstname || '').toLowerCase()) &&
          ((cand.surname || '').toLowerCase() === String(user.surname || '').toLowerCase()));

      if (isSelf) continue;

      const roleFromRow = iJob >= 0 ? String(rows[r][iJob] || "").trim().toUpperCase() : "";
      const role = (roleFromRow === "RMN" || roleFromRow === "HCA") ? roleFromRow : (cand.role || "");

      out.push({
        firstName: cand.firstname || '',
        surnameInitial: _surnameInitial(cand.surname || ''),
        role: role || '',
        msisdn07: _to07(cand.telephone || '')
      });
    }

    // De-dup by msisdn07 to keep it clean
    const seen = new Set();
    const dedup = [];
    for (const p2 of out) {
      const k = (p2.msisdn07 || '') + '|' + (p2.firstName || '') + '|' + (p2.surnameInitial || '');
      if (seen.has(k)) continue;
      seen.add(k);
      dedup.push(p2);
    }
    return dedup;
  } catch (_) {
    return [];
  }
}

/**
 * Resolve candidate by occupant key ("Surname First") or fall back to scanning
 * Candidates sheet by best effort name match.
 */
function _findCandidateByOccupantKeyOrName(ss, occupantKey) {
  try {
    const key = String(occupantKey || '').toLowerCase().trim();
    if (!key) return null;

    const sh = ss.getSheetByName(_P().SH_CAND);
    if (!sh) return null;

    const vals = sh.getDataRange().getDisplayValues();
    if (vals.length < 2) return null;

    for (let i = 1; i < vals.length; i++) {
      const sur = String(vals[i][0] || '').trim();
      const fir = String(vals[i][1] || '').trim();
      const tel = _normaliseLocalTel(String(vals[i][3] || ''));
      const nameKey = (sur + ' ' + fir).toLowerCase().trim();

      if (nameKey === key) {
        return {
          rowIndex: i + 1,
          surname: sur,
          firstname: fir,
          telephone: tel,
          role: String(vals[i][7] || '').toUpperCase() // if present (defensive)
        };
      }
    }
    return null;
  } catch (_) {
    return null;
  }
}

/* ------------ tiny format helpers ------------ */

function _to07(msisdn) {
  const s = _normaliseLocalTel(msisdn || '');
  return s || '';
}

function _surnameInitial(surname) {
  const s = String(surname || '').trim();
  return s ? (s[0].toUpperCase() + '.') : '';
}












/** ───────────────────────── DNA/helpers (shared) ───────────────────────── **/

/** Uppercase/trim utility (safe across inputs) */
function __U(x){ return String(x == null ? '' : x).trim().replace(/\s+/g,' ').toUpperCase(); }

/** Canonicalise shift type (falls back to UPPER if global helper absent) */
function __canonicalShiftTypeLocal(raw){
  return (typeof __canonicalShiftType === 'function') ? __canonicalShiftType(raw) : __U(raw);
}

/** Build the shift signature BASE (no issue suffix). Matches writer/reader logic. */
/** Build the shift signature BASE (no issue suffix). Matches writer/reader logic. */
function __sigBaseFromItem(it){
  const ymd      = it.ymd || (it.shift && it.shift.ymd) || '';
  const hospital = it.hospital || (it.shift && it.shift.hospital) || '';
  const ward     = it.ward || (it.shift && it.shift.ward) || '';
  const jobTitle = it.job_title || (it.shift && (it.shift.job_title || it.shift.jobTitle)) || '';

  // Prefer resolved start/end; else the label
  const startIso = it.startAtIso_resolved || it.startAtIso ||
                   (it.shift && (it.shift.startAtIso || it.shift.startIso)) || null;
  const endIso   = it.endAtIso_resolved   || it.endAtIso   ||
                   (it.shift && (it.shift.endAtIso   || it.shift.endIso))   || null;
  const trLabel  = it.time_range_label || (it.shift && it.shift.time_range_label) || '';
  const timeKey  = (startIso && endIso) ? (String(startIso) + '|' + String(endIso)) : String(trLabel);

  // ── helpers to deduce type from TIMES ONLY (ignore dates) ──
  function minutesFromIso(iso){
    try{
      const d = new Date(iso);
      if (isNaN(d)) return null;
      return d.getUTCHours()*60 + d.getUTCMinutes(); // only HH:MM (UTC) matter
    }catch(_){ return null; }
  }
  function deduceFromIsoTimes(sIso, eIso){
    const sMin = minutesFromIso(sIso);
    const eMin = minutesFromIso(eIso);
    if (sMin == null || eMin == null) return { deduced:'', sMin:null, eMin:null, source:'iso_invalid' };
    // end ≤ start ⇒ crosses midnight ⇒ NIGHT; else LONG DAY
    return { deduced: (eMin <= sMin) ? 'NIGHT' : 'LONG DAY', sMin, eMin, source:'iso' };
  }
  function deduceFromLabel(lbl){
    try{
      const m = String(lbl||'').match(/(\d{1,2}):(\d{2}).*?(\d{1,2}):(\d{2})/);
      if (!m) return { deduced:'', sMin:null, eMin:null, source:'label_no_match' };
      const sH = Math.min(23, Math.max(0, parseInt(m[1],10)||0));
      const sM = Math.min(59, Math.max(0, parseInt(m[2],10)||0));
      const eH = Math.min(23, Math.max(0, parseInt(m[3],10)||0));
      const eM = Math.min(59, Math.max(0, parseInt(m[4],10)||0));
      const sMin = sH*60 + sM;
      const eMin = eH*60 + eM;
      return { deduced: (eMin <= sMin) ? 'NIGHT' : 'LONG DAY', sMin, eMin, source:'label' };
    }catch(_){ return { deduced:'', sMin:null, eMin:null, source:'label_error' }; }
  }

  const rawShiftType =
    (it.resolved_type || (it.resolved && it.resolved.type) ||
     it.parsed_type   || (it.parsed   && it.parsed.type)) ||
    it.shift_type || (it.shift && it.shift.shift_type) ||
    it.shiftType  || (it.shift && it.shift.shiftType)  ||
    it.shiftInfo  || (it.shift && it.shift.shiftInfo)  || '';

  // Try ISO times first, then label
  const byIso   = (startIso && endIso) ? deduceFromIsoTimes(startIso, endIso) : { deduced:'', sMin:null, eMin:null, source:'no_iso' };
  const byLabel = (!byIso.deduced && trLabel) ? deduceFromLabel(trLabel) : { deduced:'', sMin:null, eMin:null, source: byIso.source || 'skip_label' };

  const deducedType = byIso.deduced || byLabel.deduced || '';
  const sourceUsed  = byIso.deduced ? byIso.source : byLabel.source;

  const shiftTypeChosen = deducedType || rawShiftType || 'UNKNOWN';
  const shiftTypeCanon  = __canonicalShiftTypeLocal(shiftTypeChosen);

  const sig = [__U(ymd), timeKey, __U(hospital), __U(ward), __U(jobTitle), __U(shiftTypeCanon)].join('|');

  // ── diagnostic logging ──
  try {
    _log('sig', 'sigBase:input', {
      ymd, hospital, ward, jobTitle,
      startIso, endIso, trLabel, timeKey,
      rawShiftType: String(rawShiftType || ''),
    });
    _log('sig', 'sigBase:decision', {
      deducedType,
      sourceUsed,
      iso_sMin: byIso.sMin, iso_eMin: byIso.eMin,
      label_sMin: byLabel.sMin, label_eMin: byLabel.eMin,
      chosen: shiftTypeChosen,
      canonical: shiftTypeCanon,
      sig_preview: sig.slice(0, 120) + (sig.length > 120 ? '…' : '')
    });
  } catch (_) { /* never throw from logging */ }

  return sig;
}



/** Robust name key for DNA subject (tolerates FIRST SURNAME / SURNAME FIRST, initials, spacing) */
function _dnaRobustNameKey(name){
  const s = String(name || '')
    .replace(/[^\p{L}\p{N}\s]/gu, ' ')
    .replace(/\s+/g,' ')
    .trim()
    .toLowerCase();
  if (!s) return '';
  // We’ll store in SURNAME FIRST canonical form when possible, but accept either on compare.
  // Here we just return the cleaned string; comparison logic will consider both orders.
  return s;
}

/** Subject identity key: prefer E.164; else a robust name key (both are stored with a prefix). */
function _dnaSubjectKeyFromInputs(subjectName, subjectMsisdn447){
  const tel = String(subjectMsisdn447 || '').replace(/\D/g,'');
  if (/^447\d{9}$/.test(tel)) return 'TEL:' + tel;
  const nk = _dnaRobustNameKey(subjectName);
  return nk ? ('NAME:' + nk) : 'UNKNOWN';
}

/** Lazy purge for the per-shift DNA map (mutates a copy; returns {map,purged}) */
function _purgeExpiredDnaExclusions(mapIn){
  const nowMs = Date.now();
  const map = Object.assign({}, mapIn || {});
  let purged = 0;
  Object.keys(map).forEach(k => {
    const it = map[k] || {};
    const expMs = new Date(it.exp || 0).getTime();
    if (!isFinite(expMs) || expMs < nowMs) { delete map[k]; purged++; }
  });
  return { map, purged };
}

/** Read shift-scoped DNA exclusions for a sigBase: returns {map, purged} */
function _dnaExclusionStoreRead(sigBase){
  const props = PropertiesService.getScriptProperties();
  const key   = 'EA_EXC_SHIFT::' + String(sigBase || '');
  let map = {};
  try { map = JSON.parse(props.getProperty(key) || '{}') || {}; } catch (_) { map = {}; }
  const res = _purgeExpiredDnaExclusions(map);
  if (res.purged) { try { props.setProperty(key, JSON.stringify(res.map)); } catch(_) {} }
  return { key, map: res.map, purged: res.purged };
}

/** Upsert one DNA subject into the shift-scoped store (with TTL already computed by caller). */
function _dnaExclusionStoreUpsert(sigBase, subjectKey, entry){
  const props = PropertiesService.getScriptProperties();
  const lock  = LockService.getScriptLock();
  const key   = 'EA_EXC_SHIFT::' + String(sigBase || '');
  try { lock.waitLock(5000); } catch(_) {}

  let map = {};
  try { map = JSON.parse(props.getProperty(key) || '{}') || {}; } catch(_) { map = {}; }

  const res = _purgeExpiredDnaExclusions(map);
  map = res.map;

  if (subjectKey && subjectKey !== 'UNKNOWN') {
    map[subjectKey] = Object.assign({}, entry || {});
  }
  try { props.setProperty(key, JSON.stringify(map)); } catch(_) {}
  try { lock.releaseLock(); } catch(_) {}
  return { key, ok: true };
}

/** Identity key for a cohort member (prefer 447, else robust name in SURNAME FIRST canonical form). */
function _dnaMemberKey(member){
  // Prefer a canonical 447… TEL key (try msisdn07, then msisdn)
  try {
    const raw = member ? (member.msisdn07 || member.msisdn || '') : '';
    const e164 = (typeof _normaliseUkMsisdnTo447 === 'function')
      ? _normaliseUkMsisdnTo447(String(raw))
      : __to447(String(raw));
    if (e164 && /^447\d{9}$/.test(e164)) return 'TEL:' + e164;
  } catch (_) {}

  // Else use robust NAME key in SURNAME FIRST (lowercased, single-spaced)
  const first = String(member && member.firstName || '').trim();
  const sur   = String(member && member.surname   || '').trim();
  const a = (sur + ' ' + first).replace(/\s+/g,' ').trim().toLowerCase(); // SURNAME FIRST
  return a ? ('NAME:' + a) : 'UNKNOWN';
}


/**
 * Apply shift-scoped DNA exclusions to an eligible item.
 * - PROD: filter cohort; if none remain, remove DNA from allowed_issues.
 * - TEST (testMode && testPhoneMatch): do NOT filter; annotate dna_excluded_preview + label.
 *
 * @returns the mutated item (for chaining)
 */
function _applyDnaExclusionsToItem(item, excludedSet, opts){
  const testMode = !!(opts && opts.testMode);
  const isTestPhone = !!(opts && opts.isTestPhone);

  if (!item || !Array.isArray(item.allowed_issues)) return item;
  const dnaAllowed = item.allowed_issues.some(x => String(x || '').toUpperCase() === 'DNA');
  if (!dnaAllowed) return item;

  const members = Array.isArray(item.cohort) ? item.cohort.slice() : [];
  if (!members.length || !(excludedSet && excludedSet.size)) return item;

  // Compute would-remove list (by comparing identity keys)
  const wouldRemove = [];
  const keep = [];
  members.forEach(m => {
    const k = _dnaMemberKey(m);
    const hit = k && excludedSet.has(k);
    if (hit) wouldRemove.push({ firstName: m.firstName || '', surname: m.surname || '' });
    else keep.push(m);
  });

  if (testMode && isTestPhone) {
    // Annotate, do not filter
    item.dna_excluded_preview = wouldRemove;
    try {
      if (wouldRemove.length) {
        const names = wouldRemove.map(x => (x.firstName + ' ' + x.surname).trim()).filter(Boolean).join(', ');
        const note = names ? ` (dna excluding: ${names})` : '';
        if (note) {
          if (item.time_range_label && !String(item.time_range_label).includes(note)) item.time_range_label += note;
          else if (item.shift && item.shift.time_range_label && !String(item.shift.time_range_label).includes(note)) item.shift.time_range_label += note;
        }
      }
    } catch (_) {}
    return item;
  }

  // PROD: commit filtering
  item.cohort = keep;

  // If nobody left to report, remove DNA from allowed_issues
  if (keep.length === 0) {
    item.allowed_issues = (item.allowed_issues || []).filter(x => String(x || '').toUpperCase() !== 'DNA');
  }
  return item;
}

/** Compute TTL (ISO) = shift end (if known) + 3 days; else end-of-day(ymd) + 3 days. */
function _dnaTtlIso(ymd, endAtIso){
  function __iso(d){ try { return new Date(d).toISOString(); } catch(_) { return new Date().toISOString(); } }
  let endIsoForTtl = endAtIso;
  if (!endIsoForTtl && ymd) {
    const [Y,M,D] = String(ymd || '').split('-').map(Number);
    const localEnd = new Date(Y || 1970, (M||1)-1, D || 1, 23, 59, 59, 999);
    endIsoForTtl = __iso(localEnd);
  }
  const expMs  = (new Date(endIsoForTtl || new Date())).getTime() + 3*24*60*60*1000;
  return new Date(expMs).toISOString();
}






function doPost(e) {
  // --------------- lightweight tracer (no sheet writes here) ---------------
  const __RID = Utilities.getUuid();
  const __T0  = Date.now();
  const __spans = [];
  let __action = '';
  let __hadK = false;
  let __emailSeen = '';
  let __msisdnSeen = '';
  let __statusCode = 200;

  function __mark(name, extra) {
    try { __spans.push(Object.assign({ n: name, ms: Date.now() - __T0 }, extra || {})); } catch (_) {}
  }
  function __maskEmail(s) {
    try {
      if (!s) return '';
      const m = String(s).trim().toLowerCase();
      const [u, d] = m.split('@');
      if (!d) return '•••';
      const head = u.length ? u[0] : '';
      return head + '•••@' + d;
    } catch (_) { return '•••'; }
  }
  function __maskMsisdn(s) {
    try {
      if (!s) return '';
      const m = String(s).replace(/\s+/g,'');
      return (m.slice(0,3) + '•••••' + m.slice(-4));
    } catch (_) { return '•••••'; }
  }
  function __finalizeAndReturn(resp, status) {
    try { __statusCode = status || __statusCode || 200; } catch (_) {}
    return resp;
  }
// --------------------------- Logs sheet helpers ---------------------------

// Keep the function, but disable its internals.
function __logsSheet() {
  /*
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName('Logs');
    if (!sh) {
      sh = ss.insertSheet('Logs');
      sh.getRange(1,1,1,6).setValues([['Timestamp','ReqId','Route','Event','Action','DataJSON']]);
      sh.setFrozenRows(1);
    }
    return sh;
  } catch (_) { return null; }
  */
}


// Keep the function, but disable its internals.
function __log(eventName, dataObj) {
  /*
  try {
    const sh = __logsSheet();
    if (!sh) return;
    const tz = (typeof _P === 'function' && _P().TZ) ? _P().TZ : 'Europe/London';
    const stamp = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
    const payload = Object.assign({
      ms: Date.now() - __T0,
      action: __action || '',
    }, dataObj || {});
    sh.appendRow([stamp, __RID, 'doPost', String(eventName || ''), String(__action || ''), JSON.stringify(payload)]);
  } catch (_) {}
  */
  // Logging disabled: no-op.
}


  // Wrap your existing _ok/_err so every exit path logs once.
  const __OK = _ok, __ERR = _err;
  function OK(payload) { return __finalizeAndReturn(__OK(payload), 200); }
  function ERR(msg, code) { return __finalizeAndReturn(__ERR(msg, code || 500), code || 500); }

  // --------------------------- general helpers ---------------------------
  function _roundUpTo5Min(dateObj, tz) {
    const d = new Date(dateObj);
    d.setSeconds(0, 0);
    const m = d.getMinutes();
    const up = Math.ceil((m || 0) / 5) * 5;
    if (up === 60) { d.setHours(d.getHours() + 1); d.setMinutes(0); }
    else { d.setMinutes(up); }
    return d;
  }
  function _formatHHmmHrs(d, tz) {
    return Utilities.formatDate(new Date(d), tz || ((typeof _P==='function' && _P().TZ) || 'Europe/London'), 'HH:mm') + 'hrs';
  }
 function _etaLabelToMinutes(lbl) {
  try {
    let L = String(lbl || '').toLowerCase().trim();
    if (!L) return null;

    // Remove parenthetical notes (e.g., "(arrive by 10:55 at the latest)")
    L = L.replace(/\([^)]*\)/g, ' ').trim();

    // Remove clock times like "10:55" to avoid accidental "55 minutes" matches
    L = L.replace(/\b\d{1,2}:\d{2}\b/g, ' ').trim();

    // "Less than X hours and Y minutes" (Y optional)
    let m = L.match(/less\s*than\s*(\d+)\s*hour(?:s)?(?:\s*and\s*(\d+)\s*(?:minute|min)(?:s)?)?/);
    if (m) {
      const h = parseInt(m[1], 10) || 0;
      const mi = parseInt(m[2] || '0', 10) || 0;
      const total = h * 60 + mi;
      if (total > 0) return total;
    }

    // "X hours and Y minutes" (Y optional)
    m = L.match(/(\d+)\s*hour(?:s)?(?:\s*and\s*(\d+)\s*(?:minute|min)(?:s)?)?/);
    if (m) {
      const h = parseInt(m[1], 10) || 0;
      const mi = parseInt(m[2] || '0', 10) || 0;
      const total = h * 60 + mi;
      if (total > 0) return total;
    }

    // "X minutes"
    m = L.match(/(\d+)\s*(?:minute|min)(?:s)?\b/);
    if (m) {
      const mi = parseInt(m[1], 10);
      if (isFinite(mi) && mi > 0) return mi;
    }

    // "Less than X" (no unit) → treat X as minutes
    m = L.match(/less\s*than\s*(\d+)/);
    if (m) {
      const mi = parseInt(m[1], 10);
      if (isFinite(mi) && mi > 0) return mi;
    }

    // Legacy fallbacks
    if (/\b15\b/.test(L)) return 15;
    if (/\b30\b/.test(L)) return 30;
    if (/\b1\s*hour/.test(L)) return 60;
    if (/\b2\s*hour/.test(L)) return 120;
    if (/\b3\s*hour/.test(L)) return 180;

    return null;
  } catch (_) {
    return null;
  }
}

  // ────────── NEW: safe normaliser for ETA/leave-time labels (no validation) ──────────
  function _normaliseEtaOrLeaveTimeLabel(s) {
    // Accept "now" (any case/whitespace) and "HH MM" or "HH:MM"; zero-pad hours/mins; do not reject anything else.
    try {
      const raw = String(s == null ? '' : s).trim();
      if (!raw) return '';
      const lower = raw.toLowerCase();
      if (lower === 'now' || lower === 'now!') return 'NOW';

      // Extract digits around a possible colon/space separator
      const mm = raw.match(/^\s*(\d{1,2})[\s:](\d{1,2})\s*$/);
      if (mm) {
        let hh = parseInt(mm[1], 10);
        let mi = parseInt(mm[2], 10);
        if (isFinite(hh) && isFinite(mi)) {
          if (hh < 0) hh = 0; if (hh > 23) hh = 23;
          if (mi < 0) mi = 0; if (mi > 59) mi = 59;
          const HH = (hh < 10 ? '0' + hh : String(hh));
          const MM = (mi < 10 ? '0' + mi : String(mi));
          return HH + ':' + MM;
        }
      }

      // Already "HH:MM" format? Normalise padding.
      const mm2 = raw.match(/^\s*(\d{1,2}):(\d{1,2})\s*$/);
      if (mm2) {
        let hh = parseInt(mm2[1], 10);
        let mi = parseInt(mm2[2], 10);
        if (isFinite(hh) && isFinite(mi)) {
          if (hh < 0) hh = 0; if (hh > 23) hh = 23;
          if (mi < 0) mi = 0; if (mi > 59) mi = 59;
          const HH = (hh < 10 ? '0' + hh : String(hh));
          const MM = (mi < 10 ? '0' + mi : String(mi));
          return HH + ':' + MM;
        }
      }

      // Fallback: return the original label untouched (preserves backwards compatibility).
      return raw;
    } catch (_) {
      return String(s == null ? '' : s);
    }
  }

  // --------------------------- ClickSend diagnostics config ---------------------------
  const ENFORCE_CLICKSEND_DEDICATED_FROM = false;
  const CLICKSEND_DEDICATED_FROM = '+447507320592';

  // --------------------------- begin logic ---------------------------
  try {
    // ---- EARLY ARRIVAL LOG (always) ----
    const ctype = (e && e.postData && e.postData.type) || '';
    const clen  = (e && e.postData && e.postData.contents) ? (e.postData.contents.length || -1) : -1;
    __log('post:arrived', {
      contentType: ctype,
      hasParams: !!(e && e.parameter),
      hasBody: !!(e && e.postData && e.postData.contents),
      bodyLen: clen
    });
// 🔧 NEW: log raw JSON body preview for debugging
try {
  if (e && e.postData && typeof e.postData.contents === 'string') {
    const rawPreview = e.postData.contents.slice(0, 1000); // cap to 1k chars
    __log('post:raw_body', { preview: rawPreview });
  }
} catch (ex) {
  __log('post:raw_body_error', { error: String(ex) });
}

    // ---- Parse body: tolerant JSON OR form-encoded ----
    let body = {};
    if (e && e.postData && typeof e.postData.contents === 'string') {
      const raw = e.postData.contents;
      const looksJson = /^\s*[\{\[]/.test(raw || '');
      if ((ctype && ctype.indexOf('application/json') === 0) || looksJson) {
        try { body = JSON.parse(raw || '{}'); }
        catch (err) {
          __log('post:json_parse_error', { error: String(err), rawPreview: (raw || '').slice(0, 500) });
          body = Object.assign({}, e.parameter || {}); // fallback
        }
      } else {
        body = Object.assign({}, e.parameter || {});   // x-www-form-urlencoded
      }
    } else {
      body = Object.assign({}, e && e.parameter || {}); // query params
    }

    // ---- Normalise keys lower-case for convenience ----
    function _lowerKeys(obj) {
      const out = {};
      if (obj && typeof obj === 'object') {
        Object.keys(obj).forEach(k => { out[String(k).toLowerCase()] = obj[k]; });
      }
      return out;
    }
    const bodyLC = _lowerKeys(body);
    const paramsLC = _lowerKeys(e && e.parameter || {});

    // ---- Action from body OR query (case-insensitive) ----
    const actionQ = paramsLC['action'] ? String(paramsLC['action']) : '';
    const actionB = bodyLC['action'] ? String(bodyLC['action']) : '';
    __action = (actionB || actionQ || '').toUpperCase();

    // ---- Note if a "k" token was supplied (legacy behaviour) ----
    __hadK = !!(
      (typeof paramsLC['k'] !== 'undefined' && String(paramsLC['k'] || '').trim() !== '') ||
      (typeof bodyLC['k'] !== 'undefined' && String(bodyLC['k'] || '').trim() !== '')
    );

    // ---- Webhook allow-list (by action) ----
    const WEBHOOK_ACTIONS = new Set([
      'EMERGENCY_WEBHOOK_WATI',
      'EMERGENCY_WEBHOOK_CLICKSEND'
    ]);

    // ---- If this is a webhook action, verify secret and route quickly ----
    if (WEBHOOK_ACTIONS.has(__action)) {
      const props = (typeof _getEmergencyProps === 'function') ? _getEmergencyProps() : null;
      const expected = props && props.WEBHOOK_SECRET;
      const provided =
        (body && (body.secret || body.EMERGENCY_WEBHOOK_SECRET)) ||
        (e && e.parameter && (e.parameter.secret || e.parameter.EMERGENCY_WEBHOOK_SECRET)) || '';

      if (!expected || String(expected) !== String(provided)) {
        __log('webhook:unauthorized', { hasExpected: !!expected, provided: provided ? 'present' : 'missing', action: __action });
        // For ClickSend, avoid retries with 200; for WATI, 401 is OK.
        if (__action === 'EMERGENCY_WEBHOOK_CLICKSEND') return __simpleText('OK', 200);
        return __simpleText('UNAUTHORIZED_WEBHOOK', 401);
      }

      __log('webhook:authorized', { action: __action, contentType: ctype });

      // ---- ClickSend Voice webhook path (delivery + DTMF) ----
      if (__action === 'EMERGENCY_WEBHOOK_CLICKSEND') {
        const diag = __clicksendDiagnostics(e, { enforceFrom: ENFORCE_CLICKSEND_DEDICATED_FROM, expectFrom: CLICKSEND_DEDICATED_FROM });
        __log('clicksend:webhook:diagnostics', diag);
        // ⬇️ Place this immediately after the existing diagnostics line:
        // __log('clicksend:webhook:diagnostics', diag);

        try {
          const p = Object.assign({}, (e && e.parameter) || {});
          const lc = {};
          Object.keys(p).forEach(k => { lc[String(k).toLowerCase()] = String(p[k]); });

          // Redact sensitive values
          const redacted = {};
          Object.keys(p).forEach(k => {
            const key = String(k).toLowerCase();
            const val = String(p[k]);
            redacted[k] = /secret|token|auth|bearer|password|apikey/i.test(key) ? '•••REDACTED•••' : val.slice(0, 200);
          });

          // What your binder SHOULD pick up
          const bind = {
            call_id: lc.call_id || lc.voice_id || lc.message_id || lc.messageid || null,
            digits:  lc.digits  || lc.keypress || lc.dtmf       || lc.input     || null,
            custom_string: lc.custom_string || lc.customstring || lc.reference || null,
            from:   lc.from   || lc.caller     || lc.from_number || null,
            to:     lc.to     || lc.to_number  || lc.recipient   || lc.msisdn || null,
            status: lc.status || lc.event      || lc.description || null
          };

          __log('clicksend:webhook:raw_bind_view', {
            keys: Object.keys(p),
            bind,
            params_snapshot_redacted: redacted
          });
        } catch (_) {
          __log('clicksend:webhook:raw_bind_view_error', { error: String(_) });
        }


        // Optional enforcement of your dedicated CLI
        if (diag.enforced && !diag.from_ok) {
          __log('clicksend:webhook:rejected_by_from_filter', { from: diag.from || null, expect: CLICKSEND_DEDICATED_FROM });
          return __simpleText('OK', 200); // stop ClickSend retries
        }

        if (typeof hookClickSendVoice === 'function') {
          try {
            const out = hookClickSendVoice(e);
            __log('clicksend:webhook:handler_returned', { ok: true });
            return out && typeof out.getContent === 'function' ? out : __simpleText('OK', 200);
          } catch (ex) {
            __log('clicksend:webhook:handler_exception', { error: String(ex) });
            return __simpleText('OK', 200);
          }
        } else {
          __log('clicksend:webhook:no_handler', {});
        }
        return __simpleText('OK', 200);
      }

      // ---- WATI webhook path ----
      if (__action === 'EMERGENCY_WEBHOOK_WATI') {
        if (typeof hookWatiAck === 'function') {
          try {
            const out = hookWatiAck(e);
            __log('wati:webhook:handled', {});
            return out && typeof out.getContent === 'function' ? out : __simpleText('OK', 200);
          } catch (ex) {
            __log('wati:webhook:exception', { error: String(ex) });
            return __simpleText('OK', 200);
          }
        }
        __log('wati:webhook:no_handler', {});
        return __simpleText('OK', 200);
      }
    }

// ─────────────────────────── EMBED helpers (new, additive) ───────────────────────────

// ── New PUBLIC EMBED routes (parallel to legacy HTML pages; leave old ones intact) ──

// 0) ENTRY: sign a short-lived token for this composite alert link (no UI)

if (__action === 'EMERGENCY_ACK_LINK_EMBED') {
  var composite = String(
    (e && e.parameter && (e.parameter.p || e.parameter.alert_id)) ||
    (e && e.postData && e.postData.contents && (function(){try{
      var raw=e.postData.contents; var looksJson=/^\s*[\{\[]/.test(raw||'');
      var b=looksJson?JSON.parse(raw||'{}'):(e.parameter||{}); return b.p||b.alert_id||'';
    }catch(_){return '';} })()) || ''
  ).trim();

  __mark('cta_ack_embed_link:entry', { composite: composite });

  if (!composite) {
    return __jsonp(e, { type: 'ACK_LINK', payload: { ok:false, error:'MISSING_ALERT' } }, 400);
  }

  var alertId = composite, msisdnFromLink = '';
  if (composite.indexOf('~') !== -1) {
    var i = composite.lastIndexOf('~');
    alertId = composite.slice(0, i);
    msisdnFromLink = composite.slice(i+1);
  }

  var alert = _emergencyFindById(alertId);
  if (!alert) {
    __mark('cta_ack_embed_link:not_found', { alert_id: alertId });
    return __jsonp(e, { type: 'ACK_LINK', payload: { ok:false, error:'ALERT_NOT_FOUND' } }, 404);
  }

  try {
    var expUnix = Math.floor(Date.now()/1000) + 180; // 3 minutes
    var t = __signAck(alertId, expUnix, msisdnFromLink || null);
    return __jsonp(e, {
      type: 'ACK_LINK',
      payload: { ok: true, alert_id: alertId, msisdn: msisdnFromLink || '', t: t, exp: expUnix }
    }, 200);
  } catch (ex) {
    __mark('cta_ack_embed_link:token_error', { error: String(ex && ex.message || ex) });
    return __jsonp(e, { type: 'ACK_LINK', payload: { ok:false, error:'TOKEN_ERROR' } }, 500);
  }
}

if (__action === 'EMERGENCY_ACK_COMMIT_EMBED') {
  var alertIdIn = String(
    (e && e.parameter && (e.parameter.alert_id || e.parameter.p)) ||
    (e && e.postData && e.postData.contents && (function(){try{
      var raw=e.postData.contents; var looksJson=/^\s*[\{\[]/.test(raw||'');
      var b=looksJson?JSON.parse(raw||'{}'):(e.parameter||{}); return b.alert_id||b.p||'';
    }catch(_){return '';} })()) || ''
  ).trim();

  var msisdnIn = String(
    (e && e.parameter && e.parameter.msisdn) ||
    (e && e.postData && e.postData.contents && (function(){try{
      var raw=e.postData.contents; var looksJson=/^\s*[\{\[]/.test(raw||'');
      var b=looksJson?JSON.parse(raw||'{}'):(e.parameter||{}); return b.msisdn||'';
    }catch(_){return '';} })()) || ''
  ).trim();

  var tokenIn = String(
    (e && e.parameter && e.parameter.t) ||
    (e && e.postData && e.postData.contents && (function(){try{
      var raw=e.postData.contents; var looksJson=/^\s*[\{\[]/.test(raw||'');
      var b=looksJson?JSON.parse(raw||'{}'):(e.parameter||{}); return b.t||'';
    }catch(_){return '';} })()) || ''
  ).trim();

  __mark('cta_ack_embed_commit:entry', { alert_id: alertIdIn || '(missing)', msisdn_masked: __maskMsisdn(msisdnIn) });

  if (!alertIdIn || !tokenIn) {
    return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'MISSING_PARAMS' } }, 400);
  }

  var v = __verifyAck(alertIdIn, msisdnIn || null, tokenIn);
  if (!v.ok) {
    __mark('cta_ack_embed_commit:token_fail', { alert_id: alertIdIn, reason: v.reason || 'VERIFY_FAIL' });
    return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'INVALID_OR_EXPIRED_LINK' } }, 403);
  }

  var lock = LockService.getScriptLock();
  try { lock.waitLock(5000); } catch (_) {
    __mark('cta_ack_embed_commit:lock_fail', { alert_id: alertIdIn });
    return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'BUSY' } }, 503);
  }

  try {
    var alert = _emergencyFindById(alertIdIn);
    if (!alert) {
      __mark('cta_ack_embed_commit:not_found', { alert_id: alertIdIn });
      return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'ALERT_NOT_FOUND' } }, 404);
    }

    var already = (alert.acknowledged === true || String(alert.acknowledged) === 'true');
    var ms447 = (typeof _normaliseUkMsisdnTo447 === 'function') ? _normaliseUkMsisdnTo447(msisdnIn || '') : (msisdnIn || '');
    var responderName = __resolveResponder(ms447) || 'Web link';

    if (!already) {
      _emergencyUpdateById(alertIdIn, {
        acknowledged: true,
        status: 'ACKED',
        ack_source: 'CTA_EMBED',
        ack_name: responderName,
        ack_number: String(msisdnIn || ''),
        ack_when_iso: _nowIso()
      });

      var fresh = _emergencyFindById(alertIdIn) || alert;
      try { _cancelTriggersForAlert(fresh); } catch(_) {}
      try { _eaAppendLog_(alertIdIn, 'ACK via CTA EMBED'); } catch(_) {}
      try { _sendEmergencyAckEmailPA(fresh, responderName, fresh.ack_when_iso || _nowIso()); } catch(_) {}
      try { _sendWatiBulkTemplate_EMERGENCY_RESPONDED(fresh, responderName, fresh.ack_when_iso || _nowIso()); } catch(_) {}
      try { __sendResponderConfirm(msisdnIn, responderName, 'EMERG_ACCEPT_CONFIRM'); } catch(_) {}
      alert = fresh;
    }

    var others = __listOtherOutstandingAlerts(alertIdIn) || [];
    var bulk = null;
    if (others.length) {
      var idsCsv = others.map(function(o){ return o.alert_id; }).join(',');
      var expUnix = Math.floor(Date.now()/1000) + 180;
      try {
        var bulkToken = __signBulk(idsCsv, expUnix, msisdnIn || null);
        bulk = { idsCsv: idsCsv, msisdn: String(msisdnIn||''), t: bulkToken, exp: expUnix };
      } catch (_) {}
    }

    return __jsonp(e, {
      type: 'ACK_RESULT',
      payload: {
        ok: true,
        kind: already ? 'already' : 'done',
        alert: __alertSummary_(alert),
        others: (others || []).map(__alertSummary_),
        bulk: bulk
      }
    }, 200);

  } catch (ex) {
    __mark('cta_ack_embed_commit:exception', { error: String(ex && ex.message || ex) });
    return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'EXCEPTION', detail:String(ex && ex.message || ex) } }, 500);
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}

if (__action === 'EMERGENCY_ACK_ALL_EMBED') {
  var idsCsv = String(
    (e && e.parameter && e.parameter.ids) ||
    (e && e.postData && e.postData.contents && (function(){try{
      var raw=e.postData.contents; var looksJson=/^\s*[\{\[]/.test(raw||'');
      var b=looksJson?JSON.parse(raw||'{}'):(e.parameter||{}); return b.ids||'';
    }catch(_){return '';} })()) || ''
  ).trim();

  var msisdn = String(
    (e && e.parameter && e.parameter.msisdn) ||
    (e && e.postData && e.postData.contents && (function(){try{
      var raw=e.postData.contents; var looksJson=/^\s*[\{\[]/.test(raw||'');
      var b=looksJson?JSON.parse(raw||'{}'):(e.parameter||{}); return b.msisdn||'';
    }catch(_){return '';} })()) || ''
  ).trim();

  var token = String(
    (e && e.parameter && e.parameter.t) ||
    (e && e.postData && e.postData.contents && (function(){try{
      var raw=e.postData.contents; var looksJson=/^\s*[\{\[]/.test(raw||'');
      var b=looksJson?JSON.parse(raw||'{}'):(e.parameter||{}); return b.t||'';
    }catch(_){return '';} })()) || ''
  ).trim();

  __mark('ack_all_embed:entry', { count: idsCsv ? idsCsv.split(',').filter(Boolean).length : 0, msisdn_masked: __maskMsisdn(msisdn) });

  var v = __verifyBulk(idsCsv, msisdn || null, token);
  if (!v.ok) {
    __mark('ack_all_embed:token_fail', { reason: v.reason || 'VERIFY_FAIL' });
    return __jsonp(e, { type:'ACK_ALL_RESULT', payload:{ ok:false, error:'INVALID_OR_EXPIRED_LINK' } }, 403);
  }

  var ids = idsCsv.split(',').map(function(s){ return String(s || '').trim(); }).filter(Boolean);
  var responderName = __resolveResponder((typeof _normaliseUkMsisdnTo447==='function')?_normaliseUkMsisdnTo447(msisdn):msisdn) || 'Web link (bulk)';

  var lock = LockService.getScriptLock();
  try { lock.waitLock(5000); } catch (_) {
    __mark('ack_all_embed:lock_fail', {});
    return __jsonp(e, { type:'ACK_ALL_RESULT', payload:{ ok:false, error:'BUSY' } }, 503);
  }

  var ackedIds = [];
  try {
    ids.forEach(function(id){
      try {
        var a = _emergencyFindById(id);
        if (!a) { __mark('ack_all_embed:skip_missing', { id:id }); return; }
        if (a.acknowledged === true || String(a.acknowledged) === 'true') { __mark('ack_all_embed:skip_already', { id:id }); return; }

        _emergencyUpdateById(id, {
          acknowledged: true,
          status: 'ACKED',
          ack_source: 'CTA_ALL_EMBED',
          ack_name: responderName,
          ack_number: String(msisdn || ''),
          ack_when_iso: _nowIso()
        });

        var fresh = _emergencyFindById(id);
        try { _cancelTriggersForAlert(fresh); } catch(_) {}
        try { _eaAppendLog_(id, 'ACK via CTA ALL EMBED'); } catch(_) {}
        __mark('ack_all_embed:acked', { id:id });
        ackedIds.push(id);

      } catch (innerEx) {
        __mark('ack_all_embed:item_exception', { id:id, error: String(innerEx && innerEx.message || innerEx) });
      }
    });

    if (ackedIds.length > 0) {
      try { __sendBulkResponderConfirm(msisdn, responderName); } catch (_) {}
      try {
        var whenIso = _nowIso();
        __sendBulkAllAlertsRespondedToOthers(responderName, whenIso, msisdn);
      } catch (_) {}
    } else {
      __mark('ack_all_embed:no_new_acks', { count: ids.length });
    }

    __mark('ack_all_embed:done', { count: ids.length, ackedCount: ackedIds.length });
    return __jsonp(e, { type:'ACK_ALL_RESULT', payload:{ ok:true, ackedCount: ackedIds.length, ids: ackedIds } }, 200);

  } catch (ex) {
    __mark('ack_all_embed:exception', { error: String(ex && ex.message || ex) });
    return __jsonp(e, { type:'ACK_ALL_RESULT', payload:{ ok:false, error:'EXCEPTION', detail:String(ex && ex.message || ex) } }, 500);

  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}

// ───────────────────────────────────────────────────────────────
  // TIMESHEET AUTH WEBHOOK (broker → availability sheet)
  // Body shape (JSON): { booking_id: "bk_...", authorised: true|false, timesheet_id?: "...", idempotency_key?: "..." }
  // Note: Broker sends an X-Signature header we can't read in Apps Script; we trust allow-listing the URL.
  // ───────────────────────────────────────────────────────────────
  try {
    const looksTimesheetWebhook =
      (ctype && ctype.indexOf('application/json') === 0) &&
      body && typeof body === 'object' &&
      typeof body.booking_id === 'string' &&
      typeof body.authorised !== 'undefined';

    if (looksTimesheetWebhook) {
      const bookingId   = String(body.booking_id || '').trim();
      const authorised  = (String(body.authorised).toLowerCase() === 'true' || body.authorised === true);
      const timesheetId = body.timesheet_id ? String(body.timesheet_id) : '';

      __log('ts_webhook:arrival', {
        booking_id_present: !!bookingId,
        authorised,
        timesheet_id_present: !!timesheetId
      });

      if (bookingId) {
        __tsAuthUpsert(bookingId, authorised, timesheetId);
        __log('ts_webhook:stored', { booking_id: bookingId, authorised });
        // return simple 200 to stop retries
        return __simpleText('OK', 200);
      } else {
        __log('ts_webhook:bad_payload', { body_preview: JSON.stringify(body).slice(0, 200) });
        return __simpleText('BAD_PAYLOAD', 400);
      }
    }
  } catch (exTs) {
    __log('ts_webhook:error', { error: String(exTs && exTs.message || exTs) });
    return __simpleText('ERROR', 500);
  }


    // ---- Not a webhook action: enforce token as before ----
    if (!WEBHOOK_ACTIONS.has(__action)) {
      __mark('enforceToken:start');
      _enforceToken(e);
      __mark('enforceToken:end');
    }


    // --------------------------- existing routes (unchanged) ---------------------------

    // Helper to resolve msisdn and emit the correct error shape (unchanged)
    function _requireMsisdn(ctx) {
      __mark('resolveIdentity:start');
      const m = _resolveMsisdnFromRequest(ctx);
      __mark('resolveIdentity:end', { got: !!m });
      if (m) { __msisdnSeen = m; return m; }
      if (__hadK) return { __error: ERR("UNAUTHORIZED_TOKEN", 401, { hadK: true }) };
      return { __error: ERR("INVALID_OR_UNKNOWN_IDENTITY", 400) };
    }

    if (body && (body.action || paramsLC['action'])) {

      // ================== RUNNING LATE — options ==================
      if (__action === 'RUNNING_LATE_OPTIONS') {
        const ms = _requireMsisdn(body); if (ms && ms.__error) return ms.__error;
        const msisdn = ms;
        if (typeof _runningLateResolveEligibility !== 'function') {
          __log('rl_options:feature_not_deployed', { msisdn_masked: __maskMsisdn(msisdn) });
          return ERR("FEATURE_NOT_DEPLOYED", 501);
        }
        const s = body.shift || body;
        const ymd         = String(s.ymd || '');
        const shift_type  = String(s.shift_type || '');
        const hospital    = String(s.hospital || '');
        const ward        = String(s.ward || '');
       const job_title = String(s.job_title || s.jobTitle || '');
const jobTitle  = job_title; // alias, so both spellings resolve

        const booking_ref = String(s.booking_ref || s.bookingRef || '');
        const shiftInfo   = String(s.shiftInfo || '');
        __log('rl_options:start', {
          msisdn_masked: __maskMsisdn(msisdn), ymd, shift_type, hospital, ward,
          job_title, booking_ref_masked: booking_ref ? (booking_ref.slice(0,3)+'•••') : '', has_shiftInfo: !!shiftInfo
        });
        const res = _runningLateResolveEligibility({ ymd, shift_type, hospital, ward, job_title, booking_ref, shiftInfo, msisdn });
        __log('rl_options:eligibility', { eligible: !!(res && res.eligible), reason: res && res.reason || '', startAtIso: res && res.startAtIso || null, endAtIso: res && res.endAtIso || null });

        // --- UPDATED: dynamic option generation if shift already started ---
        const baseBuckets = [15, 30, 60, 120, 180];
        const nowIso = (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString();
        let alreadyLateRounded = 0;
        try {
          if (res && res.startAtIso) {
            const now = new Date(nowIso);
            const start = new Date(res.startAtIso);
            const nms = now.getTime();
            const sms = start.getTime();
            if (isFinite(nms) && isFinite(sms) && nms > sms) {
              const diffMin = Math.ceil((nms - sms) / 60000);
              alreadyLateRounded = Math.ceil(diffMin / 5) * 5; // round up to nearest 5
            }
          }
        } catch (_) { alreadyLateRounded = 0; }

       function labelFor(minutes) {
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;

  // If under 1 hour, keep minutes-only
  if (h < 1) {
    return `Less than ${minutes} minute${minutes === 1 ? '' : 's'}`;
  }

  // Otherwise, use "hours and minutes" (drop minutes part when zero)
  const hoursPart = `${h} hour${h === 1 ? '' : 's'}`;
  const minsPart  = m > 0 ? ` and ${m} minute${m === 1 ? '' : 's'}` : '';
  return `Less than ${hoursPart}${minsPart}`;
}

// Helper: compute HH:MM local arrival time for a given minutes option
function arriveByHHmm(minutes) {
  try {
    const tz = (typeof _P === 'function' && _P().TZ) ? _P().TZ : 'Europe/London';
    const startAtIso = res && res.startAtIso;
    const nowIsoUse = (typeof _nowIso === 'function') ? _nowIso() : nowIso;

    let arrival;
    if (typeof _runningLateComputeArrivalBy === 'function' && startAtIso) {
      arrival = _runningLateComputeArrivalBy({ startAtIso, minutes, nowIso: nowIsoUse, tz });
    } else {
      // Fallback: now + minutes
      const ms = new Date(nowIsoUse).getTime() + minutes * 60000;
      arrival = new Date(ms);
    }
    return Utilities.formatDate(new Date(arrival), tz, 'HH:mm');
  } catch (_) {
    return '';
  }
}

const minuteOptions = (alreadyLateRounded > 0)
  ? baseBuckets.map(b => alreadyLateRounded + b)
  : baseBuckets.slice();

const tiles = minuteOptions.map(m => {
  const hhmm = arriveByHHmm(m);
  const base = labelFor(m);
  const label = hhmm ? `${base} (arrive by ${hhmm} at the latest)` : base;
  return { label, minutes: m, arrive_by_label: hhmm };
});


        const resp = OK({
          ok: true,
          eligible: !!res.eligible,
          reason: res.reason || "",
          context: {
            ymd, shift_type, hospital, ward, job_title, booking_ref, shiftInfo,
            startAtIso: res.startAtIso || null,
            endAtIso:   res.endAtIso   || null,
            time_range_label: String(s.time_range_label || body.time_range_label || '') ||
              (typeof _formatTimeRangeLabel === 'function' && res.startAtIso && res.endAtIso ? _formatTimeRangeLabel(res.startAtIso, res.endAtIso, (typeof _P==='function' && _P().TZ) || 'Europe/London') : '')
          },
          options: tiles,
          tiles
        });
        __log('rl_options:ok', { returned_tiles: tiles.length });
        return resp;
      }

      // ================== RUNNING LATE — preview ==================
      if (__action === 'RUNNING_LATE_PREVIEW') {
        const ms = _requireMsisdn(body); if (ms && ms.__error) return ms.__error;
        const msisdn = ms;
        const P  = (typeof _P === 'function') ? _P() : { TZ: 'Europe/London' };
        const tz = P.TZ || 'Europe/London';
        const ctx = body && body.context ? body.context : {};
        const etaLabelIn = body.eta_label || body.eta || body.late_by_label || null;
        let minutes = Number(body.minutes || 0);
        if (!minutes && etaLabelIn) minutes = _etaLabelToMinutes(etaLabelIn) || 0;
        minutes = Math.max(1, minutes);
        if (typeof _runningLateResolveEligibility !== 'function' || typeof _runningLateComputeArrivalBy !== 'function') {
          __log('rl_preview:feature_not_deployed', { msisdn_masked: __maskMsisdn(msisdn) });
          return ERR("FEATURE_NOT_DEPLOYED", 501);
        }
        __log('rl_preview:start', {
          msisdn_masked: __maskMsisdn(msisdn), minutes, eta_label_in: etaLabelIn || null,
          ctx_summary: {
            ymd: String(ctx.ymd || ''), shift_type: String(ctx.shift_type || ''), hospital: String(ctx.hospital || ''),
            ward: String(ctx.ward || ''), job_title: String(ctx.job_title || ''), booking_ref_masked: ctx.booking_ref ? (String(ctx.booking_ref).slice(0,3)+'•••') : '', has_shiftInfo: !!ctx.shiftInfo
          }
        });
        const elig = _runningLateResolveEligibility({
          ymd: String(ctx.ymd || ''), shift_type: String(ctx.shift_type || ''), hospital: String(ctx.hospital || ''), ward: String(ctx.ward || ''),
          job_title: String(ctx.job_title || ''), booking_ref: String(ctx.booking_ref || ''), shiftInfo: String(ctx.shiftInfo || ''), msisdn
        });
        __log('rl_preview:eligibility', { eligible: !!(elig && elig.eligible), reason: elig && elig.reason || '' });
        if (!elig.eligible) {
          __log('rl_preview:not_eligible', { reason: String(elig.reason || 'NOT_ELIGIBLE') });
          return ERR(String(elig.reason || 'NOT_ELIGIBLE'), 400);
        }
       const arrivalBy = _runningLateComputeArrivalBy({
  // Prefer the value the FE already received with OPTIONS to avoid drift
  startAtIso: String(ctx.startAtIso || elig.startAtIso || ''), minutes,
  nowIso: (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString(), tz
});

        const arrivalLabel = _formatHHmmHrs(arrivalBy, tz);
        __log('rl_preview:arrival', { arrivalByIso: (new Date(arrivalBy)).toISOString(), arrivalLabel });
        const previewText = "We will inform all your colleagues on shift that you are running late and will arrive no later than " + arrivalLabel + " and provide them with your contact mobile number.";
        const resp = OK({
          ok: true,
          previewHtml: previewText,
          preview: { text: previewText, arrival_by_label: arrivalLabel },
          display: { masked_msisdn: __maskMsisdn(msisdn) },
          context: Object.assign({}, ctx, { startAtIso: ctx.startAtIso || elig.startAtIso || null, endAtIso: ctx.endAtIso || elig.endAtIso || null }),

          minutes
        });
        __log('rl_preview:ok', { arrival_by_label: arrivalLabel });
        return resp;
      }

      // ================== RUNNING LATE — send ==================
      if (__action === 'RUNNING_LATE_SEND') {
        const ms = _requireMsisdn(body); if (ms && ms.__error) return ms.__error;
        const msisdn = ms;
        const P  = (typeof _P === 'function') ? _P() : { TZ: 'Europe/London' };
        const tz = P.TZ || 'Europe/London';
        const ctx = body && body.context ? body.context : {};
        const etaLabelIn = body.eta_label || body.eta || body.late_by_label || null;
        let minutes = Number(body.minutes || 0);
        if (!minutes && etaLabelIn) minutes = _etaLabelToMinutes(etaLabelIn) || 0;
        minutes = Math.max(1, minutes);
        if (typeof _runningLateResolveEligibility !== 'function' ||
            typeof _runningLateComputeArrivalBy !== 'function' ||
            typeof _runningLateFindCohortsFromEmailHistory !== 'function' ||
            typeof _runningLateSendNotifications !== 'function') {
          __log('rl_send:feature_not_deployed', { msisdn_masked: __maskMsisdn(msisdn) });
          return ERR("FEATURE_NOT_DEPLOYED", 501);
        }
        __log('rl_send:start', {
          msisdn_masked: __maskMsisdn(msisdn), minutes, eta_label_in: etaLabelIn || null,
          ctx_summary: {
            ymd: String(ctx.ymd || ''), shift_type: String(ctx.shift_type || ''), hospital: String(ctx.hospital || ''), ward: String(ctx.ward || ''),
            job_title: String(ctx.job_title || ''), booking_ref_masked: ctx.booking_ref ? (String(ctx.booking_ref).slice(0,3)+'•••') : '', has_shiftInfo: !!ctx.shiftInfo
          }
        });
        const elig = _runningLateResolveEligibility({
          ymd: String(ctx.ymd || ''), shift_type: String(ctx.shift_type || ''), hospital: String(ctx.hospital || ''), ward: String(ctx.ward || ''),
          job_title: String(ctx.job_title || ''), booking_ref: String(ctx.booking_ref || ''), shiftInfo: String(ctx.shiftInfo || ''), msisdn
        });
        if (!elig.eligible) {
          __log('rl_send:not_eligible', { reason: String(elig.reason || 'NOT_ELIGIBLE') });
          return ERR(String(elig.reason || 'NOT_ELIGIBLE'), 400);
        }
        const arrivalBy = _runningLateComputeArrivalBy({
  startAtIso: String(ctx.startAtIso || elig.startAtIso || ''), minutes,
  nowIso: (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString(), tz
});

        const arrivalLabel = _formatHHmmHrs(arrivalBy, tz);
        const date_label = (typeof _formatDateLabel === 'function' && ctx.ymd) ? _formatDateLabel(ctx.ymd, tz) : (body.date_label || '');
        const time_range_label =
          String(ctx.time_range_label || body.time_range_label || '') ||
          (typeof _formatTimeRangeLabel === 'function' && elig.startAtIso && elig.endAtIso
            ? _formatTimeRangeLabel(elig.startAtIso, elig.endAtIso, tz)
            : '');
        let candidateName = '';
        let candidateRole = String(ctx.job_title || '').toUpperCase();
        try {
          const ss = _ssRota();
          const c  = _findCandidateByMobile(ss, msisdn);
          if (c) candidateName = ((c.firstname || '') + ' ' + (c.surname || '')).trim();
        } catch (_) {}
        __log('rl_send:cohorts:resolve:start', {});
        const cohorts = _runningLateFindCohortsFromEmailHistory({
          ymd: String(ctx.ymd || ''),
          shift_type: String(ctx.shift_type || ''),
          hospital: String(ctx.hospital || ''),
          ward: String(ctx.ward || ''),
          msisdn,
          candidate_name: candidateName,

          // canonical hints for accurate shift matching
          shift_start_iso: String(ctx.startAtIso || elig.startAtIso || ''),
shift_end_iso:   String(ctx.endAtIso   || elig.endAtIso   || ''),

          time_range_label:
            String(ctx.time_range_label || body.time_range_label || '') ||
            (typeof _formatTimeRangeLabel === 'function' && elig.startAtIso && elig.endAtIso
              ? _formatTimeRangeLabel(elig.startAtIso, elig.endAtIso, tz)
              : ''),
          // if you carry raw shift label like "NIGHT" in shiftInfo, pass it through
          shift_type_raw: String((ctx.shiftInfo || '')).toUpperCase()
        }) || { sameShift: [], previousShift: [], candidate_msisdn_07: '' };

        __log('rl_send:cohorts:resolved', {
          sameShift_count: (cohorts.sameShift || []).length,
          previousShift_count: (cohorts.previousShift || []).length
        });
        const model = {
          // keep E.164 for transport and add local 07 for display/templates
          candidate_msisdn_447: _normaliseUkMsisdnTo447(msisdn),
          candidate_msisdn_07: String(cohorts && cohorts.candidate_msisdn_07 || ''),

          candidate_name: candidateName || 'A colleague',
          role: candidateRole || '',
          hospital: String(ctx.hospital || ''),
          ward: String(ctx.ward || ''),
          date_label: String(date_label || ''),
          shift_type: String(ctx.shift_type || ''),
          time_range_label: String(time_range_label || ''),
          late_by_minutes: minutes,
          late_by_label: (minutes < 60 ? `Less than ${minutes} minutes` : `Less than ${Math.round(minutes/60)} hour${minutes>=120?'s':''}`),
          arrival_by_label: arrivalLabel,

          // 🔑 Normalise cohorts to E.164 before handing off
          sameShift: __normaliseCohortList(cohorts.sameShift),
          previousShift: __normaliseCohortList(cohorts.previousShift),

          notifyEmergencyContacts: true,
          placeVoiceCalls: false
        };

        // --- helper added just above dispatch ---
        function __to447(x){
          try { return _normaliseUkMsisdnTo447(String(x||'')); } catch (_) { return ''; }
        }
        function __normaliseCohortList(arr){
          return (Array.isArray(arr)?arr:[]).map(p => {
            const out = Object.assign({}, p);
            if (out.msisdn447) out.msisdn447 = __to447(out.msisdn447);
            if (out.msisdn)    out.msisdn    = __to447(out.msisdn);
            if (out.tel)       out.tel       = __to447(out.tel);
            return out;
          }).filter(p => /^(447\d{9})$/.test(p.msisdn447 || p.msisdn || p.tel || ''));
        }

        __log('rl_send:dispatch:start', {
          model_redacted: Object.assign({}, model, {
            candidate_msisdn_447: model.candidate_msisdn_447 ? (String(model.candidate_msisdn_447).slice(0,5)+'•••••'+String(model.candidate_msisdn_447).slice(-2)) : '',
          }),
          policyFlags: { notifyEmergencyContacts: true, placeVoiceCalls: false }
        });
        const dispatch = _runningLateSendNotifications(model);
        __log('rl_send:dispatch:result', {
          sameShift: (dispatch && dispatch.sameShift) || { attempted: 0, success: 0 },
          previousShift: (dispatch && dispatch.previousShift) || { attempted: 0, success: 0 },
          emergencyContacts: (dispatch && dispatch.emergencyContacts) || { attempted: 0, success: 0 }
        });
        const resp = OK({
          ok: true,
          sent: {
            sameShift: (dispatch && dispatch.sameShift) || { attempted: 0, success: 0 },
            previousShift: (dispatch && dispatch.previousShift) || { attempted: 0, success: 0 },
            emergencyContacts: (dispatch && dispatch.emergencyContacts) || { attempted: 0, success: 0 }
          },
          tokens: {
            Arrival_by_label: arrivalLabel,
            Late_by_label: model.late_by_label,
            Candidate_name: model.candidate_name,
            Role: model.role,
            Hospital: model.hospital,
            Ward: model.ward,
            Date_label: model.date_label,
            Shift_type: model.shift_type,
            // expose 07 for UI/template previews
            Candidate_msisdn: model.candidate_msisdn_07 || ''
          }

        });
        __log('rl_send:ok', {});
        return resp;
      }
// ================== EMERGENCY – raise alert ==================
if (__action === 'EMERGENCY_RAISE') {
  if (typeof _emergencyCreateRecord !== 'function' || typeof _emergencySendInitialNotifications !== 'function') {
    __log('emergency_raise:feature_not_deployed', {});
    return ERR("FEATURE_NOT_DEPLOYED", 501);
  }

  const ms = _requireMsisdn(body); if (ms && ms.__error) return ms.__error;
  const msisdn = ms; __msisdnSeen = msisdn;

  try {
    const cfg = _getEmergencyProps();
    if (cfg.TEST_MODE && cfg.TEST_PHONE && _normaliseUkMsisdnTo447(msisdn) !== cfg.TEST_PHONE) {
      __log('emergency_raise:test_gate_forbidden', { msisdn_masked: __maskMsisdn(msisdn), test_mode: !!cfg.TEST_MODE });
      return ERR("FORBIDDEN_TEST_MODE", 403);
    }
  } catch(_) {}

  try {
    const s = body.shift || body;
    const i = body.issue || body;

    // ---- NORMALISE INPUTS FROM CLIENT ----
    const ymd        = String(s.ymd || '');
    const shift_type = String(s.shift_type || s.shift_type_raw || '');
    const hospital   = String(s.hospital || '');
    const ward       = String(s.ward || '');
    const job_title  = String(s.job_title || s.jobTitle || '');
    const jobTitle   = job_title; // alias, for any downstream camelCase usage
    const bookingRef = String(s.booking_ref || s.bookingRef || '');
    const shiftInfo  = String(s.shiftInfo || '');

    // Prefer explicit DNA eventType if present
    const eventTypeIn = String(body.eventType || body.event_type || '').toUpperCase();
    let issue_type    = String(i.type || i.issue_type || '').toUpperCase();
    if (eventTypeIn === 'DNA') issue_type = 'DNA';

    // Reason + DNA subject fields
    const reason = String(
      (i.reason_text ?? i.reason ?? body.reason_text ?? body.reason ?? '')
    ).trim();
    const subjectNameIn   = String(i.subject_name   || body.subject_name   || '');
    const subjectMsisdnIn = _normaliseUkMsisdnTo447(i.subject_msisdn || body.subject_msisdn || '');

    // ETA/leave label (normalised)
    const etaLabelIn = String(i.eta_or_leave_time_label || i.eta_or_leave_time || '');
    const etaLabel   = _normaliseEtaOrLeaveTimeLabel(etaLabelIn);

    // Resolve concrete times if missing (align with reader)
    const tz = (typeof _P === 'function' && _P().TZ) ? _P().TZ : 'Europe/London';
    let startAtIso = String(s.startAtIso || s.startIso || '');
    let endAtIso   = String(s.endAtIso   || s.endIso   || '');
    let resolved   = null;
    if ((!startAtIso || !endAtIso) && typeof _runningLateResolveShiftTimes === 'function') {
      resolved   = _runningLateResolveShiftTimes({ ymd, shift_type, shiftInfo, tz }) || null;
      startAtIso = startAtIso || (resolved && resolved.startAtIso) || '';
      endAtIso   = endAtIso   || (resolved && resolved.endAtIso)   || '';
    }

    const time_range_label =
      String(s.time_range_label || body.time_range_label || '') ||
      (startAtIso && endAtIso && typeof _formatTimeRangeLabel === 'function'
        ? _formatTimeRangeLabel(startAtIso, endAtIso, tz)
        : '');

    // Reporter name: try body first; fall back to lookup
    let reporterName = String(body.reporter_name || body.reporterName || '').trim();
    if (!reporterName) {
      try {
        const ss = _ssRota();
        const cand = _findCandidateByMobile(ss, msisdn);
        if (cand) reporterName = ((cand.firstname || '') + ' ' + (cand.surname || '')).trim();
      } catch (_) {}
    }

    // 🔧 NEW: resolve candidate identity (the reporter is the candidate)
    let candidateName = '';
    let candidateMsisdn447 = '';
    let candidateMsisdn07  = '';
    try {
      candidateMsisdn447 = _normaliseUkMsisdnTo447(msisdn);
      candidateMsisdn07  = candidateMsisdn447 ? ('0' + candidateMsisdn447.slice(2)) : '';
    } catch (_) {}
    try {
      const ss = _ssRota();
      const c  = _findCandidateByMobile(ss, msisdn);
      if (c) candidateName = ((c.firstname || '') + ' ' + (c.surname || '')).trim();
    } catch (_) {}

    __log('emergency_raise:start', {
      issue: {
        type: issue_type,
        eta_or_leave_time_label_in: etaLabelIn,
        eta_or_leave_time_label: etaLabel,
        has_reason: !!reason
      },
      candidate_name_present: !!candidateName
    });

    // ✅ Build payload for record creation (now includes candidate fields)
    const payload = {
      reporter_name: reporterName || '',
      candidate_name: candidateName || 'A colleague',
      candidate_role: String(job_title || ''),
      candidate_msisdn_447: candidateMsisdn447 || '',
      candidate_msisdn_07:  candidateMsisdn07  || '',
      shift: {
        ymd, hospital, ward, job_title, booking_ref: bookingRef, shift_type, shiftInfo,
        startAtIso_resolved: startAtIso || undefined,
        endAtIso_resolved:   endAtIso   || undefined,
        time_range_label:    time_range_label || undefined
      },
      issue: {
        type: issue_type,
        eta_or_leave_time_label: etaLabel,
        reason_text: reason,
        subject_name:   issue_type === 'DNA' ? subjectNameIn   : '',
        subject_msisdn: issue_type === 'DNA' ? subjectMsisdnIn : ''
      }
    };

    // Create + notify
    const created = _emergencyCreateRecord(msisdn, payload);
    __log('emergency_raise:created', { ok: !!(created && created.alert_id), alert_id: (created && created.alert_id) || null, reporter_name: reporterName });
    if (!created || !created.alert_id) return ERR("EMERGENCY_CREATE_FAILED", 500);

    const notify = _emergencySendInitialNotifications(created);
    __log('emergency_raise:initial_notifications', { alert_id: created.alert_id, reporter_name: reporterName, notify });

    // ───────────────────── exclusions ─────────────────────
    try {
      
      
      

      const issueU = __U(issue_type);

      // Build sigBase (shift-only) + timeKey
      const timeKey = (startAtIso && endAtIso) ? (startAtIso + '|' + endAtIso) : String(time_range_label || '');
      const canonicalShiftType = (resolved && resolved.type) ? String(resolved.type) : __canonicalShiftTypeLocal(shift_type);
      const sigBase = __sigBaseFromItem({
  ymd,
  hospital,
  ward,
  job_title,
  shift_type,                 // raw; helper will canonicalise
  startAtIso_resolved: startAtIso || undefined,
  endAtIso_resolved:   endAtIso   || undefined,
  time_range_label:    time_range_label || ''
});

      // Expiry = shift end + 3 days (fallback: end-of-day)
      function __iso(d){ try { return new Date(d).toISOString(); } catch(_) { return new Date().toISOString(); } }
      let endIsoForTtl = endAtIso;
      if (!endIsoForTtl && ymd) {
        const [Y,M,D] = String(ymd || '').split('-').map(Number);
        const localEnd = new Date(Y || 1970, (M||1)-1, D || 1, 23, 59, 59, 999);
        endIsoForTtl = __iso(localEnd);
      }
      const expMs  = (new Date(endIsoForTtl || new Date())).getTime() + 3*24*60*60*1000;
      const expIso = new Date(expMs).toISOString();

      const props = PropertiesService.getScriptProperties();
      const lock  = LockService.getScriptLock();
      try { lock.waitLock(5000); } catch(_) {}

      if (issueU === 'RUNNING_LATE') {
        // skip writing exclusions for running-late
        __log('emergency_raise:exclude_skip', { reason: 'RUNNING_LATE', note: 'no exclusion written' });
      } else if (issueU === 'DNA') {
        // ✅ NEW: shift-scoped DNA subject exclusion (visible to all on this exact shift)
     
// ── DNA subject exclusion (drop-in replacement) ────────────────────────────────
const subjName = String(subjectNameIn || '').trim();
const subj447  = (typeof _normaliseUkMsisdnTo447 === 'function')
  ? _normaliseUkMsisdnTo447(subjectMsisdnIn || '')
  : String(subjectMsisdnIn || '').replace(/[^\d]/g,''); // ultra-safe fallback
const subjectKey = _dnaSubjectKeyFromInputs(subjName, subj447); // ← global helper

const keyShift = 'EA_EXC_SHIFT::' + sigBase;
let mapShift = {};
try { mapShift = JSON.parse(props.getProperty(keyShift) || '{}') || {}; } catch(_) { mapShift = {}; }

// purge expired entries
const nowMs = Date.now();
Object.keys(mapShift).forEach(k => {
  const it = mapShift[k] || {};
  const expK = new Date(it.exp || 0).getTime();
  if (!isFinite(expK) || expK < nowMs) delete mapShift[k];
});

// upsert subject entry (store canonical 447 for matching)
mapShift[subjectKey] = {
  key: subjectKey,
  name: subjName,
  msisdn447: /^447\d{9}$/.test(subj447) ? subj447 : '',
  exp: expIso,
  aid: (created && created.alert_id) || '',
  at:  (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString()
};

props.setProperty(keyShift, JSON.stringify(mapShift));
__log('emergency_raise:dna_exclude_upsert', {
  key: keyShift,
  subject_key: subjectKey,
  sig_preview: sigBase.slice(0,60) + (sigBase.length>60?'…':''),
  exp: expIso
});
// ───────────────────────────────────────────────────────────────────────────────

      } else {
        // Existing per-person exclusions for non-DNA issues (EA_EXC::<msisdn447>)
        const ms447 = (typeof _normaliseUkMsisdnTo447 === 'function')
          ? _normaliseUkMsisdnTo447(msisdn)
          : String(msisdn || '').replace(/[^\d]/g,'');

        const sig = [ sigBase, issueU ].join('|'); // shift + issue
        const key = 'EA_EXC::' + String(ms447 || '');

        let map = {};
        try { map = JSON.parse(props.getProperty(key) || '{}') || {}; } catch(_) { map = {}; }

        // purge expired
        const nowMs = Date.now();
        Object.keys(map).forEach(k => {
          const it = map[k] || {};
          const expK = new Date(it.exp || 0).getTime();
          if (!isFinite(expK) || expK < nowMs) delete map[k];
        });

        // upsert exclusion for this issue on this shift (per reporter)
        if (sig) {
          map[sig] = {
            sig,
            sig_base: sigBase,
            issue: issueU,
            end: endIsoForTtl || '',
            exp: expIso,
            aid: (created && created.alert_id) || '',
            at:  (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString()
          };
        }

        props.setProperty(key, JSON.stringify(map));

        __log('emergency_raise:exclude_upsert', {
          key,
          issue: issueU,
          sig_preview: sig ? (sig.slice(0, 60) + (sig.length > 60 ? '…' : '')) : '',
          exp: expIso
        });
      }

      try { lock.releaseLock(); } catch(_) {}
    } catch (exUp) {
      __log('emergency_raise:exclude_upsert_error', { error: String(exUp && exUp.message || exUp) });
    }
    // ──────────────────── END exclusions ────────────────────

    // ✅ success
    return OK({ ok: true, alert_id: created.alert_id });
  } catch (eRaise) {
    __log('emergency_raise:exception', { error: String(eRaise && eRaise.message || eRaise) });
    return ERR(String(eRaise && eRaise.message || 'EMERGENCY_RAISE_EXCEPTION'), 500);
  }
} // ← closes: if (__action === 'EMERGENCY_RAISE')


      // ================== EMERGENCY – manual/cron escalation kick ==================
      if (__action === 'EMERGENCY_ESCALATE_CRON') {
        if (typeof emergencyEscalate !== 'function') {
          __log('emergency_escalate:feature_not_deployed', {});
          return ERR("FEATURE_NOT_DEPLOYED", 501);
        }
        try {
          __log('emergency_escalate:start', {});
          const results = emergencyEscalate();
          __log('emergency_escalate:results', { results_summary: results });
          return OK({ ok: true, results });
        } catch (e1) {
          __log('emergency_escalate:exception', { error: String(e1 && e1.message || e1) });
          return ERR(String(e1 && e1.message || 'EMERGENCY_ESCALATE_EXCEPTION'), 500);
        }
      }

      // ================== WATI webhook (ACK quick-reply) ==================
      if (__action === 'EMERGENCY_WEBHOOK_WATI') {
        if (typeof hookWatiAck !== 'function') {
          __log('wati_webhook:feature_not_deployed', {});
          return ERR("FEATURE_NOT_DEPLOYED", 501);
        }
        const props = (typeof _getEmergencyProps === 'function') ? _getEmergencyProps() : null;
        const expected = props && props.WEBHOOK_SECRET;
        const provided =
          (body && body.secret) ||
          (e && e.parameter && e.parameter.secret) ||
          (e && e.postData && e.postData.type === 'application/json' && body && body.EMERGENCY_WEBHOOK_SECRET) ||
          (e && e.parameter && e.parameter.EMERGENCY_WEBHOOK_SECRET);
        if (!expected || !provided || String(expected) !== String(provided)) {
          __log('wati_webhook:unauthorized', { hasExpected: !!expected, hasProvided: !!provided });
          return ERR("UNAUTHORIZED_WEBHOOK", 401);
        }
        try {
          __log('wati_webhook:authorized_invoke', {});
          const out = hookWatiAck(e);
          __log('wati_webhook:handler_returned', { ok: true });
          return out;
        } catch (e1) {
          __log('wati_webhook:exception', { error: String(e1 && e1.message || e1) });
          return ERR(String(e1 && e1.message || 'WATI_WEBHOOK_EXCEPTION'), 500);
        }
      }

      // ================== ClickSend Voice webhook (secondary guard) ==================
      if (__action === 'EMERGENCY_WEBHOOK_CLICKSEND') {
        if (typeof hookClickSendVoice !== 'function') {
          __log('clicksend_webhook:feature_not_deployed', {});
          return __simpleText('OK', 200);
        }
        try {
          const out = hookClickSendVoice(e);
          __log('clicksend_webhook:handler_returned', { ok: true });
          return out && typeof out.getContent === 'function' ? out : __simpleText('OK', 200);
        } catch (e1) {
          __log('clicksend_webhook:exception', { error: String(e1 && e1.message || e1) });
          return __simpleText('OK', 200);
        }
      }

      // ======== Email + Password login ========
      if (__action === "AUTH_LOGIN") {
        const email = String(body.email || "").trim().toLowerCase();
        const password = String(body.password || "");
        __emailSeen = email;
        if (!email || !password) return ERR("MISSING_CREDENTIALS", 400);

        __mark('findLinksByEmail:start');
        const rec = _findLinksRowByEmail(email);
        __mark('findLinksByEmail:end', { found: !!rec });
        if (!rec) return ERR("INVALID_CREDENTIALS", 401);

        const hash = String(rec.row[rec.m.PWH] || "");
        const salt = String(rec.row[rec.m.PWS] || "");
        const tel  = _normaliseLocalTel(String(rec.row[rec.m.TEL] || ""));
        const active = String(rec.row[rec.m.STA] || "") === "Active";
        if (!tel || !active || !hash || !salt) {
          return ERR("INVALID_CREDENTIALS", 401);
        }

        __mark('passwordVerify:start');
        const ok = _verifyPassword(password, salt, hash);
        __mark('passwordVerify:end', { ok: !!ok });
        if (!ok) return ERR("INVALID_CREDENTIALS", 401);

        const msisdn = tel;
        __msisdnSeen = msisdn;

        try {
          if (rec.m.NEW >= 0 && String(rec.row[rec.m.NEW] || "").toUpperCase() !== "NO") {
            __mark('sheetWrite:newUserFlag:start');
            rec.sh.getRange(rec.rowIndex, rec.m.NEW + 1).setValue("NO");
            __mark('sheetWrite:newUserFlag:end');
          }
        } catch(_) {}

        let candidateName = "";
        try {
          const first = rec.m.FIR >= 0 ? String(rec.row[rec.m.FIR] || "").trim() : "";
          const surname = rec.m.SUR >= 0 ? String(rec.row[rec.m.SUR] || "").trim() : "";
          candidateName = (first + " " + surname).trim();
        } catch(_) {}

        return OK({ ok: true, msisdn, candidateName });
      }

      // ======== Forgot Password ========
      if (__action === "AUTH_FORGOT_PASSWORD") {
        const email = String(body.email || "").trim().toLowerCase();
        __emailSeen = email;
        if (!email) return ERR("MISSING_EMAIL", 400);

        __mark('findLinksByEmail:start');
        const rec = _findLinksRowByEmail(email);
        __mark('findLinksByEmail:end', { found: !!rec });

        if (!rec) return OK({ ok: true });

        const tel  = _normaliseLocalTel(String(rec.row[rec.m.TEL] || ""));
        const active = String(rec.row[rec.m.STA] || "") === "Active";
        if (!tel || !active) return OK({ ok: true });

        const resetToken = _generateResetToken();
        const expiresIso = _nowPlusMinutesIso(30);
        const url = _buildUrlForToken(resetToken);

        __mark('sheetWrite:storeReset:start');
        rec.sh.getRange(rec.rowIndex, rec.m.TOK + 1).setValue(resetToken);
        rec.sh.getRange(rec.rowIndex, rec.m.URL + 1).setValue(url);
        if (rec.m.RST >= 0) rec.sh.getRange(rec.rowIndex, rec.m.RST + 1).setValue(resetToken);
        if (rec.m.REX >= 0) rec.sh.getRange(rec.rowIndex, rec.m.REX + 1).setValue(expiresIso);
        __mark('sheetWrite:storeReset:end');

        __mark('powerAutomate:sendReset:start');
        _sendPasswordResetEmail(email, url);
        __mark('powerAutomate:sendReset:end');

        return OK({ ok: true });
      }

      // ======== Reset Password ========
      if (__action === "AUTH_RESET_PASSWORD") {
        const k = String(body.k || "").trim();
        const newPassword = String(body.newPassword || "");
        if (!k || !_isPlausibleToken(k)) return ERR("INVALID_RESET_TOKEN", 400);
        if (!_isStrongPassword(newPassword)) return ERR("WEAK_PASSWORD", 400);

        __mark('findLinksByResetToken:start');
        const rec = _findLinksRowByResetToken(k);
        __mark('findLinksByResetToken:end', { found: !!rec });
        if (!rec) return ERR("INVALID_OR_EXPIRED_RESET", 401);

        if (rec.m.REX >= 0) {
          const expiresIso = String(rec.row[rec.m.REX] || "");
          if (!_isFutureIso(expiresIso)) return ERR("INVALID_OR_EXPIRED_RESET", 401);
        }

        __mark('passwordHash:start');
        const hp = _hashPassword(newPassword);
        __mark('passwordHash:end');

        __mark('sheetWrite:updatePassword:start');
        rec.sh.getRange(rec.rowIndex, rec.m.PWH + 1).setValue(hp.hash);
        rec.sh.getRange(rec.rowIndex, rec.m.PWS + 1).setValue(hp.salt);
        if (rec.m.PWD >= 0) rec.sh.getRange(rec.rowIndex, rec.m.PWD + 1).setValue(_nowIso());
        if (rec.m.RST >= 0) rec.sh.getRange(rec.rowIndex, rec.m.RST + 1).setValue("");
        if (rec.m.REX >= 0) rec.sh.getRange(rec.rowIndex, rec.m.REX + 1).setValue("");
        rec.sh.getRange(rec.rowIndex, rec.m.TOK + 1).setValue("");
        rec.sh.getRange(rec.rowIndex, rec.m.URL + 1).setValue("");
        if (rec.m.NEW >= 0) rec.sh.getRange(rec.rowIndex, rec.m.NEW + 1).setValue("NO");
        __mark('sheetWrite:updatePassword:end');

        try { __msisdnSeen = _normaliseLocalTel(String(rec.row[rec.m.TEL] || "")); } catch (_) {}

        return OK({ ok: true });
      }

      // ---- TOKEN_CLAIM ----
      if (__action === "TOKEN_CLAIM") {
        const userText = String(body.userText || body.text || body.paste || body.tokenText || body.k || "").trim();
        __mark('tokenClaim:start');
        const res = _acceptTokenPaste(userText);
        __mark('tokenClaim:end', { ok: !!(res && res.ok), mode: res && res.mode });

        if (res && res.ok) {
          if (res.mode === 'RESET') return OK({ ok: true, mode: 'RESET' });
          if (res.msisdn) __msisdnSeen = res.msisdn;
          return OK({ ok: true, token: res.token, msisdn: res.msisdn, mode: 'LEGACY' });
        }

        const err = (res && res.error) || "TOKEN_CLAIM_FAILED";
        const code = (err === "TOKEN_NOT_ACTIVE_OR_UNKNOWN") ? 401 : 400;
        return ERR(err, code);
      }

      // ======== SEND_TIMESHEET ========
      if (__action === "SEND_TIMESHEET") {
        const ms = _requireMsisdn(body); if (ms && ms.__error) return ms.__error;
        const msisdn = ms; __msisdnSeen = msisdn;

        __mark('timesheet:send:start');
        const res = _sendTimesheetForMsisdn(msisdn) || { ok: false, error: "SEND_TIMESHEET_FAILED" };
        __mark('timesheet:send:end', { ok: !!res.ok, status: res.status });

        if (res.ok) return OK({ ok: true });

        if (res.status === 429 || res.status === 503) return ERR("TEMPORARILY_BUSY_TRY_AGAIN", 503);
        const err = String(res.error || "SEND_TIMESHEET_FAILED");
        if (err === "LINKS_ROW_NOT_FOUND") return ERR("INVALID_OR_UNKNOWN_IDENTITY", 400);
        if (err === "NO_EMAIL_IN_LINKS") return ERR("NO_EMAIL_IN_LINKS", 400);
        if (err === "POWER_EMAIL_NOT_CONFIGURED") return ERR("POWER_EMAIL_NOT_CONFIGURED", 500);
        if (err === "TIMESHEET_ATTACHMENT_MISSING") return ERR("TIMESHEET_ATTACHMENT_MISSING", 500);
        return ERR(err, 500);
      }

      // ===== Existing orchestrations =====
    // ===== AVAILABILITY_UPDATE_START — with logging & error capture =====
if (__action === "AVAILABILITY_UPDATE_START") {
  try {
    const runId = String(body.runId || "");
    _log('rota', 'AVAILABILITY_UPDATE_START:enter', {
      runId,
      writeLocked: !!_isWriteLocked()
    });

    if (_isWriteLocked()) {
      _log('rota', 'AVAILABILITY_UPDATE_START:busy_write_locked', { runId });
      return OK({ ok: true, status: "BUSY", reason: "API_WRITE_IN_PROGRESS" });
    }

    __mark('rotaBusy:set:true');
    _setRotaBusy(runId, true);

    // clear warms queue & stage headers/rev
    try { _setPendingWarms([]); } catch (qErr) {
      _log('rota', 'AVAILABILITY_UPDATE_START:setPendingWarms_error', { error: String(qErr) });
    }
    try {
      if (typeof _headersStageClear === 'function') { _headersStageClear(); }
    } catch (hdrErr) {
      _log('rota', 'AVAILABILITY_UPDATE_START:headersStageClear_error', { error: String(hdrErr) });
    }
    try {
      if (typeof _prepareNextTilesRev === 'function') { _prepareNextTilesRev(); }
    } catch (revErr) {
      _log('rota', 'AVAILABILITY_UPDATE_START:prepareNextTilesRev_error', { error: String(revErr) });
    }

    const nextRev = (typeof _revNextGet === 'function') ? _revNextGet() : null;

    _log('rota', 'AVAILABILITY_UPDATE_START:ok', { runId, nextRev });
    return OK({ ok: true, status: "ACK", runId, nextRev });
  } catch (eStart) {
    _log('rota', 'AVAILABILITY_UPDATE_START:error', { error: String(eStart && eStart.message || eStart) });
    return ERR("AVAILABILITY_UPDATE_START_EXCEPTION", 500);
  }
}

// ===== AVAILABILITY_HEADERS_CHANGED — with logging & error capture =====
if (__action === "AVAILABILITY_HEADERS_CHANGED") {
  try {
    const ymds = Array.isArray(body.ymds) ? body.ymds.map(String) : [];
    _log('rota', 'AVAILABILITY_HEADERS_CHANGED:enter', { ymds_len: ymds.length });

    if (ymds.length !== 14) {
      _log('rota', 'AVAILABILITY_HEADERS_CHANGED:bad_len', { ymds_len: ymds.length });
      return OK({ ok: true, staged: false, reason: "BAD_YMDS_LENGTH" });
    }

    const tz = (typeof _P==='function' && _P().TZ) || 'Europe/London';
    const stagedHeaders = ymds.map(ymd => {
      const [Y, M, D] = ymd.split("-").map(Number);
      const d = new Date(Y, M - 1, D);
      return {
        ymd,
        displayDay: Utilities.formatDate(d, tz, "EEE").toUpperCase(),
        displayDate: Utilities.formatDate(d, tz, "d MMMM")
      };
    });

    try {
      if (typeof _headersStageSet === 'function') { _headersStageSet(stagedHeaders); }
      else { _headersSet(stagedHeaders); }
    } catch (stageErr) {
      _log('rota', 'AVAILABILITY_HEADERS_CHANGED:stage_error', { error: String(stageErr) });
      // continue; we still try to compute nextRev & warm
    }

    try {
      if (typeof _prepareNextTilesRev === 'function') { _prepareNextTilesRev(); }
    } catch (prepErr) {
      _log('rota', 'AVAILABILITY_HEADERS_CHANGED:prepareNextTilesRev_error', { error: String(prepErr) });
    }

    const nextRev = (typeof _revNextGet === 'function') ? _revNextGet() : null;

    let activeMsisdns = [];
    try {
      activeMsisdns = (typeof _listActiveTokenMsisdns === 'function') ? _listActiveTokenMsisdns() : [];
    } catch (listErr) {
      _log('rota', 'AVAILABILITY_HEADERS_CHANGED:listActive_error', { error: String(listErr) });
    }

    let warmStats = { attempted: 0, warmed: 0, failed: 0, queued: 0 };
    try {
      if (activeMsisdns && activeMsisdns.length) {
        if (typeof _warmTilesForMsisdnsWithRev === 'function') warmStats = _warmTilesForMsisdnsWithRev(activeMsisdns, nextRev);
        else if (typeof _warmTilesForMsisdns === 'function')      warmStats = _warmTilesForMsisdns(activeMsisdns);
      }
    } catch (warmErr) {
      _log('rota', 'AVAILABILITY_HEADERS_CHANGED:warm_error', { error: String(warmErr), activeCount: (activeMsisdns || []).length });
    }

    _log('rota', 'AVAILABILITY_HEADERS_CHANGED:ok', {
      nextRev,
      activeCount: (activeMsisdns || []).length,
      warmed: warmStats
    });
    return OK({ ok: true, staged: true, nextRev, activeCount: (activeMsisdns || []).length, warmed: warmStats });
  } catch (eHdr) {
    _log('rota', 'AVAILABILITY_HEADERS_CHANGED:error', { error: String(eHdr && eHdr.message || eHdr) });
    return ERR("AVAILABILITY_HEADERS_CHANGED_EXCEPTION", 500);
  }
}


// ===== AVAILABILITY_UPDATE_END — flip busy off, flush writes, drain warms, publish =====
if (__action === "AVAILABILITY_UPDATE_END") {
  const runId = String(body.runId || "");
  __mark('rotaBusy:set:false');
  // Clear busy flag so queued warms can run
  _setRotaBusy("", false);

  // Best-effort flush of any staged writes then drain queued warms
  const flushed = (typeof _flushPendingWrites === 'function')
    ? _flushPendingWrites()
    : { ok: true, reason: "NO_FLUSH_HELPER" };

  const warmed = (typeof _drainPendingWarms === 'function')
    ? _drainPendingWarms()
    : { attempted: 0, warmed: 0, failed: 0, queued: 0, reason: "NO_DRAIN_HELPER" };

  // Publish staged headers + flip rev if available
  let publish = { published: false, reason: "NO_PUBLISH_HELPER" };
  if (typeof _publishStagedHeadersAndFlip === 'function') {
    publish = _publishStagedHeadersAndFlip();
  }

  return OK({ ok: true, runId, flushed, warmed, publish });
}

      // ================== EMAILHISTORY_UPDATED — tiles-only warm (no peers) ==================


if (__action === "EMAILHISTORY_UPDATED") {
  _bump(K_EH_REV);

  // Accept multiple payload shapes (back-compat)
  const bodyObj    = (typeof body === 'object' && body) ? body : {};
  const recOne     = bodyObj.record   || bodyObj.booking  || bodyObj.change  || null;
  const recMany    = bodyObj.records  || bodyObj.bookings || bodyObj.changes || bodyObj.items || null;

  // Keep original casing for legacy keys
  const legacyKeys = Array.isArray(bodyObj.keys)
    ? bodyObj.keys.map(k => String(k || '').trim()).filter(Boolean)
    : [];

  // Derive impacted msisdns (union)
  const msisdnSet = new Set();

  // 1) Preferred path — derive cohort from record(s) using the legacy key builder
  const records = Array.isArray(recMany) ? recMany : (recOne ? [recOne] : []);
  records.forEach(r => {
    try {
      const key = _cohortKeyFromBooking_vLegacy(r); // { ymd, hospital, ward, shiftType }
      if (!key || !key.ymd) return;
      const arr = _listCohortMsisdns(key) || [];
      arr.forEach(m => {
        const t = _normaliseLocalTel(m || "");
        if (t) msisdnSet.add(t);
      });
    } catch (_) {}
  });

  // 2) Fallback — resolve legacy occupant keys -> msisdns
  if (msisdnSet.size === 0 && legacyKeys.length) {
    try {
      const arr = _msisdnListFromOccupantKeys(legacyKeys) || [];
      arr.forEach(m => {
        const t = _normaliseLocalTel(m || "");
        if (t) msisdnSet.add(t);
      });
    } catch (_) {}
  }

  const msisdns = Array.from(msisdnSet);
  let warmed = { attempted: 0, warmed: 0, failed: 0, queued: 0 };

  if (!msisdns.length) {
    __log('eh_updated:no_targets', { hadRecords: !!records.length, keysCount: legacyKeys.length });
    return OK({ ok: true, ehRev: _getInt(K_EH_REV, 0), warmed });
  }

  if (_isRotaBusy()) {
    _queueWarms(msisdns);
    warmed.queued = msisdns.length;
    return OK({ ok: true, ehRev: _getInt(K_EH_REV, 0), warmed });
  }

  // Tiles-only warm (peers retired)
  warmed = _warmTilesForMsisdns(msisdns);
  return OK({ ok: true, ehRev: _getInt(K_EH_REV, 0), warmed });
}

      // ================== AVAILABILITY_PARTIAL_UPDATED — tiles-only warm (no peers) ==================
      if (__action === "AVAILABILITY_PARTIAL_UPDATED") {
        const items = Array.isArray(body.items) ? body.items : [];
        const msisdns = Array.from(
          new Set(
            items
              .map(it => _normaliseLocalTel((it && it.msisdn) || ""))
              .filter(Boolean)
          )
        );

        if (!msisdns.length) {
          return OK({ ok: true, warmed: { attempted: 0, warmed: 0, failed: 0, queued: 0 } });
        }

        if (_isRotaBusy()) {
          _queueWarms(msisdns);
          return OK({ ok: true, warmed: { attempted: 0, warmed: 0, failed: 0, queued: msisdns.length } });
        }

        const warmed = _warmTilesForMsisdns(msisdns);
        return OK({ ok: true, warmed });
      }

      if (__action === "MARK_MESSAGE_SEEN") {
        const ms = _requireMsisdn(body); if (ms && ms.__error) return ms.__error;
        const msisdn = ms; __msisdnSeen = msisdn;
        const rec = _getLinksRowByTel(msisdn);
        if (rec && rec.m.NEW >= 0) {
          const cur = String(rec.row[rec.m.NEW] || "").toUpperCase();
          if (cur === "YES" || cur === "ALERT") {
            __mark('sheetWrite:welcomeSeen:start');
            _setLinksNewUser(msisdn, "NO");
            __mark('sheetWrite:welcomeSeen:end');
          }
        }
        return OK({ ok: true });
      }

      // Unknown action
      return ERR("UNKNOWN_ACTION", 400);
    }

    // ----- legacy: candidate availability changes (no action) -----
    {
      const ms = _requireMsisdn(body);
      if (ms && ms.__error) return ms.__error;
      const msisdn = ms;
      __msisdnSeen = msisdn;

      const changes = Array.isArray(body.changes) ? body.changes : [];
      if (!changes.length) return ERR("NO_CHANGES", 400);
      if (changes.length > 14) return ERR("TOO_MANY_CHANGES", 400);

      if (_isRotaBusy()) {
        __mark('queueChanges:start');
        _queueChanges(msisdn, changes);
        _applyChangesToTilesCache(msisdn, changes);
        __mark('queueChanges:end');
        return OK({
          ok: true,
          results: changes.map(c => ({ ymd: String(c.ymd), applied: true, code: String(c.code || ""), deferred: true, reason: "QUEUED_UNTIL_ROTA_IDLE" }))
        });
      }

      __mark('sheetOpen:ssRota:start');
      const ss = _ssRota();
      __mark('sheetOpen:ssRota:end');

      __mark('sheetRead:findCandidate:start');
      const cand = _findCandidateByMobile(ss, msisdn);
      __mark('sheetRead:findCandidate:end', { found: !!cand });
      if (!cand) return ERR("NOT_IN_CANDIDATE_LIST", 403);

      __mark('sheetRead:headers:start');
      const liveHeaders = _readAvailabilityHeaders(ss) || [];
      __mark('sheetRead:headers:end', { count: liveHeaders.length });
      if (liveHeaders.length !== 14) return ERR("BAD_HEADER_WINDOW", 500);

      const ymdToIndex = Object.fromEntries(liveHeaders.map((h, i) => [h.ymd, i]));
      const ymdToCol   = Object.fromEntries(liveHeaders.map(h => [h.ymd, h.col]));

      __mark('sheetRead:findAvRow:start');
      const avRow = _findAvailabilityRowByTelephone(ss, cand.telephone);
      __mark('sheetRead:findAvRow:end', { ok: !!avRow });
      if (!avRow) return ERR("CANDIDATE_ROW_NOT_FOUND", 404);

      const nameKey    = (cand.surname + " " + cand.firstname).toLowerCase().trim();
      const targetYmds = changes.map(c => String(c.ymd || ""));
      __mark('sheetRead:bookedMap:start');
      const bookedMap  = _readBookedMap(ss, nameKey, targetYmds);
      __mark('sheetRead:bookedMap:end');

      const results = [];
      const setList = []; // { r, c, value, bg }

      try {
        __mark('lock:setWriteLock:true');
        _setWriteLock(true);

        const p = _P();
        const avSh = ss.getSheetByName(p.SH_AV);
        const currentCells = _readAvailabilityCells(ss, avRow.rowIndex, liveHeaders);

        for (const ch of changes) {
          const ymd  = String(ch.ymd || "");
          const code = (ch.code == null ? "" : String(ch.code).toUpperCase());
          const col  = ymdToCol[ymd];
          const idx  = ymdToIndex[ymd];

          if (!col || idx == null) { results.push({ ymd, applied: false, reason: "INVALID_DATE" }); continue; }
          if (bookedMap[ymd])      { results.push({ ymd, applied: false, reason: "BOOKED_LOCK" }); continue; }

          const cell = currentCells[idx];
          if (_isBlockedCell(cell.value, cell.bg, p)) { results.push({ ymd, applied: false, reason: "BLOCKED_RULE" }); continue; }

          if (!["", "N/A", "LD", "N", "LD/N"].includes(code)) {
            results.push({ ymd, applied: false, reason: "VALIDATION" });
            continue;
          }

          const m = _mapWrite(code, p);
          setList.push({ r: avRow.rowIndex, c: col, value: m.value, bg: m.bg });
          results.push({ ymd, applied: true, code });
        }

        if (setList.length) {
          __mark('sheetWrite:applyChanges:start');
          const nots = setList.map(x => avSh.getRange(x.r, x.c, 1, 1).getA1Notation());
          const ranges = avSh.getRangeList(nots).getRanges();
          ranges.forEach((range, i) => range.setValue(setList[i].value));
          ranges.forEach((range, i) => range.setBackground(setList[i].bg));
          __mark('sheetWrite:applyChanges:end', { count: setList.length });
        }
      } finally {
        __mark('lock:setWriteLock:false');
        _setWriteLock(false);
      }

      __mark('cachePatch:start');
      _applyChangesToTilesCache(msisdn, results.filter(r => r.applied).map(r => ({ ymd: r.ymd, code: r.code })));
      __mark('cachePatch:end');

      return OK({ ok: true, results });
    }
  } catch (e2) {
    __log('post:exception', { error: String(e2 && e2.message || e2) });
    return ERR(e2 && e2.message || "SERVER_ERROR", 500);
  }

  // --------------------------- local helpers (webhook response & diagnostics) ---------------------------
  function __simpleText(txt, code) {
    try { __statusCode = code || 200; } catch(_) {}
    return ContentService.createTextOutput(String(txt || 'OK'))
      .setMimeType(ContentService.MimeType.TEXT);
  }

  function __clicksendDiagnostics(e, opts) {
    // Extract everything we might care about from params (form-encoded).
    const p = Object.assign({}, e && e.parameter || {});
    const lc = {};
    Object.keys(p).forEach(k => {
      lc[String(k).toLowerCase()] = String(p[k]);
    });

    // Common fields we expect (defensive)
    const from      = lc['from'] || lc['caller'] || lc['from_number'] || '';
    const to        = lc['to'] || lc['to_number'] || lc['recipient'] || lc['msisdn'] || '';
    const status    = lc['status'] || lc['event'] || lc['description'] || '';
    const call_id   = lc['call_id'] || lc['voice_id'] || lc['messageid'] || lc['message_id'] || '';
    const digit     = lc['keypress'] || lc['dtmf'] || lc['digits'] || lc['input'] || '';
    const custom    = lc['custom_string'] || lc['customstring'] || lc['reference'] || '';

    // One-time schema snapshot (field names + sample values)
    try {
      const props = PropertiesService.getScriptProperties();
      const key = 'CLICKSEND_SCHEMA_DUMPED';
      if (props.getProperty(key) !== '1') {
        const sample = {};
        Object.keys(p).forEach(k => { sample[k] = String(p[k]).slice(0, 200); });
        __log('clicksend:schema:first_payload', { keys: Object.keys(p), sample });
        props.setProperty(key, '1');
      }
    } catch (_) {}

    // Optional enforcement of dedicated From number
    const enforceFrom = !!(opts && opts.enforceFrom);
    const expectFrom = (opts && opts.expectFrom) || '';
    const fromOK = !enforceFrom || (from && expectFrom && from.replace(/\s+/g,'') === expectFrom.replace(/\s+/g,''));

    // Truncate full param snapshot to keep logs sane
    const snapshot = {};
    Object.keys(p).forEach(k => snapshot[k] = String(p[k]).slice(0, 500));

    return {
      enforced: enforceFrom,
      expect_from: expectFrom || null,
      from_ok: fromOK,
      from: from || null,
      to: to || null,
      status: status || null,
      digit: digit || null,
      custom_string: custom || null,
      call_id: call_id || null,
      params_snapshot: snapshot
    };
  }
}

function _cohortKeyFromBooking_vLegacy(b) {
  const P = (typeof _P === 'function') ? _P() : { TZ: 'Europe/London' };
  function _toYmdFromDDMMYYYY(s) {
    try {
      const m = String(s || '').match(/\b(\d{2})\/(\d{2})\/(\d{4})\b/);
      if (!m) return '';
      const dd = Number(m[1]), mm = Number(m[2]), yyyy = Number(m[3]);
      const d = new Date(yyyy, mm - 1, dd);
      return Utilities.formatDate(d, P.TZ || 'Europe/London', 'yyyy-MM-dd');
    } catch (_) { return ''; }
  }
  function _normHosp(h) {
    try { return (typeof _normalizeHospitalName === 'function') ? _normalizeHospitalName(h) : String(h || '').trim(); }
    catch (_) { return String(h || '').trim(); }
  }
  function _normShiftType(raw) {
    const s = String(raw || '').toUpperCase();
    if (s.includes('NIGHT') || s === 'N') return 'NIGHT';
    if (s.includes('DAY') || s === 'LD' || s === 'LD/N' || s === 'LONG DAY') return 'LONG DAY';
    // fallback: prefer LONG DAY when ambiguous
    return 'LONG DAY';
  }

  const ymd =
    String(b && (b.ymd || b.YMD || '')) ||
    _toYmdFromDDMMYYYY(b && (b.date || b.Date || b.DATE || ''));

  const hospitalRaw = b && (b.hospital || b.Hospital || '');
  const wardRaw     = b && (b.ward || b.Ward || '');
  const shiftRaw    = b && (b.shift || b.Shift || b.shift_type || b.shiftType || '');
  const notesRaw    = b && (b.notes || b.Notes || '');

  // shift inference also looks at notes when shift is blank/ambiguous
  const shiftType = _normShiftType(shiftRaw || notesRaw || '');

  return {
    ymd: String(ymd || '').trim(),
    hospital: _normHosp(hospitalRaw),
    ward: String(wardRaw || '').trim(),
    shiftType
  };
}












function __embedMessage_(type, payload, targetOrigin) {
  try {
    var data = { type: String(type || ''), payload: payload || {} };
    var json = JSON.stringify(data).replace(/</g, '\\u003c');
    var origin = String(targetOrigin || 'https://interactions.arthur-rai.co.uk');
    var html = [
      '<!doctype html><meta charset="utf-8"><title>OK</title>',
      '<script>',
      'try { parent.postMessage(', json, ', ', JSON.stringify(origin), '); }',
      'catch(e){ try{ parent.postMessage(', json, ', "*"); }catch(_){ } }',
      '</script>',
      '<body>OK</body>'
    ].join('');
    return HtmlService.createHtmlOutput(html)
      .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
  } catch (ex) {
    return HtmlService.createHtmlOutput('<!doctype html><title>Error</title><pre>EMBED_ERROR</pre>')
      .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
  }
}

function __alertSummary_(a) {
  if (!a) return null;
  try {
    return {
      alert_id: a.alert_id || '',
      candidate_name: a.candidate_name || a.candidateName || '',
      role: a.job_title || (a.shift && (a.shift.job_title || a.shift.jobTitle)) || '',
      hospital: a.hospital || (a.shift && a.shift.hospital) || '',
      ward: a.ward || (a.shift && a.shift.ward) || '',
      date_label: a.date_label || '',
      time_range_label: a.time_range_label || '',
      shift_type: a.shift_type || '',
      issue_type: a.issue_type || '',
      eta_or_leave_time_label:
        a.eta_or_leave_time_label ||
        (a.issue && (a.issue.eta_or_leave_time_label || a.issue.eta_or_leave_time)) || '',
      reason_text: a.reason_text || '',
      ack_when_iso: a.ack_when_iso || ''
    };
  } catch (_) { return null; }
}


  // ─────────────────────────── CTA helpers (unchanged) ───────────────────────────
  function __b64url(bytes) {
    return Utilities.base64EncodeWebSafe(bytes).replace(/=+$/,'');
  }
  function __getSecret() {
    const props = PropertiesService.getScriptProperties();
    return (props.getProperty('URL_TOKEN_SECRET') || '').trim();
  }
  function __signAck(alertId, expUnix, msisdnOpt) {
    const secret = __getSecret();
    if (!secret) throw new Error('URL_TOKEN_SECRET not configured');
    const parts = [String(alertId), String(expUnix)];
    if (msisdnOpt) parts.push(String(msisdnOpt));
    const msg = parts.join('.');
    const sigBytes = Utilities.computeHmacSha256Signature(msg, secret);
    return String(expUnix) + '.' + __b64url(sigBytes);
  }
  function __verifyAck(alertId, msisdnOpt, token) {
    try {
      const secret = __getSecret();
      if (!secret) return { ok:false, reason:'SECRET_MISSING' };
      const parts = String(token || '').split('.');
      if (parts.length !== 2) return { ok:false, reason:'BAD_TOKEN_FORMAT' };
      const exp = Number(parts[0]);
      const sig = parts[1];
      if (!isFinite(exp) || exp <= 0) return { ok:false, reason:'BAD_EXP' };
      const nowSec = Math.floor(Date.now()/1000);
      if (nowSec > exp) return { ok:false, reason:'TOKEN_EXPIRED' };
      const msgParts = [String(alertId), String(exp)];
      if (msisdnOpt) msgParts.push(String(msisdnOpt));
      const expect = __b64url(Utilities.computeHmacSha256Signature(msgParts.join('.'), secret));
      if (expect.length !== sig.length) return { ok:false, reason:'SIG_LEN' };
      let same = 0;
      for (let i=0;i<sig.length;i++) same |= (sig.charCodeAt(i) ^ expect.charCodeAt(i));
      if (same !== 0) return { ok:false, reason:'SIG_MISMATCH' };
      return { ok:true, exp };
    } catch (ex) {
      return { ok:false, reason:String(ex && ex.message || ex) };
    }
  }
  function __signBulk(idsCsv, expUnix, msisdnOpt) {
    const secret = __getSecret();
    if (!secret) throw new Error('URL_TOKEN_SECRET not configured');
    const parts = [String(idsCsv), String(expUnix)];
    if (msisdnOpt) parts.push(String(msisdnOpt));
    const msg = parts.join('.');
    const sigBytes = Utilities.computeHmacSha256Signature(msg, secret);
    return String(expUnix) + '.' + __b64url(sigBytes);
  }
  function __verifyBulk(idsCsv, msisdnOpt, token) {
    try {
      const secret = __getSecret();
      if (!secret) return { ok:false, reason:'SECRET_MISSING' };
      const parts = String(token || '').split('.');
      if (parts.length !== 2) return { ok:false, reason:'BAD_TOKEN_FORMAT' };
      const exp = Number(parts[0]);
      const sig = Number.isNaN(Number(parts[1])) ? String(parts[1]) : String(parts[1]);
      if (!isFinite(exp) || exp <= 0) return { ok:false, reason:'BAD_EXP' };
      const nowSec = Math.floor(Date.now()/1000);
      if (nowSec > exp) return { ok:false, reason:'TOKEN_EXPIRED' };
      const msgParts = [String(idsCsv), String(exp)];
      if (msisdnOpt) msgParts.push(String(msisdnOpt));
      const expect = __b64url(Utilities.computeHmacSha256Signature(msgParts.join('.'), secret));
      if (expect.length !== sig.length) return { ok:false, reason:'SIG_LEN' };
      let same = 0;
      for (let i=0;i<sig.length;i++) same |= (sig.charCodeAt(i) ^ expect.charCodeAt(i));
      if (same !== 0) return { ok:false, reason:'SIG_MISMATCH' };
      return { ok:true, exp };
    } catch (ex) {
      return { ok:false, reason:String(ex && ex.message || ex) };
    }
  }



function __sendResponderConfirm(msisdnRaw, responderName, broadcastName) {
  function _mask447(n){ try{ const s=String(n||''); return s.length>=6 ? (s.slice(0,3)+'…'+s.slice(-3)) : s; } catch(_){ return ''; } }
  try {
    const cfg = _getEmergencyProps();
    const hasGeneric = (typeof _sendWATIBulkTemplate_Generic === 'function');

    _log('EMERG_ACCEPT_CONFIRM', 'responder_confirm:entry', {
      msisdnRaw: String(msisdnRaw || ''),
      responderName_in: String(responderName || ''),
      hasGenericFn: hasGeneric,
      templateCfg: (cfg && cfg.TEMPLATE_NAME_EMERGACCEPTCONFIRM) || null
    });

    if (!hasGeneric) { _log('EMERG_ACCEPT_CONFIRM','responder_confirm:skip_no_generic_fn'); return; }

    const n447 = __to447(msisdnRaw);
    if (!n447) { _log('EMERG_ACCEPT_CONFIRM','responder_confirm:skip_bad_number', { msisdnRaw: String(msisdnRaw||'') }); return; }

    let resolvedName = (responderName && String(responderName).trim()) || '';
    if (!resolvedName && typeof __resolveResponder === 'function') {
      try { resolvedName = String(__resolveResponder(n447) || '').trim(); } catch(_) {}
    }
    if (!resolvedName) resolvedName = 'Responder ' + _mask447(n447);

    const templateName = (cfg && cfg.TEMPLATE_NAME_EMERGACCEPTCONFIRM) || 'emergacceptconfirm01';
    const receivers = [{ whatsappNumber: n447, customParams: [{ name: 'responder_name', value: resolvedName }] }];

    _log('EMERG_ACCEPT_CONFIRM', 'responder_confirm:send_start', {
      template: templateName,
      receiver_masked: _mask447(n447),
      broadcast: broadcastName || 'EMERG_ACCEPT_CONFIRM',
      responderName_sent: resolvedName
    });

    const res = _sendWATIBulkTemplate_Generic(templateName, receivers, broadcastName || 'EMERG_ACCEPT_CONFIRM');

    _log('EMERG_ACCEPT_CONFIRM', 'responder_confirm:send_result', {
      http: res && res.httpCode,
      ok: !!(res && res.httpCode && res.httpCode >= 200 && res.httpCode < 300),
      raw: res && res.json ? { hasJson: true } : { hasJson: false }
    });
  } catch (err) {
    _log('EMERG_ACCEPT_CONFIRM', 'responder_confirm:error', { error: String((err && err.message) || err) });
  }
}


 function __sendBulkResponderConfirm(msisdnRaw, responderName) {
  // local masker for safe logging
  function _mask447(n) {
    try {
      if (!n) return '';
      const s = String(n);
      return s.length >= 6 ? (s.slice(0,3) + '…' + s.slice(-3)) : s;
    } catch (_) { return ''; }
  }

  try {
    const cfg = _getEmergencyProps();
    const hasGeneric = (typeof _sendWATIBulkTemplate_Generic === 'function');

    _log('BULK_CONFIRM', 'responder_confirm:entry', {
      msisdnRaw: String(msisdnRaw || ''),
      responderName_in: String(responderName || ''),
      hasGenericFn: hasGeneric,
      templateCfg: (cfg && cfg.TEMPLATE_NAME_CONFIRMALLALERTS) || null
    });

    if (!hasGeneric) {
      _log('BULK_CONFIRM', 'responder_confirm:skip_no_generic_fn');
      return;
    }

    const n447 = __to447(msisdnRaw);
    if (!n447) {
      _log('BULK_CONFIRM', 'responder_confirm:skip_bad_number', { msisdnRaw: String(msisdnRaw || '') });
      return;
    }

    // Resolve the name we will actually send
    let resolvedName = (responderName && String(responderName).trim()) || '';
    if (!resolvedName && typeof __resolveResponder === 'function') {
      try {
        resolvedName = String(__resolveResponder(n447) || '').trim();
      } catch (_) {}
    }
    if (!resolvedName) resolvedName = 'Responder ' + _mask447(n447); // final fallback

    const templateName = (cfg && cfg.TEMPLATE_NAME_CONFIRMALLALERTS) || 'emergencyconfirmallalerts';
    const receivers = [{
      whatsappNumber: n447,
      customParams: [
        { name: 'responder_name', value: resolvedName }
      ]
    }];

    _log('BULK_CONFIRM', 'responder_confirm:send_start', {
      template: templateName,
      receiver_masked: _mask447(n447),
      broadcast: 'EMERG_CONFIRM_ALL',
      responderName_sent: resolvedName
    });

    const res = _sendWATIBulkTemplate_Generic(
      templateName,
      receivers,
      'EMERG_CONFIRM_ALL'
    );

    _log('BULK_CONFIRM', 'responder_confirm:send_result', {
      http: res && res.httpCode,
      ok: !!(res && res.httpCode && res.httpCode >= 200 && res.httpCode < 300),
      raw: res && res.json ? { hasJson: true } : { hasJson: false }
    });

  } catch (err) {
    _log('BULK_CONFIRM', 'responder_confirm:error', { error: String((err && err.message) || err) });
  }
}
function __collectOtherEmergencyReceivers(responder447) {
  const contacts = (typeof _getEmergencyContacts === 'function') ? (_getEmergencyContacts() || []) : [];
  const receivers = [];
  contacts.forEach(c => {
    if (__isContactInBlackout(c)) return;
    const n = __to447(c && (c.mobile447 || c.whatsappNumber || c.msisdn || ''));
    if (!/^447\d{9}$/.test(n)) return;
    if (responder447 && n === responder447) return;
    receivers.push({ whatsappNumber: n, customParams: [] });
  });
  return receivers;
}


function __resolveResponder(msisdn447) {
  try {
    const list = (typeof _getEmergencyContacts === 'function') ? _getEmergencyContacts() : [];
    const found = (list || []).find(c => String(c.mobile447 || c.whatsappNumber || '') === String(msisdn447 || ''));
    return found ? (found.display_name || found.name || '').trim() : '';
  } catch (_) { return ''; }
}


function __sendBulkAllAlertsRespondedToOthers(responderName, whenIso, responderMsisdnRaw) {
  // local masker for safe logging
  function _mask447(n) {
    try {
      if (!n) return '';
      const s = String(n);
      return s.length >= 6 ? (s.slice(0,3) + '…' + s.slice(-3)) : s;
    } catch (_) { return ''; }
  }

  try {
    const cfg = _getEmergencyProps();
    const hasGeneric = (typeof _sendWATIBulkTemplate_Generic === 'function');
    const responder447 = __to447(responderMsisdnRaw);

    // Note: we intentionally do NOT change the original logic that formats with __TZ.
    const tzSeen = (typeof __TZ !== 'undefined') ? String(__TZ) : '(undefined)';

    _log('BULK_CONFIRM', 'others_confirm:entry', {
      responderName: String(responderName || ''),
      whenIso: String(whenIso || ''),
      responder_msisdn_raw: String(responderMsisdnRaw || ''),
      responder447_masked: _mask447(responder447),
      hasGenericFn: hasGeneric,
      tzSeen,
      templateCfg: (cfg && cfg.TEMPLATE_NAME_ALLALERTSRESPONDED) || null
    });

    if (!hasGeneric) {
      _log('BULK_CONFIRM', 'others_confirm:skip_no_generic_fn');
      return;
    }

    const receivers = __collectOtherEmergencyReceivers(responder447) || [];
    _log('BULK_CONFIRM', 'others_confirm:receivers_built', {
      count: receivers.length,
      sample_masked: receivers.slice(0, 5).map(r => _mask447(r && r.whatsappNumber))
    });

    if (!receivers.length) {
      _log('BULK_CONFIRM', 'others_confirm:skip_no_receivers');
      return;
    }

    // Best-effort preview for logging: try to format with __TZ, but don’t alter the main logic.
    let respondedAtPreview = '';
    try {
      respondedAtPreview =
        Utilities.formatDate(new Date(whenIso || Date.now()), __TZ, 'EEEE d MMMM yyyy HH:mm') + 'hrs';
    } catch (fmtErr) {
      _log('BULK_CONFIRM', 'others_confirm:preview_format_error', { error: String((fmtErr && fmtErr.message) || fmtErr) });
    }

    receivers.forEach(r => {
      r.customParams = [
        { name: 'responder_name',     value: responderName || 'A team member' },
        { name: 'Responded_at_label', value: respondedAtPreview || '' }
      ];
    });

    const templateName = (cfg && cfg.TEMPLATE_NAME_ALLALERTSRESPONDED) || 'allalertsrespondedother';

    _log('BULK_CONFIRM', 'others_confirm:send_start', {
      template: templateName,
      receivers_count: receivers.length,
      broadcast: 'EMERG_ALL_ALERTS_RESPONDED',
      respondedAtPreview
    });

    const res = _sendWATIBulkTemplate_Generic(
      templateName,
      receivers,
      'EMERG_ALL_ALERTS_RESPONDED'
    );

    _log('BULK_CONFIRM', 'others_confirm:send_result', {
      http: res && res.httpCode,
      ok: !!(res && res.httpCode && res.httpCode >= 200 && res.httpCode < 300),
      raw: res && res.json ? { hasJson: true } : { hasJson: false }
    });

  } catch (err) {
    _log('BULK_CONFIRM', 'others_confirm:error', { error: String((err && err.message) || err) });
  }
}

function __to447(n){
  if (!n) return '';
  let s = String(n).replace(/[^\d+]/g,'');
  if (s.startsWith('+')) s = s.slice(1);
  if (s.startsWith('07') && s.length === 11) return '44' + s.slice(1);
  if (s.startsWith('447') && s.length === 12) return s;
  if (s.startsWith('44')  && s.length === 12) return s;
  return '';
}

function __isContactInBlackout(contact) {
  try {
    if (typeof _isEmergencyContactInBlackout === 'function') {
      return !!_isEmergencyContactInBlackout(contact);
    }
    const flags = [
      contact && contact.blackout,
      contact && contact.blackout_today,
      contact && contact.isBlackout,
      contact && contact.is_blackout,
      contact && contact.blackoutNow
    ];
    return flags.some(v => v === true || v === 1 || String(v).toLowerCase() === 'true');
  } catch (_) { return false; }
}


function __jsonp(e, obj, status) {
  var cb = (e && e.parameter && e.parameter.callback) || '';
  var body = JSON.stringify(obj || {});
  var out = cb ? (cb + '(' + body + ');') : body;
  try { __statusCode = status || 200; } catch (_) {}
  return ContentService.createTextOutput(out)
    .setMimeType(cb ? ContentService.MimeType.JAVASCRIPT : ContentService.MimeType.JSON);
}












































function doGet(e) {
  // ────────────────────────── inline tracer + sheet logger ──────────────────────────
  const __RID = Utilities.getUuid();
  const __T0  = Date.now();

  function __maskMsisdn(s) {
    try {
      const m = String(s || "").replace(/\s+/g,'');
      if (!m) return "";
      return m.slice(0,3) + "•••••" + m.slice(-4);
    } catch (_) { return ""; }
  }
// ────────────────────────── logging disabled: no-ops with originals kept below ──────────────────────────
function __logsSheet() {
  // Logging disabled — returning null to avoid any Sheets I/O.
  // Original implementation kept below for easy re-enable.
  /*
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName('Logs');
  if (!sh) {
    sh = ss.insertSheet('Logs');
    sh.getRange(1,1,1,5).setValues([['Timestamp','ReqId','Route','Event','DataJSON']]);
    sh.setFrozenRows(1);
  }
  return sh;
  */
  return null;
}

function __log(eventName, dataObj) {
  // Logging disabled — no-op.
  // Original implementation kept below for easy re-enable.
  /*
  try {
    const sh = __logsSheet();
    const stamp = Utilities.formatDate(new Date(), (_P().TZ || 'Europe/London'), 'yyyy-MM-dd HH:mm:ss');
    const payload = Object.assign({
      view: (e && e.parameter && String(e.parameter.view || '').toLowerCase()) || '',
      hadK: !!(e && e.parameter && typeof e.parameter.k !== 'undefined' && String(e.parameter.k || '').trim() !== ''),
      q: e && e.parameter ? Object.keys(e.parameter).reduce((o,k)=>{ if(k!=='k') o[k]=e.parameter[k]; return o; },{}) : {},
    }, dataObj || {});
    sh.appendRow([stamp, __RID, 'doGet', String(eventName || ''), JSON.stringify(payload)]);
  } catch (_) {}
  */
  return;
}

function __mark(eventName, extra) {
  // Logging disabled — no-op.
  // Original implementation kept below for easy re-enable.
  /*
  __log(eventName, Object.assign({ ms: Date.now() - __T0 }, extra || {}));
  */
  return;
}
// ───────────────────────────────────────────────────────────────────────────────────────────────────────

  // ───────────────────────── helpers used by CTA + cohorts ─────────────────────────
  const __P = _P();
  const __TZ = __P.TZ || 'Europe/London';
  function __esc(s){ return String(s == null ? '' : s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
  function __fmtWhen(iso){
    try { return Utilities.formatDate(new Date(iso), __TZ, 'EEE d MMM yyyy HH:mm') + 'hrs'; } catch (_) { return iso || ''; }
  }

  function __to07(n){
    const s = String(n || '').replace(/\s+/g,'');
    if (/^\+447\d{9}$/.test(s)) return '0' + s.slice(3);
    if (/^447\d{9}$/.test(s))   return '0' + s.slice(2);
    if (/^07\d{9}$/.test(s))    return s;
    const digits = s.replace(/[^\d]/g, '');
    if (digits.startsWith('447') && digits.length === 12) return '0' + digits.slice(2);
    return digits;
  }
  function __surnameInitial(surname) {
    const s = String(surname || '').trim();
    return s ? s[0].toUpperCase() : '';
  }
  function __sample(arr, n) {
    const a = Array.isArray(arr) ? arr : [];
    return a.slice(0, Math.max(0, n|0 || 0));
  }
  function __ensureArray(a) {
    return Array.isArray(a) ? a : (a ? [a] : []);
  }
  function __parseIsoOrNull(v) {
    try { return v ? new Date(v) : null; } catch (_) { return null; }
  }
  function __formatHHMM(d, tz) {
    try {
      return Utilities.formatDate(d, tz || __TZ, 'HH:mm');
    } catch (_) {
      return Utilities.formatDate(new Date(d), tz || __TZ, 'HH:mm');
    }
  }
  function __roundToNext(minutes, step) {
    return Math.ceil(minutes / step) * step;
  }
  function __buildLeaveTimeOptions(nowIso, tz, endIsoOpt) {
    const tzUse = tz || __TZ;
    const now = new Date(nowIso || new Date().toISOString());
    const labels = ['NOW'];

    const mins = now.getMinutes();
    const n15  = new Date(now.getTime());
    n15.setMinutes(__roundToNext(mins, 15), 0, 0);
    labels.push(__formatHHMM(n15, tzUse));

    const n30  = new Date(n15.getTime());
    n30.setMinutes(n15.getMinutes() + 15, 0, 0);
    labels.push(__formatHHMM(n30, tzUse));

    const end = __parseIsoOrNull(endIsoOpt);
    if (end && end.getTime() - now.getTime() > 0 && (end.getTime() - now.getTime()) <= (12 * 60 * 60 * 1000)) {
      labels.push(__formatHHMM(end, tzUse));
    }
    // Deduplicate while preserving order
    const seen = {};
    const uniq = [];
    labels.forEach(l => { if (!seen[l]) { seen[l] = true; uniq.push(l); } });
    return uniq;
  }
function __augmentEligibleForLeaveEarly(eligible, serverTimeIso, tz) {
  const nowIso = serverTimeIso || new Date().toISOString();
  const now = new Date(nowIso);

  return (eligible || []).map((item, idx) => {
    try {
      const startIso = item.startAtIso ||
        (item.shift && (item.shift.startAtIso || item.shift.startIso)) ||
        (item.ctx && item.ctx.startAtIso) || null;
      const endIso   = item.endAtIso ||
        (item.shift && (item.shift.endAtIso   || item.shift.endIso)) ||
        (item.ctx && item.ctx.endAtIso)   || null;

      const start = __parseIsoOrNull(startIso);
      const end   = __parseIsoOrNull(endIso);
      const inProgress = !!(start && end && start.getTime() <= now.getTime() && now.getTime() < end.getTime());

      const beforeAI = __ensureArray(item.allowed_issues || item.allowedIssues || item.issuesAllowed);
      const ai = beforeAI.slice();

      if (inProgress && ai.indexOf('LEAVE_EARLY') === -1) ai.push('LEAVE_EARLY');

      // Log (note: no late_options generated anymore)
      try {
        _log('eligible', 'leave_early_augment', {
          idx, nowIso, startIso, endIso, inProgress,
          before_allowed_issues: beforeAI,
          after_allowed_issues: ai
        });
      } catch (_) {}

      item.allowed_issues = ai;
      // DO NOT set item.late_options here anymore
    } catch (err) {
      try { _log('eligible', 'leave_early_augment_error', { idx, error: String(err && err.message || err) }); } catch (_) {}
    }
    return item;
  });
}



  function __renderHtmlPage(title, innerHtml) {
    return HtmlService.createHtmlOutput([
      '<!doctype html><html><head><meta charset="utf-8">',
      '<meta name="viewport" content="width=device-width, initial-scale=1">',
      '<title>', __esc(title), '</title>',
      '<style>',
      'body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#f7f7fb;margin:0;line-height:1.35}',
      '.wrap{max-width:900px;margin:0 auto;padding:24px}',
      '.card{background:#fff;border-radius:14px;box-shadow:0 1px 3px rgba(0,0,0,.08);padding:20px}',
      'h1{font-size:20px;margin:0 0 8px}.ok{color:#137333}.warn{color:#b06000}.info{color:#1a73e8}.err{color:#b00020}',
      '.muted{color:#666}.btns{margin-top:14px;display:flex;gap:10px;flex-wrap:wrap}',
      'a.btn{display:inline-block;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:600;border:1px solid #1a73e8;color:#1a73e8}',
      'a.btn.primary{background:#1a73e8;color:#fff;border-color:#1a73e8}',
      '.tablewrap{overflow:auto;margin-top:12px}',
      'table{border-collapse:collapse;width:100%;min-width:760px}',
      'th,td{border:1px solid #e8e8ef;padding:8px 10px;text-align:left;font-size:13px}',
      'th{background:#f2f3f7;color:#333}',
      '.status{margin-top:10px;font-size:12px;color:#666}',
      '.status .ok{color:#137333}.status .err{color:#b00020}',
      'iframe.beacon{position:absolute;left:-9999px;width:1px;height:1px;border:0}',
      'img.beacon{position:absolute;left:-9999px;width:1px;height:1px;opacity:0}',
      '</style></head><body><div class="wrap"><div class="card">',
      innerHtml,
      '</div></div></body></html>'
    ].join('')).setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
  }
  function __renderPendingRedirectPage(commitUrl) {
    const html = HtmlService.createHtmlOutput([
      '<!doctype html><html><head><meta charset="utf-8">',
      '<meta name="viewport" content="width=device-width, initial-scale=1">',
      '<title>Working…</title>',
      '<style>',
      'body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#f7f7fb;margin:0;line-height:1.35}',
      '.wrap{max-width:680px;margin:0 auto;padding:24px}',
      '.card{background:#fff;border-radius:14px;box-shadow:0 1px 3px rgba(0,0,0,.08);padding:20px}',
      'h1{font-size:20px;margin:0 0 8px}.info{color:#1a73e8}.muted{color:#666}',
      '</style></head><body>',
      '<div class="wrap"><div class="card">',
      '<h1 class="info">Please wait…</h1>',
      '<p class="muted">Updating system…</p>',
      '</div></div>',
      '<script>setTimeout(function(){window.location.replace(', JSON.stringify(commitUrl), ');}, 30);</script>',
      '</body></html>'
    ].join('')).setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
    return html;
  }
  function __renderAckResultPage(kind, alert, others, responder) {
  // kind: 'done' | 'already' | 'error' | 'bulk_done' | 'bulk_error'
  let header = '';
  let body   = '';

  if (kind === 'bulk_done') {
    header = '<h1 class="ok">All selected alerts acknowledged.</h1>';
    body   = '<p class="muted">Thanks ' + __esc(responder && responder.name || 'team') + '.</p>';
    return __renderHtmlPage('Acknowledged', [header, body, '<div class="btns"><a class="btn" href="about:blank" onclick="window.close();return false;">Close</a></div>'].join(''));
  }
  if (kind === 'bulk_error') {
    header = '<h1 class="err">Bulk acknowledgement failed.</h1>';
    body   = '<p>Please try again.</p>';
    return __renderHtmlPage('Error', [header, body, '<div class="btns"><a class="btn" href="about:blank" onclick="window.close();return false;">Close</a></div>'].join(''));
  }

  if (kind === 'done') {
    const when = alert && alert.ack_when_iso ? __fmtWhen(alert.ack_when_iso) : '';
    header = '<h1 class="ok">Acknowledgement confirmed.</h1>';
    body   = '<p>We’ve recorded your acknowledgement and stopped further escalations.</p>' +
             (when ? ('<p class="muted">When: ' + __esc(when) + '</p>') : '');
  } else if (kind === 'already') {
    const who  = (alert && (alert.ack_name || (alert.ack_number ? __maskMsisdn(alert.ack_number) : 'another team member'))) || 'another team member';
    const when = alert && alert.ack_when_iso ? __fmtWhen(alert.ack_when_iso) : '';
    header = '<h1 class="warn">Already acknowledged.</h1>';
    body   = '<p>This alert was already acknowledged by <strong>' + __esc(who) + '</strong>' +
             (when ? (' at <strong>' + __esc(when) + '</strong>') : '') + '.</p>';
  } else {
    header = '<h1 class="err">Something went wrong.</h1>';
    body   = '<p>Please try again.</p>';
  }

  let othersHtml = '';
  if (others && others.length) {
    const rows = others.map(x => {
      return [
        '<tr>',
        '<td>', __esc(x.alert_id || ''), '</td>',
        '<td>', __esc(x.candidate_name || ''), '</td>',
        '<td>', __esc(x.job_title || (x.shift && (x.shift.job_title || x.shift.jobTitle) || '') || ''), '</td>',
        '<td>', __esc(x.hospital || ''), ' / ', __esc(x.ward || ''), '</td>',
        '<td>', __esc(x.date_label || ''), ' · ', __esc(x.time_range_label || ''), '</td>',
        '<td>', __esc(x.shift_type || ''), '</td>',
        '<td>', __esc(x.issue_type || ''), '</td>',
        '<td>', __esc(x.eta_or_leave_time_label || (x.issue && (x.issue.eta_or_leave_time_label || x.issue.eta_or_leave_time) || '') || ''), '</td>',
        '<td>', __esc(x.reason_text || ''), '</td>',
        '</tr>'
      ].join('');
    }).join('');

    const idsCsv = others.map(o => o.alert_id).join(',');
    const expUnix = Math.floor(Date.now()/1000) + 180;
    let acceptAllUrl = '';
    try {
      const msisdn = responder && responder.msisdn ? responder.msisdn : '';
      acceptAllUrl = (function(idsCsv, expUnix, msisdnOpt){
        const base = ScriptApp.getService().getUrl();
        const token = __signBulk(idsCsv, expUnix, msisdnOpt);
        const qs = 'action=EMERGENCY_ACK_ALL&ids=' + encodeURIComponent(idsCsv) +
                   (msisdnOpt ? ('&msisdn=' + encodeURIComponent(msisdnOpt)) : '') +
                   '&t=' + encodeURIComponent(token);
        return base + '?' + qs;
      })(idsCsv, expUnix, responder && responder.msisdn || '');
    } catch (_) {}

    othersHtml = [
      '<div class="tablewrap" style="margin-top:18px">',
      '<h2>Other outstanding alerts</h2>',
      '<p>Would you like to accept them all as well?</p>',
      '<table><thead><tr>',
      '<th>Alert ID</th><th>Candidate</th><th>Role</th><th>Hospital/Ward</th><th>Date/Time</th><th>Shift</th><th>Issue</th><th>Time detail</th><th>Reason</th>',
      '</tr></thead><tbody>', rows, '</tbody></table>',
      '</div>',
      (acceptAllUrl ? ('<div class="btns"><a class="btn primary" href="' + __esc(acceptAllUrl) + '">Yes, accept all outstanding alerts</a>' +
                       '<a class="btn" href="about:blank" onclick="window.close();return false;">No, just this one</a></div>')
                    : '')
    ].join('');
  }

  const details = alert ? (function(){
    const role      = alert.job_title || (alert.shift && (alert.shift.job_title || alert.shift.jobTitle)) || '';
    const hospital  = alert.hospital || (alert.shift && alert.shift.hospital) || '';
    const ward      = alert.ward || (alert.shift && alert.shift.ward) || '';
    const dateLabel = alert.date_label || '';
    const timeRange = alert.time_range_label || '';
    const shiftType = alert.shift_type || '';
    const issueType = alert.issue_type || '';
    const etaLabel  = alert.eta_or_leave_time_label || (alert.issue && (alert.issue.eta_or_leave_time_label || alert.issue.eta_or_leave_time)) || '';
    const reason    = alert.reason_text || '';
    const candidate = alert.candidate_name || alert.candidateName || 'Colleague';

    return [
      '<dl>',
      '<dt>Alert ID</dt><dd>', __esc(alert.alert_id || ''), '</dd>',
      '<dt>Candidate</dt><dd>', __esc(candidate), ' (', __esc(role), ')</dd>',
      '<dt>Hospital/Ward</dt><dd>', __esc(hospital), ' / ', __esc(ward), '</dd>',
      '<dt>Date / Time</dt><dd>', __esc(dateLabel), ' · ', __esc(timeRange), '</dd>',
      '<dt>Shift</dt><dd>', __esc(shiftType), '</dd>',
      '<dt>Issue</dt><dd>', __esc(issueType), '</dd>',
      '<dt>Time detail</dt><dd>', __esc(etaLabel || 'N/A'), '</dd>',
      '<dt>Reason</dt><dd>', __esc(reason), '</dd>',
      '</dl>'
    ].join('');
  })() : '';

  return __renderHtmlPage(
    (kind === 'done' ? 'Acknowledged' : kind === 'already' ? 'Already acknowledged' : 'Error'),
    [header, body, details, othersHtml, '<div class="btns"><a class="btn" href="about:blank" onclick="window.close();return false;">Close</a></div>'].join('')
  );
}


  // ───────────────────────── cohort helpers used by tiles + emergency ─────────────────────────
  function __shiftInProgress(startIso, endIso, nowIso) {
    const now = new Date(nowIso || new Date().toISOString());
    const s = __parseIsoOrNull(startIso);
    const e = __parseIsoOrNull(endIso);
    return !!(s && e && s.getTime() <= now.getTime() && now.getTime() < e.getTime());
  }

  /**
   * Ensure we have a fresh-ish cohort object for a key.
   * Strategy:
   *  - Try _readCohort(key).
   *  - If missing/stale and fallback inputs given (rota, bookedMap), recompute with _computeCohortFromBookedMap and _writeCohort.
   */
  function __readOrComputeCohort(key, opts) {
  opts = opts || {};
  try {
    let coh = _readCohort(key); // { key, state, ts, members:[{msisdn07,firstName,surname,surnameInitial,role,bookingRef}] }
    const stale = false;

    if (!coh || stale) {
      if (opts.bookedMap && typeof _computeCohortFromBookedMap === 'function') {
        coh = _computeCohortFromBookedMap(key, opts.bookedMap) || { key, state:'none', members:[] };
        try { if (coh) _writeCohort(key, coh); } catch (_) {}
      }
    }

    // ── fill missing msisdn07 via Links index (requires FULL surname & FULL firstName)
    if (coh && Array.isArray(coh.members) && coh.members.length) {
      let filled = 0;

      function __mask07(s) {
        try {
          const v = String(s || '').replace(/\s+/g,'');
          return v ? (v.slice(0,3) + '•••••' + v.slice(-4)) : '';
        } catch (_) { return ''; }
      }

      coh.members = coh.members.map(function (m, idx) {
        try {
          const out = Object.assign({}, m);
          const has07 = out.msisdn07 && /^07\d{9}$/.test(String(out.msisdn07));

          if (!has07) {
            const surname = String(out.surname || '').trim();         // FULL surname expected
            const first   = String(out.firstName || '').trim();       // FULL first name expected

            if (surname && first) {
              try { _log('cohort','links_lookup_attempt', { key, idx, first, surname }); } catch (_) {}
              const looked = getPhoneFromLinksIndex(surname, first);
              try { _log('cohort','links_lookup_result', { key, idx, first, surname, found: !!looked, resultMasked: __mask07(looked) }); } catch (_) {}
              if (looked) { out.msisdn07 = looked; filled++; }
            } else {
              try {
                _log('cohort','links_lookup_skipped', {
                  key, idx, reason: 'missing_name_parts',
                  haveFirst: !!first, haveSurname: !!surname
                });
              } catch (_) {}
            }
          }

          // Ensure convenience initial (storage keeps FULL surname; this is non-destructive)
          if (!out.surnameInitial) {
            const s = String(out.surname || '').trim();
            out.surnameInitial = s ? s[0].toUpperCase() : '';
          }
          return out;
        } catch (ex) {
          try { _log('cohort','links_lookup_member_exception',{ key, idx, message: String(ex && ex.message || ex) }); } catch(__){}
          return m;
        }
      });

      try { if (filled) _log('cohort','enriched_numbers', { key, filled, total: coh.members.length }); } catch (_){}
    }

    return coh || { key, state:'none', members:[] };
  } catch (ex) {
    try { _log('cohort','read_or_compute_exception',{ key, message: String(ex && ex.message || ex) }); } catch(_){}
    return { key, state:'none', members:[] };
  }
}


function __dropSelfFromMembers(members, msisdn07, selfName) {
  const me = __to07(msisdn07);
  const selfFirst = String(selfName && selfName.first || '').trim().toLowerCase();
  const selfSur   = String(selfName && selfName.surname || '').trim().toLowerCase();
  const selfInit  = String(selfName && selfName.surnameInitial || '').trim().toUpperCase();

  return (members || []).filter(m => {
    const mNum = __to07(m && (m.msisdn07 || m.msisdn || m.mobile));
    if (mNum && me) return mNum !== me; // primary: phone match

    // fallback: name match
    const f   = String(m && m.firstName || '').trim().toLowerCase();
    const sur = String(m && m.surname || '').trim().toLowerCase();           // ← now available
    const si  = String(m && m.surnameInitial || '').trim().toUpperCase();

    // strict: full surname match
    if (f && sur && selfFirst && selfSur && f === selfFirst && sur === selfSur) return false;

    // relaxed: initial match
    if (f && si && selfFirst && selfInit && f === selfFirst && si === selfInit) return false;

    return true;
  });
}

  // ────────────────────────────────────────────────────────────────────────────────
  // AFTER
try {
  __mark('start', { rawParams: !!(e && e.parameter) });

  // ⬇️ entry breadcrumb
  logminimal('doGet','start',{
    rid: __RID,
    view: (e && e.parameter && String(e.parameter.view||'').toLowerCase()) || '',
    action: (e && e.parameter && String(e.parameter.action||'').toUpperCase()) || '',
    hadK: !!(e && e.parameter && typeof e.parameter.k !== 'undefined' && String(e.parameter.k||'').trim() !== ''),
    t_present: !!(e && e.parameter && typeof e.parameter.t !== 'undefined' && String(e.parameter.t||'').trim() !== ''),
    t_len: (e && e.parameter && e.parameter.t) ? String(e.parameter.t).length : 0
  });
  // ...
// ── Public CTA routes (NO token auth; secured via HMAC tokens) ──
const action = (e && e.parameter && String(e.parameter.action || '').toUpperCase()) || '';
// (Place these two lines near the top of doGet(e), right after you derive `action`)
try { __action = action; } catch(_) {}
logminimal('HTTP', 'doGet_entry', {
  action: String(action||''),
  qs_keys: Object.keys((e && e.parameter) || {})
});

// ───────────── NEW: EMBED GET routes (JSONP responses for public page) ─────────────
if (action === 'EMERGENCY_ACK_LINK_EMBED') {
  var composite = String((e && e.parameter && ((e.parameter.p || e.parameter.alert_id) || ''))).trim();
  __mark('cta_ack_embed_link:get_entry', { composite });

  // NEW: log request shape (p length + preview)
  var pRaw = composite || '';
  logminimal('ACK_LINK_EMBED', 'request', {
    p_len: pRaw.length,
    p_preview: pRaw.slice(0, 16) + '…'
  });

  if (!composite) {
    return __jsonp(e, { type: 'ACK_LINK', payload: { ok:false, error:'MISSING_ALERT' } }, 400);
  }
  var alertId = composite, msisdnFromLink = '';
  if (composite.indexOf('~') !== -1) {
    var i = composite.lastIndexOf('~');
    alertId = composite.slice(0, i);
    msisdnFromLink = composite.slice(i+1);
  }
  var alert = _emergencyFindById(alertId);
  if (!alert) {
    __mark('cta_ack_embed_link:get_not_found', { alert_id: alertId });
    return __jsonp(e, { type: 'ACK_LINK', payload: { ok:false, error:'ALERT_NOT_FOUND' } }, 404);
  }
  try {
    var expUnix = Math.floor(Date.now()/1000) + 180;
    var t = __signAck(alertId, expUnix, msisdnFromLink || null);

    // NEW: log token signing outcome
    logminimal('ACK_LINK_EMBED', 'signed', {
      ok: !!(t),
      exp: expUnix || null
    });

    return __jsonp(e, { type: 'ACK_LINK', payload: { ok:true, alert_id: alertId, msisdn: msisdnFromLink || '', t: t, exp: expUnix } }, 200);
  } catch (ex) {
    __mark('cta_ack_embed_link:get_token_error', { error: String(ex && ex.message || ex) });
    return __jsonp(e, { type: 'ACK_LINK', payload: { ok:false, error:'TOKEN_ERROR' } }, 500);
  }
}

if (action === 'EMERGENCY_ACK_COMMIT_EMBED') {
  var alertIdIn = String((e && e.parameter && ((e.parameter.alert_id || e.parameter.p) || ''))).trim();
  var msisdnIn  = String((e && e.parameter && (e.parameter.msisdn || ''))).trim();
  var tokenIn   = String((e && e.parameter && (e.parameter.t || ''))).trim();
  __mark('cta_ack_embed_commit:get_entry', { alert_id: alertIdIn || '(missing)', msisdn_masked: __maskMsisdn(msisdnIn) });

  // NEW: log pre-verify inputs (masked)
  logminimal('ACK_COMMIT_EMBED', 'preverify', {
    alert_id: alertIdIn,
    msisdn_masked: (typeof __maskMsisdn==='function' ? __maskMsisdn(msisdnIn) : (msisdnIn ? msisdnIn.slice(0,3)+'…'+msisdnIn.slice(-3) : '')),
    t_len: tokenIn.length
  });

  if (!alertIdIn || !tokenIn) {
    return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'MISSING_PARAMS' } }, 400);
  }
  var v = __verifyAck(alertIdIn, msisdnIn || null, tokenIn);

  // NEW: log verify outcome
  logminimal('ACK_COMMIT_EMBED', v.ok ? 'verify_ok' : 'verify_fail', {
    reason: v.reason || '',
    now: v.now || null,
    exp: v.exp || null
  });

  if (!v.ok) {
    __mark('cta_ack_embed_commit:get_token_fail', { alert_id: alertIdIn, reason: v.reason || 'VERIFY_FAIL' });
    return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'INVALID_OR_EXPIRED_LINK' } }, 403);
  }

  var lock = LockService.getScriptLock();
  try { lock.waitLock(5000); } catch (_) {
    __mark('cta_ack_embed_commit:get_lock_fail', { alert_id: alertIdIn });
    return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'BUSY' } }, 503);
  }

  try {
    var alert = _emergencyFindById(alertIdIn);
    if (!alert) {
      __mark('cta_ack_embed_commit:get_not_found', { alert_id: alertIdIn });
      return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'ALERT_NOT_FOUND' } }, 404);
    }
    var already = (alert.acknowledged === true || String(alert.acknowledged) === 'true');
    var responderName = __resolveResponder(__to447(msisdnIn)) || 'Web link';

    if (!already) {
      _emergencyUpdateById(alertIdIn, {
        acknowledged: true,
        status: 'ACKED',
        ack_source: 'CTA_EMBED',
        ack_name: responderName,
        ack_number: String(msisdnIn || ''),
        ack_when_iso: _nowIso()
      });
      var fresh = _emergencyFindById(alertIdIn) || alert;
      try { _cancelTriggersForAlert(fresh); } catch(_) {}
      try { _eaAppendLog_(alertIdIn, 'ACK via CTA EMBED'); } catch(_) {}
      try { _sendEmergencyAckEmailPA(fresh, responderName, fresh.ack_when_iso || _nowIso()); } catch(_) {}
      try { _sendWatiBulkTemplate_EMERGENCY_RESPONDED(fresh, responderName, fresh.ack_when_iso || _nowIso()); } catch(_) {}
      try { __sendResponderConfirm(msisdnIn, responderName, 'EMERG_ACCEPT_CONFIRM'); } catch(_) {}
      alert = fresh;
    }

    var others = __listOtherOutstandingAlerts(alertIdIn) || [];
    var bulk = null;
    if (others.length) {
      var idsCsv = others.map(function(o){ return o.alert_id; }).join(',');
      var expUnix = Math.floor(Date.now()/1000) + 180;
      try {
        var bulkToken = __signBulk(idsCsv, expUnix, msisdnIn || null);
        bulk = { idsCsv: idsCsv, msisdn: String(msisdnIn||''), t: bulkToken, exp: expUnix };
      } catch (_) {}
    }

    // NEW: log final result shape
    var kind = already ? 'already' : 'done';
    logminimal('ACK_COMMIT_EMBED', 'result', {
      kind: String(kind || ''),
      others_count: (others && others.length) || 0,
      has_bulk: !!bulk
    });

    return __jsonp(e, {
      type: 'ACK_RESULT',
      payload: {
        ok: true,
        kind: kind,
        alert: __alertSummary_(alert),
        others: (others || []).map(__alertSummary_),
        bulk: bulk
      }
    }, 200);

  } catch (ex) {
    __mark('cta_ack_embed_commit:get_exception', { error: String(ex && ex.message || ex) });
    return __jsonp(e, { type:'ACK_RESULT', payload:{ ok:false, error:'EXCEPTION', detail:String(ex && ex.message || ex) } }, 500);

  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}

if (action === 'EMERGENCY_ACK_ALL_EMBED') {
  var idsCsv = String((e && e.parameter && (e.parameter.ids || ''))).trim();
  var msisdnParam = String((e && e.parameter && (e.parameter.msisdn || ''))).trim();
  var token  = String((e && e.parameter && (e.parameter.t || ''))).trim();
  __mark('ack_all_embed:get_entry', { count: idsCsv ? idsCsv.split(',').filter(Boolean).length : 0, msisdn_masked: __maskMsisdn(msisdnParam) });

  // NEW: log pre-verify inputs (masked)
  logminimal('ACK_ALL_EMBED', 'preverify', {
    ids_count: (idsCsv ? idsCsv.split(',').filter(Boolean).length : 0),
    msisdn_masked: (typeof __maskMsisdn==='function' ? __maskMsisdn(msisdnParam) : (msisdnParam ? msisdnParam.slice(0,3)+'…'+msisdnParam.slice(-3) : '')),
    t_len: token.length
  });

  var v = __verifyBulk(idsCsv, msisdnParam || null, token);

  // NEW: log verify outcome
  logminimal('ACK_ALL_EMBED', v.ok ? 'verify_ok' : 'verify_fail', { reason: v.reason || '' });

  if (!v.ok) {
    __mark('ack_all_embed:get_token_fail', { reason: v.reason || 'VERIFY_FAIL' });
    return __jsonp(e, { type:'ACK_ALL_RESULT', payload:{ ok:false, error:'INVALID_OR_EXPIRED_LINK' } }, 403);
  }

  var ids = idsCsv.split(',').map(function(s){ return String(s || '').trim(); }).filter(Boolean);
  var responderName = __resolveResponder(__to447(msisdnParam)) || 'Web link (bulk)';

  var lock = LockService.getScriptLock();
  try { lock.waitLock(5000); } catch (_) {
    __mark('ack_all_embed:get_lock_fail', {});
    return __jsonp(e, { type:'ACK_ALL_RESULT', payload:{ ok:false, error:'BUSY' } }, 503);
  }

  var ackedIds = [];
  try {
    ids.forEach(function(id){
      try {
        var a = _emergencyFindById(id);
        if (!a) { __mark('ack_all_embed:get_skip_missing', { id:id }); return; }
        if (a.acknowledged === true || String(a.acknowledged) === 'true') { __mark('ack_all_embed:get_skip_already', { id:id }); return; }

        _emergencyUpdateById(id, {
          acknowledged: true,
          status: 'ACKED',
          ack_source: 'CTA_ALL_EMBED',
          ack_name: responderName,
          ack_number: String(msisdnParam || ''),
          ack_when_iso: _nowIso()
        });
        var fresh = _emergencyFindById(id);
        try { _cancelTriggersForAlert(fresh); } catch(_) {}
        try { _eaAppendLog_(id, 'ACK via CTA ALL EMBED'); } catch(_) {}
        __mark('ack_all_embed:get_acked', { id:id });
        ackedIds.push(id);
      } catch (innerEx) {
        __mark('ack_all_embed:get_item_exception', { id:id, error: String(innerEx && innerEx.message || innerEx) });
      }
    });

    if (ackedIds.length > 0) {
      try { __sendBulkResponderConfirm(msisdnParam, responderName); } catch (_) {}
      try {
        var whenIso = _nowIso();
        __sendBulkAllAlertsRespondedToOthers(responderName, whenIso, msisdnParam);
      } catch (_) {}
    } else {
      __mark('ack_all_embed:get_no_new_acks', { count: ids.length });
    }

    // NEW: log acked summary
    logminimal('ACK_ALL_EMBED', 'acked_summary', {
      requested: ids.length,
      ackedCount: ackedIds.length
    });

    __mark('ack_all_embed:get_done', { count: ids.length, ackedCount: ackedIds.length });
    return __jsonp(e, { type:'ACK_ALL_RESULT', payload:{ ok:true, ackedCount: ackedIds.length, ids: ackedIds } }, 200);

  } catch (ex) {
    __mark('ack_all_embed:get_exception', { error: String(ex && ex.message || ex) });
    return __jsonp(e, { type:'ACK_ALL_RESULT', payload:{ ok:false, error:'EXCEPTION', detail:String(ex && ex.message || ex) } }, 500);

  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}
    // 1) ENTRY PAGE for single ACK
    if (action === 'EMERGENCY_ACK_LINK') {
      let composite = (e && e.parameter && String(e.parameter.alert_id || '').trim()) || '';
      __mark('cta_ack:entry', { composite });

      let alertId = composite;
      let msisdnFromLink = '';
      if (composite.indexOf('~') !== -1) {
        const i = composite.lastIndexOf('~');
        alertId = composite.slice(0, i);
        msisdnFromLink = composite.slice(i+1);
      }

      if (!alertId) {
        return __renderHtmlPage('Acknowledge', '<h2>Missing alert</h2><p>Sorry, we could not identify the alert you attempted to acknowledge.</p>');
      }

      const alert = _emergencyFindById(alertId);
      if (!alert) {
        __mark('cta_ack:not_found', { alert_id: alertId });
        return __renderHtmlPage('Acknowledge', '<h2>Alert not found</h2><p>The alert could not be found or may have expired.</p>');
      }

      const expUnix = Math.floor(Date.now()/1000) + 180;
      let token;
      try {
        token = __signAck(alertId, expUnix, msisdnFromLink || null);
      } catch (ex) {
        __mark('cta_ack:token_error', { error: String(ex && ex.message || ex) });
        return __renderHtmlPage('Acknowledge', '<h2>Temporary issue</h2><p>We could not prepare the acknowledgement page. Please try the link again in a moment.</p>');
      }

      const base = ScriptApp.getService().getUrl();
      const params = [
        'action=EMERGENCY_ACK_COMMIT',
        'alert_id=' + encodeURIComponent(alertId),
        (msisdnFromLink ? ('&msisdn=' + encodeURIComponent(msisdnFromLink)) : ''),
        't=' + encodeURIComponent(token),
        'r=' + encodeURIComponent(__RID)
      ].filter(Boolean).join('&');
      const commitUrl = base + '?' + params;

      __mark('cta_ack:redirect_pending_page_return', { alert_id: alertId, msisdn_masked: __maskMsisdn(msisdnFromLink) });
      return __renderPendingRedirectPage(commitUrl);
    }

    // 1b) BULK ACK ALL outstanding
    if (action === 'EMERGENCY_ACK_ALL') {
      const idsCsv = (e && e.parameter && String(e.parameter.ids || '')) || '';
      const msisdn = (e && e.parameter && String(e.parameter.msisdn || '')) || '';
      __mark('ack_all:entry', { count: idsCsv ? idsCsv.split(',').filter(Boolean).length : 0, msisdn_masked: __maskMsisdn(msisdn) });

      const v = __verifyBulk(idsCsv, msisdn || null, (e && e.parameter && String(e.parameter.t || '').trim()) || '');
      if (!v.ok) {
        __mark('ack_all:token_fail', { reason: v.reason || 'VERIFY_FAIL' });
        return __renderHtmlPage('Bulk acknowledgement', '<h1 class="err">Invalid or expired link</h1><p>Please initiate the action again.</p>');
      }

      const ids = idsCsv.split(',').map(s => String(s || '').trim()).filter(Boolean);
      const responderName = __resolveResponder(__to447(msisdn)) || 'Web link (bulk)';

      const lock = LockService.getScriptLock();
      try { lock.waitLock(5000); } catch (_) {
        __mark('ack_all:lock_fail');
        return __renderHtmlPage('Bulk acknowledgement', '<h1 class="err">System busy</h1><p>Please try again.</p>');
      }

      const ackedAlerts = [];
      try {
        ids.forEach(id => {
          try {
            const a = _emergencyFindById(id);
            if (!a) { __mark('ack_all:skip_missing', { id }); return; }
            if (a.acknowledged === true || String(a.acknowledged) === 'true') { __mark('ack_all:skip_already', { id }); return; }

            _emergencyUpdateById(id, {
              acknowledged: true,
              status: 'ACKED',
              ack_source: 'CTA_ALL',
              ack_name: responderName,
              ack_number: String(msisdn || ''),
              ack_when_iso: _nowIso()
            });
            const fresh = _emergencyFindById(id);
            try { _cancelTriggersForAlert(fresh); } catch(_) {}
            try { _eaAppendLog_(id, 'ACK via CTA ALL link'); } catch(_) {}
            __mark('ack_all:acked', { id });
            ackedAlerts.push(fresh || a);
          } catch (innerEx) {
            __mark('ack_all:item_exception', { id, error: String(innerEx && innerEx.message || innerEx) });
          }
        });

     // ...after the forEach(ids) loop that fills ackedAlerts...
if (ackedAlerts.length > 0) {
  try { __sendBulkResponderConfirm(msisdn, responderName) } catch (_) {}
  try {
    const whenIso = (ackedAlerts[0] && ackedAlerts[0].ack_when_iso) || _nowIso();
    __sendBulkAllAlertsRespondedToOthers(responderName, whenIso, msisdn);
  } catch (_) {}
} else {
  __mark('ack_all:no_new_acks', { count: ids.length });
}

__mark('ack_all:done', { count: ids.length, ackedCount: ackedAlerts.length });

const responder = { msisdn: msisdn || '', name: responderName };
return __renderAckResultPage('bulk_done', null, null, responder);

      } catch (ex) {
        __mark('ack_all:exception', { error: String(ex && ex.message || ex) });
        return __renderAckResultPage('bulk_error', null, null, null);
      } finally {
        try { lock.releaseLock(); } catch (_) {}
      }
    }

    // 1c) BULK ACK ALL COMMIT (optional second hop)
    if (action === 'EMERGENCY_ACK_ALL_COMMIT') {
      const idsCsv = (e && e.parameter && String(e.parameter.ids || '')) || '';
      const msisdn = (e && e.parameter && String(e.parameter.msisdn || '')) || '';
      __mark('ack_all_commit:entry', { count: idsCsv ? idsCsv.split(',').filter(Boolean).length : 0, msisdn_masked: __maskMsisdn(msisdn) });

      const v = __verifyBulk(idsCsv, msisdn || null, (e && e.parameter && String(e.parameter.t || '').trim()) || '');
      if (!v.ok) {
        __mark('ack_all_commit:token_fail', { reason: v.reason || 'VERIFY_FAIL' });
        return __renderHtmlPage('Bulk acknowledgement', '<h1 class="err">Invalid or expired link</h1><p>Please try the link again.</p>');
      }

      const ids = idsCsv.split(',').map(s => String(s || '').trim()).filter(Boolean);
      const responderName = __resolveResponder(__to447(msisdn)) || 'Web link (bulk)';

      const lock = LockService.getScriptLock();
      try { lock.waitLock(5000); } catch (_) {
        __mark('ack_all_commit:lock_fail');
        return __renderHtmlPage('Bulk acknowledgement', '<h1 class="err">System busy</h1><p>Please try again.</p>');
      }

      const ackedAlerts = [];
      try {
        ids.forEach(id => {
          try {
            const a = _emergencyFindById(id);
            if (!a) { __mark('ack_all_commit:skip_missing', { id }); return; }
            if (a.acknowledged === true || String(a.acknowledged) === 'true') { __mark('ack_all_commit:skip_already', { id }); return; }

            _emergencyUpdateById(id, {
              acknowledged: true,
              status: 'ACKED',
              ack_source: 'CTA_ALL',
              ack_name: responderName,
              ack_number: String(msisdn || ''),
              ack_when_iso: _nowIso()
            });
            const fresh = _emergencyFindById(id);
            try { _cancelTriggersForAlert(fresh); } catch(_) {}
            try { _eaAppendLog_(id, 'ACK via CTA ALL link'); } catch(_) {}
            __mark('ack_all_commit:acked', { id });
            ackedAlerts.push(fresh || a);
          } catch (innerEx) {
            __mark('ack_all_commit:item_exception', { id, error: String(innerEx && innerEx.message || innerEx) });
          }
        });

        if (ackedAlerts.length > 0) {
  try { __sendBulkResponderConfirm(msisdn, responderName); } catch (_) {}
  try {
    const whenIso = (ackedAlerts[0] && ackedAlerts[0].ack_when_iso) || _nowIso();
    __sendBulkAllAlertsRespondedToOthers(responderName, whenIso, msisdn);
  } catch (_) {}
} else {
  __mark('ack_all_commit:no_new_acks', { count: ids.length });
}

__mark('ack_all_commit:done', { count: ids.length, ackedCount: ackedAlerts.length });

const responder = { msisdn: msisdn || '', name: responderName };
return __renderAckResultPage('bulk_done', null, null, responder);

      } catch (ex) {
        __mark('ack_all_commit:exception', { error: String(ex && ex.message || ex) });
        return __renderAckResultPage('bulk_error', null, null, null);
      } finally {
        try { lock.releaseLock(); } catch (_) {}
      }
    }

    // 2) COMMIT: single ACK
    if (action === 'EMERGENCY_ACK_COMMIT') {
      const alertId = (e && e.parameter && String(e.parameter.alert_id || '').trim()) || '';
      const msisdn  = (e && e.parameter && String(e.parameter.msisdn || '').trim()) || '';
      const token   = (e && e.parameter && String(e.parameter.t || '').trim()) || '';
      __mark('cta_ack_commit:entry', { alert_id: alertId || '(missing)', msisdn_masked: __maskMsisdn(msisdn) });

      if (!alertId) {
        return __renderHtmlPage('Acknowledge', '<h1 class="err">Missing alert</h1><p>We could not identify the alert you attempted to acknowledge.</p>');
      }

      const v = __verifyAck(alertId, msisdn || null, token);
      if (!v.ok) {
        __mark('cta_ack_commit:token_fail', { alert_id: alertId, reason: v.reason || 'VERIFY_FAIL' });
        return __renderHtmlPage('Acknowledge', '<h1 class="err">Invalid or expired link</h1><p>Please try the link again.</p>');
      }

      const lock = LockService.getScriptLock();
      try { lock.waitLock(5000); } catch (_) {
        __mark('cta_ack_commit:lock_fail', { alert_id: alertId });
        return __renderHtmlPage('Acknowledge', '<h1 class="err">System busy</h1><p>Please try again.</p>');
      }

      try {
        const alert = _emergencyFindById(alertId);
        if (!alert) {
          __mark('cta_ack_commit:not_found', { alert_id: alertId });
          return __renderHtmlPage('Acknowledge', '<h1 class="err">Alert not found</h1><p>The alert could not be found or may have expired.</p>');
        }

        const already = (alert.acknowledged === true || String(alert.acknowledged) === 'true');
        const responderName = __resolveResponder(__to447(msisdn)) || 'Web link';

        if (already) {
          const others = __listOtherOutstandingAlerts(alertId);
          const responder = { msisdn: msisdn || '', name: responderName };
          return __renderAckResultPage('already', alert, others, responder);
        }

        _emergencyUpdateById(alertId, {
          acknowledged: true,
          status: 'ACKED',
          ack_source: 'CTA',
          ack_name: responderName,
          ack_number: String(msisdn || ''),
          ack_when_iso: _nowIso()
        });

        const fresh = _emergencyFindById(alertId) || alert;
        try { _cancelTriggersForAlert(fresh); } catch(_) {}
        try { _eaAppendLog_(alertId, 'ACK via CTA link (commit)'); } catch(_) {}
        try { _sendEmergencyAckEmailPA(fresh, responderName, fresh.ack_when_iso || _nowIso()); } catch (_) {}
        try { _sendWatiBulkTemplate_EMERGENCY_RESPONDED(fresh, responderName, fresh.ack_when_iso || _nowIso()); } catch (_) {}
        try { __sendResponderConfirm(msisdn, responderName, 'EMERG_ACCEPT_CONFIRM'); } catch (_) {}

        const others = __listOtherOutstandingAlerts(alertId);
        const responder = { msisdn: msisdn || '', name: responderName };
        return __renderAckResultPage('done', fresh, others, responder);
      } catch (ex) {
        __mark('cta_ack_commit:exception', { error: String(ex && ex.message || ex) });
        return __renderHtmlPage('Acknowledge', '<h1 class="err">Unexpected error</h1><p>' + __esc(String(ex && ex.message || ex)) + '</p>');
      } finally {
        try { lock.releaseLock(); } catch (_) {}
      }
    }

    // ── Normal (token-protected) routes & full app behaviour ──
    // AFTER
try {
  logminimal('auth','enforceToken:enter',{
    rid: __RID,
    view: (e && e.parameter && String(e.parameter.view||'').toLowerCase()) || '',
    hadK: !!(e && e.parameter && typeof e.parameter.k !== 'undefined' && String(e.parameter.k||'').trim() !== ''),
    t_present: !!(e && e.parameter && typeof e.parameter.t !== 'undefined' && String(e.parameter.t||'').trim() !== '')
  });

  _enforceToken(e);
  __mark('auth:ok');
  logminimal('auth','enforceToken:ok',{ rid: __RID });
} catch (authErr) {
  const hadK = !!(e && e.parameter && typeof e.parameter.k !== 'undefined' && String(e.parameter.k || '').trim() !== '');
  __mark('auth:fail', { error: String(authErr && authErr.message || authErr), hadK });

  // ⬇️ explicit 401 trace
  logminimal('auth','enforceToken:fail',{
    rid: __RID,
    hadK,
    view: (e && e.parameter && String(e.parameter.view||'').toLowerCase()) || '',
    t_present: !!(e && e.parameter && typeof e.parameter.t !== 'undefined' && String(e.parameter.t||'').trim() !== ''),
    willReturn: { code: 401, err: 'UNAUTHORIZED' }
  });

  return _err("UNAUTHORIZED", 401);
}

    const hadK = !!(e && e.parameter && typeof e.parameter.k !== 'undefined' && String(e.parameter.k || '').trim() !== '');

    // Resolve identity
   // AFTER
let msisdn = null;
try {
  msisdn = _resolveMsisdnFromRequest(e);
  __mark('identity:resolved', { msisdn_masked: __maskMsisdn(msisdn) });
  logminimal('identity','resolved',{ rid: __RID, msisdn_masked: __maskMsisdn(msisdn) });
} catch (idErr) {
  __mark('identity:exception', { error: String(idErr && idErr.message || idErr) });
  logminimal('identity','exception',{ rid: __RID, reason: String(idErr && idErr.message || idErr) });
}
if (!msisdn) {
  __mark('identity:missing', { hadK });
  if (hadK) {
    logminimal('identity','missing_with_token',{
      rid: __RID, hadK: true,
      willReturn: { code: 401, err: 'UNAUTHORIZED_TOKEN' }
    });
    return _err("UNAUTHORIZED_TOKEN", 401, { hadK: true });
  }
  logminimal('identity','missing_no_token',{
    rid: __RID, hadK: false,
    willReturn: { code: 400, err: 'INVALID_OR_UNKNOWN_IDENTITY' }
  });
  return _err("INVALID_OR_UNKNOWN_IDENTITY", 400);
}


    const props = PropertiesService.getScriptProperties();
    const appInfo = {
      version: props.getProperty("VERSION") || "",
      buildTs: props.getProperty("BUILD_TS") || ""
    };

   // AFTER
const view = (e.parameter && String(e.parameter.view || "").toLowerCase()) || "";
__mark('route', { view });
logminimal('route','enter',{ rid: __RID, view });

    // ---- content views
    if (view === "content") {
      const kind = String(e.parameter.kind || "").toUpperCase();
      let key = null, title = "";
      if (kind === "ACCOMMODATION") { key = "ACCOMMODATION_CONTACTS_HTML"; title = "Accommodation Contacts"; }
      else if (kind === "HOSPITAL") { key = "HOSPITAL_ADDRESSES_HTML"; title = "Hospital Addresses"; }
      else {
        __mark('content:unknown_kind', { kind });
        return _err("UNKNOWN_CONTENT_KIND", 400);
      }
      const html = props.getProperty(key) || "";
      __mark('content:ok', { kind, hasHtml: !!html });
      return _ok({ ok: true, title, html, appInfo });
    }

    // ---- appinfo
    if (view === "appinfo") {
      __mark('appinfo:return', appInfo);
      return _ok({ ok: true, appInfo });
    }
// ---- emergency_window (DNA = TODAY && shift IN-PROGRESS via resolver; cohorts are display-only)
function __injectRunningLateOptionsFixed(eligible) {
  // Fixed RL options we always want to present
  const FIXED_RL_OPTIONS = [
    'Less than 15 minutes',
    'Less than 30 minutes',
    'Less than 1 hour',
    'Less than 2 hours'
  ];

  return (eligible || []).map((it, idx) => {
    try {
      // Only attach when the shift is eligible to run late
      const canRL = (it && it.canRunLate === true);
      if (!canRL) return it;

      // If something already filled late_options, leave it alone
      const hasOpts = Array.isArray(it.late_options) && it.late_options.length > 0;
      if (hasOpts) return it;

      // Inject the fixed options and scope them clearly
      it.late_options = FIXED_RL_OPTIONS.slice();
      it.options_scope = 'RUNNING_LATE';

      try {
        _log('eligible', 'running_late_injected_fixed', {
          idx,
          options_len: it.late_options.length,
          preview: it.late_options.slice(0, 4)
        });
      } catch (_) {}

      return it;
    } catch (e) {
      try { _log('eligible', 'running_late_injected_error', { idx, error: String(e && e.message || e) }); } catch (_) {}
      return it;
    }
  });
}

if (view === "emergency_window") {
      // One-per-request telemetry for TEST_PHONE calls (independent of TEST_MODE)
    try {
      const tpRaw      = (props.getProperty("TEST_PHONE") || "").trim();
      const caller447  = __to447(msisdn);
      const test447    = __to447(tpRaw);
      if (caller447 && test447 && caller447 === test447) {
        logminimal('emergency_window', 'poll_from_TEST_PHONE', {
          msisdn_masked: __maskMsisdn(msisdn)
        });
      }
    } catch (_) {}

  try {
    const serverTime = (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString();

    // Optional TEST gating
    const TEST_MODE  = String(props.getProperty("TEST_MODE") || "").toUpperCase() === "TRUE";
    const TEST_PHONE = (props.getProperty("TEST_PHONE") || "").trim();
    let normTest = TEST_PHONE;
    try { normTest = (typeof _normaliseLocalTel === 'function') ? _normaliseLocalTel(TEST_PHONE) : TEST_PHONE; } catch(_) {}

    if (TEST_MODE && normTest && normTest !== msisdn) {
      __mark('emergency:test_gate_block', {
        TEST_MODE: true,
        TEST_PHONE_masked: __maskMsisdn(normTest),
        caller_masked: __maskMsisdn(msisdn)
      });
      return _ok({ ok: true, reason: "TEST_MODE", eligible: [], serverTime, appInfo });
    }

    const hasHelper = (typeof _getEmergencyEligibleShifts === 'function');
    __mark('emergency:helper_check', { hasHelper });

    if (!hasHelper) {
      __mark('emergency:no_helper_safe_empty');
      return _ok({ ok: true, eligible: [], serverTime, appInfo });
    }

    // Get eligible items
    let eligible = _getEmergencyEligibleShifts(msisdn, serverTime) || [];

    // Leave-early augmentation (so logging includes it)
    try {
      eligible = __augmentEligibleForLeaveEarly(eligible, serverTime, __TZ);
    } catch (ex) {
      __mark('leave_early_augment_error', { error: String(ex && ex.message || ex) });
    }

    // Do NOT inject RUNNING LATE options on the server — client fetches options later.

    // Single log (no late_options)
    __mark('emergency:eligible_result', {
      count: eligible.length,
      sample: (eligible || []).slice(0, 3).map((it, i) => ({
        idx: i,
        ymd: it.ymd,
        shift_type: it.shift_type || it.shiftType || '',
        startAtIso: it.startAtIso || (it.shift && (it.shift.startAtIso || it.shift.startIso)) || null,
        endAtIso:   it.endAtIso   || (it.shift && (it.shift.endAtIso   || it.shift.endIso))   || null,
        canRunLate: !!it.canRunLate,
        canCancel:  !!it.canCancel,
        allowed_issues: it.allowed_issues || it.allowedIssues || it.issuesAllowed || []
      }))
    });

    // DNA gating: TODAY && IN-PROGRESS via resolver (cohorts do NOT affect gating)
    try {
      const tz = __TZ;
      const todayYmd = Utilities.formatDate(new Date(serverTime), tz, 'yyyy-MM-dd');

      // (Optional) If you still want to show names on shift in the UI, keep this fallback builder; otherwise remove this block.
      let rota = null, cand = null, bookedMap = null;
      function ensureBookedMap() {
        if (!rota) {
          rota = _ssRota();
          cand = _findCandidateByMobile(rota, msisdn);
          if (!cand) return null;
          const headersMem = _ensureHeaders(rota);
          const wantYmds = headersMem.map(h => h.ymd);
          const nameKey = (cand.surname + " " + cand.firstname).toLowerCase().trim();
          bookedMap = _readBookedMap(rota, nameKey, wantYmds);
        }
        return bookedMap;
      }

      // Build a helper to compute the shift signature BASE (no issue) — used for shift-scoped DNA store
      
      

      eligible = (eligible || []).map(it => {
        try {
          const ymd        = it.ymd || (it.shift && it.shift.ymd) || '';
          const hospital   = it.hospital || (it.shift && it.shift.hospital) || '';
          const ward       = it.ward || (it.shift && it.shift.ward) || '';

          const rawShiftType = it.shift_type || (it.shift && it.shift.shift_type) || '';
          const shiftType = (function (S) {
            S = String(S || '').toUpperCase();
            if (S === 'N' || S.includes('NIGHT')) return 'NIGHT';
            if (S === 'LD' || S === 'LONG' || S.includes('LONG DAY') || S.includes('LONGDAY')) return 'LONG';
            if (S === 'D' || S.includes('DAY') || S.includes('EARLY') || S.includes('LATE')) return 'DAY';
            return 'UNKNOWN';
          })(rawShiftType);

          const shiftInfo  = it.shiftInfo || (it.shift && it.shift.shiftInfo) || it.time_range_label || '';

          // Resolve start/end using canonical helper
          let startAtIso = null, endAtIso = null, inProgress = false, resolved = null;
          try {
            if (typeof _runningLateResolveShiftTimes === 'function') {
              resolved   = _runningLateResolveShiftTimes({ ymd, shift_type: shiftType, shiftInfo, tz }) || null;
              startAtIso = resolved && resolved.startAtIso || null;
              endAtIso   = resolved && resolved.endAtIso   || null;
              inProgress = __shiftInProgress(startAtIso, endAtIso, serverTime);
            }
          } catch (_) {}
          const isToday = (ymd === todayYmd);
          const canDNA  = !!(isToday && inProgress);

          // Attach resolved times for the client (kept for EA exclusion + debugging)
          it.startAtIso_resolved = startAtIso;
          it.endAtIso_resolved   = endAtIso;
          it.inProgress          = inProgress;

          // Update allowed_issues with the new rule
          const ai = __ensureArray(it.allowed_issues || it.allowedIssues || it.issuesAllowed).filter(x => x !== 'DNA');
          if (canDNA && ai.indexOf('DNA') === -1) ai.push('DNA');
          it.allowed_issues = ai;
          it.canDNA = canDNA;

          // Cohorts for display only (names), not gating
          const canonicalType = (resolved && resolved.type) ? resolved.type : shiftType;

          const key = (typeof _cohortKey === 'function')
            ? _cohortKey(ymd, hospital, ward, canonicalType)
            : [ymd, hospital, ward, canonicalType].join('|');
          it.cohortKey = key;

          try {
            const coh = __readOrComputeCohort(key, { bookedMap: ensureBookedMap() });
            const selfName = {
              first: (cand && cand.firstname) || '',
              surname: (cand && cand.surname) || '',
              surnameInitial: __surnameInitial(cand && cand.surname) // used only by __dropSelfFromMembers
            };
            const members = __dropSelfFromMembers(coh.members || [], msisdn, selfName).map(m => ({
              firstName: m.firstName || '',
              surname:   m.surname || '',  // full surname
              msisdn07:  (m.msisdn07 && String(m.msisdn07)) || (m.msisdn ? __to07(m.msisdn) : '')
            }));
            it.cohort = members;
          } catch (_) {}

 // ───────────── // ───────────── NEW: shift-scoped DNA exclusions enforcement ─────────────
const sigBase = __sigBaseFromItem(it);
const propsSvc = PropertiesService.getScriptProperties();
let dnaExc = {};
try { dnaExc = JSON.parse(propsSvc.getProperty('EA_EXC_SHIFT::' + sigBase) || '{}') || {}; } catch (_) { dnaExc = {}; }

// Build the set of subject keys excluded for DNA on this shift
// subjectKey is either E.164 (447...) or a robust name key stored by the writer.
const excludedKeys = new Set(Object.keys(dnaExc || {}));

// If DNA is allowed on this item, filter its cohort by the shift-scoped exclusions
if (it.canDNA) {
  const members = __ensureArray(it.cohort || []);
  const wouldRemove = [];
  const filtered = members.filter(m => {
    const k = _dnaMemberKey(m);          // ← global helper (adds TEL:/NAME: + canonical form)
    const hit = k && excludedKeys.has(k);
    if (hit) wouldRemove.push({ firstName: m.firstName || '', surname: m.surname || '' });
    return !hit;
  });

  const isTestPhone = !!(TEST_MODE && normTest && normTest === msisdn);
  if (isTestPhone) {
    // TEST: do not actually filter; annotate what would be removed
    it.dna_excluded_preview = wouldRemove;
    try {
      if (wouldRemove.length) {
        const names = wouldRemove.map(x => (x.firstName + ' ' + x.surname).trim()).filter(Boolean).join(', ');
        const note = names ? ` (dna excluding: ${names})` : '';
        if (note) {
          if (it.time_range_label && !String(it.time_range_label).includes(note)) it.time_range_label += note;
          else if (it.shift && it.shift.time_range_label && !String(it.shift.time_range_label).includes(note)) it.shift.time_range_label += note;
        }
      }
    } catch (_) {}
  } else {
    // PROD: commit the filtering
    it.cohort = filtered;

    const aiBefore = __ensureArray(it.allowed_issues);
    let removedDNA = false;

    // If nobody left in the cohort, DNA cannot be raised — remove it from allowed_issues
    if (filtered.length === 0) {
      it.allowed_issues = aiBefore.filter(x => String(x || '').toUpperCase() !== 'DNA');
      removedDNA = (aiBefore.length !== it.allowed_issues.length);
    }

    // Keep canDNA consistent with allowed_issues
    const aiAfter = __ensureArray(it.allowed_issues);
    const dnaStillAllowed = aiAfter.some(x => String(x || '').toUpperCase() === 'DNA');
    if (!dnaStillAllowed) it.canDNA = false;  // invariant: no DNA in allowed_issues ⇒ canDNA=false
  }
}
// ───────────── END shift-scoped DNA exclusions enforcement ─────────────

} catch (_) {}
return it;
});

// (logs removed here)

} catch (errDNA) {}

// ── Exclusion filter — per-issue suppression (do not drop shifts) ──
try {
  const ms447 = __to447(msisdn);
  const key = 'EA_EXC::' + String(ms447 || '');
  const propsSvc = PropertiesService.getScriptProperties();
  const nowMs = Date.now();
  let excMap = {};
  try { excMap = JSON.parse(propsSvc.getProperty(key) || '{}') || {}; } catch (_) { excMap = {}; }

  // Lazy purge expired
  let purged = 0;
  Object.keys(excMap).forEach(sig => {
    const k = excMap[sig] || {};
    const expMs = new Date(k.exp || 0).getTime();
    if (!isFinite(expMs) || expMs < nowMs) { delete excMap[sig]; purged++; }
  });
  if (purged) {
    try { propsSvc.setProperty(key, JSON.stringify(excMap)); } catch(_) {}
  }

  function __U2(x){ return String(x == null ? '' : x).trim().replace(/\s+/g,' ').toUpperCase(); }
  function __sigFromItem(it) {
    // Use the shared helper so writer/reader signatures stay identical.
    return __sigBaseFromItem(it);
  }


  // Identify if this caller is the configured TEST_PHONE (so we annotate instead of removing)
  const isTestPhone = !!(TEST_MODE && normTest && normTest === msisdn);

  const before = eligible.length;
  let issuesRemovedTotal = 0;
  let itemsTouched = 0;

  eligible = (eligible || []).map(it => {
    // Build the signature for this shift and collect ALL exclusion records for it
    const sig = __sigFromItem(it);
    const recs = (function () {
      const out = [];
      if (!sig) return out;
      if (excMap[sig]) out.push(excMap[sig]); // exact shift-only exclusion
      const prefix = sig + '|';
      for (const k of Object.keys(excMap)) {
        if (k.startsWith(prefix)) out.push(excMap[k]); // per-issue exclusions for this shift
      }
      return out;
    })();

    // Gather all excluded issues for this shift (prefer writer's `issue`)
    const exclIssues = Array.from(new Set(recs.map(r => {
      const fromRec = r.issue || r.issue_type || '';
      if (fromRec) return String(fromRec).toUpperCase().trim();
      if (r.aid && typeof _emergencyFindById === 'function') {
        const a = _emergencyFindById(String(r.aid));
        const t = a && (a.issue_type || (a.issue && a.issue.type));
        return String(t || '').toUpperCase().trim();
      }
      return '';
    }).filter(Boolean)));

    // ⬅️ NEW: mark if cancel has already been actioned for this exact shift
    const cancelBlocked = exclIssues.includes('CANNOT_ATTEND');
    it.__cancel_blocked = !!cancelBlocked;

    if (!exclIssues.length) return it;

    itemsTouched++;

    // TEST phone: annotate only, do NOT remove
    if (isTestPhone) {
      const note = ' (excluding ' + exclIssues.map(s => s.toLowerCase()).join(', ') + ')';
      try {
        if (it.time_range_label) {
          if (!String(it.time_range_label).includes(note)) it.time_range_label += note;
        } else if (it.shift && it.shift.time_range_label) {
          if (!String(it.shift.time_range_label).includes(note)) it.shift.time_range_label += note;
        } else if (it.shiftInfo) {
          if (!String(it.shiftInfo).includes(note)) it.shiftInfo += note;
        }
      } catch (_) {}
      return it;
    }

    // Normal mode: remove all matched issues from allowed_issues (unchanged)
    const aiBefore = __ensureArray(it.allowed_issues || it.allowedIssues || it.issuesAllowed);
    const toRemove = new Set(exclIssues);
    const aiAfter = aiBefore.filter(x => !toRemove.has(String(x || '').toUpperCase().trim()));
    issuesRemovedTotal += Math.max(0, aiBefore.length - aiAfter.length);
    it.allowed_issues = aiAfter;
    return it;
  }).filter(it => {
    // ⬅️ NEW: keep cancel-only items unless already cancelled
    const ai = __ensureArray(it.allowed_issues || it.allowedIssues || it.issuesAllowed);
    const keepCancel = (it.canCancel === true && it.__cancel_blocked !== true);
     const keepRunLate = (it.canRunLate === true); // ← added: allow RL-only to pass
    return keepCancel || keepRunLate || ai.length > 0; 
  });

  __mark('emergency:exclusion_filter', {
    before,
    after: eligible.length,
    purged,
    itemsTouched,
    issuesRemovedTotal,
    TEST_MODE,
    isTestPhone
  });
} catch (fErr) {
  __mark('emergency:exclusion_filter_error', { error: String(fErr && fErr.message || fErr) });
}


  // Never send late_options to the client
const safeEligible = (eligible || []).map(x => {
  const y = Object.assign({}, x);

  // strip internals
  delete y.startAtIso_resolved;
  delete y.endAtIso_resolved;
  delete y.__cancel_blocked; // ⬅️ NEW: do not leak internal flag

  // strip running-late choice tiles (server will provide via RUNNING_LATE_OPTIONS later)
  if ('late_options' in y) delete y.late_options;
  if (y.shift && 'late_options' in y.shift) delete y.shift.late_options;

  return y;
});


    // Build the exact response we send to the client
    const __response = { ok: true, eligible: safeEligible, serverTime, appInfo };

    /** 1) Log the exact JSON being returned (truncated so the Logs sheet stays happy) */
    try {
      const raw = JSON.stringify(__response);
      _log('emergency', 'eligible_response_out', {
        bytes: raw.length,
        preview: raw.slice(0, 4000)   // first 4k chars for inspection
      });
    } catch (_) { /* never throw from logging */ }

    /** 2) Log a compact, analysis-friendly summary (focus on Running Late fields) */
    try {
      _log('emergency', 'eligible_summary_out', {
        count: (__response.eligible || []).length,
        contains_runlate: (__response.eligible || []).some(it => it.canRunLate === true),
        items: (__response.eligible || []).slice(0, 10).map(it => ({
          ymd: it.ymd || (it.shift && it.shift.ymd) || '',
          label: it.time_range_label || it.shiftInfo || it.shift_type || it.shiftType || '',
          allowed_issues: it.allowed_issues || it.allowedIssues || it.issuesAllowed || [],
          canRunLate: !!it.canRunLate,
          // late options are no longer sent in this response
          late_options_len: 0,
          late_options_preview: [],
          context_present: !!it.context,
        }))
      });
    } catch (_) { /* never throw from logging */ }

    return _ok(__response);

  } catch (ex) {
    __mark('emergency:exception', { error: String(ex && ex.message || ex) });
    const serverTime = (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString();
    return _ok({ ok: true, eligible: [], serverTime, error: String(ex && ex.message || ex), appInfo });
  }
}

    // ---- past14
    if (view === "past14") {
      const rota = _ssRota();
      const cand = _findCandidateByMobile(rota, msisdn);
      if (!cand) {
        __mark('past14:not_in_candidate_list', { caller_masked: __maskMsisdn(msisdn) });
        return _err("NOT_IN_CANDIDATE_LIST", 403);
      }

      const tz = __TZ;
      const today = _dateOnly(new Date(), tz);
      const end = _addDays(today, -1, tz);
      const start = _addDays(today, -14, tz);

      if (_isRotaBusy()) {
        __mark('past14:rota_busy');
        return _ok({ ok: true, items: [], appInfo });
      }

      const nameKey = (cand.surname + " " + cand.firstname).toLowerCase().trim();
      const items = _listPastShifts(rota, nameKey, start, end);
      __mark('past14:return', { count: (items || []).length });
      return _ok({ ok: true, items, appInfo });
    }

    // ---------- default: tiles + cohorts (new DNA logic) ----------
    {
      const rota = _ssRota();
      const cand = _findCandidateByMobile(rota, msisdn);
      if (!cand) {
        __mark('tiles:not_in_candidate_list', { caller_masked: __maskMsisdn(msisdn) });
        return _err("NOT_IN_CANDIDATE_LIST", 403);
      }

      const headersMem = _ensureHeaders(rota);
      if (!headersMem || headersMem.length !== 14) {
        __mark('tiles:bad_header_window', { count: (headersMem || []).length });
        return _err("BAD_HEADER_WINDOW", 500, { count: (headersMem || []).length });
      }

      // newUserHint (unchanged)
      let newUserHint = null, newUserHintIos = null, newUserHintAndroid = null;
      try {
        const rec = _getLinksRowByTel(msisdn);
        if (rec && rec.m.NEW >= 0) {
          const flag = String(rec.row[rec.m.NEW] || "").toUpperCase();
          if (flag === "YES") {
            const appleHtml   = props.getProperty("WELCOME_MESSAGE_APPLE_HTML")   || "";
            const androidHtml = props.getProperty("WELCOME_MESSAGE_ANDROID_HTML") || "";
            const genericHtml = props.getProperty("WELCOME_MESSAGE_HTML")         || "";
            if (appleHtml)   newUserHintIos      = { title: "Welcome (iPhone/iPad)", html: appleHtml };
            if (androidHtml) newUserHintAndroid  = { title: "Welcome (Android)",     html: androidHtml };
            if (genericHtml) newUserHint = { title: "Welcome", html: genericHtml };
            else if (!newUserHint && androidHtml) newUserHint = { title: "Welcome", html: androidHtml };
            else if (!newUserHint && appleHtml)   newUserHint = { title: "Welcome", html: appleHtml };
            __mark('tiles:new_user_hint', { flag });
          } else if (flag === "ALERT") {
            const alertHtml = props.getProperty("ALERT_MESSAGE_HTML") || "";
            if (alertHtml) {
              newUserHint = { title: "Important", html: alertHtml };
              __mark('tiles:new_user_hint_alert');
            }
          }
        }
      } catch (hintErr) {
        __mark('tiles:new_user_hint_exception', { error: String(hintErr && hintErr.message || hintErr) });
      }

      const cached = _tilesGet(msisdn);
      const wantYmds = headersMem.map(h => h.ymd);

      let candidateIdLoaded = false;
      let candidateIdRaw = '';
      function getCandidateIdRaw() {
        if (!candidateIdLoaded) {
          const telToCidJson = props.getProperty('LINKS_TEL_TO_CANDIDATE_ID_JSON') || '{}';
          const telToCid = JSON.parse(telToCidJson);
          candidateIdRaw = String(telToCid[msisdn] || '');
          candidateIdLoaded = true;
        }
        return candidateIdRaw;
      }

      if (_isRotaBusy() && cached) {
        const candidateName = (cand.firstname ? (cand.firstname + " ") : "") + (cand.surname || "");

        // Build from cache only; do not reference fresh-path locals.
        const tz     = __TZ;
        const nowIso = (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString();

       const tilesFromCache = (cached && Array.isArray(cached.tiles)) ? cached.tiles.slice() : [];
const occupantKey    = (String(cand.surname || '') + ' ' + String(cand.firstname || '')).trim();

// Fast, read-only overlay of timesheet flags on the cached tiles (booking_id logic unchanged)
const tilesDecorated = __overlayTimesheetFlagsOnTiles(tilesFromCache, msisdn, tz, nowIso, occupantKey);

// Attach Candidate_ID (from the request-local tel→CID map) to each tile for the frontend
const candidateId  = getCandidateIdRaw();
const tilesWithCid = candidateId ? tilesDecorated.map(t => Object.assign({}, t, { candidate_id: candidateId })) : tilesDecorated;

// Cohorts are not recomputed in busy path; return empty or any cached cohorts if you later add them to cache.
const cohortsFromCache = [];

logminimal('tiles','busy_cached',{
  rid: __RID,
  tile_count: tilesWithCid.length,
  cohort_count: cohortsFromCache.length
});

return _ok({
  ok: true,
  tiles: tilesWithCid,
  cohorts: cohortsFromCache,
  lastLoadedAt: nowIso,
  candidateName,
  candidate: { firstName: cand.firstname || "", surname: cand.surname || "" },
  appInfo,
  newUserHint,
  newUserHintIos,
  newUserHintAndroid
});

      }

      if (_isRotaBusy() && !cached) {

        // AFTER
__mark('tiles:busy_no_cache');
logminimal('tiles','error',{
  rid: __RID,
  code: 503,
  err: "TEMPORARILY_BUSY_TRY_AGAIN"
});
return _err("TEMPORARILY_BUSY_TRY_AGAIN", 503);

      }

      // Fresh compose
      const liveHeaders = _readAvailabilityHeaders(rota);
      const byYmd = Object.fromEntries(liveHeaders.map(h => [h.ymd, h]));
      const headersForRead = wantYmds.map(ymd => byYmd[ymd]).filter(Boolean);
      if (headersForRead.length !== wantYmds.length) {
        __mark('tiles:headers_mismatch', { expected: wantYmds.length, got: headersForRead.length });
        return _err("HEADERS_MISMATCH", 500);
      }

      const avRow = _findAvailabilityRowByTelephone(rota, cand.telephone);
      if (!avRow) {
        __mark('tiles:candidate_row_not_found');
        return _err("CANDIDATE_ROW_NOT_FOUND", 404);
      }

      const nameKey = (cand.surname + " " + cand.firstname).toLowerCase().trim();
      const bookedMap = _readBookedMap(rota, nameKey, wantYmds);
      const cells = _readAvailabilityCells(rota, avRow.rowIndex, headersForRead);

      const tiles = wantYmds.map((ymd, i) => {
        const H = headersMem[i];
        const c = cells[i];
        const b = bookedMap[ymd];
        if (b) {
          return {
            ymd, displayDay: H.displayDay, displayDate: H.displayDate,
            booked: true, editable: false, status: "BOOKED",
            shiftInfo: b.notes || b.shift, hospital: b.hospital || "", ward: b.ward || "",
            jobTitle: b.jobTitle || "", bookingRef: b.bookingRef || "",
           shiftType: (function(s){
  const S = String((b.shift || s || '')).toUpperCase();
  if (S === 'N' || S.includes('NIGHT')) return 'NIGHT';
  if (S === 'LD' || S === 'LONG' || S.includes('LONG DAY') || S.includes('LONGDAY')) return 'LONG';
  if (S === 'D' || S.includes('DAY') || S.includes('EARLY') || S.includes('LATE')) return 'DAY';
  return 'UNKNOWN';
})('')

          };
        }
        if (_isBlockedCell(c.value, c.bg, __P)) {
          return { ymd, displayDay: H.displayDay, displayDate: H.displayDate, booked: false, editable: false, status: "BLOCKED" };
        }
        const stat = _statusFromCell(c.value, c.bg, __P);
        return { ymd, displayDay: H.displayDay, displayDate: H.displayDate, booked: false, editable: true, status: stat };
      });

      _tilesPut(msisdn, tiles, wantYmds);

// ───────────────────────────────────────────────────────────────
      // Augment tiles for Timesheet feature:
      //  - booking_id (deterministic hash used by the broker)
      //  - timesheet_authorised (boolean if webhook said ✓)
      //  - timesheet_eligible (boolean if in progress or finished < 4h)
      // Scope: only booked tiles for:
      //   • today LONG DAY, • today NIGHT, • yesterday NIGHT
      // ───────────────────────────────────────────────────────────────
  (function annotateTimesheets() {
  try {
    const tz = __TZ;
    const nowIso = (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString();

    const todayYmd = Utilities.formatDate(new Date(nowIso), tz, 'yyyy-MM-dd');
    const yestYmd  = Utilities.formatDate(new Date(new Date(nowIso).getTime() - 24*60*60*1000), tz, 'yyyy-MM-dd');

    // STRICT: use Candidate_ID from tel→CID map (never a name) for booking_id seed
    let candidateId = '';
    try {
      candidateId = getCandidateIdRaw().trim();
    } catch (_) { candidateId = ''; }

    if (!candidateId) {
      try { logminimal('ts_annotate', 'no_candidate_id', { why: 'cid_map_miss' }); } catch(_) {}
      return;
    }

    const tsMap = __tsAuthMapRead(); // booking_id → { a:bool, ts:iso, tid?:string }
    try {
      logminimal('ts_annotate', 'enter', {
        tiles_total: Array.isArray(tiles) ? tiles.length : 0,
        todayYmd, yestYmd,
        cid_preview: candidateId.slice(0, 24)
      });
    } catch(_) {}

    tiles.forEach(function(t, idx) {
      try {
        if (!t || !t.booked) return;

        // Resolve canonical shift type and concrete times (prefer explicit, else defaults)
        let resolved = null;
        try {
          if (typeof _runningLateResolveShiftTimes === 'function') {
            resolved = _runningLateResolveShiftTimes({
              ymd: t.ymd,
              shift_type: t.shiftType || (t.shiftInfo || ''),
              shiftInfo: t.shiftInfo || '',
              tz
            }) || null;
          }
        } catch (_) {}

        const canonicalType = resolved && resolved.type ? resolved.type : null; // 'LONG DAY' or 'NIGHT'
        if (!canonicalType) {
          try { logminimal('ts_annotate', 'skip_no_canonical', { idx, ymd: t.ymd, shiftInfo: t.shiftInfo || '' }); } catch(_){}
          return;
        }

        // Only consider: today LONG DAY, today NIGHT, yesterday NIGHT
        const isToday = (t.ymd === todayYmd);
        const isYest  = (t.ymd === yestYmd);
        const consider =
          (isToday && (canonicalType === 'LONG DAY' || canonicalType === 'NIGHT')) ||
          (isYest  && canonicalType === 'NIGHT');
        if (!consider) {
          try { logminimal('ts_annotate', 'skip_out_of_window', { idx, ymd: t.ymd, canonicalType }); } catch(_){}
          return;
        }

        // booking_id — MUST be seeded with Candidate_ID to match broker
        const bookingId = __makeBookingId_GAS({
          occupant_key: candidateId,           // ← CID
          date_start_local: t.ymd,
          hospital: String(t.hospital || ''),
          ward:     String(t.ward || ''),
          job_title:String(t.jobTitle || ''),
          shift_label: canonicalType
        });

        if (bookingId) t.booking_id = bookingId;

        // authorised? (set only when true; FE checks === true)
        try {
          const rec = bookingId && tsMap && tsMap[bookingId];
          if (rec && rec.a === true) {
            t.timesheet_authorised = true;
            try { logminimal('ts_annotate', 'authorised_marked', { idx, ymd: t.ymd, booking_id: bookingId, tid: rec.tid || null }); } catch(_){}
          } else {
            try { logminimal('ts_annotate', 'not_authorised_or_missing', { idx, ymd: t.ymd, booking_id: bookingId, hasTsMapEntry: !!rec }); } catch(_){}
          }
        } catch (eAuth) {
          try { logminimal('ts_annotate', 'auth_check_exception', { idx, ymd: t.ymd, error: String(eAuth && eAuth.message || eAuth) }); } catch(_){}
        }

        // eligible? (in progress OR finished < 4h)
        try {
          if (resolved && resolved.startAtIso && resolved.endAtIso) {
            if (__timesheetEligibleNow(resolved.startAtIso, resolved.endAtIso, nowIso)) {
              t.timesheet_eligible = true;
              try { logminimal('ts_annotate', 'eligible_true', { idx, ymd: t.ymd }); } catch(_){}
            }
          }
        } catch (eElig) {
          try { logminimal('ts_annotate', 'elig_check_exception', { idx, ymd: t.ymd, error: String(eElig && eElig.message || eElig) }); } catch(_){}
        }
      } catch (innerEx) {
        try { logminimal('ts_annotate', 'tile_exception', { idx, error: String(innerEx && innerEx.message || innerEx) }); } catch(_){}
      }
    });

    try { logminimal('ts_annotate', 'exit', { tiles_total: Array.isArray(tiles) ? tiles.length : 0 }); } catch(_){}
  } catch (ex) {
    try { logminimal('ts_annotate', 'exception', { error: String(ex && ex.message || ex) }); } catch(_){}
  }
})();








      // Build cohorts for today/tomorrow booked shifts only (deterministic DNA)
      const tz = __TZ;
      const nowIso = (typeof _nowIso === 'function') ? _nowIso() : new Date().toISOString();
      const todayYmd = Utilities.formatDate(new Date(nowIso), tz, 'yyyy-MM-dd');
      const tomorrowYmd = Utilities.formatDate(new Date(new Date(nowIso).getTime() + 24*60*60*1000), tz, 'yyyy-MM-dd');

      const caller07 = __to07(msisdn);
      const cohorts = [];

      tiles
        .filter(t => t.booked && (t.ymd === todayYmd || t.ymd === tomorrowYmd))
        .forEach(t => {
          try {
            // Resolve canonical type + inProgress using the time resolver
let resolved = null, inProgress = false, canonicalType = t.shiftType || '';
if (typeof _runningLateResolveShiftTimes === 'function') {
  resolved = _runningLateResolveShiftTimes({
    ymd: t.ymd,
    shift_type: t.shiftType,
    shiftInfo: t.shiftInfo,
    tz
  }) || null;

  if (resolved && resolved.startAtIso && resolved.endAtIso) {
    inProgress = __shiftInProgress(resolved.startAtIso, resolved.endAtIso, nowIso);
  }
  if (resolved && resolved.type) {
    canonicalType = resolved.type; // 'LONG DAY' or 'NIGHT'
  }
}

// Build cohort key with the canonical shift type
const key = (typeof _cohortKey === 'function')
  ? _cohortKey(t.ymd, t.hospital || '', t.ward || '', canonicalType || '')
  : [t.ymd, t.hospital || '', t.ward || '', canonicalType || ''].join('|');
const coh = __readOrComputeCohort(key, { bookedMap });
const membersSansMe = __dropSelfFromMembers(coh.members || [], caller07);

cohorts.push({
  key,
  ymd: t.ymd,
  hospital: t.hospital || '',
  ward: t.ward || '',
  shiftType: canonicalType || '',
  inProgress,
  members: membersSansMe
});


          } catch (_) {}
        });

  const candidateName = (cand.firstname ? (cand.firstname + " ") : "") + (cand.surname|| "");

// Reuse the request-local Candidate_ID and attach it to every tile
const candidateId   = getCandidateIdRaw();
const tilesWithCid2 = candidateId ? tiles.map(t => Object.assign({}, t, { candidate_id: candidateId })) : tiles;

// Build and return the response.
const __tilesResponse = {
  ok: true,
  tiles: tilesWithCid2,
  cohorts,                 // ward–shift cohort entries (sans self)
  lastLoadedAt: _nowIso(),
  candidateName,
  candidate: { firstName: cand.firstname || "", surname: cand.surname || "" },
  appInfo,
  newUserHint,
  newUserHintIos,
  newUserHintAndroid
};

return _ok(__tilesResponse);

    }
  } catch (e2) {
  // AFTER
__mark('fatal_exception', {
  error: String(e2 && e2.message || e2),
  stack: (e2 && e2.stack) ? String(e2.stack).slice(0, 1000) : ''
});
logminimal('doGet','exception',{
  rid: __RID,
  message: String(e2 && e2.message || e2)
});
return _err(e2 && e2.message || "SERVER_ERROR", 500);

  }
}






































/**
 * Read-only overlay to decorate an array of tiles with:
 *  - booking_id (deterministic, matches FE/broker)
 *  - timesheet_authorised (true when present in TS_AUTH_MAP)
 *  - timesheet_eligible (shift in-progress OR ended < 4h ago)
 *
 * Scope: booked tiles for today LONG DAY, today NIGHT, and yesterday NIGHT.
 * No writes to cache. Returns a new array (defensive copy).
 */
function __overlayTimesheetFlagsOnTiles(tilesIn, msisdn, tz, nowIso, occupantKey) {
  try {
    const tzUse   = tz || ((_P() && _P().TZ) || 'Europe/London');
    const now     = (typeof _nowIso === 'function') ? (_ => new Date(nowIso || _nowIso()))() : new Date(nowIso || new Date().toISOString());
    const todayY  = Utilities.formatDate(now, tzUse, 'yyyy-MM-dd');
    const yest    = Utilities.formatDate(new Date(now.getTime() - 24*60*60*1000), tzUse, 'yyyy-MM-dd');

    // STRICT: use Candidate_ID from tel→CID map for booking_id seed (never name)
    let cid = '';
    try {
      const telToCidJson = (PropertiesService.getScriptProperties().getProperty('LINKS_TEL_TO_CANDIDATE_ID_JSON') || '{}');
      const telToCid     = JSON.parse(telToCidJson);
      cid                = String(telToCid[msisdn] || '').trim();
    } catch (_) { cid = ''; }

    if (!cid) return Array.isArray(tilesIn) ? tilesIn.slice() : [];

    const tsMap = (typeof __tsAuthMapRead === 'function') ? (__tsAuthMapRead() || {}) : {};

    return (Array.isArray(tilesIn) ? tilesIn : []).map(function (t) {
      const out = Object.assign({}, t);
      if (!out || out.booked !== true) return out;

      // Resolve canonical type
      let resolved = null, canonicalType = null;
      try {
        if (typeof _runningLateResolveShiftTimes === 'function') {
          resolved = _runningLateResolveShiftTimes({
            ymd: out.ymd,
            shift_type: out.shiftType || (out.shiftInfo || ''),
            shiftInfo: out.shiftInfo || '',
            tz: tzUse
          }) || null;
          canonicalType = (resolved && resolved.type) ? resolved.type : null; // 'LONG DAY' or 'NIGHT'
        }
      } catch (_) {}

      if (!canonicalType) return out;

      const isToday = (out.ymd === todayY);
      const isYest  = (out.ymd === yest);
      const consider =
        (isToday && (canonicalType === 'LONG DAY' || canonicalType === 'NIGHT')) ||
        (isYest  && canonicalType === 'NIGHT');
      if (!consider) return out;

      // booking_id — match broker exactly by seeding with Candidate_ID
      try {
        if (typeof __makeBookingId_GAS === 'function') {
          const bookingId = __makeBookingId_GAS({
            occupant_key: cid,                         // ← CID
            date_start_local: out.ymd,
            hospital: String(out.hospital || ''),
            ward:     String(out.ward || ''),
            job_title:String(out.jobTitle || ''),
            shift_label: canonicalType
          });
          if (bookingId) out.booking_id = bookingId;

          // Authorised?
          const rec = bookingId && tsMap[bookingId];
          if (rec && rec.a === true) out.timesheet_authorised = true;
        }
      } catch (_) {}

      // Eligible? (in progress OR finished < 4h)
      try {
        if (resolved && resolved.startAtIso && resolved.endAtIso) {
          if (__timesheetEligibleNow(resolved.startAtIso, resolved.endAtIso, now.toISOString())) {
            out.timesheet_eligible = true;
          }
        }
      } catch (_) {}

      return out;
    });
  } catch (_) {
    return Array.isArray(tilesIn) ? tilesIn.slice() : [];
  }
}

/**
 * True if now is inside the shift window, or within 4 hours after it ended.
 * Inputs are ISO strings (UTC Z). Pure calculation, no I/O.
 */
function __timesheetEligibleNow(startAtIso, endAtIso, nowIso) {
  try {
    const start = new Date(startAtIso);
    const end   = new Date(endAtIso);
    const now   = new Date(nowIso || new Date().toISOString());
    if (!(+start) || !(+end)) return false;

    const inProgress = (start.getTime() <= now.getTime() && now.getTime() < end.getTime());
    const justAfter  = (now.getTime() >= end.getTime() && (now.getTime() - end.getTime()) <= (4 * 60 * 60 * 1000));

    return inProgress || justAfter;
  } catch (_) {
    return false;
  }
}



// ───────────────────────────────────────────────────────────────
// Timesheet authorisation cache (booking_id → { a:bool, ts:iso, tid?:string })
//   - Stored in Script Properties under key: 'TS_AUTH_MAP'
//   - Capped opportunistically to ~200 entries (trim oldest by ts)
//   - Cleanup helper (3-day TTL) for a daily time-based trigger
// ───────────────────────────────────────────────────────────────
function __tsAuthMapRead() {
  try {
    const props = PropertiesService.getScriptProperties();
    const raw = props.getProperty('TS_AUTH_MAP') || '{}';
    const obj = JSON.parse(raw);
    return (obj && typeof obj === 'object') ? obj : {};
  } catch (_) { return {}; }
}

function __tsAuthMapWrite(map) {
  try {
    const props = PropertiesService.getScriptProperties();
    props.setProperty('TS_AUTH_MAP', JSON.stringify(map || {}));
  } catch (_) {}
}

// Upsert one record and keep the map reasonably small (≈200)
function __tsAuthUpsert(bookingId, authorised, timesheetIdOpt) {
  try {
    const map = __tsAuthMapRead();
    map[bookingId] = { a: !!authorised, ts: _nowIso(), tid: timesheetIdOpt || '' };

    // Trim if too large (oldest first by ts)
    const keys = Object.keys(map);
    const LIMIT = 200;
    if (keys.length > LIMIT) {
      keys.sort(function(a, b) {
        const ta = new Date(map[a].ts || 0).getTime();
        const tb = new Date(map[b].ts || 0).getTime();
        return ta - tb; // oldest first
      });
      const toDelete = keys.slice(0, keys.length - LIMIT);
      toDelete.forEach(k => { try { delete map[k]; } catch (_) {} });
    }

    __tsAuthMapWrite(map);
  } catch (_) {}
}

// Daily cleanup: remove entries older than 3 days
function cleanupTimesheetAuthCache() {
  try {
    const map = __tsAuthMapRead();
    const now = Date.now();
    const TTL_MS = 3 * 24 * 60 * 60 * 1000;
    Object.keys(map).forEach(k => {
      const ts = new Date(map[k].ts || 0).getTime();
      if (!isFinite(ts) || (now - ts) > TTL_MS) delete map[k];
    });
    __tsAuthMapWrite(map);
  } catch (_) {}
}

// ───────────────────────────────────────────────────────────────
// Eligibility check: true if now ∈ [start,end] OR now ≤ end+4h
// ───────────────────────────────────────────────────────────────
function __timesheetEligibleNow(startIso, endIso, nowIsoOpt) {
  try {
    const now = new Date(nowIsoOpt || _nowIso()).getTime();
    const s = new Date(startIso).getTime();
    const e = new Date(endIso).getTime();
    if (!isFinite(s) || !isFinite(e) || !isFinite(now)) return false;
    if (now >= s && now <= e) return true;
    const FOUR_H = 4 * 60 * 60 * 1000;
    return (now > e && (now - e) <= FOUR_H);
  } catch (_) { return false; }
}

// ───────────────────────────────────────────────────────────────
// booking_id generator (must match FE/broker):
//  bk_ + first 16 hex chars of SHA-256 over:
//   norm(occupant_key) + '|' + ymd + '|' + norm(hospital) + '|' + norm(ward) + '|' + norm(job_title) + '|' + norm(shift_label)
// ───────────────────────────────────────────────────────────────
function __normForBooking(s) {
  return String(s || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[^\w\s\-@&\/,.:]/g, '');
}

function __sha256Hex_GAS(str) {
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, str, Utilities.Charset.UTF_8);
  return bytes.map(function(b){ return (b & 0xFF).toString(16).padStart(2,'0'); }).join('');
}

function __makeBookingId_GAS({ occupant_key, date_start_local, hospital, ward, job_title, shift_label }) {
  try {
    // Broker-normalisation: trim, lowercase, collapse internal whitespace,
    // and strip any char not in: \w, whitespace, - @ & / , . :
    function _norm(s) {
      let x = String(s == null ? '' : s);
      x = x.trim().toLowerCase();
      x = x.replace(/\s+/g, ' ');
      x = x.replace(/[^\w\s\-@&\/,\.:]/g, '');
      return x;
    }

    const base =
      _norm(occupant_key)        + '|' +
      String(date_start_local||'') + '|' +  // date kept as raw YYYY-MM-DD
      _norm(hospital)            + '|' +
      _norm(ward)                + '|' +
      _norm(job_title)           + '|' +
      _norm(shift_label || '');

    const hash = __sha256Hex_GAS(base);
    return 'bk_' + String(hash || '').slice(0, 16);
  } catch (_) {
    return '';
  }
}












// ───────────────────────────── Canonicalisation helpers ─────────────────────────────
function __canonicalShiftType(s) {
  const S = String(s || '').trim().toUpperCase();
  if (!S) return 'UNKNOWN';
  if (S === 'N' || S.indexOf('NIGHT') >= 0) return 'NIGHT';
  if (S === 'LD' || S === 'LONG' || S.indexOf('LONG DAY') >= 0 || S.indexOf('LONGDAY') >= 0) return 'LONG';
  if (S === 'D'  || S.indexOf('DAY') >= 0 || S.indexOf('EARLY') >= 0 || S.indexOf('LATE') >= 0) return 'DAY';
  return 'UNKNOWN';
}
function _cohortKey(ymd, hospital, ward, canonicalShiftType) {
  function U(x){ return String(x == null ? '' : x).trim(); }
  return [U(ymd), U(hospital), U(ward), __canonicalShiftType(canonicalShiftType)].join('|');
}

// Internal: parse a cohort key back into parts
function __parseCohortKey(key) {
  var parts = String(key || '').split('|');
  return {
    ymd: parts[0] || '',
    hospital: parts[1] || '',
    ward: parts[2] || '',
    shiftType: __canonicalShiftType(parts[3] || '')
  };
}

// Internal: safe phone normaliser to 07 (lightweight, local)
function __to07Lite(n){
  var s = String(n || '').replace(/\s+/g,'');
  if (/^\+447\d{9}$/.test(s)) return '0' + s.slice(3);
  if (/^447\d{9}$/.test(s))   return '0' + s.slice(2);
  if (/^07\d{9}$/.test(s))    return s;
  var digits = s.replace(/[^\d]/g, '');
  if (/^447\d{9}$/.test(digits)) return '0' + digits.slice(2);
  if (/^07\d{9}$/.test(digits))  return digits;
  return ''; // unknown
}

// ───────────────────────────── Cohort storage (ScriptProperties) ─────────────────────────────
function _readCohort(key) {
  try {
    var k = 'COHORT::' + String(key || '');
    var raw = PropertiesService.getScriptProperties().getProperty(k);
    if (!raw) return null;
    var obj = JSON.parse(raw);
    // Minimal shape validation
    if (!obj || obj.key !== key || !('members' in obj)) return null;
    return obj;
  } catch (e) {
    return null;
  }
}

function _writeCohort(key, cohortObj) {
  try {
    var k = 'COHORT::' + String(key || '');
    var nowIso = new Date().toISOString();
    var out = Object.assign(
      { key: key, state: 'none', ts: nowIso, members: [] },
      (cohortObj || {})
    );
    if (!out.ts) out.ts = nowIso;
    PropertiesService.getScriptProperties().setProperty(k, JSON.stringify(out));
    return true;
  } catch (e) {
    return false;
  }
}

// ───────────────────────────── Cohort compute from bookedMap ─────────────────────────────
/**
 * bookedMap expectations (flexible, defensive):
 * - bookedMap[ymd] can be:
 *   a) an Array of booking objects for that date, OR
 *   b) a single booking object (for the caller) — will be wrapped as a 1-elem array
 * Booking fields probed per item:
 *   - hospital, ward
 *   - shift or shiftType (normalized to canonical)
 *   - firstName / firstname, surname / lastName
 *   - role / jobTitle
 *   - bookingRef / booking_reference
 *   - mobile / msisdn / msisdn07 / telephone
 */
function _computeCohortFromBookedMap(key, bookedMap) {
  try {
    var meta = __parseCohortKey(key);
    var ymd = meta.ymd, targetHosp = String(meta.hospital || ''), targetWard = String(meta.ward || '');
    var targetType = __canonicalShiftType(meta.shiftType);

    var dayBucket = (bookedMap && bookedMap[ymd]) || null;
    if (!dayBucket) {
      return { key: key, state: 'none', ts: new Date().toISOString(), members: [] };
    }

    var list = Array.isArray(dayBucket) ? dayBucket : [dayBucket];

    // Normalise + pick matches for same hospital/ward/shiftType
    var members = list.map(function(b){
      try {
        var hosp = String(b.hospital || '').trim();
        var ward = String(b.ward || '').trim();
        var rawT = b.shiftType || b.shift || '';
        var t = __canonicalShiftType(rawT);

        if (hosp !== targetHosp || ward !== targetWard || t !== targetType) return null;

        var first = String(b.firstName || b.firstname || '').trim();
        var sur   = String(b.surname || b.lastName || '').trim();
        var role  = String(b.role || b.jobTitle || '').trim();
        var ref   = String(b.bookingRef || b.booking_reference || '').trim();
        var mobile= b.msisdn07 || b.mobile || b.msisdn || b.telephone || '';

       return {
  msisdn07: __to07Lite(mobile),
  firstName: first,
  surname: sur,                                 // ← keep full surname internally
  surnameInitial: sur ? sur[0].toUpperCase() : '',
  role: role,
  bookingRef: ref
};

      } catch (e) {
        return null;
      }
    }).filter(function(x){ return !!x; });

    var state = members.length ? 'some' : 'none';
    return { key: key, state: state, ts: new Date().toISOString(), members: members };
  } catch (e) {
    return { key: key, state: 'none', ts: new Date().toISOString(), members: [] };
  }
}

function _runningLateResolveShiftTimes({ ymd, shift_type, shiftInfo, tz }) {
  tz = tz || (_P().TZ || 'Europe/London');

  // 1) If notes contain explicit times, trust them exactly
  const parsed = _parseNotesToTimes(shiftInfo || '', ymd, tz);
  if (parsed && parsed.startAtIso && parsed.endAtIso) {
    const hasNightHint =
      /\bNIGHT(S)?\b/i.test(String(shift_type || '')) ||
      /\bNIGHT(S)?\b/i.test(String(shiftInfo || ''));
    const type = hasNightHint ? 'NIGHT' : (parsed.overnight ? 'NIGHT' : 'LONG DAY');
    return { startAtIso: parsed.startAtIso, endAtIso: parsed.endAtIso, type };
  }

  // 2) No explicit times → infer type and use canonical defaults, converting local→UTC correctly
  const [Y, M, D] = String(ymd).split('-').map(Number);
  const uType = String(shift_type || '').toUpperCase();
  const uInfo = String(shiftInfo || '').toUpperCase();
  const isNight = /\bNIGHT(S)?\b/.test(uType) || /\bNIGHT(S)?\b/.test(uInfo);
  const type = isNight ? 'NIGHT' : 'LONG DAY';

  function localTimeToIsoZ(YY, MM, DD, h, m, zone) {
    const noonUtc = Date.UTC(YY, MM - 1, DD, 12, 0, 0);
    const z = Utilities.formatDate(new Date(noonUtc), zone || 'Europe/London', 'Z'); // +0100 / +0000
    const sign = z[0] === '-' ? -1 : 1;
    const offMin = sign * (parseInt(z.slice(1,3),10) * 60 + parseInt(z.slice(3,5),10));
    const utcMidnight = Date.UTC(YY, MM - 1, DD, 0, 0, 0);
    const localMin = h * 60 + m;
    const ms = utcMidnight + (localMin - offMin) * 60000;
    return _toIsoZ_(new Date(ms));
  }

  const startLocal = isNight ? { h: 19, m: 30 } : { h: 7,  m: 30 };
  const endLocal   = isNight ? { h: 8,  m: 0  } : { h: 20, m: 0  };

  const startAtIso = localTimeToIsoZ(Y, M, D, startLocal.h, startLocal.m, tz);
  const endAtIso   = localTimeToIsoZ(Y, M, D + (isNight ? 1 : 0), endLocal.h, endLocal.m, tz);

  return { startAtIso, endAtIso, type };
}

/** ───────────────────────── Build & persist cohorts from EmailHistory (UPDATED) ─────────────────────────
 * - Uses _cohortKey + __canonicalShiftType for keys
 * - Normalizes members to { msisdn07, firstName, surnameInitial, role, bookingRef }
 * - Persists each cohort via _writeCohort(key, { key, state, ts, members })
 * - Guards writes with a ScriptLock
 * - Returns and logs a concise summary
 */



function buildAndLogCohortsFromEmailHistoryTodayTomorrow() {
  function logStep(event, data) { try { _log('cohort', event, data || {}); } catch (_) {} }

  const DOC_ID = '1eEnrLMhLX_FzuO7sdUfAzEnvuXAwmR79fBntKe5Gp04'; // given
  const KEEP_DAYS = 3; // ← keep only cohorts whose YMD is within the last N days (today = 0)

  // ───────── purge old cohorts ─────────
  function purgeOldCohorts(keepDays, tz) {
    const sp = PropertiesService.getScriptProperties();
    const all = sp.getProperties();
    const todayYmd = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');

    function daysBetween(ymdA, ymdB) {
      const [aY,aM,aD] = ymdA.split('-').map(Number);
      const [bY,bM,bD] = ymdB.split('-').map(Number);
      const a = new Date(aY, aM-1, aD);
      const b = new Date(bY, bM-1, bD);
      return Math.round((a - b) / (24*3600*1000));
    }

    let deleted = 0, scanned = 0;
    Object.keys(all).forEach(k => {
      if (!k.startsWith('COHORT::')) return;
      scanned++;
      const keyPart = k.slice('COHORT::'.length);
      const ymd = keyPart.split('|')[0] || '';
      if (!/^\d{4}-\d{2}-\d{2}$/.test(ymd)) return;
      const age = daysBetween(todayYmd, ymd);
      if (age > keepDays) { sp.deleteProperty(k); deleted++; }
    });
    logStep('purge', { scanned, deleted, keepDays, todayYmd });
    return { scanned, deleted };
  }

  // Normalisers
  function canonWSQ(s) {
    let x = String(s || '');
    x = x.replace(/[‘’]/g, "'").replace(/[“”]/g, '"');
    x = x.replace(/\s+/g, ' ').trim();
    return x;
  }
  function softTitle(s) {
    const x = canonWSQ(s);
    if (!x) return '';
    return x.split(' ').map(tok => (/^[A-Z0-9]{3,}$/.test(tok) ? tok
      : (tok.charAt(0).toUpperCase() + tok.slice(1).toLowerCase()))).join(' ');
  }
  function normalizeHospital(raw) {
    const base = canonWSQ(String(raw || '').split(',')[0] || '');
    try {
      if (typeof _normalizeHospitalName === 'function') {
        const n = _normalizeHospitalName(base);
        return canonWSQ(n || '');
      }
    } catch (_) {}
    return softTitle(base);
  }
  function normalizeWard(raw) { return softTitle(raw); }

  function inferShiftType(shiftText) {
    const s = String(shiftText || '').toUpperCase();
    if (!s) return 'LONG';
    if (s === 'N' || s.includes('NIGHT')) return 'NIGHT';
    if (s.includes('LONG DAY') || s.includes('LONGDAY') || s === 'LD' || s === 'LONG') return 'LONG';
    if (s.includes('DAY') || s.includes('EARLY') || s.includes('LATE') || s === 'D') return 'DAY';
    return 'UNKNOWN';
  }

  function toYmd(val, tz) {
    if (Object.prototype.toString.call(val) === '[object Date]' && !isNaN(val)) {
      return Utilities.formatDate(val, tz, 'yyyy-MM-dd');
    }
    if (typeof val === 'number' && isFinite(val)) {
      const epoch = new Date(Date.UTC(1899, 11, 30));
      const ms = Math.round(val * 24 * 60 * 60 * 1000);
      const d = new Date(epoch.getTime() + ms);
      return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
    }
    const s = String(val || '').trim();
    if (!s) return null;

    let m = /^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})$/.exec(s);
    if (m) {
      let d = parseInt(m[1], 10);
      let mo = parseInt(m[2], 10) - 1;
      let y = parseInt(m[3], 10);
      if (y < 100) y += 2000;
      const dt = new Date(y, mo, d, 0, 0, 0, 0);
      if (!isNaN(dt)) return Utilities.formatDate(dt, tz, 'yyyy-MM-dd');
    }
    m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
    if (m) return s;

    const parsed = new Date(s);
    if (!isNaN(parsed)) return Utilities.formatDate(parsed, tz, 'yyyy-MM-dd');
    return null;
  }

  // IMPORTANT: EmailHistory OccupantKey format is **"Surname First"** (possibly multi-token surname).
  // We must store FULL surname + FULL firstName (no clipping).
  function parseSurnameThenFirst(display) {
    const tokens = String(display || '').trim().split(/\s+/).filter(Boolean);
    if (tokens.length < 2) return { firstName: '', surname: '' };
    const firstName = tokens[tokens.length - 1];
    const surname   = tokens.slice(0, -1).join(' ');  // preserve compound surnames
    return { firstName, surname };
  }

  try {
    const P = (typeof _P === 'function') ? (_P() || {}) : {};
    const tz = P.TZ || 'Europe/London';

    purgeOldCohorts(KEEP_DAYS, tz);

    const now = new Date();
    const todayYmd = Utilities.formatDate(now, tz, 'yyyy-MM-dd');
    const tomorrow = new Date(now.getTime() + 24*60*60*1000);
    const tomorrowYmd = Utilities.formatDate(tomorrow, tz, 'yyyy-MM-dd');

    logStep('eh:start', { tz, nowIso: Utilities.formatDate(now, tz, "yyyy-MM-dd'T'HH:mm:ss"), todayYmd, tomorrowYmd });

    const ss = SpreadsheetApp.openById(DOC_ID);
    const sh = ss.getSheetByName('EmailHistory');
    if (!sh) throw new Error('EmailHistory sheet not found');

    const values = sh.getDataRange().getValues();
    if (!values || !values.length) {
      logStep('eh:empty', {});
      _log('cohort', 'eh:rebuildTodayTomorrow', { cohorts: {} });
      return { ok: true, keysWritten: 0, rowsRead: 0, rowsKept: 0, totalMembers: 0, purged: true };
    }

    const header = values[0].map(String);
    const idx = {
      occupant: header.indexOf('OccupantKey'),
      date:     header.indexOf('Date'),
      shift:    header.indexOf('Shift'),
      hospital: header.indexOf('Hospital'),
      ward:     header.indexOf('Ward'),
    };
    logStep('eh:header', { header, idx });

    if (idx.occupant < 0 || idx.date < 0 || idx.shift < 0 || idx.hospital < 0 || idx.ward < 0) {
      throw new Error('EmailHistory missing required columns (need OccupantKey, Date, Shift, Hospital, Ward)');
    }

    const aggregated = {};   // keyStr -> { ymd, hospital, ward, shiftTypeCanonical, members:[…] }
    const memberDedup = {};  // keyStr -> Set(lower occupant key)
    let rowsRead = 0, rowsKept = 0;
    const skipReasons = { notTodayOrTomorrow: 0, badDate: 0, missingFields: 0, badName: 0 };

    for (let r = 1; r < values.length; r++) {
      rowsRead++;
      const row = values[r];

      const occupantRaw = row[idx.occupant];
      const dateRaw     = row[idx.date];
      const shiftRaw    = row[idx.shift];
      const hospRaw     = row[idx.hospital];
      const wardRaw     = row[idx.ward];

      const ymd = toYmd(dateRaw, tz);
      if (!ymd) { skipReasons.badDate++; continue; }
      if (ymd !== todayYmd && ymd !== tomorrowYmd) { skipReasons.notTodayOrTomorrow++; continue; }

      const occupantDisp = softTitle(occupantRaw);
      const occupantKeyLower = canonWSQ(String(occupantRaw || '').toLowerCase());
      const hospital = normalizeHospital(hospRaw);
      const ward     = normalizeWard(wardRaw);
      const inferred = inferShiftType(shiftRaw);
      const shiftTypeCanonical = __canonicalShiftType(inferred);

      if (!occupantKeyLower || !hospital || !ward) { skipReasons.missingFields++; continue; }

      // Parse FULL names from "Surname First"
      const parsed = parseSurnameThenFirst(occupantDisp);
      if (!parsed.firstName || !parsed.surname) { skipReasons.badName++; continue; }
      const firstName = parsed.firstName;
      const surname   = parsed.surname;

      const keyStr = _cohortKey(ymd, hospital, ward, shiftTypeCanonical);

      if (!aggregated[keyStr]) {
        aggregated[keyStr] = { ymd, hospital, ward, shiftTypeCanonical, members: [] };
        memberDedup[keyStr] = new Set();
      }
      if (!memberDedup[keyStr].has(occupantKeyLower)) {
        aggregated[keyStr].members.push({
          msisdn07: '',                    // unknown from EmailHistory
          firstName: firstName,            // FULL first name
          surname: surname,                // FULL surname
          surnameInitial: surname ? surname[0].toUpperCase() : '', // convenience only
          role: '',                        // unknown from EmailHistory
          bookingRef: ''                   // unknown from EmailHistory
        });
        memberDedup[keyStr].add(occupantKeyLower);
        rowsKept++;
      }
    }

    // Persist cohorts with a lightweight lock
    const lock = LockService.getScriptLock();
    try { lock.waitLock(5000); } catch (_) {
      logStep('eh:lock_fail', {});
      return { ok: false, reason: 'LOCK_FAIL' };
    }

    const tsIso = Utilities.formatDate(new Date(), tz, "yyyy-MM-dd'T'HH:mm:ss");
    let keysWritten = 0, totalMembers = 0;
    const keys = Object.keys(aggregated).sort();

    try {
      for (const keyStr of keys) {
        const agg = aggregated[keyStr];
        const members = agg.members || [];
        const state = members.length ? 'some' : 'none';
        totalMembers += members.length;

        const cohortObj = {
          key: keyStr,
          state,
          ts: tsIso,
          members
        };

        try {
          _writeCohort(keyStr, cohortObj);  // overwrite unconditionally
          keysWritten++;
        } catch (wErr) {
          logStep('eh:write_error', { keyStr, error: String(wErr && wErr.message || wErr) });
        }
      }
    } finally {
      try { lock.releaseLock(); } catch (_) {}
    }

    const summary = {
      ok: true,
      rowsRead,
      rowsKept,
      keysWritten,
      totalMembers,
      keysPreview: keys.slice(0, 50),
      skipReasons
    };

    logStep('eh:summary', summary);
    _log('cohort', 'eh:rebuildTodayTomorrow', { keysWritten, keysPreview: keys.slice(0, 20) });

    return summary;

  } catch (err) {
    logStep('eh:error', { message: err && err.message, stack: err && err.stack });
    return { ok: false, error: String(err && err.message || err) };
  }
}














// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _warmTilesForMsisdns — unchanged batching, but single-item warm now
// persists peers via __getOrComputePeers (tri-state) when includePeers=true.
// ─────────────────────────────────────────────────────────────────────────────
function _warmTilesForMsisdns(msisdns, opts) {
  const options = opts || {};
  if (!Array.isArray(msisdns) || !msisdns.length) {
    return { attempted: 0, warmed: 0, failed: 0, queued: 0 };
  }
  if (_isRotaBusy()) {
    _queueWarms(msisdns);
    return { attempted: 0, warmed: 0, failed: 0, queued: msisdns.length };
  }

  let attempted = 0, warmed = 0, failed = 0;
  const seen = new Set();

  for (const raw of msisdns) {
    const tel = _normaliseLocalTel(raw || "");
    if (!tel || seen.has(tel)) continue;
    seen.add(tel);

    attempted++;
    // ⬇️ forward includePeers, cohortKey, and the new "force none if no anchor"
    const ok = _warmTilesForMsisdn(tel, {
      includePeers: !!options.includePeers,
      cohortKey: options.cohortKey || null,
      forcePeersNoneIfNoAnchor: !!options.forcePeersNoneIfNoAnchor
    });
    if (ok) warmed++; else failed++;
  }

  _logWarm('WARM_BATCH', { attempted, warmed, failed });
  return { attempted, warmed, failed, queued: 0 };
}



// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _warmTilesForMsisdn — after building tiles, compute/persist peers
// into dedicated tri-state cache via __getOrComputePeers (forceWrite).
// Keeps legacy peers mirror in _tilesPut for back-compat.
// ─────────────────────────────────────────────────────────────────────────────
function _warmTilesForMsisdn(msisdn, opts) {
  try {
    const tel = _normaliseLocalTel(msisdn || "");
    if (!tel) return false;

    if (_isRotaBusy()) { _queueWarms([tel]); return false; }

    const ss = _ssRota();

    const headersMem = _ensureHeaders(ss);
    if (!Array.isArray(headersMem) || headersMem.length !== 14) {
      _logWarm('WARM_FAIL_HEADERS_MEM', { msisdn: tel, have: Array.isArray(headersMem) ? headersMem.length : 0 });
      return false;
    }

    const liveHeaders = _readAvailabilityHeaders(ss) || [];
    if (liveHeaders.length !== 14) {
      _logWarm('WARM_FAIL_HEADERS_LIVE', { msisdn: tel, have: liveHeaders.length });
      return false;
    }
    const byYmdLive = Object.fromEntries(liveHeaders.map(h => [h.ymd, h]));

    const ymds = headersMem.map(h => h.ymd);
    const headersForRead = [];
    for (const ymd of ymds) {
      const H = byYmdLive[ymd];
      if (!H) {
        _logWarm('WARM_FAIL_HEADERS_MISMATCH', { msisdn: tel, ymdMissing: ymd });
        return false;
      }
      headersForRead.push(H);
    }

    const cand = _findCandidateByMobile(ss, tel);
    if (!cand) { _logWarm('WARM_FAIL_CAND', { msisdn: tel }); return false; }

    const avRow = _findAvailabilityRowByTelephone(ss, cand.telephone);
    if (!avRow) { _logWarm('WARM_FAIL_ROW', { msisdn: tel }); return false; }

    const nameKey = (cand.surname + " " + cand.firstname).toLowerCase().trim();
    const bookedMap = _readBookedMap(ss, nameKey, ymds);
    const cells = _readAvailabilityCells(ss, avRow.rowIndex, headersForRead);

    const tiles = ymds.map((ymd, i) => {
      const H = headersMem[i];
      const c = cells[i];
      const b = bookedMap[ymd];

      if (b) {
        return {
          ymd,
          displayDay: H.displayDay,
          displayDate: H.displayDate,
          booked: true,
          editable: false,
          status: "BOOKED",
          shiftInfo: b.notes || b.shift,
          hospital: b.hospital || "",
          ward: b.ward || "",
          jobTitle: b.jobTitle || "",
          bookingRef: b.bookingRef || ""
        };
      }
      if (_isBlockedCell(c.value, c.bg)) {
        return {
          ymd,
          displayDay: H.displayDay,
          displayDate: H.displayDate,
          booked: false,
          editable: false,
          status: "BLOCKED"
        };
      }
      const stat = _statusFromCell(c.value, c.bg);
      return {
        ymd,
        displayDay: H.displayDay,
        displayDate: H.displayDate,
        booked: false,
        editable: true,
        status: stat
      };
    });

    // Determine cohort anchorKey: use provided override or infer from nearest upcoming booked day
    const P = _P() || { TZ: 'Europe/London' };
    let cohortKey = (opts && opts.cohortKey) ? opts.cohortKey : null;
    if (!cohortKey) {
      const todayYmd = Utilities.formatDate(new Date(), P.TZ || 'Europe/London', 'yyyy-MM-dd');
      const bookedYmds = ymds.filter(y => bookedMap[y]).sort();
      let pick = null;
      for (const y of bookedYmds) { if (y >= todayYmd) { pick = y; break; } }
      if (!pick) pick = bookedYmds[0] || null;

      if (pick) {
        const b = bookedMap[pick] || {};
        const rawShift = (b.shift || b.notes || '').toString();
        const normShiftType = (function (s) {
          const S = s.toUpperCase();
          if (S.includes('NIGHT') || S === 'N') return 'NIGHT';
          if (S.includes('LD') || S.includes('LONG DAY') || S === 'LD') return 'LONG DAY';
          return S || 'LONG DAY';
        })(rawShift);

        cohortKey = {
          ymd: pick,
          hospital: (typeof _normalizeHospitalName === 'function') ? _normalizeHospitalName(b.hospital || '') : String(b.hospital || '').trim(),
          ward: String(b.ward || '').trim(),
          shiftType: normShiftType
        };
      }
    }

    // Persist peers tri-state via orchestrator:
    // - If we have an anchorKey and includePeers → compute & write peers.
    // - If we do NOT have an anchorKey and caller asked to force-none → write 'none' now.
    let peersMirror = undefined;
    if (opts && opts.includePeers && cohortKey) {
      const tri = __getOrComputePeers(__to07(tel), cohortKey, {
        ss,
        user: { tel: cand.telephone, firstname: cand.firstname, surname: cand.surname },
        headersYmds: ymds,
        bookedMap,
        computeFn: _computePeersForUser,
        forceWrite: true
      });
      if (tri && tri.state === 'some' && tri.peers) {
        peersMirror = { current: tri.peers.current, next: tri.peers.next };
      }
    } else if (opts && opts.includePeers && !cohortKey && opts.forcePeersNoneIfNoAnchor) {
      const synthKey = {
        ymd: String(ymds && ymds[0] || ''),
        hospital: '',
        ward: '',
        shiftType: 'NONE'
      };
      __getOrComputePeers(__to07(tel), synthKey, {
        // no computeFn → function will persist a tri-state of 'none'
        forceRecompute: true,
        forceWrite: true
      });
    }

    _tilesPut(tel, tiles, ymds, null, peersMirror);
    _logWarm('WARM_OK', {
      msisdn: tel,
      tiles: tiles.length,
      peersTriStatePersisted: !!peersMirror,
      anchorKey: cohortKey || null,
      forcedNone: (!!(opts && opts.includePeers && !cohortKey && opts.forcePeersNoneIfNoAnchor))
    });
    return true;
  } catch (e) {
    _logWarm('WARM_EXCEPTION', { msisdn: String(msisdn || ""), error: String(e && e.message || e) });
    return false;
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _warmTilesForMsisdnWithRev — same idea as above but for staged revs.
// Persists peers via orchestrator (forceWrite) using staged anchor.
// ─────────────────────────────────────────────────────────────────────────────
function _warmTilesForMsisdnWithRev(msisdn, revOverride) {
  try {
    const tel = _normaliseLocalTel(msisdn || "");
    if (!tel) return false;

    if (_isRotaBusy()) {
      _logWarm('WARM_WITHREV_BUSY_SKIP', { msisdn: tel });
      return false;
    }

    const staged = _headersStageGet();
    if (!staged || staged.length !== 14) {
      _logWarm('WARM_WITHREV_NO_STAGED', { msisdn: tel });
      return false;
    }

    const ss = _ssRota();

    const liveHeaders = _readAvailabilityHeaders(ss) || [];
    if (liveHeaders.length !== 14) {
      _logWarm('WARM_WITHREV_FAIL_HEADERS', { msisdn: tel, have: liveHeaders.length });
      return false;
    }

    const byYmdLive = Object.fromEntries(liveHeaders.map(h => [h.ymd, h]));
    const desiredYmds = staged.map(h => h.ymd);

    const headersForRead = [];
    for (const ymd of desiredYmds) {
      const H = byYmdLive[ymd];
      if (!H) {
        _logWarm('WARM_WITHREV_MISMATCH', { msisdn: tel, ymdMissing: ymd });
        return false;
      }
      headersForRead.push(H);
    }

    const cand = _findCandidateByMobile(ss, tel);
    if (!cand) {
      _logWarm('WARM_WITHREV_FAIL_CAND', { msisdn: tel });
      return false;
    }

    const avRow = _findAvailabilityRowByTelephone(ss, cand.telephone);
    if (!avRow) {
      _logWarm('WARM_WITHREV_FAIL_ROW', { msisdn: tel });
      return false;
    }

    const nameKey = (cand.surname + " " + cand.firstname).toLowerCase().trim();
    const bookedMap = _readBookedMap(ss, nameKey, desiredYmds);
    const cells = _readAvailabilityCells(ss, avRow.rowIndex, headersForRead);

    const tiles = desiredYmds.map((ymd, i) => {
      const disp = staged[i];
      const c = cells[i];
      const b = bookedMap[ymd];

      if (b) {
        return {
          ymd,
          displayDay: disp.displayDay,
          displayDate: disp.displayDate,
          booked: true,
          editable: false,
          status: "BOOKED",
          shiftInfo: b.notes || b.shift,
          hospital: b.hospital || "",
          ward: b.ward || "",
          jobTitle: b.jobTitle || "",
          bookingRef: b.bookingRef || ""
        };
      }
      if (_isBlockedCell(c.value, c.bg)) {
        return {
          ymd,
          displayDay: disp.displayDay,
          displayDate: disp.displayDate,
          booked: false,
          editable: false,
          status: "BLOCKED"
        };
      }
      const stat = _statusFromCell(c.value, c.bg);
      return {
        ymd,
        displayDay: disp.displayDay,
        displayDate: disp.displayDate,
        booked: false,
        editable: true,
        status: stat
      };
    });

    // Choose an anchor from the staged window (nearest upcoming booked or first booked)
    let cohortKey = null;
    const P = _P() || { TZ: 'Europe/London' };
    const todayYmd = Utilities.formatDate(new Date(), P.TZ || 'Europe/London', 'yyyy-MM-dd');
    const bookedYmds = desiredYmds.filter(y => bookedMap[y]).sort();
    let pick = null;
    for (const y of bookedYmds) { if (y >= todayYmd) { pick = y; break; } }
    if (!pick) pick = bookedYmds[0] || null;

    if (pick) {
      const b = bookedMap[pick] || {};
      cohortKey = {
        ymd: pick,
        hospital: (typeof _normalizeHospitalName === 'function') ? _normalizeHospitalName(b.hospital || '') : String(b.hospital || '').trim(),
        ward: String(b.ward || '').trim(),
        shiftType: (function(s){
          const S = String(s || '').toUpperCase();
          if (S.includes('NIGHT') || S === 'N') return 'NIGHT';
          return 'LONG DAY';
        })(b.shift || b.notes || '')
      };
    }

    // Persist tri-state peers (forceWrite, since this is prewarm for a new rev)
    let peersMirror = undefined;
    if (cohortKey) {
      const tri = __getOrComputePeers(__to07(tel), cohortKey, {
        ss, user: { tel: cand.telephone, firstname: cand.firstname, surname: cand.surname },
        headersYmds: desiredYmds, bookedMap,
        computeFn: _computePeersForUser,
        forceWrite: true
      });
      if (tri && tri.state === 'some' && tri.peers) {
        peersMirror = { current: tri.peers.current, next: tri.peers.next };
      }
    }

    const rev =
      Number.isFinite(parseInt(revOverride, 10)) ? parseInt(revOverride, 10) : _revNextGet();

    _tilesPut(tel, tiles, desiredYmds, rev, peersMirror);
    _logWarm('WARM_WITHREV_OK', { msisdn: tel, rev, tiles: tiles.length, peersTriState: !!peersMirror, anchorKey: cohortKey || null });
    return true;
  } catch (e) {
    _logWarm('WARM_WITHREV_EXCEPTION', { msisdn: String(msisdn || ""), error: String(e && e.message || e) });
    return false;
  }
}


// Warm a batch of msisdns with a specific revision (revOverride).
// Returns { attempted, warmed, failed }.

// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _warmTilesForMsisdnsWithRev — unchanged batching, but each call
// persists peers via the updated single-msisdn warm (with forceWrite).
// ─────────────────────────────────────────────────────────────────────────────
function _warmTilesForMsisdnsWithRev(msisdns, revOverride) {
  if (!Array.isArray(msisdns) || !msisdns.length) {
    return { attempted: 0, warmed: 0, failed: 0 };
  }

  if (_isRotaBusy()) {
    _logWarm('WARM_WITHREV_BUSY_SKIP_BATCH', { count: msisdns.length });
    return { attempted: 0, warmed: 0, failed: 0 };
  }

  let attempted = 0, warmed = 0, failed = 0;
  const seen = new Set();
  for (const raw of msisdns) {
    const tel = _normaliseLocalTel(raw || "");
    if (!tel || seen.has(tel)) continue;
    seen.add(tel);
    attempted++;
    const ok = _warmTilesForMsisdnWithRev(tel, revOverride);
    if (ok) warmed++; else failed++;
  }
  _logWarm('WARM_WITHREV_BATCH_DONE', { attempted, warmed, failed, revOverride });
  return { attempted, warmed, failed };
}



// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _zeroLatencyFlipWarmActiveAndPublish — no logic change to publish,
// but ensures the batch warm writes peers via updated WithRev path.
// (Functionally the same call chain; included for completeness.)
// ─────────────────────────────────────────────────────────────────────────────
function _zeroLatencyFlipWarmActiveAndPublish() {
  const staged = _headersStageGet();
  if (!staged || staged.length !== 14) {
    return { ok: false, reason: "NO_STAGED_HEADERS" };
  }

  const nextRev = _prepareNextTilesRev();
  const actives = _listActiveTokenMsisdns();
  const warmed = _warmTilesForMsisdnsWithRev(actives, nextRev);
  const pub = _publishStagedHeadersAndFlip();

  return {
    ok: !!pub.ok,
    nextRev,
    activeCount: actives.length,
    warmed,
    publish: pub
  };
}


/**
 * Store tiles in cache. Backward-compatible:
 * - signature unchanged for existing callers (new 5th arg is optional).
 * - adds optional peersPrevious/current/next to the stored object.
 */

// Tiles cache helpers
// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _tilesPut — keep legacy peers fields (mirror-only), but do not rely
// on tiles as the source of truth. No behavior change for old readers.
// ─────────────────────────────────────────────────────────────────────────────
function _tilesPut(msisdn, tiles, headersYmds, revOverride, peers) {
  // Normalise the cache identity to a consistent key
  const tel = _normaliseLocalTel(msisdn || "");
  if (!tel) return;

  const p = _P();
  const curRev = _getInt(K_TILES_REV, 0);
  const targetRev = (revOverride == null || isNaN(parseInt(revOverride, 10)))
    ? curRev
    : (parseInt(revOverride, 10) || curRev);

  // Defensive copies and shape guarantees
  const headers  = Array.isArray(headersYmds) ? headersYmds.slice(0) : [];
  const tilesArr = Array.isArray(tiles)       ? tiles.slice(0)       : [];

  // Base object (unchanged fields)
  const obj = {
    _rev:   targetRev,
    _ehRev: _getInt(K_EH_REV, 0),
    headers,
    tiles:  tilesArr,
    ts:     _nowIso()
  };

  // Legacy peers mirror for back-compat with old readers (not the source of truth).
  if (peers && typeof peers === 'object') {
    if (Array.isArray(peers.current))  obj.peersCurrent  = peers.current.slice(0);
    if (Array.isArray(peers.next))     obj.peersNext     = peers.next.slice(0);
    if (Array.isArray(peers.previous)) obj.peersPrevious = peers.previous.slice(0);
  }

  // Prewarm handling (unchanged)
  if (targetRev !== curRev) {
    const key = _cacheKeyTiles(tel);
    const existing = _cacheGetJSON(key);

    if (existing && (existing._rev | 0) === curRev) {
      existing._prewarm = obj; // legacy mirror fields carried forward by _tilesGet
      _cachePutJSON(key, existing, p.TTL_TILES);
      return;
    }

    _cachePutJSON(key, obj, p.TTL_TILES);
    return;
  }

  _cachePutJSON(_cacheKeyTiles(tel), obj, p.TTL_TILES);
}


/**
 * Read tiles from cache (with zero-latency prewarm promotion).
 * Backward-compatible fields preserved; peers fields are optional.
 */

// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _tilesGet — unchanged semantics, but comment clarifies peers fields
// are legacy mirrors (source of truth is peers cache). Kept behavior intact.
// ─────────────────────────────────────────────────────────────────────────────
function _tilesGet(msisdn) {
  const key = _cacheKeyTiles(msisdn);
  const obj = _cacheGetJSON(key);
  const curRev = _getInt(K_TILES_REV, 0);
  const curEhRev = _getInt(K_EH_REV, 0);
  if (!obj) return null;

  if ((obj._rev | 0) === curRev && (obj._ehRev | 0) === curEhRev) {
    return obj; // may carry legacy peers* fields for old readers
  }

  const pw = obj._prewarm;
  if (pw && (pw._rev | 0) === curRev && (pw._ehRev | 0) === curEhRev) {
    const promoted = {
      _rev: pw._rev | 0,
      _ehRev: pw._ehRev | 0,
      headers: Array.isArray(pw.headers) ? pw.headers.slice(0) : [],
      tiles: Array.isArray(pw.tiles) ? pw.tiles.slice(0) : [],
      ts: pw.ts || _nowIso()
    };

    // Carry forward optional legacy peers mirrors if available
    if (Array.isArray(pw.peersPrevious)) promoted.peersPrevious = pw.peersPrevious.slice(0);
    if (Array.isArray(pw.peersCurrent))  promoted.peersCurrent  = pw.peersCurrent.slice(0);
    if (Array.isArray(pw.peersNext))     promoted.peersNext     = pw.peersNext.slice(0);

    _cachePutJSON(key, promoted, _P().TTL_TILES);
    return promoted;
  }

  return null; // stale
}


/**
 * Build a normalized cohort key from a booking-like record.
 * Accepts rows/objects from EmailHistory (or similar) with fields such as:
 *  - date (dd/MM/yyyy), shift, hospital, ward, job title, notes, occupantKey, etc.
 * Returns { ymd, hospital, ward, shiftType } where:
 *  - ymd: "YYYY-MM-DD"
 *  - hospital: normalized via _normalizeHospitalName if available
 *  - ward: raw string trimmed
 *  - shiftType: "LONG DAY" or "NIGHT" (fallbacks defensively)
 */

// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _cohortKeyFromBooking — unchanged semantics, but guarantees
// shiftType normalization to "NIGHT"/"LONG DAY" for stable keying.
// (Kept your original logic; no breaking changes.)
// ─────────────────────────────────────────────────────────────────────────────
function _cohortKeyFromBooking(b) {
  const P = (typeof _P === 'function') ? _P() : { TZ: 'Europe/London' };
  function _toYmdFromDDMMYYYY(s) {
    try {
      const m = String(s || '').match(/\b(\d{2})\/(\d{2})\/(\d{4})\b/);
      if (!m) return '';
      const dd = Number(m[1]), mm = Number(m[2]), yyyy = Number(m[3]);
      const d = new Date(yyyy, mm - 1, dd);
      return Utilities.formatDate(d, P.TZ || 'Europe/London', 'yyyy-MM-dd');
    } catch (_) { return ''; }
  }
  function _normHosp(h) {
    try { return (typeof _normalizeHospitalName === 'function') ? _normalizeHospitalName(h) : String(h || '').trim(); }
    catch (_) { return String(h || '').trim(); }
  }
  function _normShiftType(raw) {
    const s = String(raw || '').toUpperCase();
    if (s.includes('NIGHT') || s === 'N') return 'NIGHT';
    if (s.includes('DAY') || s === 'LD' || s === 'LD/N' || s === 'LONG DAY') return 'LONG DAY';
    return 'LONG DAY';
  }

  const ymd =
    String(b && (b.ymd || b.YMD || '')) ||
    _toYmdFromDDMMYYYY(b && (b.date || b.Date || b.DATE || ''));

  const hospitalRaw = b && (b.hospital || b.Hospital || '');
  const wardRaw     = b && (b.ward || b.Ward || '');
  const shiftRaw    = b && (b.shift || b.Shift || b.shift_type || b.shiftType || '');
  const notesRaw    = b && (b.notes || b.Notes || '');

  const shiftType = _normShiftType(shiftRaw || notesRaw || '');

  return {
    ymd: String(ymd || '').trim(),
    hospital: _normHosp(hospitalRaw),
    ward: String(wardRaw || '').trim(),
    shiftType
  };
}

/**
 * Given a cohort key { ymd, hospital, ward, shiftType }, list all impacted
 * colleague MSISDNs (local 07 form) on that shift.
 * Scans EmailHistory for matches and resolves occupant → MSISDN via Candidates sheet.
 */
function _listCohortMsisdns(key) {
  const out = new Set();
  try {
    if (!key || !key.ymd) return [];
    const P = (typeof _P === 'function') ? _P() : { TZ: 'Europe/London' };
    const ss = (typeof _ssRota === 'function') ? _ssRota() : null;
    if (!ss) return [];

    // Load EmailHistory
    const shEH = ss.getSheetByName(P.SH_EH);
    if (!shEH) return [];
    const rows = shEH.getDataRange().getDisplayValues();
    if (rows.length < 2) return [];

    const hdr = rows[0].map(s => String(s || '').toLowerCase());
    const idx = (h) => hdr.indexOf(h);

    const iDate  = idx('date');
    const iHosp  = idx('hospital');
    const iWard  = idx('ward');
    const iShift = idx('shift');
    const iOcc   = idx('occupantkey');
    const iJob   = (function() { const j = idx('job title'); return j >= 0 ? j : (rows[0].length >= 8 ? 7 : -1); })();

    function toYmd(s) {
      try {
        const m = String(s || '').match(/\b(\d{2})\/(\d{2})\/(\d{4})\b/);
        if (!m) return '';
        const dd = Number(m[1]), mm = Number(m[2]), yyyy = Number(m[3]);
        const d = new Date(yyyy, mm - 1, dd);
        return Utilities.formatDate(d, P.TZ || 'Europe/London', 'yyyy-MM-dd');
      } catch(_) { return ''; }
    }
    function normHosp(h) {
      try { return (typeof _normalizeHospitalName === 'function') ? _normalizeHospitalName(h) : String(h || '').trim(); }
      catch(_) { return String(h || '').trim(); }
    }
    function normShift(s) {
      const S = String(s || '').toUpperCase();
      if (S.includes('NIGHT') || S === 'N') return 'NIGHT';
      return 'LONG DAY';
    }

    // Build occupantKey -> msisdn map from Candidates
    const shC = ss.getSheetByName(P.SH_CAND);
    if (!shC) return [];
    const candRows = shC.getDataRange().getDisplayValues();
    const occToMsisdn = {};
    for (let i = 1; i < candRows.length; i++) {
      const sur = String(candRows[i][0] || '').trim();
      const fir = String(candRows[i][1] || '').trim();
      const tel = (typeof _normaliseLocalTel === 'function') ? _normaliseLocalTel(String(candRows[i][3] || '')) : String(candRows[i][3] || '');
      if (!tel) continue;
      const occ = (sur + ' ' + fir).toLowerCase().trim();
      if (occ) occToMsisdn[occ] = tel;
    }

    // Scan EmailHistory for matching cohort entries
    for (let r = 1; r < rows.length; r++) {
      const ymd = toYmd(iDate >= 0 ? rows[r][iDate] : '');
      if (ymd !== key.ymd) continue;

      const hos = normHosp(iHosp >= 0 ? rows[r][iHosp] : '');
      const wad = String(iWard >= 0 ? rows[r][iWard] : '').trim();
      if (hos !== key.hospital) continue;
      if (wad !== key.ward) continue;

      const st  = normShift(iShift >= 0 ? rows[r][iShift] : '');
      if (st !== key.shiftType) continue;

      const occ = String(iOcc >= 0 ? rows[r][iOcc] : '').toLowerCase().trim();
      const ms  = occToMsisdn[occ];
      if (ms) out.add(ms);
    }
  } catch (_) {
    // soft-fail
  }
  return Array.from(out);
}


/**
 * Compute peers for a given cohort key.
 * Returns { current[], next[], previous[] } where each array contains
 * { firstName, surnameInitial, role, msisdn07 } objects.
 * - current  : same (ymd, hospital, ward, shiftType)
 * - next     : same (ymd, hospital, ward) + next shiftType
 * - previous : same (ymd, hospital, ward) + previous shiftType
 *
 * NOTE: previous is intended for server-side use only (not surfaced via doGet).
 */
function _computePeersFor(key) {
  const result = { current: [], next: [], previous: [] };
  try {
    if (!key || !key.ymd) return result;
    const P = (typeof _P === 'function') ? _P() : { TZ: 'Europe/London' };
    const ss = (typeof _ssRota === 'function') ? _ssRota() : null;
    if (!ss) return result;

    const shEH = ss.getSheetByName(P.SH_EH);
    const shC  = ss.getSheetByName(P.SH_CAND);
    if (!shEH || !shC) return result;

    const rows = shEH.getDataRange().getDisplayValues();
    const hdr  = rows[0].map(s => String(s || '').toLowerCase());
    const idx  = (h) => hdr.indexOf(h);

    const iDate  = idx('date');
    const iHosp  = idx('hospital');
    const iWard  = idx('ward');
    const iShift = idx('shift');
    const iOcc   = idx('occupantkey');
    const iJob   = (function(){ const j = idx('job title'); return j >= 0 ? j : (rows[0].length >= 8 ? 7 : -1); })();

    function toYmd(s) {
      try {
        const m = String(s || '').match(/\b(\d{2})\/(\d{2})\/(\d{4})\b/);
        if (!m) return '';
        const dd = Number(m[1]), mm = Number(m[2]), yyyy = Number(m[3]);
        const d = new Date(yyyy, mm - 1, dd);
        return Utilities.formatDate(d, P.TZ || 'Europe/London', 'yyyy-MM-dd');
      } catch(_) { return ''; }
    }
    function normHosp(h) {
      try { return (typeof _normalizeHospitalName === 'function') ? _normalizeHospitalName(h) : String(h || '').trim(); }
      catch(_) { return String(h || '').trim(); }
    }
    function normShift(s) {
      const S = String(s || '').toUpperCase();
      if (S.includes('NIGHT') || S === 'N') return 'NIGHT';
      return 'LONG DAY';
    }
    function nextShiftOf(t) { return (String(t).toUpperCase() === 'NIGHT') ? 'LONG DAY' : 'NIGHT'; }
    function prevShiftOf(t) { return (String(t).toUpperCase() === 'NIGHT') ? 'LONG DAY' : 'NIGHT'; }

    function msisdnTo07(ms) {
      try {
        if (!ms) return '';
        const digits = String(ms).replace(/\s+/g,'');
        if (/^07\d{9}$/.test(digits)) return digits;
        if (/^\+447\d{9}$/.test(digits)) return '0' + digits.slice(3);
        if (/^447\d{9}$/.test(digits))   return '0' + digits.slice(2);
        const d = digits.replace(/[^\d]/g,'');
        if (d.startsWith('447') && d.length === 12) return '0' + d.slice(2);
        return d;
      } catch(_) { return String(ms || ''); }
    }
    function surnameInitial(surname) {
      const s = String(surname || '').trim();
      return s ? s[0].toUpperCase() : '';
    }

    // Build occupantKey -> { firstName, surname, msisdn07 } map
    const candRows = shC.getDataRange().getDisplayValues();
    const occMap = {};
    for (let i = 1; i < candRows.length; i++) {
      const sur = String(candRows[i][0] || '').trim();
      const fir = String(candRows[i][1] || '').trim();
      const tel = (typeof _normaliseLocalTel === 'function') ? _normaliseLocalTel(String(candRows[i][3] || '')) : String(candRows[i][3] || '');
      if (!tel) continue;
      const occ = (sur + ' ' + fir).toLowerCase().trim();
      occMap[occ] = { firstName: fir, surname: sur, msisdn07: msisdnTo07(tel) };
    }

    // Filter function by (ymd, hosp, ward, shiftType)
    function collectForShift(shiftTypeWanted) {
      const arr = [];
      for (let r = 1; r < rows.length; r++) {
        const ymd  = toYmd(iDate >= 0 ? rows[r][iDate] : '');
        if (ymd !== key.ymd) continue;

        const hosp = normHosp(iHosp >= 0 ? rows[r][iHosp] : '');
        const ward = String(iWard >= 0 ? rows[r][iWard] : '').trim();
        if (hosp !== key.hospital) continue;
        if (ward !== key.ward) continue;

        const st = normShift(iShift >= 0 ? rows[r][iShift] : '');
        if (st !== shiftTypeWanted) continue;

        const occ = String(iOcc >= 0 ? rows[r][iOcc] : '').toLowerCase().trim();
        if (!occ) continue;

        const roleRaw = iJob >= 0 ? String(rows[r][iJob] || '') : '';
        let role = String(roleRaw || '').toUpperCase().trim();
        if (role && role !== 'RMN' && role !== 'HCA') { role = role; } // keep as-is if custom

        const id = occMap[occ];
        if (!id) continue;

        arr.push({
          firstName: id.firstName || '',
          surnameInitial: surnameInitial(id.surname || ''),
          role: role || '',
          msisdn07: id.msisdn07 || ''
        });
      }
      return arr;
    }

    result.current  = collectForShift(String(key.shiftType || '').toUpperCase() || 'LONG DAY');
    result.next     = collectForShift(nextShiftOf(key.shiftType));
    result.previous = collectForShift(prevShiftOf(key.shiftType));
  } catch (_) {
    // soft-fail with empty arrays
  }
  return result;
}


/**
 * Warm a single user with optional peers.
 * Options: { includePeers: boolean, cohortKey?: {ymd,hospital,ward,shiftType} }
 * - Rebuilds full 14 tiles aligned to current memory headers.
 * - If includePeers is true, computes peers for the *nearest booked* day in-window,
 *   unless cohortKey is supplied (then uses that explicitly).
 * - Writes via _tilesPut(msisdn, tiles, ymds, null, peers).
 * Returns true on success, false otherwise.
 */



// ─────────────────────────────────────────────────────────────────────────────
// UPDATED: _warmOneUser — single user warm that respects includePeers and
// persists via tri-state orchestrator (forceWrite). Keeps tiles mirror.
// ─────────────────────────────────────────────────────────────────────────────
function _warmOneUser(msisdn, opts) {
  try {
    const tel = (typeof _normaliseLocalTel === 'function') ? _normaliseLocalTel(msisdn || '') : String(msisdn || '');
    if (!tel) return false;
    if (typeof _isRotaBusy === 'function' && _isRotaBusy()) { if (typeof _queueWarms === 'function') _queueWarms([tel]); return false; }

    const ss = (typeof _ssRota === 'function') ? _ssRota() : null;
    if (!ss) return false;

    const headersMem = (typeof _ensureHeaders === 'function') ? _ensureHeaders(ss) : null;
    if (!Array.isArray(headersMem) || headersMem.length !== 14) return false;

    const liveHeaders = (typeof _readAvailabilityHeaders === 'function') ? _readAvailabilityHeaders(ss) : [];
    if (liveHeaders.length !== 14) return false;
    const byYmdLive = Object.fromEntries(liveHeaders.map(h => [h.ymd, h]));

    const ymds = headersMem.map(h => h.ymd);
    const headersForRead = [];
    for (const y of ymds) {
      const H = byYmdLive[y];
      if (!H) return false;
      headersForRead.push(H);
    }

    const cand = (typeof _findCandidateByMobile === 'function') ? _findCandidateByMobile(ss, tel) : null;
    if (!cand) return false;
    const avRow = (typeof _findAvailabilityRowByTelephone === 'function') ? _findAvailabilityRowByTelephone(ss, cand.telephone) : null;
    if (!avRow) return false;

    const nameKey = (cand.surname + ' ' + cand.firstname).toLowerCase().trim();
    const bookedMap = (typeof _readBookedMap === 'function') ? _readBookedMap(ss, nameKey, ymds) : {};
    const cells = (typeof _readAvailabilityCells === 'function') ? _readAvailabilityCells(ss, avRow.rowIndex, headersForRead) : [];

    const tiles = ymds.map((ymd, i) => {
      const disp = headersMem[i];
      const c = cells[i] || {};
      const b = bookedMap[ymd];

      if (b) {
        return {
          ymd,
          displayDay: disp.displayDay,
          displayDate: disp.displayDate,
          booked: true,
          editable: false,
          status: 'BOOKED',
          shiftInfo: b.notes || b.shift || '',
          hospital: b.hospital || '',
          ward: b.ward || '',
          jobTitle: b.jobTitle || '',
          bookingRef: b.bookingRef || ''
        };
      }
      if (typeof _isBlockedCell === 'function' && _isBlockedCell(c.value, c.bg)) {
        return { ymd, displayDay: disp.displayDay, displayDate: disp.displayDate, booked: false, editable: false, status: 'BLOCKED' };
      }
      const stat = (typeof _statusFromCell === 'function') ? _statusFromCell(c.value, c.bg) : '';
      return { ymd, displayDay: disp.displayDay, displayDate: disp.displayDate, booked: false, editable: true, status: stat };
    });

    // Peers persist (optional)
    let peersMirror = undefined;
    const includePeers = !!(opts && opts.includePeers);
    const forceNoneIfNoAnchor = !!(opts && opts.forcePeersNoneIfNoAnchor);
    let cohortKey = (opts && opts.cohortKey) ? opts.cohortKey : null;

    if (includePeers) {
      if (!cohortKey) {
        const P = (_P && _P()) || { TZ: 'Europe/London' };
        const todayYmd = Utilities.formatDate(new Date(), P.TZ || 'Europe/London', 'yyyy-MM-dd');
        const bookedYmds = ymds.filter(y => bookedMap[y]).sort();
        let pick = null;
        for (const y of bookedYmds) { if (y >= todayYmd) { pick = y; break; } }
        if (!pick) pick = bookedYmds[0] || null;

        if (pick) {
          const b = bookedMap[pick] || {};
          cohortKey = {
            ymd: pick,
            hospital: (typeof _normalizeHospitalName === 'function') ? _normalizeHospitalName(b.hospital || '') : String(b.hospital || '').trim(),
            ward: String(b.ward || '').trim(),
            shiftType: (function(s){
              const S = String(s || '').toUpperCase();
              if (S.includes('NIGHT') || S === 'N') return 'NIGHT';
              return 'LONG DAY';
            })(b.shift || b.notes || '')
          };
        }
      }

      if (cohortKey) {
        const tri = __getOrComputePeers(__to07(tel), cohortKey, {
          ss, user: { tel: cand.telephone, firstname: cand.firstname, surname: cand.surname },
          headersYmds: ymds, bookedMap,
          computeFn: _computePeersForUser,
          forceWrite: true
        });
        if (tri && tri.state === 'some' && tri.peers) {
          peersMirror = { current: tri.peers.current, next: tri.peers.next };
        }
      } else if (forceNoneIfNoAnchor) {
        // No in-window bookings → immediately publish negative tri-state ('none')
        const synthKey = {
          ymd: String(ymds && ymds[0] || ''),
          hospital: '',
          ward: '',
          shiftType: 'NONE'
        };
        __getOrComputePeers(__to07(tel), synthKey, {
          // no computeFn → will persist 'none'
          forceRecompute: true,
          forceWrite: true
        });
      }
    }

    if (typeof _tilesPut === 'function') _tilesPut(tel, tiles, ymds, null, peersMirror);
    if (typeof _logWarm === 'function') _logWarm('WARM_ONE_OK', { msisdn: tel, tiles: tiles.length, peersTriState: !!peersMirror, anchorKey: cohortKey || null });
    return true;
  } catch (e) {
    if (typeof _logWarm === 'function') _logWarm('WARM_ONE_EXCEPTION', { msisdn: String(msisdn || ''), error: String(e && e.message || e) });
    return false;
  }
}


function _emergencyCreateRecord(msisdn, payload) {
  // payload.shift: { ymd, shift_type?, hospital, ward, jobTitle, bookingRef, shiftInfo? }
  // payload.issue: { type, eta_or_leave_time_label?, reason_text?, subject_name?, subject_msisdn? }
  // issue types now include: RUNNING_LATE | CANNOT_ATTEND | LEAVE_EARLY | DNA
  const tz = _P().TZ || 'Europe/London';
  const nowIso = _nowIso();
  const alert_id = Utilities.getUuid();

  // ── NEW: local, non-breaking normaliser used only for LEAVE_EARLY
  function __normaliseEtaOrLeaveTimeLabel_(s) {
    try {
      const raw = String(s == null ? '' : s).trim();
      if (!raw) return '';
      const lower = raw.toLowerCase();
      if (lower === 'now' || lower === 'now!') return 'NOW';

      // "HH MM" or "HH:MM"
      let m = raw.match(/^\s*(\d{1,2})[\s:](\d{1,2})\s*$/);
      if (m) {
        let hh = parseInt(m[1], 10);
        let mi = parseInt(m[2], 10);
        if (!isFinite(hh) || !isFinite(mi)) return raw;
        if (hh < 0) hh = 0; if (hh > 23) hh = 23;
        if (mi < 0) mi = 0; if (mi > 59) mi = 59;
        const HH = (hh < 10 ? '0' + hh : String(hh));
        const MM = (mi < 10 ? '0' + mi : String(mi));
        return HH + ':' + MM;
      }

      // Already like "H:M" (fallback pad)
      m = raw.match(/^\s*(\d{1,2}):(\d{1,2})\s*$/);
      if (m) {
        let hh = parseInt(m[1], 10);
        let mi = parseInt(m[2], 10);
        if (!isFinite(hh) || !isFinite(mi)) return raw;
        if (hh < 0) hh = 0; if (hh > 23) hh = 23;
        if (mi < 0) mi = 0; if (mi > 59) mi = 59;
        const HH = (hh < 10 ? '0' + hh : String(hh));
        const MM = (mi < 10 ? '0' + mi : String(mi));
        return HH + ':' + MM;
      }

      // Pass through unchanged for backward compatibility
      return raw;
    } catch (_) {
      return String(s == null ? '' : s);
    }
  }

  // Prefer name provided by caller (from Rota) and only fall back to Links
  // IMPORTANT: In DNA flow this is the REPORTER (the person raising the alert)
  const providedCandidateName = String((payload && payload.candidate_name) || '').trim();
  let candidate_name = providedCandidateName;
  let candidate_email = '';

  if (!candidate_name) {
    const candidate = _getLinksRowByTel ? _getLinksRowByTel(msisdn) : null;
    candidate_name = (candidate && candidate.row)
      ? ((String(candidate.row[candidate.m.FN] || '') + ' ' + String(candidate.row[candidate.m.SN] || '')).trim())
      : '';
    candidate_email = (candidate && candidate.row) ? String(candidate.row[candidate.m.EM] || '') : '';
  } else {
    // We still try to pick up email from Links if available
    const candidate = _getLinksRowByTel ? _getLinksRowByTel(msisdn) : null;
    candidate_email = (candidate && candidate.row) ? String(candidate.row[candidate.m.EM] || '') : '';
  }

  const ymd = String(payload.shift && payload.shift.ymd || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(ymd)) throw new Error('BAD_YMD');

  // Resolve times (using Notes parsing if available; else canonical)
  const booking = {
    ymd,
    shiftInfo: payload.shift && payload.shift.shiftInfo,      // optional
    shiftType: payload.shift && payload.shift.shift_type,     // optional hint
  };
  const times = _resolveShiftTimesForBooking(booking, tz);    // { startAtIso, endAtIso, type }
  const startIso = times.startAtIso;
  const endIso   = times.endAtIso;
  const type     = times.type;

  const date_label = _formatDateLabel(ymd, tz);

  // Prefer the tile’s label if provided; only compute if missing
  const time_range_label =
    String((payload.shift && payload.shift.time_range_label) || payload.time_range_label || '') ||
    (startIso && endIso && typeof _formatTimeRangeLabel === 'function'
      ? _formatTimeRangeLabel(startIso, endIso, tz)
      : '');

  const hospital    = String(payload.shift && payload.shift.hospital || '');
const ward        = String(payload.shift && payload.shift.ward || '');
const job_title   = String(
  (payload.shift && (payload.shift.job_title || payload.shift.jobTitle)) ||
  (payload && (payload.job_title || payload.jobTitle)) ||
  ''
);
const booking_ref = String(
  (payload.shift && (payload.shift.booking_ref || payload.shift.bookingRef)) ||
  (payload && (payload.booking_ref || payload.bookingRef)) ||
  ''
);


  const issue_type = String(payload.issue && payload.issue.type || '').toUpperCase(); // … + DNA

  // ── Normalise eta/leave label when LEAVE_EARLY; DNA & others pass through unchanged
  const eta_or_leave_time_label_in = String(payload.issue && payload.issue.eta_or_leave_time_label || '');
  const eta_or_leave_time_label =
    (issue_type === 'LEAVE_EARLY')
      ? __normaliseEtaOrLeaveTimeLabel_(eta_or_leave_time_label_in)
      : eta_or_leave_time_label_in;

  const reason_text = String(payload.issue && payload.issue.reason_text || '');

  // ── NEW (DNA): capture absentee identity if provided by reporter/app
  const subject_name   = String(payload.issue && payload.issue.subject_name || '').trim();
  const subject_msisdn = _normaliseLocalTel(String(payload.issue && payload.issue.subject_msisdn || ''));

  const base = {
    alert_id,
    created_iso: nowIso,
    updated_iso: nowIso,
    candidate_msisdn: msisdn,             // reporter msisdn
    candidate_name,                        // reporter name
    candidate_email,
    shift_ymd: ymd,
    shift_type: type,
    shift_start_iso: startIso,
    shift_end_iso: endIso,
    hospital,
    ward,
    job_title,
    booking_ref,
    date_label,
    time_range_label,
    issue_type,                  // may be 'DNA'
    eta_or_leave_time_label,     // relevant for LEAVE_EARLY; harmless for others/DNA
    reason_text,

    // ── NEW: persist absentee identity for DNA
    subject_name,                // empty for non-DNA
    subject_msisdn,              // empty for non-DNA

    // Start lifecycle at CREATED; will advance to WATI_SENT after sending
    status: 'CREATED',
    acknowledged: false,
    ack_source: '',
    ack_name: '',
    ack_number: '',
    ack_when_iso: '',
    wati_message_ids_json: '[]',
    call_queue_json: '[]',
    call_index: 0,
    call_next_at_iso: '',
    escalation_trigger_id: '',
    stepper_trigger_id: '',
    call_map_json: '{}',
    log_json: '[]',
  };

  const { rowIndex } = _eaAppend_(base);
  _eaAppendLog_(alert_id, 'Created emergency record');

  // Return enriched object so downstream notifiers can use it immediately
  return {
    alert_id,
    rowIndex,
    candidate_name,   // reporter
    candidate_email,
    job_title,
    hospital,
    ward,
    date_label,
    time_range_label,
    shift_type: type,
    startAtIso: startIso,
    endAtIso: endIso,
    booking_ref,
    // include issue + subject fields so senders don’t lose them
    issue_type,                  // may be 'DNA'
    eta_or_leave_time_label,
    reason_text,
    subject_name,
    subject_msisdn
  };
}


function _emergencySendInitialNotifications(alert_id) {
  var alert = (typeof alert_id === 'object' && alert_id.alert_id)
    ? alert_id
    : _emergencyFindById(alert_id);
  if (!alert) throw new Error('ALERT_NOT_FOUND');

  var contacts = _getEmergencyContacts() || [];
  try { _log('EMERG_INIT', 'contacts_overview', { count: contacts.length }); } catch(_) {}

  // 1) Update WATI contact attributes
  var upd = null;
  try {
    upd = _watiUpdateAlertIdForContacts(alert.alert_id, contacts);
    _eaAppendLog_(alert.alert_id, 'WATI contact attribute updates: ' + JSON.stringify({ ok: upd && upd.ok, updated: upd && upd.updated, total: upd && upd.total }));
    try { _log('EMERG_INIT', 'wati_attr_update_result', upd); } catch(_) {}
  } catch (e) {
    try { _log('EMERG_INIT', 'wati_attr_update_exception', String(e && e.message || e)); } catch(_) {}
    _eaAppendLog_(alert.alert_id, 'WATI contact attribute update exception');
  }

  // 2) Send the WATI template broadcast
  var res;
  try {
    var issue = String(alert.issue_type || '').toUpperCase();
    if (issue === 'LEAVE_EARLY' && typeof _sendWatiBulkTemplate_LEAVEEARLY === 'function') {
      try { _log('EMERG_INIT', 'wati_dispatch', { mode: 'LEAVE_EARLY' }); } catch(_) {}
      res = _sendWatiBulkTemplate_LEAVEEARLY(alert, contacts);
    } else if (issue === 'DNA' && typeof _sendWatiBulkTemplate_DNA === 'function') {
      try { _log('EMERG_INIT', 'wati_dispatch', { mode: 'DNA' }); } catch(_) {}
      res = _sendWatiBulkTemplate_DNA(alert, contacts);
    } else {
      try { _log('EMERG_INIT', 'wati_dispatch', { mode: 'DEFAULT' }); } catch(_) {}
      res = _sendWatiBulkTemplate_APPEMERGENCY(alert, contacts);
    }
  } catch (exSend) {
    try { _log('EMERG_INIT', 'wati_send_exception', String(exSend && exSend.message || exSend)); } catch(_) {}
    res = null;
  }

  _eaAppendLog_(alert.alert_id, 'WATI bulk sent: ' + JSON.stringify(res && res.httpCode ? { http: res.httpCode } : {}));
  try { _log('EMERG_INIT', 'wati_result', { http: res && res.httpCode, json: res && res.json }); } catch(_) {}

  try {
    if (res && typeof res.httpCode === 'number' && res.httpCode >= 200 && res.httpCode < 300) {
      var ids = (res && res.json && (res.json.data || res.json.messageIds || res.json.ids)) || [];
      var patch = { status: 'WATI_SENT' };
      if (ids && ids.length) patch.wati_message_ids_json = JSON.stringify(ids);
      _emergencyUpdateById(alert.alert_id, patch);
    }
  } catch(_) {}

  // 3) Side-channel email to PA
  _sendEmergencyEmailPA(alert);

  // 4) Schedule escalation trigger (registry-aware + duplicate-safe)
  var cfg = _getEmergencyProps();
  var at  = new Date(Date.now() + cfg.ACK_TIMEOUT_MIN * 60 * 1000);

  // If we already have a live escalation trigger for this alert, re-register and return it
  try {
    var existingUid = String(alert.escalation_trigger_id || '');
    if (existingUid) {
      var existing = (ScriptApp.getProjectTriggers() || []).find(function(t){
        return t.getUniqueId && t.getUniqueId() === existingUid;
      });
      if (existing) {
        _registerTrigger(existingUid, alert.alert_id, 'emergencyEscalate'); // ensure registry has owner+handler
        try { _log('EMERG_INIT', 'reuse_escalation_trigger', { uid: existingUid, alert_id: alert.alert_id }); } catch(_) {}
        return { ok: true }; // done — don’t create another trigger
      }
    }
  } catch(_) {}

  // Create fresh escalation trigger
  var trig = ScriptApp.newTrigger('emergencyEscalate').timeBased().at(at).create();
  var escUid = (trig.getUniqueId ? trig.getUniqueId() : '');

  // Persist UID to the row, then bind it in the registry
  try { _emergencyUpdateById(alert.alert_id, { escalation_trigger_id: escUid }); } catch(_) {}
  _registerTrigger(escUid, alert.alert_id, 'emergencyEscalate');

  try { _log('EMERG_INIT', 'create_escalation_trigger', { uid: escUid, alert_id: alert.alert_id, at: at.toISOString() }); } catch(_) {}

  return { ok: true };
}


function emergencyEscalate(e) {
  _log('ESCALATE', 'start', { e: e });

  // Trigger target: escalate if not acknowledged
  const t = _getTriggerSourceAlertId_(e);
  const alert_id = t || (e && e.alert_id) || null;
  if (!alert_id) {
    _log('ESCALATE', 'no_alert_id', { e });
    return;
  }

  const alert = _emergencyFindById(alert_id);
  if (!alert) {
    _log('ESCALATE', 'alert_not_found', { alert_id });
    return;
  }
  if (String(alert.acknowledged) === 'true' || alert.acknowledged === true) {
    _cancelTriggersForAlert(alert);
    _eaAppendLog_(alert_id, 'Escalation skipped (already ACK)');
    _log('ESCALATE', 'already_acknowledged', { alert_id });
    return;
  }

  const cfg = _getEmergencyProps();
  const tzDefault = (cfg && cfg.TZ) || 'Europe/London';

  // Helpers for availability
  function todayYmdInTz(d, tz) {
    return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
  }
  function nowHHmmInTz(d, tz) {
    return Utilities.formatDate(d, tz, 'HH:mm');
  }
  function weekdayKeyInTz(d, tz) {
    const key = Utilities.formatDate(d, tz, 'EEE').toLowerCase();
    const map = {mon:'mon', tue:'tue', wed:'wed', thu:'thu', fri:'fri', sat:'sat', sun:'sun'};
    return map[key] || map[key.slice(0,3)] || 'mon';
  }
  function inBlackout(contact, ymdToday) {
    const bl = Array.isArray(contact.blackouts) ? contact.blackouts : [];
    for (let i=0;i<bl.length;i++) {
      const item = bl[i] || {};
      const one = String(item.date || '').trim();
      const from = String(item.from || '').trim();
      const to   = String(item.to   || '').trim();
      if (one) {
        if (ymdToday === one) return true;
      } else if (from && to) {
        if (ymdToday >= from && ymdToday <= to) return true;
      }
    }
    return false;
  }
  function timeInWindow(nowHHmm, start, end) {
    if (!start || !end) return false;
    if (start <= end) {
      return (nowHHmm >= start && nowHHmm < end);
    } else {
      return (nowHHmm >= start || nowHHmm < end);
    }
  }
  function isWithinAnyWindow(nowHHmm, windows) {
    if (!Array.isArray(windows) || !windows.length) return false;
    for (let i=0;i<windows.length;i++) {
      const w = windows[i] || {};
      const s = String(w.start || '').trim();
      const e = String(w.end   || '').trim();
      if (s && e && timeInWindow(nowHHmm, s, e)) return true;
    }
    return false;
  }

  // Build eligible queue
  const now = new Date();
  const all = _getEmergencyContacts();
  const eligible = [];

  for (let i=0;i<all.length;i++) {
    const c = all[i] || {};
    const enabled   = (c.enabled !== false);
    const mobile447 = String(c.mobile447 || '').trim();
    if (!enabled || mobile447.length < 11) {
      _log('ESCALATE', 'skip_contact_invalid', { c });
      continue;
    }

    const ctz       = c.timezone || tzDefault;
    const ymdToday  = todayYmdInTz(now, ctz);
    const hhmmNow   = nowHHmmInTz(now, ctz);
    const wkKey     = weekdayKeyInTz(now, ctz);

    if (inBlackout(c, ymdToday)) {
      _log('ESCALATE', 'skip_contact_blackout', { c, ymdToday });
      continue;
    }

    const cw = c.call_windows || {};
    const todays = (Array.isArray(cw[wkKey]) && cw[wkKey].length) ? cw[wkKey]
                  : (Array.isArray(cw.default) ? cw.default : null);

    const available = (!todays || todays.length === 0) ? true : isWithinAnyWindow(hhmmNow, todays);
    if (!available) {
      _log('ESCALATE', 'skip_contact_outside_window', { c, hhmmNow, todays });
      continue;
    }

    eligible.push({
      priority: (typeof c.priority === 'number' && isFinite(c.priority)) ? c.priority : 9999,
      mobile447: mobile447
    });
    _log('ESCALATE', 'eligible_contact', { c, hhmmNow, ymdToday });
  }

  eligible.sort((a, b) => a.priority - b.priority);
  const queue = eligible.map(x => x.mobile447);

  if (!queue.length) {
    _eaAppendLog_(alert_id, 'No eligible emergency contacts to call');
    _log('ESCALATE', 'no_eligible_contacts', { alert_id });
    return;
  }

  // Small helper to find a display name for the callee
  function resolveResponderName(msisdn447) {
    try {
      const list = _getEmergencyContacts() || [];
      const hit = list.find(c => String(c.mobile447 || '').trim() === String(msisdn447));
      if (!hit) return 'there';
      const dn =
        (hit.display_name && String(hit.display_name).trim()) ||
        [hit.firstname, hit.surname].filter(Boolean).join(' ').trim() ||
        (hit.name && String(hit.name).trim()) ||
        (hit.fullname && String(hit.fullname).trim()) ||
        '';
      if (dn && String(dn).trim()) return String(dn).trim();
      const s = String(msisdn447 || '');
      return s ? (s.slice(0,3) + ' ' + s.slice(3,7) + ' ' + s.slice(7)) : 'there';
    } catch (_) { return 'there'; }
  }

  // Quick "when" label for speech (same heuristic as in _composeTts_)
  function deriveSpeakableWhenLabel(a) {
    try {
      const tz = (cfg && cfg.TZ) || 'Europe/London';

      const now = new Date();
      const todayYmd    = Utilities.formatDate(now, tz, 'yyyy-MM-dd');
      const tomorrowYmd = Utilities.formatDate(new Date(now.getTime() + 86400000), tz, 'yyyy-MM-dd');

      // Prefer explicit ISO start/end if present (either flat or under a.shift)
      const startIso = a.shift_start_iso || a.startAtIso || (a.shift && a.shift.startAtIso) || null;
      const endIso   = a.shift_end_iso   || a.endAtIso   || (a.shift && a.shift.endAtIso)   || null;

      // Compute start/end YMDs in TZ. If end is missing, assume same-day (long day).
      const start    = startIso ? new Date(startIso) : null;
      const end      = endIso   ? new Date(endIso)   : null;
      const startYmd = start ? Utilities.formatDate(start, tz, 'yyyy-MM-dd')
                             : (a.shift_ymd || (a.shift && a.shift.ymd) || null);
      const endYmd   = end   ? Utilities.formatDate(end,   tz, 'yyyy-MM-dd')
                             : startYmd;

      // Core rule: crosses midnight → NIGHT; same calendar day → LONG DAY
      const crossesMidnight = !!(startYmd && endYmd && endYmd !== startYmd);

      if (startYmd === todayYmd) {
        return crossesMidnight ? 'tonight' : 'today long day';
      }
      if (startYmd === tomorrowYmd) {
        return crossesMidnight ? 'tomorrow night' : 'tomorrow long day';
      }

      // Fallback: explicit date label, annotated with night/long day when we can
      const dateLabel =
        a.date_label ||
        (a.shift_ymd && typeof _formatDateLabel === 'function' ? _formatDateLabel(a.shift_ymd, tz) : '') ||
        (a.shift && a.shift.ymd && typeof _formatDateLabel === 'function' ? _formatDateLabel(a.shift.ymd, tz) : '') ||
        '';

      if (dateLabel) {
        return crossesMidnight ? (dateLabel + ' night') : (dateLabel + ' long day');
      }

      return 'the scheduled time';
    } catch (_) {
      return 'the scheduled time';
    }
  }

  // Place first call immediately (PASS alert_id so webhook can bind correctly)
  const firstTo = queue[0];
  const responderName = resolveResponderName(firstTo);
  const speakableWhen = deriveSpeakableWhenLabel(alert);
  const GAP = (Number(cfg.CALL_GAP_MIN) > 0 ? Number(cfg.CALL_GAP_MIN) : 2);

  // ── NEW: pass issueType through to TTS composer for DNA-aware copy (composer may ignore if not supported)
  const tts = _composeTts_(alert, cfg, {
    responderName,
    speakableWhenLabel: speakableWhen,
    callGapMin: GAP,
    issueType: String(alert.issue_type || '').toUpperCase() // may be 'DNA'
  });

  _log('ESCALATE', 'placing_first_call', { alert_id, to:firstTo, queue });

  const callRes = _clickSendPlaceTTSCall(firstTo, tts, { require_input: true, dtmf_ack: '1', alert_id: alert_id });
  _log('ESCALATE', 'call_result', { callRes });

  const map = {};
  map[callRes.call_id || Utilities.getUuid()] = { to:firstTo, placed_at_iso:_nowIso() };

  const nextAt = new Date(Date.now() + GAP * 60 * 1000);
  const startedIso = _nowIso();

  _emergencyUpdateById(alert_id, {
    status: 'ESCALATING',
    call_queue_json: JSON.stringify(queue),
    call_index: 1,
    call_next_at_iso: _toIsoZ_(nextAt),
    call_map_json: JSON.stringify(map),
    stepper_started_at_iso: startedIso,
    timeout_min: Number(cfg.TIMEOUT_ALERTS_MIN || 0)
  });

  _ensureStepperTrigger(alert_id);
  _eaAppendLog_(alert_id, `Escalation started: first call to ${firstTo} (eligible count=${queue.length})`);

  _log('ESCALATE', 'done', { alert_id, firstTo, queue_length: queue.length });
}
function hookClickSendVoice(e) {
  if (!_validateWebhook_(e)) return _err('UNAUTHORIZED', 401);

  // Merge tolerant body + form params so ClickSend x-www-form-urlencoded fields are always visible.
  const parsed = _safeParseBody_(e) || {};
  const params = (e && e.parameter) ? e.parameter : {};
  const body = Object.assign({}, params, parsed); // params act as a fallback

  // Lower-case view for robust key access
  const lc = {};
  Object.keys(body).forEach(k => { lc[String(k).toLowerCase()] = body[k]; });

  // ---- Accept both form and JSON shapes (robust aliases) ----
  const call_id = String(
    lc.call_id ||
    lc.voice_id ||
    lc.message_id ||
    lc.messageid ||   // ← alias
    lc._id ||
    lc.callid ||
    ''
  ).trim();

  const dtmf = String(
    lc.dtmf ||
    lc.dtmf_input ||
    lc.keypress ||
    lc.digit ||
    lc.digits ||      // ← alias
    lc.input ||
    ''
  ).trim();

  const to = String(
    lc.to ||
    lc.to_number ||
    lc.recipient ||
    lc.msisdn ||
    lc.to ||
    ''
  ).replace(/[^\d+]/g, '');

  // ---- Robust timestamp parsing (handles epoch seconds/ms and ISO) ----
  const _tsRaw = (lc.timestamp ?? lc.timestamp_send ?? lc.date ?? lc.when ?? null);
  let whenIso;
  if (_tsRaw != null && _tsRaw !== '') {
    const s = String(_tsRaw).trim();
    let ms = NaN;
    if (/^\d+$/.test(s)) {
      const n = Number(s);
      ms = n < 1e12 ? n * 1000 : n; // epoch seconds -> ms; if already ms, use as-is
    } else {
      ms = Date.parse(s);           // fall back to Date-parsable string
    }
    whenIso = Number.isFinite(ms) ? new Date(ms).toISOString() : _nowIso();
  } else {
    whenIso = _nowIso();
  }

  // Prefer explicit binding via ClickSend custom_string (we pass alert_id|msisdn there)
  const custom_string = String(
    lc.custom_string ||
    lc.customstring ||
    lc.reference ||
    ''
  ).trim();

  // ───────── helpers ─────────
  function _to447(n) {
    if (!n) return '';
    let s = String(n).replace(/[^\d+]/g, '');
    if (s.startsWith('+')) s = s.slice(1);
    if (s.startsWith('07') && s.length === 11) return '44' + s.slice(1);
    if (s.startsWith('447') && s.length === 12) return s;
    if (s.startsWith('44')  && s.length === 12) return s;
    return '';
  }
  function _sendResponderConfirm(msisdnRaw, responderName) {
    try {
      const cfg = _getEmergencyProps();
      const n447 = _to447(msisdnRaw);
      if (!n447) return;
      if (typeof _sendWATIBulkTemplate_Generic !== 'function') return;

      const receivers = [{
        whatsappNumber: n447,
        customParams: [
          { name: 'responder_name', value: responderName || '' }
        ]
      }];

      _sendWATIBulkTemplate_Generic(
        (cfg && cfg.TEMPLATE_NAME_EMERGACCEPTCONFIRM) || 'emergacceptconfirm01',
        receivers,
        'EMERG_ACCEPT_CONFIRM'
      );
    } catch (_) {}
  }

  // Resolve responder name from EMERGENCY_CONTACTS by normalised number
  function _resolveNameBy447(n447) {
    try {
      if (!n447) return '';
      const list = (typeof _getEmergencyContacts === 'function') ? (_getEmergencyContacts() || []) : [];
      for (const c of list) {
        const cn = _to447(c && (c.mobile447 || c.whatsappNumber || c.msisdn || c.mobile || ''));
        if (cn && cn === n447) {
          const dn =
            (c.display_name && String(c.display_name).trim()) ||
            [c.firstname, c.surname].filter(Boolean).join(' ').trim() ||
            (c.name && String(c.name).trim()) ||
            (c.fullname && String(c.fullname).trim()) ||
            '';
          return dn;
        }
      }
      return '';
    } catch (_) { return ''; }
  }

  function _ackAndFinish(alert_id, displayName, number447ForAck) {
    const who = displayName || (to ? `Phone ${to}` : 'Phone responder');
    const ackNum = number447ForAck || _to447(to);

    _emergencyUpdateById(alert_id, {
      acknowledged: true,
      status: 'ACKED_BY_CALL',
      ack_source: 'CLICKSEND',
      ack_name: who,
      ack_number: ackNum,
      ack_when_iso: whenIso
    });

    const fresh = _emergencyFindById(alert_id);
    _cancelTriggersForAlert(fresh);

    try { _sendWatiBulkTemplate_EMERGENCY_RESPONDED(fresh, who, whenIso); } catch (_) {}
    try { _sendResponderConfirm(ackNum, who); } catch (_) {}
    try { _sendEmergencyAckEmailPA(fresh, who, whenIso); } catch (_) {}

    _eaAppendLog_(alert_id, `ClickSend DTMF=1 ACK from ${ackNum || to || 'unknown'}`);
    return _ok({ ok: true, alert_id });
  }

  // 1) Authoritative bind via custom_string (can be "alertId|msisdn")
  if (custom_string) {
    // Parse "alert_id|msisdn" if present; else treat whole as alert_id
    let alertId = custom_string;
    let msisdn447 = '';
    const bar = custom_string.indexOf('|');
    if (bar > 0) {
      alertId = custom_string.slice(0, bar);
      msisdn447 = _to447(custom_string.slice(bar + 1));
    }

    const alert = _emergencyFindById(alertId);
    if (alert) {
      if (alert.acknowledged === true || String(alert.acknowledged) === 'true') {
        _eaAppendLog_(alertId, 'ClickSend receipt (custom_string) after ACK (ignored)');
        return _ok({ ok: true, duplicate: true, alert_id: alertId });
      }
      if (dtmf === '1') { // digits treated as first-class alias via lc.digits above
        _eaAppendLog_(alertId, 'ClickSend webhook bound via custom_string — applying ACK');

        // If msisdn present in custom_string, use it to resolve the name
        const resolvedName = msisdn447 ? _resolveNameBy447(msisdn447) : '';
        return _ackAndFinish(alertId, resolvedName || '', msisdn447 || '');
      } else {
        _eaAppendLog_(alertId, `ClickSend (custom_string) no ACK: ${JSON.stringify({ dtmf, to })}`);
        return _ok({ ok: true, ack: false, alert_id: alertId });
      }
    } else {
      try { _log('CLICKSEND', 'custom_string_alert_not_found', { custom_string }); } catch (_) {}
      // fall through to call_id mapping
    }
  }

  // 2) Fallback: resolve by call_id via call_map_json (unchanged behaviour)
  const { sh, m } = _eaIndexMap_();
  const lastRow = sh.getLastRow();
  for (let r = 2; r <= lastRow; r++) {
    const mapTxt = String(sh.getRange(r, m.call_map_json + 1).getValue() || '{}');
    let map = {}; try { map = JSON.parse(mapTxt); } catch (_) {}
    if (call_id && map[call_id]) {
      const alert_id = String(sh.getRange(r, m.alert_id + 1).getValue() || '');
      const alert = _emergencyFindById(alert_id);
      if (!alert) break;

      if (alert.acknowledged === true || String(alert.acknowledged) === 'true') {
        _eaAppendLog_(alert_id, 'ClickSend receipt (call_id) after ACK (ignored)');
        return _ok({ ok: true, duplicate: true, alert_id });
      }

      if (dtmf === '1') {
        _eaAppendLog_(alert_id, 'ClickSend webhook bound via call_id map — applying ACK');
        // If map contains a 'to' value, try to normalise and resolve name; otherwise fall back to existing flow
        let msisdn447 = '';
        try {
          const entry = map[call_id] || {};
          const raw = entry.to || entry.msisdn || entry.number || '';
          msisdn447 = _to447(raw);
        } catch (_) {}
        const resolvedName = msisdn447 ? _resolveNameBy447(msisdn447) : '';
        return _ackAndFinish(alert_id, resolvedName || '', msisdn447 || '');
      } else {
        _eaAppendLog_(alert_id, `ClickSend receipt (call_id map, no ACK): ${JSON.stringify({ dtmf, to })}`);
        return _ok({ ok: true, ack: false, alert_id });
      }
    }
  }

  // 3) No binding found — do not guess
  try { _log('CLICKSEND', 'unbound_call_webhook', { call_id, to, dtmf, whenIso }); } catch (_) {}
  return _ok({ ok: false, reason: 'CALL_ID_NOT_FOUND_AND_NO_CUSTOM_STRING' });
}




function _clickSendPlaceTTSCall(to447, text, options) {
  const cfg = _getEmergencyProps();

  // ✅ Correct Voice endpoint
  const url = 'https://rest.clicksend.com/v3/voice/send';

  // Optional: dedicated outbound number & webhook (put these in _getEmergencyProps)
  // cfg.CLICKSEND_DEDICATED_NUMBER   e.g. "+447507320592" (if your account is allowed to present CLI)
  // cfg.CLICKSEND_WEBHOOK_URL        e.g. "https://script.google.com/macros/s/XXXXX/exec?action=EMERGENCY_WEBHOOK_CLICKSEND&secret=YOUR_SECRET"
  //                                  (ClickSend will POST x-www-form-urlencoded here)

  // Build one voice message inside `messages` array
  const msg = {
    source: 'apps_script',
    to: '+' + String(to447).replace(/^\+?/, ''),   // ensure leading +
    body: String(text || ''),
    voice: (options && options.voice) || 'female',
    lang: (options && options.lang) || 'en-gb',
    require_input: (options && options.require_input) ? 1 : 0, // DTMF capture
    // Correlate back to your alert/webhook handler: "alert_id|to447"
    custom_string: (function() {
      const aid = (options && (options.alert_id || options.reference)) || '';
      const n447 = String(to447 || '').replace(/[^\d+]/g, '').replace(/^\+?/, '');
      return aid ? (aid + '|' + n447) : n447;
    })()
  };

  // Include a dedicated caller ID if configured/allowed
  if (cfg.CLICKSEND_DEDICATED_NUMBER) {
    msg.from = cfg.CLICKSEND_DEDICATED_NUMBER;
  }

  // Include webhook if configured so DTMF & delivery hit your Apps Script
  if (cfg.CLICKSEND_WEBHOOK_URL) {
    msg.webhook_url = cfg.CLICKSEND_WEBHOOK_URL;
  }

  const payload = { messages: [ msg ] };

  try {
    _log('CLICKSEND', 'tts_call_request', { to: to447, payload });

    const resp = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
      headers: {
        Authorization: 'Basic ' + Utilities.base64Encode(cfg.CLICKSEND_USER + ':' + cfg.CLICKSEND_API_KEY)
      }
    });

    const httpCode = resp.getResponseCode();
    const bodyText = resp.getContentText() || '';
    let json = null;

    try {
      json = JSON.parse(bodyText);
    } catch (parseErr) {
      _log('CLICKSEND', 'tts_call_parse_error', {
        httpCode,
        bodyPreview: bodyText.slice(0, 1000),
        error: String((parseErr && parseErr.message) || parseErr)
      });
    }

    // Typical success looks like:
    // { http_code:200, response_code:"SUCCESS", data:{ messages:[ { message_id:"...", status:"QUEUED"/"SUCCESS", ... } ] } }
    // Some accounts/regions may return call_id; handle both defensively.
    const firstMsg = json && json.data && Array.isArray(json.data.messages) ? json.data.messages[0] : null;
    const message_id = firstMsg && (firstMsg.message_id || firstMsg.voice_id || firstMsg.call_id) || '';
    const call_id    = (json && (json.call_id || (json.data && json.data.call_id))) || message_id;

    const okResponseCode = json && (/^SUCCESS$/i).test(String(json.response_code || ''));
    const is2xx = httpCode >= 200 && httpCode < 300;

    if (is2xx && (call_id || message_id) && okResponseCode) {
      _log('CLICKSEND', 'tts_call_success', { to: to447, httpCode, response_code: json.response_code, call_id, message_id, json });
    } else {
      // Log ClickSend validation hints if present
      const reason =
        (json && (json.response_msg || json.message || json.error)) ||
        (firstMsg && firstMsg.error) ||
        'UNKNOWN';
      _log('CLICKSEND', 'tts_call_failure', { to: to447, httpCode, response_code: json && json.response_code, reason, json });
    }

    return { httpCode, json, call_id, message_id };

  } catch (e) {
    _log('CLICKSEND', 'tts_call_exception', {
      to: to447,
      error: String((e && e.message) || e)
    });
    return { httpCode: 0, json: null, call_id: '', message_id: '' };
  }
}


function hookWatiAck(e) {
  // Local helper → call the global _log in a safe way
  function log(event, data) {
    try { _log('WATI_ACK', event, data || {}); } catch (_) {}
  }

  // ───────────────────────────────
  // Logs sheet helper (local)
  // ───────────────────────────────
  const __TZ = (typeof _getEmergencyProps === 'function' && _getEmergencyProps() && _getEmergencyProps().TZ) || 'Europe/London';
  function __logsAppend(route, event, data) {
  /*
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName('Logs');
    if (!sh) {
      sh = ss.insertSheet('Logs');
      sh.getRange(1,1,1,6).setValues([['Timestamp','ReqId','Route','Event','Action','DataJSON']]);
      sh.setFrozenRows(1);
    }
    const stamp = Utilities.formatDate(new Date(), __TZ, 'yyyy-MM-dd HH:mm:ss');
    sh.appendRow([stamp, '', String(route||''), String(event||''), 'WATI_ACK', JSON.stringify(data||{})]);
  } catch (_) {}
  */
  // logging disabled
}


  var __responder447 = '';

  // ────────────────────────────────────────────────────────────────
  // 0) Webhook receipt + validation + parsing
  // ────────────────────────────────────────────────────────────────
  log('Webhook received', {
    hasPostData: !!(e && e.postData && e.postData.contents),
    contentLength: (e && e.postData && e.postData.contents && String(e.postData.contents).length) || 0
  });

  // Secret validation (lenient if outer doPost already enforced)
  try {
    if (typeof _validateWebhook_ === 'function') {
      var ok = _validateWebhook_(e);
      log('Validation check executed', { ok: !!ok });
      if (!ok) {
        log('Validation failed → returning 401');
        return _err('UNAUTHORIZED', 401);
      }
    } else {
      log('No _validateWebhook_ present; skipping validation');
    }
  } catch (vErr) {
    log('Validation exception (ignored per lenient policy)', { error: String((vErr && vErr.message) || vErr) });
  }

  // Safe body parsing
  let body = null;
  try {
    body = (typeof _safeParseBody_ === 'function')
      ? _safeParseBody_(e)
      : JSON.parse(e.postData && e.postData.contents || '{}');
    log('Body parsed OK', { keys: body && Object.keys(body || {}) });
  } catch (pErr) {
    log('Body parse failed', { error: String((pErr && pErr.message) || pErr) });
    return _ok({ ok:false, reason:'BAD_JSON' });
  }

  // Pull fields we care about (now INCLUDING alert_id and conversation/message context from webhook)
  const replyRaw = String((body && body.reply) || '').trim().toUpperCase();
  const phoneRaw = String((body && (body.phone || body.whatsappNumber || body.from)) || '').trim();
  const whenIso  = String((body && (body.responded_at_iso || body.respondedAtIso)) || _nowIso());

  // Extractors
  function _extractAlertId(b) {
    if (!b) return '';
    if (b.alert_id) return String(b.alert_id).trim();
    if (b.AlertID)  return String(b.AlertID).trim();
    if (b.alertId)  return String(b.alertId).trim();
    try {
      if (b.customParams && Array.isArray(b.customParams)) {
        var hit = b.customParams.find(function(p){ return p && (p.name === 'alert_id' || p.Name === 'alert_id'); });
        if (hit && (hit.value || hit.Value)) return String(hit.value || hit.Value).trim();
      }
      if (b.customParams && typeof b.customParams === 'object') {
        if (b.customParams.alert_id) return String(b.customParams.alert_id).trim();
      }
    } catch(_) {}
    return '';
  }
  function _extractConversationId(b) {
    try {
      if (b.conversationId) return String(b.conversationId);
      if (b.conversation_id) return String(b.conversation_id);
      if (b.messages && Array.isArray(b.messages) && b.messages[0]) {
        const m = b.messages[0];
        if (m.conversationId) return String(m.conversationId);
        if (m.context && (m.context.conversationId || m.context.conversation_id)) {
          return String(m.context.conversationId || m.context.conversation_id);
        }
      }
      if (b.context && (b.context.conversationId || b.context.conversation_id)) {
        return String(b.context.conversationId || b.context.conversation_id);
      }
    } catch(_) {}
    return '';
  }
  function _extractContextMessageId(b) {
    try {
      if (b.messages && Array.isArray(b.messages) && b.messages[0]) {
        const m = b.messages[0];
        if (m.context && (m.context.id || m.context.message_id)) return String(m.context.id || m.context.message_id);
      }
      if (b.context && (b.context.id || b.context.message_id)) return String(b.context.id || b.context.message_id);
      if (b.reply_to_id) return String(b.reply_to_id);
    } catch(_) {}
    return '';
  }

  const cfg = (typeof _getEmergencyProps === 'function') ? _getEmergencyProps() : {};
  const allowLegacyFallback = !!(cfg && cfg.ALLOW_LEGACY_NO_ALERT_ID);

  const alertIdFromWebhook = _extractAlertId(body);
  const conversationIdFromWebhook = _extractConversationId(body);
  const contextMessageId = _extractContextMessageId(body);

  // Log to Logs sheet for correlation diagnostics
  __logsAppend('WATI_ACK', 'ack_webhook_received', {
    replyRaw, phoneRaw_present: !!phoneRaw, whenIso,
    alertId_present: !!alertIdFromWebhook,
    conversationId_present: !!conversationIdFromWebhook,
    contextMessageId_present: !!contextMessageId
  });

  log('Core fields extracted', {
    replyRaw,
    phoneRaw_present: !!phoneRaw,
    whenIso,
    alertIdFromWebhook_present: !!alertIdFromWebhook,
    conversationIdFromWebhook_present: !!conversationIdFromWebhook,
    contextMessageId_present: !!contextMessageId,
    allowLegacyFallback
  });

  if (replyRaw && replyRaw !== 'ACCEPT_ALERT') {
    log('Ignoring non-accept reply', { replyRaw });
    return _ok({ ok:true, ignored:true });
  }

  // Normalise numbers for reliable matching
  function to447(n) {
    if (!n) return '';
    let s = String(n).replace(/[^\d+]/g,'');
    if (s.startsWith('+')) s = s.slice(1);
    if (s.startsWith('07') && s.length === 11) return '44' + s.slice(1);
    if (s.startsWith('447') && s.length === 12) return s;
    if (s.startsWith('44')  && s.length === 12) return s;
    return '';
  }
  const responder447 = to447(phoneRaw);
  __responder447 = responder447;
  log('Responder phone normalised', { responder447 });

  // ────────────────────────────────────────────────────────────────
  // 1) Resolve target alert (prefer webhook alert_id; fallback optional)
  // ────────────────────────────────────────────────────────────────
  function _isAcked(a) {
    return !!(a && (a.acknowledged === true || String(a.acknowledged) === 'true'));
  }
  function _resolveMostRecentAlert() {
    try { if (typeof _emergencyFindMostRecent === 'function') { const x = _emergencyFindMostRecent(); if (x) return x; } } catch(_) {}
    try { if (typeof _emergencyFindMostRecentUnacked === 'function') { const x = _emergencyFindMostRecentUnacked(); if (x) return x; } } catch(_) {}
    try { if (typeof _eaFindMostRecentUnacked_ === 'function') { const x = _eaFindMostRecentUnacked_(); if (x) return x; } } catch(_) {}
    try { if (typeof _eaFindMostRecent_ === 'function') { const x = _eaFindMostRecent_(); if (x) return x; } } catch(_) {}
    return null;
  }

  let alert = null;
  let usingExplicitAlertId = false;

  if (alertIdFromWebhook) {
    alert = _emergencyFindById(alertIdFromWebhook);
    // Cross-reference log: we saw a conversation/message context with this ACK
    __logsAppend('WATI_ACK','ack_cross_reference', {
      alert_id_from_webhook: alertIdFromWebhook,
      conversation_id: conversationIdFromWebhook || null,
      context_message_id: contextMessageId || null,
      resolved_via: 'alert_id_in_webhook',
      found: !!alert
    });

    if (!alert) {
      log('Alert not found via webhook alert_id', { alert_id: alertIdFromWebhook });
      return _ok({ ok:false, reason:'ALERT_NOT_FOUND_BY_ID', alert_id: alertIdFromWebhook });
    }
    usingExplicitAlertId = true;
    log('Resolved alert via webhook alert_id', {
      alert_id: String(alert.alert_id || ''),
      status: alert && alert.status,
      acknowledged: _isAcked(alert)
    });
  } else {
    // Legacy fallback (optional)
    if (!allowLegacyFallback) {
      log('Missing alert_id and legacy fallback disabled → refusing', {});
      __logsAppend('WATI_ACK','ack_missing_alert_id_legacy_disabled',{
        conversation_id: conversationIdFromWebhook || null,
        context_message_id: contextMessageId || null
      });
      return _ok({ ok:false, reason:'ALERT_ID_REQUIRED' });
    }
    alert = _resolveMostRecentAlert();
    __logsAppend('WATI_ACK','ack_cross_reference', {
      alert_id_from_webhook: null,
      conversation_id: conversationIdFromWebhook || null,
      context_message_id: contextMessageId || null,
      resolved_via: 'LEGACY_MOST_RECENT',
      found: !!alert
    });
    if (!alert) {
      log('No target alert could be resolved via legacy fallback');
      return _ok({ ok:false, reason:'ALERT_NOT_FOUND' });
    }
    log('Resolved alert via LEGACY FALLBACK (most recent)', {
      alert_id: String(alert.alert_id || ''),
      status: alert && alert.status,
      acknowledged: _isAcked(alert)
    });
  }

  const alert_id = String(alert.alert_id || '');

  // ────────────────────────────────────────────────────────────────
  // 2) Contacts snapshot + match responder
  // ────────────────────────────────────────────────────────────────
  const contacts = (typeof _getEmergencyContacts === 'function') ? (_getEmergencyContacts() || []) : [];

  // Full + compact snapshots
  try {
    const compact = contacts.map(function (c) {
      function mask(n) {
        if (!n) return '';
        var s = String(n);
        return s.length <= 6 ? s : (s.slice(0,3) + '…' + s.slice(-3));
      }
      return {
        name: c.display_name || c.name || '',
        whatsappNumber: mask(c.whatsappNumber || c.mobile447 || ''),
        enabled: (c.enabled !== false),
        priority: c.priority,
        timezone: c.timezone,
        has_windows: !!(c.call_windows && typeof c.call_windows === 'object'),
        blackout_count: Array.isArray(c.blackouts) ? c.blackouts.length : 0
      };
    });
    log('Emergency contacts snapshot (raw)', { count: contacts.length, raw: contacts });
    log('Emergency contacts snapshot (compact)', { count: contacts.length, list: compact });
  } catch (snapErr) {
    log('Error while logging contacts snapshot (continuing)', { error: String((snapErr && snapErr.message) || snapErr) });
  }

  const matched = contacts.find(c => to447(c && (c.mobile447 || c.whatsappNumber || c.msisdn || '')) === responder447);

  log('Contacts resolved', {
    contacts_count: contacts.length,
    matched_found: !!matched,
    matched_name: matched && (matched.display_name || matched.name || matched.fullname || matched.displayName) || null
  });

  // Responder display name (prefer contacts)
  let responderName =
    (matched && (matched.display_name ||
                 [matched.firstname, matched.surname].filter(Boolean).join(' ').trim())) ||
    String((body && (body.responder_name || body.name)) || '').trim() ||
    'Responder';

  log(matched ? 'Responder name from contacts' : 'Responder name via fallback', {
    responder447,
    responderName
  });

  // ────────────────────────────────────────────────────────────────
  // 3) Idempotency: already ACKed?
  // ────────────────────────────────────────────────────────────────
  if (_isAcked(alert)) {
    log(usingExplicitAlertId ? 'Duplicate ACK (via webhook alert_id)' : 'Duplicate ACK (via legacy fallback)', { alert_id });

    // Cross-reference log row for duplicate too
    __logsAppend('WATI_ACK','ack_duplicate',{
      alert_id,
      conversation_id: conversationIdFromWebhook || null,
      context_message_id: contextMessageId || null
    });

    try {
      if (responder447 && typeof _sendWATIBulkTemplate_Generic === 'function') {
        const tnThanks = (cfg && cfg.TEMPLATE_NAME_EMERGTHANKS) || 'emergthanks';
        const receivers = [{
          whatsappNumber: responder447,
          customParams: [
            { name: 'responder_name', value: String(alert.ack_name || responderName || 'a colleague') },
            { name: 'alert_id', value: String(alert_id) }
          ]
        }];
        log('Sending emergthanks to duplicate responder', { template: tnThanks, to: responder447, alert_id });
        _sendWATIBulkTemplate_Generic(tnThanks, receivers, 'EMERGTHANKS', (cfg && cfg.TZ) || 'Europe/London');
        try { _eaAppendLog_(alert_id, 'Duplicate ACK → emergthanks to ' + responder447); } catch (_) {}
      } else {
        log('Skipped emergthanks (missing sender or responder number)');
      }
    } catch (thxErr) {
      log('Error sending emergthanks', { error: String((thxErr && thxErr.message) || thxErr) });
    }
    log('Returning OK(duplicate)');
    return _ok({ ok:true, duplicate:true, alert_id });
  }

  // ────────────────────────────────────────────────────────────────
  // 4) First acceptance → persist ACK
  // ────────────────────────────────────────────────────────────────
  try {
    log(usingExplicitAlertId ? 'Persisting ACK (webhook alert_id path)' : 'Persisting ACK (legacy fallback path)', {
      alert_id,
      ack_source: 'WATI', ack_name: responderName, ack_number: responder447, ack_when_iso: whenIso
    });
    _emergencyUpdateById(alert_id, {
      acknowledged: true,
      status: 'ACKED',
      ack_source: 'WATI',
      ack_name: responderName,
      ack_number: responder447,
      ack_when_iso: whenIso
    });

    // Explicit success marker for the new path + Logs sheet record
    if (usingExplicitAlertId) {
      log('ACK persisted using webhook alert_id (no fallback)', { alert_id });
      try { _eaAppendLog_(alert_id, 'ACK persisted using webhook alert_id'); } catch(_) {}
      __logsAppend('WATI_ACK','ack_persisted_using_alert_id',{
        alert_id,
        conversation_id: conversationIdFromWebhook || null,
        context_message_id: contextMessageId || null,
        responder447
      });
    } else {
      log('ACK persisted using legacy fallback', { alert_id });
      try { _eaAppendLog_(alert_id, 'ACK persisted via legacy fallback'); } catch(_) {}
      __logsAppend('WATI_ACK','ack_persisted_using_legacy',{
        alert_id,
        conversation_id: conversationIdFromWebhook || null,
        context_message_id: contextMessageId || null,
        responder447
      });
    }
  } catch (updErr) {
    log('ACK persist failed (continuing anyway)', { error: String((updErr && updErr.message) || updErr) });
  }

  // Refetch & cancel timers
  const fresh = _emergencyFindById(alert_id);
  try {
    log('Cancelling escalation triggers (if any)', { alert_id });
    _cancelTriggersForAlert(fresh);
    log('Escalation triggers cancelled', { alert_id });
  } catch (cErr) {
    log('Error cancelling triggers (continuing)', { error: String((cErr && cErr.message) || cErr) });
  }

  // ────────────────────────────────────────────────────────────────
  // 5) Confirm to acceptor (always)
  // ────────────────────────────────────────────────────────────────
  try {
    const tnConfirm = (cfg && cfg.TEMPLATE_NAME_EMERGACCEPTCONFIRM) || 'emergacceptconfirm01';
    if (responder447 && typeof _sendWATIBulkTemplate_Generic === 'function') {
      const receiversConfirm = [{
        whatsappNumber: responder447,
        customParams: [
          { name: 'responder_name', value: String(responderName || 'Responder') },
          { name: 'alert_id', value: String(alert_id) }
        ]
      }];
      log('Sending emergacceptconfirm01 to acceptor', { template: tnConfirm, to: responder447, alert_id });
      _sendWATIBulkTemplate_Generic(tnConfirm, receiversConfirm, 'EMERGACCEPTCONFIRM', (cfg && cfg.TZ) || 'Europe/London');
      try { _eaAppendLog_(alert_id, 'emergacceptconfirm01 → ' + responder447); } catch (_) {}
    } else {
      log('Skipped emergacceptconfirm01 (missing sender function or responder number)');
    }
  } catch (confErr) {
    log('Error sending emergacceptconfirm01', { error: String((confErr && confErr.message) || confErr) });
  }

  // ────────────────────────────────────────────────────────────────
  // 6) Broadcast to others (exclude acceptor) — respect **blackouts only**
  // ────────────────────────────────────────────────────────────────
  try {
    const contacts2 = (typeof _getEmergencyContacts === 'function') ? (_getEmergencyContacts() || []) : [];

    // Helpers for blackout check (WhatsApp uses ONLY blackouts; windows are ignored)
    function todayYmdInTz(d, tz) {
      return Utilities.formatDate(d, tz, 'yyyy-MM-dd');
    }
    function inBlackout(contact, ymdToday) {
      const bl = Array.isArray(contact.blackouts) ? contact.blackouts : [];
      for (let i=0; i<bl.length; i++) {
        const item = bl[i] || {};
        const one = String(item.date || '').trim();
        const from = String(item.from || '').trim();
        const to   = String(item.to   || '').trim();
        if (one) {
          if (ymdToday === one) return true;
        } else if (from && to) {
          if (ymdToday >= from && ymdToday <= to) return true; // inclusive
        }
      }
      return false;
    }

    const tzDefault = (cfg && cfg.TZ) || 'Europe/London';
    const now = new Date();

    // Optional second compact snapshot
    try {
      const compact2 = contacts2.map(function (c) {
        function mask(n) {
          if (!n) return '';
          var s = String(n);
          return s.length <= 6 ? s : (s.slice(0,3) + '…' + s.slice(-3));
        }
        return {
          name: c.display_name || c.name || '',
          whatsappNumber: mask(c.whatsappNumber || c.mobile447 || ''),
          enabled: (c.enabled !== false),
          priority: c.priority,
          timezone: c.timezone,
          has_windows: !!(c.call_windows && typeof c.call_windows === 'object'),
          blackout_count: Array.isArray(c.blackouts) ? c.blackouts.length : 0
        };
      });
      log('Emergency contacts snapshot #2 (compact)', { count: contacts2.length, list: compact2 });
    } catch (snapErr2) {
      log('Error while logging contacts snapshot #2 (continuing)', { error: String((snapErr2 && snapErr2.message) || snapErr2) });
    }

    // Inclusion diagnostics with explicit reason codes
    function evalInclusion(c) {
      const number = (c && (c.mobile447 || c.whatsappNumber || c.msisdn || '')) || '';
      if (!number) return { include:false, reason:'no_number' };
      if (c && c.enabled === false) return { include:false, reason:'disabled_false' };
      if (to447(number) === responder447) return { include:false, reason:'is_self' };

      const tz = (c && c.timezone) ? c.timezone : tzDefault;
      const ymd = todayYmdInTz(now, tz);
      if (inBlackout(c, ymd)) return { include:false, reason:'blackout_today', meta:{ tz, ymd } };

      return { include:true, reason:'ok' };
    }

    const diag = contacts2.map(c => {
      const r = evalInclusion(c);
      return {
        name: (c && (c.display_name || c.name)) || '',
        number_masked: (function(n){
          n = (c && (c.whatsappNumber || c.mobile447 || '')) || '';
          return n ? (String(n).slice(0,3) + '…' + String(n).slice(-3)) : '';
        })(),
        enabled: (c && c.enabled !== false),
        priority: (c && c.priority),
        timezone: (c && c.timezone) || tzDefault,
        result: r
      };
    });

    log('WA broadcast inclusion diagnostics (pre-filter)', {
      contacts_total: contacts2.length,
      responder447,
      diag
    });

    // Apply filter
    const others = contacts2.filter(c => evalInclusion(c).include);

    // Who made it through?
    try {
      const othersCompact = others.map(function (c) {
        function mask(n) {
          if (!n) return '';
          var s = String(n);
          return s.length <= 6 ? s : (s.slice(0,3) + '…' + s.slice(-3));
        }
        return {
          name: c.display_name || c.name || '',
          whatsappNumber: mask(c.whatsappNumber || c.mobile447 || ''),
          enabled: (c.enabled !== false),
          priority: c.priority,
          timezone: c.timezone
        };
      });
      log('Others list after filtering (final)', { count: others.length, list: othersCompact });
    } catch (olErr) {
      log('Error while logging others list (continuing)', { error: String((olErr && olErr.message) || olErr) });
    }

    const tnBroadcast = (cfg && cfg.TEMPLATE_NAME_RESPONDED) || 'emergencyresp01';

    const respondedAtLabel = Utilities.formatDate(
      new Date(whenIso || Date.now()),
      (cfg && cfg.TZ) || 'Europe/London',
      'EEEE d MMMM yyyy HH:mm'
    ) + 'hrs';

    if (typeof _sendWATIBulkTemplate_Generic === 'function' && others.length) {
      const receivers = others.map(c => ({
        whatsappNumber: c.mobile447,
        customParams: [
          { name: 'responder_name', value: String(responderName || 'A team member') },
          { name: 'Responded_at_label', value: respondedAtLabel },
          { name: 'alert_id', value: String(alert_id) }
        ]
      }));

      // Final preflight log before WATI call
      log('WATI preflight (RESPONDED broadcast)', {
        template: tnBroadcast,
        receivers_count: receivers.length,
        sample_first: receivers[0] || null,
        alert_id
      });

      _sendWATIBulkTemplate_Generic(tnBroadcast, receivers, 'EMERGENCYRESP01', (cfg && cfg.TZ) || 'Europe/London');
      try { _eaAppendLog_(alert_id, 'emergencyresp01 → ' + receivers.length + ' contacts (excl. acceptor; blackouts respected)'); } catch (_) {}
    } else {
      log('No others to notify or sender unavailable', { others_count: others.length, alert_id });
      try { _eaAppendLog_(alert_id, 'No emergencyresp01 sender available or no other contacts after blackout filtering'); } catch (_) {}
    }
  } catch (othErr) {
    log('Error broadcasting to others', { error: String((othErr && othErr.message) || othErr) });
  }

  // ────────────────────────────────────────────────────────────────
  // 7) Email confirmation (unchanged)
  // ────────────────────────────────────────────────────────────────
  try {
    log('Triggering email confirmation via PA', { alert_id });
    _sendEmergencyAckEmailPA(fresh, responderName, whenIso);
    log('Email confirmation triggered', { alert_id });
  } catch (emErr) {
    log('Email confirmation failed (continuing)', { error: String((emErr && emErr.message) || emErr), alert_id });
  }

  try {
    log('Append EA log: WATI ACK complete', { alert_id, usingExplicitAlertId });
    _eaAppendLog_(alert_id, (usingExplicitAlertId ? 'WATI ACK via webhook alert_id' : 'WATI ACK via legacy fallback') + ' by ' + responderName);
  } catch (_) {}

  // Final Logs sheet confirmation row (so you can track end-to-end)
  __logsAppend('WATI_ACK','ack_complete',{
    alert_id,
    used_webhook_alert_id: usingExplicitAlertId,
    conversation_id: conversationIdFromWebhook || null,
    context_message_id: contextMessageId || null
  });

  log('Returning OK(true)', { alert_id, usingExplicitAlertId });
  return _ok({ ok:true, alert_id, used_webhook_alert_id: usingExplicitAlertId });
}




function _clickSendPlaceTTSCall(to447, text, options) {
  const cfg = _getEmergencyProps();

  // ✅ Correct Voice endpoint
  const url = 'https://rest.clicksend.com/v3/voice/send';

  // Optional: dedicated outbound number & webhook (put these in _getEmergencyProps)
  // cfg.CLICKSEND_DEDICATED_NUMBER   e.g. "+447507320592" (if your account is allowed to present CLI)
  // cfg.CLICKSEND_WEBHOOK_URL        e.g. "https://script.google.com/macros/s/XXXXX/exec?action=EMERGENCY_WEBHOOK_CLICKSEND&secret=YOUR_SECRET"
  //                                  (ClickSend will POST x-www-form-urlencoded here)

  // Build one voice message inside `messages` array
  const msg = {
    source: 'apps_script',
    to: '+' + String(to447).replace(/^\+?/, ''),   // ensure leading +
    body: String(text || ''),
    voice: (options && options.voice) || 'female',
    lang: (options && options.lang) || 'en-gb',
    require_input: (options && options.require_input) ? 1 : 0, // DTMF capture
    // Correlate back to your alert/webhook handler: "alert_id|to447"
    custom_string: (function() {
      const aid = (options && (options.alert_id || options.reference)) || '';
      const n447 = String(to447 || '').replace(/[^\d+]/g, '').replace(/^\+?/, '');
      return aid ? (aid + '|' + n447) : n447;
    })()
  };

  // Include a dedicated caller ID if configured/allowed
  if (cfg.CLICKSEND_DEDICATED_NUMBER) {
    msg.from = cfg.CLICKSEND_DEDICATED_NUMBER;
  }

  // Include webhook if configured so DTMF & delivery hit your Apps Script
  if (cfg.CLICKSEND_WEBHOOK_URL) {
    msg.webhook_url = cfg.CLICKSEND_WEBHOOK_URL;
  }

  const payload = { messages: [ msg ] };

  try {
    _log('CLICKSEND', 'tts_call_request', { to: to447, payload });

    const resp = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
      headers: {
        Authorization: 'Basic ' + Utilities.base64Encode(cfg.CLICKSEND_USER + ':' + cfg.CLICKSEND_API_KEY)
      }
    });

    const httpCode = resp.getResponseCode();
    const bodyText = resp.getContentText() || '';
    let json = null;

    try {
      json = JSON.parse(bodyText);
    } catch (parseErr) {
      _log('CLICKSEND', 'tts_call_parse_error', {
        httpCode,
        bodyPreview: bodyText.slice(0, 1000),
        error: String((parseErr && parseErr.message) || parseErr)
      });
    }

    // Typical success looks like:
    // { http_code:200, response_code:"SUCCESS", data:{ messages:[ { message_id:"...", status:"QUEUED"/"SUCCESS", ... } ] } }
    // Some accounts/regions may return call_id; handle both defensively.
    const firstMsg = json && json.data && Array.isArray(json.data.messages) ? json.data.messages[0] : null;
    const message_id = firstMsg && (firstMsg.message_id || firstMsg.voice_id || firstMsg.call_id) || '';
    const call_id    = (json && (json.call_id || (json.data && json.data.call_id))) || message_id;

    const okResponseCode = json && (/^SUCCESS$/i).test(String(json.response_code || ''));
    const is2xx = httpCode >= 200 && httpCode < 300;

    if (is2xx && (call_id || message_id) && okResponseCode) {
      _log('CLICKSEND', 'tts_call_success', { to: to447, httpCode, response_code: json.response_code, call_id, message_id, json });
    } else {
      // Log ClickSend validation hints if present
      const reason =
        (json && (json.response_msg || json.message || json.error)) ||
        (firstMsg && firstMsg.error) ||
        'UNKNOWN';
      _log('CLICKSEND', 'tts_call_failure', { to: to447, httpCode, response_code: json && json.response_code, reason, json });
    }

    return { httpCode, json, call_id, message_id };

  } catch (e) {
    _log('CLICKSEND', 'tts_call_exception', {
      to: to447,
      error: String((e && e.message) || e)
    });
    return { httpCode: 0, json: null, call_id: '', message_id: '' };
  }
}

function _sendWATIBulkTemplate_Generic(templateName, receiversModel, broadcastName, tz) { 
  // ───── config ─────
  const cfg      = _getEmergencyProps();
  const endpoint = (cfg.WATI_ENDPOINT || '').replace(/\/+$/, ''); // e.g. "https://eu-api.wati.io/601205"
  if (!endpoint) throw new Error('WATI_ENDPOINT is not set.');
  const url      = `${endpoint}/api/v2/sendTemplateMessages`;
  const tzUse    = tz || cfg.TZ || Session.getScriptTimeZone() || 'Etc/GMT';

  // ───── Logs sheet (always writes; creates if missing) ─────
  function __logsSheet() {
  /*
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName('Logs');
    if (!sh) {
      sh = ss.insertSheet('Logs');
      sh.getRange(1,1,1,7).setValues([[
        'Timestamp','Route','Event','Template','Broadcast','HttpCode','DataJSON'
      ]]);
      sh.setFrozenRows(1);
    }
    return sh;
  } catch (e) { return null; }
  */
  // logging disabled
  return null;
}

  function __log(event, obj) {
  /*
  try {
    const sh = __logsSheet();
    if (!sh) return;
    const stamp = Utilities.formatDate(new Date(), tzUse, 'yyyy-MM-dd HH:mm:ss');
    sh.appendRow([
      stamp,
      '_sendWATIBulkTemplate_Generic',
      String(event || ''),
      String(templateName || ''),
      String(broadcastName || ''),
      (obj && obj.httpCode) || '',
      JSON.stringify(obj || {}, null, 0)
    ]);
  } catch (_) {}
  */
  // logging disabled
}


  // ───── small helpers ─────
  function stripHrs(t) { return String(t || '').trim().replace(/\s*hrs?$/i, ''); }

  // map reason/issue → friendly string (for WATI only)
  function mapReasonForWati(reasonOrIssue) {
    const r = String(reasonOrIssue || '').toUpperCase();
    if (r === 'CANNOT_ATTEND') return 'Cancelling Shift';
    if (r === 'LEAVE_EARLY')   return 'Needs to leave their current shift early';
    if (r === 'DNA')           return 'Did Not Arrive';
    return reasonOrIssue || '';
  }

  // LEAVE_EARLY: craft Arrival_by_label as sentence for WATI
  function formatLeaveEarlyArrivalLabel(m) {
    const raw = m && (m.Arrival_by_label || m.arrival_by_label || m.arrivalLabel) || '';
    const s = String(raw).trim();
    if (!s) return '';
    const up = s.toUpperCase();
    if (up === 'NOW' || up === 'ASAP') {
      return 'This candidate needs to leave immediately';
    }
    const core = stripHrs(s);
    if (/^\d{1,2}:\d{2}$/.test(core)) {
      return `This candidate needs to leave at ${core}hrs`;
    }
    // Fallback: if not a clear time label, return the stripped label as-is
    return stripHrs(s);
  }

  // ───── customParams builders (fallbacks; PASS THROUGH if provided on receiver) ─────
  function buildPrevOrSameParams(m) {
    return [
      { name: "firstName",          value: m.firstName || "" },
      { name: "lateWorkerName",     value: m.lateWorkerName || "" },
      { name: "lateWorkerJobTitle", value: m.lateWorkerJobTitle || "" },
      { name: "shiftType",          value: m.shiftType || "" },
      { name: "hospital",           value: m.hospital || "" },
      { name: "ward",               value: m.ward || "" },
      { name: "hhmm",               value: stripHrs(m.hhmm || m.arrival_by_label || m.arrivalLabel || "") },
      { name: "mobile",             value: m.mobile || m.candidate_msisdn_447 || "" },
      { name: "reasonText",         value: mapReasonForWati(m.reason || m.issue_type) }
    ];
  }
  function buildEmergencyParams(m) {
    const arrivalText = (String(m.reason || m.issue_type).toUpperCase() === 'LEAVE_EARLY')
      ? formatLeaveEarlyArrivalLabel(m)
      : stripHrs(m.Arrival_by_label || m.arrival_by_label || m.arrivalLabel || "");
    return [
      { name: "Candidate_name",   value: m.Candidate_name || m.candidate_name || "" },
      { name: "Role",             value: m.Role || m.role || "" },
      { name: "Shift_type",       value: m.Shift_type || m.shift_type || "" },
      { name: "Hospital",         value: m.Hospital || m.hospital || "" },
      { name: "Ward",             value: m.Ward || m.ward || "" },
      { name: "Date_label",       value: m.Date_label || m.date_label || "" },
      { name: "Arrival_by_label", value: arrivalText },
      { name: "Candidate_msisdn", value: m.Candidate_msisdn || m.candidate_msisdn_447 || "" },
      { name: "reasonText",       value: mapReasonForWati(m.reason || m.issue_type) }
    ];
  }
  function paramsBuilderFor(name) {
    return String(name || '').trim().toLowerCase() === 'lateemergency'
      ? buildEmergencyParams
      : buildPrevOrSameParams;
  }
  const buildParams = paramsBuilderFor(templateName);

  // ───── receivers (normalize to array of { whatsappNumber, customParams[] }) ─────
  const list = Array.isArray(receiversModel) ? receiversModel : [receiversModel];
  const receivers = list.map(r => {
    const whatsappNumber = String(r.whatsappNumber || '').trim();
    const customParams = Array.isArray(r.customParams) && r.customParams.length
      ? r.customParams                         // ← PASS THROUGH unchanged (keeps CTA param "1")
      : buildParams(r);
    return { whatsappNumber, customParams };
  }).filter(r => r.whatsappNumber);

  // ───── payload ─────
  const payload = {
    template_name:  templateName,
    broadcast_name: broadcastName || ('Rota_' + Utilities.formatDate(new Date(), tzUse, 'yyyyMMdd_HHmmss')),
    receivers,
    ...(cfg.WATI_CHANNEL_NUMBER ? { channelNumber: cfg.WATI_CHANNEL_NUMBER } : {})
  };

  // ───── request headers (full, unredacted) ─────
  const auth = String(cfg.WATI_TOKEN || '').trim(); // expected to be "Bearer …"
  const headers = { Authorization: auth };

  // ───── log request (ALWAYS full, unredacted) ─────
  try {
    __log('wati:request', {
      url,
      headers,      // includes full Bearer token (INTENTIONALLY UNREDACTED)
      payload       // includes full receivers and params
    });
  } catch (_) {}

  // ───── call WATI ─────
  const resp = UrlFetchApp.fetch(url, {
    method:             'post',
    contentType:        'application/json',
    headers,
    payload:            JSON.stringify(payload),
    muteHttpExceptions: true
  });

  const httpCode = resp.getResponseCode();
  const rawText  = resp.getContentText() || '';
  let json = null; try { json = JSON.parse(rawText); } catch (_) {}

  // ───── log response (ALWAYS full, unredacted, no truncation) ─────
  try {
    __log('wati:response', {
      httpCode,
      json,
      rawText
    });
  } catch (_) {}

  return { httpCode, json, rawText };
}


function _sendEmergencyEmailPA(alert) {
  const props = PropertiesService.getScriptProperties();
  const hook = props.getProperty('POWER_EMAIL');
  if (!hook) return { ok: false, error: 'POWER_EMAIL_NOT_CONFIGURED' };

  const tz = props.getProperty('TIMEZONE') || 'Europe/London';
  const to = props.getProperty('EMERGENCY_FROM_EMAIL') || 'sussex@arthur-rai.co.uk';

  // For DNA, make absentee (subject) the headline; otherwise keep reporter (candidate)
  const isDNA = String(alert.issue_type || (alert.issue && alert.issue.type) || '').toUpperCase() === 'DNA';
  const displayNamePrimary = isDNA
    ? ((alert.subject_name && String(alert.subject_name).trim()) || 'Unknown')
    : ((alert.candidate_name && String(alert.candidate_name).trim()) || 'Unknown');
  const displayTelPrimary = isDNA
    ? (alert.subject_msisdn || (alert.issue && alert.issue.subject_msisdn) || '')
    : (alert.candidate_msisdn || '');

  // Subject
  var subject = `[EMERGENCY] ${displayNamePrimary} — ${alert.shift_type || ''} — ${alert.date_label || ''}`.trim();

  // ───── helpers for display-only transformations ─────
  function stripHrs(t) { return String(t || '').trim().replace(/\s*hrs?$/i, ''); }
  function mapIssueTypeLabel(issue) {
    const r = String(issue || '').toUpperCase();
    if (r === 'CANNOT_ATTEND') return 'Cancelling Shift';
    if (r === 'LEAVE_EARLY')   return 'Needs to leave their current shift early';
    if (r === 'DNA')           return 'Did Not Arrive';
    return String(issue || '');
  }
  // (kept as-is: display tidy for free-text reason if it happens to equal the enums)
  function mapReasonForEmail(reason) {
    const r = String(reason || '').toUpperCase();
    if (r === 'CANNOT_ATTEND') return 'Cancelling Shift';
    if (r === 'LEAVE_EARLY')   return 'Needs to leave their current shift early';
    if (r === 'DNA')           return 'Did Not Arrive';
    return reason || '';
  }
  function formatLeaveEarlyEmailLabel(src) {
    const s = String(src || '').trim();
    if (!s) return '';
    const up = s.toUpperCase();
    if (up === 'NOW' || up === 'ASAP') return 'This candidate needs to leave immediately';
    const core = stripHrs(s);
    if (/^\d{1,2}:\d{2}$/.test(core)) return `This candidate needs to leave at ${core}hrs`;
    return stripHrs(s);
  }

  // derive display values (render-time only; underlying alert is unchanged)
  const issueLabel = mapIssueTypeLabel(alert.issue_type || (alert.issue && alert.issue.type) || '');
  const isLeaveEarly = String(alert.issue_type || (alert.issue && alert.issue.type) || '')
    .toUpperCase() === 'LEAVE_EARLY';
  const displayEta = isLeaveEarly
    ? formatLeaveEarlyEmailLabel(alert.eta_or_leave_time_label || '')
    : (alert.eta_or_leave_time_label || '');
  const displayReason = mapReasonForEmail(alert.reason_text || '');

  // If DNA, add a clear reporter line
  const reporterLine = isDNA
    ? `<p><b>Reported by:</b> ${_escapeHtml(alert.candidate_name || '')} (${_escapeHtml(alert.candidate_msisdn || '')})</p>`
    : '';

  // HTML body (simple, safe; no attachment)
  var html =
    '<div style="font-family:Arial,Helvetica,sans-serif;line-height:1.45;font-size:14px;color:#111;">' +
      `<p><b>Emergency alert raised</b></p>` +
      `<p><b>Candidate:</b> ${_escapeHtml(displayNamePrimary)} (${_escapeHtml(displayTelPrimary || '')})</p>` +
      (alert.job_title ? `<p><b>Role:</b> ${_escapeHtml(alert.job_title)}</p>` : '') +
      `<p><b>Hospital/Ward:</b> ${_escapeHtml(alert.hospital || '')} / ${_escapeHtml(alert.ward || '')}</p>` +
      `<p><b>Date:</b> ${_escapeHtml(alert.date_label || '')}</p>` +
      `<p><b>Time:</b> ${_escapeHtml(alert.time_range_label || '')}</p>` +
      `<p><b>Shift:</b> ${_escapeHtml(alert.shift_type || '')}</p>` +
      `<p><b>Issue:</b> ${_escapeHtml(issueLabel)}</p>` +
      (alert.eta_or_leave_time_label ? `<p><b>ETA/Leave time:</b> ${_escapeHtml(displayEta)}</p>` : '') +
      (alert.reason_text ? `<p><b>Reason:</b> ${_escapeHtml(displayReason)}</p>` : '') +
      reporterLine +
      (alert.booking_ref ? `<p><b>Booking Ref:</b> ${_escapeHtml(alert.booking_ref)}</p>` : '') +
      `<p><b>Alert ID:</b> ${_escapeHtml(alert.alert_id || '')}</p>` +
      `<p>Timezone: ${_escapeHtml(tz)}</p>` +
    '</div>';

  var payload = {
    to: to,
    subject: subject,
    htmlBody: html,
    body: html // fallback
  };

  var res = _postToPowerEmail(hook, payload);
  try {
    _eaAppendLog_(alert.alert_id, 'PowerEmail initial: ' + JSON.stringify({ status: res && res.status, ok: !!(res && res.ok) }));
  } catch (_) {}
  return res;
}


function _emergencyUpdateById(alert_id, patchObj) {
  const ok = _eaPatch_(alert_id, patchObj);
  if (ok) _eaAppendLog_(alert_id, 'Patched: '+Object.keys(patchObj).join(', '));
  return ok;
}
function _emergencyFindById(alert_id) {
  const rec = _eaFindRowById_(alert_id);
  if (!rec) return null;
  const { sh, m, rowIndex } = rec;

  const rowVals  = sh.getRange(rowIndex, 1, 1, sh.getLastColumn()).getValues()[0];
  const rowDisp  = sh.getRange(rowIndex, 1, 1, sh.getLastColumn()).getDisplayValues()[0];

  const obj = {};
  Object.keys(m).forEach(k => {
    // For the label field, take the displayed string to avoid Date coercion
    if (k === 'eta_or_leave_time_label') {
      obj[k] = rowDisp[m[k]];                  // e.g. "21:00"
    } else {
      obj[k] = rowVals[m[k]];
    }
  });

  // expand JSON fields
  try { obj.wati_message_ids = JSON.parse(String(obj.wati_message_ids_json || '[]')); } catch(_){ obj.wati_message_ids=[]; }
  try { obj.call_queue       = JSON.parse(String(obj.call_queue_json       || '[]')); } catch(_){ obj.call_queue=[]; }
  try { obj.call_map         = JSON.parse(String(obj.call_map_json         || '{}')); } catch(_){ obj.call_map={}; }
  try { obj.log              = JSON.parse(String(obj.log_json              || '[]')); } catch(_){ obj.log=[]; }

  return obj;
}

function _eaIndexMap_() {
  const sh = _eaSheet_();
  const hdr = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0];
  const m = {};
  hdr.forEach((h,i)=> m[h]=i);
  return { sh, m };
}
function _composeTts_(alert, cfg, opts) {
  // Log raw inputs up-front
  try {
    logminimal('TTS', 'compose:start', {
      alert,
      cfg_snapshot: { TZ: (cfg && cfg.TZ) || 'n/a', CALL_GAP_MIN: cfg && cfg.CALL_GAP_MIN, CLICKSEND_WEBHOOK_URL: cfg && cfg.CLICKSEND_WEBHOOK_URL ? 'set' : 'unset' },
      opts
    });
  } catch (_) {}

  const tz  = (cfg && cfg.TZ) || 'Europe/London';
  const gap = (opts && Number(opts.callGapMin) > 0)
    ? Number(opts.callGapMin)
    : (Number(cfg && cfg.CALL_GAP_MIN) || 2);

  // ── First-name only for responder
  const rawResponder = (opts && opts.responderName) ? String(opts.responderName).trim() : '';
  const name = rawResponder ? rawResponder.split(/\s+/)[0] : 'there';

  // ── Helpers ─────────────────────────────────────────────────────────────
  function _formatLocation(hospital, ward) {
    const h = String(hospital || '').trim();
    const w = String(ward || '').trim();
    if (!h && !w) return '';
    if (h && w) return ` at ${h} on ${w} ward`;
    if (h)      return ` at ${h}`;
    return ` on ${w} ward`;
  }

  // Article "a/an" for role (works for acronym speech like "R, M, N")
  function _articleFor(s) {
    const t = String(s || '').trim();
    if (!t) return 'a';
    if (/^[aeiou]/i.test(t) || /^[EFHILMNRSX]/i.test(t)) return 'an';
    return 'a';
  }

  // Speak acronyms like "RMN" or "HCA" as "R, M, N" / "H, C, A"
  function _speakRole(roleRaw) {
    const r = String(roleRaw || '').trim();
    const upper = r.toUpperCase();
    if (/^[A-Z]{2,6}$/.test(upper)) {
      return upper.split('').join(', ');
    }
    return r;
  }

  // Derive a friendly "when" label if not provided, and detect shift kind
  function _deriveWhenAndKind(a) {
    if (opts && opts.speakableWhenLabel) return { when: String(opts.speakableWhenLabel), kind: '' };

    try {
      // Prefer ISO start/end if present
      var startIso =
        a.shift_start_iso || a.startAtIso || (a.shift && a.shift.startAtIso) || null;
      var endIso =
        a.shift_end_iso || a.endAtIso || (a.shift && a.shift.endAtIso) || null;

      var now = new Date();
      var todayYmd    = Utilities.formatDate(now, tz, 'yyyy-MM-dd');
      var tomorrowYmd = Utilities.formatDate(new Date(now.getTime() + 24*60*60*1000), tz, 'yyyy-MM-dd');

      var start = startIso ? new Date(startIso) : null;
      var end   = endIso   ? new Date(endIso)   : null;

      var startYmd = start
        ? Utilities.formatDate(start, tz, 'yyyy-MM-dd')
        : (a.shift_ymd || (a.shift && a.shift.ymd) || null);

      var shiftTypeRaw = String(
        a.shift_type_raw ||
        (a.shift && (a.shift.shift_type_raw || a.shift.type || a.shift.shift_type)) ||
        a.shift_type ||
        ''
      ).toLowerCase();

      var timeRange = String(
        a.time_range_label ||
        (a.shift && a.shift.time_range_label) ||
        ''
      ).toLowerCase();

      var crossesMidnight = false;
      if (start && end) {
        const sY = Utilities.formatDate(start, tz, 'yyyy-MM-dd');
        const eY = Utilities.formatDate(end,   tz, 'yyyy-MM-dd');
        crossesMidnight = (sY !== eY);
      }

      var isNight   = crossesMidnight || /night/.test(shiftTypeRaw) || /22|23|00|01|02|03|04|05/.test(timeRange);
      var isLongDay = !crossesMidnight && (/long\s*day/.test(shiftTypeRaw) || /07|08|09.*19|20/.test(timeRange));

      let whenLabel;
      if (startYmd === todayYmd) {
        whenLabel = isNight ? 'tonight' : (isLongDay ? 'today long day' : 'today');
      } else if (startYmd === tomorrowYmd) {
        whenLabel = isNight ? 'tomorrow night' : (isLongDay ? 'tomorrow long day' : 'tomorrow');
      } else {
        whenLabel =
          a.date_label ||
          (a.shift_ymd && typeof _formatDateLabel === 'function' ? _formatDateLabel(a.shift_ymd, tz) : '') ||
          (a.shift && a.shift.ymd && typeof _formatDateLabel === 'function' ? _formatDateLabel(a.shift.ymd, tz) : '') ||
          'the scheduled time';
      }

      const kind = isNight ? 'night' : (isLongDay ? 'long day' : '');

      try {
        logminimal('TTS', 'compose:derived_when_kind', {
          startIso, endIso, startYmd, timeRange, shiftTypeRaw, crossesMidnight, isNight, isLongDay, whenLabel, kind
        });
      } catch (_) {}

      return { when: whenLabel, kind };
    } catch (err) {
      try { logminimal('TTS', 'compose:derive_when_error', { error: String(err && err.message || err) }); } catch (_) {}
      return { when: 'the scheduled time', kind: '' };
    }
  }
// ── Inputs used in the script ───────────────────────────────────────────
const { when: whenLabel, kind: shiftKind } = _deriveWhenAndKind(alert);

const reporter = String(alert.candidate_name || alert.candidateName || 'the reporter'); // reporter (caller)
const absentee = String(alert.subject_name || 'the worker');                              // absentee (DNA)
const candidate = String(alert.candidate_name || alert.candidateName || 'the candidate'); // retained for non-DNA copy

const roleRaw    = String(alert.job_title || (alert.shift && (alert.shift.job_title || alert.shift.jobTitle)) || 'colleague');
const roleSpoken = _speakRole(roleRaw);
const article    = _articleFor(roleSpoken);

const hospital = String(alert.hospital || (alert.shift && alert.shift.hospital) || '').trim();
const ward     = String(alert.ward || (alert.shift && alert.shift.ward) || '').trim();
const where    = _formatLocation(hospital, ward);

const reasonText = String(alert.reason_text || (alert.issue && alert.issue.reason_text) || '').trim();
const because    = reasonText ? ` because ${reasonText}` : '';

const issueType  = String((opts && opts.issueType) || alert.issue_type || (alert.issue && alert.issue.type) || '').toUpperCase();

// --- TIME DETAIL (with deep diagnostics) ---
const _src_label_flat    = alert.eta_or_leave_time_label;
const _src_label_nested  = alert.issue && alert.issue.eta_or_leave_time_label;
const _src_value_nested  = alert.issue && alert.issue.eta_or_leave_time;

function _previewVal(v) {
  try {
    if (v == null) return { kind: 'nullish', value: null };
    const tag = Object.prototype.toString.call(v); // [object Date], [object String], etc.
    if (tag === '[object Date]') {
      const y = v.getUTCFullYear && v.getUTCFullYear();
      return {
        kind: 'Date',
        year: y,
        isoZ: Utilities.formatDate(v, tz, "yyyy-MM-dd'T'HH:mm:ss'Z'")
      };
    }
    const s = String(v);
    return {
      kind: typeof v,
      value: s.length > 120 ? (s.slice(0,120) + '…') : s,
      is_1899_iso: /^1899-12-30T/i.test(s)
    };
  } catch (e) {
    return { kind: 'error', value: String(v) };
  }
}

try {
  _log('TTS', 'compose:time_inputs_raw', {
    flat_eta_or_leave_time_label: _previewVal(_src_label_flat),
    nested_eta_or_leave_time_label: _previewVal(_src_label_nested),
    nested_eta_or_leave_time_value: _previewVal(_src_value_nested),
    shift_ymd_preview: _previewVal(alert.shift_ymd),
    start_iso_preview: _previewVal(alert.shift_start_iso || (alert.shift && alert.shift.startAtIso)),
    end_iso_preview:   _previewVal(alert.shift_end_iso   || (alert.shift && alert.shift.endAtIso))
  });
} catch (_) {}

const timeDetail = String(
  _src_label_flat ||
  _src_label_nested ||
  _src_value_nested ||
  ''
).trim();

// Log the chosen output + flags
try {
  _log('TTS', 'compose:time_detail_resolved', {
    chosen_timeDetail: timeDetail,
    chosen_is_1899_iso: /^1899-12-30T/i.test(timeDetail),
    source_used: (_src_label_flat && 'flat_label') ||
                 (_src_label_nested && 'nested_label') ||
                 (_src_value_nested && 'nested_value') || 'none'
  });
} catch (_) {}

// --- continue as before ---
try {
  logminimal('TTS', 'compose:inputs_normalised', {
    tz, gap, name, reporter, absentee, candidate,
    roleRaw, roleSpoken, article, hospital, ward, where,
    reasonText, because, issueType, timeDetail, whenLabel, shiftKind
  });
} catch (_) {}

  // ── Short, clear voice script ───────────────────────────────────────────
  const parts = [];
  parts.push(`Hi ${name}.`);

  if (issueType === 'DNA') {
    // "<absentee> has failed to arrive ... at <hospital> <ward> <shift type>. This was reported by <reporter>."
    const shiftBit = shiftKind ? ` on a ${shiftKind} shift` : '';
    parts.push(`${absentee} has failed to arrive for ${whenLabel}${where}${shiftBit}. This was reported by ${reporter}.`);
  } else if (issueType === 'LEAVE_EARLY') {
    const leaveWhen = timeDetail
      ? (timeDetail.toUpperCase() === 'NOW' ? 'now' : `at ${timeDetail}`)
      : 'soon';
    parts.push(`${candidate}, ${article} ${roleSpoken}, needs to leave ${leaveWhen} for ${whenLabel}${where}${because}.`);
  } else {
    parts.push(`${candidate}, ${article} ${roleSpoken}, has cancelled for ${whenLabel}${where}${because}.`);
  }

  parts.push(`Press 1 to take responsibility; otherwise I’ll call someone else in ${gap} minutes. Press 1 now.`);

  const script = parts.join(' ');

  // Final log with the composed script and key context
  try {
    logminimal('TTS', 'compose:done', {
      script_preview: script.slice(0, 1000),
      total_chars: script.length,
      context: { issueType, whenLabel, shiftKind, where, roleSpoken, name, gap }
    });
  } catch (_) {}

  return script;
}

function _sendWatiBulkTemplate_APPEMERGENCY(alert, contacts) {
  const cfg = _getEmergencyProps();
  const tz  = (cfg && cfg.TZ) || 'Europe/London';

  // ───────────────────────────────
  // Logs sheet helper (local)
  // ───────────────────────────────
  function __logsAppend(route, event, data) {
  /*
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName('Logs');
    if (!sh) {
      sh = ss.insertSheet('Logs');
      sh.getRange(1,1,1,6).setValues([['Timestamp','ReqId','Route','Event','Action','DataJSON']]);
      sh.setFrozenRows(1);
    }
    const stamp = Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd HH:mm:ss');
    sh.appendRow([stamp, '', String(route||''), String(event||''), 'APPEMERGENCY', JSON.stringify(data||{})]);
  } catch (_) {}
  */
  // logging disabled
}


  // Normalise to 447xxxxxxxxx
  function to447(n) {
    if (!n) return '';
    let s = String(n).replace(/[^\d+]/g,'');
    if (s.startsWith('+')) s = s.slice(1);
    if (s.startsWith('07') && s.length === 11) return '44' + s.slice(1);
    if (s.startsWith('447') && s.length === 12) return s;
    if (s.startsWith('44')  && s.length === 12) return s;
    return '';
  }
  // Local display helper for UK 07 format
  function to07(n) {
    const s = to447(n);
    if (!s) return '';
    return '0' + s.slice(2);
  }

  // Pull any start/end ISO to synthesize a time label if needed
  const startIso =
    alert.shift_start_iso || alert.startAtIso || (alert.shift && alert.shift.startAtIso) || '';
  const endIso =
    alert.shift_end_iso || alert.endAtIso || (alert.shift && alert.shift.endAtIso) || '';

  // Friendly mappings for template tokens
  function mapIssueTypeLabel(issue) {
    const r = String(issue || '').toUpperCase();
    if (r === 'CANNOT_ATTEND') return 'Cancelling Shift';
    if (r === 'LEAVE_EARLY')   return 'Needs to leave their current shift early';
    if (r === 'DNA')           return 'Did Not Arrive';
    return String(issue || '');
  }
  function stripHrs(t) { return String(t || '').trim().replace(/\s*hrs?$/i, ''); }
  function formatLeaveEarlyLabel(src) {
    const s = String(src || '').trim();
    if (!s) return '';
    const up = s.toUpperCase();
    if (up === 'NOW' || up === 'ASAP') return 'This candidate needs to leave immediately';
    const core = stripHrs(s);
    if (/^\d{1,2}:\d{2}$/.test(core)) return `This candidate needs to leave at ${core}hrs`;
    return stripHrs(s);
  }

  const rawIssue = alert.issue_type || (alert.issue && alert.issue.type) || 'CANCELLING SHIFT';
  const issueUpper = String(rawIssue).toUpperCase();
  const isLeaveEarly = issueUpper === 'LEAVE_EARLY';
  const isDNA = issueUpper === 'DNA';
  const friendlyIssue = mapIssueTypeLabel(rawIssue);

  // Build tokens from alert only
  const tokens = {
    // For DNA ONLY → Candidate_name must be the ABSENTEE (subject).
    // Otherwise keep existing behaviour (reporter).
    Candidate_name: (isDNA
      ? ((alert.subject_name && String(alert.subject_name).trim()) ||
         (alert.issue && alert.issue.subject_name) ||
         'Colleague')
      : ((alert.candidate_name && String(alert.candidate_name).trim()) ||
         (alert.candidateName && String(alert.candidateName).trim()) ||
         'Colleague')),
    Role:
      alert.job_title || (alert.shift && (alert.shift.job_title || alert.shift.jobTitle)) || '',
    Hospital:
      alert.hospital || (alert.shift && alert.shift.hospital) || '',
    Ward:
      alert.ward || (alert.shift && alert.shift.ward) || '',
    Date_label:
      alert.date_label ||
      (alert.shift_ymd && typeof _formatDateLabel === 'function' ? _formatDateLabel(alert.shift_ymd, tz) : '') ||
      (alert.shift && alert.shift.ymd && typeof _formatDateLabel === 'function' ? _formatDateLabel(alert.shift.ymd, tz) : '') ||
      '',
    Time_range_label:
      alert.time_range_label ||
      ((typeof _formatTimeRangeLabel === 'function' && startIso && endIso) ? _formatTimeRangeLabel(startIso, endIso, tz) : '') ||
      (alert.shift && alert.shift.time_range_label) ||
      '',
    Shift_type:
      alert.shift_type || (alert.shift && alert.shift.shift_type) || '',
    // ⬇️ IMPORTANT: pass friendly label so template shows human text without changing placeholders
    Issue_type:
      friendlyIssue,
    Eta_or_leave_time_label:
      isLeaveEarly
        ? (formatLeaveEarlyLabel(alert.eta_or_leave_time_label || (alert.issue && (alert.issue.eta_or_leave_time_label || alert.issue.eta_or_leave_time)) || '') || 'N/A')
        : (alert.eta_or_leave_time_label || (alert.issue && (alert.issue.eta_or_leave_time_label || alert.issue.eta_or_leave_time)) || 'N/A'),
    // Default Reason_text; will be overridden for DNA below
    Reason_text:
      alert.reason_text || (alert.issue && alert.issue.reason_text) || ''
  };

  // ── NEW (DNA): override Reason_text to carry the full reporter/absentee message
  if (isDNA) {
    const reporter =
      (alert.candidate_name && String(alert.candidate_name).trim()) ||
      (alert.candidateName && String(alert.candidateName).trim()) ||
      'A colleague';
    const absentee = tokens.Candidate_name || 'A worker';
    const ms = to07(alert.subject_msisdn || (alert.issue && alert.issue.subject_msisdn) || '');
    const mobilePart = ms ? ` (mobile: ${ms})` : '';
    tokens.Reason_text = `${reporter} has advised that ${absentee} has failed to arrive on shift${mobilePart}`;
  }

  // Log tokens for traceability
  try { if (alert.alert_id) _eaAppendLog_(alert.alert_id, 'APPEMERGENCY tokens: ' + JSON.stringify(tokens)); } catch (_) {}
  try { _log('WATI_APPEMERGENCY', 'tokens', tokens); } catch(_) {}

  // Validate/normalise receivers from ANY contact number field
  const valid = [];
  const invalid = [];
  (contacts || []).forEach(c => {
    const raw = (c && (c.mobile447 || c.whatsappNumber || c.msisdn || '')) || '';
    const n = to447(raw);
    if (n && /^447\d{9}$/.test(n)) valid.push(n); else invalid.push(raw);
  });

  // Preflight logging so you can see why it would be empty
  try {
    const diag = { valid_count: valid.length, invalid_count: invalid.length, sample_valid: valid.slice(0,3), sample_invalid: invalid.slice(0,3) };
    if (alert.alert_id) _eaAppendLog_(alert.alert_id, 'APPEMERGENCY recipients ' + JSON.stringify(diag));
    _log('WATI_APPEMERGENCY', 'recipients_diag', diag);
    __logsAppend('WATI_APPEMERGENCY','recipients_diag', Object.assign({ alert_id: alert.alert_id || '' }, diag));
  } catch(_) {}

  // IMPORTANT: include both template tokens and CTA dynamic param name "1"
  // {{1}} will be "<alert_id>~<msisdn447>" so our endpoint can recover both.
  const receivers = valid.map(n => ({
    whatsappNumber: n,
    customParams: [
      { name: 'Candidate_name', value: String(tokens.Candidate_name || '') },
      { name: 'Role', value: String(tokens.Role || '') },
      { name: 'Hospital', value: String(tokens.Hospital || '') },
      { name: 'Ward', value: String(tokens.Ward || '') },
      { name: 'Date_label', value: String(tokens.Date_label || '') },
      { name: 'Time_range_label', value: String(tokens.Time_range_label || '') },
      { name: 'Shift_type', value: String(tokens.Shift_type || '') },
      { name: 'Issue_type', value: String(tokens.Issue_type || '') }, // friendly text
      { name: 'Eta_or_leave_time_label', value: String(tokens.Eta_or_leave_time_label || '') },
      { name: 'Reason_text', value: String(tokens.Reason_text || '') },
      { name: 'alert_id', value: String(alert.alert_id || '') },
      // 👇 {{1}} carries BOTH alert id and the recipient's msisdn447
      { name: '1', value: String(alert.alert_id || '') + '~' + String(n) }
    ]
  }));

  // If nothing valid, return a synthetic “no receivers” result (and log)
  if (!receivers.length) {
    const out = { httpCode: 200, json: { result: false, error: 'No valid receivers after normalisation' } };
    try {
      if (alert.alert_id) _eaAppendLog_(alert.alert_id, 'APPEMERGENCY preflight: no valid receivers');
      _log('WATI_APPEMERGENCY', 'preflight_no_receivers', {});
      __logsAppend('WATI_APPEMERGENCY','preflight_no_receivers',{ alert_id: alert.alert_id || '', reason: 'no_valid_receivers' });
    } catch(_) {}
    return out;
  }

  // Helper: extract message/conversation IDs from provider result
  function _extractMsgConvPairs(json, numbers) {
    const pairs = [];
    try {
      const dataArr = (json && (json.data || json.messages || json.results)) || null;
      if (Array.isArray(dataArr) && dataArr.length) {
        for (let i = 0; i < dataArr.length; i++) {
          const it = dataArr[i] || {};
          const message_id =
            it.messageId || it.id || (it.message && it.message.id) || (it.ids && it.ids[0]) || '';
          const conversation_id =
            it.conversationId || it.conversation_id || (it.message && it.message.conversationId) || '';
          const msisdn =
            (it.whatsappNumber || it.to || it.phone || numbers && numbers[i]) || '';
          pairs.push({ whatsappNumber: String(msisdn||''), message_id: String(message_id||''), conversation_id: String(conversation_id||'') });
        }
        return pairs;
      }
      const idsArr = (json && (json.messageIds || json.ids)) || null;
      if (Array.isArray(idsArr) && idsArr.length) {
        for (let i = 0; i < idsArr.length; i++) {
          pairs.push({ whatsappNumber: numbers[i] || '', message_id: String(idsArr[i]||''), conversation_id: '' });
        }
        return pairs;
      }
    } catch(_) {}
    return pairs;
  }

  // Use your generic sender; allow an optional LEAVE_EARLY-specific template if configured.
  const templateName =
    (isLeaveEarly && cfg && cfg.TEMPLATE_NAME_LEAVEEARLY)
      ? cfg.TEMPLATE_NAME_LEAVEEARLY
      : ((cfg && cfg.TEMPLATE_NAME_EMERGENCY) ? cfg.TEMPLATE_NAME_EMERGENCY : 'appemergency');

  if (typeof _sendWATIBulkTemplate_Generic === 'function') {
    try { _log('WATI_APPEMERGENCY', 'preflight_send', { template: templateName, receivers_count: receivers.length, sample_first: receivers[0] }); } catch(_) {}
    const out = _sendWATIBulkTemplate_Generic(templateName, receivers, 'APPEMERGENCY', tz);

    try {
      const ids = (out && out.json && (out.json.data || out.json.messageIds || out.json.ids)) || [];
      if (alert.alert_id && ids && ids.length) _emergencyUpdateById(alert.alert_id, { wati_message_ids_json: JSON.stringify(ids) });

      // Record conversation/message IDs to Logs sheet (for correlation diagnostics)
      const msisdns = receivers.map(r => r.whatsappNumber);
      const pairs = _extractMsgConvPairs(out && out.json, msisdns);
      if (pairs && pairs.length) {
        __logsAppend('WATI_APPEMERGENCY','conversation_id_recorded',{
          alert_id: alert.alert_id || '',
          count: pairs.length,
          sample: pairs.slice(0,3)
        });
        try { _emergencyUpdateById(alert.alert_id, { wati_conversation_ids_json: JSON.stringify(pairs) }); } catch(_) {}
      } else {
        __logsAppend('WATI_APPEMERGENCY','conversation_id_recorded_empty',{
          alert_id: alert.alert_id || '',
          json_keys: Object.keys((out && out.json) || {}),
          raw_summary_present: !!(out && out.json)
        });
      }
    } catch (_) {}

    return out;
  }

  // Fallback direct API call
  const url = `https://eu-api.wati.io/${cfg.WATI_TENANT}/api/v2/sendTemplateMessages`;
  const payload = {
    template_namespace: cfg.TEMPLATE_NAMESPACE,
    template_name: templateName,
    broadcast_name: `Emergency_${Utilities.formatDate(new Date(), tz, 'yyyyMMdd_HHmmss')}`,
    receivers
  };
  try { _log('WATI_APPEMERGENCY', 'direct_api_request', { url, payload_summary: { template: payload.template_name, receivers: receivers.length } }); } catch(_) {}
  const resp = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: { Authorization: cfg.WATI_TOKEN },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  const httpCode = resp.getResponseCode();
  let json = null; try { json = JSON.parse(resp.getContentText() || ''); } catch(_) {}
  try { _log('WATI_APPEMERGENCY', 'direct_api_response', { httpCode, json_summary: (json && (json.result || json.error)) }); } catch(_) {}
  if (alert.alert_id) _emergencyUpdateById(alert.alert_id, { wati_message_ids_json: JSON.stringify((json && json.data) || []) });

  // Record conversation/message IDs to Logs sheet (for correlation diagnostics)
  try {
    const msisdns = receivers.map(r => r.whatsappNumber);
    const pairs = _extractMsgConvPairs(json, msisdns);
    if (pairs && pairs.length) {
      __logsAppend('WATI_APPEMERGENCY','conversation_id_recorded',{
        alert_id: alert.alert_id || '',
        count: pairs.length,
        sample: pairs.slice(0,3)
      });
      try { _emergencyUpdateById(alert.alert_id, { wati_conversation_ids_json: JSON.stringify(pairs) }); } catch(_) {}
    } else {
      __logsAppend('WATI_APPEMERGENCY','conversation_id_recorded_empty',{
        alert_id: alert.alert_id || '',
        json_keys: Object.keys(json || {}),
        raw_summary_present: !!json
      });
    }
  } catch(_) {}

  return { httpCode, json };
}



function _applyChangesToTilesCache(msisdn, changes) {
  try {
    const tilesObj = _tilesGet(msisdn);
    const headers = _headersGet();
    if (!tilesObj || !headers) return; // nothing to patch safely
    const arr = tilesObj.tiles.slice();
    const headersYmds = tilesObj.headers;
    const byYmd = Object.fromEntries(changes.map(c => [String(c.ymd), (c.code==null?"":String(c.code).toUpperCase())]));

    headersYmds.forEach((ymd, i) => {
      const code = byYmd[ymd];
      if (code == null) return;
      const H = headers[i] || { ymd, displayDay:"", displayDate:"" };
      if (arr[i] && arr[i].booked) return;

      const status = (code === "") ? "PENDING AVAILABILITY"
                  : (code === "N/A") ? "NOT AVAILABLE"
                  : (code === "LD") ? "LONG DAY"
                  : (code === "N") ? "NIGHT"
                  : (code === "LD/N") ? "LONG DAY/NIGHT"
                  : "PENDING AVAILABILITY";
      arr[i] = { ymd, displayDay:H.displayDay, displayDate:H.displayDate, booked:false, editable:true, status };
    });

    // Write patched tiles (legacy mirror only)
    _tilesPut(msisdn, arr, headersYmds);

    // ── Peers refresh:
    // a) If any booked day remains → recompute peers for the nearest upcoming booked anchor.
    // b) If no booked day remains → force-write tri-state 'none' immediately (avoid TTL staleness).
    try {
      const ymds = headersYmds.slice(0);
      const booked = [];
      for (let i = 0; i < arr.length; i++) {
        if (arr[i] && arr[i].booked) booked.push(ymds[i]);
      }

      if (booked.length) {
        const P = _P() || { TZ: 'Europe/London' };
        const todayYmd = Utilities.formatDate(new Date(), P.TZ || 'Europe/London', 'yyyy-MM-dd');
        booked.sort();
        let pick = null;
        for (const y of booked) { if (y >= todayYmd) { pick = y; break; } }
        if (!pick) pick = booked[0];

        const bTileIdx = ymds.indexOf(pick);
        const bTile = (bTileIdx >= 0) ? arr[bTileIdx] : null;
        if (bTile) {
          const cohortKey = {
            ymd: pick,
            hospital: bTile.hospital || '',
            ward: bTile.ward || '',
            shiftType: (function(s){
              const S = String((bTile.shiftInfo || s || '')).toUpperCase();
              if (S.includes('NIGHT') || S === 'N') return 'NIGHT';
              return 'LONG DAY';
            })('')
          };
          __getOrComputePeers(__to07(msisdn), cohortKey, {
            computeFn: _computePeersForUser,
            forceRecompute: true,
            forceWrite: true
          });
        }
      } else {
        // No in-window bookings remain → publish tri-state 'none' right now.
        const synthKey = {
          ymd: String(headersYmds && headersYmds[0] || ''),
          hospital: '',
          ward: '',
          shiftType: 'NONE'
        };
        __getOrComputePeers(__to07(msisdn), synthKey, {
          // no computeFn → __getOrComputePeers will persist 'none'
          forceRecompute: true,
          forceWrite: true
        });
      }
    } catch (_) { /* best-effort */ }
  } catch (_) {}
}

// ───────────────────────── Links phone index (rebuild) ─────────────────────────
function rebuildLinksPhoneIndex() {
  const PROP_KEY      = 'LINKS_PHONE_INDEX_V1';
  const LINKS_DOC_ID  = '1BSomZL0jRse5SGfTgADwswVmIjY4mCMfvDAfQxxIUA8'; // ← hard-coded sheet ID
  const SHEET_NAME    = 'Availability API Links';

  // local 'safe' 07 normaliser (uses your existing __to07Lite if present)
  function to07(n) {
    try { return (typeof __to07Lite === 'function') ? __to07Lite(n) : String(n || '').replace(/\s+/g,''); }
    catch(_) { return String(n || '').replace(/\s+/g,''); }
  }
  function nameKey(surname, first) {
    return String((surname || '') + ' ' + (first || ''))
      .trim().replace(/\s+/g,' ').toLowerCase();
  }

  const ss = SpreadsheetApp.openById(LINKS_DOC_ID);
  const sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) throw new Error('Sheet "Availability API Links" not found');

  const vals = sh.getDataRange().getValues();
  if (!vals || vals.length < 2) {
    PropertiesService.getScriptProperties().setProperty(PROP_KEY, JSON.stringify({ ts:new Date().toISOString(), map:{} }));
    return { ok:true, rows:0, kept:0 };
  }

  // header → columns
  const header = vals[0].map(String);
  const iSur = header.findIndex(h => /^surname$/i.test(h));
  const iFir = header.findIndex(h => /^first$/i.test(h));
  const iTel = header.findIndex(h => /^telephone$/i.test(h));
  if (iSur < 0 || iFir < 0 || iTel < 0) {
    throw new Error('Expected columns: Surname, First, Telephone');
  }

  const map = {};
  let kept = 0;
  for (let r = 1; r < vals.length; r++) {
    const row = vals[r];
    const sur = String(row[iSur] || '').trim();
    const fir = String(row[iFir] || '').trim();
    const tel = to07(row[iTel] || '');
    if (!sur || !fir) continue;
    const k = nameKey(sur, fir);
    const t07 = to07(tel);
    // keep only proper 07… numbers
    if (/^07\d{9}$/.test(t07)) {
      map[k] = t07;
      kept++;
    }
  }

  PropertiesService.getScriptProperties()
    .setProperty(PROP_KEY, JSON.stringify({ ts:new Date().toISOString(), map }));

  return { ok:true, rows: vals.length-1, kept };
}
function getPhoneFromLinksIndex(surname, first) {
  // Small local masker so logs never print full numbers
  function _mask07(n){
    try{
      const s = String(n || '').replace(/\s+/g,'');
      if (!/^0?\d{9,}$/.test(s)) return '';
      if (s.length < 7) return s;
      return s.slice(0,3) + '•••••' + s.slice(-4);
    } catch(_) { return ''; }
  }
  function _normKey(a, b){
    return String((a || '') + ' ' + (b || ''))
      .trim().replace(/\s+/g, ' ').toLowerCase();
  }

  const PROP_KEY = 'LINKS_PHONE_INDEX_V1';
  let raw = '', obj = null, map = {};
  const inputSurname = String(surname || '');
  const inputFirst   = String(first   || '');
  const keyPrimary   = _normKey(inputSurname, inputFirst);
  const keyAlt       = _normKey(inputFirst,   inputSurname);

  // ---- read property
  try {
    raw = PropertiesService.getScriptProperties().getProperty(PROP_KEY);
    if (!raw) {
      try { _log('links_index', 'no_property', { key: PROP_KEY }); } catch(_) {}
      return '';
    }
  } catch (e) {
    try { _log('links_index', 'prop_read_error', { message: String(e && e.message || e) }); } catch(_) {}
    return '';
  }

  // ---- parse json
  try {
    obj = JSON.parse(raw);
  } catch (e) {
    try { _log('links_index', 'json_parse_error', { sample: String(raw).slice(0, 120) }); } catch(_) {}
    return '';
  }

  map = (obj && obj.map) || {};
  const mapSize = (function(){ try { return Object.keys(map).length; } catch(_) { return 0; } })();

  // ---- primary exact match
  const valPrimaryRaw = map[keyPrimary] || '';
  const valPrimary07  = String(valPrimaryRaw || '').replace(/\s+/g,'');
  const validPrimary  = /^07\d{9}$/.test(valPrimary07);

  // ---- alt (reversed) ONLY for diagnostics
  const valAltRaw = map[keyAlt] || '';
  const valAlt07  = String(valAltRaw || '').replace(/\s+/g,'');
  const existsAlt = !!valAltRaw;

  // ---- log the attempt
  try {
    _log('links_index', 'lookup', {
      inputSurname,
      inputFirst,
      keyPrimary,
      keyAlt,
      mapSize,
      foundPrimary: !!valPrimaryRaw,
      validPrimary: validPrimary,
      primaryMasked: _mask07(valPrimary07),
      foundAlt: existsAlt,
      altMasked: _mask07(valAlt07),
      note: existsAlt && !valPrimaryRaw ? 'Name-order mismatch candidate present (not used).' : ''
    });
  } catch(_) {}

  // ---- return ONLY exact (primary) match if it’s a clean 07xxxxxxxxx
  if (validPrimary) return valPrimary07;

  // Additional failure reasons (logged once more when we miss)
  try {
    let reason = 'MISS_UNKNOWN';
    if (!keyPrimary) reason = 'EMPTY_KEY';
    else if (!valPrimaryRaw) reason = existsAlt ? 'NO_PRIMARY_BUT_ALT_PRESENT' : 'NO_PRIMARY';
    else if (!validPrimary) reason = 'PRIMARY_INVALID_FORMAT';
    _log('links_index', 'miss_detail', {
      keyPrimary,
      reason,
      primaryMasked: _mask07(valPrimary07)
    });
  } catch(_) {}

  return '';
}
function clearRotaBusyLock() {
  const sp = PropertiesService.getScriptProperties();

  // Prefer constants if they exist; fall back to legacy literals.
  const KEY_BUSY  = (typeof K_ROTABUSY !== 'undefined')       ? K_ROTABUSY       : 'ROTABUSY';
  const KEY_RUN   = (typeof K_ROTABUSY_RUN !== 'undefined')   ? K_ROTABUSY_RUN   :
                    (typeof K_ROTABUSY_RUNID !== 'undefined') ? K_ROTABUSY_RUNID : 'ROTABUSY_RUNID';
  const KEY_SINCE = (typeof K_ROTABUSY_SINCE !== 'undefined') ? K_ROTABUSY_SINCE : 'ROTABUSY_SINCE';

  // Optional: capture previous values for debugging/return
  const before = {
    busy:  sp.getProperty(KEY_BUSY)  || null,
    run:   sp.getProperty(KEY_RUN)   || null,
    since: sp.getProperty(KEY_SINCE) || null
  };

  sp.deleteProperty(KEY_BUSY);
  sp.deleteProperty(KEY_RUN);
  sp.deleteProperty(KEY_SINCE);

  const after = {
    busy:  sp.getProperty(KEY_BUSY)  || null,
    run:   sp.getProperty(KEY_RUN)   || null,
    since: sp.getProperty(KEY_SINCE) || null
  };

  return {
    ok: true,
    deleted: [KEY_BUSY, KEY_RUN, KEY_SINCE],
    before,
    after
  };
}
function deleteThatCohortProperty() {
  const key = 'EA_EXC_SHIFT::2025-09-28|2025-09-28T06:30:00.000Z|2025-09-28T19:00:00.000Z|ST RICHARDS HOSPITAL|TEST|RMN|LONG';
  PropertiesService.getScriptProperties().deleteProperty(key);
}
function setTestModeFalse() {
  const sp = PropertiesService.getScriptProperties();
  sp.setProperty('TEST_MODE', 'FALSE'); // store as string
  const now = sp.getProperty('TEST_MODE');
  Logger.log('TEST_MODE set to: %s', now);
  return now;
}

function updateEmergencyFromEmail() {
  const props = PropertiesService.getScriptProperties();
  props.setProperty('EMERGENCY_FROM_EMAIL', 'sussex@arthur-rai.co.uk');
  const now = props.getProperty('EMERGENCY_FROM_EMAIL');
  Logger.log('EMERGENCY_FROM_EMAIL set to: %s', now);
}

function __listOtherOutstandingAlerts(excludeAlertId) {
  try {
    const out = [];
    const idx = _eaIndexMap_(); // { sh, m }
    const sh  = idx.sh, m = idx.m;
    const last = sh.getLastRow();

    for (let r = 2; r <= last; r++) {
      const alert_id = String(sh.getRange(r, m.alert_id + 1).getValue() || '');
      if (!alert_id || alert_id === excludeAlertId) continue;

      const acknowledged = sh.getRange(r, m.acknowledged + 1).getValue();
      const status = String(sh.getRange(r, m.status + 1).getValue() || '').toUpperCase();

      // Skip anything already closed/acknowledged or explicitly ended
      const isAcked = (acknowledged === true || String(acknowledged).toLowerCase() === 'true'
                       || status === 'ACKED' || status === 'ACKED_BY_CALL');
      if (isAcked) continue;
      if (status === 'TIMEOUT' || status === 'CANCELLED') continue;

      const a = _emergencyFindById(alert_id);
      if (a) out.push(a);
    }
    return out;
  } catch (ex) {
    return [];
  }
}
function debugShowTestMode() {
  const props = PropertiesService.getScriptProperties();
  const raw = props.getProperty('TEST_MODE') || '';
  const normalized = String(raw).trim().toUpperCase() === 'TRUE';
  Logger.log('TEST_MODE raw="%s" normalized=%s', raw, normalized);
  return { raw, normalized };
}
function purgeEAExcScriptProperties() {
  const props = PropertiesService.getScriptProperties();
  const all   = props.getProperties();              // { key: value, ... }
  const keys  = Object.keys(all || {});
  const toDel = keys.filter(k => k && k.indexOf('EA_EXC') === 0);

  toDel.forEach(k => {
    try {
      props.deleteProperty(k);
      Logger.log('Deleted Script Property: %s', k);
    } catch (e) {
      Logger.log('Failed to delete %s: %s', k, (e && e.message) || e);
    }
  });

  Logger.log('Done. Deleted %s properties (of %s total).', toDel.length, keys.length);
  return { deletedCount: toDel.length, deletedKeys: toDel };
}
function updateNewUserFlags() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName('Availability API Links');
  if (!sh) throw new Error('Sheet "Availability API Links" not found.');

  // Ensure column J header is "NewUser"
  const header = String(sh.getRange(1, 10).getDisplayValue()).trim();
  if (header.toLowerCase() !== 'newuser') {
    throw new Error('Column J header is not "NewUser". Please check the sheet.');
  }

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return 0; // nothing to do

  // Read all values in column J from row 2 down
  const rng = sh.getRange(2, 10, lastRow - 1, 1);
  const values = rng.getValues();

  let changed = 0;
  for (let i = 0; i < values.length; i++) {
    const v = String(values[i][0]).trim();
    if (v.toUpperCase() === 'NO') {
      values[i][0] = 'ALERT';
      changed++;
    }
  }

  if (changed > 0) rng.setValues(values);
  return changed; // optional: returns how many cells were updated
}
/**
 * Lists all Script Properties to the Executions log.
 * View logs in: Apps Script editor → Executions → select run → Logs
 *
 * @param {boolean} maskSensitive  If true, masks values for keys that look secret. Default: true.
 * @param {number} truncateAt      If >0, long values are truncated for readability. Default: 500.
 */
function listScriptProperties(maskSensitive = true, truncateAt = 500) {
  const props = PropertiesService.getScriptProperties().getProperties(); // { key: value, ... }
  const keys = Object.keys(props).sort();

  if (keys.length === 0) {
    console.log('No Script Properties found.');
    return;
  }

  const secretKeyRx = /(secret|token|key|password|pass|auth|bearer|api|credential)/i;

  console.log(`=== Script Properties (${keys.length}) ===`);
  keys.forEach(k => {
    let v = String(props[k] ?? '');

    if (maskSensitive && secretKeyRx.test(k)) {
      v = v.length > 8 ? `${v.slice(0,3)}…${v.slice(-3)}` : '•••';
    } else if (truncateAt > 0 && v.length > truncateAt) {
      v = `${v.slice(0, truncateAt)}… [${v.length} chars]`;
    }

    // Log with both consoles to be safe across runtimes
    console.log(`${k} = ${v}`);
    Logger.log('%s = %s', k, v);
  });
}
function setEmergencyTemplateNameToGithub() {
  const key = 'EMERGENCY_TEMPLATE_NAME';
  const value = 'emergencyurlgithub';
  const props = PropertiesService.getScriptProperties();
  props.setProperty(key, value);
  Logger.log('Set %s = %s', key, props.getProperty(key));
}


/** Main entry: opens one dialog that can Add / Edit / Delete */
function spManageScriptProperties() {
  const html = HtmlService.createHtmlOutput(_spManagerHtml())
    .setWidth(520)
    .setHeight(420);
  SpreadsheetApp.getUi().showModalDialog(html, 'Script Properties Manager');
}

/* ------- Server helpers (PUBLIC names; callable from HTML) ------- */
function spListScriptProps() {
  const map = PropertiesService.getScriptProperties().getProperties();
  const keys = Object.keys(map).sort();
  return { keys, map };
}
function spSetScriptProp(key, value) {
  if (!key) throw new Error('Key is required');
  PropertiesService.getScriptProperties().setProperty(key, String(value ?? ''));
  return true;
}
function spDeleteScriptProp(key) {
  if (!key) throw new Error('Key is required');
  PropertiesService.getScriptProperties().deleteProperty(key);
  return true;
}

/* --------------------- UI (inline HTML string) --------------------- */
function _spManagerHtml() {
  return `
<!DOCTYPE html>
<html>
  <head>
    <base target="_top">
    <meta charset="utf-8">
    <style>
      body { font: 13px/1.5 Arial, sans-serif; padding: 14px; }
      h2 { font-size: 16px; margin: 0 0 10px; }
      fieldset { border: 1px solid #ddd; border-radius: 6px; padding: 10px 12px; margin-bottom: 12px; }
      legend { padding: 0 6px; color: #444; }
      label { display:block; margin: 8px 0 4px; }
      input[type="text"], select, textarea { width: 100%; box-sizing: border-box; padding: 6px; border: 1px solid #ccc; border-radius: 4px; font: inherit; }
      textarea { resize: vertical; min-height: 88px; }
      .row { margin: 8px 0; }
      .muted { color: #666; }
      .actions { display:flex; gap:10px; margin-top: 12px; }
      button { padding: 7px 12px; cursor: pointer; }
      .hidden { display:none; }
      .mode-row { display:flex; gap:16px; align-items:center; }
      .warn { color: #b00020; }
    </style>
  </head>
  <body>
    <h2>Script Properties Manager</h2>

    <fieldset>
      <legend>Mode</legend>
      <div class="mode-row">
        <label><input type="radio" name="mode" value="add" checked> Add</label>
        <label><input type="radio" name="mode" value="edit"> Edit</label>
        <label><input type="radio" name="mode" value="delete"> Delete</label>
      </div>
    </fieldset>

    <!-- ADD -->
    <div id="panel-add">
      <label for="addKey">Key (name)</label>
      <input id="addKey" type="text" placeholder="e.g. API_SHARED_TOKEN">
      <label for="addVal">Value</label>
      <textarea id="addVal" placeholder="Enter value…"></textarea>
      <div class="actions">
        <button id="btnAdd">Save</button>
        <button onclick="google.script.host.close()">Cancel</button>
      </div>
    </div>

    <!-- EDIT -->
    <div id="panel-edit" class="hidden">
      <label for="editKeySel">Choose a property</label>
      <select id="editKeySel"></select>

      <label for="editVal">Value</label>
      <textarea id="editVal" placeholder="Edit value…"></textarea>

      <div class="actions">
        <button id="btnEdit">Save</button>
        <button onclick="google.script.host.close()">Cancel</button>
      </div>
    </div>

    <!-- DELETE -->
    <div id="panel-delete" class="hidden">
      <label for="delKeySel">Choose a property to delete</label>
      <select id="delKeySel"></select>
      <div class="row warn muted">This action cannot be undone.</div>
      <div class="actions">
        <button id="btnDelete">Delete</button>
        <button onclick="google.script.host.close()">Cancel</button>
      </div>
    </div>

    <div id="status" class="row muted"></div>

    <script>
      let MAP = {}; // key -> value
      const $$ = (id) => document.getElementById(id);
      const statusEl = $$('status');

      function setStatus(msg) { statusEl.textContent = msg || ''; }
      function showPanel(which) {
        $$('panel-add').classList.toggle('hidden', which !== 'add');
        $$('panel-edit').classList.toggle('hidden', which !== 'edit');
        $$('panel-delete').classList.toggle('hidden', which !== 'delete');
      }

      // Load on open
      google.script.run
        .withSuccessHandler(function(data){
          MAP = data.map || {};
          const keys = data.keys || [];

          // Populate edit & delete dropdowns
          const editSel = $$('editKeySel');
          const delSel  = $$('delKeySel');
          [editSel, delSel].forEach(sel => { sel.innerHTML = ''; });

          if (!keys.length) {
            setStatus('No existing script properties found yet.');
            [editSel, delSel].forEach(sel => { sel.disabled = true; });
          } else {
            [editSel, delSel].forEach(sel => { sel.disabled = false; });
            keys.forEach(k => {
              const opt1 = document.createElement('option');
              opt1.value = k; opt1.textContent = k;
              editSel.appendChild(opt1);

              const opt2 = document.createElement('option');
              opt2.value = k; opt2.textContent = k;
              delSel.appendChild(opt2);
            });
            // Prefill edit value
            $$('editVal').value = MAP[editSel.value] ?? '';
            editSel.addEventListener('change', () => {
              $$('editVal').value = MAP[editSel.value] ?? '';
            });
          }
        })
        .withFailureHandler(function(err){ alert('Init error: ' + err); })
        .spListScriptProps();

      // Mode switching
      document.querySelectorAll('input[name="mode"]').forEach(r => {
        r.addEventListener('change', e => showPanel(e.target.value));
      });

      // ADD
      $$('btnAdd').addEventListener('click', function(){
        const key = String($$('addKey').value || '').trim();
        const val = $$('addVal').value;
        if (!key) { alert('Key is required.'); return; }

        // If exists, confirm overwrite
        if (Object.prototype.hasOwnProperty.call(MAP, key)) {
          if (!confirm('“' + key + '” already exists. Overwrite its value?')) return;
        }

        setStatus('Saving…');
        google.script.run.withSuccessHandler(function(){
          alert('Saved: ' + key);
          google.script.host.close();
        }).withFailureHandler(function(err){
          alert('Error: ' + (err && err.message ? err.message : err));
          setStatus('');
        }).spSetScriptProp(key, val);
      });

      // EDIT
      $$('btnEdit').addEventListener('click', function(){
        const key = $$('editKeySel').value;
        const val = $$('editVal').value;
        if (!key) { alert('No property selected.'); return; }
        if (!confirm('Save new value for “' + key + '”?')) return;

        setStatus('Saving…');
        google.script.run.withSuccessHandler(function(){
          alert('Saved: ' + key);
          google.script.host.close();
        }).withFailureHandler(function(err){
          alert('Error: ' + (err && err.message ? err.message : err));
          setStatus('');
        }).spSetScriptProp(key, val);
      });

      // DELETE
      $$('btnDelete').addEventListener('click', function(){
        const key = $$('delKeySel').value;
        if (!key) { alert('No property selected.'); return; }
        if (!confirm('Delete “' + key + '”? This cannot be undone.')) return;

        setStatus('Deleting…');
        google.script.run.withSuccessHandler(function(){
          alert('Deleted: ' + key);
          google.script.host.close();
        }).withFailureHandler(function(err){
          alert('Error: ' + (err && err.message ? err.message : err));
          setStatus('');
        }).spDeleteScriptProp(key);
      });
    </script>
  </body>
</html>`;
}
function testLinksSpreadsheetAccess() {
  const started = Date.now();
  const p = _P();
  const ss = _ssLinks();
  const sh = ss.getSheetByName(p.SH_LINKS);

  if (!sh) throw new Error('LINKS_SHEET_MISSING');

  const result = {
    ok: true,
    rows: sh.getLastRow(),
    columns: sh.getLastColumn(),
    elapsedMs: Date.now() - started
  };

  console.log(JSON.stringify(result));
  return result;
}
