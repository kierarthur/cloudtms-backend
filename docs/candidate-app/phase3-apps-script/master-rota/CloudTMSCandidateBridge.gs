/**
 * CloudTMS Candidate Daily Phase 3 compatibility bridge (NEW MASTER ROTA).
 *
 * The existing Availability API publication remains the primary legacy action.
 * This bridge runs only after that action returns. Its failures are contained and
 * cannot replace, suppress or alter the legacy result.
 *
 * Binding invariant:
 *   CLOUDTMS_CANDIDATE_BRIDGE_ENABLED missing/false => no CloudTMS network
 *   call, no bridge retry, no bridge log, no bridge property mutation and no
 *   bridge-owned Sheet write.
 */

var CTMS_P3_MASTER_BASE_PATH_ = '/candidate-system/v1/google-availability';
var CTMS_P3_MASTER_INDEX_KEY_ = 'CTMS_P3_ROTA_PENDING_INDEX';
var CTMS_P3_MASTER_MANIFEST_PREFIX_ = 'CTMS_P3_ROTA_MANIFEST_';
var CTMS_P3_MASTER_BODY_PREFIX_ = 'CTMS_P3_ROTA_BODY_';
var CTMS_P3_MASTER_SCHEMA_ = 2;
var CTMS_P3_MASTER_ITEM_LIMIT_ = 50;
var CTMS_P3_MASTER_REQUEST_BYTES_ = 245760;
var CTMS_P3_MASTER_PROPERTY_VALUE_BYTES_ = 7000;
var CTMS_P3_MASTER_STORE_BYTES_ = 480000;
var CTMS_P3_MASTER_ULID_ALPHABET_ = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
var CTMS_P3_MASTER_UUID_RE_ = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
var CTMS_P3_MASTER_GLOBAL_CANDIDATE_KEY_RE_ = /^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$/;
var CTMS_P3_MASTER_SOURCE_HMAC_KEY_VERSION_ = 1;
var CTMS_P3_MASTER_TERMINAL_ITEM_ERRORS_ = {
  SOURCE_EVENT_CONFLICT: true,
  GENERATION_INCOMPLETE: true,
  IDENTITY_LINK_MISSING: true,
  IDENTITY_LINK_AMBIGUOUS: true,
  IDENTITY_LINK_CONFLICT: true,
  CANDIDATE_DAILY_NOT_READY: true
};

function ctmsP3_masterIsEnabled_() {
  var value = PropertiesService.getScriptProperties()
    .getProperty('CLOUDTMS_CANDIDATE_BRIDGE_ENABLED');
  return String(value || '').trim().toLowerCase() === 'true';
}

function ctmsP3_masterConfigurationStatus() {
  var props = PropertiesService.getScriptProperties();
  var names = [
    'CLOUDTMS_CANDIDATE_BRIDGE_ENABLED',
    'CLOUDTMS_CANDIDATE_BASE_URL',
    'CLOUDTMS_CANDIDATE_ENVIRONMENT',
    'CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID',
    'CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET',
    'CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET'
  ];
  var present = {};
  names.forEach(function (name) { present[name] = Boolean(props.getProperty(name)); });
  return {
    bridge_enabled: ctmsP3_masterIsEnabled_(),
    properties_present: present,
    values_exposed: false
  };
}

function ctmsP3_masterProperty_(name) {
  var value = PropertiesService.getScriptProperties().getProperty(name);
  if (!value) throw new Error('CTMS_CONFIGURATION_MISSING:' + name);
  return String(value);
}

function ctmsP3_masterEnvironment_() {
  var value = ctmsP3_masterProperty_('CLOUDTMS_CANDIDATE_ENVIRONMENT').trim().toUpperCase();
  if (value !== 'TEST' && value !== 'LIVE') throw new Error('CTMS_ENVIRONMENT_INVALID');
  return value;
}

function ctmsP3_masterBytes_(text) {
  return Utilities.newBlob(String(text), 'application/json; charset=utf-8').getBytes();
}

function ctmsP3_masterByteLength_(text) {
  return ctmsP3_masterBytes_(String(text)).length;
}

function ctmsP3_masterHex_(bytes) {
  return (bytes || []).map(function (value) {
    var unsigned = value < 0 ? value + 256 : value;
    return ('0' + unsigned.toString(16)).slice(-2);
  }).join('');
}

function ctmsP3_masterSha_(text) {
  return ctmsP3_masterHex_(Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    ctmsP3_masterBytes_(String(text))
  ));
}

function ctmsP3_masterHmac_(text, secret) {
  return ctmsP3_masterHex_(Utilities.computeHmacSha256Signature(
    ctmsP3_masterBytes_(String(text)),
    ctmsP3_masterBytes_(String(secret))
  ));
}

function ctmsP3_masterUuid_() {
  return String(Utilities.getUuid()).toLowerCase();
}

function ctmsP3_masterNonce_() {
  var bytes = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    ctmsP3_masterBytes_(Utilities.getUuid() + '|' + Date.now() + '|' + Math.random())
  );
  return Utilities.base64EncodeWebSafe(bytes).replace(/=+$/g, '').slice(0, 32);
}

function ctmsP3_masterUlidPart_(value, length) {
  var result = '';
  var current = value;
  while (result.length < length) {
    result = CTMS_P3_MASTER_ULID_ALPHABET_.charAt(current % 32) + result;
    current = Math.floor(current / 32);
  }
  return result.slice(-length);
}

function ctmsP3_masterCorrelation_() {
  var timePart = ctmsP3_masterUlidPart_(Date.now(), 10);
  var entropy = ctmsP3_masterSha_(Utilities.getUuid() + '|' + Math.random()).toUpperCase();
  var randomPart = '';
  for (var index = 0; index < 16; index++) {
    randomPart += CTMS_P3_MASTER_ULID_ALPHABET_.charAt(
      parseInt(entropy.substr(index * 2, 2), 16) % 32
    );
  }
  return timePart + randomPart;
}

function ctmsP3_masterLog_(event, facts) {
  if (!ctmsP3_masterIsEnabled_()) return;
  var safe = facts || {};
  var rejectionItems = Array.isArray(safe.rejection_items)
    ? safe.rejection_items.slice(0, CTMS_P3_MASTER_ITEM_LIMIT_).map(function (item) {
      return {
        index: Number(item && item.index),
        error_code: String(item && item.error_code || '')
      };
    }) : [];
  console.log(JSON.stringify({
    system: 'CANDIDATE_DAILY_PHASE3',
    event: String(event || 'UNKNOWN'),
    status: String(safe.status || ''),
    error_code: String(safe.error_code || ''),
    correlation_id: String(safe.correlation_id || ''),
    operation_id: String(safe.operation_id || ''),
    duration_ms: Number(safe.duration_ms || 0),
    rejection_items: rejectionItems
  }));
}

function ctmsP3_masterSignedPost_(path, body, idempotencyKey, correlationId) {
  if (!ctmsP3_masterIsEnabled_()) return { disabled: true };
  var started = Date.now();
  var json = JSON.stringify(body || {});
  var rawBody = ctmsP3_masterBytes_(json);
  if (rawBody.length > CTMS_P3_MASTER_REQUEST_BYTES_) {
    return { http_code: -1, json: null, uncertain: true, local_error: 'CTMS_REQUEST_TOO_LARGE' };
  }
  var bodyHash = ctmsP3_masterHex_(Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256, rawBody
  ));
  var timestamp = String(Math.floor(Date.now() / 1000));
  var nonce = ctmsP3_masterNonce_();
  var correlation = correlationId || ctmsP3_masterCorrelation_();
  var key = String(idempotencyKey || '');
  var canonical = 'CLOUDTMS-HMAC-V1\nPOST\n' + path + '\n\n'
    + timestamp + '\n' + nonce + '\n' + bodyHash + '\n' + key + '\n'
    + correlation + '\n\n' + json;
  var signature = ctmsP3_masterHmac_(
    canonical,
    ctmsP3_masterProperty_('CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET')
  );
  try {
    var response = UrlFetchApp.fetch(
      ctmsP3_masterProperty_('CLOUDTMS_CANDIDATE_BASE_URL').replace(/\/+$/, '') + path,
      {
        method: 'post',
        contentType: 'application/json; charset=utf-8',
        muteHttpExceptions: true,
        payload: Utilities.newBlob(rawBody, 'application/json; charset=utf-8'),
        headers: {
          'x-cloudtms-key-id': ctmsP3_masterProperty_('CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID'),
          'x-cloudtms-signature-version': 'v1',
          'x-cloudtms-timestamp': timestamp,
          'x-cloudtms-nonce': nonce,
          'x-cloudtms-content-sha256': bodyHash,
          'x-cloudtms-signature': signature,
          'x-correlation-id': correlation,
          'idempotency-key': key
        }
      }
    );
    var code = response.getResponseCode();
    var parsed = null;
    try { parsed = JSON.parse(response.getContentText() || 'null'); } catch (_) {}
    ctmsP3_masterLog_('SIGNED_POST', {
      status: String(code), correlation_id: correlation,
      operation_id: key, duration_ms: Date.now() - started
    });
    return {
      http_code: code,
      json: parsed,
      correlation_id: correlation,
      uncertain: code === 408 || code === 425 || code === 429 || code >= 500
    };
  } catch (error) {
    ctmsP3_masterLog_('SIGNED_POST_UNCERTAIN', {
      status: 'NETWORK_ERROR', error_code: 'NETWORK_ERROR',
      correlation_id: correlation, operation_id: key,
      duration_ms: Date.now() - started
    });
    return { http_code: -1, json: null, correlation_id: correlation, uncertain: true };
  }
}

function ctmsP3_masterSourceHmac_(publicId) {
  var value = String(publicId || '').trim();
  if (!value) throw new Error('CTMS_PUBLIC_ID_MISSING');
  var canonical = 'CLOUDTMS-CANDIDATE-SOURCE-V1\n'
    + ctmsP3_masterEnvironment_() + '\nGOOGLE_CREDENTIALLY_PUBLIC_ID\n'
    + value + '\n';
  return ctmsP3_masterHmac_(
    canonical,
    ctmsP3_masterProperty_('CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET')
  );
}

function ctmsP3_masterGlobalCandidateKey_(publicId) {
  if (typeof buildCandidateIdFromPublicId_ !== 'function') {
    throw new Error('CTMS_GLOBAL_CANDIDATE_KEY_AUTHORITY_MISSING');
  }
  var key = String(buildCandidateIdFromPublicId_(String(publicId || '').trim()) || '')
    .trim().toUpperCase();
  if (!CTMS_P3_MASTER_GLOBAL_CANDIDATE_KEY_RE_.test(key)) {
    throw new Error('CTMS_GLOBAL_CANDIDATE_KEY_INVALID');
  }
  return key;
}

function ctmsP3_masterDate_(value, timezone) {
  if (value instanceof Date && !isNaN(value.getTime())) {
    return Utilities.formatDate(value, timezone, 'yyyy-MM-dd');
  }
  var text = String(value || '').trim();
  var match = text.match(/\b(\d{2})\/(\d{2})\/(\d{4})\b/);
  if (!match) return '';
  return match[3] + '-' + match[2] + '-' + match[1];
}

function ctmsP3_masterIso_(date) {
  return date instanceof Date && !isNaN(date.getTime()) ? date.toISOString() : '';
}

function ctmsP3_masterHeaders_(sheet, timezone) {
  var lastColumn = sheet.getLastColumn();
  if (lastColumn < 20) throw new Error('CTMS_AVAILABILITY_WINDOW_INCOMPLETE');
  var values = sheet.getRange(1, 7, 1, lastColumn - 6).getDisplayValues()[0];
  var headers = [];
  for (var index = 0; index < values.length; index++) {
    var ymd = ctmsP3_masterDate_(values[index], timezone);
    if (!ymd) continue;
    headers.push({ column: 7 + index, ymd: ymd, display_date: String(values[index] || '') });
  }
  if (headers.length !== 14) throw new Error('CTMS_AVAILABILITY_WINDOW_NOT_14_DAYS');
  return headers;
}

function ctmsP3_masterBookings_(spreadsheet, timezone) {
  var sheet = spreadsheet.getSheetByName('EmailHistory');
  var result = {};
  if (!sheet || sheet.getLastRow() < 2) return result;
  var rows = sheet.getDataRange().getDisplayValues();
  var headers = rows[0].map(function (value) { return String(value || '').trim().toLowerCase(); });
  function column(name, fallback) {
    var index = headers.indexOf(name);
    return index >= 0 ? index : fallback;
  }
  var occupantColumn = column('occupantkey', 0);
  var dateColumn = column('date', 1);
  var shiftColumn = column('shift', 2);
  var referenceColumn = column('booking reference', 3);
  var hospitalColumn = column('hospital', 5);
  var wardColumn = column('ward', 6);
  var jobColumn = column('job title', 7);
  var notesColumn = column('notes', 9);
  for (var rowIndex = 1; rowIndex < rows.length; rowIndex++) {
    var occupant = String(rows[rowIndex][occupantColumn] || '').toLowerCase().trim();
    var ymd = ctmsP3_masterDate_(rows[rowIndex][dateColumn], timezone);
    if (!occupant || !ymd) continue;
    var key = occupant + '|' + ymd;
    if (result[key]) continue;
    result[key] = {
      raw_date: String(rows[rowIndex][dateColumn] || ''),
      shift: String(rows[rowIndex][shiftColumn] || '').trim(),
      reference: String(rows[rowIndex][referenceColumn] || '').trim(),
      hospital: String(rows[rowIndex][hospitalColumn] || '').trim(),
      ward: String(rows[rowIndex][wardColumn] || '').trim(),
      job_title: String(rows[rowIndex][jobColumn] || '').trim(),
      notes: String(rows[rowIndex][notesColumn] || '').trim()
    };
  }
  return result;
}

function ctmsP3_masterShift_(booking, timezone) {
  var window = getShiftWindow(booking.raw_date, booking.shift, booking.notes, timezone);
  var start = ctmsP3_masterIso_(window && window.start);
  var end = ctmsP3_masterIso_(window && window.end);
  if (!start || !end || Date.parse(end) <= Date.parse(start) || Date.parse(start) <= 0) {
    throw new Error('CTMS_BOOKED_SHIFT_TIME_UNRESOLVED');
  }
  return { start: start, end: end };
}

function ctmsP3_masterBuildGenerationItems_(runId) {
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  var timezone = spreadsheet.getSpreadsheetTimeZone();
  var candidatesSheet = spreadsheet.getSheetByName('Candidate List');
  var availabilitySheet = spreadsheet.getSheetByName(
    typeof SH_AVAIL === 'string' ? SH_AVAIL : 'Availability'
  );
  if (!candidatesSheet || !availabilitySheet) throw new Error('CTMS_MASTER_SHEETS_MISSING');
  var candidateRows = candidatesSheet.getDataRange().getDisplayValues();
  var candidateHeaders = candidateRows[0].map(function (value) {
    return String(value || '').trim().toLowerCase();
  });
  var publicIdColumn = candidateHeaders.indexOf('public id - credentially');
  if (publicIdColumn < 0) throw new Error('CTMS_PUBLIC_ID_COLUMN_MISSING');
  var availabilityRows = availabilitySheet.getDataRange().getDisplayValues();
  var availabilityBackgrounds = availabilitySheet.getDataRange().getBackgrounds();
  var headers = ctmsP3_masterHeaders_(availabilitySheet, timezone);
  var bookings = ctmsP3_masterBookings_(spreadsheet, timezone);
  var availabilityByTelephone = {};
  for (var avIndex = 1; avIndex < availabilityRows.length; avIndex++) {
    var telephone = String(availabilityRows[avIndex][3] || '').replace(/\D/g, '');
    if (telephone) availabilityByTelephone[telephone] = avIndex;
  }
  var sourceEventTime = new Date().toISOString();
  var safeRun = String(runId || '').replace(/[^A-Za-z0-9._~-]/g, '').slice(0, 80);
  if (safeRun.length < 8) safeRun = ctmsP3_masterSha_(sourceEventTime).slice(0, 16);
  var items = [];
  for (var candidateIndex = 1; candidateIndex < candidateRows.length; candidateIndex++) {
    var publicId = String(candidateRows[candidateIndex][publicIdColumn] || '').trim();
    var telephoneDigits = String(candidateRows[candidateIndex][3] || '').replace(/\D/g, '');
    var avRowIndex = availabilityByTelephone[telephoneDigits];
    if (!publicId || avRowIndex == null) continue;
    var candidateGlobalKey = ctmsP3_masterGlobalCandidateKey_(publicId);
    var sourceHmac = ctmsP3_masterSourceHmac_(publicId);
    var occupant = (String(candidateRows[candidateIndex][0] || '') + ' '
      + String(candidateRows[candidateIndex][1] || '')).toLowerCase().trim();
    var days = headers.map(function (header) {
      var cellValue = String(availabilityRows[avRowIndex][header.column - 1] || '').trim();
      var background = String(availabilityBackgrounds[avRowIndex][header.column - 1] || '').toLowerCase();
      var booking = bookings[occupant + '|' + header.ymd];
      var day = {
        date: header.ymd,
        booked: Boolean(booking),
        system_blocked: cellValue.toUpperCase() === 'BLOCKED'
          || (typeof COLOR_BLOCKED === 'string' && background === String(COLOR_BLOCKED).toLowerCase())
      };
      if (booking) {
        var shift = ctmsP3_masterShift_(booking, timezone);
        day.booking_id = booking.reference || ('legacy-' + ctmsP3_masterSha_(
          occupant + '|' + header.ymd + '|' + booking.shift + '|' + booking.notes
        ).slice(0, 32));
        day.shift_starts_at = shift.start;
        day.shift_ends_at = shift.end;
        day.shift_info = booking.shift;
        day.hospital = booking.hospital;
        day.ward = booking.ward;
        day.job_title = booking.job_title;
      }
      var hashFacts = Object.assign({}, day);
      day.source_row_hash = ctmsP3_masterSha_(JSON.stringify(hashFacts));
      return day;
    });
    var itemFacts = {
      candidate_global_key: candidateGlobalKey,
      candidate_source_hmac: sourceHmac,
      source_hmac_key_version: CTMS_P3_MASTER_SOURCE_HMAC_KEY_VERSION_,
      window_start: headers[0].ymd,
      days: days
    };
    var sourceHash = ctmsP3_masterSha_(JSON.stringify(itemFacts));
    items.push({
      candidate_global_key: candidateGlobalKey,
      candidate_source_hmac: sourceHmac,
      source_hmac_key_version: CTMS_P3_MASTER_SOURCE_HMAC_KEY_VERSION_,
      source_event_id: 'master-rota.' + safeRun,
      source_revision: 'phase3.' + sourceHash,
      source_hash: sourceHash,
      window_start: headers[0].ymd,
      days: days,
      source_event_time: sourceEventTime,
      item_key: 'rota.' + sourceHmac.slice(0, 24) + '.' + sourceHash.slice(0, 24)
    });
  }
  return items;
}

function ctmsP3_masterSplitText_(text) {
  var pieces = [];
  var offset = 0;
  while (offset < text.length) {
    var low = offset + 1;
    var high = Math.min(text.length, offset + CTMS_P3_MASTER_PROPERTY_VALUE_BYTES_);
    var best = offset;
    while (low <= high) {
      var middle = Math.floor((low + high) / 2);
      if (ctmsP3_masterByteLength_(text.slice(offset, middle))
          <= CTMS_P3_MASTER_PROPERTY_VALUE_BYTES_) {
        best = middle;
        low = middle + 1;
      } else {
        high = middle - 1;
      }
    }
    if (best <= offset) throw new Error('CTMS_PROPERTY_CHUNK_UNSPLITTABLE');
    pieces.push(text.slice(offset, best));
    offset = best;
  }
  if (!pieces.length) pieces.push('');
  return pieces;
}

function ctmsP3_masterAllProperties_() {
  var service = PropertiesService.getScriptProperties();
  return typeof service.getProperties === 'function' ? service.getProperties() : {};
}

function ctmsP3_masterStoredBytes_(values) {
  var total = 0;
  Object.keys(values || {}).forEach(function (key) {
    total += ctmsP3_masterByteLength_(key) + ctmsP3_masterByteLength_(values[key]);
  });
  return total;
}

function ctmsP3_masterPendingIndex_() {
  var raw = PropertiesService.getScriptProperties().getProperty(CTMS_P3_MASTER_INDEX_KEY_);
  if (!raw) return null;
  var parsed;
  try { parsed = JSON.parse(raw); } catch (_) { throw new Error('CTMS_PENDING_INDEX_CORRUPT'); }
  if (!parsed || parsed.schema !== CTMS_P3_MASTER_SCHEMA_
      || !/^[a-f0-9]{64}$/.test(String(parsed.event_fingerprint || ''))
      || !Array.isArray(parsed.manifest_keys) || !parsed.manifest_keys.length
      || parsed.manifest_keys.some(function (key) {
        return !String(key || '').startsWith(CTMS_P3_MASTER_MANIFEST_PREFIX_);
      })) throw new Error('CTMS_PENDING_INDEX_CORRUPT');
  return parsed;
}

function ctmsP3_masterStateFromManifest_(manifestKey) {
  var props = PropertiesService.getScriptProperties();
  var manifest;
  try { manifest = JSON.parse(props.getProperty(manifestKey) || 'null'); } catch (_) {}
  if (!manifest || manifest.schema !== CTMS_P3_MASTER_SCHEMA_
      || manifest.manifest_key !== manifestKey
      || typeof manifest.body_prefix !== 'string'
      || !Number.isInteger(manifest.body_chunk_count) || manifest.body_chunk_count < 1
      || !/^[a-f0-9]{64}$/.test(String(manifest.body_sha256 || ''))
      || typeof manifest.idempotency_key !== 'string'
      || typeof manifest.correlation_id !== 'string') {
    throw new Error('CTMS_PENDING_MANIFEST_CORRUPT');
  }
  var bodyText = Array.from({ length: manifest.body_chunk_count }, function (_, index) {
    var key = manifest.body_prefix + String(index + 1);
    var value = props.getProperty(key);
    if (value == null) throw new Error('CTMS_PENDING_BODY_MISSING');
    return value;
  }).join('');
  if (ctmsP3_masterByteLength_(bodyText) !== Number(manifest.body_bytes)
      || ctmsP3_masterSha_(bodyText) !== manifest.body_sha256) {
    throw new Error('CTMS_PENDING_BODY_CORRUPT');
  }
  var body;
  try { body = JSON.parse(bodyText); } catch (_) { throw new Error('CTMS_PENDING_BODY_CORRUPT'); }
  if (!body || body.batch_request_id !== manifest.batch_request_id
      || !Array.isArray(body.items) || !body.items.length
      || body.items.length > CTMS_P3_MASTER_ITEM_LIMIT_
      || ctmsP3_masterByteLength_(bodyText) > CTMS_P3_MASTER_REQUEST_BYTES_) {
    throw new Error('CTMS_PENDING_BODY_INVALID');
  }
  return { manifest: manifest, body: body, body_text: bodyText };
}

function ctmsP3_masterDeleteState_(state) {
  var props = PropertiesService.getScriptProperties();
  for (var index = 1; index <= state.manifest.body_chunk_count; index++) {
    props.deleteProperty(state.manifest.body_prefix + String(index));
  }
  props.deleteProperty(state.manifest.manifest_key);
}

function ctmsP3_masterClearAllPending_(index) {
  var active = index || ctmsP3_masterPendingIndex_();
  var props = PropertiesService.getScriptProperties();
  props.deleteProperty(CTMS_P3_MASTER_INDEX_KEY_);
  if (active) {
    active.manifest_keys.forEach(function (key) {
      try { ctmsP3_masterDeleteState_(ctmsP3_masterStateFromManifest_(key)); }
      catch (_) { props.deleteProperty(key); }
    });
  }
}

function ctmsP3_masterCleanupOrphans_() {
  var props = PropertiesService.getScriptProperties();
  if (props.getProperty(CTMS_P3_MASTER_INDEX_KEY_)) return;
  var all = ctmsP3_masterAllProperties_();
  Object.keys(all).forEach(function (key) {
    if (key.startsWith(CTMS_P3_MASTER_MANIFEST_PREFIX_)
        || key.startsWith(CTMS_P3_MASTER_BODY_PREFIX_)) props.deleteProperty(key);
  });
}

function ctmsP3_masterPersistEvent_(items) {
  if (!Array.isArray(items) || !items.length) return null;
  var groups = [];
  var current = [];
  items.forEach(function (item) {
    var candidate = current.concat([item]);
    var probe = JSON.stringify({
      batch_request_id: '00000000-0000-4000-8000-000000000000',
      items: candidate
    });
    if (candidate.length > CTMS_P3_MASTER_ITEM_LIMIT_
        || ctmsP3_masterByteLength_(probe) > CTMS_P3_MASTER_REQUEST_BYTES_) {
      if (!current.length) throw new Error('CTMS_ROTA_ITEM_EXCEEDS_ROUTE_LIMIT');
      groups.push(current);
      current = [item];
    } else {
      current = candidate;
    }
  });
  if (current.length) groups.push(current);

  var eventFingerprint = ctmsP3_masterSha_(JSON.stringify(items));
  var pendingWrites = {};
  var manifests = [];
  groups.forEach(function (group, groupIndex) {
    var batchId = ctmsP3_masterUuid_();
    var bodyText = JSON.stringify({ batch_request_id: batchId, items: group });
    if (ctmsP3_masterByteLength_(bodyText) > CTMS_P3_MASTER_REQUEST_BYTES_) {
      throw new Error('CTMS_ROTA_BATCH_EXCEEDS_ROUTE_LIMIT');
    }
    var stateId = eventFingerprint.slice(0, 32) + '_' + String(groupIndex + 1);
    var manifestKey = CTMS_P3_MASTER_MANIFEST_PREFIX_ + stateId;
    var bodyPrefix = CTMS_P3_MASTER_BODY_PREFIX_ + stateId + '_';
    var bodyChunkCount = 0;
    ctmsP3_masterSplitText_(bodyText).forEach(function (piece, pieceIndex) {
      var bodyKey = bodyPrefix + String(pieceIndex + 1);
      bodyChunkCount += 1;
      pendingWrites[bodyKey] = piece;
    });
    var manifest = {
      schema: CTMS_P3_MASTER_SCHEMA_,
      manifest_key: manifestKey,
      event_fingerprint: eventFingerprint,
      batch_request_id: batchId,
      idempotency_key: 'rota.generation.' + batchId,
      correlation_id: ctmsP3_masterCorrelation_(),
      body_sha256: ctmsP3_masterSha_(bodyText),
      body_bytes: ctmsP3_masterByteLength_(bodyText),
      body_prefix: bodyPrefix,
      body_chunk_count: bodyChunkCount
    };
    var manifestText = JSON.stringify(manifest);
    if (ctmsP3_masterByteLength_(manifestText) > CTMS_P3_MASTER_PROPERTY_VALUE_BYTES_) {
      throw new Error('CTMS_ROTA_MANIFEST_TOO_LARGE');
    }
    pendingWrites[manifestKey] = manifestText;
    manifests.push(manifestKey);
  });
  var index = {
    schema: CTMS_P3_MASTER_SCHEMA_,
    event_fingerprint: eventFingerprint,
    created_ms: Date.now(),
    manifest_keys: manifests
  };
  var indexText = JSON.stringify(index);
  if (ctmsP3_masterByteLength_(indexText) > CTMS_P3_MASTER_PROPERTY_VALUE_BYTES_) {
    throw new Error('CTMS_ROTA_INDEX_TOO_LARGE');
  }

  var props = PropertiesService.getScriptProperties();
  var lock = LockService.getScriptLock();
  lock.waitLock(20000);
  var written = [];
  try {
    if (props.getProperty(CTMS_P3_MASTER_INDEX_KEY_)) {
      throw new Error('CTMS_PENDING_EVENT_ALREADY_EXISTS');
    }
    var all = ctmsP3_masterAllProperties_();
    var projected = {};
    Object.keys(all).forEach(function (key) { projected[key] = all[key]; });
    Object.keys(pendingWrites).forEach(function (key) { projected[key] = pendingWrites[key]; });
    projected[CTMS_P3_MASTER_INDEX_KEY_] = indexText;
    if (ctmsP3_masterStoredBytes_(projected) > CTMS_P3_MASTER_STORE_BYTES_) {
      throw new Error('CTMS_ROTA_PROPERTY_STORE_CAPACITY');
    }
    Object.keys(pendingWrites).forEach(function (key) {
      props.setProperty(key, pendingWrites[key]);
      written.push(key);
    });
    props.setProperty(CTMS_P3_MASTER_INDEX_KEY_, indexText);
    return index;
  } catch (error) {
    written.forEach(function (key) { props.deleteProperty(key); });
    throw error;
  } finally {
    lock.releaseLock();
  }
}

function ctmsP3_masterGenerationOutcomeAuthority_(result, expectedItemCount) {
  var invalid = { valid: false, rejection_items: [] };
  if (!result || !result.json || result.json.ok !== true
      || !Number.isInteger(expectedItemCount) || expectedItemCount < 1
      || expectedItemCount > CTMS_P3_MASTER_ITEM_LIMIT_) return invalid;
  var body = result.json.result;
  if (!body || typeof body !== 'object' || Array.isArray(body)
      || !CTMS_P3_MASTER_UUID_RE_.test(String(body.batch_receipt_id || ''))
      || !Array.isArray(body.outcomes) || body.outcomes.length !== expectedItemCount) return invalid;
  var seen = {};
  var rejectionItems = [];
  for (var position = 0; position < body.outcomes.length; position++) {
    var outcome = body.outcomes[position];
    if (!outcome || typeof outcome !== 'object' || Array.isArray(outcome)
        || !Number.isInteger(outcome.index) || outcome.index < 0
        || outcome.index >= expectedItemCount || seen[String(outcome.index)]) return invalid;
    seen[String(outcome.index)] = true;
    var status = String(outcome.status || '');
    if (status === 'COMMITTED' || status === 'REPLAYED') continue;
    if (status !== 'REJECTED') return invalid;
    var errorCode = String(outcome.error_code || '');
    if (!CTMS_P3_MASTER_TERMINAL_ITEM_ERRORS_[errorCode]) return invalid;
    rejectionItems.push({ index: outcome.index, error_code: errorCode });
  }
  for (var expectedIndex = 0; expectedIndex < expectedItemCount; expectedIndex++) {
    if (!seen[String(expectedIndex)]) return invalid;
  }
  return { valid: true, rejection_items: rejectionItems };
}

function ctmsP3_masterContractDisposition_(result, expectedItemCount) {
  if (result && result.http_code >= 200 && result.http_code < 300
      && result.json && result.json.ok === true) {
    var authority = ctmsP3_masterGenerationOutcomeAuthority_(result, expectedItemCount);
    if (!authority.valid) return 'PRESERVE';
    return authority.rejection_items.length ? 'TERMINAL_REJECTION' : 'SUCCESS';
  }
  if (!result || result.uncertain === true) return 'PRESERVE';
  var body = result.json;
  if (!body || body.ok !== false || typeof body.error_code !== 'string'
      || typeof body.retry_class !== 'string') return 'PRESERVE';
  var triple = String(result.http_code) + ':' + body.error_code + ':' + body.retry_class;
  if (triple === '409:SOURCE_EVENT_CONFLICT:DO_NOT_RETRY'
      || triple === '409:IDENTITY_LINK_CONFLICT:DO_NOT_RETRY'
      || triple === '422:GENERATION_INCOMPLETE:DO_NOT_RETRY') return 'TERMINAL_REJECTION';
  if (triple === '409:BATCH_IN_PROGRESS:STATUS_CHECK') return 'PRESERVE';
  return 'PRESERVE';
}

function ctmsP3_masterPublishState_(state) {
  var result = ctmsP3_masterSignedPost_(
    CTMS_P3_MASTER_BASE_PATH_ + '/rota-generations',
    state.body,
    state.manifest.idempotency_key,
    state.manifest.correlation_id
  );
  return {
    result: result,
    disposition: ctmsP3_masterContractDisposition_(result, state.body.items.length)
  };
}

function ctmsP3_masterRecoverPending_() {
  var index = ctmsP3_masterPendingIndex_();
  if (!index) return { had_pending: false, resolved: true };
  while (index.manifest_keys.length) {
    var state = ctmsP3_masterStateFromManifest_(index.manifest_keys[0]);
    var published = ctmsP3_masterPublishState_(state);
    if (published.disposition === 'PRESERVE') {
      return { had_pending: true, resolved: false, result: published.result };
    }
    if (published.disposition === 'TERMINAL_REJECTION') {
      var rejectionAuthority = ctmsP3_masterGenerationOutcomeAuthority_(
        published.result, state.body.items.length
      );
      var itemRejections = rejectionAuthority.valid ? rejectionAuthority.rejection_items : [];
      ctmsP3_masterLog_('ROTA_GENERATION_TERMINAL_REJECTION', {
        status: String(published.result.http_code),
        error_code: itemRejections.length ? 'GENERATION_ITEM_REJECTED'
          : String(published.result.json && published.result.json.error_code || ''),
        correlation_id: state.manifest.correlation_id,
        operation_id: state.manifest.idempotency_key,
        rejection_items: itemRejections
      });
      ctmsP3_masterClearAllPending_(index);
      return { had_pending: true, resolved: true, terminal_rejection: true };
    }
    index.manifest_keys.shift();
    if (index.manifest_keys.length) {
      PropertiesService.getScriptProperties().setProperty(
        CTMS_P3_MASTER_INDEX_KEY_, JSON.stringify(index)
      );
    } else {
      PropertiesService.getScriptProperties().deleteProperty(CTMS_P3_MASTER_INDEX_KEY_);
    }
    ctmsP3_masterDeleteState_(state);
    if (!index.manifest_keys.length) {
      ctmsP3_masterLog_('ROTA_GENERATION_MIRROR_COMPLETE', {
        status: 'LEGACY_UNCHANGED', operation_id: index.event_fingerprint.slice(0, 24)
      });
    }
  }
  return { had_pending: true, resolved: true };
}

function ctmsP3_masterPublishBatch_(items) {
  var existing = ctmsP3_masterRecoverPending_();
  if (existing.had_pending) return existing;
  var index = ctmsP3_masterPersistEvent_(items);
  return index ? ctmsP3_masterRecoverPending_() : { had_pending: false, resolved: true };
}

function ctmsP3_masterMirrorLegacyEvent_(action, payload, legacyResult) {
  if (!ctmsP3_masterIsEnabled_()) return;
  if (String(action || '') !== 'AVAILABILITY_UPDATE_END') return;
  if (!legacyResult || Number(legacyResult.httpCode) < 200
      || Number(legacyResult.httpCode) >= 300) {
    ctmsP3_masterLog_('ROTA_GENERATION_NOT_MIRRORED', {
      status: 'LEGACY_PUBLICATION_NOT_ACCEPTED',
      error_code: 'LEGACY_PUBLICATION_NOT_ACCEPTED'
    });
    return;
  }
  try {
    ctmsP3_masterCleanupOrphans_();
    var recovered = ctmsP3_masterRecoverPending_();
    if (recovered.had_pending) return;
    var items = ctmsP3_masterBuildGenerationItems_(payload && payload.runId);
    if (!items.length) return;
    ctmsP3_masterPersistEvent_(items);
    ctmsP3_masterRecoverPending_();
  } catch (error) {
    ctmsP3_masterLog_('ROTA_GENERATION_FAIL_OPEN', {
      status: 'LEGACY_UNCHANGED', error_code: String(error && error.message || error)
    });
  }
}
