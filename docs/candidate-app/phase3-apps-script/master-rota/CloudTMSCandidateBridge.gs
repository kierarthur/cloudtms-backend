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
var CTMS_P3_MASTER_STATE_PREFIX_ = 'CTMS_P3_ROTA_';
var CTMS_P3_MASTER_STATE_TTL_MS_ = 7 * 24 * 60 * 60 * 1000;
var CTMS_P3_MASTER_ULID_ALPHABET_ = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

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
  console.log(JSON.stringify({
    system: 'CANDIDATE_DAILY_PHASE3',
    event: String(event || 'UNKNOWN'),
    status: String(safe.status || ''),
    error_code: String(safe.error_code || ''),
    correlation_id: String(safe.correlation_id || ''),
    operation_id: String(safe.operation_id || ''),
    duration_ms: Number(safe.duration_ms || 0)
  }));
}

function ctmsP3_masterSignedPost_(path, body, idempotencyKey, correlationId) {
  if (!ctmsP3_masterIsEnabled_()) return { disabled: true };
  var started = Date.now();
  var json = JSON.stringify(body || {});
  var rawBody = ctmsP3_masterBytes_(json);
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
      candidate_source_hmac: sourceHmac,
      window_start: headers[0].ymd,
      days: days
    };
    var sourceHash = ctmsP3_masterSha_(JSON.stringify(itemFacts));
    items.push({
      candidate_source_hmac: sourceHmac,
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

function ctmsP3_masterState_(body) {
  var fingerprint = ctmsP3_masterSha_(JSON.stringify(body.items));
  var key = CTMS_P3_MASTER_STATE_PREFIX_ + fingerprint.slice(0, 48);
  var props = PropertiesService.getScriptProperties();
  var lock = LockService.getScriptLock();
  lock.waitLock(20000);
  try {
    var existing = null;
    try { existing = JSON.parse(props.getProperty(key) || 'null'); } catch (_) {}
    if (existing && existing.fingerprint === fingerprint
        && Date.now() - Number(existing.created_ms || 0) <= CTMS_P3_MASTER_STATE_TTL_MS_) {
      return existing;
    }
    var batchId = ctmsP3_masterUuid_();
    var state = {
      key: key,
      fingerprint: fingerprint,
      created_ms: Date.now(),
      batch_request_id: batchId,
      idempotency_key: 'rota.generation.' + batchId,
      correlation_id: ctmsP3_masterCorrelation_(),
      items: body.items
    };
    props.setProperty(key, JSON.stringify(state));
    return state;
  } finally {
    lock.releaseLock();
  }
}

function ctmsP3_masterPublishBatch_(items) {
  var state = ctmsP3_masterState_({ items: items });
  var result = ctmsP3_masterSignedPost_(
    CTMS_P3_MASTER_BASE_PATH_ + '/rota-generations',
    { batch_request_id: state.batch_request_id, items: state.items },
    state.idempotency_key,
    state.correlation_id
  );
  if (result && result.http_code >= 200 && result.http_code < 500 && !result.uncertain) {
    PropertiesService.getScriptProperties().deleteProperty(state.key);
  }
  return result;
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
    var items = ctmsP3_masterBuildGenerationItems_(payload && payload.runId);
    for (var offset = 0; offset < items.length; offset += 50) {
      ctmsP3_masterPublishBatch_(items.slice(offset, offset + 50));
    }
    ctmsP3_masterLog_('ROTA_GENERATION_MIRROR_COMPLETE', {
      status: 'LEGACY_UNCHANGED', operation_id: String(payload && payload.runId || '')
    });
  } catch (error) {
    ctmsP3_masterLog_('ROTA_GENERATION_FAIL_OPEN', {
      status: 'LEGACY_UNCHANGED', error_code: String(error && error.message || error)
    });
  }
}
