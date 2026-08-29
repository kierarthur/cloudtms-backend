/**
 * CloudTMS Candidate Daily Phase 3 compatibility bridge (Availability API).
 *
 * This file is additive. It does not replace the legacy browser, login,
 * msisdn lookup, Availability Sheet, emergency or specialist services.
 *
 * Binding invariant:
 *   CLOUDTMS_CANDIDATE_BRIDGE_ENABLED missing/false => no CloudTMS network
 *   call, no bridge retry, no bridge log, no bridge property mutation and no
 *   bridge-owned Sheet write.
 *
 * Required Script Properties are named in SCRIPT_PROPERTIES.md. Secret values
 * must be installed in Apps Script and must never be placed in source control.
 */

var CTMS_P3_BASE_PATH_ = '/candidate-system/v1/google-availability';
var CTMS_P3_STATE_PREFIX_ = 'CTMS_P3_OP_';
var CTMS_P3_STATE_TTL_MS_ = 7 * 24 * 60 * 60 * 1000;
var CTMS_P3_ULID_ALPHABET_ = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

function ctmsP3_isEnabled_() {
  var value = PropertiesService.getScriptProperties()
    .getProperty('CLOUDTMS_CANDIDATE_BRIDGE_ENABLED');
  return String(value || '').trim().toLowerCase() === 'true';
}

function ctmsP3_configurationStatus() {
  var props = PropertiesService.getScriptProperties();
  var names = [
    'CLOUDTMS_CANDIDATE_BRIDGE_ENABLED',
    'CLOUDTMS_CANDIDATE_BASE_URL',
    'CLOUDTMS_CANDIDATE_ENVIRONMENT',
    'CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID',
    'CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET',
    'CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET',
    'CLOUDTMS_CANDIDATE_EXECUTOR_ID'
  ];
  var present = {};
  names.forEach(function (name) {
    present[name] = Boolean(props.getProperty(name));
  });
  return {
    bridge_enabled: ctmsP3_isEnabled_(),
    properties_present: present,
    values_exposed: false
  };
}

function ctmsP3_property_(name) {
  var value = PropertiesService.getScriptProperties().getProperty(name);
  if (!value) throw new Error('CTMS_CONFIGURATION_MISSING:' + name);
  return String(value);
}

function ctmsP3_baseUrl_() {
  return ctmsP3_property_('CLOUDTMS_CANDIDATE_BASE_URL').replace(/\/+$/, '');
}

function ctmsP3_environment_() {
  var value = ctmsP3_property_('CLOUDTMS_CANDIDATE_ENVIRONMENT').trim().toUpperCase();
  if (value !== 'TEST' && value !== 'LIVE') throw new Error('CTMS_ENVIRONMENT_INVALID');
  return value;
}

function ctmsP3_utf8Bytes_(text) {
  return Utilities.newBlob(String(text), 'application/json; charset=utf-8').getBytes();
}

function ctmsP3_hex_(bytes) {
  return (bytes || []).map(function (value) {
    var unsigned = value < 0 ? value + 256 : value;
    return ('0' + unsigned.toString(16)).slice(-2);
  }).join('');
}

function ctmsP3_sha256_(text) {
  return ctmsP3_hex_(Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    ctmsP3_utf8Bytes_(String(text))
  ));
}

function ctmsP3_hmacHex_(text, secret) {
  return ctmsP3_hex_(Utilities.computeHmacSha256Signature(
    ctmsP3_utf8Bytes_(String(text)),
    ctmsP3_utf8Bytes_(String(secret))
  ));
}

function ctmsP3_uuid_() {
  return String(Utilities.getUuid()).toLowerCase();
}

function ctmsP3_nonce_() {
  var raw = Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    ctmsP3_utf8Bytes_(Utilities.getUuid() + '|' + Date.now() + '|' + Math.random())
  );
  return Utilities.base64EncodeWebSafe(raw).replace(/=+$/g, '').slice(0, 32);
}

function ctmsP3_ulidPart_(value, length) {
  var result = '';
  var current = value;
  while (result.length < length) {
    result = CTMS_P3_ULID_ALPHABET_.charAt(current % 32) + result;
    current = Math.floor(current / 32);
  }
  return result.slice(-length);
}

function ctmsP3_correlationId_() {
  var timePart = ctmsP3_ulidPart_(Date.now(), 10);
  var entropy = ctmsP3_hex_(Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    ctmsP3_utf8Bytes_(Utilities.getUuid() + '|' + Math.random())
  )).toUpperCase();
  var randomPart = '';
  for (var i = 0; i < 16; i++) {
    randomPart += CTMS_P3_ULID_ALPHABET_.charAt(parseInt(entropy.substr(i * 2, 2), 16) % 32);
  }
  return timePart + randomPart;
}

function ctmsP3_log_(event, facts) {
  if (!ctmsP3_isEnabled_()) return;
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

function ctmsP3_signedPost_(path, body, idempotencyKey, correlationId) {
  if (!ctmsP3_isEnabled_()) return { disabled: true };
  var started = Date.now();
  var json = JSON.stringify(body || {});
  var rawBody = ctmsP3_utf8Bytes_(json);
  var bodyHash = ctmsP3_hex_(Utilities.computeDigest(
    Utilities.DigestAlgorithm.SHA_256,
    rawBody
  ));
  var timestamp = String(Math.floor(Date.now() / 1000));
  var nonce = ctmsP3_nonce_();
  var correlation = correlationId || ctmsP3_correlationId_();
  var key = String(idempotencyKey || '');
  var canonical = 'CLOUDTMS-HMAC-V1\nPOST\n' + path + '\n\n'
    + timestamp + '\n' + nonce + '\n' + bodyHash + '\n' + key + '\n'
    + correlation + '\n\n' + json;
  var signature = ctmsP3_hmacHex_(
    canonical,
    ctmsP3_property_('CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET')
  );
  try {
    var response = UrlFetchApp.fetch(ctmsP3_baseUrl_() + path, {
      method: 'post',
      contentType: 'application/json; charset=utf-8',
      muteHttpExceptions: true,
      payload: Utilities.newBlob(rawBody, 'application/json; charset=utf-8'),
      headers: {
        'x-cloudtms-key-id': ctmsP3_property_('CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID'),
        'x-cloudtms-signature-version': 'v1',
        'x-cloudtms-timestamp': timestamp,
        'x-cloudtms-nonce': nonce,
        'x-cloudtms-content-sha256': bodyHash,
        'x-cloudtms-signature': signature,
        'x-correlation-id': correlation,
        'idempotency-key': key
      }
    });
    var code = response.getResponseCode();
    var text = response.getContentText() || '';
    var parsed = null;
    try { parsed = text ? JSON.parse(text) : null; } catch (_) {}
    var uncertain = code === 408 || code === 425 || code === 429 || code >= 500;
    ctmsP3_log_('SIGNED_POST', {
      status: String(code), correlation_id: correlation,
      operation_id: key, duration_ms: Date.now() - started
    });
    return {
      http_code: code,
      json: parsed,
      correlation_id: correlation,
      uncertain: uncertain,
      not_found: code === 404
    };
  } catch (error) {
    ctmsP3_log_('SIGNED_POST_UNCERTAIN', {
      status: 'NETWORK_ERROR', error_code: 'NETWORK_ERROR',
      correlation_id: correlation, operation_id: key,
      duration_ms: Date.now() - started
    });
    return {
      http_code: -1,
      json: null,
      correlation_id: correlation,
      uncertain: true,
      not_found: false
    };
  }
}

function ctmsP3_sourceHmacFromPublicId_(publicId) {
  var canonical = 'CLOUDTMS-CANDIDATE-SOURCE-V1\n'
    + ctmsP3_environment_() + '\nGOOGLE_CREDENTIALLY_PUBLIC_ID\n'
    + String(publicId || '').trim() + '\n';
  if (!String(publicId || '').trim()) throw new Error('CTMS_PUBLIC_ID_MISSING');
  return ctmsP3_hmacHex_(
    canonical,
    ctmsP3_property_('CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET')
  );
}

function ctmsP3_candidateIdentityByMobile_(msisdn) {
  var ss = _ssRota();
  var sh = ss.getSheetByName(_P().SH_CAND);
  if (!sh || sh.getLastRow() < 2) throw new Error('CTMS_CANDIDATE_LIST_UNAVAILABLE');
  var rows = sh.getDataRange().getDisplayValues();
  var headers = rows[0].map(function (value) { return String(value || '').trim().toLowerCase(); });
  var publicIdIndex = headers.indexOf('public id - credentially');
  if (publicIdIndex < 0) throw new Error('CTMS_PUBLIC_ID_COLUMN_MISSING');
  for (var index = 1; index < rows.length; index++) {
    var telephone = _normaliseLocalTel(String(rows[index][3] || ''));
    if (telephone === msisdn) {
      var publicId = String(rows[index][publicIdIndex] || '').trim();
      return {
        candidate_source_hmac: ctmsP3_sourceHmacFromPublicId_(publicId),
        telephone: telephone
      };
    }
  }
  throw new Error('CTMS_CANDIDATE_SOURCE_LINK_NOT_FOUND');
}

function ctmsP3_stateKey_(fingerprint) {
  return CTMS_P3_STATE_PREFIX_ + String(fingerprint).slice(0, 48);
}

function ctmsP3_getOrCreateOperation_(fingerprint, factualBody) {
  var props = PropertiesService.getScriptProperties();
  var key = ctmsP3_stateKey_(fingerprint);
  var lock = LockService.getScriptLock();
  lock.waitLock(20000);
  try {
    var existing = null;
    try { existing = JSON.parse(props.getProperty(key) || 'null'); } catch (_) {}
    if (existing && existing.fingerprint === fingerprint
        && Date.now() - Number(existing.created_ms || 0) <= CTMS_P3_STATE_TTL_MS_) {
      return existing;
    }
    var requestId = ctmsP3_uuid_();
    var operation = {
      fingerprint: fingerprint,
      created_ms: Date.now(),
      request_id: requestId,
      idempotency_key: 'legacy.availability.' + requestId,
      correlation_id: ctmsP3_correlationId_(),
      retry_consumed: false,
      recovery_only: false,
      factual_body: factualBody
    };
    props.setProperty(key, JSON.stringify(operation));
    return operation;
  } finally {
    lock.releaseLock();
  }
}

function ctmsP3_saveOperation_(operation) {
  PropertiesService.getScriptProperties().setProperty(
    ctmsP3_stateKey_(operation.fingerprint),
    JSON.stringify(operation)
  );
}

function ctmsP3_clearOperation_(operation) {
  PropertiesService.getScriptProperties().deleteProperty(
    ctmsP3_stateKey_(operation.fingerprint)
  );
}

function ctmsP3_terminalSuccess_(result) {
  return result && result.http_code >= 200 && result.http_code < 300
    && result.json && result.json.ok === true;
}

function ctmsP3_contractDisposition_(result, routeKind) {
  if (ctmsP3_terminalSuccess_(result)) return 'SUCCESS';
  if (!result || result.uncertain === true) return 'PRESERVE';
  var body = result.json;
  if (!body || body.ok !== false
      || typeof body.error_code !== 'string'
      || typeof body.retry_class !== 'string') return 'PRESERVE';

  var triple = String(result.http_code) + ':' + body.error_code + ':' + body.retry_class;
  var applyTriples = {
    '400:VALIDATION_FAILED:DO_NOT_RETRY': true,
    '401:SYSTEM_AUTH_FAILED:DO_NOT_RETRY': true,
    '403:FORBIDDEN:DO_NOT_RETRY': true,
    '409:IDEMPOTENCY_KEY_REUSED:DO_NOT_RETRY': true,
    '409:COMMAND_IN_PROGRESS:STATUS_CHECK': true,
    '409:SOURCE_IDENTITY_NOT_READY:STATUS_CHECK': true,
    '409:IDENTITY_LINK_MISSING:STATUS_CHECK': true,
    '409:IDENTITY_LINK_AMBIGUOUS:DO_NOT_RETRY': true,
    '409:AVAILABILITY_VERSION_CONFLICT:REFRESH': true,
    '422:AVAILABILITY_DATE_NOT_EDITABLE:DO_NOT_RETRY': true,
    '429:RATE_LIMITED:RETRY_AFTER': true,
    '500:INTERNAL_ERROR:STATUS_CHECK': true,
    '503:DEPENDENCY_UNAVAILABLE:RETRY_AFTER': true
  };
  var statusTriples = {
    '400:VALIDATION_FAILED:DO_NOT_RETRY': true,
    '401:SYSTEM_AUTH_FAILED:DO_NOT_RETRY': true,
    '403:FORBIDDEN:DO_NOT_RETRY': true,
    '404:NOT_FOUND:DO_NOT_RETRY': true,
    '409:SOURCE_IDENTITY_NOT_READY:STATUS_CHECK': true,
    '409:IDENTITY_LINK_MISSING:STATUS_CHECK': true,
    '409:IDENTITY_LINK_AMBIGUOUS:DO_NOT_RETRY': true,
    '429:RATE_LIMITED:RETRY_AFTER': true,
    '500:INTERNAL_ERROR:RETRY_AFTER': true,
    '503:DEPENDENCY_UNAVAILABLE:RETRY_AFTER': true
  };
  var catalogue = routeKind === 'STATUS' ? statusTriples : applyTriples;
  if (!catalogue[triple]) return 'PRESERVE';
  if (routeKind === 'STATUS' && triple === '404:NOT_FOUND:DO_NOT_RETRY') {
    return 'AUTHORITATIVE_NOT_FOUND';
  }
  if (body.retry_class === 'DO_NOT_RETRY' || body.retry_class === 'REFRESH') {
    return 'TERMINAL_REJECTION';
  }
  return body.retry_class === 'STATUS_CHECK' ? 'STATUS_CHECK' : 'PRESERVE';
}

function ctmsP3_logTerminalRejection_(result, operation) {
  ctmsP3_log_('LEGACY_AVAILABILITY_TERMINAL_REJECTION', {
    status: String(result.http_code),
    error_code: String(result.json && result.json.error_code || ''),
    correlation_id: operation.correlation_id,
    operation_id: operation.idempotency_key
  });
}

function ctmsP3_recoverLegacyAvailability_(operation) {
  var status = ctmsP3_signedPost_(
    CTMS_P3_BASE_PATH_ + '/legacy/availability-status',
    {
      candidate_source_hmac: operation.factual_body.candidate_source_hmac,
      request_id: operation.request_id
    },
    '',
    operation.correlation_id
  );
  if (ctmsP3_terminalSuccess_(status)) {
    var state = status.json.result && status.json.result.state;
    if (state === 'COMPLETED' || state === 'FAILED_FINAL') {
      ctmsP3_clearOperation_(operation);
      return status;
    }
    operation.recovery_only = true;
    ctmsP3_saveOperation_(operation);
    return status;
  }
  var statusDisposition = ctmsP3_contractDisposition_(status, 'STATUS');
  if (statusDisposition === 'TERMINAL_REJECTION') {
    ctmsP3_logTerminalRejection_(status, operation);
    ctmsP3_clearOperation_(operation);
    return status;
  }
  if (statusDisposition === 'AUTHORITATIVE_NOT_FOUND' && !operation.retry_consumed) {
    operation.retry_consumed = true;
    operation.recovery_only = true;
    ctmsP3_saveOperation_(operation);
    var replayBody = {
      candidate_source_hmac: operation.factual_body.candidate_source_hmac,
      request_id: operation.request_id,
      changes: operation.factual_body.changes
    };
    var retry = ctmsP3_signedPost_(
      CTMS_P3_BASE_PATH_ + '/legacy/availability',
      replayBody,
      operation.idempotency_key,
      operation.correlation_id
    );
    var retryDisposition = ctmsP3_contractDisposition_(retry, 'APPLY');
    if (retryDisposition === 'SUCCESS') {
      ctmsP3_clearOperation_(operation);
    } else if (retryDisposition === 'TERMINAL_REJECTION') {
      ctmsP3_logTerminalRejection_(retry, operation);
      ctmsP3_clearOperation_(operation);
    } else {
      ctmsP3_saveOperation_(operation);
    }
    return retry;
  }
  operation.recovery_only = true;
  ctmsP3_saveOperation_(operation);
  return status;
}

function ctmsP3_appliedLegacyChanges_(legacyResults) {
  if (!Array.isArray(legacyResults)) throw new Error('CTMS_LEGACY_RESULTS_INVALID');
  var allowed = { '': true, 'N/A': true, 'LD': true, 'N': true, 'LD/N': true };
  var seen = {};
  var accepted = [];
  legacyResults.forEach(function (row) {
    if (!row || typeof row !== 'object') throw new Error('CTMS_LEGACY_RESULT_INVALID');
    if (row.applied !== true || row.deferred === true) return;
    var date = String(row.ymd || '');
    var availability = row.code == null ? '' : String(row.code).toUpperCase();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !allowed[availability]) {
      throw new Error('CTMS_LEGACY_RESULT_INVALID');
    }
    if (Object.prototype.hasOwnProperty.call(seen, date)) {
      throw new Error('CTMS_LEGACY_RESULT_CONTRADICTORY');
    }
    seen[date] = availability;
    accepted.push({ date: date, availability: availability });
  });
  return accepted;
}

function ctmsP3_mirrorLegacyAvailability_(msisdn, changes, legacyResults) {
  if (!ctmsP3_isEnabled_()) return;
  var normalizedChanges;
  try {
    normalizedChanges = ctmsP3_appliedLegacyChanges_(legacyResults);
  } catch (validationError) {
    ctmsP3_log_('LEGACY_AVAILABILITY_RESULT_REJECTED', {
      status: 'LEGACY_UNCHANGED',
      error_code: String(validationError && validationError.message || validationError)
    });
    return;
  }
  if (!normalizedChanges.length) return;
  try {
    var identity = ctmsP3_candidateIdentityByMobile_(msisdn);
    var factual = {
      candidate_source_hmac: identity.candidate_source_hmac,
      changes: normalizedChanges
    };
    var fingerprint = ctmsP3_sha256_(JSON.stringify(factual));
    var operation = ctmsP3_getOrCreateOperation_(fingerprint, factual);
    if (operation.recovery_only) {
      ctmsP3_recoverLegacyAvailability_(operation);
      return;
    }
    var result = ctmsP3_signedPost_(
      CTMS_P3_BASE_PATH_ + '/legacy/availability',
      {
        candidate_source_hmac: factual.candidate_source_hmac,
        request_id: operation.request_id,
        changes: factual.changes
      },
      operation.idempotency_key,
      operation.correlation_id
    );
    var disposition = ctmsP3_contractDisposition_(result, 'APPLY');
    if (disposition === 'SUCCESS') {
      ctmsP3_clearOperation_(operation);
      return;
    }
    if (disposition === 'TERMINAL_REJECTION') {
      ctmsP3_logTerminalRejection_(result, operation);
      ctmsP3_clearOperation_(operation);
      return;
    }
    operation.recovery_only = true;
    ctmsP3_saveOperation_(operation);
    ctmsP3_recoverLegacyAvailability_(operation);
  } catch (error) {
    ctmsP3_log_('LEGACY_AVAILABILITY_FAIL_OPEN', {
      status: 'LEGACY_UNCHANGED', error_code: String(error && error.message || error)
    });
  }
}

function ctmsP3_mergeLegacyTiles_(legacyEnvelope, msisdn, fromYmd) {
  if (!ctmsP3_isEnabled_()) return legacyEnvelope;
  try {
    var identity = ctmsP3_candidateIdentityByMobile_(msisdn);
    var response = ctmsP3_signedPost_(
      CTMS_P3_BASE_PATH_ + '/legacy/tiles',
      {
        candidate_source_hmac: identity.candidate_source_hmac,
        from: String(fromYmd || ''),
        days: 14
      },
      '',
      ctmsP3_correlationId_()
    );
    if (!ctmsP3_terminalSuccess_(response) || !response.json.result
        || !Array.isArray(response.json.result.tiles)) return legacyEnvelope;
    var canonicalByDate = {};
    response.json.result.tiles.forEach(function (tile) {
      if (tile && tile.ymd) canonicalByDate[String(tile.ymd)] = tile;
    });
    var merged = Object.assign({}, legacyEnvelope);
    merged.tiles = (legacyEnvelope.tiles || []).map(function (legacyTile) {
      var canonical = canonicalByDate[String(legacyTile.ymd || '')];
      return canonical ? Object.assign({}, legacyTile, canonical) : legacyTile;
    });
    if (response.json.result.lastLoadedAt) {
      merged.lastLoadedAt = response.json.result.lastLoadedAt;
    }
    return merged;
  } catch (error) {
    ctmsP3_log_('LEGACY_TILES_FAIL_OPEN', {
      status: 'LEGACY_UNCHANGED', error_code: String(error && error.message || error)
    });
    return legacyEnvelope;
  }
}

function ctmsP3_candidateMapBySourceHmac_() {
  var ss = _ssRota();
  var sh = ss.getSheetByName(_P().SH_CAND);
  var rows = sh.getDataRange().getDisplayValues();
  var headers = rows[0].map(function (value) { return String(value || '').trim().toLowerCase(); });
  var publicIdIndex = headers.indexOf('public id - credentially');
  var result = {};
  if (publicIdIndex < 0) return result;
  for (var index = 1; index < rows.length; index++) {
    var publicId = String(rows[index][publicIdIndex] || '').trim();
    var telephone = _normaliseLocalTel(String(rows[index][3] || ''));
    if (!publicId || !telephone) continue;
    result[ctmsP3_sourceHmacFromPublicId_(publicId)] = {
      telephone: telephone,
      name_key: (String(rows[index][0] || '') + ' ' + String(rows[index][1] || ''))
        .toLowerCase().trim()
    };
  }
  return result;
}

function ctmsP3_projectionDrainOnce() {
  if (!ctmsP3_isEnabled_()) return { disabled: true, claimed: 0, completed: 0 };
  var claimant = ctmsP3_property_('CLOUDTMS_CANDIDATE_EXECUTOR_ID');
  var claimId = ctmsP3_uuid_();
  var claimKey = 'projection.claim.' + claimId;
  var claim = ctmsP3_signedPost_(
    CTMS_P3_BASE_PATH_ + '/projection/claim',
    {
      claim_request_id: claimId,
      target: 'MASTER_AVAILABILITY_SHEET',
      claimant: claimant,
      max_items: 50
    },
    claimKey,
    ctmsP3_correlationId_()
  );
  if (!ctmsP3_terminalSuccess_(claim)) return { claimed: 0, completed: 0, uncertain: true };
  var items = claim.json.result && Array.isArray(claim.json.result.items)
    ? claim.json.result.items : [];
  if (!items.length) return { claimed: 0, completed: 0 };
  var ss = _ssRota();
  var headers = _readAvailabilityHeaders(ss);
  var headerByDate = {};
  headers.forEach(function (header) { headerByDate[header.ymd] = header; });
  var candidateMap = ctmsP3_candidateMapBySourceHmac_();
  var completionItems = [];
  items.forEach(function (item) {
    var completion = {
      outbox_id: item.outbox_id,
      lease_token: item.lease_token,
      outcome: 'RETRY',
      error_code: 'LEGACY_TARGET_UNAVAILABLE'
    };
    try {
      var candidate = candidateMap[item.candidate_source_hmac];
      var header = headerByDate[item.date];
      var avRow = candidate && _findAvailabilityRowByTelephone(ss, candidate.telephone);
      if (!candidate || !header || !avRow) throw new Error('LEGACY_TARGET_UNAVAILABLE');
      var booked = _readBookedMap(ss, candidate.name_key, [item.date]);
      var current = _readAvailabilityCells(ss, avRow.rowIndex, [header])[0];
      var revisionBefore = ctmsP3_sha256_(JSON.stringify({
        date: item.date, value: current.value, background: current.bg
      })).slice(0, 64);
      if (booked[item.date] || _isBlockedCell(current.value, current.bg, _P())) {
        completion = {
          outbox_id: item.outbox_id,
          lease_token: item.lease_token,
          outcome: 'DEFERRED_OVERLAY',
          observed_sheet_revision: revisionBefore
        };
      } else {
        var mapped = _mapWrite(item.availability, _P());
        var range = ss.getSheetByName(_P().SH_AV).getRange(avRow.rowIndex, header.col, 1, 1);
        range.setValue(mapped.value);
        range.setBackground(mapped.bg);
        var revisionAfter = ctmsP3_sha256_(JSON.stringify({
          date: item.date, value: mapped.value, background: mapped.bg
        })).slice(0, 64);
        completion = {
          outbox_id: item.outbox_id,
          lease_token: item.lease_token,
          outcome: 'DELIVERED',
          observed_sheet_revision: revisionAfter
        };
      }
    } catch (error) {
      completion.error_code = String(error && error.message || error).slice(0, 80);
    }
    completionItems.push(completion);
  });
  var completeId = ctmsP3_uuid_();
  var complete = ctmsP3_signedPost_(
    CTMS_P3_BASE_PATH_ + '/projection/complete',
    { batch_request_id: completeId, items: completionItems },
    'projection.complete.' + completeId,
    ctmsP3_correlationId_()
  );
  return {
    claimed: items.length,
    completed: ctmsP3_terminalSuccess_(complete) ? completionItems.length : 0,
    uncertain: !ctmsP3_terminalSuccess_(complete)
  };
}

function ctmsP3_effectClaim_(effectKey, operation, candidateSourceHmac, requestHash) {
  if (!ctmsP3_isEnabled_()) return { disabled: true };
  return ctmsP3_signedPost_(
    CTMS_P3_BASE_PATH_ + '/effects/claim',
    {
      effect_key: String(effectKey),
      operation: String(operation),
      candidate_source_hmac: String(candidateSourceHmac),
      request_hash: String(requestHash),
      executor_id: ctmsP3_property_('CLOUDTMS_CANDIDATE_EXECUTOR_ID')
    },
    'effect.claim.' + ctmsP3_sha256_(String(effectKey)).slice(0, 48),
    ctmsP3_correlationId_()
  );
}

function ctmsP3_effectComplete_(effectReceiptId, leaseToken, outcome, providerReferenceHash) {
  if (!ctmsP3_isEnabled_()) return { disabled: true };
  var body = {
    effect_receipt_id: String(effectReceiptId),
    lease_token: String(leaseToken),
    outcome: String(outcome)
  };
  if (providerReferenceHash) body.provider_reference_hash = String(providerReferenceHash);
  return ctmsP3_signedPost_(
    CTMS_P3_BASE_PATH_ + '/effects/complete', body, '', ctmsP3_correlationId_()
  );
}

function ctmsP3_effectStatus_(effectKey) {
  if (!ctmsP3_isEnabled_()) return { disabled: true };
  return ctmsP3_signedPost_(
    CTMS_P3_BASE_PATH_ + '/effects/status',
    { effect_key: String(effectKey) },
    '',
    ctmsP3_correlationId_()
  );
}
