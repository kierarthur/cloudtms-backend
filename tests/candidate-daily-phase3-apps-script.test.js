import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import vm from 'node:vm';

const root = process.cwd();
const repositorySourceRoot = path.join(root, 'docs', 'candidate-app', 'phase3-apps-script');
const packagedSourceRoot = path.join(root, 'source');
const sourceRoot = fs.existsSync(repositorySourceRoot) ? repositorySourceRoot : packagedSourceRoot;
const availabilityCode = path.join(sourceRoot, 'availability-api', 'Code.gs');
const availabilityHelper = path.join(sourceRoot, 'availability-api', 'CloudTMSCandidateBridge.gs');
const availabilityRollback = path.join(sourceRoot, 'availability-api', 'rollback', 'Code.gs');
const masterCode = path.join(sourceRoot, 'master-rota', 'Code.gs');
const masterHelper = path.join(sourceRoot, 'master-rota', 'CloudTMSCandidateBridge.gs');
const masterRollback = path.join(sourceRoot, 'master-rota', 'rollback', 'Code.gs');

function read(file) {
  return fs.readFileSync(file, 'utf8');
}

function lf(text) {
  return String(text).replace(/\r\n/g, '\n');
}

function sha(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function appsScriptUtilities() {
  let uuidCounter = 0;
  const bytes = (value) => Buffer.isBuffer(value)
    ? value
    : Buffer.from(Array.isArray(value) ? value.map((item) => item < 0 ? item + 256 : item) : String(value),
      Array.isArray(value) ? undefined : 'utf8');
  return {
    DigestAlgorithm: { SHA_256: 'SHA_256' },
    newBlob(value) {
      const body = bytes(value);
      return { getBytes: () => [...body].map((item) => item > 127 ? item - 256 : item) };
    },
    computeDigest(_algorithm, value) {
      return [...crypto.createHash('sha256').update(bytes(value)).digest()]
        .map((item) => item > 127 ? item - 256 : item);
    },
    computeHmacSha256Signature(value, key) {
      return [...crypto.createHmac('sha256', bytes(key)).update(bytes(value)).digest()]
        .map((item) => item > 127 ? item - 256 : item);
    },
    base64EncodeWebSafe(value) {
      return bytes(value).toString('base64url');
    },
    getUuid() {
      uuidCounter += 1;
      return `00000000-0000-4000-8000-${String(uuidCounter).padStart(12, '0')}`;
    },
    formatDate(value, _timezone, format) {
      const date = new Date(value);
      if (format === 'yyyy-MM-dd') return date.toISOString().slice(0, 10);
      throw new Error(`unsupported test format ${format}`);
    }
  };
}

function propertyStore(initial = {}) {
  const values = new Map(Object.entries(initial));
  let mutations = 0;
  return {
    values,
    get mutations() { return mutations; },
    service: {
      getScriptProperties() {
        return {
          getProperty: (name) => values.get(name) ?? null,
          setProperty(name, value) { mutations += 1; values.set(name, String(value)); },
          deleteProperty(name) { mutations += 1; values.delete(name); }
        };
      }
    }
  };
}

function lockService() {
  return { getScriptLock: () => ({ waitLock() {}, releaseLock() {} }) };
}

function evaluate(file, extras = {}) {
  const context = vm.createContext({
    Utilities: appsScriptUtilities(),
    Date,
    JSON,
    Math,
    Object,
    Array,
    String,
    Number,
    Boolean,
    Set,
    Map,
    ...extras
  });
  new vm.Script(read(file), { filename: file }).runInContext(context);
  return context;
}

function evaluateFiles(files, extras = {}) {
  const context = vm.createContext({
    Utilities: appsScriptUtilities(),
    Date,
    JSON,
    Math,
    Object,
    Array,
    String,
    Number,
    Boolean,
    Set,
    Map,
    ...extras
  });
  files.forEach((file) => new vm.Script(read(file), { filename: file }).runInContext(context));
  return context;
}

function fixture(name) {
  const candidates = [
    path.join(root, 'tests', 'fixtures', 'candidate-daily-r5', name),
    path.join(root, 'fixtures', 'candidate-daily-r5', name)
  ];
  const found = candidates.find((candidate) => fs.existsSync(candidate));
  if (!found) throw new Error(`fixture missing: ${name}`);
  return found;
}

test('certified rollback sources retain the independently recorded SHA-256 authorities', () => {
  assert.equal(sha(availabilityRollback), 'eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f');
  assert.equal(sha(masterRollback), 'c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8');
});

test('Availability revised Code.gs differs from certified source only at the approved Phase 3 seams', () => {
  const result = spawnSync('git', ['diff', '--no-index', '--unified=0', '--', availabilityRollback, availabilityCode],
    { encoding: 'utf8' });
  assert.equal(result.status, 1);
  const hunks = [...result.stdout.matchAll(/^@@[^\n]+@@/gm)].map((match) => match[0]);
  assert.deepEqual(hunks, [
    '@@ -508 +508 @@',
    '@@ -525 +525,3 @@',
    '@@ -540 +542 @@',
    '@@ -555 +557 @@',
    '@@ -571 +573 @@',
    '@@ -579,3 +581,7 @@',
    '@@ -624,6 +630,15 @@',
    '@@ -9300,9 +9315,10 @@',
    '@@ -9389 +9405,10 @@',
    '@@ -11499,4 +11524,4 @@',
    '@@ -11507,4 +11532,7 @@',
    '@@ -11771 +11799,3 @@'
  ]);
  assert.match(result.stdout, /ctmsP3_mirrorLegacyAvailability_/);
  assert.match(result.stdout, /ctmsP3_mergeLegacyTiles_/);
  const busyStart = read(availabilityCode).indexOf('const __legacyQueuedAvailabilityResponse');
  const busyEnd = read(availabilityCode).indexOf('return OK(__legacyQueuedAvailabilityResponse)', busyStart);
  assert.doesNotMatch(read(availabilityCode).slice(busyStart, busyEnd), /ctmsP3_mirrorLegacyAvailability_/);
});

test('Master Rota revised Code.gs differs from certified source only at the post-legacy additive mirror seam', () => {
  const result = spawnSync('git', ['diff', '--no-index', '--unified=0', '--', masterRollback, masterCode],
    { encoding: 'utf8' });
  assert.equal(result.status, 1);
  const hunks = [...result.stdout.matchAll(/^@@[^\n]+@@/gm)].map((match) => match[0]);
  assert.equal(hunks.length, 1);
  assert.equal(hunks[0], '@@ -19849,5 +19849,9 @@');
  assert.match(result.stdout, /const legacyResult = _postWithRetry/);
  assert.match(result.stdout, /ctmsP3_masterMirrorLegacyEvent_\(action, payload, legacyResult\)/);
  assert.match(result.stdout, /return legacyResult/);
});

test('false or missing bridge flag is a hard no-op in Availability helper', () => {
  for (const flag of [undefined, 'false', 'FALSE', '0', '']) {
    const props = propertyStore(flag === undefined ? {} : { CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: flag });
    let network = 0;
    let logged = 0;
    const context = evaluate(availabilityHelper, {
      PropertiesService: props.service,
      LockService: lockService(),
      UrlFetchApp: { fetch() { network += 1; throw new Error('network forbidden'); } },
      console: { log() { logged += 1; } }
    });
    const legacy = { ok: true, tiles: [{ ymd: '2026-08-17', legacy: true }] };
    assert.equal(context.ctmsP3_mergeLegacyTiles_(legacy, '07000000000', '2026-08-17'), legacy);
    assert.equal(context.ctmsP3_mirrorLegacyAvailability_('07000000000', [], []), undefined);
    assert.deepEqual(JSON.parse(JSON.stringify(context.ctmsP3_projectionDrainOnce())),
      { disabled: true, claimed: 0, completed: 0 });
    assert.equal(network, 0);
    assert.equal(logged, 0);
    assert.equal(props.mutations, 0);
  }
});

test('false or missing bridge flag is a hard no-op in Master Rota helper', () => {
  for (const flag of [undefined, 'false', 'FALSE', '0', '']) {
    const props = propertyStore(flag === undefined ? {} : { CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: flag });
    let network = 0;
    let logged = 0;
    const context = evaluate(masterHelper, {
      PropertiesService: props.service,
      LockService: lockService(),
      UrlFetchApp: { fetch() { network += 1; throw new Error('network forbidden'); } },
      console: { log() { logged += 1; } }
    });
    assert.equal(context.ctmsP3_masterMirrorLegacyEvent_('AVAILABILITY_UPDATE_END', { runId: 'run-1' }), undefined);
    assert.equal(network, 0);
    assert.equal(logged, 0);
    assert.equal(props.mutations, 0);
  }
});

test('Master Rota never advances CloudTMS when the primary Availability publication was not accepted', () => {
  const props = propertyStore({ CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true' });
  let network = 0;
  const context = evaluate(masterHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    UrlFetchApp: { fetch() { network += 1; throw new Error('network forbidden'); } },
    console: { log() {} }
  });
  context.ctmsP3_masterMirrorLegacyEvent_('AVAILABILITY_UPDATE_END', { runId: 'run-1' },
    { httpCode: 503 });
  assert.equal(network, 0);
  assert.equal(props.mutations, 0);
});

test('Apps Script HMAC implementation matches frozen R5 UTF-8 canonicalization vector', () => {
  const props = propertyStore({ CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true' });
  const context = evaluate(availabilityHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    console: { log() {} }
  });
  const vector = JSON.parse(read(fixture('canonicalization-v1-vectors.json')))
    .positive_vectors.find((item) => item.id === 'canonical_utf8_edge');
  const canonical = `CLOUDTMS-HMAC-V1\n${vector.method}\n${vector.normalized_path}\n${vector.normalized_query}\n`
    + `${vector.timestamp}\n${vector.nonce}\n${vector.body_sha256}\n${vector.idempotency_key}\n`
    + `${vector.correlation_id}\n\n${vector.body}`;
  assert.equal(context.ctmsP3_hmacHex_(canonical,
    'phase0-r5-cross-language-test-key'), vector.signature_hex);
});

test('source-link identity HMAC is deterministic, bounded and never transmits the raw public ID', () => {
  const secret = 'non-production-source-test-secret';
  const props = propertyStore({
    CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true',
    CLOUDTMS_CANDIDATE_ENVIRONMENT: 'TEST',
    CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET: secret
  });
  const context = evaluate(availabilityHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    console: { log() {} }
  });
  const publicId = 'Credentially-Public-123';
  const canonical = `CLOUDTMS-CANDIDATE-SOURCE-V1\nTEST\nGOOGLE_CREDENTIALLY_PUBLIC_ID\n${publicId}\n`;
  const expected = crypto.createHmac('sha256', secret).update(canonical).digest('hex');
  assert.equal(context.ctmsP3_sourceHmacFromPublicId_(publicId), expected);
  assert.match(expected, /^[0-9a-f]{64}$/);
  assert.equal(expected.includes(publicId), false);
});

test('legacy availability sends only durable applied non-deferred rows', () => {
  const props = propertyStore({ CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true' });
  const calls = [];
  let identityLookups = 0;
  const context = evaluate(availabilityHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    console: { log() {} }
  });
  context.ctmsP3_candidateIdentityByMobile_ = () => {
    identityLookups += 1;
    return { candidate_source_hmac: 'a'.repeat(64) };
  };
  context.ctmsP3_getOrCreateOperation_ = (_fingerprint, factual) => ({
    fingerprint: 'f'.repeat(64), request_id: '00000000-0000-4000-8000-000000000001',
    idempotency_key: 'legacy.availability.fixed', correlation_id: '01K2ABCDEF0123456789ABCDE1',
    retry_consumed: false, recovery_only: false, factual_body: factual
  });
  context.ctmsP3_signedPost_ = (route, body) => {
    calls.push({ route, body: JSON.parse(JSON.stringify(body)) });
    return { http_code: 200, json: { ok: true, result: {} }, uncertain: false };
  };
  context.ctmsP3_clearOperation_ = () => {};
  context.ctmsP3_mirrorLegacyAvailability_('07000000000', [
    { ymd: '2026-08-18', code: 'LD' },
    { ymd: '2026-08-19', code: 'N' },
    { ymd: '2026-08-20', code: 'LD/N' }
  ], [
    { ymd: '2026-08-18', applied: true, code: 'LD' },
    { ymd: '2026-08-19', applied: false, reason: 'BOOKED_LOCK' },
    { ymd: '2026-08-20', applied: true, deferred: true, code: 'LD/N' }
  ]);
  assert.equal(identityLookups, 1);
  assert.deepEqual(calls[0].body.changes, [{ date: '2026-08-18', availability: 'LD' }]);
});

test('legacy availability all-accepted result preserves every durable accepted row', () => {
  const props = propertyStore({ CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true' });
  const calls = [];
  const context = evaluate(availabilityHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    console: { log() {} }
  });
  context.ctmsP3_candidateIdentityByMobile_ = () => ({ candidate_source_hmac: 'a'.repeat(64) });
  context.ctmsP3_getOrCreateOperation_ = (_fingerprint, factual) => ({
    fingerprint: 'f'.repeat(64), request_id: '00000000-0000-4000-8000-000000000001',
    idempotency_key: 'legacy.availability.fixed', correlation_id: '01K2ABCDEF0123456789ABCDE1',
    retry_consumed: false, recovery_only: false, factual_body: factual
  });
  context.ctmsP3_signedPost_ = (route, body) => {
    calls.push({ route, body: JSON.parse(JSON.stringify(body)) });
    return { http_code: 200, json: { ok: true, result: {} }, uncertain: false };
  };
  context.ctmsP3_clearOperation_ = () => {};
  context.ctmsP3_mirrorLegacyAvailability_('07000000000', [], [
    { ymd: '2026-08-18', applied: true, code: 'LD' },
    { ymd: '2026-08-19', applied: true, code: 'N' },
    { ymd: '2026-08-20', applied: true, code: 'LD/N' },
    { ymd: '2026-08-21', applied: true, code: 'N/A' },
    { ymd: '2026-08-22', applied: true, code: '' }
  ]);
  assert.deepEqual(calls[0].body.changes, [
    { date: '2026-08-18', availability: 'LD' },
    { date: '2026-08-19', availability: 'N' },
    { date: '2026-08-20', availability: 'LD/N' },
    { date: '2026-08-21', availability: 'N/A' },
    { date: '2026-08-22', availability: '' }
  ]);
});

test('legacy availability all-rejected result performs no identity, state, log or network work', () => {
  const props = propertyStore({ CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true' });
  let identities = 0;
  let network = 0;
  let logged = 0;
  const context = evaluate(availabilityHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    console: { log() { logged += 1; } },
    UrlFetchApp: { fetch() { network += 1; throw new Error('network forbidden'); } }
  });
  context.ctmsP3_candidateIdentityByMobile_ = () => { identities += 1; return {}; };
  const beforeMutations = props.mutations;
  context.ctmsP3_mirrorLegacyAvailability_('07000000000', [], [
    { ymd: '2026-08-18', applied: false, reason: 'BOOKED_LOCK' },
    { ymd: '2026-08-19', applied: true, deferred: true, code: 'N' }
  ]);
  assert.deepEqual({ identities, network, logged }, { identities: 0, network: 0, logged: 0 });
  assert.equal(props.mutations, beforeMutations);
});

test('queued Availability rows mirror only after flush writes and lock release', () => {
  const ymds = Array.from({ length: 14 }, (_, index) => `2026-08-${String(18 + index).padStart(2, '0')}`);
  const writes = [];
  const mirrorCalls = [];
  const events = [];
  const avSheet = {
    getRange(row, column) {
      return { getA1Notation: () => `R${row}C${column}` };
    },
    getRangeList() {
      const ranges = [{
        setValue(value) { writes.push(['value', value]); },
        setBackground(value) { writes.push(['background', value]); }
      }];
      return { getRanges: () => ranges };
    }
  };
  const context = evaluateFiles([availabilityCode], { console: { log() {} } });
  context._getPendingWrites = () => [{
    msisdn: '07000000000',
    changes: [
      { ymd: ymds[0], code: 'LD' },
      { ymd: ymds[1], code: 'N' },
      { ymd: ymds[2], code: 'LD/N' },
      { ymd: ymds[3], code: 'INVALID' },
      { ymd: '2099-01-01', code: 'N/A' }
    ]
  }];
  context._isRotaBusy = () => false;
  context._P = () => ({ SH_AV: 'Availability' });
  context._ssRota = () => ({ getSheetByName: () => avSheet });
  context._readAvailabilityHeaders = () => ymds.map((ymd, index) => ({
    ymd, col: index + 7, displayDay: '', displayDate: ''
  }));
  context._findCandidateByMobile = () => ({ telephone: '07000000000', surname: 'Worker', firstname: 'Test' });
  context._findAvailabilityRowByTelephone = () => ({ rowIndex: 2 });
  context._readBookedMap = () => ({ [ymds[1]]: true });
  context._readOneAvailabilityCell = (_ss, _row, col) => ({ value: col === 9 ? 'BLOCK' : '', bg: '#ffffff' });
  context._isBlockedCell = (value) => value === 'BLOCK';
  context._mapWrite = (code) => ({ value: code, bg: '#ffffff' });
  context._tilesGet = () => null;
  context._setWriteLock = (locked) => events.push(locked ? 'lock' : 'unlock');
  context._setPendingWrites = () => events.push('queue-cleared');
  context.ctmsP3_mirrorLegacyAvailability_ = (msisdn, _changes, results) => {
    events.push('mirror');
    mirrorCalls.push({ msisdn, results: JSON.parse(JSON.stringify(results)) });
  };
  const outcome = context._flushPendingWrites();
  assert.deepEqual(JSON.parse(JSON.stringify(outcome)), { count: 1, applied: 1, failed: 4 });
  assert.deepEqual(writes, [['value', 'LD'], ['background', '#ffffff']]);
  assert.deepEqual(mirrorCalls, [{
    msisdn: '07000000000',
    results: [{ ymd: ymds[0], applied: true, code: 'LD' }]
  }]);
  assert.ok(events.indexOf('unlock') < events.indexOf('mirror'));
});

function runAvailabilityContractCase(initialResult, statusResult) {
  const props = propertyStore({ CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true' });
  const calls = [];
  let clears = 0;
  let saves = 0;
  const context = evaluate(availabilityHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    console: { log() {} }
  });
  context.ctmsP3_candidateIdentityByMobile_ = () => ({ candidate_source_hmac: 'a'.repeat(64) });
  context.ctmsP3_getOrCreateOperation_ = (_fingerprint, factual) => ({
    fingerprint: 'f'.repeat(64), request_id: '00000000-0000-4000-8000-000000000001',
    idempotency_key: 'legacy.availability.fixed', correlation_id: '01K2ABCDEF0123456789ABCDE1',
    retry_consumed: false, recovery_only: false, factual_body: factual
  });
  const responses = [initialResult, statusResult];
  context.ctmsP3_signedPost_ = (route) => {
    calls.push(route);
    return responses.shift();
  };
  context.ctmsP3_clearOperation_ = () => { clears += 1; };
  context.ctmsP3_saveOperation_ = () => { saves += 1; };
  context.ctmsP3_mirrorLegacyAvailability_('07000000000', [], [
    { ymd: '2026-08-18', applied: true, code: 'LD' }
  ]);
  return { calls, clears, saves };
}

test('STATUS_CHECK contract responses retain the operation and invoke exact status', () => {
  for (const errorCode of ['COMMAND_IN_PROGRESS', 'SOURCE_IDENTITY_NOT_READY', 'IDENTITY_LINK_MISSING']) {
    const observed = runAvailabilityContractCase({
      http_code: 409,
      json: { ok: false, error_code: errorCode, retry_class: 'STATUS_CHECK' },
      uncertain: false
    }, {
      http_code: 200,
      json: { ok: true, result: { state: 'IN_PROGRESS' } },
      uncertain: false
    });
    assert.equal(observed.clears, 0, errorCode);
    assert.ok(observed.saves >= 2, errorCode);
    assert.equal(observed.calls.length, 2, errorCode);
    assert.match(observed.calls[1], /legacy\/availability-status$/, errorCode);
  }
});

test('explicit REFRESH and DO_NOT_RETRY errors are terminal but malformed 409 is retained', () => {
  for (const result of [
    { http_code: 409, json: { ok: false, error_code: 'AVAILABILITY_VERSION_CONFLICT', retry_class: 'REFRESH' }, uncertain: false },
    { http_code: 409, json: { ok: false, error_code: 'IDEMPOTENCY_KEY_REUSED', retry_class: 'DO_NOT_RETRY' }, uncertain: false }
  ]) {
    const observed = runAvailabilityContractCase(result, null);
    assert.deepEqual({ calls: observed.calls.length, clears: observed.clears, saves: observed.saves },
      { calls: 1, clears: 1, saves: 0 });
  }
  const malformed = runAvailabilityContractCase({
    http_code: 409, json: { ok: false, error_code: 'UNKNOWN', retry_class: 'DO_NOT_RETRY' }, uncertain: false
  }, {
    http_code: 503,
    json: { ok: false, error_code: 'DEPENDENCY_UNAVAILABLE', retry_class: 'RETRY_AFTER' },
    uncertain: true
  });
  assert.equal(malformed.clears, 0);
  assert.ok(malformed.saves >= 2);
  assert.equal(malformed.calls.length, 2);
});

test('legacy availability uncertainty retains one operation and consumes at most one exact retry', () => {
  const props = propertyStore({
    CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true',
    CLOUDTMS_CANDIDATE_ENVIRONMENT: 'TEST',
    CLOUDTMS_CANDIDATE_BASE_URL: 'https://test.invalid',
    CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID: 'test-key',
    CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET: 'test-secret',
    CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET: 'source-secret'
  });
  const calls = [];
  const responses = [
    new Error('lost execute'),
    { code: 404, body: { ok: false, error_code: 'NOT_FOUND', retry_class: 'DO_NOT_RETRY' } },
    new Error('lost exact retry'),
    { code: 404, body: { ok: false, error_code: 'NOT_FOUND', retry_class: 'DO_NOT_RETRY' } },
    { code: 200, body: { ok: true, result: { request_id: 'x', state: 'COMPLETED' } } }
  ];
  const context = evaluate(availabilityHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    console: { log() {} },
    UrlFetchApp: {
      fetch(url, options) {
        calls.push({
          url,
          body: JSON.parse(Buffer.from(options.payload.getBytes().map((b) => b < 0 ? b + 256 : b)).toString()),
          idempotencyKey: options.headers['idempotency-key'],
          correlationId: options.headers['x-correlation-id']
        });
        const next = responses.shift();
        if (next instanceof Error) throw next;
        return {
          getResponseCode: () => next.code,
          getContentText: () => JSON.stringify(next.body)
        };
      }
    }
  });
  context.ctmsP3_candidateIdentityByMobile_ = () => ({ candidate_source_hmac: 'a'.repeat(64) });
  const changes = [{ ymd: '2026-08-18', code: 'LD' }];
  const legacyResults = [{ ymd: '2026-08-18', applied: true, code: 'LD' }];
  context.ctmsP3_mirrorLegacyAvailability_('07000000000', changes, legacyResults);
  assert.equal(calls.length, 3);
  assert.equal(calls.filter((call) => call.url.endsWith('/legacy/availability')).length, 2);
  assert.deepEqual(calls[0].body, calls[2].body);
  assert.equal(calls[0].idempotencyKey, calls[2].idempotencyKey);
  assert.equal(calls[0].correlationId, calls[2].correlationId);
  assert.equal(calls[0].body.candidate_source_hmac, calls[2].body.candidate_source_hmac);
  context.ctmsP3_mirrorLegacyAvailability_('07000000000', changes, legacyResults);
  assert.equal(calls.length, 4);
  assert.equal(calls.filter((call) => call.url.endsWith('/legacy/availability')).length, 2);
  context.ctmsP3_mirrorLegacyAvailability_('07000000000', changes, legacyResults);
  assert.equal(calls.length, 5);
  assert.equal(calls.filter((call) => call.url.endsWith('/legacy/availability')).length, 2);
  assert.equal([...props.values.keys()].some((key) => key.startsWith('CTMS_P3_OP_')), false);
});

function sheet(values, backgrounds) {
  return {
    getLastRow: () => values.length,
    getLastColumn: () => values[0].length,
    getDataRange: () => ({
      getDisplayValues: () => values,
      getBackgrounds: () => backgrounds || values.map((row) => row.map(() => '#ffffff'))
    }),
    getRange: (row, column, rowCount, columnCount) => ({
      getDisplayValues: () => values.slice(row - 1, row - 1 + rowCount)
        .map((source) => source.slice(column - 1, column - 1 + columnCount))
    })
  };
}

test('Master Rota generation builder emits exact 14-day hashed items without raw identity', () => {
  const dates = Array.from({ length: 14 }, (_, index) => {
    const date = new Date(Date.UTC(2026, 7, 17 + index));
    return `${String(date.getUTCDate()).padStart(2, '0')}/${String(date.getUTCMonth() + 1).padStart(2, '0')}/${date.getUTCFullYear()}`;
  });
  const candidateRows = [
    ['Surname', 'First', 'Email', 'Telephone', 'Public ID - Credentially'],
    ['Worker', 'Test', 'hidden@example.invalid', '07000000000', 'public-id-not-transmitted']
  ];
  const availabilityRows = [
    ['Surname', 'First', 'Email', 'Telephone', '', '', ...dates],
    ['Worker', 'Test', '', '07000000000', '', '', ...Array(14).fill('')]
  ];
  const availabilityBackgrounds = availabilityRows.map((row) => row.map(() => '#ffffff'));
  const historyRows = [
    ['OccupantKey', 'Date', 'Shift', 'Booking Reference', 'Day', 'Hospital', 'Ward', 'Job Title', '', 'Notes'],
    ['worker test', dates[0], '0730-2000', 'BOOK-1', '', 'Test Hospital', 'Ward A', 'HCA', '', '']
  ];
  const sheets = {
    'Candidate List': sheet(candidateRows),
    Availability: sheet(availabilityRows, availabilityBackgrounds),
    EmailHistory: sheet(historyRows)
  };
  const props = propertyStore({
    CLOUDTMS_CANDIDATE_BRIDGE_ENABLED: 'true',
    CLOUDTMS_CANDIDATE_ENVIRONMENT: 'TEST',
    CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET: 'source-secret'
  });
  const context = evaluate(masterHelper, {
    PropertiesService: props.service,
    LockService: lockService(),
    console: { log() {} },
    SpreadsheetApp: {
      getActiveSpreadsheet: () => ({
        getSpreadsheetTimeZone: () => 'Europe/London',
        getSheetByName: (name) => sheets[name]
      })
    },
    SH_AVAIL: 'Availability',
    COLOR_BLOCKED: '#ccffcc',
    getShiftWindow(rawDate, shiftLabel) {
      const [day, month, year] = rawDate.split('/').map(Number);
      const match = shiftLabel.match(/(\d{2})(\d{2})-(\d{2})(\d{2})/);
      return {
        start: new Date(Date.UTC(year, month - 1, day, Number(match[1]), Number(match[2]))),
        end: new Date(Date.UTC(year, month - 1, day, Number(match[3]), Number(match[4])))
      };
    }
  });
  const items = context.ctmsP3_masterBuildGenerationItems_('phase3-test-run');
  const plain = JSON.parse(JSON.stringify(items));
  assert.equal(plain.length, 1);
  assert.equal(plain[0].days.length, 14);
  assert.equal(plain[0].days[0].booked, true);
  assert.equal(plain[0].days[0].booking_id, 'BOOK-1');
  assert.match(plain[0].source_hash, /^[0-9a-f]{64}$/);
  assert.match(plain[0].candidate_source_hmac, /^[0-9a-f]{64}$/);
  assert.match(plain[0].item_key, /^[A-Za-z0-9._~-]{8,160}$/);
  const serialized = JSON.stringify(plain);
  assert.equal(serialized.includes('public-id-not-transmitted'), false);
  assert.equal(serialized.includes('07000000000'), false);
  assert.equal(serialized.includes('hidden@example.invalid'), false);
});

test('Phase 3 code preserves orphan ai_startDailyPings state and introduces no trigger owner', () => {
  for (const file of [availabilityCode, availabilityHelper, masterCode, masterHelper]) {
    assert.doesNotMatch(read(file), /function\s+ai_startDailyPings\s*\(/);
  }
  assert.doesNotMatch(read(availabilityHelper) + read(masterHelper),
    /newTrigger\s*\(|ScriptApp\.newTrigger|createTrigger/i);
});

test('copy/paste source is unredacted and contains no secret values or placeholders', () => {
  const sources = [availabilityCode, availabilityHelper, masterCode, masterHelper].map(read).join('\n');
  const helpers = [availabilityHelper, masterHelper].map(read).join('\n');
  assert.doesNotMatch(sources,
    /\[REDACTED BY HANDOVER\]|CODE OMITTED|TRUNCATED SOURCE|<SECRET_VALUE>|BEGIN PRIVATE KEY/i);
  assert.doesNotMatch(helpers, /INSERT[_ -]SECRET|YOUR[_ -]SECRET|<SECRET>|BEGIN PRIVATE KEY/i);
  assert.match(sources, /CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET/);
  assert.match(sources, /CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET/);
});
