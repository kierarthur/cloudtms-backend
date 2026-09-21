const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DATE_PATTERN = /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const BYTEA_SHA256_PATTERN = /^\\x[0-9a-f]{64}$/;
const MONEY_PATTERN = /^-?(?:0|[1-9][0-9]*)\.[0-9]{2}$/;
const RATE_PATTERN = /^-?(?:0|[1-9][0-9]*)\.[0-9]{6}$/;
const HEX_BYTES_PATTERN = /^(?:[0-9a-f]{2})+$/;
const INTEGER_TOKEN_PATTERN = /^-?(?:0|[1-9][0-9]*)$/;
const MIN_BIGINT = -(2n ** 63n);
const MAX_BIGINT = (2n ** 63n) - 1n;
const MAX_RESPONSE_BYTES = 32_768;
const MAX_CONTROL_REQUEST_BYTES = 32_768;
const MAX_STAGE_REQUEST_BYTES = 262_144;

const SOURCE_MODES = new Set(['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY']);
const SOURCE_AUTHORITY_KINDS = new Set([
  'CANDIDATE_SUBMISSION',
  'CLIENT_SOURCE',
  'OFFICE_APPROVAL',
  'ROOT_FINANCIAL',
  'PROVIDER',
  'SOURCE_EXPENSE',
  'ORDINARY_EXPENSE',
  // S7 (WB-007, WB-013, 24 section 5): 'NONADVANCE_ADJUSTMENT' is not a legal
  // C1 source authority kind.  An adjustment is never copied into the immutable
  // head, because an adjustment created later would make that snapshot stale
  // and create a second owner.
  'APPROVED_COMPONENT'
]);
const PENDING_REASONS = new Set([
  'OFFICE_WAIT',
  'FROZEN_FINANCIAL_STATE',
  'PROVIDER_REVALIDATION'
]);
const CONTROL_STATUSES = new Set([
  'PROGRESS',
  'READY',
  'PUBLISHED',
  'PENDING',
  'ABORTED',
  'RETIRED',
  'BUSY',
  'STALE',
  'REPLAYED',
  'REFUSED',
  'COMPACTED',
  'SEALED',
  'ADOPTED',
  'TERMINAL'
]);

const RPC = Object.freeze({
  start: 'weekly_source_start_c1',
  stage: 'weekly_source_stage_c1',
  continueValidation: 'weekly_source_continue_c1',
  certify: 'weekly_source_certify_c1',
  publish: 'weekly_source_publish_c1',
  setPending: 'weekly_source_pending_c1',
  abort: 'weekly_source_abort_c1',
  retire: 'weekly_source_retire_c1',
  repair: 'weekly_source_repair_c1',
  status: 'weekly_source_status_c1',
  prepareSourceScope: 'weekly_source_scope_start_c1',
  continueSourceScope: 'weekly_source_scope_continue_c1',
  beginR01: 'weekly_source_begin_r01_c1',
  sourceScopeStatus: 'weekly_source_scope_status_c1',
  abortSourceScope: 'weekly_source_scope_abort_c1',
  releaseSourceScope: 'weekly_source_scope_release_c1',
  executeWorkbenchAttempt: 'weekly_source_workbench_execute_c1',
  cleanup: 'weekly_source_cleanup_c1'
});

const CONTROL_KEYS = Object.freeze([
  'contract', 'ok', 'status', 'code', 'operation_id', 'scope_id',
  'owner_epoch', 'sequence', 'next_sequence', 'phase', 'source_cursor',
  'component_cursor', 'verify_cursor', 'rows_read', 'rows_written',
  'work_used', 'processed_bytes', 'has_more', 'replayed', 'retry_after_ms',
  'request_sha256', 'checkpoint_sha256', 'receipt_sha256', 'publication_id',
  'head_revision', 'source_identity_sha256', 'operation_created',
  'input_records_consumed', 'part_cursor', 'verify_part_cursor',
  'verify_byte_offset'
]);

const STATUS_KEYS = Object.freeze([
  ...CONTROL_KEYS.slice(0, CONTROL_KEYS.indexOf('input_records_consumed')),
  'state', 'terminal_receipt_sha256', 'retained_sequence_low',
  'retained_sequence_high'
]);

const SCOPE_STATUS_KEYS = Object.freeze([
  'scope_id', 'state', 'owner_epoch', 'next_sequence', 'visited_root_count',
  'source_root_count', 'protected_root_count', 'held_freeze_count',
  'held_evidence_count', 'r01_generation_id', 'begin_receipt_sha256',
  'last_receipt_sha256', 'release_phase', 'release_member_cursor'
]);

const START_KEYS = Object.freeze([
  'request_id', 'request_sequence', 'actor_user_id', 'candidate_id',
  'contract_id', 'root_timesheet_id', 'week_ending_date', 'source_mode',
  'expected_head_revision', 'expected_source_count',
  'expected_component_count', 'expected_payload_bytes',
  'source_manifest_sha256', 'entitlement_sha256', 'approval_sha256',
  'is_zero_entitlement', 'financial_row_id'
]);

const SOURCE_RECORD_KEYS = Object.freeze([
  'source_ordinal', 'source_id', 'authority_kind', 'source_system',
  'external_identity', 'external_revision', 'source_document_sha256',
  'payload_bytes', 'part_count', 'work_date', 'root_timesheet_id',
  'candidate_id', 'contract_id', 'source_row_sha256', 'record_type'
]);

const PART_RECORD_KEYS = Object.freeze([
  'source_ordinal', 'part_ordinal', 'payload_utf8', 'fragment_sha256',
  'record_type'
]);

const COMPONENT_RECORD_KEYS = Object.freeze([
  'component_ordinal', 'component_id', 'source_ordinal', 'source_key',
  'component_kind', 'economic_key_type', 'economic_key_value',
  'component_member_identity', 'segment_id', 'segment_key',
  'segment_stable_key', 'work_date', 'reference_number', 'hours_day',
  'hours_night', 'hours_sat', 'hours_sun', 'hours_bh',
  'additional_code_raw', 'unit_count', 'unit_pay_rate', 'unit_charge_rate',
  // S7: 'adjustment_id' is deliberately absent, matching the exact-key
  // component allowlist of the SQL owner, which now rejects that key outright.
  'expense_code', 'pay_ex_vat', 'charge_ex_vat',
  'exclude_from_pay', 'origin', 'component_sha256', 'record_type'
]);

const OPERATION_CURSOR_KEYS = Object.freeze([
  'operation_id', 'owner_epoch', 'next_sequence', 'receipt_sha256'
]);
const SCOPE_CURSOR_KEYS = Object.freeze([
  'scope_id', 'owner_epoch', 'next_sequence', 'receipt_sha256'
]);
const STREAM_CURSOR_KEYS = Object.freeze([
  'stream_id', 'owner_epoch', 'next_sequence', 'receipt_sha256'
]);

const SCOPE_REQUEST_KEYS = Object.freeze([
  'scope_id', 'r01_operation_id', 'r01_request_id', 'pay_batch_id',
  'actor_user_id', 'base_source_identity_sha256', 'selection_sha256',
  'source_build_count', 'source_root_count', 'request_sha256'
]);

const R01_KEYS = Object.freeze([
  'p_operation_id', 'p_request_id', 'p_pay_batch_id', 'p_actor_user_id',
  'p_base_generation_id', 'p_base_revision', 'p_kind', 'p_declared_count',
  'p_policy_sha256', 'p_source_identity_sha256', 'p_selection_sha256'
]);

const WORKBENCH_KEYS = Object.freeze([
  'p_job_id', 'p_build_id', 'p_private_stage', 'p_attempt_id',
  'p_attempt_nonce', 'p_worker_id', 'p_lane_identity'
]);

const UNKNOWN_CALL_KEYS = Object.freeze([
  'contract', 'method', 'rpc_name', 'parameters_json',
  'parameters_sha256', 'result_kind', 'stream_kind', 'stream_id',
  'recovery_attempted'
]);

const RPC_OPTIONS = Object.freeze({
  automaticRetry: false,
  requestBody: 'LOSSLESS_JSON_TEXT',
  responseBody: 'LOSSLESS_JSON_TEXT'
});

export const WEEKLY_SOURCE_C1_RPC_DEPENDENCY_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_C1_RAW_RPC_V1',
  call: 'rpc(functionName, parametersJsonText, options) -> Promise<responseJsonText>',
  automaticRetry: false
});

export class WeeklySourceC1AdapterInputError extends TypeError {
  constructor(message, path = '$') {
    super(`${path}: ${message}`);
    this.name = 'WeeklySourceC1AdapterInputError';
    this.code = 'C1_INPUT_INVALID';
    this.path = path;
  }
}

export class WeeklySourceC1AdapterProtocolError extends Error {
  constructor(message, code = 'C1_RESPONSE_INVALID') {
    super(message);
    this.name = 'WeeklySourceC1AdapterProtocolError';
    this.code = code;
  }
}

export class WeeklySourceC1UnknownOutcomeError extends Error {
  constructor(recoveryCall, cause) {
    super('The C1 RPC outcome is unknown; persist recoveryCall and invoke recoverUnknown explicitly.');
    this.name = 'WeeklySourceC1UnknownOutcomeError';
    this.code = 'C1_OUTCOME_UNKNOWN';
    this.recoveryCall = recoveryCall;
    Object.defineProperty(this, 'cause', { value: cause, enumerable: false });
  }
}

function isPlainObject(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function assertPlainObject(value, path) {
  if (!isPlainObject(value)) throw new WeeklySourceC1AdapterInputError('must be a plain object', path);
  return value;
}

function assertExactKeys(value, expectedKeys, path) {
  assertPlainObject(value, path);
  const actualKeys = Object.keys(value).sort();
  const expected = [...expectedKeys].sort();
  if (actualKeys.length !== expected.length || actualKeys.some((key, index) => key !== expected[index])) {
    throw new WeeklySourceC1AdapterInputError(
      `must contain exactly: ${expectedKeys.join(', ')}`,
      path
    );
  }
  return value;
}

function assertString(value, path) {
  if (typeof value !== 'string') throw new WeeklySourceC1AdapterInputError('must be a string', path);
  return value;
}

function assertBoolean(value, path) {
  if (typeof value !== 'boolean') throw new WeeklySourceC1AdapterInputError('must be a boolean', path);
  return value;
}

function assertNullable(value, validator, path) {
  if (value === null) return null;
  return validator(value, path);
}

function assertPattern(value, pattern, description, path) {
  assertString(value, path);
  if (!pattern.test(value)) throw new WeeklySourceC1AdapterInputError(`must be ${description}`, path);
  return value;
}

function assertUuid(value, path) {
  return assertPattern(value, UUID_PATTERN, 'a canonical lowercase UUID', path);
}

function assertSha256(value, path) {
  return assertPattern(value, SHA256_PATTERN, '64 lowercase hexadecimal characters', path);
}

function assertDate(value, path) {
  assertPattern(value, DATE_PATTERN, 'a YYYY-MM-DD date', path);
  const [year, month, day] = value.split('-').map(Number);
  const candidate = new Date(Date.UTC(year, month - 1, day));
  if (
    candidate.getUTCFullYear() !== year ||
    candidate.getUTCMonth() !== month - 1 ||
    candidate.getUTCDate() !== day
  ) {
    throw new WeeklySourceC1AdapterInputError('must be a real calendar date', path);
  }
  return value;
}

function integerToBigInt(value, path, { minimum = MIN_BIGINT, maximum = MAX_BIGINT } = {}) {
  let parsed;
  if (typeof value === 'bigint') {
    parsed = value;
  } else if (typeof value === 'number' && Number.isSafeInteger(value) && !Object.is(value, -0)) {
    parsed = BigInt(value);
  } else {
    throw new WeeklySourceC1AdapterInputError(
      'must be a safe integer Number or a BigInt; integer strings and unsafe Numbers are forbidden',
      path
    );
  }
  if (parsed < minimum || parsed > maximum) {
    throw new WeeklySourceC1AdapterInputError(`must be between ${minimum} and ${maximum}`, path);
  }
  return parsed;
}

function assertIntegerResult(value, path, { minimum = MIN_BIGINT, maximum = MAX_BIGINT } = {}) {
  if (typeof value !== 'bigint' || value < minimum || value > maximum) {
    throw new WeeklySourceC1AdapterProtocolError(`${path}: invalid canonical integer in RPC result`);
  }
  return value;
}

function byteaSha256(value, path) {
  if (typeof value === 'string' && SHA256_PATTERN.test(value)) return `\\x${value}`;
  if (typeof value === 'string' && BYTEA_SHA256_PATTERN.test(value)) return value;
  throw new WeeklySourceC1AdapterInputError('must be a SHA-256 hex value', path);
}

function normalizeIntegerFields(value, names, path) {
  const result = { ...value };
  for (const name of names) result[name] = integerToBigInt(value[name], `${path}.${name}`);
  return result;
}

function validateStartRequest(request) {
  assertExactKeys(request, START_KEYS, '$.request');
  for (const name of ['request_id', 'actor_user_id', 'candidate_id', 'contract_id', 'root_timesheet_id', 'financial_row_id']) {
    assertUuid(request[name], `$.request.${name}`);
  }
  assertDate(request.week_ending_date, '$.request.week_ending_date');
  if (!SOURCE_MODES.has(request.source_mode)) {
    throw new WeeklySourceC1AdapterInputError(
      'must be NHSP_WEEKLY or HEALTHROSTER_WEEKLY',
      '$.request.source_mode'
    );
  }
  for (const name of ['source_manifest_sha256', 'entitlement_sha256', 'approval_sha256']) {
    assertSha256(request[name], `$.request.${name}`);
  }
  assertBoolean(request.is_zero_entitlement, '$.request.is_zero_entitlement');
  return normalizeIntegerFields(request, [
    'request_sequence', 'expected_head_revision', 'expected_source_count',
    'expected_component_count', 'expected_payload_bytes'
  ], '$.request');
}

function validateSourceRecord(record, path) {
  assertExactKeys(record, SOURCE_RECORD_KEYS, path);
  if (record.record_type !== 'SOURCE') throw new WeeklySourceC1AdapterInputError('must equal SOURCE', `${path}.record_type`);
  if (!SOURCE_AUTHORITY_KINDS.has(record.authority_kind)) {
    throw new WeeklySourceC1AdapterInputError('has an unsupported authority kind', `${path}.authority_kind`);
  }
  for (const name of ['source_id', 'root_timesheet_id', 'candidate_id', 'contract_id']) {
    assertUuid(record[name], `${path}.${name}`);
  }
  for (const name of ['source_system', 'external_identity', 'external_revision']) {
    assertString(record[name], `${path}.${name}`);
  }
  assertSha256(record.source_document_sha256, `${path}.source_document_sha256`);
  assertSha256(record.source_row_sha256, `${path}.source_row_sha256`);
  assertNullable(record.work_date, assertDate, `${path}.work_date`);
  const normalized = normalizeIntegerFields(record, ['source_ordinal', 'payload_bytes', 'part_count'], path);
  if (normalized.payload_bytes < 1n || normalized.payload_bytes > 16_384n) {
    throw new WeeklySourceC1AdapterInputError('must be between 1 and 16384', `${path}.payload_bytes`);
  }
  if (normalized.part_count < 1n || normalized.part_count > 6n) {
    throw new WeeklySourceC1AdapterInputError('must be between 1 and 6', `${path}.part_count`);
  }
  return normalized;
}

function validatePartRecord(record, path) {
  assertExactKeys(record, PART_RECORD_KEYS, path);
  if (record.record_type !== 'PART') throw new WeeklySourceC1AdapterInputError('must equal PART', `${path}.record_type`);
  assertPattern(record.payload_utf8, HEX_BYTES_PATTERN, 'lowercase hexadecimal bytes', `${path}.payload_utf8`);
  const payloadBytes = record.payload_utf8.length / 2;
  if (payloadBytes < 1 || payloadBytes > 3072) {
    throw new WeeklySourceC1AdapterInputError('must contain 1 to 3072 bytes', `${path}.payload_utf8`);
  }
  assertSha256(record.fragment_sha256, `${path}.fragment_sha256`);
  return normalizeIntegerFields(record, ['source_ordinal', 'part_ordinal'], path);
}

function validateComponentRecord(record, path) {
  assertExactKeys(record, COMPONENT_RECORD_KEYS, path);
  if (record.record_type !== 'COMPONENT') {
    throw new WeeklySourceC1AdapterInputError('must equal COMPONENT', `${path}.record_type`);
  }
  assertUuid(record.component_id, `${path}.component_id`);
  for (const name of [
    'source_key', 'component_kind', 'economic_key_type',
    'economic_key_value', 'component_member_identity', 'origin'
  ]) {
    assertString(record[name], `${path}.${name}`);
  }
  for (const name of [
    'segment_id', 'segment_key', 'segment_stable_key', 'reference_number',
    'additional_code_raw', 'expense_code'
  ]) {
    assertNullable(record[name], assertString, `${path}.${name}`);
  }
  assertNullable(record.work_date, assertDate, `${path}.work_date`);
  for (const name of [
    'hours_day', 'hours_night', 'hours_sat', 'hours_sun', 'hours_bh',
    'unit_count', 'unit_pay_rate', 'unit_charge_rate'
  ]) {
    assertNullable(
      record[name],
      (value, valuePath) => assertPattern(value, RATE_PATTERN, 'a fixed six-decimal value', valuePath),
      `${path}.${name}`
    );
  }
  assertPattern(record.pay_ex_vat, MONEY_PATTERN, 'a fixed two-decimal value', `${path}.pay_ex_vat`);
  assertNullable(
    record.charge_ex_vat,
    (value, valuePath) => assertPattern(value, MONEY_PATTERN, 'a fixed two-decimal value', valuePath),
    `${path}.charge_ex_vat`
  );
  assertBoolean(record.exclude_from_pay, `${path}.exclude_from_pay`);
  assertSha256(record.component_sha256, `${path}.component_sha256`);
  return normalizeIntegerFields(record, ['component_ordinal', 'source_ordinal'], path);
}

function validateStageRecords(records) {
  if (!Array.isArray(records)) throw new WeeklySourceC1AdapterInputError('must be an array', '$.records');
  if (records.length > 256) throw new WeeklySourceC1AdapterInputError('must contain at most 256 records', '$.records');
  return records.map((record, index) => {
    const path = `$.records[${index}]`;
    assertPlainObject(record, path);
    if (record.record_type === 'SOURCE') return validateSourceRecord(record, path);
    if (record.record_type === 'PART') return validatePartRecord(record, path);
    if (record.record_type === 'COMPONENT') return validateComponentRecord(record, path);
    throw new WeeklySourceC1AdapterInputError('record_type is not supported', `${path}.record_type`);
  });
}

function validateOperationCursor(operation) {
  assertExactKeys(operation, OPERATION_CURSOR_KEYS, '$.operation');
  assertUuid(operation.operation_id, '$.operation.operation_id');
  assertSha256(operation.receipt_sha256, '$.operation.receipt_sha256');
  return normalizeIntegerFields(operation, ['owner_epoch', 'next_sequence'], '$.operation');
}

function validateScopeCursor(scope) {
  assertExactKeys(scope, SCOPE_CURSOR_KEYS, '$.scope');
  assertUuid(scope.scope_id, '$.scope.scope_id');
  assertSha256(scope.receipt_sha256, '$.scope.receipt_sha256');
  return normalizeIntegerFields(scope, ['owner_epoch', 'next_sequence'], '$.scope');
}

function validateStreamCursor(stream) {
  assertExactKeys(stream, STREAM_CURSOR_KEYS, '$.stream');
  const normalized = normalizeIntegerFields(stream, ['owner_epoch', 'next_sequence'], '$.stream');
  normalized.stream_id = integerToBigInt(stream.stream_id, '$.stream.stream_id', {
    minimum: -32_768n,
    maximum: 32_767n
  });
  assertSha256(stream.receipt_sha256, '$.stream.receipt_sha256');
  return normalized;
}

function validateScopeRequest(request) {
  assertExactKeys(request, SCOPE_REQUEST_KEYS, '$.scopeRequest');
  for (const name of ['scope_id', 'r01_operation_id', 'r01_request_id', 'pay_batch_id', 'actor_user_id']) {
    assertUuid(request[name], `$.scopeRequest.${name}`);
  }
  for (const name of ['base_source_identity_sha256', 'selection_sha256', 'request_sha256']) {
    assertSha256(request[name], `$.scopeRequest.${name}`);
  }
  return normalizeIntegerFields(request, ['source_build_count', 'source_root_count'], '$.scopeRequest');
}

function validateR01Args(args) {
  assertExactKeys(args, R01_KEYS, '$.originalElevenArgs');
  const normalized = { ...args };
  for (const name of [
    'p_operation_id', 'p_request_id', 'p_pay_batch_id', 'p_actor_user_id',
    'p_base_generation_id'
  ]) {
    assertUuid(args[name], `$.originalElevenArgs.${name}`);
  }
  normalized.p_base_revision = integerToBigInt(args.p_base_revision, '$.originalElevenArgs.p_base_revision');
  normalized.p_declared_count = integerToBigInt(args.p_declared_count, '$.originalElevenArgs.p_declared_count');
  assertString(args.p_kind, '$.originalElevenArgs.p_kind');
  for (const name of ['p_policy_sha256', 'p_source_identity_sha256', 'p_selection_sha256']) {
    normalized[name] = byteaSha256(args[name], `$.originalElevenArgs.${name}`);
  }
  return normalized;
}

function validateWorkbenchArgs(args) {
  assertExactKeys(args, WORKBENCH_KEYS, '$.originalSevenArgs');
  for (const name of ['p_job_id', 'p_build_id', 'p_attempt_id', 'p_attempt_nonce']) {
    assertUuid(args[name], `$.originalSevenArgs.${name}`);
  }
  for (const name of ['p_private_stage', 'p_worker_id', 'p_lane_identity']) {
    assertString(args[name], `$.originalSevenArgs.${name}`);
  }
  return { ...args };
}

function encodeLossless(value, seen = new Set(), path = '$') {
  if (value === null) return 'null';
  if (typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'bigint') {
    if (value < MIN_BIGINT || value > MAX_BIGINT) {
      throw new WeeklySourceC1AdapterInputError('BigInt is outside signed bigint range', path);
    }
    return value.toString(10);
  }
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || Object.is(value, -0)) {
      throw new WeeklySourceC1AdapterInputError('Number must be a safe non-negative-zero integer', path);
    }
    return String(value);
  }
  if (Array.isArray(value)) {
    if (seen.has(value)) throw new WeeklySourceC1AdapterInputError('must not contain a cycle', path);
    seen.add(value);
    const encoded = value.map((entry, index) => encodeLossless(entry, seen, `${path}[${index}]`));
    seen.delete(value);
    return `[${encoded.join(',')}]`;
  }
  if (isPlainObject(value)) {
    if (seen.has(value)) throw new WeeklySourceC1AdapterInputError('must not contain a cycle', path);
    seen.add(value);
    const encoded = Object.keys(value).map((key) => (
      `${JSON.stringify(key)}:${encodeLossless(value[key], seen, `${path}.${key}`)}`
    ));
    seen.delete(value);
    return `{${encoded.join(',')}}`;
  }
  throw new WeeklySourceC1AdapterInputError('contains an unsupported value', path);
}

export function stringifyWeeklySourceC1Json(value) {
  return encodeLossless(value);
}

export function parseWeeklySourceC1Json(text) {
  if (typeof text !== 'string') throw new WeeklySourceC1AdapterProtocolError('RPC response must be JSON text');
  try {
    return JSON.parse(text, (key, value, context) => {
      if (typeof value === 'number' && context && INTEGER_TOKEN_PATTERN.test(context.source)) {
        const parsed = BigInt(context.source);
        if (parsed < MIN_BIGINT || parsed > MAX_BIGINT) {
          throw new WeeklySourceC1AdapterProtocolError(`RPC result integer is outside signed bigint range at ${key || '$'}`);
        }
        return parsed;
      }
      if (typeof value === 'number' && !Number.isFinite(value)) {
        throw new WeeklySourceC1AdapterProtocolError(`RPC result contains a non-finite number at ${key || '$'}`);
      }
      return value;
    });
  } catch (error) {
    if (error instanceof WeeklySourceC1AdapterProtocolError) throw error;
    throw new WeeklySourceC1AdapterProtocolError('RPC response is not valid lossless JSON');
  }
}

function assertResultExactKeys(value, expectedKeys, label) {
  if (!isPlainObject(value)) throw new WeeklySourceC1AdapterProtocolError(`${label} must be an object`);
  const actual = Object.keys(value).sort();
  const expected = [...expectedKeys].sort();
  if (actual.length !== expected.length || actual.some((key, index) => key !== expected[index])) {
    throw new WeeklySourceC1AdapterProtocolError(`${label} has missing or unexpected fields`);
  }
}

function validateControlResult(value) {
  assertResultExactKeys(value, CONTROL_KEYS, 'C1 control result');
  if (value.contract !== 'WEEKLY_SOURCE_C1_V1') throw new WeeklySourceC1AdapterProtocolError('C1 control contract is invalid');
  if (typeof value.ok !== 'boolean' || typeof value.has_more !== 'boolean' || typeof value.replayed !== 'boolean' || typeof value.operation_created !== 'boolean') {
    throw new WeeklySourceC1AdapterProtocolError('C1 control boolean field is invalid');
  }
  if (!CONTROL_STATUSES.has(value.status)) throw new WeeklySourceC1AdapterProtocolError('C1 control status is invalid');
  for (const name of ['code', 'phase']) if (typeof value[name] !== 'string') throw new WeeklySourceC1AdapterProtocolError(`C1 control ${name} is invalid`);
  for (const name of ['operation_id', 'scope_id', 'publication_id']) {
    if (value[name] !== null && !UUID_PATTERN.test(value[name])) throw new WeeklySourceC1AdapterProtocolError(`C1 control ${name} is invalid`);
  }
  for (const name of ['request_sha256', 'checkpoint_sha256', 'receipt_sha256']) {
    if (typeof value[name] !== 'string' || !SHA256_PATTERN.test(value[name])) throw new WeeklySourceC1AdapterProtocolError(`C1 control ${name} is invalid`);
  }
  for (const name of ['source_identity_sha256']) {
    if (value[name] !== null && (typeof value[name] !== 'string' || !SHA256_PATTERN.test(value[name]))) {
      throw new WeeklySourceC1AdapterProtocolError(`C1 control ${name} is invalid`);
    }
  }
  for (const name of [
    'owner_epoch', 'sequence', 'next_sequence', 'source_cursor',
    'component_cursor', 'verify_cursor', 'rows_read', 'rows_written',
    'work_used', 'processed_bytes', 'retry_after_ms', 'part_cursor',
    'verify_part_cursor'
  ]) assertIntegerResult(value[name], `$.result.${name}`);
  assertIntegerResult(value.input_records_consumed, '$.result.input_records_consumed', { minimum: 0n, maximum: 256n });
  assertIntegerResult(value.verify_byte_offset, '$.result.verify_byte_offset', { minimum: 0n, maximum: 16_384n });
  if (value.head_revision !== null) assertIntegerResult(value.head_revision, '$.result.head_revision');
  return value;
}

function validateStatusResult(value) {
  assertResultExactKeys(value, STATUS_KEYS, 'C1 status result');
  if (value.contract !== 'WEEKLY_SOURCE_C1_V1') throw new WeeklySourceC1AdapterProtocolError('C1 status contract is invalid');
  if (typeof value.state !== 'string') throw new WeeklySourceC1AdapterProtocolError('C1 status state is invalid');
  if (value.terminal_receipt_sha256 !== null && !SHA256_PATTERN.test(value.terminal_receipt_sha256)) {
    throw new WeeklySourceC1AdapterProtocolError('C1 status terminal receipt is invalid');
  }
  const commonControl = {};
  for (const key of CONTROL_KEYS.slice(0, CONTROL_KEYS.indexOf('input_records_consumed'))) commonControl[key] = value[key];
  const controlWithCursorFields = {
    ...commonControl,
    input_records_consumed: 0n,
    part_cursor: 0n,
    verify_part_cursor: 0n,
    verify_byte_offset: 0n
  };
  validateControlResult(controlWithCursorFields);
  assertIntegerResult(value.retained_sequence_low, '$.result.retained_sequence_low');
  assertIntegerResult(value.retained_sequence_high, '$.result.retained_sequence_high');
  return value;
}

function validateScopeStatusResult(value) {
  assertResultExactKeys(value, SCOPE_STATUS_KEYS, 'C1 scope status result');
  if (!UUID_PATTERN.test(value.scope_id)) throw new WeeklySourceC1AdapterProtocolError('C1 scope status scope_id is invalid');
  if (typeof value.state !== 'string' || typeof value.release_phase !== 'string') {
    throw new WeeklySourceC1AdapterProtocolError('C1 scope status state is invalid');
  }
  for (const name of [
    'owner_epoch', 'next_sequence', 'visited_root_count', 'source_root_count',
    'protected_root_count', 'held_freeze_count', 'held_evidence_count',
    'release_member_cursor'
  ]) assertIntegerResult(value[name], `$.result.${name}`);
  if (value.r01_generation_id !== null && !UUID_PATTERN.test(value.r01_generation_id)) {
    throw new WeeklySourceC1AdapterProtocolError('C1 scope status r01_generation_id is invalid');
  }
  const beginReceipt = value.begin_receipt_sha256;
  if (beginReceipt !== null && !(SHA256_PATTERN.test(beginReceipt) || BYTEA_SHA256_PATTERN.test(beginReceipt))) {
    throw new WeeklySourceC1AdapterProtocolError('C1 scope status begin_receipt_sha256 is invalid');
  }
  if (
    typeof value.last_receipt_sha256 !== 'string' ||
    !(SHA256_PATTERN.test(value.last_receipt_sha256) || BYTEA_SHA256_PATTERN.test(value.last_receipt_sha256))
  ) {
    throw new WeeklySourceC1AdapterProtocolError('C1 scope status last_receipt_sha256 is invalid');
  }
  return value;
}

function normalizeHash(value) {
  return typeof value === 'string' && value.startsWith('\\x') ? value.slice(2) : value;
}

async function sha256Text(text) {
  const bytes = new TextEncoder().encode(text);
  const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((value) => value.toString(16).padStart(2, '0')).join('');
}

function responseText(result, maximumBytes = MAX_RESPONSE_BYTES) {
  if (typeof result !== 'string') {
    throw new WeeklySourceC1AdapterProtocolError(
      'Injected RPC must return the untouched response JSON text; parsed objects are refused to prevent bigint rounding'
    );
  }
  if (maximumBytes !== null && new TextEncoder().encode(result).byteLength > maximumBytes) {
    throw new WeeklySourceC1AdapterProtocolError(`RPC response exceeds the ${maximumBytes}-byte C1 limit`);
  }
  return result;
}

function isKnownOutcomeError(error) {
  return error && (error.outcomeKnown === true || error.unknownOutcome === false);
}

function cursorParams(cursor, idName) {
  return {
    [`p_${idName}`]: cursor[idName],
    p_owner_epoch: cursor.owner_epoch,
    p_sequence: cursor.next_sequence,
    p_previous_receipt_sha256: byteaSha256(cursor.receipt_sha256, `$.${idName}.receipt_sha256`)
  };
}

function controlResultKindFor(method) {
  if (method === 'status') return 'STATUS';
  if (method === 'sourceScopeStatus') return 'SCOPE_STATUS';
  if (method === 'beginR01' || method === 'executeWorkbenchAttempt') return 'PASSTHROUGH';
  return 'CONTROL';
}

function recoveryMetaFor(method, params) {
  if (method === 'start') return { stream_kind: 'START', stream_id: params.p_request_json.request_id };
  if (method === 'prepareSourceScope') return { stream_kind: 'SCOPE', stream_id: params.p_scope_request_json.scope_id };
  if (method === 'status') return { stream_kind: 'READ_OPERATION', stream_id: params.p_operation_id };
  if (method === 'sourceScopeStatus') return { stream_kind: 'READ_SCOPE', stream_id: params.p_scope_id };
  if (Object.prototype.hasOwnProperty.call(params, 'p_operation_id') && !['beginR01'].includes(method)) {
    return { stream_kind: 'OPERATION', stream_id: params.p_operation_id };
  }
  if (Object.prototype.hasOwnProperty.call(params, 'p_scope_id')) {
    return { stream_kind: 'SCOPE', stream_id: params.p_scope_id };
  }
  return { stream_kind: 'UNRECOVERABLE', stream_id: null };
}

function validatePreparedParams(method, params) {
  assertPlainObject(params, '$.parameters');
  switch (method) {
    case 'start':
      assertExactKeys(params, ['p_request_json'], '$.parameters');
      return { p_request_json: validateStartRequest(params.p_request_json) };
    case 'stage': {
      assertExactKeys(params, ['p_operation_id', 'p_owner_epoch', 'p_sequence', 'p_previous_receipt_sha256', 'p_records_json'], '$.parameters');
      assertUuid(params.p_operation_id, '$.parameters.p_operation_id');
      return {
        p_operation_id: params.p_operation_id,
        p_owner_epoch: integerToBigInt(params.p_owner_epoch, '$.parameters.p_owner_epoch'),
        p_sequence: integerToBigInt(params.p_sequence, '$.parameters.p_sequence'),
        p_previous_receipt_sha256: byteaSha256(params.p_previous_receipt_sha256, '$.parameters.p_previous_receipt_sha256'),
        p_records_json: validateStageRecords(params.p_records_json)
      };
    }
    case 'continueValidation':
    case 'certify':
    case 'publish': {
      assertExactKeys(params, ['p_operation_id', 'p_owner_epoch', 'p_sequence', 'p_previous_receipt_sha256'], '$.parameters');
      assertUuid(params.p_operation_id, '$.parameters.p_operation_id');
      return {
        p_operation_id: params.p_operation_id,
        p_owner_epoch: integerToBigInt(params.p_owner_epoch, '$.parameters.p_owner_epoch'),
        p_sequence: integerToBigInt(params.p_sequence, '$.parameters.p_sequence'),
        p_previous_receipt_sha256: byteaSha256(params.p_previous_receipt_sha256, '$.parameters.p_previous_receipt_sha256')
      };
    }
    case 'setPending':
    case 'abort': {
      assertExactKeys(params, ['p_operation_id', 'p_owner_epoch', 'p_sequence', 'p_previous_receipt_sha256', 'p_reason'], '$.parameters');
      const base = validatePreparedParams('continueValidation', {
        p_operation_id: params.p_operation_id,
        p_owner_epoch: params.p_owner_epoch,
        p_sequence: params.p_sequence,
        p_previous_receipt_sha256: params.p_previous_receipt_sha256
      });
      assertString(params.p_reason, '$.parameters.p_reason');
      if (method === 'setPending' && !PENDING_REASONS.has(params.p_reason)) {
        throw new WeeklySourceC1AdapterInputError('has an unsupported pending reason', '$.parameters.p_reason');
      }
      return { ...base, p_reason: params.p_reason };
    }
    case 'retire':
    case 'repair': {
      const hashName = method === 'retire' ? 'p_stop_ack_sha256' : 'p_checkpoint_sha256';
      assertExactKeys(params, ['p_operation_id', 'p_owner_epoch', 'p_sequence', 'p_previous_receipt_sha256', hashName], '$.parameters');
      const base = validatePreparedParams('continueValidation', {
        p_operation_id: params.p_operation_id,
        p_owner_epoch: params.p_owner_epoch,
        p_sequence: params.p_sequence,
        p_previous_receipt_sha256: params.p_previous_receipt_sha256
      });
      return { ...base, [hashName]: byteaSha256(params[hashName], `$.parameters.${hashName}`) };
    }
    case 'status':
      assertExactKeys(params, ['p_operation_id'], '$.parameters');
      assertUuid(params.p_operation_id, '$.parameters.p_operation_id');
      return { p_operation_id: params.p_operation_id };
    case 'prepareSourceScope':
      assertExactKeys(params, ['p_scope_request_json'], '$.parameters');
      return { p_scope_request_json: validateScopeRequest(params.p_scope_request_json) };
    case 'continueSourceScope':
      assertExactKeys(params, ['p_scope_id', 'p_owner_epoch', 'p_sequence', 'p_previous_receipt_sha256'], '$.parameters');
      assertUuid(params.p_scope_id, '$.parameters.p_scope_id');
      return {
        p_scope_id: params.p_scope_id,
        p_owner_epoch: integerToBigInt(params.p_owner_epoch, '$.parameters.p_owner_epoch'),
        p_sequence: integerToBigInt(params.p_sequence, '$.parameters.p_sequence'),
        p_previous_receipt_sha256: byteaSha256(params.p_previous_receipt_sha256, '$.parameters.p_previous_receipt_sha256')
      };
    case 'abortSourceScope':
      assertExactKeys(params, ['p_scope_id', 'p_owner_epoch', 'p_sequence', 'p_previous_receipt_sha256', 'p_stop_ack_sha256'], '$.parameters');
      return {
        ...validatePreparedParams('continueSourceScope', {
          p_scope_id: params.p_scope_id,
          p_owner_epoch: params.p_owner_epoch,
          p_sequence: params.p_sequence,
          p_previous_receipt_sha256: params.p_previous_receipt_sha256
        }),
        p_stop_ack_sha256: byteaSha256(params.p_stop_ack_sha256, '$.parameters.p_stop_ack_sha256')
      };
    case 'releaseSourceScope':
      assertExactKeys(params, ['p_scope_id', 'p_owner_epoch', 'p_sequence', 'p_previous_receipt_sha256', 'p_terminal_operation_id', 'p_authority_receipt_sha256'], '$.parameters');
      assertUuid(params.p_terminal_operation_id, '$.parameters.p_terminal_operation_id');
      return {
        ...validatePreparedParams('continueSourceScope', {
          p_scope_id: params.p_scope_id,
          p_owner_epoch: params.p_owner_epoch,
          p_sequence: params.p_sequence,
          p_previous_receipt_sha256: params.p_previous_receipt_sha256
        }),
        p_terminal_operation_id: params.p_terminal_operation_id,
        p_authority_receipt_sha256: byteaSha256(params.p_authority_receipt_sha256, '$.parameters.p_authority_receipt_sha256')
      };
    case 'sourceScopeStatus':
      assertExactKeys(params, ['p_scope_id'], '$.parameters');
      assertUuid(params.p_scope_id, '$.parameters.p_scope_id');
      return { p_scope_id: params.p_scope_id };
    case 'beginR01':
      return validateR01Args(params);
    case 'executeWorkbenchAttempt':
      return validateWorkbenchArgs(params);
    case 'cleanup':
      assertExactKeys(params, ['p_stream_id', 'p_owner_epoch', 'p_sequence', 'p_previous_receipt_sha256'], '$.parameters');
      return {
        p_stream_id: integerToBigInt(params.p_stream_id, '$.parameters.p_stream_id', { minimum: -32_768n, maximum: 32_767n }),
        p_owner_epoch: integerToBigInt(params.p_owner_epoch, '$.parameters.p_owner_epoch'),
        p_sequence: integerToBigInt(params.p_sequence, '$.parameters.p_sequence'),
        p_previous_receipt_sha256: byteaSha256(params.p_previous_receipt_sha256, '$.parameters.p_previous_receipt_sha256')
      };
    default:
      throw new WeeklySourceC1AdapterInputError('method is not recoverable by this adapter', '$.method');
  }
}

function recoveryDecision({ ok, decision, code, status = null, result = null, replayAttempted = false }) {
  return Object.freeze({
    contract: 'WEEKLY_SOURCE_C1_RECOVERY_V1',
    ok,
    decision,
    code,
    status,
    result,
    replay_attempted: replayAttempted
  });
}

export class WeeklySourceC1Adapter {
  #rpc;

  constructor(rpc) {
    if (typeof rpc !== 'function') {
      throw new WeeklySourceC1AdapterInputError('must be a function', '$.rpc');
    }
    this.#rpc = rpc;
  }

  async #decodeResult(rawResult, resultKind) {
    const rawText = responseText(rawResult, resultKind === 'PASSTHROUGH' ? null : MAX_RESPONSE_BYTES);
    const parsed = parseWeeklySourceC1Json(rawText);
    if (resultKind === 'CONTROL') return validateControlResult(parsed);
    if (resultKind === 'STATUS') return validateStatusResult(parsed);
    if (resultKind === 'SCOPE_STATUS') return validateScopeStatusResult(parsed);
    return parsed;
  }

  async #makeUnknownCall(method, rpcName, parametersJson, resultKind, recoveryMeta, recoveryAttempted = false) {
    return Object.freeze({
      contract: 'WEEKLY_SOURCE_C1_UNKNOWN_CALL_V1',
      method,
      rpc_name: rpcName,
      parameters_json: parametersJson,
      parameters_sha256: await sha256Text(parametersJson),
      result_kind: resultKind,
      stream_kind: recoveryMeta.stream_kind,
      stream_id: recoveryMeta.stream_id,
      recovery_attempted: recoveryAttempted
    });
  }

  async #invokePrepared(method, params, { recoveryAttempted = false } = {}) {
    const rpcName = RPC[method];
    if (!rpcName) throw new WeeklySourceC1AdapterInputError('method is not supported', '$.method');
    const normalized = validatePreparedParams(method, params);
    const parametersJson = stringifyWeeklySourceC1Json(normalized);
    const limit = method === 'stage' ? MAX_STAGE_REQUEST_BYTES : MAX_CONTROL_REQUEST_BYTES;
    if (new TextEncoder().encode(parametersJson).byteLength > limit) {
      throw new WeeklySourceC1AdapterInputError(`encoded RPC parameters exceed ${limit} bytes`, '$.parameters');
    }
    const resultKind = controlResultKindFor(method);
    const recoveryMeta = recoveryMetaFor(method, normalized);
    const unknownCall = await this.#makeUnknownCall(
      method,
      rpcName,
      parametersJson,
      resultKind,
      recoveryMeta,
      recoveryAttempted
    );
    try {
      const rawResult = await this.#rpc(rpcName, parametersJson, RPC_OPTIONS);
      return await this.#decodeResult(rawResult, resultKind);
    } catch (error) {
      if (
        error instanceof WeeklySourceC1AdapterInputError ||
        error instanceof WeeklySourceC1AdapterProtocolError ||
        isKnownOutcomeError(error)
      ) {
        throw error;
      }
      throw new WeeklySourceC1UnknownOutcomeError(unknownCall, error);
    }
  }

  async start(request) {
    return this.#invokePrepared('start', { p_request_json: request });
  }

  async stage(operation, records) {
    const cursor = validateOperationCursor(operation);
    return this.#invokePrepared('stage', {
      ...cursorParams(cursor, 'operation_id'),
      p_records_json: records
    });
  }

  async continueValidation(operation) {
    const cursor = validateOperationCursor(operation);
    return this.#invokePrepared('continueValidation', cursorParams(cursor, 'operation_id'));
  }

  async certify(operation) {
    const cursor = validateOperationCursor(operation);
    return this.#invokePrepared('certify', cursorParams(cursor, 'operation_id'));
  }

  async publish(operation) {
    const cursor = validateOperationCursor(operation);
    return this.#invokePrepared('publish', cursorParams(cursor, 'operation_id'));
  }

  async setPending(operation, reason) {
    const cursor = validateOperationCursor(operation);
    return this.#invokePrepared('setPending', { ...cursorParams(cursor, 'operation_id'), p_reason: reason });
  }

  async abort(operation, reason) {
    const cursor = validateOperationCursor(operation);
    return this.#invokePrepared('abort', { ...cursorParams(cursor, 'operation_id'), p_reason: reason });
  }

  async retire(operation, stopAck) {
    const cursor = validateOperationCursor(operation);
    return this.#invokePrepared('retire', {
      ...cursorParams(cursor, 'operation_id'),
      p_stop_ack_sha256: stopAck
    });
  }

  async repair(operation, checkpoint) {
    const cursor = validateOperationCursor(operation);
    return this.#invokePrepared('repair', {
      ...cursorParams(cursor, 'operation_id'),
      p_checkpoint_sha256: checkpoint
    });
  }

  async status(operationId) {
    return this.#invokePrepared('status', { p_operation_id: operationId });
  }

  async prepareSourceScope(scopeRequest) {
    return this.#invokePrepared('prepareSourceScope', { p_scope_request_json: scopeRequest });
  }

  async continueSourceScope(scope) {
    const cursor = validateScopeCursor(scope);
    return this.#invokePrepared('continueSourceScope', cursorParams(cursor, 'scope_id'));
  }

  async beginR01(originalElevenArgs) {
    return this.#invokePrepared('beginR01', originalElevenArgs);
  }

  async sourceScopeStatus(scopeId) {
    return this.#invokePrepared('sourceScopeStatus', { p_scope_id: scopeId });
  }

  async abortSourceScope(scope, stopAck) {
    const cursor = validateScopeCursor(scope);
    return this.#invokePrepared('abortSourceScope', {
      ...cursorParams(cursor, 'scope_id'),
      p_stop_ack_sha256: stopAck
    });
  }

  async releaseSourceScope(scope, terminalOperationId, terminalHash) {
    const cursor = validateScopeCursor(scope);
    return this.#invokePrepared('releaseSourceScope', {
      ...cursorParams(cursor, 'scope_id'),
      p_terminal_operation_id: terminalOperationId,
      p_authority_receipt_sha256: terminalHash
    });
  }

  async executeWorkbenchAttempt(originalSevenArgs) {
    return this.#invokePrepared('executeWorkbenchAttempt', originalSevenArgs);
  }

  async cleanup(stream) {
    const cursor = validateStreamCursor(stream);
    return this.#invokePrepared('cleanup', {
      p_stream_id: cursor.stream_id,
      p_owner_epoch: cursor.owner_epoch,
      p_sequence: cursor.next_sequence,
      p_previous_receipt_sha256: cursor.receipt_sha256
    });
  }

  async recoverUnknown(savedCall) {
    assertExactKeys(savedCall, UNKNOWN_CALL_KEYS, '$.savedCall');
    if (savedCall.contract !== 'WEEKLY_SOURCE_C1_UNKNOWN_CALL_V1') {
      throw new WeeklySourceC1AdapterInputError('has an invalid recovery contract', '$.savedCall.contract');
    }
    if (RPC[savedCall.method] !== savedCall.rpc_name) {
      throw new WeeklySourceC1AdapterInputError('method and rpc_name do not match', '$.savedCall.rpc_name');
    }
    assertString(savedCall.parameters_json, '$.savedCall.parameters_json');
    assertSha256(savedCall.parameters_sha256, '$.savedCall.parameters_sha256');
    assertString(savedCall.result_kind, '$.savedCall.result_kind');
    assertString(savedCall.stream_kind, '$.savedCall.stream_kind');
    if (savedCall.stream_id !== null) assertUuid(savedCall.stream_id, '$.savedCall.stream_id');
    assertBoolean(savedCall.recovery_attempted, '$.savedCall.recovery_attempted');
    if (savedCall.recovery_attempted) {
      return recoveryDecision({
        ok: false,
        decision: 'REFUSED',
        code: 'C1_RECOVERY_ALREADY_ATTEMPTED'
      });
    }
    if (await sha256Text(savedCall.parameters_json) !== savedCall.parameters_sha256) {
      throw new WeeklySourceC1AdapterInputError('parameters_json digest does not match', '$.savedCall.parameters_sha256');
    }
    const parsedParams = parseWeeklySourceC1Json(savedCall.parameters_json);
    const normalizedParams = validatePreparedParams(savedCall.method, parsedParams);
    const canonicalParams = stringifyWeeklySourceC1Json(normalizedParams);
    if (canonicalParams !== savedCall.parameters_json) {
      throw new WeeklySourceC1AdapterInputError('parameters_json is not canonical', '$.savedCall.parameters_json');
    }
    if (savedCall.result_kind !== controlResultKindFor(savedCall.method)) {
      throw new WeeklySourceC1AdapterInputError('result_kind does not match method', '$.savedCall.result_kind');
    }
    const expectedRecoveryMeta = recoveryMetaFor(savedCall.method, normalizedParams);
    if (
      savedCall.stream_kind !== expectedRecoveryMeta.stream_kind ||
      savedCall.stream_id !== expectedRecoveryMeta.stream_id
    ) {
      throw new WeeklySourceC1AdapterInputError('recovery stream does not match the saved method parameters', '$.savedCall.stream_kind');
    }

    if (savedCall.stream_kind === 'START') {
      try {
        const result = await this.#invokePrepared('start', normalizedParams, { recoveryAttempted: true });
        return recoveryDecision({
          ok: true,
          decision: 'REPLAYED',
          code: 'C1_RECOVERY_START_EXACT_REPLAY',
          result,
          replayAttempted: true
        });
      } catch (error) {
        if (error instanceof WeeklySourceC1UnknownOutcomeError) {
          error.recoveryCall = Object.freeze({ ...savedCall, recovery_attempted: true });
        }
        throw error;
      }
    }

    if (savedCall.stream_kind === 'READ_OPERATION' || savedCall.stream_kind === 'READ_SCOPE') {
      const result = savedCall.stream_kind === 'READ_OPERATION'
        ? await this.status(savedCall.stream_id)
        : await this.sourceScopeStatus(savedCall.stream_id);
      return recoveryDecision({
        ok: true,
        decision: 'REPLAYED_READ',
        code: 'C1_RECOVERY_READ_REPLAY',
        result,
        replayAttempted: true
      });
    }

    if (!['OPERATION', 'SCOPE'].includes(savedCall.stream_kind)) {
      return recoveryDecision({
        ok: false,
        decision: 'REFUSED',
        code: 'C1_RECOVERY_STATUS_ID_UNAVAILABLE'
      });
    }

    let status;
    if (savedCall.stream_kind === 'SCOPE' && savedCall.method === 'prepareSourceScope') {
      let statusProvedAbsent = false;
      try {
        status = await this.sourceScopeStatus(savedCall.stream_id);
      } catch (error) {
        statusProvedAbsent = isKnownOutcomeError(error) && (
          error.notFound === true ||
          error.status === 404 ||
          error.code === 'C1_SCOPE_NOT_FOUND'
        );
        if (!statusProvedAbsent) throw error;
      }
      if (!statusProvedAbsent && status.scope_id !== savedCall.stream_id) {
        throw new WeeklySourceC1AdapterProtocolError('C1 scope status returned a different scope');
      }
      const result = await this.#invokePrepared('prepareSourceScope', normalizedParams, { recoveryAttempted: true });
      return recoveryDecision({
        ok: true,
        decision: 'REPLAYED',
        code: statusProvedAbsent
          ? 'C1_RECOVERY_SCOPE_START_EXACT_REPLAY'
          : 'C1_RECOVERY_SCOPE_START_VERIFIED_REPLAY',
        status,
        result,
        replayAttempted: true
      });
    }

    status = savedCall.stream_kind === 'OPERATION'
      ? await this.status(savedCall.stream_id)
      : await this.sourceScopeStatus(savedCall.stream_id);
    if (
      (savedCall.stream_kind === 'OPERATION' && status.operation_id !== savedCall.stream_id) ||
      (savedCall.stream_kind === 'SCOPE' && status.scope_id !== savedCall.stream_id)
    ) {
      throw new WeeklySourceC1AdapterProtocolError('C1 recovery status returned a different stream identity');
    }
    const sentSequence = integerToBigInt(normalizedParams.p_sequence, '$.savedCall.parameters_json.p_sequence');
    const nextSequence = integerToBigInt(status.next_sequence, '$.status.next_sequence');
    const previousReceipt = normalizeHash(normalizedParams.p_previous_receipt_sha256);
    const currentReceipt = normalizeHash(
      savedCall.stream_kind === 'OPERATION' ? status.receipt_sha256 : status.last_receipt_sha256
    );

    if (
      nextSequence === sentSequence + 1n &&
      savedCall.stream_kind === 'OPERATION' &&
      integerToBigInt(status.sequence, '$.status.sequence') !== sentSequence
    ) {
      return recoveryDecision({
        ok: false,
        decision: 'REFUSED',
        code: 'C1_RECOVERY_CHECKPOINT_CONFLICT',
        status
      });
    }
    if (nextSequence === sentSequence + 1n) {
      if (savedCall.method === 'stage') {
        try {
          const result = await this.#invokePrepared('stage', normalizedParams, { recoveryAttempted: true });
          return recoveryDecision({
            ok: true,
            decision: 'REPLAYED',
            code: 'C1_RECOVERY_STAGE_COMMITTED_EXACT_REPLAY',
            status,
            result,
            replayAttempted: true
          });
        } catch (error) {
          if (error instanceof WeeklySourceC1UnknownOutcomeError) {
            error.recoveryCall = Object.freeze({ ...savedCall, recovery_attempted: true });
          }
          throw error;
        }
      }
      return recoveryDecision({
        ok: true,
        decision: 'COMMITTED',
        code: 'C1_RECOVERY_COMMITTED_RECEIPT',
        status,
        result: status
      });
    }
    if (nextSequence > sentSequence + 1n) {
      if (savedCall.stream_kind === 'OPERATION') {
        const retainedLow = integerToBigInt(status.retained_sequence_low, '$.status.retained_sequence_low');
        const retainedHigh = integerToBigInt(status.retained_sequence_high, '$.status.retained_sequence_high');
        if (sentSequence < retainedLow || sentSequence > retainedHigh) {
          return recoveryDecision({
            ok: false,
            decision: 'REFUSED',
            code: 'C1_RECEIPT_COMPACTED',
            status
          });
        }
        const result = await this.#invokePrepared(savedCall.method, normalizedParams, { recoveryAttempted: true });
        return recoveryDecision({
          ok: true,
          decision: 'REPLAYED',
          code: 'C1_RECOVERY_RETAINED_EXACT_REPLAY',
          status,
          result,
          replayAttempted: true
        });
      }
      return recoveryDecision({
        ok: false,
        decision: 'REFUSED',
        code: 'C1_RECOVERY_RECEIPT_NOT_CURRENT',
        status
      });
    }
    if (nextSequence < sentSequence || currentReceipt !== previousReceipt) {
      return recoveryDecision({
        ok: false,
        decision: 'REFUSED',
        code: 'C1_RECOVERY_CHECKPOINT_CONFLICT',
        status
      });
    }

    const result = await this.#invokePrepared(savedCall.method, normalizedParams, { recoveryAttempted: true });
    return recoveryDecision({
      ok: true,
      decision: 'REPLAYED',
      code: 'C1_RECOVERY_EXACT_REPLAY',
      status,
      result,
      replayAttempted: true
    });
  }
}

export function createWeeklySourceC1Adapter(rpc) {
  return new WeeklySourceC1Adapter(rpc);
}

export const WEEKLY_SOURCE_C1_RPC_NAMES = RPC;
