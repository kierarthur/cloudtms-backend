const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH = /^[0-9a-f]{64}$/;
const INTEGER = /^(?:0|[1-9][0-9]*)$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const MONEY = /^-?(?:0|[1-9][0-9]*)\.\d{2}$/;
const RATE = /^-?(?:0|[1-9][0-9]*)\.\d{6}$/;

const SOURCE_COLUMNS = Object.freeze([
  ['source_ordinal', 'I'],
  ['source_id', 'U'],
  ['authority_kind', 'T'],
  ['source_system', 'T'],
  ['external_identity', 'T'],
  ['external_revision', 'T'],
  ['source_document_sha256', 'H'],
  ['payload_bytes', 'I'],
  ['part_count', 'I'],
  ['work_date', 'D?'],
  ['root_timesheet_id', 'U'],
  ['candidate_id', 'U'],
  ['contract_id', 'U'],
]);

const COMPONENT_COLUMNS = Object.freeze([
  ['component_ordinal', 'I'],
  ['component_id', 'U'],
  ['source_ordinal', 'I'],
  ['source_key', 'T'],
  ['component_kind', 'T'],
  ['economic_key_type', 'T'],
  ['economic_key_value', 'T'],
  ['component_member_identity', 'T'],
  ['segment_id', 'T?'],
  ['segment_key', 'T?'],
  ['segment_stable_key', 'T?'],
  ['work_date', 'D?'],
  ['reference_number', 'T?'],
  ['hours_day', 'R?'],
  ['hours_night', 'R?'],
  ['hours_sat', 'R?'],
  ['hours_sun', 'R?'],
  ['hours_bh', 'R?'],
  ['additional_code_raw', 'T?'],
  ['unit_count', 'R?'],
  ['unit_pay_rate', 'R?'],
  ['unit_charge_rate', 'R?'],
  ['expense_code', 'T?'],
  // S7 (WB-007, WB-013, 24 section 5): no 'adjustment_id' column.  An
  // adjustment is never copied into the immutable head, so it is neither
  // carried nor folded into the C1 component TLV digest.
  ['pay_ex_vat', 'M'],
  ['charge_ex_vat', 'M?'],
  ['exclude_from_pay', 'B'],
  ['origin', 'T'],
]);

const START_COLUMNS = Object.freeze([
  ['request_id', 'U'],
  ['request_sequence', 'I'],
  ['actor_user_id', 'U'],
  ['candidate_id', 'U'],
  ['contract_id', 'U'],
  ['root_timesheet_id', 'U'],
  ['week_ending_date', 'D'],
  ['source_mode', 'T'],
  ['expected_head_revision', 'I'],
  ['expected_source_count', 'I'],
  ['expected_component_count', 'I'],
  ['expected_payload_bytes', 'I'],
  ['source_manifest_sha256', 'H'],
  ['entitlement_sha256', 'H'],
  ['approval_sha256', 'H'],
  ['is_zero_entitlement', 'B'],
  ['financial_row_id', 'U'],
]);

const encoder = new TextEncoder();

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function concat(...values) {
  const length = values.reduce((sum, value) => sum + value.byteLength, 0);
  const output = new Uint8Array(length);
  let offset = 0;
  for (const value of values) {
    output.set(value, offset);
    offset += value.byteLength;
  }
  return output;
}

function uint32(value) {
  const output = new Uint8Array(4);
  new DataView(output.buffer).setUint32(0, value, false);
  return output;
}

function uint64(value) {
  const parsed = BigInt(value);
  if (parsed < 0n || parsed > 0xffffffffffffffffn) {
    fail('C1_TLV_INTEGER_RANGE', 'The C1 stream counter is outside the unsigned 64-bit range.');
  }
  const output = new Uint8Array(8);
  new DataView(output.buffer).setBigUint64(0, parsed, false);
  return output;
}

function hexBytes(value, pattern, code, label) {
  const normalised = String(value ?? '').trim().toLowerCase();
  if (!pattern.test(normalised)) fail(code, `${label} is invalid.`);
  return Uint8Array.from(normalised.match(/../g).map((pair) => Number.parseInt(pair, 16)));
}

function uuidBytes(value) {
  return hexBytes(
    String(value ?? '').replaceAll('-', ''),
    /^[0-9a-f]{32}$/,
    'C1_TLV_UUID_INVALID',
    'UUID',
  );
}

function hashBytes(value) {
  return hexBytes(value, HASH, 'C1_TLV_HASH_INVALID', 'SHA-256');
}

function integerBytes(value) {
  const normalised = String(value ?? '');
  if (!INTEGER.test(normalised)) fail('C1_TLV_INTEGER_INVALID', 'The C1 integer is invalid.');
  return encoder.encode(normalised);
}

function dateBytes(value) {
  const normalised = String(value ?? '');
  if (!DATE.test(normalised)) fail('C1_TLV_DATE_INVALID', 'The C1 date is invalid.');
  return encoder.encode(normalised);
}

function decimalBytes(value, pattern, code) {
  const normalised = String(value ?? '');
  if (!pattern.test(normalised) || Object.is(Number(normalised), -0)) {
    fail(code, 'The C1 decimal is invalid.');
  }
  return encoder.encode(normalised);
}

function field(tag, bytes) {
  return concat(encoder.encode(tag), uint32(bytes.byteLength), bytes);
}

function typedField(type, value) {
  const nullable = type.endsWith('?');
  const base = nullable ? type.slice(0, -1) : type;
  if (value == null) {
    if (!nullable) fail('C1_TLV_NULL_INVALID', 'A required C1 field is null.');
    return field('Z', new Uint8Array());
  }
  if (base === 'U') return field('U', uuidBytes(value));
  if (base === 'H') return field('H', hashBytes(value));
  if (base === 'I') return field('I', integerBytes(value));
  if (base === 'D') return field('D', dateBytes(value));
  if (base === 'T') return field('T', encoder.encode(String(value)));
  if (base === 'B') {
    if (value !== true && value !== false) fail('C1_TLV_BOOLEAN_INVALID', 'The C1 boolean is invalid.');
    return field('B', Uint8Array.of(value ? 1 : 0));
  }
  if (base === 'M') return field('M', decimalBytes(value, MONEY, 'C1_TLV_MONEY_INVALID'));
  if (base === 'R') return field('R', decimalBytes(value, RATE, 'C1_TLV_RATE_INVALID'));
  fail('C1_TLV_TYPE_INVALID', 'The C1 TLV type is unsupported.', { type });
}

async function digest(bytes) {
  return new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
}

function hex(bytes) {
  return [...bytes].map((value) => value.toString(16).padStart(2, '0')).join('');
}

async function tupleDigest(domain, columns, value, { exclude = new Set(), suffix = [] } = {}) {
  const fields = [field('T', encoder.encode(domain))];
  for (const [name, type] of columns) {
    if (!exclude.has(name)) fields.push(typedField(type, value[name]));
  }
  fields.push(...suffix);
  return digest(concat(...fields));
}

async function fold(domain, items) {
  let state = await digest(field('T', encoder.encode(domain)));
  for (let index = 0; index < items.length; index += 1) {
    state = await digest(concat(state, uint64(index + 1), items[index]));
  }
  return digest(concat(
    field('T', encoder.encode(`${domain}/EOF`)),
    state,
    uint64(items.length),
  ));
}

function requireUuid(value, label) {
  const normalised = String(value ?? '').trim().toLowerCase();
  if (!UUID.test(normalised)) fail('C1_AUTHORING_UUID_INVALID', `${label} is invalid.`);
  return normalised;
}

function utf8Document(document) {
  if (!document || typeof document !== 'object' || Array.isArray(document)) {
    fail('C1_AUTHORING_DOCUMENT_INVALID', 'A normalized C1 evidence document is invalid.');
  }
  const bytes = encoder.encode(JSON.stringify(document));
  if (bytes.byteLength < 1 || bytes.byteLength > 16_384) {
    fail('C1_AUTHORING_DOCUMENT_SIZE_INVALID', 'A normalized C1 evidence document has an invalid size.');
  }
  return bytes;
}

function splitParts(bytes) {
  const parts = [];
  for (let offset = 0; offset < bytes.byteLength; offset += 3_072) {
    parts.push(bytes.slice(offset, Math.min(offset + 3_072, bytes.byteLength)));
  }
  if (parts.length < 1 || parts.length > 6) {
    fail('C1_AUTHORING_PART_COUNT_INVALID', 'A normalized C1 evidence document has too many parts.');
  }
  return parts;
}

export async function hashWeeklySourceC1OfficeIntent(input) {
  const values = [
    ['agency_id', 'U'], ['actor_user_id', 'U'], ['request_id', 'U'],
    ['root_timesheet_id', 'U'], ['candidate_id', 'U'], ['contract_id', 'U'],
    ['week_ending_date', 'D'], ['entitlement_sha256', 'H'], ['decision', 'T'],
  ];
  return hex(await tupleDigest('C1/OFFICE_INTENT/1', values, input));
}

export async function authorWeeklySourceC1Stream(input = {}) {
  const agencyId = requireUuid(input.agency_id, 'Agency');
  const sources = Array.isArray(input.sources) ? input.sources : [];
  const components = Array.isArray(input.components) ? input.components : [];
  if (sources.length < 1 || sources.length > 65_536 || components.length > 65_536) {
    fail('C1_AUTHORING_STREAM_COUNT_INVALID', 'The C1 source or component count is invalid.');
  }

  const sourceRecords = [];
  const sourceHashes = [];
  let payloadBytes = 0;
  for (let index = 0; index < sources.length; index += 1) {
    const supplied = sources[index];
    if (String(supplied.source_ordinal) !== String(index + 1)) {
      fail('C1_AUTHORING_SOURCE_ORDER_INVALID', 'C1 source records are not in contiguous order.');
    }
    const documentBytes = utf8Document(supplied.document);
    const normalizedDocumentHash = await digest(documentBytes);
    const parts = splitParts(documentBytes);
    const suppliedDocumentHash = String(supplied.source_document_sha256 ?? '')
      .trim()
      .toLowerCase();
    const documentAuthorityHash = String(supplied.document?.document_sha256 ?? '')
      .trim()
      .toLowerCase();
    if (!HASH.test(suppliedDocumentHash) || suppliedDocumentHash !== documentAuthorityHash) {
      fail(
        'C1_AUTHORING_DOCUMENT_AUTHORITY_MISMATCH',
        'The normalized C1 evidence record does not bind the supplied original evidence digest.',
      );
    }
    const record = {
      ...supplied,
      source_id: requireUuid(supplied.source_id, 'Source record'),
      source_document_sha256: suppliedDocumentHash,
      payload_bytes: String(documentBytes.byteLength),
      part_count: String(parts.length),
    };
    delete record.document;
    const rowHash = await tupleDigest('C1/SOURCE/1', SOURCE_COLUMNS, record, {
      suffix: [typedField('H', hex(normalizedDocumentHash))],
    });
    record.source_row_sha256 = hex(rowHash);
    record.record_type = 'SOURCE';
    sourceRecords.push(Object.freeze({
      ...record,
      parts: Object.freeze(await Promise.all(parts.map(async (part, partIndex) => Object.freeze({
        source_ordinal: index + 1,
        part_ordinal: partIndex + 1,
        payload_utf8: hex(part),
        fragment_sha256: hex(await digest(part)),
        record_type: 'PART',
      })))),
    }));
    sourceHashes.push(rowHash);
    payloadBytes += documentBytes.byteLength;
  }

  const sourceOrdinals = new Set(sourceRecords.map((record) => Number(record.source_ordinal)));
  const componentRecords = [];
  const componentHashes = [];
  let previousSourceKey = null;
  for (let index = 0; index < components.length; index += 1) {
    const supplied = components[index];
    if (String(supplied.component_ordinal) !== String(index + 1)) {
      fail('C1_AUTHORING_COMPONENT_ORDER_INVALID', 'C1 components are not in contiguous order.');
    }
    if (!sourceOrdinals.has(Number(supplied.source_ordinal))) {
      fail('C1_AUTHORING_COMPONENT_SOURCE_INVALID', 'A C1 component has no source record.');
    }
    const sourceKey = String(supplied.source_key ?? '');
    if (!sourceKey || (previousSourceKey != null && sourceKey <= previousSourceKey)) {
      fail('C1_AUTHORING_COMPONENT_KEY_ORDER_INVALID', 'C1 component keys are not strictly increasing.');
    }
    previousSourceKey = sourceKey;
    const record = {
      ...supplied,
      component_id: requireUuid(supplied.component_id, 'Component'),
    };
    const rowHash = await tupleDigest('C1/COMPONENT/1', COMPONENT_COLUMNS, record);
    record.component_sha256 = hex(rowHash);
    record.record_type = 'COMPONENT';
    componentRecords.push(Object.freeze(record));
    componentHashes.push(rowHash);
  }

  return Object.freeze({
    agency_id: agencyId,
    sources: Object.freeze(sourceRecords),
    components: Object.freeze(componentRecords),
    source_manifest_sha256: hex(await fold('C1/SOURCE/1', sourceHashes)),
    entitlement_sha256: hex(await fold('C1/COMPONENT/1', componentHashes)),
    expected_source_count: String(sourceRecords.length),
    expected_component_count: String(componentRecords.length),
    expected_payload_bytes: String(payloadBytes),
    is_zero_entitlement: componentRecords.length === 0,
  });
}

export async function bindWeeklySourceC1Start(input = {}) {
  const agencyId = requireUuid(input.agency_id, 'Agency');
  const start = Object.fromEntries(START_COLUMNS.map(([name]) => [name, input[name]]));
  const approvalColumns = START_COLUMNS.filter(([name]) => name !== 'approval_sha256');
  start.approval_sha256 = hex(await tupleDigest(
    'C1/OFFICE_APPROVAL/1',
    [['agency_id', 'U'], ...approvalColumns],
    { agency_id: agencyId, ...start },
  ));
  const requestSha256 = hex(await tupleDigest(
    'C1/REQUEST/1',
    [['agency_id', 'U'], ...START_COLUMNS],
    { agency_id: agencyId, ...start },
  ));
  return Object.freeze({ start: Object.freeze(start), request_sha256: requestSha256 });
}

export const WEEKLY_SOURCE_C1_AUTHORING_CONTRACT = Object.freeze({
  tlv: 'C1-TLV-1',
  sourceDomain: 'C1/SOURCE/1',
  componentDomain: 'C1/COMPONENT/1',
  officeIntentDomain: 'C1/OFFICE_INTENT/1',
  officeApprovalDomain: 'C1/OFFICE_APPROVAL/1',
  requestDomain: 'C1/REQUEST/1',
});
