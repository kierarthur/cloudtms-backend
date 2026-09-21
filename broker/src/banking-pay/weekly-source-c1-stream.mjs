import {
  authorWeeklySourceC1Stream,
  hashWeeklySourceC1OfficeIntent,
} from './weekly-source-c1-authoring.mjs';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH = /^[0-9a-f]{64}$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const SOURCE_MODES = new Set(['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY']);
const encoder = new TextEncoder();

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function object(value, code, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(code, `${label} is unavailable.`);
  return value;
}

function uuid(value, label) {
  const result = String(value ?? '').trim().toLowerCase();
  if (!UUID.test(result)) fail('C1_STREAM_UUID_INVALID', `${label} is invalid.`);
  return result;
}

function hash(value, label) {
  const result = String(value ?? '').trim().toLowerCase();
  if (!HASH.test(result)) fail('C1_STREAM_HASH_INVALID', `${label} is invalid.`);
  return result;
}

function text(value, label) {
  const result = String(value ?? '').trim();
  if (!result) fail('C1_STREAM_TEXT_INVALID', `${label} is unavailable.`);
  return result;
}

function integer(value, label) {
  const result = Number(value);
  if (!Number.isSafeInteger(result) || result < 0) fail('C1_STREAM_INTEGER_INVALID', `${label} is invalid.`);
  return result;
}

async function deterministicPrivateUuid(seed) {
  const bytes = new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(seed)));
  bytes[6] = (bytes[6] & 0x0f) | 0x80;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = [...bytes.slice(0, 16)].map((value) => value.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function common(input) {
  const weekEndingDate = text(input.week_ending_date, 'Week ending date');
  if (!DATE.test(weekEndingDate)) fail('C1_STREAM_DATE_INVALID', 'Week ending date is invalid.');
  const sourceMode = text(input.source_mode, 'Weekly source mode').toUpperCase();
  if (!SOURCE_MODES.has(sourceMode)) fail('C1_STREAM_MODE_INVALID', 'Weekly source mode is invalid.');
  return Object.freeze({
    agency_id: uuid(input.agency_id, 'Agency'),
    actor_user_id: uuid(input.actor_user_id, 'Office user'),
    request_id: uuid(input.request_id, 'Publication request'),
    candidate_id: uuid(input.candidate_id, 'Candidate'),
    contract_id: uuid(input.contract_id, 'Contract'),
    root_timesheet_id: uuid(input.root_timesheet_id, 'Root Timesheet'),
    week_ending_date: weekEndingDate,
    source_mode: sourceMode,
  });
}

function headerAndDocument(kind, sourceId, commonFacts, fact, document) {
  const sourceSystem = text(fact.source_system, `${kind} source system`);
  const externalIdentity = text(fact.external_identity, `${kind} external identity`);
  const externalRevision = text(fact.external_revision, `${kind} external revision`);
  const documentSha256 = hash(fact.document_sha256, `${kind} evidence digest`);
  return {
    source_id: sourceId,
    authority_kind: kind,
    source_system: sourceSystem,
    external_identity: externalIdentity,
    external_revision: externalRevision,
    source_document_sha256: documentSha256,
    work_date: fact.work_date ?? null,
    root_timesheet_id: commonFacts.root_timesheet_id,
    candidate_id: commonFacts.candidate_id,
    contract_id: commonFacts.contract_id,
    document: {
      contract: `C1_${kind}_V1`,
      source_id: sourceId,
      root_timesheet_id: commonFacts.root_timesheet_id,
      candidate_id: commonFacts.candidate_id,
      contract_id: commonFacts.contract_id,
      week_ending_date: commonFacts.week_ending_date,
      source_system: sourceSystem,
      external_identity: externalIdentity,
      external_revision: externalRevision,
      document_sha256: documentSha256,
      ...document,
    },
  };
}

function componentDocument(component) {
  return {
    component_ordinal: component.component_ordinal,
    component_id: component.component_id,
    source_ordinal: component.source_ordinal,
    source_key: component.source_key,
    component_kind: component.component_kind,
    economic_key_type: component.economic_key_type,
    economic_key_value: component.economic_key_value,
    component_member_identity: component.component_member_identity,
    segment_id: component.segment_id,
    segment_key: component.segment_key,
    segment_stable_key: component.segment_stable_key,
    work_date: component.work_date,
    reference_number: component.reference_number,
    hours_day: component.hours_day,
    hours_night: component.hours_night,
    hours_sat: component.hours_sat,
    hours_sun: component.hours_sun,
    hours_bh: component.hours_bh,
    additional_code_raw: component.additional_code_raw,
    unit_count: component.unit_count,
    unit_pay_rate: component.unit_pay_rate,
    unit_charge_rate: component.unit_charge_rate,
    expense_code: component.expense_code,
    // S7 (WB-007, WB-013, 24 section 5): no 'adjustment_id'.  An adjustment is
    // never copied into the immutable head.
    pay_ex_vat: component.pay_ex_vat,
    charge_ex_vat: component.charge_ex_vat,
    exclude_from_pay: component.exclude_from_pay,
    origin: component.origin,
  };
}

async function privateId(commonFacts, type, key, factory) {
  if (factory) return uuid(await factory(type, key), `${type} source record`);
  return deterministicPrivateUuid(
    `CLOUDTMS/WEEKLY_SOURCE/C1/PRIVATE_ID/1\u0000${commonFacts.agency_id}\u0000${commonFacts.request_id}\u0000${type}\u0000${key}`,
  );
}

/**
 * Build the complete normalized C1 evidence stream for one protected Weekly
 * target.  All financial components must already come from the established
 * Weekly calculation owner.  This function only binds evidence and approval.
 */
export async function buildWeeklyProtectedC1Stream(input = {}) {
  const shared = common(input);
  const clientFacts = Array.isArray(input.client_sources) ? [...input.client_sources] : [];
  if (!clientFacts.length) fail('C1_STREAM_CLIENT_SOURCE_REQUIRED', 'At least one complete client-source observation is required.');
  clientFacts.sort((left, right) => (
    String(left.external_identity ?? '').localeCompare(String(right.external_identity ?? ''))
    || String(left.external_revision ?? '').localeCompare(String(right.external_revision ?? ''))
  ));
  const componentRows = Array.isArray(input.components) ? input.components : [];
  const componentKeys = new Set();
  let priorKey = null;
  for (let index = 0; index < componentRows.length; index += 1) {
    const component = object(componentRows[index]?.component, 'C1_STREAM_COMPONENT_INVALID', 'Approved component');
    if (integer(component.component_ordinal, 'Component ordinal') !== index + 1) {
      fail('C1_STREAM_COMPONENT_ORDER_INVALID', 'Approved component ordinals are not contiguous.');
    }
    const key = text(component.source_key, 'Approved component key');
    if (componentKeys.has(key) || (priorKey != null && key <= priorKey)) {
      fail('C1_STREAM_COMPONENT_ORDER_INVALID', 'Approved component keys are not strictly increasing.');
    }
    componentKeys.add(key);
    priorKey = key;
  }

  const sources = [];
  const append = (row) => {
    row.source_ordinal = sources.length + 1;
    sources.push(row);
    return row.source_ordinal;
  };
  const factory = typeof input.source_id_factory === 'function' ? input.source_id_factory : null;

  for (const factInput of clientFacts) {
    const fact = object(factInput, 'C1_STREAM_CLIENT_SOURCE_INVALID', 'Client-source observation');
    if (fact.source_complete !== true || typeof fact.source_present !== 'boolean') {
      fail('C1_STREAM_CLIENT_SOURCE_INCOMPLETE', 'The client-source observation is not complete.');
    }
    const sourceId = await privateId(shared, 'CLIENT_SOURCE', fact.external_identity, factory);
    append(headerAndDocument('CLIENT_SOURCE', sourceId, shared, fact, {
      client_source_id: uuid(fact.client_source_id, 'Client-source observation'),
      source_complete: true,
      source_present: fact.source_present,
      approved_minutes: integer(fact.approved_minutes, 'Approved client-source minutes'),
    }));
  }

  if (input.candidate_submission) {
    const fact = object(input.candidate_submission, 'C1_STREAM_CANDIDATE_SOURCE_INVALID', 'Candidate submission');
    const sourceId = await privateId(shared, 'CANDIDATE_SUBMISSION', fact.external_identity, factory);
    append(headerAndDocument('CANDIDATE_SUBMISSION', sourceId, shared, fact, {
      submitted_by_candidate_id: uuid(fact.submitted_by_candidate_id, 'Submitting Candidate'),
      submission_id: uuid(fact.submission_id, 'Candidate submission'),
      submitted_at_utc: text(fact.submitted_at_utc, 'Candidate submission time'),
      submitted_minutes: integer(fact.submitted_minutes, 'Candidate submitted minutes'),
    }));
  }

  const rootFact = object(input.root_financial, 'C1_STREAM_ROOT_FINANCIAL_INVALID', 'Root financial authority');
  const rootSourceId = await privateId(shared, 'ROOT_FINANCIAL', rootFact.external_identity, factory);
  append(headerAndDocument('ROOT_FINANCIAL', rootSourceId, shared, rootFact, {
    financial_row_id: uuid(rootFact.financial_row_id, 'Financial row'),
    root_version: integer(rootFact.root_version, 'Root version'),
    financial_timesheet_version: integer(rootFact.financial_timesheet_version, 'Financial Timesheet version'),
    financial_revision_digest: text(rootFact.financial_revision_digest, 'Financial revision digest'),
  }));

  const providerFact = object(input.provider, 'C1_STREAM_PROVIDER_INVALID', 'Provider authority');
  const providerSourceId = await privateId(shared, 'PROVIDER', providerFact.external_identity, factory);
  append(headerAndDocument('PROVIDER', providerSourceId, shared, providerFact, {
    source_pay_method: text(providerFact.source_pay_method, 'Source pay method').toUpperCase(),
    umbrella_id: providerFact.umbrella_id == null ? null : uuid(providerFact.umbrella_id, 'Source Umbrella'),
    provider_authority_sha256: hash(providerFact.provider_authority_sha256, 'Provider authority digest'),
    target_pay_method: text(providerFact.target_pay_method, 'Target pay method').toUpperCase(),
    target_umbrella_id: providerFact.target_umbrella_id == null
      ? null
      : uuid(providerFact.target_umbrella_id, 'Target Umbrella'),
    target_enabled: providerFact.target_enabled ?? null,
    target_vat_chargeable: providerFact.target_vat_chargeable ?? null,
  }));

  // The component fold is independent of source-document bytes.  We first
  // assign every final source ordinal, calculate that fold, then bind the
  // resulting entitlement digest into each normalized APPROVED_COMPONENT
  // authority record.  This avoids a self-hash cycle and does not trust a
  // browser-supplied financial digest.
  const approvedComponentIndexes = [];
  const approvedDocumentPlaceholder = '0'.repeat(64);
  const components = [];
  for (const row of componentRows) {
    const component = { ...row.component };
    const authority = object(row.authority ?? {}, 'C1_STREAM_COMPONENT_AUTHORITY_INVALID', 'Component authority');
    const kind = text(authority.kind, 'Component authority kind').toUpperCase();
    const sourceId = await privateId(shared, kind, component.source_key, factory);
    let source;
    if (kind === 'APPROVED_COMPONENT') {
      const fact = {
        source_system: 'CLOUDTMS_WEEKLY_SOURCE',
        external_identity: component.component_id,
        external_revision: text(input.approved_component_revision ?? '1', 'Approved component revision'),
        document_sha256: approvedDocumentPlaceholder,
        work_date: component.work_date,
      };
      component.source_ordinal = sources.length + 1;
      source = headerAndDocument(kind, sourceId, shared, fact, componentDocument(component));
      approvedComponentIndexes.push(sources.length);
    } else if (kind === 'SOURCE_EXPENSE') {
      const fact = {
        source_system: text(authority.source_system ?? 'CLOUDTMS_WEEKLY_SOURCE', 'Source-expense system'),
        external_identity: text(authority.external_identity ?? authority.source_expense_id, 'Source-expense identity'),
        external_revision: text(authority.external_revision ?? '1', 'Source-expense revision'),
        document_sha256: hash(authority.document_sha256, 'Source-expense evidence digest'),
        work_date: component.work_date,
      };
      component.source_ordinal = sources.length + 1;
      source = headerAndDocument(kind, sourceId, shared, fact, {
        source_expense_id: uuid(authority.source_expense_id, 'Source expense'),
        expense_code: component.expense_code,
        pay_ex_vat: component.pay_ex_vat,
        charge_ex_vat: component.charge_ex_vat,
      });
    } else if (kind === 'ORDINARY_EXPENSE') {
      const fact = {
        source_system: 'CLOUDTMS_TIMESHEET_FINANCIAL',
        external_identity: rootFact.financial_row_id,
        external_revision: String(rootFact.financial_timesheet_version),
        document_sha256: rootFact.document_sha256,
        work_date: component.work_date,
      };
      component.source_ordinal = sources.length + 1;
      source = headerAndDocument(kind, sourceId, shared, fact, {
        financial_row_id: uuid(rootFact.financial_row_id, 'Financial row'),
        expense_code: component.expense_code,
        pay_ex_vat: component.pay_ex_vat,
        charge_ex_vat: component.charge_ex_vat,
      });
    } else {
      // S7 (WB-007, WB-013, 24 section 5): 'NONADVANCE_ADJUSTMENT' is not a
      // permitted authority kind here.  An adjustment is never copied into the
      // immutable head; it stays independently owned and is composed exactly
      // once by the Workbench selector, outside the head.
      fail('C1_STREAM_COMPONENT_AUTHORITY_INVALID', 'The component authority kind is not permitted.');
    }
    append(source);
    components.push(component);
  }

  const preApproval = await authorWeeklySourceC1Stream({
    agency_id: shared.agency_id,
    sources,
    components,
  });
  if (
    input.expected_entitlement_sha256 != null
    && preApproval.entitlement_sha256 !== hash(input.expected_entitlement_sha256, 'Expected entitlement digest')
  ) {
    fail(
      'C1_STREAM_APPROVED_ENTITLEMENT_MISMATCH',
      'The approved complete-vector digest does not match the exact physical components.',
      { calculated: preApproval.entitlement_sha256 },
    );
  }
  for (const sourceIndex of approvedComponentIndexes) {
    sources[sourceIndex] = {
      ...sources[sourceIndex],
      source_document_sha256: preApproval.entitlement_sha256,
      document: {
        ...sources[sourceIndex].document,
        document_sha256: preApproval.entitlement_sha256,
      },
    };
  }
  const intent = await hashWeeklySourceC1OfficeIntent({
    agency_id: shared.agency_id,
    actor_user_id: shared.actor_user_id,
    request_id: shared.request_id,
    root_timesheet_id: shared.root_timesheet_id,
    candidate_id: shared.candidate_id,
    contract_id: shared.contract_id,
    week_ending_date: shared.week_ending_date,
    entitlement_sha256: preApproval.entitlement_sha256,
    decision: 'APPROVE_ENTITLEMENT',
  });
  const officeSourceId = await privateId(shared, 'OFFICE_APPROVAL', shared.request_id, factory);
  append(headerAndDocument('OFFICE_APPROVAL', officeSourceId, shared, {
    source_system: 'CLOUDTMS_OFFICE',
    external_identity: shared.request_id,
    external_revision: text(input.office_approval_revision ?? '1', 'Office approval revision'),
    document_sha256: intent,
    work_date: null,
  }, {
    actor_user_id: shared.actor_user_id,
    request_id: shared.request_id,
    approval_intent_sha256: intent,
    entitlement_sha256: preApproval.entitlement_sha256,
    decision: 'APPROVE_ENTITLEMENT',
  }));

  const stream = await authorWeeklySourceC1Stream({
    agency_id: shared.agency_id,
    sources,
    components,
  });
  return Object.freeze({
    ...shared,
    office_approval_intent_sha256: intent,
    stream,
  });
}

export const WEEKLY_SOURCE_C1_STREAM_CONTRACT = Object.freeze({
  version: 'WEEKLY_SOURCE_C1_NORMALIZED_STREAM_V1',
  ordinaryRootOnly: true,
  calculatesResidual: false,
  sourceModes: Object.freeze([...SOURCE_MODES]),
});
