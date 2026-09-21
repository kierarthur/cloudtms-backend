import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';

const SHA256 = /^[a-f0-9]{64}$/;
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[1-8a-f][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i;
const ALLOWED_CATEGORIES = new Set(['COMPLETE_ENTITLEMENT', 'CERTIFIED_EMPTY', 'WAITING', 'REFUSED']);
const FORBIDDEN_RESULT_FIELDS = /residual|headroom|amount_due|recovery_due|draft|provider|settlement|remittance/i;
const NON_NEGATIVE_INTEGER = /^(?:0|[1-9][0-9]*)$/;

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceC1ContractEmulatorError';
  error.code = code;
  error.details = deepFreeze(cloneJson(details));
  throw error;
}

function object(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    fail('C1_EMULATOR_INPUT_INVALID', `${label} must be a JSON object.`);
  }
  return value;
}

function assertNoCalculatedBankingResult(value, path = '$') {
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertNoCalculatedBankingResult(item, `${path}[${index}]`));
    return;
  }
  if (!value || typeof value !== 'object') return;
  for (const [key, item] of Object.entries(value)) {
    if (FORBIDDEN_RESULT_FIELDS.test(key)) {
      fail('C1_EMULATOR_BANKING_RESULT_FORBIDDEN', `${path}.${key} is owned by Workbench/Banking Pay and cannot be emulated.`);
    }
    assertNoCalculatedBankingResult(item, `${path}.${key}`);
  }
}

function integer(value, label) {
  const text = String(value ?? '');
  if (!NON_NEGATIVE_INTEGER.test(text)) {
    fail('C1_EMULATOR_STAGE_CONTRACT_INVALID', `${label} must be a canonical non-negative integer.`);
  }
  const number = Number(text);
  if (!Number.isSafeInteger(number)) {
    fail('C1_EMULATOR_STAGE_CONTRACT_INVALID', `${label} is outside the safe harness range.`);
  }
  return number;
}

function assertProvenanceRows(request) {
  request.c1_sources.forEach((source, index) => {
    object(source, `c1_sources[${index}]`);
    if (
      source.record_type !== 'SOURCE'
      || integer(source.source_ordinal, `c1_sources[${index}].source_ordinal`) !== index + 1
      || !UUID.test(String(source.source_id ?? ''))
      || !SHA256.test(String(source.source_row_sha256 ?? ''))
      || typeof source.authority_kind !== 'string'
      || !source.authority_kind
      || typeof source.source_system !== 'string'
      || !source.source_system
      || typeof source.external_identity !== 'string'
      || !source.external_identity
      || !Array.isArray(source.parts)
    ) {
      fail('C1_EMULATOR_SOURCE_PROVENANCE_INVALID', `c1_sources[${index}] has incomplete provenance.`);
    }
  });
  request.c1_components.forEach((component, index) => {
    object(component, `c1_components[${index}]`);
    const sourceOrdinal = integer(component.source_ordinal, `c1_components[${index}].source_ordinal`);
    if (
      component.record_type !== 'COMPONENT'
      || integer(component.component_ordinal, `c1_components[${index}].component_ordinal`) !== index + 1
      || !UUID.test(String(component.component_id ?? ''))
      || !SHA256.test(String(component.component_sha256 ?? ''))
      || sourceOrdinal < 1
      || sourceOrdinal > request.c1_sources.length
      || typeof component.source_key !== 'string'
      || !component.source_key
      || typeof component.component_kind !== 'string'
      || !component.component_kind
    ) {
      fail('C1_EMULATOR_COMPONENT_PROVENANCE_INVALID', `c1_components[${index}] has incomplete provenance.`);
    }
  });
}

function normalizeStageRequest(value) {
  const request = object(value, 'C1 stage request');
  if (request.schema_version !== 'WEEKLY_PROTECTED_C1_STAGE_REQUEST_V1') {
    fail('C1_EMULATOR_STAGE_CONTRACT_INVALID', 'The C1 stage request uses the wrong schema version.');
  }
  const c1Request = object(request.c1_request, 'c1_request');
  if (!UUID.test(String(c1Request.request_id ?? ''))) {
    fail('C1_EMULATOR_STAGE_CONTRACT_INVALID', 'The C1 request identity is invalid.');
  }
  if (!SHA256.test(String(c1Request.request_sha256 ?? ''))) {
    fail('C1_EMULATOR_STAGE_CONTRACT_INVALID', 'The C1 request seal is invalid.');
  }
  if (!Array.isArray(request.c1_sources) || !Array.isArray(request.c1_components)) {
    fail('C1_EMULATOR_STAGE_CONTRACT_INVALID', 'The complete C1 source/component stream is unavailable.');
  }
  if (!['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY'].includes(c1Request.source_mode)) {
    fail('C1_EMULATOR_STAGE_CONTRACT_INVALID', 'The C1 source mode is not an approved Weekly source mode.');
  }
  for (const field of ['source_manifest_sha256', 'entitlement_sha256', 'approval_sha256']) {
    if (!SHA256.test(String(c1Request[field] ?? ''))) {
      fail('C1_EMULATOR_STAGE_CONTRACT_INVALID', `c1_request.${field} is invalid.`);
    }
  }
  const sourceCount = integer(c1Request.expected_source_count, 'c1_request.expected_source_count');
  const componentCount = integer(c1Request.expected_component_count, 'c1_request.expected_component_count');
  if (sourceCount !== request.c1_sources.length || componentCount !== request.c1_components.length) {
    fail('C1_EMULATOR_STAGE_COUNT_MISMATCH', 'The C1 request counts do not match its complete evidence stream.');
  }
  if (typeof c1Request.is_zero_entitlement !== 'boolean'
      || c1Request.is_zero_entitlement !== (componentCount === 0)) {
    fail('C1_EMULATOR_EMPTY_CERTIFICATION_INVALID', 'The certified-empty flag disagrees with the component stream.');
  }
  assertProvenanceRows(request);
  assertNoCalculatedBankingResult(request);
  return deepFreeze(cloneJson(request));
}

function normalizeDeclaredResponse(value) {
  const response = object(value, 'Declared C1 response');
  if (!ALLOWED_CATEGORIES.has(response.category)) {
    fail('C1_EMULATOR_RESPONSE_INVALID', 'The declared C1 response category is unsupported.');
  }
  if (typeof response.ok !== 'boolean' || !response.response_code || typeof response.response_code !== 'string') {
    fail('C1_EMULATOR_RESPONSE_INVALID', 'The declared C1 response is incomplete.');
  }
  assertNoCalculatedBankingResult(response);
  return deepFreeze(cloneJson(response));
}

function assertResponseSemantics(request, response) {
  const empty = request.c1_request.is_zero_entitlement;
  if (response.ok && !['COMPLETE_ENTITLEMENT', 'CERTIFIED_EMPTY'].includes(response.category)) {
    fail('C1_EMULATOR_RESPONSE_INVALID', 'An accepted response must be complete entitlement or certified empty.');
  }
  if (!response.ok && !['WAITING', 'REFUSED'].includes(response.category)) {
    fail('C1_EMULATOR_RESPONSE_INVALID', 'A non-accepted response must be waiting or refused.');
  }
  if (response.category === 'CERTIFIED_EMPTY' && !empty) {
    fail('C1_EMULATOR_EMPTY_CERTIFICATION_INVALID', 'A non-empty component stream cannot be certified empty.');
  }
  if (response.category === 'COMPLETE_ENTITLEMENT' && empty) {
    fail('C1_EMULATOR_EMPTY_CERTIFICATION_INVALID', 'An empty component stream must use the certified-empty category.');
  }
}

export function createC1ContractEmulator({ exchanges = [], verifyRequestSeal = null } = {}) {
  if (!Array.isArray(exchanges)) throw new TypeError('exchanges must be an array');
  if (verifyRequestSeal !== null && typeof verifyRequestSeal !== 'function') {
    throw new TypeError('verifyRequestSeal must be a function when supplied');
  }
  const queue = exchanges.map((exchange, index) => {
    const item = object(exchange, `exchanges[${index}]`);
    if (!SHA256.test(String(item.expectedRequestSha256 ?? ''))) {
      fail('C1_EMULATOR_EXCHANGE_INVALID', `Exchange ${index + 1} has no valid request seal.`);
    }
    if (!UUID.test(String(item.expectedRequestId ?? '')) || !SHA256.test(String(item.expectedInputDigest ?? ''))) {
      fail('C1_EMULATOR_EXCHANGE_INVALID', `Exchange ${index + 1} has no exact request identity/input digest.`);
    }
    return {
      expectedRequestId: item.expectedRequestId,
      expectedRequestSha256: item.expectedRequestSha256,
      expectedInputDigest: item.expectedInputDigest,
      response: normalizeDeclaredResponse(item.response),
    };
  });
  const calls = [];
  const seen = new Map();
  return Object.freeze({
    contract: WEEKLY_SOURCE_C1_EMULATOR_CONTRACT,
    async publish(stageRequest) {
      const request = normalizeStageRequest(stageRequest);
      const inputDigest = canonicalDigest(request);
      const requestId = request.c1_request.request_id;
      const replay = seen.get(requestId);
      if (replay) {
        if (replay.requestSha256 !== request.c1_request.request_sha256 || replay.inputDigest !== inputDigest) {
          fail('C1_EMULATOR_IDEMPOTENCY_CONFLICT', 'A repeated C1 request identity changed its sealed input.');
        }
        calls.push(deepFreeze({
          sequence: calls.length + 1,
          requestId,
          requestSha256: replay.requestSha256,
          inputDigest,
          responseCategory: replay.response.category,
          replay: true,
        }));
        return cloneJson(replay.response);
      }
      const exchange = queue.shift();
      if (!exchange) fail('C1_EMULATOR_EXCHANGE_MISSING', 'No declared C1 emulator exchange remains.');
      if (requestId !== exchange.expectedRequestId) {
        fail('C1_EMULATOR_REQUEST_ID_MISMATCH', 'The C1 request identity does not match the declared exchange.');
      }
      if (request.c1_request.request_sha256 !== exchange.expectedRequestSha256) {
        fail('C1_EMULATOR_REQUEST_SEAL_MISMATCH', 'The staged C1 request does not match the declared exchange.', {
          expected: exchange.expectedRequestSha256,
          actual: request.c1_request.request_sha256,
        });
      }
      if (inputDigest !== exchange.expectedInputDigest) {
        fail('C1_EMULATOR_REQUEST_BODY_MISMATCH', 'The complete staged C1 request differs from the declared sealed input.');
      }
      if (verifyRequestSeal && await verifyRequestSeal(cloneJson(request)) !== true) {
        fail('C1_EMULATOR_REQUEST_SEAL_INVALID', 'The injected product seal verifier rejected the C1 request.');
      }
      assertResponseSemantics(request, exchange.response);
      seen.set(requestId, deepFreeze({
        requestSha256: request.c1_request.request_sha256,
        inputDigest,
        response: exchange.response,
      }));
      calls.push(deepFreeze({
        sequence: calls.length + 1,
        requestId,
        requestSha256: request.c1_request.request_sha256,
        inputDigest,
        responseCategory: exchange.response.category,
        replay: false,
      }));
      return cloneJson(exchange.response);
    },
    calls() {
      return cloneJson(calls);
    },
    assertComplete() {
      if (queue.length) fail('C1_EMULATOR_EXCHANGE_UNUSED', `${queue.length} declared C1 emulator exchange(s) were not used.`);
      return deepFreeze({
        complete: true,
        callCount: calls.length,
        uniqueExchangeCount: seen.size,
        callDigest: canonicalDigest(calls),
        releaseEvidenceEligible: false,
      });
    },
    assertFinalReleaseEligible() {
      fail(
        'C1_EMULATOR_NOT_RELEASE_EVIDENCE',
        'The C1 contract emulator validates sealed transport only and cannot satisfy final end-to-end release evidence.',
      );
    },
  });
}

export const WEEKLY_SOURCE_C1_EMULATOR_CONTRACT = deepFreeze({
  version: 'WEEKLY_SOURCE_C1_CONTRACT_EMULATOR_V1',
  testOnly: true,
  validatesSealedTransportOnly: true,
  requiresExactDeclaredInputDigest: true,
  validatesIdempotentReplay: true,
  validatesCertifiedEmptySemantics: true,
  supportsInjectedProductSealVerifier: true,
  calculatesResidual: false,
  createsDraft: false,
  executesPayment: false,
  releaseEvidenceEligible: false,
});
