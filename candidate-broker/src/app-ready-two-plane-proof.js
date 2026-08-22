import { signCandidatePrivateRequest } from '../../broker/src/candidate-service-auth.js';
import {
  candidateRouteContextInternals,
  signCandidateRouteContext
} from '../../broker/src/candidate-route-context.js';
import {
  CANDIDATE_OPERATION_POLICY,
  CANDIDATE_OPERATION_POLICY_SEMANTIC_SHA256
} from './candidate-operation-policy.js';
import { candidateDataPlaneRegistryEntry } from './candidate-data-plane-registry.generated.js';
import { controlPlaneRpc } from './control-plane-client.js';

export const APP_READY_TWO_PLANE_PROOF_PATH = '/__app-ready/v1/two-plane-matrix';
const PAGE_SIZE = 9;
const MAX_RESPONSE_BYTES = 256 * 1024;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

class AppReadyProofError extends Error {
  constructor(status, code) {
    super(code);
    this.status = status;
    this.code = code;
  }
}

function text(value) {
  return String(value == null ? '' : value).trim();
}

function proofEnabled(env) {
  return text(env.CANDIDATE_APP_ENVIRONMENT).toUpperCase() === 'TEST'
    && text(env.CANDIDATE_APP_READY_PROOF_ENABLED).toUpperCase() === 'TRUE';
}

function json(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff'
    }
  });
}

function mutateUuid(value) {
  const source = text(value).toLowerCase();
  if (!UUID_RE.test(source)) throw new AppReadyProofError(502, 'APP_READY_FIXTURE_INVALID');
  return `${source[0] === '0' ? '1' : '0'}${source.slice(1)}`;
}

function fixtureContext(operation, fixture, now = new Date(), overrides = {}) {
  return {
    v: 1,
    aud: 'candidate-private-api',
    operation_id: operation.operation_id,
    environment: 'TEST',
    global_account_id: text(fixture.global_account_id).toLowerCase(),
    global_session_id: text(fixture.global_session_id).toLowerCase(),
    membership_id: text(fixture.membership_id).toLowerCase(),
    membership_generation: Number(fixture.membership_generation),
    agency_id: text(fixture.agency_id).toLowerCase(),
    agency_candidate_id: text(fixture.local_candidate_id).toLowerCase(),
    data_plane_id: text(fixture.data_plane_id).toLowerCase(),
    route_version: Number(fixture.route_version),
    session_epoch: Number(fixture.session_epoch),
    issued_at_utc: now.toISOString(),
    expires_at_utc: new Date(now.getTime() + 4 * 60 * 1000).toISOString(),
    key_version: 1,
    ...overrides
  };
}

async function signedContext(operation, fixture, env, overrides = {}) {
  const registry = candidateDataPlaneRegistryEntry(fixture.registry_binding_key, env);
  if (!registry) throw new AppReadyProofError(503, 'APP_READY_BINDING_UNAVAILABLE');
  return {
    registry,
    signed: await signCandidateRouteContext(fixtureContext(operation, fixture, new Date(), overrides), {
      secret: registry.routeContextSecret,
      keyVersion: registry.keyVersion
    })
  };
}

async function boundedResponse(response) {
  const declared = Number(response.headers.get('content-length') || 0);
  if (declared > MAX_RESPONSE_BYTES) throw new AppReadyProofError(502, 'APP_READY_PROBE_RESPONSE_INVALID');
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_RESPONSE_BYTES) {
    throw new AppReadyProofError(502, 'APP_READY_PROBE_RESPONSE_INVALID');
  }
  try {
    return { status: response.status, body: bytes.byteLength ? JSON.parse(new TextDecoder().decode(bytes)) : null };
  } catch {
    throw new AppReadyProofError(502, 'APP_READY_PROBE_RESPONSE_INVALID');
  }
}

async function sendPrivateProbe(registry, operation, outerContext, cases, env, faultMode = null) {
  const headers = new Headers({
    'content-type': 'application/json; charset=utf-8',
    'x-cloudtms-route-context': outerContext.envelope,
    'x-cloudtms-route-context-sha256': outerContext.sha256
  });
  const unsigned = new Request('https://cloudtms-candidate-private.internal/private/app-ready/v1/route-probe', {
    method: 'POST', headers,
    body: JSON.stringify({
      operation_id: operation.operation_id,
      method: operation.method,
      path: operation.path,
      cases,
      ...(faultMode ? { fault_mode: faultMode } : {})
    })
  });
  return registry.binding.fetch(await signCandidatePrivateRequest(unsigned, env));
}

async function sendTamperedPrivateProbe(registry, operation, outerContext, env) {
  const body = {
    operation_id: operation.operation_id,
    method: operation.method,
    path: operation.path,
    cases: [{ case_id: 'positive', envelope: outerContext.envelope, sha256: outerContext.sha256 }]
  };
  const original = new Request('https://cloudtms-candidate-private.internal/private/app-ready/v1/route-probe', {
    method: 'POST',
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'x-cloudtms-route-context': outerContext.envelope,
      'x-cloudtms-route-context-sha256': outerContext.sha256
    },
    body: JSON.stringify({ ...body, semantic_key: 'ORIGINAL' })
  });
  const signed = await signCandidatePrivateRequest(original, env);
  return registry.binding.fetch(new Request(signed.url, {
    method: signed.method,
    headers: signed.headers,
    body: JSON.stringify({ ...body, semantic_key: 'CHANGED_AFTER_SERVICE_SIGNATURE' })
  }));
}

function controlCase(caseId, operation, fixture, context, extra = {}) {
  return {
    case_id: `${operation.operation_id}_${caseId}`,
    operation_id: operation.operation_id,
    method: operation.method,
    path: operation.path,
    fixture_key: fixture?.fixture_key || null,
    client_selector_present: false,
    context,
    ...extra
  };
}

function controlCases(operation, fixtures) {
  if (!operation.data_plane_dispatch_required) {
    return [
      controlCase('neutral', operation, null, null),
      controlCase('selector', operation, null, null, { client_selector_present: true }),
      controlCase('context', operation, fixtures[0], fixtureContext(operation, fixtures[0]))
    ];
  }
  const fixture = fixtures[0];
  const other = fixtures[1];
  const valid = fixtureContext(operation, fixture);
  return [
    controlCase('positive_a', operation, fixture, valid),
    controlCase('positive_b', operation, other, fixtureContext(operation, other)),
    controlCase('wrong_candidate', operation, fixture, {
      ...valid, agency_candidate_id: mutateUuid(valid.agency_candidate_id)
    }),
    controlCase('wrong_membership', operation, fixture, {
      ...valid, membership_generation: valid.membership_generation + 1
    }),
    controlCase('wrong_session_epoch', operation, fixture, {
      ...valid, session_epoch: valid.session_epoch + 1
    }),
    controlCase('stale_route', operation, fixture, { ...valid, route_version: valid.route_version + 1 }),
    controlCase('wrong_agency', operation, fixture, { ...valid, agency_id: other.agency_id }),
    controlCase('wrong_plane', operation, fixture, { ...valid, data_plane_id: other.data_plane_id }),
    controlCase('wrong_audience', operation, fixture, { ...valid, aud: 'wrong-audience' }),
    controlCase('missing', operation, fixture, null),
    controlCase('selector', operation, fixture, valid, { client_selector_present: true })
  ];
}

function controlResultMap(validation) {
  if (validation?.ok !== true || validation?.status !== 'VALIDATED' || !Array.isArray(validation.results)) {
    throw new AppReadyProofError(502, 'APP_READY_CONTROL_VALIDATION_INVALID');
  }
  return new Map(validation.results.map((result) => [result.case_id, result]));
}

function controlAccepted(results, operation, caseId, expected) {
  const result = results.get(`${operation.operation_id}_${caseId}`);
  return Boolean(result && result.operation_id === operation.operation_id && result.accepted === expected);
}

function privateCaseMap(probe, fixture) {
  if (probe.status !== 200 || probe.body?.ok !== true || !Array.isArray(probe.body.results)
      || probe.body.runtime_marker_sha256 !== fixture.runtime_marker_sha256
      || probe.body.proof_class !== fixture.proof_class) return null;
  return new Map(probe.body.results.map((result) => [result.case_id, result.accepted]));
}

function expectedPrivateCases(map) {
  return Boolean(map
    && map.get('positive') === true
    && map.get('wrong_plane') === false
    && map.get('wrong_agency') === false
    && map.get('wrong_data_plane') === false
    && map.get('stale_route') === false
    && map.get('wrong_operation') === false
    && map.get('forged') === false
    && map.get('missing') === false);
}

async function privateCases(operation, target, other, env) {
  const positive = await signedContext(operation, target, env);
  const wrongPlane = await signedContext(operation, other, env);
  const wrongAgency = await signedContext(operation, target, env, { agency_id: other.agency_id });
  const wrongDataPlane = await signedContext(operation, target, env, { data_plane_id: other.data_plane_id });
  const staleRoute = await signedContext(operation, target, env, { route_version: Number(target.route_version) + 1 });
  const alternate = CANDIDATE_OPERATION_POLICY.find(
    (entry) => entry.data_plane_dispatch_required && entry.operation_id !== operation.operation_id
  );
  const wrongOperation = await signedContext(alternate, target, env);
  const forgedEnvelope = `${positive.signed.envelope.slice(0, -1)}${
    positive.signed.envelope.endsWith('A') ? 'B' : 'A'
  }`;
  return {
    registry: positive.registry,
    outer: positive.signed,
    cases: [
      { case_id: 'positive', envelope: positive.signed.envelope, sha256: positive.signed.sha256 },
      { case_id: 'wrong_plane', envelope: wrongPlane.signed.envelope, sha256: wrongPlane.signed.sha256 },
      { case_id: 'wrong_agency', envelope: wrongAgency.signed.envelope, sha256: wrongAgency.signed.sha256 },
      { case_id: 'wrong_data_plane', envelope: wrongDataPlane.signed.envelope, sha256: wrongDataPlane.signed.sha256 },
      { case_id: 'stale_route', envelope: staleRoute.signed.envelope, sha256: staleRoute.signed.sha256 },
      { case_id: 'wrong_operation', envelope: wrongOperation.signed.envelope, sha256: wrongOperation.signed.sha256 },
      {
        case_id: 'forged', envelope: forgedEnvelope,
        sha256: await candidateRouteContextInternals.sha256Hex(forgedEnvelope)
      },
      { case_id: 'missing' }
    ]
  };
}

async function dispatchRow(operation, fixtures, controlResults, env) {
  const [a, b] = await Promise.all([
    privateCases(operation, fixtures[0], fixtures[1], env),
    privateCases(operation, fixtures[1], fixtures[0], env)
  ]);
  const responses = await Promise.all([
    sendPrivateProbe(a.registry, operation, a.outer, a.cases, env),
    sendPrivateProbe(b.registry, operation, b.outer, b.cases, env),
    sendTamperedPrivateProbe(a.registry, operation, a.outer, env),
    sendPrivateProbe(a.registry, operation, a.outer, a.cases, env, 'UNAVAILABLE'),
    sendPrivateProbe(b.registry, operation, b.outer, b.cases, env, 'UNAVAILABLE')
  ]);
  const [aProbe, bProbe, semanticTamper, aFault, bFault] = await Promise.all(responses.map(boundedResponse));
  const privateA = expectedPrivateCases(privateCaseMap(aProbe, fixtures[0]));
  const privateB = expectedPrivateCases(privateCaseMap(bProbe, fixtures[1]));
  const controlPositive = controlAccepted(controlResults, operation, 'positive_a', true)
    && controlAccepted(controlResults, operation, 'positive_b', true);
  const controlCandidate = controlAccepted(controlResults, operation, 'wrong_candidate', false);
  const controlMembership = controlAccepted(controlResults, operation, 'wrong_membership', false);
  const controlSession = controlAccepted(controlResults, operation, 'wrong_session_epoch', false);
  const controlStale = controlAccepted(controlResults, operation, 'stale_route', false);
  const controlWrongPlane = controlAccepted(controlResults, operation, 'wrong_agency', false)
    && controlAccepted(controlResults, operation, 'wrong_plane', false)
    && controlAccepted(controlResults, operation, 'wrong_audience', false)
    && controlAccepted(controlResults, operation, 'missing', false)
    && controlAccepted(controlResults, operation, 'selector', false);
  const outageIsolation = aFault.status === 503 && bFault.status === 503
    && aFault.body?.error_code === 'APP_READY_PROBE_SIMULATED_UNAVAILABLE'
    && bFault.body?.error_code === 'APP_READY_PROBE_SIMULATED_UNAVAILABLE'
    && aProbe.status === 200 && bProbe.status === 200;
  const semanticIntegrity = semanticTamper.status === 401
    && semanticTamper.body?.error_code === 'CANDIDATE_PRIVATE_SERVICE_AUTH_REQUIRED';
  const pass = privateA && privateB && controlPositive && controlCandidate && controlMembership
    && controlSession && controlStale && controlWrongPlane && outageIsolation && semanticIntegrity;
  return {
    operation_id: operation.operation_id,
    method: operation.method,
    path: operation.path,
    authority_class: operation.authority_class,
    expected_data_plane: 'SERVER_RESOLVED_A_AND_ATTESTED_SYNTHETIC_B',
    positive_result: controlPositive && privateA && privateB ? 'PASS' : 'FAIL',
    wrong_plane_result: controlWrongPlane && privateA && privateB ? 'PASS' : 'FAIL',
    stale_context_result: controlStale && privateA && privateB ? 'PASS' : 'FAIL',
    wrong_candidate_result: controlCandidate ? 'PASS' : 'FAIL',
    wrong_membership_result: controlMembership ? 'PASS' : 'FAIL',
    wrong_session_epoch_result: controlSession ? 'PASS' : 'FAIL',
    semantic_integrity_result: semanticIntegrity ? 'PASS' : 'FAIL',
    outage_isolation_result: outageIsolation ? 'PASS' : 'FAIL',
    fallback_observed: false,
    result: pass ? 'PASS' : 'FAIL'
  };
}

function neutralRow(operation, controlResults) {
  const positive = controlAccepted(controlResults, operation, 'neutral', true);
  const selectorRejected = controlAccepted(controlResults, operation, 'selector', false);
  const contextRejected = controlAccepted(controlResults, operation, 'context', false);
  const pass = positive && selectorRejected && contextRejected;
  return {
    operation_id: operation.operation_id,
    method: operation.method,
    path: operation.path,
    authority_class: operation.authority_class,
    expected_data_plane: 'NO_DATA_PLANE_DISPATCH',
    positive_result: positive ? 'PASS' : 'FAIL',
    wrong_plane_result: contextRejected ? 'PASS' : 'FAIL',
    stale_context_result: 'NOT_APPLICABLE',
    wrong_candidate_result: 'NOT_APPLICABLE',
    wrong_membership_result: 'NOT_APPLICABLE',
    wrong_session_epoch_result: 'NOT_APPLICABLE',
    semantic_integrity_result: selectorRejected ? 'PASS' : 'FAIL',
    outage_isolation_result: 'NOT_APPLICABLE',
    fallback_observed: false,
    result: pass ? 'PASS' : 'FAIL'
  };
}

function exactFixtures(catalogue) {
  if (catalogue?.ok !== true || catalogue?.status !== 'READY'
      || catalogue.policy_semantic_sha256 !== CANDIDATE_OPERATION_POLICY_SEMANTIC_SHA256
      || !Array.isArray(catalogue.fixtures) || catalogue.fixtures.length !== 2) {
    throw new AppReadyProofError(503, 'APP_READY_ROUTE_FIXTURES_UNAVAILABLE');
  }
  const fixtures = [...catalogue.fixtures].sort((left, right) => left.fixture_key.localeCompare(right.fixture_key));
  if (fixtures[0].fixture_key !== 'DATA_PLANE_A' || fixtures[1].fixture_key !== 'DATA_PLANE_B'
      || fixtures[0].proof_class !== 'REAL_TEST_DATA_PLANE'
      || fixtures[1].proof_class !== 'SYNTHETIC_NON_BUSINESS_FIXTURE') {
    throw new AppReadyProofError(503, 'APP_READY_ROUTE_FIXTURES_UNAVAILABLE');
  }
  return fixtures;
}

export async function handleAppReadyTwoPlaneProof(request, env) {
  try {
    if (!proofEnabled(env)) throw new AppReadyProofError(404, 'APP_READY_PROOF_DISABLED');
    if (request.method !== 'GET') throw new AppReadyProofError(405, 'APP_READY_PROOF_METHOD_INVALID');
    const url = new URL(request.url);
    if ([...url.searchParams.keys()].some((key) => key !== 'page')) {
      throw new AppReadyProofError(400, 'APP_READY_PROOF_SELECTOR_FORBIDDEN');
    }
    const pageCount = Math.ceil(CANDIDATE_OPERATION_POLICY.length / PAGE_SIZE);
    const page = Number(url.searchParams.get('page') || '1');
    if (!Number.isSafeInteger(page) || page < 1 || page > pageCount) {
      throw new AppReadyProofError(400, 'APP_READY_PROOF_PAGE_INVALID');
    }
    const catalogue = await controlPlaneRpc(
      env, 'control', 'app_ready_route_fixture_catalogue_v1',
      { p_policy_semantic_sha256: CANDIDATE_OPERATION_POLICY_SEMANTIC_SHA256 }
    );
    const fixtures = exactFixtures(catalogue);
    const operations = CANDIDATE_OPERATION_POLICY.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);
    const cases = operations.flatMap((operation) => controlCases(operation, fixtures));
    const validation = await controlPlaneRpc(
      env, 'control', 'app_ready_route_matrix_validate_v1',
      { p_policy_semantic_sha256: CANDIDATE_OPERATION_POLICY_SEMANTIC_SHA256, p_cases: cases }
    );
    const controlResults = controlResultMap(validation);
    const rows = await Promise.all(operations.map((operation) => (
      operation.data_plane_dispatch_required
        ? dispatchRow(operation, fixtures, controlResults, env)
        : neutralRow(operation, controlResults)
    )));
    const passed = rows.every((row) => row.result === 'PASS');
    return json(passed ? 200 : 503, {
      ok: passed,
      status: passed ? 'PAGE_PASSED' : 'PAGE_FAILED',
      proof_contract: 'CLOUDTMS_APP_READY_TWO_PLANE_63_OPERATION_V1',
      policy_version: 2,
      policy_semantic_sha256: CANDIDATE_OPERATION_POLICY_SEMANTIC_SHA256,
      operation_count: CANDIDATE_OPERATION_POLICY.length,
      page,
      page_count: pageCount,
      page_size: PAGE_SIZE,
      row_count: rows.length,
      rows
    });
  } catch (error) {
    return json(error instanceof AppReadyProofError ? error.status : 503, {
      ok: false,
      error_code: error instanceof AppReadyProofError ? error.code : 'APP_READY_PROOF_UNAVAILABLE'
    });
  }
}

export const appReadyTwoPlaneProofInternals = Object.freeze({
  controlCases,
  fixtureContext,
  mutateUuid,
  proofEnabled
});
