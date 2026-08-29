import candidateOperationPolicy from '../policy/candidate-operation-policy.json' with { type: 'json' };

function escapePattern(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function pathPattern(path) {
  return new RegExp(`^${escapePattern(path).replace(/\\\{[^/]+\\\}/g, '[^/]+')}$`);
}

function closedPolicy() {
  if (candidateOperationPolicy?.version !== 2
      || candidateOperationPolicy?.operation_count !== 63
      || candidateOperationPolicy?.routing_authority !== 'SERVER_OWNED_ONLY'
      || !Array.isArray(candidateOperationPolicy.operations)
      || candidateOperationPolicy.operations.length !== 63) {
    throw new Error('CANDIDATE_OPERATION_POLICY_INVALID');
  }
  const ids = new Set();
  const routes = new Set();
  return Object.freeze(candidateOperationPolicy.operations.map((source) => {
    const operationId = String(source?.operation_id || '').trim();
    const method = String(source?.method || '').trim().toUpperCase();
    const path = String(source?.path || '').trim();
    const routeKey = `${method} ${path}`;
    if (!operationId || !method || !path.startsWith('/') || ids.has(operationId) || routes.has(routeKey)
        || source?.client_agency_selector_allowed !== false
        || source?.preserves_business_rpc_meaning !== true) {
      throw new Error('CANDIDATE_OPERATION_POLICY_INVALID');
    }
    ids.add(operationId);
    routes.add(routeKey);
    return Object.freeze({ ...source, method, path, pattern: pathPattern(path) });
  }));
}

export const CANDIDATE_OPERATION_POLICY = closedPolicy();
export const CANDIDATE_OPERATION_POLICY_VERSION = candidateOperationPolicy.version;
export const CANDIDATE_OPERATION_POLICY_SEMANTIC_SHA256 =
  '43d3ea72395354bdac311a29ec15fd290694c1ee128050e48d309ed6a782a478';

const BY_ID = new Map(CANDIDATE_OPERATION_POLICY.map((entry) => [entry.operation_id, entry]));

export function candidateOperationById(operationId) {
  return BY_ID.get(String(operationId == null ? '' : operationId).trim()) || null;
}

export function candidateOperationForRequest(method, pathname) {
  const verb = String(method == null ? '' : method).trim().toUpperCase();
  const path = String(pathname == null ? '' : pathname).trim();
  return CANDIDATE_OPERATION_POLICY.find(
    (entry) => entry.method === verb && entry.pattern.test(path)
  ) || null;
}

export function operationPolicyProofRows() {
  return CANDIDATE_OPERATION_POLICY.map(({ pattern, ...entry }) => ({ ...entry }));
}
