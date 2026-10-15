// Transport for the existing ten Contract create/date/settings writes.
// Native owner124 validates admin/CAS and derives the fixed key atomically.
export function createContractPresetWriteTransport({ sbRpc, unwrapRpcJsonb }) {
  if (![sbRpc, unwrapRpcJsonb].every(value => typeof value === 'function')) {
    throw new Error('CONTRACT_WRITE_TRANSPORT_DEPENDENCY_REQUIRED');
  }
  return async function contractPresetWriteFetch(env, actorUserId, url, options, context = {}) {
    const fail = (code, status) => new Response(JSON.stringify({ code }), {
      status, headers: { 'content-type': 'application/json' }
    });
    const uuid = value => typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
    try {
      const target = new URL(url), native = new URL(env.SUPABASE_URL);
      const mode = String(options?.method || '').toUpperCase();
      if (!uuid(actorUserId) || target.origin !== native.origin || target.pathname !== '/rest/v1/contracts'
          || !['POST', 'PATCH'].includes(mode) || typeof options?.body !== 'string'
          || options.body.length > 262144
          || [...target.searchParams.keys()].some(key => !['id', 'updated_at'].includes(key))) {
        return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID', 400);
      }
      const values = JSON.parse(options.body);
      if (!values || typeof values !== 'object' || Array.isArray(values) || !Object.keys(values).length) {
        return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID', 400);
      }
      const request = { actor_user_id: actorUserId, mode, values: { ...values } };
      if (mode === 'PATCH') {
        const idFilter = target.searchParams.get('id') || '';
        const id = idFilter.startsWith('eq.') ? idFilter.slice(3) : '';
        if (!uuid(id) || context.policySource || target.searchParams.getAll('id').length !== 1
            || target.searchParams.getAll('updated_at').length > 1) return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID', 400);
        request.contract_id = id;
        const timestampFilter = target.searchParams.get('updated_at');
        if (timestampFilter !== null && (!timestampFilter.startsWith('eq.') || !timestampFilter.slice(3))) return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID', 400);
        const expected = timestampFilter ? timestampFilter.slice(3) : context.expectedUpdatedAt;
        if (!expected) return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_CONTEXT_STALE', 409);
        request.expected_updated_at = expected;
        if (typeof expected !== 'string' || !expected.trim()) return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID', 400);
      } else {
        if ([...target.searchParams.keys()].length) return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID', 400);
        if (context.policySource) {
          if (!uuid(context.policySource.id) || typeof context.policySource.updated_at !== 'string' || !context.policySource.updated_at.trim()) {
            return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID', 400);
          }
          // Only the actual duplicate handler supplies a native source identity.
          // No policy object or fixed approval value crosses this RPC boundary.
          delete request.values.candidate_manager_approval_policy_json;
          request.policy_source_contract_id = context.policySource.id;
          request.policy_source_expected_updated_at = context.policySource.updated_at;
        }
      }
      if (Object.hasOwn(request.values, 'candidate_manager_approval_policy_json')) {
        return fail('CANDIDATE_MANAGER_CONTRACT_POLICY_FIELD_INVALID', 400);
      }
      const raw = await sbRpc(env, 'weekly_source_contract_write_atomic_v1', {
        p_request: request
      }, { timeoutMs: 20000 });
      const row = unwrapRpcJsonb(raw, 'weekly_source_contract_write_atomic_v1');
      if (!row || typeof row !== 'object' || Array.isArray(row) || !uuid(row.id)
          || (mode === 'PATCH' && row.id !== request.contract_id)) {
        return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_RESULT_INVALID', 503);
      }
      return new Response(JSON.stringify([row]), {
        status: mode === 'POST' ? 201 : 200, headers: { 'content-type': 'application/json' }
      });
    } catch (error) {
      if (error instanceof SyntaxError) return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_INVALID', 400);
      if (String(error?.code || error?.json?.code || '').toUpperCase() === 'PGRST202') {
        return fail('CANDIDATE_FIXED_APPROVAL_CONTRACT_DEPENDENCY_UNAVAILABLE', 503);
      }
      const closed = String(error?.json?.message || error?.code || error?.message || '')
        .match(/\b(?:CANDIDATE|WEEKLY_SOURCE|ADMIN)_[A-Z0-9_]+\b/)?.[0] || 'CANDIDATE_FIXED_APPROVAL_CONTRACT_WRITE_FAILED';
      return fail(closed, closed.endsWith('_STALE') ? 409 : closed.includes('ADMIN_REQUIRED') ? 403 : closed.endsWith('_INVALID') ? 400 : 503);
    }
  };
}
