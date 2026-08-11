const objectValue = value => (
  value && typeof value === 'object' && !Array.isArray(value) ? value : {}
);

const text = value => String(value == null ? '' : value).trim();
const sha256 = value => text(value).replace(/^\\x/i, '').toLowerCase();
async function readEnvironment(fetchImpl, url, headers) {
  const response = await fetchImpl(url, { method: 'GET', headers });
  if (!response.ok) return null;
  const rows = await response.json().catch(() => []);
  if (!Array.isArray(rows) || rows.length !== 1) return null;
  const value = text(rows[0] && rows[0].candidate_app_environment).toUpperCase();
  return ['TEST', 'LIVE'].includes(value) ? value : null;
}

export async function candidatePaperProviderAuthorityCurrent({
  env,
  claimedRow,
  currentLeaseToken,
  fetchImpl = fetch,
  headers,
  nowUtc = () => new Date().toISOString()
}) {
  const claimedScope = objectValue(claimedRow && claimedRow.payment_scope_json);
  const workflowId = text(claimedScope.candidate_workflow_id);
  if (!workflowId) return { candidate_bound: false, authorised: true };

  const generation = Number(claimedScope.candidate_workflow_generation);
  const manifestHash = sha256(claimedScope.paper_return_manifest_sha256);
  const outboxId = text(claimedRow && claimedRow.id);
  const leaseToken = text(currentLeaseToken);
  if (!env || !text(env.SUPABASE_URL) || !outboxId
      || !leaseToken
      || !Number.isSafeInteger(generation) || generation < 1
      || !/^[0-9a-f]{64}$/.test(manifestHash)) {
    return { candidate_bound: true, authorised: false, reason: 'CANDIDATE_PAPER_PROVIDER_BINDING_INVALID' };
  }

  const environment = await readEnvironment(
    fetchImpl,
    `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1`
      + '&select=candidate_app_environment&limit=2',
    headers
  );
  if (!environment) {
    return { candidate_bound: true, authorised: false, reason: 'CANDIDATE_PAPER_PROVIDER_BINDING_INVALID' };
  }

  // The existing mail lease is promoted to a bounded provider-submit permit
  // inside the workflow RPC. That transaction locks and revalidates both the
  // workflow and exact mail row, then renews the permit before this Worker may
  // invoke the external provider. Every lifecycle transition uses the same
  // lease row as its barrier.
  const response = await fetchImpl(
    `${env.SUPABASE_URL}/rest/v1/rpc/candidate_workflow_transition_atomic_v1`,
    {
      method: 'POST',
      headers,
      body: JSON.stringify({
        p_session_id: null,
        p_environment: environment,
        p_workflow_id: workflowId,
        p_action: 'PAPER_PROVIDER_SUBMIT_PERMIT',
        p_expected_generation: generation,
        p_payload: {
          service_paper_provider_submit_permit: true,
          mail_outbox_id: outboxId,
          attempt_lease_token: leaseToken,
          paper_return_manifest_sha256: manifestHash
        },
        p_idempotency_key: null,
        p_now_utc: nowUtc()
      })
    }
  );
  const result = await response.json().catch(() => null);
  const permitCurrent = response.ok
    && objectValue(result).ok === true
    && objectValue(result).provider_submit_permit === true
    && text(result.workflow_id) === workflowId
    && Number(result.generation) === generation
    && text(result.mail_outbox_id) === outboxId;

  return {
    candidate_bound: true,
    authorised: permitCurrent,
    reason: permitCurrent ? null : 'CANDIDATE_PAPER_PROVIDER_SUBMIT_PERMIT_REFUSED'
  };
}
