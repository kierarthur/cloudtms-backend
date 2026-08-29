const objectValue = value => (
  value && typeof value === 'object' && !Array.isArray(value) ? value : {}
);

const text = value => String(value == null ? '' : value).trim();
const uuid = value => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(text(value));

async function readEnvironment(fetchImpl, url, headers) {
  const response = await fetchImpl(url, { method: 'GET', headers });
  if (!response.ok) return null;
  const rows = await response.json().catch(() => []);
  if (!Array.isArray(rows) || rows.length !== 1) return null;
  const value = text(rows[0] && rows[0].candidate_app_environment).toUpperCase();
  return ['TEST', 'LIVE'].includes(value) ? value : null;
}

export async function candidateManagerProviderAuthorityCurrent({
  env,
  claimedRow,
  currentLeaseToken,
  fetchImpl = fetch,
  headers,
  nowUtc = () => new Date().toISOString()
}) {
  const scope = objectValue(claimedRow && claimedRow.payment_scope_json);
  if (text(scope.candidate_mail_authority).toUpperCase() !== 'MANAGER_APPROVAL_V1') {
    return { candidate_bound: false, authorised: true };
  }

  const kind = text(scope.candidate_manager_mail_kind).toUpperCase();
  const workflowId = text(scope.candidate_manager_workflow_id);
  const workflowGeneration = Number(scope.candidate_manager_workflow_generation);
  const requestId = text(scope.candidate_approval_request_id);
  const requestGeneration = Number(scope.candidate_approval_request_generation);
  const routeReceiptId = text(scope.candidate_manager_route_receipt_id);
  const routeTicketId = text(scope.candidate_manager_route_ticket_id);
  const routeRevision = Number(scope.candidate_manager_route_revision);
  const routeRegistrationSha256 = text(scope.candidate_manager_route_registration_sha256);
  const outboxId = text(claimedRow && claimedRow.id);
  const leaseToken = text(currentLeaseToken);
  if (!env || !text(env.SUPABASE_URL) || !outboxId || !leaseToken
      || !['INITIAL', 'REMINDER', 'RENEWAL', 'WITHDRAWAL', 'CANCELLATION'].includes(kind)
      || !uuid(workflowId) || !uuid(requestId)
      || !Number.isSafeInteger(workflowGeneration) || workflowGeneration < 1
      || !Number.isSafeInteger(requestGeneration) || requestGeneration < 1
      || (['INITIAL', 'REMINDER', 'RENEWAL'].includes(kind) && (
        !uuid(routeReceiptId) || !uuid(routeTicketId)
        || !Number.isSafeInteger(routeRevision) || routeRevision < 1
        || !/^[0-9a-f]{64}$/.test(routeRegistrationSha256)
      ))
      || scope.candidate_manager_mail_retired === true) {
    return { candidate_bound: true, authorised: false, reason: 'CANDIDATE_MANAGER_PROVIDER_BINDING_INVALID' };
  }

  const environment = await readEnvironment(
    fetchImpl,
    `${env.SUPABASE_URL}/rest/v1/settings_defaults?id=eq.1`
      + '&select=candidate_app_environment&limit=2',
    headers
  );
  if (!environment) {
    return { candidate_bound: true, authorised: false, reason: 'CANDIDATE_MANAGER_PROVIDER_BINDING_INVALID' };
  }

  const response = await fetchImpl(
    `${env.SUPABASE_URL}/rest/v1/rpc/candidate_workflow_transition_atomic_v1`,
    {
      method: 'POST',
      headers,
      body: JSON.stringify({
        p_session_id: null,
        p_environment: environment,
        p_workflow_id: workflowId,
        p_action: 'MANAGER_PROVIDER_SUBMIT_PERMIT',
        p_expected_generation: ['WITHDRAWAL', 'CANCELLATION'].includes(kind) ? null : workflowGeneration,
        p_payload: {
          service_manager_provider_submit_permit: true,
          mail_outbox_id: outboxId,
          attempt_lease_token: leaseToken,
          approval_request_id: requestId,
          approval_request_generation: requestGeneration,
          manager_mail_kind: kind
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
    && text(result.approval_request_id) === requestId
    && text(result.mail_outbox_id) === outboxId
    && Number(result.approval_request_generation) === requestGeneration
    && Number(result.approval_workflow_generation) === workflowGeneration
    && text(result.manager_mail_kind).toUpperCase() === kind;

  return {
    candidate_bound: true,
    authorised: permitCurrent,
    reason: permitCurrent ? null : 'CANDIDATE_MANAGER_PROVIDER_SUBMIT_PERMIT_REFUSED'
  };
}
