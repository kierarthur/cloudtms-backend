const objectValue = value => (
  value && typeof value === 'object' && !Array.isArray(value) ? value : {}
);

const text = value => String(value == null ? '' : value).trim();
const sha256 = value => text(value).replace(/^\\x/i, '').toLowerCase();
const trueValue = value => value === true || text(value).toLowerCase() === 'true';

async function readExact(fetchImpl, url, headers) {
  const response = await fetchImpl(url, { method: 'GET', headers });
  if (!response.ok) return { ok: false, rows: [] };
  const rows = await response.json().catch(() => []);
  return { ok: Array.isArray(rows) && rows.length === 1, rows: Array.isArray(rows) ? rows : [] };
}

export async function candidatePaperProviderAuthorityCurrent({
  env,
  claimedRow,
  currentLeaseToken,
  fetchImpl = fetch,
  headers
}) {
  const claimedScope = objectValue(claimedRow && claimedRow.payment_scope_json);
  const workflowId = text(claimedScope.candidate_workflow_id);
  if (!workflowId) return { candidate_bound: false, authorised: true };

  const generation = Number(claimedScope.candidate_workflow_generation);
  const manifestHash = sha256(claimedScope.paper_return_manifest_sha256);
  const outboxId = text(claimedRow && claimedRow.id);
  if (!env || !text(env.SUPABASE_URL) || !outboxId
      || !Number.isSafeInteger(generation) || generation < 1
      || !/^[0-9a-f]{64}$/.test(manifestHash)) {
    return { candidate_bound: true, authorised: false, reason: 'CANDIDATE_PAPER_PROVIDER_BINDING_INVALID' };
  }

  const mailResult = await readExact(
    fetchImpl,
    `${env.SUPABASE_URL}/rest/v1/mail_outbox?id=eq.${encodeURIComponent(outboxId)}`
      + '&select=id,type,status,sent_at,attempt_lease_token,context_kind,context_id,attachments,payment_scope_json&limit=2',
    headers
  );
  if (!mailResult.ok) {
    return { candidate_bound: true, authorised: false, reason: 'CANDIDATE_PAPER_PROVIDER_MAIL_STALE' };
  }
  const mail = mailResult.rows[0];
  const scope = objectValue(mail.payment_scope_json);
  const attachments = Array.isArray(mail.attachments) ? mail.attachments : [];
  const mailCurrent = text(mail.type).toUpperCase() === 'TIMESHEET_QR'
    && text(mail.status).toUpperCase() === 'QUEUED'
    && !mail.sent_at
    && text(mail.attempt_lease_token) === text(currentLeaseToken)
    && text(mail.context_kind).toLowerCase() === 'timesheets'
    && text(scope.candidate_workflow_id) === workflowId
    && Number(scope.candidate_workflow_generation) === generation
    && sha256(scope.paper_return_manifest_sha256) === manifestHash
    && !trueValue(scope.candidate_paper_generation_retired)
    && trueValue(scope.candidate_paper_pack_ready)
    && !trueValue(scope.mail_held_until_pdf_rendered)
    && attachments.length > 0;
  if (!mailCurrent) {
    return { candidate_bound: true, authorised: false, reason: 'CANDIDATE_PAPER_PROVIDER_MAIL_STALE' };
  }

  const workflowResult = await readExact(
    fetchImpl,
    `${env.SUPABASE_URL}/rest/v1/candidate_submission_workflows?id=eq.${encodeURIComponent(workflowId)}`
      + '&select=id,route,state,generation,target_timesheet_id,anchor_timesheet_id,paper_return_manifest_sha256&limit=2',
    headers
  );
  if (!workflowResult.ok) {
    return { candidate_bound: true, authorised: false, reason: 'CANDIDATE_PAPER_PROVIDER_WORKFLOW_STALE' };
  }
  const workflow = workflowResult.rows[0];
  const workflowTimesheetId = text(
    workflow.target_timesheet_id || workflow.anchor_timesheet_id
  );
  const workflowCurrent = text(workflow.route).toUpperCase() === 'PAPER'
    && text(workflow.state).toUpperCase() === 'AWAITING_PAPER_RETURN'
    && Number(workflow.generation) === generation
    && sha256(workflow.paper_return_manifest_sha256) === manifestHash
    && workflowTimesheetId === text(mail.context_id);

  return {
    candidate_bound: true,
    authorised: workflowCurrent,
    reason: workflowCurrent ? null : 'CANDIDATE_PAPER_PROVIDER_WORKFLOW_STALE'
  };
}
