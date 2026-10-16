import { decodeSourcePayDecisionBrowserRequest } from './source-pay-decision-request.mjs';
import { decodeSourcePayDecisionConfirmationPrepared, decodeSourcePayDecisionConfirmationContext, sourcePayDecisionContinuationFromRpcError,
  sourcePayDecisionReadAction } from './source-native-contract.mjs';
import { WeeklyProtectedActionError } from './protected-action-orchestrator.mjs';

// Only the authenticated route supplies actorUserId. The unchanged browser body
// carries no member/assembly/calculation fields. This call creates retained
// native run metadata atomically and performs no native financial publication.
export async function prepareSourcePayDecisionConfirmation({ request, actorUserId, dataRpc }) {
  return confirmationRpc({ request, actorUserId, dataRpc, contextOnly: false });
}

export async function loadSourcePayDecisionConfirmationContext({ request, actorUserId, dataRpc }) {
  return confirmationRpc({ request, actorUserId, dataRpc, contextOnly: true });
}

async function confirmationRpc({ request, actorUserId, dataRpc, contextOnly }) {
  const confirmation = decodeSourcePayDecisionBrowserRequest(request, request?.action);
  if (!['ACCEPT', 'RESUME_FINANCIAL'].includes(confirmation.action)
      || typeof actorUserId !== 'string'
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(actorUserId)
      || typeof dataRpc !== 'function') {
    const error = new Error('Source confirmation preparation is unavailable.');
    error.code = 'SOURCE_PAY_DECISION_PREPARATION_INVALID';error.status = 400;throw error;
  }
  let value;
  const name = contextOnly ? 'weekly_source_pay_decision_confirmation_context_v1' : 'weekly_source_pay_decision_confirmation_prepare_v1';
  try {
    value = await dataRpc(name, { p_request: {
      version: contextOnly ? 'SOURCE_PAY_DECISION_CONFIRMATION_CONTEXT_V1' : 'SOURCE_PAY_DECISION_CONFIRMATION_PREPARE_V1', actor_user_id: actorUserId,
      confirmation_request: confirmation,
    } });
  } catch (error) {
    const continuation = sourcePayDecisionContinuationFromRpcError(error, name);
    if (!continuation || continuation.decision_id !== confirmation.decision_id || continuation.request_id !== confirmation.request_id) throw error;
    throw savedResultRequired(continuation.decision_id, continuation.request_id);
  }
  const prepared = contextOnly ? decodeSourcePayDecisionConfirmationContext(value, confirmation)
    : decodeSourcePayDecisionConfirmationPrepared(value, confirmation);
  if (confirmation.action === 'ACCEPT' && prepared.confirmation_reason !== confirmation.reason.replace(/^ +| +$/g, '')) {
    const error = new Error('The saved Source confirmation reason does not match.');
    error.code = 'SOURCE_NATIVE_RESULT_INVALID';error.status = 502;throw error;
  }
  if (confirmation.decision === 'ACCEPT_FINAL_SOURCE_PAY' && (contextOnly ? prepared.required_members : prepared.members).length) {
    const error = new Error('A positive Source pay decision cannot prepare Unprotect.');
    error.code = 'SOURCE_NATIVE_RESULT_INVALID';error.status = 502;throw error;
  }
  if (contextOnly) return prepared;
  if (prepared.members.some((member) => member.preparation_state === 'CONSUMED')) {
    throw savedResultRequired(prepared.decision_id, prepared.request_id);
  }
  if (prepared.members.some((member) => member.preparation_state === 'STALE' || Date.parse(member.expires_at_utc) <= Date.now())) {
    const error = new WeeklyProtectedActionError('SOURCE_PAY_DECISION_REVIEW_CHANGED',
      'Review the Source pay decision again before continuing.', { retryable: false });
    error.status = 409;throw error;
  }
  return prepared;
}

function savedResultRequired(decisionId, requestId) {
  const error = new WeeklyProtectedActionError('SOURCE_PAY_DECISION_RESULT_REQUIRED',
    'Open the saved Source pay decision to continue.', { retryable: false, decision_id: decisionId,
      request_id: requestId, next_action: sourcePayDecisionReadAction(decisionId, requestId) });
  error.status = 409;return error;
}
