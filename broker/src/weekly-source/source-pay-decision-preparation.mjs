import { decodeSourcePayDecisionBrowserRequest } from './source-pay-decision-request.mjs';
import { decodeSourcePayDecisionConfirmationPrepared, sourcePayDecisionContinuationFromRpcError,
  sourcePayDecisionReadAction } from './source-native-contract.mjs';
import { WeeklyProtectedActionError } from './protected-action-orchestrator.mjs';

// Only the authenticated route supplies actorUserId. The unchanged browser body
// carries no member/assembly/calculation fields. This call creates retained
// native run metadata atomically and performs no native financial publication.
export async function prepareSourcePayDecisionConfirmation({ request, actorUserId, dataRpc }) {
  const confirmation = decodeSourcePayDecisionBrowserRequest(request, request?.action);
  if (!['ACCEPT', 'RESUME_FINANCIAL'].includes(confirmation.action)
      || typeof actorUserId !== 'string'
      || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(actorUserId)
      || typeof dataRpc !== 'function') {
    const error = new Error('Source confirmation preparation is unavailable.');
    error.code = 'SOURCE_PAY_DECISION_PREPARATION_INVALID';error.status = 400;throw error;
  }
  let value;
  const name = 'weekly_source_pay_decision_confirmation_prepare_v1';
  try {
    value = await dataRpc(name, { p_request: {
      version: 'SOURCE_PAY_DECISION_CONFIRMATION_PREPARE_V1', actor_user_id: actorUserId,
      confirmation_request: confirmation,
    } });
  } catch (error) {
    const continuation = sourcePayDecisionContinuationFromRpcError(error, name);
    if (!continuation) throw error;
    throw savedResultRequired(continuation.decision_id, continuation.request_id);
  }
  const prepared = decodeSourcePayDecisionConfirmationPrepared(value, confirmation);
  if (confirmation.decision === 'ACCEPT_FINAL_SOURCE_PAY' && prepared.members.length) {
    const error = new Error('A positive Source pay decision cannot prepare Unprotect.');
    error.code = 'SOURCE_NATIVE_RESULT_INVALID';error.status = 502;throw error;
  }
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
