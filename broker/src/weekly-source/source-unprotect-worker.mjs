import { composeWeeklyProtectedTargetSchedule } from './protected-target-schedule.js';
import { buildWeeklyProtectedNativePayload } from './protected-action-orchestrator.mjs';
import { prepareSourcePayDecisionConfirmation } from './source-pay-decision-preparation.mjs';
import { decodeSourceUnprotectReviewContext, normalizeSourceNativeCalculationContext,
  decodeSourceUnprotectPreparationResult, decodeSourcePreviewQuoteContext, decodeSourceUnprotectQuoteContext } from './source-native-contract.mjs';
import { sourceProtectedCalculationContext, buildSourceUnprotectEconomicTarget,
  hashSourceNativeJson } from './source-native-economic.mjs';

const encoder = new TextEncoder();
function fail(code) { const error = new Error(code);error.code = code;error.status = 409;throw error; }
function dependencies(value) {
  if (typeof value?.calculateWeeklySnapshot !== 'function' || typeof value?.dataRpc !== 'function') {
    fail('SOURCE_UNPROTECT_DEPENDENCIES_REQUIRED');
  }
  return value;
}
async function calculate(context, owner) {
  if (context.requires_zero_financial || context.current_entitlement.current_financial === null) {
    fail('SOURCE_NATIVE_PREREQUISITE_NOT_PREPARED');
  }
  const target = composeWeeklyProtectedTargetSchedule({ sourceSegments: context.source_segments, decisions: context.protected_decisions });
  const calculation = await owner({ context: sourceProtectedCalculationContext(context), schedule: target.segments, zeroRoot: false,
    ...(context.next_source_segments !== null ? { retainNextPreviewFacts: true } : {}) });
  return { target, calculation };
}

// The caller obtains operands from the genuine saved assembly/cut, never from
// browser numeric fields. Preview performs the actual native calculator only;
// it allocates no run, C1 component UUID, publication request or financial row.
export async function calculateSourceUnprotectPreviewQuote({ reviewContext, quantityPartition, priorCommitmentCutSha256,
  calculateWeeklySnapshot }) {
  const reviewed = decodeSourceUnprotectReviewContext(reviewContext);
  if (typeof calculateWeeklySnapshot !== 'function') fail('SOURCE_UNPROTECT_DEPENDENCIES_REQUIRED');
  const { target, calculation } = await calculate(reviewed.context, calculateWeeklySnapshot);
  const economics = await buildSourceUnprotectEconomicTarget({ context: reviewed.context, calculation, schedule: target.segments,
    sourceObservationSha256: reviewed.source_observation_sha256, quantityPartition, priorCommitmentCutSha256 });
  return Object.freeze({ member_key: reviewed.member_key, context_sha256: reviewed.context_sha256,
    source_observation_sha256: reviewed.source_observation_sha256, calculation_owner: 'buildWeeklyScheduleSegmentsSnapshot',
    calculation_policy_sha256: economics.calculation_policy_sha256, economic_target: economics.economic_target,
    economic_target_sha256: economics.economic_target_sha256 });
}
export async function calculateSourceUnprotectAssemblyQuote({ assemblyId, decisionId, memberKey, actorUserId, cutId,
  expectedPayCutHash, ...provided }) {
  const owner = dependencies(provided);
  const operands = decodeSourcePreviewQuoteContext(await owner.dataRpc('weekly_source_pay_decision_preview_quote_context_v1', {
    p_request: { version: 'SOURCE_PAY_DECISION_PREVIEW_QUOTE_CONTEXT_V1', assembly_id: assemblyId, decision_id: decisionId,
      member_key: memberKey, actor_user_id: actorUserId },
  }), { assembly_id: assemblyId, decision_id: decisionId, member_key: memberKey, cut_id: cutId,
    prior_commitment_cut_sha256: expectedPayCutHash });
  return calculateSourceUnprotectPreviewQuote({ reviewContext: operands.review_context, quantityPartition: operands.quantity_partition,
    priorCommitmentCutSha256: operands.prior_commitment_cut_sha256, calculateWeeklySnapshot: owner.calculateWeeklySnapshot });
}
async function childKey(decisionId, memberKey, requestId) {
  const digest = await crypto.subtle.digest('SHA-256', encoder.encode(memberKey));
  const hex = [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
  return `source-pay-unprotect-v1:${decisionId}:${hex}:${requestId}`;
}

// Full post-confirmation service composition. Every RPC here is metadata/read
// preparation. Native stage/publish/complete are called later by SQL acceptance
// under its real PREVALIDATED application and one database transaction.
export async function prepareSourcePayDecisionServiceRequest({ request, actorUserId, ...provided }) {
  const owner = dependencies(provided);
  const batch = await prepareSourcePayDecisionConfirmation({ request, actorUserId, dataRpc: owner.dataRpc });
  const references = [];
  for (const member of batch.members) {
    const prepared = member.prepared;
    const quote = decodeSourceUnprotectQuoteContext(await owner.dataRpc('weekly_source_pay_decision_unprotect_quote_context_v1', { p_request: {
      version: 'SOURCE_UNPROTECT_QUOTE_CONTEXT_V1', actor_user_id: actorUserId, decision_id: batch.decision_id,
      review_id: batch.review_id, member_key: member.member_key, accept_or_resume_request_id: batch.request_id,
    } }), { preparation_id: member.preparation_id, decision_id: batch.decision_id, review_id: batch.review_id, member_key: member.member_key,
      accept_or_resume_request_id: batch.request_id, orchestration_run_id: prepared.orchestration_run_id,
      prior_commitment_cut_sha256: request.expected_pay_cut_hash });
    const rawContext = await owner.dataRpc('weekly_exceptional_pay_action_context_v1', { p_request: {
      schema_version: 'WEEKLY_PROTECTED_ACTION_CONTEXT_V1', actor_user_id: actorUserId, family_id: prepared.family_id,
      orchestration_run_id: prepared.orchestration_run_id, source_cycle_id: prepared.source_cycle_id,
      work_event_id: prepared.work_event_id, protected_schedule: prepared.protected_schedule, evidence_timesheet_id: null,
    } });
    const context = normalizeSourceNativeCalculationContext(rawContext, { family_id: prepared.family_id,
      root_timesheet_id: prepared.root_timesheet_id, work_event_id: prepared.work_event_id, orchestration_run_id: prepared.orchestration_run_id });
    if (context.family_bound_version !== prepared.family_bound_version || context.candidate_id !== prepared.candidate_id
        || context.client_id !== prepared.client_id || context.contract_id !== prepared.contract_id
        || context.source_cycle_id !== prepared.source_cycle_id
        || (member.route === 'NEXT') !== (context.next_source_segments !== null)) fail('SOURCE_PAY_DECISION_REVIEW_CHANGED');
    const { target, calculation } = await calculate(context, owner.calculateWeeklySnapshot);
    const economics = await buildSourceUnprotectEconomicTarget({ context, calculation, schedule: target.segments,
      quantityPartition: quote.quantity_partition, priorCommitmentCutSha256: quote.prior_commitment_cut_sha256,
      sourceObservationSha256: quote.source_observation_sha256 });
    if (economics.economic_target_sha256 !== quote.economic_target_sha256) fail('SOURCE_PAY_DECISION_REVIEW_CHANGED');
    const key = await childKey(batch.decision_id, member.member_key, batch.request_id);
    const authored = await buildWeeklyProtectedNativePayload({ context: { ...context, orchestration_run_id: prepared.orchestration_run_id },
      target, calculation, nextRoute: member.route === 'NEXT', actorUserId, reason: batch.confirmation_reason, idempotencyKey: key });
    const payload = { route: member.route, prepare_request: {
      schema_version: 'WEEKLY_PROTECTED_ACTION_PREPARE_V1', actor_user_id: actorUserId, family_id: prepared.family_id,
      source_cycle_id: prepared.source_cycle_id, work_event_id: prepared.work_event_id, action: 'WITHDRAW',
      expected_family_bound_version: prepared.family_bound_version, protected_schedule: null,
      reason: batch.confirmation_reason, idempotency_key: key,
    }, ...(member.route === 'NEXT' ? { publish_request: authored.publish_request }
      : { stage_request: authored.stage_request, complete_request: authored.complete_request }) };
    const payloadHash = await hashSourceNativeJson('SOURCE_UNPROTECT_NATIVE_PAYLOAD_V1', payload);
    const registered = decodeSourceUnprotectPreparationResult(await owner.dataRpc('weekly_source_pay_decision_unprotect_prepare_v1', {
      p_request: { version: 'SOURCE_UNPROTECT_PREPARATION_V1', decision_id: batch.decision_id, member_key: member.member_key,
        review_id: batch.review_id, review_digest: request.review_digest, accept_or_resume_request_id: batch.request_id,
        actor_user_id: actorUserId, native_payload: payload },
    }), { preparation_id: member.preparation_id, decision_id: batch.decision_id, member_key: member.member_key,
      review_id: batch.review_id, accept_or_resume_request_id: batch.request_id, orchestration_run_id: prepared.orchestration_run_id,
      payload_sha256: payloadHash });
    if (Date.parse(registered.expires_at_utc) !== Date.parse(member.expires_at_utc) || Date.parse(registered.expires_at_utc) <= Date.now()) {
      fail('SOURCE_PAY_DECISION_REVIEW_CHANGED');
    }
    references.push(Object.freeze({ member_key: member.member_key, status: 'READY', preparation_id: registered.preparation_id,
      payload_sha256: registered.payload_sha256 }));
  }
  return Object.freeze({ ...request, actor_user_id: actorUserId, preview_assembly_ref: null,
    unprotect_preparations: Object.freeze(references) });
}
